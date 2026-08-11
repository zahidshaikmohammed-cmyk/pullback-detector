"""Persistent per-instrument lifecycle management for experimental V1 pullback setups."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

from .models import Candle, PullbackSignal, Tick


TERMINAL_OUTCOMES = {
    "TARGET_1_HIT",
    "TARGET_2_HIT",
    "INVALIDATION_HIT",
    "STRUCTURE_FAILED",
    "EXPIRED",
}


@dataclass(frozen=True)
class SetupSnapshot:
    signal_id: str
    instrument_id: int
    direction: str
    trigger_price: Decimal
    invalidation_price: Decimal
    target_1: Decimal
    target_2: Decimal
    impulse_start: Decimal
    impulse_end: Decimal
    impulse_magnitude: Decimal
    impulse_high: Decimal
    impulse_low: Decimal
    pullback_depth: float
    pullback_price: Decimal
    pullback_duration_seconds: float
    confidence: float
    creation_timestamp: datetime


@dataclass(frozen=True)
class SetupState:
    snapshot: SetupSnapshot
    current_price: Decimal
    current_timestamp: datetime
    status: str = "ACTIVE"
    target_1_hit: bool = False
    target_2_hit: bool = False
    mfe: Decimal = Decimal("0")
    mae: Decimal = Decimal("0")
    closed_timestamp: datetime | None = None
    outcome: str | None = None
    cooldown_until: datetime | None = None


class PullbackLifecycleEngine:
    """One active setup maximum per instrument, with immutable creation snapshots."""

    def __init__(
        self,
        root: str | Path = "data/runtime",
        target_1_multiple: Decimal = Decimal("1.0"),
        target_2_multiple: Decimal = Decimal("2.0"),
        cooldown_seconds: int = 300,
        expiry_seconds: int = 3600,
        now: Callable[[], datetime] | None = None,
    ):
        self.root = Path(root)
        self.events_path = self.root / "setup_events.jsonl"
        self.target_1_multiple = Decimal(str(target_1_multiple))
        self.target_2_multiple = Decimal(str(target_2_multiple))
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.expiry = timedelta(seconds=expiry_seconds)
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.active: dict[int, SetupState] = {}
        self.closed: list[SetupState] = []
        self._cooldowns: dict[int, datetime] = {}
        self._replay()

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value.astimezone(timezone.utc)

    def _append(self, event: dict) -> None:
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=self._jsonable, separators=(",", ":")) + "\n")
            handle.flush()

    @staticmethod
    def _jsonable(value):
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return value

    @staticmethod
    def _dt(value: str | None) -> datetime | None:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None

    @classmethod
    def _snapshot_from_dict(cls, d: dict) -> SetupSnapshot:
        return SetupSnapshot(
            signal_id=d["signal_id"], instrument_id=int(d["instrument_id"]), direction=d["direction"],
            trigger_price=Decimal(d["trigger_price"]), invalidation_price=Decimal(d["invalidation_price"]),
            target_1=Decimal(d["target_1"]), target_2=Decimal(d["target_2"]),
            impulse_start=Decimal(d["impulse_start"]), impulse_end=Decimal(d["impulse_end"]),
            impulse_magnitude=Decimal(d["impulse_magnitude"]), impulse_high=Decimal(d["impulse_high"]),
            impulse_low=Decimal(d["impulse_low"]), pullback_depth=float(d["pullback_depth"]),
            pullback_price=Decimal(d["pullback_price"]), pullback_duration_seconds=float(d["pullback_duration_seconds"]),
            confidence=float(d["confidence"]), creation_timestamp=cls._dt(d["creation_timestamp"]),
        )

    def _replay(self) -> None:
        if not self.events_path.exists():
            return
        for line in self.events_path.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = event.get("event")
            sid = event.get("signal_id")
            if kind == "SETUP_TRIGGERED":
                snapshot = self._snapshot_from_dict(event["snapshot"])
                state = SetupState(snapshot, snapshot.trigger_price, snapshot.creation_timestamp)
                self.active[snapshot.instrument_id] = state
            elif kind == "SETUP_UPDATED" and sid:
                instrument = int(event["instrument_id"])
                state = self.active.get(instrument)
                if state and state.snapshot.signal_id == sid:
                    self.active[instrument] = replace(
                        state,
                        current_price=Decimal(event["current_price"]),
                        current_timestamp=self._dt(event["current_timestamp"]),
                        mfe=Decimal(event["mfe"]), mae=Decimal(event["mae"]),
                    )
            elif kind == "TARGET_1_REACHED":
                instrument = int(event["instrument_id"])
                state = self.active.get(instrument)
                if state and state.snapshot.signal_id == sid:
                    self.active[instrument] = replace(state, target_1_hit=True)
            elif kind == "TARGET_2_REACHED":
                instrument = int(event["instrument_id"])
                state = self.active.get(instrument)
                if state and state.snapshot.signal_id == sid:
                    self.active[instrument] = replace(state, target_2_hit=True)
            elif kind == "SETUP_CLOSED":
                instrument = int(event["instrument_id"])
                state = self.active.pop(instrument, None)
                if state and state.snapshot.signal_id == sid:
                    closed = replace(
                        state,
                        status="CLOSED",
                        closed_timestamp=self._dt(event["closed_timestamp"]),
                        outcome=event["outcome"],
                        current_price=Decimal(event["current_price"]),
                        current_timestamp=self._dt(event["current_timestamp"]),
                        mfe=Decimal(event["mfe"]), mae=Decimal(event["mae"]),
                        target_1_hit=bool(event.get("target_1_hit", state.target_1_hit)),
                        target_2_hit=bool(event.get("target_2_hit", state.target_2_hit)),
                    )
                    self.closed.append(closed)
                    cooldown_until = self._dt(event.get("cooldown_until"))
                    if cooldown_until:
                        self._cooldowns[instrument] = cooldown_until

    def _snapshot(self, signal: PullbackSignal, candle: Candle) -> SetupSnapshot:
        magnitude = abs(signal.impulse_end - signal.impulse_start)
        direction = signal.direction
        if direction == "LONG":
            target_1 = signal.trigger_price + magnitude * self.target_1_multiple
            target_2 = signal.trigger_price + magnitude * self.target_2_multiple
        else:
            target_1 = signal.trigger_price - magnitude * self.target_1_multiple
            target_2 = signal.trigger_price - magnitude * self.target_2_multiple
        return SetupSnapshot(
            signal_id=str(uuid.uuid4()), instrument_id=signal.instrument_id, direction=direction,
            trigger_price=signal.trigger_price, invalidation_price=signal.invalidation_level,
            target_1=target_1, target_2=target_2, impulse_start=signal.impulse_start,
            impulse_end=signal.impulse_end, impulse_magnitude=magnitude,
            impulse_high=max(signal.impulse_start, signal.impulse_end),
            impulse_low=min(signal.impulse_start, signal.impulse_end),
            pullback_depth=signal.retracement, pullback_price=candle.close,
            pullback_duration_seconds=max(0.0, (candle.end - candle.start).total_seconds()),
            confidence=signal.confidence_score, creation_timestamp=self._utc(signal.timestamp),
        )

    def trigger(self, signal: PullbackSignal, candle: Candle) -> SetupState | None:
        now = self._utc(signal.timestamp)
        existing = self.active.get(signal.instrument_id)
        if existing is not None:
            return None
        cooldown_until = self._cooldowns.get(signal.instrument_id)
        if cooldown_until and now < cooldown_until:
            return None
        snapshot = self._snapshot(signal, candle)
        state = SetupState(snapshot, snapshot.trigger_price, snapshot.creation_timestamp)
        self.active[signal.instrument_id] = state
        self._append({"event": "SETUP_TRIGGERED", "signal_id": snapshot.signal_id, "snapshot": asdict(snapshot)})
        return state

    def update_tick(self, tick: Tick) -> list[dict]:
        state = self.active.get(tick.instrument_id)
        if state is None:
            return []
        return self._update(state, tick.price, self._utc(tick.timestamp))

    def update_candle(self, candle: Candle) -> list[dict]:
        state = self.active.get(candle.instrument_id)
        if state is None or not candle.complete:
            return []
        return self._update(state, candle.close, self._utc(candle.end), candle)

    def _update(self, state: SetupState, price: Decimal, timestamp: datetime, candle: Candle | None = None) -> list[dict]:
        snap = state.snapshot
        move = price - snap.trigger_price if snap.direction == "LONG" else snap.trigger_price - price
        adverse = snap.trigger_price - price if snap.direction == "LONG" else price - snap.trigger_price
        mfe = max(state.mfe, move, Decimal("0"))
        mae = max(state.mae, adverse, Decimal("0"))
        state = replace(state, current_price=price, current_timestamp=timestamp, mfe=mfe, mae=mae)
        self.active[snap.instrument_id] = state
        self._append({"event": "SETUP_UPDATED", "signal_id": snap.signal_id, "instrument_id": snap.instrument_id,
                      "current_price": price, "current_timestamp": timestamp, "mfe": mfe, "mae": mae})
        events: list[dict] = []
        if not state.target_1_hit and self._reached(price, snap.target_1, snap.direction):
            state = replace(state, target_1_hit=True)
            self.active[snap.instrument_id] = state
            event = {"event": "TARGET_1_REACHED", "signal_id": snap.signal_id, "instrument_id": snap.instrument_id,
                     "timestamp": timestamp, "price": price, "target": snap.target_1}
            self._append(event); events.append(event)
            return self._close(state, "TARGET_1_HIT", price, timestamp, events)
        if not state.target_2_hit and self._reached(price, snap.target_2, snap.direction):
            state = replace(state, target_2_hit=True)
            self.active[snap.instrument_id] = state
            event = {"event": "TARGET_2_REACHED", "signal_id": snap.signal_id, "instrument_id": snap.instrument_id,
                     "timestamp": timestamp, "price": price, "target": snap.target_2}
            self._append(event); events.append(event)
            return self._close(state, "TARGET_2_HIT", price, timestamp, events)
        if self._reached(price, snap.invalidation_price, "SHORT" if snap.direction == "LONG" else "LONG"):
            return self._close(state, "INVALIDATION_HIT", price, timestamp, events)
        if timestamp - snap.creation_timestamp >= self.expiry:
            return self._close(state, "EXPIRED", price, timestamp, events)
        if candle is not None and self._structure_failed(state, candle):
            return self._close(state, "STRUCTURE_FAILED", price, timestamp, events)
        return events

    @staticmethod
    def _reached(price: Decimal, level: Decimal, direction: str) -> bool:
        return price >= level if direction == "LONG" else price <= level

    @staticmethod
    def _structure_failed(state: SetupState, candle: Candle) -> bool:
        # A completed candle closing through the frozen invalidation is structural failure;
        # the tick-level invalidation path remains the exact price-hit terminal event.
        snap = state.snapshot
        return candle.close < snap.invalidation_price if snap.direction == "LONG" else candle.close > snap.invalidation_price

    def _close(self, state: SetupState, outcome: str, price: Decimal, timestamp: datetime, events: list[dict]) -> list[dict]:
        snap = state.snapshot
        cooldown_until = timestamp + self.cooldown
        closed = replace(state, status="CLOSED", outcome=outcome, closed_timestamp=timestamp,
                         current_price=price, current_timestamp=timestamp, cooldown_until=cooldown_until)
        self.active.pop(snap.instrument_id, None)
        self._cooldowns[snap.instrument_id] = cooldown_until
        event = {"event": "SETUP_CLOSED", "signal_id": snap.signal_id, "instrument_id": snap.instrument_id,
                 "outcome": outcome, "closed_timestamp": timestamp, "current_timestamp": timestamp,
                 "current_price": price, "mfe": closed.mfe, "mae": closed.mae,
                 "target_1_hit": closed.target_1_hit, "target_2_hit": closed.target_2_hit,
                 "cooldown_until": cooldown_until}
        self._append(event)
        self.closed.append(closed)
        events.append(event)
        return events

    def snapshot(self) -> dict:
        return {
            "active": list(self.active.values()),
            "closed": list(reversed(self.closed[-100:])),
        }
