"""Persistent per-instrument setup lifecycle shared by V1 and V2 signals."""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

TERMINAL_OUTCOMES = {"TARGET_1_HIT", "TARGET_2_HIT", "INVALIDATION_HIT", "STRUCTURE_FAILED", "EXPIRED"}


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
    health_score: int | None = None
    classification: str = ""
    session: str = ""
    impulse_atr_multiple: float | None = None
    impulse_efficiency: float | None = None
    directional_candle_ratio: float | None = None
    countertrend_excursion: float | None = None
    pullback_duration_candles: int | None = None
    pullback_speed: float | None = None
    pullback_efficiency: float | None = None
    volume_ratio: float | None = None
    protected_level: Decimal | None = None


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
    """One active setup maximum per instrument; creation snapshots are immutable and append-only."""

    def __init__(self, root="data/runtime", target_1_multiple=Decimal("1.0"), target_2_multiple=Decimal("2.0"), cooldown_seconds=300, expiry_seconds=3600, now: Callable[[], datetime] | None = None, cooldown_candles: int | None = None, candle_seconds: int = 300):
        self.root = Path(root)
        self.events_path = self.root / "setup_events.jsonl"
        self.target_1_multiple = Decimal(str(target_1_multiple))
        self.target_2_multiple = Decimal(str(target_2_multiple))
        self.cooldown = timedelta(seconds=(cooldown_candles * candle_seconds if cooldown_candles is not None else cooldown_seconds))
        self.expiry = timedelta(seconds=expiry_seconds)
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.active = {}
        self.closed = []
        self._cooldowns = {}
        self._replay()

    @staticmethod
    def _utc(v):
        return v.astimezone(timezone.utc)

    @staticmethod
    def _jsonable(v):
        if isinstance(v, datetime): return v.astimezone(timezone.utc).isoformat()
        if isinstance(v, Decimal): return str(v)
        return v

    def _append(self, event):
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as h:
            h.write(json.dumps(event, default=self._jsonable, separators=(",", ":")) + "\n")
            h.flush(); os.fsync(h.fileno())
        try: os.chmod(self.events_path, 0o600)
        except OSError: pass

    @staticmethod
    def _dt(v): return datetime.fromisoformat(v.replace("Z", "+00:00")) if v else None

    @classmethod
    def _snapshot_from_dict(cls, d):
        return SetupSnapshot(
            signal_id=d["signal_id"], instrument_id=int(d["instrument_id"]), direction=d["direction"],
            trigger_price=Decimal(d["trigger_price"]), invalidation_price=Decimal(d["invalidation_price"]),
            target_1=Decimal(d["target_1"]), target_2=Decimal(d["target_2"]), impulse_start=Decimal(d["impulse_start"]),
            impulse_end=Decimal(d["impulse_end"]), impulse_magnitude=Decimal(d["impulse_magnitude"]),
            impulse_high=Decimal(d["impulse_high"]), impulse_low=Decimal(d["impulse_low"]),
            pullback_depth=float(d["pullback_depth"]), pullback_price=Decimal(d["pullback_price"]),
            pullback_duration_seconds=float(d["pullback_duration_seconds"]), confidence=float(d["confidence"]),
            creation_timestamp=cls._dt(d["creation_timestamp"]), health_score=d.get("health_score"),
            classification=d.get("classification", ""), session=d.get("session", ""),
            impulse_atr_multiple=d.get("impulse_atr_multiple"), impulse_efficiency=d.get("impulse_efficiency"),
            directional_candle_ratio=d.get("directional_candle_ratio"), countertrend_excursion=d.get("countertrend_excursion"),
            pullback_duration_candles=d.get("pullback_duration_candles"), pullback_speed=d.get("pullback_speed"),
            pullback_efficiency=d.get("pullback_efficiency"), volume_ratio=d.get("volume_ratio"),
            protected_level=Decimal(d["protected_level"]) if d.get("protected_level") is not None else None,
        )

    def _replay(self):
        if not self.events_path.exists(): return
        for line in self.events_path.read_text(encoding="utf-8").splitlines():
            try: e = json.loads(line)
            except json.JSONDecodeError: continue
            k, sid = e.get("event"), e.get("signal_id")
            iid = int(e["instrument_id"]) if e.get("instrument_id") is not None else None
            if k == "SETUP_TRIGGERED":
                s = self._snapshot_from_dict(e["snapshot"]); self.active[s.instrument_id] = SetupState(s, s.trigger_price, s.creation_timestamp)
            elif k == "SETUP_UPDATED" and sid:
                s = self.active.get(iid)
                if s and s.snapshot.signal_id == sid:
                    self.active[iid] = replace(s, current_price=Decimal(e["current_price"]), current_timestamp=self._dt(e["current_timestamp"]), mfe=Decimal(e["mfe"]), mae=Decimal(e["mae"]))
            elif k in {"TARGET_1_REACHED", "TARGET_2_REACHED"}:
                s = self.active.get(iid)
                if s and s.snapshot.signal_id == sid:
                    self.active[iid] = replace(s, **({"target_1_hit": True} if k == "TARGET_1_REACHED" else {"target_2_hit": True}))
            elif k == "SETUP_CLOSED":
                s = self.active.pop(iid, None)
                if s and s.snapshot.signal_id == sid:
                    c = replace(s, status="CLOSED", closed_timestamp=self._dt(e["closed_timestamp"]), outcome=e["outcome"], current_price=Decimal(e["current_price"]), current_timestamp=self._dt(e["current_timestamp"]), mfe=Decimal(e["mfe"]), mae=Decimal(e["mae"]), target_1_hit=bool(e.get("target_1_hit", s.target_1_hit)), target_2_hit=bool(e.get("target_2_hit", s.target_2_hit)))
                    self.closed.append(c); cd = self._dt(e.get("cooldown_until"))
                    if cd: self._cooldowns[iid] = cd

    def _snapshot(self, signal, candle):
        r = abs(signal.trigger_price - signal.invalidation_level)
        if r == 0: r = abs(signal.impulse_end - signal.impulse_start)
        if signal.direction == "LONG": t1 = signal.trigger_price + r * self.target_1_multiple; t2 = signal.trigger_price + r * self.target_2_multiple
        else: t1 = signal.trigger_price - r * self.target_1_multiple; t2 = signal.trigger_price - r * self.target_2_multiple
        return SetupSnapshot(
            signal_id=signal.signal_id or str(uuid.uuid4()), instrument_id=signal.instrument_id, direction=signal.direction,
            trigger_price=signal.trigger_price, invalidation_price=signal.invalidation_level, target_1=t1, target_2=t2,
            impulse_start=signal.impulse_start, impulse_end=signal.impulse_end, impulse_magnitude=signal.impulse_range or abs(signal.impulse_end-signal.impulse_start),
            impulse_high=signal.impulse_high or max(signal.impulse_start, signal.impulse_end), impulse_low=signal.impulse_low or min(signal.impulse_start, signal.impulse_end),
            pullback_depth=signal.retracement, pullback_price=candle.close,
            pullback_duration_seconds=float((candle.end-candle.start).total_seconds()), confidence=signal.confidence_score,
            creation_timestamp=self._utc(signal.timestamp), health_score=signal.health_score, classification=signal.classification,
            session=signal.session, impulse_atr_multiple=signal.impulse_atr_multiple, impulse_efficiency=signal.impulse_efficiency,
            directional_candle_ratio=signal.directional_candle_ratio, countertrend_excursion=signal.countertrend_excursion,
            pullback_duration_candles=signal.pullback_duration_candles, pullback_speed=signal.pullback_speed,
            pullback_efficiency=signal.pullback_efficiency, volume_ratio=signal.volume_ratio, protected_level=signal.protected_level,
        )

    def trigger(self, signal, candle):
        ts = self._utc(signal.timestamp)
        if signal.instrument_id in self.active: return None
        cd = self._cooldowns.get(signal.instrument_id)
        if cd and ts < cd: return None
        snap = self._snapshot(signal, candle)
        state = SetupState(snap, snap.trigger_price, snap.creation_timestamp)
        self.active[snap.instrument_id] = state
        self._append({"event": "SETUP_TRIGGERED", "signal_id": snap.signal_id, "snapshot": asdict(snap)})
        return state

    def update_tick(self, tick):
        s = self.active.get(tick.instrument_id)
        return [] if s is None else self._update(s, tick.price, self._utc(tick.timestamp))

    def update_candle(self, candle):
        s = self.active.get(candle.instrument_id)
        return [] if s is None or not candle.complete else self._update(s, candle.close, self._utc(candle.end), candle)

    def _update(self, state, price, timestamp, candle=None):
        snap = state.snapshot
        move = price-snap.trigger_price if snap.direction == "LONG" else snap.trigger_price-price
        adverse = snap.trigger_price-price if snap.direction == "LONG" else price-snap.trigger_price
        state = replace(state, current_price=price, current_timestamp=timestamp, mfe=max(state.mfe, move, Decimal("0")), mae=max(state.mae, adverse, Decimal("0")))
        self.active[snap.instrument_id] = state
        self._append({"event": "SETUP_UPDATED", "signal_id": snap.signal_id, "instrument_id": snap.instrument_id, "current_price": price, "current_timestamp": timestamp, "mfe": state.mfe, "mae": state.mae})
        events = []
        if not state.target_1_hit and self._reached(price, snap.target_1, snap.direction):
            state = replace(state, target_1_hit=True); self.active[snap.instrument_id] = state
            e = {"event":"TARGET_1_REACHED","signal_id":snap.signal_id,"instrument_id":snap.instrument_id,"timestamp":timestamp,"price":price,"target":snap.target_1}; self._append(e); events.append(e)
            return self._close(state,"TARGET_1_HIT",price,timestamp,events)
        if not state.target_2_hit and self._reached(price, snap.target_2, snap.direction):
            state = replace(state, target_2_hit=True); self.active[snap.instrument_id] = state
            e = {"event":"TARGET_2_REACHED","signal_id":snap.signal_id,"instrument_id":snap.instrument_id,"timestamp":timestamp,"price":price,"target":snap.target_2}; self._append(e); events.append(e)
            return self._close(state,"TARGET_2_HIT",price,timestamp,events)
        if candle is not None and self._structure_failed(state,candle): return self._close(state,"STRUCTURE_FAILED",price,timestamp,events)
        if self._reached(price,snap.invalidation_price,"SHORT" if snap.direction == "LONG" else "LONG"):
            e={"event":"INVALIDATION_REACHED","signal_id":snap.signal_id,"instrument_id":snap.instrument_id,"timestamp":timestamp,"price":price,"invalidation":snap.invalidation_price}; self._append(e); events.append(e)
            return self._close(state,"INVALIDATION_HIT",price,timestamp,events)
        if timestamp-snap.creation_timestamp >= self.expiry: return self._close(state,"EXPIRED",price,timestamp,events)
        return events

    @staticmethod
    def _reached(price, level, direction): return price >= level if direction == "LONG" else price <= level

    @staticmethod
    def _structure_failed(state, candle):
        snap=state.snapshot
        return candle.close < snap.invalidation_price if snap.direction == "LONG" else candle.close > snap.invalidation_price

    def _close(self, state, outcome, price, timestamp, events):
        snap=state.snapshot; cd=timestamp+self.cooldown
        closed=replace(state,status="CLOSED",outcome=outcome,closed_timestamp=timestamp,current_price=price,current_timestamp=timestamp,cooldown_until=cd)
        self.active.pop(snap.instrument_id,None); self._cooldowns[snap.instrument_id]=cd; self.closed.append(closed)
        e={"event":"SETUP_CLOSED","signal_id":snap.signal_id,"instrument_id":snap.instrument_id,"outcome":outcome,"closed_timestamp":timestamp,"current_timestamp":timestamp,"current_price":price,"mfe":closed.mfe,"mae":closed.mae,"target_1_hit":closed.target_1_hit,"target_2_hit":closed.target_2_hit,"cooldown_until":cd}
        self._append(e); events.append(e); return events

    def snapshot(self): return {"active":list(self.active.values()),"closed":list(reversed(self.closed[-100:]))}
