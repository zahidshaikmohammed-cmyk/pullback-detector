from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

from .models import Candle, Tick


class CandleAggregator:
    """Deterministic fixed-boundary candle engine consuming validated normalized ticks."""

    def __init__(self, interval_seconds: int = 300, late_tolerance_seconds: int = 0):
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.interval = interval_seconds
        self.late_tolerance_seconds = max(0, int(late_tolerance_seconds))
        self._bars: dict[tuple[int, datetime], dict] = defaultdict(dict)
        self._finalized: dict[int, set[datetime]] = defaultdict(set)
        self._seen_events: set[str] = set()
        self.duplicate_ticks = 0
        self.out_of_order_ticks = 0
        self.late_ticks = 0
        self.ignored_late_ticks = 0
        self._last_timestamp: dict[int, datetime] = {}

    @staticmethod
    def _event_key(tick: Tick) -> str:
        raw = "|".join((str(tick.instrument_id), tick.timestamp.astimezone(timezone.utc).isoformat(), str(tick.price), str(tick.quantity), str(tick.cumulative_volume)))
        return sha256(raw.encode("utf-8")).hexdigest()

    def _start(self, timestamp: datetime) -> datetime:
        timestamp = timestamp.astimezone(timezone.utc)
        epoch = int(timestamp.timestamp())
        start_epoch = epoch - (epoch % self.interval)
        return datetime.fromtimestamp(start_epoch, tz=timezone.utc)

    def update(self, tick: Tick) -> Candle:
        if tick.timestamp.tzinfo is None:
            raise ValueError("candle aggregation requires timezone-aware timestamp")
        if tick.price <= 0 or tick.quantity < 0:
            raise ValueError("cannot aggregate invalid tick")
        key_id = self._event_key(tick)
        if key_id in self._seen_events:
            self.duplicate_ticks += 1
            return self._snapshot(tick.instrument_id, self._start(tick.timestamp), complete=False)
        self._seen_events.add(key_id)
        if len(self._seen_events) > 250000:
            self._seen_events = set(list(self._seen_events)[-125000:])
        last = self._last_timestamp.get(tick.instrument_id)
        if last is not None and tick.timestamp < last:
            self.out_of_order_ticks += 1
        self._last_timestamp[tick.instrument_id] = max(last, tick.timestamp) if last else tick.timestamp
        start = self._start(tick.timestamp)
        if start in self._finalized.get(tick.instrument_id, set()):
            age = (self._last_timestamp[tick.instrument_id] - (start + timedelta(seconds=self.interval))).total_seconds()
            self.late_ticks += 1
            if age > self.late_tolerance_seconds:
                self.ignored_late_ticks += 1
                return self._snapshot(tick.instrument_id, start, complete=True)
        state = self._bars[(tick.instrument_id, start)]
        if not state:
            state.update(open=tick.price, high=tick.price, low=tick.price, close=tick.price, volume=0, tick_count=0)
        state["high"] = max(state["high"], tick.price)
        state["low"] = min(state["low"], tick.price)
        state["close"] = tick.price
        state["volume"] += tick.quantity
        state["tick_count"] += 1
        return self._snapshot(tick.instrument_id, start, complete=False)

    def _snapshot(self, instrument_id: int, start: datetime, complete: bool) -> Candle:
        state = self._bars.get((instrument_id, start))
        if not state:
            finalized = self._finalized.get(instrument_id, set())
            if start in finalized:
                return Candle(instrument_id, start, start + timedelta(seconds=self.interval), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), 0, True, self.interval, 0, 0)
            raise ValueError("no candle state for requested interval")
        return Candle(instrument_id, start, start + timedelta(seconds=self.interval), Decimal(state["open"]), Decimal(state["high"]), Decimal(state["low"]), Decimal(state["close"]), int(state["volume"]), complete, self.interval, int(state["tick_count"]), int(state["tick_count"]))

    def flush(self, now: datetime) -> list[Candle]:
        now = now.astimezone(timezone.utc)
        closed: list[Candle] = []
        for (instrument_id, start), state in list(self._bars.items()):
            end = start + timedelta(seconds=self.interval)
            if end <= now:
                candle = Candle(instrument_id, start, end, Decimal(state["open"]), Decimal(state["high"]), Decimal(state["low"]), Decimal(state["close"]), int(state["volume"]), True, self.interval, int(state["tick_count"]), int(state["tick_count"]))
                closed.append(candle)
                self._finalized[instrument_id].add(start)
                del self._bars[(instrument_id, start)]
        closed.sort(key=lambda c: (c.instrument_id, c.start))
        return closed

    def state_snapshot(self) -> dict:
        return {"interval_seconds": self.interval, "open_bars": len(self._bars), "finalized_intervals": sum(len(v) for v in self._finalized.values()), "duplicate_ticks": self.duplicate_ticks, "out_of_order_ticks": self.out_of_order_ticks, "late_ticks": self.late_ticks, "ignored_late_ticks": self.ignored_late_ticks}
