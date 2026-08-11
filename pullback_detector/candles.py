from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from .models import Candle, Tick


class CandleAggregator:
    """Aggregate ticks into fixed wall-clock candles."""

    def __init__(self, interval_seconds: int = 300):
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.interval = interval_seconds
        self._bars: dict[tuple[int, datetime], dict] = defaultdict(dict)

    def _start(self, timestamp: datetime) -> datetime:
        epoch = int(timestamp.timestamp())
        start_epoch = epoch - (epoch % self.interval)
        return datetime.fromtimestamp(start_epoch, tz=timestamp.tzinfo)

    def update(self, tick: Tick) -> Candle:
        start = self._start(tick.timestamp)
        key = (tick.instrument_id, start)
        state = self._bars[key]
        if not state:
            state.update(open=tick.price, high=tick.price, low=tick.price, close=tick.price, volume=0)
        state["high"] = max(state["high"], tick.price)
        state["low"] = min(state["low"], tick.price)
        state["close"] = tick.price
        state["volume"] += tick.quantity
        return Candle(
            instrument_id=tick.instrument_id,
            start=start,
            end=start + timedelta(seconds=self.interval),
            open=Decimal(state["open"]),
            high=Decimal(state["high"]),
            low=Decimal(state["low"]),
            close=Decimal(state["close"]),
            volume=int(state["volume"]),
        )
