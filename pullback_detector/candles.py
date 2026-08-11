from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from .models import Candle, Tick


class CandleAggregator:
    """Aggregate validated trade ticks into fixed wall-clock candles.

    Volume is the sum of last-traded quantities from Quote packets. Dhan's
    Quote packet also contains cumulative day volume; it is retained on the
    Tick for diagnostics but is deliberately not summed into candle volume.
    """

    def __init__(self, interval_seconds: int = 300):
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.interval = interval_seconds
        self._bars: dict[tuple[int, datetime], dict] = defaultdict(dict)

    def _start(self, timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        epoch = int(timestamp.timestamp())
        start_epoch = epoch - (epoch % self.interval)
        return datetime.fromtimestamp(start_epoch, tz=timezone.utc)

    def update(self, tick: Tick) -> Candle:
        if tick.price <= 0 or tick.quantity < 0:
            raise ValueError("cannot aggregate invalid tick")
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
            complete=False,
            timeframe_seconds=self.interval,
        )

    def flush(self, now: datetime) -> list[Candle]:
        """Close every bar whose end time is at or before *now*."""
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        closed: list[Candle] = []
        for (instrument_id, start), state in list(self._bars.items()):
            end = start + timedelta(seconds=self.interval)
            if end <= now:
                closed.append(Candle(
                    instrument_id=instrument_id,
                    start=start,
                    end=end,
                    open=Decimal(state["open"]),
                    high=Decimal(state["high"]),
                    low=Decimal(state["low"]),
                    close=Decimal(state["close"]),
                    volume=int(state["volume"]),
                    complete=True,
                    timeframe_seconds=self.interval,
                ))
                del self._bars[(instrument_id, start)]
        closed.sort(key=lambda c: (c.instrument_id, c.start))
        return closed
