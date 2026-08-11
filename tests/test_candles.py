from datetime import datetime, timezone
from decimal import Decimal

from pullback_detector.candles import CandleAggregator
from pullback_detector.models import Tick


def test_aggregator_builds_ohlcv():
    agg = CandleAggregator(300)
    base = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc)
    agg.update(Tick(1, base, Decimal("100"), 10))
    candle = agg.update(Tick(1, base.replace(second=30), Decimal("102"), 5))
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("102")
    assert candle.low == Decimal("100")
    assert candle.close == Decimal("102")
    assert candle.volume == 15
