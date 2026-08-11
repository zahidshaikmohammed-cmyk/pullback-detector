from datetime import datetime, timedelta, timezone
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
    assert candle.complete is False


def test_flush_closes_only_completed_bars():
    agg = CandleAggregator(60)
    base = datetime(2026, 8, 11, 9, 15, 10, tzinfo=timezone.utc)
    agg.update(Tick(1, base, Decimal("100"), 10))
    assert agg.flush(base + timedelta(seconds=59)) == []
    closed = agg.flush(base + timedelta(seconds=60))
    assert len(closed) == 1
    assert closed[0].complete is True
    assert closed[0].volume == 10


def test_multiple_timeframes_form_independently():
    one = CandleAggregator(60)
    five = CandleAggregator(300)
    base = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc)
    for seconds, price in ((0, "100"), (65, "101"), (125, "102"), (245, "99"), (305, "103")):
        tick = Tick(1, base + timedelta(seconds=seconds), Decimal(price), 1)
        one.update(tick)
        five.update(tick)
    assert len(one.flush(base + timedelta(seconds=360))) == 5
    assert len(five.flush(base + timedelta(seconds=360))) == 2
