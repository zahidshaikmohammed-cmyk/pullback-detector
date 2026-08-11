from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.detector import PullbackDetector
from pullback_detector.models import Candle


def candle(i, close, low=None, high=None):
    start = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc) + timedelta(minutes=5 * i)
    value = Decimal(str(close))
    low_value = Decimal(str(low if low is not None else close))
    high_value = Decimal(str(high if high is not None else close))
    return Candle(1, start, start + timedelta(minutes=5), value, high_value, low_value, value, 100)


def test_detects_long_pullback_with_v1_schema():
    detector = PullbackDetector(lookback_bars=5, min_retrace=0.2, max_retrace=0.7)
    signal = None
    for i, close in enumerate([100, 105, 110, 108, 107]):
        signal = detector.update(candle(i, close, low=close - 1, high=close + 1))
    assert signal is not None
    assert signal.direction == "LONG"
    assert 0.2 <= signal.retracement <= 0.7
    assert signal.trigger_price == Decimal("107")
    assert signal.invalidation_level == Decimal("106")
    assert 0.0 <= signal.confidence_score <= 1.0
    assert signal.experimental_v1 is True
    assert "EXPERIMENTAL_V1" in signal.reason


def test_no_signal_without_retrace():
    detector = PullbackDetector(lookback_bars=5)
    signal = None
    for i, close in enumerate([100, 105, 110, 111, 112]):
        signal = detector.update(candle(i, close))
    assert signal is None
