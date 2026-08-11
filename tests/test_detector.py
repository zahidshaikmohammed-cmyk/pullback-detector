from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.detector import PullbackDetector
from pullback_detector.models import Candle


def candle(i, close):
    start = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc) + timedelta(minutes=5 * i)
    value = Decimal(str(close))
    return Candle(1, start, start + timedelta(minutes=5), value, value, value, value, 100)


def test_detects_long_pullback():
    detector = PullbackDetector(lookback_bars=5, min_retrace=0.2, max_retrace=0.7)
    for i, close in enumerate([100, 105, 110, 108, 107]):
        signal = detector.update(candle(i, close))
    assert signal is not None
    assert signal.direction == "LONG"
    assert 0.2 <= signal.retracement <= 0.7


def test_no_signal_without_retrace():
    detector = PullbackDetector(lookback_bars=5)
    signal = None
    for i, close in enumerate([100, 105, 110, 111, 112]):
        signal = detector.update(candle(i, close))
    assert signal is None
