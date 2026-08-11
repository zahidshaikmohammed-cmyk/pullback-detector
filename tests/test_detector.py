from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.v1_detector import V1PullbackDetector
from pullback_detector.models import Candle


def candle(i, close, low=None, high=None):
    start = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc) + timedelta(minutes=5*i)
    value=Decimal(str(close)); low_value=Decimal(str(low if low is not None else close)); high_value=Decimal(str(high if high is not None else close))
    return Candle(1,start,start+timedelta(minutes=5),value,high_value,low_value,value,100)


def test_detects_long_pullback_with_v1_schema():
    detector=V1PullbackDetector(lookback_bars=5,min_retrace=.2,max_retrace=.7); signal=None
    for i,close in enumerate([100,105,110,108,106.4]): signal=detector.update(candle(i,close,low=close-1,high=close+1))
    assert signal is not None and signal.direction=="LONG" and .2 <= signal.retracement <= .7
    assert signal.trigger_price==Decimal("106.4") and signal.invalidation_level==Decimal("105.4")
    assert 0.0 <= signal.confidence_score <= 1.0 and signal.experimental_v1 is True and "EXPERIMENTAL_V1" in signal.reason


def test_no_signal_without_retrace():
    detector=V1PullbackDetector(lookback_bars=5); signal=None
    for i,close in enumerate([100,105,110,111,112]): signal=detector.update(candle(i,close))
    assert signal is None
