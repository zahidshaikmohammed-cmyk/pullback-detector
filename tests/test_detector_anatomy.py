from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.detector import PullbackDetector
from pullback_detector.models import Candle


def candle(i, close, high, low, volume):
    start = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc) + timedelta(minutes=5 * i)
    return Candle(25, start, start + timedelta(minutes=5), Decimal(str(close)), Decimal(str(high)), Decimal(str(low)), Decimal(str(close)), volume, True, 300)


def test_anatomy_comes_from_5m_history_and_is_available_before_signal():
    detector = PullbackDetector(lookback_bars=5, min_retrace=0.25, max_retrace=0.618)
    detector.update(candle(0, 100, 102, 99, 1000))
    detector.update(candle(1, 110, 112, 109, 900))
    detector.update(candle(2, 106, 108, 105, 500))
    anatomy = detector.anatomy()
    assert anatomy["instrument_id"] == 25
    assert anatomy["impulse_direction"] == "LONG"
    assert anatomy["impulse_high"] == Decimal("112")
    assert anatomy["impulse_low"] == Decimal("99")
    assert anatomy["retracement_price"] == Decimal("106")
    assert anatomy["retracement_depth_pct"] > 0
    assert anatomy["volume_behavior"] in {"CONTRACTING", "STABLE", "EXPANDING"}
    assert anatomy["detection_phase"] in {"PULLBACK_DEVELOPING", "CONTINUATION_READY", "SIGNAL_FIRED"}


def test_anatomy_has_no_signal_only_placeholder_for_unready_state():
    detector = PullbackDetector()
    detector.update(candle(0, 100, 101, 99, 1000))
    state = detector.anatomy()
    assert state["detection_phase"] == "BUILDING_5M_HISTORY"
    assert state["structural_state"] == "INSUFFICIENT_HISTORY"
    assert state["trigger_price"] is None if "trigger_price" in state else True
