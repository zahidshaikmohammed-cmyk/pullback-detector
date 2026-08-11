from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.detector import PullbackDetector
from pullback_detector.models import Candle


def candle(i, close, high=None, low=None, volume=1000):
    start = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc) + timedelta(minutes=5 * i)
    close = Decimal(str(close))
    high = Decimal(str(high if high is not None else close + 1))
    low = Decimal(str(low if low is not None else close - 1))
    return Candle(25, start, start + timedelta(minutes=5), close, high, low, close, volume, True, 300)


def make_detector():
    return PullbackDetector(
        instrument_id=25,
        config={**PullbackDetector.default_config(), "live_mode": False, "min_history": 5, "atr_period": 3},
        audit_root="/tmp/pullback-v2-detector-tests",
    )


def test_v2_anatomy_is_available_before_signal():
    detector = make_detector()
    detector.update(candle(0, 100, 102, 99))
    anatomy = detector.anatomy()
    assert anatomy["instrument_id"] == 25
    assert anatomy["state"] == "WATCHING"
    assert anatomy["detection_phase"] == "WATCHING"
    assert anatomy["current_price"] == Decimal("100")
    assert anatomy["history_bars"] == 1


def test_v2_anatomy_tracks_completed_5m_history():
    detector = make_detector()
    for i, close in enumerate([100, 103, 106, 104, 105]):
        detector.update(candle(i, close, close + 1, close - 1))
    anatomy = detector.anatomy()
    assert anatomy["instrument_id"] == 25
    assert anatomy["history_bars"] == 5
    assert anatomy["experimental_v2"] is True
    assert anatomy["state"] in {"WATCHING", "IMPULSE_DETECTED", "IMPULSE_VALIDATED", "PULLBACK_DEVELOPING", "HEALTHY_CANDIDATE", "TRIGGER_PENDING", "FAILED"}
