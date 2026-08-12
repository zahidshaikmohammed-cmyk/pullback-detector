from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.models import Tick
from pullback_detector.validation import validate_tick_detailed, normalize_live_tick_clock


def tick(ts, segment="NSE_EQ", security_id=25):
    return Tick(security_id, ts, Decimal("100"), 1, segment)


def test_normal_utc_timestamp_is_preserved():
    receive = datetime(2026, 8, 12, 12, 8, 9, tzinfo=timezone.utc)
    source = receive - timedelta(seconds=2)
    normalized, skew = normalize_live_tick_clock(tick(source), receive)
    assert normalized.timestamp == source
    assert normalized.source_timestamp == source
    assert normalized.timestamp_normalization_reason == "RAW_EPOCH_UTC"
    assert skew is None
    assert validate_tick_detailed(normalized, receive).valid


def test_documented_epoch_field_is_decoded_as_epoch_seconds():
    receive = datetime(2026, 8, 12, 12, 8, 9, tzinfo=timezone.utc)
    source = datetime.fromtimestamp(1786536487, tz=timezone.utc)
    normalized, _ = normalize_live_tick_clock(tick(source), receive)
    assert normalized.source_timestamp == source


def test_plus_530_local_epoch_is_corrected_once():
    receive = datetime(2026, 8, 12, 9, 15, 2, tzinfo=timezone.utc)
    actual_utc = datetime(2026, 8, 12, 9, 15, 0, tzinfo=timezone.utc)
    raw_local_epoch = actual_utc + timedelta(hours=5, minutes=30)
    normalized, skew = normalize_live_tick_clock(tick(raw_local_epoch), receive)
    assert normalized.timestamp == actual_utc
    assert normalized.source_timestamp == raw_local_epoch
    assert normalized.timestamp_normalization_reason == "DHAN_NSE_LOCAL_EPOCH_PLUS_IST_OFFSET_CORRECTED"
    assert skew == 19800
    assert validate_tick_detailed(normalized, receive).valid


def test_post_session_local_epoch_is_classified_as_stale_not_future():
    receive = datetime(2026, 8, 12, 12, 8, 9, tzinfo=timezone.utc)
    source_local_epoch = datetime(2026, 8, 12, 15, 59, 42, tzinfo=timezone.utc)
    normalized, _ = normalize_live_tick_clock(tick(source_local_epoch), receive)
    result = validate_tick_detailed(normalized, receive, max_age_seconds=300)
    assert normalized.source_timestamp == source_local_epoch
    assert normalized.timestamp == datetime(2026, 8, 12, 10, 29, 42, tzinfo=timezone.utc)
    assert normalized.timestamp_normalization_reason == "DHAN_NSE_LOCAL_EPOCH_PLUS_IST_OFFSET_CORRECTED"
    assert not result.valid
    assert result.reason_code == "STALE_TIMESTAMP"


def test_genuinely_future_timestamp_is_rejected_fail_closed():
    receive = datetime(2026, 8, 12, 12, 8, 9, tzinfo=timezone.utc)
    source = receive + timedelta(minutes=10)
    normalized, _ = normalize_live_tick_clock(tick(source), receive)
    result = validate_tick_detailed(normalized, receive)
    assert not result.valid
    assert result.reason_code == "FUTURE_TIMESTAMP"


def test_stale_timestamp_is_rejected():
    receive = datetime(2026, 8, 12, 12, 8, 9, tzinfo=timezone.utc)
    source = receive - timedelta(minutes=6)
    normalized, _ = normalize_live_tick_clock(tick(source), receive)
    result = validate_tick_detailed(normalized, receive)
    assert not result.valid
    assert result.reason_code == "STALE_TIMESTAMP"


def test_nifty_and_banknifty_normal_utc_timestamps_are_not_offset_corrected():
    receive = datetime(2026, 8, 12, 9, 15, 2, tzinfo=timezone.utc)
    source = receive - timedelta(seconds=1)
    for security_id in (13, 25):
        normalized, _ = normalize_live_tick_clock(tick(source, "IDX_I", security_id), receive)
        assert normalized.timestamp == source
        assert normalized.timestamp_normalization_reason == "RAW_EPOCH_UTC"


def test_raw_source_timestamp_is_never_overwritten():
    receive = datetime(2026, 8, 12, 9, 15, 2, tzinfo=timezone.utc)
    actual = datetime(2026, 8, 12, 9, 15, 0, tzinfo=timezone.utc)
    raw = actual + timedelta(hours=5, minutes=30)
    normalized, _ = normalize_live_tick_clock(tick(raw), receive)
    assert normalized.source_timestamp == raw
    assert normalized.timestamp == actual
