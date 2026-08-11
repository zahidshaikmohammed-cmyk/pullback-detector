from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pullback_detector.models import Tick
from pullback_detector.validation import PacketDeduplicator, normalize_live_tick_clock, validate_tick


def test_duplicate_raw_packet_is_detected():
    d = PacketDeduplicator(max_items=2)
    assert d.seen(b"abc") is False
    assert d.seen(b"abc") is True
    assert d.seen(b"def") is False
    assert d.seen(b"ghi") is False
    assert d.seen(b"abc") is False


def test_stale_tick_rejected():
    received = datetime.now(timezone.utc)
    tick = Tick(1, received - timedelta(minutes=10), Decimal("100"), 1)
    with pytest.raises(ValueError, match="stale"):
        validate_tick(tick, received, max_age_seconds=60)


def test_future_tick_rejected():
    received = datetime.now(timezone.utc)
    tick = Tick(1, received + timedelta(seconds=20), Decimal("100"), 1)
    with pytest.raises(ValueError, match="future"):
        validate_tick(tick, received, max_future_seconds=5)


def test_future_tick_is_normalized_during_nse_session_and_source_time_preserved():
    received = datetime(2026, 8, 11, 5, 30, 0, tzinfo=timezone.utc)  # 11:00 IST
    source = received + timedelta(hours=4)
    tick = Tick(1, source, Decimal("100"), 1)
    normalized, skew = normalize_live_tick_clock(tick, received, max_future_seconds=5)
    assert skew == 14400
    assert normalized.timestamp == received
    assert normalized.source_timestamp == source
    assert normalized.source_clock_skew_seconds == 14400
    validate_tick(normalized, received, max_future_seconds=5)


def test_future_tick_is_not_normalized_outside_nse_session():
    received = datetime(2026, 8, 11, 11, 52, 0, tzinfo=timezone.utc)  # 17:22 IST
    source = received + timedelta(hours=4)
    tick = Tick(1, source, Decimal("100"), 1)
    normalized, skew = normalize_live_tick_clock(tick, received, max_future_seconds=5)
    assert skew is None
    assert normalized.timestamp == tick.timestamp
    assert normalized.source_timestamp == source
