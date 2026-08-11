from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pullback_detector.models import Tick
from pullback_detector.validation import PacketDeduplicator, validate_tick


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
