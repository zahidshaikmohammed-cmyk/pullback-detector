from collections import deque
from datetime import datetime, time, timezone
from hashlib import sha256
from zoneinfo import ZoneInfo
from dataclasses import replace

from .models import Tick

IST = ZoneInfo("Asia/Kolkata")
VALID_MARKET_SEGMENTS = {"NSE_EQ", "IDX_I"}


class PacketDeduplicator:
    """Bounded exact-packet deduplicator for reconnect/replay safety."""

    def __init__(self, max_items: int = 100_000):
        self.max_items = max_items
        self._seen = set()
        self._queue = deque()

    def seen(self, payload: bytes) -> bool:
        key = sha256(payload).digest()
        if key in self._seen:
            return True
        self._seen.add(key)
        self._queue.append(key)
        if len(self._queue) > self.max_items:
            self._seen.remove(self._queue.popleft())
        return False


def validate_tick(tick: Tick, received_at: datetime, max_future_seconds: int = 5, max_age_seconds: int = 300) -> None:
    if tick.instrument_id <= 0:
        raise ValueError("invalid security ID")
    if tick.exchange_segment not in VALID_MARKET_SEGMENTS:
        raise ValueError(f"unexpected exchange segment: {tick.exchange_segment}")
    if tick.price <= 0:
        raise ValueError("invalid non-positive price")
    if tick.quantity < 0:
        raise ValueError("invalid negative trade quantity")
    if tick.timestamp.tzinfo is None:
        raise ValueError("tick timestamp must be timezone-aware")
    received_at = received_at.astimezone(timezone.utc)
    timestamp = tick.timestamp.astimezone(timezone.utc)
    age = (received_at - timestamp).total_seconds()
    if age < -max_future_seconds:
        raise ValueError(f"tick timestamp is in the future by {-age:.1f}s")
    if age > max_age_seconds:
        raise ValueError(f"stale tick is {age:.1f}s old")


def normalize_live_tick_clock(tick: Tick, received_at: datetime, max_future_seconds: int = 5) -> tuple[Tick, float | None]:
    """Normalize a clearly skewed Dhan source clock during NSE cash hours.

    Dhan documents LTT as Unix epoch seconds in the binary packet. A large positive
    offset is therefore treated as a verified source-clock anomaly, not as a timezone
    conversion. The raw source timestamp is copied to ``source_timestamp`` and the
    receipt timestamp is used only for normalized live ordering/candle timing.
    """
    received_at = received_at.astimezone(timezone.utc)
    source = tick.timestamp.astimezone(timezone.utc)
    skew = (source - received_at).total_seconds()
    if skew <= max_future_seconds:
        return replace(tick, source_timestamp=source, source_clock_skew_seconds=None), None

    local_time = received_at.astimezone(IST).time()
    if not (time(9, 15) <= local_time <= time(15, 30)):
        return replace(tick, source_timestamp=source, source_clock_skew_seconds=None), None

    return replace(tick, timestamp=received_at, source_timestamp=source, source_clock_skew_seconds=skew), skew
