from collections import deque
from datetime import datetime, timezone
from hashlib import sha256

from .models import Tick


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
