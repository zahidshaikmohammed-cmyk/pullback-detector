from dataclasses import dataclass, replace
from datetime import datetime, timezone, timedelta
from hashlib import sha256
from collections import deque

from .models import Tick

VALID_MARKET_SEGMENTS = {"NSE_EQ", "IDX_I"}
Dhan_IST_EPOCH_SKEW_SECONDS = 19800
Dhan_IST_EPOCH_TOLERANCE_SECONDS = 90


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reason_code: str | None = None
    actual_value: str | None = None
    threshold: str | None = None


class PacketDeduplicator:
    def __init__(self, max_items: int = 100_000):
        self.max_items = max_items; self._seen = set(); self._queue = deque()
    def seen(self, payload: bytes) -> bool:
        key = sha256(payload).digest()
        if key in self._seen: return True
        self._seen.add(key); self._queue.append(key)
        if len(self._queue) > self.max_items: self._seen.remove(self._queue.popleft())
        return False


def validate_tick_detailed(tick: Tick, received_at: datetime, max_future_seconds: int = 5, max_age_seconds: int = 300) -> ValidationResult:
    try:
        received_at = received_at.astimezone(timezone.utc); timestamp = tick.timestamp.astimezone(timezone.utc)
    except (AttributeError, ValueError): return ValidationResult(False, "INVALID_TIMESTAMP")
    if tick.instrument_id <= 0: return ValidationResult(False, "UNKNOWN_INSTRUMENT", str(tick.instrument_id), ">0")
    if tick.exchange_segment not in VALID_MARKET_SEGMENTS: return ValidationResult(False, "INVALID_SEGMENT", tick.exchange_segment, "NSE_EQ|IDX_I")
    if tick.price.is_nan() or tick.price.is_infinite() or tick.price <= 0: return ValidationResult(False, "INVALID_PRICE", str(tick.price), ">0 finite")
    if tick.quantity < 0: return ValidationResult(False, "INVALID_VOLUME", str(tick.quantity), ">=0")
    if tick.timestamp.tzinfo is None: return ValidationResult(False, "INVALID_TIMESTAMP")
    age = (received_at - timestamp).total_seconds()
    if age < -max_future_seconds: return ValidationResult(False, "FUTURE_TIMESTAMP", f"{-age:.3f}s", f"<= {max_future_seconds}s")
    if age > max_age_seconds: return ValidationResult(False, "STALE_TIMESTAMP", f"{age:.3f}s", f"<= {max_age_seconds}s")
    return ValidationResult(True)


def validate_tick(tick: Tick, received_at: datetime, max_future_seconds: int = 5, max_age_seconds: int = 300) -> None:
    result = validate_tick_detailed(tick, received_at, max_future_seconds, max_age_seconds)
    if not result.valid:
        human = {"FUTURE_TIMESTAMP": "future timestamp", "STALE_TIMESTAMP": "stale timestamp", "INVALID_PRICE": "invalid price", "INVALID_VOLUME": "invalid volume", "INVALID_SEGMENT": "unexpected exchange segment", "UNKNOWN_INSTRUMENT": "unknown instrument", "INVALID_TIMESTAMP": "invalid timestamp"}.get(result.reason_code or "", result.reason_code or "validation failure")
        raise ValueError(f"{result.reason_code}: {human}; actual={result.actual_value} threshold={result.threshold}")


def normalize_live_tick_clock(tick: Tick, received_at: datetime, max_future_seconds: int = 5) -> tuple[Tick, float | None]:
    received_at = received_at.astimezone(timezone.utc); source = tick.timestamp.astimezone(timezone.utc); skew = (source - received_at).total_seconds()
    if skew <= max_future_seconds: return replace(tick, timestamp=source, source_timestamp=source, source_clock_skew_seconds=None, validation_status="NORMALIZED"), None
    if abs(skew - Dhan_IST_EPOCH_SKEW_SECONDS) <= Dhan_IST_EPOCH_TOLERANCE_SECONDS:
        corrected = source - timedelta(seconds=Dhan_IST_EPOCH_SKEW_SECONDS); corrected_skew = (corrected - received_at).total_seconds()
        if corrected_skew <= max_future_seconds: return replace(tick, timestamp=corrected, source_timestamp=source, source_clock_skew_seconds=skew, validation_status="NORMALIZED_CLOCK_SKEW"), skew
    return replace(tick, timestamp=source, source_timestamp=source, source_clock_skew_seconds=None, validation_status="UNNORMALIZED"), None
