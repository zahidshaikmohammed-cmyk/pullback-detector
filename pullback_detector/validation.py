from collections import deque
from datetime import datetime, timezone, timedelta
from hashlib import sha256
from dataclasses import replace

from .models import Tick

VALID_MARKET_SEGMENTS={"NSE_EQ","IDX_I"}
Dhan_IST_EPOCH_SKEW_SECONDS=19800
Dhan_IST_EPOCH_TOLERANCE_SECONDS=90

class PacketDeduplicator:
    def __init__(self,max_items:int=100_000):self.max_items=max_items;self._seen=set();self._queue=deque()
    def seen(self,payload:bytes)->bool:
        key=sha256(payload).digest()
        if key in self._seen:return True
        self._seen.add(key);self._queue.append(key)
        if len(self._queue)>self.max_items:self._seen.remove(self._queue.popleft())
        return False

def validate_tick(tick:Tick,received_at:datetime,max_future_seconds:int=5,max_age_seconds:int=300)->None:
    if tick.instrument_id<=0:raise ValueError("invalid security ID")
    if tick.exchange_segment not in VALID_MARKET_SEGMENTS:raise ValueError(f"unexpected exchange segment: {tick.exchange_segment}")
    if tick.price<=0:raise ValueError("invalid non-positive price")
    if tick.quantity<0:raise ValueError("invalid negative trade quantity")
    if tick.timestamp.tzinfo is None:raise ValueError("tick timestamp must be timezone-aware")
    received_at=received_at.astimezone(timezone.utc);timestamp=tick.timestamp.astimezone(timezone.utc);age=(received_at-timestamp).total_seconds()
    if age<-max_future_seconds:raise ValueError(f"tick timestamp is in the future by {-age:.1f}s")
    if age>max_age_seconds:raise ValueError(f"stale tick is {age:.1f}s old")

def normalize_live_tick_clock(tick:Tick,received_at:datetime,max_future_seconds:int=5)->tuple[Tick,float|None]:
    """Normalize the observed Dhan +05:30 source-clock encoding exactly once.

    Dhan v2 specifies LTT as Unix epoch seconds. Production packets have shown
    a reproducible representation approximately 19,800 seconds ahead of the
    receive clock. Preserve the raw source timestamp, correct it once, and
    leave all other future timestamps untouched for quarantine by validation.
    """
    received_at=received_at.astimezone(timezone.utc);source=tick.timestamp.astimezone(timezone.utc);skew=(source-received_at).total_seconds()
    if skew<=max_future_seconds:return replace(tick,timestamp=source,source_timestamp=source,source_clock_skew_seconds=None),None
    if abs(skew-Dhan_IST_EPOCH_SKEW_SECONDS)<=Dhan_IST_EPOCH_TOLERANCE_SECONDS:
        corrected=source-timedelta(seconds=Dhan_IST_EPOCH_SKEW_SECONDS);corrected_skew=(corrected-received_at).total_seconds()
        if corrected_skew<=max_future_seconds:
            return replace(tick,timestamp=corrected,source_timestamp=source,source_clock_skew_seconds=skew),skew
    return replace(tick,timestamp=source,source_timestamp=source,source_clock_skew_seconds=None),None
