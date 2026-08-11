from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Instrument:
    security_id: int
    exchange_segment: str
    symbol: str
    trading_symbol: str
    instrument_type: str
    series: str = ""
    isin: str = ""
    source: str = "dhan_scrip_master"


@dataclass(frozen=True)
class Tick:
    instrument_id: int
    timestamp: datetime
    price: Decimal
    quantity: int = 0
    exchange_segment: str = "NSE_EQ"
    cumulative_volume: int | None = None
    feed_response_code: int = 2
    source_timestamp: datetime | None = None
    source_clock_skew_seconds: float | None = None


@dataclass(frozen=True)
class Candle:
    instrument_id: int
    start: datetime
    end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    complete: bool = False
    timeframe_seconds: int = 300


@dataclass(frozen=True)
class PullbackSignal:
    instrument_id: int
    timestamp: datetime
    direction: str
    impulse_start: Decimal
    impulse_end: Decimal
    retracement: float
    score: float
    reason: str
