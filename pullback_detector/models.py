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
    symbol: str | None = None
    instrument_type: str | None = None
    source: str = "DHAN"
    validation_status: str = "VALIDATED"
    sequence: int | None = None


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
    tick_count: int = 0
    source_event_count: int = 0


@dataclass(frozen=True)
class PullbackSignal:
    instrument_id: int
    timestamp: datetime
    direction: str
    impulse_start: Decimal
    impulse_end: Decimal
    retracement: float
    trigger_price: Decimal
    invalidation_level: Decimal
    confidence_score: float
    experimental_v1: bool = True
    reason: str = ""
    signal_id: str | None = None
    health_score: int | None = None
    classification: str = ""
    session: str = ""
    impulse_range: Decimal | None = None
    impulse_atr_multiple: float | None = None
    impulse_efficiency: float | None = None
    directional_candle_ratio: float | None = None
    countertrend_excursion: float | None = None
    pullback_duration_candles: int | None = None
    pullback_speed: float | None = None
    pullback_efficiency: float | None = None
    volume_ratio: float | None = None
    impulse_high: Decimal | None = None
    impulse_low: Decimal | None = None
    protected_level: Decimal | None = None
