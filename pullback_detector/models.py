from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Tick:
    instrument_id: int
    timestamp: datetime
    price: Decimal
    quantity: int = 0


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
