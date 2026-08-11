from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from .models import PullbackSignal


@dataclass(frozen=True)
class BacktestResult:
    signals: int
    wins: int
    losses: int
    win_rate: float
    total_return: float


def evaluate_signals(signals: list[PullbackSignal], candles: pd.DataFrame, horizon_bars: int = 3) -> BacktestResult:
    if horizon_bars <= 0:
        raise ValueError("horizon_bars must be positive")
    if not signals:
        return BacktestResult(0, 0, 0, 0.0, 0.0)
    indexed = candles.sort_values("timestamp").reset_index(drop=True)
    wins = losses = 0
    total_return = Decimal("0")
    for signal in signals:
        matches = indexed[indexed["timestamp"] >= signal.timestamp]
        if len(matches) <= horizon_bars:
            continue
        entry = Decimal(str(matches.iloc[0]["close"]))
        exit_price = Decimal(str(matches.iloc[horizon_bars]["close"]))
        pnl = (exit_price - entry) / entry
        if signal.direction == "SHORT":
            pnl = -pnl
        total_return += pnl
        if pnl > 0:
            wins += 1
        else:
            losses += 1
    tested = wins + losses
    return BacktestResult(len(signals), wins, losses, wins / tested if tested else 0.0, float(total_return))
