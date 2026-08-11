from collections import deque
from decimal import Decimal

from .models import Candle, PullbackSignal


class PullbackDetector:
    """Rule-based baseline detector; thresholds are explicit and backtestable."""

    def __init__(self, lookback_bars=20, min_retrace=0.25, max_retrace=0.618, min_trend_strength=0.0):
        if lookback_bars < 3:
            raise ValueError("lookback_bars must be >= 3")
        if not 0 <= min_retrace <= max_retrace <= 1:
            raise ValueError("retrace bounds must satisfy 0 <= min <= max <= 1")
        self.lookback_bars = lookback_bars
        self.min_retrace = min_retrace
        self.max_retrace = max_retrace
        self.min_trend_strength = min_trend_strength
        self.history: deque[Candle] = deque(maxlen=lookback_bars)

    def update(self, candle: Candle) -> PullbackSignal | None:
        self.history.append(candle)
        if len(self.history) < 3:
            return None
        bars = list(self.history)
        impulse_start = bars[0].close
        impulse_end = bars[-2].close
        impulse = impulse_end - impulse_start
        if impulse == 0:
            return None
        retrace = float((impulse_end - bars[-1].close) / impulse) if impulse > 0 else float((bars[-1].close - impulse_end) / (-impulse))
        if not self.min_retrace <= retrace <= self.max_retrace:
            return None
        direction = "LONG" if impulse > 0 else "SHORT"
        strength = abs(float(impulse / impulse_start)) if impulse_start else 0.0
        if strength < self.min_trend_strength:
            return None
        score = min(1.0, retrace / self.max_retrace)
        return PullbackSignal(
            instrument_id=candle.instrument_id,
            timestamp=candle.end,
            direction=direction,
            impulse_start=Decimal(impulse_start),
            impulse_end=Decimal(impulse_end),
            retracement=retrace,
            score=score,
            reason=f"{direction.lower()} impulse followed by {retrace:.1%} retracement",
        )
