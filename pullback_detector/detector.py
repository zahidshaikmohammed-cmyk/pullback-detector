from collections import deque
from decimal import Decimal

from .models import Candle, PullbackSignal


class PullbackDetector:
    """Experimental V1 baseline detector with observable 5m pullback anatomy."""

    LABEL = "EXPERIMENTAL_V1_NOT_PROFITABILITY_VALIDATED"

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
        self.last_signal: PullbackSignal | None = None
        self.last_state: dict = {}

    def update(self, candle: Candle) -> PullbackSignal | None:
        self.history.append(candle)
        self.last_state = self._anatomy()
        signal = self._evaluate_signal()
        if signal is not None:
            self.last_signal = signal
            self.last_state = self._anatomy(signal)
        return signal

    def _evaluate_signal(self) -> PullbackSignal | None:
        if len(self.history) < 3:
            return None
        bars = list(self.history)
        impulse_start = bars[0].close
        impulse_end = bars[-2].close
        impulse = impulse_end - impulse_start
        if impulse == 0:
            return None
        retrace = (
            float((impulse_end - bars[-1].close) / impulse)
            if impulse > 0
            else float((bars[-1].close - impulse_end) / (-impulse))
        )
        if not self.min_retrace <= retrace <= self.max_retrace:
            return None
        direction = "LONG" if impulse > 0 else "SHORT"
        strength = abs(float(impulse / impulse_start)) if impulse_start else 0.0
        if strength < self.min_trend_strength:
            return None
        trigger_price = bars[-1].close
        invalidation_level = min(bar.low for bar in bars[-2:]) if direction == "LONG" else max(bar.high for bar in bars[-2:])
        score = min(1.0, retrace / self.max_retrace)
        reason = f"{direction.lower()} impulse followed by {retrace:.1%} retracement; continuation trigger evaluated on latest 5m close"
        return PullbackSignal(
            instrument_id=bars[-1].instrument_id,
            timestamp=bars[-1].end,
            direction=direction,
            impulse_start=Decimal(impulse_start),
            impulse_end=Decimal(impulse_end),
            retracement=retrace,
            trigger_price=trigger_price,
            invalidation_level=Decimal(invalidation_level),
            confidence_score=score,
            experimental_v1=True,
            reason=f"{self.LABEL}: {reason}",
        )

    def _anatomy(self, signal: PullbackSignal | None = None) -> dict:
        bars = list(self.history)
        if not bars:
            return {"instrument_id": None, "detection_phase": "WAITING_FOR_5M_CANDLES"}
        latest = bars[-1]
        if len(bars) < 3:
            return {
                "instrument_id": latest.instrument_id,
                "timestamp": latest.end,
                "detection_phase": "BUILDING_5M_HISTORY",
                "structural_state": "INSUFFICIENT_HISTORY",
                "continuation_state": "NOT_EVALUABLE",
                "volume_behavior": "INSUFFICIENT_HISTORY",
            }

        impulse_start = bars[0].close
        impulse_end = bars[-2].close
        impulse = impulse_end - impulse_start
        direction = "LONG" if impulse > 0 else "SHORT" if impulse < 0 else "NEUTRAL"
        impulse_high = max(bar.high for bar in bars[:-1])
        impulse_low = min(bar.low for bar in bars[:-1])
        retracement_price = latest.close
        retrace = 0.0
        if impulse > 0:
            retrace = float((impulse_end - retracement_price) / impulse)
        elif impulse < 0:
            retrace = float((retracement_price - impulse_end) / (-impulse))
        retrace = max(0.0, retrace)
        duration_minutes = max(0.0, (bars[-2].end - bars[0].start).total_seconds() / 60.0)
        prior_volume = [bar.volume for bar in bars[:-1] if bar.volume is not None]
        latest_volume = latest.volume
        median_volume = sorted(prior_volume)[len(prior_volume) // 2] if prior_volume else None
        if median_volume is None or latest_volume is None:
            volume_behavior = "INSUFFICIENT_HISTORY"
        elif latest_volume < median_volume * 0.8:
            volume_behavior = "CONTRACTING"
        elif latest_volume > median_volume * 1.2:
            volume_behavior = "EXPANDING"
        else:
            volume_behavior = "STABLE"

        in_range = self.min_retrace <= retrace <= self.max_retrace
        if signal is not None:
            phase = "SIGNAL_FIRED"
            structural = "PULLBACK_STRUCTURE_VALID"
            continuation = "TRIGGER_CONFIRMED"
            trigger = signal.trigger_price
            invalidation = signal.invalidation_level
            confidence = signal.confidence_score
        elif direction == "NEUTRAL":
            phase = "NO_IMPULSE"
            structural = "NEUTRAL"
            continuation = "NOT_READY"
            trigger = None
            invalidation = None
            confidence = None
        elif in_range:
            phase = "CONTINUATION_READY"
            structural = "PULLBACK_STRUCTURE_VALID"
            continuation = "AWAITING_TRIGGER"
            trigger = latest.close
            invalidation = min(bar.low for bar in bars[-2:]) if direction == "LONG" else max(bar.high for bar in bars[-2:])
            confidence = min(1.0, retrace / self.max_retrace)
        elif retrace > 0:
            phase = "PULLBACK_DEVELOPING"
            structural = "PULLBACK_IN_PROGRESS"
            continuation = "AWAITING_VALID_RETRACE"
            trigger = None
            invalidation = None
            confidence = None
        else:
            phase = "IMPULSE_DETECTED"
            structural = "TREND_STRUCTURE"
            continuation = "AWAITING_PULLBACK"
            trigger = None
            invalidation = None
            confidence = None

        return {
            "instrument_id": latest.instrument_id,
            "timestamp": latest.end,
            "current_price": latest.close,
            "impulse_magnitude": abs(impulse),
            "impulse_direction": direction,
            "impulse_high": impulse_high,
            "impulse_low": impulse_low,
            "retracement_depth_pct": retrace * 100.0,
            "retracement_price": retracement_price,
            "pullback_duration_minutes": duration_minutes if retrace > 0 else 0.0,
            "volume_behavior": volume_behavior,
            "latest_volume": latest_volume,
            "median_prior_volume": median_volume,
            "structural_state": structural,
            "continuation_state": continuation,
            "trigger_price": trigger,
            "invalidation_price": invalidation,
            "distance_to_trigger": abs(latest.close - trigger) if trigger is not None else None,
            "confidence": confidence,
            "detection_phase": phase,
            "experimental_v1": True,
        }

    def anatomy(self) -> dict:
        return dict(self.last_state or self._anatomy())
