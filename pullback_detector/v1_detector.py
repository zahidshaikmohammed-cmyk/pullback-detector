from collections import deque
from decimal import Decimal

from .models import Candle, PullbackSignal


class V1PullbackDetector:
    """Original experimental V1 retracement detector retained for benchmark comparison."""
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
        self.last_signal = None
        self.last_state = {}

    def update(self, candle):
        self.history.append(candle)
        self.last_state = self._anatomy()
        signal = self._evaluate_signal()
        if signal is not None:
            self.last_signal = signal
            self.last_state = self._anatomy(signal)
        return signal

    def _evaluate_signal(self):
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
        trigger_price = bars[-1].close
        invalidation_level = min(bar.low for bar in bars[-2:]) if direction == "LONG" else max(bar.high for bar in bars[-2:])
        score = min(1.0, retrace / self.max_retrace)
        return PullbackSignal(bars[-1].instrument_id, bars[-1].end, direction, Decimal(impulse_start), Decimal(impulse_end), retrace, trigger_price, Decimal(invalidation_level), score, True, f"{self.LABEL}: {direction.lower()} impulse followed by {retrace:.1%} retracement")

    def _anatomy(self, signal=None):
        bars = list(self.history)
        if not bars:
            return {"instrument_id": None, "detection_phase": "WAITING_FOR_5M_CANDLES"}
        latest = bars[-1]
        if len(bars) < 3:
            return {"instrument_id": latest.instrument_id, "timestamp": latest.end, "detection_phase": "BUILDING_5M_HISTORY", "structural_state": "INSUFFICIENT_HISTORY", "continuation_state": "NOT_EVALUABLE", "volume_behavior": "INSUFFICIENT_HISTORY"}
        impulse_start, impulse_end = bars[0].close, bars[-2].close
        impulse = impulse_end - impulse_start
        direction = "LONG" if impulse > 0 else "SHORT" if impulse < 0 else "NEUTRAL"
        impulse_high, impulse_low = max(b.high for b in bars[:-1]), min(b.low for b in bars[:-1])
        retrace = max(0.0, float((impulse_end-latest.close)/impulse) if impulse > 0 else float((latest.close-impulse_end)/(-impulse)) if impulse < 0 else 0.0)
        prior_volume = [b.volume for b in bars[:-1] if b.volume is not None]
        median_volume = sorted(prior_volume)[len(prior_volume)//2] if prior_volume else None
        volume_behavior = "INSUFFICIENT_HISTORY" if median_volume is None or latest.volume is None else "CONTRACTING" if latest.volume < median_volume*0.8 else "EXPANDING" if latest.volume > median_volume*1.2 else "STABLE"
        in_range = self.min_retrace <= retrace <= self.max_retrace
        phase = "SIGNAL_FIRED" if signal else "NO_IMPULSE" if direction == "NEUTRAL" else "CONTINUATION_READY" if in_range else "PULLBACK_DEVELOPING" if retrace > 0 else "IMPULSE_DETECTED"
        return {"instrument_id": latest.instrument_id, "timestamp": latest.end, "current_price": latest.close, "impulse_magnitude": abs(impulse), "impulse_direction": direction, "impulse_high": impulse_high, "impulse_low": impulse_low, "retracement_depth_pct": retrace*100, "retracement_price": latest.close, "pullback_duration_minutes": max(0,(bars[-2].end-bars[0].start).total_seconds()/60) if retrace>0 else 0, "volume_behavior": volume_behavior, "latest_volume": latest.volume, "median_prior_volume": median_volume, "structural_state": "PULLBACK_STRUCTURE_VALID" if in_range else "TREND_STRUCTURE", "continuation_state": "TRIGGER_CONFIRMED" if signal else "AWAITING_TRIGGER" if in_range else "AWAITING_PULLBACK", "trigger_price": signal.trigger_price if signal else latest.close if in_range else None, "invalidation_price": signal.invalidation_level if signal else None, "confidence": signal.confidence_score if signal else None, "detection_phase": phase, "experimental_v1": True}

    def anatomy(self):
        return dict(self.last_state or self._anatomy())
