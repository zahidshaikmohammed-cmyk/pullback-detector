from pathlib import Path
from datetime import datetime, timezone

from .healthy_pullback_v2 import HealthyPullbackV2
from .v1_detector import V1PullbackDetector


class PullbackDetector(HealthyPullbackV2):
    """Production entry point for deterministic Healthy Pullback V2 plus market context evidence."""

    LABEL = "EXPERIMENTAL_V2_NOT_PROFITABILITY_VALIDATED"

    def __init__(self, instrument_id: int, config=None, audit_root="data/runtime", rules_path="config/pullback_rules.yaml", **overrides):
        config = dict(config or HealthyPullbackV2.default_config())
        path = Path(rules_path)
        if path.exists():
            try:
                import yaml
                config.update(yaml.safe_load(path.read_text(encoding="utf-8")) or {})
            except Exception:
                pass
        config.update(overrides)
        super().__init__(instrument_id, config, audit_root)
        self.market_context: dict = {}

    def set_market_context(self, context: dict | None) -> None:
        self.market_context = dict(context or {})

    def _context_direction_match(self, direction: str) -> float:
        values = [self.market_context.get(k) for k in ("day_trend", "h1_trend", "m15_trend", "m5_trend", "current_trend")]
        target = "BULLISH" if direction == "LONG" else "BEARISH"
        usable = [v for v in values if v and v != "INSUFFICIENT_DATA"]
        if not usable:
            return 0.5
        return sum(str(v).endswith(target) for v in usable) / len(usable)

    def _score(self, imp, p, continuation=False) -> int:
        """V2 multi-factor score. Critical failures remain hard overrides."""
        impulse = 0.5 * min(1.0, imp.atr_multiple / max(1e-9, self.cfg["preferred_impulse_atr"])) + 0.5 * min(1.0, (imp.efficiency + imp.directional_ratio) / 2 / max(1e-9, self.cfg["preferred_impulse_efficiency"]))
        pullback = 0.5 * max(0.0, 1 - min(1.0, p["relative_speed"])) + 0.5 * max(0.0, 1 - min(1.0, p["efficiency"]))
        structure = 1.0
        context_structure = self.market_context.get("structure_state")
        if context_structure in ("BEARISH_STRUCTURE", "BULLISH_STRUCTURE"):
            target = "BULLISH_STRUCTURE" if imp.direction == "LONG" else "BEARISH_STRUCTURE"
            structure = 1.0 if context_structure == target else 0.35
        participation = max(0.0, 1 - min(1.0, p["volume_ratio"] / max(1e-9, self.cfg["volume_reject_ratio"])))
        alignment = self._context_direction_match(imp.direction)
        market_alignment = self.market_context.get("market_alignment")
        market = 0.5 if market_alignment in (None, "INSUFFICIENT_DATA") else {"ALIGNED":1.0,"PARTIALLY_ALIGNED":0.7,"DIVERGING":0.35,"STRONGLY_DIVERGING":0.15}.get(market_alignment,0.5)
        continuation_quality = 1.0 if continuation else 0.0
        score = 20*impulse + 20*pullback + 20*structure + 15*participation + 10*alignment + 5*market + 10*continuation_quality
        return int(round(max(0, min(100, score))))

    def _critical_impulse_gate(self, candle):
        if not self.impulse:
            return None
        checks = (
            (self.impulse.efficiency < self.cfg["min_impulse_efficiency"], "WEAK_IMPULSE", self.impulse.efficiency, self.cfg["min_impulse_efficiency"]),
            (self.impulse.directional_ratio < self.cfg["min_directional_candle_ratio"], "WEAK_IMPULSE", self.impulse.directional_ratio, self.cfg["min_directional_candle_ratio"]),
            (self.impulse.countertrend_excursion > self.cfg["max_impulse_countertrend_excursion"], "IMPULSE_COUNTERTREND_INSTABILITY", self.impulse.countertrend_excursion, self.cfg["max_impulse_countertrend_excursion"]),
        )
        for failed, reason, actual, threshold in checks:
            if failed:
                return self._reject(candle.end, "IMPULSE_QUALITY", reason, actual, threshold)
        return None

    def _stale_gate(self, candle):
        if not self.cfg.get("live_mode", True):
            return None
        age = (datetime.now(timezone.utc) - candle.end.astimezone(timezone.utc)).total_seconds()
        if age > float(self.cfg["stale_seconds"]):
            return self._reject(candle.end, "DATA", "STALE_DATA", age, self.cfg["stale_seconds"])
        return None

    def update(self, candle):
        stale = self._stale_gate(candle)
        if stale is not None:
            return None
        if self.state in ("PULLBACK_DEVELOPING", "HEALTHY_CANDIDATE", "TRIGGER_PENDING") and self.market_context.get("chop_score") is not None and self.market_context["chop_score"] >= 70:
            self._reject(candle.end, "MARKET_CONTEXT", "SEVERE_CHOP", self.market_context["chop_score"], 70)
            return None
        signal = super().update(candle)
        if self._critical_impulse_gate(candle) is not None:
            return None
        return signal


__all__ = ["PullbackDetector", "HealthyPullbackV2", "V1PullbackDetector"]
