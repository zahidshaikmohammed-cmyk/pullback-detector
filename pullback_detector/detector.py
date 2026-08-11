from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

from .healthy_pullback_v2 import HealthyPullbackV2
from .v1_detector import V1PullbackDetector


class PullbackDetector(HealthyPullbackV2):
    """Production entry point for deterministic Healthy Pullback Qualification Engine V2."""

    LABEL = "EXPERIMENTAL_V2_NOT_PROFITABILITY_VALIDATED"

    def __init__(self, instrument_id=None, config=None, audit_root="data/runtime", rules_path="config/pullback_rules.yaml", **overrides):
        config = dict(config or HealthyPullbackV2.default_config())
        path = Path(rules_path)
        if path.exists():
            try:
                import yaml
                config.update(yaml.safe_load(path.read_text(encoding="utf-8")) or {})
            except Exception:
                pass
        config.update(overrides)
        self._bootstrap_config = config
        self._bootstrap_audit_root = audit_root
        self._bootstrapped = instrument_id is not None
        if self._bootstrapped:
            super().__init__(instrument_id, config, audit_root)

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
        if not self._bootstrapped:
            self._bootstrapped = True
            super().__init__(candle.instrument_id, self._bootstrap_config, self._bootstrap_audit_root)
        stale = self._stale_gate(candle)
        if stale is not None:
            return None
        signal = super().update(candle)
        if self._critical_impulse_gate(candle) is not None:
            return None
        return signal


__all__ = ["PullbackDetector", "HealthyPullbackV2", "V1PullbackDetector"]
