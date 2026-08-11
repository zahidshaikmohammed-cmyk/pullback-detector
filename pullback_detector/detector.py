from pathlib import Path

from .healthy_pullback_v2 import HealthyPullbackV2
from .v1_detector import V1PullbackDetector


class PullbackDetector(HealthyPullbackV2):
    """Production entry point for deterministic Healthy Pullback Qualification Engine V2."""

    LABEL = "EXPERIMENTAL_V2_NOT_PROFITABILITY_VALIDATED"

    def __init__(self, instrument_id=None, config=None, audit_root="data/runtime", rules_path="config/pullback_rules.yaml", **overrides):
        if config is None:
            config = HealthyPullbackV2.default_config()
            path = Path(rules_path)
            if path.exists():
                try:
                    import yaml
                    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
                    config.update(loaded)
                except Exception:
                    pass
        config.update(overrides)
        self._deferred_instrument_id = instrument_id
        if instrument_id is None:
            self._bootstrap_config = config
            self._bootstrap_audit_root = audit_root
            self._bootstrapped = False
        else:
            super().__init__(instrument_id, config, audit_root)
            self._bootstrapped = True

    def update(self, candle):
        if not self._bootstrapped:
            self._deferred_instrument_id = candle.instrument_id
            super().__init__(self._deferred_instrument_id, self._bootstrap_config, self._bootstrap_audit_root)
            self._bootstrapped = True
        return super().update(candle)


__all__ = ["PullbackDetector", "HealthyPullbackV2", "V1PullbackDetector"]
