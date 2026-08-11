from functools import lru_cache
from decimal import Decimal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Secrets are loaded only from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    timezone: str = "Asia/Kolkata"

    dhan_client_id: str = Field(default="")
    dhan_access_token: str = Field(default="")
    dhan_ws_url: str = "wss://api-feed.dhan.co"

    data_root: str = "data/runtime"
    max_reconnects: int = 5
    max_future_seconds: int = 5
    max_tick_age_seconds: int = 300
    dedupe_capacity: int = 100_000
    min_live_instruments: int = 3
    live_duration_seconds: int = 600

    # Pullback/alert settings remain unused by the Dhan connectivity layer.
    pullback_lookback_bars: int = 20
    pullback_min_retrace: float = 0.25
    pullback_max_retrace: float = 0.618
    pullback_min_trend_strength: float = 0.0
    pullback_target_1_multiple: Decimal = Decimal("1.0")
    pullback_target_2_multiple: Decimal = Decimal("2.0")
    pullback_cooldown_seconds: int = 300
    pullback_expiry_seconds: int = 3600
    alert_webhook_url: str = ""
    alert_cooldown_seconds: int = 300


@lru_cache
def get_settings() -> Settings:
    return Settings()
