from functools import lru_cache

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

    instrument_source: str = "static"
    instrument_file: str = "config/instruments.csv"

    candle_interval_seconds: int = 300
    pullback_lookback_bars: int = 20
    pullback_min_retrace: float = 0.25
    pullback_max_retrace: float = 0.618
    pullback_min_trend_strength: float = 0.0

    alert_webhook_url: str = ""
    alert_cooldown_seconds: int = 300


@lru_cache
def get_settings() -> Settings:
    return Settings()
