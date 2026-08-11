"""Production data-ingestion service boundary.

Pullback detection and alerting are intentionally not invoked in this stage.
"""

from .config import Settings
from .live import run_live as run_live_connectivity


async def run_live(settings: Settings) -> dict:
    return await run_live_connectivity(settings, duration_seconds=settings.live_duration_seconds)
