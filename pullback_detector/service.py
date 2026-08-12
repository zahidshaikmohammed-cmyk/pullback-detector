"""Production data-ingestion service boundary.

Pullback detection and alerting are intentionally not invoked in this stage.
"""

import asyncio

from .config import Settings
from .live import run_live as run_live_connectivity
from .phase1_validation import Phase1Validator


async def run_live(settings: Settings) -> dict:
    validator = Phase1Validator(settings.data_root)
    validator.start()
    task = asyncio.create_task(run_live_connectivity(settings, duration_seconds=settings.live_duration_seconds))
    try:
        while not task.done():
            await validator.poll()
            await asyncio.sleep(2)
        try:
            report = await task
        except Exception as exc:
            await validator.fail(f"RUN_LIVE_EXCEPTION:{type(exc).__name__}:{exc}")
            raise
        await validator.poll(force=True)
        return report
    finally:
        validator.stop()
