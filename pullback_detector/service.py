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
    deadline = asyncio.get_running_loop().time() + settings.live_duration_seconds + 5
    try:
        while not task.done():
            await validator.poll()
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                await validator.poll()
                snapshot = validator._canonical_snapshot()
                if snapshot is None:
                    snapshot = {"validation_timeout": True, "validation_reason": validator.reason, "deployment_sha": validator.sha}
                snapshot["validation_timeout"] = True
                snapshot["validation_reason"] = validator.reason
                return snapshot
            await asyncio.sleep(min(2, remaining))
        try:
            report = await task
        except Exception as exc:
            await validator.fail(f"RUN_LIVE_EXCEPTION:{type(exc).__name__}:{exc}")
            raise
        await validator.poll()
        return report
    finally:
        validator.stop()
