import asyncio
import logging

from .config import get_settings
from .service import run_live


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger = logging.getLogger(__name__)
    if not settings.dhan_access_token or not settings.dhan_client_id:
        logger.warning("Dhan credentials are not configured; refusing to start live ingestion")
        return
    asyncio.run(run_live(settings))


if __name__ == "__main__":
    main()
