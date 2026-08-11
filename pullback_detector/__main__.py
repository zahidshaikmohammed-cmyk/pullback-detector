import asyncio
import logging
import sys

from .config import get_settings
from .service import run_live


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if not settings.dhan_access_token or not settings.dhan_client_id:
        logging.getLogger(__name__).error("Dhan credentials are not configured; live validation cannot run")
        sys.exit(2)
    try:
        asyncio.run(run_live(settings))
    except Exception:
        logging.getLogger(__name__).exception("live connectivity validation failed")
        sys.exit(2)


if __name__ == "__main__":
    main()
