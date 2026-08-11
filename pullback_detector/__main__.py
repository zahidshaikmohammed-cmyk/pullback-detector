import asyncio
import logging

from .config import get_settings


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger(__name__).info("pullback-detector started; live validation requires configured Dhan credentials")
    if not settings.dhan_access_token or not settings.dhan_client_id:
        logging.getLogger(__name__).warning("Dhan credentials are not configured; no live market connection will be attempted")


if __name__ == "__main__":
    main()
