"""Render-compatible live service entrypoint."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import get_settings
from .service import run_live


LOGGER = logging.getLogger(__name__)
_STATE = {
    "status": "starting",
    "started_at": datetime.now(timezone.utc).isoformat(),
    "last_report": None,
    "last_error": None,
}


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path not in {"/", "/health", "/healthz"}:
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(_STATE, default=str).encode("utf-8")
        self.send_response(200 if _STATE["status"] in {"starting", "live", "degraded"} else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:
        return


def serve_health() -> None:
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    LOGGER.info("health server listening on 0.0.0.0:%s", port)
    server.serve_forever()


async def service_loop() -> None:
    settings = get_settings()
    if not settings.dhan_access_token or not settings.dhan_client_id:
        _STATE["status"] = "degraded"
        _STATE["last_error"] = "Dhan credentials are not configured"
        LOGGER.error("Dhan credentials are not configured")
        return

    _STATE["status"] = "live"
    while True:
        try:
            report = await run_live(settings)
            _STATE["last_report"] = report
            _STATE["last_error"] = None
        except Exception as exc:
            _STATE["status"] = "degraded"
            _STATE["last_error"] = str(exc)
            LOGGER.exception("live scanner cycle failed")
            await asyncio.sleep(5)
            _STATE["status"] = "live"


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    threading.Thread(target=serve_health, name="health-server", daemon=True).start()
    try:
        asyncio.run(service_loop())
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested")
    except Exception:
        LOGGER.exception("service failed")
        sys.exit(2)


if __name__ == "__main__":
    main()
