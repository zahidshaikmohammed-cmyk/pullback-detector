"""Render-compatible live service entrypoint and browser dashboard."""

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
from .dashboard import DashboardData, HTML
from .dashboard_ui import candle_history
from .service import run_live

LOGGER = logging.getLogger(__name__)
_STATE = {"status": "starting", "started_at": datetime.now(timezone.utc).isoformat(), "last_report": None, "last_error": None}
_DASHBOARD_DATA: DashboardData | None = None

class AppHandler(BaseHTTPRequestHandler):
    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path in {"/", "/dashboard"}:
            self._send(200, "text/html; charset=utf-8", HTML.encode("utf-8")); return
        if path in {"/health", "/healthz"}:
            payload = json.dumps(_STATE, default=str).encode("utf-8")
            self._send(200 if _STATE["status"] in {"starting", "live", "degraded"} else 503, "application/json", payload); return
        if path == "/api/dashboard":
            if _DASHBOARD_DATA is None:
                self._send(503, "application/json", b'{"error":"dashboard not initialized"}'); return
            try:
                payload = json.dumps(_DASHBOARD_DATA.snapshot(), default=str, separators=(",", ":")).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", payload)
            except Exception as exc:
                LOGGER.exception("dashboard snapshot failed")
                self._send(500, "application/json; charset=utf-8", json.dumps({"error": str(exc)}).encode("utf-8"))
            return
        if path == "/api/dashboard/candles":
            if _DASHBOARD_DATA is None:
                self._send(503, "application/json", b'{"error":"dashboard not initialized"}'); return
            try:
                from urllib.parse import parse_qs, urlsplit
                sid = parse_qs(urlsplit(self.path).query).get("instrument_id", [""])[0]
                rows = candle_history(_DASHBOARD_DATA.root, sid, 80) if sid else []
                self._send(200, "application/json; charset=utf-8", json.dumps(rows, default=str, separators=(",", ":")).encode("utf-8"))
            except Exception as exc:
                LOGGER.exception("dashboard candle history failed")
                self._send(500, "application/json; charset=utf-8", json.dumps({"error": str(exc)}).encode("utf-8"))
            return
        self._send(404, "text/plain; charset=utf-8", b"Not found")

    def log_message(self, format: str, *args) -> None:
        return

def serve_http() -> None:
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), AppHandler)
    LOGGER.info("web server listening on 0.0.0.0:%s", port)
    server.serve_forever()

async def service_loop() -> None:
    settings = get_settings()
    if not settings.dhan_access_token or not settings.dhan_client_id:
        _STATE["status"] = "degraded"; _STATE["last_error"] = "Dhan credentials are not configured"; LOGGER.error("Dhan credentials are not configured"); return
    _STATE["status"] = "live"
    while True:
        try:
            report = await run_live(settings)
            _STATE["last_report"] = report; _STATE["last_error"] = None; _STATE["status"] = "live"
        except Exception as exc:
            _STATE["status"] = "degraded"; _STATE["last_error"] = str(exc); LOGGER.exception("live scanner cycle failed")
            await asyncio.sleep(5); _STATE["status"] = "live"

def main() -> None:
    global _DASHBOARD_DATA
    settings = get_settings(); _DASHBOARD_DATA = DashboardData(settings.data_root, _STATE)
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    threading.Thread(target=serve_http, name="http-server", daemon=True).start()
    try: asyncio.run(service_loop())
    except KeyboardInterrupt: LOGGER.info("shutdown requested")
    except Exception: LOGGER.exception("service failed"); sys.exit(2)

if __name__ == "__main__": main()
