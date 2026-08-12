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
from urllib.parse import parse_qs, urlsplit

from .config import get_settings
from .dashboard import DashboardData, HTML
from .dashboard_runtime import canonical_snapshot
from .dashboard_ui import candle_history
from .service import run_live

LOGGER = logging.getLogger(__name__)
_STATE = {"status": "starting", "started_at": datetime.now(timezone.utc).isoformat(), "last_report": None, "last_error": None}
_DASHBOARD_DATA: DashboardData | None = None

PIPELINE_SCRIPT = r"""
<script>
(function(){
  function ensurePanel(){
    var root=document.getElementById('system-health');
    if(!root || document.getElementById('phase1-pipeline')) return;
    var panel=document.createElement('div'); panel.id='phase1-pipeline'; panel.className='section';
    panel.innerHTML='<div class="sh"><h2>DATA PIPELINE</h2><span>Canonical Phase-1 runtime telemetry</span><div class="rule"></div></div><div class="telemetry" id="phase1-pipeline-grid"></div><div id="phase1-pipeline-status" class="empty" style="margin-top:7px"></div>';
    root.appendChild(panel);
  }
  function fmt(v){return (v===null||v===undefined)?'—':String(v)}
  async function refresh(){
    try{
      var r=await fetch('/api/dashboard?runtime='+Date.now(),{cache:'no-store'}); var d=await r.json(); var h=d.health||{}, p=h.pipeline||{};
      ensurePanel(); var grid=document.getElementById('phase1-pipeline-grid'), status=document.getElementById('phase1-pipeline-status'); if(!grid||!status)return;
      var cells=[['WebSocket',fmt(p.websocket)],['Subscriptions',fmt(p.subscriptions)],['Producing',fmt(p.producing)],['Raw packets',fmt(p.raw_packets)],['Decoded',fmt(p.decoded_packets)],['Accepted ticks',fmt(p.accepted_ticks)],['To candle engine',fmt(p.to_candle_engine)],['Rejected ticks',fmt(p.rejected_ticks)],['Active 1M',fmt(p.active_1m_buckets)],['Completed 1M',fmt(p.completed_1m_candles)],['Active 5M',fmt(p.active_5m_buckets)],['Completed 5M',fmt(p.completed_5m_candles)],['Persisted 1M',fmt(p.persisted_1m_candles)],['Persisted 5M',fmt(p.persisted_5m_candles)],['Global feed',fmt(h.feed_state)],['Data age',h.global_data_age_seconds==null?'—':Math.round(h.global_data_age_seconds)+'s']];
      grid.innerHTML=cells.map(function(c){return '<div class="metric"><label>'+c[0]+'</label><b>'+c[1]+'</b></div>';}).join('');
      var st=p.status||'BLOCKED'; var reason=p.first_failure||'None'; status.textContent='PIPELINE STATUS: '+st+' · FIRST FAILURE: '+reason;
      status.className='empty '+(st==='PASS'?'live':'bad');
    }catch(e){}
  }
  document.addEventListener('DOMContentLoaded',function(){ensurePanel();refresh();setInterval(refresh,3000);});
})();
</script>
"""
DASHBOARD_HTML = HTML.replace("</body>", PIPELINE_SCRIPT + "</body>")


def _json(body):
    return json.dumps(body, default=str, separators=(",", ":")).encode("utf-8")


class AppHandler(BaseHTTPRequestHandler):
    def _send(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _snapshot(self):
        if _DASHBOARD_DATA is None:
            raise RuntimeError("dashboard not initialized")
        return canonical_snapshot(_DASHBOARD_DATA)

    def _health(self):
        return self._snapshot().get("health", {})

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in {"/", "/dashboard"}:
            self._send(200, "text/html; charset=utf-8", DASHBOARD_HTML.encode("utf-8"))
            return
        if path in {"/health", "/healthz"}:
            health = self._health()
            http_status = 200 if health.get("feed_state") in {"LIVE", "STALE", "NO_DATA"} else 503
            self._send(http_status, "application/json", _json({"service_status": _STATE["status"], "last_error": _STATE.get("last_error"), "health": health}))
            return
        if path in {"/feed-status", "/system-health"}:
            self._send(200, "application/json; charset=utf-8", _json({"service_status": _STATE["status"], "last_error": _STATE.get("last_error"), "health": self._health()}))
            return
        if path == "/instruments":
            try:
                self._send(200, "application/json; charset=utf-8", _json(self._snapshot().get("instruments", [])))
            except Exception as exc:
                self._send(500, "application/json; charset=utf-8", _json({"error": str(exc)}))
            return
        if path in {"/ticks", "/market-state"}:
            try:
                self._send(200, "application/json; charset=utf-8", _json(self._snapshot().get("instruments", [])))
            except Exception as exc:
                self._send(500, "application/json; charset=utf-8", _json({"error": str(exc)}))
            return
        if path == "/candles":
            try:
                query = parse_qs(urlsplit(self.path).query)
                sid = query.get("instrument_id", [""])[0]
                limit = int(query.get("limit", ["80"])[0])
                rows = candle_history(_DASHBOARD_DATA.root, sid, max(1, min(limit, 250))) if sid and _DASHBOARD_DATA else []
                self._send(200, "application/json; charset=utf-8", _json(rows))
            except Exception as exc:
                self._send(500, "application/json; charset=utf-8", _json({"error": str(exc)}))
            return
        if path == "/session":
            health = self._health()
            self._send(200, "application/json; charset=utf-8", _json({"market_status": health.get("market_status"), "session_state": health.get("session_state"), "generated_at": health.get("generated_at"), "feed_state": health.get("feed_state")}))
            return
        if path == "/api/dashboard":
            try:
                self._send(200, "application/json; charset=utf-8", _json(self._snapshot()))
            except Exception as exc:
                LOGGER.exception("dashboard snapshot failed")
                self._send(500, "application/json; charset=utf-8", _json({"error": str(exc)}))
            return
        if path == "/api/dashboard/candles":
            try:
                sid = parse_qs(urlsplit(self.path).query).get("instrument_id", [""])[0]
                rows = candle_history(_DASHBOARD_DATA.root, sid, 80) if sid and _DASHBOARD_DATA else []
                self._send(200, "application/json; charset=utf-8", _json(rows))
            except Exception as exc:
                LOGGER.exception("dashboard candle history failed")
                self._send(500, "application/json; charset=utf-8", _json({"error": str(exc)}))
            return
        self._send(404, "text/plain; charset=utf-8", b"Not found")

    def log_message(self, format, *args):
        return


def serve_http():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), AppHandler)
    LOGGER.info("web server listening on 0.0.0.0:%s", port)
    server.serve_forever()


async def service_loop():
    settings = get_settings()
    if not settings.dhan_access_token or not settings.dhan_client_id:
        _STATE["status"] = "degraded"
        _STATE["last_error"] = "Dhan credentials are not configured"
        LOGGER.error(_STATE["last_error"])
        return
    _STATE["status"] = "live"
    while True:
        try:
            report = await run_live(settings)
            _STATE["last_report"] = report
            _STATE["last_error"] = None
            _STATE["status"] = "live"
        except Exception as exc:
            _STATE["status"] = "degraded"
            _STATE["last_error"] = str(exc)
            LOGGER.exception("live scanner cycle failed")
            await asyncio.sleep(5)


def main():
    global _DASHBOARD_DATA
    settings = get_settings()
    _DASHBOARD_DATA = DashboardData(settings.data_root, _STATE)
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    threading.Thread(target=serve_http, name="http-server", daemon=True).start()
    try:
        asyncio.run(service_loop())
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested")
    except Exception:
        LOGGER.exception("service failed")
        sys.exit(2)


if __name__ == "__main__":
    main()
