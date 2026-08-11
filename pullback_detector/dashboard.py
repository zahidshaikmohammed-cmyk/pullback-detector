"""Lightweight browser dashboard backed by the existing runtime event store."""

from __future__ import annotations

import csv
import json
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _read_tail(path: Path, max_lines: int = 5000) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()
        return lines[-max_lines:]
    except OSError:
        return []


class DashboardData:
    """Read-only dashboard view over the same data already persisted by EventStore."""

    def __init__(self, data_root: str | Path, state: dict):
        self.root = Path(data_root)
        self.state = state
        self._lock = threading.Lock()
        self._cache: tuple[tuple[float, str, str], dict] | None = None

    def _universe(self) -> list[dict]:
        path = self.root / "universe.csv"
        if not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                return list(csv.DictReader(handle))
        except (OSError, csv.Error):
            return []

    def _runtime_file(self, directory: str) -> Path:
        return self.root / directory / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"

    def snapshot(self) -> dict:
        now = datetime.now(timezone.utc)
        minute_file = self._runtime_file("normalized")
        cache_key = (
            minute_file.stat().st_mtime if minute_file.exists() else 0.0,
            str(self.state.get("status", "starting")),
            str(self.state.get("last_error") or ""),
        )
        with self._lock:
            if self._cache and self._cache[0] == cache_key:
                return self._cache[1]

            universe = self._universe()
            latest: dict[str, dict] = {}
            for line in _read_tail(minute_file):
                try:
                    tick = json.loads(line)
                except json.JSONDecodeError:
                    continue
                latest[str(tick.get("instrument_id"))] = tick

            candles: dict[str, dict[str, dict]] = {}
            candle_file = self._runtime_file("candles")
            for line in _read_tail(candle_file):
                try:
                    candle = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not candle.get("complete"):
                    continue
                instrument = str(candle.get("instrument_id"))
                tf = str(candle.get("timeframe_seconds", 300))
                candles.setdefault(instrument, {})[tf] = candle

            signals: list[dict] = []
            signal_file = self._runtime_file("signals")
            for line in _read_tail(signal_file, 1000):
                try:
                    signal = json.loads(line)
                except json.JSONDecodeError:
                    continue
                signals.append(signal)
            signals.sort(key=lambda item: item.get("timestamp", ""), reverse=True)

            instruments = []
            for row in universe:
                sid = str(row.get("security_id"))
                tick = latest.get(sid, {})
                instrument_candles = candles.get(sid, {})
                instruments.append({
                    "security_id": row.get("security_id"),
                    "symbol": row.get("symbol") or row.get("trading_symbol") or sid,
                    "trading_symbol": row.get("trading_symbol") or row.get("symbol") or sid,
                    "latest_price": tick.get("price"),
                    "tick_timestamp": tick.get("timestamp"),
                    "receive_timestamp": tick.get("received_at"),
                    "stale": self._stale(tick, now),
                    "candle_1m": instrument_candles.get("60"),
                    "candle_5m": instrument_candles.get("300"),
                })

            active_cutoff = now - timedelta(minutes=30)
            active = [
                signal for signal in signals
                if self._parse_time(signal.get("timestamp")) and self._parse_time(signal["timestamp"]) >= active_cutoff
            ]
            health = dict(self.state.get("last_report") or {})
            health["service_status"] = self.state.get("status", health.get("service_status", "starting"))
            health["last_error"] = self.state.get("last_error")

            payload = {
                "generated_at": now.isoformat(),
                "health": health,
                "instruments": instruments,
                "active_signals": active[:25],
                "recent_signals": signals[:50],
                "signal_window_minutes": 30,
            }
            self._cache = (cache_key, payload)
            return payload

    @staticmethod
    def _parse_time(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None

    @classmethod
    def _stale(cls, tick: dict, now: datetime) -> bool:
        received = cls._parse_time(tick.get("received_at"))
        return received is None or (now - received).total_seconds() > 60


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pullback Detector</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#e8edf4;background:#0b0f14}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#17202c,#0b0f14 42%);min-height:100vh}.wrap{max-width:1500px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:20px}.brand h1{margin:0;font-size:28px}.brand p{margin:5px 0 0;color:#8e9aaa}.pill{border:1px solid #263241;background:#121923;border-radius:999px;padding:8px 12px;font-size:13px}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#87909d;margin-right:7px}.live .dot{background:#38d39f}.bad .dot{background:#ff6b6b}.grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px;margin-bottom:20px}.card{background:#111720;border:1px solid #202b38;border-radius:14px;padding:16px;box-shadow:0 10px 30px #0002}.metric .label{font-size:12px;color:#7f8c9d;text-transform:uppercase;letter-spacing:.06em}.metric .value{font-size:22px;font-weight:700;margin-top:7px}.section{margin-top:20px}.section h2{font-size:17px;margin:0 0 10px}.table-wrap{overflow:auto;border:1px solid #202b38;border-radius:14px}.table{width:100%;border-collapse:collapse;min-width:1000px;background:#10161f}.table th,.table td{padding:11px 12px;border-bottom:1px solid #202936;text-align:left;font-size:13px;white-space:nowrap}.table th{color:#8190a3;font-weight:600;background:#131b25;position:sticky;top:0}.price{font-weight:700}.muted{color:#7e8b9b}.good{color:#3dd6a0}.warn{color:#ffc857}.badtxt{color:#ff7777}.signal{display:grid;grid-template-columns:1.1fr .7fr 1fr 1fr 1fr 1fr 1fr 1.2fr;gap:8px;align-items:center}.signal .cell{padding:11px 10px;background:#111720;border:1px solid #202b38;border-radius:10px;font-size:12px}.signal .title{font-weight:700;font-size:14px}.empty{padding:24px;text-align:center;color:#748196;background:#10161f;border:1px dashed #273342;border-radius:12px}.footer{color:#697789;font-size:12px;margin-top:20px;text-align:right}@media(max-width:1100px){.grid{grid-template-columns:repeat(3,minmax(0,1fr))}.signal{grid-template-columns:repeat(4,minmax(0,1fr))}}@media(max-width:650px){.wrap{padding:14px}.top{flex-direction:column}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.brand h1{font-size:23px}}
</style>
</head>
<body>
<div class="wrap">
<header class="top"><div class="brand"><h1>Pullback Detector</h1><p>Live NSE equity monitor · Experimental V1</p></div><div id="status" class="pill"><span class="dot"></span><span>Connecting…</span></div></header>
<section class="grid" id="metrics"></section>
<section class="section"><h2>Monitored Instruments</h2><div class="table-wrap"><table class="table"><thead><tr><th>Instrument</th><th>Latest Price</th><th>Last Tick</th><th>Receive</th><th>1m Candle</th><th>5m Candle</th><th>Feed</th></tr></thead><tbody id="instruments"></tbody></table></div></section>
<section class="section"><h2>Active Pullback Signals <span class="muted">(last 30 minutes)</span></h2><div id="active"></div></section>
<section class="section"><h2>Recent Signal History</h2><div class="table-wrap"><table class="table"><thead><tr><th>Time</th><th>Instrument</th><th>Direction</th><th>Impulse</th><th>Retracement</th><th>Trigger</th><th>Invalidation</th><th>Confidence</th><th>Label</th></tr></thead><tbody id="history"></tbody></table></div></section>
<div class="footer" id="updated">Updating automatically…</div>
</div>
<script>
const $=id=>document.getElementById(id);
const fmt=v=>v==null?'—':String(v);
const time=v=>{if(!v)return'—';try{return new Date(v).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})}catch{return v}};
const candle=c=>c?`${fmt(c.open)} → ${fmt(c.close)} · ${time(c.start)}`:'—';
function render(d){const h=d.health||{}, ok=h.service_status==='live';
 $('status').className='pill '+(ok?'live':'bad');$('status').innerHTML=`<span class="dot"></span>${ok?'LIVE':'DEGRADED'} · ${h.dhan_connection_status||'unknown'}`;
 const metrics=[['Accepted ticks',h.accepted_tick_count??0],['Packets',h.packet_count??0],['Producing instruments',h.instrument_count_received??0],['1m candles',h.candle_count_1m??0],['5m candles',h.candle_count_5m??0],['Median latency',h.latency_ms_median==null?'—':Math.round(h.latency_ms_median)+' ms']];
 $('metrics').innerHTML=metrics.map(x=>`<div class="card metric"><div class="label">${x[0]}</div><div class="value">${x[1]}</div></div>`).join('');
 $('instruments').innerHTML=d.instruments.map(i=>`<tr><td><b>${fmt(i.trading_symbol)}</b><div class="muted">SID ${fmt(i.security_id)}</div></td><td class="price">${fmt(i.latest_price)}</td><td>${time(i.tick_timestamp)}</td><td>${time(i.receive_timestamp)}</td><td>${candle(i.candle_1m)}</td><td>${candle(i.candle_5m)}</td><td class="${i.stale?'warn':'good'}">${i.stale?'STALE':'LIVE'}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">No instrument snapshot available yet.</td></tr>';
 const signalCard=s=>`<div class="signal"><div class="cell title">${fmt(s.direction)}<div class="muted">SID ${fmt(s.instrument_id)}</div></div><div class="cell"><b>Impulse</b><br>${fmt(s.impulse_start)} → ${fmt(s.impulse_end)}</div><div class="cell"><b>Retracement</b><br>${s.retracement==null?'—':(Number(s.retracement)*100).toFixed(1)+'%'}</div><div class="cell"><b>Trigger</b><br>${fmt(s.trigger_price)}</div><div class="cell"><b>Invalidation</b><br>${fmt(s.invalidation_level)}</div><div class="cell"><b>Confidence</b><br>${s.confidence_score==null?'—':(Number(s.confidence_score)*100).toFixed(1)+'%'}</div><div class="cell"><b>Timestamp</b><br>${time(s.timestamp)}</div><div class="cell"><b>Label</b><br>EXPERIMENTAL_V1</div></div>`;
 $('active').innerHTML=d.active_signals.length?d.active_signals.map(signalCard).join(''):'<div class="empty">No active pullback signals.</div>';
 $('history').innerHTML=d.recent_signals.map(s=>`<tr><td>${time(s.timestamp)}</td><td>SID ${fmt(s.instrument_id)}</td><td class="${s.direction==='LONG'?'good':'warn'}"><b>${fmt(s.direction)}</b></td><td>${fmt(s.impulse_start)} → ${fmt(s.impulse_end)}</td><td>${s.retracement==null?'—':(Number(s.retracement)*100).toFixed(1)+'%'}</td><td>${fmt(s.trigger_price)}</td><td>${fmt(s.invalidation_level)}</td><td>${s.confidence_score==null?'—':(Number(s.confidence_score)*100).toFixed(1)+'%'}</td><td>EXPERIMENTAL_V1</td></tr>`).join('')||'<tr><td colspan="9" class="muted">No V1 signals generated yet.</td></tr>';
 $('updated').textContent='Updated '+new Date(d.generated_at).toLocaleTimeString()+' · refreshes every 3 seconds';
}
async function refresh(){try{const r=await fetch('/api/dashboard',{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);render(await r.json())}catch(e){$('status').className='pill bad';$('status').innerHTML='<span class="dot"></span>DASHBOARD ERROR';$('updated').textContent='Backend unavailable: '+e.message}}
refresh();setInterval(refresh,3000);
</script>
</body></html>"""
