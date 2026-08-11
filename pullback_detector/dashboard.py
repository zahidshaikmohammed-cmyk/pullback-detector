"""Lightweight browser dashboard backed by accepted market data and persistent V1 setup lifecycle state."""
from __future__ import annotations

import csv
import json
import threading
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .lifecycle import PullbackLifecycleEngine
from .live import LIVE_ANATOMY


def _jsonable(value: Any):
    if isinstance(value, datetime): return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal): return str(value)
    return value


def _read_tail(path: Path, max_lines: int = 5000) -> list[str]:
    if not path.exists(): return []
    try:
        with path.open("r", encoding="utf-8") as handle: return handle.readlines()[-max_lines:]
    except OSError: return []


class DashboardData:
    """Read-only projection of accepted market data, V1 anatomy and persistent setup lifecycle."""
    def __init__(self, data_root: str | Path, state: dict):
        self.root = Path(data_root); self.state = state; self._lock = threading.Lock(); self._cache = None

    def _universe(self):
        path = self.root / "universe.csv"
        if not path.exists(): return []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle: return list(csv.DictReader(handle))
        except (OSError, csv.Error): return []

    def _runtime_file(self, directory):
        return self.root / directory / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"

    @staticmethod
    def _parse_time(value):
        if not value: return None
        try: return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except (ValueError, TypeError): return None

    @classmethod
    def _stale(cls, tick, now):
        received = cls._parse_time(tick.get("received_at"))
        return received is None or (now - received).total_seconds() > 60

    def _lifecycle(self):
        engine = PullbackLifecycleEngine(self.root)
        snap = engine.snapshot()
        def state_dict(state):
            d = asdict(state)
            d["snapshot"] = asdict(state.snapshot)
            return json.loads(json.dumps(d, default=_jsonable))
        return [state_dict(x) for x in snap["active"]], [state_dict(x) for x in snap["closed"]]

    def snapshot(self):
        now = datetime.now(timezone.utc)
        minute_file = self._runtime_file("normalized")
        lifecycle_file = self.root / "setup_events.jsonl"
        cache_key = (
            minute_file.stat().st_mtime if minute_file.exists() else 0.0,
            lifecycle_file.stat().st_mtime if lifecycle_file.exists() else 0.0,
            str(self.state.get("status")), str(self.state.get("last_error") or ""), len(LIVE_ANATOMY),
        )
        with self._lock:
            if self._cache and self._cache[0] == cache_key: return self._cache[1]
            universe = self._universe(); latest = {}
            for line in _read_tail(minute_file):
                try: tick = json.loads(line); latest[str(tick.get("instrument_id"))] = tick
                except json.JSONDecodeError: continue
            candles = {}; candle_file = self._runtime_file("candles")
            for line in _read_tail(candle_file):
                try: candle = json.loads(line)
                except json.JSONDecodeError: continue
                if candle.get("complete"):
                    candles.setdefault(str(candle.get("instrument_id")), {})[str(candle.get("timeframe_seconds", 300))] = candle
            signals = []; signal_file = self._runtime_file("signals")
            for line in _read_tail(signal_file, 1000):
                try: signals.append(json.loads(line))
                except json.JSONDecodeError: continue
            signals.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            instruments = []
            for row in universe:
                sid = str(row.get("security_id")); tick = latest.get(sid, {}); c = candles.get(sid, {})
                anatomy = dict(LIVE_ANATOMY.get(int(sid), {})) if sid.isdigit() else {}
                instruments.append({
                    "security_id": row.get("security_id"), "symbol": row.get("symbol") or row.get("trading_symbol") or sid,
                    "trading_symbol": row.get("trading_symbol") or row.get("symbol") or sid,
                    "latest_price": tick.get("price"), "tick_timestamp": tick.get("timestamp"), "receive_timestamp": tick.get("received_at"),
                    "stale": self._stale(tick, now), "candle_1m": c.get("60"), "candle_5m": c.get("300"), "anatomy": anatomy,
                })
            active_signals = [s for s in signals if self._parse_time(s.get("timestamp")) and self._parse_time(s["timestamp"]) >= now - timedelta(minutes=30)]
            active_setups, closed_setups = self._lifecycle()
            health = dict(self.state.get("last_report") or {}); health["service_status"] = self.state.get("status", health.get("service_status", "starting")); health["last_error"] = self.state.get("last_error")
            payload = {
                "generated_at": now.isoformat(), "health": health, "instruments": instruments,
                "active_signals": active_signals[:25], "recent_signals": signals[:50],
                "active_setups": active_setups, "recently_closed_setups": closed_setups[:50], "signal_window_minutes": 30,
            }
            self._cache = (cache_key, payload); return payload


HTML = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pullback Detector</title><style>
:root{font-family:Inter,ui-sans-serif,system-ui;color:#e8edf4;background:#0b0f14}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#17202c,#0b0f14 42%);min-height:100vh}.wrap{max-width:1500px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:20px}.brand h1{margin:0;font-size:28px}.brand p{margin:5px 0;color:#8e9aaa}.pill{border:1px solid #263241;background:#121923;border-radius:999px;padding:8px 12px;font-size:13px}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#87909d;margin-right:7px}.live .dot{background:#38d39f}.bad .dot{background:#ff6b6b}.grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px;margin-bottom:20px}.card,.anatomy,.setup{background:#111720;border:1px solid #202b38;border-radius:14px;padding:16px;box-shadow:0 10px 30px #0002}.metric .label{font-size:12px;color:#7f8c9d;text-transform:uppercase;letter-spacing:.06em}.metric .value{font-size:22px;font-weight:700;margin-top:7px}.section{margin-top:20px}.section h2{font-size:17px;margin:0 0 10px}.table-wrap{overflow:auto;border:1px solid #202b38;border-radius:14px}.table{width:100%;border-collapse:collapse;min-width:1000px;background:#10161f}.table th,.table td{padding:11px 12px;border-bottom:1px solid #202936;text-align:left;font-size:13px;white-space:nowrap}.table th{color:#8190a3;font-weight:600;background:#131b25;position:sticky;top:0}.price{font-weight:700}.muted{color:#7e8b9b}.good{color:#3dd6a0}.warn{color:#ffc857}.anatomy-grid,.setup-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px}.anatomy-head,.setup-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:12px}.anatomy-head b,.setup-head b{font-size:16px}.phase{padding:5px 9px;border:1px solid #2a394a;border-radius:999px;font-size:11px}.fact{padding:10px;border:1px solid #202b38;border-radius:9px;background:#0f151d}.fact label{display:block;color:#7d8b9d;font-size:10px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}.fact strong{font-size:13px}.empty{padding:24px;text-align:center;color:#748196;background:#10161f;border:1px dashed #273342;border-radius:12px}.footer{color:#697789;font-size:12px;margin-top:20px;text-align:right}@media(max-width:1100px){.grid{grid-template-columns:repeat(3,minmax(0,1fr))}.anatomy-grid,.setup-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:650px){.wrap{padding:14px}.top{flex-direction:column}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.anatomy-grid,.setup-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.brand h1{font-size:23px}}
</style></head><body><div class="wrap"><header class="top"><div class="brand"><h1>Pullback Detector</h1><p>Live NSE equity monitor · Experimental V1</p></div><div id="status" class="pill"><span class="dot"></span>Connecting…</div></header><section class="grid" id="metrics"></section><section class="section"><h2>Monitored Instruments</h2><div class="table-wrap"><table class="table"><thead><tr><th>Instrument</th><th>Latest Price</th><th>Last Tick</th><th>Receive</th><th>1m Candle</th><th>5m Candle</th><th>Feed</th></tr></thead><tbody id="instruments"></tbody></table></div></section><section class="section"><h2>Live Pullback Anatomy</h2><div id="anatomy"></div></section><section class="section"><h2>ACTIVE SETUPS</h2><div id="setups"></div></section><section class="section"><h2>RECENTLY CLOSED SETUPS</h2><div class="table-wrap"><table class="table"><thead><tr><th>Closed</th><th>Signal ID</th><th>Instrument</th><th>Direction</th><th>Trigger</th><th>Target 1</th><th>Target 2</th><th>Invalidation</th><th>Outcome</th><th>MFE</th><th>MAE</th></tr></thead><tbody id="closed"></tbody></table></div></section><section class="section"><h2>Active Pullback Signals <span class="muted">(last 30 minutes)</span></h2><div id="active"></div></section><section class="section"><h2>Recent Signal History</h2><div class="table-wrap"><table class="table"><thead><tr><th>Time</th><th>Instrument</th><th>Direction</th><th>Impulse</th><th>Retracement</th><th>Trigger</th><th>Invalidation</th><th>Confidence</th><th>Label</th></tr></thead><tbody id="history"></tbody></table></div></section><div class="footer" id="updated">Updating automatically…</div></div><script>
const $=id=>document.getElementById(id),fmt=v=>v==null?'—':String(v),num=(v,s=2)=>v==null?'—':Number(v).toFixed(s),time=v=>{if(!v)return'—';try{return new Date(v).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})}catch{return v}},candle=c=>c?`${fmt(c.open)} → ${fmt(c.close)} · ${time(c.start)}`:'—';function fact(label,value){return `<div class="fact"><label>${label}</label><strong>${fmt(value)}</strong></div>`}
function anatomyCard(i){const a=i.anatomy||{};return `<div class="anatomy"><div class="anatomy-head"><b>${fmt(i.trading_symbol)}</b><span class="phase">${fmt(a.detection_phase)}</span></div><div class="anatomy-grid">${fact('Impulse magnitude',num(a.impulse_magnitude))}${fact('Impulse direction',a.impulse_direction)}${fact('Impulse high',num(a.impulse_high))}${fact('Impulse low',num(a.impulse_low))}${fact('Retracement depth',a.retracement_depth_pct==null?'—':num(a.retracement_depth_pct,1)+'%')}${fact('Retracement price',num(a.retracement_price))}${fact('Pullback duration',a.pullback_duration_minutes==null?'—':num(a.pullback_duration_minutes,1)+' min')}${fact('Volume behavior',a.volume_behavior)}${fact('Structural state',a.structural_state)}${fact('Continuation state',a.continuation_state)}${fact('Trigger price',num(a.trigger_price))}${fact('Invalidation price',num(a.invalidation_price))}${fact('Distance to trigger',num(a.distance_to_trigger))}${fact('Confidence',a.confidence==null?'—':num(a.confidence*100,1)+'%')}${fact('5m state timestamp',time(a.timestamp))}</div></div>`}
function setupCard(s){const x=s.snapshot||{},p=Number(s.current_price),long=x.direction==='LONG',d=(level)=>Number.isFinite(p)?Math.abs(Number(level)-p):null,age=()=>{const t=new Date(x.creation_timestamp);return Math.max(0,(Date.now()-t.getTime())/60000)};return `<div class="setup"><div class="setup-head"><b>${fmt(x.instrument_id)} · ${fmt(x.direction)}</b><span class="phase">ACTIVE · ${fmt(x.signal_id)}</span></div><div class="setup-grid">${fact('Current price',num(s.current_price))}${fact('Trigger',num(x.trigger_price))}${fact('Distance to trigger',num(d(x.trigger_price)))}${fact('Target 1',num(x.target_1))}${fact('Distance to T1',num(d(x.target_1)))}${fact('Target 2',num(x.target_2))}${fact('Distance to T2',num(d(x.target_2)))}${fact('Invalidation',num(x.invalidation_price))}${fact('Distance to invalidation',num(d(x.invalidation_price)))}${fact('Setup age',num(age(),1)+' min')}${fact('MFE',num(s.mfe))}${fact('MAE',num(s.mae))}${fact('Confidence',num(Number(x.confidence)*100,1)+'%')}${fact('Pullback depth',num(Number(x.pullback_depth)*100,1)+'%')}${fact('Created',time(x.creation_timestamp))}</div></div>`}
function render(d){const h=d.health||{},ok=h.service_status==='live';$('status').className='pill '+(ok?'live':'bad');$('status').innerHTML=`<span class="dot"></span>${ok?'LIVE':'DEGRADED'} · ${h.dhan_connection_status||'unknown'}`;const m=[['Accepted ticks',h.accepted_tick_count??0],['Packets',h.packet_count??0],['Producing instruments',h.instrument_count_received??0],['1m candles',h.candle_count_1m??0],['5m candles',h.candle_count_5m??0],['Active setups',d.active_setups.length],['Median latency',h.latency_ms_median==null?'—':Math.round(h.latency_ms_median)+' ms']];$('metrics').innerHTML=m.map(x=>`<div class="card metric"><div class="label">${x[0]}</div><div class="value">${x[1]}</div></div>`).join('');$('instruments').innerHTML=d.instruments.map(i=>`<tr><td><b>${fmt(i.trading_symbol)}</b><div class="muted">SID ${fmt(i.security_id)}</div></td><td class="price">${fmt(i.latest_price)}</td><td>${time(i.tick_timestamp)}</td><td>${time(i.receive_timestamp)}</td><td>${candle(i.candle_1m)}</td><td>${candle(i.candle_5m)}</td><td class="${i.stale?'warn':'good'}">${i.stale?'STALE':'LIVE'}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">No instrument snapshot available yet.</td></tr>';$('anatomy').innerHTML=d.instruments.map(anatomyCard).join('')||'<div class="empty">No monitored instruments available.</div>';$('setups').innerHTML=d.active_setups.length?d.active_setups.map(setupCard).join(''):'<div class="empty">No active lifecycle setups.</div>';$('closed').innerHTML=d.recently_closed_setups.map(s=>{const x=s.snapshot||{};return `<tr><td>${time(s.closed_timestamp)}</td><td>${fmt(x.signal_id)}</td><td>SID ${fmt(x.instrument_id)}</td><td>${fmt(x.direction)}</td><td>${fmt(x.trigger_price)}</td><td>${fmt(x.target_1)}</td><td>${fmt(x.target_2)}</td><td>${fmt(x.invalidation_price)}</td><td><b>${fmt(s.outcome)}</b></td><td>${fmt(s.mfe)}</td><td>${fmt(s.mae)}</td></tr>`}).join('')||'<tr><td colspan="11" class="muted">No closed setups yet.</td></tr>';const sc=s=>`<div class="anatomy"><div class="anatomy-head"><b>${fmt(s.direction)} · SID ${fmt(s.instrument_id)}</b><span class="phase">EXPERIMENTAL_V1</span></div><div class="anatomy-grid">${fact('Impulse',fmt(s.impulse_start)+' → '+fmt(s.impulse_end))}${fact('Retracement',s.retracement==null?'—':num(s.retracement*100,1)+'%')}${fact('Trigger',s.trigger_price)}${fact('Invalidation',s.invalidation_level)}${fact('Confidence',s.confidence_score==null?'—':num(s.confidence_score*100,1)+'%')}${fact('Timestamp',time(s.timestamp))}</div></div>`;$('active').innerHTML=d.active_signals.length?d.active_signals.map(sc).join(''):'<div class="empty">No active pullback signals.</div>';$('history').innerHTML=d.recent_signals.map(s=>`<tr><td>${time(s.timestamp)}</td><td>SID ${fmt(s.instrument_id)}</td><td><b>${fmt(s.direction)}</b></td><td>${fmt(s.impulse_start)} → ${fmt(s.impulse_end)}</td><td>${s.retracement==null?'—':num(s.retracement*100,1)+'%'}</td><td>${fmt(s.trigger_price)}</td><td>${fmt(s.invalidation_level)}</td><td>${s.confidence_score==null?'—':num(s.confidence_score*100,1)+'%'}</td><td>EXPERIMENTAL_V1</td></tr>`).join('')||'<tr><td colspan="9" class="muted">No V1 signals generated yet.</td></tr>';$('updated').textContent='Updated '+new Date(d.generated_at).toLocaleTimeString()+' · refreshes every 3 seconds'}async function refresh(){try{const r=await fetch('/api/dashboard',{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);render(await r.json())}catch(e){$('status').className='pill bad';$('status').innerHTML='<span class="dot"></span>DASHBOARD ERROR';$('updated').textContent='Backend unavailable: '+e.message}}refresh();setInterval(refresh,3000);
</script></body></html>'''
