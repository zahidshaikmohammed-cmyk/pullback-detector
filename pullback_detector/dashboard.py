"""Read-only browser dashboard backed by accepted market data and persistent setup lifecycle state."""
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
            return handle.readlines()[-max_lines:]
    except OSError:
        return []


class DashboardData:
    """Read-only projection of accepted market data, V2 anatomy and persistent setup lifecycle."""

    def __init__(self, data_root: str | Path, state: dict):
        self.root = Path(data_root)
        self.state = state
        self._lock = threading.Lock()
        self._cache = None

    def _universe(self):
        path = self.root / "universe.csv"
        if not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                return list(csv.DictReader(handle))
        except (OSError, csv.Error):
            return []

    def _runtime_file(self, directory):
        return self.root / directory / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"

    @staticmethod
    def _parse_time(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except (ValueError, TypeError):
            return None

    @classmethod
    def _stale(cls, tick, now):
        received = cls._parse_time(tick.get("received_at"))
        return received is None or (now - received).total_seconds() > 60

    def _lifecycle(self):
        engine = PullbackLifecycleEngine(self.root)
        snap = engine.snapshot()

        def state_dict(state):
            data = asdict(state)
            data["snapshot"] = asdict(state.snapshot)
            return json.loads(json.dumps(data, default=_jsonable))

        return [state_dict(x) for x in snap["active"]], [state_dict(x) for x in snap["closed"]]

    def snapshot(self):
        now = datetime.now(timezone.utc)
        minute_file = self._runtime_file("normalized")
        lifecycle_file = self.root / "setup_events.jsonl"
        cache_key = (
            minute_file.stat().st_mtime if minute_file.exists() else 0.0,
            lifecycle_file.stat().st_mtime if lifecycle_file.exists() else 0.0,
            str(self.state.get("status")),
            str(self.state.get("last_error") or ""),
            len(LIVE_ANATOMY),
        )
        with self._lock:
            if self._cache and self._cache[0] == cache_key:
                return self._cache[1]
            universe = self._universe()
            latest = {}
            for line in _read_tail(minute_file):
                try:
                    tick = json.loads(line)
                    latest[str(tick.get("instrument_id"))] = tick
                except json.JSONDecodeError:
                    continue
            candles = {}
            candle_file = self._runtime_file("candles")
            for line in _read_tail(candle_file):
                try:
                    candle = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if candle.get("complete"):
                    candles.setdefault(str(candle.get("instrument_id")), {})[str(candle.get("timeframe_seconds", 300))] = candle
            signals = []
            signal_file = self._runtime_file("signals")
            for line in _read_tail(signal_file, 1000):
                try:
                    signals.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
            signals.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            instruments = []
            for row in universe:
                sid = str(row.get("security_id"))
                tick = latest.get(sid, {})
                c = candles.get(sid, {})
                anatomy = dict(LIVE_ANATOMY.get(int(sid), {})) if sid.isdigit() else {}
                instruments.append({
                    "security_id": row.get("security_id"),
                    "symbol": row.get("symbol") or row.get("trading_symbol") or sid,
                    "trading_symbol": row.get("trading_symbol") or row.get("symbol") or sid,
                    "latest_price": tick.get("price"),
                    "tick_timestamp": tick.get("timestamp"),
                    "receive_timestamp": tick.get("received_at"),
                    "stale": self._stale(tick, now),
                    "candle_1m": c.get("60"),
                    "candle_5m": c.get("300"),
                    "anatomy": anatomy,
                })
            active_signals = [s for s in signals if self._parse_time(s.get("timestamp")) and self._parse_time(s["timestamp"]) >= now - timedelta(minutes=30)]
            active_setups, closed_setups = self._lifecycle()
            health = dict(self.state.get("last_report") or {})
            health["service_status"] = self.state.get("status", health.get("service_status", "starting"))
            health["last_error"] = self.state.get("last_error")
            payload = {
                "generated_at": now.isoformat(),
                "health": health,
                "instruments": instruments,
                "active_signals": active_signals[:25],
                "recent_signals": signals[:50],
                "active_setups": active_setups,
                "recently_closed_setups": closed_setups[:50],
                "signal_window_minutes": 30,
            }
            self._cache = (cache_key, payload)
            return payload


HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#071019"><title>PSYCHO // PULLBACK DETECTOR</title>
<style>
:root{--bg:#071019;--panel:#0d1822;--panel2:#111f2b;--panel3:#142633;--line:#29404f;--line2:#3a5566;--text:#f7fbff;--text2:#d6e1e9;--muted:#9fb1bf;--dim:#718594;--cyan:#48dcff;--cyan2:#86ebff;--green:#50e5a7;--red:#ff7186;--amber:#ffd166;--blue:#6ea8ff;--shadow:0 22px 65px rgba(0,0,0,.48);--mono:"SFMono-Regular",Consolas,"Liberation Mono",monospace}
*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;min-height:100vh;color:var(--text);background:radial-gradient(900px 520px at 72% -12%,rgba(72,220,255,.11),transparent 67%),radial-gradient(760px 520px at -8% 48%,rgba(67,112,255,.09),transparent 70%),linear-gradient(180deg,#08131d,#071019 60%,#060d14);font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;overflow-x:hidden}.scene{position:fixed;inset:0;pointer-events:none;z-index:-2;overflow:hidden}.grid{position:absolute;inset:-20%;opacity:.35;background-image:linear-gradient(rgba(125,183,211,.055) 1px,transparent 1px),linear-gradient(90deg,rgba(125,183,211,.055) 1px,transparent 1px);background-size:58px 58px;transform:perspective(700px) rotateX(62deg) translateY(27%);animation:drift 25s linear infinite}.watermark{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);font-size:clamp(110px,22vw,360px);font-weight:950;letter-spacing:.16em;color:#dff8ff;opacity:.025;white-space:nowrap;filter:blur(.4px)}.candles{position:absolute;inset:0;opacity:.14;background:repeating-linear-gradient(90deg,transparent 0 47px,rgba(72,220,255,.05) 48px 49px);animation:float 30s ease-in-out infinite}.candles:before{content:"";position:absolute;left:10%;top:22%;width:75%;height:46%;background:linear-gradient(90deg,transparent,rgba(72,220,255,.08),transparent);clip-path:polygon(0 65%,3% 48%,5% 56%,8% 35%,11% 43%,14% 24%,17% 37%,20% 29%,23% 52%,26% 39%,29% 47%,32% 20%,35% 31%,38% 18%,41% 41%,44% 35%,47% 58%,50% 44%,53% 55%,56% 31%,59% 39%,62% 25%,65% 43%,68% 34%,71% 53%,74% 45%,77% 60%,80% 41%,83% 48%,86% 29%,89% 37%,92% 22%,95% 35%,100% 25%,100% 100%,0 100%)}.bloom{position:absolute;width:52vw;height:52vw;border-radius:50%;left:53%;top:7%;background:radial-gradient(circle,rgba(72,220,255,.055),transparent 68%);filter:blur(22px);animation:bloom 14s ease-in-out infinite}@keyframes drift{to{transform:perspective(700px) rotateX(62deg) translateY(35%)}}@keyframes float{50%{transform:translateX(-32px)}}@keyframes bloom{50%{transform:translate(2%,5%) scale(1.08)}}
.shell{width:min(1680px,100%);margin:auto;padding:18px clamp(13px,2vw,34px) 50px}.top{position:relative;border:1px solid var(--line2);border-radius:15px;background:linear-gradient(135deg,rgba(17,31,43,.96),rgba(9,19,28,.93));box-shadow:var(--shadow),0 0 40px rgba(72,220,255,.035);backdrop-filter:blur(18px);overflow:hidden}.top:after{content:"";position:absolute;left:-25%;top:0;width:22%;height:2px;background:linear-gradient(90deg,transparent,var(--cyan2),transparent);animation:scan 6s linear infinite;opacity:.8}@keyframes scan{to{left:125%}}.topin{display:grid;grid-template-columns:1fr auto;gap:20px;align-items:center;padding:21px 23px}.kicker{font-size:10px;color:var(--cyan2);letter-spacing:.22em;font-weight:750}.brand{margin-top:6px;font-size:clamp(24px,3.2vw,39px);font-weight:900;letter-spacing:.055em;color:#fff}.brand .slash{color:var(--cyan)}.sub{margin-top:8px;color:var(--text2);font-size:11px;letter-spacing:.14em;text-transform:uppercase}.topstats{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px}.chip{min-width:105px;padding:9px 11px;border:1px solid var(--line2);border-radius:9px;background:rgba(3,9,14,.48)}.chip label{display:block;color:var(--muted);font-size:8px;letter-spacing:.13em;text-transform:uppercase}.chip strong{display:block;margin-top:4px;color:#fff;font:700 12px var(--mono)}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--muted);margin-right:5px}.ok .dot{background:var(--green);box-shadow:0 0 12px rgba(80,229,167,.55)}.bad .dot{background:var(--red);box-shadow:0 0 12px rgba(255,113,134,.45)}.warn .dot{background:var(--amber)}
.marquee{margin-top:10px;border:1px solid var(--line2);border-radius:9px;background:rgba(6,15,22,.9);overflow:hidden;white-space:nowrap}.track{display:flex;width:max-content;animation:ticker 38s linear infinite}.marquee:hover .track{animation-play-state:paused}.tickitem{padding:10px 17px;border-right:1px solid var(--line);font:700 11px var(--mono);color:var(--text2)}.tickitem b{color:#fff}.up{color:var(--green)!important}.down{color:var(--red)!important}@keyframes ticker{to{transform:translateX(-50%)}}
.hero{display:grid;grid-template-columns:1.55fr .7fr;gap:11px;margin-top:13px}.panel{border:1px solid var(--line2);border-radius:13px;background:linear-gradient(145deg,rgba(17,31,42,.94),rgba(10,20,29,.92));box-shadow:var(--shadow);backdrop-filter:blur(16px)}.heroMain{padding:19px}.label{font-size:9px;color:var(--cyan2);letter-spacing:.19em;text-transform:uppercase;font-weight:800}.heroRow{display:flex;align-items:end;justify-content:space-between;gap:12px;margin-top:5px}.heroRow h1{margin:0;font-size:18px;letter-spacing:.1em}.heroNum{font:800 38px var(--mono);color:#fff}.heroMeta{display:flex;gap:17px;flex-wrap:wrap;margin-top:13px;color:var(--text2);font:11px var(--mono)}.health{height:4px;background:#1a2a36;margin-top:16px;border-radius:99px;overflow:hidden}.health i{display:block;height:100%;background:linear-gradient(90deg,var(--red),var(--amber),var(--green));box-shadow:0 0 14px rgba(72,220,255,.22);transition:width .5s}.regime{padding:19px;display:flex;flex-direction:column;justify-content:center}.regime strong{font-size:21px;color:#fff}.regime small{margin-top:6px;color:var(--text2);font:11px var(--mono)}
.section{margin-top:20px}.head{display:flex;align-items:center;gap:11px;margin-bottom:10px}.head h2{margin:0;font-size:11px;letter-spacing:.2em;font-weight:850;color:#fff}.head .line{height:1px;flex:1;background:linear-gradient(90deg,var(--line2),transparent)}.head em{font:10px var(--mono);font-style:normal;color:var(--muted)}.telemetry{display:grid;grid-template-columns:repeat(7,1fr);gap:8px}.telemetry .panel{padding:13px}.metricLabel{font-size:8px;color:var(--muted);letter-spacing:.14em;text-transform:uppercase;font-weight:750}.metricValue{margin-top:6px;font:800 18px var(--mono);color:#fff}.metricSub{margin-top:3px;color:var(--dim);font-size:9px}
.activeGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(500px,1fr));gap:12px}.activeCard{position:relative;padding:18px;overflow:hidden;border-color:rgba(72,220,255,.32);transition:transform .25s,border-color .25s,box-shadow .25s}.activeCard:hover{transform:translateY(-4px);border-color:rgba(72,220,255,.7);box-shadow:0 28px 80px rgba(0,0,0,.55),0 0 35px rgba(72,220,255,.07)}.activeCard:before{content:"";position:absolute;left:0;top:15px;bottom:15px;width:3px;background:var(--cyan);box-shadow:0 0 15px rgba(72,220,255,.45)}.cardTop{display:flex;justify-content:space-between;align-items:center;gap:10px}.symbol{font-size:22px;font-weight:900;letter-spacing:.04em}.price{font:800 25px var(--mono);color:#fff}.badge{padding:6px 8px;border:1px solid var(--line2);border-radius:6px;font-size:9px;font-weight:850;letter-spacing:.1em}.badge.long{color:var(--green);border-color:rgba(80,229,167,.35);background:rgba(80,229,167,.07)}.badge.short{color:var(--red);border-color:rgba(255,113,134,.35);background:rgba(255,113,134,.07)}.pipeline{display:flex;gap:4px;align-items:center;overflow:auto;margin:16px 0 13px;padding-bottom:2px}.stage{padding:6px 7px;border:1px solid var(--line);border-radius:5px;color:#728694;font-size:8px;letter-spacing:.07em;white-space:nowrap}.stage.on{color:#041016;background:var(--cyan);border-color:var(--cyan);font-weight:900;box-shadow:0 0 18px rgba(72,220,255,.22)}.arrow{color:#526674;font-size:10px}.facts{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}.fact{padding:9px;background:rgba(3,10,16,.48);border:1px solid var(--line);border-radius:7px}.fact label{display:block;color:var(--muted);font-size:8px;letter-spacing:.1em;text-transform:uppercase}.fact strong{display:block;margin-top:5px;color:#fff;font:700 12px var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.score{display:grid;grid-template-columns:auto 1fr auto;gap:10px;align-items:center;margin-top:11px}.score b{font:900 22px var(--mono)}.bar{height:7px;background:#1b2b37;border-radius:99px;overflow:hidden}.bar i{display:block;height:100%;width:0;background:linear-gradient(90deg,var(--blue),var(--cyan),var(--green));transition:width .55s}.levels{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-top:11px}.level{padding:8px 7px;border:1px solid var(--line);border-radius:6px;text-align:center}.level small{display:block;color:var(--muted);font-size:8px}.level b{display:block;margin-top:4px;font:700 11px var(--mono);color:#fff}.level.current{border-color:rgba(72,220,255,.5);background:rgba(72,220,255,.055)}
.anatomyGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:10px}.anatomy{padding:15px;transition:transform .22s,border-color .22s}.anatomy:hover{transform:translateY(-3px);border-color:var(--line2)}.anHead{display:flex;justify-content:space-between;align-items:center}.anSymbol{font-size:16px;font-weight:850}.anPrice{font:700 18px var(--mono)}.state{margin-top:5px;color:var(--cyan2);font-size:9px;font-weight:850;letter-spacing:.12em}.anFacts{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-top:12px}.mini{padding:8px;background:rgba(3,10,16,.46);border:1px solid var(--line);border-radius:6px}.mini span{display:block;color:var(--muted);font-size:7px;letter-spacing:.09em;text-transform:uppercase}.mini b{display:block;margin-top:4px;font:700 11px var(--mono);color:#fff}.spark{height:42px;margin-top:10px;border:1px solid var(--line);border-radius:6px;position:relative;overflow:hidden;background:linear-gradient(180deg,rgba(72,220,255,.035),rgba(3,10,16,.55))}.spark svg{width:100%;height:100%}.closedGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:9px}.closed{padding:13px}.closedTop{display:flex;justify-content:space-between;gap:10px}.outcome{font-size:9px;font-weight:900;letter-spacing:.08em;padding:5px 7px;border-radius:5px}.t1,.t2{color:var(--green);background:rgba(80,229,167,.09);border:1px solid rgba(80,229,167,.3)}.invalid,.failed{color:var(--red);background:rgba(255,113,134,.08);border:1px solid rgba(255,113,134,.3)}.expired{color:var(--amber);background:rgba(255,209,102,.08);border:1px solid rgba(255,209,102,.3)}.closedMeta{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:9px}.closedMeta div{font:700 10px var(--mono);color:#fff}.closedMeta span{display:block;color:var(--muted);font:8px Inter,sans-serif;margin-bottom:3px}.reject{display:grid;grid-template-columns:auto 1fr auto;gap:12px;align-items:center;padding:10px 12px;border-bottom:1px solid var(--line);background:rgba(255,113,134,.025)}.reject:last-child{border-bottom:0}.reject time{font:10px var(--mono);color:var(--muted)}.reject strong{font-size:11px;color:#fff}.reject small{display:block;margin-top:3px;color:var(--red);font:700 9px var(--mono)}.reject em{font:9px var(--mono);font-style:normal;color:var(--text2)}.matrix{overflow:auto}.matrix table{width:100%;border-collapse:collapse;min-width:900px}.matrix th{padding:11px;text-align:left;color:var(--muted);font-size:8px;letter-spacing:.13em;text-transform:uppercase;border-bottom:1px solid var(--line2)}.matrix td{padding:11px;color:#edf4f8;font:600 11px var(--mono);border-bottom:1px solid var(--line)}.matrix tr:hover td{background:rgba(72,220,255,.035)}.liveDot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);margin-right:6px;box-shadow:0 0 9px rgba(80,229,167,.5)}.stale{color:var(--amber)!important}.empty{padding:24px;text-align:center;color:var(--muted);font-size:11px}.footer{margin-top:20px;color:var(--dim);font:9px var(--mono);text-align:center}
@media(max-width:1100px){.telemetry{grid-template-columns:repeat(4,1fr)}.hero{grid-template-columns:1fr}.topin{grid-template-columns:1fr}.topstats{justify-content:flex-start}}@media(max-width:720px){.shell{padding:10px 9px 30px}.topin{padding:16px}.brand{font-size:24px}.heroNum{font-size:28px}.telemetry{grid-template-columns:repeat(2,1fr)}.activeGrid{grid-template-columns:1fr}.facts{grid-template-columns:repeat(2,1fr)}.levels{grid-template-columns:repeat(5,minmax(72px,1fr));overflow:auto}.anFacts{grid-template-columns:repeat(2,1fr)}.topstats{display:grid;grid-template-columns:repeat(2,1fr)}.chip{min-width:0}.marquee{margin-top:8px}.watermark{font-size:90px;transform:translate(-50%,-50%) rotate(-8deg)}body{font-size:14px}}
@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.001ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important;transition:none!important}}
</style></head>
<body>
<div class="scene"><div class="grid"></div><div class="watermark" id="wm">PSYCHO</div><div class="candles"></div><div class="bloom"></div></div>
<main class="shell">
<header class="top"><div class="topin"><div><div class="kicker">LIVE MARKET INTELLIGENCE</div><div class="brand">PSYCHO <span class="slash">//</span> PULLBACK DETECTOR</div><div class="sub">Deterministic V2 · NSE Equity · Experimental · Not Profitability Validated</div></div><div class="topstats" id="topstats"></div></div></header>
<div class="marquee"><div class="track" id="ticker"></div></div>
<section class="hero"><div class="panel heroMain"><div class="label">SYSTEM OVERVIEW</div><div class="heroRow"><h1 id="heroState">INITIALISING</h1><div class="heroNum" id="heroCount">—</div></div><div class="heroMeta"><span id="heroFeed">FEED —</span><span id="heroPackets">PACKETS —</span><span id="heroTicks">TICKS —</span><span id="heroUpdated">UPDATE —</span></div><div class="health"><i id="healthbar"></i></div></div><div class="panel regime"><div class="label">MARKET REGIME</div><strong id="regime">WAITING</strong><small id="regimeSub">Awaiting backend state</small></div></section>
<section class="section"><div class="head"><h2>LIVE TELEMETRY</h2><div class="line"></div><em id="freshness">—</em></div><div class="telemetry" id="telemetry"></div></section>
<section class="section"><div class="head"><h2>ACTIVE SETUPS</h2><div class="line"></div><em id="activeCount">0</em></div><div id="active" class="activeGrid"></div></section>
<section class="section"><div class="head"><h2>LIVE PULLBACK ANATOMY</h2><div class="line"></div><em id="anatomyCount">0 instruments</em></div><div id="anatomy" class="anatomyGrid"></div></section>
<section class="section"><div class="head"><h2>MONITORED INSTRUMENTS</h2><div class="line"></div><em>REAL BACKEND VALUES</em></div><div class="panel matrix"><table><thead><tr><th>Instrument</th><th>Price</th><th>State</th><th>Impulse</th><th>Pullback</th><th>5M</th><th>Feed</th></tr></thead><tbody id="matrix"></tbody></table></div></section>
<section class="section"><div class="head"><h2>RECENTLY CLOSED SETUPS</h2><div class="line"></div><em id="closedCount">0</em></div><div id="closed" class="closedGrid"></div></section>
<section class="section"><div class="head"><h2>REJECTION MONITOR</h2><div class="line"></div><em>EXPLAINABLE V2 FILTERS</em></div><div class="panel" id="rejections"></div></section>
<div class="footer">PSYCHO // PULLBACK DETECTOR · READ-ONLY VISUAL LAYER · MARKET DATA AND DETECTOR STATE ARE SOURCED FROM THE EXISTING BACKEND</div>
</main>
<script>
const $=id=>document.getElementById(id);let previousPrices=new Map();
const esc=v=>String(v??'—').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const num=(v,d=2)=>v===null||v===undefined||v===''?'—':Number(v).toFixed(d);const pct=v=>v===null||v===undefined?'—':`${num(v,1)}%`;
const age=t=>{if(!t)return'—';const s=Math.max(0,(Date.now()-new Date(t).getTime())/1000);return s<60?`${Math.round(s)}s`:s<3600?`${Math.floor(s/60)}m`:`${Math.floor(s/3600)}h`};
function stages(state){const all=['EXPANSION','IMPULSE','PULLBACK','HEALTHY','TRIGGER','ACTIVE','OUTCOME'];const s=String(state||'WATCHING').toUpperCase();let idx=s.includes('IMPULSE')?1:s.includes('PULLBACK')?2:s.includes('HEALTHY')?3:s.includes('TRIGGER')?4:s.includes('ACTIVE')?5:['FAILED','TARGET_1_HIT','TARGET_2_HIT','INVALIDATED','EXPIRED'].some(x=>s.includes(x))?6:0;return all.map((x,i)=>`${i?'<span class="arrow">›</span>':''}<span class="stage ${i===idx?'on':''}">${x}</span>`).join('')}
function spark(c){if(!c)return'<div class="spark"></div>';const o=Number(c.open),h=Number(c.high),l=Number(c.low),cl=Number(c.close),lo=Math.min(l,o,cl),hi=Math.max(h,o,cl),span=Math.max(hi-lo,1e-9),y=v=>36-((v-lo)/span)*28;const up=cl>=o;return`<div class="spark"><svg viewBox="0 0 160 42" preserveAspectRatio="none"><line x1="48" x2="48" y1="${y(h)}" y2="${y(l)}" stroke="#9bb0bd"/><rect x="43" y="${Math.min(y(o),y(cl))}" width="10" height="${Math.max(3,Math.abs(y(o)-y(cl)))}" fill="${up?'#50e5a7':'#ff7186'}"/><line x1="100" x2="100" y1="${y(h)}" y2="${y(l)}" stroke="#9bb0bd"/><rect x="95" y="${Math.min(y(o),y(cl))}" width="10" height="${Math.max(3,Math.abs(y(o)-y(cl)))}" fill="${up?'#50e5a7':'#ff7186'}"/></svg></div>`}
function activeCard(s){const snap=s.snapshot||s,dir=snap.direction||snap.impulse_direction||'',cur=s.current_price??snap.trigger_price,inv=snap.invalidation_price??snap.invalidation_level,t1=snap.target_1,t2=snap.target_2;return`<article class="panel activeCard"><div class="cardTop"><div><div class="symbol">${esc(snap.instrument||snap.symbol||s.instrument_id||'—')}</div><div class="state">ACTIVE · ${esc(s.state||snap.classification||'SETUP')}</div></div><div style="text-align:right"><div class="price">${num(cur)}</div><span class="badge ${dir==='SHORT'?'short':'long'}">${esc(dir||'—')}</span></div></div><div class="pipeline">${stages('ACTIVE')}</div><div class="levels"><div class="level"><small>INVALIDATION</small><b>${num(inv)}</b></div><div class="level current"><small>CURRENT</small><b>${num(cur)}</b></div><div class="level"><small>TARGET 1</small><b>${num(t1)}</b></div><div class="level"><small>TARGET 2</small><b>${num(t2)}</b></div><div class="level"><small>AGE</small><b>${age(snap.creation_timestamp||snap.created_at)}</b></div></div><div class="facts" style="margin-top:9px"><div class="fact"><label>MFE</label><strong>${num(s.mfe??snap.mfe)}</strong></div><div class="fact"><label>MAE</label><strong>${num(s.mae??snap.mae)}</strong></div><div class="fact"><label>DIST T1</label><strong>${num(s.distance_to_target_1??snap.distance_to_target_1)}</strong></div><div class="fact"><label>DIST INV</label><strong>${num(s.distance_to_invalidation??snap.distance_to_invalidation)}</strong></div></div><div class="score"><b>${snap.health_score??'—'}</b><div class="bar"><i style="width:${Math.max(0,Math.min(100,Number(snap.health_score||0)))}%"></i></div><span style="color:var(--muted);font-size:9px">HEALTH</span></div></article>`}
function anatomyCard(i){const a=i.anatomy||{},p=i.latest_price??a.current_price,state=a.state||a.detection_phase||'WATCHING',direction=a.impulse_direction||'—';return`<article class="panel anatomy"><div class="anHead"><div><div class="anSymbol">${esc(i.symbol)}</div><div class="state">${esc(state)}</div></div><div class="anPrice">${num(p)}</div></div><div class="pipeline">${stages(state)}</div><div class="anFacts"><div class="mini"><span>IMPULSE</span><b>${num(a.impulse_magnitude)}</b></div><div class="mini"><span>DIRECTION</span><b class="${direction==='SHORT'?'down':'up'}">${esc(direction)}</b></div><div class="mini"><span>DEPTH</span><b>${pct(a.retracement_depth_pct)}</b></div><div class="mini"><span>DURATION</span><b>${a.pullback_duration_candles??'—'} × 5M</b></div><div class="mini"><span>SPEED</span><b>${num(a.pullback_speed)}</b></div><div class="mini"><span>EFFICIENCY</span><b>${num(a.pullback_efficiency)}</b></div><div class="mini"><span>STRUCTURE</span><b>${a.protected_level?'PRESERVED':'—'}</b></div><div class="mini"><span>VOLUME</span><b>${num(a.volume_ratio)}</b></div><div class="mini"><span>HEALTH</span><b>${a.health_score??'—'}</b></div></div>${spark(i.candle_5m)}</article>`}
function closedCard(s){const snap=s.snapshot||s,outcome=String(s.outcome||s.state||'CLOSED').toUpperCase(),cls=outcome.includes('TARGET_2')?'t2':outcome.includes('TARGET_1')?'t1':outcome.includes('INVALID')?'invalid':outcome.includes('FAILED')?'failed':'expired';return`<article class="panel closed"><div class="closedTop"><strong>${esc(snap.instrument||snap.symbol||'—')}</strong><span class="outcome ${cls}">${esc(outcome.replaceAll('_',' '))}</span></div><div class="closedMeta"><div><span>DIRECTION</span>${esc(snap.direction||'—')}</div><div><span>TRIGGER</span>${num(snap.trigger_price)}</div><div><span>T1</span>${num(snap.target_1)}</div><div><span>T2</span>${num(snap.target_2)}</div><div><span>MFE</span>${num(s.mfe??s.max_favorable_excursion)}</div><div><span>MAE</span>${num(s.mae??s.max_adverse_excursion)}</div></div></article>`}
function rejectionRows(instruments){let rows=[];for(const i of instruments){const r=i.anatomy?.last_rejection;if(r)rows.push(r)}rows.sort((a,b)=>String(b.timestamp).localeCompare(String(a.timestamp)));return rows.slice(0,12).map(r=>`<div class="reject"><time>${esc(new Date(r.timestamp).toLocaleTimeString())}</time><div><strong>${esc(r.symbol||r.instrument_id||'—')}</strong><small>${esc(String(r.reason||'REJECTED').replaceAll('_',' '))}</small></div><em>${esc(r.stage||'—')} · ${num(r.actual_value)} / ${num(r.threshold)}</em></div>`).join('')}
function render(d){const h=d.health||{},ins=d.instruments||[],active=d.active_setups||[],closed=d.recently_closed_setups||[],producing=ins.filter(x=>x.latest_price!==null&&x.latest_price!==undefined&&!x.stale).length,status=String(h.service_status||'UNKNOWN').toUpperCase(),market=producing?'FEED CONNECTED':status==='LIVE'?'WAITING FOR MARKET':'Dhan DISCONNECTED';$('topstats').innerHTML=`<div class="chip ${status==='LIVE'?'ok':'bad'}"><label>CONNECTION</label><strong><span class="dot"></span>${esc(status)}</strong></div><div class="chip"><label>INSTRUMENTS</label><strong>${producing} / ${ins.length}</strong></div><div class="chip"><label>PACKETS</label><strong>${h.packets??'—'}</strong></div><div class="chip"><label>TICKS</label><strong>${h.ticks??h.accepted_ticks??'—'}</strong></div><div class="chip"><label>LAST UPDATE</label><strong>${d.generated_at?new Date(d.generated_at).toLocaleTimeString():'—'}</strong></div>`;$('heroState').textContent=market;$('heroCount').textContent=active.length;$('heroFeed').textContent=`FEED ${status}`;$('heroPackets').textContent=`PACKETS ${h.packets??'—'}`;$('heroTicks').textContent=`TICKS ${h.ticks??h.accepted_ticks??'—'}`;$('heroUpdated').textContent=`UPDATE ${d.generated_at?new Date(d.generated_at).toLocaleTimeString():'—'}`;$('regime').textContent=h.session||h.market_session||'WAITING';$('regimeSub').textContent=market;$('healthbar').style.width=`${producing?Math.round(producing/Math.max(1,ins.length)*100):0}%`;$('freshness').textContent=`${producing} LIVE`;
const metric=(l,v,s='')=>`<div class="panel"><div class="metricLabel">${l}</div><div class="metricValue">${v}</div><div class="metricSub">${s}</div></div>`;$('telemetry').innerHTML=metric('ACCEPTED TICKS',h.ticks??h.accepted_ticks??'—','accepted feed')+metric('RAW PACKETS',h.packets??'—','Dhan WebSocket')+metric('PRODUCING',producing,`of ${ins.length}`)+metric('1M CANDLES',h.candles_1m??'—','completed')+metric('5M CANDLES',h.candles_5m??'—','completed')+metric('RECONNECTS',h.reconnects??'—','WebSocket')+metric('ACTIVE SETUPS',active.length,'lifecycle');
$('activeCount').textContent=`${active.length} LIVE`;$('active').innerHTML=active.length?active.map(activeCard).join(''):'<div class="panel empty">NO ACTIVE SETUPS — waiting for a genuine V2 trigger.</div>';$('anatomyCount').textContent=`${ins.length} instruments`;$('anatomy').innerHTML=ins.length?ins.map(anatomyCard).join(''):'<div class="panel empty">No monitored instruments available.</div>';
$('matrix').innerHTML=ins.map(i=>{const a=i.anatomy||{};return`<tr><td><span class="liveDot"></span><strong>${esc(i.symbol)}</strong></td><td>${num(i.latest_price)}</td><td>${esc(a.state||'WATCHING')}</td><td>${num(a.impulse_magnitude)} · ${esc(a.impulse_direction||'—')}</td><td>${pct(a.retracement_depth_pct)}</td><td>${i.candle_5m?'READY':'—'}</td><td class="${i.stale?'stale':''}">${i.stale?'STALE':'LIVE'}</td></tr>`}).join('');$('closedCount').textContent=`${closed.length} RECENT`;$('closed').innerHTML=closed.length?closed.map(closedCard).join(''):'<div class="panel empty">NO CLOSED SETUPS YET.</div>';$('rejections').innerHTML=rejectionRows(ins)||'<div class="empty">NO RECENT REJECTIONS REPORTED BY THE DETECTOR.</div>';
for(const i of ins){const p=i.latest_price;if(p!==null&&p!==undefined&&previousPrices.has(i.symbol)&&previousPrices.get(i.symbol)!==p){const el=[...document.querySelectorAll('.anatomy')].find(x=>x.querySelector('.anSymbol')?.textContent===i.symbol);if(el){el.style.boxShadow='0 0 28px rgba(72,220,255,.16)';setTimeout(()=>el.style.boxShadow='',650)}}if(p!==null&&p!==undefined)previousPrices.set(i.symbol,p)}
const ticker=ins.map(i=>`<div class="tickitem"><b>${esc(i.symbol)}</b> <span>${num(i.latest_price)}</span> <small>${esc(i.anatomy?.state||'WATCHING')}</small></div>`).join('');$('ticker').innerHTML=ticker+ticker;
}
async function refresh(){try{const r=await fetch('/api/dashboard',{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);render(await r.json())}catch(e){$('heroState').textContent='DASHBOARD DEGRADED';$('heroFeed').textContent='API OFFLINE';console.warn(e)}}
window.addEventListener('mousemove',e=>{const wm=$('wm');if(!wm)return;wm.style.marginLeft=`${(e.clientX/innerWidth-.5)*7}px`;wm.style.marginTop=`${(e.clientY/innerHeight-.5)*5}px`});refresh();setInterval(refresh,3000);
</script></body></html>'''
