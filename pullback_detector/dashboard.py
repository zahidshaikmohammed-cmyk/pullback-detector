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
                    candles.setdefault(str(candle.get("instrument_id")), {})[
                        str(candle.get("timeframe_seconds", 300))
                    ] = candle
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
                candle_set = candles.get(sid, {})
                anatomy = dict(LIVE_ANATOMY.get(int(sid), {})) if sid.isdigit() else {}
                instruments.append(
                    {
                        "security_id": row.get("security_id"),
                        "symbol": row.get("symbol") or row.get("trading_symbol") or sid,
                        "trading_symbol": row.get("trading_symbol") or row.get("symbol") or sid,
                        "latest_price": tick.get("price"),
                        "tick_timestamp": tick.get("timestamp"),
                        "receive_timestamp": tick.get("received_at"),
                        "stale": self._stale(tick, now),
                        "candle_1m": candle_set.get("60"),
                        "candle_5m": candle_set.get("300"),
                        "anatomy": anatomy,
                    }
                )
            active_signals = [
                s
                for s in signals
                if self._parse_time(s.get("timestamp"))
                and self._parse_time(s["timestamp"]) >= now - timedelta(minutes=30)
            ]
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
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070808">
<title>PSYCHO // PULLBACK DETECTOR</title>
<style>
:root{--bg:#070808;--ink:#f2f3f4;--muted:#87909a;--dim:#56606a;--graphite:#111416;--graphite2:#171b1e;--line:rgba(220,228,234,.105);--line-hi:rgba(220,228,234,.19);--silver:#c9d0d5;--cyan:#69d7e8;--green:#4cc995;--red:#d96b78;--amber:#d7ae64;--shadow:0 26px 80px rgba(0,0,0,.48);--mono:"SFMono-Regular",Consolas,"Liberation Mono",monospace}*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;min-height:100vh;color:var(--ink);background:radial-gradient(900px 520px at 70% -15%,rgba(157,174,186,.075),transparent 68%),radial-gradient(700px 500px at 0 55%,rgba(73,92,103,.045),transparent 70%),var(--bg);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;overflow-x:hidden}.scene,.scene *{pointer-events:none}.scene{position:fixed;inset:0;overflow:hidden;z-index:-3;background:linear-gradient(180deg,rgba(255,255,255,.012),transparent 25%)}.grid{position:absolute;inset:-20%;opacity:.23;background-image:linear-gradient(rgba(180,193,203,.028) 1px,transparent 1px),linear-gradient(90deg,rgba(180,193,203,.028) 1px,transparent 1px);background-size:64px 64px;transform:perspective(700px) rotateX(63deg) translateY(27%);animation:grid-drift 28s linear infinite}.watermark{position:absolute;left:50%;top:50%;font-size:clamp(120px,24vw,390px);font-weight:900;letter-spacing:.18em;color:#fff;opacity:.014;white-space:nowrap;transform:translate(-50%,-50%);filter:blur(.35px);will-change:transform}.candlefield{position:absolute;inset:0;opacity:.12;background:repeating-linear-gradient(90deg,transparent 0 45px,rgba(105,215,232,.025) 46px 47px);animation:field-drift 36s ease-in-out infinite}.candlefield:before{content:"";position:absolute;inset:12% 4%;background:linear-gradient(90deg,transparent 0 8%,rgba(105,215,232,.07) 8.1% 8.25%,transparent 8.3% 17%,rgba(76,201,149,.05) 17.1% 17.25%,transparent 17.3% 29%,rgba(217,107,120,.05) 29.1% 29.25%,transparent 29.3%);mask-image:linear-gradient(90deg,transparent,black 20%,black 80%,transparent);animation:signal-flow 18s linear infinite}.bloom{position:absolute;width:45vw;height:45vw;border-radius:50%;background:radial-gradient(circle,rgba(190,202,211,.025),transparent 68%);filter:blur(30px);left:55%;top:15%;animation:bloom 16s ease-in-out infinite}.particles{position:absolute;inset:0;background-image:radial-gradient(circle,rgba(200,210,218,.12) 0 1px,transparent 1.5px);background-size:150px 170px;opacity:.08;animation:particles 40s linear infinite}@keyframes grid-drift{to{transform:perspective(700px) rotateX(63deg) translateY(34%)}}@keyframes field-drift{50%{transform:translateX(-34px)}}@keyframes signal-flow{to{transform:translateX(160px)}}@keyframes bloom{50%{transform:translate(-3%,4%) scale(1.08)}}@keyframes particles{to{transform:translateY(-170px)}}
.shell{width:min(1660px,100%);margin:auto;padding:18px clamp(12px,2vw,32px) 46px}.top{position:relative;border:1px solid var(--line);border-radius:13px;background:linear-gradient(180deg,rgba(22,25,27,.9),rgba(12,14,15,.82));box-shadow:var(--shadow);backdrop-filter:blur(22px);overflow:hidden}.top:after{content:"";position:absolute;top:0;left:-25%;width:22%;height:1px;background:linear-gradient(90deg,transparent,var(--silver),transparent);opacity:.55;animation:scan 7s linear infinite}@keyframes scan{to{left:125%}}.top-inner{display:grid;grid-template-columns:minmax(300px,1fr) auto;gap:20px;align-items:center;padding:19px 21px}.kicker{font-size:9px;letter-spacing:.24em;color:var(--dim);text-transform:uppercase}.brand{margin-top:5px;font-size:clamp(22px,3.2vw,37px);line-height:1;font-weight:760;letter-spacing:.065em}.brand .slash{color:var(--cyan);font-weight:500}.sub{margin-top:7px;color:var(--muted);font-size:10px;letter-spacing:.13em;text-transform:uppercase}.top-stats{display:flex;justify-content:flex-end;flex-wrap:wrap;gap:7px}.stat-chip{min-width:100px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:rgba(0,0,0,.2)}.stat-chip label{display:block;color:var(--dim);font-size:7px;letter-spacing:.14em;text-transform:uppercase}.stat-chip strong{display:block;margin-top:3px;font:600 11px var(--mono);color:#dce1e4}.light{display:inline-block;width:6px;height:6px;border-radius:50%;margin-right:5px;background:#697179}.ok .light{background:var(--green);box-shadow:0 0 10px rgba(76,201,149,.45)}.warn .light{background:var(--amber)}.bad .light{background:var(--red);box-shadow:0 0 10px rgba(217,107,120,.35)}
.marquee{margin-top:10px;border:1px solid var(--line);border-radius:9px;background:rgba(10,12,13,.74);overflow:hidden;white-space:nowrap}.marquee:hover .ticker-track{animation-play-state:paused}.ticker-track{display:flex;width:max-content;animation:ticker 42s linear infinite}.ticker-item{position:relative;padding:9px 18px;border-right:1px solid var(--line);font:600 10px var(--mono);color:#b7bec3;transition:transform .2s ease,background .2s ease}.ticker-item:hover{transform:scale(1.045);background:rgba(105,215,232,.05);z-index:2}.ticker-item b{color:#f1f3f4}.ticker-item small{color:var(--dim);margin-left:7px}@keyframes ticker{to{transform:translateX(-50%)}}
.hero{display:grid;grid-template-columns:1.65fr .75fr;gap:11px;margin-top:13px}.panel{border:1px solid var(--line);border-radius:12px;background:linear-gradient(145deg,rgba(21,24,26,.9),rgba(10,12,13,.79));box-shadow:var(--shadow);backdrop-filter:blur(18px)}.hero-main{padding:18px}.hero-label{font-size:9px;color:var(--dim);letter-spacing:.18em;text-transform:uppercase}.hero-row{display:flex;align-items:end;justify-content:space-between;gap:12px;margin-top:5px}.hero-row h1{margin:0;font-size:17px;letter-spacing:.1em;font-weight:680}.hero-number{font:600 36px var(--mono);letter-spacing:-.04em}.hero-meta{display:flex;flex-wrap:wrap;gap:14px;margin-top:12px;color:var(--muted);font:10px var(--mono)}.health-strip{height:2px;margin-top:16px;background:#25292b;overflow:hidden}.health-strip i{display:block;height:100%;background:linear-gradient(90deg,var(--red),var(--amber),var(--green));transition:width .5s ease}.regime{padding:18px;display:flex;flex-direction:column;justify-content:center}.regime strong{font-size:20px;letter-spacing:.04em}.regime small{margin-top:5px;color:var(--muted);font:10px var(--mono)}
.section{margin-top:19px}.section-head{display:flex;align-items:center;gap:11px;margin-bottom:9px}.section-head h2{margin:0;font-size:10px;letter-spacing:.2em;font-weight:680}.section-head .line{height:1px;flex:1;background:linear-gradient(90deg,var(--line-hi),transparent)}.section-head em{font:8px var(--mono);font-style:normal;color:var(--dim)}.telemetry{display:grid;grid-template-columns:repeat(7,1fr);gap:7px}.telemetry .panel{padding:12px 13px}.metric-label{font-size:7px;color:var(--dim);letter-spacing:.15em;text-transform:uppercase}.metric-value{margin-top:6px;font:600 17px var(--mono);color:#e6e9ea}.metric-sub{margin-top:3px;font-size:8px;color:#596269}
.active-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(480px,1fr));gap:11px}.active-card{position:relative;padding:17px;border-color:rgba(105,215,232,.17);overflow:hidden;transition:transform .28s ease,border-color .28s ease,box-shadow .28s ease}.active-card:hover{transform:translateY(-4px);border-color:rgba(105,215,232,.35);box-shadow:0 30px 85px rgba(0,0,0,.54),0 0 32px rgba(105,215,232,.035)}.active-card:before{content:"";position:absolute;left:0;top:13px;bottom:13px;width:2px;background:linear-gradient(var(--cyan),transparent);box-shadow:0 0 15px rgba(105,215,232,.35)}.setup-head{display:flex;justify-content:space-between;gap:12px}.setup-symbol{font-size:20px;font-weight:750;letter-spacing:.04em}.setup-dir{margin-top:3px;font:8px var(--mono);letter-spacing:.15em;color:var(--cyan)}.setup-state{text-align:right;color:var(--green);font:700 9px var(--mono);letter-spacing:.12em}.setup-price{margin-top:11px;font:600 28px var(--mono)}.distance{margin-top:12px}.distance-labels{display:flex;justify-content:space-between;color:var(--dim);font:7px var(--mono)}.distance-track{position:relative;height:20px;margin-top:4px;border-top:1px solid var(--line-hi)}.distance-track i{position:absolute;top:-4px;width:9px;height:9px;border-radius:50%;background:var(--cyan);box-shadow:0 0 14px rgba(105,215,232,.55);transition:left .5s ease}.distance-mark{position:absolute;top:-7px;width:1px;height:12px;background:var(--line-hi)}.mark-inv{left:0}.mark-t1{left:66%}.mark-t2{left:100%}.candlebox{height:110px;margin-top:10px;border:1px solid var(--line);border-radius:8px;background:linear-gradient(180deg,rgba(0,0,0,.18),rgba(255,255,255,.008));position:relative;overflow:hidden}.candlebox:before{content:"";position:absolute;inset:0;background:linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.025) 1px,transparent 1px);background-size:40px 27px}.actual-candle{position:absolute;left:50%;top:16px;width:30px;height:78px;transform:translateX(-50%)}.actual-candle .wick{position:absolute;left:14px;top:0;bottom:0;width:1px;background:#7b858c}.actual-candle .body{position:absolute;left:8px;width:13px;border-radius:2px;background:var(--silver)}.actual-candle.up .body{background:var(--green)}.actual-candle.down .body{background:var(--red)}.candle-note{position:absolute;right:8px;bottom:6px;color:#4e585f;font:7px var(--mono);letter-spacing:.08em}.setup-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-top:9px}.data-box{min-width:0;padding:9px;border:1px solid var(--line);border-radius:7px;background:rgba(0,0,0,.16)}.data-box label{display:block;color:var(--dim);font-size:7px;letter-spacing:.1em;text-transform:uppercase}.data-box b{display:block;margin-top:4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font:600 10px var(--mono)}.levels{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-top:7px}.level b{font-size:11px}.t1,.t2{color:var(--green)}.inv{color:var(--red)}
.pipeline{display:flex;align-items:center;gap:4px;margin:13px 0 4px;overflow:auto;padding-bottom:2px}.stage{position:relative;flex:1;min-width:60px;padding:7px 5px;text-align:center;border:1px solid var(--line);border-radius:5px;color:#566169;font:7px var(--mono);letter-spacing:.08em;transition:.2s ease}.stage.done{color:#9da7ad;border-color:rgba(201,208,213,.16);background:rgba(201,208,213,.025)}.stage.current{color:#071011;background:var(--cyan);border-color:var(--cyan);box-shadow:0 0 18px rgba(105,215,232,.15)}.stage.future{opacity:.48}.pipe-arrow{color:#394147;font-size:9px}.anatomy-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:9px}.instrument{position:relative;padding:14px;overflow:hidden;transition:transform .25s ease,border-color .25s ease,box-shadow .25s ease}.instrument:hover{transform:translateY(-5px);border-color:var(--line-hi);box-shadow:0 24px 65px rgba(0,0,0,.46)}.instrument:after{content:"";position:absolute;width:130px;height:130px;border-radius:50%;background:radial-gradient(circle,rgba(105,215,232,.08),transparent 70%);top:var(--my,50%);left:var(--mx,50%);transform:translate(-50%,-50%);opacity:0;transition:opacity .25s ease;pointer-events:none}.instrument:hover:after{opacity:1}.inst-head{display:flex;justify-content:space-between;align-items:start}.inst-symbol{font-size:15px;font-weight:740}.inst-state{margin-top:4px;color:var(--cyan);font:700 8px var(--mono);letter-spacing:.1em}.inst-price{text-align:right;font:600 19px var(--mono)}.inst-dir{margin-top:4px;font:8px var(--mono);color:var(--muted)}.anatomy-facts{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-top:11px}.health-score{margin-top:10px;padding-top:10px;border-top:1px solid var(--line);display:grid;grid-template-columns:1fr auto;gap:10px}.segments{display:grid;gap:4px}.seg{height:3px;background:#242a2e;overflow:hidden}.seg i{display:block;height:100%;background:var(--silver)}.seg:nth-child(2) i{background:#aab4ba}.seg:nth-child(3) i{background:var(--cyan)}.seg:nth-child(4) i{background:var(--green)}.seg:nth-child(5) i{background:#b8c0c5}.score-num{font:600 22px var(--mono);text-align:right}.score-num small{display:block;color:var(--dim);font:7px var(--mono)}.micro-one{height:64px;margin-top:9px;border:1px solid var(--line);border-radius:7px;position:relative;overflow:hidden;background:rgba(0,0,0,.16)}.micro-one:before{content:"";position:absolute;inset:0;background:linear-gradient(rgba(255,255,255,.022) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.022) 1px,transparent 1px);background-size:35px 21px}.micro-one .actual-candle{top:9px;height:45px;transform:translateX(-50%) scale(.72)}
.rejection-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:7px}.reject{position:relative;padding:11px;border:1px solid rgba(217,107,120,.13);border-radius:8px;background:rgba(217,107,120,.018);transition:.2s ease}.reject:hover{border-color:rgba(217,107,120,.3);transform:translateY(-2px)}.reject-time{font:8px var(--mono);color:var(--dim)}.reject-symbol{margin-top:5px;font-weight:700;font-size:11px}.reject-reason{margin-top:3px;color:#df858f;font:8px var(--mono);letter-spacing:.06em}.reject-detail{display:none;margin-top:8px;padding-top:7px;border-top:1px solid var(--line);color:var(--muted);font:8px var(--mono)}.reject:hover .reject-detail{display:block}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:10px;background:rgba(10,12,13,.76)}table{width:100%;border-collapse:collapse;min-width:960px}th,td{padding:10px 11px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap;font:9px var(--mono)}th{position:sticky;top:0;background:#121517;color:var(--dim);font-size:7px;letter-spacing:.13em;text-transform:uppercase}td{color:#b9c0c4}.outcome{display:inline-block;padding:4px 7px;border-radius:4px;border:1px solid var(--line-hi);font-weight:700}.outcome.t1{color:var(--cyan)}.outcome.t2{color:var(--green)}.outcome.invalid,.outcome.failed{color:var(--red)}.outcome.expired{color:var(--amber)}.empty{padding:26px;text-align:center;border:1px dashed #2b3135;border-radius:10px;color:#667078;background:rgba(10,12,13,.65);font:9px var(--mono);letter-spacing:.08em}.system{display:grid;grid-template-columns:1fr 1fr;gap:9px}.system .panel{padding:13px}.system-row{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid var(--line);font:9px var(--mono)}.system-row:last-child{border-bottom:0}.system-row span{color:var(--dim)}.footer{display:flex;justify-content:space-between;gap:10px;margin-top:24px;color:#4f585e;font:7px var(--mono);letter-spacing:.12em;text-transform:uppercase}.stale{color:var(--amber)!important}.offline{filter:saturate(.68)}
@media(max-width:1180px){.telemetry{grid-template-columns:repeat(4,1fr)}.hero{grid-template-columns:1fr}.top-inner{grid-template-columns:1fr}.top-stats{justify-content:flex-start}}@media(max-width:760px){.shell{padding:9px 9px 28px}.top-inner{padding:15px}.top-stats{display:grid;grid-template-columns:repeat(2,1fr)}.stat-chip{min-width:0}.brand{font-size:23px}.telemetry{grid-template-columns:repeat(2,1fr)}.active-grid{grid-template-columns:1fr}.anatomy-grid{grid-template-columns:1fr}.setup-grid{grid-template-columns:repeat(2,1fr)}.levels{grid-template-columns:repeat(2,1fr)}.system{grid-template-columns:1fr}.watermark{font-size:75px;letter-spacing:.08em}.marquee{margin-top:7px}.ticker-item{padding:8px 12px}.footer{flex-direction:column}}@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.01ms!important;animation-iteration-count:1!important;transition:none!important;scroll-behavior:auto!important}}
</style>
</head>
<body>
<div class="scene" aria-hidden="true"><div class="grid"></div><div class="candlefield"></div><div class="particles"></div><div class="bloom"></div><div class="watermark" id="wm">PSYCHO</div></div>
<main class="shell" id="app">
<header class="top">
<div class="top-inner"><div><div class="kicker">Live Market Intelligence</div><div class="brand">PSYCHO <span class="slash">//</span> PULLBACK DETECTOR</div><div class="sub">Deterministic NSE equity monitoring · visual interface</div></div><div class="top-stats"><div class="stat-chip" id="statusChip"><label>Market / Service</label><strong><i class="light"></i><span id="statusText">CONNECTING</span></strong></div><div class="stat-chip"><label>Feeds</label><strong id="feeds">—</strong></div><div class="stat-chip"><label>Packets</label><strong id="packets">—</strong></div><div class="stat-chip"><label>Accepted ticks</label><strong id="ticks">—</strong></div><div class="stat-chip"><label>Last update</label><strong id="lastUpdate">—</strong></div></div></div>
</header>
<div class="marquee"><div class="ticker-track" id="ticker"></div></div>
<section class="hero"><div class="panel hero-main"><div class="hero-label">Active setups</div><div class="hero-row"><h1>LIVE MARKET EVENTS</h1><div class="hero-number" id="activeCount">0</div></div><div class="hero-meta"><span id="receiving">— instruments receiving</span><span id="candles">1M — · 5M —</span><span id="latency">latency —</span><span id="stale">feed state —</span></div><div class="health-strip"><i id="healthBar"></i></div></div><div class="panel regime"><div class="hero-label">Market regime</div><strong id="regime">—</strong><small id="clock">—</small></div></section>
<section class="section"><div class="section-head"><h2>SYSTEM TELEMETRY</h2><div class="line"></div><em>BACKEND SOURCED</em></div><div class="telemetry" id="telemetry"></div></section>
<section class="section"><div class="section-head"><h2>ACTIVE SETUPS</h2><div class="line"></div><em>HERO MONITOR</em></div><div class="active-grid" id="active"></div></section>
<section class="section"><div class="section-head"><h2>LIVE PULLBACK ANATOMY</h2><div class="line"></div><em>COMPLETED 5M STRUCTURE</em></div><div class="anatomy-grid" id="anatomy"></div></section>
<section class="section"><div class="section-head"><h2>REJECTION MONITOR</h2><div class="line"></div><em>EXPLAINABLE FILTERING</em></div><div class="rejection-grid" id="rejections"></div></section>
<section class="section"><div class="section-head"><h2>RECENTLY CLOSED SETUPS</h2><div class="line"></div><em>TRADE REVIEW</em></div><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Direction</th><th>Trigger</th><th>T1</th><th>T2</th><th>Invalidation</th><th>MFE</th><th>MAE</th><th>Duration</th><th>Outcome</th></tr></thead><tbody id="closed"></tbody></table></div></section>
<section class="section"><div class="section-head"><h2>INSTRUMENT MATRIX</h2><div class="line"></div><em>20 FEEDS / LIVE STATE</em></div><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Price</th><th>State</th><th>Direction</th><th>Impulse</th><th>Pullback</th><th>Structure</th><th>Health</th><th>1M</th><th>5M</th><th>Feed</th></tr></thead><tbody id="matrix"></tbody></table></div></section>
<section class="section"><div class="section-head"><h2>SIGNAL HISTORY</h2><div class="line"></div><em>EXPERIMENTAL V1 / V2</em></div><div class="table-wrap"><table><thead><tr><th>Time</th><th>Instrument</th><th>Direction</th><th>Impulse</th><th>Retracement</th><th>Trigger</th><th>Invalidation</th><th>Confidence</th><th>Classification</th></tr></thead><tbody id="history"></tbody></table></div></section>
<section class="section"><div class="section-head"><h2>SYSTEM STATE</h2><div class="line"></div></div><div class="system"><div class="panel" id="systemLeft"></div><div class="panel" id="systemRight"></div></div></section>
<footer class="footer"><span>PSYCHO // PULLBACK DETECTOR · REAL BACKEND DATA ONLY</span><span id="refreshNote">3s refresh · reduced-motion supported</span></footer>
</main>
<script>
const $=id=>document.getElementById(id),fmt=v=>v==null||v===''?'—':String(v),num=(v,d=2)=>{const n=Number(v);return v==null||v===''||!Number.isFinite(n)?'—':n.toFixed(d)},pct=v=>{const n=Number(v);return v==null||!Number.isFinite(n)?'—':(Math.abs(n)<=1?(n*100):n).toFixed(1)+'%'},esc=v=>String(fmt(v)).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])),clock=v=>{if(!v)return'—';try{return new Date(v).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})}catch{return v}},stages=['EXPANSION','IMPULSE','PULLBACK','HEALTHY','TRIGGER','ACTIVE','OUTCOME'];
function stateIndex(s){s=String(s||'').toUpperCase();if(s.includes('IMPULSE_VALIDATED'))return 1;if(s.includes('IMPULSE'))return 1;if(s.includes('PULLBACK'))return 2;if(s.includes('HEALTHY'))return 3;if(s.includes('TRIGGER'))return 4;if(s==='ACTIVE')return 5;if(['FAILED','TARGET_1_HIT','TARGET_2_HIT','INVALIDATED','EXPIRED'].includes(s))return 6;return 0}
function pipeline(state){const ix=stateIndex(state);return '<div class="pipeline">'+stages.map((x,i)=>(i?'<span class="pipe-arrow">›</span>':'')+'<span class="stage '+(i<ix?'done ':'')+(i===ix?'current ':'')+(i>ix?'future':'')+'" title="Detector stage: '+x+'">'+x+'</span>').join('')+'</div>'}
function box(label,value,klass=''){return '<div class="data-box '+klass+'"><label>'+label+'</label><b>'+esc(value)+'</b></div>'}
function sessionName(){const d=new Date(),m=d.getHours()*60+d.getMinutes();if(m<555)return'PRE-MARKET';if(m<600)return'OPENING';if(m<720)return'MORNING';if(m<840)return'MIDDAY';if(m<915)return'AFTERNOON';if(m<930)return'CLOSING REGIME';return'MARKET CLOSED'}
function candle(c,small=false){if(!c||c.open==null||c.high==null||c.low==null||c.close==null)return '<span class="candle-note">NO COMPLETED 5M DATA</span>';const o=Number(c.open),h=Number(c.high),l=Number(c.low),cl=Number(c.close),span=Math.max(h-l,1e-9),body=Math.max(5,Math.abs(cl-o)/span*70),top=(h-Math.max(o,cl))/span*70;return '<div class="actual-candle '+(cl>=o?'up':'down')+'" style="transform:translateX(-50%) '+(small?'scale(.65)':'')+'"><i class="wick"></i><b class="body" style="top:'+top+'px;height:'+body+'px"></b></div><span class="candle-note">ACTUAL COMPLETED 5M OHLC · '+clock(c.timestamp||c.time)+'</span>'}
function scoreSegments(a){const score=Number(a.health_score);const p=Number.isFinite(score)?Math.max(0,Math.min(100,score)):0;return '<div class="health-score"><div class="segments"><div class="seg"><i style="width:'+(p*.2)+'%"></i></div><div class="seg"><i style="width:'+(p*.25)+'%"></i></div><div class="seg"><i style="width:'+(p*.2)+'%"></i></div><div class="seg"><i style="width:'+(p*.15)+'%"></i></div><div class="seg"><i style="width:'+(p*.2)+'%"></i></div></div><div class="score-num">'+(Number.isFinite(score)?score.toFixed(0):'—')+'<small>HEALTH</small></div></div>'}
function anatomyCard(i){const a=i.anatomy||{},state=a.detection_phase||a.state||'WATCHING',dir=String(a.impulse_direction||'').toUpperCase();return '<article class="panel instrument" data-symbol="'+esc(i.trading_symbol)+'"><div class="inst-head"><div><div class="inst-symbol">'+esc(i.trading_symbol)+'</div><div class="inst-state">'+esc(state)+'</div></div><div><div class="inst-price">'+esc(i.latest_price)+'</div><div class="inst-dir '+(dir==='SHORT'?'short':'')+'">'+esc(dir||'DIRECTION —')+'</div></div></div>'+pipeline(state)+'<div class="anatomy-facts">'+box('Impulse',num(a.impulse_magnitude))+box('Depth',a.retracement_depth_pct==null?'—':num(a.retracement_depth_pct,1)+'%')+box('Duration',a.pullback_duration_candles==null?'—':a.pullback_duration_candles+' × 5M')+box('Speed',num(a.pullback_speed))+box('Efficiency',num(a.pullback_efficiency))+box('Structure',a.structural_state||a.structure_status||'—')+box('Participation',a.volume_behavior||a.volume_ratio==null?'—':num(a.volume_ratio,2)+'×')+box('Trigger',num(a.trigger_price))+box('Invalidation',num(a.invalidation_price))+'</div><div class="micro-one">'+candle(i.candle_5m,true)+'</div>'+scoreSegments(a)+'</article>'}
function activeCard(s,instruments){const x=s.snapshot||{},i=instruments.find(q=>String(q.security_id)===String(x.instrument_id))||{},p=Number(s.current_price),inv=Number(x.invalidation_price),t1=Number(x.target_1),t2=Number(x.target_2),span=Math.abs(t2-inv)||1;let pos=((p-inv)/span)*100;if(String(x.direction).toUpperCase()==='SHORT')pos=((inv-p)/span)*100;pos=Math.max(2,Math.min(98,pos));const age=x.creation_timestamp?Math.max(0,(Date.now()-new Date(x.creation_timestamp).getTime())/60000):null;const pulseKey=String(x.signal_id||x.creation_timestamp||'');return '<article class="panel active-card" data-signal="'+esc(pulseKey)+'"><div class="setup-head"><div><div class="setup-symbol">'+esc(i.trading_symbol||('SID '+x.instrument_id))+'</div><div class="setup-dir">● '+esc(x.direction)+' · LIVE MONITORED SETUP</div></div><div class="setup-state">ACTIVE</div></div><div class="setup-price">'+esc(s.current_price)+'</div><div class="distance"><div class="distance-labels"><span>INVALIDATION</span><span>T1 · 1R</span><span>T2 · 2R</span></div><div class="distance-track"><i style="left:'+pos+'%"></i><b class="distance-mark mark-inv"></b><b class="distance-mark mark-t1"></b><b class="distance-mark mark-t2"></b></div></div><div class="candlebox">'+candle(i.candle_5m)+' </div><div class="setup-grid">'+box('Trigger',num(x.trigger_price))+box('Current',num(s.current_price))+box('Invalidation',num(x.invalidation_price),'inv')+box('Health',num(x.health_score,0))+box('T1',num(x.target_1),'t1')+box('T2',num(x.target_2),'t2')+box('MFE',num(s.mfe))+box('MAE',num(s.mae))+'</div><div class="setup-grid">'+box('Age',age==null?'—':age<60?num(age,1)+'m':num(age/60,1)+'h')+box('Depth',pct(x.pullback_depth))+box('Speed',num(x.pullback_speed))+box('Efficiency',num(x.pullback_efficiency))+'</div>'+pipeline('ACTIVE')+'</article>'}
function outcome(o){o=String(o||'').toUpperCase();const c=o==='TARGET_1_HIT'?'t1':o==='TARGET_2_HIT'?'t2':o.includes('INVALID')?'invalid':o.includes('FAIL')?'failed':o.includes('EXPIR')?'expired':'';const text=o==='TARGET_1_HIT'?'TARGET 1':o==='TARGET_2_HIT'?'TARGET 2':o.includes('INVALID')?'INVALIDATED':o.includes('FAIL')?'FAILED':o.includes('EXPIR')?'EXPIRED':o;return '<span class="outcome '+c+'">'+esc(text)+'</span>'}
function rejectionCards(d){const out=[];(d.instruments||[]).forEach(i=>{const r=i.anatomy&&i.anatomy.last_rejection;if(r&&r.reason)out.push('<div class="reject"><div class="reject-time">'+clock(r.timestamp||r.time)+'</div><div class="reject-symbol">'+esc(i.trading_symbol)+'</div><div class="reject-reason">REJECTED · '+esc(r.reason)+'</div><div class="reject-detail">stage='+esc(r.stage)+' · actual='+esc(r.actual_value)+' · threshold='+esc(r.threshold)+'</div></div>')});return out.length?out.slice(0,12).join(''):'<div class="empty">NO REJECTION EVENTS EXPOSED BY CURRENT BACKEND SNAPSHOT</div>'}
function renderTicker(d){const items=(d.instruments||[]).map(i=>'<div class="ticker-item"><b>'+esc(i.trading_symbol)+'</b><small>'+esc(i.latest_price==null?'PRICE —':'PRICE '+i.latest_price)+'</small></div>');const html=items.concat(items).join('');$('ticker').innerHTML=html||'<div class="ticker-item">WAITING FOR MONITORED INSTRUMENTS</div>'}
function render(d){const h=d.health||{},status=String(h.service_status||'starting').toLowerCase(),inst=d.instruments||[],receiving=inst.filter(i=>!i.stale&&i.latest_price!=null).length,all=inst.length,closed=d.recently_closed_setups||[];$('statusChip').className='stat-chip '+(status==='live'?'ok':status==='degraded'?'bad':'warn');$('statusText').textContent=status==='live'?'MARKET / FEED LIVE':status==='degraded'?'DEGRADED':'STARTING';$('feeds').textContent=receiving+' / '+all;$('packets').textContent=fmt(h.packet_count);$('ticks').textContent=fmt(h.accepted_tick_count);$('lastUpdate').textContent=clock(d.generated_at);$('activeCount').textContent=(d.active_setups||[]).length;$('receiving').textContent=receiving+' / '+all+' instruments receiving';$('candles').textContent='1M '+fmt(h.candle_count_1m)+' · 5M '+fmt(h.candle_count_5m);$('latency').textContent='latency '+(h.latency_ms_median==null?'—':Math.round(h.latency_ms_median)+' ms');$('stale').textContent=receiving===0?'STALE / WAITING':'FEED CONNECTED';$('regime').textContent=sessionName();$('clock').textContent=new Date().toLocaleString();$('healthBar').style.width=(all?Math.max(2,receiving/all*100):status==='live'?8:2)+'%';$('telemetry').innerHTML=[['PACKETS',h.packet_count,'received'],['ACCEPTED TICKS',h.accepted_tick_count,'validated'],['INSTRUMENTS',receiving,'producing'],['1M CANDLES',h.candle_count_1m,'completed'],['5M CANDLES',h.candle_count_5m,'completed'],['ACTIVE SETUPS',(d.active_setups||[]).length,'lifecycle'],['MEDIAN LATENCY',h.latency_ms_median==null?'—':Math.round(h.latency_ms_median)+' ms','receive']].map(x=>'<div class="panel"><div class="metric-label">'+x[0]+'</div><div class="metric-value">'+esc(x[1])+'</div><div class="metric-sub">'+x[2]+'</div></div>').join('');renderTicker(d);$('active').innerHTML=d.active_setups&&d.active_setups.length?d.active_setups.map(s=>activeCard(s,inst)).join(''):'<div class="empty">NO ACTIVE SETUPS · ENGINE IS MONITORING FOR QUALIFYING STRUCTURE</div>';$('anatomy').innerHTML=inst.length?inst.map(anatomyCard).join(''):'<div class="empty">NO MONITORED INSTRUMENTS</div>';$('rejections').innerHTML=rejectionCards(d);$('closed').innerHTML=closed.length?closed.map(s=>{const x=s.snapshot||{},i=inst.find(q=>String(q.security_id)===String(x.instrument_id));return '<tr><td><b>'+esc(i?.trading_symbol||('SID '+x.instrument_id))+'</b></td><td>'+esc(x.direction)+'</td><td>'+esc(x.trigger_price)+'</td><td>'+esc(x.target_1)+'</td><td>'+esc(x.target_2)+'</td><td>'+esc(x.invalidation_price)+'</td><td>'+esc(s.mfe)+'</td><td>'+esc(s.mae)+'</td><td>'+esc(s.duration_minutes==null?'—':num(s.duration_minutes,1)+'m')+'</td><td>'+outcome(s.outcome)+'</td></tr>'}).join(''):'<tr><td colspan="10">No closed setups yet.</td></tr>';$('matrix').innerHTML=inst.map(i=>{const a=i.anatomy||{};return '<tr><td><b>'+esc(i.trading_symbol)+'</b><small style="display:block;color:#56606a">SID '+esc(i.security_id)+'</small></td><td>'+esc(i.latest_price)+'</td><td>'+esc(a.detection_phase||a.state||'WATCHING')+'</td><td>'+esc(a.impulse_direction||'—')+'</td><td>'+esc(a.impulse_magnitude==null?'—':num(a.impulse_magnitude))+'</td><td>'+esc(a.retracement_depth_pct==null?'—':num(a.retracement_depth_pct,1)+'%')+'</td><td>'+esc(a.structural_state||a.structure_status||'—')+'</td><td>'+esc(a.health_score==null?'—':num(a.health_score,0))+'</td><td>'+(i.candle_1m?'OK':'—')+'</td><td>'+(i.candle_5m?'OK':'—')+'</td><td class="'+(i.stale?'stale':'t1')+'">'+(i.stale?'STALE':'LIVE')+'</td></tr>'}).join('')||'<tr><td colspan="11">No instrument data.</td></tr>';$('history').innerHTML=(d.recent_signals||[]).map(s=>'<tr><td>'+clock(s.timestamp)+'</td><td>SID '+esc(s.instrument_id)+'</td><td>'+esc(s.direction)+'</td><td>'+esc(s.impulse_start)+' → '+esc(s.impulse_end)+'</td><td>'+pct(s.retracement)+'</td><td>'+esc(s.trigger_price)+'</td><td>'+esc(s.invalidation_level)+'</td><td>'+pct(s.confidence_score)+'</td><td>'+esc(s.experimental_v1?'V1 EXPERIMENTAL':'V2')+'</td></tr>').join('')||'<tr><td colspan="9">No signal history available.</td></tr>';$('systemLeft').innerHTML='<div class="system-row"><span>Service</span><b>'+esc(status.toUpperCase())+'</b></div><div class="system-row"><span>Dhan / feed</span><b>'+esc(receiving?'CONNECTED':'WAITING / STALE')+'</b></div><div class="system-row"><span>Last accepted tick</span><b>'+esc(h.last_tick_timestamp?clock(h.last_tick_timestamp):'—')+'</b></div><div class="system-row"><span>Last receive</span><b>'+esc(h.receive_timestamp?clock(h.receive_timestamp):'—')+'</b></div>';$('systemRight').innerHTML='<div class="system-row"><span>Malformed packets</span><b>'+esc(h.malformed_packet_count??'—')+'</b></div><div class="system-row"><span>Rejected ticks</span><b>'+esc(h.rejected_tick_count??'—')+'</b></div><div class="system-row"><span>Duplicate packets</span><b>'+esc(h.duplicate_packet_count??'—')+'</b></div><div class="system-row"><span>Reconnects</span><b>'+esc(h.reconnect_count??'—')+'</b></div>';document.body.classList.toggle('offline',receiving===0&&status!=='live')}
let previous=new Map();function markMotion(d){const current=new Map((d.instruments||[]).map(i=>[String(i.security_id),String(i.latest_price)]));document.querySelectorAll('.active-card').forEach(el=>{const key=el.dataset.signal;if(previous.get(key)!==undefined){el.classList.toggle('price-pulse',previous.get(key)!==current.get(key))}});previous=current}
async function refresh(){try{const r=await fetch('/api/dashboard',{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);const d=await r.json();render(d);markMotion(d)}catch(e){$('statusChip').className='stat-chip bad';$('statusText').textContent='DASHBOARD OFFLINE';$('regime').textContent='BACKEND UNAVAILABLE'}}
const wm=$('wm');document.addEventListener('pointermove',e=>{const x=(e.clientX/innerWidth-.5)*7,y=(e.clientY/innerHeight-.5)*5;wm.style.transform='translate(calc(-50% + '+x+'px),calc(-50% + '+y+'px))'});document.addEventListener('pointermove',e=>{const card=e.target.closest('.instrument');if(!card)return;const r=card.getBoundingClientRect();card.style.setProperty('--mx',(e.clientX-r.left)+'px');card.style.setProperty('--my',(e.clientY-r.top)+'px')});refresh();setInterval(refresh,3000);
</script>
</body>
</html>'''
