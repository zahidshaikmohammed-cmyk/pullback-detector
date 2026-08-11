from __future__ import annotations

import csv
import json
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .lifecycle import PullbackLifecycleEngine
from .live import LIVE_ANATOMY


def _j(v: Any):
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    if isinstance(v, Decimal):
        return str(v)
    return v


def _tail(path: Path, n: int = 5000) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as f:
            return f.readlines()[-n:]
    except OSError:
        return []


def _read_jsonl(path: Path, limit: int = 5000) -> list[dict]:
    rows = []
    for line in _tail(path, limit):
        try:
            rows.append(json.loads(line))
        except (json.JSONDecodeError, TypeError):
            continue
    return rows


def _display_state(v2_state: str | None, rejection: dict | None = None) -> str:
    s = str(v2_state or "WATCHING").upper()
    if s in {"WATCHING", "COOLDOWN"}:
        return "SCANNING"
    if s in {"IMPULSE_DETECTED", "IMPULSE_VALIDATED"}:
        return "IMPULSE"
    if s == "PULLBACK_DEVELOPING":
        return "PULLBACK"
    if s in {"HEALTHY_CANDIDATE", "TRIGGER_PENDING"}:
        return "CANDIDATE"
    if s == "TRIGGER_CONFIRMED":
        return "ACTIVE"
    if s == "FAILED":
        reason = str((rejection or {}).get("reason", "")).upper()
        return "STRUCTURE_FAILED" if "STRUCTURE" in reason else "INVALIDATED"
    return s if s in {"ACTIVE", "TARGET_1_HIT", "TARGET_2_HIT", "INVALIDATED", "STRUCTURE_FAILED", "EXPIRED"} else "SCANNING"


def _next_condition(state: str, anatomy: dict, rejection: dict | None) -> str:
    s = _display_state(state, rejection)
    if s == "SCANNING":
        return "VALIDATED IMPULSE"
    if s == "IMPULSE":
        return "CONTROLLED PULLBACK"
    if s == "PULLBACK":
        return "HEALTHY PULLBACK CONDITIONS"
    if s == "CANDIDATE":
        return "CONTINUATION / TRIGGER CONFIRMATION"
    if s == "ACTIVE":
        return "MONITOR ACTIVE SETUP"
    if s in {"INVALIDATED", "STRUCTURE_FAILED", "EXPIRED"}:
        return "NEW VALID IMPULSE"
    return "—"


def _direction_context(candles: list[dict]) -> dict:
    closes = [float(x["close"]) for x in candles if x.get("close") is not None]
    if len(closes) < 3:
        return {"available": False, "reason": "Insufficient completed 5M candles"}

    def trend(values: list[float]) -> str:
        if len(values) < 2:
            return "NEUTRAL"
        delta = values[-1] - values[0]
        return "BULLISH" if delta > 0 else "BEARISH" if delta < 0 else "NEUTRAL"

    day = trend(closes)
    one_h = trend(closes[-12:])
    current = trend(closes[-3:])
    alignment = sum(1 for x in (day, one_h, current) if x == current)
    momentum = "STRENGTHENING" if len(closes) >= 4 and abs(closes[-1] - closes[-2]) >= abs(closes[-2] - closes[-3]) else "STABLE"
    return {
        "available": True,
        "day": {"trend": day},
        "one_hour": {"trend": one_h},
        "current": {"trend": current},
        "alignment": f"{alignment} / 3",
        "trend_momentum": momentum,
        "trend_stability": "STABLE",
    }


class DashboardData:
    def __init__(self, root, state):
        self.root = Path(root)
        self.state = state
        self.lock = threading.Lock()
        self.cache = None

    def _dt(self, v):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc) if v else None
        except (ValueError, TypeError):
            return None

    def _universe(self):
        p = self.root / "universe.csv"
        try:
            with p.open(encoding="utf-8", newline="") as f:
                return list(csv.DictReader(f))
        except (OSError, csv.Error):
            return []

    def _life(self):
        s = PullbackLifecycleEngine(self.root).snapshot()
        def c(x):
            d = asdict(x)
            d["snapshot"] = asdict(x.snapshot)
            return json.loads(json.dumps(d, default=_j))
        return [c(x) for x in s["active"]], [c(x) for x in s["closed"]]

    def _persisted_anatomy(self, sid: str) -> dict:
        p = self.root / "anatomy" / f"{sid}.json"
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _events(self, sid: str) -> list[dict]:
        return _read_jsonl(self.root / f"pullback_candidates_{sid}.jsonl", 200)

    def _performance(self, closed: list[dict], active: list[dict]) -> dict:
        outcomes = [str(x.get("outcome") or "") for x in closed]
        total = len(outcomes)
        pct = lambda n: round(n * 100 / total, 1) if total else None
        rs = [float(x["r_multiple"]) for x in closed if x.get("r_multiple") is not None]
        return {
            "total_signals": total + len(active),
            "t1_hit_rate": pct(sum(x == "TARGET_1_HIT" for x in outcomes)),
            "t2_hit_rate": pct(sum(x == "TARGET_2_HIT" for x in outcomes)),
            "invalidation_rate": pct(sum(x == "INVALIDATION_HIT" for x in outcomes)),
            "structure_failure_rate": pct(sum(x == "STRUCTURE_FAILED" for x in outcomes)),
            "expiry_rate": pct(sum(x == "EXPIRED" for x in outcomes)),
            "average_r": round(sum(rs) / len(rs), 3) if rs else None,
            "expectancy": None,
        }

    def snapshot(self):
        now = datetime.now(timezone.utc)
        universe = self._universe()
        normalized = self.root / "normalized" / f"{now:%Y-%m-%d}.jsonl"
        candles = self.root / "candles" / f"{now:%Y-%m-%d}.jsonl"
        signals = self.root / "signals" / f"{now:%Y-%m-%d}.jsonl"
        life = self.root / "setup_events.jsonl"
        anatomy_dir = self.root / "anatomy"
        key = tuple(p.stat().st_mtime if p.exists() else 0 for p in (normalized, candles, signals, life)) + (anatomy_dir.stat().st_mtime if anatomy_dir.exists() else 0, len(LIVE_ANATOMY), str(self.state.get("status")), str(self.state.get("last_error")))
        with self.lock:
            if self.cache and self.cache[0] == key:
                return self.cache[1]
            ticks = {}
            for row in _read_jsonl(normalized, 5000):
                ticks[str(row.get("instrument_id"))] = row
            histories = {}
            for row in _read_jsonl(candles, 15000):
                if not row.get("complete") or str(row.get("timeframe_seconds")) != "300":
                    continue
                histories.setdefault(str(row.get("instrument_id")), []).append(row)
            for sid in histories:
                histories[sid] = sorted(histories[sid], key=lambda x: x.get("end", ""))[-80:]
            active, closed = self._life()
            active_by_id = {str(x["snapshot"]["instrument_id"]): x for x in active}
            instruments = []
            for r in universe:
                sid = str(r.get("security_id"))
                anatomy = dict(LIVE_ANATOMY.get(int(sid), {})) if sid.isdigit() else {}
                if not anatomy:
                    anatomy = self._persisted_anatomy(sid)
                events = self._events(sid)
                rejection = anatomy.get("last_rejection") or next((x for x in reversed(events) if x.get("event") == "candidate_rejected"), None)
                hist = histories.get(sid, [])
                tick = ticks.get(sid, {})
                active_setup = active_by_id.get(sid)
                display = "ACTIVE" if active_setup else _display_state(anatomy.get("state"), rejection)
                direction = anatomy.get("impulse_direction") or anatomy.get("direction") or (active_setup or {}).get("snapshot", {}).get("direction")
                instruments.append({
                    "security_id": r.get("security_id"), "symbol": r.get("symbol") or r.get("trading_symbol") or sid,
                    "exchange_segment": r.get("exchange_segment") or "NSE_EQ",
                    "price": tick.get("price") if tick.get("price") is not None else anatomy.get("current_price"),
                    "timestamp": tick.get("received_at") or anatomy.get("timestamp"),
                    "price_source": "LIVE_TICK" if tick.get("price") is not None else "LAST_KNOWN_STATE" if anatomy.get("current_price") is not None else "NONE",
                    "data_status": "LIVE" if tick.get("price") is not None else "STALE" if anatomy.get("current_price") is not None else "NO_DATA",
                    "v2_state": anatomy.get("state") or "WATCHING", "state": display, "direction": direction or "NEUTRAL",
                    "health": anatomy.get("health_score"), "anatomy": anatomy, "direction_context": _direction_context(hist),
                    "current_stage": display, "next_required_condition": _next_condition(anatomy.get("state"), anatomy, rejection),
                    "primary_rejection_reason": rejection, "lifecycle_events": events[-30:], "active_setup": active_setup,
                    "candle_5m_history": hist,
                })
            h = dict(self.state.get("last_report") or {})
            h["service_status"] = self.state.get("status", h.get("service_status")); h["last_error"] = self.state.get("last_error")
            lr = self._dt(h.get("last_receive_timestamp"))
            h["feed_connected"] = bool(h.get("dhan_connection_status") in ("connected", "connected_after_reconnect") and lr and (now - lr).total_seconds() <= 60)
            rejections = []
            for i in instruments:
                for event in i["lifecycle_events"]:
                    if event.get("event") == "candidate_rejected":
                        event = dict(event); event["symbol"] = i["symbol"]; rejections.append(event)
            p = {"generated_at": now.isoformat(), "health": h, "instruments": instruments, "active_setups": active, "recently_closed_setups": closed[:100], "rejections": sorted(rejections, key=lambda x: x.get("timestamp", ""), reverse=True)[:100], "performance": self._performance(closed, active)}
            self.cache = (key, p)
            return p


HTML = Path(__file__).with_name("dashboard_monitor.html").read_text(encoding="utf-8")
