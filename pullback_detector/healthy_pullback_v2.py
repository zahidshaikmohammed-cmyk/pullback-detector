"""Deterministic, stateful Healthy Pullback Qualification Engine V2.

V2 is an explicit market-structure hypothesis, not a profitability claim.
It consumes completed 5-minute Candle objects only for structural decisions.
"""
from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

from .models import Candle, PullbackSignal


STATES = ("WATCHING", "IMPULSE_DETECTED", "IMPULSE_VALIDATED", "PULLBACK_DEVELOPING", "HEALTHY_CANDIDATE", "TRIGGER_PENDING", "TRIGGER_CONFIRMED", "ACTIVE", "FAILED", "TARGET_1_HIT", "TARGET_2_HIT", "INVALIDATED", "EXPIRED", "COOLDOWN")
CRITICAL = {
    "DATA_INVALID", "WEAK_IMPULSE", "IMPULSE_COUNTERTREND_INSTABILITY", "PROTECTED_STRUCTURE_BROKEN",
    "EXCESSIVE_RETRACEMENT", "COUNTERTREND_ACCELERATION", "PULLBACK_REVERSAL_EVIDENCE", "SEVERE_CHOP",
    "TRIGGER_FAILURE", "STALE_DATA", "SESSION_INVALID",
}


@dataclass(frozen=True)
class Swing:
    kind: str
    price: Decimal
    timestamp: datetime
    confirmation_timestamp: datetime


@dataclass
class Impulse:
    direction: str
    origin: Swing
    extreme: Swing
    range_price: Decimal
    atr_multiple: float
    efficiency: float
    directional_ratio: float
    countertrend_excursion: float
    median_body_ratio: float
    volume_ratio: float
    exhaustion_risk: bool


class HealthyPullbackV2:
    """One deterministic state machine per monitored instrument."""

    def __init__(self, instrument_id: int, config: dict | None = None, audit_root: str = "data/runtime"):
        self.instrument_id = instrument_id
        self.cfg = config or self.default_config()
        self.history: list[Candle] = []
        self.swings: list[Swing] = []
        self.impulse: Impulse | None = None
        self.state = "WATCHING"
        self.last_state = {}
        self.last_signal: PullbackSignal | None = None
        self.last_rejection: dict | None = None
        self.candidate_id: str | None = None
        self.candidate_created_at: datetime | None = None
        self.audit_path = Path(audit_root) / f"pullback_candidates_{instrument_id}.jsonl"
        self.cooldown_until: datetime | None = None
        self._seen_candles: set[datetime] = set()

    @staticmethod
    def default_config() -> dict:
        return {
            "timezone": "Asia/Kolkata", "min_history": 50, "atr_period": 14,
            "min_impulse_atr": 1.25, "preferred_impulse_atr": 1.75,
            "min_impulse_efficiency": 0.50, "preferred_impulse_efficiency": 0.65,
            "min_directional_candle_ratio": 0.55, "preferred_directional_candle_ratio": 0.65,
            "max_impulse_countertrend_excursion": 0.35, "warning_body_ratio": 0.45,
            "min_pullback_depth": 0.15, "normal_pullback_depth_max": 0.60,
            "deep_pullback_depth_max": 0.75, "max_pullback_candles": 12,
            "max_relative_pullback_speed": 1.10, "controlled_speed": 0.50, "warning_speed": 0.80,
            "acceptable_pullback_efficiency": 0.70, "warning_pullback_efficiency": 0.80,
            "max_pullback_efficiency": 0.85, "countertrend_body_multiplier": 1.25,
            "max_internal_swings": 4, "volume_warning_ratio": 1.20, "volume_reject_ratio": 1.30,
            "volume_efficiency_reject": 0.70, "continuation_atr": 0.10,
            "candidate_score_min": 75, "live_alert_score_min": 82,
            "cooldown_candles": 3, "stale_seconds": 300,
            "opening": ("09:15", "10:00"), "morning": ("10:00", "12:00"),
            "midday": ("12:00", "14:00"), "afternoon": ("14:00", "15:15"),
            "continuous_end": "15:30", "post_continuous": "15:30",
        }

    def _audit(self, event: str, timestamp: datetime, **values):
        payload = {"event": event, "instrument_id": self.instrument_id, "timestamp": timestamp.isoformat(), **values}
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str, separators=(",", ":")) + "\n")

    def _session(self, ts: datetime) -> str:
        local = ts.astimezone(ZoneInfo(self.cfg["timezone"])).time()
        def t(v): return time.fromisoformat(v)
        for name in ("opening", "morning", "midday", "afternoon"):
            start, end = self.cfg[name]
            if t(start) <= local < t(end): return name.upper()
        if local >= t(self.cfg["post_continuous"]): return "CLOSING_AUCTION_POST_CONTINUOUS"
        return "OUTSIDE_CONTINUOUS_SESSION"

    def _validate(self, c: Candle) -> str | None:
        if not c.complete: return "DATA_INVALID"
        if c.instrument_id != self.instrument_id: return "DATA_INVALID"
        if c.end.tzinfo is None or c.start.tzinfo is None: return "DATA_INVALID"
        if c.end <= c.start: return "DATA_INVALID"
        if c.end > datetime.now(timezone.utc) + __import__('datetime').timedelta(seconds=self.cfg["stale_seconds"]): return "DATA_INVALID"
        if c.open <= 0 or c.high <= 0 or c.low <= 0 or c.close <= 0: return "DATA_INVALID"
        if c.high < max(c.open, c.close) or c.low > min(c.open, c.close) or c.high < c.low: return "DATA_INVALID"
        if c.volume is None or c.volume < 0: return "DATA_INVALID"
        if self.history and c.start < self.history[-1].start: return "DATA_INVALID"
        if c.start in self._seen_candles: return "DATA_INVALID"
        return None

    def _true_ranges(self) -> list[Decimal]:
        out = []
        for i, c in enumerate(self.history):
            prev = self.history[i-1].close if i else c.close
            out.append(max(c.high-c.low, abs(c.high-prev), abs(c.low-prev)))
        return out

    def atr(self) -> Decimal | None:
        p = int(self.cfg["atr_period"]); trs = self._true_ranges()
        if len(trs) < p: return None
        return sum(trs[-p:], Decimal(0)) / Decimal(p)

    def atr_percentile(self) -> float | None:
        p = int(self.cfg["atr_period"]); trs = self._true_ranges()
        if len(trs) < p: return None
        current = float(sum(trs[-p:], Decimal(0)) / Decimal(p))
        windows = [float(sum(trs[i-p+1:i+1], Decimal(0))/Decimal(p)) for i in range(p-1, len(trs))]
        return 100.0 * sum(x <= current for x in windows) / len(windows)

    def _confirm_swing(self):
        if len(self.history) < 5: return None
        center = self.history[-3]
        left = self.history[-5:-3]; right = self.history[-2:]
        if center.high > max(x.high for x in left+right):
            s = Swing("HIGH", center.high, center.end, self.history[-1].end)
        elif center.low < min(x.low for x in left+right):
            s = Swing("LOW", center.low, center.end, self.history[-1].end)
        else:
            return None
        if not self.swings or self.swings[-1].timestamp != s.timestamp or self.swings[-1].kind != s.kind:
            self.swings.append(s)
        return s

    def _directional_metrics(self, bars: list[Candle], direction: str):
        if len(bars) < 1: return 0.0, 0.0, 0.0, 0.0
        net = abs(float(bars[-1].close-bars[0].open))
        movement = sum(abs(float(b.close-b.open)) for b in bars)
        efficiency = min(1.0, net / movement) if movement else 0.0
        directional = sum((b.close > b.open) if direction == "LONG" else (b.close < b.open) for b in bars) / len(bars)
        start = bars[0].open
        extreme = bars[0].low if direction == "LONG" else bars[0].high
        counter = 0.0
        for b in bars:
            if direction == "LONG": counter = max(counter, float(extreme-b.low)); extreme = min(extreme, b.low)
            else: counter = max(counter, float(b.high-extreme)); extreme = max(extreme, b.high)
        return efficiency, directional, counter, movement

    def _find_impulse(self, atr: Decimal) -> Impulse | None:
        if len(self.swings) < 2: return None
        for a, b in reversed(list(zip(self.swings[:-1], self.swings[1:]))):
            if a.kind == "LOW" and b.kind == "HIGH" and b.timestamp > a.timestamp:
                direction = "LONG"
            elif a.kind == "HIGH" and b.kind == "LOW" and b.timestamp > a.timestamp:
                direction = "SHORT"
            else: continue
            rng = abs(b.price-a.price)
            if rng < atr*Decimal(str(self.cfg["min_impulse_atr"])): continue
            bars = [x for x in self.history if a.timestamp <= x.end <= b.timestamp]
            eff, ratio, counter, movement = self._directional_metrics(bars, direction)
            excursion = counter/float(rng) if rng else 1.0
            bodies = [float(abs(x.close-x.open)/(x.high-x.low)) for x in bars if x.high>x.low]
            vols = [x.volume for x in bars]
            vr = float(median(vols[-3:]) / median(vols[:-3])) if len(vols) >= 6 and median(vols[:-3]) else 1.0
            exhaustion = eff < self.cfg["preferred_impulse_efficiency"] and (b.timestamp == self.history[-3].end or ratio < self.cfg["preferred_directional_candle_ratio"])
            return Impulse(direction, a, b, rng, float(rng/atr), eff, ratio, excursion, median(bodies) if bodies else 0.0, vr, exhaustion)
        return None

    def _pullback_stats(self, imp: Impulse, bars: list[Candle], atr: Decimal):
        pb = [b for b in bars if b.start >= imp.extreme.timestamp]
        if not pb: return None
        if imp.direction == "LONG":
            extreme = max(b.high for b in pb)
            low = min(b.low for b in pb); depth = float((extreme-low)/imp.range_price)
            adverse_net = max(Decimal(0), extreme-pb[-1].close)
        else:
            extreme = min(b.low for b in pb)
            high = max(b.high for b in pb); depth = float((high-extreme)/imp.range_price)
            adverse_net = max(Decimal(0), pb[-1].close-extreme)
        movements = sum(abs(float(b.close-b.open)) for b in pb)
        efficiency = float(adverse_net)/movements if movements else 0.0
        duration = len(pb)
        impulse_bars = max(1, sum(1 for b in bars if imp.origin.timestamp <= b.end <= imp.extreme.timestamp))
        impulse_speed = float(imp.range_price) / impulse_bars
        pull_speed = float(adverse_net) / duration
        relative_speed = pull_speed/impulse_speed if impulse_speed else 99.0
        bodies = [float(abs(b.close-b.open)) for b in pb]
        impulse_bodies = [float(abs(b.close-b.open)) for b in bars if imp.origin.timestamp <= b.end <= imp.extreme.timestamp]
        body_ratio = median(bodies)/median(impulse_bodies) if bodies and impulse_bodies and median(impulse_bodies) else 0.0
        overlap = []
        alternating = []
        for x, y in zip(pb[:-1], pb[1:]):
            overlap.append(max(0.0, float(min(x.high,y.high)-max(x.low,y.low))) / max(1e-9, float(max(x.high,x.low)-min(x.low,x.low))))
            alternating.append((x.close-x.open)*(y.close-y.open) < 0)
        internal = 0
        if len(pb) >= 5:
            for i in range(2, len(pb)-2):
                if pb[i].high > max(pb[i-2].high,pb[i-1].high,pb[i+1].high,pb[i+2].high): internal += 1
                if pb[i].low < min(pb[i-2].low,pb[i-1].low,pb[i+1].low,pb[i+2].low): internal += 1
        pull_vol = median([b.volume for b in pb]) if pb else 0
        imp_vol = median([b.volume for b in bars if imp.origin.timestamp <= b.end <= imp.extreme.timestamp]) or 0
        volume_ratio = float(pull_vol/imp_vol) if imp_vol else 0.0
        return {"bars": pb, "depth": max(0.0, depth), "duration": duration, "efficiency": efficiency, "relative_speed": relative_speed, "body_ratio": body_ratio, "overlap": median(overlap) if overlap else 0.0, "alternating_ratio": sum(alternating)/len(alternating) if alternating else 0.0, "internal_swings": internal, "volume_ratio": volume_ratio, "atr": float(atr), "extreme": extreme}

    def _score(self, imp: Impulse, p: dict, continuation: bool = False) -> int:
        s = 0.0
        s += 20 * min(1, imp.atr_multiple/max(1e-9,self.cfg["preferred_impulse_atr"]))
        s += 15 * (0.5*min(1,imp.efficiency/self.cfg["preferred_impulse_efficiency"]) + 0.5*min(1,imp.directional_ratio/self.cfg["preferred_directional_candle_ratio"]))
        pull_quality = max(0.0, 1 - min(1, p["relative_speed"])) * 0.5 + max(0.0, 1-min(1,p["efficiency"])) * 0.5
        s += 20*pull_quality
        s += 15 if p["depth"] <= self.cfg["normal_pullback_depth_max"] else 8
        s += 10 * max(0, 1-min(1,p["volume_ratio"]/self.cfg["volume_reject_ratio"]))
        s += 5 if self._location_bonus() else 0
        s += 15 if continuation else 0
        return int(round(max(0,min(100,s))))

    def _location_bonus(self):
        return False  # VWAP/structure context is optional until supplied by the existing market context layer.

    def _reject(self, ts: datetime, stage: str, reason: str, actual=None, threshold=None):
        self.state = "FAILED" if reason in CRITICAL else self.state
        self.last_rejection = {"symbol": self.instrument_id, "timestamp": ts.isoformat(), "stage": stage, "reason": reason, "actual_value": actual, "threshold": threshold}
        self._audit("candidate_rejected", ts, stage=stage, reason=reason, actual_value=actual, threshold=threshold, candidate_id=self.candidate_id)
        return None

    def update(self, candle: Candle) -> PullbackSignal | None:
        ts = candle.end
        reason = self._validate(candle)
        if reason:
            return self._reject(ts, "DATA", reason)
        session = self._session(ts)
        if session == "CLOSING_AUCTION_POST_CONTINUOUS" or session == "OUTSIDE_CONTINUOUS_SESSION":
            self.state = "WATCHING"
            self._reject(ts, "SESSION", "SESSION_INVALID", session, "continuous")
            return None
        if self.cooldown_until and ts < self.cooldown_until:
            self.state = "COOLDOWN"
            return None
        self.history.append(candle); self._seen_candles.add(candle.start)
        if len(self.history) > 500: self.history.pop(0)
        if len(self.history) < int(self.cfg["min_history"]):
            self.state = "WATCHING"; self.last_state = self.anatomy(session=session); return None
        atr = self.atr()
        if atr is None or atr <= 0:
            return self._reject(ts, "DATA", "DATA_INVALID", "ATR_UNAVAILABLE", self.cfg["atr_period"])
        self._confirm_swing()
        new_impulse = self._find_impulse(atr)
        if new_impulse and (self.impulse is None or new_impulse.extreme.timestamp != self.impulse.extreme.timestamp or new_impulse.direction != self.impulse.direction):
            self.impulse = new_impulse
            self.state = "IMPULSE_DETECTED"
            self._audit("candidate_created", ts, candidate_id=self.candidate_id or uuid.uuid4().hex, stage="IMPULSE", direction=new_impulse.direction, impulse_atr=new_impulse.atr_multiple)
            self.candidate_id = self.candidate_id or uuid.uuid4().hex; self.candidate_created_at = ts
        if self.impulse is None:
            self.state = "WATCHING"; self.last_state = self.anatomy(session=session); return None
        imp = self.impulse
        if ts <= imp.extreme.timestamp:
            self.state = "IMPULSE_VALIDATED"; self.last_state = self.anatomy(session=session); return None
        bars = list(self.history)
        if imp.direction == "LONG" and min(b.low for b in bars if b.start >= imp.extreme.timestamp) <= imp.origin.price:
            return self._reject(ts, "STRUCTURE", "PROTECTED_STRUCTURE_BROKEN", imp.origin.price, imp.origin.price)
        if imp.direction == "SHORT" and max(b.high for b in bars if b.start >= imp.extreme.timestamp) >= imp.origin.price:
            return self._reject(ts, "STRUCTURE", "PROTECTED_STRUCTURE_BROKEN", imp.origin.price, imp.origin.price)
        p = self._pullback_stats(imp, bars, atr)
        if not p: self.state="PULLBACK_DEVELOPING"; self.last_state=self.anatomy(session=session); return None
        self.state = "PULLBACK_DEVELOPING"
        if p["duration"] > self.cfg["max_pullback_candles"]: return self._reject(ts,"PULLBACK_HEALTH","PULLBACK_TOO_LONG",p["duration"],self.cfg["max_pullback_candles"])
        if p["depth"] < self.cfg["min_pullback_depth"]: self.last_state=self.anatomy(session=session); return None
        if p["depth"] > self.cfg["deep_pullback_depth_max"]: return self._reject(ts,"PULLBACK_HEALTH","EXCESSIVE_RETRACEMENT",p["depth"],self.cfg["deep_pullback_depth_max"])
        if p["relative_speed"] > self.cfg["max_relative_pullback_speed"]: return self._reject(ts,"PULLBACK_HEALTH","COUNTERTREND_ACCELERATION",p["relative_speed"],self.cfg["max_relative_pullback_speed"])
        if p["efficiency"] > self.cfg["max_pullback_efficiency"]: return self._reject(ts,"PULLBACK_HEALTH","PULLBACK_REVERSAL_EVIDENCE",p["efficiency"],self.cfg["max_pullback_efficiency"])
        if p["internal_swings"] > self.cfg["max_internal_swings"] and p["efficiency"] < self.cfg["acceptable_pullback_efficiency"] and p["overlap"] > 0.35:
            return self._reject(ts,"PULLBACK_HEALTH","SEVERE_CHOP",p["internal_swings"],self.cfg["max_internal_swings"])
        if p["body_ratio"] > self.cfg["countertrend_body_multiplier"] and p["efficiency"] > self.cfg["acceptable_pullback_efficiency"]:
            return self._reject(ts,"PULLBACK_HEALTH","COUNTERTREND_BODY_EXPANSION",p["body_ratio"],self.cfg["countertrend_body_multiplier"])
        if p["volume_ratio"] > self.cfg["volume_reject_ratio"] and p["efficiency"] > self.cfg["volume_efficiency_reject"]:
            return self._reject(ts,"PARTICIPATION","PULLBACK_REVERSAL_EVIDENCE",p["volume_ratio"],self.cfg["volume_reject_ratio"])
        score = self._score(imp,p)
        if score < self.cfg["candidate_score_min"]:
            self._audit("candidate_updated",ts,candidate_id=self.candidate_id,stage="HEALTH",score=score)
            self.last_state=self.anatomy(session=session); return None
        self.state = "HEALTHY_CANDIDATE"
        trigger_level = max(b.high for b in p["bars"][:-1]) if imp.direction == "LONG" and len(p["bars"])>1 else min(b.low for b in p["bars"][:-1]) if imp.direction == "SHORT" and len(p["bars"])>1 else None
        if trigger_level is None: self.state="TRIGGER_PENDING"; self.last_state=self.anatomy(session=session); return None
        displacement = (candle.close-Decimal(str(trigger_level))) if imp.direction=="LONG" else (Decimal(str(trigger_level))-candle.close)
        continuation = displacement >= atr*Decimal(str(self.cfg["continuation_atr"]))
        if not continuation:
            self.state="TRIGGER_PENDING"; self.last_state=self.anatomy(session=session); return None
        abnormal = float(abs(candle.close-candle.open)/(candle.high-candle.low)) if candle.high>candle.low else 0.0
        if abnormal > 0.90 and float(abs(candle.close-candle.open)) > float(atr)*2.5:
            return self._reject(ts,"CONTINUATION","TRIGGER_FAILURE",abnormal,0.90)
        score = self._score(imp,p,True)
        if score < self.cfg["live_alert_score_min"]:
            self.state="HEALTHY_CANDIDATE"; self.last_state=self.anatomy(session=session); return None
        self.state="TRIGGER_CONFIRMED"
        invalidation = imp.origin.price
        signal = PullbackSignal(
            self.instrument_id, ts, imp.direction, imp.origin.price, imp.extreme.price, p["depth"], candle.close,
            invalidation, score/100.0, False,
            f"EXPERIMENTAL_V2: validated structural impulse, controlled pullback and continuation; score={score}",
            signal_id=self.candidate_id or uuid.uuid4().hex, health_score=score, classification="TRIGGER_CONFIRMED",
            session=session, impulse_range=imp.range_price, impulse_atr_multiple=imp.atr_multiple,
            impulse_efficiency=imp.efficiency, directional_candle_ratio=imp.directional_ratio,
            countertrend_excursion=imp.countertrend_excursion, pullback_duration_candles=p["duration"],
            pullback_speed=p["relative_speed"], pullback_efficiency=p["efficiency"], volume_ratio=p["volume_ratio"],
            impulse_high=max(imp.origin.price,imp.extreme.price), impulse_low=min(imp.origin.price,imp.extreme.price),
            protected_level=invalidation,
        )
        self.last_signal=signal
        self._audit("candidate_triggered",ts,candidate_id=signal.signal_id,health_score=score,direction=imp.direction,trigger_price=candle.close,invalidation_price=invalidation)
        self.last_state=self.anatomy(session=session, signal=signal)
        return signal

    def anatomy(self, session: str | None = None, signal: PullbackSignal | None = None) -> dict:
        c = self.history[-1] if self.history else None
        if c is None: return {"instrument_id": self.instrument_id, "state":"WATCHING", "detection_phase":"WATCHING"}
        atr = self.atr()
        data = {"instrument_id": self.instrument_id, "timestamp": c.end, "current_price": c.close, "state": self.state, "detection_phase": self.state, "session": session or self._session(c.end), "history_bars": len(self.history), "atr14": atr, "atr_percentile": self.atr_percentile(), "experimental_v2": True}
        if self.impulse:
            data.update({"impulse_direction": self.impulse.direction, "impulse_magnitude": self.impulse.range_price, "impulse_high": max(self.impulse.origin.price,self.impulse.extreme.price), "impulse_low": min(self.impulse.origin.price,self.impulse.extreme.price), "impulse_atr_multiple": self.impulse.atr_multiple, "impulse_efficiency": self.impulse.efficiency, "directional_candle_ratio": self.impulse.directional_ratio, "countertrend_excursion": self.impulse.countertrend_excursion, "impulse_exhaustion_risk": self.impulse.exhaustion_risk})
            p = self._pullback_stats(self.impulse, self.history, atr) if atr else None
            if p: data.update({"retracement_depth_pct":p["depth"]*100,"pullback_duration_candles":p["duration"],"pullback_speed":p["relative_speed"],"pullback_efficiency":p["efficiency"],"pullback_overlap":p["overlap"],"alternating_direction_ratio":p["alternating_ratio"],"internal_swing_count":p["internal_swings"],"volume_ratio":p["volume_ratio"]})
        if signal:
            data.update({"health_score":signal.health_score,"trigger_price":signal.trigger_price,"invalidation_price":signal.invalidation_level,"classification":signal.classification,"signal_id":signal.signal_id})
        if self.last_rejection: data["last_rejection"] = self.last_rejection
        return data

    def rejection_history(self, limit=100):
        if not self.audit_path.exists(): return []
        rows=[]
        for line in self.audit_path.read_text(encoding="utf-8").splitlines()[-limit:]:
            try: rows.append(json.loads(line))
            except json.JSONDecodeError: pass
        return rows
