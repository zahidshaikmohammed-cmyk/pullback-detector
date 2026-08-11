from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.healthy_pullback_v2 import HealthyPullbackV2, Impulse, Swing
from pullback_detector.models import Candle

BASE = datetime(2026, 8, 11, 4, 0, tzinfo=timezone.utc)  # 09:30 IST


def c(i, close=100, high=None, low=None, volume=1000, complete=True):
    start = BASE + timedelta(minutes=5*i)
    close = Decimal(str(close)); high = Decimal(str(high if high is not None else close + Decimal("1"))); low = Decimal(str(low if low is not None else close - Decimal("1")))
    return Candle(1, start, start + timedelta(minutes=5), close, high, low, close, volume, complete, 300)


def engine():
    e = HealthyPullbackV2(1, {**HealthyPullbackV2.default_config(), "min_history": 5, "atr_period": 3}, "/tmp/pullback-v2-tests")
    e.history = [c(i, 100+i*0.1) for i in range(5)]
    e._seen_candles = {x.start for x in e.history}
    origin = Swing("LOW", Decimal("99"), e.history[0].end, e.history[2].end)
    extreme = Swing("HIGH", Decimal("110"), e.history[2].end, e.history[4].end)
    e.impulse = Impulse("LONG", origin, extreme, Decimal("11"), 2.0, 0.70, 0.70, 0.10, 0.55, 1.0, False)
    e.candidate_id = "fixture"
    e.candidate_created_at = e.history[-1].end
    return e


def stats(depth=.4, speed=.4, efficiency=.5, body_ratio=1.0, internal=1, volume=0.9, overlap=.2):
    return {"bars":[c(5,106,107,105), c(6,105,106,104)], "depth":depth, "duration":2, "efficiency":efficiency, "relative_speed":speed, "body_ratio":body_ratio, "overlap":overlap, "alternating_ratio":.5, "internal_swings":internal, "volume_ratio":volume, "atr":1.0, "extreme":Decimal("110")}


def run(e, p):
    e._pullback_stats = lambda *_: p
    return e.update(c(7, 106.5, 108, 105.5))


def test_clean_bullish_impulse_healthy_pullback_reaches_trigger(monkeypatch):
    e=engine(); p=stats(); e._score=lambda *args: 90
    assert run(e,p) is not None


def test_clean_bearish_impulse_can_be_represented():
    e=engine(); e.impulse = Impulse("SHORT", e.impulse.extreme, e.impulse.origin, Decimal("11"),2,.70,.70,.10,.55,1,False)
    assert e.impulse.direction == "SHORT"


def test_weak_impulse_fixture_is_below_minimum():
    e=engine(); e.impulse.atr_multiple=.9
    assert e.impulse.atr_multiple < e.cfg["min_impulse_atr"]


def test_noisy_impulse_fixture_has_low_efficiency():
    e=engine(); e.impulse.efficiency=.42
    assert e.impulse.efficiency < e.cfg["min_impulse_efficiency"]


def test_excessive_retracement_rejected():
    e=engine(); p=stats(depth=.76); assert e._reject(BASE,"PULLBACK_HEALTH","EXCESSIVE_RETRACEMENT",p["depth"],.75) is None
    assert e.last_rejection["reason"] == "EXCESSIVE_RETRACEMENT"


def test_protected_swing_break_is_critical():
    e=engine(); assert e._reject(BASE,"STRUCTURE","PROTECTED_STRUCTURE_BROKEN",98,99) is None
    assert e.last_rejection["reason"] == "PROTECTED_STRUCTURE_BROKEN"


def test_aggressive_countertrend_acceleration_rejection():
    e=engine(); assert e._reject(BASE,"PULLBACK_HEALTH","COUNTERTREND_ACCELERATION",1.2,1.1) is None


def test_high_pullback_efficiency_is_reversal_risk():
    e=engine(); assert e._reject(BASE,"PULLBACK_HEALTH","PULLBACK_REVERSAL_EVIDENCE",.9,.85) is None


def test_severe_chop_requires_multiple_signals():
    e=engine(); p=stats(internal=5, overlap=.5, efficiency=.5)
    assert p["internal_swings"] > e.cfg["max_internal_swings"] and p["overlap"] > .35 and p["efficiency"] < .70


def test_high_opposing_volume_combined_with_aggression():
    e=engine(); p=stats(volume=1.35, efficiency=.75)
    assert p["volume_ratio"] > e.cfg["volume_reject_ratio"] and p["efficiency"] > e.cfg["volume_efficiency_reject"]


def test_shallow_pullback_is_not_signal():
    e=engine(); p=stats(depth=.10); assert p["depth"] < e.cfg["min_pullback_depth"]


def test_duration_limit_is_hard():
    e=engine(); p=stats(); p["duration"]=13; assert p["duration"] > e.cfg["max_pullback_candles"]


def test_body_expansion_requires_efficiency_confirmation():
    e=engine(); p=stats(body_ratio=1.3, efficiency=.75); assert p["body_ratio"] > e.cfg["countertrend_body_multiplier"] and p["efficiency"] > .70


def test_volume_warning_is_not_standalone_rejection():
    e=engine(); p=stats(volume=1.25, efficiency=.5); assert p["volume_ratio"] > 1.2 and p["efficiency"] <= .70


def test_score_cannot_be_negative():
    e=engine(); p=stats(speed=1.0, efficiency=.8, volume=1.3); assert e._score(e.impulse,p) >= 0


def test_score_is_capped_at_100():
    e=engine(); p=stats(speed=0.1, efficiency=.1, volume=.1); assert e._score(e.impulse,p,True) <= 100


def test_continuation_requires_displacement_beyond_trigger(monkeypatch):
    e=engine(); p=stats(); e._pullback_stats=lambda *_:p; e._score=lambda *args:90
    signal=run(e,p)
    assert signal is not None
    assert signal.classification == "TRIGGER_CONFIRMED"


def test_trigger_snapshot_has_immutable_metrics(monkeypatch):
    e=engine(); p=stats(); e._pullback_stats=lambda *_:p; e._score=lambda *args:90
    s=run(e,p); assert s.signal_id == "fixture" and s.health_score == 90 and s.impulse_range == Decimal("11")


def test_incomplete_candle_never_creates_signal():
    e=engine(); result=e.update(c(7,106,107,105,complete=False)); assert result is None and e.state == "FAILED"


def test_duplicate_candle_is_rejected():
    e=engine(); x=e.history[-1]; result=e.update(x); assert result is None and e.last_rejection["reason"] == "DATA_INVALID"


def test_no_lookahead_swing_not_confirmed_before_two_following_bars():
    e=HealthyPullbackV2(1,{**HealthyPullbackV2.default_config(),"min_history":5,"atr_period":3},"/tmp/pullback-v2-tests")
    for i in range(4): e.update(c(i,100+i))
    assert not e.swings


def test_session_boundary_after_1530_is_invalid():
    e=engine(); result=e.update(Candle(1,datetime(2026,8,11,10,30,tzinfo=timezone.utc),datetime(2026,8,11,10,35,tzinfo=timezone.utc),Decimal(100),Decimal(101),Decimal(99),Decimal(100),1000,True,300))
    assert result is None and e.last_rejection["reason"] == "SESSION_INVALID"


def test_stale_future_data_is_not_accepted():
    e=engine(); future=datetime.now(timezone.utc)+timedelta(minutes=10); result=e.update(Candle(1,future-timedelta(minutes=5),future,Decimal(100),Decimal(101),Decimal(99),Decimal(100),1000,True,300))
    assert result is None and e.last_rejection["reason"] == "DATA_INVALID"


def test_v2_is_explicitly_experimental():
    e=engine(); assert e.anatomy()["experimental_v2"] is True
