from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.lifecycle import PullbackLifecycleEngine
from pullback_detector.models import Candle, PullbackSignal, Tick

BASE = datetime(2026, 8, 11, 9, 30, tzinfo=timezone.utc)


def signal(direction="LONG", ts=BASE):
    return PullbackSignal(25, ts, direction, Decimal("100"), Decimal("110"), 0.4, Decimal("106"), Decimal("102"), 0.8, True, "EXPERIMENTAL_V1")


def candle(close="106", ts=BASE):
    return Candle(25, ts - timedelta(minutes=5), ts, Decimal("105"), Decimal("110"), Decimal("102"), Decimal(close), 1000, True, 300)


def tick(price, ts): return Tick(25, ts, Decimal(str(price)), 10)


def engine(tmp_path, **kwargs): return PullbackLifecycleEngine(tmp_path, **kwargs)


def test_trigger_freezes_snapshot_and_prevents_duplicate(tmp_path):
    e = engine(tmp_path); state = e.trigger(signal(), candle()); duplicate = e.trigger(signal(ts=BASE + timedelta(minutes=5)), candle(ts=BASE + timedelta(minutes=5)))
    assert state is not None and duplicate is None
    assert state.snapshot.trigger_price == Decimal("106") and state.snapshot.target_1 == Decimal("116") and state.snapshot.target_2 == Decimal("126")
    assert state.snapshot.invalidation_price == Decimal("102") and state.snapshot.signal_id and len(e.active) == 1


def test_target_one_hit_closes_and_emits_event(tmp_path):
    e = engine(tmp_path); state = e.trigger(signal(), candle()); events = e.update_tick(tick("116", BASE + timedelta(minutes=1)))
    assert [x["event"] for x in events] == ["TARGET_1_REACHED", "SETUP_CLOSED"] and events[-1]["outcome"] == "TARGET_1_HIT"
    assert not e.active and e.closed[-1].snapshot.signal_id == state.snapshot.signal_id and e.closed[-1].snapshot.trigger_price == Decimal("106")


def test_target_two_can_close_directly_on_jump(tmp_path):
    e = engine(tmp_path); e.trigger(signal(), candle()); events = e.update_tick(tick("126", BASE + timedelta(minutes=1)))
    assert [x["event"] for x in events] == ["TARGET_2_REACHED", "SETUP_CLOSED"] and events[-1]["outcome"] == "TARGET_2_HIT"


def test_invalidation_hit_closes_setup_and_emits_event(tmp_path):
    e = engine(tmp_path); e.trigger(signal(), candle()); events = e.update_tick(tick("102", BASE + timedelta(minutes=1)))
    assert [x["event"] for x in events] == ["INVALIDATION_REACHED", "SETUP_CLOSED"] and events[-1]["outcome"] == "INVALIDATION_HIT" and not e.active


def test_structure_failed_is_distinct_candle_terminal(tmp_path):
    e = engine(tmp_path); e.trigger(signal(), candle()); events = e.update_candle(candle("101", BASE + timedelta(minutes=5)))
    assert events[-1]["outcome"] == "STRUCTURE_FAILED" and not e.active


def test_expiry_closes_setup(tmp_path):
    e = engine(tmp_path, expiry_seconds=60); e.trigger(signal(), candle()); events = e.update_tick(tick("106.5", BASE + timedelta(seconds=61)))
    assert events[-1]["outcome"] == "EXPIRED" and not e.active


def test_cooldown_blocks_rearm_then_allows_rearm(tmp_path):
    e = engine(tmp_path, cooldown_seconds=300); first = e.trigger(signal(), candle()); e.update_tick(tick("116", BASE + timedelta(minutes=1)))
    assert e.trigger(signal(ts=BASE + timedelta(minutes=2)), candle(ts=BASE + timedelta(minutes=2))) is None
    second = e.trigger(signal(ts=BASE + timedelta(minutes=7)), candle(ts=BASE + timedelta(minutes=7)))
    assert second is not None and second.snapshot.signal_id != first.snapshot.signal_id


def test_mfe_mae_are_tracked_from_live_prices(tmp_path):
    e = engine(tmp_path); e.trigger(signal(), candle()); e.update_tick(tick("112", BASE + timedelta(minutes=1))); e.update_tick(tick("104", BASE + timedelta(minutes=2)))
    assert e.active[25].mfe == Decimal("6") and e.active[25].mae == Decimal("2")


def test_lifecycle_replays_after_process_restart(tmp_path):
    e = engine(tmp_path, cooldown_seconds=300); e.trigger(signal(), candle()); e.update_tick(tick("116", BASE + timedelta(minutes=1))); restored = engine(tmp_path, cooldown_seconds=300)
    assert not restored.active and restored.closed[-1].outcome == "TARGET_1_HIT" and restored.closed[-1].snapshot.trigger_price == Decimal("106")
