from datetime import datetime, timezone, timedelta
from decimal import Decimal

from pullback_detector.health import ConnectivityHealth
from pullback_detector.models import Candle, Tick
from pullback_detector.persistence import EventStore


def _expected(count=22):
    return [{"symbol": f"SYM{i}", "security_id": i, "exchange_segment": "NSE_EQ" if i > 2 else "IDX_I", "instrument_type": "EQUITY" if i > 2 else "INDEX"} for i in range(1, count + 1)]


def _tick(sid, ts):
    segment = "IDX_I" if sid <= 2 else "NSE_EQ"
    return Tick(sid, ts, Decimal("100"), quantity=1, cumulative_volume=1, source_timestamp=ts, exchange_segment=segment)


def _candle(sid, ts, tf):
    return Candle(sid, ts, ts + timedelta(seconds=tf), Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100.5"), 10, True, tf, 10, 10)


def test_producing_and_missing_sets_are_exact(tmp_path):
    EventStore(tmp_path); health = ConnectivityHealth(); now = datetime.now(timezone.utc)
    for sid in range(1, 22): health.record_tick(_tick(sid, now), now)
    report = health.report(now, 22, 0, 0, _expected(), True, False)
    assert len(report["producing_instruments"]) == 21
    assert len(report["not_producing_instruments"]) == 1
    assert report["not_producing_instruments"][0]["security_id"] == 22
    assert report["not_producing_instruments"][0]["reason"] == "NO_TICK_RECEIVED"


def test_stale_instrument_is_explicit(tmp_path):
    EventStore(tmp_path); health = ConnectivityHealth(); now = datetime.now(timezone.utc); old = now - timedelta(seconds=120)
    for sid in range(1, 23): health.record_tick(_tick(sid, old), old)
    report = health.report(now, 22, 0, 0, _expected(), True, False)
    assert report["not_producing_instruments"] == []
    assert all(row["reason"] == "STALE_FEED" for row in report["producing_instruments"])


def test_persistence_ack_and_count_accuracy(tmp_path):
    store = EventStore(tmp_path); ts = datetime(2026, 8, 12, 7, 0, tzinfo=timezone.utc)
    assert store.candle(_candle(1, ts, 60)) is True; assert store.candle(_candle(1, ts, 60)) is False; assert store.candle(_candle(1, ts, 300)) is True
    snap = store.persistence_snapshot(); assert snap["persisted_1m_candles"] == 1; assert snap["persisted_5m_candles"] == 1; assert snap["persistence_failure_count"] == 0


def test_restart_recovery_requires_real_rehydration_and_continuation(tmp_path):
    ts = datetime(2026, 8, 12, 7, 0, tzinfo=timezone.utc); first = EventStore(tmp_path)
    first.candle(_candle(1, ts, 60)); first.candle(_candle(1, ts, 300)); first.health({"generated_at": ts.isoformat(), "accepted_tick_count": 10, "ticks_sent_to_candle_engine": 10, "completed_1m_candles": 1, "completed_5m_candles": 1, "persisted_candle_count_1m": 1, "persisted_candle_count_5m": 1})
    second = EventStore(tmp_path); assert second.persistence_snapshot()["persisted_1m_candles"] == 1; assert second.persistence_snapshot()["persisted_5m_candles"] == 1
    second.tick(ts, _tick(1, ts + timedelta(seconds=1))); second.candle(_candle(1, ts + timedelta(minutes=1), 60)); recovery = second.recovery_snapshot()
    assert recovery["restart_recovery_verified"] is True; assert recovery["continuity_status"] == "PASS"; assert recovery["recovered_candle_counts"] == {"1m": 1, "5m": 1}; assert recovery["duplicate_count"] == 0


def test_counter_progression_uses_cumulative_values(tmp_path):
    store = EventStore(tmp_path); ts = datetime.now(timezone.utc)
    first = {"generated_at": ts.isoformat(), "accepted_tick_count": 100, "ticks_sent_to_candle_engine": 100, "completed_1m_candles": 10, "completed_5m_candles": 2, "persisted_candle_count_1m": 10, "persisted_candle_count_5m": 2}
    store._previous_health_report = first; result = store.counter_progression({**first, "generated_at": (ts + timedelta(minutes=10)).isoformat(), "accepted_tick_count": 50, "ticks_sent_to_candle_engine": 50, "completed_1m_candles": 5, "completed_5m_candles": 1, "persisted_candle_count_1m": 5, "persisted_candle_count_5m": 1})
    assert result["counter_progression_verified"] is True; assert result["after"]["accepted_tick_count"] == 50


def test_readiness_gate_blocks_until_restart_and_progression(tmp_path):
    EventStore(tmp_path); health = ConnectivityHealth(); now = datetime.now(timezone.utc)
    for sid in range(1, 23): health.record_tick(_tick(sid, now), now)
    report = health.report(now, 22, 0, 0, _expected(), True, False)
    assert report["overall_phase1_status"] == "NOT_READY_FOR_PHASE_2"
    assert report["first_failure_reason"] in {"1m_candle_generation_verified", "5m_candle_generation_verified", "persistence_verified", "counter_progression_verified", "restart_recovery_verified"}


def test_all_22_producing_set_is_verified(tmp_path):
    EventStore(tmp_path); health = ConnectivityHealth(); now = datetime.now(timezone.utc)
    for sid in range(1, 23): health.record_tick(_tick(sid, now), now)
    report = health.report(now, 22, 0, 0, _expected(), True, False)
    assert len(report["producing_instruments"]) == 22; assert report["not_producing_instruments"] == []
    assert report["phase1_gates"]["producing_set_verified"] is True; assert report["phase1_gates"]["exact_non_producing_set_verified"] is True
