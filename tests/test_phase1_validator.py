from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pullback_detector.health import ConnectivityHealth
from pullback_detector.models import Candle, Tick
from pullback_detector.persistence import EventStore
from pullback_detector.phase1_validation import Phase1Validator
from pullback_detector import phase1_validation
from pullback_detector.live import LIVE_RUNTIME


def _expected():
    return [
        {"symbol": f"EQ{i}", "security_id": i, "exchange_segment": "NSE_EQ", "instrument_type": "EQUITY"}
        for i in range(1, 21)
    ] + [
        {"symbol": "NIFTY", "security_id": 1, "exchange_segment": "IDX_I", "instrument_type": "INDEX"},
        {"symbol": "BANKNIFTY", "security_id": 2, "exchange_segment": "IDX_I", "instrument_type": "INDEX"},
    ]


def _tick(sid=1, ts=None):
    ts = ts or datetime.now(timezone.utc)
    return Tick(sid, ts, Decimal("100"), 1, "NSE_EQ", 10, sequence=1, source_timestamp=ts)


def _candle(sid=1, start=None, tf=60):
    start = start or datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return Candle(sid, start, start + timedelta(seconds=tf), Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), 10, True, tf)


@pytest.fixture(autouse=True)
def reset_runtime(monkeypatch):
    LIVE_RUNTIME.clear()
    phase1_validation.ACTIVE_VALIDATOR = None
    yield
    LIVE_RUNTIME.clear()
    phase1_validation.ACTIVE_VALIDATOR = None


def _live_runtime(tmp_path):
    store = EventStore(tmp_path)
    health = ConnectivityHealth()
    now = datetime.now(timezone.utc)
    health.record_tick(_tick(1, now), now)
    LIVE_RUNTIME.update({
        "health": health,
        "expected_instruments": _expected(),
        "persisted_1m": 0,
        "persisted_5m": 0,
        "websocket_connected": True,
        "restart_recovery_verified": False,
        "one_min": None,
        "five_min": None,
    })
    return store, health


@pytest.mark.asyncio
async def test_no_live_data_is_observable(tmp_path, monkeypatch):
    EventStore(tmp_path)
    health = ConnectivityHealth()
    LIVE_RUNTIME.update({"health": health, "expected_instruments": _expected(), "websocket_connected": True})
    validator = Phase1Validator(tmp_path)
    validator.start()
    await validator.poll()
    assert validator.state == "VALIDATION_WAITING_FOR_DATA"
    assert validator.reason == "WAITING_FOR_LIVE_DATA"
    assert validator.snapshot_count == 0


@pytest.mark.asyncio
async def test_snapshot_one_emits_from_real_runtime_state(tmp_path):
    _live_runtime(tmp_path)
    validator = Phase1Validator(tmp_path)
    validator.start()
    await validator.poll()
    assert validator.snapshot_count == 1
    assert validator.snapshots[0]["accepted_ticks"] == 1
    assert validator.snapshots[0]["deployment_sha"] == validator.sha


@pytest.mark.asyncio
async def test_snapshot_two_requires_real_progression(tmp_path):
    store, health = _live_runtime(tmp_path)
    validator = Phase1Validator(tmp_path)
    validator.SNAPSHOT_INTERVAL_SECONDS = 0
    validator.start()
    await validator.poll()
    assert validator.snapshot_count == 1
    health.record_tick(_tick(1, datetime.now(timezone.utc) + timedelta(seconds=1)), datetime.now(timezone.utc))
    await validator.poll()
    assert validator.snapshot_count == 2
    assert validator.snapshots[1]["accepted_ticks"] > validator.snapshots[0]["accepted_ticks"]


def test_deployment_sha_invalidates_prior_validation(tmp_path, monkeypatch):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "old-sha")
    first = Phase1Validator(tmp_path)
    first.start()
    first.snapshot_count = 1
    first._save()
    monkeypatch.setenv("RENDER_GIT_COMMIT", "new-sha")
    second = Phase1Validator(tmp_path)
    assert second.snapshot_count == 0
    assert second.reason == "NEW_DEPLOYMENT_SHA_INVALIDATED_PRIOR_VALIDATION"


def test_restart_failure_is_explicit_without_real_pre_restart_state(tmp_path):
    _live_runtime(tmp_path)
    validator = Phase1Validator(tmp_path)
    snap = validator._canonical_snapshot()
    validator._update_restart(snap)
    assert validator.restart_state == "NOT_STARTED"
    assert validator.restart_reason == "NO_REAL_PRE_RESTART_CHECKPOINT_WITH_CANDLE_STATE"


def test_restart_success_uses_real_persisted_state(tmp_path):
    ts = datetime(2026, 8, 12, 7, 30, tzinfo=timezone.utc)
    first = EventStore(tmp_path)
    first.candle(_candle(start=ts, tf=60))
    first.candle(_candle(start=ts, tf=300))
    first.health({"generated_at": ts.isoformat(), "accepted_tick_count": 1, "ticks_sent_to_candle_engine": 1, "completed_1m_candles": 1, "completed_5m_candles": 1, "persisted_candle_count_1m": 1, "persisted_candle_count_5m": 1})
    recovered = EventStore(tmp_path)
    recovered.tick(ts, _tick(1, ts + timedelta(seconds=1)))
    recovered.candle(_candle(start=ts + timedelta(minutes=1), tf=60))
    health = ConnectivityHealth()
    health.record_tick(_tick(1, ts + timedelta(minutes=1)), ts + timedelta(minutes=1))
    LIVE_RUNTIME.update({"health": health, "expected_instruments": _expected(), "websocket_connected": True, "persisted_1m": 1, "persisted_5m": 1})
    validator = Phase1Validator(tmp_path)
    snap = validator._canonical_snapshot()
    validator._update_restart(snap)
    assert validator.restart_state == "PASS"


@pytest.mark.asyncio
async def test_validator_exception_is_not_swallowed(tmp_path, monkeypatch):
    validator = Phase1Validator(tmp_path)
    validator.start()
    def explode():
        raise RuntimeError("diagnostic boom")
    monkeypatch.setattr(validator, "_canonical_snapshot", explode)
    await validator.poll()
    assert validator.state == "VALIDATION_FAILED"
    assert "VALIDATOR_EXCEPTION:RuntimeError:diagnostic boom" == validator.reason
