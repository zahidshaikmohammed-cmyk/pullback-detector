from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.health import ConnectivityHealth
from pullback_detector.models import Candle, Tick
from pullback_detector.persistence import EventStore


def _tick(instrument_id=100, segment="NSE_EQ", sequence=1):
    ts = datetime(2026, 8, 12, 7, 30, tzinfo=timezone.utc)
    return Tick(instrument_id, ts, Decimal("100"), 1, segment, 10, sequence=sequence)


def _candle(instrument_id=100, start=None):
    start = start or datetime(2026, 8, 12, 7, 30, tzinfo=timezone.utc)
    return Candle(instrument_id, start, start + timedelta(minutes=1), Decimal("99"), Decimal("101"), Decimal("98"), Decimal("100"), 10, True, 60)


def test_canonical_22_instrument_registry_uses_segment_and_security_id():
    equities = [{"symbol": f"EQ{i}", "security_id": i, "exchange_segment": "NSE_EQ", "instrument_type": "EQUITY"} for i in range(1, 21)]
    benchmarks = [
        {"symbol": "NIFTY", "security_id": 1, "exchange_segment": "IDX_I", "instrument_type": "INDEX"},
        {"symbol": "BANKNIFTY", "security_id": 2, "exchange_segment": "IDX_I", "instrument_type": "INDEX"},
    ]
    expected = equities + benchmarks
    expected_keys, producing, missing = ConnectivityHealth._expected_sets(expected, set())
    assert len(expected) == 22
    assert len(expected_keys) == 22
    assert producing == set()
    assert len(missing) == 22


def test_producing_and_missing_sets_are_exact_and_disjoint():
    expected = [
        {"symbol": "EQ1", "security_id": 1, "exchange_segment": "NSE_EQ", "instrument_type": "EQUITY"},
        {"symbol": "EQ2", "security_id": 2, "exchange_segment": "NSE_EQ", "instrument_type": "EQUITY"},
        {"symbol": "NIFTY", "security_id": 1, "exchange_segment": "IDX_I", "instrument_type": "INDEX"},
    ]
    expected_keys, producing, missing = ConnectivityHealth._expected_sets(expected, {("NSE_EQ", 1), ("IDX_I", 1)})
    assert len(expected_keys) == 3
    assert producing == {("NSE_EQ", 1), ("IDX_I", 1)}
    assert missing == {("NSE_EQ", 2)}
    assert expected_keys == producing | missing
    assert producing.isdisjoint(missing)


def test_replayed_event_is_rejected_but_new_event_is_accepted(tmp_path):
    store = EventStore(tmp_path)
    received = datetime(2026, 8, 12, 7, 30, 1, tzinfo=timezone.utc)
    assert store.tick(received, _tick(sequence=1)) is True
    assert store.tick(received, _tick(sequence=1)) is False
    assert store.duplicate_event_count == 1
    assert store.tick(received, _tick(sequence=2)) is True


def test_restart_hydration_preserves_identity_and_candle_continuity(tmp_path):
    received = datetime(2026, 8, 12, 7, 30, 1, tzinfo=timezone.utc)
    first = EventStore(tmp_path)
    assert first.tick(received, _tick(sequence=1)) is True
    assert first.candle(_candle()) is True
    first.health({"generated_at": received.isoformat(), "cumulative_counter_values": {"accepted_tick_count": 1}})

    recovered = EventStore(tmp_path)
    assert recovered._recovery_probe_passed is True
    assert recovered.tick(received, _tick(sequence=1)) is False
    assert recovered.tick(received, _tick(sequence=2)) is True
    next_start = datetime(2026, 8, 12, 7, 31, tzinfo=timezone.utc)
    assert recovered.candle(_candle(start=next_start)) is True
    recovery = recovered.recovery_snapshot()
    assert recovery["canonical_duplicate_events"] == 0
    assert recovery["duplicate_count"] == 0
    assert recovery["continuity_status"] == "PASS"
    assert recovery["restart_recovery_verified"] is True


def test_duplicate_candle_contribution_is_rejected(tmp_path):
    store = EventStore(tmp_path)
    candle = _candle()
    assert store.candle(candle) is True
    assert store.candle(candle) is False
    assert store.duplicate_candle_contribution_count == 1
