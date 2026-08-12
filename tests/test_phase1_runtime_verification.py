from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.health import ConnectivityHealth
from pullback_detector.models import Tick

UTC = timezone.utc


def make_tick(sid: int, ts: datetime, price: str = "100") -> Tick:
    return Tick(
        instrument_id=sid,
        timestamp=ts,
        price=Decimal(price),
        quantity=1,
        exchange_segment="NSE_EQ",
        cumulative_volume=1,
        source_timestamp=ts + timedelta(hours=5, minutes=30),
        source_clock_skew_seconds=19800.0,
    )


def test_canonical_feed_state_uses_normalized_event_age():
    now = datetime(2026, 8, 12, 7, 0, tzinfo=UTC)
    health = ConnectivityHealth()
    health.record_tick(make_tick(1, now - timedelta(seconds=2)), now - timedelta(seconds=1))
    report = health.report(now, subscribed_instruments=1, expected_instruments=[{"symbol": "RELIANCE", "security_id": 1}], websocket_connected=True, restart_recovery_verified=True)
    assert report["feed_state"] == "LIVE"
    assert 1.5 <= report["global_data_age_seconds"] <= 2.5
    assert report["last_tick"]["1"] == (now - timedelta(seconds=2)).isoformat()


def test_stale_state_when_socket_connected_but_no_recent_valid_events():
    now = datetime(2026, 8, 12, 7, 0, tzinfo=UTC)
    health = ConnectivityHealth()
    health.record_tick(make_tick(1, now - timedelta(seconds=90)), now - timedelta(seconds=89))
    report = health.report(now, subscribed_instruments=1, expected_instruments=[{"symbol": "RELIANCE", "security_id": 1}], websocket_connected=True)
    assert report["feed_state"] == "STALE"
    assert report["dhan_connection_status"] == "stale"


def test_disconnect_state_requires_no_recent_events_and_socket_disconnected():
    now = datetime(2026, 8, 12, 7, 0, tzinfo=UTC)
    health = ConnectivityHealth()
    health.record_tick(make_tick(1, now - timedelta(seconds=90)), now - timedelta(seconds=89))
    report = health.report(now, subscribed_instruments=1, expected_instruments=[{"symbol": "RELIANCE", "security_id": 1}], websocket_connected=False)
    assert report["feed_state"] == "DISCONNECTED"
    assert report["dhan_connection_status"] == "disconnected"


def test_exact_missing_instrument_is_exposed():
    now = datetime(2026, 8, 12, 7, 0, tzinfo=UTC)
    health = ConnectivityHealth()
    health.record_tick(make_tick(1, now - timedelta(seconds=1)), now - timedelta(seconds=1))
    expected = [
        {"symbol": "RELIANCE", "security_id": 1, "exchange_segment": "NSE_EQ"},
        {"symbol": "TCS", "security_id": 2, "exchange_segment": "NSE_EQ"},
    ]
    report = health.report(now, subscribed_instruments=2, expected_instruments=expected, websocket_connected=True, restart_recovery_verified=True)
    assert len(report["producing_instruments"]) == 1
    assert report["producing_instruments"][0]["security_id"] == 1
    assert report["not_producing_instruments"][0]["security_id"] == 2
    assert report["not_producing_instruments"][0]["reason"] == "NO_TICK_RECEIVED"


def test_candle_engine_counter_break_is_visible():
    now = datetime(2026, 8, 12, 7, 0, tzinfo=UTC)
    health = ConnectivityHealth()
    health.record_tick(make_tick(1, now - timedelta(seconds=1)), now - timedelta(seconds=1))
    health.record_candle_engine_tick()
    health.record_candle_engine_reject()
    report = health.report(now, subscribed_instruments=1, expected_instruments=[{"symbol": "RELIANCE", "security_id": 1}], websocket_connected=True)
    assert report["ticks_sent_to_candle_engine"] == 1
    assert report["ticks_rejected_by_candle_engine"] == 1
    assert report["overall_phase1_status"] == "NOT_READY_FOR_PHASE_2"
