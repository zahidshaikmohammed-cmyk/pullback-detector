from datetime import datetime, timezone
from decimal import Decimal
import json

from pullback_detector.models import PullbackSignal, Tick
from pullback_detector.persistence import EventStore


def test_event_store_persists_raw_and_normalized_events(tmp_path):
    store = EventStore(tmp_path)
    received = datetime(2026, 8, 11, 9, 15, tzinfo=timezone.utc)
    payload = b"\x02\x10\x00\x01\x35\x05\x00\x00"
    tick = Tick(1333, received, Decimal("2450.5"), 4)
    store.raw_packet(received, payload, 2)
    store.tick(received, tick)

    raw = next((tmp_path / "raw").glob("*.jsonl")).read_text()
    normalized = next((tmp_path / "normalized").glob("*.jsonl")).read_text()
    assert json.loads(raw)["payload_hex"] == payload.hex()
    assert json.loads(normalized)["instrument_id"] == 1333


def test_event_store_persists_experimental_v1_signal(tmp_path):
    store = EventStore(tmp_path)
    timestamp = datetime(2026, 8, 11, 9, 20, tzinfo=timezone.utc)
    signal = PullbackSignal(
        instrument_id=1333,
        timestamp=timestamp,
        direction="LONG",
        impulse_start=Decimal("100"),
        impulse_end=Decimal("110"),
        retracement=0.3,
        trigger_price=Decimal("107"),
        invalidation_level=Decimal("106"),
        confidence_score=0.48,
        experimental_v1=True,
        reason="EXPERIMENTAL_V1_NOT_PROFITABILITY_VALIDATED",
    )
    store.signal(signal)
    saved = next((tmp_path / "signals").glob("*.jsonl")).read_text()
    record = json.loads(saved)
    assert record["experimental_v1"] is True
    assert record["trigger_price"] == "107"
    assert record["invalidation_level"] == "106"
