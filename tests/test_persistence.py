from datetime import datetime, timezone
from decimal import Decimal
import json

from pullback_detector.models import Tick
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
