import struct

import pytest

from pullback_detector.dhan_protocol import parse_market_packet


def test_parse_dhan_ticker_packet():
    header = struct.pack("<BhBi", 2, 16, 1, 1333)
    payload = header + struct.pack("<fi", 2450.5, 1786430000)
    tick = parse_market_packet(payload)
    assert tick is not None
    assert tick.instrument_id == 1333
    assert float(tick.price) == 2450.5
    assert tick.feed_response_code == 2


def test_parse_dhan_quote_packet_with_quantity_and_volume():
    values = (2450.5, 12, 1786430000, 2449.0, 123456, 100, 120, 2400.0, 2300.0, 2500.0, 2200.0)
    body = struct.pack("<fhifiiiffff", *values)
    # 8-byte header + 42-byte Quote body = 50-byte packet.
    payload = struct.pack("<BhBi", 4, 50, 1, 1333) + body
    tick = parse_market_packet(payload)
    assert tick is not None
    assert tick.instrument_id == 1333
    assert tick.quantity == 12
    assert tick.cumulative_volume == 123456


def test_malformed_length_is_rejected():
    payload = struct.pack("<BhBi", 2, 99, 1, 1333) + struct.pack("<fi", 100.0, 1786430000)
    with pytest.raises(ValueError, match="length mismatch"):
        parse_market_packet(payload)


def test_truncated_ticker_is_rejected():
    payload = struct.pack("<BhBi", 2, 10, 1, 1333) + b"\x00"
    with pytest.raises(ValueError):
        parse_market_packet(payload)


def test_control_packet_is_ignored():
    payload = struct.pack("<BhBi", 6, 16, 1, 1333) + b"\x00" * 8
    assert parse_market_packet(payload) is None
