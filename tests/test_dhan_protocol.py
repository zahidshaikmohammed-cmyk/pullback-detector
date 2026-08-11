import struct

from pullback_detector.dhan_protocol import parse_market_packet


def test_parse_dhan_ticker_packet():
    header = struct.pack("<B h B i", 2, 16, 1, 1333)
    payload = header + struct.pack("<f i", 2450.5, 1786430000)
    tick = parse_market_packet(payload)
    assert tick is not None
    assert tick.instrument_id == 1333
    assert float(tick.price) == 2450.5


def test_ignore_non_ticker_packet():
    payload = struct.pack("<B h B i", 6, 16, 1, 1333) + b"\x00" * 8
    assert parse_market_packet(payload) is None
