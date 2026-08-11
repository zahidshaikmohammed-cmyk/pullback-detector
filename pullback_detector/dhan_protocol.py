"""Strict DhanHQ v2 binary market-feed decoder.

The decoder follows Dhan's documented little-endian packet layouts. It never
silently converts malformed packets into market events.
"""

import math
import struct
from datetime import datetime, timezone
from decimal import Decimal

from .models import Tick

HEADER = struct.Struct("<BhBi")
TICKER = struct.Struct("<fi")
QUOTE = struct.Struct("<fhiffiiff")
# Header + 43 bytes of quote payload = 51 bytes total.
QUOTE_PAYLOAD_SIZE = 43
TICKER_PAYLOAD_SIZE = 8

RESPONSE_TICKER = 2
RESPONSE_QUOTE = 4
RESPONSE_PREV_CLOSE = 6
RESPONSE_FULL = 8
RESPONSE_DISCONNECT = 50


def _header(payload: bytes) -> tuple[int, int, int, int]:
    if len(payload) < HEADER.size:
        raise ValueError("Dhan packet shorter than 8-byte header")
    response_code, message_length, exchange_segment, security_id = HEADER.unpack_from(payload)
    if message_length != len(payload):
        raise ValueError(f"Dhan packet length mismatch: header={message_length}, actual={len(payload)}")
    return response_code, message_length, exchange_segment, security_id


def _timestamp(epoch: int) -> datetime:
    if epoch <= 0:
        raise ValueError("Dhan packet contains invalid epoch timestamp")
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _price(value: float) -> Decimal:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"Dhan packet contains invalid price: {value!r}")
    return Decimal(str(value))


def parse_market_packet(payload: bytes) -> Tick | None:
    """Decode ticker, quote, and full packets; return None for control packets."""
    response_code, _, exchange_segment, security_id = _header(payload)

    if response_code == RESPONSE_TICKER:
        if len(payload) != HEADER.size + TICKER_PAYLOAD_SIZE:
            raise ValueError("Dhan ticker packet has invalid length")
        price, epoch = TICKER.unpack_from(payload, HEADER.size)
        return Tick(security_id, _timestamp(epoch), _price(price), 0, "NSE_EQ", None, response_code)

    if response_code == RESPONSE_QUOTE:
        if len(payload) != HEADER.size + QUOTE_PAYLOAD_SIZE:
            raise ValueError("Dhan quote packet has invalid length")
        price, quantity, epoch, _atp, volume, _sell, _buy, _open, _close, _high, _low = struct.unpack_from(
            "<fhiffiiff", payload, HEADER.size
        )
        if quantity < 0 or volume < 0:
            raise ValueError("Dhan quote packet contains negative quantity/volume")
        return Tick(
            security_id,
            _timestamp(epoch),
            _price(price),
            int(quantity),
            "NSE_EQ",
            int(volume),
            response_code,
        )

    if response_code == RESPONSE_FULL:
        # Full packet is intentionally not consumed by the candle path yet.
        # It remains a supported packet type at the transport boundary but
        # requires a separate depth model before being normalized.
        if len(payload) < 63:
            raise ValueError("Dhan full packet is truncated")
        return None

    if response_code in {1, RESPONSE_PREV_CLOSE, 5, 7, RESPONSE_DISCONNECT}:
        return None

    return None


def parse_quote_packet(payload: bytes) -> Tick | None:
    """Compatibility wrapper that accepts only Quote packets."""
    response_code, *_ = _header(payload)
    if response_code != RESPONSE_QUOTE:
        return None
    return parse_market_packet(payload)
