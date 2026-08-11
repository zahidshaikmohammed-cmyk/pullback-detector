"""DhanHQ v2 binary market-feed decoder."""

import struct
from datetime import datetime, timezone
from decimal import Decimal

from .models import Tick

HEADER = struct.Struct("<B h B i")
TICKER = struct.Struct("<f i")
QUOTE = struct.Struct("<f h i f i i i f f f f")


def parse_market_packet(payload: bytes) -> Tick | None:
    if len(payload) < HEADER.size:
        raise ValueError("Dhan packet shorter than 8-byte header")
    response_code, _length, _exchange_segment, security_id = HEADER.unpack_from(payload)
    if response_code != 2:
        return None
    if len(payload) < HEADER.size + TICKER.size:
        raise ValueError("Dhan ticker packet is truncated")
    price, epoch = TICKER.unpack_from(payload, HEADER.size)
    return Tick(
        instrument_id=security_id,
        timestamp=datetime.fromtimestamp(epoch, tz=timezone.utc),
        price=Decimal(str(price)),
        quantity=0,
    )


def parse_quote_packet(payload: bytes) -> Tick | None:
    if len(payload) < HEADER.size:
        raise ValueError("Dhan packet shorter than 8-byte header")
    response_code, _length, _exchange_segment, security_id = HEADER.unpack_from(payload)
    if response_code != 4:
        return None
    if len(payload) < HEADER.size + QUOTE.size:
        raise ValueError("Dhan quote packet is truncated")
    price, quantity, epoch, *_ = QUOTE.unpack_from(payload, HEADER.size)
    return Tick(
        instrument_id=security_id,
        timestamp=datetime.fromtimestamp(epoch, tz=timezone.utc),
        price=Decimal(str(price)),
        quantity=int(quantity),
    )
