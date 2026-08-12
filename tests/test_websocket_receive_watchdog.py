import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pullback_detector import dhan
from pullback_detector.dhan import DhanWebSocketClient
from pullback_detector.models import Tick


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        item = self.messages.pop(0)
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            return await item()
        return item


class FakeConnect:
    def __init__(self, socket):
        self.socket = socket

    async def __aenter__(self):
        return self.socket

    async def __aexit__(self, exc_type, exc, tb):
        return False


def make_tick():
    return Tick(
        instrument_id=25,
        timestamp=datetime.now(timezone.utc),
        price=Decimal("100"),
        quantity=1,
        exchange_segment="NSE_EQ",
    )


@pytest.mark.asyncio
async def test_packet_arrives_before_timeout(monkeypatch):
    client = DhanWebSocketClient("id", "token", max_reconnects=0)
    client.RECEIVE_TIMEOUT_SECONDS = 0.05
    socket = FakeSocket([b"packet"])
    monkeypatch.setattr(dhan.websockets, "connect", lambda *args, **kwargs: FakeConnect(socket))
    monkeypatch.setattr(dhan, "parse_market_packets", lambda payload: [make_tick()])

    stream = client.stream([{"ExchangeSegment": "NSE_EQ", "SecurityId": "25"}])
    payload, tick, _ = await stream.__anext__()

    assert payload == b"packet"
    assert tick.instrument_id == 25
    assert client.websocket_state == "PACKET_RECEIVED"
    assert client.packets_received == 1
    assert client.packets_processed == 1

    await stream.aclose()


@pytest.mark.asyncio
async def test_receive_timeout_is_explicit_and_reconnects(monkeypatch):
    client = DhanWebSocketClient("id", "token", max_reconnects=1)
    client.RECEIVE_TIMEOUT_SECONDS = 0.01
    socket1 = FakeSocket([asyncio.TimeoutError()])
    socket2 = FakeSocket([asyncio.TimeoutError()])
    sockets = iter([socket1, socket2])
    monkeypatch.setattr(dhan.websockets, "connect", lambda *args, **kwargs: FakeConnect(next(sockets)))

    stream = client.stream([{"ExchangeSegment": "NSE_EQ", "SecurityId": "25"}])
    with pytest.raises(asyncio.TimeoutError):
        await stream.__anext__()

    assert client.receive_timeout_count == 2
    assert client.reconnects == 2
    assert client.websocket_state == "FAILED"
    await stream.aclose()


def test_watchdog_state_is_machine_readable():
    client = DhanWebSocketClient("id", "token")
    client.websocket_state = "CONNECTED_WAITING_FOR_PACKET"
    client.current_receive_started_at = datetime.now(timezone.utc)
    client._subscription_count = 22

    state = client.status_snapshot()

    assert state["websocket_state"] == "CONNECTED_WAITING_FOR_PACKET"
    assert state["subscription_count"] == 22
    assert state["data_flow_status"] == "WAITING_FOR_PACKET"
    assert state["current_receive_duration_ms"] is not None
