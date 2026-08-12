import asyncio
from datetime import datetime, timezone

import pytest

from pullback_detector import dhan
from pullback_detector.dhan import DhanWebSocketClient


class FakeSocket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.closed = False

    async def send(self, _message):
        return None

    async def recv(self):
        value = next(self.messages)
        if isinstance(value, BaseException):
            raise value
        return value

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.closed = True


class FakeConnect:
    def __init__(self, socket):
        self.socket = socket

    def __call__(self, *_args, **_kwargs):
        return self.socket


@pytest.mark.asyncio
async def test_packet_arrives_before_timeout(monkeypatch):
    socket = FakeSocket([b"not-a-valid-dhan-frame"])
    monkeypatch.setattr(dhan.websockets, "connect", FakeConnect(socket))
    monkeypatch.setattr(DhanWebSocketClient, "RECEIVE_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(dhan, "parse_market_packets", lambda _payload: [])

    client = DhanWebSocketClient("client", "token", max_reconnects=0)
    item = await client.stream([{"ExchangeSegment": "NSE_EQ", "SecurityId": "1"}]).__anext__()

    assert item[0] == b"not-a-valid-dhan-frame"
    assert client.packets_received == 1
    assert client.websocket_state == "CONNECTED_WAITING_FOR_PACKET"


@pytest.mark.asyncio
async def test_receive_timeout_is_explicit_and_does_not_silently_block(monkeypatch):
    socket = FakeSocket([])
    monkeypatch.setattr(dhan.websockets, "connect", FakeConnect(socket))
    monkeypatch.setattr(DhanWebSocketClient, "RECEIVE_TIMEOUT_SECONDS", 0.01)

    client = DhanWebSocketClient("client", "token", max_reconnects=0)
    stream = client.stream([{"ExchangeSegment": "NSE_EQ", "SecurityId": "1"}])
    item = await asyncio.wait_for(stream.__anext__(), timeout=0.25)

    assert item[0] == b""
    assert item[1] is None
    assert client.receive_timeout_count == 1
    assert client.websocket_state == "RECEIVE_TIMEOUT"
    assert client.last_receive_error == "RECEIVE_TIMEOUT"


@pytest.mark.asyncio
async def test_cancelled_receive_is_not_swallowed(monkeypatch):
    async def never_connect(*_args, **_kwargs):
        await asyncio.sleep(60)

    monkeypatch.setattr(dhan.websockets, "connect", never_connect)
    client = DhanWebSocketClient("client", "token")
    task = asyncio.create_task(client.stream([{"ExchangeSegment": "NSE_EQ", "SecurityId": "1"}]).__anext__())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.websocket_state == "DISCONNECTED"
