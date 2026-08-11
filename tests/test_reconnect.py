import struct

import pytest

from pullback_detector.dhan import DhanWebSocketClient


class FakeSocket:
    def __init__(self, packets, fail_enter=False):
        self.packets = packets
        self.fail_enter = fail_enter

    async def __aenter__(self):
        if self.fail_enter:
            raise ConnectionError("simulated disconnect")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, _message):
        return None

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for packet in self.packets:
            yield packet
        raise ConnectionError("simulated socket drop")


class FakeConnectFactory:
    def __init__(self, packet):
        self.calls = 0
        self.packet = packet

    def __call__(self, *_args, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            return FakeSocket([], fail_enter=True)
        return FakeSocket([self.packet])


@pytest.mark.asyncio
async def test_reconnects_after_connection_failure(monkeypatch):
    packet = struct.pack("<BhBi", 2, 16, 1, 1333) + struct.pack("<fi", 100.0, 1786430000)
    factory = FakeConnectFactory(packet)
    monkeypatch.setattr("pullback_detector.dhan.websockets.connect", factory)

    client = DhanWebSocketClient("id", "token", max_reconnects=2)
    stream = client.stream([{"ExchangeSegment": "NSE_EQ", "SecurityId": "1333"}])
    raw, tick, _received = await stream.__anext__()
    await stream.aclose()

    assert raw == packet
    assert tick is not None
    assert client.reconnects == 1
    assert factory.calls == 2
