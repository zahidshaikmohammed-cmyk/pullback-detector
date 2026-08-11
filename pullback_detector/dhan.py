import json
import logging
from collections.abc import AsyncIterator
from urllib.parse import urlencode

import websockets

from .dhan_protocol import parse_market_packet
from .models import Tick

logger = logging.getLogger(__name__)


class DhanWebSocketClient:
    """DhanHQ v2 live-feed adapter using the documented binary response protocol."""

    def __init__(self, client_id: str, access_token: str, ws_url: str = "wss://api-feed.dhan.co"):
        self.client_id = client_id
        self.access_token = access_token
        self.ws_url = ws_url

    def _url(self) -> str:
        if not self.client_id or not self.access_token:
            raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are required for live ingestion")
        return f"{self.ws_url}?{urlencode({'version': '2', 'token': self.access_token, 'clientId': self.client_id, 'authType': '2'})}"

    @staticmethod
    def subscription_message(instruments: list[dict], request_code: int = 15) -> str:
        if not 1 <= len(instruments) <= 100:
            raise ValueError("Dhan accepts at most 100 instruments per subscription message")
        return json.dumps({
            "RequestCode": request_code,
            "InstrumentCount": len(instruments),
            "InstrumentList": instruments,
        })

    async def stream(self, subscriptions: list[dict], request_code: int = 15) -> AsyncIterator[Tick]:
        if not subscriptions:
            raise ValueError("subscriptions cannot be empty")
        async with websockets.connect(self._url(), ping_interval=None) as socket:
            for offset in range(0, len(subscriptions), 100):
                await socket.send(self.subscription_message(subscriptions[offset:offset + 100], request_code))
            logger.info("connected to Dhan v2 feed; subscriptions=%d", len(subscriptions))
            async for message in socket:
                if not isinstance(message, bytes):
                    logger.warning("ignoring non-binary Dhan feed message")
                    continue
                tick = parse_market_packet(message)
                if tick is not None:
                    yield tick
