import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from urllib.parse import urlencode

import websockets

from .dhan_protocol import parse_market_packet
from .models import Tick

logger = logging.getLogger(__name__)


class DhanWebSocketClient:
    """DhanHQ v2 live-feed adapter with bounded reconnect backoff."""

    def __init__(self, client_id: str, access_token: str, ws_url: str = "wss://api-feed.dhan.co", max_reconnects: int = 5):
        self.client_id = client_id
        self.access_token = access_token
        self.ws_url = ws_url
        self.max_reconnects = max_reconnects

    def _url(self) -> str:
        if not self.client_id or not self.access_token:
            raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are required for live ingestion")
        return f"{self.ws_url}?{urlencode({'version': '2', 'token': self.access_token, 'clientId': self.client_id, 'authType': '2'})}"

    @staticmethod
    def subscription_message(instruments: list[dict], request_code: int = 17) -> str:
        if not 1 <= len(instruments) <= 100:
            raise ValueError("Dhan accepts 1-100 instruments per subscription message")
        return json.dumps({
            "RequestCode": request_code,
            "InstrumentCount": len(instruments),
            "InstrumentList": instruments,
        })

    async def stream(self, subscriptions: list[dict], request_code: int = 17) -> AsyncIterator[tuple[bytes, Tick | None, datetime]]:
        """Yield raw binary packet, normalized tick/control result, and receipt time."""
        if not subscriptions:
            raise ValueError("subscriptions cannot be empty")
        reconnects = 0
        while True:
            try:
                async with websockets.connect(self._url(), ping_interval=20, ping_timeout=20) as socket:
                    for offset in range(0, len(subscriptions), 100):
                        await socket.send(self.subscription_message(subscriptions[offset:offset + 100], request_code))
                    logger.info("connected to Dhan v2 feed; subscriptions=%d", len(subscriptions))
                    reconnects = 0
                    async for message in socket:
                        received_at = datetime.now(timezone.utc)
                        if not isinstance(message, bytes):
                            logger.warning("ignoring non-binary Dhan feed message")
                            continue
                        try:
                            tick = parse_market_packet(message)
                        except ValueError:
                            logger.exception("malformed Dhan packet received")
                            yield message, None, received_at
                            continue
                        yield message, tick, received_at
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnects += 1
                if reconnects > self.max_reconnects:
                    logger.exception("Dhan feed failed after %d reconnect attempts", self.max_reconnects)
                    raise
                delay = min(30.0, 2 ** (reconnects - 1))
                logger.warning("Dhan feed disconnected: %s; reconnect=%d in %.1fs", exc, reconnects, delay)
                await asyncio.sleep(delay)
