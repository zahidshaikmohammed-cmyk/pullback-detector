import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal

from .models import Tick

logger = logging.getLogger(__name__)


class DhanWebSocketClient:
    """Dhan v2 feed adapter. Credentials are supplied at runtime, never persisted."""

    def __init__(self, client_id: str, access_token: str, ws_url: str):
        self.client_id = client_id
        self.access_token = access_token
        self.ws_url = ws_url

    def _headers(self) -> dict[str, str]:
        if not self.client_id or not self.access_token:
            raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are required for live ingestion")
        return {"access-token": self.access_token, "client-id": self.client_id}

    @staticmethod
    def parse_tick(payload: str | bytes) -> Tick:
        data = json.loads(payload)
        # Adapter expects a normalized Dhan message shape at the boundary.
        return Tick(
            instrument_id=int(data["security_id"]),
            timestamp=datetime.fromtimestamp(float(data["timestamp"]), tz=timezone.utc),
            price=Decimal(str(data["price"])),
            quantity=int(data.get("quantity", 0)),
        )

    async def stream(self, subscriptions: list[dict]) -> AsyncIterator[Tick]:
        """Connect and yield normalized ticks. Network execution is intentionally isolated here."""
        import websockets

        headers = self._headers()
        logger.info("connecting to Dhan feed with %d subscriptions", len(subscriptions))
        async with websockets.connect(self.ws_url, additional_headers=headers) as socket:
            await socket.send(json.dumps({"action": "subscribe", "instruments": subscriptions}))
            async for message in socket:
                yield self.parse_tick(message)
