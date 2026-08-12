import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from urllib.parse import urlencode

import websockets

from .dhan_protocol import parse_market_packets
from .models import Tick

logger = logging.getLogger(__name__)


class DhanWebSocketClient:
    """DhanHQ v2 live-feed adapter with bounded receive and reconnect control."""

    ACTIVE_CLIENT = None
    RECEIVE_TIMEOUT_SECONDS = 15.0

    def __init__(self, client_id: str, access_token: str, ws_url: str = "wss://api-feed.dhan.co", max_reconnects: int = 5):
        self.client_id = client_id
        self.access_token = access_token
        self.ws_url = ws_url
        self.max_reconnects = max_reconnects
        self.reconnects = 0
        self.websocket_state = "DISCONNECTED"
        self.connected_at: datetime | None = None
        self.last_packet_received_at: datetime | None = None
        self.last_packet_processed_at: datetime | None = None
        self.packets_received = 0
        self.packets_processed = 0
        self.receive_timeout_count = 0
        self.last_receive_error: str | None = None
        self.current_receive_started_at: datetime | None = None
        self._subscription_count = 0
        DhanWebSocketClient.ACTIVE_CLIENT = self

    def _url(self) -> str:
        if not self.client_id or not self.access_token:
            raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are required for live ingestion")
        return f"{self.ws_url}?{urlencode({'version': '2', 'token': self.access_token, 'clientId': self.client_id, 'authType': '2'})}"

    @staticmethod
    def subscription_message(instruments: list[dict], request_code: int = 17) -> str:
        if not 1 <= len(instruments) <= 100:
            raise ValueError("Dhan accepts 1-100 instruments per subscription message")
        return json.dumps({"RequestCode": request_code, "InstrumentCount": len(instruments), "InstrumentList": instruments})

    def status_snapshot(self) -> dict:
        now = datetime.now(timezone.utc)
        waiting_seconds = None
        if self.current_receive_started_at is not None:
            waiting_seconds = max(0.0, (now - self.current_receive_started_at).total_seconds())
        last_packet_age = None
        if self.last_packet_received_at is not None:
            last_packet_age = max(0.0, (now - self.last_packet_received_at).total_seconds())
        return {
            "websocket_state": self.websocket_state,
            "connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "last_packet_received_at": self.last_packet_received_at.isoformat() if self.last_packet_received_at else None,
            "last_packet_processed_at": self.last_packet_processed_at.isoformat() if self.last_packet_processed_at else None,
            "packets_received": self.packets_received,
            "packets_processed": self.packets_processed,
            "receive_timeout_count": self.receive_timeout_count,
            "reconnect_count": self.reconnects,
            "last_receive_error": self.last_receive_error,
            "current_receive_duration_ms": round(waiting_seconds * 1000, 3) if waiting_seconds is not None else None,
            "seconds_since_last_packet": last_packet_age,
            "subscription_count": self._subscription_count,
            "data_flow_status": "LIVE" if last_packet_age is not None and last_packet_age <= 60 else "WAITING_FOR_PACKET",
        }

    async def stream(self, subscriptions: list[dict], request_code: int = 17) -> AsyncIterator[tuple[bytes, Tick | None, datetime]]:
        """Yield decoded Dhan packets; never leave socket.recv blocked indefinitely."""
        if not subscriptions:
            raise ValueError("subscriptions cannot be empty")
        attempts = 0
        self._subscription_count = len(subscriptions)
        while True:
            try:
                self.websocket_state = "CONNECTING"
                async with websockets.connect(self._url(), ping_interval=20, ping_timeout=20) as socket:
                    for offset in range(0, len(subscriptions), 100):
                        await socket.send(self.subscription_message(subscriptions[offset:offset + 100], request_code))
                    self.connected_at = datetime.now(timezone.utc)
                    self.websocket_state = "CONNECTED_WAITING_FOR_PACKET"
                    self.last_receive_error = None
                    logger.info("WEBSOCKET_CONNECTED subscriptions=%d", len(subscriptions))
                    attempts = 0
                    while True:
                        self.websocket_state = "CONNECTED_WAITING_FOR_PACKET"
                        self.current_receive_started_at = datetime.now(timezone.utc)
                        logger.info("WEBSOCKET_WAITING_FOR_PACKET timeout_seconds=%.1f", self.RECEIVE_TIMEOUT_SECONDS)
                        try:
                            message = await asyncio.wait_for(socket.recv(), timeout=self.RECEIVE_TIMEOUT_SECONDS)
                        except asyncio.TimeoutError as exc:
                            self.receive_timeout_count += 1
                            self.last_receive_error = "RECEIVE_TIMEOUT"
                            self.websocket_state = "RECEIVE_TIMEOUT"
                            logger.warning("WEBSOCKET_RECEIVE_TIMEOUT timeout_seconds=%.1f count=%d", self.RECEIVE_TIMEOUT_SECONDS, self.receive_timeout_count)
                            raise exc
                        finally:
                            self.current_receive_started_at = None

                        received_at = datetime.now(timezone.utc)
                        self.last_packet_received_at = received_at
                        self.packets_received += 1
                        self.websocket_state = "PACKET_RECEIVED"
                        logger.info("WEBSOCKET_PACKET_RECEIVED packets=%d", self.packets_received)
                        if not isinstance(message, bytes):
                            logger.warning("ignoring non-binary Dhan feed message")
                            continue
                        try:
                            packets = parse_market_packets(message)
                        except ValueError:
                            self.packets_processed += 1
                            self.last_packet_processed_at = received_at
                            logger.exception("malformed Dhan packet frame received")
                            yield message, None, received_at
                            continue
                        for tick in packets:
                            self.packets_processed += 1
                            self.last_packet_processed_at = received_at
                            yield message, tick, received_at
            except asyncio.CancelledError:
                self.websocket_state = "DISCONNECTED"
                raise
            except Exception as exc:
                self.current_receive_started_at = None
                attempts += 1
                self.reconnects += 1
                self.websocket_state = "RECONNECTING"
                self.last_receive_error = str(exc)
                logger.warning("WEBSOCKET_RECONNECTING reason=%s reconnect=%d", exc, self.reconnects)
                if attempts > self.max_reconnects:
                    self.websocket_state = "FAILED"
                    logger.exception("Dhan feed failed after %d reconnect attempts", self.max_reconnects)
                    raise
                delay = min(30.0, 2 ** (attempts - 1))
                await asyncio.sleep(delay)
                logger.info("WEBSOCKET_RECONNECTED attempt=%d subscriptions=%d", attempts, len(subscriptions))
