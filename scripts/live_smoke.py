import argparse
import asyncio

from pullback_detector.config import get_settings
from pullback_detector.dhan import DhanWebSocketClient


async def main(security_id: str, exchange_segment: str) -> None:
    settings = get_settings()
    client = DhanWebSocketClient(settings.dhan_client_id, settings.dhan_access_token, settings.dhan_ws_url)
    subscription = [{"ExchangeSegment": exchange_segment, "SecurityId": security_id}]
    async for tick in client.stream(subscription, request_code=15):
        print(f"REAL_DHAN_PACKET_RECEIVED security_id={tick.instrument_id} timestamp={tick.timestamp.isoformat()} price={tick.price}")
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("security_id")
    parser.add_argument("--exchange-segment", default="NSE_EQ")
    args = parser.parse_args()
    asyncio.run(main(args.security_id, args.exchange_segment))
