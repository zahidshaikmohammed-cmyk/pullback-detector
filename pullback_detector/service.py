import asyncio
import logging

from .alerts import WebhookAlertSink
from .candles import CandleAggregator
from .config import Settings
from .dhan import DhanWebSocketClient
from .detector import PullbackDetector
from .instruments import InstrumentUniverse

logger = logging.getLogger(__name__)


async def run_live(settings: Settings) -> None:
    universe = InstrumentUniverse.from_csv(settings.instrument_file)
    instruments = universe.liquid()
    if not instruments:
        raise RuntimeError("instrument universe is empty; add verified Dhan mappings before live operation")

    subscriptions = [
        {"ExchangeSegment": item.exchange_segment, "SecurityId": str(item.security_id)}
        for item in instruments
    ]
    feed = DhanWebSocketClient(settings.dhan_client_id, settings.dhan_access_token, settings.dhan_ws_url)
    aggregators = {}
    detectors = {}
    alert_sink = WebhookAlertSink(settings.alert_webhook_url, settings.alert_cooldown_seconds)

    while True:
        try:
            async for tick in feed.stream(subscriptions, request_code=15):
                aggregator = aggregators.setdefault(tick.instrument_id, CandleAggregator(settings.candle_interval_seconds))
                detector = detectors.setdefault(
                    tick.instrument_id,
                    PullbackDetector(
                        settings.pullback_lookback_bars,
                        settings.pullback_min_retrace,
                        settings.pullback_max_retrace,
                        settings.pullback_min_trend_strength,
                    ),
                )
                candle = aggregator.update(tick)
                signal = detector.update(candle)
                if signal:
                    logger.info("pullback signal: %s", signal)
                    alert_sink.publish(signal)
        except Exception:
            logger.exception("live feed stopped; retrying in 5 seconds")
            await asyncio.sleep(5)
