import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from .candles import CandleAggregator
from .config import Settings
from .dhan import DhanWebSocketClient
from .dhan_http import DhanMarketQuote
from .detector import PullbackDetector
from .health import ConnectivityHealth
from .persistence import EventStore
from .universe import InstrumentUniverse
from .validation import PacketDeduplicator, normalize_live_tick_clock, validate_tick

logger = logging.getLogger(__name__)


def _emit_v1_signal(detectors: dict[int, PullbackDetector], candle, store: EventStore) -> None:
    detector = detectors.get(candle.instrument_id)
    if detector is None:
        return
    signal = detector.update(candle)
    if signal is None:
        return
    store.signal(signal)
    logger.info(
        "EXPERIMENTAL_V1_PULLBACK_SIGNAL instrument_id=%s timestamp=%s direction=%s trigger=%s invalidation=%s confidence=%.3f label=%s",
        signal.instrument_id,
        signal.timestamp.isoformat(),
        signal.direction,
        signal.trigger_price,
        signal.invalidation_level,
        signal.confidence_score,
        PullbackDetector.LABEL,
    )


async def run_live(settings: Settings, duration_seconds: int = 600) -> dict:
    """Run the live Dhan pipeline and feed accepted 5m candles into experimental V1 detection."""
    instruments = InstrumentUniverse.fetch()
    store = EventStore(settings.data_root)
    InstrumentUniverse.write_snapshot(instruments, Path(settings.data_root) / "universe.csv")

    verifier = DhanMarketQuote(settings.dhan_client_id, settings.dhan_access_token)
    verifier.ltp(instruments)
    logger.info("verified %d NSE_EQ security IDs through Dhan market quote", len(instruments))

    subscriptions = [{"ExchangeSegment": i.exchange_segment, "SecurityId": str(i.security_id)} for i in instruments]
    client = DhanWebSocketClient(settings.dhan_client_id, settings.dhan_access_token, settings.dhan_ws_url, settings.max_reconnects)
    health = ConnectivityHealth()
    dedupe = PacketDeduplicator(settings.dedupe_capacity)
    one_min = CandleAggregator(60)
    five_min = CandleAggregator(300)
    detectors = {
        instrument.security_id: PullbackDetector(
            lookback_bars=settings.pullback_lookback_bars,
            min_retrace=settings.pullback_min_retrace,
            max_retrace=settings.pullback_max_retrace,
            min_trend_strength=settings.pullback_min_trend_strength,
        )
        for instrument in instruments
    }
    deadline = asyncio.get_running_loop().time() + duration_seconds

    async for payload, tick, received_at in client.stream(subscriptions, request_code=17):
        if asyncio.get_running_loop().time() >= deadline:
            break
        response_code = payload[0] if payload else None
        store.raw_packet(received_at, payload, response_code)
        if dedupe.seen(payload):
            health.duplicate_packets += 1
            continue
        health.packets += 1
        if tick is None:
            continue

        normalized_tick, source_skew = normalize_live_tick_clock(
            tick, received_at, settings.max_future_seconds
        )
        if source_skew is not None:
            logger.warning(
                "Dhan source clock skew %.1fs for security_id=%s; raw_source_ts=%s normalized_ts=%s receive_ts=%s",
                source_skew,
                tick.instrument_id,
                tick.timestamp.isoformat(),
                normalized_tick.timestamp.isoformat(),
                received_at.isoformat(),
            )
            tick = normalized_tick

        try:
            validate_tick(tick, received_at, settings.max_future_seconds, settings.max_tick_age_seconds)
        except ValueError as exc:
            health.malformed_packets += 1
            logger.warning("discarding invalid tick security_id=%s: %s", tick.instrument_id, exc)
            continue

        health.record_tick(tick, received_at)
        store.tick(received_at, tick)

        for candle in one_min.flush(tick.timestamp):
            store.candle(candle)
            health.candles_1m += 1
        for candle in five_min.flush(tick.timestamp):
            store.candle(candle)
            health.candles_5m += 1
            _emit_v1_signal(detectors, candle, store)

        one_min.update(tick)
        five_min.update(tick)
        logger.info(
            "LIVE_DHAN_TICK security_id=%s price=%s qty=%s source_ts=%s normalized_ts=%s receive_ts=%s",
            tick.instrument_id,
            tick.price,
            tick.quantity,
            (tick.source_timestamp or tick.timestamp).isoformat(),
            tick.timestamp.isoformat(),
            received_at.isoformat(),
        )

    now = datetime.now(timezone.utc)
    for candle in one_min.flush(now):
        store.candle(candle)
        health.candles_1m += 1
    for candle in five_min.flush(now):
        store.candle(candle)
        health.candles_5m += 1
        _emit_v1_signal(detectors, candle, store)

    health.reconnects = client.reconnects
    report = health.report(now, len(instruments))
    report["real_dhan_packets_received"] = health.ticks > 0
    report["verification"] = "Dhan official scrip master + Dhan market quote + live WebSocket"
    report["v1_detector"] = PullbackDetector.LABEL
    report["alerts_enabled"] = False
    store.health(report)

    if health.ticks == 0:
        raise RuntimeError("LIVE_VALIDATION_FAILED: no real Dhan market packets were received during this session")
    if len(health.instruments_seen) < settings.min_live_instruments:
        raise RuntimeError(
            f"LIVE_VALIDATION_FAILED: only {len(health.instruments_seen)} instruments produced packets; minimum={settings.min_live_instruments}"
        )
    logger.info("LIVE_VALIDATION_SUCCESS report=%s", report)
    return report
