import asyncio
import json
import logging
from datetime import datetime, time, timezone
from pathlib import Path

from .candles import CandleAggregator
from .config import Settings
from .dhan import DhanWebSocketClient
from .dhan_http import DhanMarketQuote
from .detector import PullbackDetector
from .health import ConnectivityHealth
from .lifecycle import PullbackLifecycleEngine
from .market_context import MarketContextEngine
from .persistence import EventStore
from .universe import InstrumentUniverse
from .validation import PacketDeduplicator, normalize_live_tick_clock, validate_tick

logger = logging.getLogger(__name__)
LIVE_ANATOMY: dict[int, dict] = {}


def _persist_anatomy(data_root: str | Path, instrument_id: int, anatomy: dict) -> None:
    root=Path(data_root)/"anatomy"; root.mkdir(parents=True,exist_ok=True); target=root/f"{instrument_id}.json"; tmp=target.with_suffix(".tmp")
    try: tmp.write_text(json.dumps(anatomy,default=str,separators=(",", ":")),encoding="utf-8"); tmp.replace(target)
    except OSError: logger.exception("failed to persist anatomy instrument_id=%s",instrument_id)


def _session_metrics(context: MarketContextEngine, ts: datetime) -> dict:
    bars=context.candles_1m or context.candles_5m
    local_day=ts.astimezone(context.tz).date(); day=[b for b in bars if b.start.astimezone(context.tz).date()==local_day]
    if not day:return {"session_open":None,"session_high":None,"session_low":None}
    return {"session_open":str(day[0].open),"session_high":str(max(b.high for b in day)),"session_low":str(min(b.low for b in day))}


def _publish_anatomy(detectors, contexts, instrument_id: int, data_root: str | Path | None = None) -> None:
    detector=detectors.get(instrument_id); context=contexts.get(instrument_id)
    if detector is not None:
        anatomy=dict(context.snapshot() if context else {}); anatomy.update(_session_metrics(context, datetime.now(timezone.utc)) if context else {}); anatomy.update(detector.last_state or detector.anatomy())
        anatomy["market_context"]=context.snapshot() if context else {}; anatomy["market_context_version"]="1.0"
        LIVE_ANATOMY[instrument_id]=anatomy
        if data_root is not None:_persist_anatomy(data_root,instrument_id,anatomy)


def _emit_v2_signal(detectors,contexts,candle,store,lifecycle,data_root=None):
    detector=detectors.get(candle.instrument_id); context=contexts.get(candle.instrument_id)
    if detector is None:return
    if context:detector.set_market_context(context.snapshot())
    signal=detector.update(candle);_publish_anatomy(detectors,contexts,candle.instrument_id,data_root)
    if signal is None:return
    store.signal(signal);setup=lifecycle.trigger(signal,candle)
    logger.info("EXPERIMENTAL_V2_PULLBACK_SIGNAL instrument_id=%s timestamp=%s direction=%s trigger=%s invalidation=%s health=%s classification=%s setup_id=%s",signal.instrument_id,signal.timestamp.isoformat(),signal.direction,signal.trigger_price,signal.invalidation_level,signal.health_score,signal.classification,setup.snapshot.signal_id if setup else "duplicate_or_cooldown")


def _nse_cash_session_open(now: datetime) -> bool:
    local=now.astimezone(__import__("zoneinfo").ZoneInfo("Asia/Kolkata"));return local.weekday()<5 and time(9,15)<=local.time()<time(15,30)


async def run_live(settings: Settings,duration_seconds: int=600)->dict:
    instruments=InstrumentUniverse.fetch();store=EventStore(settings.data_root);InstrumentUniverse.write_snapshot(instruments,Path(settings.data_root)/"universe.csv")
    verifier=DhanMarketQuote(settings.dhan_client_id,settings.dhan_access_token);verifier.ltp(instruments);logger.info("verified %d NSE_EQ security IDs through Dhan market quote",len(instruments))
    subscriptions=[{"ExchangeSegment":i.exchange_segment,"SecurityId":str(i.security_id)} for i in instruments];client=DhanWebSocketClient(settings.dhan_client_id,settings.dhan_access_token,settings.dhan_ws_url,settings.max_reconnects)
    health=ConnectivityHealth();dedupe=PacketDeduplicator(settings.dedupe_capacity);one_min=CandleAggregator(60);five_min=CandleAggregator(300)
    lifecycle=PullbackLifecycleEngine(settings.data_root,target_1_multiple=settings.pullback_target_1_multiple,target_2_multiple=settings.pullback_target_2_multiple,cooldown_seconds=settings.pullback_cooldown_seconds,expiry_seconds=settings.pullback_expiry_seconds)
    detectors={i.security_id:PullbackDetector(instrument_id=i.security_id,audit_root=settings.data_root) for i in instruments};contexts={i.security_id:MarketContextEngine(i.security_id) for i in instruments}
    LIVE_ANATOMY.clear()
    for i in instruments:
        for c in store.recent_candles(i.security_id,60,2500):contexts[i.security_id].update_candle(c)
        for c in store.recent_candles(i.security_id,300,2500):
            contexts[i.security_id].update_candle(c);detectors[i.security_id].history.append(c);detectors[i.security_id]._seen_candles.add(c.start)
        _publish_anatomy(detectors,contexts,i.security_id,settings.data_root)
    deadline=asyncio.get_running_loop().time()+duration_seconds
    async for payload,tick,received_at in client.stream(subscriptions,request_code=17):
        if asyncio.get_running_loop().time()>=deadline:break
        response_code=payload[0] if payload else None;store.raw_packet(received_at,payload,response_code)
        if dedupe.seen(payload):health.duplicate_packets+=1;continue
        health.packets+=1
        if tick is None:continue
        normalized_tick,source_skew=normalize_live_tick_clock(tick,received_at,settings.max_future_seconds)
        if source_skew is not None:logger.warning("Dhan source clock skew %.1fs for security_id=%s; raw_source_ts=%s normalized_ts=%s receive_ts=%s",source_skew,tick.instrument_id,tick.timestamp.isoformat(),normalized_tick.timestamp.isoformat(),received_at.isoformat());tick=normalized_tick
        try:validate_tick(tick,received_at,settings.max_future_seconds,settings.max_tick_age_seconds)
        except ValueError as exc:health.malformed_packets+=1;logger.warning("discarding invalid tick security_id=%s: %s",tick.instrument_id,exc);continue
        health.record_tick(tick,received_at);store.tick(received_at,tick);lifecycle.update_tick(tick);context=contexts.get(tick.instrument_id)
        if context:context.update_tick(tick)
        for candle in one_min.flush(tick.timestamp):
            store.candle(candle);health.candles_1m+=1;context=contexts.get(candle.instrument_id)
            if context:context.update_candle(candle);_publish_anatomy(detectors,contexts,candle.instrument_id,settings.data_root)
        for candle in five_min.flush(tick.timestamp):
            store.candle(candle);health.candles_5m+=1;context=contexts.get(candle.instrument_id)
            if context:context.update_candle(candle)
            lifecycle.update_candle(candle);_emit_v2_signal(detectors,contexts,candle,store,lifecycle,settings.data_root)
        one_min.update(tick);five_min.update(tick);logger.info("LIVE_DHAN_TICK security_id=%s price=%s qty=%s source_ts=%s normalized_ts=%s receive_ts=%s",tick.instrument_id,tick.price,tick.quantity,(tick.source_timestamp or tick.timestamp).isoformat(),tick.timestamp.isoformat(),received_at.isoformat())
    now=datetime.now(timezone.utc)
    for candle in one_min.flush(now):
        store.candle(candle);health.candles_1m+=1;context=contexts.get(candle.instrument_id)
        if context:context.update_candle(candle);_publish_anatomy(detectors,contexts,candle.instrument_id,settings.data_root)
    for candle in five_min.flush(now):
        store.candle(candle);health.candles_5m+=1;context=contexts.get(candle.instrument_id)
        if context:context.update_candle(candle)
        lifecycle.update_candle(candle);_emit_v2_signal(detectors,contexts,candle,store,lifecycle,settings.data_root)
    for instrument in instruments:_publish_anatomy(detectors,contexts,instrument.security_id,settings.data_root)
    now=datetime.now(timezone.utc);health.reconnects=client.reconnects;report=health.report(now,len(instruments));report.update({"real_dhan_packets_received":health.ticks>0,"verification":"Dhan official scrip master + Dhan market quote + live WebSocket","v2_detector":PullbackDetector.LABEL,"market_context_engine":"deterministic_v1","alerts_enabled":False,"active_setup_count":len(lifecycle.active),"closed_setup_count":len(lifecycle.closed)})
    market_open=_nse_cash_session_open(now)
    if not market_open:
        report.update({"market_status":"CLOSED","session_state":"NSE_CASH_SESSION_CLOSED"})
        if health.ticks==0:report.update({"dhan_connection_status":"session_closed","no_live_data":True,"last_known_data_retained":True})
    else:report.update({"market_status":"OPEN","session_state":"NSE_CASH_SESSION_OPEN"})
    store.health(report)
    if health.ticks==0 and market_open:raise RuntimeError("LIVE_VALIDATION_FAILED: no real Dhan market packets were received during this open-session validation window")
    if health.ticks>0 and len(health.instruments_seen)<settings.min_live_instruments:raise RuntimeError(f"LIVE_VALIDATION_FAILED: only {len(health.instruments_seen)} instruments produced packets; minimum={settings.min_live_instruments}")
    logger.info("LIVE_VALIDATION_SUCCESS report=%s",report);return report
