import asyncio, json, logging
from dataclasses import replace
from datetime import datetime, time, timezone
from pathlib import Path
from .candles import CandleAggregator
from .config import Settings
from .dhan import DhanWebSocketClient
from .dhan_http import DhanMarketQuote
from .detector import PullbackDetector
from .health import ConnectivityHealth
from .lifecycle import PullbackLifecycleEngine
from .market_context import MarketContextEngine, benchmark_alignment
from .persistence import EventStore
from .universe import InstrumentUniverse
from .validation import PacketDeduplicator, normalize_live_tick_clock, validate_tick_detailed
logger=logging.getLogger(__name__)
LIVE_ANATOMY:dict[int,dict]={}

def _safe_store(health,operation,*args):
    try:return getattr(operation[0],operation[1])(*args)
    except Exception as exc:health.persistence_errors+=1;logger.exception("PERSISTENCE_ERROR operation=%s error=%s",operation[1],exc);return None

def _persist_anatomy(data_root,instrument_id,anatomy):
    root=Path(data_root)/"anatomy";root.mkdir(parents=True,exist_ok=True);target=root/f"{instrument_id}.json";tmp=target.with_suffix(".tmp")
    try:tmp.write_text(json.dumps(anatomy,default=str,separators=(",",":")),encoding="utf-8");tmp.replace(target)
    except OSError:logger.exception("failed to persist anatomy instrument_id=%s",instrument_id)

def _equity_context(contexts,benchmark_contexts,instrument_id):
    context=contexts.get(instrument_id);stock=dict(context.snapshot() if context else {});benchmarks={name:ctx.snapshot() for name,ctx in benchmark_contexts.items()};alignment=benchmark_alignment(stock,benchmarks);stock.update({"benchmark_context":benchmarks,"stock_vs_market_alignment":alignment["status"],"stock_vs_market_alignment_score":alignment["score"],"stock_vs_market_compared_timeframes":alignment["compared_timeframes"],"market_alignment":alignment["status"]});return stock

def _publish_anatomy(detectors,contexts,benchmark_contexts,instrument_id,data_root=None):
    detector=detectors.get(instrument_id)
    if detector is None:return
    try:
        anatomy=_equity_context(contexts,benchmark_contexts,instrument_id);anatomy.update(detector.last_state or detector.anatomy());anatomy["market_context"]=_equity_context(contexts,benchmark_contexts,instrument_id);anatomy["market_context_version"]="1.1";LIVE_ANATOMY[instrument_id]=anatomy
        if data_root is not None:_persist_anatomy(data_root,instrument_id,anatomy)
    except Exception as exc:logger.exception("anatomy publication failed instrument_id=%s",instrument_id);LIVE_ANATOMY[instrument_id]={"instrument_id":instrument_id,"data_freshness":"CALCULATION_ERROR","context_error":f"{type(exc).__name__}: {exc}"}

def _safe_context_tick(context,tick):
    try:context.update_tick(tick)
    except Exception:logger.exception("context tick update failed instrument_id=%s",context.instrument_id)

def _safe_context_candle(context,candle):
    try:context.update_candle(candle)
    except Exception:logger.exception("context candle update failed instrument_id=%s",context.instrument_id)

def _emit_v2_signal(detectors,contexts,benchmark_contexts,candle,store,lifecycle,data_root,health):
    detector=detectors.get(candle.instrument_id)
    if detector is None:return
    try:
        detector.set_market_context(_equity_context(contexts,benchmark_contexts,candle.instrument_id));signal=detector.update(candle);_publish_anatomy(detectors,contexts,benchmark_contexts,candle.instrument_id,data_root)
        if signal is None:return
        _safe_store(health,(store,"signal"),signal);setup=lifecycle.trigger(signal,candle);logger.info("EXPERIMENTAL_V2_PULLBACK_SIGNAL instrument_id=%s timestamp=%s direction=%s trigger=%s invalidation=%s health=%s classification=%s setup_id=%s",signal.instrument_id,signal.timestamp.isoformat(),signal.direction,signal.trigger_price,signal.invalidation_level,signal.health_score,signal.classification,setup.snapshot.signal_id if setup else "duplicate_or_cooldown")
    except Exception:logger.exception("V2 detector cycle failed instrument_id=%s; continuing other instruments",candle.instrument_id)

def _nse_cash_session_open(now):
    local=now.astimezone(__import__("zoneinfo").ZoneInfo("Asia/Kolkata"));return local.weekday()<5 and time(9,15)<=local.time()<time(15,30)

async def run_live(settings:Settings,duration_seconds:int=600)->dict:
    instruments=InstrumentUniverse.fetch();benchmarks=InstrumentUniverse.fetch_benchmarks();store=EventStore(settings.data_root);InstrumentUniverse.write_snapshot(instruments,Path(settings.data_root)/"universe.csv")
    if benchmarks:InstrumentUniverse.write_snapshot(benchmarks,Path(settings.data_root)/"benchmarks.csv")
    verifier=DhanMarketQuote(settings.dhan_client_id,settings.dhan_access_token)
    try:verifier.ltp(instruments);logger.info("verified %d NSE_EQ security IDs through Dhan market quote",len(instruments))
    except Exception as exc:logger.warning("NSE_EQ Market Quote verification unavailable; live WebSocket remains authoritative: %s",exc)
    if benchmarks:logger.info("benchmark LTP REST verification skipped; Dhan WebSocket is authoritative for NIFTY/BANKNIFTY and 429 cannot break live feed")
    subscriptions=[{"ExchangeSegment":i.exchange_segment,"SecurityId":str(i.security_id)} for i in instruments+benchmarks];client=DhanWebSocketClient(settings.dhan_client_id,settings.dhan_access_token,settings.dhan_ws_url,settings.max_reconnects);health=ConnectivityHealth();dedupe=PacketDeduplicator(settings.dedupe_capacity);one_min=CandleAggregator(60);five_min=CandleAggregator(300);lifecycle=PullbackLifecycleEngine(settings.data_root,target_1_multiple=settings.pullback_target_1_multiple,target_2_multiple=settings.pullback_target_2_multiple,cooldown_seconds=settings.pullback_cooldown_seconds,expiry_seconds=settings.pullback_expiry_seconds);registry={i.security_id:i for i in instruments+benchmarks};detectors={i.security_id:PullbackDetector(instrument_id=i.security_id,audit_root=settings.data_root) for i in instruments};contexts={i.security_id:MarketContextEngine(i.security_id) for i in instruments};benchmark_contexts={i.symbol:MarketContextEngine(i.security_id) for i in benchmarks};LIVE_ANATOMY.clear()
    persisted_1m=0;persisted_5m=0
    for instrument in instruments:
        c1=store.recent_candles(instrument.security_id,60,2500);c5=store.recent_candles(instrument.security_id,300,2500);persisted_1m+=len(c1);persisted_5m+=len(c5)
        for candle in c1:_safe_context_candle(contexts[instrument.security_id],candle)
        for candle in c5:_safe_context_candle(contexts[instrument.security_id],candle);detectors[instrument.security_id].history.append(candle);detectors[instrument.security_id]._seen_candles.add(candle.start)
        _publish_anatomy(detectors,contexts,benchmark_contexts,instrument.security_id,settings.data_root)
    for benchmark in benchmarks:
        for candle in store.recent_candles(benchmark.security_id,60,2500):_safe_context_candle(benchmark_contexts[benchmark.symbol],candle)
        for candle in store.recent_candles(benchmark.security_id,300,2500):_safe_context_candle(benchmark_contexts[benchmark.symbol],candle)
    logger.info("FEED_CONNECTED expected_subscriptions=%d",len(subscriptions));deadline=asyncio.get_running_loop().time()+duration_seconds
    async for payload,tick,received_at in client.stream(subscriptions,request_code=17):
        if asyncio.get_running_loop().time()>=deadline:break
        _safe_store(health,(store,"raw_packet"),received_at,payload,payload[0] if payload else None)
        if dedupe.seen(payload):health.duplicate_packets+=1;continue
        health.packets+=1
        if tick is None:health.malformed_packets+=1;continue
        instrument=registry.get(tick.instrument_id)
        if instrument is None:health.malformed_packets+=1;logger.warning("TICK_REJECTED reason=UNKNOWN_INSTRUMENT instrument_id=%s",tick.instrument_id);continue
        tick=replace(tick,symbol=instrument.symbol,instrument_type=instrument.instrument_type,source="DHAN",validation_status="RAW");normalized_tick,_=normalize_live_tick_clock(tick,received_at,settings.max_future_seconds);result=validate_tick_detailed(normalized_tick,received_at,settings.max_future_seconds,settings.max_tick_age_seconds)
        if not result.valid:health.malformed_packets+=1;logger.warning("TICK_REJECTED reason=%s instrument_id=%s actual=%s threshold=%s source_ts=%s receive_ts=%s",result.reason_code,tick.instrument_id,result.actual_value,result.threshold,tick.timestamp.isoformat(),received_at.isoformat());continue
        tick=replace(normalized_tick,validation_status="VALIDATED");health.record_tick(tick,received_at);_safe_store(health,(store,"tick"),received_at,tick);lifecycle.update_tick(tick)
        context=contexts.get(tick.instrument_id);benchmark_context=next((ctx for ctx in benchmark_contexts.values() if ctx.instrument_id==tick.instrument_id),None)
        if context:_safe_context_tick(context,tick)
        if benchmark_context:_safe_context_tick(benchmark_context,tick)
        one_min.update(tick);five_min.update(tick)
        for candle in one_min.flush(tick.timestamp):
            _safe_store(health,(store,"candle"),candle);health.candles_1m+=1;health.record_candle(candle);context=contexts.get(candle.instrument_id);benchmark_context=next((ctx for ctx in benchmark_contexts.values() if ctx.instrument_id==candle.instrument_id),None)
            if context:_safe_context_candle(context,candle);_publish_anatomy(detectors,contexts,benchmark_contexts,candle.instrument_id,settings.data_root)
            if benchmark_context:_safe_context_candle(benchmark_context,candle)
        for candle in five_min.flush(tick.timestamp):
            _safe_store(health,(store,"candle"),candle);health.candles_5m+=1;health.record_candle(candle);context=contexts.get(candle.instrument_id);benchmark_context=next((ctx for ctx in benchmark_contexts.values() if ctx.instrument_id==candle.instrument_id),None)
            if context:_safe_context_candle(context,candle)
            if benchmark_context:_safe_context_candle(benchmark_context,candle)
            lifecycle.update_candle(candle)
            if context:_emit_v2_signal(detectors,contexts,benchmark_contexts,candle,store,lifecycle,settings.data_root,health)
    now=datetime.now(timezone.utc)
    for candle in one_min.flush(now):
        _safe_store(health,(store,"candle"),candle);health.candles_1m+=1;health.record_candle(candle);context=contexts.get(candle.instrument_id);benchmark_context=next((ctx for ctx in benchmark_contexts.values() if ctx.instrument_id==candle.instrument_id),None)
        if context:_safe_context_candle(context,candle);_publish_anatomy(detectors,contexts,benchmark_contexts,candle.instrument_id,settings.data_root)
        if benchmark_context:_safe_context_candle(benchmark_context,candle)
    for candle in five_min.flush(now):
        _safe_store(health,(store,"candle"),candle);health.candles_5m+=1;health.record_candle(candle);context=contexts.get(candle.instrument_id);benchmark_context=next((ctx for ctx in benchmark_contexts.values() if ctx.instrument_id==candle.instrument_id),None)
        if context:_safe_context_candle(context,candle)
        if benchmark_context:_safe_context_candle(benchmark_context,candle)
        lifecycle.update_candle(candle)
        if context:_emit_v2_signal(detectors,contexts,benchmark_contexts,candle,store,lifecycle,settings.data_root,health)
    for instrument in instruments:_publish_anatomy(detectors,contexts,benchmark_contexts,instrument.security_id,settings.data_root)
    now=datetime.now(timezone.utc);health.reconnects=client.reconnects;report=health.report(now,len(subscriptions),persisted_1m,persisted_5m);report.update({"real_dhan_packets_received":health.ticks>0,"verification":"Dhan official scrip master + Dhan market quote when available + live WebSocket","v2_detector":PullbackDetector.LABEL,"market_context_engine":"deterministic_v1","benchmark_instruments":[{"symbol":b.symbol,"security_id":b.security_id,"exchange_segment":b.exchange_segment} for b in benchmarks],"benchmark_count":len(benchmarks),"alerts_enabled":False,"active_setup_count":len(lifecycle.active),"closed_setup_count":len(lifecycle.closed),"aggregator_1m":one_min.state_snapshot(),"aggregator_5m":five_min.state_snapshot(),"configured_instruments":len(subscriptions),"resolved_instruments":len(registry),"subscribed_instruments":len(subscriptions),"producing_instruments":len(health.instruments_seen)})
    market_open=_nse_cash_session_open(now);report.update({"market_status":"OPEN" if market_open else "CLOSED","session_state":"NSE_CASH_SESSION_OPEN" if market_open else "NSE_CASH_SESSION_CLOSED"})
    if not market_open and health.ticks==0:report.update({"dhan_connection_status":"session_closed","no_live_data":True,"last_known_data_retained":True})
    _safe_store(health,(store,"health"),report)
    if health.ticks==0 and market_open:raise RuntimeError("LIVE_VALIDATION_FAILED: no real Dhan market packets were received during this open-session validation window")
    equity_seen=sum(1 for instrument_id in health.instruments_seen if instrument_id in detectors)
    if health.ticks>0 and equity_seen<settings.min_live_instruments:raise RuntimeError(f"LIVE_VALIDATION_FAILED: only {equity_seen} equity instruments produced packets; minimum={settings.min_live_instruments}")
    logger.info("LIVE_VALIDATION_SUCCESS report=%s",report);return report
