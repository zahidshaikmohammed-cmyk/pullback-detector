import json
import struct
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.candles import CandleAggregator
from pullback_detector.dhan import DhanWebSocketClient
from pullback_detector.dhan_protocol import parse_market_packet, parse_market_packets
from pullback_detector.health import ConnectivityHealth
from pullback_detector.market_context import MarketContextEngine
from pullback_detector.models import Tick
from pullback_detector.validation import normalize_live_tick_clock, validate_tick


def test_exact_dhan_five_thirty_clock_skew_is_normalized_once():
    received=datetime(2026,8,12,5,31,39,tzinfo=timezone.utc);source=received+timedelta(hours=5,minutes=30);tick=Tick(25,source,Decimal("25000"),1,"IDX_I");normalized,skew=normalize_live_tick_clock(tick,received);assert skew==19800 and normalized.source_timestamp==source and normalized.timestamp==received and normalized.timestamp.tzinfo==timezone.utc;assert validate_tick(normalized,received) is None


def test_other_future_clock_skew_is_quarantined_not_normalized():
    received=datetime(2026,8,12,5,31,39,tzinfo=timezone.utc);source=received+timedelta(hours=2);tick=Tick(25,source,Decimal("25000"),1,"IDX_I");normalized,skew=normalize_live_tick_clock(tick,received);assert skew is None
    try:validate_tick(normalized,received)
    except ValueError as exc:assert "future" in str(exc)
    else:raise AssertionError("unexpected future timestamp was accepted")


def test_none_chop_score_never_enters_numeric_comparison():
    ctx=MarketContextEngine(1);ctx._chop=lambda bars:{"score":None,"state":"INSUFFICIENT_DATA"};ctx.update_tick(Tick(1,datetime.now(timezone.utc),Decimal("100"),1));snap=ctx.snapshot();assert snap["trend_stability"]=="INSUFFICIENT_DATA" and snap["chop_score"] is None


def test_none_atr_vwap_volume_are_insufficient_data():
    ctx=MarketContextEngine(1);ctx._atr=staticmethod(lambda bars,period=14:None);ctx._vwap=lambda bars:{"state":"INSUFFICIENT_DATA","distance_atr":None,"slope":None,"crosses":None,"time_above":None,"time_below":None,"vwap":None};ctx._volume=staticmethod(lambda bars:{"state":"INSUFFICIENT_DATA","relative_volume":None,"trend":"INSUFFICIENT_DATA"});ctx.update_tick(Tick(1,datetime.now(timezone.utc),Decimal("100"),1));snap=ctx.snapshot();assert snap["volatility_state"]=="INSUFFICIENT_DATA" and snap["vwap_state"]=="INSUFFICIENT_DATA" and snap["relative_volume"] is None


def test_one_bad_instrument_context_does_not_raise():
    ctx=MarketContextEngine(1);ctx._rebuild=lambda:(_ for _ in ()).throw(RuntimeError("synthetic calculation failure"));ctx.update_tick(Tick(1,datetime.now(timezone.utc),Decimal("100"),1));assert ctx.snapshot()["data_freshness"]=="CALCULATION_ERROR" and "synthetic calculation failure" in ctx.snapshot()["context_error"]


def test_benchmark_idx_i_tick_decodes_from_dhan_binary_packet():
    payload=struct.pack("<BhBifi",2,16,0,25,25000.0,int(datetime(2026,8,12,5,31,39,tzinfo=timezone.utc).timestamp()));tick=parse_market_packet(payload);assert tick is not None and tick.exchange_segment=="IDX_I" and tick.instrument_id==25


def test_concatenated_dhan_websocket_frame_is_split_into_packets():
    epoch=int(datetime(2026,8,12,5,31,39,tzinfo=timezone.utc).timestamp());p1=struct.pack("<BhBifi",2,16,1,1594,1169.0,epoch);p2=struct.pack("<BhBifi",2,16,0,13,24298.0,epoch);ticks=parse_market_packets(p1+p2);assert len(ticks)==2;assert ticks[0].instrument_id==1594 and ticks[1].instrument_id==13


def test_twenty_two_subscriptions_are_encoded_without_loss():
    subscriptions=[{"ExchangeSegment":"NSE_EQ","SecurityId":str(i)} for i in range(20)]+[{"ExchangeSegment":"IDX_I","SecurityId":"25"},{"ExchangeSegment":"IDX_I","SecurityId":"13"}];message=json.loads(DhanWebSocketClient.subscription_message(subscriptions,17));assert message["InstrumentCount"]==22 and len(message["InstrumentList"])==22


def test_stale_feed_state_is_authoritative():
    now=datetime.now(timezone.utc);health=ConnectivityHealth();tick=Tick(1,now-timedelta(seconds=120),Decimal("100"),1);health.record_tick(tick,now-timedelta(seconds=120));report=health.report(now,22);assert report["feed_state"]=="FEED_STALE" and report["dhan_connection_status"]=="stale" and report["data_age_seconds"]["1"]>=120


def test_completed_one_minute_and_five_minute_boundaries_are_utc_aware():
    agg1=CandleAggregator(60);agg5=CandleAggregator(300);ts=datetime(2026,8,12,5,31,42,tzinfo=timezone.utc);tick=Tick(1,ts,Decimal("100"),1);bar1=agg1.update(tick);bar5=agg5.update(tick);assert bar1.start.tzinfo==timezone.utc and bar1.end-bar1.start==timedelta(minutes=1);assert bar5.start.tzinfo==timezone.utc and bar5.end-bar5.start==timedelta(minutes=5)
