from datetime import datetime,timedelta,timezone
from decimal import Decimal
import json,struct
from pullback_detector.health import ConnectivityHealth
from pullback_detector.models import Tick
from pullback_detector.candles import CandleAggregator
from pullback_detector.dhan import DhanWebSocketClient
from pullback_detector.dhan_protocol import parse_market_packets


def test_concatenated_dhan_websocket_frame_is_split_into_packets():
    epoch=int(datetime(2026,8,12,5,31,39,tzinfo=timezone.utc).timestamp());p1=struct.pack("<BhBifi",2,16,1,1594,1169.0,epoch);p2=struct.pack("<BhBifi",2,16,0,13,24298.0,epoch);ticks=parse_market_packets(p1+p2);assert len(ticks)==2;assert ticks[0].instrument_id==1594 and ticks[1].instrument_id==13


def test_twenty_two_subscriptions_are_encoded_without_loss():
    subscriptions=[{"ExchangeSegment":"NSE_EQ","SecurityId":str(i)} for i in range(20)]+[{"ExchangeSegment":"IDX_I","SecurityId":"25"},{"ExchangeSegment":"IDX_I","SecurityId":"13"}];message=json.loads(DhanWebSocketClient.subscription_message(subscriptions,17));assert message["InstrumentCount"]==22 and len(message["InstrumentList"])==22


def test_stale_feed_state_is_authoritative():
    now=datetime.now(timezone.utc);health=ConnectivityHealth();tick=Tick(1,now-timedelta(seconds=120),Decimal("100"),1);health.record_tick(tick,now-timedelta(seconds=120));report=health.report(now,22,websocket_connected=True);assert report["feed_state"]=="STALE" and report["dhan_connection_status"]=="stale" and report["data_age_seconds"]["1"]>=120


def test_completed_one_minute_and_five_minute_boundaries_are_utc_aware():
    agg1=CandleAggregator(60);agg5=CandleAggregator(300);ts=datetime(2026,8,12,5,31,42,tzinfo=timezone.utc);tick=Tick(1,ts,Decimal("100"),1);bar1=agg1.update(tick);bar5=agg5.update(tick);assert bar1.start.tzinfo==timezone.utc and bar1.end-bar1.start==timedelta(minutes=1);assert bar5.start.tzinfo==timezone.utc and bar5.end-bar5.start==timedelta(minutes=5)
