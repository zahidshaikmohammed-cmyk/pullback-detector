"""Strict DhanHQ v2 binary market-feed decoder.

Dhan documents little-endian binary packets with an 8-byte header. A websocket
message may contain multiple complete feed packets; parse_market_packets walks
those packets without treating a valid concatenated message as malformed.
"""
import math
import struct
from datetime import datetime, timezone
from decimal import Decimal

from .models import Tick

HEADER=struct.Struct("<BhBi");TICKER=struct.Struct("<fi");QUOTE=struct.Struct("<fhifiiiffff")
EXCHANGE_SEGMENTS={0:"IDX_I",1:"NSE_EQ",2:"NSE_FNO",3:"NSE_CURRENCY",4:"BSE_EQ",5:"MCX_COMM",7:"BSE_CURRENCY",8:"BSE_FNO"}
RESPONSE_TICKER=2;RESPONSE_QUOTE=4;RESPONSE_PREV_CLOSE=6;RESPONSE_FULL=8;RESPONSE_DISCONNECT=50


def _header(payload:bytes)->tuple[int,int,str,int]:
    if len(payload)<HEADER.size:raise ValueError("Dhan packet shorter than 8-byte header")
    response_code,message_length,exchange_segment,security_id=HEADER.unpack_from(payload)
    if message_length<HEADER.size or message_length>len(payload):raise ValueError(f"Dhan packet length mismatch: header={message_length}, actual={len(payload)}")
    if exchange_segment not in EXCHANGE_SEGMENTS:raise ValueError(f"Dhan packet contains unknown exchange segment: {exchange_segment}")
    return response_code,message_length,EXCHANGE_SEGMENTS[exchange_segment],security_id


def _timestamp(epoch:int)->datetime:
    if epoch<=0:raise ValueError("Dhan packet contains invalid epoch timestamp")
    return datetime.fromtimestamp(epoch,tz=timezone.utc)


def _price(value:float)->Decimal:
    if not math.isfinite(value) or value<=0:raise ValueError(f"Dhan packet contains invalid price: {value!r}")
    return Decimal(str(value))


def _parse_one(payload:bytes)->Tick|None:
    response_code,message_length,exchange_segment,security_id=_header(payload)
    if message_length!=len(payload):raise ValueError(f"Dhan packet length mismatch: header={message_length}, actual={len(payload)}")
    if response_code==RESPONSE_TICKER:
        if len(payload)!=HEADER.size+TICKER.size:raise ValueError("Dhan ticker packet has invalid length")
        price,epoch=TICKER.unpack_from(payload,HEADER.size);return Tick(security_id,_timestamp(epoch),_price(price),0,exchange_segment,None,response_code)
    if response_code==RESPONSE_QUOTE:
        if len(payload)!=HEADER.size+QUOTE.size:raise ValueError("Dhan quote packet has invalid length")
        price,quantity,epoch,_atp,volume,_sell,_buy,_open,_close,_high,_low=QUOTE.unpack_from(payload,HEADER.size)
        if quantity<0 or volume<0:raise ValueError("Dhan quote packet contains negative quantity/volume")
        return Tick(security_id,_timestamp(epoch),_price(price),int(quantity),exchange_segment,int(volume),response_code)
    if response_code==RESPONSE_FULL:
        if len(payload)<63:raise ValueError("Dhan full packet is truncated")
        price,quantity,epoch=struct.unpack_from("<fh i",payload,HEADER.size)
        if quantity<0:return None
        return Tick(security_id,_timestamp(epoch),_price(price),int(quantity),exchange_segment,None,response_code)
    if response_code in {1,RESPONSE_PREV_CLOSE,5,7,RESPONSE_DISCONNECT}:return None
    return None


def parse_market_packets(payload:bytes)->list[Tick|None]:
    """Decode every complete packet contained in one websocket binary frame."""
    packets=[];offset=0
    while offset<len(payload):
        if len(payload)-offset<HEADER.size:raise ValueError("Dhan message ends with truncated packet header")
        _,message_length,_,_= _header(payload[offset:])
        end=offset+message_length
        if end>len(payload):raise ValueError(f"Dhan concatenated message truncated: packet_end={end}, actual={len(payload)}")
        packets.append(_parse_one(payload[offset:end]));offset=end
    return packets


def parse_market_packet(payload:bytes)->Tick|None:
    """Decode exactly one Dhan packet; use parse_market_packets for frames."""
    if len(payload)<HEADER.size:raise ValueError("Dhan packet shorter than 8-byte header")
    _,message_length,_,_= _header(payload)
    if message_length!=len(payload):raise ValueError(f"Dhan packet length mismatch: header={message_length}, actual={len(payload)}")
    return _parse_one(payload)


def parse_quote_packet(payload:bytes)->Tick|None:
    response_code,*_= _header(payload)
    if response_code!=RESPONSE_QUOTE:return None
    return parse_market_packet(payload)
