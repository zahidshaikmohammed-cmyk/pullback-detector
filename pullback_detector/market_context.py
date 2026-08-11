"""Deterministic multi-timeframe market-context evidence engine.

This module does not generate trades. It converts accepted candles/ticks into
measurable context for Healthy Pullback V2. Missing history is represented as
INSUFFICIENT_DATA; no values are invented.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, time, timezone
from decimal import Decimal
from statistics import median
from typing import Iterable
from zoneinfo import ZoneInfo

from .models import Candle, Tick


DIRECTIONS = ("BULLISH", "BEARISH", "NEUTRAL", "INSUFFICIENT_DATA")


class MarketContextEngine:
    def __init__(self, instrument_id: int, timezone_name: str = "Asia/Kolkata", max_candles: int = 2500):
        self.instrument_id = instrument_id
        self.tz = ZoneInfo(timezone_name)
        self.max_candles = max_candles
        self.candles_1m: list[Candle] = []
        self.candles_5m: list[Candle] = []
        self.last_tick: Tick | None = None
        self.last_context: dict = {"instrument_id": instrument_id, "data_freshness": "NO_DATA"}

    def update_tick(self, tick: Tick) -> None:
        if tick.instrument_id == self.instrument_id:
            self.last_tick = tick
            self._rebuild()

    def update_candle(self, candle: Candle) -> None:
        if candle.instrument_id != self.instrument_id or not candle.complete:
            return
        target = self.candles_1m if candle.timeframe_seconds == 60 else self.candles_5m if candle.timeframe_seconds == 300 else None
        if target is None:
            return
        if target and candle.start < target[-1].start:
            return
        if target and candle.start == target[-1].start:
            target[-1] = candle
        else:
            target.append(candle)
        if len(target) > self.max_candles:
            del target[:-self.max_candles]
        self._rebuild()

    @staticmethod
    def _ema(values: list[float], period: int) -> float | None:
        if len(values) < period:
            return None
        k = 2.0 / (period + 1)
        value = sum(values[:period]) / period
        for x in values[period:]:
            value = x * k + value * (1-k)
        return value

    @staticmethod
    def _atr(bars: list[Candle], period: int = 14) -> float | None:
        if len(bars) < period:
            return None
        trs=[]
        for i,b in enumerate(bars):
            prev=float(bars[i-1].close) if i else float(b.close)
            trs.append(max(float(b.high-b.low), abs(float(b.high)-prev), abs(float(b.low)-prev)))
        return sum(trs[-period:])/period

    @staticmethod
    def _efficiency(bars: list[Candle]) -> float | None:
        if len(bars)<2:return None
        net=abs(float(bars[-1].close-bars[0].open))
        travel=sum(abs(float(b.close-b.open)) for b in bars)
        return min(1.0, net/travel) if travel else 0.0

    @staticmethod
    def _roc(bars: list[Candle], n: int) -> float | None:
        if len(bars)<=n:return None
        base=float(bars[-n-1].close)
        return (float(bars[-1].close)-base)/base if base else None

    @staticmethod
    def _slope(values: list[float], lookback: int = 5) -> float | None:
        if len(values)<=lookback:return None
        return (values[-1]-values[-1-lookback])/lookback

    @staticmethod
    def _atr_percentile(bars: list[Candle], period: int = 14) -> float | None:
        if len(bars)<period:return None
        vals=[]
        for i in range(period-1,len(bars)):
            vals.append(MarketContextEngine._atr(bars[:i+1],period))
        vals=[x for x in vals if x is not None]
        if not vals:return None
        return 100*sum(x<=vals[-1] for x in vals)/len(vals)

    @staticmethod
    def _swing_structure(bars: list[Candle]) -> tuple[str, float | None, float | None, float | None]:
        if len(bars)<9:return "INSUFFICIENT_DATA",None,None,None
        highs=[]; lows=[]
        for i in range(2,len(bars)-2):
            if bars[i].high>max(bars[i-2].high,bars[i-1].high,bars[i+1].high,bars[i+2].high): highs.append((bars[i].end,float(bars[i].high)))
            if bars[i].low<min(bars[i-2].low,bars[i-1].low,bars[i+1].low,bars[i+2].low): lows.append((bars[i].end,float(bars[i].low)))
        if len(highs)<2 or len(lows)<2:return "NEUTRAL",None,None,None
        hh=highs[-1][1]>highs[-2][1]; hl=lows[-1][1]>lows[-2][1]
        lh=highs[-1][1]<highs[-2][1]; ll=lows[-1][1]<lows[-2][1]
        if hh and hl:return "BULLISH_STRUCTURE",lows[-1][1],highs[-1][1],0.0
        if lh and ll:return "BEARISH_STRUCTURE",highs[-1][1],lows[-1][1],0.0
        return "NEUTRAL_STRUCTURE",lows[-1][1],highs[-1][1],0.0

    def _resample(self, bars: list[Candle], seconds: int) -> list[Candle]:
        if not bars:return []
        out=[]; bucket=None; state=None
        for b in bars:
            epoch=int(b.start.timestamp()); start=datetime.fromtimestamp(epoch-(epoch%seconds),tz=timezone.utc)
            if bucket!=start:
                if state: out.append(Candle(self.instrument_id,bucket,bucket.fromtimestamp(bucket.timestamp()+seconds,tz=timezone.utc),state["open"],state["high"],state["low"],state["close"],state["volume"],True,seconds))
                bucket=start; state={"open":b.open,"high":b.high,"low":b.low,"close":b.close,"volume":b.volume}
            else:
                state["high"]=max(state["high"],b.high); state["low"]=min(state["low"],b.low); state["close"]=b.close; state["volume"]+=b.volume
        if state:
            out.append(Candle(self.instrument_id,bucket,bucket.fromtimestamp(bucket.timestamp()+seconds,tz=timezone.utc),state["open"],state["high"],state["low"],state["close"],state["volume"],True,seconds))
        return out

    def _trend(self, bars: list[Candle], strong_threshold: float = 0.004) -> dict:
        if len(bars)<20:return {"direction":"INSUFFICIENT_DATA","score":None,"strength":None,"momentum":"INSUFFICIENT_DATA","structure":"INSUFFICIENT_DATA","volatility":"INSUFFICIENT_DATA","efficiency":None}
        closes=[float(b.close) for b in bars]; e20=self._ema(closes,20); e50=self._ema(closes,50); atr=self._atr(bars)
        structure,protected,reference,_=self._swing_structure(bars)
        roc=self._roc(bars,min(10,max(2,len(bars)//5))) or 0
        slope=self._slope(closes,5) or 0
        eff=self._efficiency(bars[-20:]) or 0
        bull=0; bear=0
        if e20 is not None and e50 is not None:
            bull+=20 if e20>e50 else 0; bear+=20 if e20<e50 else 0
        if slope>0: bull+=15
        elif slope<0: bear+=15
        if structure=="BULLISH_STRUCTURE": bull+=25
        elif structure=="BEARISH_STRUCTURE": bear+=25
        if roc>0: bull+=20*min(1,abs(roc)/strong_threshold)
        elif roc<0: bear+=20*min(1,abs(roc)/strong_threshold)
        if eff>=0.65:
            if roc>0: bull+=20
            elif roc<0: bear+=20
        score=max(bull,bear)
        direction="BULLISH" if bull>bear+10 else "BEARISH" if bear>bull+10 else "NEUTRAL"
        if score>=75: label="STRONG "+direction
        elif score>=55: label=direction
        else: label="NEUTRAL"
        mom="STRENGTHENING" if (slope*roc)>0 and abs(roc)>0 else "WEAKENING" if (slope*roc)<0 else "STABLE"
        vol_pct=self._atr_percentile(bars)
        vol="INSUFFICIENT_DATA" if vol_pct is None else "VERY LOW" if vol_pct<10 else "LOW" if vol_pct<30 else "NORMAL" if vol_pct<70 else "HIGH" if vol_pct<90 else "EXTREME"
        return {"direction":label,"score":int(round(score)),"strength":int(round(score)),"momentum":mom,"structure":structure,"volatility":vol,"efficiency":eff,"protected_level":protected,"reference_level":reference,"ema20":e20,"ema50":e50,"atr":atr,"roc":roc,"atr_percentile":vol_pct}

    def _vwap(self, bars:list[Candle]) -> dict:
        if not bars:return {"state":"INSUFFICIENT_DATA","distance_atr":None,"slope":None,"crosses":None,"time_above":None,"time_below":None}
        local_day=bars[-1].start.astimezone(self.tz).date(); day=[b for b in bars if b.start.astimezone(self.tz).date()==local_day]
        if not day:return {"state":"INSUFFICIENT_DATA","distance_atr":None,"slope":None,"crosses":None,"time_above":None,"time_below":None}
        den=sum(b.volume for b in day); vwap=float(sum(((b.high+b.low+b.close)/Decimal(3))*b.volume for b in day)/den) if den else None
        if vwap is None:return {"state":"INSUFFICIENT_DATA","distance_atr":None,"slope":None,"crosses":None,"time_above":None,"time_below":None}
        atr=self._atr(bars); price=float(self.last_tick.price) if self.last_tick else float(day[-1].close)
        crosses=sum((float(a.close)-vwap)*(float(b.close)-vwap)<0 for a,b in zip(day[-10:-1],day[-9:]))
        above=sum(float(b.close)>=vwap for b in day); below=len(day)-above
        slope=(vwap-float(((day[-2].high+day[-2].low+day[-2].close)/Decimal(3)))) if len(day)>1 else 0
        state="ABOVE_ACCEPTANCE" if price>vwap and above>=max(3,int(.7*len(day))) and slope>=0 else "BELOW_ACCEPTANCE" if price<vwap and below>=max(3,int(.7*len(day))) and slope<=0 else "TRANSITIONING"
        return {"state":state,"price":price,"vwap":vwap,"distance_atr":(price-vwap)/atr if atr else None,"slope":slope,"crosses":crosses,"time_above":above,"time_below":below}

    def _volume(self,bars:list[Candle])->dict:
        if len(bars)<10:return {"state":"INSUFFICIENT_DATA","relative_volume":None,"trend":"INSUFFICIENT_DATA"}
        vals=[b.volume for b in bars]; med=float(median(vals[-20:])); cur=float(vals[-1]); rv=cur/med if med else None
        state="VERY LOW" if rv is not None and rv<.5 else "LOW" if rv is not None and rv<.8 else "NORMAL" if rv is not None and rv<1.2 else "ELEVATED" if rv is not None and rv<1.5 else "HIGH" if rv is not None and rv<2 else "EXTREME"
        trend="RISING" if sum(vals[-5:])/5>sum(vals[-10:-5])/5 else "FALLING" if sum(vals[-5:])/5<sum(vals[-10:-5])/5 else "STABLE"
        return {"state":state,"relative_volume":rv,"trend":trend,"median_volume":med}

    def _chop(self,bars:list[Candle])->dict:
        if len(bars)<10:return {"score":None,"state":"INSUFFICIENT_DATA"}
        eff=self._efficiency(bars[-20:]) or 0; directions=[b.close>b.open for b in bars[-20:]]
        alt=sum(a!=b for a,b in zip(directions[:-1],directions[1:]))/max(1,len(directions)-1)
        overlap=sum(max(0.0,min(float(a.high),float(b.high))-max(float(a.low),float(b.low))) for a,b in zip(bars[-20:-1],bars[-19:]))/max(1,len(bars)-1)
        rng=[float(b.high-b.low) for b in bars[-20:]]; compression=(median(rng)/max(rng)) if rng else 0
        crosses=self._vwap(bars).get("crosses") or 0
        score=max(0,min(100,round((1-eff)*45+alt*25+min(1,overlap/max(1,median(rng)))*15+min(1,crosses/5)*10+(1-compression)*5)))
        state="CLEAN" if score<25 else "MILD CHOP" if score<45 else "CHOPPY" if score<70 else "SEVERE CHOP"
        return {"score":score,"state":state}

    def _rebuild(self) -> None:
        bars5=self.candles_5m
        if not bars5 and not self.last_tick:
            self.last_context={"instrument_id":self.instrument_id,"data_freshness":"NO_DATA"}; return
        now=self.last_tick.timestamp if self.last_tick else (bars5[-1].end if bars5 else datetime.now(timezone.utc))
        trends={"day":self._trend(self._resample(bars5,86400)),"h1":self._trend(self._resample(bars5,3600)),"m15":self._trend(self._resample(bars5,900)),"m5":self._trend(bars5),"current":self._trend(bars5[-12:])}
        vwap=self._vwap(self.candles_1m or bars5); volume=self._volume(bars5); chop=self._chop(bars5)
        m5dir=trends["m5"]["direction"]; h1dir=trends["h1"]["direction"]; daydir=trends["day"]["direction"]; m15dir=trends["m15"]["direction"]
        dirs=[x for x in (daydir,h1dir,m15dir,m5dir) if x in ("BULLISH","STRONG BULLISH","BEARISH","STRONG BEARISH")]
        bulls=sum(x.endswith("BULLISH") for x in dirs); bears=sum(x.endswith("BEARISH") for x in dirs)
        alignment=f"{bulls}/{len(dirs)} BULLISH" if bulls>=bears else f"{bears}/{len(dirs)} BEARISH" if bears>bulls else "CONFLICTED"
        structure=trends["m5"].get("structure","INSUFFICIENT_DATA")
        primary=trends["current"]["direction"]
        if primary=="INSUFFICIENT_DATA": primary="INSUFFICIENT_DATA"
        health=None
        evidence=[]; conflicts=[]
        if trends["m5"]["score"] is not None: evidence.append({"factor":"5M TREND","score":trends["m5"]["score"]})
        if vwap.get("distance_atr") is not None: evidence.append({"factor":"VWAP","score":round(min(100,max(0,50+vwap["distance_atr"]*25)))} )
        if volume.get("relative_volume") is not None:evidence.append({"factor":"VOLUME","score":round(min(100,volume["relative_volume"]*50))})
        if chop.get("score") is not None: conflicts.append({"factor":"CHOP","score":chop["score"]})
        if evidence:
            health=int(round(sum(x["score"] for x in evidence)/len(evidence)-sum(x["score"]*.15 for x in conflicts)/max(1,len(conflicts))))
        freshness="LIVE" if self.last_tick and (datetime.now(timezone.utc)-self.last_tick.timestamp).total_seconds()<300 else "STALE" if self.last_tick else "NO_LIVE_DATA"
        self.last_context={
            "instrument_id":self.instrument_id,"price":str(self.last_tick.price) if self.last_tick else None,"timestamp":now.isoformat(),"day_trend":trends["day"]["direction"],"day_score":trends["day"]["score"],"h1_trend":trends["h1"]["direction"],"h1_score":trends["h1"]["score"],"m15_trend":trends["m15"]["direction"],"m15_score":trends["m15"]["score"],"m5_trend":trends["m5"]["direction"],"m5_score":trends["m5"]["score"],"current_trend":primary,"current_score":trends["current"]["score"],"trend_strength":trends["current"]["strength"],"trend_stability":"CHOPPY" if chop.get("state")=="SEVERE CHOP" else "UNSTABLE" if chop.get("score",0)>=45 else "STABLE","momentum_state":trends["current"]["momentum"],"momentum_score":trends["current"]["score"],"volatility_state":trends["m5"]["volatility"],"volatility_percentile":trends["m5"]["atr_percentile"],"vwap_state":vwap.get("state"),"vwap_distance_atr":vwap.get("distance_atr"),"relative_volume":volume.get("relative_volume"),"volume_state":volume.get("state"),"efficiency":trends["m5"]["efficiency"],"chop_score":chop.get("score"),"structure_state":structure,"protected_level":trends["m5"].get("protected_level"),"structure_risk":None,"market_alignment":"INSUFFICIENT_DATA","relative_strength":None,"pullback_state":"NONE_DETECTED","health_score":health,"primary_reason":None,"next_required_condition":"NO_PULLBACK_DETECTED","data_freshness":freshness,"evidence":evidence,"conflicts":conflicts,"session_phase":self._session_phase(now),"session_open":None,"session_high":None,"session_low":None,"session_vwap":vwap.get("vwap"),"session_volume":sum(b.volume for b in (self.candles_1m or bars5) if b.start.astimezone(self.tz).date()==now.astimezone(self.tz).date()),"minutes_since_open":self._minutes_since_open(now),"minutes_to_close":self._minutes_to_close(now),"market_context_version":"1.0"
        }

    def _session_phase(self, ts:datetime)->str:
        t=ts.astimezone(self.tz).time()
        if t<time(9,15):return "PRE_OPEN"
        if t<time(9,30):return "OPENING"
        if t<time(10,30):return "OPENING_TREND"
        if t<time(12):return "MORNING_TREND_DIGESTION"
        if t<time(14):return "MIDDAY"
        if t<time(15):return "AFTERNOON"
        if t<time(15,30):return "CLOSING"
        return "CLOSED"

    def _minutes_since_open(self,ts):
        local=ts.astimezone(self.tz); open_dt=local.replace(hour=9,minute=15,second=0,microsecond=0)
        return max(0,int((local-open_dt).total_seconds()/60)) if local>=open_dt else 0

    def _minutes_to_close(self,ts):
        local=ts.astimezone(self.tz); close_dt=local.replace(hour=15,minute=30,second=0,microsecond=0)
        return max(0,int((close_dt-local).total_seconds()/60)) if local<=close_dt else 0

    def snapshot(self)->dict:
        return dict(self.last_context)
