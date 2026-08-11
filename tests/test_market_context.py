from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.market_context import MarketContextEngine, benchmark_alignment
from pullback_detector.models import Candle, Tick
from pullback_detector.universe import InstrumentUniverse


def bars(instrument=25, n=60, direction=1):
    start=datetime(2026,8,10,9,15,tzinfo=timezone.utc)
    out=[]; price=Decimal("100")
    for i in range(n):
        move=Decimal(direction) * Decimal("0.20")
        o=price; c=price+move; h=max(o,c)+Decimal("0.05"); l=min(o,c)-Decimal("0.05")
        out.append(Candle(instrument,start+timedelta(minutes=5*i),start+timedelta(minutes=5*(i+1)),o,h,l,c,1000,True,300))
        price=c
    return out


def test_context_has_no_fabricated_daily_history():
    e=MarketContextEngine(25)
    for b in bars(): e.update_candle(b)
    s=e.snapshot()
    assert s["m5_trend"] in {"BULLISH","STRONG BULLISH","NEUTRAL","INSUFFICIENT_DATA"}
    assert s["day_trend"] == "INSUFFICIENT_DATA"
    assert s["relative_strength"] is None
    assert s["market_alignment"] == "INSUFFICIENT_DATA"


def test_context_tracks_live_price_and_vwap():
    e=MarketContextEngine(25)
    for b in bars(): e.update_candle(b)
    ts=datetime(2026,8,10,10,30,tzinfo=timezone.utc)
    e.update_tick(Tick(25,ts,Decimal("112.5"),10))
    s=e.snapshot()
    assert s["price"] == "112.5"
    assert s["session_vwap"] is not None
    assert s["vwap_state"] in {"ABOVE_ACCEPTANCE","TRANSITIONING","BELOW_ACCEPTANCE"}
    assert s["data_freshness"] in {"LIVE","STALE"}


def test_directional_efficiency_is_deterministic():
    e=MarketContextEngine(25)
    clean=bars(direction=1)
    noisy=[]
    start=clean[0].start; price=Decimal("100")
    for i in range(30):
        move=Decimal("1") if i%2==0 else Decimal("-0.8")
        o=price; c=price+move; noisy.append(Candle(25,start+timedelta(minutes=5*i),start+timedelta(minutes=5*(i+1)),o,max(o,c)+Decimal('.1'),min(o,c)-Decimal('.1'),c,1000,True,300)); price=c
    assert e._efficiency(clean[-20:]) > e._efficiency(noisy[-20:])


def test_session_phase_is_session_aware():
    e=MarketContextEngine(25)
    assert e._session_phase(datetime(2026,8,10,3,50,tzinfo=timezone.utc)) == "OPENING"
    assert e._session_phase(datetime(2026,8,10,7,0,tzinfo=timezone.utc)) == "MIDDAY"
    assert e._session_phase(datetime(2026,8,10,10,0,tzinfo=timezone.utc)) == "CLOSING"


def test_benchmark_resolution_uses_index_instruments_not_equities():
    csv="""SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_TRADING_SYMBOL,SEM_CUSTOM_SYMBOL\nNSE,E,13,INDEX,NIFTY,NIFTY 50\nNSE,E,25,INDEX,BANKNIFTY,BANK NIFTY\nNSE,E,1333,EQUITY,RELIANCE,RELIANCE\n"""
    benchmarks=InstrumentUniverse.from_dhan_csv_benchmarks(csv)
    assert [(x.symbol,x.security_id,x.exchange_segment,x.instrument_type) for x in benchmarks] == [("BANKNIFTY",25,"IDX_I","INDEX"),("NIFTY",13,"IDX_I","INDEX")]


def _context(direction):
    return {f"{tf}_trend": direction for tf in ("day","h1","m15","m5","current")}


def test_stock_market_alignment_requires_real_benchmark_evidence():
    stock=_context("BULLISH")
    assert benchmark_alignment(stock,{})["status"] == "INSUFFICIENT_DATA"
    aligned=benchmark_alignment(stock,{"NIFTY":_context("BULLISH"),"BANKNIFTY":_context("BULLISH")})
    assert aligned["status"] == "ALIGNED"
    assert aligned["score"] == 100
    diverged=benchmark_alignment(stock,{"NIFTY":_context("BEARISH"),"BANKNIFTY":_context("BEARISH")})
    assert diverged["status"] == "DIVERGING"
    assert diverged["score"] == 0


def test_partial_benchmark_alignment_is_deterministic():
    stock=_context("BULLISH")
    result=benchmark_alignment(stock,{"NIFTY":_context("BULLISH"),"BANKNIFTY":_context("BEARISH")})
    assert result["status"] == "PARTIALLY_ALIGNED"
    assert result["score"] == 50
