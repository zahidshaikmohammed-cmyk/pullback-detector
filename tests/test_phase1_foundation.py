from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pullback_detector.candles import CandleAggregator
from pullback_detector.market_context import MarketContextEngine
from pullback_detector.models import Tick
from pullback_detector.validation import normalize_live_tick_clock, validate_tick_detailed

UTC = timezone.utc


def tick(ts, price=100, sid=25, segment="NSE_EQ"):
    return Tick(sid, ts, Decimal(str(price)), 10, segment, 100, 2, None, None)


def test_exact_dhan_plus_5h30_skew_normalizes_once():
    receive = datetime(2026, 8, 12, 5, 46, 40, tzinfo=UTC); source = receive + timedelta(hours=5, minutes=30)
    normalized, skew = normalize_live_tick_clock(tick(source), receive)
    assert normalized.source_timestamp == source and normalized.timestamp == receive and abs((normalized.timestamp - receive).total_seconds()) < 1 and 19790 < skew < 19810


def test_non_dhan_future_timestamp_is_quarantined():
    receive = datetime(2026, 8, 12, 5, 46, 40, tzinfo=UTC); normalized, _ = normalize_live_tick_clock(tick(receive + timedelta(minutes=10)), receive); result = validate_tick_detailed(normalized, receive, 5, 300)
    assert not result.valid and result.reason_code == "FUTURE_TIMESTAMP"


def test_none_chop_score_is_supported():
    engine = MarketContextEngine(25); engine._chop = lambda bars: {"score": None, "state": "INSUFFICIENT_DATA"}; engine.update_tick(tick(datetime(2026, 8, 12, 5, 46, 40, tzinfo=UTC)))
    assert engine.snapshot()["data_freshness"] in {"LIVE", "INSUFFICIENT_DATA", "STALE"}


def test_insufficient_atr_vwap_volume_do_not_raise():
    from pullback_detector.models import Candle
    engine = MarketContextEngine(25); engine.update_candle(Candle(25, datetime(2026,8,12,4,0,tzinfo=UTC), datetime(2026,8,12,4,5,tzinfo=UTC), Decimal('100'), Decimal('101'), Decimal('99'), Decimal('100'), 100, True, 300)); snap = engine.snapshot()
    assert snap["volatility_percentile"] is None and snap["relative_volume"] is None and snap["vwap_state"] in {"ABOVE_ACCEPTANCE", "INSUFFICIENT_DATA", "TRANSITIONING"}


def test_one_bad_instrument_does_not_break_another_context():
    good = MarketContextEngine(25); bad = MarketContextEngine(13); bad._rebuild = lambda: (_ for _ in ()).throw(TypeError("synthetic calculation failure")); bad.update_tick(tick(datetime(2026,8,12,5,46,40,tzinfo=UTC), sid=13, segment="IDX_I")); good.update_tick(tick(datetime(2026,8,12,5,46,41,tzinfo=UTC), sid=25))
    assert bad.snapshot()["data_freshness"] == "CALCULATION_ERROR" and good.snapshot()["instrument_id"] == 25


def test_benchmark_idx_i_tick_is_valid():
    ts = datetime(2026,8,12,5,46,40,tzinfo=UTC); result = validate_tick_detailed(tick(ts, 24300, sid=13, segment="IDX_I"), ts); assert result.valid


def test_one_minute_finalizes_only_after_interval():
    agg = CandleAggregator(60); base = datetime(2026,8,12,5,46,10,tzinfo=UTC); agg.update(tick(base,100)); agg.update(tick(base+timedelta(seconds=20),101)); assert agg.flush(base+timedelta(seconds=50)) == []; bars = agg.flush(base+timedelta(seconds=60)); assert len(bars) == 1 and bars[0].complete and bars[0].open == Decimal('100') and bars[0].high == Decimal('101')


def test_five_minute_ohlc_is_deterministic():
    agg = CandleAggregator(300); base = datetime(2026,8,12,5,46,10,tzinfo=UTC)
    for offset, price in ((0,100),(60,102),(180,99),(230,101)): agg.update(tick(base+timedelta(seconds=offset),price))
    bars = agg.flush(datetime(2026,8,12,5,50,0,tzinfo=UTC)); assert len(bars)==1 and bars[0].open==Decimal('100') and bars[0].high==Decimal('102') and bars[0].low==Decimal('99') and bars[0].close==Decimal('101')


def test_duplicate_event_does_not_double_count():
    agg=CandleAggregator(60); ts=datetime(2026,8,12,5,46,10,tzinfo=UTC); t=tick(ts,100); agg.update(t); agg.update(t); assert agg.duplicate_ticks==1
