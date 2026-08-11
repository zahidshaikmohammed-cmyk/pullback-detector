import pytest

from pullback_detector.universe import InstrumentUniverse


def _csv(rows: str) -> str:
    return "SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_TRADING_SYMBOL,SM_SYMBOL_NAME,SEM_SERIES\n" + rows


def test_universe_resolves_security_ids_from_official_master():
    text = _csv(
        "NSE,E,111,EQUITY,AAA,AAA,EQ\n"
        "NSE,E,222,EQUITY,BBB,BBB,EQ\n"
        "BSE,E,999,EQUITY,AAA,AAA,EQ\n"
    )
    instruments = InstrumentUniverse.from_dhan_csv(text, ("AAA", "BBB"))
    assert [(x.symbol, x.security_id) for x in instruments] == [("AAA", 111), ("BBB", 222)]
    assert all(x.source == "dhan_scrip_master" for x in instruments)


def test_universe_rejects_missing_requested_symbol():
    with pytest.raises(ValueError, match="did not resolve"):
        InstrumentUniverse.from_dhan_csv(_csv("NSE,E,111,EQUITY,AAA,AAA,EQ\n"), ("AAA", "BBB"))
