"""Verified NSE equity universe resolution from Dhan's official scrip master."""

import csv
import io
import logging
from dataclasses import asdict
from pathlib import Path

import httpx

from .models import Instrument

logger = logging.getLogger(__name__)
DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Symbols are names only. Security IDs are NEVER hardcoded; they are resolved
# from Dhan's current official instrument master at runtime.
DEFAULT_LIQUID_SYMBOLS = (
    "RELIANCE", "HDFCBANK", "ICICIBANK", "SBIN", "INFY", "TCS", "ITC",
    "BHARTIARTL", "LT", "AXISBANK", "KOTAKBANK", "M&M", "MARUTI", "SUNPHARMA",
    "TITAN", "BAJFINANCE", "HINDUNILVR", "ADANIENT", "ADANIPORTS", "NTPC",
)


class InstrumentUniverse:
    def __init__(self, symbols: tuple[str, ...] = DEFAULT_LIQUID_SYMBOLS):
        self.symbols = tuple(s.upper().strip() for s in symbols if s.strip())

    @staticmethod
    def _candidate_symbol(row: dict[str, str]) -> str:
        return (row.get("SEM_TRADING_SYMBOL") or row.get("SM_SYMBOL_NAME") or "").strip().upper()

    @classmethod
    def from_dhan_csv(cls, csv_text: str, symbols: tuple[str, ...] = DEFAULT_LIQUID_SYMBOLS) -> list[Instrument]:
        reader = csv.DictReader(io.StringIO(csv_text))
        required = {"SEM_EXM_EXCH_ID", "SEM_SMST_SECURITY_ID", "SEM_INSTRUMENT_NAME"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Dhan scrip master missing required columns: {sorted(missing)}")

        wanted = {s.upper() for s in symbols}
        found: dict[str, Instrument] = {}
        for row in reader:
            if row.get("SEM_EXM_EXCH_ID", "").strip().upper() != "NSE":
                continue
            if row.get("SEM_SEGMENT", "").strip().upper() not in {"E", "EQ", "EQUITY"}:
                continue
            if row.get("SEM_INSTRUMENT_NAME", "").strip().upper() != "EQUITY":
                continue
            symbol = cls._candidate_symbol(row)
            if symbol not in wanted:
                continue
            security_id = row.get("SEM_SMST_SECURITY_ID", "").strip()
            if not security_id.isdigit() or int(security_id) <= 0:
                continue
            found[symbol] = Instrument(
                security_id=int(security_id),
                exchange_segment="NSE_EQ",
                symbol=symbol,
                trading_symbol=(row.get("SEM_TRADING_SYMBOL") or symbol).strip(),
                instrument_type="EQUITY",
                series=(row.get("SEM_SERIES") or "").strip(),
                isin=(row.get("SEM_ISIN") or "").strip(),
            )

        missing_symbols = sorted(wanted - set(found))
        if missing_symbols:
            raise ValueError(f"Dhan official master did not resolve requested NSE equities: {missing_symbols}")
        result = [found[s] for s in sorted(wanted)]
        if len({x.security_id for x in result}) != len(result):
            raise ValueError("Dhan official master produced duplicate security IDs")
        return result

    @classmethod
    def fetch(cls, symbols: tuple[str, ...] = DEFAULT_LIQUID_SYMBOLS, timeout: float = 20.0) -> list[Instrument]:
        response = httpx.get(DHAN_SCRIP_MASTER_URL, timeout=timeout)
        response.raise_for_status()
        instruments = cls.from_dhan_csv(response.text, symbols)
        logger.info("resolved %d NSE_EQ instruments from Dhan official master", len(instruments))
        return instruments

    @staticmethod
    def write_snapshot(instruments: list[Instrument], path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(asdict(instruments[0]).keys()) if instruments else ["security_id", "exchange_segment", "symbol", "trading_symbol", "instrument_type", "series", "isin", "source"])
            writer.writeheader()
            for instrument in instruments:
                writer.writerow(asdict(instrument))
            handle.flush()
        tmp.replace(path)
