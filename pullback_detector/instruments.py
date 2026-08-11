"""Compatibility wrapper around the Dhan-derived instrument universe."""

import csv
from pathlib import Path

from .models import Instrument


class InstrumentUniverse:
    def __init__(self, instruments: list[Instrument]):
        self._by_id = {item.security_id: item for item in instruments}

    @classmethod
    def from_csv(cls, path: str | Path) -> "InstrumentUniverse":
        with Path(path).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = []
            for row in reader:
                rows.append(Instrument(
                    security_id=int(row["security_id"]),
                    exchange_segment=row["exchange_segment"],
                    symbol=row["symbol"],
                    trading_symbol=row.get("trading_symbol", row["symbol"]),
                    instrument_type=row.get("instrument_type", "EQUITY"),
                    series=row.get("series", ""),
                    isin=row.get("isin", ""),
                    source=row.get("source", "dhan_scrip_master"),
                ))
            return cls(rows)

    def get(self, security_id: int) -> Instrument | None:
        return self._by_id.get(security_id)

    def liquid(self) -> list[Instrument]:
        return list(self._by_id.values())
