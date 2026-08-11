import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Instrument:
    security_id: int
    exchange_segment: str
    symbol: str
    security_type: str


class InstrumentUniverse:
    def __init__(self, instruments: list[Instrument]):
        self._by_id = {item.security_id: item for item in instruments}

    @classmethod
    def from_csv(cls, path: str | Path) -> "InstrumentUniverse":
        with Path(path).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return cls([
                Instrument(
                    security_id=int(row["security_id"]),
                    exchange_segment=row["exchange_segment"],
                    symbol=row["symbol"],
                    security_type=row["security_type"],
                )
                for row in reader
            ])

    def get(self, security_id: int) -> Instrument | None:
        return self._by_id.get(security_id)

    def liquid(self) -> list[Instrument]:
        return list(self._by_id.values())
