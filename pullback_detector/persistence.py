"""Append-only market event persistence with atomic flushes and no secrets."""

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from .models import Candle, PullbackSignal, Tick


def _jsonable(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


class EventStore:
    def __init__(self, root: str | Path = "data/runtime"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.root, 0o700)
        except OSError:
            pass

    def _append(self, path: Path, record: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, default=_jsonable, separators=(",", ":")) + "\n"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    def raw_packet(self, received_at: datetime, payload: bytes, response_code: int | None = None) -> None:
        day = received_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        self._append(self.root / "raw" / f"{day}.jsonl", {
            "received_at": received_at,
            "response_code": response_code,
            "payload_hex": payload.hex(),
        })

    def tick(self, received_at: datetime, tick: Tick) -> None:
        day = received_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        record = asdict(tick)
        record["received_at"] = received_at
        self._append(self.root / "normalized" / f"{day}.jsonl", record)

    def candle(self, candle: Candle) -> None:
        day = candle.start.astimezone(timezone.utc).strftime("%Y-%m-%d")
        self._append(self.root / "candles" / f"{day}.jsonl", asdict(candle))

    def signal(self, signal: PullbackSignal) -> None:
        day = signal.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d")
        self._append(self.root / "signals" / f"{day}.jsonl", asdict(signal))

    def health(self, report: dict) -> None:
        self._append(self.root / "health.jsonl", report)
