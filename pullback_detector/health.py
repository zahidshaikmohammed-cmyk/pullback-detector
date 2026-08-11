from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import median

from .models import Tick


@dataclass
class ConnectivityHealth:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reconnects: int = 0
    packets: int = 0
    malformed_packets: int = 0
    duplicate_packets: int = 0
    ticks: int = 0
    candles_1m: int = 0
    candles_5m: int = 0
    instruments_seen: set[int] = field(default_factory=set)
    last_tick_by_instrument: dict[int, datetime] = field(default_factory=dict)
    latencies_ms: list[float] = field(default_factory=list)

    def record_tick(self, tick: Tick, received_at: datetime) -> None:
        self.packets += 1
        self.ticks += 1
        self.instruments_seen.add(tick.instrument_id)
        self.last_tick_by_instrument[tick.instrument_id] = tick.timestamp
        latency = (received_at - tick.timestamp).total_seconds() * 1000
        if 0 <= latency < 120_000:
            self.latencies_ms.append(latency)
            self.latencies_ms = self.latencies_ms[-5000:]

    def report(self, now: datetime | None = None, subscribed_instruments: int = 0) -> dict:
        now = now or datetime.now(timezone.utc)
        staleness = {
            str(k): max(0.0, (now - v).total_seconds())
            for k, v in self.last_tick_by_instrument.items()
        }
        return {
            "generated_at": now.isoformat(),
            "started_at": self.started_at.isoformat(),
            "subscribed_instruments": subscribed_instruments,
            "instruments_received": sorted(self.instruments_seen),
            "instrument_count_received": len(self.instruments_seen),
            "packet_count": self.packets,
            "tick_count": self.ticks,
            "malformed_packets": self.malformed_packets,
            "duplicate_packets": self.duplicate_packets,
            "reconnects": self.reconnects,
            "candle_count_1m": self.candles_1m,
            "candle_count_5m": self.candles_5m,
            "last_tick": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()},
            "staleness_seconds": staleness,
            "latency_ms_median": median(self.latencies_ms) if self.latencies_ms else None,
            "latency_ms_max": max(self.latencies_ms) if self.latencies_ms else None,
        }
