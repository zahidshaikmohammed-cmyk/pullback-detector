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
    last_received_at_by_instrument: dict[int, datetime] = field(default_factory=dict)
    latencies_ms: list[float] = field(default_factory=list)
    source_clock_skew_ticks: int = 0
    last_source_timestamp_by_instrument: dict[int, datetime] = field(default_factory=dict)

    def record_tick(self, tick: Tick, received_at: datetime) -> None:
        self.ticks += 1
        self.instruments_seen.add(tick.instrument_id)
        self.last_tick_by_instrument[tick.instrument_id] = tick.timestamp
        self.last_received_at_by_instrument[tick.instrument_id] = received_at
        if tick.source_clock_skew_seconds is not None:
            self.source_clock_skew_ticks += 1
        source_ts = tick.source_timestamp or tick.timestamp
        self.last_source_timestamp_by_instrument[tick.instrument_id] = source_ts
        latency = (received_at - tick.timestamp).total_seconds() * 1000
        if 0 <= latency < 120_000:
            self.latencies_ms.append(latency)
            self.latencies_ms = self.latencies_ms[-5000:]

    def report(self, now: datetime | None = None, subscribed_instruments: int = 0) -> dict:
        now = now or datetime.now(timezone.utc)
        now = now.astimezone(timezone.utc)
        staleness = {
            str(k): max(0.0, (now - v).total_seconds())
            for k, v in self.last_tick_by_instrument.items()
        }
        receive_staleness = {
            str(k): max(0.0, (now - v).total_seconds())
            for k, v in self.last_received_at_by_instrument.items()
        }
        latest = max(self.last_tick_by_instrument.values(), default=None)
        latest_received = max(self.last_received_at_by_instrument.values(), default=None)
        return {
            "generated_at": now.isoformat(),
            "started_at": self.started_at.isoformat(),
            "service_status": "live",
            "dhan_connection_status": "connected" if self.reconnects == 0 else "connected_after_reconnect",
            "subscribed_instruments": subscribed_instruments,
            "instruments_producing_accepted_ticks": sorted(self.instruments_seen),
            "instruments_received": sorted(self.instruments_seen),
            "instrument_count_received": len(self.instruments_seen),
            "packet_count": self.packets,
            "accepted_tick_count": self.ticks,
            "rejected_tick_count": self.malformed_packets,
            "malformed_packets": self.malformed_packets,
            "duplicate_packets": self.duplicate_packets,
            "reconnects": self.reconnects,
            "candle_count_1m": self.candles_1m,
            "candle_count_5m": self.candles_5m,
            "last_tick_timestamp": latest.isoformat() if latest else None,
            "last_receive_timestamp": latest_received.isoformat() if latest_received else None,
            "last_tick": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()},
            "last_source_timestamp": {str(k): v.isoformat() for k, v in self.last_source_timestamp_by_instrument.items()},
            "receive_staleness_seconds": receive_staleness,
            "staleness_seconds": staleness,
            "stale_data": bool(receive_staleness) and all(value > 60 for value in receive_staleness.values()),
            "latency_ms_median": median(self.latencies_ms) if self.latencies_ms else None,
            "latency_ms_max": max(self.latencies_ms) if self.latencies_ms else None,
            "source_clock_skew_ticks": self.source_clock_skew_ticks,
        }
