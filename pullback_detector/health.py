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
    last_valid_1m_by_instrument: dict[int, datetime] = field(default_factory=dict)
    last_valid_5m_by_instrument: dict[int, datetime] = field(default_factory=dict)

    def record_tick(self, tick: Tick, received_at: datetime) -> None:
        self.ticks += 1; self.instruments_seen.add(tick.instrument_id); self.last_tick_by_instrument[tick.instrument_id] = tick.timestamp.astimezone(timezone.utc); self.last_received_at_by_instrument[tick.instrument_id] = received_at.astimezone(timezone.utc)
        if tick.source_clock_skew_seconds is not None: self.source_clock_skew_ticks += 1
        self.last_source_timestamp_by_instrument[tick.instrument_id] = (tick.source_timestamp or tick.timestamp).astimezone(timezone.utc)
        latency = (received_at.astimezone(timezone.utc) - tick.timestamp.astimezone(timezone.utc)).total_seconds() * 1000
        if 0 <= latency < 120_000: self.latencies_ms.append(latency); self.latencies_ms = self.latencies_ms[-5000:]

    def record_candle(self, candle) -> None:
        target = self.last_valid_1m_by_instrument if candle.timeframe_seconds == 60 else self.last_valid_5m_by_instrument
        if candle.complete: target[candle.instrument_id] = candle.end.astimezone(timezone.utc)

    def report(self, now: datetime | None = None, subscribed_instruments: int = 0) -> dict:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        receive_age = {str(k): max(0.0, (now - v).total_seconds()) for k, v in self.last_received_at_by_instrument.items()}
        normalized_age = {str(k): max(0.0, (now - v).total_seconds()) for k, v in self.last_tick_by_instrument.items()}
        latest = max(self.last_tick_by_instrument.values(), default=None); latest_received = max(self.last_received_at_by_instrument.values(), default=None)
        max_receive_age = max(receive_age.values(), default=None)
        if not self.ticks: feed_state = "FEED_CONNECTING"
        elif self.reconnects and max_receive_age is not None and max_receive_age > 60: feed_state = "FEED_STALE"
        elif max_receive_age is not None and max_receive_age > 60: feed_state = "FEED_STALE"
        else: feed_state = "FEED_LIVE"
        producing = len(self.instruments_seen); states = {str(k): ("LIVE" if age <= 60 else "STALE") for k, age in receive_age.items()}
        return {"generated_at": now.isoformat(), "started_at": self.started_at.isoformat(), "service_status": "live", "feed_state": feed_state, "dhan_connection_status": "connected" if feed_state == "FEED_LIVE" else "stale" if feed_state == "FEED_STALE" else "connecting", "subscribed_instruments": subscribed_instruments, "configured_instruments": subscribed_instruments, "resolved_instruments": subscribed_instruments, "instruments_producing_accepted_ticks": sorted(self.instruments_seen), "instrument_count_received": producing, "producing_instruments": producing, "packet_count": self.packets, "accepted_tick_count": self.ticks, "rejected_tick_count": self.malformed_packets, "malformed_packets": self.malformed_packets, "duplicate_packets": self.duplicate_packets, "duplicate_ticks": self.duplicate_packets, "reconnects": self.reconnects, "candle_count_1m": self.candles_1m, "candle_count_5m": self.candles_5m, "last_tick_timestamp": latest.isoformat() if latest else None, "last_receive_timestamp": latest_received.isoformat() if latest_received else None, "last_tick": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()}, "last_valid_tick": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()}, "last_source_timestamp": {str(k): v.isoformat() for k, v in self.last_source_timestamp_by_instrument.items()}, "normalized_timestamp": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()}, "receive_staleness_seconds": receive_age, "data_age_seconds": receive_age, "staleness_seconds": normalized_age, "feed_state_by_instrument": states, "last_valid_1m": {str(k): v.isoformat() for k, v in self.last_valid_1m_by_instrument.items()}, "last_valid_5m": {str(k): v.isoformat() for k, v in self.last_valid_5m_by_instrument.items()}, "stale_data": feed_state == "FEED_STALE", "latency_ms_median": median(self.latencies_ms) if self.latencies_ms else None, "latency_ms_max": max(self.latencies_ms) if self.latencies_ms else None, "source_clock_skew_ticks": self.source_clock_skew_ticks}
