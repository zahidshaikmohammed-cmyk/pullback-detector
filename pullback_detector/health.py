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
    ticks_sent_to_candle_engine: int = 0
    ticks_rejected_by_candle_engine: int = 0
    candles_1m: int = 0
    candles_5m: int = 0
    persistence_errors: int = 0
    instruments_seen: set[int] = field(default_factory=set)
    last_tick_by_instrument: dict[int, datetime] = field(default_factory=dict)
    last_received_at_by_instrument: dict[int, datetime] = field(default_factory=dict)
    last_price_by_instrument: dict[int, str] = field(default_factory=dict)
    latencies_ms: list[float] = field(default_factory=list)
    source_clock_skew_ticks: int = 0
    last_source_timestamp_by_instrument: dict[int, datetime] = field(default_factory=dict)
    last_valid_1m_by_instrument: dict[int, datetime] = field(default_factory=dict)
    last_valid_5m_by_instrument: dict[int, datetime] = field(default_factory=dict)

    def record_tick(self, tick: Tick, received_at: datetime) -> None:
        received_at = received_at.astimezone(timezone.utc)
        normalized_ts = tick.timestamp.astimezone(timezone.utc)
        self.ticks += 1
        self.instruments_seen.add(tick.instrument_id)
        self.last_tick_by_instrument[tick.instrument_id] = normalized_ts
        self.last_received_at_by_instrument[tick.instrument_id] = received_at
        self.last_price_by_instrument[tick.instrument_id] = str(tick.price)
        if tick.source_clock_skew_seconds is not None:
            self.source_clock_skew_ticks += 1
        self.last_source_timestamp_by_instrument[tick.instrument_id] = (tick.source_timestamp or tick.timestamp).astimezone(timezone.utc)
        latency = (received_at - normalized_ts).total_seconds() * 1000
        if 0 <= latency < 120_000:
            self.latencies_ms.append(latency)
            self.latencies_ms = self.latencies_ms[-5000:]

    def record_candle_engine_tick(self) -> None:
        self.ticks_sent_to_candle_engine += 1

    def record_candle_engine_reject(self) -> None:
        self.ticks_rejected_by_candle_engine += 1

    def record_candle(self, candle) -> None:
        target = self.last_valid_1m_by_instrument if candle.timeframe_seconds == 60 else self.last_valid_5m_by_instrument
        if candle.complete:
            target[candle.instrument_id] = candle.end.astimezone(timezone.utc)

    def report(
        self,
        now: datetime | None = None,
        subscribed_instruments: int = 0,
        persisted_1m: int = 0,
        persisted_5m: int = 0,
        expected_instruments: list[dict] | None = None,
        websocket_connected: bool = False,
        restart_recovery_verified: bool = False,
    ) -> dict:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        receive_age = {str(k): max(0.0, (now - v).total_seconds()) for k, v in self.last_received_at_by_instrument.items()}
        normalized_age = {str(k): max(0.0, (now - v).total_seconds()) for k, v in self.last_tick_by_instrument.items()}
        latest = max(self.last_tick_by_instrument.values(), default=None)
        latest_received = max(self.last_received_at_by_instrument.values(), default=None)
        max_normalized_age = max(normalized_age.values(), default=None)

        if not self.ticks:
            feed_state = "NO_DATA" if not websocket_connected else "STALE"
        elif max_normalized_age is not None and max_normalized_age <= 60:
            feed_state = "LIVE"
        elif websocket_connected:
            feed_state = "STALE"
        else:
            feed_state = "DISCONNECTED"

        states = {str(k): ("LIVE" if age <= 60 else "STALE") for k, age in normalized_age.items()}
        producing = len(self.instruments_seen)
        expected_instruments = expected_instruments or []
        producing_keys = {str(item.get("security_id")) for item in expected_instruments if item.get("security_id") is not None and int(item.get("security_id")) in self.instruments_seen}
        not_producing = []
        for item in expected_instruments:
            sid = item.get("security_id")
            if sid is None or str(sid) not in producing_keys:
                not_producing.append({"symbol": item.get("symbol"), "security_id": sid, "exchange_segment": item.get("exchange_segment"), "reason": "NO_PACKETS"})

        candle_ready = (self.candles_1m > 0 or persisted_1m > 0) and (self.candles_5m > 0 or persisted_5m > 0)
        universe_ready = subscribed_instruments == len(expected_instruments) if expected_instruments else subscribed_instruments == 22
        feed_ready = self.ticks > 0 and max_normalized_age is not None and max_normalized_age <= 60
        persistence_ready = self.persistence_errors == 0
        producing_ready = producing >= min(20, len(expected_instruments)) if expected_instruments else producing >= 20
        overall = "READY_FOR_PHASE_2" if universe_ready and producing_ready and feed_ready and candle_ready and persistence_ready and restart_recovery_verified else "NOT_READY_FOR_PHASE_2"
        failure = None
        if not universe_ready:
            failure = f"SUBSCRIPTIONS_NOT_COMPLETE:{subscribed_instruments}/{len(expected_instruments) if expected_instruments else 22}"
        elif not producing_ready:
            failure = f"PRODUCING_INSTRUMENTS_BELOW_MINIMUM:{producing}"
        elif not feed_ready:
            failure = "FEED_NOT_LIVE_WITH_RECENT_VALID_EVENTS"
        elif not candle_ready:
            failure = "INSUFFICIENT_1M_5M_CANDLES"
        elif not persistence_ready:
            failure = f"PERSISTENCE_ERRORS:{self.persistence_errors}"
        elif not restart_recovery_verified:
            failure = "RESTART_RECOVERY_NOT_VERIFIED"

        return {
            "generated_at": now.isoformat(),
            "started_at": self.started_at.isoformat(),
            "service_status": "live",
            "feed_state": feed_state,
            "dhan_connection_status": "connected" if feed_state == "LIVE" else "stale" if feed_state == "STALE" else "disconnected" if feed_state == "DISCONNECTED" else "no_data",
            "subscribed_instruments": subscribed_instruments,
            "configured_instruments": subscribed_instruments,
            "resolved_instruments": len(expected_instruments) if expected_instruments else subscribed_instruments,
            "instruments_producing_accepted_ticks": sorted(self.instruments_seen),
            "instrument_count_received": producing,
            "producing_instruments": producing,
            "not_producing_instruments": not_producing,
            "packet_count": self.packets,
            "accepted_tick_count": self.ticks,
            "rejected_tick_count": self.malformed_packets,
            "malformed_packets": self.malformed_packets,
            "duplicate_packets": self.duplicate_packets,
            "duplicate_ticks": self.duplicate_packets,
            "reconnects": self.reconnects,
            "ticks_sent_to_candle_engine": self.ticks_sent_to_candle_engine,
            "ticks_rejected_by_candle_engine": self.ticks_rejected_by_candle_engine,
            "active_1m_buckets": None,
            "active_5m_buckets": None,
            "completed_1m_candles": self.candles_1m,
            "completed_5m_candles": self.candles_5m,
            "candle_count_1m": self.candles_1m,
            "candle_count_5m": self.candles_5m,
            "persisted_candle_count_1m": persisted_1m,
            "persisted_candle_count_5m": persisted_5m,
            "last_tick_timestamp": latest.isoformat() if latest else None,
            "last_receive_timestamp": latest_received.isoformat() if latest_received else None,
            "last_tick": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()},
            "last_valid_tick": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()},
            "last_price": dict(self.last_price_by_instrument),
            "last_source_timestamp": {str(k): v.isoformat() for k, v in self.last_source_timestamp_by_instrument.items()},
            "normalized_timestamp": {str(k): v.isoformat() for k, v in self.last_tick_by_instrument.items()},
            "receive_staleness_seconds": receive_age,
            "data_age_seconds": normalized_age,
            "staleness_seconds": normalized_age,
            "global_data_age_seconds": max_normalized_age,
            "feed_state_by_instrument": states,
            "last_valid_1m": {str(k): v.isoformat() for k, v in self.last_valid_1m_by_instrument.items()},
            "last_valid_5m": {str(k): v.isoformat() for k, v in self.last_valid_5m_by_instrument.items()},
            "persistence_errors": self.persistence_errors,
            "persistence_state": "HEALTHY" if self.persistence_errors == 0 else "FAILED",
            "stale_data": feed_state == "STALE",
            "latency_ms_median": median(self.latencies_ms) if self.latencies_ms else None,
            "latency_ms_max": max(self.latencies_ms) if self.latencies_ms else None,
            "source_clock_skew_ticks": self.source_clock_skew_ticks,
            "restart_recovery_verified": restart_recovery_verified,
            "overall_phase1_status": overall,
            "first_failure_reason": failure,
        }
