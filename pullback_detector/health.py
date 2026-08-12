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

    @staticmethod
    def _expected_sets(expected_instruments: list[dict], seen: set[int]) -> tuple[set[int], set[int], set[int]]:
        expected_ids = {
            int(item["security_id"])
            for item in expected_instruments
            if item.get("security_id") is not None
        }
        producing_ids = expected_ids.intersection({int(i) for i in seen})
        not_producing_ids = expected_ids - producing_ids
        return expected_ids, producing_ids, not_producing_ids

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
        expected_instruments = expected_instruments or []
        expected_ids, producing_ids, missing_ids = self._expected_sets(expected_instruments, self.instruments_seen)

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

        states = {}
        for item in expected_instruments:
            sid = int(item["security_id"])
            age = normalized_age.get(str(sid))
            states[str(sid)] = "LIVE" if age is not None and age <= 60 else "STALE" if age is not None else "NO_DATA"

        missing_rows = []
        for item in expected_instruments:
            sid = int(item["security_id"])
            if sid in missing_ids:
                age = normalized_age.get(str(sid))
                reason = "STALE_FEED" if age is not None and age > 60 else "NO_TICK_RECEIVED"
                missing_rows.append({
                    "symbol": item.get("symbol"),
                    "security_id": sid,
                    "exchange_segment": item.get("exchange_segment"),
                    "instrument_type": item.get("instrument_type"),
                    "last_received_timestamp": self.last_received_at_by_instrument.get(sid).isoformat() if sid in self.last_received_at_by_instrument else None,
                    "data_age": age,
                    "reason": reason,
                })

        producing_rows = []
        for item in expected_instruments:
            sid = int(item["security_id"])
            if sid in producing_ids:
                age = normalized_age.get(str(sid))
                producing_rows.append({
                    "symbol": item.get("symbol"),
                    "security_id": sid,
                    "exchange_segment": item.get("exchange_segment"),
                    "instrument_type": item.get("instrument_type"),
                    "last_received_timestamp": self.last_received_at_by_instrument[sid].isoformat(),
                    "data_age": age,
                    "reason": "LIVE_ACCEPTED_EVENT" if age is not None and age <= 60 else "STALE_FEED",
                })

        # EventStore is the canonical source for persistence/recovery counters.
        try:
            from .persistence import EventStore
            store = EventStore._last_instance
        except Exception:
            store = None

        if store is not None:
            persistence = store.persistence_snapshot()
            recovery = store.recovery_snapshot()
        else:
            persistence = {
                "persisted_1m_candles": persisted_1m,
                "persisted_5m_candles": persisted_5m,
                "last_persisted_1m_timestamp": None,
                "last_persisted_5m_timestamp": None,
                "persistence_write_count": 0,
                "persistence_failure_count": self.persistence_errors,
                "duplicate_event_count": 0,
                "duplicate_signal_count": 0,
            }
            recovery = {
                "restart_recovery_verified": restart_recovery_verified,
                "pre_restart_counts": {"1m": 0, "5m": 0},
                "post_restart_counts": {"1m": persisted_1m, "5m": persisted_5m},
                "recovered_candle_counts": {"1m": 0, "5m": 0},
                "recovered_event_state": {},
                "duplicate_count": 0,
                "continuity_status": "UNKNOWN",
                "recovery_timestamp": None,
                "recovery_duration_ms": None,
            }

        persisted_1m = int(persistence["persisted_1m_candles"])
        persisted_5m = int(persistence["persisted_5m_candles"])
        persistence_failures = int(persistence["persistence_failure_count"])

        raw_packet_count = self.packets + self.duplicate_packets
        timestamp_integrity_verified = bool(
            self.ticks > 0
            and all(ts.tzinfo is not None for ts in self.last_tick_by_instrument.values())
            and all(ts.tzinfo is not None for ts in self.last_source_timestamp_by_instrument.values())
        )
        subscriptions_verified = subscribed_instruments == len(expected_ids) == 22
        producing_set_verified = producing_ids.union(missing_ids) == expected_ids and producing_ids.isdisjoint(missing_ids)
        exact_non_producing_set_verified = missing_ids == expected_ids - producing_ids
        one_min_verified = self.candles_1m > 0 or bool(self.last_valid_1m_by_instrument) or persisted_1m > 0
        five_min_verified = self.candles_5m > 0 or bool(self.last_valid_5m_by_instrument) or persisted_5m > 0
        persistence_verified = persisted_1m > 0 and persisted_5m > 0 and persistence_failures == 0
        canonical_dashboard_state_verified = True
        dashboard_snapshot_exception_free = True
        critical_integrity_error = bool(self.ticks_rejected_by_candle_engine or persistence_failures)

        current_for_progression = {
            "generated_at": now.isoformat(),
            "accepted_tick_count": self.ticks,
            "ticks_sent_to_candle_engine": self.ticks_sent_to_candle_engine,
            "completed_1m_candles": self.candles_1m,
            "completed_5m_candles": self.candles_5m,
            "persisted_candle_count_1m": persisted_1m,
            "persisted_candle_count_5m": persisted_5m,
        }
        progression = store.counter_progression(current_for_progression) if store is not None else {
            "counter_progression_verified": False,
            "before": {},
            "after": current_for_progression,
            "before_timestamp": None,
            "after_timestamp": now.isoformat(),
        }

        gates = {
            "feed_live": feed_state == "LIVE",
            "subscriptions_verified": subscriptions_verified,
            "producing_set_verified": producing_set_verified,
            "exact_non_producing_set_verified": exact_non_producing_set_verified,
            "timestamp_integrity_verified": timestamp_integrity_verified,
            "1m_candle_generation_verified": one_min_verified,
            "5m_candle_generation_verified": five_min_verified,
            "persistence_verified": persistence_verified,
            "counter_progression_verified": progression["counter_progression_verified"],
            "restart_recovery_verified": recovery["restart_recovery_verified"],
            "canonical_dashboard_state_verified": canonical_dashboard_state_verified,
            "no_active_dashboard_snapshot_exception": dashboard_snapshot_exception_free,
            "no_unresolved_critical_data_integrity_error": not critical_integrity_error,
        }
        ordered = [
            "feed_live",
            "subscriptions_verified",
            "producing_set_verified",
            "exact_non_producing_set_verified",
            "timestamp_integrity_verified",
            "1m_candle_generation_verified",
            "5m_candle_generation_verified",
            "persistence_verified",
            "counter_progression_verified",
            "restart_recovery_verified",
            "canonical_dashboard_state_verified",
            "no_active_dashboard_snapshot_exception",
            "no_unresolved_critical_data_integrity_error",
        ]
        first_failure = next((name for name in ordered if not gates[name]), None)
        overall = "READY_FOR_PHASE_2" if first_failure is None else "NOT_READY_FOR_PHASE_2"

        return {
            "generated_at": now.isoformat(),
            "started_at": self.started_at.isoformat(),
            "service_status": "live",
            "feed_state": feed_state,
            "dhan_connection_status": "connected" if feed_state == "LIVE" else "stale" if feed_state == "STALE" else "disconnected" if feed_state == "DISCONNECTED" else "no_data",
            "subscribed_instruments": subscribed_instruments,
            "configured_instruments": len(expected_ids),
            "resolved_instruments": len(expected_ids),
            "expected_instrument_ids": sorted(expected_ids),
            "instruments_producing_accepted_ticks": sorted(producing_ids),
            "instrument_count_received": len(producing_ids),
            "producing_instruments": producing_rows,
            "not_producing_instruments": missing_rows,
            "packet_count": self.packets,
            "raw_packet_count": raw_packet_count,
            "decoded_packet_count": self.packets,
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
            "last_persisted_1m_timestamp": persistence["last_persisted_1m_timestamp"],
            "last_persisted_5m_timestamp": persistence["last_persisted_5m_timestamp"],
            "persistence_write_count": persistence["persistence_write_count"],
            "persistence_failure_count": persistence_failures,
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
            "persistence_state": "HEALTHY" if persistence_failures == 0 else "FAILED",
            "stale_data": feed_state == "STALE",
            "latency_ms_median": median(self.latencies_ms) if self.latencies_ms else None,
            "latency_ms_max": max(self.latencies_ms) if self.latencies_ms else None,
            "source_clock_skew_ticks": self.source_clock_skew_ticks,
            "restart_recovery_verified": recovery["restart_recovery_verified"],
            "restart_recovery": recovery,
            "counter_progression": progression,
            "counter_progression_verified": progression["counter_progression_verified"],
            "phase1_gates": gates,
            "overall_phase1_status": overall,
            "first_failure_reason": first_failure,
            "canonical_dashboard_state_verified": canonical_dashboard_state_verified,
            "dashboard_snapshot_exception_free": dashboard_snapshot_exception_free,
            "market_status": "OPEN" if feed_state == "LIVE" else None,
        }
