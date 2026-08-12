"""Observable, resumable Phase-1 validation over canonical production runtime state."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from .live import LIVE_RUNTIME

logger = logging.getLogger(__name__)
ACTIVE_VALIDATOR = None


class Phase1Validator:
    SNAPSHOT_INTERVAL_SECONDS = 60

    def __init__(self, data_root: str | Path):
        self.root = Path(data_root)
        self.path = self.root / "phase1_validation.json"
        self.sha = os.getenv("RENDER_GIT_COMMIT") or os.getenv("RENDER_GIT_COMMIT_SHA") or "UNKNOWN"
        self.deployment_id = os.getenv("RENDER_DEPLOY_ID") or os.getenv("RENDER_DEPLOYMENT_ID") or "UNKNOWN"
        self.state = "VALIDATION_IDLE"
        self.started_at = None
        self.last_update = None
        self.reason = "NOT_STARTED"
        self.snapshot_count = 0
        self.snapshots: list[dict] = []
        self.restart_state = "NOT_STARTED"
        self.restart_reason = "NOT_STARTED"
        self._load()

    def _now(self):
        return datetime.now(timezone.utc)

    def _save(self):
        self.root.mkdir(parents=True, exist_ok=True)
        payload = {
            "deployment_sha": self.sha,
            "deployment_id": self.deployment_id,
            "validation_state": self.state,
            "validation_started_at": self.started_at,
            "validation_last_update": self.last_update,
            "validation_reason": self.reason,
            "validation_snapshot_count": self.snapshot_count,
            "snapshots": self.snapshots[-2:],
            "restart_test_state": self.restart_state,
            "restart_test_reason": self.restart_reason,
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, default=str, separators=(",", ":")), encoding="utf-8")
        tmp.replace(self.path)

    def _load(self):
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if data.get("deployment_sha") != self.sha:
            self.reason = "NEW_DEPLOYMENT_SHA_INVALIDATED_PRIOR_VALIDATION"
            return
        self.state = data.get("validation_state", self.state)
        self.started_at = data.get("validation_started_at")
        self.last_update = data.get("validation_last_update")
        self.reason = data.get("validation_reason", self.reason)
        self.snapshot_count = int(data.get("validation_snapshot_count", 0) or 0)
        self.snapshots = list(data.get("snapshots") or [])[-2:]
        self.restart_state = data.get("restart_test_state", self.restart_state)
        self.restart_reason = data.get("restart_test_reason", self.restart_reason)

    def start(self):
        global ACTIVE_VALIDATOR
        ACTIVE_VALIDATOR = self
        now = self._now().isoformat()
        if self.started_at is None:
            self.started_at = now
        self.state = "VALIDATION_WAITING_FOR_DATA"
        self.reason = "WAITING_FOR_CANONICAL_RUNTIME_DATA"
        self.last_update = now
        self._save()

    def stop(self):
        global ACTIVE_VALIDATOR
        if ACTIVE_VALIDATOR is self:
            ACTIVE_VALIDATOR = None
        try:
            self._save()
        except OSError:
            pass

    async def fail(self, reason: str):
        self.state = "VALIDATION_FAILED"
        self.reason = reason
        self.last_update = self._now().isoformat()
        self._save()

    def _canonical_snapshot(self) -> dict | None:
        health = LIVE_RUNTIME.get("health")
        expected = list(LIVE_RUNTIME.get("expected_instruments") or [])
        if health is None or not expected:
            return None
        now = self._now()
        one = LIVE_RUNTIME.get("one_min")
        five = LIVE_RUNTIME.get("five_min")
        one_state = one.state_snapshot() if one else {}
        five_state = five.state_snapshot() if five else {}
        report = health.report(
            now=now,
            subscribed_instruments=len(expected),
            persisted_1m=int(LIVE_RUNTIME.get("persisted_1m") or 0),
            persisted_5m=int(LIVE_RUNTIME.get("persisted_5m") or 0),
            expected_instruments=expected,
            websocket_connected=bool(LIVE_RUNTIME.get("websocket_connected")),
            restart_recovery_verified=bool(LIVE_RUNTIME.get("restart_recovery_verified")),
        )
        try:
            from .persistence import EventStore
            store = EventStore._last_instance
        except Exception:
            store = None
        persistence = store.persistence_snapshot() if store else {}
        recovery = store.recovery_snapshot() if store else {}
        return {
            "deployment_sha": self.sha,
            "deployment_id": self.deployment_id,
            "timestamp": now.isoformat(),
            "feed_state": report.get("feed_state"),
            "dhan_state": report.get("dhan_connection_status"),
            "expected_instruments": report.get("expected_instrument_ids", []),
            "resolved_instruments": len(expected),
            "subscribed_instruments": report.get("subscribed_instruments", 0),
            "producing_instruments": report.get("producing_instrument_ids", []),
            "not_producing_instruments": report.get("not_producing_instrument_ids", []),
            "accepted_ticks": report.get("accepted_tick_count", 0),
            "ticks_sent_to_candle_engine": report.get("ticks_sent_to_candle_engine", 0),
            "active_1m": one_state.get("open_bars", 0),
            "completed_1m": report.get("completed_1m_candles", 0),
            "active_5m": five_state.get("open_bars", 0),
            "completed_5m": report.get("completed_5m_candles", 0),
            "persisted_1m": persistence.get("persisted_1m_candles", 0),
            "persisted_5m": persistence.get("persisted_5m_candles", 0),
            "latest_accepted_timestamp": report.get("last_tick_timestamp"),
            "global_data_age": report.get("global_data_age_seconds"),
            "persistence_state": report.get("persistence_state"),
            "duplicate_raw_packets": report.get("duplicate_packets", 0),
            "duplicate_canonical_events": persistence.get("duplicate_event_count", 0),
            "duplicate_candle_contributions": persistence.get("duplicate_candle_contribution_count", 0),
            "restart_recovery": recovery,
            "phase1_gates": report.get("phase1_gates", {}),
            "overall_phase1_status": report.get("overall_phase1_status"),
            "first_failure_reason": report.get("first_failure_reason"),
        }

    def _update_restart(self, snap: dict):
        recovery = snap.get("restart_recovery") or {}
        if recovery.get("restart_recovery_verified"):
            self.restart_state = "PASS"
            self.restart_reason = "REAL_PERSISTED_STATE_RESTORED_AND_CONTINUED"
        elif recovery.get("pre_restart_counts", {}).get("1m", 0) or recovery.get("pre_restart_counts", {}).get("5m", 0):
            self.restart_state = "FAIL"
            self.restart_reason = recovery.get("continuity_status") or str(recovery.get("recovered_event_state", {}))
        else:
            self.restart_state = "NOT_STARTED"
            self.restart_reason = "NO_REAL_PRE_RESTART_CHECKPOINT_WITH_CANDLE_STATE"

    async def poll(self, force: bool = False):
        now = self._now()
        try:
            snap = self._canonical_snapshot()
            if snap is None:
                self.state = "VALIDATION_WAITING_FOR_DATA"
                self.reason = "CANONICAL_RUNTIME_NOT_INITIALIZED"
            else:
                self._update_restart(snap)
                if not LIVE_RUNTIME.get("websocket_connected"):
                    self.state = "VALIDATION_WAITING_FOR_DATA"
                    self.reason = "WEBSOCKET_NOT_CONNECTED"
                elif snap["accepted_ticks"] <= 0:
                    self.state = "VALIDATION_WAITING_FOR_DATA"
                    self.reason = "WAITING_FOR_LIVE_DATA"
                elif self.snapshot_count == 0:
                    self.state = "VALIDATION_SNAPSHOT_READY"
                    self.reason = "SNAPSHOT_1_EVIDENCE_READY"
                    self._emit_snapshot(snap)
                elif self.snapshot_count == 1:
                    first = self.snapshots[0]
                    age = (now - datetime.fromisoformat(first["timestamp"])).total_seconds()
                    progressed = any(snap[k] > first[k] for k in ("accepted_ticks", "completed_1m", "completed_5m", "persisted_1m", "persisted_5m"))
                    if force or (age >= self.SNAPSHOT_INTERVAL_SECONDS and progressed):
                        self.state = "VALIDATION_SNAPSHOT_READY"
                        self.reason = "SNAPSHOT_2_EVIDENCE_READY"
                        self._emit_snapshot(snap)
                    else:
                        self.state = "VALIDATION_COLLECTING"
                        self.reason = "WAITING_FOR_SNAPSHOT_2_INTERVAL_AND_COUNTER_PROGRESSION"
                else:
                    self.state = "VALIDATION_COMPLETE" if self.restart_state == "PASS" else "VALIDATION_RESTART_TEST"
                    self.reason = "ALL_REQUIRED_SNAPSHOTS_CAPTURED" if self.restart_state == "PASS" else self.restart_reason
            self.last_update = now.isoformat()
            self._save()
        except Exception as exc:
            self.state = "VALIDATION_FAILED"
            self.reason = f"VALIDATOR_EXCEPTION:{type(exc).__name__}:{exc}"
            self.last_update = now.isoformat()
            self._save()

    def _emit_snapshot(self, snap: dict):
        self.snapshot_count += 1
        snap["snapshot_number"] = self.snapshot_count
        self.snapshots.append(snap)
        self.snapshots = self.snapshots[-2:]
        logger.info("PHASE1_VALIDATION_SNAPSHOT_%d %s", self.snapshot_count, json.dumps(snap, separators=(",", ":")))
        self._save()

    def diagnostic(self) -> dict:
        health = LIVE_RUNTIME.get("health")
        expected = LIVE_RUNTIME.get("expected_instruments") or []
        producing = len(health.instruments_seen) if health else 0
        return {
            "title": "PHASE 1 VALIDATION",
            "deployment": self.sha,
            "deployment_id": self.deployment_id,
            "validation_state": self.state,
            "validation_started_at": self.started_at,
            "validation_last_update": self.last_update,
            "validation_reason": self.reason,
            "validation_snapshot_count": self.snapshot_count,
            "feed": "LIVE" if LIVE_RUNTIME.get("websocket_connected") else "DISCONNECTED",
            "subscriptions": f"{len(expected)}/22",
            "producing": f"{producing}/22",
            "ticks": health.ticks if health else 0,
            "1m": health.candles_1m if health else 0,
            "5m": health.candles_5m if health else 0,
            "restart": self.restart_state,
            "restart_reason": self.restart_reason,
            "snapshot": f"{self.snapshot_count}/2",
            "blocking_condition": self.reason,
        }
