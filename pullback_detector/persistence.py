"""Append-only market event persistence with deterministic Phase-1 recovery state."""

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from .models import Candle, PullbackSignal, Tick


def _jsonable(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


class EventStore:
    _last_instance = None
    _process_id = os.getpid()

    def __init__(self, root: str | Path = "data/runtime"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.root, 0o700)
        except OSError:
            pass

        self._candle_keys: set[tuple[int, int, str]] = set()
        self._event_keys: set[str] = set()
        self._signal_keys: set[str] = set()
        self._persisted_counts = {60: 0, 300: 0}
        self._last_persisted_timestamp = {60: None, 300: None}
        self.persistence_write_count = 0
        self.persistence_failure_count = 0
        self.duplicate_event_count = 0
        self.duplicate_signal_count = 0
        self._current_run_started_at = datetime.now(timezone.utc)
        self._first_tick_after_start = None
        self._first_candle_after_start = None
        self._previous_checkpoint = self._load_recovery_checkpoint()
        self._previous_health_report = self._load_last_health_report()
        self._load_indexes()
        EventStore._last_instance = self

    def _append(self, path: Path, record: dict) -> None:
        try:
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
        except Exception:
            self.persistence_failure_count += 1
            raise

    @staticmethod
    def _event_key(tick: Tick) -> str:
        raw = "|".join(
            (
                str(tick.instrument_id),
                tick.timestamp.astimezone(timezone.utc).isoformat(),
                str(tick.price),
                str(tick.quantity),
                str(tick.cumulative_volume),
                str(tick.sequence),
            )
        )
        return sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _signal_key(signal: PullbackSignal) -> str:
        return str(signal.signal_id or "|".join((str(signal.instrument_id), signal.timestamp.astimezone(timezone.utc).isoformat(), signal.direction, str(signal.trigger_price))))

    @staticmethod
    def _candle_key(candle: Candle) -> tuple[int, int, str]:
        return candle.instrument_id, candle.timeframe_seconds, candle.start.astimezone(timezone.utc).isoformat()

    def _load_indexes(self) -> None:
        candles_path = self.root / "candles"
        if candles_path.exists():
            for file in sorted(candles_path.glob("*.jsonl")):
                try:
                    for line in file.read_text(encoding="utf-8").splitlines():
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not row.get("complete"):
                            continue
                        try:
                            key = (
                                int(row["instrument_id"]),
                                int(row["timeframe_seconds"]),
                                datetime.fromisoformat(row["start"]).astimezone(timezone.utc).isoformat(),
                            )
                        except (KeyError, TypeError, ValueError):
                            continue
                        if key in self._candle_keys:
                            continue
                        self._candle_keys.add(key)
                        timeframe = key[1]
                        if timeframe in self._persisted_counts:
                            self._persisted_counts[timeframe] += 1
                            self._last_persisted_timestamp[timeframe] = max(
                                self._last_persisted_timestamp[timeframe] or key[2], key[2]
                            )
                except OSError:
                    self.persistence_failure_count += 1

        normalized_path = self.root / "normalized"
        if normalized_path.exists():
            for file in sorted(normalized_path.glob("*.jsonl"))[-120:]:
                try:
                    for line in file.read_text(encoding="utf-8").splitlines():
                        try:
                            row = json.loads(line)
                            payload = json.dumps(row, sort_keys=True, separators=(",", ":"))
                            self._event_keys.add(sha256(payload.encode("utf-8")).hexdigest())
                        except (json.JSONDecodeError, TypeError):
                            continue
                except OSError:
                    self.persistence_failure_count += 1

    def _load_recovery_checkpoint(self) -> dict:
        path = self.root / "recovery_state.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _load_last_health_report(self) -> dict:
        path = self.root / "health.jsonl"
        if not path.exists():
            return {}
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            for line in reversed(lines):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
        except OSError:
            self.persistence_failure_count += 1
        return {}

    def _write_recovery_checkpoint(self, report: dict | None = None) -> None:
        checkpoint = {
            "process_id": EventStore._process_id,
            "run_started_at": self._current_run_started_at,
            "timestamp": datetime.now(timezone.utc),
            "persisted_1m_candles": self._persisted_counts[60],
            "persisted_5m_candles": self._persisted_counts[300],
            "last_persisted_1m_timestamp": self._last_persisted_timestamp[60],
            "last_persisted_5m_timestamp": self._last_persisted_timestamp[300],
            "persistence_write_count": self.persistence_write_count,
            "persistence_failure_count": self.persistence_failure_count,
            "duplicate_event_count": self.duplicate_event_count,
            "duplicate_signal_count": self.duplicate_signal_count,
        }
        if report is not None:
            checkpoint["last_health_report"] = report
        target = self.root / "recovery_state.json"
        tmp = target.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(checkpoint, default=_jsonable, separators=(",", ":")), encoding="utf-8")
            tmp.replace(target)
        except OSError:
            self.persistence_failure_count += 1

    def raw_packet(self, received_at: datetime, payload: bytes, response_code: int | None = None) -> None:
        day = received_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        self._append(self.root / "raw" / f"{day}.jsonl", {"received_at": received_at, "response_code": response_code, "payload_hex": payload.hex()})

    def tick(self, received_at: datetime, tick: Tick) -> bool:
        key = self._event_key(tick)
        if key in self._event_keys:
            self.duplicate_event_count += 1
            return False
        self._event_keys.add(key)
        if self._first_tick_after_start is None:
            self._first_tick_after_start = datetime.now(timezone.utc)
        day = received_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        record = asdict(tick)
        record["received_at"] = received_at
        self._append(self.root / "normalized" / f"{day}.jsonl", record)
        self.persistence_write_count += 1
        return True

    def candle(self, candle: Candle) -> bool:
        key = self._candle_key(candle)
        if not candle.complete:
            return False
        if key in self._candle_keys:
            return False
        self._candle_keys.add(key)
        timeframe = candle.timeframe_seconds
        if timeframe in self._persisted_counts:
            self._persisted_counts[timeframe] += 1
            self._last_persisted_timestamp[timeframe] = max(
                self._last_persisted_timestamp[timeframe] or candle.start.astimezone(timezone.utc).isoformat(),
                candle.start.astimezone(timezone.utc).isoformat(),
            )
        if self._first_candle_after_start is None:
            self._first_candle_after_start = datetime.now(timezone.utc)
        day = candle.start.astimezone(timezone.utc).strftime("%Y-%m-%d")
        self._append(self.root / "candles" / f"{day}.jsonl", asdict(candle))
        self.persistence_write_count += 1
        return True

    def signal(self, signal: PullbackSignal) -> bool:
        key = self._signal_key(signal)
        if key in self._signal_keys:
            self.duplicate_signal_count += 1
            return False
        self._signal_keys.add(key)
        day = signal.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d")
        self._append(self.root / "signals" / f"{day}.jsonl", asdict(signal))
        self.persistence_write_count += 1
        return True

    def health(self, report: dict) -> None:
        self._append(self.root / "health.jsonl", report)
        self._write_recovery_checkpoint(report)

    def persistence_snapshot(self) -> dict:
        return {
            "persisted_1m_candles": self._persisted_counts[60],
            "persisted_5m_candles": self._persisted_counts[300],
            "last_persisted_1m_timestamp": self._last_persisted_timestamp[60],
            "last_persisted_5m_timestamp": self._last_persisted_timestamp[300],
            "persistence_write_count": self.persistence_write_count,
            "persistence_failure_count": self.persistence_failure_count,
            "duplicate_event_count": self.duplicate_event_count,
            "duplicate_signal_count": self.duplicate_signal_count,
        }

    def recovery_snapshot(self, current_health: dict | None = None) -> dict:
        previous = self._previous_checkpoint or {}
        pre_1m = int(previous.get("persisted_1m_candles", 0) or 0)
        pre_5m = int(previous.get("persisted_5m_candles", 0) or 0)
        post_1m = self._persisted_counts[60]
        post_5m = self._persisted_counts[300]
        recovered = pre_1m > 0 or pre_5m > 0
        history_restored = post_1m >= pre_1m and post_5m >= pre_5m
        continuity = history_restored and self.duplicate_event_count == 0 and self.duplicate_signal_count == 0
        continued_events = self._first_tick_after_start is not None
        continued_candles = self._first_candle_after_start is not None
        verified = bool(recovered and history_restored and continuity and continued_events and continued_candles and self.persistence_failure_count == 0)
        recovery_started = self._current_run_started_at
        recovery_time = self._first_candle_after_start or self._first_tick_after_start
        duration_ms = None
        if recovery_time is not None:
            duration_ms = max(0.0, (recovery_time - recovery_started).total_seconds() * 1000.0)
        return {
            "restart_recovery_verified": verified,
            "pre_restart_counts": {"1m": pre_1m, "5m": pre_5m},
            "post_restart_counts": {"1m": post_1m, "5m": post_5m},
            "recovered_candle_counts": {"1m": pre_1m, "5m": pre_5m},
            "recovered_event_state": {
                "history_restored": history_restored,
                "ticks_resumed": continued_events,
                "candles_resumed": continued_candles,
                "duplicate_events": self.duplicate_event_count,
                "duplicate_signals": self.duplicate_signal_count,
            },
            "duplicate_count": self.duplicate_event_count + self.duplicate_signal_count,
            "continuity_status": "PASS" if continuity else "FAIL",
            "recovery_timestamp": recovery_time.isoformat() if recovery_time else None,
            "recovery_duration_ms": duration_ms,
        }

    def counter_progression(self, current_health: dict) -> dict:
        previous = self._previous_health_report or {}
        keys = ("accepted_tick_count", "ticks_sent_to_candle_engine", "completed_1m_candles", "completed_5m_candles", "persisted_candle_count_1m", "persisted_candle_count_5m")
        before = {key: previous.get(key) for key in keys}
        after = {key: current_health.get(key) for key in keys}
        comparable = any(value is not None for value in before.values())
        if not comparable:
            verified = False
        else:
            verified = all((after[k] or 0) >= (before[k] or 0) for k in keys)
        return {
            "counter_progression_verified": verified,
            "before": before,
            "after": after,
            "before_timestamp": previous.get("generated_at"),
            "after_timestamp": current_health.get("generated_at"),
        }

    def recent_candles(self, instrument_id: int, timeframe_seconds: int, limit: int = 2500) -> list[Candle]:
        """Read persisted completed candles for deterministic context hydration."""
        path = self.root / "candles"
        if not path.exists():
            return []
        found = []
        files = sorted(path.glob("*.jsonl"))[-120:]
        for file in files:
            try:
                for line in file.read_text(encoding="utf-8").splitlines():
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if int(row.get("instrument_id", -1)) != instrument_id or int(row.get("timeframe_seconds", 0)) != timeframe_seconds or not row.get("complete"):
                        continue
                    found.append(Candle(
                        instrument_id,
                        datetime.fromisoformat(row["start"]),
                        datetime.fromisoformat(row["end"]),
                        Decimal(row["open"]),
                        Decimal(row["high"]),
                        Decimal(row["low"]),
                        Decimal(row["close"]),
                        int(row["volume"]),
                        True,
                        timeframe_seconds,
                    ))
            except OSError:
                continue
        unique = {c.start: c for c in found}
        return [unique[k] for k in sorted(unique)[-limit:]]
