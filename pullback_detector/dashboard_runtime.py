from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .dashboard import DashboardData
from .dhan import DhanWebSocketClient
from .live import LIVE_ANATOMY, LIVE_RUNTIME


def _evidence_count(evidence: Any, timeframe: str):
    if isinstance(evidence, dict):
        item = evidence.get(timeframe)
        if isinstance(item, dict):
            return item.get("candle_count")
    if isinstance(evidence, list):
        for item in evidence:
            if isinstance(item, dict) and str(item.get("timeframe", "")).lower() in {timeframe, timeframe.replace("_", "")}:
                return item.get("candle_count")
    return None


def _context_direction(context: dict) -> dict:
    evidence = context.get("evidence")
    return {
        "available": context.get("data_freshness") not in {None, "NO_DATA", "NO_LIVE_DATA", "CALCULATION_ERROR"},
        "day": {"trend": context.get("day_trend"), "score": context.get("day_score"), "source_candle_count": _evidence_count(evidence, "day")},
        "one_hour": {"trend": context.get("h1_trend"), "score": context.get("h1_score"), "source_candle_count": _evidence_count(evidence, "h1")},
        "fifteen_minute": {"trend": context.get("m15_trend"), "score": context.get("m15_score"), "source_candle_count": _evidence_count(evidence, "m15")},
        "five_minute": {"trend": context.get("m5_trend"), "score": context.get("m5_score"), "source_candle_count": _evidence_count(evidence, "m5")},
        "current": {"trend": context.get("current_trend"), "score": context.get("current_score")},
        "trend_momentum": context.get("momentum_state"),
        "trend_stability": context.get("trend_stability"),
        "calculation_status": "ERROR" if context.get("context_error") else "VALID",
        "latest_candle_timestamp": context.get("timestamp"),
    }


def canonical_snapshot(data: DashboardData) -> dict[str, Any]:
    base = data.snapshot()
    now = datetime.now(timezone.utc)
    runtime_health = LIVE_RUNTIME.get("health")
    expected = list(LIVE_RUNTIME.get("expected_instruments") or [])

    if runtime_health is not None:
        one_min = LIVE_RUNTIME.get("one_min")
        five_min = LIVE_RUNTIME.get("five_min")
        one_state = one_min.state_snapshot() if one_min is not None else {}
        five_state = five_min.state_snapshot() if five_min is not None else {}
        report = runtime_health.report(
            now=now,
            subscribed_instruments=len(expected),
            persisted_1m=int(LIVE_RUNTIME.get("persisted_1m") or 0),
            persisted_5m=int(LIVE_RUNTIME.get("persisted_5m") or 0),
            expected_instruments=expected,
            websocket_connected=bool(LIVE_RUNTIME.get("websocket_connected")),
            restart_recovery_verified=bool(LIVE_RUNTIME.get("restart_recovery_verified")),
        )
        websocket = DhanWebSocketClient.ACTIVE_CLIENT
        websocket_state = websocket.status_snapshot() if websocket is not None else {"websocket_state": "DISCONNECTED", "data_flow_status": "WAITING_FOR_PACKET"}
        report["websocket_watchdog"] = websocket_state
        report["connection_status"] = "CONNECTED" if websocket_state.get("websocket_state") in {"CONNECTED_WAITING_FOR_PACKET", "PACKET_RECEIVED"} else websocket_state.get("websocket_state", "DISCONNECTED")
        report["data_flow_status"] = websocket_state.get("data_flow_status", "WAITING_FOR_PACKET")
        report["feed_state"] = "LIVE" if report.get("accepted_tick_count", 0) > 0 and report.get("global_data_age_seconds") is not None and report["global_data_age_seconds"] <= 60 else "CONNECTED_NO_DATA" if report["connection_status"] == "CONNECTED" else "RECEIVE_TIMEOUT" if websocket_state.get("websocket_state") == "RECEIVE_TIMEOUT" else report.get("feed_state")
        report["dhan_connection_status"] = "connected" if report["connection_status"] == "CONNECTED" else "reconnecting" if websocket_state.get("websocket_state") == "RECONNECTING" else "disconnected"
        report["raw_packet_count"] = runtime_health.packets + runtime_health.duplicate_packets
        report["decoded_packet_count"] = runtime_health.packets
        report["active_1m_buckets"] = one_state.get("open_bars", 0)
        report["active_5m_buckets"] = five_state.get("open_bars", 0)
        report["completed_1m_candles"] = runtime_health.candles_1m
        report["completed_5m_candles"] = runtime_health.candles_5m
        report["aggregator_1m"] = one_state
        report["aggregator_5m"] = five_state

        pipeline_status = "PASS"
        first_failure = report.get("first_failure_reason")
        if runtime_health.ticks_sent_to_candle_engine != runtime_health.ticks:
            pipeline_status = "BLOCKED"
            first_failure = first_failure or "ACCEPTED_TICKS_NOT_ALL_REACHING_CANDLE_ENGINE"
        elif runtime_health.ticks_rejected_by_candle_engine:
            pipeline_status = "BLOCKED"
            first_failure = first_failure or f"CANDLE_ENGINE_REJECTED_TICKS:{runtime_health.ticks_rejected_by_candle_engine}"
        elif runtime_health.candles_1m <= 0:
            pipeline_status = "BLOCKED"
            first_failure = first_failure or "NO_COMPLETED_1M_CANDLES"
        elif runtime_health.candles_5m <= 0:
            pipeline_status = "BLOCKED"
            first_failure = first_failure or "NO_COMPLETED_5M_CANDLES"
        report["pipeline_status"] = pipeline_status
        report["pipeline_first_failure"] = first_failure
        report["pipeline"] = {
            "websocket": report["connection_status"],
            "data_flow": report["data_flow_status"],
            "subscriptions": f"{len(expected)} / 22",
            "producing": f"{len(runtime_health.instruments_seen)} / {len(expected)}",
            "raw_packets": report["raw_packet_count"],
            "decoded_packets": report["decoded_packet_count"],
            "accepted_ticks": runtime_health.ticks,
            "rejected_ticks": runtime_health.malformed_packets,
            "to_candle_engine": runtime_health.ticks_sent_to_candle_engine,
            "candle_engine_rejected": runtime_health.ticks_rejected_by_candle_engine,
            "active_1m_buckets": one_state.get("open_bars", 0),
            "completed_1m_candles": runtime_health.candles_1m,
            "active_5m_buckets": five_state.get("open_bars", 0),
            "completed_5m_candles": runtime_health.candles_5m,
            "persisted_1m_candles": int(LIVE_RUNTIME.get("persisted_1m") or 0),
            "persisted_5m_candles": int(LIVE_RUNTIME.get("persisted_5m") or 0),
            "status": pipeline_status,
            "first_failure": first_failure,
            "not_producing": report.get("not_producing_instruments", []),
            "receive_timeout_count": websocket_state.get("receive_timeout_count", 0),
            "seconds_since_last_packet": websocket_state.get("seconds_since_last_packet"),
        }

        base["health"] = report
        base["canonical_runtime_state"] = True

        by_id = {str(i.get("security_id")): i for i in base.get("instruments", [])}
        live_contexts = LIVE_RUNTIME.get("contexts") or {}
        for expected_item in expected:
            sid = str(expected_item.get("security_id"))
            instrument = by_id.get(sid)
            if instrument is None:
                continue
            instrument_feed_state = report.get("feed_state_by_instrument", {}).get(sid, "NO_DATA")
            instrument["feed_state"] = instrument_feed_state
            instrument["data_age_seconds"] = report.get("data_age_seconds", {}).get(sid)
            live_tick_ts = report.get("last_tick", {}).get(sid)
            if live_tick_ts:
                instrument["timestamp"] = live_tick_ts
                instrument["price"] = report.get("last_price", {}).get(sid)
                instrument["price_source"] = "LIVE_ACCEPTED_TICK"
                instrument["data_status"] = "LIVE" if instrument_feed_state == "LIVE" else instrument_feed_state
            elif instrument_feed_state in {"DISCONNECTED", "STALE", "NO_DATA"}:
                instrument["data_status"] = instrument_feed_state
                instrument["price_source"] = "NO_CURRENT_ACCEPTED_TICK"

            context_obj = live_contexts.get(int(sid)) if sid.isdigit() else None
            context = context_obj.snapshot() if context_obj is not None else {}
            if context:
                instrument["direction_context"] = _context_direction(context)
                instrument["market_context"] = context
                instrument["context_calculation_status"] = "ERROR" if context.get("context_error") else "VALID"
                instrument["context_latest_timestamp"] = context.get("timestamp")
            else:
                instrument["direction_context"] = {"available": False, "reason": "INSUFFICIENT_DATA"}

            if instrument_feed_state != "LIVE":
                instrument["state"] = "STALE_DATA" if instrument_feed_state == "STALE" else "DISCONNECTED" if instrument_feed_state == "DISCONNECTED" else "NO_DATA"
            elif not context:
                instrument["state"] = "NO_DATA"
            elif all(context.get(k) == "INSUFFICIENT_DATA" for k in ("day_trend", "h1_trend", "m15_trend", "m5_trend")):
                instrument["state"] = "WAITING_FOR_HISTORY"
            elif not instrument.get("state") or instrument.get("state") == "NO_DATA":
                instrument["state"] = "SCANNING"

    return base
