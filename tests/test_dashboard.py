import json
from datetime import datetime, timezone
from pathlib import Path

from pullback_detector.dashboard import DashboardData, HTML


def test_dashboard_projects_universe_ticks_candles_and_v2_signals(tmp_path: Path):
    root = tmp_path / "runtime"
    root.mkdir()
    (root / "universe.csv").write_text(
        "security_id,exchange_segment,symbol,trading_symbol,instrument_type,series,isin,source\n"
        "25,NSE_EQ,RELIANCE,RELIANCE,EQUITY,,IN0000000001,dhan_scrip_master\n",
        encoding="utf-8",
    )
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for name in ("normalized", "candles", "signals"):
        (root / name).mkdir()
    now = datetime.now(timezone.utc)
    (root / "normalized" / f"{day}.jsonl").write_text(
        json.dumps({
            "instrument_id": 25,
            "timestamp": now.isoformat(),
            "received_at": now.isoformat(),
            "price": "3000.50",
        }) + "\n",
        encoding="utf-8",
    )
    candle = {
        "instrument_id": 25,
        "start": now.isoformat(),
        "end": now.isoformat(),
        "open": "2990",
        "high": "3010",
        "low": "2985",
        "close": "3000.50",
        "volume": 100,
        "complete": True,
        "timeframe_seconds": 300,
    }
    (root / "candles" / f"{day}.jsonl").write_text(json.dumps(candle) + "\n", encoding="utf-8")
    signal = {
        "instrument_id": 25,
        "timestamp": now.isoformat(),
        "direction": "LONG",
        "impulse_start": "2980",
        "impulse_end": "3020",
        "retracement": 0.35,
        "trigger_price": "3010",
        "invalidation_level": "2990",
        "confidence_score": 0.82,
        "experimental_v1": False,
        "health_score": 82,
        "classification": "TRIGGER_CONFIRMED",
    }
    (root / "signals" / f"{day}.jsonl").write_text(json.dumps(signal) + "\n", encoding="utf-8")

    state = {"status": "live", "last_report": {"accepted_tick_count": 1}, "last_error": None}
    data = DashboardData(root, state).snapshot()

    assert data["health"]["service_status"] == "live"
    assert len(data["instruments"]) == 1
    assert data["instruments"][0]["symbol"] == "RELIANCE"
    assert data["instruments"][0]["latest_price"] == "3000.50"
    assert data["instruments"][0]["candle_5m_history"][0]["close"] == "3000.50"
    assert data["instruments"][0]["state"] == "WATCHING"
    assert data["active_signals"][0]["direction"] == "LONG"
    assert data["active_signals"][0]["classification"] == "TRIGGER_CONFIRMED"


def test_dashboard_is_browser_html_not_json():
    assert "<!doctype html>" in HTML.lower()
    assert "pullback detector" in HTML.lower()
    assert "/api/dashboard" in HTML
