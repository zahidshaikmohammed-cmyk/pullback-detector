import json
from datetime import datetime, timezone
from pathlib import Path

from pullback_detector.dashboard import DashboardData, HTML


def test_dashboard_projects_universe_ticks_candles_and_signals(tmp_path: Path):
    root = tmp_path / "runtime"
    root.mkdir()
    (root / "universe.csv").write_text(
        "security_id,exchange_segment,symbol,trading_symbol,instrument_type,series,isin,source\n"
        "25,NSE_EQ,RELIANCE,RELIANCE,EQUITY,,IN0000000001,dhan_scrip_master\n",
        encoding="utf-8",
    )
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    (root / "normalized").mkdir()
    (root / "candles").mkdir()
    (root / "signals").mkdir()
    (root / "normalized" / f"{day}.jsonl").write_text(
        json.dumps({
            "instrument_id": 25,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "price": "3000.50",
        }) + "\n",
        encoding="utf-8",
    )
    candle = {
        "instrument_id": 25,
        "start": datetime.now(timezone.utc).isoformat(),
        "end": datetime.now(timezone.utc).isoformat(),
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "direction": "LONG",
        "impulse_start": "2980",
        "impulse_end": "3020",
        "retracement": 0.35,
        "trigger_price": "3010",
        "invalidation_level": "2990",
        "confidence_score": 0.82,
        "experimental_v1": True,
    }
    (root / "signals" / f"{day}.jsonl").write_text(json.dumps(signal) + "\n", encoding="utf-8")

    state = {"status": "live", "last_report": {"accepted_tick_count": 1}, "last_error": None}
    data = DashboardData(root, state).snapshot()

    assert data["health"]["service_status"] == "live"
    assert data["instruments"][0]["trading_symbol"] == "RELIANCE"
    assert data["instruments"][0]["latest_price"] == "3000.50"
    assert data["instruments"][0]["candle_5m"]["close"] == "3000.50"
    assert data["active_signals"][0]["direction"] == "LONG"
    assert data["recent_signals"][0]["confidence_score"] == 0.82


def test_dashboard_is_browser_html_not_json():
    assert "<!doctype html>" in HTML.lower()
    assert "Pullback Detector" in HTML
    assert "/api/dashboard" in HTML
