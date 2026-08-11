import json
from datetime import datetime, timezone
from pathlib import Path

from pullback_detector.dashboard import DashboardData, HTML


def test_dashboard_projects_universe_ticks_candles_and_v2_state(tmp_path: Path):
    root = tmp_path / "runtime"
    root.mkdir()
    (root / "universe.csv").write_text(
        "security_id,exchange_segment,symbol,trading_symbol,instrument_type,series,isin,source\n"
        "25,NSE_EQ,RELIANCE,RELIANCE,EQUITY,,IN0000000001,dhan_scrip_master\n",
        encoding="utf-8",
    )
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for name in ("normalized", "candles"):
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

    state = {"status": "live", "last_report": {"accepted_tick_count": 1}, "last_error": None}
    data = DashboardData(root, state).snapshot()

    assert data["health"]["service_status"] == "live"
    assert len(data["instruments"]) == 1
    instrument = data["instruments"][0]
    assert instrument["symbol"] == "RELIANCE"
    assert instrument["price"] == "3000.50"
    assert instrument["candle_5m_history"][0]["close"] == "3000.50"
    assert instrument["state"] == "SCANNING"
    assert instrument["v2_state"] == "WATCHING"
    assert instrument["price_source"] == "LIVE_TICK"
    assert instrument["next_required_condition"] == "VALIDATED IMPULSE"


def test_dashboard_uses_persisted_anatomy_when_live_state_is_empty(tmp_path: Path):
    root = tmp_path / "runtime"
    (root / "anatomy").mkdir(parents=True)
    (root / "anatomy" / "25.json").write_text(
        json.dumps({
            "instrument_id": 25,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "current_price": "3010.25",
            "state": "PULLBACK_DEVELOPING",
            "impulse_direction": "LONG",
            "health_score": 81,
        }),
        encoding="utf-8",
    )
    (root / "universe.csv").write_text(
        "security_id,exchange_segment,symbol,trading_symbol\n25,NSE_EQ,RELIANCE,RELIANCE\n",
        encoding="utf-8",
    )

    data = DashboardData(root, {"status": "live", "last_report": {}, "last_error": None}).snapshot()
    instrument = data["instruments"][0]
    assert instrument["state"] == "PULLBACK"
    assert instrument["v2_state"] == "PULLBACK_DEVELOPING"
    assert instrument["price_source"] == "LAST_KNOWN_STATE"
    assert instrument["direction"] == "LONG"


def test_dashboard_is_browser_html_not_json():
    assert "<!doctype html>" in HTML.lower()
    assert "pullback monitor" in HTML.lower()
    assert "/api/dashboard" in HTML
    assert "CLICK TO INSPECT ANATOMY" in HTML
