from datetime import date

import httpx
import pandas as pd


class DhanHistoricalClient:
    """Dhan v2 historical candle API client; credentials are runtime-only."""

    def __init__(self, client_id: str, access_token: str, base_url: str = "https://api.dhan.co/v2"):
        self.client_id = client_id
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")

    def _headers(self) -> dict[str, str]:
        if not self.access_token:
            raise RuntimeError("DHAN_ACCESS_TOKEN is required for historical ingestion")
        return {"access-token": self.access_token, "client-id": self.client_id, "Content-Type": "application/json"}

    def intraday(self, security_id: int, exchange_segment: str, from_date: date, to_date: date, interval: int = 5) -> pd.DataFrame:
        if interval not in {1, 5, 15, 25, 60}:
            raise ValueError("Dhan intraday interval must be one of 1, 5, 15, 25, 60 minutes")
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": "EQUITY",
            "interval": str(interval),
            "fromDate": from_date.isoformat(),
            "toDate": to_date.isoformat(),
        }
        with httpx.Client(timeout=30) as client:
            response = client.post(f"{self.base_url}/charts/intraday", headers=self._headers(), json=payload)
            response.raise_for_status()
            body = response.json()
        data = body.get("data", body)
        frame = pd.DataFrame(data)
        if frame.empty:
            return frame
        frame = frame.rename(columns={"timestamp": "timestamp", "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"})
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
        return frame
