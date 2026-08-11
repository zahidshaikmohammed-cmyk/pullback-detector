import httpx

from .models import Instrument


class DhanMarketQuote:
    def __init__(self, client_id: str, access_token: str, base_url: str = "https://api.dhan.co/v2", timeout: float = 15.0):
        if not client_id or not access_token:
            raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are required")
        self.client_id = client_id
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def ltp(self, instruments: list[Instrument]) -> dict[int, float]:
        payload = {"NSE_EQ": [i.security_id for i in instruments]}
        response = httpx.post(
            f"{self.base_url}/marketfeed/ltp",
            headers={"access-token": self.access_token, "client-id": self.client_id, "Content-Type": "application/json"},
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("status") != "success":
            raise RuntimeError(f"Dhan LTP verification failed: {body}")
        data = body.get("data", {}).get("NSE_EQ", {})
        result = {int(k): float(v["last_price"]) for k, v in data.items() if isinstance(v, dict) and "last_price" in v}
        missing = {i.security_id for i in instruments} - set(result)
        if missing:
            raise RuntimeError(f"Dhan LTP verification missing security IDs: {sorted(missing)}")
        return result
