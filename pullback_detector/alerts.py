import json
import time
import urllib.request
from dataclasses import asdict

from .models import PullbackSignal


class AlertSink:
    def publish(self, signal: PullbackSignal) -> None:
        raise NotImplementedError


class WebhookAlertSink(AlertSink):
    def __init__(self, url: str, cooldown_seconds: int = 300):
        self.url = url
        self.cooldown_seconds = cooldown_seconds
        self._last_sent: dict[int, float] = {}

    def publish(self, signal: PullbackSignal) -> None:
        if not self.url:
            return
        now = time.monotonic()
        previous = self._last_sent.get(signal.instrument_id, 0.0)
        if now - previous < self.cooldown_seconds:
            return
        body = json.dumps(asdict(signal), default=str).encode()
        request = urllib.request.Request(self.url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=10):
            pass
        self._last_sent[signal.instrument_id] = now
