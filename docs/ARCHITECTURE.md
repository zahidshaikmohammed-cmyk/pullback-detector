# Pullback Detector Architecture

## Scope

This repository is an independent research and production service for detecting and evaluating pullbacks in liquid Indian equities. It does not depend on, inherit rules from, or share assumptions with another trading architecture.

## Data flow

1. **Instrument universe** — verified Dhan security IDs and exchange segments are maintained in `config/instruments.csv` or another approved source.
2. **Historical ingestion** — Dhan v2 intraday OHLCV API supplies replay/backtest candles.
3. **Live ingestion** — Dhan v2 WebSocket supplies tick/quote packets. Dhan responses are binary and are decoded by `dhan_protocol.py`.
4. **Candle aggregation** — ticks are converted into deterministic fixed-interval OHLCV candles.
5. **Detection engine** — the baseline detector emits explicit, reproducible pullback signals.
6. **Validation** — historical candles and generated signals are evaluated with forward-horizon outcomes. Results are empirical measurements, not guarantees.
7. **Alerts** — optional webhook delivery has per-instrument cooldown protection.
8. **Operations** — structured configuration, logging, tests, Docker, CI, and Render worker deployment support production operation.

## Security

Dhan credentials are never stored in source code, config files, tests, Docker images, or Git history. Supply `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN` through environment variables. For GitHub Actions use repository/environment Secrets; for Render use secret environment variables.

## Live-validation rule

A build, unit test, protocol parser test, or historical backtest is **not** live validation. Live validation may only be reported after the running service successfully authenticates to Dhan and receives real Dhan market packets. The repository intentionally does not fabricate such a claim.

## Testing strategy

- Unit-test candle construction.
- Unit-test Dhan binary packet decoding using synthetic protocol fixtures.
- Unit-test detector behavior with deterministic candles.
- Add integration tests with recorded, non-secret Dhan packets as fixtures.
- Add controlled historical replay tests before enabling production alerts.
