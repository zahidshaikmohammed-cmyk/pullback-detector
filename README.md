# Pullback Detector

Real-time Indian stock market pullback detection and statistical validation engine.

## Status

**Architecture:** implemented baseline production foundation  
**Live Dhan validation:** not yet claimed — real Dhan market packets must be received first.  
**Credentials:** environment/GitHub Secrets/Render secret environment variables only.

## What is included

- DhanHQ v2 live WebSocket adapter and binary ticker/quote protocol decoding
- Dhan v2 historical intraday OHLCV ingestion
- Fixed-interval candle aggregation
- Configurable instrument-universe abstraction
- Transparent, reproducible pullback detector baseline
- Forward-horizon empirical backtest evaluator
- Cooldown-aware webhook alerts
- Environment-backed configuration and logging
- Unit tests and GitHub Actions CI
- Docker and Render worker deployment baseline
- Independent architecture documentation

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
cp .env.example .env
pytest
python -m pullback_detector
```

Do not put real credentials into `.env` when working with a shared machine or repository. `.env` is gitignored. Production credentials should be supplied as secrets.

## Dhan credentials

Set:

- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`

For GitHub Actions, store them as repository/environment Secrets and expose them only to workflows/jobs that need them. For Render, configure them as secret environment variables. Never commit them.

## Live feed

Dhan v2 uses a WebSocket endpoint with authentication in the connection URL. Subscription requests are JSON, while market-data responses are binary. The implementation decodes the documented ticker packet and ignores unsupported response packet types until dedicated decoders are added.

## Historical data

The historical client uses Dhan v2 `/charts/intraday` for minute candles. Historical data is the basis for replay, backtesting, and empirical validation.

## Instrument universe

`config/instruments.csv` currently contains only the schema header. No unverified security IDs are hard-coded. Populate it with verified Dhan mappings before live subscriptions.

## Detection

The current detector is deliberately a baseline, not a claim of predictive superiority. Its thresholds are explicit and testable. Future improvements should be evaluated against out-of-sample historical data rather than being accepted because they look good on a single period.

## Deployment

`Dockerfile` and `render.yaml` define a Render worker baseline. Configure Dhan credentials as Render secrets. The service should be connected to real market data and observed before enabling production alerts.

## Documentation

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for system boundaries, data flow, security, and validation rules.
