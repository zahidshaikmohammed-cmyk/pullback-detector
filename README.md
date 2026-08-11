# Pullback Detector

Production-grade market-data foundation for Indian liquid-stock pullback research.

## Current stage

**Stage:** verified-connectivity engineering  
**Final pullback rules:** intentionally not implemented in the live path  
**Trading alerts:** intentionally disabled  
**Live Dhan validation:** only considered successful after real binary Dhan packets are received and preserved in an evidence artifact.  
**Credentials:** environment variables / GitHub Secrets / Render secret environment variables only.

## Connectivity architecture

1. Fetch Dhan's official `api-scrip-master.csv` at runtime.
2. Resolve a configurable set of liquid NSE equity symbols to current Dhan Security IDs. IDs are never guessed or hard-coded.
3. Verify every resolved ID through Dhan's Market Quote LTP API.
4. Subscribe to multiple verified instruments using Dhan v2 Quote packets (`RequestCode=17`).
5. Persist every raw binary packet and every accepted normalized tick.
6. Decode Dhan's little-endian binary protocol with strict packet-length validation.
7. Reject malformed, stale, future, and exact-duplicate events.
8. Aggregate accepted trade ticks into independent 1-minute and 5-minute candles.
9. Emit a health report with instruments received, packet/tick counts, last tick timestamps, latency, staleness, reconnects, malformed/duplicate counts, and candle counts.
10. Fail the live validation job if no real packets arrive or fewer than the configured number of instruments produce packets.

Dhan documents the live feed as JSON requests with binary responses, a common 8-byte response header, Quote packet code `4`, and Quote fields including LTP, last-traded quantity, trade timestamp and cumulative volume. citeturn1search0turn2search0

## Live validation

The GitHub Actions workflow `.github/workflows/live-dhan-connectivity.yml` runs manually or on weekday market-hour schedule. It uses only:

- `DHAN_CLIENT_ID` GitHub Secret
- `DHAN_ACCESS_TOKEN` GitHub Secret

It stores the following evidence as a workflow artifact:

- Dhan-derived `universe.csv`
- raw binary packets as hex JSONL
- normalized ticks JSONL
- closed 1-minute/5-minute candles JSONL
- `health.jsonl`

A successful workflow run is the authoritative evidence that real Dhan packets were received. A successful unit-test run is **not** live validation.

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
pytest
```

To run a real live validation locally, supply credentials through the environment and run:

```bash
export DHAN_CLIENT_ID="..."
export DHAN_ACCESS_TOKEN="..."
python -m pullback_detector
```

Never put credentials in source, configuration committed to Git, logs, artifacts, or Docker images.

## Candle volume semantics

The live path subscribes to Dhan Quote packets so it receives last-traded quantity. Candle volume is the sum of last-traded quantities for accepted ticks. Dhan's cumulative day-volume field is retained on normalized ticks for diagnostics but is **not** summed repeatedly into candle volume.

A candle is persisted as complete only after its wall-clock interval has ended. The first live candle after connection is naturally partial if the process starts mid-minute; it is not represented as a complete historical candle.

## Scope boundary

The current live service is data-only. It does **not** invoke the pullback detector and does **not** send trading alerts. Detection and alert behavior will be added only after connectivity, event quality, candle formation, persistence, and empirical validation are independently established.

## Deployment

Docker and Render worker configuration remain available, with Dhan credentials supplied only as secret environment variables. The scheduled GitHub Actions workflow is the primary connectivity evidence path because its logs and artifacts provide an auditable record of the live session.

## Official Dhan references

- Instrument master: `https://images.dhan.co/api-data/api-scrip-master.csv`
- Dhan v2 Live Market Feed documentation
- Dhan v2 Instrument List documentation
- Dhan v2 Market Quote documentation
- Dhan v2 Historical Data documentation
