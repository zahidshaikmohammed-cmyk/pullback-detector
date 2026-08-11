# Pullback Detector

Production-grade market-data foundation for Indian liquid-stock pullback research.

## Current stage

**Stage:** live connectivity + experimental V1 detection engineering  
**V1:** **EXPERIMENTAL — NOT PROFITABILITY VALIDATED — NOT INVESTMENT ADVICE**  
**Trading alerts:** intentionally disabled  
**Live Dhan validation:** considered successful only after real NSE-session binary Dhan packets are accepted and preserved in an evidence artifact.  
**Credentials:** environment variables / GitHub Secrets / Render secret environment variables only.

## Connectivity architecture

1. Fetch Dhan's official `api-scrip-master.csv` at runtime.
2. Resolve a configurable set of liquid NSE equity symbols to current Dhan Security IDs. IDs are never guessed or hard-coded.
3. Verify every resolved ID through Dhan's Market Quote LTP API.
4. Subscribe to multiple verified instruments using Dhan v2 Quote packets (`RequestCode=17`).
5. Persist every raw binary packet and every accepted normalized tick.
6. Decode Dhan's little-endian binary protocol with strict packet-length validation.
7. Reject malformed, stale, future, and exact-duplicate events.
8. During the NSE cash session only, a verified large positive source-clock skew may be normalized to receipt time for live ordering/candle timing; the original Dhan source timestamp and measured skew are preserved separately. Outside the NSE cash session, future timestamps remain rejected.
9. Aggregate accepted trade ticks into independent 1-minute and 5-minute candles.
10. Emit a health report with connection status, accepted/rejected packets, producing instruments, timestamps, latency, staleness, reconnects, and candle counts.
11. Fail the live validation cycle if no real packets arrive or fewer than the configured number of instruments produce accepted ticks.
12. Feed accepted 5-minute candles into the experimental V1 pullback detector. The detector emits no signal unless its configured pullback anatomy and continuation trigger criteria are met.

Dhan documents the live feed as JSON requests with binary responses, a common 8-byte little-endian response header, Quote packet code `4`, and Quote fields including LTP, last-traded quantity, trade timestamp (EPOCH), and cumulative volume. citeturn1search0turn2search0

## Experimental V1 signal

Every emitted V1 signal contains:

- instrument / symbol identifier
- timestamp
- direction
- impulse metrics
- retracement metrics
- trigger price
- invalidation level
- confidence score
- explicit experimental V1 label

Signals are persisted under `data/runtime/signals/`. No profitability claim is made.

## Live validation

The GitHub Actions workflow `.github/workflows/live-dhan-connectivity.yml` runs manually or on weekday market-hour schedule. It uses only:

- `DHAN_CLIENT_ID` GitHub Secret
- `DHAN_ACCESS_TOKEN` GitHub Secret

It stores the following evidence as a workflow artifact:

- Dhan-derived `universe.csv`
- raw binary packets as hex JSONL
- normalized ticks JSONL
- closed 1-minute/5-minute candles JSONL
- experimental V1 signals JSONL when generated
- `health.jsonl`

A successful workflow run is the authoritative evidence that real Dhan packets were received. A successful unit-test run is **not** live validation.

## Candle volume semantics

The live path subscribes to Dhan Quote packets so it receives last-traded quantity. Candle volume is the sum of last-traded quantities for accepted ticks. Dhan's cumulative day-volume field is retained on normalized ticks for diagnostics but is **not** summed repeatedly into candle volume.

A candle is persisted as complete only after its wall-clock interval has ended. The first live candle after connection is naturally partial if the process starts mid-minute; it is not represented as a complete historical candle.

## Alerts

Trading alerts remain disabled. The system never sends fake/test trading alerts. Alert credentials, if any, are not surfaced in logs or source code.

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

## Deployment

Render runs the existing Docker web service with the health endpoint exposed on the platform-provided `PORT`. Auto-deploy remains enabled from `main`.

## Official Dhan references

- Instrument master: `https://images.dhan.co/api-data/api-scrip-master.csv`
- Dhan v2 Live Market Feed documentation
- Dhan v2 Instrument List documentation
- Dhan v2 Market Quote documentation
- Dhan v2 Historical Data documentation
