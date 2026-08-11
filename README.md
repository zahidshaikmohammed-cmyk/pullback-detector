# Pullback Detector

Production-grade market-data and pullback-research service for liquid Indian equities.

## Current stage

**Stage:** live Dhan v2 ingestion + experimental Healthy Pullback V2 detection  
**V2:** **EXPERIMENTAL — NOT PROFITABILITY VALIDATED — NOT INVESTMENT ADVICE**  
**Trading alerts:** intentionally disabled  
**Live validation:** considered successful only after real NSE-session Dhan packets are accepted and preserved as evidence.  
**Credentials:** environment variables / GitHub Secrets / Render secret environment variables only.

## Live architecture

1. Fetch Dhan's official instrument master at runtime.
2. Resolve the configured liquid NSE equity universe to current Dhan Security IDs; IDs are never guessed or hard-coded.
3. Verify every resolved ID through Dhan Market Quote LTP.
4. Subscribe to the verified instruments using Dhan v2 Quote packets (`RequestCode=17`).
5. Persist raw packets and accepted normalized ticks.
6. Decode and validate Dhan binary packets, rejecting malformed, stale, future, and exact-duplicate events.
7. Aggregate accepted ticks independently into 1-minute and 5-minute OHLCV candles.
8. Maintain one stateful `PullbackDetector` (Healthy Pullback V2) per instrument.
9. Feed completed 5-minute candles into the V2 detector. The detector owns its thresholds from `config/pullback_rules.yaml`.
10. Persist V2 signals, lifecycle events, rejection/audit events, candles, ticks, and health reports.
11. The dashboard API exposes the 20-instrument universe, current/last-known data, V2 anatomy, active setups, history, rejections, performance, and system health.

The production live path does **not** call the legacy V1 detector. The V1 module remains importable only for historical benchmark tests.

## V2 detector interface

`PullbackDetector` requires an `instrument_id` at construction because the V2 detector is stateful per instrument. Its state is initialized immediately; there is no lazy V1-style bootstrap path.

The V2 anatomy is derived from the detector's actual 5-minute state and includes, when available:

- impulse structure and quality;
- pullback depth, duration, speed, efficiency, overlap, and internal swings;
- protected structure and reversal risk;
- volume/participation metrics;
- continuation and trigger state;
- health score and classification.

No price, signal, probability, or market state is fabricated when live data is unavailable.

## Market-closed behavior

Outside NSE cash continuous hours, `MARKET CLOSED` and `OFFLINE / SESSION CLOSED` are normal operational states, not application failures. The service retains and exposes persisted universe data, last-known timestamps/state, historical candles, lifecycle setups, rejection events, and configuration. Missing live values remain `—` / `NO LIVE DATA` rather than invented values.

## Lifecycle

The `PullbackLifecycleEngine` consumes V2 `PullbackSignal` objects and owns setup creation, target/invalidation monitoring, expiry, cooldown, persistence, MFE/MAE, and terminal outcomes.

Lifecycle target multiples are configuration for execution-state tracking; they do not modify V2 detection thresholds.

## Live validation

The GitHub Actions workflow `.github/workflows/live-dhan-connectivity.yml` runs manually or on the weekday market-hour schedule. It uses only:

- `DHAN_CLIENT_ID` GitHub Secret
- `DHAN_ACCESS_TOKEN` GitHub Secret

It stores Dhan-derived evidence under `data/runtime/`. A successful unit-test run is **not** live validation.

During an open NSE cash session, a live validation window with zero accepted Dhan ticks fails validation. Outside the session, zero live ticks is treated as a normal closed-session state.

## Candle volume semantics

The live path subscribes to Dhan Quote packets so it receives last-traded quantity. Candle volume is the sum of last-traded quantities for accepted ticks. Dhan cumulative day volume is retained for diagnostics but is not repeatedly summed into candle volume.

A candle is persisted as complete only after its wall-clock interval has ended.

## Alerts

Trading alerts remain disabled. The system never sends fake/test trading alerts.

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
pytest
```

For real live validation, supply credentials through the environment and run:

```bash
export DHAN_CLIENT_ID="..."
export DHAN_ACCESS_TOKEN="..."
python -m pullback_detector
```

Never put credentials in source, configuration committed to Git, logs, artifacts, or Docker images.

## Deployment

Render runs the existing web service with the health endpoint exposed on the platform-provided `PORT`. Auto-deploy remains enabled from `main`.
