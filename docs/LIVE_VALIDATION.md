# Live Dhan Validation Runbook

## Preconditions

1. Dhan Data APIs are enabled for the account.
2. Create a valid Dhan access token.
3. Add `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN` as GitHub Secrets.
4. Do not paste credentials into issues, commits, logs, workflow YAML, or `.env.example`.

## What the workflow proves

A successful live run proves all of the following in one auditable session:

- Dhan's current official instrument master was fetched.
- Requested NSE equity symbols were resolved to Security IDs from that master.
- Every resolved Security ID returned a Dhan Market Quote LTP response.
- A Dhan v2 WebSocket connection was established.
- Multiple verified instruments produced binary Quote packets.
- Packet lengths and binary fields decoded successfully.
- Tick timestamps/prices/quantities passed validation.
- Raw packets and normalized events were persisted.
- 1-minute and 5-minute candles were formed from accepted trade quantities.
- A health report was written.

## Evidence required for a success claim

The workflow must have:

- log lines containing `LIVE_DHAN_TICK` for real instruments;
- `data/runtime/health.jsonl` with `real_dhan_packets_received: true`;
- at least the configured `MIN_LIVE_INSTRUMENTS` instruments in `instrument_count_received`;
- non-zero `packet_count` and `tick_count`;
- persisted raw packet files and normalized tick files;
- candle counts greater than zero when the session crosses candle boundaries.

A unit-test pass, successful HTTP LTP verification, or successful WebSocket connection by itself is **not** sufficient to claim live market-data validation.

## Failure interpretation

- `LIVE_VALIDATION_FAILED: no real Dhan market packets...` means connectivity/authentication may have worked but the session did not receive market events. Do not mark the system live-validated.
- Invalid authentication/access errors must be fixed at the Dhan account/secret level.
- Invalid Security ID means the universe resolver or Dhan master changed and must be investigated; never substitute a guessed ID.
- Stale/future packets are rejected rather than silently included.

## Current scope

This validation workflow is deliberately data-only. It does not invoke pullback rules and does not send trading alerts.
