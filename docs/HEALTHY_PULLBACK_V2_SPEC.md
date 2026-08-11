# Healthy Pullback Qualification Engine V2

Status: **EXPERIMENTAL / NOT PROFITABILITY VALIDATED / NOT INVESTMENT ADVICE**

V2 is a deterministic rule engine. It is a market hypothesis intended for chronological measurement against V1, not a claim of profitability or institutional quality.

## Definition

A qualifying pullback is a process: directional expansion -> validated impulse -> countertrend digestion -> protected structure -> subordinate countertrend pressure -> re-acceleration -> continuation trigger.

Retracement depth is contextual. It is never sufficient by itself.

## Data and information availability

- Structural timeframe: completed NSE_EQ 5-minute candles.
- Minimum production history: 50 completed candles.
- ATR: ATR(14), with rolling ATR percentile available.
- Candle OHLC and volume are validated before analysis.
- Confirmed swing requires two completed candles on each side. The swing is usable only after those two following candles complete; this confirmation delay is part of the no-look-ahead model.
- No unfinished 5-minute candle can create a structural signal.
- 1-minute/tick monitoring remains an execution/lifecycle concern.

## Session rules

Default continuous-session buckets are Opening 09:15-10:00, Morning 10:00-12:00, Midday 12:00-14:00, Afternoon 14:00-15:15 IST. 15:15 onward is not treated as ordinary continuous-market behaviour. NSE's current market-timing page lists the normal equity market through 15:30 and a separate closing session at 15:40-16:00; therefore the engine tags post-continuous behaviour and does not create ordinary continuous-market pullback alerts there. citeturn0search0turn0search3

## Impulse qualification

A bullish impulse is a confirmed swing low followed by a confirmed/active swing high. Bearish is the inverse.

Hard filters:

- displacement >= 1.25 ATR14
- directional efficiency >= 0.50
- directional candle ratio >= 0.55
- maximum countertrend excursion <= 0.35 of impulse displacement

Preferred values are 1.75 ATR, 0.65 efficiency and 0.65 directional ratio; these improve score but do not automatically reject an otherwise valid setup.

Median body/range below 0.45 is a warning. It becomes a rejection only when combined with poor efficiency/consistency. Final-segment exhaustion is a score/risk factor rather than a universal large-candle rejection.

## Pullback qualification

Depth is measured from the actual structural extreme:

- <0.15: insufficient pullback
- 0.15-0.60: normal candidate zone
- 0.60-0.75: deep warning
- >0.75: excessive retracement / rejection

Maximum duration is 12 completed 5-minute candles, but duration alone does not define quality.

Relative pullback speed = pullback speed / impulse speed:

- <0.50 controlled
- 0.50-0.80 acceptable
- 0.80-1.00 warning
- >1.10 hard rejection: COUNTERTREND_ACCELERATION

Pullback efficiency = countertrend net displacement / total countertrend movement:

- <0.70 acceptable
- 0.70-0.80 warning
- >0.80 reversal risk
- >0.85 hard rejection

Countertrend body expansion is rejected only when median countertrend body >1.25x median impulse body **and** countertrend efficiency is elevated.

## Structure and reversal

The impulse-origin swing is the protected structural point. Breaking it is an immediate rejection. Internal highs/lows are not automatically treated as reversals.

Reversal evidence is conjunctive: structure break, excessive countertrend efficiency/speed, major structural violation, or aggressive opposing participation combined with aggressive price movement.

## Participation

No rule says volume must decline on every pullback. Volume is contextual. Pullback/impulse volume ratio >1.20 is a warning. Ratio >1.30 plus countertrend efficiency >0.70 is rejection-level reversal evidence.

## Chop

Overlap alone is not rejection. Severe chop requires agreement between high overlap, low efficiency and more than four meaningful internal swings.

## Location

Prior swings, breakout levels, VWAP, opening range, prior consolidation and high-volume areas can add score when supplied by the existing market context. They are not mandatory directional rules.

## Continuation trigger

A bullish trigger is a completed 5-minute close above the highest meaningful pullback high; bearish is below the lowest meaningful pullback low. Minimum displacement is 0.10 ATR beyond the trigger. An abnormal spike that immediately lacks orderly continuation is treated as trigger risk/failure rather than automatic success.

Only TRIGGER_CONFIRMED creates a signal. WATCHING and candidate states are not alerts.

## Score

100-point experimental score:

- impulse quality: 20
- impulse efficiency/consistency: 15
- pullback behaviour: 20
- structure: 15
- participation: 10
- location/context: 5
- continuation: 15

Candidate threshold: 75. Live alert threshold: 82. Critical hard rejection always overrides score.

## Critical rejection reasons

DATA_INVALID, WEAK_IMPULSE, IMPULSE_COUNTERTREND_INSTABILITY, PROTECTED_STRUCTURE_BROKEN, EXCESSIVE_RETRACEMENT, COUNTERTREND_ACCELERATION, PULLBACK_REVERSAL_EVIDENCE, SEVERE_CHOP, TRIGGER_FAILURE, STALE_DATA, SESSION_INVALID.

## State machine

WATCHING -> IMPULSE_DETECTED -> IMPULSE_VALIDATED -> PULLBACK_DEVELOPING -> HEALTHY_CANDIDATE -> TRIGGER_PENDING -> TRIGGER_CONFIRMED -> ACTIVE -> terminal outcome -> COOLDOWN.

Failure can occur from any qualifying stage when a critical rejection is reached.

## Immutable signal snapshot

At trigger time, the lifecycle freezes signal ID, direction, structural origin/extreme, impulse metrics, pullback depth/duration, health score, trigger, invalidation, 1R target, 2R target and creation timestamp. The existing lifecycle ledger remains append-only.

## Audit trail

V2 records candidate_created, candidate_updated, candidate_rejected and candidate_triggered events. Active/terminal lifecycle events continue through the existing setup lifecycle ledger.

## V1 benchmark

The original detector remains in `pullback_detector/v1_detector.py` as `V1PullbackDetector`. Historical evaluation must be chronological and must not use future candles. V2 thresholds are hypotheses and must be validated on train/validation/test periods without optimizing on the reported test period.

## Known limitations

- Location/context scoring is intentionally conservative until the existing market-context layer provides those features.
- Historical profitability is not established.
- V2 should be benchmarked against V1 before any threshold tuning.
- NSE session schedules can change; session boundaries are configuration, not hard-coded exchange policy.
