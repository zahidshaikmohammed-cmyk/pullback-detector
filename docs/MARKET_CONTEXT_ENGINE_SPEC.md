# Market Context Engine V1

The Market Context Engine is the deterministic evidence layer upstream of Healthy Pullback V2. It does not generate trades, use an LLM, or invent missing data.

## Pipeline

Dhan accepted ticks -> completed 1m/5m candles -> derived 15m/1h/daily context where history exists -> Market Context Engine -> evidence -> Healthy Pullback V2 -> lifecycle.

## Evidence

Each instrument exposes day, 1H, 15M, 5M and current direction; trend strength/stability; momentum; volatility; VWAP context; relative volume; directional efficiency; confirmed structure; chop score; multi-timeframe alignment; market/relative-strength context when benchmark data exists; pullback state; health score; blocking reason; next required condition; and data freshness.

## Timeframe integrity

Structural calculations use completed candles. The live tick is kept separately for current price. Derived 15M, 1H and daily bars are aggregated only from persisted/completed 5M candles. If enough historical data does not exist, the engine reports `INSUFFICIENT_DATA` rather than infer a value.

## Hard versus soft evidence

Hard failures remain separate from soft evidence. Severe chop, invalid/stale data, future data, invalid structure and V2 critical pullback failures cannot be overridden by a score. VWAP alignment, relative volume, momentum, volatility regime, relative strength and market alignment are contextual evidence.

## Health score

V2 uses the context evidence as part of its multi-factor qualification score: impulse 20, pullback 20, structure 20, participation 15, trend alignment 10, market context 5 and continuation evidence 10. A critical failure overrides the score.

## Benchmarks and sectors

NIFTY/BANK NIFTY and sector context are populated only when reliable benchmark/sector candles are available through the existing data pipeline. Otherwise the API returns `INSUFFICIENT_DATA` / `UNAVAILABLE`; it never fabricates benchmark or sector values.

## No look-ahead

Confirmed swings use completed candles. Historical context uses only candles available at the decision timestamp. No future candle may influence a trend, structure, pullback score, rejection or signal.

## Validation status

This is an explicit deterministic market hypothesis. It is not profitability validated and must be evaluated chronologically against V1 and V2 outcomes before any claim of improvement or profitability.
