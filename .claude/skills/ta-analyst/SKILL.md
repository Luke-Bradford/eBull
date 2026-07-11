---
name: ta-analyst
description: eBull chart-TA interpretation — what each stored indicator means HERE (the encoded ramps and gates, not textbook generalities), where it is computed and stored, who consumes it (scoring momentum family, entry_timing, thesis context block D), and the derived trend signals contract.
---

# ta-analyst

## When to use

Any change to `app/services/technical_analysis.py`, the TA columns on
`price_daily` (sql/025), `_compute_and_store_features` in
`app/services/market_data.py`, the momentum family in
`app/services/scoring.py::_momentum_score`, `app/services/entry_timing.py`,
or the thesis TA context block (`app/services/thesis.py::_shape_ta_state`).
Also read it before citing any TA figure in an operator-facing surface or
prompt — the interpretation rules below are the encoded ones, not textbook
defaults.

## Where TA lives

- **Computation** — `technical_analysis.py`: pure functions, no DB/IO.
  `compute_indicators(bars)` takes oldest-first OHLCV bars and returns a dict
  keyed by `price_daily` column names (floats or None).
- **Persistence** — `market_data.py::_compute_and_store_features`, called on
  every candle refresh: reads the newest **400 bars** (close-only for
  returns/volatility; full-OHLCV subset for indicators), UPDATEs the latest
  `price_daily` row. Candle history itself backfills at **1000 trading days**
  (eToro per-request ceiling, #603) on first seed, incremental afterwards.
  TA is written only when the newest complete OHLCV bar matches the row
  being updated — partial candles produce NULL indicators, never
  stale-by-one-day values.
- **Columns** (sql/025): `sma_20/50/200`, `ema_12/26`,
  `macd_line/signal/histogram`, `rsi_14`, `stoch_k/d`, `bb_upper/lower`,
  `atr_14`. Returns (`return_1w..1y`) and `volatility_30d` ride the same
  UPDATE but are computed separately from closes.
- **Derived at read, never stored** — `derive_trend_signals(close, sma_50,
  sma_200)` (#1989): `price_vs_sma200` ("above"/"below"; tie is "below" by
  design, strict `>`), `sma_50_200_regime` ("golden"/"death" — the CURRENT
  50-vs-200 relation, NOT a crossover event; equal-or-missing SMAs yield None:
  missing evidence, not a third regime). Single source; the thesis context
  keys are stable — the writer prompt and eval fixtures depend on them.
  Persisting these was considered and rejected: two already-stored floats
  derive them in O(1), and a stored copy could drift stale.

## Who consumes what

| Consumer | Uses | Encoding |
|---|---|---|
| scoring `_momentum_score` | sma_200, macd_histogram, rsi_14, stoch_k, bb_upper/lower, atr_14 + returns | blend below |
| `entry_timing` | rsi_14, bb_upper/lower, atr_14 | defer/SL gates below |
| thesis context block D | sma_50/200, rsi_14, macd_histogram, atr_14, volatility_30d + derived signals | statused, as-of-stamped (#1987) |

## The encoded interpretation rules (v1.1+ momentum blend)

Momentum family weight in the total score: **0.10** (balanced + conservative
modes), 0.15 (speculative). Inside `_momentum_score`, sub-blend (missing
components renormalize; ALL missing → 0.5 neutral + note):

- **Returns 40%** — 1m (0.20): clip((r+0.10)/0.30); 3m (0.50):
  clip((r+0.15)/0.45); 6m (0.30): clip((r+0.20)/0.60). No TA at all →
  return-only fallback (v1 behaviour).
- **Trend confirmation 25%** — price-vs-SMA200 distance (0.60):
  clip(0.5 + pct_from_sma × 2.5), so ±20% from the 200-day saturates;
  MACD histogram (0.40): normalized to price, clip(0.5 + macd_pct × 20) —
  ±2.5% histogram saturates.
- **Momentum quality 20%** — RSI (0.60), the encoded regime bands:
  `<30` oversold warning (score rsi/60), `30–70` recovery→healthy ramp
  (0.5 + (rsi−30)/80), `>70` overbought decay (1 − (rsi−70)/30).
  Stochastic %K (0.40): same shape with 20/80 bands.
- **Volatility regime 15%** — Bollinger position (0.60):
  (close−lower)/(upper−lower); HIGH position reads as trend *strength* here,
  deliberately opposite to the RSI/stoch overbought treatment which reads
  *exhaustion* risk — do not "fix" one to match the other. ATR (0.40):
  clip(1 − atr_pct × 10) — 10% daily true range zeroes it.

## Entry-timing gates (BUY/ADD only)

`entry_timing.evaluate_entry_conditions` — verdicts `pass`/`defer`/`skip`
(the DB CHECK also allows `error`, written by the scheduler only):

- **Defer** when RSI-14 > **75** (overbought — stricter than the scoring
  band's 70) or price within **95%** of the Bollinger range (overextended).
  Deferred recommendations retry via `deferred_retry`.
- **Stop-loss** = entry − **2.0 × ATR(14)**, floored at 5% below entry
  (`SL_FLOOR_PCT`) and at least 2% below (`SL_MIN_DISTANCE_PCT`) so spread
  noise cannot stop out.

## Thesis-writer usage (block D contract)

`_shape_ta_state` forwards floats + the two derived signals with the price
row's as-of stamp. Interpretation guidance the writer receives lives in
`_WRITER_SYSTEM` — if you change a ramp or gate above, check whether the
prompt's TA guidance still matches (#1632 evidence discipline: statuses
verbatim, no citing absent metrics as numbers).

## Invariants

- `technical_analysis.py` stays pure — no DB, no IO, floats in/out.
- Indicator column names == dict keys == `_TA_COLUMNS` in market_data.py;
  a new indicator must land in all three plus sql migration.
- `derive_trend_signals` is the ONLY producer of the trend-signal strings;
  the context keys `price_vs_sma200` / `sma_50_200_regime` are frozen
  (prompt + eval fixtures).
- RSI/stoch/BB thresholds above are ENCODED behaviour — changing them is a
  scoring-model change (model_version bump territory, operator-gated), not
  a refactor.
