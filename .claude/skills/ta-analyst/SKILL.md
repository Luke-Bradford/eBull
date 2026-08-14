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

⚠ Domain chart-reading knowledge (what a formulation means, its published
source and honest evidence status, the layered chart-read protocol) lives in
`.claude/skills/market-technician/SKILL.md` — this file stays OUR encoding
only. The platform's regime + level modules (`app/services/market_regime.py`,
`app/services/price_levels.py`, both 2026-08-14, S5-S10 §1–§2) postdate this
skill and belong to the strategy platform side of the boundary below.

## ⚠ Scope boundary — this skill is CURRENT-STATE TA only

Everything below describes TA as it works **today**: indicators computed on the
latest bar, read by scoring / entry_timing / thesis to answer *"what is this
chart saying right now"*. That is a different system from the **TA strategy
platform** (#2240) being built alongside it, and conflating them will send you
to the wrong tables and the wrong assumptions.

| | current-state TA (this skill) | TA strategy platform (#2240) |
| --- | --- | --- |
| question | what is the chart saying now | did this pattern ever pay |
| storage | latest bar only (median **1** bar/instrument has indicators) | full history over a research corpus |
| price source | `price_daily`, eToro-sourced | **licensed research corpus** — eToro is the execution venue, not the data source |
| consumers | scoring, entry_timing, thesis | strategy registry, signal ledger, backtester |

Two facts from that programme that change how you should read this skill:

- **eToro bars are Bid-derived and unadjusted.** Measured 2026-08-04 against raw
  public closes on ~1,035 bars each across 7 instruments: level bias a consistent
  **−0.14% to −0.22%** (a half-spread below public), daily-return correlation
  **0.963–0.996**, median RSI-14 difference **0.12–0.19 points**, SMA-200 regime
  agreement **99.4–100%**. So indicator *values* here are sound, but the series
  is price-only (no dividend adjustment) and sits a half-spread low.
- **eToro history caps at ~4 years** (1,000-bar API ceiling, #603). Public data
  reaches decades further. Do not conclude from `price_daily` that a given
  history depth is unavailable — that is a property of our provider, not the
  market.
- ⚠ **Depth is free; survivorship is not** (#2284, 2026-08-05). Do not read the
  bullet above as "the free feeds solve the corpus". Measured over all **382**
  common-equity delistings of 2023 (EDGAR Form 25, filtered per
  `data-sources/sec-edgar.md` §2.6): the free retail feed serves **0%** whose
  series stops at the delisting — 87.4% are absent outright, and the 12.6% that
  resolve are the ticker's *current* occupant (successor entity, OTC
  continuation, or an unrelated company that later took the symbol). Any
  backtest built on symbol lookups against a live feed selects only survivors,
  and nothing in the response says so.

### The platform's own TA module — `price_structure.py` (#2279, phase 2b)

`app/services/price_structure.py` is the strategy platform's pure-function
module, and it is **not** part of the current-state TA described below. Do not
reach for it when answering "what is this chart saying now", and do not add
scalar indicators to it.

| | `technical_analysis.py` | `price_structure.py` |
| --- | --- | --- |
| answers | what is this number on the last N closes | objects with **identity over time** — a swing has a bar, a level has a touch count |
| output | floats keyed by `price_daily` column | dataclasses carrying a `state` tri-state and a `confirmed_index` |
| reads | `price_daily` (eToro, ~4y) | `research_price_daily` (#2282, 1962→2026) |
| stored? | yes, on `price_daily` | **no** — recomputed; see the spec's §6 |

Three things about it that are load-bearing and easy to break:

- **`confirmed_index = index + n`, always.** A pivot happened at `index` but was
  not knowable until `index + n`. Anything that reads a swing price without
  gating on the confirmation has leaked look-ahead. This is why the N-bar
  fractal was chosen over a percentage ZigZag — `pandas_ta.zigzag`'s lag is
  data-dependent and measured at up to **38 bars** on our own panel.
- **Every result carries a `state` tri-state**, never a bare empty collection.
  `not_fired` (no structure here) and `not_evaluable` (cannot say) are different
  facts and collapsing them corrupts a win-rate denominator.
- **`universe` is a required keyword with no default** — the research corpus is
  survivor-only and the label is mandatory (#2284). A default would let a caller
  drop it by accident.

Its constants (`SWING_LADDER` 5/21/63, `CLUSTER_ATR_K` 0.5,
`BANDWIDTH_LOOKBACK` 126) are hashed into `RULE_SET_VERSION`. Changing one is a
rule change that invalidates dependent signals — not a refactor.

⚠ **`pandas-ta` cannot be installed here** (it needs `numba`, which caps at
Python < 3.14; this repo is 3.14.4) and **TA-Lib has no swing/pivot/fractal
function at all** — 161 functions, the closest are `MIN`/`MAX`/`MINMAX` and
`SAR`. Both were tested, not assumed. Do not re-run that search from scratch;
the evidence is in the spec's §2.

Before any work on backtesting, strategy definitions, signal recording or
historical TA, read
`docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` — especially §0
(what the data actually permits), §3.5 (execution semantics — the fill-timing
rule that prevents look-ahead) and §5 (the validity gates). Do not design a
backtest from this skill alone; it does not cover the failure modes that make
backtests lie.

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
