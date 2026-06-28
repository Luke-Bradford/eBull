---
name: ranking-engine
description: eBull deterministic scoring + ranking — the v1.3 model (families, penalties, Calmar reward, completeness), where it's stored and read, and the invariants it must preserve.
---

# ranking-engine

## When to use

Any change to `app/services/scoring.py`, the `scores` table, the ranked-candidate
read path (`app/services/portfolio.py::_load_ranked_scores`), or the
`/recommendations` / score endpoints. Also read it before touching how the
portfolio action layer consumes a score.

## What it is

`compute_score(instrument_id, conn, model_version)` produces one `ScoreResult`
per instrument; `compute_rankings(conn)` scores every eligible instrument, sorts
by `total_score` desc, assigns `rank` + `rank_delta`, and appends rows to
`scores`. It is **heuristic, explicit, auditable — no ML, no hidden weights, no
cohort-relative normalization** (settled decision).

Eligibility (`compute_rankings`): `is_tradable = TRUE`, a `coverage` row with
`filings_status = 'analysable'`, and >=1 of {thesis, fundamentals snapshot, price
data}. No tier gate — T3 names are scored so the weekly coverage review can
promote on deterministic signal alone.

## The v1.3 model

Default `model_version = "v1.3-balanced"`. `model_version` encodes the scoring
mode; `rank_delta` is only ever compared **within the same model_version**.

**Six families** (weighted sum -> `raw_total` in [0,1]):

| Family | Weight | Source |
|---|---|---|
| quality | .25 | margins, FCF, leverage (`fundamentals_snapshot`) |
| value | .25 | thesis bear/base bands -> upside; else fundamentals + price-target |
| turnaround | .20 | margin/revenue trend, filing red flags, debt stress |
| confidence | .15 | thesis `confidence_score` |
| momentum | .10 | returns + TA suite (SMA200, MACD, RSI, ...) |
| sentiment | .05 | importance-weighted news sentiment (30d lookback) |

**Penalties — additive, never multiplicative** (settled decision). Stale thesis,
low thesis confidence, missing-critical-data (only when fundamentals AND thesis
AND price are ALL absent), wide spread, plus the v1.2 **realized-risk penalty**
(tiered additive deductions for high realized vol / deep drawdown, `risk_v1` 3y
metrics; thresholds are explicit constants calibrated to the universe tail,
applied identically every run — NOT cohort-relative). Market-beta-vs-SPY is
deliberately excluded (full-population r2: noise for this universe).

**Calmar reward (v1.3, #1635)** — additive, mode-scaled, gated on `tr_status` in
{ok, no_dividends} from the SEC-derived total-return series; `tr_incomplete`
falls back to price-return Calmar + caveat. Thresholds at the universe
`tr_calmar` p75/p90.

`total_score = clip(raw_total - total_penalty + total_reward)`.

## Data completeness `C` (#1815 §4 / #1820) — evidence, not a score input

`_data_completeness(...)` (pure, table-tested) returns `(C in [0,1], tier)`:

`C = 0.30*fund + 0.30*filing + 0.15*thesis + 0.15*price + 0.10*news`, each
component graded 0 / 0.5 / 1.0 on documented thresholds (fund present; 10-K/10-Q
<=15mo/<=27mo; thesis <=90d; >=252/>=63 trading days; >=3/>=1 news in 90d). It
surfaces **missingness as missingness — never neutral-fill**. Tiers:
`insufficient_data` (C<0.40), `thin_data` (<0.70), `full`. Stored on
`scores.data_completeness` + `scores.completeness_tier`.

`C` does **not** change `total_score` — it's additive evidence (so
`model_version` is NOT bumped; same blessing as the `risk_v1` evidence layer).
Its one live effect is in the **action layer**, not scoring: a
`completeness_tier = 'insufficient_data'` name is capped at HOLD
(`_evaluate_buy` / `_evaluate_add` short-circuit). New signals (F-score, Z,
insider, 13F, SI) and the hybrid peer grade are evidence-only at weight 0 until a
backtest (#1815 §8) + operator sign-off promotes them.

## Storage + read

- Write: `_insert_score` — append-only, never mutates prior rows. `scores`
  carries every family score, `raw_total`, `total_score`, `penalties_json`
  (penalties AND rewards, disambiguated by `kind`), `explanation`, `rank`,
  `rank_delta`, and the two completeness columns. No separate rankings table
  (settled decision).
- Read for recommendations: `_load_ranked_scores` takes the latest score per
  instrument for the model_version, gated again on `filings_status='analysable'`
  so stale-ineligible scores don't surface.

## Invariants (do not break)

- No cohort-relative normalization in the headline score.
- Penalties additive only.
- `model_version` includes the mode; bump it only when an EXISTING metric's
  computation changes — additive nullable evidence columns land under the same
  version.
- `scores` is append-only; `rank_delta` compares within a model_version against
  the most recent prior run only.
- Each score row must carry enough detail to explain how it was produced.

## Failure conditions

- Missing critical source data, stale timestamps beyond threshold, or
  contradictory evidence without explicit uncertainty handling. Surface these as
  completeness/penalty signals — never paper over them with a neutral default.
