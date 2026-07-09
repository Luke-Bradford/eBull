---
name: portfolio-manager
description: eBull deterministic portfolio manager — turns ranked scores into BUY/ADD/HOLD/EXIT/CONSIDERED recommendations under hard position/sector caps in app/services/portfolio.py; recommendations only, never executes.
---

# portfolio-manager

## When to use

Any change to `app/services/portfolio.py` (`run_portfolio_review`,
`_evaluate_buy` / `_evaluate_add` / `_evaluate_exit`, the DB loaders), the
`trade_recommendations` table, the `/recommendations`
(`app/api/recommendations.py`) or `/portfolio` (`app/api/portfolio.py`) read
paths, or the review job
(`app/workers/scheduler.py::compute_morning_recommendations`, dispatched as
`morning_candidate_review`, `app/jobs/runtime.py:294`). Also read it before
changing a policy cap or how a score/thesis feeds an action.

## What it is

`run_portfolio_review(conn, model_version=None)` is the one entry point:
**deterministic, hard-rule, no ML** — it turns the ranked score list into one
action per instrument and persists *recommendations only*; nothing is sent to
eToro here (the execution guard owns that). `model_version` resolves lazily to
`scoring._DEFAULT_MODEL_VERSION` (the ranking-engine default) to avoid a cycle.

Pipeline (`run_portfolio_review`):
1. `_load_ranked_scores` — latest `scores` row per instrument for the
   model_version, re-gated on `coverage.filings_status = 'analysable'` (#268
   Chunk J) so stale-ineligible scores never surface (ranking-engine's read gate).
2. `_load_positions` — mark-to-market: latest `quotes.last` **> 0** else
   `cost_basis` with `quote_is_fallback=True` (#1428: eToro persists `0` for
   un-freshly-traded names — a non-positive mark is not a valid price).
   `_load_cash` = `SUM(cash_ledger.amount)`; empty ledger → cash unknown.
   `_load_mirror_equity` / `load_mirror_breakdowns` add copy-trading equity
   (`copy_mirrors` / `copy_mirror_positions`, 3-tier price: quote → `price_daily`
   → open rate). AUM = Σ market_value + cash (0 when unknown) + mirror_equity.
3. `_load_instrument_details` — latest `theses` row (stance, confidence_score,
   buy_zone, base_value, break_conditions_json), previous thesis confidence,
   `MAX(filing_events.red_flag_score)` over 90d, and `instruments.sector` (eToro
   industry id — the cap key) resolved via `etoro_stocks_industries` to a label
   for operator-facing reasons only.
4. Evaluate **held first** (EXIT → ADD → HOLD) then **unowned ranked** (BUY, else
   CONSIDERED). Order is load-bearing: the `pending_sector_pct` accumulator only
   tracks in-flight BUYs, so ADDs must run before BUY accumulation begins (#42).

Action priority, highest wins (`Action` Literal, `portfolio.py:73`): **EXIT**
(thesis break + red_flag ≥ 0.80, or price ≥ thesis base_value) → **ADD** →
**BUY** → **HOLD** default; blocked unowned candidates fall to **CONSIDERED**.

Policy constants (single source of truth for `docs/trading-policy.md`; `portfolio.py:47-67`):

| constant | value | rule |
|---|---|---|
| `MAX_ACTIVE_POSITIONS` | 20 | held + in-flight BUYs |
| `MAX_INITIAL_POSITION_PCT` | 0.05 | BUY size |
| `MAX_FULL_POSITION_PCT` | 0.10 | ADD ceiling |
| `MAX_SECTOR_EXPOSURE_PCT` | 0.25 | strict `>` breach (landing at 25% ok) |
| `MIN_BUY_SCORE` | 0.35 | floor for any BUY |
| `MIN_SCORE_ONLY_BUY` | 0.55 | BUY with no thesis (autonomous, no AI spend) |
| `ADD_MIN_CONFIDENCE_DELTA` / `ADD_MIN_SCORE_DELTA` | 0.05 / 0.05 | ADD conviction — need ≥1 delta |
| `EXIT_RED_FLAG_THRESHOLD` | 0.80 | severe-red-flag EXIT gate |

- **Tables:** `trade_recommendations` (`sql/001_init.sql:125`; `score_id` /
  `model_version` / `cash_balance_known` added `sql/009`; `status='considered'`
  added `sql/209`), `positions` (`sql/001:159`), `cash_ledger` (`sql/001:170`),
  read-only inputs `scores` / `coverage` / `theses` / `filing_events` /
  `instruments` / `quotes` / `price_daily`, copy-trading `copy_mirrors` /
  `copy_mirror_positions` / `copy_traders` (`sql/022`) + closed archive
  `copy_mirror_closed_positions` (`sql/214`, #1927).
- **Endpoints:** `GET /recommendations` + `/recommendations/{id}`
  (`app/api/recommendations.py`); read-only portfolio views under `/portfolio`
  (`GET`, `/instruments/{id}`, `/rolling-pnl`, `/value-history`, `/activity` —
  `app/api/portfolio.py`).
- **Job:** `compute_morning_recommendations` scores then reviews in two separate
  connections so a review failure can't roll back scoring; empty score list ⇒ no
  review.

## Invariants (do not break)

- **Long-only v1, no leverage, deterministic + auditable** (repo non-negotiables);
  recommendations only — nothing is executed here.
- settled-decisions **"Portfolio manager semantics"**:
  - *Cash semantics* — `cash_ledger.amount`: + inflow / − outflow, `SUM` = balance.
  - *Unknown cash rule* — empty/unknown cash does NOT hard-block; note
    `cash_check_deferred` in the rationale; hard cash enforcement is the
    execution guard's, not here.
  - *AUM basis* — mark-to-market first, cost-basis fallback; never unrealized P&L.
  - *ADD rule* — a new thesis version alone is not enough; conviction must
    materially improve (confidence and/or score delta ≥ 0.05).
  - *EXIT rule* — v1 EXIT only for thesis break / severe risk event / valuation
    target reached. NO superior-rotation exits.
  - *Held but unranked* — names that fall out of ranking still get a view:
    default HOLD unless an EXIT rule fires.
  - *Recommendation persistence* — history is append-oriented; do not spam
    identical HOLD rows. `_should_persist_dedup` suppresses a repeat HOLD /
    CONSIDERED only when the prior row's action AND rationale are identical.
- Data-completeness gate (#1820 §4): `completeness_tier = 'insufficient_data'`
  (C < 0.40) caps a name at HOLD (`_evaluate_buy` / `_evaluate_add` short-circuit)
  — still ranked + surfaced as CONSIDERED, never bought.
- CONSIDERED rows carry `status='considered'` (never `'proposed'`), so they stay
  invisible to every execution selector — audit that the unheld universe WAS
  evaluated.

## Failure conditions

- Missing critical source data, stale timestamps beyond threshold, or
  contradictory evidence without explicit uncertainty handling. Surface these as
  explicit signals — `quote_is_fallback` / `cash_check_deferred` notes, the
  insufficient-data HOLD cap, a CONSIDERED row with its block reason — **never
  paper over them with a neutral default**. `run_portfolio_review` does not raise
  on partial data: the affected name is held or blocked, reason in its rationale.
