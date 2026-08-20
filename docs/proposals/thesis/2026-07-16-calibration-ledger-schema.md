# Calibration ledger — schema + outcome capture (#2002 core)

Status: proposal (schema-first slice of the #2002 meta-thesis epic).

Operator direction (2026-07-11, on-issue): the calibration ledger is the
CORE of #2002 — the system grades its own research. This spec fixes the
SCHEMA + the deterministic outcome-capture job + the metric definitions
**before the wide T1+T2 backfill** (#1919 item 12), so backfill-era theses
are born measurable. Scoreboard/conviction-frontier/rec-evidence surfaces
are follow-up implementation slices of the epic, not this spec.

## Problem

Theses are versioned, dated forecasts (stance, confidence, bear/base/bull,
buy zone, model, prompt_version) — but nothing ever scores them against
what the market subsequently did. Without a ledger:

- model/prompt choices (#1995 judge, #2010 re-gate) rest on structure
  gates + content judges only — never realized accuracy;
- the wide backfill (~674 names) would mint the largest thesis corpus we
  will ever have with no outcome baseline;
- conviction surfaces (epic) have no calibration weight to rank by.

## Source rules

- **Metric definitions are the literature-standard ones, not invented:**
  MAPE = mean(|forecast − actual| / |actual|); Brier score =
  mean((p − outcome)²) over binary outcomes (Brier 1950, standard
  verification form). We apply them per (model, prompt_version, horizon)
  cohort; no eBull-specific reweighting in v1.
- **Anchor semantics = #2014 (shipped):** the write-time market reference
  for a thesis is `thesis_runs.context_summary.blocks.price_anchor`
  (persisted PRE-LLM, #2017); the trusted price value is re-read
  deterministically from `price_daily` via the `_close_at_or_before`
  pattern (`app/services/thesis_dq_audit.py:208`) — never trusted from
  JSON copy. The ledger reuses exactly this contract for its denominator.
- **Append-only forecasts (settled):** `theses` rows are never mutated;
  every version is a dated forecast. The ledger therefore scores THESIS
  VERSIONS, not instruments — supersession does not cancel measurement.
- **NULL is never 0 (#1632, prevention-log):** absent inputs produce
  absent rows/NULL metrics, never neutral values.
- **Job-lane discipline (#2052, #1526, #1707):** new nightly job gets its
  own single-job lane, non-5-minute-aligned fire time, and lane-busy skip
  rows follow the #2052 anchor-exclusion contract automatically (shipped
  in `_fire_scheduled_with_lane_retry`).
- **No ML in scoring / evidence-only (settled):** the ledger is passive
  measurement. Nothing here feeds scoring, recommendations, or any trade
  path in this slice.

## Schema (sql/NNN — next free number at implementation time)

```sql
CREATE TABLE IF NOT EXISTS thesis_outcomes (
    thesis_id       bigint      NOT NULL REFERENCES theses(thesis_id),
    horizon_days    smallint    NOT NULL CHECK (horizon_days IN (30, 90, 365)),
    anchor_date     date        NOT NULL,  -- price_anchor.as_of of the minting run
    anchor_close    numeric(18,6) NOT NULL, -- close at-or-before anchor_date (re-read)
    realized_date   date        NOT NULL,  -- trading day used: max price_date <= anchor_date + horizon
    realized_close  numeric(18,6) NOT NULL,
    realized_return numeric     NOT NULL,  -- (realized_close - anchor_close) / anchor_close
    method_version  text        NOT NULL,  -- 'oc_v1' (single-method table, below)
    computed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (thesis_id, horizon_days),
    CHECK (anchor_close > 0),
    CHECK (realized_close > 0)
);
CREATE INDEX IF NOT EXISTS idx_thesis_outcomes_horizon ON thesis_outcomes (horizon_days);
```

Notes:

- **Append-only, insert-once** per (thesis_id, horizon): `ON CONFLICT DO
  NOTHING`. Rows are never updated. **v1 is a single-method table**
  (`method_version` is provenance, deliberately NOT part of the identity):
  a definition change cannot coexist with 'oc_v1' rows here — it ships as
  its own migration with an explicit recompute-or-new-table decision at
  that time (Codex ckpt-1 Medium).
- **Positive-close guard** (Codex ckpt-1 High): both CHECKs above, plus
  the capture job skips (and counts) any pair whose anchor or realized
  close is not `> 0` — `realized_return` division and the read-side
  MAPE-form denominator are both protected at write time; a zero/negative
  close in `price_daily` is a data defect, never a ledger row.
- **No new writer-side columns.** Everything the ledger needs at write
  time already exists (`theses` forecast fields + the run's persisted
  `price_anchor`). Theses are already born measurable post-#2017; this
  spec adds zero friction to the thesis insert path.
- **Anchorless theses get NO rows** (honest missingness): a thesis whose
  minting run has no usable `price_anchor` (`available` false, or no run /
  no context_summary — pre-#2017 rows) cannot have a return computed
  against its write-time price. It is a queryable gap
  (`theses LEFT JOIN thesis_outcomes … IS NULL`), never a neutral row.
- **Currency/split caveat (accepted, documented):** returns are computed
  on `price_daily.close` in the instrument's stored price series. A split
  without back-adjustment in our series would corrupt the return; this is
  a pre-existing property of every consumer of `price_daily` (scoring
  momentum, fair-value bands), not new risk introduced here.

## Maturity + capture job

Nightly job `thesis_outcome_capture` (own `db_thesis_outcomes` lane,
05:32 — offset from dq_audit 05:12 / break_scan 05:22, non-aligned per
#1707):

1. Candidate set: (thesis, horizon) pairs with no `thesis_outcomes` row
   where the minting run has a usable anchor (`available` true + `as_of`
   parseable — latest `context_summary`-bearing run per thesis, same
   LATERAL as #2014).
2. **Maturity is DATA-anchored, not wall-clock** (dev eToro-unreachable
   lesson; EOD-snapshot settled decision precedent): a pair is mature when
   `(SELECT max(price_date) FROM price_daily WHERE instrument_id = i)
   >= anchor_date + horizon_days`. A stale price series simply defers
   capture — it can never mint a wrong-horizon row.
   **Terminal-series distinction (Codex ckpt-1 Medium):** a delisted /
   acquired / dead series never matures under this rule and must not be
   indistinguishable from temporarily stale data. Read-side counters
   split never-matured pairs: `immature_series_stalled` when the
   instrument is no longer tradable (`instruments.is_tradable = FALSE`)
   OR the series ended > 30 days before `anchor_date + horizon`
   (reported as `series_dead` in that case), vs `immature_data_current`
   otherwise. No synthetic outcome row is ever written for a dead series
   — a return against a halted print is not a market outcome.
3. For mature pairs: `anchor_close = close_at_or_before(anchor_date)`,
   `realized_close = close_at_or_before(anchor_date + horizon)` (its
   `price_date` is `realized_date`). Either side NULL (no price data at
   all) → skip, log count — never a NULL-stuffed row (all columns NOT
   NULL by design).
4. `row_count` = outcome rows inserted (0 = healthy steady state on quiet
   days). `POST /jobs/thesis_outcome_capture/run` invoker like siblings.

Idempotent: re-runs insert nothing (PK + ON CONFLICT DO NOTHING).
Deterministic: no LLM anywhere in this path, zero cloud spend.

## Metric definitions (computed on read, per (model, prompt_version, horizon) cohort)

Read-side (scoreboard endpoint = follow-up slice); definitions are fixed
HERE so the capture schema provably supports them:

1. **Target distance by horizon (MAPE-form)** — over theses with
   `base_value` non-null AND an outcome row:
   `mean(|base_value − realized_close| / realized_close)` per horizon.
   Explicitly NOT forecast MAPE (Codex ckpt-1 High): thesis targets carry
   no horizon stamp, so the same unstamped target is scored against three
   different actuals — the metric measures how far the market sat from
   the written target at each checkpoint, and per-horizon reporting is
   the caveat made structural, not a claim the writer forecast that
   horizon. (Adding a writer-emitted target horizon is a #2010-class
   prompt follow-up if this metric proves too blunt.) Null-target theses
   are EXCLUDED and counted (`targets_absent`), never imputed.
2. **Stance hit-rate** — direction-claim stances only:
   `buy` hit ⇔ `realized_return > 0`; `avoid` hit ⇔ `realized_return < 0`.
   `watch`/`hold` make no directional claim and are excluded (counted per
   cohort). Absolute return v1 — no benchmark subtraction (documented
   caveat; benchmark-relative is an epic follow-up, needs an index series
   decision).
3. **Conviction Brier (diagnostic)** — over the same direction-claim set:
   `mean((confidence_score − hit)²)` where `hit ∈ {0,1}` from (2). The
   writer contract defines `confidence_score` as stance CONVICTION, not a
   calibrated probability (Codex ckpt-1 High) — treating it as p(hit) is
   the DIAGNOSTIC PURPOSE of this metric, not an assumption: the
   scoreboard's question is precisely "does conviction behave like a
   calibrated probability, per model + prompt_version?". A cohort whose
   conviction is honest scores low; one whose conviction is decoration
   scores high. NULL `confidence_score` rows (schema-legal; today
   399/399 non-null is evidence, not an invariant) are EXCLUDED and
   counted (`confidence_absent`).
4. **Coverage counters** (every cohort, always reported): total theses,
   anchorless, immature (split: `immature_data_current` vs
   `immature_series_stalled` — see maturity note below), `series_dead`,
   targets_absent, confidence_absent, direction-claim count. Honest
   missingness is a first-class output, not a footnote.

## Full-population verification (dev, 2026-07-16)

- 399 theses total; **266/399 anchor-joinable** (run + context_summary +
  `price_anchor.available` true; all 266 carry `as_of`). The 133 gap =
  pre-#2017 rows (no context_summary) — permanent, documented, queryable.
- 78/399 carry `base_value` (MAPE cohort today); 399/399 carry
  `confidence_score`; stances: watch 192 / avoid 144 / buy 36 / hold 27
  → direction-claim cohort today = 180.
- `price_daily`: 5,222 instruments, 2020-10-19 → 2026-07-16 — the 30d
  horizon starts maturing for the oldest theses (2026-07-09 mint) around
  2026-08-08; 365d in 2027. The ledger is deliberately slow — that is
  the nature of calibration, and why the schema must precede the backfill.
- Verified during spec-research: `thesis_valuation_audit` (sql/222)
  stores `price_as_of` but NOT the anchor close → re-read contract above
  is required, not optional.

## Adjustment-event defect rows — remediation runbook (#2066)

Provider candle history is back-adjusted at fetch time, but the daily
incremental refresh only rewrites the overlap window — a FUTURE split
re-bases the fetched bars while rows older than the buffer keep the old
basis, leaving a permanent cliff at (split_date − buffer). The market-data
layer detects this at the incremental-fetch overlap (stored-vs-fetched
ratio-scale close mismatch, `detect_adjustment_event` in
`app/services/market_data.py`) and heals the series same-day with an
in-run full-history re-fetch.

The exposure window for this ledger: an outcome row captured BETWEEN the
split taking effect and the heal (same run, so in practice only rows
minted by a capture job racing the candle refresh, or captured while
detection was broken) mixes bases — `anchor_close` on one basis,
`realized_close` on the other — and insert-once means the defect row is
frozen. `method_version` covers method changes, NOT data defects.

**Append-only ≠ never-delete-defects.** Remediation for rows straddling a
detected adjustment event:

1. Identify: instrument + detection date from the refresh log line
   ("Adjustment event detected"). Defect candidates are
   `thesis_outcomes` rows for that instrument whose `(anchor_date,
   realized_date)` straddle the event date AND whose `captured_at` is
   before the heal.
2. `DELETE FROM thesis_outcomes` for exactly those rows (record thesis_ids
   in the ops note).
3. Re-run `capture_thesis_outcomes` (nightly job re-mints them from the
   healed series; insert-once makes the re-capture idempotent).

`price_move`/`band_exit` staleness triggers (#1988) need no remediation:
the heal lands the same night, so the worst case is a one-night false
fire (a 2:1 split reads as −50% until healed) — a spurious regen, not a
corrupted record. Documented as accepted.

## Non-goals (this slice)

- No scoreboard endpoint / dashboard panel / conviction frontier / rec
  evidence (epic follow-ups; they consume this schema).
- No benchmark-relative returns (index-series decision deferred).
- No backfill of outcomes for anchorless pre-#2017 theses (impossible —
  no trusted write-time anchor exists).
- No writer/prompt changes, no scoring changes, no trade-path contact.

## Files (implementation slice, after spec approval)

- sql/NNN_thesis_outcomes.sql — table above
- app/services/thesis_outcomes.py — pure maturity/candidate logic +
  capture entry point (reuse `_close_at_or_before` shape; consider
  extracting the helper rather than a third copy)
- app/jobs/sources.py — register the new `db_thesis_outcomes` lane in the
  Lane literal/source registry (Codex ckpt-1: lane registration is here,
  not only SCHEDULED_JOBS — see `db_thesis_dq`/`db_thesis_break` at
  app/jobs/sources.py:87)
- app/workers/scheduler.py — SCHEDULED_JOBS entry (db_thesis_outcomes
  lane, 05:32, catch_up_on_boot=False) + `_INVOKERS` row
- tests: pure maturity-predicate table tests; ONE db-tier test for the
  insert-once mechanism
