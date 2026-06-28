# Expected-filings poller — event-driven fundamentals catch-up (#1788 / #677 Part B)

## Problem

`daily_financial_facts` already polls SEC `submissions.json` (via `plan_refresh`,
`app/services/fundamentals/__init__.py:2027`) and refreshes companyfacts +
`financial_periods` when a new filing lands — but it runs **once a day over the full
covered universe**. A 10-Q that posts at 09:00 is not reflected on the instrument page
until the next daily run (≤ ~24 h later).

Part A (#1787) shipped the targeted `run_force_refresh(conn, symbols)` core. Part B adds
a **small, budget-bounded, high-frequency watchlist** so the operator's high-attention
instruments (held + watchlisted) get force-refreshed within minutes of an expected
10-Q/10-K appearing — without re-polling the whole universe at that cadence.

## Premise check (falsified → refined)

The handoff framed this as "event-driven catch-up is missing." **Not true** —
`plan_refresh` is already event-driven (master-index lookback → per-CIK submissions →
seeds/refreshes). The real gap is **latency**, not capability. So Part B is a *latency
optimisation for a small high-value subset*, NOT a second filing-detection implementation,
and it reuses the Part A fetch path (`run_force_refresh`).

**Strictly additive / never-worse-than-status-quo.** The daily `daily_financial_facts`
remains the universal backstop. A mis-sized window or a filing missed past
`expected_window_end` simply falls back to the ≤24 h daily path. The poller acts **only**
on a strictly-newer, exact-form, non-amendment accession (baseline-watermarked), so it
can never false-fulfil or write wrong data.

## Source rule (SEC)

- **Reports exist & cadence:** periodic reports are mandated by **Exchange Act Rule 13a-1**
  (annual 10-K) and **Rule 13a-13** (quarterly 10-Q). A domestic issuer files **three**
  10-Qs (fiscal Q1/Q2/Q3) and **one** 10-K per fiscal year — there is **no Q4 10-Q**; the
  fourth period is reported on the 10-K.
- **Deadlines run from FISCAL PERIOD-END, not from the prior filing date** (Form 10-K
  Gen. Instr. A.(2): 60/75/90 days after FY-end for large-accelerated/accelerated/non-
  accelerated; Form 10-Q Gen. Instr. A.(1): 40/45 days after quarter-end). Filer category
  is not reliably known → use the **widest** deadline as the window ceiling.
- **WHEN-to-poll is an operational schedule, not a data-treatment decision.** The window
  governs only *when we look*; it never classifies/aggregates/dedups any value. Padding
  below is conservative ops tuning; correctness is enforced by exact-form +
  non-amendment + baseline-accession matching, not by the window.

## Seed model (period-end anchored — fixes the Q3→Q1 gap)

For each in-scope instrument, read its **latest** `financial_periods` row
(`MAX(period_end_date)` → `period_type`). Map the **next** fiscal period to a form using
the 3-10-Qs-then-10-K structure, and anchor the poll window on the next period-end:

```
latest period_type   next period   expected form   next_period_end ≈
  Q1, Q2               Q2/Q3          10-Q            latest_period_end + 91d
  Q3                   Q4/FY          10-K            latest_period_end + 91d
  Q4, FY               Q1             10-Q            latest_period_end + 91d
  (unknown/sparse)     —              10-Q (default)  latest_period_end + 91d

10-Q window = [next_period_end + 30, next_period_end + 55]   (deadline 40-45d ± slack)
10-K window = [next_period_end + 50, next_period_end + 100]  (deadline 60-90d ± slack)
```

This is period-end-grounded (the documented rule), and the fiscal-position → form map
removes the false-quarterly-cadence bug (a Q3 issuer's next filing is a 10-K, not a phantom
10-Q ~91 d after the Q3 10-Q). Window start is offset to the earliest realistic filing date
so we don't burn polls during the pre-deadline dead zone; an early filer caught a few days
late just falls to the daily backstop.

The seed records `anchor_period_end = latest_period_end` (the cycle key — see Schema).
Reads only **non-superseded** `financial_periods` (`superseded_at IS NULL`) so a restated
later-dated row can't anchor the wrong window. An instrument with **no manifest baseline**
of the expected form is **skipped** (a null `last_known_filing_id` would make every recent
filing read as "new" → false-fulfil on the existing last filing). The full-scope daily
re-seed (`only_symbol=None`) also **prunes** rows for instruments that have left the
high-value set (un-watchlisted / position closed); the `--symbol` path is additive and
never prunes.

**Baseline accession:** the seed also records the accession of the instrument's latest
**non-amendment** filing of that form from `sec_filing_manifest`
(`MAX(filed_at) WHERE source IN ('sec_10k'|'sec_10q') AND NOT is_amendment`). Stored as
`baseline_accession` and passed to detection as `last_known_filing_id` so only a
**strictly-newer** accession counts as the expected filing (without it, the existing last
filing reads as "new" and instantly false-fulfils — `sec_submissions.check_freshness`).

## Scope (small high-value set — makes the latency claim true)

Seed only `instrument_id ∈ (watchlist ∪ positions WHERE current_units > 0)` that also have
≥1 `financial_periods` row. Full-pop on dev (2026-06-28): combined scope = **4** instruments
(watchlist empty + a few positions) → ≤ 8 seed rows; nowhere near the 8.3k full universe.
Empty-when-no-watchlist is **correct**, not a gap — there is nothing to fast-track and the
daily path covers everything. The operator CLI `--symbol` force-seeds an ad-hoc instrument
(used for dev verification, e.g. AAPL). Extending scope to ranking top-N is a noted
follow-up.

## Schema — `sql/207_expected_filings.sql`

**One row per instrument** = its single *next* expected filing (a domestic issuer has
exactly one next periodic filing at a time — Q-next or the FY 10-K). `UNIQUE (instrument_id)`,
not per-form: the seed derives THE next filing, so a per-form key would leave the other
form's row permanently stale/fulfilled (Codex ckpt-1 round 2). `expected_filing_type` is an
attribute that flips '10-Q'↔'10-K' across the year. Plus `baseline_accession` (correctness
watermark) and `anchor_period_end` (cycle key — the latest reported `period_end_date` the
derivation used).

```sql
CREATE TABLE IF NOT EXISTS expected_filings (
    id                    BIGSERIAL PRIMARY KEY,
    instrument_id         INT  NOT NULL UNIQUE REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    expected_filing_type  TEXT NOT NULL,            -- '10-Q' | '10-K' (flips across the year)
    anchor_period_end     DATE NOT NULL,            -- latest financial_periods.period_end_date used to derive
    expected_window_start DATE NOT NULL,
    expected_window_end   DATE NOT NULL,
    poll_interval_minutes INT  NOT NULL DEFAULT 30,
    baseline_accession    TEXT,                     -- last known non-amendment accession of expected form
    last_polled_at        TIMESTAMPTZ,
    fulfilled_at          TIMESTAMPTZ,
    fulfilled_accession   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_expected_filings_due
    ON expected_filings (expected_window_end, last_polled_at)
    WHERE fulfilled_at IS NULL;
```

(New table name → no prior shape; the prevention-log "pair CREATE with ALTER ADD COLUMN
IF NOT EXISTS" self-review resolves clean. `DEFAULT 30` aligns the per-row interval with
the 15-min scheduler cadence — no hourly-vs-15-min mismatch.)

**Conditional re-seed (cycle key = `anchor_period_end`, no fulfilment churn):**

```sql
INSERT INTO expected_filings (instrument_id, expected_filing_type, anchor_period_end,
    expected_window_start, expected_window_end, poll_interval_minutes, baseline_accession)
VALUES (...)
ON CONFLICT (instrument_id) DO UPDATE
SET expected_filing_type = EXCLUDED.expected_filing_type,
    anchor_period_end     = EXCLUDED.anchor_period_end,
    expected_window_start = EXCLUDED.expected_window_start,
    expected_window_end   = EXCLUDED.expected_window_end,
    baseline_accession    = EXCLUDED.baseline_accession,
    last_polled_at = NULL, fulfilled_at = NULL, fulfilled_accession = NULL
WHERE expected_filings.anchor_period_end IS DISTINCT FROM EXCLUDED.anchor_period_end;
```

A no-change re-seed (latest reported period unchanged) is a **no-op** — a fulfilled row
stays fulfilled. The loop closes: poller detects the new filing → writes
`sec_filing_manifest` + `run_force_refresh` re-normalizes `financial_periods` → a new
`period_end_date` appears → the *next* daily seed derives the subsequent filing (form flips,
`anchor_period_end` advances) → upsert rolls the row forward and resets for the next cycle.
If companyfacts (XBRL) lags the filing and the new `financial_period` hasn't landed yet, the
row stays fulfilled until the daily `daily_financial_facts` backstop normalizes it and the
anchor advances — never-worse-than-status-quo.

## Poller — `app/jobs/expected_filings_poller.py`

`run_expected_filings_poller(conn, *, http_get, now, max_subjects=100) -> PollStats`

1. Select due rows: `fulfilled_at IS NULL AND now::date BETWEEN expected_window_start AND
   expected_window_end AND (last_polled_at IS NULL OR last_polled_at < now -
   poll_interval_minutes)`, joined to the primary `sec.cik` external id, ordered
   `last_polled_at NULLS FIRST` (most-stale first), `LIMIT max_subjects`.
2. Commit the read tx before HTTP work (Part A commit discipline).
3. Per row: `check_freshness(http_get, cik=cik, last_known_filing_id=baseline_accession,
   sources={expected_source})` (`app/providers/implementations/sec_submissions.py`). The
   `sources` filter pre-narrows; the load-bearing match is on the returned
   `FilingIndexRow`: **`row.form == expected_filing_type AND not row.is_amendment`**
   (exact form string — excludes `10-Q/A`/`10-K/A` mapping to the same `source`).
4. On a match: `record_manifest_entry(conn, accession, cik=, form=, source=,
   subject_type='issuer', subject_id=str(instrument_id), instrument_id=, filed_at=,
   is_amendment=False)` (idempotent — `app/services/sec_manifest.py:226`), then
   `run_force_refresh(conn, [symbol])`, then `UPDATE expected_filings SET fulfilled_at=now,
   fulfilled_accession=accession, last_polled_at=now`. Commit.
5. No match / empty: `UPDATE … SET last_polled_at = now`. A probe **exception** also bumps
   `last_polled_at` (then `continue`) so a bad CIK / network failure respects
   `poll_interval_minutes` instead of staying most-stale and re-firing every tick (which
   would starve healthy due rows under the subject cap).

Unconditional `check_freshness` (not `_conditional`) in v1 — the scope is small so the
If-Modified-Since 304 path is negligible budget and adding it would reintroduce the
date-window-wedge surface (prevention-log: holiday/304 wedge). Listed as a deferred perf
optimisation. The 10 req/s SEC HTTP floor is enforced process-wide in `sec_edgar`
regardless of lane; the poller adds its own lane `sec_expected_filings` for job-overlap
serialisation.

## Scheduler + seed wiring — `app/workers/scheduler.py`

```python
JOB_EXPECTED_FILINGS_POLLER = "expected_filings_poller"
JOB_EXPECTED_FILINGS_SEED   = "expected_filings_seed"

ScheduledJob(name=JOB_EXPECTED_FILINGS_POLLER, display_name="Expected-filings poller",
    source="sec_expected_filings", cadence=Cadence.every_n_minutes(interval=15),
    catch_up_on_boot=False, prerequisite=_bootstrap_complete, description="#1788 — ...")

ScheduledJob(name=JOB_EXPECTED_FILINGS_SEED, display_name="Expected-filings seed",
    source="db_fundamentals_raw", cadence=Cadence.daily(hour=6, minute=0),
    catch_up_on_boot=True, prerequisite=_bootstrap_complete, description="#1788 — ...")
```

Body mirrors `sec_per_cik_poll` (`scheduler.py:6175`): `_tracked_job` + `connect_job` +
`SecFilingsProvider` + `_make_sec_http_get(sec)` for the poller; the seed reads only
`financial_periods` + `sec_filing_manifest` (no SEC HTTP).

**Bootstrap posture:** the seed is a daily steady-state job with `catch_up_on_boot=True`
+ `prerequisite=_bootstrap_complete`, so it self-populates within one cycle of bootstrap
completion (scope membership — watchlist/positions — is operator-mutable and changes daily,
so a daily refresh is the correct shape; a heavier bootstrap-DAG stage with capability
wiring is unnecessary and deferred). New lane → add `sec_expected_filings` to the `Lane`
literal in `app/jobs/sources.py`; register both invokers in `app/jobs/runtime.py::_INVOKERS`.

## Seed CLI — `scripts/seed_expected_filings.py`

Wraps `seed_expected_filings(conn, *, now, only_symbol=None) -> int`. `--dry-run` prints the
derived (instrument, form, window, baseline) rows; `--symbol SYM` force-seeds one
instrument regardless of watchlist/position membership (dev verification + ad-hoc operator
declaration → satisfies the #677 "operator UI/CLI to declare expected filings"). Richer
admin FE CRUD deferred to a follow-up ticket.

## Tests

- Pure-logic (no DB): `next_form_and_window(latest_period_type, latest_period_end)` —
  table-test the Q1/Q2→10-Q, **Q3→10-K**, Q4/FY→10-Q map and the window offsets.
- Pure-logic: `match_filing(delta, expected_type)` — matching exact form + non-amendment
  fulfils; `10-Q/A`, wrong form, empty, baseline-equal do **not**.
- One DB test: migration applies; seed upsert idempotency (re-seed same baseline = no-op,
  fulfilled survives; new baseline = window rolls + reset); due-row selection respects
  window + `poll_interval_minutes`.

## Dev-DB verification (DoD clauses 8-12)

`scripts/seed_expected_filings.py --symbol AAPL` → inspect the row (form/window/baseline);
run the poller once via the job invoker; confirm no false-fulfil (AAPL's last 10-Q is the
baseline, so an unchanged poll leaves `fulfilled_at` NULL and bumps `last_polled_at`).
Record figures in the PR.

## Out of scope (follow-up ticket)

Admin FE CRUD for expected_filings; per-instrument operator window override; ranking-top-N
scope; 8-K-dividend / other event forms (this PR is 10-Q/10-K only); If-Modified-Since 304
perf path.
