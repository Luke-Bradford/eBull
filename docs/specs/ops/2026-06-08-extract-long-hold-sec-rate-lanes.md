# Extract long-hold `sec_rate` producers to own lanes (#1540)

## Problem

#1538 added a general retry on a failed scheduled-fire lane acquire (~1.75 s,
`app/jobs/runtime.py::_fire_scheduled_with_lane_retry`). It rescues a job that
loses the non-blocking `pg_try_advisory_lock` to a lanemate **only when that
lanemate releases within ~1.75 s**. Dev evidence (2026-06-08, jobs proc on
a3a5f11) shows `sec_rate` is dominated by long holds the window cannot cover:

| `sec_rate` job | observed hold |
|---|---|
| `sec_filing_documents_ingest` (@:35 hourly) | **96–99 s** |
| `sec_atom_fast_lane` (every 5 min) | 0.24 s … **10.29 s** |

Both observed lane-busy skips were long-hold cases:
- 16:35 UTC: `filing_documents` won (held 96 s) → `atom` skipped (harmless,
  every-5-min, recovered next tick).
- 16:45 UTC: `atom` won (held 10.29 s) → `sec_insider_transactions_backfill`
  (hourly @:45) skipped → **missed that hour**.

`filing_documents` @:35 and `insider_transactions_backfill` @:45 collide with an
`atom` tick every hour; each hour the hourly producer runs only if it wins the
acquire or the winner releases < 1.75 s. Residual miss rate is non-trivial;
two-in-a-row → red `schedule_missed` (and #1510 watchdog can't heal it — #1536).

This is the 4th `sec_rate` starvation instance (#1478, #1526/#1527, #1534). The
proven fix is lane extraction, not a wider retry window (which would just pile
up waiters under `misfire_grace_time=1`).

## Fix

Move the two long-hold **scheduled** `sec_rate` producers onto their own
single-job JobLock lanes:

- `sec_filing_documents_ingest` → lane `sec_filing_docs`
- `sec_insider_transactions_backfill` → lane `sec_insider_backfill`

### Why this is rate-safe

The `sec_rate` JobLock lane is **not** the SEC rate limiter — the 10 req/s SEC
fair-use budget is enforced in the HTTP client (`SecFilingsProvider` /
`PipelinedSecFetcher`), shared process-wide regardless of JobLock lane
(confirmed #1534). Giving these jobs their own JobLock lanes changes
serialization, not request rate.

### Why no migration

Neither job is in `_BOOTSTRAP_STAGE_SPECS` (S19 `insider_transactions_backfill`
dropped #1413; `filing_documents` was never a stage). The lane CHECK constraint
`bootstrap_stages_lane_check` (sql/165) governs `bootstrap_stages.lane` only, so
the two new lane values never reach a CHECK-constrained column. Scheduled-only →
no migration (per #1534). Lane resolves via `SCHEDULED_JOBS[*].source`, which
covers both scheduled fires and manual triggers (neither job is in
`MANUAL_TRIGGER_JOB_SOURCES`).

### Write-ordering-safety audit (the #1534 / prevention-log 1702–1708 mandate)

Extraction makes each job run **concurrently** with its former `sec_rate`
lanemates, so per the prevention-log lesson we must prove every shared write is
ordering-safe — NOT merely assert "no watermark is written" (Codex ckpt-1
HIGH-1/HIGH-2 corrected an earlier draft that did exactly that). Full write set
audited per job:

**`sec_filing_documents_ingest`** → writes **only** `filing_documents`
(`app/services/filing_documents.py:215`). Sole writer of that table; no other
job writes it; no `data_freshness_index` / watermark. `max_instances=1` blocks
self-overlap. No cross-job race exists. ✔ Trivially safe.

**`sec_insider_transactions_backfill`** → via `upsert_filing`
(`insider_transactions.py:1104/1803`) writes the four typed insider tables
(`insider_filings`/`_holdings`/`_footnotes`/`insider_transactions`),
`ownership_insiders_observations` (`record_insider_observation`),
`filing_raw_documents` (`raw_filings.py`), and calls `refresh_insiders_current`
(→ `ownership_insiders_current` + `ownership_refresh_state` watermark).
Concurrency partner after extraction = `sec_insider_transactions_ingest`
(newest-first, stays on `sec_rate`) — same write set. Today the `sec_rate`
JobLock serialises them; after extraction they may overlap. Each shared write is
ordering-safe **independently of the JobLock lane**:

- Typed insider tables + observations + `filing_raw_documents`: row-level
  `ON CONFLICT (<natural identity key>) DO UPDATE` from the **immutable** source
  filing. Two parses of the same accession produce identical source-derived
  columns; the upsert is row-lock-atomic and converges. Only `ingested_at =
  clock_timestamp()` / `ingest_run_id` differ between racers — cosmetic, not
  inputs to any correctness decision except the refresh watermark, handled next.
- `ownership_insiders_current` + `ownership_refresh_state`
  (`last_drained_observations_max_ingested_at`): written **only** inside
  `refresh_insiders_current` (`ownership_observations.py:232`), which (a) holds a
  per-instrument `pg_advisory_xact_lock` (`:262`), (b) captures
  `watermark = MAX(ingested_at)` **pre-MERGE** in a Python var (PR12 / Codex 1b
  HIGH-2), (c) full-reconciles `_current` via MERGE over `known_to IS NULL`
  observations. The advisory lock — **not** the JobLock lane — serialises
  same-instrument refreshes, so the blind `= EXCLUDED` watermark UPSERT cannot
  regress: the second txn to take the lock sees the first's committed
  observations and captures a `>=` watermark. Different instruments hit
  different PK rows. **The JobLock lane is therefore not load-bearing for these
  writes** (the #1534 contrast: there the lane *was* the only serialisation; here
  a per-instrument advisory lock already is).
- Boundary overlap (both jobs select `fil.accession_number IS NULL`; backfill
  oldest-first, ingest newest-first → near-disjoint, but a boundary accession
  could be picked by both): worst case both process the same filing →
  idempotent double-write (above) + one wasted SEC fetch (rate-floor-bounded).
  No corruption.

No new guard needed: the existing per-instrument advisory lock + full-reconcile
MERGE + row-level `ON CONFLICT` atomicity already make the concurrent writes
ordering-safe. Existing `refresh_insiders_current` concurrency coverage (PR12)
holds; dev-verify confirms no `_current` / watermark anomaly under concurrent
backfill + ingest.

## Changes

1. `app/jobs/sources.py` — add `"sec_filing_docs"` and `"sec_insider_backfill"`
   to the `Lane` Literal + lane doc block (mirror the `sec_per_cik` entry).
2. `app/workers/scheduler.py` — change `source="sec_rate"` → `source="sec_filing_docs"`
   on the `JOB_SEC_FILING_DOCUMENTS_INGEST` `ScheduledJob`, and
   → `source="sec_insider_backfill"` on `JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL`.
3. `tests/test_job_registry.py` — extend the lane-mapping / disjointness
   invariants so both jobs resolve to their new lanes and no other job shares
   them (single-job lanes). Update any layer-123 wiring assertion that pins the
   `sec_rate` membership of these two.
4. No migration. No watermark guard.

## Out of scope / follow-up

- The bigger question — whether the catch-all `sec_rate` JobLock lane is the
  wrong model now that the HTTP semaphore is the real rate gate (5th instance
  would argue for per-job lanes universally) — is noted in #1540, not actioned
  here.
- `sec_filing_documents_ingest` holding the lane ~96 s is acceptable on its
  **own** lane (no contention). Chunking/yielding that body is not pursued.

## Validation

- Registry invariant tests green.
- Dev: restart jobs proc on the merge SHA; confirm `filing_documents` fires at
  its next @:35 and `insider_backfill` at @:45 with no lane-busy skip even when
  `atom` co-fires (job_runs gap-free across an aligned tick); verdicts green.
