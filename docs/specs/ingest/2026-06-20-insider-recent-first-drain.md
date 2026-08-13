# Recent-first Form 4 drain — re-introduce the newest-first insider ingest

Fixes universe-wide recent insider-transaction staleness (operator-reported:
eBay insider sales ~3 months stale despite recent known selling).

## Problem (verified on dev 2026-06-20)

- `sec_insider_transactions_ingest` (newest-first universe Form 4 ingest, reads
  `filing_events` newest-first, bounded 500/tick) was **retired from
  `SCHEDULED_JOBS`** post-#1155 on the premise that "Layer 1/2/3 discovery +
  `sec_manifest_worker`" is "the steady-state keeper for new Form 4s".
- **That premise is falsified.** The manifest worker drains
  **`filed_at ASC` (oldest-first)** (`sec_manifest.py::iter_pending`) against a
  **~1.46M-row pending backlog** (form4 alone: 1.07M pending / 487 parsed) at the
  shared 10 req/s. Recent Form 4s (correct `form4.xml` URLs) sit at the BACK of
  the queue and never get parsed. The round-robin backfill is also oldest-first.
  → Nothing parses recent Form 4 first → recent insider activity is discovered
  (`filing_events` + manifest `pending`) but never reaches `insider_transactions`.
- Independent crash bug (already fixed, PR #1683 / `cb33c6ab`): an empty-body 200
  crashed the backfill tick; that froze the historical drainer but is orthogonal
  to the recent-first gap.

## Fix

Re-register the **existing, proven** `sec_insider_transactions_ingest` as a
`ScheduledJob` (function body + `_INVOKERS` entry were preserved at retirement;
the only change is putting it back on the schedule). It already:
- selects newest-first from `filing_events` (`ORDER BY filing_date DESC`),
- canonicalises the URL (`_canonical_form_4_url` strips the XSL segment → raw
  `form4.xml`), so it uses the correct primary-doc URL (NOT the `-index.htm` the
  manifest sometimes stores),
- tombstones on fetch/parse failure (now empty-body-safe after #1683),
- is documented write-safe concurrently with the backfill via the per-instrument
  advisory lock in `refresh_insiders_current`.

### Lane + cadence

- **NEW dedicated lane `sec_insider_ingest`** (one job — matches the #1540
  single-job-lane pattern; the backfill's `sec_insider_backfill` lane is asserted
  single-job by `test_extracted_lanes_are_single_job`, so it cannot be reused).
  The newest-first keeper runs CONCURRENTLY with the @:45 backfill (different
  lanes) — safe because: (a) the SEC 10 req/s budget is a PROCESS-GLOBAL clock
  (`sec_edgar._PROCESS_RATE_LIMIT_CLOCK`), not per-lane, so concurrency cannot
  burst SEC; (b) both write the insider tables under the per-instrument advisory
  lock in `refresh_insiders_current` (documented write-safe); (c) they select
  different ends (newest-first vs oldest-first) so overlap is minimal and
  `ON CONFLICT DO UPDATE` makes any overlap idempotent.
- `cadence = Cadence.hourly(minute=15)`.
- `role` defaults to `steady_state` (recurring recent-keeper — must show on the
  Processes page; a hidden keeper is the #1530 footgun). `catch_up_on_boot=False`,
  `prerequisite=_bootstrap_complete`.

`ScheduledJob.source` supplies the lane for `source_for()` (Codex ckpt-1: no
`MANUAL_TRIGGER_JOB_SOURCES` entry needed). Bounded 500/tick: ample for
steady-state inflow; the current recent backlog drains newest→older over
successive ticks (operator runs it a few times now to unstale immediately).

### Registry/test updates (Codex ckpt-1)

- Add `"sec_insider_ingest"` to the `Lane` literal (`app/jobs/sources.py`).
- Remove `sec_insider_transactions_ingest` from `expected_on_demand`
  (`test_jobs_runtime.py`) and `_KNOWN_UNRESOLVABLE` (`test_layer_123_wiring.py`)
  — it now resolves a lane + is scheduled again.
- Add single-job-lane + differ-from-`sec_rate` assertions for the new lane in
  `test_job_registry.py`.
- Correct the now-stale comments: the #1155 "retired" note (scheduler.py) and the
  "stays on sec_rate" lanemate note (sources.py / backfill ScheduledJob).

## Coexistence with the manifest worker

Both write the insider tables; the per-instrument advisory lock makes that safe.
A given accession is processed by whichever reaches it first; the
`no existing insider_filings row` candidate filter + the manifest
`ingest_status` bookkeeping prevent double-parse. The newest-first job keeps
RECENT fresh; the worker + backfill grind the historical tail. (When the worker
later reaches an accession the newest-first job already parsed, it may
re-fetch/tombstone the manifest row — harmless bookkeeping; the data is already
correct in the insider tables.)

## Out of scope (follow-ups)

- The SAME oldest-first starvation affects EVERY manifest source (8-K dividend
  events, 13D/G blockholders, etc.). The principled systemic fix is a
  **recent-first slice in `run_manifest_worker`** (a `filed_at DESC` top-up
  alongside the oldest-first fairness path) so every source keeps recent fresh.
  Ticket it; this PR fixes the operator-reported insider case with proven code.
- 69 pending form4 rows + eBay's recent carried the `-index.htm` URL in the
  manifest (a discovery quirk); the newest-first job sidesteps it (reads
  `filing_events`). Note for the manifest-discovery URL audit.
- Historical backlog drain (1.46M pending) — separate capacity question.

## Verify (DoD)

- Smoke: app boots; scheduler registry accepts the re-added job (no
  source_for KeyError; SCHEDULED_JOBS/`_BOOTSTRAP_STAGE_SPECS` source agreement
  check at `app/jobs/__main__.py` passes).
- Run the job on dev → recent Form 4 parses; eBay + a 2nd known recent-filer
  advance to within days of today.
- Confirm it coexists with the backfill (no lock errors, no double rows).
