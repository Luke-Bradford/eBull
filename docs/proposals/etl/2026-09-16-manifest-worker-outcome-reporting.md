# #3111 — the manifest worker's outcome reporting: what is wrong, what the repo already has, and why it is four slices

Status: **research + slice plan, no implementation.** Revised after Codex checkpoint 1 (24
findings), which falsified three claims in the first draft and surfaced a parser-contract
blocker that decides the ticket's shape. Target of the eventual slices:
`app/jobs/sec_manifest_worker.py`, `app/workers/scheduler.py`.

## 1. The premise holds — re-run on the full population

#3111 is an inherited root-cause handoff, so its discriminator was re-run before any code
(working-order 3c).

**Structural, read at `b23d2a13`:**

- `sec_manifest_worker_tick` (`scheduler.py:8025-8043`) sets `tracker.row_count` and logs the
  breakdown. It never sets `tracker.progress`.
- `_finish_tracked` (`:2858`) computes `degradation_reason(tracker.progress)`; with `progress`
  `None` that returns `None` — documented: *"a job that does not report progress is judged
  exactly as it was before"* — so the terminal row is `status='success'`.
- A tick in which every row failed therefore writes `success`, `error_msg=NULL`,
  `progress_json=NULL`. The ticket's synthetic probe is the right proof.

**Population, dev 2026-09-16 (read-only):**

| measurement | value |
| --- | --- |
| `sec_filing_manifest` by `ingest_status` | tombstoned 1,385,991 · parsed 926,260 · deferred 377,961 · **pending 3** |
| pending/retryable/failed by source | `sec_8k` 3 — nothing else |
| `job_runs` for `sec_manifest_worker`, ALL TIME (2026-06-03 →) | 24,717 runs: success 23,260 · skipped 1,374 · failure 83 |
| of those, with `progress_json` | **0 of 24,717** |

⚠ **Correction to the first draft (Codex 24).** It said the 83 failures were "all whole-tick
exceptions, so the outer path works". Overstated: 27 of them are
`orphaned: reaped at boot (owning worker thread died without a terminal status)`, which is the
BOOT REAPER writing a terminal row for a run that never reached the handler — not evidence that
the exception path recorded anything. The remaining 56 (13 interpreter-shutdown, 3 connection
exhaustion, 1 deadlock, 1 missing manifest row, 38 `illegal transition 'parsed' -> 'parsed'`)
are caught exceptions. Full `error_msg` read, never truncated.

The drained backlog (3 pending) is load-bearing for the design: most ticks now process zero
rows, so `candidates_seen=0` cannot trip the silent-stall condition, which requires `seen > 0`.
"Legitimate zero-work stays healthy" holds by construction rather than by a special case.

## 2. The second defect, located precisely

`_dispatch_rows`' outer `except` (`sec_manifest_worker.py:800-811`) logs, rolls back and
`continue`s — **incrementing no counter**. The row is in `rows_processed` (`len(rows)`) and in
`processed_by_source`, but in none of `parsed` / `tombstoned` / `failed` / `skipped_no_parser`.
The per-tick counters are short and the shortfall is silent.

⚠ Two corrections to how the first draft described it:

- **It is not exclusively transition/commit failure (Codex 10).** Malformed parser outcomes and
  attribute access on them reach the same handler.
- **It does not prove nothing was committed (Codex 9).** A parser may have committed its own
  typed-table work, and a commit acknowledgement can fail after durability. The honest claim is
  "the manifest row keeps its prior status and re-drains", not "the row's effects were undone".

## 3. What the repo ALREADY has — and the manifest worker uses neither

This is the finding that reshapes the ticket. There are **two** complementary mechanisms, both
already built and both already surfaced:

| mechanism | what it records | where it surfaces |
| --- | --- | --- |
| `JobProgress` (#2218) → `tracker.progress` | `candidates_seen` / `outcomes` / `errors`, and the degradation verdict | `status='degraded'` + the reason in `error_msg`, which the admin row and `/system/jobs` already render |
| `JobTelemetryAggregator` (`app/services/job_telemetry.py`) | `record_processed` / `record_error(error_class, message, subject)` / `record_warning` / `record_skip(reason)` → `rows_errored`, `error_classes` JSONB, `rows_skipped_by_reason` | `scheduled_adapter.py:788-797` → `/processes` `ProcessRunSummary` |

⚠ **Codex 6 was right and it matters: `progress_json` has NO operator-facing reader.**
`app/api/jobs.py`'s queries select `row_count, error_msg, linked_request_id` and never
`progress_json`; `scheduled_adapter` does not read it either. So persisting progress alone
cannot satisfy the acceptance's *"jobs-page drilldown exposes failed scope"*.

⚠ **Codex 17 was right too.** `scheduled_adapter:794` maps `row_count → rows_processed` while
reading `rows_errored` from its own column, which this job never writes. So the first draft's
plan to change `row_count` to `parsed + tombstoned` would have made an all-failed tick display
`rows_processed=0, rows_errored=0` — strictly worse.

**Both objections dissolve against the SECOND mechanism.** `JobTelemetryAggregator` writes
`rows_errored` and a per-error-class JSONB carrying `sample_message` and `last_subject` — which
is the "failed scope" the acceptance asks for, on a surface that already renders it. Wiring it
needs no API or frontend change. `row_count` then does not need to move at all.

## 4. ⛔ The blocker that decides the ticket's shape

Classifying a per-row outcome as an ERROR versus a SKIP cannot be done today.

- `docs/etl/sources/README.md` §"Retry posture" defines `tombstone` as *"a deterministic defect
  (empty body, missing required field, score-floor miss, unparseable payload)"* — so the first
  draft's "a policy tombstone is a real disposal, therefore progress" is **wrong as a blanket
  rule** (Codex 18). Some tombstones are disposals; some are defects.
- `docs/etl/sources/sec_n_csr.md` §3 documents `PENDING_CIK_REFRESH` → **24h backoff, recorded
  as `failed`** — an expected dependency wait, by design, giving the daily `cik_refresh` time to
  write the ext_id. Putting `failed` wholesale into `errors` degrades the job for a documented
  benign wait (Codex 20).
- The right axis exists — `JobTelemetryAggregator.record_skip(reason)` vs `record_error(...)` —
  but **`ParseOutcome` (`sec_manifest_worker.py:125-136`) carries only a free-text `error`
  string and no reason code.** Classifying from that text would be a classifier over source
  text, which this repo's engineering rules treat as a data-treatment decision requiring a
  source rule found first, not a regex written first.

So an honest error/skip split needs `ParseOutcome` to carry a machine-readable reason, across
the 8+ registered parsers. That is a contract change and it is the real work in #3111.

## 5. Slice plan

Each slice is independently complete and independently useful.

**Slice 1 — count what is currently uncounted.** Add `dispatch_errors` to `WorkerStats`,
incremented in the outer `except`. Establishes the tick identity

```
rows_processed == parsed + tombstoned + failed + skipped_no_parser + dispatch_errors
```

asserted in a unit test over a synthetic batch covering every exit. ⚠ `raw_payload_violations`
is **not** a term — `_dispatch_rows` writes `failed += 1` on that path too (`:704`), so it is a
SUBSET of `failed` and adding it double-counts. ⚠ The identity holds only for a NORMALLY
RETURNED `WorkerStats` (Codex 8): a selection or prefetch failure, or an interruption, exits
before the return and loses the counts for rows already committed. State that; do not claim the
identity covers exceptional exits.

**Slice 2 — wire `JobTelemetryAggregator`.** Thread one aggregator from
`sec_manifest_worker_tick` through `run_manifest_worker` → `_dispatch_rows`;
`record_processed()` on a terminal success, `record_error(error_class=…, subject=accession)` on
the dispatch-error branch, and flush with `flush_to_job_run(conn, run_id=tracker.run_id, agg=…)`.
This is the slice that makes failures operator-visible, on an existing surface, with no API or
UI change. Until slice 3 lands, every non-success is an ERROR — which is the conservative
direction and is stated rather than hidden.

**Slice 3 — give `ParseOutcome` a reason code**, then split error vs skip so documented
dependency waits (`PENDING_CIK_REFRESH`) become `record_skip` and deterministic defects stay
errors. The contract change is the cost; it is also what makes the signal trustworthy enough to
alarm on. Source rules to cite: `docs/etl/sources/README.md` §"Retry posture" plus each
per-source §3.

**Slice 4 — `JobProgress` on the tracker**, once slice 3 makes the error set honest. Mapping,
with slice 3's classification feeding `errors`:
`candidates_seen=rows_processed`, `outcomes={parsed, tombstoned}`, `errors={…}`,
`context={raw_payload_violations, processed_by_source, skipped_no_parser_by_source}`.
⚠ `skipped_no_parser` goes in **context, not errors** — see §6.

## 6. Corrections to the first draft, recorded so they are not re-made

1. **`skipped_no_parser` is unreachable in production, and the reason given was wrong (Codex
   13/14).** The fairness path picks sources from `registered_parser_sources()`
   (`sec_manifest_worker.py:331`), so a scheduled tick can never pick an unregistered source —
   that part held. But the first draft then said the per-source rebuild path makes it reachable
   "on the operator `sec_rebuild` route". **False**: `grep -rn "run_manifest_worker("` returns
   exactly one caller, `scheduler.py:8030`, with `source=None`. `sec_rebuild` resets rows for
   the *scheduled* drain; it does not invoke the per-source worker. So the counter is dead in
   production, belongs in `context`, and the real missing-parser signal remains
   `GET /coverage/manifest-parsers`.
2. **`parsed` does not mean "successfully extracted" (Codex 19)**, and `parsed+tombstoned`
   measures terminal HANDLING, not new extraction. Any wording promising extraction is wrong.
3. **The `row_count` spike claim was wrong (Codex 15).** "At most one transition" assumed the
   baseline moves; `check_row_count_spike` compares against the previous **success**, so
   consecutive degraded runs all compare against the same one and can repeat the warning
   indefinitely. Moot now that slice 2 leaves `row_count` alone.

## 7. Deferred with the reason, not silently

**The two pollers stay out.** The ticket invites "wire per-CIK `poll_errors` too or explicitly
track that follow-up" — this is the explicit track, and the reason is evidence, not scope:

- `subjects_polled` **includes** errored subjects (per-CIK increments before probing;
  expected-filings returns `len(due)`), so using it as an `outcomes` bucket reports failed
  attempts as completed work (Codex 1).
- Recheck errors already fold into the shared `poll_errors` (`sec_per_cik_poll.py:384`), so a
  per-lane success count needs per-lane error counters first (Codex 2).
- Chronic degradation is plausible for both: an errored subject's `next_recheck_at` is left NULL
  = immediately due, so a persistently failing cohort can occupy the recheck budget and degrade
  every tick (Codex 3); expected-filings re-polls bad CIKs every interval until window expiry
  (Codex 4).
- `_probe_subject` returns `errored=False` for a rejected manifest write caught as `ValueError`
  (`:234`), and `expected_filings_poller` ignores `run_force_refresh`'s failures (`:419`) — so
  both would report silent failures as completed polls (Codex 21, 22).
- ⚠ None of that is measured: there is **no full-population error census for either poller**
  (Codex 5). Wiring them without one would set an alarm threshold on an unknown distribution.

⚠ Also recorded, not fixed: `PerCikPollStats` has no `recheck_poll_errors`, so the recheck
lane's errors are indistinguishable from the primary lane's.

## 8. What no slice does

- No change to retry behaviour, backoff or any manifest state transition. `degraded` is a
  reporting state; `record_job_finish` documents that it deliberately takes the non-failure
  retry path.
- No change to #2274's liveness (`last_progress_at` / heartbeat). Liveness and outcome quality
  are different axes and the ticket says so.
- No second health model. Both mechanisms used here already exist and are already surfaced.

Refs #3111. Refs #2218. Refs #2274. Refs #2437.
