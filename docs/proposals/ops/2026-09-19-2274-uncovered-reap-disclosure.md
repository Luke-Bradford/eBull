# #2274 — the Processes table discloses the jobs it does not carry, when those jobs would chip

Spec. Slice: the "re-scoped ProcessRow coverage gap", scoped from measurement.

Predecessors on this ticket: `cb9fefd5` (the per-row reap chip) and `9a8f401b` (making
that chip reachable from the collapsed group). This slice covers the population those two
cannot reach.

## The defect, measured at the shipped window and the shipped floor

The chip's own predicate is `recent_reap_events >= RECENT_REAP_CHIP_FLOOR (3)` over
`RECENT_REAP_WINDOW_DAYS (7)`. Applying exactly that predicate to the whole `job_runs`
population, and splitting on whether the job has a ProcessRow — dev DB, snapshot
2026-09-19 ~01:5xZ, `_gather_snapshot` called directly so the covered set is the real
served row set and not `SCHEDULED_JOBS`:

| | events / 7 d |
| --- | ---: |
| **chips today** — `sec_filing_documents_ingest` | 4 |
| **would chip, has no row** — `daily_candle_refresh` | **11** |
| **would chip, has no row** — `daily_portfolio_sync` | **4** |

**The page shows one chip and hides two, and the loudest job in the corpus is one of the
hidden ones** — `daily_candle_refresh` at 11 reap events in 7 days, roughly 1.6 a day,
nearly 3× the one job the operator can actually see.

Below the floor, and therefore not displayed by either side, the same 7-day window holds
4 more covered jobs (1 event each) and 2 more uncovered (`daily_financial_facts`,
`fx_rates_refresh`, 1 each).

### Population framing, 30 days

Anti-join of `job_runs.job_name` against the served row set, trailing 30 days:

| | |
| --- | ---: |
| distinct `job_name` that ran | **78** |
| ProcessRows served (65 scheduled + 6 sweeps + 1 bootstrap) | **72** |
| ran with **no** ProcessRow | **16** |
| reap ROWS on those 16, of 69 corpus-wide | **28 (40.6%)** |
| reap EVENTS on those 16, of 64 corpus-wide | **28 (43.8%)** |

⚠ The covered side is the **majority** of the 30-day population (41 of 69 rows, 36 of 64
events). The uncovered side is 40.6% / 43.8% — large, not dominant. An earlier draft of
this spec wrote "the minority", which is the complement and simply wrong.

The 16 split exactly as this ticket's own 2026-09-15 correction predicted: **13
sync-orchestrator layer jobs** (`_LAYER_TO_JOB`, 25 reap rows) and **3 with no row of
their own** — `strategy_backtest_run` (2), `daily_financial_facts` (1), `daily_cik_refresh`
(0).

## What "uncovered" means here — exactly one thing

**No ProcessRow whose `process_id` equals the `job_name`.** That is what the anti-join
proves and it is all the disclosure claims. It is deliberately NOT a claim that the job has
no operational surface at all:

- the 13 layer jobs are reachable through `/sync/layers/v2` and the orchestrator DAG;
- `daily_cik_refresh` and `daily_financial_facts` are invoked by `fundamentals_sync`
  (`app/workers/scheduler.py:5718`), which does have a row and does surface phase failures.

Those surfaces carry **data freshness and layer execution state**, not reap recurrence — a
different axis, as this ticket recorded on 2026-09-15: *"a job stuck `running` forever
shows up there only once its layer's data goes stale."* So the copy says "not in this
table", never "nowhere else".

## Source rule

No SEC reg governs this; the binding rules are the repo's own, and every one already
exists. This slice **reuses** them and mints no constant, no predicate and no display
policy:

- **The reap predicate is all three columns** — `status='failure'` +
  `error_category = FailureCategory.INTERNAL_ERROR` + `error_msg = ORPHAN_REAP_ERROR_MSG`
  (`scheduled_adapter::_recent_reap_counts`). A reap has no structured marker, so the
  message half is a convention pinned by `test_reaper_output_matches_the_readers_predicate`.
- **The window is `finished_at`, not `started_at`** — the reap event happens when the
  reaper runs; measured lag p99 1.86 d, max 4.375 d. `RECENT_REAP_WINDOW_DAYS = 7`.
- **An EVENT is one reap batch, not one row.** Grouping is
  `(job_name, date_trunc('second', finished_at))`, so it is per-job and second-resolution —
  the helper's docstring already states the approximation; this slice inherits it verbatim
  rather than restating it.
- **`RECENT_REAP_CHIP_FLOOR = 3`** is the display floor, measured on the corpus to bound
  the display rate (≥3/7 d: 17 of 30 days carry a firing job, max 5 concurrent). The
  disclosure applies **the same floor**, so "would chip if it had a row" is literally its
  definition and the page cannot show two disagreeing thresholds.
- **The window/floor constants are mirrored across the language boundary by name, not
  shared** — an existing decision recorded at `ProcessRow.tsx:353` and
  `scheduled_adapter.py:226` (grep `RECENT_REAP_CHIP_FLOOR`, change both). The disclosure
  follows it and does **not** add a third pattern by putting the duration on the wire.

And one design decision this slice deliberately **honours rather than reverses**
(`docs/proposals/ui/admin-control-hub-rewrite.md:379`, "Sync-orchestrator surface"):

> The 10 underlying `LAYERS` are NOT independently surfaced as process rows. […] A future
> v2 may introduce a `sync_layer` mechanism that is independently triggerable; v1 keeps the
> model honest by deferring it.

This slice mints neither 16 rows nor a mechanism. It makes the table state its own coverage
boundary when something outside that boundary would have chipped.

## ⚠ Attribution — why the number is not parked on an orchestrator row

This section is **load-bearing for rejecting the alternative designs and nothing else**.
The shipped code consumes no scope and no trigger; it is keyed on `job_name` only.

The obvious alternative is to credit each layer job's reaps to an orchestrator ProcessRow
via `JOB_TO_LAYERS`. Measurement says which orchestrator, and the answer is neither of the
candidates. Joining each reaped layer-job row to the `sync_layer_progress` / `sync_runs`
pair overlapping its `started_at` (30 d, 25 rows, every row matched, no row ambiguous):

```sql
SELECT (SELECT string_agg(DISTINCT sr.trigger||'/'||sr.scope, ',')
          FROM sync_layer_progress p JOIN sync_runs sr USING (sync_run_id)
         WHERE p.layer_name = ANY(%(layers)s)
           AND p.started_at <= r.started_at + interval '5 s'
           AND coalesce(p.finished_at, now()) >= r.started_at) AS trg,
       count(*)
  FROM job_runs r
 WHERE r.error_msg = %(reap_msg)s
   AND r.started_at >= now() - interval '30 days'
   AND r.job_name = ANY(%(layer_jobs)s)
 GROUP BY 1;
-- 23  boot_sweep/behind
--  2  scheduled/full
```

- **Zero** came from `orchestrator_high_frequency_sync`, the only orchestrator row firing
  on cadence (`job_runs`: 498 runs / 30 d, 0 reaps).
- `daily_portfolio_sync` is reachable from both `full` and `high_frequency` scopes, so a
  static `JOB_TO_LAYERS` credit has to pick one or double-count; the measurement says the
  runs that were actually reaped came from neither.
- ⚠ Temporal overlap establishes **which walk was running**, not causation, and it does not
  explain why the worker died. It is used here only to rule out a wrong subject.
- ⚠ The DAG drill-in is **not** a viable home for a different reason than an earlier draft
  claimed: its URL is restricted to `orchestrator_full_sync`, but its query takes the latest
  `sync_runs` row with **no scope filter** (`app/api/processes.py:637`), so it already shows
  boot-sweep and HF runs. It is a *latest-run* view either way, and a recurrence is a
  property of a window of runs.

The actor behind 23 of 25 is `app/jobs/boot_sweep.py`'s `SyncScope.behind()` walk, which
has no ProcessRow and is not a `job_runs` producer at all. Naming the **job** is therefore
the only honest subject available.

## Design

`_recent_reap_counts(conn, job_name=None)` already reads the whole `job_runs` population;
the dark jobs are present in its result and are dropped at the point where the adapter joins
them onto its own rows.

1. **`app/services/processes/scheduled_adapter.py`** — new public
   `uncovered_reap_counts(conn, *, covered: AbstractSet[str]) -> tuple[UncoveredReap, ...]`:
   `_recent_reap_counts(conn)` filtered to `job_name not in covered` **and**
   `events >= RECENT_REAP_CHIP_FLOOR`, sorted `(-events, job_name)`.

   ⚠ This is a **second** execution of the same aggregate (`list_rows` computes and discards
   its own copy, and threading it out would couple the adapter's internals to snapshot
   composition). Measured 5.6 ms against 139k `job_runs`; the cost is accepted and stated
   rather than claimed away.
2. **`app/services/processes/__init__.py`** — frozen `UncoveredReap` dataclass
   (`job_name`, `events`, `runs`); `ProcessSnapshot` gains
   `uncovered_reaps: tuple[UncoveredReap, ...] = ()`.
3. **`app/api/processes.py::_gather_snapshot`** — computed **inside** the
   `with snapshot_read(conn)` block, after the adapter loop, so the rows and the counts
   observe the same REPEATABLE READ snapshot. `_gather_snapshot` currently `return`s outside
   that block; the read must not follow it out.
4. **Failure semantics — three states, not two.** `None` = **not evaluated**; `()` =
   **measured, none**; a non-empty tuple = measured, these. The field is
   `tuple[...] | None`, defaulting to `None`, so a caller predating it cannot claim a
   clean measurement it never made.
   - When any adapter raised (`partial=True`), `covered` is smaller than the truth, so the
     residual would invent a coverage gap and name jobs that are perfectly well covered.
     The disclosure is **not computed** → `None`.
   - If the residual read itself raises: `None`, logged, no 500.
   - ⚠ **Neither failure sets `partial`.** `partial` has exactly one consumer meaning —
     `ProcessesTable.tsx` renders it as *"One adapter is unavailable — some lanes are
     omitted from this snapshot"* — and a failed disclosure read omits no lanes.
     Borrowing the flag would report a false operational outage for a signal nobody had
     yet missed: the #2218 shape, a status that does not match what happened. An earlier
     draft of this spec did exactly that; Codex ckpt-2 round 2 caught it.
   - The FE renders nothing for `None` and for `()` alike, but the two remain
     distinguishable on the wire so a later consumer can tell them apart.
5. **Wire** — `ProcessListResponse` (not `ProcessSnapshotResponse`; that name does not
   exist) gains `uncovered_reaps: list[UncoveredReapResponse] = []`, defaulted so existing
   fixtures and clients stay valid. `_convert_row` is row-scoped and is not involved.
   `frontend/src/api/types.ts` mirrors it.
6. **FE — reachability is the binding constraint (#3211's lesson).**
   `CollapsibleSection` **unmounts its body when closed**, so "the section defaults to open"
   only covers the first render. The count therefore goes in **two** places, the same shape
   `9a8f401b` used for the collapsed group:
   - the section `summary` carries it (`control hub · 2 jobs not listed`), which survives
     collapse;
   - a muted line inside the section names the subjects and both numbers.

   Copy — informational, never alarm (#2274's binding constraint is "must not train the
   operator to ignore it"), careful not to claim lost work (a reaped row may have committed
   its writes, and the reaper's auto-retry may have re-run it), and claiming **nothing**
   about where else to look:

   > **Not in this table:** 2 jobs have no process row here and had runs written off by the
   > orphan reaper in the last 7 days — `daily_candle_refresh` (11 reaps, 14 runs),
   > `daily_portfolio_sync` (4 reaps, 4 runs).

   ⚠ **Both directions are overclaims, and two successive drafts shipped one each.**
   "Nowhere else" is false for the sync-orchestrator layer jobs, which do have a
   `/sync/layers/v2` + DAG surface. "Some of these have other surfaces" is false whenever
   the residual happens to hold only an outside-DAG job (`strategy_backtest_run`) or one
   recently renamed — the filter proves only "no process row", so any sentence about other
   surfaces is an inference the data does not support. The note therefore ends at the list.

   Renders nothing when the list is empty or `null`. React key is `job_name`, unique by
   construction (the aggregate groups by it).

   ⚠ **The render is capped at 25 entries with an "N of M shown" disclosure**, per the
   array-size rule in `.claude/skills/frontend/api-shape-and-types.md` (#2178: a `.map()`
   over an uncapped API array committed ~150k DOM nodes and froze the tab). The backend
   applies **no** limit, so the component cap is the only bound — which is precisely the
   case that rule is written for — and 25 sits far above any plausible residual, so a later
   server-side limit cannot be silently truncated by the client. `job_name` is unconstrained
   `TEXT`, so a malformed or legacy producer is the realistic way the list gets long. The
   **count** in both the note and the section summary stays the PRE-CAP total; capping the
   render must not shrink what the operator is told exists.

### Not verdict inputs

Same rule as `recent_reap_events` on `ProcessRow`: these do **not** feed `compute_verdict`,
do not set `system_state`, and are not themselves a reason to raise `partial`. A recurrence
is a property of a window of runs; the verdict is a function of the latest terminal run plus
current staleness. Painting the page red for a chronic, largely deploy-caused condition is
#1831's measured bug.

## Tests

- `uncovered_reap_counts` returns the residual and only the residual: a covered job above
  the floor is absent; an uncovered job above the floor is present with **both** counts;
  an uncovered job below the floor is absent.
- Counts are not swapped: the fixture uses `events != runs` so a transposition reds.
- Ordering is `(-events, job_name)`: the fixture supplies equal-event jobs in reverse
  alphabetical order, and a higher-event job that sorts last alphabetically, so neither
  "alphabetical only" nor "input order" passes.
- `_gather_snapshot` attaches the field on the success path (integration, real adapters) —
  a helper-only test can pass while the field never reaches the response.
- `_gather_snapshot` emits `()` with `partial=True` when an adapter raises, even though
  uncovered jobs exist.
- `_gather_snapshot` emits `()` with `partial=True` when the residual read itself raises,
  and does not 500.
- The endpoint serialises the field (`ProcessListResponse`), so the browser can see it.
- The floor is the shipped constant, not a literal: the test imports
  `RECENT_REAP_CHIP_FLOOR` and reds if the helper stops honouring it.
- FE: renders the line and the summary count with job names and both numbers; renders
  neither on an empty list; renders neither when `partial`.

### Revert probes (each must red the test it names, against a green control)

1. Drop the `partial` suppression → the partial test reds.
2. Count rows instead of events → the transposition test reds.
3. Sort by `job_name` only → the ordering test reds.
4. Include covered jobs → the residual test reds.
5. Drop the floor (show every uncovered job with ≥1 event) → the below-floor test reds.

## Scope boundaries — named, not silently omitted

Not in this slice, and unchanged on the ticket: the watchdog's acting half, `can_cancel` on
a halted wedge, `daily_candle_refresh`'s missing `_OVERRIDES` entry (still needs the
inter-tick gap distribution that has no producer), and whether M1's fix (`e358fa86`) ended
the population — that is a wall-clock read, not work.

Two coverage questions this disclosure does **not** answer, recorded so they are not
mistaken for solved:

- **Served ≠ rendered.** `ProcessesTable` hides served rows behind bootstrap gating, lane
  filters and the collapsed bootstrap/backfill groups. A job with a row whose chip is
  unreachable for those reasons is outside this disclosure by construction.
- **Historical identity.** A renamed or retired `job_name` enters the residual on today's
  registry membership, which is not evidence about whether it had a row when it was reaped.
  With the 7-day window this is bounded, not eliminated.
