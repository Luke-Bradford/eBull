# Orchestrator inner JobLock self-skip fix — per-source JobLock re-entrancy

> Status: **2026-05-17 (v4, CLEAN per Codex 1a + operator signoff).**
>
> Issue: **#1184**. Branch: `fix/1184-orchestrator-inner-lock-removal`.
>
> Predecessors: #1183 / PR #1185 (partial fix — KeyError → graceful skip);
> #260 / PR #262 (orchestrator flip — removed standalone ScheduledJob rows);
> #1064 PR1a (introduced source-collapse via `JobLock` +
> `hashtext('job_source:<lane>')`).
>
> v1 of this spec proposed dropping the inner `_run_with_lock` JobLock
> entirely. Codex 1a flagged three BLOCKING issues with that approach
> (lost cross-job same-source serialisation on the /sync direct-call
> path, lost serialisation for non-`db` adapter targets, exposed
> `_latest_job_outcome` stale-row race). v2 keeps the inner JobLock and
> adds per-source RE-ENTRANCY via a process-local ContextVar so the
> outer orchestrator JobLock's source is recognised as already-held by
> downstream same-source inner acquisitions in the same call context.

## 1. Problem

`app/services/sync_orchestrator/adapters.py::_run_with_lock` opens a NEW
psycopg session and acquires `JobLock(database_url, <job_name>)` for the
duration of the legacy function body. `JobLock` resolves
`<job_name> → source` via `app.jobs.sources.source_for`, then calls
`pg_try_advisory_lock(hashtext('job_source:<source>')::int)`.

The orchestrator's SCHEDULED-CRON dispatch path runs INSIDE an outer
`JobLock(orchestrator_full_sync | orchestrator_high_frequency_sync,
source='db')` held by `app/jobs/runtime.py::JobRuntime._wrap_invoker`
(line 1498). The outer lock is session-scoped on its own psycopg
connection.

When the orchestrator reaches a layer whose adapter target maps to lane
`db`, the inner `_run_with_lock` opens a fresh psycopg session and tries
`pg_try_advisory_lock(hashtext('job_source:db'))`. Postgres advisory
locks are session-scoped — same source from a different session always
collides → `pg_try_advisory_lock` returns FALSE →
`JobAlreadyRunning` → `PREREQ_SKIP`.

### Affected jobs (per #1183 audit + #1185 Lane assignments)

| Job | Lane | Status today (scheduled-cron path) | Status post-fix |
|-----|------|------------------------------------|-----------------|
| `fx_rates_refresh` | `db` | self-skip → `PREREQ_SKIP` every HF tick | runs |
| `seed_cost_models` | `db` | self-skip every FULL run | runs |
| `weekly_report` | `db` | self-skip every FULL run | runs |
| `monthly_report` | `db` | self-skip every FULL run | runs |
| `daily_portfolio_sync` | `etoro` | runs (avoids `db` collision) | runs (no change) |
| `daily_research_refresh` | `sec_rate` | runs (avoids `db` collision) | runs (no change) |
| `nightly_universe_sync` | `init` | runs (bootstrap stage covers source) | runs (no change) |
| `daily_candle_refresh` | `etoro` | runs | runs (no change) |

### Two orchestrator dispatch paths, different outer-lock state

| Trigger | Path | Outer JobLock held? |
|---------|------|---------------------|
| APScheduler cron fire | `JobRuntime._wrap_invoker` → `JobLock(orchestrator_*_sync, db)` → invoker (orchestrator-opt-out of prelude) → `run_sync` | YES — `db` source held for entire run |
| `POST /sync` HTTP | API publish → listener `_dispatch_sync_request` → `sync_executor.submit(_run_sync_with_request_lifecycle, ...)` → `run_sync` | NO outer JobLock; `sync_runs.idx_sync_runs_single_running` + `acquire_prelude_lock` on `process_stop_requests` cover orchestrator-vs-orchestrator only |
| Boot sweep | `app/jobs/boot_sweep.py::run_sync(SyncScope.behind(), ...)` | NO outer JobLock |

The four db-lane adapter targets self-skip ONLY on the scheduled-cron
path. The /sync HTTP path runs them correctly today because no outer
JobLock collides. Any fix that drops the inner JobLock unconditionally
would regress the /sync path (concurrent manual job triggers + adapter
body would race on the source bucket).

### Second defect found while writing this spec — composite adapter

`refresh_scoring_and_recommendations` at
`app/services/sync_orchestrator/adapters.py:302` does NOT use the
`_run_with_lock` helper. It acquires `JobLock` inline at line 330:

```python
with JobLock(settings.database_url, job_name):
    with _tracked_job(job_name) as tracker:
        result = compute_morning_recommendations()
```

`job_name = JOB_MORNING_CANDIDATE_REVIEW = "morning_candidate_review"`.
Empirical probe (2026-05-17):

```text
$ uv run python -c "from app.jobs.sources import source_for; source_for('morning_candidate_review')"
KeyError: "unknown job_name 'morning_candidate_review': not found in
SCHEDULED_JOBS or _BOOTSTRAP_STAGE_SPECS."
```

`morning_candidate_review` is NOT in `SCHEDULED_JOBS`, NOT in
`_BOOTSTRAP_STAGE_SPECS`, and NOT in `MANUAL_TRIGGER_JOB_SOURCES`. Every
orchestrator FULL sync that reaches the `scoring`/`recommendations`
layer therefore crashes inside `JobLock.__init__` with KeyError, the
executor's blanket `except Exception` (line 435) catches it, both emits
recorded FAILED. The `#1183` AST invariant
(`test_every_adapter_job_name_resolves`) does NOT catch this because
the extractor only walks `_run_with_lock(job_name=...)` keyword args —
the composite uses `JobLock(...)` directly.

This bug is dormant in practice only because `scoring`'s upstream
deps (`candles` → `daily_candle_refresh`, `fundamentals` →
`daily_research_refresh`) themselves often PREREQ_SKIP on a partial-
bootstrap dev DB, so the scoring layer becomes DEP_SKIPPED before the
adapter is called. Once the predecessors actually run end-to-end, the
crash surfaces.

The architectural fix for #1184 covers it because (a) the inner JobLock
becomes re-entrant against the outer `db` source on the scheduled-cron
path, AND (b) `morning_candidate_review` is added to
`MANUAL_TRIGGER_JOB_SOURCES` so `source_for()` resolves on the /sync
direct-call and manual-trigger paths too.

## 2. Root cause

Postgres advisory locks are session-scoped, not process-scoped. The
inner JobLock opens its OWN psycopg session, so even though the outer
JobLock and the inner JobLock are held by the same OS process running
the same call stack, Postgres sees two different sessions competing for
`hashtext('job_source:db')` and rejects the second acquire.

There is no built-in Postgres mechanism to express "same process should
treat its own held advisory locks as re-entrant." We must add that at
the application layer.

## 3. Goals

1. All 4 db-lane orchestrator-adapter targets fire end-to-end on dev DB
   on the next cadence after merge (scheduled-cron path):
   - `fx_rates_refresh` (HF, 5-min cadence) → `job_runs.status='success'`
   - `seed_cost_models` (FULL, 03:00 UTC) → `job_runs.status='success'`
   - `weekly_report` (FULL Sundays) → `job_runs.status='success'`
   - `monthly_report` (FULL 1st of month) → `job_runs.status='success'`
2. The dormant composite-adapter `morning_candidate_review` KeyError is
   eliminated by the same fix.
3. The 2 non-db adapters (`daily_portfolio_sync` /
   `daily_research_refresh`) keep running as today — strict
   non-regression on the scheduled-cron path.
4. Source-lock semantics PRESERVED for the MANUAL trigger path:
   a manual `POST /jobs/fx_rates_refresh/run` from the admin UI still
   serialises against an in-flight orchestrator run via the source-lock
   (either via the outer JobLock on the scheduled-cron path or via the
   inner JobLock on the /sync direct-call path).
5. Source-lock semantics PRESERVED for the /sync HTTP / boot-sweep
   direct-call paths: the inner JobLock continues to serialise db-lane
   adapter bodies against concurrent db-lane manual triggers when no
   outer JobLock is held.
6. Source-lock semantics PRESERVED for non-`db` adapters across BOTH
   paths: `daily_portfolio_sync` (etoro) still serialises against a
   concurrent manual `execute_approved_orders` trigger via the inner
   `JobLock(daily_portfolio_sync, etoro)` acquire — the new re-entrancy
   bypass fires ONLY when the same source is already held by an outer
   acquisition in the same call context.
7. `_latest_job_outcome` stale-row race is impossible by construction
   because the source-lock (outer OR inner, depending on path) still
   gates concurrent bodies per source.

## 4. Non-goals

- Reintroducing standalone `ScheduledJob` rows for the affected jobs.
  Orchestrator-driven cadence is the post-#260 design intent (Settled
  Decision: "Process topology #719").
- Adding new Lane variants (Option 2 from the issue). Pollutes the Lane
  vocabulary; bigger blast radius; does not address the root cause.
- Unconditional contextvar bypass (Option 3 from the issue as stated).
  Blanket "inside orchestrator → skip JobLock" loses serialisation for
  non-`db` adapters' inner locks against concurrent manual triggers in
  the same source. The PROPOSED design (§6) is finer-grained:
  PER-SOURCE re-entrancy, not per-orchestrator bypass.
- JobLock re-entrancy at the Postgres layer (Option 4). Requires
  inspecting `pg_locks`; cross-session bookkeeping is fragile. The
  process-local ContextVar approach below is enough.
- Operator-side jobs-process restart smoke (deferred to operator per
  PR #1182 pattern + `feedback_no_sleepy_claude.md`).
- Lane C `data_freshness_index` cadence audit (separate session).

## 5. Caller audit

Repo-wide grep (`grep -rn "_run_with_lock" --include="*.py"`):

```text
app/services/sync_orchestrator/adapters.py:87   def _run_with_lock(...)
app/services/sync_orchestrator/adapters.py:163  result = _run_with_lock(job_name=..., legacy_fn=...)
tests/test_job_registry.py:165,207,211,213,233,235,242,248  # AST invariant tests
app/jobs/sources.py:162,179,194,206  # docstring references only
```

`_run_with_lock` is invoked from exactly one site: `_wrap_single`
(line 163). `_wrap_single` is invoked only from `refresh_*` adapter
functions in the same module. Each `refresh_*` is registered in
`app/services/sync_orchestrator/registry.py::LAYERS[name].refresh` and
is invoked only from `executor.py::_run_layers_loop` at line 430.

The composite `refresh_scoring_and_recommendations` (line 302) does NOT
use `_run_with_lock` — it acquires `JobLock` directly. Same conclusion:
only invoked from `executor.py::_run_layers_loop`.

Repo-wide grep (`grep -rn "JobLock(" --include="*.py"`) — production
sites:

| Site | Purpose | Touched by this fix? |
|------|---------|----------------------|
| `app/jobs/runtime.py:1498` (`_wrap_invoker`) | Outer for scheduled fires | NO — unchanged |
| `app/jobs/runtime.py:1595` (`_run_manual`) | Outer for manual queue dispatch | NO — unchanged |
| `app/services/sync_orchestrator/adapters.py:103` (`_run_with_lock`) | Inner for single-emit adapters | YES — body unchanged; benefits from JobLock-level re-entrancy |
| `app/services/sync_orchestrator/adapters.py:330` (`refresh_scoring_and_recommendations`) | Inner for composite adapter | YES — body unchanged; benefits from JobLock-level re-entrancy + `morning_candidate_review` registry entry |

No non-orchestrator caller of either adapter inner site exists.

## 6. Design

### 6.1 Per-source JobLock re-entrancy via ContextVar

Add a process-local ContextVar that tracks which sources are currently
held by a JobLock acquired in the same call context:

```python
# app/jobs/locks.py — NEW
_HELD_SOURCES: ContextVar[frozenset[Lane]] = ContextVar(
    "_jobLock_held_sources", default=frozenset()
)
```

Modify `JobLock.__enter__`:

1. Resolve `source = source_for(self._job_name)` (already done via
   `_lock_key_for`; refactor to expose `self._source`).
2. Read `held = _HELD_SOURCES.get()`.
3. If `source in held`: this acquisition is RE-ENTRANT against a
   matching outer acquire in the same context. Skip Postgres entirely:
   set `self._reentrant = True`, do NOT open a connection, do NOT add
   to `held`, return `self`.
4. Otherwise: normal acquire path — open connection,
   `pg_try_advisory_lock`, `JobAlreadyRunning` on FALSE. On success,
   `self._held_token = _HELD_SOURCES.set(held | {source})`.

Modify `JobLock.__exit__`:

1. If `self._reentrant`: nothing to release. Return.
2. Otherwise: `pg_advisory_unlock`, close connection,
   `_HELD_SOURCES.reset(self._held_token)`. The reset restores the
   PRIOR frozenset, so nested `enter/exit` pairs LIFO correctly.

### 6.2 Why ContextVar (not threading.local, not module global)

| Choice | Pros | Cons | Verdict |
|--------|------|------|---------|
| `ContextVar` | Per-task, async-safe, scoped to the call chain rather than the thread | Slightly verbose | **Chosen** |
| `threading.local()` | Simpler API | Survives across unrelated work on the same thread; leaks if `__exit__` is skipped | Rejected — leak risk |
| Module global dict keyed by `threading.get_ident()` | Explicit | Same leak risk as threading.local; manual cleanup | Rejected |
| Postgres `pg_locks` inspection | DB-authoritative | Cross-session bookkeeping; adds DB round trip; race-prone | Rejected — Option 4 from issue |

`ContextVar` semantics per dispatch path:

- **Scheduled-cron path (APScheduler BackgroundScheduler).** The
  invoker is called on a worker thread inside `_wrap_invoker`. The
  outer `JobLock(orchestrator_*_sync)` is acquired ON that worker
  thread, BEFORE the invoker calls `run_sync` on the same thread (the
  orchestrator opt-out from the prelude keeps everything synchronous in
  one call stack). The adapter `JobLock` reads `_HELD_SOURCES` from the
  same context → sees `db` already held → bypasses Postgres acquire. ✓
- **/sync HTTP path (listener `sync_executor.submit`).**
  `concurrent.futures.ThreadPoolExecutor.submit` does NOT propagate the
  submitter's ContextVar state to the worker thread. ContextVar
  propagation in Python is opt-in: `asyncio.to_thread` snapshots the
  current context via `Context.run`, and any code that explicitly calls
  `ctx.run(fn)` does too. `ThreadPoolExecutor.submit` and
  `asyncio.run_in_executor` do not. The submitted callable starts
  with `_HELD_SOURCES = frozenset()` regardless of the submitter's
  context. This is the BEHAVIOUR WE WANT here, not a propagation
  guarantee we rely on: the listener main loop holds NO JobLock at
  submit time, so even if the var WERE propagated it would be empty.
  Either way, the /sync worker's inner adapter JobLock acquires the
  real Postgres source lock and serialises against concurrent same-
  source manual triggers. ✓
- **Boot sweep path (`app/jobs/boot_sweep.py`).** Runs `run_sync`
  directly on the jobs-process main thread at startup, before
  APScheduler has fired anything. No outer JobLock; empty
  `_HELD_SOURCES`; inner adapter JobLock acquires normally. ✓
- **Manual `_run_manual` path.** Acquires `JobLock(<manual_job>)` from
  the manual queue dispatcher thread. No orchestrator outer; empty
  `_HELD_SOURCES`; normal acquire. ✓

### 6.2.1 Intentional semantic change — re-entrancy is process-wide, not orchestrator-scoped

The new behaviour applies to ANY same-source nested `JobLock`
acquisition in the same call context, not just orchestrator-internal
paths. There is no `if inside_orchestrator` guard. Today no
non-orchestrator code path nests JobLock acquisitions, so this scope
choice has no observable side effect. The intentional payoff:

- Adding a future code path that nests `JobLock(<a>) → JobLock(<b>)` on
  the same source is now silently safe (will not self-skip) instead of
  silently broken. This trades visibility (a nested same-source acquire
  is now a no-op rather than a logged `JobAlreadyRunning`) for
  correctness (it cannot accidentally PREREQ_SKIP the inner work).

The trade is acceptable because (a) JobLock acquisitions are rare and
explicitly scoped, (b) re-entrancy is the correct behaviour for any
legitimate nesting against the same source, and (c) attempting to
acquire a lock you already hold is, by application semantics, a no-op
— Postgres just happens to disagree at the session boundary.

### 6.3 `_run_with_lock` and composite adapter — bodies unchanged

The adapter code at `app/services/sync_orchestrator/adapters.py:87-122`
(`_run_with_lock`) and `:302-356` (`refresh_scoring_and_recommendations`)
keep their existing `JobLock(...)` acquisitions. With the JobLock-level
re-entrancy in §6.1, the runtime behaviour changes as follows:

| Path | Outer lock state | Inner adapter outcome |
|------|------------------|----------------------|
| Scheduled cron, db-lane adapter | Outer holds `db` | Inner sees `db` in `_HELD_SOURCES` → bypass → body runs → success |
| Scheduled cron, non-db adapter (e.g. etoro) | Outer holds `db` only | Inner sees `etoro NOT in {db}` → normal acquire → body runs under inner `etoro` lock — same serialisation as today against concurrent etoro work |
| /sync HTTP, any-lane adapter | No outer | Inner sees `_HELD_SOURCES = frozenset()` → normal acquire → body runs under inner lock — same serialisation as today |
| Boot sweep, any-lane adapter | No outer | Same as /sync HTTP |
| Manual `JobLock(<job>)` via `_run_manual` | No outer (unless triggered FROM orchestrator-internal code, which it isn't) | Normal acquire path |

The `JobAlreadyRunning` branch in `_run_with_lock` (lines 115-122) is
retained — it still fires on cross-session contention from a different
process or a different context.

### 6.4 `morning_candidate_review` source-registry entry

`morning_candidate_review` is registered in
`app/jobs/runtime.py::_INVOKERS` (line 215) so it CAN be reached via
the manual-trigger queue path AND via the composite adapter. Both
paths currently KeyError because `source_for('morning_candidate_review')`
has no entry.

Add to `app/jobs/sources.py::MANUAL_TRIGGER_JOB_SOURCES`:

```python
# morning_candidate_review — heuristic ranking + recommendation build;
# DB-bound read + write on this side, no external rate-budget. Same
# class as fx_rates_refresh / seed_cost_models / weekly_report /
# monthly_report (manual-trigger + orchestrator-adapter reach).
"morning_candidate_review": "db",
```

`db` lane matches the body's resource profile (DB-bound). On the
scheduled-cron path this maps to the outer `db` source, so the new
re-entrancy in §6.1 bypasses the inner acquire — composite body runs
to completion. On the /sync HTTP path no outer is held, so the inner
acquires `db` normally and serialises against concurrent db-lane work.

### 6.5 `MANUAL_TRIGGER_JOB_SOURCES` documentation clean-up

After this fix:

- The "Known partial-fix limitation (#1184)" comment block in
  `app/jobs/sources.py` (lines 200-207) is removed (the architectural
  fix has landed).
- The "TWO classes of job" comment block collapses to ONE class:
  jobs in `_INVOKERS` but NOT in `SCHEDULED_JOBS` /
  `_BOOTSTRAP_STAGE_SPECS` that need source-lock coverage when reached
  via ANY non-scheduled dispatch path (manual queue OR orchestrator
  inner adapter). The historical name `MANUAL_TRIGGER_JOB_SOURCES` is
  now accurate again as a superset description. Do not rename — rename
  is churn with no behavioural benefit.

### 6.6 Tests

Update / add:

1. **`tests/test_job_registry.py::TestOrchestratorAdapterSourceCoverage`**
   — keep all three existing tests unchanged. The pinned-list
   `test_known_orchestrator_adapter_targets_covered` should ADD
   `morning_candidate_review: db` to the expected dict.

2. **NEW `tests/test_job_lock_reentrancy.py`** — five integration
   tests against the dev DB. All tests use REAL registered job_names
   (no `test_only_per_name` — Codex 1a flagged that escape hatch keys
   on raw job_name and bypasses `source_for`, so it cannot model
   "two different jobs sharing a source"):

   a. `test_same_source_reentrant_bypasses_pg_lock` — acquire
      `JobLock(database_url, "orchestrator_full_sync")` (production
      source=`db`) in the test context → enter a second `JobLock(
      database_url, "fx_rates_refresh")` (production source=`db` via
      MANUAL_TRIGGER_JOB_SOURCES) → assert the second `__enter__` set
      `_reentrant=True` AND did NOT open a connection (assert via
      patching `psycopg.connect` to count calls). Pre-fix the second
      acquire would raise `JobAlreadyRunning`; post-fix it bypasses.

   b. `test_different_source_still_acquires_real_pg_lock` — outer
      `JobLock(database_url, "orchestrator_full_sync")` (source=`db`)
      → inner `JobLock(database_url, "daily_portfolio_sync")`
      (source=`etoro`) → inner DOES acquire a real
      `pg_try_advisory_lock`. From a SECOND raw psycopg connection
      (mimicking a different process), call
      `pg_try_advisory_lock(hashtext('job_source:etoro')::int)` →
      expect FALSE. Verifies non-`db` adapter source-lock against
      cross-process manual triggers still works under the new outer
      `db` re-entrancy. Regression gate for Codex 1a v1 BLOCKING 1.

   c. `test_orchestrator_outer_holds_db_inner_db_adapter_runs` —
      end-to-end: acquire
      `JobLock(database_url, "orchestrator_full_sync")` →
      call `_run_with_lock(job_name="fx_rates_refresh",
      legacy_fn=fake_fn)` where `fake_fn` writes a `job_runs` row
      with `status='success'` using `_tracked_job` → assert returned
      `LayerOutcome.SUCCESS`, NOT `PREREQ_SKIP`. Regression gate for
      #1184 symptom on the scheduled-cron path.

   d. `test_sync_http_path_inner_lock_serialises_against_manual` —
      simulates the /sync HTTP path: NO outer JobLock acquired. From
      a SECOND raw psycopg connection, hold
      `pg_try_advisory_lock(hashtext('job_source:db')::int)` to
      mimic an in-flight manual `fx_rates_refresh`. Call
      `_run_with_lock(job_name="fx_rates_refresh", legacy_fn=fake_fn)`
      → assert it returns the PREREQ_SKIP string ("legacy cron
      holder active (JobLock busy)") and that `fake_fn` was NEVER
      called. Verifies the /sync path's inner JobLock still
      serialises against concurrent same-source acquires — Codex 1a
      v2 (c) gap.

   e. `test_reset_restores_prior_held_set_on_exception` — acquire
      outer JobLock; inside, attempt a non-db inner JobLock whose
      `pg_try_advisory_lock` is patched to RAISE mid-acquire. Assert
      that on outer `__exit__`, `_HELD_SOURCES.get()` returns
      `frozenset()` (not `{"db"}` or `{"db", "etoro"}`). Pins the
      token-reset invariant Codex 1a v2 (minor) flagged.

3. **NEW `tests/test_orchestrator_adapter_morning_candidate_review.py`**
   — assert `source_for('morning_candidate_review')` returns `'db'`
   (i.e. it is registered). Pins the §6.4 fix against future drift.

No AST `TestNoAdapterJobLockAcquisition` — Codex 1a flagged this
correctly as overbroad (would forbid the legitimate inner JobLock
that still serialises non-`db` adapters and the /sync path).

### 6.7 Operator-visible adapter behaviour

The PREREQ_SKIP path for "JobLock busy" disappears from db-lane
adapter targets on the scheduled-cron path. Other paths unchanged.

| Body outcome | Adapter emit |
|--------------|--------------|
| Wrote success row with `row_count > 0` | `LayerOutcome.SUCCESS` |
| Wrote success row with `row_count == 0` | `LayerOutcome.NO_WORK` |
| Wrote skipped row (e.g. inner prerequisite) | `LayerOutcome.PREREQ_SKIP` |
| Wrote failed row OR no row | `LayerOutcome.FAILED` |

## 7. Rollout

1. Spec v2 → Codex 1a re-review → operator signoff.
2. Implementation plan → Codex 1b → operator signoff.
3. Implement + tests + local gates (ruff / format / pyright / pytest).
4. Codex 2 pre-push review.
5. Push branch → PR → poll Claude bot + CI → merge.
6. Operator-side jobs-process restart triggers the next orchestrator
   cadence (HF every 5 min). Re-smoke (operator step):
   - `orchestrator_high_frequency_sync` fires → `job_runs` row for
     `fx_rates_refresh` with `status='success'`.
   - Next FULL fire (03:00 UTC) → `job_runs` rows for `seed_cost_models`,
     `weekly_report`, `monthly_report` all `status='success'` (subject
     to their own body prerequisites — empty DB may legitimately
     PREREQ_SKIP, but not from JobLock busy).
7. Memory update: `[[us-source-coverage]]` + `[[1183 prevention-log
   limitation note]]` reflect the unblock.
8. Prevention-log entry update: amend the existing #1183 entry to
   strike the "Limitation" paragraph (architectural fix landed) and
   add a one-liner pointing at the per-source re-entrancy mechanism.

## 8. Settled decisions impact

- **Process topology (#719)** — preserved. Outer JobLock + `sync_runs`
  gate stay where they are; inner JobLock stays; only its same-source
  re-entrant behaviour is new.
- **Source-lock decision (#1064 PR1a)** — refined, not changed.
  Cross-process source serialisation is unchanged (different Postgres
  sessions still collide). Same-process same-context re-entrant
  acquisitions are now no-ops at the application layer; the Postgres
  source lock semantics are unaffected.
- **Universal bootstrap-state gate (#1064 PR1b-2)** — preserved.
- **Safety-net catch-up gate carve-out (#1181)** — preserved.

## 9. Risks + mitigations

| Risk | Mitigation |
|------|------------|
| ContextVar not propagating across `ThreadPoolExecutor.submit` | This is the actual behaviour and the design relies on it. `concurrent.futures.ThreadPoolExecutor.submit` does NOT snapshot the submitter's context; the worker thread sees `_HELD_SOURCES = frozenset()` regardless. The listener main loop holds NO outer JobLock at submit time, so empty is the correct starting state on the /sync path. Asserted by `test_sync_http_path_inner_lock_serialises_against_manual` (§6.6.2d) — an inner JobLock without an outer takes the real Postgres source lock and blocks concurrent acquires. |
| `JobLock.__exit__` skipped (exception in setup) leaves stale source in `_HELD_SOURCES` | `__enter__` sets the contextvar only AFTER the Postgres acquire succeeds; any exception during acquire propagates without mutating the var. `__exit__` is guaranteed by the `with` statement; the reset uses the saved token so even abnormal exit restores the prior value. |
| Stack-LIFO violation (releasing in wrong order) | Production code only acquires JobLock via the `with` statement, which guarantees LIFO `__enter__`/`__exit__` pairing. Each JobLock instance keeps its own `self._held_token` from `set`; `__exit__` calls `reset(self._held_token)` to restore the prior frozenset. Python's ContextVar token-reset is correct under LIFO; out-of-order manual `__exit__` calls (no production caller) could restore surprising values. The new invariant test `test_reset_restores_prior_held_set_on_exception` pins the LIFO contract. |
| Adapter adds a new JobLock acquisition without going through `_run_with_lock` or `JobLock` (e.g. raw `pg_try_advisory_lock`) | Out of scope for this fix; no existing code does this. Any future PR introducing such a path would also need to integrate with `_HELD_SOURCES`. |
| Manual `POST /jobs/<db-lane-job>/run` racing a running scheduled-cron orchestrator | Manual path acquires `JobLock(<job>, db)` from a DIFFERENT process context (no `_HELD_SOURCES` inheritance); collides with the orchestrator's outer real Postgres advisory lock → `JobAlreadyRunning` → graceful PREREQ_SKIP. Unchanged from today. |
| Manual `POST /jobs/<db-lane-job>/run` racing a running /sync HTTP orchestrator | Manual path acquires inner `JobLock(<job>, db)`; /sync's inner JobLock for its current db-lane adapter also holds real Postgres `db` lock. Manual blocks → PREREQ_SKIP. Unchanged from today. |
| Two `/sync` HTTP requests in flight simultaneously | `sync_runs.idx_sync_runs_single_running` partial unique index rejects the second `_start_sync_run` INSERT → `SyncAlreadyRunning` → listener marks request rejected. Unchanged. |

## 10. Out of scope

- Renaming `MANUAL_TRIGGER_JOB_SOURCES`.
- Removing `_INVOKERS` entries for the 4 db-lane adapter targets.
- Lane C `data_freshness_index` cadence audit.
- Operator-side jobs-process restart smoke.
