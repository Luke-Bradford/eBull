# #2274 — the `job_runs` heartbeat producer, installed where the run row is owned

Status: spec for the SECOND item in this ticket's researched ordering
(`docs/proposals/ops/2026-09-14-2274-tracked-job-heartbeat.md` §3: ceiling → heartbeat →
watchdog). The ceiling shipped as `stale_detection` rule 5 in `7f8d2b99`.

Scope: the producer, plus the one bound on its `dev_reload` consumer that the producer makes
necessary (§4.6). Not the no-progress watchdog, not `dev_reload`'s reap-age refusal, not the
consecutive-orphan-reap verdict, not the 9-dark-job ProcessRow gap.

Revision 2 — Codex checkpoint 1 falsified five claims in revision 1. They are corrected in
place and called out where the correction changed the design, not just the prose.

## 1. What is being switched on, and why it is not "a new feature"

`job_runs.processed_count`, `job_runs.target_count` and `job_runs.last_progress_at` exist
(`sql/140_per_run_progress_telemetry.sql`) and have **two** consumers already in production:

| consumer | file | what it does with the columns |
| --- | --- | --- |
| `stale_detection` rule 4 (`mid_flight_stuck`) | `app/services/processes/stale_detection.py:211` | `COALESCE(last_progress_at, started_at)` vs the per-process threshold |
| `dev_reload.live_job` | `app/jobs/dev_reload.py:209` (`_LIVE_JOB_SQL`) | defers an AUTOMATIC reload while a `running` row heartbeats inside its own threshold |

The only writer today is `_BacktestProgressWriter` (`app/workers/scheduler.py:6643` and its
`record_window_commit` sibling at `:6675`), for `strategy_backtest_run` alone. Over the last
90 days on dev, the jobs whose bodies already emit progress ticks carry
`last_progress_at IS NOT NULL` on **0** runs:

```sql
SELECT job_name, count(*) AS runs,
       count(*) FILTER (WHERE last_progress_at IS NOT NULL) AS with_lpa
  FROM job_runs
 WHERE started_at > now() - interval '90 days'
   AND job_name IN ('daily_candle_refresh','daily_financial_facts','thesis_refresh',
                    'expected_filings_poller','fundamentals_sync')
 GROUP BY 1;
```

⚠ That query bounds a 90-day window, not "never". The stronger claim is structural: the
mechanism these jobs tick through — `sync_orchestrator.progress.report_progress` — is real
and wired, but the **listener** is not. `set_active_progress` has exactly one caller
(`adapters.py:104`), so the ContextVar is unset on every APScheduler fire and
`report_progress` returns at its first line.

## 2. The thing this actually fixes is `dev_reload`, not rule 4

`dev_reload`'s own docstring (`app/jobs/dev_reload.py:38-60`) records why the deferral was
built:

> On 2026-08-21 that destroyed three consecutive `strategy_backtest_run` attempts inside
> one hour; the last died 8 seconds after its own heartbeat, i.e. while healthy and 12,625
> of 17,290 targets through. The trigger each time was an ordinary merge re-detaching the
> checkout at `origin/main`.

`docs/review-prevention-log.md:1353` records the same class on the candle path — *"four
consecutive sweeps were reaped by an ordinary jobs-daemon restart … and lost 100% of their
work"*.

`_LIVE_JOB_SQL` requires `last_progress_at IS NOT NULL`, so **the protection has never once
applied to any job but `strategy_backtest_run`.** That is the defect being closed here; the
rule-4 signal is secondary, and §5.2 shows it is narrower than revision 1 claimed.

⚠ Corrected from revision 1: "every deploy still preempts them" does not follow and is
withdrawn. A concurrent `strategy_backtest_run` can already defer the whole child, and a
short job can finish inside the drain budget. The supported claim is the one above — these
jobs cannot *themselves* defer a reload today.

## 3. Coverage — five job names, and two of them arrive by nesting

`report_progress(` call sites under `app/`, resolved to the `_tracked_job` that owns the row
they would write:

| tick site | writes the row of |
| --- | --- |
| `app/services/market_data.py:622,751,763,767` (`refresh_market_data`) | `daily_candle_refresh` (`scheduler.py:3236`) |
| `app/services/fundamentals/__init__.py:3155-3261` (`execute_refresh`) | `daily_financial_facts` (`scheduler.py:4065`) |
| `app/workers/scheduler.py:4500,4529` (thesis batch loop) | `thesis_refresh` (`scheduler.py:4389`) |
| `app/services/fundamentals/__init__.py:617-648` (`refresh_financial_facts`) | `expected_filings_poller`, via `run_force_refresh` (`app/jobs/expected_filings_poller.py:419` → `app/services/fundamentals/force_refresh.py:157`) |

⚠⚠ **Revision 1 said three and excluded the last row. That was wrong**, and so was its
claim that no nesting exists. Both were found by Codex checkpoint 1 and confirmed against
the code:

- `fundamentals_sync` (`scheduler.py:5410`) calls `daily_financial_facts()` directly, so a
  `_tracked_job` **does** nest inside another `_tracked_job` on a live path.
- `expected_filings_poller` (`scheduler.py:9220`) reaches a tick site through
  `run_force_refresh`, which revision 1 dismissed as "not a scheduled job body".

The listener is installed for every tracked job, but a job whose body never ticks never
writes a row, so coverage is these four writers plus whatever future loop adds a tick.

⚠ Of the six jobs the 2026-09-14 comment measured as *"median successful run exceeds its own
threshold with zero heartbeat"*, this fixes exactly **one**: `thesis_refresh`. The other five
have no tick site and are untouched. Adding tick sites to them is separate work and is NOT
in scope — a tick site is a claim about a loop's item granularity, and inventing one per job
without reading the loop is how a fake heartbeat gets shipped.

## 4. Design

### 4.1 Install point — `_tracked_job`, on both live branches

`_tracked_job` (`scheduler.py:2578`) owns the `job_runs` row and is where `tracker.run_id`
becomes known. It has three `yield` sites:

* the prelude branch (`:2630`), which **returns at `:2663`** — the primary path for
  scheduled fires and queue-dispatched manual triggers;
* the start-failure fallback (`:2675`), where `run_id` is `0`;
* the main fallback branch (`:2679`).

Revision 1 of the predecessor spec installed after `record_job_start` and therefore missed
the primary path entirely (its finding 1). The fix is a helper context manager wrapped
around each yield where `run_id > 0`, so both live branches are covered and the `run_id == 0`
path installs nothing.

The ContextVar set/reset mirrors `job_statement_timeout_ms` at `:2622` — same shape, same
`finally`, nesting by token. The helper must never swallow the body's exception: it restores
its token in a `finally` and re-raises, exactly as the timeout token does.

⚠ ContextVar visibility is same-thread/same-context. A job body that dispatches its loop to
a worker thread will not see the listener — which is the pre-existing contract of
`report_progress` and is unchanged here, not introduced.

### 4.2 Only the jobs process heartbeats

The heartbeat writes only when `app.db.background_write.get_background_pool()` is not
`None`. That is true in exactly one place: `app/jobs/__main__.py:1052`, which registers the
pool at daemon boot and clears it at `:1353`.

This is the predecessor spec's unanswered ownership finding, discharged.
`_LIVE_JOB_SQL` has no ownership predicate, and `dev_reload`'s docstring justifies that with
*"the singleton fence means there is exactly one jobs process, so a freshly-heartbeating
`running` row is necessarily the current child's"*. That argument holds for
`strategy_backtest_run` because it only ever runs in the daemon. It would **not** hold for a
heartbeat installed in `_tracked_job`, which a CLI invocation, a script or a test also
enters — any of which could then defer the supervisor's reload from outside the child it is
supervising. Gating on pool registration restores the premise the consumer already relies
on, rather than adding a predicate to the consumer that would have to encode process
identity.

### 4.3 No synthetic tick

`set_active_progress` fires `callback(0, None)` at install time on purpose
(`progress.py:44-52`). Installing through it would give **every** tracked job a
`last_progress_at` at t=0 — including the ~80 whose bodies never tick.

⚠ Corrected from revision 1: that would not *permanently* mute rule 4, because rule 4's
`COALESCE(last_progress_at, started_at)` fallback only shifts the deadline by the install
delay. The accurate objection is narrower and still decisive: a t=0 stamp is a **fabricated
progress claim** on a job that has made none, and `processed_count = 0, target_count = NULL`
would render as a live ticker on the admin row for a body that never reports.

`set_active_progress` therefore grows one keyword-only parameter, `initial_tick: bool = True`,
and `_tracked_job` passes `False`. Suppressing at the INSTALL site rather than by
value-sniffing `(0, None)` inside the callback is deliberate: a genuine first tick of
`(0, N)` is real information and a payload-sniffing suppressor cannot tell the two apart.

⚠ The sync-orchestrator UI is unaffected: `adapters.py:104` still installs with the default
`initial_tick=True`, and in any case `_record_layer_started`
(`app/services/sync_orchestrator/executor.py:499`) already sets the layer `running` before
the adapter runs, so the initial tick updates counts rather than a pending→running
transition. Revision 1's rationale for that tick was overstated; the parameter default
preserves current behaviour either way.

### 4.4 Chaining — to the nearest NON-heartbeat callback

The orchestrator installs its callback OUTSIDE `legacy_fn()` (`adapters.py:104`) and
`_tracked_job` opens INSIDE it, so an unconditional install would silence the sync-run
progress surface for the job's duration. The heartbeat therefore captures whatever callback
is active at install time (a small accessor on `progress.py`; the module exposes none today)
and calls it first, guarding it with `try/except` so an orchestrator-side failure cannot
cost the heartbeat write, and vice versa.

⚠⚠ **A heartbeat never chains to another heartbeat.** When the active callback is already a
heartbeat — the `fundamentals_sync` → `daily_financial_facts` nesting confirmed in §3 — the
new one chains to *that* heartbeat's inner callback instead. Otherwise the parent's
`processed_count` would be filled with the child's item counts, which is a fabricated
measurement of a different unit of work. With this rule the child writes only its own row,
and the parent keeps `last_progress_at` NULL, so rule 4 correctly falls back to
`started_at` for it — the status quo, not a regression.

Calling the captured callback directly (rather than re-entering `report_progress`) is
correct and avoids recursion: `report_progress` advances its own throttle state *before*
invoking the callback, so what we forward is exactly what the orchestrator would have
received.

### 4.5 Columns, guards, cadence and connection

```sql
UPDATE job_runs
   SET processed_count  = %(processed)s,
       target_count     = %(target)s,
       last_progress_at = now()
 WHERE run_id = %(run_id)s AND status = 'running'
```

* **`progress_json` is untouched.** `daily_candle_refresh` stores its scope/session
  watermark context there and `app/services/processes/watermarks.py::_resolve_candle_offset`
  reads it; a `(done, total)` payload written over it would erase candle population
  provenance. Predecessor finding 3.
* **`target_count` is written straight through — no `COALESCE`.** Revision 1 proposed
  `COALESCE(%(target)s, target_count)` to "preserve a total once known". `sql/140` §"Bounded
  vs unbounded" is the source rule and says otherwise: *"`target_count` is nullable — NULL
  means unbounded"*. NULL is a **value the producer asserts**, not a gap to be back-filled,
  and conflating unbounded with unchanged would invent a treatment the schema already
  defines. The only `(n, None)` tick our covered writers can emit is the synthetic one,
  which §4.3 suppresses.
* **`processed_count` is written straight through too, and the limit is stated rather than
  engineered around.** A job that invokes a ticking sub-producer repeatedly —
  `expected_filings_poller`, which calls `run_force_refresh` once per symbol and so emits
  `1/1` each time — shows the sub-producer's counts, not a job-level aggregate. Monotonic
  `GREATEST` was considered and rejected: it would invent an aggregation contract `sql/140`
  does not define, on a column whose documented meaning is what the producer processed.
  `last_progress_at` — the only column any consumer turns into a verdict — is exact either
  way.
* **`AND status = 'running'`** mirrors `checkpoint_progress` and `_BacktestProgressWriter`:
  a tick landing after terminalisation must not stamp a finished row with in-flight state.
* **`now()` is transaction-start time**, so a write delayed by pool checkout can stamp a
  slightly older event as fresh. Every other producer of this column
  (`_BacktestProgressWriter`, `bootstrap_state.set_stage_processed`) uses the same
  expression; matching them is worth more than a few hundred ms of precision on a signal
  whose thresholds are 300s and 1800s.

**Cadence — a 5-second floor in the writer itself.** `report_progress`'s throttle (5 items
or 10s) is *bypassed* by `force=True`, which `market_data.py:751,767` and
`fundamentals/__init__.py:648,3261` use, so it bounds nothing. ⚠ Revision 1's "≤0.1 writes/s
per job" was therefore false. The writer keeps its own monotonic-clock floor of **5 seconds**
— the cadence `docs/proposals/ui/admin-control-hub-rewrite.md` §A3 already documents for job
telemetry — which bounds writes deterministically regardless of the caller. Consequence,
stated: `processed_count` may lag the true count by up to 5s, including at run end. That is
immaterial to both consumers, which read the timestamp.

**Connection.** `background_write_connection()` at its **default `autocommit=True`**, with
the UPDATE inside an explicit `with conn.transaction():` carrying
`SET LOCAL statement_timeout`.

⚠ The default matters: `checkpoint_progress` asked for `autocommit=False` for a single
UPDATE and the pooled seam rolled the write back before returning the connection — the bug
`3f3c3517` fixed on this same ticket. Under the default, `conn.transaction()` issues a real
BEGIN/COMMIT (the seam's own docstring), so the write commits and the failure mode cannot be
acquired.

⚠ The timeout is not decoration. The callback is **synchronous inside the job body**: a
heartbeat UPDATE that blocks on a lock blocks the work. Neither the seam nor the pool sets a
statement timeout (Codex ckpt-1), so the writer sets its own. `SET LOCAL` is scoped to the
transaction and reverts at COMMIT, so no pooled connection is left mutated — the reason
`dev_reload.live_job` bounds its own probe the same way.

**Failures are logged, not latching.** `_BacktestProgressWriter` disables itself after one
fault because it owns a long-lived connection a fault may have poisoned. This writer borrows
per tick, so a blip poisons nothing, and latching off would lose the signal for the remaining
hours of exactly the long run this exists to protect. First failure logs at WARNING and each
subsequent one at DEBUG; the 5s floor bounds the retry rate, and the floor advances on
attempt (not on success) so a sustained fault cannot hot-loop.

### 4.6 The deferral must be finite — `_LIVE_JOB_SQL` gains the runtime ceiling

`_LIVE_JOB_SQL` selects on heartbeat freshness alone. A run that ticks forever therefore
defers every AUTOMATIC reload forever. That is latent today (one producer, one job) and this
change widens it to four more, so it is bounded here rather than left to be discovered:

```sql
AND started_at > now() - make_interval(secs => %(ceiling_s)s)
```

with `ceiling_s = stale_detection.RUNTIME_CEILING_S` — the same 24h constant, already fixed
by construction and measured against the whole population in `7f8d2b99` (9 runs exceed it in
180 days, every one a `failure`; zero successful runs reach it).

⚠ This does **not** reverse `dev_reload`'s *"deliberately NOT an age cut"*. That sentence
rejects age as a **liveness** signal — using it to decide whether a job is alive would
protect a wedged job forever, which is the opposite of what the heartbeat is for. Here age is
an **upper bound on deferral**: liveness is still decided by the heartbeat, and the ceiling
only caps how long a live-looking run may hold off code activation. A run past the ceiling is
already `attention / runtime_ceiling` on the admin surface, so the two rules agree about
which runs they are describing.

An explicit stop still drains-and-kills immediately; deferral remains a delay and never a
veto.

## 5. Consequences that must be stated, not discovered

### 5.1 Deploys on the operator's box will defer behind these jobs

`dev_reload` defers an automatic reload while a live job heartbeats, re-probing every 15s and
restating the deferral every 60s, until the heartbeat goes stale by that job's own threshold
or (after §4.6) the run passes 24h. On this box a re-detach plus a source touch IS the
deploy.

Reproduce with `PYTHONPATH=. uv run python -m scripts.verify_2274_heartbeat_exposure`
(read-only, one transaction, prints its own window and every figure below).

⚠ Revision 1's table mixed a 90-day count with a 30-day occupancy and extrapolated "one
deploy in five" from a sum of spans that double-counts overlap. Both are withdrawn. The
script reports, over ONE stated window: per-job run counts and duration percentiles with the
status filter printed, and a **true union** of in-flight spans (`range_agg`), which is an
occupancy figure and explicitly *not* a deploy-deferral probability — deploys are not
uniformly distributed in time, and the loop's own deploy step is deliberately scheduled
outside the #2833 evidence window.

⚠ Inter-tick gaps are **unmeasured and cannot be measured before the producer exists** —
`thesis_refresh` ticks once per completed generation, and `daily_financial_facts` and
`daily_candle_refresh` both have unticked planning/normalisation phases that may exceed the
300s default threshold. A gap longer than the threshold means the job stops deferring reloads
mid-run and reads `mid_flight_stuck` again; it does not mean anything is broken. Measuring
the real distribution is a dev-verify follow-up on this ticket, not a precondition.

### 5.2 Rule 4 mutes for two job names, and rule 5 covers exactly those two

⚠⚠ **Revision 1's safety argument was wrong for half the set.** `stale_detection` rules 4
and 5 only reach jobs the `scheduled_adapter` enumerates, which is `SCHEDULED_JOBS`.
Measured:

| job | in `SCHEDULED_JOBS` | rules 4/5 apply | heartbeat consumer |
| --- | --- | --- | --- |
| `thesis_refresh` | yes | yes | rule 4 + `dev_reload` |
| `expected_filings_poller` | yes | yes | rule 4 + `dev_reload` |
| `daily_candle_refresh` | **no** | **no** | `dev_reload` only |
| `daily_financial_facts` | **no** | **no** | `dev_reload` only |
| `fundamentals_sync` (parent) | yes | yes | none — §4.4 gives it no heartbeat |

So the rule-4 inversion risk — a job that ticks forever reading *Working* forever — exists
for exactly the two rows where rule 5 `runtime_ceiling` applies, and rule 5 measures the
run's AGE and is pinned against heartbeat-muting by
`test_runtime_ceiling_is_not_muted_by_a_fresh_heartbeat`. That is the discharge of the forced
ordering, now scoped to the set it actually covers rather than asserted over all of them.

For `thesis_refresh` the change removes a chronic false positive: p50 2,013s against the 300s
default means it reports `mid_flight_stuck` on essentially every healthy run today, and this
ticket's own constraint is *"whatever is chosen must not train the operator to ignore it"*.

⚠ `thesis_refresh` is also the dominant deferral source, which is worth naming next to
**#2855** — open on that job loading a 10 GB Ollama model every cycle, and observing the
thesis engine is off the product path. Not actioned here; #2855 is person-gated.

## 6. What this does NOT do

- It does not terminalise, cancel or bound any run. Nothing about `job_runs.status` changes.
- It does not add a tick site to any job. Coverage is exactly §3.
- It writes no new column, needs no migration and no backfill. Existing rows keep
  `last_progress_at IS NULL`, which every consumer already handles.
- It does not touch a threshold. `stale_thresholds._OVERRIDES` and `DEFAULT_THRESHOLD_S` are
  unchanged — retuning them mutes an alarm rather than producing a signal.
- It does not wire `app/services/job_telemetry.py`, whose `flush_to_job_run` writes the same
  columns with no `status='running'` guard and has no caller. Already recorded on the ticket;
  wiring it needs the `record_job_finish` ordering established first.
- It does not give `fundamentals_sync` a heartbeat, and does not aggregate a child's counts
  into a parent (§4.4).

## 7. Verification

- **Pure:** install suppresses the synthetic tick; chains to a non-heartbeat inner; chains
  past a heartbeat inner to its own inner; survives an inner callback that raises; writes
  nothing when `run_id <= 0`; writes nothing with no background pool registered; honours the
  5s floor and advances it on a failed attempt.
- **ContextVar lifetime across the real `yield`:** normal return, body exception (which must
  propagate unchanged), nested enter/unwind, and a second job on the same thread seeing no
  residue.
- **DB, with a registered background pool** — the pool is the test, not scaffolding: the raw
  fallback commits for unrelated reasons and would pass against a broken writer (the
  `3f3c3517` lesson), and §4.2 now makes pool presence the gate as well. Assert the three
  columns land and advance across ticks, `progress_json` is untouched, a terminalised row is
  not stamped, and an unbounded `(n, None)` tick stores NULL rather than retaining a total.
- **Install-point regression:** a tracked job on the `pre_allocated_run_id` branch produces a
  heartbeat, and so does one on the fallback branch. This is the one the predecessor spec got
  wrong, so both branches are named in the test.
- **`_LIVE_JOB_SQL` ceiling (§4.6):** a fresh heartbeat on a 25h-old run does not defer; the
  same heartbeat on a young run does.
- **Revert-probe** each guard.
- **Dev-verify after deploy:** a `running` row for one of the four covered jobs carrying a
  non-NULL `last_progress_at`, plus the observed inter-tick gap distribution (§5.1), which is
  the figure no pre-merge measurement can produce.
