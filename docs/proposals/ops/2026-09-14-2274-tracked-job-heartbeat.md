# #2274 — the in-flight checkpoint never commits, and the heartbeat must not land before a ceiling

Status: shipped finding + the researched ordering answer for #2274's four options.
Scope shipped is the COMMIT only. The heartbeat install described in revision 1 of this
document is **not** shipped, and §3 is why.

## 1. What shipped — a writer whose write was discarded

`_JobTracker.checkpoint_progress` (`app/workers/scheduler.py`) writes through
`background_write_connection(autocommit=False)` and never committed.

That seam yields a POOLED connection and its own `finally` rolls back any non-IDLE
transaction before returning it — *"Return only an idle, autocommit-restored connection to
this shared pool"* (`app/db/background_write.py`). So the UPDATE was discarded on the
pooled path, which is the path the jobs process always takes. The raw fallback
(`psycopg.connect`, which commits on clean `with` exit) made the identical code land in a
CLI run. **Same code, opposite outcome, decided by whether a pool is registered.**

`record_job_start` and `record_job_finish` (`app/services/ops_monitor.py:637` and `:714`)
both commit internally. This was the one writer in the family that did not.

### The guarantee it falsified

Its call site claims one, in `daily_candle_refresh`:

> Per-instrument commits intentionally survive a worker restart. **Persist the denominator
> first so an orphaned sweep cannot leave partial bars with no population identity.**

An orphaned sweep is exactly the run with no terminal write. Measured on the dev DB over
60 days before the fix, `daily_candle_refresh` carried `progress_json` on **0** of its
failures, **0** of its skips and **0** of its one running row, against a majority of its
successes — every stored value written by the terminal path, none by this one. The
reproducing query is on the ticket; it is not copied here, because a derived statistic
written into prose goes stale in the place a reader trusts most.

Downstream, `_resolve_candle_offset` (`app/services/processes/watermarks.py`) prefers that
declared boundary and falls back to raw `MAX(price_date)` — which its own docstring warns
*"can be a forming or partial newest date after an orphaned per-instrument-commit sweep"*.
So the fallback was being taken on precisely the runs the checkpoint exists to protect.

Also added: `AND status = 'running'`, mirroring the strategy-evidence telemetry writer
(`scheduler.py:6606-6613`). A checkpoint landing after terminalisation must not stamp a
finished row with in-flight state.

## 2. What was specced and is NOT shipped

Revision 1 proposed wiring `_tracked_job` to install a `ProgressCallback` that writes the
`job_runs` heartbeat, so that every job whose loop already calls
`sync_orchestrator.progress.report_progress` — `market_data.py:622/751/763/767`,
`scheduler.py:4463/4492` — would produce `last_progress_at` for free. The mechanism is
real and the tick sites genuinely exist; only the listener is missing, because
`set_active_progress` is installed by one caller (`sync_orchestrator/adapters.py:104`) and
the ordinary APScheduler path installs nothing.

It is not shipped because Codex checkpoint 1 found four independent reasons it is unsafe
as specced, and two of them invert the ticket's own goal.

## 3. The researched answer to #2274's option list — there is a forced ORDER

**A real heartbeat must not land before a wall-clock ceiling.**

`stale_detection.compute` rule 4 is
`status == "running" AND COALESCE(last_progress_at, started_at) < now() - threshold`. With
`last_progress_at` NULL it degrades to elapsed time, which is noisy — it reports healthy
long jobs stuck. But it is **not nothing**: it is the only rule that fires on runaway work
at all, because `schedule_missed` and `watermark_gap` are both suppressed while a row is
`running`.

Give those jobs a true heartbeat and a job that ticks forever reads *Working* forever.
Coverage does not improve; it inverts. So the ceiling is not an independent option that
can be sequenced later — it is the thing that makes the heartbeat safe to have.

**And `dev_reload` already consumes the fields.** `app/jobs/dev_reload.py::live_job`
selects `processed_count`, `target_count` and `last_progress_at` and defers an automatic
reload while a job heartbeats — its docstring already says *"as the pre-#2274 code did"*.
So the CONSUMER of this ticket has shipped and the producer has not. Switching the producer
on changes deploy behaviour on the operator's box the same hour: on this box a re-detach
plus a source touch **is** the deploy, so a long ticking job would defer code activation,
and the probe's own query assumes every fresh running row belongs to its child, which a
direct or CLI invocation breaks.

Four further gaps that a heartbeat implementation has to answer and revision 1 did not:

1. **The install point is on the wrong branch.** Scheduled and manual runs consume
   `pre_allocated_run_id` (`scheduler.py:2608`) and yield from a separate branch, so
   installing after `record_job_start` would miss the primary path entirely.
2. **`set_active_progress` fires an immediate `(0, None)` tick.** Installing it in
   `_tracked_job` therefore gives *every* tracked job a heartbeat at t=0 — including jobs
   whose bodies never tick. That is precisely the fake heartbeat this ticket forbids, and
   it would SILENCE rule 4 for the jobs with no progress signal at all: strictly worse than
   today.
3. **`progress_json` is not a free field.** `daily_candle_refresh` stores its
   scope/session watermark context there and `watermarks.py` reads it. A `(done, total)`
   payload written over it would erase candle population provenance.
4. **Chaining is required and is not sufficient.** The orchestrator installs outside
   `legacy_fn()` and `_tracked_job` opens inside it, so an unconditional install silences
   the sync-run progress surface; but several adapters never pass `progress=` at all, so
   chaining alone does not establish coverage.

### Recommended order, for whoever takes the rest of #2274

1. **Wall-clock ceiling first** (this ticket's option 2), since it is what a real heartbeat
   removes the current stand-in for.
2. **Then the heartbeat**, installed on the `pre_allocated_run_id` branch as well, not
   writing on the synthetic zero tick, chained rather than overriding, and writing the
   count columns **without** overwriting `progress_json`.
3. **Then the no-progress watchdog**, which is the only one of the four that needs the
   signal to exist.
4. The consecutive-orphan-reap verdict is independent of all three and can go any time.

No threshold constant is touched by any of this. `stale_thresholds._OVERRIDES` and
`DEFAULT_THRESHOLD_S` are unchanged — retuning them mutes an alarm rather than producing a
signal.

## 4. Verification of what shipped

- Three DB tests (`tests/test_2274_checkpoint_progress_commits.py`), all against a real
  Postgres with a **registered background pool**. ⚠ The pool is the test, not scaffolding:
  letting the seam fall through to the raw fallback passes against the broken code, because
  that path commits for reasons unrelated to this function.
- Both guards revert-probed: dropping the commit fails the survival test, dropping
  `AND status = 'running'` fails the terminal-row test.
