# #2274 — a running `sync_runs` row is opaque, and the singleton it holds is invisible

Status: proposal (revision 3, after two Codex checkpoint-1 passes — 29 then 20
findings; the second killed the heartbeat-stale threshold, see §3).
Issue #2274, item 1 on the sync-orchestrator surface.

## 1. The gap, measured

### 1.1 `sync_runs.last_progress_at` has zero writers

```sql
SELECT count(*) AS total, count(last_progress_at) AS with_hb
  FROM sync_runs WHERE started_at > now() - interval '90 days';
-- 18831 | 0
```

⚠ `processed_count` is `NOT NULL DEFAULT`, so `count(processed_count)` returns the
row count and says nothing — read `with_hb` only.

`rg "UPDATE sync_runs"` returns six sites. Five are terminal (two cancel
transitions, the counts recompute, `_finalize_sync_run`, the reaper). The sixth,
`app/api/processes.py:1624`, writes `cancel_requested_at` on a **running** row — so
the precise claim is that **no write advances a progress-bearing column while the
run is in flight**, not that every write is terminal. A `status='running'` row moves
only `started_at`, which does not move.

This is the third instance of one class on this ticket: a shipped consumer with no
producer (`dev_reload.live_job`), a shipped state whose actor was never built
(`RETRYING`), and now a shipped **column** with no producer.

### 1.2 The ProcessRow ceiling cannot reach this work

`stale_detection` rule 5 (`runtime_ceiling`, `7f8d2b99`) is `mechanism="scheduled_job"`
only. ⚠ Rule 4 (`mid_flight_stuck`) is **not** mechanism-gated — it fires on any row
with `status == "running"`; the earlier draft of this spec said otherwise. Both
reach a row only through `scheduled_adapter`, whose `_read_running_run` reads
`job_runs`.

| surface | rows | newest |
| --- | ---: | --- |
| `job_runs WHERE job_name='orchestrator_full_sync'` | **4** | **2026-08-09** |
| `sync_runs` | **22,588** | today |

The scheduled wrappers call `run_sync(...)` directly (`workers/scheduler.py:7315`,
`:7345`) with no `_tracked_job` wrapper, and `POST /sync` reaches the executor
through the dispatcher — which is why the `job_runs` side has four rows since
August. `scheduled_adapter._resolve_terminal_row` already re-homed the **terminal**
read to `sync_runs` for these two jobs (#1474 Part 2) and its docstring records the
active read as a deliberate deferral.

⚠⚠ **Widening that active read is NOT the fix, and the measurement is why.**
`_resolve_terminal_row` is keyed by `_ORCHESTRATOR_SYNC_SCOPE`
(`high_frequency`, `full`). `sync_runs` by trigger is `scheduled` 21,305 ·
`boot_sweep` 1,283, and a boot sweep runs `SyncScope.behind()` — a scope in neither
map. Of the **157** rows ever reaped `orchestrator_crash`, **148 are `boot_sweep`**.
A scope-keyed ProcessRow read structurally misses 94% of the population this ticket
is about. The singleton is a global resource and needs a global consumer.

### 1.3 The singleton makes the opacity expensive

```
idx_sync_runs_single_running UNIQUE ON sync_runs ((true)) WHERE status = 'running'
```

A lock expressed as data: at most one `running` row per database, with no owner and
no expiry. `_start_sync_run` maps the `UniqueViolation` to `SyncAlreadyRunning`;
`boot_sweep.py:40` logs one info line and returns. A stranded row therefore blocks
**every** sync, silently.

The only thing that clears it is `reap_orphaned_syncs`, which has **one production
caller** — `app/jobs/__main__.py`, `reap_all=True`, at boot. Its steady-state
`timeout=1h` predicate has no caller: the same "exists for a future periodic
watchdog" shape this ticket already found and fixed on `reap_orphaned_job_runs` /
`jobs_liveness_watchdog`.

Observed: **157** rows reaped `orchestrator_crash`, p50 35s, **max 14,287s (4.0h)**.
⚠ Those durations are **right-censored** — a reaped row records when a restart
happened, not when the work would have finished — exactly as `RUNTIME_CEILING_S`'s
own comment records for `job_runs`. They bound the harm from below, not above.

⚠ On this box a SIGKILLed jobs child does not respawn (`dev_reload` respawns on
`app/**` mtime, not on child death), so "until the next boot" has no upper bound.

### 1.4 A second symptom, found while checking the consumer

`AdminPage.tsx:135-136` is the ONLY consumer of `GET /sync/status`:

```ts
const isRunning = status.data?.is_running ?? false;
const refreshInterval = isRunning ? 10_000 : 60_000;
```

Nothing renders the payload. So a stranded row also pins the admin page to a 10s
refetch-everything cadence permanently — a 6× poll-load increase that no operator
can see the cause of.

## 2. What ships

### 2.1 Producer — `sync_runs.last_progress_at`

One helper in `executor.py`:

```python
def _touch_run_heartbeat(sync_run_id: int) -> None
```

- Its **own** short transaction on its own `background_write_connection()`, never
  sharing the layer-write transaction. A heartbeat failure must not roll back an
  authoritative layer result — inside `_record_layer_result`'s transaction it would,
  and committed layer work would be recorded as failed (Codex ckpt-1 #6).
- Sole statement, so it takes only the parent-row lock. The reaper locks
  parent→child; a helper that appended a parent UPDATE to a child transaction would
  invert that order (#7). A separate transaction holds one lock at a time.
- `SET LOCAL statement_timeout` — the cancel path holds the `sync_runs` row under
  `SELECT FOR UPDATE` (`app/api/processes.py:1624`) and
  `background_write_connection` sets no timeout of its own (#5).
- Never raises; failures log at debug and are counted, so "writer broken" is
  distinguishable from "no progress" (#12).
- Monotonic and status-guarded:

```sql
UPDATE sync_runs
   SET last_progress_at = now()
 WHERE sync_run_id = %s
   AND status = 'running'
   AND (last_progress_at IS NULL OR last_progress_at < now())
```

  The status guard stops a late callback resurrecting a terminal row's heartbeat;
  the monotonic guard stops an overlapping callback moving it backwards, since
  `now()` is transaction-start time (#11).

**Call sites — every path that can leave a row `running`** (#1, #2):
`_record_layer_started`, `_make_progress_callback` (after its own transaction
commits), `_record_layer_result`, `_record_layer_failed`, `_record_layer_skipped`,
`_fail_unfinished_layers`.

**Only `last_progress_at` is written.** `processed_count` / `target_count` /
`layers_done` stay out of scope: the run's unit is layers and a tick's unit is
items, and stamping a count from a tick would redefine "processed work" as
"executor activity" (#4). ⚠ This also settles the synthetic-tick question (#3):
`set_active_progress(..., initial_tick=True)` fires `(0, None)` at layer install,
and on this surface that IS a genuine liveness event — a layer just started — whereas
in `_tracked_job` it would have fabricated a progress *counter*. Liveness is stamped;
no counter is.


### 2.2 Consumer — `GET /sync/status` + the one page that reads it

`/sync/status` gains, on `current_run`: `last_progress_at` and `liveness`.

New pure function, `app/services/sync_orchestrator/run_liveness.py`:

```
assess_run(*, started_at, last_progress_at, now) -> Literal["live", "over_ceiling"]
```

- `over_ceiling` — `started_at < now - RUNTIME_CEILING_S`. Age only, never consults
  the heartbeat, so it cannot be muted: rule 5's argument, unchanged.
- `live` — otherwise. ⚠ `live` means "below the ceiling", **not** "a worker is
  alive". A dead worker at 23h reads `live`. Stated in the docstring, not only here.
- Inputs must be timezone-aware; `now` is the caller's clock and the timestamps are
  DB-written, so the function documents that it compares across those two clocks.

`AdminPage` renders the holder whenever a run is in flight — run id, scope, trigger,
started-at age and heartbeat age — and marks it when the verdict is `over_ceiling`.
Information first: the operator's complaint on this ticket is that a held singleton
has **no** signal, and an age is a signal at any duration.

- ⚠ `useAsync(fetchSyncStatus, [])` clears `data` on every refetch (it does not pass
  `preserveOnRefetch`), so today `isRunning` flips false on each 10s/60s poll and
  the interval re-arms. The chip would flicker the same way. Fixed by opting into
  the existing stale-while-revalidate path.
- ⚠ An absent `liveness` field (mismatched FE/BE deploy) must read as `live`, never
  as a warning.
- The poll cadence is deliberately **not** re-keyed on the verdict: a genuinely live
  sync past 24h would otherwise be slow-polled, and a verdict about severity cannot
  establish worker inactivity.

### 2.3 Constants

`RUNTIME_CEILING_S = 86_400` is **imported** from `stale_detection`, not re-declared.
Measured over EVERY status, 22,588 runs: 28 exceed 1h, 5 exceed 4h, **0 exceed 12h**,
**0 exceed 24h**; longest run of any status 16,659s (4.63h). ⚠ Right-censored, as
that constant's own comment records for `job_runs`: a reaped row dates the restart,
not the work.

No second constant is minted. §3 says why.

## 3. What deliberately does NOT ship, and the measurement that decided it

**A heartbeat-stale rule.** Revision 1 proposed `SYNC_HEARTBEAT_STALE_S = 3_600`,
justified as ~2× the longest non-ticking layer. Checking the tick coverage in code
rather than by module falsified the input:

- `refresh_fundamentals` (`adapters.py:207`) accepts `progress` and **does not
  forward it** to `_wrap_single`, so the `fundamentals` layer installs no callback
  and never ticks. Its max duration is **4,721s**. ⚠ Noted as a defect on the PR;
  not fixed here, because `daily_research_refresh` no longer calls
  `refresh_fundamentals` at all (#2008 removed the sweep), so forwarding the
  parameter would not by itself produce ticks and needs its own look.
- Of the adapters that DO forward it, only `candles` reaches a loop that calls
  `report_progress` recurrently. `risk_metrics`, `price_quarantine`,
  `research_price_quarantine` and `fair_value_band` forward the callback but receive
  only the synthetic install tick.

So the longest legitimate inter-heartbeat gap is at least **4,721s**, while the
observed strandings run p50 35s to max 14,287s. **The two populations overlap over
their whole range**: no threshold separates a stranded sync from a legitimately-long
one on today's evidence. A rule fitted anyway would fire on real work, which is the
one thing #2274's constraint forbids.

The prerequisite is not this heartbeat — it is tick coverage across more than one
layer. Recorded on the issue as the next piece.

**A steady-state reaper.** Terminalising a `running` row whose worker may be alive is
the destructive path #2274 already refused for `job_runs`, and here the row IS the
mutual exclusion — reaping a live holder would let two syncs run concurrently.

**Widening `scheduled_adapter._read_running_run`.** §1.2: scope-keyed, misses 94% of
the population. If done later it must land WITH a `mid_flight_stuck` threshold
override for the two orchestrator process_ids, because `DEFAULT_THRESHOLD_S` is 300s
and 28 runs exceed an hour.

## 4. Coverage claim, stated honestly

The six call sites cover every **layer** transition. They do **not** cover every
interval in which a row can be `running`:

- between `_insert_sync_run` committing and the first `_record_layer_started`
  (prelude, credential/init/cascade checks);
- a plan with zero layers, which reaches no per-layer hook at all;
- after the last layer, if the post-loop cancel check or `_finalize_sync_run` blocks
  or raises.

In all three the heartbeat is NULL and `assess_run` falls back to `started_at`, which
is the correct and intended degradation — but the claim is "layer coverage", not
"whole-lifecycle coverage".

⚠ `started_at` defaults to `now()` inside the insertion transaction, which begins
before `build_execution_plan` runs, so a long prelude publishes a run whose age
already includes its own planning.

## 5. Acceptance

1. `SELECT count(last_progress_at) FROM sync_runs WHERE started_at > <deploy>` is
   non-zero after one real sync — the query that returns 0 today. Exercised on a run
   reaching a **non-ticking** layer, so `_record_layer_result` is proved and not just
   the tick path.
2. `GET /sync/status` carries `last_progress_at` + `liveness`; `AdminPage` renders
   the holder and does not flicker across a poll.
3. Pure-logic table tests on `assess_run`: both sides of the boundary (`= ceiling` is
   `live`, `> ceiling` is `over_ceiling`), NULL heartbeat, and a fresh heartbeat NOT
   muting `over_ceiling`.
4. DB test against a **registered background pool** with an independent reader — a
   raw-connection test would conceal the pooled-seam rollback that produced the
   `3f3c3517` `checkpoint_progress` bug. Asserts per hook that the heartbeat
   advances, that a terminal row's heartbeat is not resurrected, that the guard is
   monotonic, and that a heartbeat-writer failure does not change the layer result.
