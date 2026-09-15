# RETRYING is locked out of the one scope that recovers a behind layer

Refs #2274 (its item 2: `dev_reload` reaps long jobs, and the operator gets no signal beyond a
`failure` row). Not the watchdog half — that stays open.

## The defect

`_scope_to_candidate_jobs(scope="behind")` selects **direct targets** from
`{DEGRADED, ACTION_NEEDED}` (`planner.py:126-127`). `compute_layer_state` returns `RETRYING` for a
failed layer in a `self_heal` category whose consecutive-failure streak is below `max_attempts`
(`layer_state.py:60-71`).

The resulting direct-target gate is **non-monotone in failure count**:

| consecutive failures | state | direct target of `behind`? |
| ---: | --- | --- |
| 0 (stale / content not ok) | `DEGRADED` | ✅ |
| 1 … `max_attempts`−1 | `RETRYING` | ❌ |
| ≥ `max_attempts` | `ACTION_NEEDED` | ✅ |

A layer that has failed **less** is treated as less recoverable. The table assumes the default
policy, a `self_heal` category, and no overriding `DISABLED` / `RUNNING` / `SECRET_MISSING` /
`CASCADE_WAITING` state; it describes direct selection, not every `behind` plan.

### The exclusion rests on an actor that was never built

`freshness-unification.md:97` — *"these are self-healing states, the orchestrator is actively
catching up"*; line 78 — *"Self-heal category, budget remains, orchestrator will re-fire with
backoff."*

**No production code re-fires a layer.** `RetryPolicy.backoff_seconds` is read only by its own
`__post_init__` validation and by tests; the sole production reader of `retry_policy` is
`layer_state.py:184`, taking `max_attempts`. So `RETRYING` names a catch-up that does not happen,
and the planner excludes the layer on the strength of the name.

The other recovery path is dead by design: `ops_monitor._reap_orphaned_job_runs` routes each
reaped row through `_retry_plan` and stamps whatever it returns (a timestamp, or `None` once the
cap is exceeded), but `job_retry.sweep_due_retries` clears it as a stray for any job outside
`eligible_job_names` — which is every sync-runs-tracked layer job, `daily_candle_refresh`
included (`job_retry.py:80-86`; the behaviour is documented at `ops_monitor.py:784-789`).

### RETRYING is already planned via closure — just not when it is alone

`_transitive_upstreams_not_healthy` (`planner.py:131`) pulls in **every** non-HEALTHY upstream of
a selected target, and that set includes `RETRYING`. Codex reproduced both halves: an isolated
`RETRYING` candles layer yields an empty plan, while `RETRYING` candles + `DEGRADED` scoring
plans both. The same bypass exists for composite jobs — `scoring` and `recommendations` share one
job, so a `DEGRADED` scoring already fires a `RETRYING` recommendations.

So firing a `RETRYING` layer from a `behind` sweep is **not a new behaviour class**. It is what
the planner already does whenever anything else happens to be degraded. The defect is that
recovery of a failed layer depends on an unrelated layer also being unhealthy.

## Measured consequence

`SyncScope.behind()` has exactly two callers — `boot_sweep.py:37` and the operator's
`POST /sync/run` default (`api/sync.py:185`); the docstring at `freshness.py:384` says the same.
Neither is on a cadence, so between them nothing re-plans a behind layer. The daily `full`
scheduled sync does recover `candles` through freshness, which bounds the outage at ~24h rather
than forever — but that run is itself the multi-thousand-instrument sweep, i.e. the run most
likely to be reaped, which is how the state persists across days.

Live at 2026-09-15 14:3x UTC: `candles` = `RETRYING`; `candles_is_fresh` =
`(False, 'latest daily_candle_refresh has status=failure, not a counting row')`;
`daily_candle_refresh` last ran at 03:00Z and was reaped. Two redeploys (13:22Z, 14:22Z) each
fired a boot sweep; each planned **0** layers, while the 14:22:16Z sweep correctly planned
`fx_rates` (`DEGRADED`). `price_daily`: **4,292 instruments** still missing their 2026-09-14 bar.

Full population, 14 days of `behind` sweeps — **5 of 64 planned 0 layers, and all 5 had `candles`
as a failed head row with an as-of streak of 1 or 2**, i.e. inside the RETRYING band:

| sweep | at | candles head | streak |
| ---: | --- | --- | ---: |
| 24298 | 09-13 17:58Z | failed | 1 |
| 24314 | 09-13 18:58Z | failed | 1 |
| 24339 | 09-13 20:34Z | failed | 1 |
| 24395 | 09-14 00:20Z | failed | 1 |
| 24892 | 09-15 14:22Z | failed | 2 |

Zero-plan is a legitimate outcome in general (everything healthy, disabled, running,
secret-blocked). Over these 14 days it was not: every instance had this defect underneath it.
`candles` is the most-failed layer in the corpus — 112 failed `sync_layer_progress` rows against
48 / 22 / 8 / 1 for every other layer — because it is the only one whose sweeps run long enough
to be reaped.

## The fix

Add `RETRYING` to the `behind` direct-target set:

```python
target_layers = {n for n, s in states.items()
                 if s in {LayerState.DEGRADED, LayerState.RETRYING, LayerState.ACTION_NEEDED}}
```

That is the whole change.

⚠ It is **not** the same predicate as the closure arm, and an earlier draft of this spec said it
was. `_transitive_upstreams_not_healthy` excludes only `HEALTHY` and `DISABLED` — it can pull in
`RUNNING`, `SECRET_MISSING` and `CASCADE_WAITING` upstreams — and it stops traversal at a healthy
intermediate. The direct-target set stays strictly narrower. Neither is a restatement of the other.

### Why not the backoff gate the spec describes

The first draft of this spec gated inclusion on `now - last_failure >= backoff_seconds[streak-1]`,
to implement line 78 and to brake redeploy thrash. Codex checkpoint 1 killed it on two grounds,
both verified:

- **The deferral has no wake-up, and it fires on exactly the wrong case.** The reaper stamps
  `finished_at = now()` moments before the boot sweep runs, so a freshly-reaped layer is always
  *inside* its backoff at the one sweep meant to rescue it, and nothing revisits it when the
  window expires. Live instance above: sweep 24298 ran **3 seconds** after the failure it should
  have recovered.
- **The brake is unreachable where thrash is worst.** At streak ≥ `max_attempts` the state is
  `ACTION_NEEDED`, which the gate does not cover — so the final `backoff_seconds` entry can never
  apply, and a repeatedly-failing layer ends up firing on every boot unbraked anyway. The bound
  the design claimed does not exist.

It is also self-contradictory: a layer deferred as a direct target is re-added by the closure arm
whenever another layer is degraded, so the "include iff past backoff" rule cannot hold.

Dropping it removes the whole surface Codex flagged around it — failure-timestamp anchoring,
nullable `finished_at`, the `[streak-1]` index contract, streak durability across
skipped/cancelled rows, and state/history snapshot consistency.

**This leaves `backoff_seconds` still unconsumed, and the spec's promised re-fire still
unimplemented.** Stated rather than papered over: this change makes the existing `behind` trigger
recover the layer, it does not add the scheduler line 78 describes.

### What this does not claim

`behind` bypasses the freshness re-filter (`planner.py:38-45` — state selection is authoritative),
so an included layer is planned unconditionally. It is **not** guaranteed to run: credentials,
layer-initialization gates, failed dependencies, cancellation and the adapter's `JobLock` can all
stop it at execution. Planning inclusion is necessary for recovery, not sufficient.

Two safety claims from the previous draft were too strong and are withdrawn:

- **"`JobLock` prevents a boot sweep double-firing a running sweep."** `JobLock` guards *adapter
  overlap*; whole-sync overlap is guarded by the `sync_runs` unique index. Different mechanisms.
  And a newly-selected target can pull in a `RUNNING` upstream that finishes before the lock is
  acquired, which then runs again without freshness revalidation.
- **"No new thrash risk."** False for a previously-isolated `RETRYING` layer. Repeated
  restart/reap cycles now retry it on every boot, and repeated operator `Sync now` requests queue
  on a single-worker executor and run in sequence, so fast failures can re-fire back-to-back
  without `SyncAlreadyRunning` ever tripping. `max_attempts` does not brake this — it only
  relabels the layer `ACTION_NEEDED`, which is selected too. The honest statement is that the
  retry is now *unbraked and intended*; braking it is what `backoff_seconds` is for, and that
  remains unbuilt.

Recovery also stays lossy in ways this change does not address: a boot sweep rejected for overlap
is swallowed with no deferred wake-up, and an adapter skipped for lock contention is not
rescheduled. So the ~24h outage bound rests on the daily `full` sync, not on this path.

### Explicitly out of scope — pre-existing, now more reachable

- **An operator-DISABLED layer can be written by an enabled sibling.** A job is selected if *any*
  emit is a target and the executor does not recheck the enabled flag, so `scoring=DISABLED` +
  `recommendations=RETRYING` runs `morning_candidate_review` and emits both. That was already true
  for a `DEGRADED` recommendations; this change adds one more trigger. `morning_candidate_review`
  is the registry's **only** multi-emit job and dev currently has **zero** disabled layers, so the
  reachable surface is one pair. Pinned by `test_behind_composite_job_runs_disabled_sibling` so it
  is explicit; the fix belongs in the executor.
- **A skipped run can erase a failure signal without recovering.** Lock contention, dependency /
  init / credential skips and cancellation replace the failed head row and reset the streak, so a
  layer can go `RETRYING → HEALTHY` without ever succeeding. History semantics, not selection.
- **Refreshing a layer alone does not invalidate healthy downstreams.** An isolated `candles`
  refresh leaves scoring, risk metrics and quarantine verdicts describing pre-retry prices;
  quarantine freshness is age-based, not keyed to candle revisions. Pre-existing architectural
  gap, newly reachable through this path.
- `ACTION_NEEDED` gets no backoff today and keeps none.
- `jobs_retry_sweeper`'s stray-clearing stays. It is correct for a sync-tracked job — the
  orchestrator owns that recovery, which is what this change makes true.
- #2274's no-progress watchdog. Untouched.

## Acceptance

All in `tests/test_sync_orchestrator_planner.py::TestBehindScopeTargetStates`.

1. **All eight `LayerState`s as an isolated direct target**, parametrised, on a layer with no
   unhealthy neighbours — so selection is attributable to the direct-target predicate rather than
   to closure. `DEGRADED` / `RETRYING` / `ACTION_NEEDED` selected; `HEALTHY`, `RUNNING`,
   `DISABLED`, `SECRET_MISSING`, `CASCADE_WAITING` not.
2. **`RETRYING` fires with every freshness predicate returning fresh** — pins that `behind`
   bypasses the re-filter, the interaction the backoff design would have broken.
3. **Closure regression:** `RETRYING` upstream + `DEGRADED` downstream still plans both, exactly
   once each (no duplicate now that a layer can be target *and* upstream).
4. **Composite pin:** `recommendations=RETRYING` + `scoring=DISABLED` runs the shared job. Asserts
   the pre-existing gap explicitly rather than widening it silently.
5. **Non-`behind` scopes ignore layer state:** `high_frequency` selects by emit name and is
   unaffected by a `RETRYING` candles.
6. **Revert-probe, run:** with `LayerState.RETRYING` removed from the target set, exactly three
   tests fail (`[retrying-True]`, the freshness-bypass test, the composite pin) and the other nine
   pass in both arms — so they pin existing behaviour, not the change.
7. **Dev-verify on the real stack, both arms at the same instant on the same DB state:**
   `build_execution_plan(conn, SyncScope.behind())` returns
   `['daily_portfolio_sync', 'fx_rates_refresh']` before and
   `['daily_portfolio_sync', 'fx_rates_refresh', 'daily_candle_refresh']` after, with `candles`
   = `retrying`. Note closure did **not** rescue `candles` despite two `DEGRADED` layers being
   present, because neither is a descendant of it — Codex's point that closure only helps when the
   unhealthy layer is downstream.

The five-sweep table is reproduced by, over `now() - interval '14 days'`:

```sql
select sync_run_id, started_at from sync_runs
 where scope='behind' and layers_planned=0 and started_at > now() - interval '14 days'
 order by started_at;
```

then, per sweep, walking `sync_layer_progress` for `candles` backwards from that `started_at` and
counting consecutive `failed` rows until the first `complete`/`skipped`. Re-run it rather than
copying the table — the window is relative and the numbers move.
