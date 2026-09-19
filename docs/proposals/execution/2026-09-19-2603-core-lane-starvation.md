# The core sleeve's two observation jobs are starved by the candle sweep's lane hold

**Issue:** #2603 (items 2 and 3b-3). **Found on:** #2274's drill-in pass, recorded there
and deferred to the core path. **Status:** proposal, 2026-09-19, revised after Codex
checkpoint 1 (15 findings; 3 changed the design, 4 corrected claims — see "What ckpt-1
changed").

## The defect

`core_rebalance_observation` — the only *scheduled* producer of
`strategy_core_rebalance_intents`, and #2603 step 3b-3's whole deliverable — **has never
completed a run**.

```sql
SELECT status, count(*), min(started_at)::date, max(started_at)::date
  FROM job_runs WHERE job_name = 'core_rebalance_observation' GROUP BY 1;
```

→ `skipped` 12, `failure` 6, **`success` 0**, 2026-08-22 → 2026-09-18.

⚠ "Only scheduled producer" is the accurate form: `strategy_core_executor.py:704` also
calls `record_core_rebalance_intent` on the attended execution path, so zero scheduled
successes does not mean zero intent rows.

The failure mode changed twice and only the current one is live:

| window | outcome | status |
| --- | --- | --- |
| 08-22 → 09-12 | `prereq_missing: no core mandate configured` + a paired orphan `failure` | the orphan pairing is **already fixed** (#2972: `_record_prereq_skip` now consumes the prelude row id); the prereq skip itself was truthful |
| 09-13 → 09-14 | `prereq_missing` only | truthful — no mandate existed yet |
| **09-15 → 09-18** | **`lane_busy`, 4 of 4 fires** | **this document** |

## Root cause, verified on the full population

`daily_candle_refresh` runs as an orchestrator LAYER, and `_run_legacy`
(`app/services/sync_orchestrator/adapters.py:103`) holds `JobLock` across the entire body
and the outcome read — per-instrument commits do not release a session-scoped advisory
lock. So the sweep holds `job_source:etoro` for **3.2–3.8 hours**:

| date | sweep window | `last_progress_at` vs `finished_at` | end status |
| --- | --- | --- | --- |
| 09-15 | 20:26:33 → 00:14:49 | −5 s | success |
| 09-16 | 20:14:38 → 00:03:30 | −0.1 s | degraded |
| 09-17 | 20:10:26 → 23:25:36 | **−3 s** | failure (orphan reap) |
| 09-18 | 20:21:25 → 23:45:14 | **−4 s** | failure (orphan reap) |

⚠ The third column is load-bearing and was added at ckpt-1's insistence. Two of the four
rows end `orphaned: reaped at boot`, where `finished_at` is the **reap** time and proves
nothing about when the thread died — so the interval alone cannot bound the lock hold.
`last_progress_at` (the #2274 heartbeat) is the independent witness: the sweep was
emitting progress 3–4 s before the reap, so it was alive — and holding — straight through
22:45.

`core_rebalance_observation` fires daily at **22:45**, inside every one of those windows.
Its patient acquire-backoff is `_LANE_BACKOFF_DAILY_PATIENT` = ~11.5 s
(`app/jobs/runtime.py:1065`), sized by #1707/#1710 against
`orchestrator_high_frequency_sync` holding a gate for "~2-5s". It cannot outlast a
3.4-hour holder, so every fire records `lane_busy` and returns.

**Discriminator, over every row rather than a sample — 22/22, no exceptions:**

| job | rows classified | `lane_busy` with the sweep live | non-`lane_busy` with the sweep live |
| --- | ---: | ---: | ---: |
| `core_rebalance_observation` | 18 (all it has) | **4 / 4** | **0 / 14** |
| `core_eligibility_refresh` | 18 `lane_busy` rows | **18 / 18** | — |

```sql
SELECT (SELECT count(*) FROM job_runs d
          WHERE d.job_name = 'daily_candle_refresh'
            AND d.started_at <= j.started_at
            AND coalesce(d.finished_at, now()) >= j.started_at)
  FROM job_runs j WHERE j.job_name = 'core_rebalance_observation';
```

`core_eligibility_refresh` (#2603 item 2, hourly @ :20) is the second victim: 18 lost
fires 09-14 → 09-18 against 26 successes. Hourly fires get the hours outside the sweep;
the daily job gets none.

### Two mechanisms ruled out by measurement, not by argument

1. **Not the execution semaphore.** `core_rebalance_observation`'s 09-18 row carries
   `execution_slot_wait_seconds = 0.003`, and seven other `general_non_sec` jobs started
   and succeeded inside the same sweep (`expected_filings_poller`,
   `jobs_liveness_watchdog`, `sec_manifest_worker`, `account_reconciliation_check`, …).
   That establishes the general permit was **available at the contended instant** — not
   that it is free throughout, which the evidence does not support and the fix does not
   need.
2. **Not a lanemate holding the lane while parked on a slot.** `_job_execution_slot` is
   the OUTERMOST boundary (`app/jobs/runtime.py:2862`, above
   `_fire_scheduled_with_lane_retry` at `:2786`), so a job cannot own the lane while
   waiting for a permit. `strategy_intraday_harvest`'s co-timed skips never touched the
   lane at all — the `prerequisite` gate runs at `:2706`, *before* the acquire.

## Fix

Move each core job onto its **own** source lane:

| job | new lane |
| --- | --- |
| `core_rebalance_observation` | `etoro_core_rebalance` |
| `core_eligibility_refresh` | `etoro_core_eligibility` |

- `app/jobs/sources.py` — add both to the `Lane` Literal and document them. The existing
  `etoro` bullet is stale (it omits both core jobs, `daily_portfolio_sync` and
  `recommendation_order_reconcile`) and is corrected in the same edit.
- `app/workers/scheduler.py` — `source=` on the two `ScheduledJob` rows.

Precedent: **#2934** moved `quotes_refresh` off this same holder to `etoro_quotes`, and
**#1526** split `jobs_liveness_watchdog` / `jobs_retry_sweeper` off the catch-all `db`
lane. The precedent moves the **victim**.

### Why two lanes and not one shared `etoro_core`

The first draft proposed one shared lane on the rationale that "eligibility proofs feed
the rebalance read". **That is false** — `core_rebalance_observation` reads the mandate,
the capital authority and one broker snapshot; it never reads
`strategy_core_eligibility_proofs`. With no ordering dependency, a shared lane only adds
a collision surface:

- `core_eligibility_refresh` is capped at `CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN = 100`
  requests at `REQUEST_INTERVAL_S = 3.33 s` → **up to 333 s of explicit sleeps** before
  HTTP and DB time. Its longest observed `success` is 63.9 s, and one `failure` ran
  **1022.8 s**.
- So a :20 fire can still be running at :45, and a max is not a bound — a shared lane
  would re-create this ticket's own starvation between the two jobs.

That is exactly the reasoning `tests/test_db_lane_infra_starvation.py` records for keeping
`db_liveness` and `db_retry` separate rather than minting one `db_infra`. Two lanes cost
nothing extra: a `JobLock` connection is held per *running job*, so moving a job between
lanes does not change the count of concurrently-held locks.

### No constant is chosen

No threshold, window or ratio is introduced, so there is nothing to fix by construction.
That is a reason to prefer this shape: rejected alternative (3) below *would* have needed
one, and only a historical extremum was available to pick it from.

## Safety

### 1. No new execution permit; #1472's ceiling untouched

`execution_lane_for` (`app/jobs/runtime.py:867`) branches on `sec_rate`, then
`JOB_STRATEGY_PAPER_CYCLE`, then `_CORE_PREFLIGHT_FRESHNESS_PRODUCERS`, then falls through
to `EXECUTION_LANE_GENERAL`. `build_scheduler_executors` (`:904`) enumerates the fixed
reserved execution lanes, **not** `Lane` literals, and the connection-budget formula does
not count source lanes. Both jobs stay on `general_non_sec` / executor alias `default`
before and after. Pinned by a test.

### 2. Upstream request budget — per job, from the vendor's documented quota map

⚠⚠ **The draft's claim that `implementations/etoro.py`'s process-wide clock bounds this
was FALSE and must not be reused.** That clock belongs to `EtoroMarketDataProvider`, which
is what #2934's split relied on. Both jobs here use `EtoroBrokerProvider`, whose
`shared_ts` / `shared_throttle_lock` are constructed **per instance**
(`etoro_broker.py:287-288`) — two concurrent sessions pace independently. The correct
source rule is `app/providers/implementations/etoro_quota_lanes.py`, which encodes eToro's
published per-endpoint budgets:

| job | call | quota lane | documented |
| --- | --- | --- | --- |
| `core_eligibility_refresh` | eligibility POST ×N | `B_eligibility` | 20/min, **dedicated — "not shared with any other endpoint"** |
| `core_rebalance_observation` | `get_account_risk_snapshot` → `GET /api/v1/trading/info/demo/pnl` | `E_account_read` | 60/min, **shared** (pnl / portfolio / instrument-breakdown) |

- **Eligibility is budget-isolated by the vendor's own rule.** Nothing else can draw on
  `B_eligibility`, so de-serialising it from the `etoro` lane cannot breach any budget.
  Its existing 3.33 s pacing is already derived from that budget and is unchanged.
- **Rebalance shares `E_account_read` with `daily_portfolio_sync`'s `get_portfolio`.** It
  issues **exactly one** request per fire, once per day. Its co-tenant is independently
  paced by `_ETORO_READ_INTERVAL_S = 1.1 s` (≤ ~54 stamps/min). One extra stamp in a
  rolling minute leaves the 60/min ceiling intact. This is arithmetic against a published
  budget, not an appeal to a shared throttle that does not exist on this path.

### 3. Output writes are disjoint

`daily_candle_refresh` writes `price_daily`, `price_daily_revision`,
`price_daily_backdated_insert` and `instrument_price_supply`, with `skip_quotes=True`
(prevention-log #211) so it does not write `quotes`. `core_rebalance_observation` writes
`strategy_core_rebalance_intents` (`strategy_core_rebalance_intent.py:95`);
`core_eligibility_refresh` appends `strategy_core_eligibility_proofs`
(`strategy_core_eligibility.py:327`), whose only other writer is a hand-run script. Both
also append credential-access audit rows and job telemetry, which are append-only. No
shared row, no shared watermark.

### 4. ⚠ A real cost the split introduces: input coherence with `daily_portfolio_sync`

Disjoint output tables do not prove coherent **inputs**, and this one is not disjoint:

- `load_engine_capital_authority` builds `realised_delta` from `_load_realised_delta`
  (`strategy_engine_capital.py:376` → `:112`), which reads **`trade_events`**.
- `daily_portfolio_sync` **writes** `trade_events` (`portfolio_sync.py:851`,
  `record_trade_events`) and is an `etoro` lanemate today — **953 successful runs in 7
  days, avg 5.2 s** (~0.9 % lane occupancy).

So the shared `etoro` lane currently excludes that overlap, and the split removes that
exclusion. Accepted deliberately, because:

- the job **already guards it**: it re-reads the authority under
  `PAPER_ALLOCATOR_ADVISORY_LOCK` + `CORE_MANDATE_ADVISORY_LOCK` (`scheduler.py:6586-6587`)
  and raises `RuntimeError("assigned-capital authority changed during core observation")`
  on any change. The race is **fail-loud**, not silent misattribution;
- the exposure is one ~1 s window per day against a writer occupying ~0.9 % of wall-clock,
  and the next daily tick is correct;
- the status quo is 100 % of fires lost. A ~1 %/day loud failure strictly dominates.

⚠ Residual, stated rather than left to be found: a `trade_events` write landing *after*
the final re-read is not caught by the equality check. That is inherent to snapshot
reads and is unchanged in kind by this fix — only in probability.

### 5. The lane is not load-bearing for the moved jobs' own writes

Both jobs already carry their own locking discipline for anything authority-bearing:
rebalance takes the two `pg_advisory_xact_lock`s above; eligibility uses a locked
credential reader at record time (`FOR SHARE`) rather than relying on the source lane.
That answers the #1534 test from the code rather than from the absence of a
counter-example — with the §4 exception, which is named rather than waved away.

## Rejected alternatives

1. **Lengthen the patient backoff.** A 3.4-hour acquire wait would pin an APScheduler
   worker thread and one of only three `_LANE_WAIT_SLOTS` (`_MAX_CONCURRENT_LANE_WAITERS
   = 3`). The constant is not the bug; the shared lane is.
2. **Move the holder (`daily_candle_refresh`) instead.** Larger disjointness proof — it is
   the bulk writer of `price_daily` — and it inverts the #2934/#1526 precedent.
3. **Re-time `core_rebalance_observation` off 22:45.** The holder's end varies 23:25 →
   00:14 across four observed days, so any new slot is a constant picked from a historical
   extremum, which is not a bound. It also discards the 22:45 anchor's stated rationale.
4. **Re-arm a lost daily fire (one-shot retry).** The correct *general* fix for the class
   and genuinely larger: it changes the scheduler substrate and needs double-fire and
   stop-signal handling. Recorded as the follow-up shape rather than built. Note both
   movers are `catch_up_on_boot=False`, so a missed fire is still lost — this fix removes
   the recurring blocker, not that property.

## Acceptance

- `source_for` returns the new lane for each mover; `execution_lane_for` still returns
  `general_non_sec` and the executor alias/count are unchanged.
- **Update the existing source assertions** that pin `etoro` today:
  `tests/test_2603_core_rebalance_observation_job.py:394` and
  `tests/test_2603_core_eligibility_refresh.py:136`. `_ALLOWED_SOURCES` alone cannot
  prevent a re-collapse, because `etoro` legitimately remains an allowed lane — so the
  guard has to assert the movers are **not** on it, and are not on each other's.
- A real-lock test proving the split: a sibling holds `job_source:etoro` while each mover
  acquires its own lane. ⚠ Two traps, both already solved in
  `tests/test_db_lane_family_split.py` and reused: advisory locks are **per-database**, so
  it must target `test_database_url()` on the `postgres-test` cluster (never the dev DB the
  jobs daemon is locking), under the shared `xdist_group`; and `JobLock` has a #1184
  same-context re-entrancy bypass, so nested acquires in one thread/context would fake a
  pass — the holder must be an independent connection.
- **Dev-verify**: the next 22:45 fire records a terminal status that is not `lane_busy`.
  ⚠ `lane_busy` is an `error_msg` prefix on a `skipped` row, and a *prerequisite* skip is
  also `skipped` — so "not red" is not the check. The check is that the row's reason is not
  the lane, and ideally that the body was entered. It may legitimately still be
  `prereq_missing`: this change removes the lane as the blocker, it does not assert a
  mandate exists.

## What ckpt-1 changed

Three findings changed the design, four corrected claims I had already written:

1. **The HTTP-throttle safety proof was false** — per-instance broker throttle, not the
   process-wide market-data clock. Replaced with the documented quota-lane rule (§2).
   This is the finding that mattered: it would have shipped a safety argument that reads
   correctly and cites the wrong provider.
2. **The shared-lane ordering rationale was false** — rebalance does not read proofs.
   Design changed to two lanes.
3. **Eligibility's workload was understated** — one POST *per instrument*, cap 100, 3.33 s
   spacing (up to 333 s), not "one read per fire, ~30 s".
4. **An input race was omitted** — `trade_events` (§4), now stated with its arithmetic.
5. **"Sole producer" was stale**, **"general permit free throughout" overclaimed**, the
   `etoro` bullet correction **missed `recommendation_order_reconcile`**, and the
   discriminator needed the `last_progress_at` corroboration against orphan-reap intervals.

## Not fixed here, stated rather than left to be found

- `daily_candle_refresh` ends `failure` or `degraded` on 3 of 4 observed days and holds one
  lane for 3.4 h doing it. That belongs to the candle path, not #2603.
- The health surface reported this correctly throughout: #2052 makes `lane_busy`
  non-anchoring, so the job sat pinned red with `schedule_missed` — which is what #2274's
  pass surfaced. The signal existed and was not read.
