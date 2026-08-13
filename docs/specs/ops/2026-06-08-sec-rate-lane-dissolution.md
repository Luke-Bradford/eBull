# Dissolve the `sec_rate` JobLock lane → in-process bounded SEC-job concurrency

Issue: #1542 (`tech-debt` / `area: ops`). Closes #1536 (made moot).
Date: 2026-06-08. Status: spec, pre-implementation.
Predecessors: #1478, #1526, #1527, #1534, #1538, #1540 (the lane-extraction whack-a-mole).
Memory: `[[project-sec-rate-lane-wrong-model]]`.

## 1. Problem

`JobLock(source)` is `pg_try_advisory_lock(hashtext('job_source:<lane>')::int)` —
a **1-wide mutex per lane** ([app/jobs/locks.py:314-324](../../../app/jobs/locks.py#L314)).
Every `_LANE_MAX_CONCURRENCY` entry is `1`
([app/services/bootstrap_orchestrator.py:245](../../../app/services/bootstrap_orchestrator.py#L245)).
So at most **one** of the ~20 jobs sharing the `sec_rate` lane touches the DB at any instant; the
loser of a non-blocking acquire **skips its whole cadence period**.

The SEC 10 req/s ceiling is enforced **process-globally at the HTTP layer**
(`_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK`,
[app/providers/implementations/sec_edgar.py:72-81](../../../app/providers/implementations/sec_edgar.py#L72)),
**not** by the lane. Running all `sec_rate` jobs concurrently cannot breach the ceiling — the lane gives
**zero rate protection**. Its only effect is serialization, which is the **sole cause** of the recurring
starvation (#1478 → #1526 → #1527 → #1534 → #1538 → #1540) and the un-healable stall #1536.

#1538 added a ~1.75s acquire-retry, but on a **1-wide** gate a single slow holder still blocks everyone;
cadence validation (2026-06-08) confirmed it as a partial fix — long-hold producers still miss when they
lose to a slow holder.

## 2. Goal

Replace the 1-wide `sec_rate` lane with an **in-process bounded semaphore (width N=4)** so up to 4 SEC jobs
run concurrently, bounded only by the HTTP rate clock (already process-global) and the connection budget
(#1472). Preserve the lane's one genuinely load-bearing property — **no two instances of the same job_name
run at once** — with a per-job-name in-process lock. No SQL migration.

## 3. Audit findings (the grounding — both gates from #1542 cleared)

### 3a. Write-safety — going 1-wide → N-wide is safe

The lane masks pairwise write races between members. Traced every member's write-set
(7 parallel read-only tracers, 2026-06-08). Result: write-correctness is guaranteed by mechanisms
**other than the lane** in every case:

| Write target | Concurrency guard (NOT the lane) | Verdict |
|---|---|---|
| `ownership_{insiders,institutions,funds,def14a,esop}_current` + `ownership_refresh_state` | per-instrument `pg_advisory_xact_lock(hashtextextended('refresh_*_current',0) # instrument_id)`; watermark captured pre-MERGE inside the lock | safe |
| `ownership_*_observations` (all 5) | `ON CONFLICT` on natural key incl `source_document_id` (= accession) | safe |
| `sec_filing_manifest` (`record_manifest_entry`) | `ON CONFLICT (accession_number)`; `ingest_status` untouched on conflict | safe |
| `data_freshness_index` (`seed_freshness_for_manifest_row`) | monotonic `CASE` (newer `filed_at` wins) — the #1534 fix, now general | safe |
| `filing_raw_documents`, `*_ingest_log`, `filing_events`, `external_identifiers` | `ON CONFLICT` idempotent on immutable accession / stable identifier keys | safe |
| `institutional_filers`, `sec_*_filer_directory`, `sec_fund_series`, `ncen_filer_classifications` | `GREATEST()` / monotonic `CASE` / monotonic `WHERE`; sole-writer for the directory + ncen tables | safe |
| `instrument_business_summary` | `ON CONFLICT DO UPDATE ... WHERE` references the **target** column; under a concurrent conflict Postgres re-evaluates the gate against the winner's committed row via EvalPlanQual → loser no-ops | safe (EPQ) |
| 8-K (`eight_k_*`), def14a (`def14a_*`), funds (`n_port_*`) | `ON CONFLICT` / savepoint-scoped `DELETE+INSERT` on immutable accession data | safe |
| `cik_refresh_mf_directory` (`cik_refresh` + `mf_directory_sync`) | blind `ON CONFLICT` upsert on stable SEC `class_id` key → last-write-wins on identical source data | safe |
| `daily_research_refresh` | disjoint write-set (no manifest / freshness / ownership) | safe |
| **`institutional_holdings`** (`sec_13f_quarterly_sweep` vs `cusip_universe_backfill` rewash) | today: the lane, in the operator-retry window only → **add per-accession `pg_advisory_xact_lock` (Task A)** | **guarded (new)** |

**The one suspect — `institutional_holdings` — needs a guard (Codex ckpt-1, MED).**
`sec_13f_quarterly_sweep` (`_upsert_holding`, `ON CONFLICT DO NOTHING`) vs `cusip_universe_backfill`
(rewash `DELETE FROM institutional_holdings WHERE accession_number=%s` then re-INSERT,
[rewash_filings.py:1031](../../../app/services/rewash_filings.py#L1031)) were the only pair where the lane
was the *sole* same-row guard. **Disjoint in normal operation** — sweep skips any accession already in
`_existing_accessions_for_filer`, which reads `institutional_holdings_ingest_log`
([institutional_holdings.py:566-573](../../../app/services/institutional_holdings.py#L566)), so it writes
only not-yet-ingested accessions; backfill rewashes accessions from
`unresolved_13f_cusips WHERE resolution_status IS NULL`
([cusip_resolver.py:1024](../../../app/services/cusip_resolver.py#L1024)), which exist only after ingest.
**But disjointness is NOT absolute:** the documented operator-retry path deletes the ingest-log row to force
re-ingest ([institutional_holdings.py:560-564](../../../app/services/institutional_holdings.py#L560)) while
leaving the `unresolved_13f_cusips` row — so sweep re-ingests accession A (log row gone) while backfill
rewashes A (unresolved row present) → concurrent `DELETE+INSERT` vs `ON CONFLICT DO NOTHING` on the same
accession. This window **already exists cross-lane today** (`cusip_extid_sweep`, lane `db_cusip`, runs the
same rewash concurrently with the sweep). → **Add a per-accession `pg_advisory_xact_lock`** (keyed on the
accession) held by BOTH the 13F ingest apply path and the rewash path, so same-accession ingest vs rewash
serialize regardless of lane/job. Specced as **Task A** (§5) — independently justified (it closes the
pre-existing cross-lane race) and a prerequisite for the lane swap (Task B).

**Residual = self-overlap only.** Form-3 `upsert_form_3_filing` uses `DELETE+INSERT` without
`ON CONFLICT` on `insider_initial_holdings`; the 13F rewash is `DELETE+INSERT`. Two instances of the
**same** job racing the same accession would corrupt. Today the lane prevents same-name overlap incidentally
(same source → second acquire fails). The per-job-name lock (§4) preserves exactly this.

**Accepted / pre-existing (NOT introduced by #1542).** `sec_rebuild`'s blind resets
(`ingest_status`→`pending`, freshness→`unknown`, no monotonic guard) can race a producer mid-write — but
`sec_rebuild` (lane `sec_rate`) already runs concurrently with `sec_manifest_worker` (lane `sec_manifest`)
and `sec_per_cik_poll` (lane `sec_per_cik`), so this race is **pre-existing cross-lane**; the lane never
guarded it. `sec_rebuild` is rare operator triage whose reset is intentional. Out of scope; noted for a
possible follow-up but not gating.

### 3b. Connection budget — N=4 fits

- Postgres dev `max_connections=30`, `superuser_reserved_connections=3` → **27 usable**
  ([docker-compose.yml:75](../../../docker-compose.yml#L75); [app/db/pg_settings.py:325](../../../app/db/pg_settings.py#L325)).
- Fixed baseline demand **17** + reserve **3** = 20; headroom ≈ 7
  ([app/db/pg_settings.py:246](../../../app/db/pg_settings.py#L246)).
- Per running SEC job **today** = 2 raw conns: the `JobLock` advisory conn
  ([app/jobs/locks.py:314](../../../app/jobs/locks.py#L314)) + the job-body conn. Neither is pooled.
- Dissolving makes the semaphore **in-process** → sec_rate jobs **drop the JobLock conn** → **1 conn/job**
  (body only). Worst case: `17 baseline + 3 reserve + 4 sec bodies + 2 unmodeled non-sec bodies = 26 ≤ 27`.
- `_dev_profile_connection_demand` ([app/db/pg_settings.py:246](../../../app/db/pg_settings.py#L246)) must be
  updated so the startup budget guard reflects the new shape (sec_rate no longer charges a JobLock conn;
  up to N=4 concurrent sec bodies) and still passes with margin ≥ 1.

### 3c. Single jobs process — in-process is sound

`_INVOKERS` (the job bodies) is consumed only by [app/jobs/runtime.py](../../../app/jobs/runtime.py),
[app/jobs/listener.py](../../../app/jobs/listener.py), [app/jobs/__main__.py](../../../app/jobs/__main__.py),
and [app/services/bootstrap_orchestrator.py:2817](../../../app/services/bootstrap_orchestrator.py#L2817) —
all inside the **single jobs process**. The API process imports only `VALID_JOB_NAMES`, never `_INVOKERS`;
it publishes manual requests to a queue the jobs-process listener drains. → an in-process semaphore bounds
**100%** of sec_rate execution.

### 3d. Lane membership — defined canonically (Codex ckpt-1, LOW)

The member set is **every job_name where `source_for(job_name) == "sec_rate"`**
([app/jobs/sources.py:511](../../../app/jobs/sources.py#L511)) — NOT a hand-union of `SCHEDULED_JOBS` +
`MANUAL_TRIGGER_JOB_SOURCES` + `_STAGE_LANE_OVERRIDES`, which misses the `_BOOTSTRAP_STAGE_SPECS` members
that resolve to `sec_rate` via the `StageSpec.lane` **fallback** (not an override):
`sec_first_install_drain`, `mf_directory_sync`, `sec_master_idx_gap_close`. Their write-sets were traced and
are safe (manifest/freshness via the monotonic-CASE path; `cik_refresh_mf_directory` blind upsert on the
stable `class_id` key). A test pins the resolved set `{j : source_for(j) == "sec_rate"}` (§7) so any future
sec_rate addition is caught and audited rather than silently inheriting the new concurrency.

## 4. Design

### 4.1 Two in-process primitives (jobs process, zero new connections)

In [app/jobs/locks.py](../../../app/jobs/locks.py), module-level:

```python
_SEC_LANE: Final[Lane] = "sec_rate"
_SEC_MAX_CONCURRENCY: Final[int] = 4          # the only new tunable
_SEC_SLOTS: Final[threading.BoundedSemaphore] = threading.BoundedSemaphore(_SEC_MAX_CONCURRENCY)
_SEC_NAME_LOCKS_GUARD: Final[threading.Lock] = threading.Lock()
_SEC_NAME_LOCKS: Final[dict[str, threading.Lock]] = {}   # job_name -> Lock, lazily created
```

- `_SEC_SLOTS` — the **count** gate (4 concurrent sec_rate jobs).
- `_SEC_NAME_LOCKS[job_name]` — per-job-name **identity** gate (no same-name overlap). The dict is mutated
  only under `_SEC_NAME_LOCKS_GUARD`; each `Lock` is created once and reused.

### 4.2 `JobLock` branch

`JobLock.__enter__` keeps the #1184 re-entrancy short-circuit first (unchanged — if `sec_rate ∈
_HELD_SOURCES`, bypass: take no slot, no name-lock). After that check, branch on source:

- **`self._source != "sec_rate"`** → existing pg-advisory path, **unchanged**.
- **`self._source == "sec_rate"`** → acquire, in order:
  1. per-name lock **non-blocking**: `lock.acquire(blocking=False)`. If `False` → raise
     `JobAlreadyRunning(job_name)` (same-name already running). No conn opened.
  2. count slot **non-blocking**: `_SEC_SLOTS.acquire(blocking=False)`. If `False` → release the name-lock,
     raise `JobAlreadyRunning(job_name)` (all 4 slots busy).
  3. else hold both for the job duration. Set `_HELD_SOURCES` as today (so a re-entrant inner
     `JobLock(sec_rate)` bypasses).

`__exit__` (when not re-entrant): release count slot, then per-name lock, then restore `_HELD_SOURCES`.
**No psycopg connection is opened or closed on the sec_rate path.**

### 4.3 Wait/skip behaviour — reuse #1538, add nothing

`_fire_scheduled_with_lane_retry` ([app/jobs/runtime.py:458](../../../app/jobs/runtime.py#L458)) already
wraps every scheduled lane fire under `JobLock(job_name)` and **retries `JobAlreadyRunning` from the
acquire** (backoff `(0.25, 0.5, 1.0)s`, bounded by the shared `_LANE_WAIT_SLOTS = BoundedSemaphore(3)` so
retry sleeps never drain the size-10 APScheduler pool; the `acquired` flag re-raises body-origin
`JobAlreadyRunning`). Because the new sec_rate branch raises `JobAlreadyRunning` when full, this helper
**transparently** retries the semaphore-full case with **no change**. De-starvation = N=4 (contention ~4×
rarer) × the existing retry absorbing the transient remainder.

**Behaviour table (a sec_rate scheduled fire):**

| Situation | Today (1-wide lane) | After (#1542) |
|---|---|---|
| no peer running | runs | runs |
| a *different* sec_rate job running | **skips** (the bug) | runs (free slot) |
| 1–3 different sec_rate jobs running | **skips** | runs (free slot) |
| all 4 slots busy | n/a | retry ~1.75s (≤3 shared waiters); still full → skip one cadence, recover next tick |
| same job_name already running | skips | skips (per-name lock) |
| >3 fires already retrying (any lane) | skips immediately | skips immediately (shared cap, == today) |

Worst case is exactly the old skip — **no new failure mode**, matching the #1538 invariant.

### 4.4 Manual-queue and bootstrap paths

Both acquire `JobLock(job_name)` **directly** (no `_fire_scheduled_with_lane_retry` wrapper): manual via the
listener's `_run_manual` on the separate 1-worker manual executor; bootstrap stages via the orchestrator's
executor. For a sec_rate job there, a full semaphore raises `JobAlreadyRunning` → the caller's existing
handling applies. N=4 makes a full semaphore far rarer than today's 1-wide lane.

- **Manual:** surfaces `JobAlreadyRunning` as the trigger result — **identical to today's lane-busy** (the
  operator re-triggers). No worse.
- **Bootstrap:** `_run_one_stage` ([bootstrap_orchestrator.py:1580](../../../app/services/bootstrap_orchestrator.py#L1580))
  marks the stage `error` / `success=False` on `JobAlreadyRunning` — there is **no** stage-level
  acquire-retry (Codex ckpt-1 MED corrected the earlier draft, which wrongly claimed one). This is **not a
  regression**: today's 1-wide lane *already* terminally fails a bootstrap sec_rate stage on any lane
  contention. After #1542 it is **strictly rarer** — during bootstrap the sec_rate scheduled producers are
  gated (`prerequisite=_bootstrap_complete`, `catch_up_on_boot=False`, e.g.
  [scheduler.py:1314](../../../app/workers/scheduler.py#L1314)) and the orchestrator dispatches sec_rate
  stages one-at-a-time (`_LANE_MAX_CONCURRENCY[sec_rate]=1`,
  [bootstrap_orchestrator.py:245](../../../app/services/bootstrap_orchestrator.py#L245)), so the 4-wide
  semaphore cannot saturate from bootstrap activity → a stage always gets a slot. The existing operator
  retry-failed path (`POST /system/bootstrap/retry-failed`) remains the recovery for the now-rare residual.
  No new mechanism added.

## 5. What changes / what does not

Two tasks, sequenced (Task A is a prerequisite — it removes the only write-safety blocker the lane swap
would otherwise expose):

**Task A — per-accession 13F ingest/rewash guard (§3a).**
[app/services/institutional_holdings.py](../../../app/services/institutional_holdings.py) +
[app/services/rewash_filings.py](../../../app/services/rewash_filings.py): hold
`pg_advisory_xact_lock(hashtextextended('ingest_13f_accession', 0) # <accession-hash>)` inside the
transaction that does the per-accession ingest apply (`_upsert_holding` path) AND the rewash `DELETE+INSERT`
(`_apply_13f_infotable`), so same-accession ingest vs rewash serialize regardless of lane/job. Xact-scoped →
auto-releases on commit. Independently justified (closes the pre-existing cross-lane `cusip_extid_sweep`
race), so it can land as its own commit/PR ahead of Task B.

**Task B — dissolve the lane.** [app/jobs/locks.py](../../../app/jobs/locks.py) (the sec_rate branch + the
two primitives, §4.1–4.2); [app/db/pg_settings.py](../../../app/db/pg_settings.py)
(`_dev_profile_connection_demand` accounting, §3b).

**Both:** tests (§7); this spec; memory + prevention-log if a lesson surfaces.

**Does NOT change:** the `Lane` literal keeps `"sec_rate"` (now the routing marker that tells `JobLock`
to use the semaphore); `JOB_NAME_TO_SOURCE` / `MANUAL_TRIGGER_JOB_SOURCES` / `_STAGE_LANE_OVERRIDES` keep
mapping the ~20 members to `"sec_rate"`; **no `bootstrap_stages.lane` CHECK change, no SQL migration**;
`_fire_scheduled_with_lane_retry` and the pg-advisory path for all other lanes are untouched.

## 6. Edge cases / invariants

- **Re-entrancy (#1184):** the `_HELD_SOURCES` short-circuit runs before the branch, so a re-entrant inner
  `JobLock(sec_rate)` takes neither a slot nor the name-lock and releases neither — preserved.
- **Slot/name-lock leak on body exception:** `__exit__` releases both in a `finally`-safe path regardless of
  body outcome (mirror the existing conn-release discipline). `BoundedSemaphore` raises `ValueError` on
  over-release — release exactly once, guarded by the same `_reentrant` flag.
- **Acquire order:** name-lock before slot; on slot-full, release the name-lock before raising (no orphaned
  name-lock). No blocking acquire anywhere → no deadlock.
- **Same-name retry waste (conscious tradeoff):** a same-name duplicate also raises `JobAlreadyRunning` and
  is retried ~1.75s by #1538 before skipping. Harmless (the running instance continues; the duplicate
  correctly skips) and rare (`max_instances=1` blocks scheduled self-overlap; only manual+scheduled overlap
  reaches it). Not worth distinguishing from semaphore-full.
- **Test isolation:** the two primitives are module-level singletons. Provide a test-only reset
  (`_reset_sec_concurrency_for_tests()`) mirroring `reset_job_name_to_source_cache`, so unit tests start
  from a known state.

## 7. Testing (lean — pure-logic + smoke)

Extract the sec_rate gate decision into a pure, table-testable helper (acquire/skip logic separated from the
psycopg path) and table-test:
- free slot → acquires (runs);
- same-name held → `JobAlreadyRunning`, **no** slot consumed;
- all N busy → `JobAlreadyRunning`, name-lock released;
- release returns the slot and frees the name for re-acquire;
- re-entrant (`sec_rate ∈ held`) → bypass, no slot/name change.

Plus:
- one assertion that `_dev_profile_connection_demand` with N=4 stays `≤ usable` with margin ≥ 1;
- a **membership-pin** test asserting `{j : source_for(j) == "sec_rate"}` equals the audited set (§3d), so a
  future sec_rate addition fails the test until its write-set is audited;
- the existing smoke test (`tests/smoke/test_app_boots.py`) for lifespan.

**Task A** (per-accession 13F lock) correctness is a DB-concurrency property — covered by the dev-verify step
(§8), not a new integration test, per the lean-tests rule. **No new DB integration test** otherwise.

## 8. Rollout + #1536

No schema change. Land → restart the jobs process onto the new SHA (operator-approved method: `kill -9`
the `python -m app.jobs` child + uv parent, confirm `JOBS_PROCESS_LOCK` free, `nohup uv run python -m
app.jobs` detached; confirm single scheduler = one child PID + one `ebull-jobs-singleton-fence` backend).

**Dev-verify (operator-visible):**
1. Hold k slots from a side thread; confirm a (k+1)-th up to 4 still acquires, and a 5th retries-then-skips.
2. Trigger two *different* sec_rate producers and confirm they run **concurrently** (pre-#1542 one would
   skip) — e.g. `sec_atom_fast_lane` + a manual `sec_13f_quarterly_sweep`; inspect `job_runs` overlap.
3. Confirm a same-name second dispatch skips with `JobAlreadyRunning`.
4. Confirm `/system/postgres-health` connection count stays under budget during a 4-concurrent window.

**#1536** ("watchdog can't heal lane-starvation; kicks forever, never takes") **closes / is moot:** there is
no lane to wedge on. The chronic 1-wide block is gone; the only residual stall is 4 genuine concurrent
long-holds, which is transient backpressure that clears as drains finish (and the long-hold producers
filing_docs / insider_backfill are already on their own lanes since #1540).

## 9. Out of scope (YAGNI)

- Body-conn pooling to push N>4 (Approach 2) — long-hold starvers already extracted; N=4 with margin is
  enough. N is one constant, trivially tuned later if steady-state shows saturation.
- #1484 cross-process HTTP token bucket — orthogonal; the per-process rate clock is unaffected by dissolving
  the jobs-side lane.
- A guard for `sec_rebuild`'s blind resets — pre-existing cross-lane, not introduced here.

## 10. Open verification items for implementation

1. Pin the exact `pg_advisory_xact_lock` key + the two transaction sites for Task A (ingest apply +
   rewash) so both use the identical key derivation; verify the lock is acquired before the first write in
   each transaction.
2. Confirm the exact `_dev_profile_connection_demand` edit keeps margin ≥ 1 at N=4 and the startup guard
   ([app/db/pg_settings.py:296](../../../app/db/pg_settings.py#L296)) still passes.
3. Confirm no non-jobs-process caller acquires `JobLock` for a sec_rate job (re-grep at implementation time).

## 11. Codex checkpoint-1 — findings folded in

- MED (bootstrap stage failure): §4.4 corrected — no stage-retry exists; bootstrap can't saturate the 4-wide
  semaphore (gated producers + `_LANE_MAX_CONCURRENCY[sec_rate]=1`); strictly rarer than today. No regression.
- MED (sweep/backfill not absolutely disjoint): §3a + Task A — per-accession advisory lock added.
- LOW (membership incomplete): §3d — defined via `source_for`; added `sec_first_install_drain`,
  `mf_directory_sync`, `sec_master_idx_gap_close`; pinned by a membership test (§7).
