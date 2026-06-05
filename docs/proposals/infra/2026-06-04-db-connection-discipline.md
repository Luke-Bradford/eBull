# Dev Postgres connection discipline — bound demand, don't raise the ceiling

**Status:** proposed (2026-06-04). Codex checkpoint-1 reviewed (7 plan-shape issues folded). Umbrella: #1472.
**Goal:** stop the dev DB hitting `FATAL: sorry, too many clients already` under cadence-boundary bursts, by **tightening** connection demand to fit `max_connections=30` — NOT by raising the ceiling.

## RCA (evidence-grounded, Codex-verified)

Symptom (2026-06-03): Admin showed `13 stale / SCHEDULE MISSED` on a cohort of scheduled jobs + one job stuck `running` 20h. Pipeline data was actually still advancing (orchestrator `sync_runs` every 5m `complete`, `sec_manifest_worker` draining).

Chain:
- dev PG container **restarted 2026-06-03 17:27** (`RestartCount=0`, `OOMKilled=false` — a deliberate recreate, not a crash) **under a jobs process up since 06-02**.
- PG log: **91× `FATAL: sorry, too many clients already`**, clustered 06-03 19:00–23:59 at cadence boundaries (`:00`/`:05`), cleared by 06-04 01:30.
- jobs log: 300k+ `connection refused ::1:5432` + `listener: loop crashed; supervisor will restart` (~5s loop) → heartbeat `listener_restarts=1439`. The standalone scheduled-cron cohort coalesced/froze; the orchestrator (`sync_runs`) kept running.
- **Not a leak:** `pg_stat_activity` stable 22–24, `idle_in_transaction=0`; both LISTEN loops `finally: conn.close()` on reconnect (verified `listener.py:541`, `credential_health_listener.py:102`). It is **burst pressure**.

Numbers:
- `max_connections=30`, `superuser_reserved_connections=3` → **27 usable**. Idle baseline **~22/30**.
- Baseline decomposes: **10× idle `COMMIT`** = API `db_pool` (`main.py:234`, `max_size=10`) holding for one user; **5× LISTEN** (`ebull_job_request ×2` + `ebull_credential_health ×3`, expected ~2–3); 1 singleton-fence; 1 `SELECT 1` health-check; 1 JobLock advisory hb; 1 stray idle-in-tx.
- Pools: API `db_pool` min1/max10 + `audit_pool` min1/max2 (`main.py:234,246`); `jobs_pool` min1/max4 (`__main__.py:824`). `open_pool` (`db/pool.py`) sets `max_idle=600`, `max_lifetime=1800`, `timeout=15`, `check=ConnectionPool.check_connection`.
- **134 raw `psycopg.connect(settings.database_url)` sites bypass the pools** (scheduler 79, listener 19, `__main__` 16, `sec_bulk_orchestrator_jobs` 14, `sync_orchestrator/executor` 13, …). Each scheduled job + each orchestrator **layer / gate-check** opens its own short-lived raw connection at fire time → cadence-boundary thundering herd against ~8 slots of headroom.

**Root cause:** a single-user dev DB over-provisions (10 idle API conns) and the background services open an **unbounded raw-connection herd** at cadence boundaries. 30 is not too low for the real workload; the demand is undisciplined.

## Decision: do NOT raise `max_connections`

Rejected (option A). On an OOM/WAL-fragile box each backend ≈ 5–10 MB; 30→150 adds ~0.6–1.2 GB of backend memory and more concurrent txns → more WAL/checkpoint pressure. It converts a **clean, recoverable** `FATAL: too many clients` into the **catastrophic** mode actually feared (OOM-kill a backend → `restart_after_crash` → postmaster restart-all → WAL recovery). It masks the root cause. A temporary `30→40` is a diagnostic escape hatch only, not the fix.

## Workstreams (each its own PR, ordered by ROI-vs-risk)

### PR1 — fail-fast "demand fits supply" boot guard  *(do first; pure tightening, zero runtime risk)*
Mirror the existing `check_max_locks_per_transaction` (#1187) in `app/db/pg_settings.py`.
- New `check_connection_budget(conn, *, process)`: each boot path asserts a **per-process "enabled local processes" budget**, NOT a blind sum (Codex ckpt-1 #1). The budget = *this* process's pool max(es) + the fixed long-lived conns it owns + the fixed demand of the peers that co-run in the **dev single-box profile** (API + jobs + 1 DB always co-deployed — state this assumption explicitly; prod sizing is out of scope). The model must not fail the API for jobs capacity that is not actually running, so the peer term is the documented dev co-deployment, conservative (over-estimates → only fails when genuinely impossible).
- Pool max-sizes from a **single source of truth** — extract API/jobs pool sizes to named constants so the guard and `open_pool` callers agree (no magic numbers).
- Wire into both boot paths: `app/main.py` (API) + `app/jobs/__main__.py` (jobs).
- **Operator message (Codex ckpt-1 #2):** primary remediation = **shrink configured demand / stop duplicate processes**. Raising `max_connections` is named **diagnostic-only**, explicitly NOT a normal remediation (don't reintroduce option A through the error text).
- Tests: guard passes at the real config; raises when a synthetic over-budget config is injected.
- **Acceptance:** boot refuses to start when the configured demand cannot fit the dev profile — a mathematically-impossible config can never again silently degrade at runtime.

### PR2 — connection-lifetime audit + fixes + right-size the API pool  *(one PR: audit → fix → shrink, in order)*
Restructured so it is independently safe (Codex ckpt-1 #3, #4): the shrink does NOT land until the audit is clean.
- **Step 1 — contract-based audit (blocking):** the contract is *no checked-out pooled connection may span slow external I/O, `sleep`/long-poll, or any unbounded work — anywhere in the request call graph*, not just the route body (services/helpers reached by the route count). `psycopg_pool` `timeout=15` + default `max_waiting=0` means a shrunk pool **queues** waiters (block-then-`PoolTimeout`), it does not drop them — so a conn held across eToro/SEC/GLEIF I/O becomes a latency stall under a smaller pool. Produce a **recorded manual trace** of the high-risk routes (anything that calls an external provider) + a lint/test that flags a pooled conn held across an outbound HTTP call.
- **Step 2 — fix any violations** found (release the conn around the external call) *before* shrinking. If none, record "audit clean."
- **Step 3 — shrink:** `db_pool` `max_size 10→4`, `audit_pool` `max_size 2→1` (`app/main.py:234,246`), via the PR1 named constants.
- **Acceptance:** audit recorded + clean; steady baseline drops ~22→~12; no endpoint stalls on `PoolTimeout` under single-user load.

### PR3 — listener cardinality assertion  *(small; reframed from "leak"; INDEPENDENT — does not block PR4)*
Not a leak (verified `finally: conn.close()` on reconnect). But `ebull_job_request ×2 + ebull_credential_health ×3` > expected = duplicate listener instances / reload duplication. **Runs in parallel with PR4a; if the duplicate-instance source is non-trivial, defer it — it must NOT block the root-cause work** (Codex ckpt-1 #5).
- Add boot-id / process-label (`application_name`) to listener connections so `pg_stat_activity` shows ownership.
- Assert expected listener cardinality at boot (1 `job_request` in the jobs process; `credential_health` = exactly the processes that subscribe).
- Investigate + fix the duplicate-instance source.
- **Acceptance:** listener connection count = the expected fixed number; surfaced in `/system/postgres-health` or the PR1 budget.

### PR4 — background connection discipline  *(staged; the root-cause refactor; biggest)*
The 134 raw-connect sites. Stage strictly; **PR4b must land before the PR4c sweep** (the bounded pool is the new contract — sweeping before it exists risks inconsistent partial conversions, Codex ckpt-1 #7).
- **PR4a — orchestrator gate-check connection reuse:** `_run_layers_loop` (`executor.py:380`) reuses **one run-scoped `autocommit=True` connection** for the per-layer read gate-checks (cancel / credential / init / dependency lookup). **Must be `autocommit=True`** (or explicit short-tx boundaries) so the shared connection never accumulates one long implicit transaction across the walk (Codex ckpt-1 #6). Writes stay in their own short txns. Do NOT touch the sites that deliberately use fresh autocommit conns for snapshot-freshness / rollback-isolation (`executor.py:674` comment) unless a consistent snapshot is explicitly not required there.
- **PR4b — bounded background pool:** a small dedicated pool (max_size ≈ 2–3) for high-frequency short-lived background writes (progress/result rows, audit-style), capping the jobs process's concurrent footprint regardless of cadence concurrency. Add `PoolTimeout` / reconnect **metrics** + a **hard-recreate path** after repeated pool failures (self-heal on PG restart — `check_connection` only helps on checkout). **Lands before PR4c.**
- **PR4c — sweep remaining eligible raw connects** onto the PR4b pool where the per-job transaction shape allows.
- **Acceptance:** at a `:00` cadence boundary the jobs process holds ≤ (jobs_pool + background_pool) connections, not a herd; a test fires N jobs concurrently and asserts the connection ceiling holds.

## Sequencing + done-criteria
Codex-ckpt-1 sequence:
`PR1 (guard + constants)` → `PR2 (audit + lifetime fixes + shrink)` → `PR4a` **and** `PR3` independently/in parallel → `PR4b (bounded background pool)` → `PR4c (sweep remaining eligible raw connects)`.
- PR3 is non-dependency-bearing — parallel to PR4a, deferrable if messy.
- PR4c is gated on PR4b.
- Operator discipline (no code): when the dev PG container is recreated, **restart the jobs process** too — the 06-02 jobs vs 06-03-17:27 PG mismatch is what cascaded. The eventual "PG restart → jobs self-heal pool" coupling lives in PR4b's hard-recreate path.
- Each PR through the normal gate (lint/typecheck/pytest, Codex ckpt-2, review-bot APPROVE).

## Out of scope
- Raising `max_connections` (rejected above).
- Production topology (this is the dev single-user box; prod sizing is separate).

## Amendment 2026-06-04b — validated live-RCA + 4 gaps (discovery-layer freeze, #1474)

Triage of #1474 (operator-console "stale") proved the herd has a **second, more severe consequence than the original RCA captured.** The RCA above (line 8) says *"Pipeline data was actually still advancing"* and treats the cohort as cosmetic. That is **wrong** — it credited health to the 2 jobs that survived. At the **2026-06-04 15:26 jobs restart** the herd **wedged the SEC discovery layer**: `sec_atom_fast_lane` (5-min), `sec_per_cik_poll`, `sec_daily_index_reconcile`, `sec_filing_documents_ingest`, `ownership_observations_sync`, `cusip_extid_sweep`, `daily_cik_refresh`, `monitor_positions` — **zero `job_runs` rows of any status since 15:26**, while gate-exempt `sec_manifest_worker` + `daily_portfolio_sync` kept firing. Manifest is draining residual backlog, not discovering new filings.

### Mechanism (proven both ways: live thread dump ×2 + code logic)
1. `wrapped()` opens **two raw `psycopg.connect()` per scheduled fire** — bootstrap-gate (`app/jobs/runtime.py:1511`) + prereq (`:1541`) — for every NON-exempt job, BEFORE `record_job_start`.
2. **No `connect_timeout` anywhere** (0 hits in `app/`; DSN `app/config.py:16`). A connect can block unbounded. (Fix verified: `connect_timeout` + `options=-c statement_timeout` in the URI are honored by psycopg3 — black-hole connect fails in ~3s instead of hanging.)
3. Under the restart herd those connects **wedge mid-SCRAM-auth** (thread dump: `PQconnectPoll → pg_fe_sendauth → scram_exchange → pg_hmac_init → HMAC_Init_ex`; OpenSSL-3 `EVP` provider-fetch contention). TCP-ESTABLISHED but pre-auth → shows as **NULL-state** in `pg_stat_activity`, not a stuck query — which is why it read as "no leak / healthy."
4. `wrapped()` never returns → APScheduler `_instances[job]` (`apscheduler/executors/base.py:71` submit-increment, `:88`/`:98` decrement-on-completion) **never decrements** → `max_instances=1` skips every later fire **silently** (no row, no thread). Permanent until process restart.
5. Exempt + no-prereq jobs skip the pre-tracked connects → never wedge → keep firing. **That asymmetry IS the "13 stale" cohort.**

### Gaps in this plan vs the validated RCA — fold in
- **GAP-A — `connect_timeout` via `PGCONNECT_TIMEOUT` env. NEW PR0, do FIRST (before PR1). — MERGED `a76593d` (#1475), 2026-06-04.** Set `PGCONNECT_TIMEOUT` (`DB_CONNECT_TIMEOUT_S=10`) at **`app/config.py` import time** (`os.environ.setdefault`, so an operator override wins). Chosen over wiring `app/main.py` + `app/jobs/__main__.py` separately because `app.config` is imported before any connect in EVERY entrypoint — API, jobs, one-off scripts, tests — so a single line covers them all with no ordering risk. **Verified empirically**: libpq honours it for EVERY connection — raw + pooled — with **zero DSN/call-site change and no URL-parser risk** (a black-hole connect fails in ~timeout s instead of hanging; real connect + lifespan smoke unaffected). This alone removes the proven wedge: a slow connect now raises → the `wrapped()` gate/prereq `except` fails-open → the job RUNS instead of pinning `max_instances` forever. Highest leverage / lowest risk in the plan. (Chosen over mutating `settings.database_url`, which would risk consumers that parse the URL — migrations, psql shell-outs, test-DB-name derivation.)
  - **`statement_timeout` is deliberately NOT global here.** A blanket `statement_timeout` (via process-wide `PGOPTIONS`) would kill legitimate long ETL — bulk SEC downloads (`sec_bulk_download` runs minutes), large MERGEs, multi-minute bootstrap stages. Apply it **scoped** to the short-lived gate/prereq/progress connections only (folded into **GAP-B**), via per-connection `SET LOCAL statement_timeout` / a scoped `options` on that pool.
- **GAP-B — scheduled-fire prelude connects.** PR4a covers the **orchestrator** gate-checks (`executor.py:380`) but NOT the **runtime.py scheduled-fire** gate+prereq connects — a distinct per-fire-per-job herd source AND the freeze surface. Fold gate+prereq onto ONE pooled, `autocommit=True`, timeout-bounded connection (reuse for both checks) before `record_job_start`. Add as **PR4a-bis**.
- **GAP-C — hang-proof scheduler + surface the wedge.** APScheduler cannot kill threads → the only release for a wedged instance is bounded blocking calls (GAP-A). Visibility (Codex ckpt-1 correction): (i) connect timeout [GAP-A] + scoped `statement_timeout` [GAP-B]; (ii) **add a scheduler listener on `EVENT_JOB_MAX_INSTANCES` (APScheduler event `65536`; verified — no listener exists today)** that records each suppressed fire as a `job_runs` `'skipped'` row (reason `max_instances_wedged`) + metric — converts the silent permanent freeze into an immediate operator signal. **NOTE (Codex):** the `job_runs` orphaned-`running` reaper is a *separate* failure mode (a row that WAS written then stranded, e.g. `sec_daily_index_reconcile` run_id 67) — it can NOT catch the pre-`record_job_start` wedge (no row exists) and can NOT decrement APScheduler `_instances`; it is alert/mark-only. "Zero fires for ≥K cycles" belongs in GAP-D, not the reaper.
- **GAP-D — liveness watchdog (silent-failure alarm).** The freeze was invisible ~21h and read as healthy. Operationalise the ops-monitor mandate ("silent failure = failure"): a watchdog comparing **expected fires (cadence) vs actual `job_runs`** per job; alert on a gap ≥K cycles. This is the guard that CATCHES a recurrence regardless of mechanism. New work item under the #1472 umbrella.

### Revised sequence
`PR0 (PGCONNECT_TIMEOUT env)` → `PR1 (boot guard)` → `PR4a-bis (scheduled-fire prelude: collapse gate+prereq to ONE connection + scoped statement_timeout — the validated freeze surface, moved up per Codex)` → `PR2 (audit + shrink)` → `PR4a ∥ PR3` → `PR4b (bounded bg pool + hard-recreate self-heal)` → `PR4c (sweep)` → `PR-visibility (EVENT_JOB_MAX_INSTANCES listener + liveness watchdog + job_runs orphan reaper)`.
(With PR0 landed, the wedge is already *prevented*; PR4a-bis additionally removes those per-fire connects from the cadence-boundary herd that makes SCRAM auth slow.)

### Immediate operational relief (separate from code)
Discovery layer is wedged NOW. A clean **jobs-process restart** un-wedges it (connections healthy 24/30, herd subsided; thread-dump evidence captured in `/tmp/python3_*.sample.txt`). It WILL recur on the next herd until PR0 lands. Restart = operator action.
