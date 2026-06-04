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
