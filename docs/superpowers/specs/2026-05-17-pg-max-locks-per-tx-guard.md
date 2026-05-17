# Postgres `max_locks_per_transaction` floor + boot guard

> Status: **2026-05-17 (v4, CLEAN per Codex 1a + operator signoff).**
>
> Issue: **#1187**. Branch: `fix/1187-pg-max-locks-per-tx-guard`.
>
> Surfaced via PR #1186 (#1184) operator smoke. Bootstrap_run id=3 (2026-05-08
> → 2026-05-09, status='partial_error') has 4 stages erroring with
> `OutOfMemory: out of shared memory / HINT: increase
> max_locks_per_transaction`.

## 1. Problem

Bootstrap stages that write to partitioned ownership tables fail with:

```text
OutOfMemory: out of shared memory
HINT: You might need to increase "max_locks_per_transaction".
```

Affected stages (latest bootstrap_run id=3):

- `sec_def14a_bootstrap` → writes `ownership_def14a_observations`
- `sec_business_summary_bootstrap` → writes
  `instrument_sec_profile` + indirectly several ownership tables
- `sec_insider_transactions_backfill` → writes `insider_transactions` +
  `ownership_insiders_observations`
- `sec_13f_recent_sweep` → writes `ownership_institutions_observations`

Downstream blocked: `ownership_observations_backfill`. Bootstrap
stuck `status='partial_error'`. The `bootstrap_state` gate continues
to block scheduled fires (`app/jobs/runtime.py::_wrap_invoker` —
cannot be overridden; no operator at the keyboard) and catch-up.
Manual queue dispatch can override the universal `bootstrap_state`
gate via `{control:{override_bootstrap_gate:true}}` envelope
(`app/jobs/listener.py:201`). The override fires the listener gate
short-circuit. AFTER the universal-gate decision, the listener still
evaluates each job's `ScheduledJob.prerequisite` (listener.py:252):
for `orchestrator_full_sync` the prerequisite is `_bootstrap_complete`
at `app/workers/scheduler.py:428` which checks
`bootstrap_state.status='complete'` and returns
`(False, "first-install bootstrap not complete; visit /admin to run")`
on miss. That per-job prerequisite is the second gate the operator hit
on 2026-05-17 (PR #1186); the `override_bootstrap_gate` envelope
intentionally does NOT bypass per-job prerequisites — those are the
job's own pre-flight, not the cross-cutting universal gate.

Net: bootstrap completion is the real unblock. Blocks #1184's FULL-sync
end-to-end smoke + legacy cron retirement memo pre-condition + any
future feature gated on bootstrap completion.

## 2. Root cause (measured against PG17 dev DB, 2026-05-17)

eBull's ownership schema partitions 8 observation tables quarterly,
2010q1 → 2030q4 + default = 85 partitions per parent:

| Parent table | Partitions | Indexes per partition |
|---|---|---|
| `ownership_insiders_observations` | 85 | 4 |
| `ownership_institutions_observations` | 85 | 4 |
| `ownership_funds_observations` | 85 | 4 |
| `ownership_blockholders_observations` | 85 | 4 |
| `ownership_def14a_observations` | 85 | 4 |
| `ownership_treasury_observations` | 85 | 3 |
| `ownership_esop_observations` | 85 | 3 |
| `fund_metadata_observations` | 85 | 5 |

Postgres lock semantics — empirically probed via `pg_locks`
(`pid = pg_backend_pid() AND locktype = 'relation'`) on the live dev
DB at PG17:

| Statement | Distinct relation locks |
|---|---|
| `SELECT 1 FROM ownership_insiders_observations LIMIT 1` (unpruned, no WHERE) | **431** |
| `SELECT 1 FROM ownership_insiders_observations WHERE period_end = '2024-03-31' LIMIT 1` (pruned) | 11 |

The 431-lock count for an unpruned SELECT confirms: any
SELECT / UPDATE / DELETE / aggregate against a partitioned parent
WITHOUT a partition-key predicate (`period_end`) locks the parent +
every partition + every partition's indexes. 1 + 85 + 85×4 = 426 plus
TOAST + autovacuum bookkeeping ≈ 431.

INSERT path differs (per PG14+ partition routing): an INSERT routes to
the target partition + locks only that partition + its indexes (~5
locks per INSERT). Cumulative INSERTs across many partitions in one tx
still add up, but the dominant overrun source is the unpruned
SELECT / UPDATE / aggregate.

Failing stages contain inner loops that issue per-CIK or per-filing
queries against the partitioned parents. Many of these queries
legitimately lack a partition-key filter (existence checks, dedup
queries, mark-as-superseded UPDATEs that scan history). Each such
query reserves ~431 relation slots inside the tx; if 2+ accumulate
without intermediate COMMIT, the tx exhausts its lock allotment.

`max_locks_per_transaction` controls the SIZE of the shared lock
table = `max_locks_per_transaction × (max_connections +
max_prepared_transactions)`. PG default = 64. eBull's dev DB is at
the default. Current settings (probed 2026-05-17):

```text
max_locks_per_transaction       = 64
max_connections                 = 100
shared_buffers                  = 128MB
work_mem                        = 4MB
maintenance_work_mem            = 64MB
max_pred_locks_per_transaction  = 64
```

Total shared lock slots = 64 × 100 = 6400 cluster-wide. A single
backend can EXCEED its 64-slot nominal slice as long as the cluster
total remains within 6400; but with 4-12 concurrent backends each
hitting 431-lock unpruned-parent queries, the cumulative pressure
overruns the shared lock table → `OutOfMemory: out of shared memory`.

## 3. Goals

1. Eliminate the `out of shared memory` failure mode for bootstrap +
   ingest writes against partitioned ownership tables.
2. Provide a structural guard that hard-fails at boot when Postgres
   tuning is inadequate — both the API process (FastAPI lifespan) and
   the jobs process entrypoint. Operator sees the fatal error
   immediately, not N hours into a failed bootstrap run. Env-var
   escape hatch (`EBULL_ALLOW_LOW_PG_LOCKS=1`) for niche dev/CI.
3. Document the floor in the operator setup README + .env.example so
   new clones don't trip the same wall.
4. NO schema change. NO code change to ownership write paths. The
   schema design (quarterly partitions, 20-year window) is
   intentional; the Postgres setting is what's wrong.

## 4. Non-goals

- Reducing partition count (quarterly is the design; covered in §10).
- Auditing + rewriting every unpruned SELECT/UPDATE on partitioned
  parents to add `period_end` predicates. Tractable but multi-PR and
  brittle — covered in §10 as the post-floor-bump structural fix.
- Tuning `max_connections` / `shared_buffers` / `work_mem` — orthogonal.
- Auto-applying `ALTER SYSTEM SET` from the boot guard — too magical;
  the guard hard-fails + the operator runs the one-line SQL.

## 5. Design

### 5.1 Floor

Empirical: 1 unpruned SELECT on `ownership_insiders_observations`
parent reserves 431 distinct relation locks (measured §2). Codex 1a
correctly flagged that PG can exceed the nominal per-tx slice when
the shared lock table has headroom, but the operational hazard is
cluster-wide exhaustion under concurrent backends, not the per-tx
arithmetic.

Pick `max_locks_per_transaction ≥ 1024` as the floor.

Justification:

+ 1 unpruned-parent SELECT = ~431 locks (measured).
+ 2 unpruned scans against DISTINCT parents in one tx = ~860 locks
  (repeating the same parent reuses already-held locks; the worst
  case is distinct parents).
+ The largest measured single-table footprint is 425 (fund_metadata,
  85 × 5). A bootstrap stage that scans 2 such tables in one tx
  approaches 900 locks.
+ Headroom for future growth: post-2030q4 partitions, new partitioned
  tables (memory `[[us-source-coverage]]` notes ~85 partitions per
  ownership family is the steady-state design), additional indexes
  per partition.
+ Cluster-wide budget at 1024 × 100 = 102 400 lock slots remains
  well within Postgres's documented sane range; eBull's dev DB has
  plenty of headroom (`shared_buffers=128MB` already-modest is a
  bigger concern at scale).

Floor of **256** (v1 of this spec) would have insufficient headroom
for a single unpruned-parent scan + any concurrent backend pressure.
Floor of **512** would scrape past a single scan but fail under load.
Floor of **1024** is the operationally-safe choice given measured
worst-case + 2× concurrent-backend headroom.

### 5.2 Boot guard

A small helper in `app/db/pg_settings.py` (new file):

```python
PG_LOCKS_FLOOR: Final[int] = 1024
PG_LOCKS_OVERRIDE_ENV: Final[str] = "EBULL_ALLOW_LOW_PG_LOCKS"


def check_max_locks_per_transaction(
    conn: psycopg.Connection[Any],
    *,
    floor: int = PG_LOCKS_FLOOR,
) -> tuple[bool, int]:
    """Probe ``max_locks_per_transaction``; return ``(passes, value)``.

    Used at boot by both the FastAPI lifespan (``app/main.py``) and
    the jobs process entrypoint (``app/jobs/__main__.py``) to refuse
    startup before any partitioned-table write can OOM under load.
    The floor is calibrated for eBull's quarterly partition layout —
    see spec ``docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md``.

    Fail-open on SHOW exception (returns ``(True, 0)``): the probe is
    informational; a transient SHOW failure must not block startup.
    """
    try:
        row = conn.execute("SHOW max_locks_per_transaction").fetchone()
    except Exception:
        logger.warning("pg_settings: SHOW max_locks_per_transaction failed; skipping guard", exc_info=True)
        return True, 0
    if row is None:
        return True, 0
    value = int(row[0])
    return value >= floor, value


class PgLocksFloorBreached(RuntimeError):
    """Raised at boot when max_locks_per_transaction < floor.

    Caller (lifespan / jobs entrypoint) maps this to a FATAL exit so
    the operator must tune Postgres before retrying.
    """

    def __init__(self, value: int, floor: int) -> None:
        super().__init__(
            f"max_locks_per_transaction={value} < floor={floor} — "
            f"eBull's partitioned ownership tables routinely reserve "
            f"~431 locks per unpruned-parent statement. Run "
            f"`ALTER SYSTEM SET max_locks_per_transaction = {floor};` "
            f"then restart Postgres. Set {PG_LOCKS_OVERRIDE_ENV}=1 to "
            f"bypass (development only, expect OOM under load)."
        )
        self.value = value
        self.floor = floor


def enforce_max_locks_floor(conn: psycopg.Connection[Any]) -> None:
    """Hard-fail wrapper. Honours ``EBULL_ALLOW_LOW_PG_LOCKS=1`` escape
    hatch for niche dev / CI environments where the cluster setting is
    out of the operator's control. The escape hatch logs a loud warning
    every boot so it stays visible.
    """
    if os.environ.get(PG_LOCKS_OVERRIDE_ENV) == "1":
        passes, value = check_max_locks_per_transaction(conn)
        if not passes:
            logger.warning(
                "pg_settings: max_locks_per_transaction=%d below floor=%d; "
                "running anyway because %s=1 is set",
                value, PG_LOCKS_FLOOR, PG_LOCKS_OVERRIDE_ENV,
            )
        return
    passes, value = check_max_locks_per_transaction(conn)
    if not passes:
        raise PgLocksFloorBreached(value=value, floor=PG_LOCKS_FLOOR)
```

Call sites:

+ `app/jobs/__main__.py::main` — after singleton fence + before any
  scheduler / queue dispatch wiring. HARD-FAIL on breach (raises
  `PgLocksFloorBreached` → FATAL exit). Bootstrap-capable process
  must refuse to start with insufficient lock budget; transient warn
  would just re-fail the same N-hour bootstrap.
+ `app/main.py::lifespan` — after migrations + before pool open.
  HARD-FAIL on breach (same exception → uvicorn exits with non-zero).
  The API process serves request paths that touch the partitioned
  tables too; a lock-starved API would partially-serve requests
  before silently failing.
+ Both call sites honour `EBULL_ALLOW_LOW_PG_LOCKS=1` as an explicit
  operator override (logs warning each boot so the escape hatch
  stays visible).

Wording (operator-facing):

```text
FATAL: PostgreSQL max_locks_per_transaction=64 < floor=1024.
   eBull's 8 quarterly-partitioned ownership tables routinely reserve
   ~431 relation locks per unpruned-parent statement (measured
   2026-05-17, PG17). Bootstrap and heavy ownership ingest will OOM
   the shared lock table under concurrent backends with the default 64.

   Tune via:

     ALTER SYSTEM SET max_locks_per_transaction = 1024;
     -- then restart Postgres for the change to take effect.

   Override for development / CI only (expect OOM under load):
     export EBULL_ALLOW_LOW_PG_LOCKS=1

   See docs/superpowers/specs/2026-05-17-pg-max-locks-per-tx-guard.md.
```

### 5.3 Documentation

Touch the following:

- `README.md` — add a "Postgres tuning" section under "Setup" naming
  the floor + the ALTER SYSTEM command.
- `.env.example` — add a comment near the `EBULL_DATABASE_URL` block
  noting the requirement.
- Spec doc itself — this file is the canonical reference; the boot
  guard message + README link here.

No `docker-compose.yml` / `Dockerfile` change — eBull's dev Postgres
is operator-managed per env.

### 5.4 Operator-side remediation runbook

Spec includes a short runbook so the operator can apply the fix to
the local DB while the PR is in flight:

```bash
psql "$EBULL_DATABASE_URL" -c "ALTER SYSTEM SET max_locks_per_transaction = 1024;"
# Restart Postgres (Mac Homebrew):
brew services restart postgresql@<version>
# Verify:
psql "$EBULL_DATABASE_URL" -c "SHOW max_locks_per_transaction;"
# Reset failed bootstrap stages for retry (via admin UI OR direct SQL):
#   admin UI: /admin → Bootstrap → "Retry failed"
#   SQL:      UPDATE bootstrap_state SET status = 'partial_error' WHERE id = 1;
#             (then retry via API/UI)
```

## 6. Test plan

1. `tests/test_pg_settings_guard.py` (NEW):
   - `test_check_returns_passes_when_above_floor` — patches `SHOW`
     result via a fake conn → `passes=True, value=1024`.
   - `test_check_returns_fail_when_below_floor` — fake conn returning
     `64` → `passes=False, value=64`.
   - `test_enforce_raises_when_below_floor_no_override` — fake conn
     `64` → `enforce_max_locks_floor` raises `PgLocksFloorBreached`.
   - `test_enforce_skips_when_env_override_set` — same setup with
     `monkeypatch.setenv("EBULL_ALLOW_LOW_PG_LOCKS", "1")` →
     no raise, warning logged.
   - `test_enforce_fail_open_on_show_error` — fake conn whose
     `execute` raises → `check_*` returns `(True, 0)`,
     `enforce_*` returns without raising.
2. `tests/test_pg_settings_lock_count.py` (NEW integration probe —
   decisive Codex 1a WARNING fix):
   - `test_unpruned_parent_select_locks_exceed_default_floor` —
     against `ebull_test_conn`, BEGIN tx, run `SELECT 1 FROM
     ownership_insiders_observations LIMIT 1`, count distinct
     relation locks in `pg_locks` for `pg_backend_pid()`, assert
     count > 64. Pins the measured 431-lock empirical claim.
   - `test_pruned_parent_select_locks_within_default_floor` — same
     setup with `WHERE period_end = '2024-03-31'`, assert count
     < 64. Pins partition-pruning works.
   - Tests gated `if test_db_available()` so a missing dev DB skips
     instead of failing.
3. `tests/smoke/test_app_boots.py` — already exercises lifespan.
   NEW: monkeypatch `check_max_locks_per_transaction` to return
   `(False, 64)` + assert TestClient enter raises `PgLocksFloorBreached`
   (or its uvicorn-equivalent). Also assert that with
   `EBULL_ALLOW_LOW_PG_LOCKS=1` set, TestClient enter succeeds +
   warning logs captured by caplog.
4. Jobs entrypoint smoke (`tests/test_jobs_entrypoint_smoke.py` if it
   exists, OR add a focused unit test). Same hard-fail / override
   matrix as #3.

## 7. Risks + mitigations

| Risk | Mitigation |
|------|-----------|
| Operator ignores guard + bootstrap OOMs again | Hard-fail (§5.2) — startup refuses below floor. No silent recurrence. Override env var stays visible (loud warning every boot) for niche dev/CI |
| Boot guard breaks startup on transient `SHOW` failure | `check_*` catches `Exception`, logs warning, returns `(True, 0)` — fail-open. `enforce_*` then sees `passes=True` and proceeds. Probe is informational, not safety-critical |
| Cluster-wide setting affects other DBs on the same cluster | Acceptable — eBull's dev cluster is dedicated. Production deployments use eBull-only clusters per ADR-0001 |
| Floor of 1024 still insufficient for some future write path | Guard surfaces the value at every boot; floor is a constant — future PRs can raise it as needed. Single-source change vs hunting OOM in production |
| ALTER SYSTEM + restart on shared dev DB is disruptive | Operator-controlled — they pick the restart window. Single-operator dev env per `feedback_keep_stack_running.md` |
| Existing dev DB stuck on `partial_error` after operator restarts Postgres | §5.4 runbook covers the retry path — reset failed stages via admin UI / SQL |

## 8. Settled decisions impact

- **Process topology (#719)** — preserved. Boot guard runs in both
  processes; no IPC change.
- **Source-lock decision (#1064 PR1a)** — preserved. Lock-table
  capacity is orthogonal to source-bucket semantics.
- **Operator auth / broker secrets (ADR-0001 / ADR-0003)** — preserved.
  Guard runs after migrations + before any secrets work. Hard-fail
  blocks master-key bootstrap only when the floor breach would have
  caused downstream OOM anyway; the override env var unblocks operator-
  controlled CI environments where the cluster setting is fixed.

## 9. Rollout

1. Spec → Codex 1a → operator signoff.
2. Plan → Codex 1b → operator signoff.
3. Implement helper + 2 call sites + tests + docs.
4. Local gates (ruff / format / pyright / pytest impacted).
5. Codex 2 pre-push review.
6. Push branch → PR → poll review + CI → merge.
7. **Operator-side immediate fix** (parallel to PR review): apply
   ALTER SYSTEM + restart Postgres + reset bootstrap_state +
   retry failed stages via admin UI. Verify bootstrap reaches
   `status='complete'`. Verify #1184 FULL-sync runs end-to-end and
   `fx_rates_refresh` / `seed_cost_models` / `weekly_report` /
   `monthly_report` all land `status='success'`.
8. Memory update: this fix closes the [[us-source-coverage]] +
   [[legacy-cron-retirement]] bootstrap pre-condition note.

## 10. Future work (out of scope)

- **Audit unpruned SELECT / UPDATE / aggregate queries against
  partitioned parents.** Per §2 the dominant lock-table consumer is
  per-row existence checks + dedup queries + mark-as-superseded
  UPDATEs that scan history without a `period_end` predicate. Each
  such query reserves ~431 locks. A grep-driven sweep of ownership
  service code to add partition-key WHERE clauses (or per-partition
  iteration) would drop the per-query cost from ~431 to ~11,
  removing the floor's headroom dependence. File separately if PG
  tuning proves insufficient at scale.
- **Yearly partitions instead of quarterly.** 4× fewer partitions →
  4× fewer locks per unpruned scan. Requires re-partition migration
  with downtime. Defer until the lock budget is the bottleneck even
  after the floor bump.
- **Materialized partition-aware views for hot-path lookups.** The
  most frequent "did we already record this observation?" check could
  back onto a non-partitioned shadow index keyed on the natural
  unique tuple. Eliminates the unpruned-parent scan entirely for
  that path.
