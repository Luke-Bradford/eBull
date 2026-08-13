# Postgres runtime tuning + `runtime_config` boot guard

> Status: **2026-05-18 (v1).**
>
> Issue: **#1208 Subs 1 + 6.** Branch: `feature/1208-phase1-postgres-tuning-runtime-config-boot-guard`.
>
> Phase 1 of `docs/superpowers/plans/2026-05-18-backend-stability.md`.

## 1. Problem

Two coupled defects observed on 2026-05-18:

1. **Postgres on Docker Desktop macOS** runs with PG17 defaults that are wrong for our partitioned-ownership workload — `max_wal_size=1024 MB`, `shared_buffers=128 MB`, `wal_compression=off`, `work_mem=4 MB`, `maintenance_work_mem=64 MB`. The container has crashed twice (09:05 + 12:54 UTC) with `PANIC: could not create file "pg_wal/xlogtemp.NNN": No space left on device` despite host disk having free space. Recovery takes 45+ min because Docker walks every relfile on fsync (relfile count on `ebull` DB = 4,723).
2. **`runtime_config` singleton vanished** from dev DB. `GET /config` returned `503` twice. Migration `sql/015_runtime_config.sql` seeds the row via `INSERT ... ON CONFLICT (id) DO NOTHING`; once that one-shot write is lost (operator `DELETE`, snapshot restore from pre-seed era, future bootstrap reset script) nothing re-creates it.

The two defects are structurally identical: a one-time configuration write whose absence at runtime is a fail-closed corner. Postgres tuning is a `ALTER SYSTEM SET` one-shot per knob; `runtime_config` is a one-shot row. Both need a presence guard so the live system tolerates losing the write.

## 2. Spike receipts (2026-05-18 dev DB)

```text
checkpoint_completion_target | 0.9     | default              (already at target)
effective_cache_size         | 524288  | default = 4 GB       (already at target)
maintenance_work_mem         | 65536   | default = 64 MB      → 512 MB
max_locks_per_transaction    | 1024    | configuration file   (PR #1188, leave alone)
max_wal_size                 | 1024    | configuration file   → 4096
min_wal_size                 | 80      | configuration file   → 512
shared_buffers               | 16384   | configuration file   → 262144 (restart req)
wal_compression              | off     | default              → on
work_mem                     | 4096    | default              → 32768

pg_database_size('ebull') = 46 GB
SELECT count(*) FROM runtime_config = 1   (operator re-seeded earlier in session)
```

Closure framing: **TUNING PRIMITIVE + RESILIENCE PRIMITIVE.** Same fail-closed posture as `kill_switch` (sql/010) and the existing `max_locks_per_transaction` floor guard (#1187 / PR #1188).

## 3. Scope

| Task | Deliverable | Closure framing |
|---|---|---|
| T0 | `app/db/migrations.py` — `-- runner: autocommit` file-header directive | RUNNER PRIMITIVE |
| T1 | `sql/155_postgres_runtime_tuning.sql` — `-- runner: autocommit` header + `ALTER SYSTEM SET` + `pg_reload_conf()` | TUNING PRIMITIVE |
| T2 | `docker-compose.yml` — `mem_limit: 4g` + `shm_size: 1g` on the `postgres` service | TUNING PRIMITIVE |
| T3 | `app/services/runtime_config.py::ensure_runtime_config_singleton(conn)` | RESILIENCE PRIMITIVE |
| T4 | `app/main.py` lifespan + `app/jobs/__main__.py` boot wiring | RESILIENCE PRIMITIVE |
| T5 | `tests/test_runtime_config_boot_guard.py` | TEST PRIMITIVE |
| T6 | `tests/test_dev_db_no_test_writes.py` | TEST PRIMITIVE |
| T7 | `docs/review-prevention-log.md` — Postgres-on-Docker section + extend the existing "singleton-row migrations" entry with the merge SHA after PR lands | DOCS PRIMITIVE |
| T8 | `.claude/skills/engineering/test-quality.md` §"Dev-DB isolation invariant" | SKILL PRIMITIVE |

Subs 2-5 of #1208 are **OUT OF SCOPE** for Phase 1 (handled in Phases 2-5 of the parent plan).

## 3.4 Codex 1b addressed findings (2026-05-18)

| Finding | Resolution |
|---|---|
| HIGH: jobs entrypoint never calls `run_migrations()`; a jobs-first boot on a fresh DB would hit the helper before `runtime_config` exists. | T4 spec narrows to **API-first migration contract**: only `app/main.py` runs `run_migrations()`. The jobs boot guard is defensive against post-migration vanish, not pre-migration absence. Added explicit pre-condition note in §8.2. This matches the existing contract (jobs has never called `run_migrations()` since #719). |
| MEDIUM: jobs insertion point underspecified vs existing ordering. | T4 §8.2 pins the insertion point: AFTER `_enforce_pg_locks_with_cleanup(fence_conn, pool)` (`app/jobs/__main__.py:277`) and BEFORE `_bootstrap_master_key(pool)` (`:280`). Uses the same fence+pool cleanup-on-raise pattern as the max-locks guard (extracted into `_ensure_runtime_config_singleton_with_cleanup`). |
| LOW: directive parser accepts the directive on any of the first 3 stripped lines; could false-positive on a migration whose body literally contains the line. | T0 narrowed to `sql.lstrip().splitlines()[0].strip() == AUTOCOMMIT_DIRECTIVE` — strict line 1 (after leading blank lines). |

## 3.5 Codex 1a addressed findings (2026-05-18)

| Finding | Resolution |
|---|---|
| BLOCKING: `ALTER SYSTEM` errors inside the migration runner's implicit tx (verified empirically: `ERROR: ALTER SYSTEM cannot run inside a transaction block`). | T0 — add `-- runner: autocommit` file-header directive support to the runner. Migration declares autocommit; runner opens conn with `autocommit=True`; each statement runs as its own implicit tx. `schema_migrations` INSERT still tracks the migration. Idempotent on re-run if INSERT happens to fail after body succeeds (ALTER SYSTEM is idempotent). |
| HIGH: Jobs process boots from `app/jobs/__main__.py` and reads `runtime_config` in scheduler paths; needs the same boot guard. | T4 — wire `ensure_runtime_config_singleton` into the jobs entrypoint with the same lifespan-style invocation. |
| HIGH: Helper inserts the singleton row without `runtime_config_audit` rows, violating the module invariant ("every mutation writes one audit row per changed field, in the same transaction as the UPDATE"). | T3 — write 3 audit rows (`enable_auto_trading`, `enable_live_trading`, `display_currency`) with `old_value=NULL`, `changed_by='boot_recovery'`, `reason='singleton vanished — re-seeded by boot guard'` inside a single `with conn.transaction():` block. Caller passes an autocommit conn so `conn.transaction()` opens a real new tx (not SAVEPOINT). Service-no-commit invariant is preserved: the boot guard OWNS the connection lifecycle (called from lifespan, not from a request handler holding an open tx). |
| MEDIUM: Corruption check uses `count(*) WHERE id = TRUE`; a row with `id != TRUE` would be treated as missing instead of failing loud. | T3 — `SELECT id FROM runtime_config`; assert at most one row with `id=TRUE`; raise `RuntimeError` on any row whose `id != TRUE` (defensive — the `CHECK (id = TRUE)` constraint should forbid this, but corruption scenarios this guard exists for include constraint drops). |
| MEDIUM: `guard_conn.autocommit = True` after connect leaves a window where an implicit tx could open. | T4 — `psycopg.connect(settings.database_url, autocommit=True)` at connect time so no tx is ever implicitly opened on this conn. |
| MEDIUM: `pg_database_size` invariant is a tripwire, not proof — misses deletes/HOT updates and can false-positive on idle Postgres background growth. | T6 — explicitly document as a TRIPWIRE in the file header. Primary defense remains `_assert_test_db` in `tests/fixtures/ebull_test_db.py` (rejects any destructive op against non-`ebull_test_*` DBs). Tripwire catches the residue: writes that went through a non-fixture path (raw `psycopg.connect(settings.database_url)` in a test). |

## 4. T0 — Migration runner `-- runner: autocommit` directive

`ALTER SYSTEM` cannot run inside a transaction block (empirically: `ERROR: ALTER SYSTEM cannot run inside a transaction block`). The current runner at `app/db/migrations.py:70-78` uses `with psycopg.connect()` (autocommit=False) which implicitly opens a tx on the first `execute()`, so a migration containing `ALTER SYSTEM` fails.

Resolution: support a single-line directive at the very top of a migration file (`-- runner: autocommit`) that switches the runner into autocommit mode for that file. Each statement then runs as its own implicit tx — `ALTER SYSTEM` is happy. `schema_migrations` INSERT runs as a separate autocommit statement. On a partial failure (body succeeds, INSERT fails) the next boot re-runs the body; safe because `ALTER SYSTEM SET` is naturally idempotent (overwrites the line in `postgresql.auto.conf`).

Runner change shape (sketch):

```python
AUTOCOMMIT_DIRECTIVE = "-- runner: autocommit"

def _wants_autocommit(sql: str) -> bool:
    """Strict: directive MUST be the first non-blank line of the file.

    Codex 1b LOW: looser parsing (e.g. checking the first 3 lines) could
    false-positive on a migration whose body literally contains the
    directive string. Line 1 only — unambiguous.
    """
    lines = sql.lstrip().splitlines()
    return bool(lines) and lines[0].strip() == AUTOCOMMIT_DIRECTIVE

# inside the per-file loop:
wants_autocommit = _wants_autocommit(sql)
with psycopg.connect(settings.database_url, autocommit=wants_autocommit) as conn:
    try:
        with psycopg.ClientCursor(conn) as cur:
            cur.execute(sql)
            cur.execute("INSERT INTO schema_migrations (filename) VALUES (%s)", (path.name,))
        if not wants_autocommit:
            conn.commit()
        logger.info("Applied: %s%s", path.name, " (autocommit)" if wants_autocommit else "")
    except Exception:
        if not wants_autocommit:
            conn.rollback()
        logger.exception("Migration failed: %s -- rolled back" if not wants_autocommit else "Migration failed: %s -- partial in autocommit mode", path.name)
        raise
```

Blast radius: only files containing the directive change behaviour. Existing migrations (all non-autocommit) take the unchanged path. Covered by a unit test in `tests/test_migration_runner_autocommit.py` (new) that parses the directive on a fixture migration file.

## 5. T1 — `sql/155_postgres_runtime_tuning.sql`

```sql
-- runner: autocommit
-- 155: Postgres runtime tuning for partitioned-ownership workload (#1208 Sub 1)
--
-- ALTER SYSTEM cannot run inside a transaction block (PG limitation).
-- The directive on line 1 tells the migration runner to apply this file
-- with autocommit=True so each statement gets its own implicit tx.
--
-- Container defaults (PG17) are too small for eBull's partitioned-ownership
-- schema. max_wal_size=1024 MB triggers WAL PANIC under autovacuum bursts
-- on the 28 GB unpartitioned financial_facts_raw table. shared_buffers
-- (128 MB) is laughable for a 46 GB DB. wal_compression=off leaves an
-- easy win on the table for partition-heavy churn.
--
-- ALTER SYSTEM SET persists to postgresql.auto.conf; pg_reload_conf() applies
-- everything except shared_buffers (which requires a container restart).
-- The operator runbook (#1208 issue body) covers the restart sequencing.

ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET min_wal_size = '512MB';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET shared_buffers = '2GB';            -- restart required
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET effective_cache_size = '4GB';
ALTER SYSTEM SET work_mem = '32MB';

SELECT pg_reload_conf();
```

**Idempotency:** `ALTER SYSTEM SET` overwrites the line in `postgresql.auto.conf`. Tracked in `schema_migrations` so it runs exactly once per DB; partial-failure re-run is safe.

## 5. T2 — `docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:17
    container_name: ebull-postgres
    restart: unless-stopped
    mem_limit: 4g          # NEW — bound container memory
    shm_size: 1g           # NEW — PG uses shared mem for parallel workers + sorts
    environment:
      ...
```

Justification: `shared_buffers=2 GB` + `effective_cache_size=4 GB` + `maintenance_work_mem=512 MB` requires the container to actually have memory available. `shm_size` default of 64 MB is enough for tiny workloads but PG17 parallel workers + autovacuum on partitioned tables will starve on it.

## 7. T3 — `ensure_runtime_config_singleton(conn)`

Add to `app/services/runtime_config.py`:

```python
BOOT_RECOVERY_REASON = "singleton vanished — re-seeded by boot guard"

def ensure_runtime_config_singleton(conn: psycopg.Connection[Any]) -> None:
    """Re-seed the runtime_config singleton row if it vanished.

    Migration sql/015_runtime_config.sql seeds the row via
    INSERT ... ON CONFLICT DO NOTHING — a one-time write. If the row is
    later lost (manual DELETE, snapshot restore from pre-seed era, future
    bootstrap reset script), every endpoint that reads runtime_config
    fail-closes with RuntimeConfigCorrupt → 503. This boot-time guard
    inspects the singleton and re-seeds with safe defaults on absence,
    writing one runtime_config_audit row per re-seeded field so the
    module-level audit invariant ("every mutation writes one audit row
    per changed field, in the same transaction as the UPDATE") still
    holds for boot recovery.

    Posture: fail-closed defaults match the migration seed
    (enable_auto_trading=FALSE, enable_live_trading=FALSE,
    display_currency='GBP'). A WARNING is logged so the operator notices.

    Idempotent: no-op when exactly one row with id=TRUE exists.
    Fail-loud when a non-canonical row exists (id != TRUE; would only
    happen under constraint corruption).

    Connection contract: caller must supply a conn in autocommit mode
    (e.g. `psycopg.connect(url, autocommit=True)`). The helper opens its
    own real new transaction via `conn.transaction()` to keep the seed
    INSERT + the three audit INSERTs atomic. Because the caller's conn
    has no outer tx open, `conn.transaction()` is a real BEGIN (not a
    SAVEPOINT), so the service-no-commit invariant
    [[psycopg3_savepoint_commit]] is preserved.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM runtime_config")
        rows = cur.fetchall()

    if len(rows) == 1 and rows[0][0] is True:
        return

    if len(rows) > 1 or (rows and rows[0][0] is not True):
        # CHECK (id = TRUE) PRIMARY KEY should forbid this. Fail-loud
        # rather than mask constraint corruption.
        raise RuntimeError(
            f"runtime_config singleton constraint violated — rows={rows!r}"
        )

    logger.warning(
        "runtime_config singleton vanished — re-seeding with safe defaults "
        "(enable_auto_trading=FALSE, enable_live_trading=FALSE, "
        "display_currency='GBP'). See docs/review-prevention-log.md "
        "§'Singleton-row migrations need a boot-time presence guard'."
    )
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runtime_config
                    (id, enable_auto_trading, enable_live_trading,
                     updated_by, reason, display_currency)
                VALUES
                    (TRUE, FALSE, FALSE,
                     'boot_recovery', %(reason)s, 'GBP')
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                """,
                {"reason": BOOT_RECOVERY_REASON},
            )
            inserted = cur.fetchone()

        if inserted is None:
            # Race: another process re-seeded between our SELECT and
            # INSERT. Their seed row stands; don't write phantom audit
            # rows for a recovery we didn't actually perform.
            return

        for field, new_value in (
            ("enable_auto_trading", "false"),
            ("enable_live_trading", "false"),
            ("display_currency", "GBP"),
        ):
            _insert_audit_row(
                conn,
                changed_at=_utcnow(),
                changed_by="boot_recovery",
                reason=BOOT_RECOVERY_REASON,
                field=cast(AuditField, field),
                old_value=None,
                new_value=new_value,
            )
```

The `ON CONFLICT DO NOTHING` + `RETURNING id` pair defends against a race where another process re-seeded between the `SELECT` and the `INSERT`. If `inserted is None`, our INSERT was suppressed — skip the audit rows so we don't record a phantom recovery.

## 8. T4 — Boot wiring (API + jobs)

Two entrypoints boot the runtime: `app/main.py` (FastAPI HTTP) and `app/jobs/__main__.py` (orchestrator/jobs runtime). Both read `runtime_config` on startup (scheduler reads at `app/workers/scheduler.py:2497` + `:2979`), so both need the guard. Either entrypoint may boot first in a fresh deploy.

### 8.1 `app/main.py` lifespan

Insertion point: AFTER `enforce_max_locks_floor` and BEFORE `open_pool`. Same `asyncio.to_thread` shape as the max-locks probe.

```python
# After enforce_max_locks_floor and before open_pool:

from app.services.runtime_config import ensure_runtime_config_singleton

def _ensure_runtime_config_singleton_probe() -> None:
    # autocommit=True at connect time so no implicit tx is ever opened
    # on this conn (Codex 1a finding 5). The helper opens its own real
    # tx via conn.transaction() — safe under autocommit per psycopg3
    # docs.
    with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
        ensure_runtime_config_singleton(guard_conn)

await asyncio.to_thread(_ensure_runtime_config_singleton_probe)
```

### 8.2 `app/jobs/__main__.py` boot

**Pre-condition (API-first migration contract):** jobs does NOT call `run_migrations()` (since #719). This boot guard is defensive against a runtime_config singleton vanishing AFTER the API has applied migrations, not against pre-migration absence. If jobs boots first on a totally fresh DB, the helper's SELECT will fail with `UndefinedTable` — which is the correct fail-loud signal that the operator launched jobs before the API ever ran migrations.

Insertion point: AFTER `_enforce_pg_locks_with_cleanup(fence_conn, pool)` at `app/jobs/__main__.py:277` and BEFORE `_bootstrap_master_key(pool)` at `:280`. Uses the same fence+pool cleanup-on-raise shape as `_enforce_pg_locks_with_cleanup` so a raise here releases the singleton-fence advisory lock + closes the pool:

```python
def _ensure_runtime_config_singleton_with_cleanup(
    fence_conn: psycopg.Connection[Any],
    pool: Any,
) -> None:
    """Run the #1208 runtime_config singleton-vanish guard with fence
    + pool cleanup on raise. Mirrors _enforce_pg_locks_with_cleanup.
    """
    from app.services.runtime_config import ensure_runtime_config_singleton

    try:
        with psycopg.connect(settings.database_url, autocommit=True) as guard_conn:
            ensure_runtime_config_singleton(guard_conn)
    except BaseException:
        with contextlib.suppress(Exception):
            fence_conn.close()
        with contextlib.suppress(Exception):
            pool.close()
        raise

# inside serve() at line ~278:
_enforce_pg_locks_with_cleanup(fence_conn, pool)
logger.info("jobs entrypoint: max_locks_per_transaction guard passed")

_ensure_runtime_config_singleton_with_cleanup(fence_conn, pool)
logger.info("jobs entrypoint: runtime_config singleton guard passed")

_bootstrap_master_key(pool)
```

Idempotency under both entrypoints booting concurrently is covered by the `RETURNING id` race-guard inside the helper (§7) — phantom audit rows are not written.

## 9. T5 — `tests/test_runtime_config_boot_guard.py`

Test cases, all using the real `ebull_test_conn` fixture (not mocks — this exercises the actual SQL). The fixture yields a `autocommit=False` conn — fine because the test wraps DELETE then provides a fresh autocommit conn to the helper:

```python
class TestEnsureRuntimeConfigSingleton:
    def test_noop_when_row_exists(self, ebull_test_conn, caplog) -> None:
        # template DB carries the seeded row; helper must be a quiet no-op.
        caplog.set_level("WARNING", logger="app.services.runtime_config")
        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_runtime_config_singleton(guard_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM runtime_config")
            assert cur.fetchone()[0] == 1
        assert not any(
            "singleton vanished" in record.message for record in caplog.records
        )

    def test_reseeds_when_row_missing_with_audit_rows(
        self, ebull_test_conn, caplog
    ) -> None:
        caplog.set_level("WARNING", logger="app.services.runtime_config")
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM runtime_config WHERE id = TRUE")
            cur.execute("DELETE FROM runtime_config_audit")
        ebull_test_conn.commit()

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_runtime_config_singleton(guard_conn)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT enable_auto_trading, enable_live_trading, "
                "display_currency, updated_by, reason FROM runtime_config"
            )
            row = cur.fetchone()
        assert row is not None
        assert row["enable_auto_trading"] is False
        assert row["enable_live_trading"] is False
        assert row["display_currency"] == "GBP"
        assert row["updated_by"] == "boot_recovery"
        assert "vanished" in row["reason"]
        assert any(
            "singleton vanished" in record.message for record in caplog.records
        )
        # Audit invariant: one row per re-seeded field.
        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT field, old_value, new_value, changed_by, reason "
                "FROM runtime_config_audit ORDER BY field"
            )
            audit_rows = cur.fetchall()
        assert [r["field"] for r in audit_rows] == sorted([
            "display_currency", "enable_auto_trading", "enable_live_trading",
        ])
        assert all(r["old_value"] is None for r in audit_rows)
        assert all(r["changed_by"] == "boot_recovery" for r in audit_rows)
        assert all("vanished" in r["reason"] for r in audit_rows)

    def test_atomic_failure_rolls_back_seed(
        self, ebull_test_conn, monkeypatch
    ) -> None:
        # Force the audit insert to raise; assert the singleton seed
        # was also rolled back. Proves the helper's tx is real (not a
        # SAVEPOINT) and that we don't leave a row with no audit trail.
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM runtime_config WHERE id = TRUE")
            cur.execute("DELETE FROM runtime_config_audit")
        ebull_test_conn.commit()

        from app.services import runtime_config as rc_mod

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated audit insert failure")

        monkeypatch.setattr(rc_mod, "_insert_audit_row", _boom)

        url = test_database_url()
        with pytest.raises(RuntimeError, match="simulated audit insert"):
            with psycopg.connect(url, autocommit=True) as guard_conn:
                ensure_runtime_config_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM runtime_config")
            assert cur.fetchone()[0] == 0

    def test_raises_on_non_canonical_row(self, ebull_test_conn) -> None:
        # CHECK (id = TRUE) PRIMARY KEY would forbid this on a real
        # schema; simulate corruption by dropping + re-creating the
        # table without the CHECK, inserting a FALSE row, and asserting
        # the helper fails loud.
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM runtime_config")
            cur.execute("ALTER TABLE runtime_config DROP CONSTRAINT runtime_config_single_row")
            cur.execute(
                "INSERT INTO runtime_config "
                "(id, enable_auto_trading, enable_live_trading, "
                " updated_by, reason, display_currency) "
                "VALUES (FALSE, FALSE, FALSE, 'corrupt', 'corrupt', 'GBP')"
            )
        ebull_test_conn.commit()

        url = test_database_url()
        try:
            with pytest.raises(RuntimeError, match="singleton constraint violated"):
                with psycopg.connect(url, autocommit=True) as guard_conn:
                    ensure_runtime_config_singleton(guard_conn)
        finally:
            # Restore for following tests on the same worker DB.
            with ebull_test_conn.cursor() as cur:
                cur.execute("DELETE FROM runtime_config")
                cur.execute(
                    "ALTER TABLE runtime_config ADD CONSTRAINT "
                    "runtime_config_single_row CHECK (id = TRUE)"
                )
                cur.execute(
                    "INSERT INTO runtime_config "
                    "(id, enable_auto_trading, enable_live_trading, "
                    " updated_by, reason, display_currency) "
                    "VALUES (TRUE, FALSE, FALSE, 'test', 'restore', 'GBP')"
                )
            ebull_test_conn.commit()
```

## 10. T6 — `tests/test_dev_db_no_test_writes.py` (TRIPWIRE, not proof)

Codex 1a finding 6: this is a tripwire, not proof of no dev writes. Document explicitly. The PRIMARY defense against tests writing to dev DB remains `_assert_test_db` in `tests/fixtures/ebull_test_db.py` — a hard guard that rejects any destructive op against a database whose name does not match `ebull_test_*`. The tripwire catches the residual case: a test that opens a raw `psycopg.connect(settings.database_url)` outside the fixture and writes through it.

```python
"""Tripwire test: flag growth of pg_database_size('ebull') across the suite.

THIS IS A TRIPWIRE, NOT PROOF. The primary defense against tests writing to
dev DB is `tests/fixtures/ebull_test_db.py::_assert_test_db`, which rejects
destructive ops against any DB not matching `ebull_test_*`. This file catches
the residual case where a test opens a raw `psycopg.connect(settings.database_url)`
outside the fixture and writes through it.

Known limitations (Codex 1a finding 6):
- Misses deletes (DB size decreases or stays flat).
- Misses HOT updates (in-place page rewrites with no size growth).
- May false-positive on autovacuum work running concurrently.

When this test fires, find the offending test's raw psycopg.connect call
and route it through the fixture per [[test_db_isolation]].
"""

@pytest.fixture(scope="session", autouse=True)
def _dev_db_size_tripwire() -> Iterator[None]:
    if os.getenv("CI") == "true":
        yield
        return
    try:
        with psycopg.connect(settings.database_url) as conn:
            row = conn.execute(
                "SELECT pg_database_size('ebull')"
            ).fetchone()
            start_size = int(row[0])
    except Exception:
        # Dev DB unreachable — nothing to invariant-check.
        yield
        return

    yield

    try:
        with psycopg.connect(settings.database_url) as conn:
            row = conn.execute("SELECT pg_database_size('ebull')").fetchone()
            end_size = int(row[0])
    except Exception:
        return
    delta_bytes = end_size - start_size
    assert delta_bytes < 1_000_000, (
        f"TRIPWIRE: dev DB grew by {delta_bytes} bytes during the test "
        "session — likely a test wrote to ebull (not ebull_test_*). Per "
        "feedback_test_db_isolation, the test suite MUST point at "
        "ebull_test_*. Grep for raw `psycopg.connect(settings.database_url)` "
        "in the tests directory + verify each use goes through the fixture."
    )
```

Threshold rationale: 1 MB tolerates normal WAL/page-level Postgres background work (autovacuum bumps, stats updates) but flags a test that wrote even a single row to a sizeable dev table. Larger threshold would mask the 150 MB-per-leak failure mode this tripwire exists to catch.

## 10. T7 — Prevention-log

Extend the existing `### Singleton-row migrations need a boot-time presence guard` entry's "Enforced in" line with the actual landing SHA + helper path once the PR merges. Add a new section:

```
### Postgres on Docker Desktop macOS — defaults blow up partition-heavy workloads

- First seen in: 2026-05-18, two PANIC events at 09:05 + 12:54 UTC on dev
  container `ebull-postgres`. `xlogtemp.NNN: No space left on device`
  despite host disk having free space.
- Symptom: WAL writer SIGABRTs under autovacuum bursts on the 28 GB
  unpartitioned `financial_facts_raw` table. Recovery walks every relfile
  on fsync (relfile count on `ebull` = 4,723) and takes 45+ min.
- Prevention: any PG container hosting eBull MUST set:
  - `max_wal_size >= 4GB`
  - `shared_buffers >= 1GB`
  - `wal_compression = on`
  - `maintenance_work_mem >= 256MB`
  - `work_mem >= 16MB`
  - container `mem_limit >= 4g` + `shm_size >= 1g`
- Enforced in: `sql/155_postgres_runtime_tuning.sql` (ALTER SYSTEM
  + pg_reload_conf), `docker-compose.yml` (mem_limit + shm_size). New
  mega-tables (>1 M rows / >1 GB) MUST be partitioned at design time;
  retrofit is the subject of #1208 Sub 3.
```

## 11. T8 — Skill updates

`.claude/skills/engineering/test-quality.md` gets a new §"Dev-DB isolation invariant" linking to `tests/test_dev_db_no_test_writes.py` and explaining the 1 MB threshold rationale.

## 12. Acceptance / DoD

ETL DoD clauses #8-#12 (see CLAUDE.md "ETL / parser / schema-migration additional clauses"):

- **#8 Smoke:** delete the runtime_config row → restart `python -m app.main` → observe WARNING line + `GET /config` returns 200.
- **#9 Cross-source:** N/A (not a data-source change).
- **#10 Backfill:** N/A.
- **#11 Operator-visible:** `GET /config` returns 200 after the boot guard re-seeds; app log shows the WARNING line.
- **#12 PR records verification + SHA:** PR description records the smoke command + commit SHA the smoke was executed against.

## 13. Out of scope

- Sub 2 (test-fixture orphan sweep) — Phase 2.
- Sub 3 (`financial_facts_raw` partitioning + retention) — Phase 3.
- Sub 4 (`/system/postgres-health` + pre-push bloat warn) — Phase 4.
- Sub 5 (full prevention-log section beyond what this PR ships) — Phase 5 (folded into P4 PR).
- Production HA / replication tuning.
- Postgres-on-K8s / managed-Postgres migration.

## 14. References

- Parent maintenance plan: `docs/superpowers/plans/2026-05-18-backend-stability.md`.
- Issue: #1208.
- Sibling resilience pattern: `sql/010_execution_guard.sql` (kill_switch singleton + seed), `sql/015_runtime_config.sql` (runtime_config singleton + seed), PR #1188 (`enforce_max_locks_floor` boot guard — same lifespan shape).
- Live evidence: 2026-05-18 operator-reported `/config` 503 + slow login; PANIC log evidence in #1208 body.
