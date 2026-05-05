"""Reusable helpers for tests that need a real ``ebull_test`` Postgres.

Originally extracted from ``tests/test_operator_setup_race.py``. As of
issue #893, this module owns:

* per-worker, per-invocation private databases
  (``ebull_test_<run_id>_<worker_id>``)
* a session-shared template database (``ebull_test_template``) built
  once per migration-set hash and reused across runs
* a session-end teardown that drops the worker's private DB

The strategy:

1. **Controller process** (the pytest top-level / ``main`` worker)
   builds ``ebull_test_template`` if its migration hash is stale,
   under a Postgres advisory lock so concurrent pytest invocations
   serialise on template construction.
2. **Each xdist worker** (gw0, gw1, ...) creates its own private DB
   from the template via ``CREATE DATABASE ... TEMPLATE
   ebull_test_template``. Postgres copies pages directly so this is
   sub-second on local SSD.
3. The fixture's per-test ``TRUNCATE`` runs against the worker's
   private DB. Cross-worker contention is impossible because each
   worker owns its DB.
4. ``settings.database_url`` (the operator's dev DB) is never written
   to by the test suite, with the documented exception of
   ``tests/smoke/test_app_boots.py`` (the lifespan smoke gate).

Critically: the dev DB at ``settings.database_url`` is never touched
by anything in this module. ``_assert_test_db`` enforces that any
destructive op runs against an ``ebull_test_*`` database, never
``ebull``.
"""

from __future__ import annotations

import hashlib
import os
import time
import warnings
from collections.abc import Iterator
from pathlib import Path
from secrets import token_hex
from urllib.parse import urlparse, urlunparse

import psycopg
import psycopg.rows
import pytest
from psycopg import sql

from app.config import settings

TEMPLATE_DB_NAME = "ebull_test_template"
_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# Advisory lock keys. Cross-pytest-invocation locks live on the
# maintenance ``postgres`` DB so they don't collide with application
# advisory locks. Constants are documented for the audit trail —
# application code must not pick keys in this range.
EBULL_TEMPLATE_LOCK = 0x65427554455354  # ASCII "eBuTEST"
EBULL_SMOKE_LIFESPAN_LOCK = 0x65427554534D4B  # ASCII "eBuTSMK"

# Run-id env var. Set once in the controller; xdist propagates env to
# spawned workers automatically.
_RUN_ID_ENV = "EBULL_PYTEST_RUN_ID"


# Path to the migration-hash cache file. Lives under the user's cache
# dir so the value survives across pytest invocations.
def _hash_cache_path() -> Path:
    try:
        from platformdirs import user_cache_dir
    except ImportError:  # pragma: no cover
        cache_root = Path.home() / ".cache" / "ebull"
    else:
        cache_root = Path(user_cache_dir("ebull"))
    cache_root.mkdir(parents=True, exist_ok=True)
    return cache_root / "test_template_hash"


# Tables the per-test fixture truncates between tests. Keep child-to-
# parent so CASCADE handles any FK we missed. New tables added by a
# migration that introduces FKs MUST be appended here in the same PR
# (review-prevention-log entry "Test-teardown list missing new FK-child
# tables").
_PLANNER_TABLES: tuple[str, ...] = (
    "cascade_retry_queue",
    "cik_upsert_timing",
    "financial_facts_raw",
    "sec_facts_concept_catalog",
    "sec_entity_change_log",
    "data_ingestion_runs",
    "layer_enabled_audit",
    "layer_enabled",
    "external_identifiers",
    "external_data_watermarks",
    "coverage_status_events",
    "coverage",
    "position_alerts",
    "watchlist",
    "broker_positions",
    "positions",
    "quotes",
    "instruments",
    "job_runs",
    "financial_periods_raw",
    "financial_periods",
    "dividend_events",
    "eight_k_exhibits",
    "eight_k_items",
    "eight_k_filings",
    "filing_documents",
    "instrument_business_summary_sections",
    "instrument_business_summary",
    "instrument_sec_profile",
    "insider_transaction_footnotes",
    "insider_transactions",
    "insider_initial_holdings",
    "insider_filers",
    "insider_filings",
    "unresolved_13f_cusips",
    "institutional_holdings_ingest_log",
    "institutional_holdings",
    "institutional_filers",
    "institutional_filer_seeds",
    "etf_filer_cik_seeds",
    "ncen_filer_classifications",
    "blockholder_filings_ingest_log",
    "blockholder_filings",
    "blockholder_filers",
    "blockholder_filer_seeds",
    "def14a_drift_alerts",
    "def14a_ingest_log",
    "def14a_beneficial_holdings",
    "filing_events",
    "instrument_cik_history",
    "instrument_symbol_history",
    "ingest_backfill_queue",
    "filing_raw_documents",
    "data_reconciliation_findings",
    "data_reconciliation_runs",
    "cik_raw_documents",
    "sec_filing_manifest",
    "data_freshness_index",
    "decision_audit",
    "trade_recommendations",
    "operators",
    "ownership_insiders_current",
    "ownership_insiders_observations",
    "ownership_institutions_current",
    "ownership_institutions_observations",
    "ownership_blockholders_current",
    "ownership_blockholders_observations",
    "ownership_treasury_current",
    "ownership_treasury_observations",
    "ownership_def14a_current",
    "ownership_def14a_observations",
    # #893 — dev-DB writers migrated onto worker test DB; tables they
    # touched now need per-test cleanup.
    "job_runtime_heartbeat",
    "pending_job_requests",
)


def _swap_database(url: str, new_db: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{new_db}"))


def _admin_database_url() -> str:
    """URL for the maintenance ``postgres`` DB.

    Used for ``CREATE DATABASE``, ``DROP DATABASE``, and the
    cross-invocation advisory lock. Must never be confused with the
    operator's dev DB.
    """
    return _swap_database(settings.database_url, "postgres")


def _run_id() -> str:
    """Return the per-pytest-invocation run id.

    Set once on first call (in the controller, before workers spawn)
    and stored in the environment so xdist's worker-spawn propagation
    delivers the same id to every worker. ``int(time.time())`` is
    seconds resolution; the 6 hex chars from ``token_hex(3)`` add 24
    bits of entropy → collision probability across two invocations
    starting in the same second is ~1 / 16M.
    """
    rid = os.environ.get(_RUN_ID_ENV)
    if rid is None:
        rid = f"{int(time.time())}_{token_hex(3)}"
        os.environ[_RUN_ID_ENV] = rid
    return rid


def _worker_id() -> str:
    """Return the xdist worker id, or ``"main"`` in single-process pytest."""
    return os.environ.get("PYTEST_XDIST_WORKER", "main")


def test_db_name() -> str:
    """Compute the per-worker, per-invocation private DB name."""
    return f"ebull_test_{_run_id()}_{_worker_id()}"


# Opt-out of pytest's test-collection. The function names are
# ``test_*`` because that's the public API the rest of the suite has
# always called them by, but they are helpers — not tests — and
# pytest would otherwise auto-collect them when imported into a test
# module. ``__test__ = False`` is the documented escape hatch.
test_db_name.__test__ = False  # type: ignore[attr-defined]


def test_database_url() -> str:
    return _swap_database(settings.database_url, test_db_name())


test_database_url.__test__ = False  # type: ignore[attr-defined]


def template_database_url() -> str:
    return _swap_database(settings.database_url, TEMPLATE_DB_NAME)


def _migration_hash() -> str:
    """Hash of the (filename, bytes) sequence of every migration file.

    Including the filename catches renames that would otherwise leave
    a stale template after migrations were re-numbered. Sorted by
    filename so the order is deterministic across platforms.
    """
    h = hashlib.sha256()
    for path in sorted(_SQL_DIR.glob("*.sql"), key=lambda p: p.name):
        h.update(path.name.encode("utf-8"))
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def _read_stored_hash() -> str | None:
    cache_path = _hash_cache_path()
    try:
        return cache_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None


def _write_stored_hash(value: str) -> None:
    _hash_cache_path().write_text(value, encoding="utf-8")


def _ensure_database(admin: psycopg.Connection[object], db_name: str) -> bool:
    """Return True if the database already existed."""
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        return cur.fetchone() is not None


def _drop_database_force(admin: psycopg.Connection[object], db_name: str) -> None:
    """Drop a database, forcibly evicting any open connections.

    PG13+ supports ``DROP DATABASE ... WITH (FORCE)`` which terminates
    backends connected to the target. Older clusters need a manual
    ``pg_terminate_backend`` loop, but operator confirmed PG15+ on the
    dev box (spec risk-mitigations row).
    """
    # ``db_name`` is composed in this module from constants and a
    # run id we compute. Never user input. ``sql.SQL`` composition
    # via ``Identifier`` is the standard psycopg-typed idiom.
    query = sql.SQL("DROP DATABASE IF EXISTS {name} WITH (FORCE)").format(
        name=sql.Identifier(db_name),
    )
    with admin.cursor() as cur:
        cur.execute(query)


def _create_database_from_template(
    admin: psycopg.Connection[object],
    db_name: str,
    template_name: str,
) -> None:
    query = sql.SQL("CREATE DATABASE {name} TEMPLATE {tpl}").format(
        name=sql.Identifier(db_name),
        tpl=sql.Identifier(template_name),
    )
    with admin.cursor() as cur:
        cur.execute(query)


def _create_empty_database(admin: psycopg.Connection[object], db_name: str) -> None:
    query = sql.SQL("CREATE DATABASE {name}").format(name=sql.Identifier(db_name))
    with admin.cursor() as cur:
        cur.execute(query)


def _apply_migrations(target_url: str) -> None:
    """Apply every ``sql/NNN_*.sql`` file to the target DB.

    Uses a per-file connection so a single transaction-hostile
    migration cannot poison the tracking state of earlier ones. Mirror
    of ``app/db/migrations.run_migrations`` but targeted at an
    arbitrary URL (the test template, not the dev DB).
    """
    files = sorted(_SQL_DIR.glob("*.sql"))
    if not files:
        return

    with psycopg.connect(target_url) as bootstrap:
        with psycopg.ClientCursor(bootstrap) as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "filename TEXT PRIMARY KEY, "
                "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            )
        bootstrap.commit()

    with psycopg.connect(target_url) as reader:
        with reader.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute("SELECT filename FROM schema_migrations")
            done = {row[0] for row in cur.fetchall()}

    for path in files:
        if path.name in done:
            continue
        sql_text = path.read_text(encoding="utf-8")
        with psycopg.connect(target_url) as conn:
            try:
                with psycopg.ClientCursor(conn) as cur:
                    cur.execute(sql_text)  # type: ignore[call-overload]
                    cur.execute(  # type: ignore[call-overload]
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (path.name,),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise


def build_template_if_stale() -> None:
    """Build or rebuild ``ebull_test_template`` under a cluster-wide lock.

    Idempotent: if the migration hash matches the cached value and the
    template exists, this is a no-op (one cheap SELECT). Called from
    the controller-only branch of ``pytest_configure`` in the project
    conftest.
    """
    current = _migration_hash()
    cached = _read_stored_hash()

    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (EBULL_TEMPLATE_LOCK,))
        try:
            template_exists = _ensure_database(admin, TEMPLATE_DB_NAME)
            if template_exists and cached == current:
                return

            if template_exists:
                _drop_database_force(admin, TEMPLATE_DB_NAME)

            _create_empty_database(admin, TEMPLATE_DB_NAME)
            # Apply migrations on a separate connection (we still hold
            # the advisory lock on the postgres DB).
            _apply_migrations(template_database_url())
            _write_stored_hash(current)
        finally:
            with admin.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (EBULL_TEMPLATE_LOCK,))


def _worker_lock_key() -> int:
    """Deterministic per-worker advisory lock key.

    ``hash()`` is salted across Python processes, so we can't use it.
    blake2b is stable; first 8 bytes give us a signed bigint that fits
    Postgres' advisory-lock parameter type.
    """
    payload = f"{_run_id()}:{_worker_id()}".encode()
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


def ensure_worker_database() -> None:
    """Ensure the per-worker private DB exists.

    Idempotent: if the DB already exists for this run + worker, this
    is a no-op. The first worker call inside a run materialises the
    DB from ``ebull_test_template``; subsequent calls (e.g. when
    multiple test files invoke ``test_db_available`` for skipif) do
    nothing.

    Held under three locks while creating from the template:

    * the per-worker advisory lock so a worker re-running itself
      (CI retry) can't race itself, and
    * the cluster-wide ``EBULL_TEMPLATE_LOCK`` while ``CREATE
      DATABASE ... TEMPLATE`` reads the template, so a concurrent
      pytest invocation cannot drop + rebuild the template mid-copy
      (Codex pre-push #2).
    """
    db_name = test_db_name()
    lock_key = _worker_lock_key()

    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
        try:
            if _ensure_database(admin, db_name):
                # Already materialised earlier in this invocation.
                # Subsequent test_db_available probes must NOT drop
                # this DB — that would wipe state mid-run (Codex
                # pre-push #1).
                return
            # Hold EBULL_TEMPLATE_LOCK while reading the template so
            # a concurrent invocation cannot rebuild the template
            # mid-copy. The lock is brief (page-level COPY); it will
            # not throttle template builds materially.
            with admin.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (EBULL_TEMPLATE_LOCK,))
            try:
                _create_database_from_template(admin, db_name, TEMPLATE_DB_NAME)
            finally:
                with admin.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (EBULL_TEMPLATE_LOCK,))
        finally:
            with admin.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))


def drop_worker_database() -> None:
    """Drop the worker's private DB at session end."""
    db_name = test_db_name()
    try:
        with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
            _drop_database_force(admin, db_name)
    except Exception as exc:  # pragma: no cover - best-effort cleanup
        warnings.warn(
            f"Failed to drop test database {db_name!r}: "
            f"{type(exc).__name__}: {exc}. "
            f"Run `uv run python -m tests.fixtures.cleanup_test_dbs` to "
            f"reclaim leaked databases.",
            stacklevel=2,
        )


def test_db_available() -> bool:  # noqa: D401 — `test_*` here is the legacy public name, not a pytest test
    """Probe the test DB stack.

    Triggers the controller-side template build if it hasn't run yet
    (e.g. when used outside a pytest_configure-aware test runner) and
    materialises the per-worker DB.

    Returns False on any failure so the test skips cleanly in
    environments without a Postgres at all. Logs a warning so
    configuration bugs (role lacks CREATEDB privilege, etc.) don't
    hide under the same skip path as "no Postgres".
    """
    try:
        build_template_if_stale()
        ensure_worker_database()
        with psycopg.connect(test_database_url(), connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception as exc:
        warnings.warn(
            f"ebull_test DB unavailable -- {type(exc).__name__}: {exc}. "
            f"Tests that depend on it will be skipped. If this is "
            f"unexpected, check that the configured Postgres role has "
            f"CREATEDB privilege and that the host/port in "
            f"EBULL_DATABASE_URL is reachable.",
            stacklevel=2,
        )
        return False


test_db_available.__test__ = False  # type: ignore[attr-defined]


def assert_test_db(conn: psycopg.Connection[object]) -> None:
    """Refuse to run a destructive op against anything but the worker's test DB.

    Paranoid backstop: a future refactor could accidentally pass a
    connection to ``settings.database_url`` (the dev DB) or to the
    shared template into a cleanup fixture. This guard fails the test
    loudly instead of silently TRUNCATing the operator's working
    state or corrupting the reusable template (Codex pre-push #3).
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
        assert row is not None
        db_name = row[0]
    expected = test_db_name()
    if db_name != expected:
        raise RuntimeError(
            f"Refusing to TRUNCATE: connected to database {db_name!r}; "
            f"expected this worker's private DB {expected!r}. "
            f"Neither the dev DB nor the {TEMPLATE_DB_NAME!r} template "
            f"may be wiped by tests."
        )


# Back-compat alias — older test modules imported the underscore-prefixed
# private form. Both names point at the same callable.
_assert_test_db = assert_test_db


def _truncate_planner_tables(conn: psycopg.Connection[tuple]) -> None:
    """Truncate the planner table set in chunks.

    A single ``TRUNCATE ... CASCADE`` over 70+ tables on a worker
    running concurrently with other workers exhausts Postgres'
    ``max_locks_per_transaction`` (default 64). Splitting into
    bounded chunks keeps the per-transaction lock count safe even
    when CASCADE pulls in extra child tables.
    """
    assert_test_db(conn)
    chunk_size = 20
    chunks = [_PLANNER_TABLES[i : i + chunk_size] for i in range(0, len(_PLANNER_TABLES), chunk_size)]
    with conn.cursor() as cur:
        for chunk in chunks:
            query = sql.SQL("TRUNCATE {tables} RESTART IDENTITY CASCADE").format(
                tables=sql.SQL(", ").join(sql.Identifier(t) for t in chunk),
            )
            cur.execute(query)
            conn.commit()


@pytest.fixture
def ebull_test_conn() -> Iterator[psycopg.Connection[tuple]]:
    """Yield a fresh connection to the worker's private test DB.

    TRUNCATE before and after each test. Both passes go through
    ``_assert_test_db`` so the dev DB can never be wiped by a
    misconfigured connection.
    """
    if not test_db_available():
        pytest.skip("ebull_test DB unavailable")

    url = test_database_url()
    with psycopg.connect(url) as setup_conn:
        _truncate_planner_tables(setup_conn)

    conn = psycopg.connect(url)
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            _truncate_planner_tables(conn)
        finally:
            conn.close()


__all__ = [
    "EBULL_SMOKE_LIFESPAN_LOCK",
    "EBULL_TEMPLATE_LOCK",
    "TEMPLATE_DB_NAME",
    "assert_test_db",
    "build_template_if_stale",
    "drop_worker_database",
    "ebull_test_conn",
    "ensure_worker_database",
    "test_database_url",
    "test_db_available",
    "test_db_name",
    "template_database_url",
]
