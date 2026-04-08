"""Real-DB race-safety test for first-run operator setup.

The advisory lock in ``perform_setup`` (sql/017 + operator_setup.py) is
the load-bearing piece that makes simultaneous /auth/setup requests safe
under READ COMMITTED. Mocks cannot exercise it -- we need a real
Postgres connection so the lock is actually taken.

Strategy:
  1. Open a "blocker" connection and take ``pg_advisory_xact_lock`` on
     the same key inside an open transaction.
  2. Spawn a worker thread that calls ``perform_setup`` with a fresh
     connection. The worker must block on the advisory lock.
  3. Assert the worker is still blocked after a short wait.
  4. Commit the blocker -- the worker now proceeds and succeeds.
  5. Run a second perform_setup against an already-populated table and
     assert it returns ALREADY_SETUP.

Skipped automatically if no Postgres URL is configured or the DB is
unreachable -- we never want this test to fail in a CI run that has no
Postgres at all, but we do want it to fail loudly if the lock is
removed.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
import psycopg.rows
import pytest

from app.config import settings  # used by _swap_database to derive test URL
from app.services.operator_setup import (
    _BOOTSTRAP_LOCK_KEY,
    SetupOutcome,
    perform_setup,
    reset_token_slot_for_tests,
)

# ---------------------------------------------------------------------------
# Isolated test database
# ---------------------------------------------------------------------------
#
# This test runs ``TRUNCATE operators, sessions, operator_audit RESTART
# IDENTITY CASCADE`` and the CASCADE follows the FK from
# ``broker_credentials.operator_id`` -- so a TRUNCATE here also wipes
# every saved broker credential. The dev database (typically ``ebull``)
# holds the user's real operator account and live broker keys, and a
# pytest run that points at that database destroys their working state
# without warning. This was discovered the hard way on 2026-04-08 when
# multiple pytest runs during a PR cycle silently wiped the user's
# operator + demo eToro key, locking them out of the running app.
#
# Fix: derive an isolated ``ebull_test`` database URL from
# ``settings.database_url`` (same host, same credentials, different
# database name), create the database if it does not yet exist, apply
# the project's SQL migrations to it, and run every connection in this
# test against ``_TEST_DATABASE_URL`` instead of ``settings.database_url``.
# A paranoid ``_assert_test_db`` guard runs before every TRUNCATE so a
# future refactor that accidentally re-introduces ``settings.database_url``
# fails loud instead of silently destroying user data.

_TEST_DB_NAME = "ebull_test"
_SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def _swap_database(url: str, new_db: str) -> str:
    """Return *url* with the path component replaced by ``/{new_db}``."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{new_db}"))


def _test_database_url() -> str:
    return _swap_database(settings.database_url, _TEST_DB_NAME)


def _admin_database_url() -> str:
    # Admin connection used only to ``CREATE DATABASE``. Connecting to
    # the built-in ``postgres`` maintenance DB avoids any chance of
    # holding a session on the database we are about to create.
    return _swap_database(settings.database_url, "postgres")


def _ensure_test_db_exists() -> None:
    """Create ``ebull_test`` if it does not yet exist.

    ``CREATE DATABASE`` cannot run inside a transaction, so the admin
    connection is opened in autocommit mode. Idempotent: if the
    database already exists we return without touching it.
    """
    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (_TEST_DB_NAME,))
            if cur.fetchone() is None:
                # Identifier interpolation: _TEST_DB_NAME is a hard-coded
                # constant, never user input, so SQL injection is not a
                # concern. The double-quoting still defends against a
                # future change to the constant.
                cur.execute(f'CREATE DATABASE "{_TEST_DB_NAME}"')


def _apply_migrations_to_test_db() -> None:
    """Apply every ``sql/NNN_*.sql`` file to the test DB.

    Mirrors ``app/db/migrations.run_migrations`` but targets the test
    DB URL directly instead of reading ``settings.database_url``. We
    deliberately do not call ``run_migrations()`` itself because it
    hard-codes ``settings.database_url`` and re-pointing it would
    require monkeypatching settings before any other test imports
    them, which is too fragile to rely on.

    Connection lifecycle mirrors production exactly:
      1. A dedicated bootstrap connection creates ``schema_migrations``
         and commits + closes before any migration file runs. This
         is critical -- if a future migration uses transaction-hostile
         DDL (e.g. ``CREATE INDEX CONCURRENTLY``), a single shared
         connection would roll back the entire batch *including the
         tracking table itself*, leaving the test DB in a half-
         migrated state with no record and the next run would
         re-apply every migration and likely error on duplicate
         objects (BLOCKING comment, PR #129 round 1).
      2. A reader connection fetches the applied set, then closes.
      3. Each pending migration file runs in its **own** connection,
         committing on success and rolling back on failure -- so a
         single broken migration cannot corrupt the tracking state
         of the migrations that ran before it. This matches
         ``app/db/migrations.run_migrations`` line-for-line.
    """
    files = sorted(_SQL_DIR.glob("*.sql"))
    if not files:
        return

    # 1. Bootstrap: schema_migrations exists and is committed before
    #    we even look at the migration files.
    with psycopg.connect(_test_database_url()) as bootstrap:
        with psycopg.ClientCursor(bootstrap) as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "filename TEXT PRIMARY KEY, "
                "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            )
        bootstrap.commit()

    # 2. Reader: fetch the applied set on its own connection so the
    #    per-file connections below see a consistent committed view.
    with psycopg.connect(_test_database_url()) as reader:
        with reader.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute("SELECT filename FROM schema_migrations")
            done = {row[0] for row in cur.fetchall()}

    # 3. Per-file: a fresh connection per migration so a transaction-
    #    hostile statement in one file cannot poison the tracking
    #    state of the others.
    for path in files:
        if path.name in done:
            continue
        sql = path.read_text(encoding="utf-8")
        with psycopg.connect(_test_database_url()) as conn:
            try:
                with psycopg.ClientCursor(conn) as cur:
                    cur.execute(sql)  # type: ignore[call-overload]
                    cur.execute(  # type: ignore[call-overload]
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (path.name,),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise


def _test_db_available() -> bool:
    """Probe (and lazily create + migrate) the test DB.

    Returns False on any failure -- DNS, refused, auth, permission to
    CREATE DATABASE, migration error -- so the test skips cleanly in
    environments without a Postgres at all (and so the user's dev DB
    is *never* fallen back to as a side effect of an unreachable test
    DB).

    The exception is logged via ``warnings.warn`` rather than
    swallowed silently. A bare ``except Exception: return False``
    looks identical in CI logs whether the cause is "no Postgres at
    all" (an expected skip) or "configured role lacks CREATEDB"
    (a real configuration bug masquerading as a clean skip). The
    warning gives the operator something actionable.
    """
    import warnings

    try:
        _ensure_test_db_exists()
        _apply_migrations_to_test_db()
        with psycopg.connect(_test_database_url(), connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception as exc:
        warnings.warn(
            f"ebull_test DB unavailable -- {type(exc).__name__}: {exc}. "
            f"Race test will be skipped. If this is unexpected, check "
            f"that the configured Postgres role has CREATEDB privilege "
            f"and that the host/port in EBULL_DATABASE_URL is reachable.",
            stacklevel=2,
        )
        return False


pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable -- skipping real-DB race test",
)


def _assert_test_db(conn: psycopg.Connection[object]) -> None:
    """Refuse to run a destructive op against anything but ``ebull_test``.

    Paranoid backstop: if a future refactor accidentally passes a
    connection to ``settings.database_url`` (the dev DB) into the
    ``clean_operators`` fixture, this guard fails the test loudly
    instead of silently TRUNCATing the user's working state.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
        assert row is not None
        db_name = row[0]
    if db_name != _TEST_DB_NAME:
        raise RuntimeError(
            f"Refusing to TRUNCATE: connected to database {db_name!r}, "
            f"expected {_TEST_DB_NAME!r}. The dev DB must never be wiped by tests."
        )


@pytest.fixture
def clean_operators() -> Iterator[None]:
    """Wipe operators + sessions + audit on the isolated test DB.

    Uses TRUNCATE ... CASCADE so any FK-referenced rows are also
    cleared. Every connection here points at ``ebull_test`` and the
    ``_assert_test_db`` guard runs before every TRUNCATE so a future
    refactor cannot regress this back onto the dev DB.
    """
    test_url = _test_database_url()
    with psycopg.connect(test_url) as conn:
        _assert_test_db(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE operators, sessions, operator_audit RESTART IDENTITY CASCADE")
        conn.commit()
    reset_token_slot_for_tests()
    yield
    with psycopg.connect(test_url) as conn:
        _assert_test_db(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE operators, sessions, operator_audit RESTART IDENTITY CASCADE")
        conn.commit()
    reset_token_slot_for_tests()


def test_advisory_lock_serialises_concurrent_setup(clean_operators: None) -> None:
    """A held advisory lock must block perform_setup until released."""
    test_url = _test_database_url()
    blocker = psycopg.connect(test_url)
    blocker.autocommit = False
    with blocker.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (_BOOTSTRAP_LOCK_KEY,))

    result: dict[str, object] = {}

    def worker() -> None:
        worker_conn = psycopg.connect(test_url)
        try:
            outcome, success = perform_setup(
                worker_conn,
                username="alice",
                password="correct horse battery staple",
                submitted_token=None,
                request_client_ip="127.0.0.1",
                user_agent="pytest",
            )
            result["outcome"] = outcome
            result["success"] = success
        finally:
            worker_conn.close()

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    # Worker should be blocked on the advisory lock.
    time.sleep(0.5)
    assert t.is_alive(), "perform_setup did not block on the advisory lock"
    assert "outcome" not in result

    # Release the blocker -- worker proceeds.
    blocker.commit()
    blocker.close()

    t.join(timeout=5.0)
    assert not t.is_alive(), "perform_setup did not finish after lock release"
    assert result["outcome"] is SetupOutcome.OK

    # Second call against the now-populated table must short-circuit.
    with psycopg.connect(test_url) as conn:
        outcome, success = perform_setup(
            conn,
            username="bob",
            password="correct horse battery staple",
            submitted_token=None,
            request_client_ip="127.0.0.1",
            user_agent="pytest",
        )
    assert outcome is SetupOutcome.ALREADY_SETUP
    assert success is None
