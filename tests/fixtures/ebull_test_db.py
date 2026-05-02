"""Reusable helpers for tests that need a real ``ebull_test`` Postgres.

Extracted from ``tests/test_operator_setup_race.py`` so future tasks
(SEC incremental planner + executor, integration tests) can import a
single canonical implementation rather than re-inlining the connect /
create / migrate / guard dance.

The strategy mirrors ``test_operator_setup_race.py`` exactly:

1. Derive an isolated ``ebull_test`` URL from ``settings.database_url``
   (same host, same credentials, different database name).
2. Create the database on demand (admin connection against the
   maintenance ``postgres`` DB, autocommit — ``CREATE DATABASE`` cannot
   run inside a transaction).
3. Apply every ``sql/NNN_*.sql`` migration file to the test DB.
4. Expose a pytest fixture ``ebull_test_conn`` that yields a fresh
   ``psycopg.Connection[tuple]`` and TRUNCATEs the planner/executor
   tables after each test.
5. Guard every destructive op with ``_assert_test_db`` so a future
   refactor cannot regress the TRUNCATE onto the dev DB.

``tests/test_operator_setup_race.py`` deliberately keeps its own
inlined copies; the migration of that file to this module is a
follow-up PR, not part of this task's scope.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
import psycopg.rows
import pytest
from psycopg import sql

from app.config import settings

TEST_DB_NAME = "ebull_test"
_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# Tables touched by the SEC incremental planner + executor tests.
# TRUNCATE in dependency order with CASCADE to handle FKs cleanly.
_PLANNER_TABLES: tuple[str, ...] = (
    "cascade_retry_queue",
    "cik_upsert_timing",  # #418 per-CIK timing audit (FK → data_ingestion_runs)
    "financial_facts_raw",
    "sec_facts_concept_catalog",  # #451 — per-concept metadata
    "sec_entity_change_log",  # #463 — entity-field change events
    "data_ingestion_runs",
    # layer_enabled is the home of the #414 fundamentals_ingest pause
    # flag and is written by several observability/planner tests. Keep
    # it in the planner truncation set so a failed test cannot leak
    # a disabled state across the next function-scoped run and
    # silently make subsequent "not paused" assertions trip.
    # layer_enabled_audit is the #346 append-only history table — also
    # truncated so audit-row leakage between cases doesn't make a
    # later assertion observe a phantom prior toggle.
    "layer_enabled_audit",
    "layer_enabled",
    "external_identifiers",
    "external_data_watermarks",
    "coverage_status_events",  # #397 transition log (child of coverage)
    "coverage",  # #397 truncate needed to reset trigger-driven state cleanly
    "position_alerts",  # #396 position-alert episodes
    "watchlist",  # #042 — FK → instruments
    "broker_positions",  # #024 — FK → instruments
    "positions",  # #186 — FK → instruments; truncated so the upsert
    # reset-on-reopen integration tests don't see stale state from
    # earlier in the test run.
    "quotes",  # #002 — FK → instruments (live-tick target #471)
    "instruments",
    "job_runs",
    "financial_periods_raw",
    "financial_periods",
    "dividend_events",  # #434 — 8-K 8.01 calendar, FK → instruments
    # #450 8-K structured-event tables. Children → parent:
    # items + exhibits FK into eight_k_filings; eight_k_filings FKs
    # into instruments (so the instrument truncation below would
    # cascade, but listing them explicitly keeps teardown deterministic
    # when a test populates a filings-only row without touching
    # instruments).
    "eight_k_exhibits",
    "eight_k_items",
    "eight_k_filings",
    "filing_documents",  # #452 — child of filing_events
    "instrument_business_summary_sections",  # #449 — FK → instruments
    "instrument_business_summary",  # #428 — 10-K Item 1 body, FK → instruments
    "instrument_sec_profile",  # #427 — SEC entity profile, FK → instruments
    # #429 Form 4 tables. Child-to-parent truncation order: transactions
    # and footnotes FK into filings; filers also FK into filings;
    # filings FKs into instruments (so instrument truncation further
    # down would already cascade, but listing them explicitly keeps
    # teardown deterministic when a test populates filings-only rows
    # without touching instruments).
    "insider_transaction_footnotes",
    "insider_transactions",
    "insider_initial_holdings",  # #768 — Form 3 baseline, FK → instruments
    "insider_filers",
    "insider_filings",
    # #730 — 13F-HR institutional holdings. Child-to-parent:
    # institutional_holdings FKs into institutional_filers AND
    # instruments (so the instrument truncation further down would
    # cascade), but listing them explicitly keeps teardown
    # deterministic when a test populates filer / holding rows
    # without touching the instruments row in the same case.
    # #781 — unresolved 13F CUSIPs tracking. No FK to instruments
    # (PK is the CUSIP) so it's safe in any order, but listed
    # alongside the institutional set for cohesion.
    "unresolved_13f_cusips",
    "institutional_holdings_ingest_log",
    "institutional_holdings",
    "institutional_filers",
    "institutional_filer_seeds",
    "etf_filer_cik_seeds",
    # #766 — 13D/G blockholders. Child-to-parent: blockholder_filings
    # FKs into blockholder_filers AND instruments. The instrument row
    # truncation further down would cascade, but listing them
    # explicitly keeps teardown deterministic when a test populates
    # filer / filing rows without touching the instruments row in the
    # same case.
    "blockholder_filings_ingest_log",
    "blockholder_filings",
    "blockholder_filers",
    "blockholder_filer_seeds",
    # #769 — DEF 14A beneficial ownership cross-check. FK → instruments
    # (nullable). Listing explicitly keeps teardown deterministic when
    # a test populates DEF 14A rows without touching instruments.
    "def14a_drift_alerts",
    "def14a_ingest_log",
    "def14a_beneficial_holdings",
    "filing_events",
    "decision_audit",  # #315 Phase 3 alerts
    "trade_recommendations",  # #315 Phase 3 alerts (FK parent of decision_audit)
    "operators",  # #315 Phase 3 alerts (cursor column)
)


def _swap_database(url: str, new_db: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{new_db}"))


def test_database_url() -> str:
    return _swap_database(settings.database_url, TEST_DB_NAME)


def _admin_database_url() -> str:
    # Admin connection used only to ``CREATE DATABASE``. Connect to
    # the built-in ``postgres`` maintenance DB so we never hold a
    # session on the database we are about to create.
    return _swap_database(settings.database_url, "postgres")


def ensure_test_db_exists() -> None:
    """Create ``ebull_test`` if it does not yet exist.

    ``CREATE DATABASE`` cannot run inside a transaction, so the admin
    connection is opened in autocommit mode. Idempotent: if the
    database already exists we return without touching it.
    """
    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB_NAME,))
            if cur.fetchone() is None:
                # ``TEST_DB_NAME`` is a hard-coded constant, never user
                # input, so SQL injection is not a concern. The
                # double-quoting guards against a future rename that
                # introduces an unusual character.
                cur.execute(f'CREATE DATABASE "{TEST_DB_NAME}"')


def apply_migrations_to_test_db() -> None:
    """Apply every ``sql/NNN_*.sql`` file to the test DB.

    Mirrors ``app/db/migrations.run_migrations`` but targets the test
    DB URL directly. Uses a per-file connection so a single
    transaction-hostile migration cannot poison the tracking state of
    earlier migrations.
    """
    files = sorted(_SQL_DIR.glob("*.sql"))
    if not files:
        return

    # Bootstrap: schema_migrations exists and is committed before any
    # migration file runs.
    with psycopg.connect(test_database_url()) as bootstrap:
        with psycopg.ClientCursor(bootstrap) as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "filename TEXT PRIMARY KEY, "
                "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
            )
        bootstrap.commit()

    # Reader: fetch the applied set on its own connection so the
    # per-file connections below see a consistent committed view.
    with psycopg.connect(test_database_url()) as reader:
        with reader.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute("SELECT filename FROM schema_migrations")
            done = {row[0] for row in cur.fetchall()}

    for path in files:
        if path.name in done:
            continue
        sql_text = path.read_text(encoding="utf-8")
        with psycopg.connect(test_database_url()) as conn:
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


def test_db_available() -> bool:
    """Probe (and lazily create + migrate) the test DB.

    Returns False on any failure so the test skips cleanly in
    environments without a Postgres at all. Logs a warning with the
    underlying exception — a bare ``except Exception: return False``
    hides configuration bugs (e.g. the configured role lacks
    CREATEDB privilege) under the same log message as "no Postgres".
    """
    try:
        ensure_test_db_exists()
        apply_migrations_to_test_db()
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


def _assert_test_db(conn: psycopg.Connection[object]) -> None:
    """Refuse to run a destructive op against anything but ``ebull_test``.

    Paranoid backstop: if a future refactor accidentally passes a
    connection to ``settings.database_url`` (the dev DB) into a
    cleanup fixture, this guard fails the test loudly instead of
    silently TRUNCATing the user's working state.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
        assert row is not None
        db_name = row[0]
    if db_name != TEST_DB_NAME:
        raise RuntimeError(
            f"Refusing to TRUNCATE: connected to database {db_name!r}, "
            f"expected {TEST_DB_NAME!r}. The dev DB must never be wiped by tests."
        )


def _truncate_planner_tables(conn: psycopg.Connection[tuple]) -> None:
    _assert_test_db(conn)
    # TRUNCATE cannot parameterise table names, so compose via
    # ``psycopg.sql.Identifier`` which quotes each name safely. The
    # table list is a module-level constant — never user input — but
    # composition is still the right habit.
    query = sql.SQL("TRUNCATE {tables} RESTART IDENTITY CASCADE").format(
        tables=sql.SQL(", ").join(sql.Identifier(t) for t in _PLANNER_TABLES),
    )
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


@pytest.fixture
def ebull_test_conn() -> Iterator[psycopg.Connection[tuple]]:
    """Yield a fresh ``ebull_test`` connection, TRUNCATE before + after.

    Function-scoped so each test starts with a clean slate and no test
    leaks state into the next. Both the pre-yield and post-yield
    TRUNCATE go through ``_assert_test_db`` so the dev DB can never be
    wiped by a misconfigured connection.
    """
    url = test_database_url()
    with psycopg.connect(url) as setup_conn:
        _truncate_planner_tables(setup_conn)

    conn = psycopg.connect(url)
    try:
        yield conn
    finally:
        # Roll back any in-flight transaction before we hand the
        # connection to the TRUNCATE path — TRUNCATE on an aborted
        # transaction would itself fail with ``current transaction is
        # aborted``.
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            _truncate_planner_tables(conn)
        finally:
            conn.close()
