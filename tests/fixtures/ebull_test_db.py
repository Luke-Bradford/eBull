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

C1 (#1447 / RCA 2026-06-03): every DB this module touches lives on a
SEPARATE cluster (``postgres-test``, port 5433) resolved via
``_test_cluster_base_url()`` — NOT the dev ``ebull`` cluster (5432).
This is the structural guarantee that a leaked/abandoned test DB can
never bloat ebull's WAL and wedge its crash recovery (the failure that
looped the dev DB for 18h). ``_assert_not_dev_cluster`` fails loud if a
misconfiguration ever re-couples them; the orphan reaper is pinned to
the test cluster via its ``admin_url`` argument. ``_assert_test_db``
still enforces that any destructive op targets an ``ebull_test_*``
database, never ``ebull``.
"""

from __future__ import annotations

import hashlib
import os
import time
import warnings
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from secrets import token_hex
from urllib.parse import urlparse, urlunparse

import psycopg
import psycopg.errors
import psycopg.rows
import pytest
from psycopg import sql

from app.config import settings
from app.db.dev_test_db_reaper import (
    NEVER_DROP as _NEVER_DROP,  # noqa: F401 — re-exported for tests/conftest + test_orphan_sweep
)
from app.db.dev_test_db_reaper import (
    force_drop_invalid_test_dbs as _prod_force_drop_invalid,
)
from app.db.dev_test_db_reaper import (
    sweep_orphan_test_databases as _prod_sweep_orphans,
)

TEMPLATE_DB_NAME = "ebull_test_template"
_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# #1208 Phase 2 / #1444 — the orphan-sweep safety rails (name regex +
# ``_NEVER_DROP`` protect-set) now live in ``app/db/dev_test_db_reaper.py``
# so the jobs-process boot/cadence reaper and this test fixture share ONE
# source of truth. ``_NEVER_DROP`` is re-exported above for the existing
# ``test_orphan_sweep`` + ``conftest`` consumers. ``app`` must not import
# ``tests``, hence the rails live under ``app/`` and the fixture consumes
# them — not the reverse.

# Advisory lock keys. Cross-pytest-invocation locks live on the
# maintenance ``postgres`` DB so they don't collide with application
# advisory locks. Constants are documented for the audit trail —
# application code must not pick keys in this range.
EBULL_TEMPLATE_LOCK = 0x65427554455354  # ASCII "eBuTEST"
EBULL_SMOKE_LIFESPAN_LOCK = 0x65427554534D4B  # ASCII "eBuTSMK"

# Run-id env var. Set once in the controller; xdist propagates env to
# spawned workers automatically.
_RUN_ID_ENV = "EBULL_PYTEST_RUN_ID"

# C1 (#1447 / RCA 2026-06-03): the pytest suite MUST run on a cluster
# separate from the operator's dev ``ebull`` so its WAL can never enter
# ebull's crash recovery (leaked test-DB relations once bloated the shared
# pg_wal and wedged ebull recovery in an 18h OOM loop). The suite's base
# URL is resolved here, NOT from ``settings.database_url`` (the dev DB).
# Default = the dev URL with the port swapped to the dedicated test cluster
# (compose service ``postgres-test``, disk-backed, port 5433); override via
# ``EBULL_TEST_DATABASE_URL`` (e.g. CI). ``_assert_not_dev_cluster`` makes a
# misconfiguration fail loud instead of silently re-coupling the clusters.
_TEST_DB_URL_ENV = "EBULL_TEST_DATABASE_URL"
_TEST_CLUSTER_PORT = os.environ.get("POSTGRES_TEST_PORT", "5433")

# Snapshot the operator's dev database URL AT IMPORT — before any test can
# ``monkeypatch.setattr(settings, "database_url", test_database_url())`` to
# redirect app-under-test code at the per-worker test DB. Both the test-base-URL
# derivation and the C1 dev-cluster guard must reference the REAL dev DB, not a
# live (mutable) ``settings.database_url``: once a test redirects it to the test
# cluster (5433), reading it live makes the guard compute dev==test and mis-fire
# on the SECOND ``test_database_url()`` call (e.g. an autouse redirect fixture
# followed by a ``psycopg.connect(test_database_url())`` in a `conn` fixture —
# the redirect-then-reconnect pattern in test_reaper_split / test_jobs_queue_* /
# test_sync_orchestrator_dispatcher). Module import happens at collection time,
# which always precedes per-test fixtures, so this captures the genuine dev URL.
_DEV_DATABASE_URL: str = settings.database_url


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
    # #554 — dimensional XBRL facts (segments / product / geographic).
    "instrument_dimensional_facts",
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
    "sec_reference_documents",
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
    # #917 — N-PORT mutual-fund holdings ingest (Phase 3 PR1).
    "ownership_funds_current",
    "ownership_funds_observations",
    "n_port_ingest_log",
    "sec_fund_series",
    # #963 — N-PORT RIC trust-CIK directory.
    "sec_nport_filer_directory",
    # #843 — DEF 14A bene-table ESOP plan extraction.
    "ownership_esop_current",
    "ownership_esop_observations",
    # #1233 PR12 — ownership-current writer MERGE rewrite state table.
    # No FK cascade from instruments; must be TRUNCATED explicitly to
    # prevent state rows leaking between tests (Codex 1a HIGH-3).
    "ownership_refresh_state",
    # #893 — dev-DB writers migrated onto worker test DB; tables they
    # touched now need per-test cleanup.
    "job_runtime_heartbeat",
    "pending_job_requests",
    # #993 — first-install bootstrap orchestrator. Truncating
    # ``bootstrap_runs`` cascades to ``bootstrap_stages`` via FK.
    # ``bootstrap_state`` is the singleton row and intentionally
    # NOT FK-linked (see migration 129); test bodies that exercise
    # state transitions are responsible for resetting the singleton
    # back to ``status='pending'`` themselves.
    "bootstrap_runs",
    # #1065 — admin control hub cooperative-cancel signals.
    "process_stop_requests",
    # #1171 — N-CSR / N-CSRS fund-metadata extraction (sql/149).
    "fund_metadata_current",
    "fund_metadata_observations",
    "cik_refresh_mf_directory",
    # G8 — company_tickers_exchange.json snapshot (sql/150).
    "cik_refresh_exchange_directory",
    # G6 / #915 — FINRA bimonthly short interest (sql/152). Phase 6 PR 11.
    "finra_short_interest_current",
    "finra_short_interest_observations",
    # G6 / #916 — FINRA RegSHO daily short volume (sql/154). Phase 6 PR 12.
    "finra_regsho_daily_observations",
)


# #1401 — worker-DB relation-count tripwire ceiling.
#
# The per-worker private DB is cloned from ``ebull_test_template``
# (≈9.6k pg_class rows: tables + indexes + toast + sequences across the
# full migration set) and is REUSED across every test on that worker —
# per-test cleanup is ``TRUNCATE`` only, which wipes rows but never
# drops relations. Any test (or app code under test) that ``CREATE``s a
# table/index/partition without dropping it leaks relations that
# accumulate for the whole session. One such runaway ballooned a worker
# DB past ~2.1M relations and bloated the dev-PG data dir to 13.1M
# files (#1401). 50k gives ~5x headroom over the template baseline so
# legitimate transient relations never trip it, while catching a
# runaway long before it becomes a data-dir disaster. When this fires,
# the FAILING TEST is the (or the first) culprit — bound its relation
# creation and tear it down via a registered finalizer.
_WORKER_DB_RELATION_CEILING = 50_000


def _swap_database(url: str, new_db: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{new_db}"))


def _swap_port(url: str, new_port: str) -> str:
    """Return ``url`` with its port replaced, preserving the ORIGINAL
    (percent-encoded) netloc — userinfo, IPv6 brackets, and all.

    Rebuilding the netloc from ``urlparse``'s ``.username`` / ``.password``
    would corrupt the URL, because those accessors return the percent-DECODED
    values: a password URL-encoded as ``p%40ss`` round-trips to a literal
    ``p@ss`` and the connection breaks (#1448 bot BLOCKING). So splice only
    the port out of the raw netloc string.
    """
    parsed = urlparse(url)
    if parsed.port is None:
        netloc = f"{parsed.netloc}:{new_port}"
    else:
        # When a port is present it is always the final ``:``-segment of the
        # netloc (IPv6 hosts are bracketed, so the only bare trailing colon is
        # the port). rsplit-from-the-right is robust to ``:`` inside userinfo.
        netloc = f"{parsed.netloc.rsplit(':', 1)[0]}:{new_port}"
    return urlunparse(parsed._replace(netloc=netloc))


def _assert_not_dev_cluster(test_base_url: str, dev_url: str | None = None) -> None:
    """Fail loud if the test base URL resolves to the dev ``ebull`` cluster.

    C1 invariant (#1447): the suite must never share a cluster with the
    operator's dev DB. Same (host, port) ⇒ same pg_wal ⇒ a leaked/abandoned
    test DB can wedge ebull's crash recovery. Enforced in code so a stray
    ``EBULL_TEST_DATABASE_URL`` or a future default change can't silently
    re-couple them.

    ``dev_url`` defaults to the import-time dev-URL snapshot
    (``_DEV_DATABASE_URL``), NOT live ``settings.database_url``: a test may
    have already redirected the live value to the test cluster, which would
    make the guard compare the test cluster against itself and mis-fire (the
    redirect-then-reconnect pattern — #1445). The snapshot is the genuine dev
    DB captured before any redirect. Tests inject ``dev_url`` explicitly to
    exercise the comparison without depending on the import-time value.
    """
    dev_url = dev_url if dev_url is not None else _DEV_DATABASE_URL

    def _canon(host: str | None) -> str:
        # Loopback aliases all name the same local cluster — collapse them so
        # localhost:5432 vs 127.0.0.1:5432 vs ::1:5432 can't bypass the guard.
        h = (host or "localhost").lower()
        return "localhost" if h in {"localhost", "127.0.0.1", "::1", "0.0.0.0", ""} else h

    dev = urlparse(dev_url)
    test = urlparse(test_base_url)
    dev_hostport = (_canon(dev.hostname), dev.port or 5432)
    test_hostport = (_canon(test.hostname), test.port or 5432)
    if dev_hostport == test_hostport:
        raise RuntimeError(
            f"Test cluster {test_hostport} == dev cluster {dev_hostport}. The "
            "pytest suite must run on the SEPARATE 'postgres-test' cluster "
            "(port 5433) so its WAL can never enter ebull's crash recovery "
            "(C1 / #1447). Start it:  docker compose --profile test up -d "
            "postgres-test.  Override the URL via EBULL_TEST_DATABASE_URL."
        )


def _test_cluster_base_url() -> str:
    """Base URL for the dedicated pytest cluster (NOT the dev ``ebull`` DB).

    Default derives from the import-time dev-URL snapshot (``_DEV_DATABASE_URL``)
    with the port swapped to the test cluster, so creds/host stay aligned with
    the dev setup while the cluster is physically distinct — and so a test that
    has redirected the live ``settings.database_url`` to the test DB can't skew
    the derivation. Override via ``EBULL_TEST_DATABASE_URL``.
    """
    explicit = os.environ.get(_TEST_DB_URL_ENV)
    base = explicit if explicit else _swap_port(_DEV_DATABASE_URL, _TEST_CLUSTER_PORT)
    _assert_not_dev_cluster(base)
    return base


def _admin_database_url() -> str:
    """URL for the maintenance ``postgres`` DB.

    Used for ``CREATE DATABASE``, ``DROP DATABASE``, and the
    cross-invocation advisory lock. Must never be confused with the
    operator's dev DB.
    """
    return _swap_database(_test_cluster_base_url(), "postgres")


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
    return _swap_database(_test_cluster_base_url(), test_db_name())


test_database_url.__test__ = False  # type: ignore[attr-defined]


def template_database_url() -> str:
    return _swap_database(_test_cluster_base_url(), TEMPLATE_DB_NAME)


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


def _force_drop_invalid_test_dbs() -> list[str]:
    """Force-drop INVALID (``datconnlimit = -2``) test-DB corpses.

    Thin delegate to the canonical reaper in
    ``app/db/dev_test_db_reaper.py`` (#1444 single-source-of-truth).
    A SIGKILL'd worker or a wedged ``DROP ... WITH (FORCE)`` leaves a
    ``datconnlimit = -2`` corpse that refuses ALL new connections — no
    age/activity rail is needed, ``WITH (FORCE)`` is required. Targets
    ``ebull_test_*`` + ``ebull_mig*``; ``_NEVER_DROP`` names skipped.
    Best-effort; returns the dropped names.
    """
    # admin_url pins the reaper to the SEPARATE test cluster (C1 #1447) — the
    # reaper's own default is the dev ``ebull`` cluster (jobs-process context).
    return _prod_force_drop_invalid(admin_url=_admin_database_url())


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


def _drop_orphan_workers_older_than(
    min_age: timedelta = timedelta(hours=1),
    *,
    now: datetime | None = None,
) -> list[str]:
    """Drop stale-named inactive worker DBs (test-session-start path).

    Thin delegate to the canonical reaper in
    ``app/db/dev_test_db_reaper.py`` (#1444). The sweep uses plain
    ``DROP`` (never ``WITH (FORCE)``): it raises ``ObjectInUse``
    (skipped) if a backend reconnected in the Rail-2→DROP gap, rather
    than evicting it — the #1208 concurrent-pytest-safe semantics. The
    three rails (name regex + ``pg_stat_activity`` + age) plus the
    ``_NEVER_DROP`` guard live in the prod module now so the
    jobs-process cadence reaper shares them. Returns dropped names;
    never raises except the Rail-0 ``AssertionError``.
    """
    # admin_url pins the reaper to the SEPARATE test cluster (C1 #1447).
    return _prod_sweep_orphans(min_age, now=now, admin_url=_admin_database_url())


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


def _apply_migrations(target_url: str, *, stop_after: str | None = None) -> None:
    """Apply every ``sql/NNN_*.sql`` file to the target DB.

    Uses a per-file connection so a single transaction-hostile
    migration cannot poison the tracking state of earlier ones. Mirror
    of ``app/db/migrations.run_migrations`` but targeted at an
    arbitrary URL (the test template, not the dev DB).

    ``stop_after`` (#1208 Phase 3): if provided, applies migrations
    only up to and INCLUDING the file with that name (lexicographic
    comparison via ``path.name <= stop_after``). Tests that need to
    exercise a specific migration's swap shape pre-apply 1..N-1 then
    invoke migration N separately.
    """
    files = sorted(_SQL_DIR.glob("*.sql"))
    if stop_after is not None:
        files = [p for p in files if p.name <= stop_after]
    if not files:
        return

    with psycopg.connect(target_url) as bootstrap:
        with psycopg.ClientCursor(bootstrap) as cur:
            # Mirror app/db/migrations.CREATE_TRACKING_TABLE (#1333 —
            # content_sha256 drift guard) so per-worker DBs exercise the
            # normal hashed-applied state, not the legacy-NULL backfill
            # path.
            cur.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "filename TEXT PRIMARY KEY, "
                "applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                "content_sha256 TEXT)"
            )
            cur.execute("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS content_sha256 TEXT")
        bootstrap.commit()

    with psycopg.connect(target_url) as reader:
        with reader.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute("SELECT filename FROM schema_migrations")
            done = {row[0] for row in cur.fetchall()}

    # Imported here (not at module top) so this fixture can be loaded
    # by tooling that doesn't have the full ``app`` package on the path
    # yet — keeps the test-helper import surface narrow.
    from app.db.migrations import _content_sha256, _split_autocommit_statements, _wants_autocommit

    for path in files:
        if path.name in done:
            continue
        sql_text = path.read_text(encoding="utf-8")
        autocommit = _wants_autocommit(sql_text)
        with psycopg.connect(target_url, autocommit=autocommit) as conn:
            try:
                with psycopg.ClientCursor(conn) as cur:
                    if autocommit:
                        # Multi-statement batch under autocommit still
                        # wraps in an implicit tx; split + per-statement.
                        # See app/db/migrations._split_autocommit_statements.
                        for stmt in _split_autocommit_statements(sql_text):
                            cur.execute(stmt)  # type: ignore[call-overload]
                    else:
                        cur.execute(sql_text)  # type: ignore[call-overload]
                    cur.execute(  # type: ignore[call-overload]
                        "INSERT INTO schema_migrations (filename, content_sha256) VALUES (%s, %s)",
                        (path.name, _content_sha256(path)),
                    )
                if not autocommit:
                    conn.commit()
            except Exception:
                if not autocommit:
                    conn.rollback()
                raise


def build_template_if_stale() -> None:
    """Build or rebuild ``ebull_test_template`` under a cluster-wide lock.

    Idempotent: if the migration hash matches the cached value and the
    template exists, this is a no-op (one cheap SELECT). Called from
    the controller-only branch of ``pytest_configure`` in the project
    conftest.

    **Must never be called from an xdist worker.** A worker that
    rebuilds the template would invalidate the per-worker DBs that
    sibling workers have already materialised via ``CREATE DATABASE
    ... TEMPLATE``. Enforced at runtime so the contract is impossible
    to misread (review-bot prevention follow-up).
    """
    if "PYTEST_XDIST_WORKER" in os.environ:
        raise RuntimeError(
            "build_template_if_stale() must run only in the xdist "
            "controller. A worker rebuilding the template would corrupt "
            "sibling workers that have already CREATE-FROM-TEMPLATE'd."
        )

    current = _migration_hash()
    cached = _read_stored_hash()

    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (EBULL_TEMPLATE_LOCK,))
        try:
            # #1208 Phase 2 — sweep orphan worker DBs from prior crashed
            # invocations before any template work. Holds the
            # template-build advisory lock so concurrent pytest
            # controllers serialise sweep+rebuild as a unit. Best-effort;
            # the helper never raises on operational failure.
            _drop_orphan_workers_older_than()

            # #1401 — force-drop INVALID (datconnlimit=-2) corpses the
            # age-gated, plain-DROP sweep above cannot clear. These are
            # the leaked worker/mig DBs that bloated the data dir to
            # 13.1M files. Runs every controller start; cheap (one
            # SELECT, rare drops). Best-effort, never raises.
            _force_drop_invalid_test_dbs()

            template_exists = _ensure_database(admin, TEMPLATE_DB_NAME)
            if template_exists and cached == current:
                return

            if template_exists:
                _drop_database_force(admin, TEMPLATE_DB_NAME)

            _create_empty_database(admin, TEMPLATE_DB_NAME)
            # Apply migrations on a separate connection (we still hold
            # the advisory lock on the postgres DB).
            _apply_migrations(template_database_url())
            # Provision pgstattuple extension (not in migrations — needed
            # by the no-op-churn test case in PR12; #1233 Codex 1a MED-6).
            with psycopg.connect(template_database_url()) as tpl_conn:
                with tpl_conn.cursor() as cur:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
                tpl_conn.commit()
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
                # Unlock on a connection that may be in an error
                # state after a failed DDL; swallow secondary failures
                # so the primary error reaches the caller. Same
                # rationale for the outer lock_key release below.
                # (review-bot 2026-05-05 WARN).
                try:
                    with admin.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s)",
                            (EBULL_TEMPLATE_LOCK,),
                        )
                except Exception:
                    pass
        finally:
            try:
                with admin.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
            except Exception:
                pass


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

    Materialises the per-worker private DB on first call and verifies
    the connection works. **Does not touch the template** — the
    controller's ``pytest_configure`` is the sole template builder
    (review-bot 2026-05-05 BLOCKING: a worker rebuilding the template
    after sibling workers have already CREATE-FROM-TEMPLATE'd
    invalidates their schema).

    Returns False on any failure so the test skips cleanly in
    environments without a Postgres at all. Logs a warning so
    configuration bugs (role lacks CREATEDB privilege, etc.) don't
    hide under the same skip path as "no Postgres".
    """
    try:
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


def _assert_worker_relations_under_ceiling(conn: psycopg.Connection[tuple]) -> None:
    """Fail if the worker DB's relation count exceeds the ceiling.

    Tripwire (#1401): a test that ``CREATE``s relations without
    dropping them leaks into the session-reused worker DB (per-test
    cleanup is ``TRUNCATE`` only — it never drops relations). This
    catches the runaway at the first test that crosses the ceiling
    instead of letting it silently bloat the data dir to millions of
    files. See ``_WORKER_DB_RELATION_CEILING``.
    """
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute("SELECT count(*) FROM pg_class")
        row = cur.fetchone()
    count = int(row[0]) if row and row[0] is not None else 0
    assert count <= _WORKER_DB_RELATION_CEILING, (
        f"TRIPWIRE: worker test DB {test_db_name()!r} holds {count} "
        f"pg_class relations (ceiling {_WORKER_DB_RELATION_CEILING}; "
        f"template baseline ≈9.6k). A test CREATEd relations without "
        f"dropping them — they accumulate across the session because "
        f"per-test cleanup is TRUNCATE only. The failing test is the "
        f"(or first) culprit: bound its relation creation and tear it "
        f"down via a registered finalizer. Do NOT raise this ceiling to "
        f"silence it. See #1401."
    )


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
        # #1444 — creation-time relation budget. The teardown tripwire
        # below is skipped by a ``kill -9`` (OOM / Ctrl-C), which is
        # exactly how a runaway test left ~6-10M-relfile worker DBs that
        # stalled crash recovery for hours (2026-06-02). Asserting at
        # SETUP too means the FIRST surviving test after a skipped
        # teardown fails fast and names the worker DB, bounding the
        # accumulation a single session can reach.
        _assert_worker_relations_under_ceiling(setup_conn)

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
            # #1401 — tripwire on the same conn before close so a
            # relation leak fails THIS test and names the culprit.
            _assert_worker_relations_under_ceiling(conn)
        finally:
            conn.close()


@pytest.fixture(scope="session", autouse=True)
def _worker_db_keepalive() -> Iterator[None]:
    """Hold one autocommit connection to the worker's private DB for the whole session.

    Rail 1 of the orphan-sweep safety model (#1208 Phase 2 spec §4.3).

    Without this fixture, ``ebull_test_conn`` is function-scoped and a
    worker DB has NO backend in ``pg_stat_activity`` between tests. A
    sibling pytest controller's orphan sweep would then see the DB as
    inactive and (if older than ``min_age``) drop it mid-suite. The
    keepalive guarantees the worker DB shows up in
    ``pg_stat_activity`` from session start to session end — the
    activity rail then becomes load-bearing rather than aspirational.

    Skip-silently posture: if ``test_db_available()`` returns False
    (no Postgres, no CREATEDB privilege) we yield without the
    keepalive; tests that need the DB will skip cleanly via the
    existing fixture.

    Must be re-exported from ``tests/conftest.py`` so pytest's
    fixture discovery picks it up (only ``conftest.py`` is scanned,
    not modules under ``tests/fixtures/``).
    """
    if not test_db_available():
        yield
        return
    keepalive: psycopg.Connection[object] | None = None
    try:
        keepalive = psycopg.connect(
            test_database_url(),
            autocommit=True,
            connect_timeout=2,
        )
    except Exception as exc:
        warnings.warn(
            f"Could not open _worker_db_keepalive on {test_db_name()!r}: "
            f"{type(exc).__name__}: {exc}. The orphan-sweep activity "
            f"rail is degraded for this worker.",
            stacklevel=2,
        )
    try:
        yield
    finally:
        if keepalive is not None:
            try:
                keepalive.close()
            except Exception:  # pragma: no cover - best-effort
                pass


__all__ = [
    "EBULL_SMOKE_LIFESPAN_LOCK",
    "EBULL_TEMPLATE_LOCK",
    "TEMPLATE_DB_NAME",
    "_drop_orphan_workers_older_than",
    "_force_drop_invalid_test_dbs",
    "_worker_db_keepalive",
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
