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
3. The fixture's per-test cleanup (``_reset_planner_tables``, #1568)
   runs against the worker's private DB. Cross-worker contention is
   impossible because each worker owns its DB.
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
import json
import os
import time
import warnings
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from secrets import token_hex
from typing import Any
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

#: Repo root of the checkout that invoked pytest. Stamped into the template
#: alongside the migration hash so a mismatch can NAME the sibling worktree that
#: last rebuilt it, rather than reporting an anonymous schema drift (#2342).
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SQL_DIR = _REPO_ROOT / "sql"

#: The §4.0 validated-universe anchor. `etoro_instrument_types` is created empty
#: by `sql/070` and filled by the nightly eToro universe sync, so a test DB never
#: has it, and since #2809 anything reading the scan freshness bar through
#: `load_validated_universe` REFUSES on zero rows.
#:
#: ⚠ ONE source of truth (#2859). This literal was copied into three test modules
#: — `test_validated_universe`, `test_api_strategies` and `test_strategy_monitoring`
#: — so a change to the anchor id would have had to be found by grepping the VALUE.
#: The review bot flagged one of the three; the other two were only visible by
#: grepping `5`, which is the recurring shape behind this repo's "extract once"
#: rule.
#:
#: ⚠ `tests/test_validated_universe.py` deliberately does NOT import this and
#: keeps its own literal. It is the test OF `resolve_stocks_type_id`, so sharing
#: the constant would let fixture and assertion drift together — the one place
#: where duplicating the value is the point.
STOCKS_TYPE_ID = 5


def seed_universe_anchor(conn: psycopg.Connection[Any]) -> None:
    """Insert the §4.0 `Stocks` anchor and COMMIT it.

    ⚠ The commit is load-bearing. `update_strategy_paper_pool` calls
    `conn.rollback()` before opening its own transaction, so an uncommitted
    anchor is discarded and the overview it returns at the end raises again.

    Deliberately only the ANCHOR — seeding instruments too would hand a caller a
    non-empty validated universe it never asked for.
    """
    from app.services.strategies.validated_universe import STOCKS_TYPE_DESCRIPTION

    conn.execute(
        """
        INSERT INTO etoro_instrument_types (instrument_type_id, description)
        VALUES (%(stocks)s, %(description)s)
        ON CONFLICT (instrument_type_id) DO NOTHING
        """,
        {"stocks": STOCKS_TYPE_ID, "description": STOCKS_TYPE_DESCRIPTION},
    )
    conn.commit()


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


# ROOTS of the per-test wipe set. The set actually emptied is this list plus
# everything reachable from it through inbound FKs — 450 tables today. That
# closure is DERIVED from ``pg_constraint`` at session start (see
# ``_build_cleanup_plan``, #1568), so a migration that adds an FK CHILD of a
# table already listed here needs no edit: it is picked up automatically, and
# the delete order with it.
#
# What still MUST be appended here in the same PR is a table with NO inbound-FK
# path from an existing entry — a standalone table, or one whose "link" to
# instruments is a bare BIGINT rather than a real FK (``copy_mirror_positions``,
# ``fx_rates_daily`` below). Nothing can derive those, so rows leak across tests
# exactly as the review-prevention-log entry "Test-teardown list missing new
# FK-child tables" describes.
#
# Ordering within this tuple is now irrelevant — the delete order is computed
# topologically. It is kept child-to-parent only because that also happens to
# be a safe TRUNCATE order for the fallback path.
_PLANNER_TABLES: tuple[str, ...] = (
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
    # #1593 — trade ledger + closed-position archive (FK → instruments).
    "trade_events",
    "broker_positions_closed",
    "broker_positions",
    # #1594 — EOD equity snapshots (child → parent; parent → instruments).
    "portfolio_eod_position_snapshots",
    "portfolio_eod_snapshots",
    # #2559 — one compact official broker-equity row per environment/day.
    # Standalone by design: account evidence is not tied to an instrument.
    "broker_account_equity_snapshots",
    # #1594 — dated FX (standalone, no FK). Listed so DB tests inserting FX
    # rows don't leak across tests (Codex ckpt-3).
    "fx_rates_daily",
    # #2240 phase 5c (sql/262) — backtest result provenance. STANDALONE: it
    # carries an instrument COUNT rather than instrument ids (spec §6 — the set
    # is thousands per row and the promotion gate compares it against a
    # freshly-loaded universe), so it has no FK to `instruments` and nothing
    # derives it from `pg_constraint`. ⚠ The decision that keeps the row narrow
    # is the same one that makes it invisible to the cleanup planner; without
    # this line `tests/test_strategy_results_table.py`'s committed rows leak
    # and collide on `strategy_results_unique` under a reordered run.
    # ⚠ `strategy_signals` / `strategy_outcomes` are NOT listed and must not be:
    # both have an inbound-FK path (→ instruments, → strategy_signals) and are
    # picked up automatically.
    #
    # ⚠⚠ #2240 phase 5e-1 (sql/264) RENAMED the storage. `strategy_results` is
    # now a VIEW (in-sample only, criterion 5), and naming a view here would
    # wipe half the rows while reporting success — the hold-out half survives
    # and collides on the next test. The STORE is the relation to truncate.
    "strategy_results_store",
    # #2240 phase 5e-1 (sql/264) — criterion 5's access log. Also STANDALONE, and
    # for a reason worth stating: an access may name a result_version that no row
    # carries yet (the record is written BEFORE the row it authorises), so an FK
    # would refuse the exact ordering the trigger requires.
    "strategy_holdout_accesses",
    # #2611 (sql/340) — refused outcome-access attempts. STANDALONE for the same
    # reason its writer needs it to be: no FK to the declaration, because the
    # audit row is written from a SECOND connection that cannot see a
    # declaration the caller froze in its own open transaction.
    # ⚠⚠ LISTED BECAUSE IT IS THE ONE TABLE A DB TEST CANNOT ROLL BACK. Every
    # other row a test writes dies with the fixture's transaction; this one
    # commits on its own connection by design, so without this line a refusal
    # from one test is still there for the next one to count.
    "strategy_holdout_access_refusals",
    # #2454 — governance roots.  Their children are discovered through inbound
    # FKs, but neither root has an FK path from instruments.  Keeping them here
    # prevents a promotion/deployment in one DB test becoming another test's
    # current operator decision.
    "strategy_promotions",
    "strategy_deployments",
    # #2450 — immutable preregistered live threshold root. Drill and
    # assessment children are discovered through their FKs.
    "strategy_live_gate_policies",
    # #2599 (sql/333) — the frozen preregistration declaration. ⚠ LISTED
    # BECAUSE IT IS A PARENT, NOT A CHILD: `strategy_live_gate_policies`
    # references IT, so the inbound-FK closure walking down from the roots
    # above never reaches it. Measured, not assumed — two new live-gate tests
    # failed with `DID NOT RAISE` and a UniqueViolation on a declaration a
    # previous test had frozen. Listed AFTER its own child so the fallback
    # TRUNCATE order stays child-to-parent.
    "strategy_preregistration_declarations",
    # #2451 — bounded current kill state has no FK by design.
    "strategy_execution_blocks",
    # #2469 — the shared paper-pool current state is an append-only standalone
    # event stream. It intentionally has no FK to a deployment, so the inbound
    # FK closure cannot discover it from the strategy roots above.
    "strategy_paper_pool_events",
    # #2545 — calibration evidence is an immutable standalone root. Forecasts
    # reference both it and signals, so those children are derived; the parent
    # itself cannot be discovered from an existing inbound-FK root.
    "strategy_forecast_calibrations",
    # #2555 — immutable prospective-assessment policy is a standalone root;
    # assessment evidence and bounded current pointers are FK descendants.
    "strategy_forecast_assessment_policies",
    # #2553 — the forecast outcome round-robin cursor is standalone. Outcome
    # rows are discovered through their FK to forecasts/signals.
    "strategy_forecast_outcome_cursor",
    # #2448/#2449 — bounded strategy current-state roots have no FKs. Their
    # signal/deployment children are derived by the planner from roots above.
    "strategy_scan_watermark",
    "strategy_halt_feed_state",
    "strategy_market_halts",
    "strategy_paper_account_risk_state",
    "positions",
    "quotes",
    # #1919 — thesis generation attempts (FK → instruments + theses).
    "thesis_runs",
    # #2002 — calibration-ledger realized outcomes (FK → theses).
    "thesis_outcomes",
    "instruments",
    "job_runs",
    # #1508 C6 / migration 185 — per-job first-seen anchor. Standalone: its only
    # constraint is the PK on job_name, so no inbound-FK path reaches it and the
    # derived closure cannot pick it up. Omitted when it shipped, so anchors
    # leaked between tests: tests/test_job_first_seen.py's "no anchor row" case
    # read whichever anchor the previously-run test in that file had committed
    # and flipped never_started with it. Presented as an xdist flake (#2212) —
    # it is order-dependence, and reproduces every time when the 7-day-anchor
    # test runs immediately before it.
    "job_first_seen",
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
    "def14a_exec_compensation",
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
    # #1602 — tables the v2 report reads that were never truncated, so
    # rows leaked across tests sharing a worker DB and corrupted the
    # report cover under xdist colocation. None are reachable from an
    # existing CASCADE chain, so each MUST be listed explicitly.
    #   AUM = Σ position market_value + cash_balance + mirror_equity
    #         (app/services/valuation.py). cash_ledger feeds cash_balance;
    #   the copy_* cluster feeds mirror_equity
    #         (app/services/portfolio.py::load_mirror_breakdowns) — a
    #   leaked mirror inflated opening/closing_value (2100 → 3600).
    # capital_events feeds the flow-adjusted return; report_snapshots
    # feeds `_prior_v2_chain` across reporting tests (a leaked prior
    # breaks first-snapshot parity). Same class as the "Test-teardown
    # list missing new FK-child tables" prevention entry.
    # copy_* listed child→parent: copy_mirror_positions FK→copy_mirrors
    # FK→copy_traders (copy_mirror_positions.instrument_id is a bare
    # BIGINT, not an FK, so instruments' CASCADE never reaches it).
    # copy_mirror_closed_positions (#1927) has NO FK to the copy cluster
    # (bare LIKE) → a CASCADE truncate of copy_mirror_positions never
    # reaches it; listed explicitly so archive rows do not leak.
    "copy_mirror_closed_positions",
    "copy_mirror_positions",
    "copy_mirrors",
    "copy_traders",
    "cash_ledger",
    "capital_events",
    "report_snapshots",
)

# DELETE intentionally cannot remove these immutable audit children. Test DB
# cleanup is allowed to empty them, but must do so explicitly before deleting
# their parents; otherwise every test that stores real promotion evidence falls
# through to the much slower whole-schema TRUNCATE recovery path (#2737).
_TRUNCATE_BEFORE_DELETE: frozenset[str] = frozenset({"strategy_result_universe"})


# #1401 — worker-DB relation-count tripwire ceiling.
#
# The per-worker private DB is cloned from ``ebull_test_template``
# (≈9.6k pg_class rows: tables + indexes + toast + sequences across the
# full migration set) and is REUSED across every test on that worker —
# per-test cleanup wipes rows but never drops relations. Any test (or
# app code under test) that ``CREATE``s a
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


def _read_template_stamp(admin: psycopg.Connection[Any]) -> tuple[str, str] | None:
    """``(migration_hash, built_from)`` recorded on the template, or ``None``.

    The stamp lives in the template's DATABASE COMMENT, not in a file. That
    matters for two measured reasons (#2342):

    * A machine-global cache file under ``user_cache_dir("ebull")`` is shared by
      every worktree on the box, so a sibling loop with different pending
      migrations wrote ITS hash and the next run here saw a match and SKIPPED
      the rebuild — testing against a schema without its own migration. The
      failure is silent and directional: tests assert the OLD behaviour and fail,
      which reads exactly like "your migration is wrong".
    * ``pg_shdescription`` is a shared catalog keyed by database OID, so
      ``CREATE DATABASE ... TEMPLATE`` does NOT copy the comment. Verified
      against the test cluster before adopting it — every per-worker clone comes
      out with a NULL stamp, so this leaves no residue in the DBs tests run on.
    """
    with admin.cursor() as cur:
        cur.execute(
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = %s",
            (TEMPLATE_DB_NAME,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    try:
        stamp = json.loads(row[0])
        return str(stamp["migration_hash"]), str(stamp["built_from"])
    except ValueError, KeyError, TypeError:
        # A comment we did not write (or an older format) means "unknown", which
        # forces a rebuild. Never treat an unparseable stamp as a match.
        return None


def _write_template_stamp(admin: psycopg.Connection[Any], migration_hash: str) -> None:
    """Record this worktree's migration hash on the template we just built.

    ``COMMENT ON DATABASE`` takes no bound parameters, so the value is rendered
    as a ``sql.Literal``. It runs on the admin connection (a different database),
    which is permitted and was verified against the cluster.
    """
    payload = json.dumps({"migration_hash": migration_hash, "built_from": str(_REPO_ROOT)})
    with admin.cursor() as cur:
        cur.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(sql.Identifier(TEMPLATE_DB_NAME), sql.Literal(payload))
        )


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
            # Read the stamp INSIDE the lock and after the existence check: a
            # sibling worktree may have rebuilt the template since this process
            # started, and its hash is the only thing that says whose schema the
            # template currently holds.
            if template_exists:
                stamp = _read_template_stamp(admin)
                if stamp is not None and stamp[0] == current:
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
            _write_template_stamp(admin, current)
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


def _assert_holds_template_lock(admin: psycopg.Connection[Any]) -> None:
    """Fail if this session does not hold ``EBULL_TEMPLATE_LOCK``.

    The stamp check below is a read-then-act, so without the lock a sibling can
    drop and rebuild the template between the read and the CREATE — the guard
    would pass on a stamp that no longer describes the template being copied.
    The precondition was documented but unenforced (review bot), which is worth
    a query here specifically because the failure it admits is SILENT.

    A bigint advisory key is stored split across ``classid`` (high 32 bits) and
    ``objid`` (low 32). Verified against the cluster: the reconstruction below
    returns EBULL_TEMPLATE_LOCK exactly while held, and no row after unlock.
    """
    with admin.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_locks
             WHERE locktype = 'advisory'
               AND granted
               AND pid = pg_backend_pid()
               AND ((classid::bigint << 32) | objid::bigint) = %s
            """,
            (EBULL_TEMPLATE_LOCK,),
        )
        if cur.fetchone() is None:
            raise RuntimeError(
                "_assert_template_matches_this_worktree() ran without holding "
                "EBULL_TEMPLATE_LOCK on this connection. The stamp it reads could "
                "then be replaced before the template is copied, which is the "
                "silent wrong-schema clone this check exists to prevent."
            )


class TemplateWorktreeMismatch(RuntimeError):
    """The template on the cluster was built from a different checkout's ``sql/``.

    A distinct type, not a bare ``RuntimeError``, because ``test_db_available``
    catches ``Exception`` and converts it into a warning + skip. Swallowed there,
    this condition would silently skip the whole db tier and let the run pass —
    which is #2342's defect wearing a different hat. It is re-raised by name.
    """


def _assert_template_matches_this_worktree(admin: psycopg.Connection[Any]) -> None:
    """Refuse to clone a template built from a different checkout's ``sql/``.

    ``build_template_if_stale`` runs in the controller and RELEASES the template
    lock before the workers clone, so a sibling pytest controller in another
    worktree can rebuild the template in that gap. This is the only window the
    lock does not cover, and without this check it is silent: the clone succeeds
    and the tests assert against the sibling's schema.

    Deliberately raises rather than rebuilding — a worker must never rebuild the
    template (it would invalidate the DBs sibling workers already cloned), which
    ``build_template_if_stale`` enforces at its own entry.

    Must be called while holding ``EBULL_TEMPLATE_LOCK``.
    """
    _assert_holds_template_lock(admin)
    stamp = _read_template_stamp(admin)
    current = _migration_hash()
    if stamp is not None and stamp[0] == current:
        return
    built_from = "an unknown checkout (no readable stamp)" if stamp is None else stamp[1]
    raise TemplateWorktreeMismatch(
        f"{TEMPLATE_DB_NAME!r} was built from {built_from}, whose migrations differ "
        f"from this checkout's ({_REPO_ROOT}). Cloning it would run these tests "
        "against the wrong schema, which fails as if the migration under test were "
        "wrong. A sibling pytest run rebuilt the template after this one started — "
        "re-run pytest, and avoid running the db tier in two worktrees at once."
    )


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
                _assert_template_matches_this_worktree(admin)
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
    # The DB is about to stop existing, so nothing may go on believing it is
    # available or holding a cleanup plan derived from its catalog (#1568).
    _TEST_DB_AVAILABLE.discard(db_name)
    _CLEANUP_PLANS.pop(db_name, None)
    _close_janitor_conn()
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


# Worker DBs whose availability has already been established. A SUCCESS is
# memoised because re-answering it costs ~10 ms per test (admin connect, two
# advisory-lock round trips, a second connect and a probe query) to re-derive
# something that cannot change: the ``_worker_db_keepalive`` session fixture
# holds a backend open for the whole run precisely so a sibling controller's
# orphan sweep sees the DB as active and leaves it alone. A FAILURE is never
# memoised — a cluster that was not up yet may come up (#1568).
_TEST_DB_AVAILABLE: set[str] = set()


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
    if test_db_name() in _TEST_DB_AVAILABLE:
        return True
    try:
        ensure_worker_database()
        with psycopg.connect(test_database_url(), connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        _TEST_DB_AVAILABLE.add(test_db_name())
        return True
    except TemplateWorktreeMismatch:
        # ⚠ NOT "unavailable". The cluster is fine and the template is fine —
        # it just belongs to another checkout. Letting this fall into the skip
        # path below would reinstate #2342's actual defect one layer up: the db
        # tier would be silently skipped and the run would go GREEN having
        # exercised no database at all. Must stay fatal (Codex checkpoint 2).
        raise
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
    """Truncate the planner table set — the FALLBACK cleanup path.

    ``_reset_planner_tables`` is the normal path (see #1568); this runs
    only when the probe/DELETE plan cannot be built or a DELETE hits a
    constraint the cached plan did not know about (a test that CREATEd
    its own FK-bearing table, say). Correct but slow: TRUNCATE takes
    ACCESS EXCLUSIVE and rewrites a relfilenode per relation whether or
    not the table holds rows, measured at ~845 ms per call on the
    Docker-Desktop-macOS test cluster.

    Issued as one statement. The chunking this function used to do cited
    ``max_locks_per_transaction`` "(default 64)", but the test cluster is
    configured at 1024 (``docker-compose.yml`` ``postgres-test``), and
    CASCADE reaches 450 relations — under the real ceiling, and one shot
    measured ~20% faster than six chunks (#1568).
    """
    assert_test_db(conn)
    with conn.cursor() as cur:
        query = sql.SQL("TRUNCATE {tables} RESTART IDENTITY CASCADE").format(
            tables=sql.SQL(", ").join(sql.Identifier(t) for t in _PLANNER_TABLES),
        )
        cur.execute(query)
    conn.commit()


# ---------------------------------------------------------------------------
# #1568 — per-test cleanup by probe + FK-topological DELETE.
#
# The old lifecycle TRUNCATEd the whole planner set twice per test (setup +
# teardown) at ~845 ms a pass, ~1.7 s/test, ~99% of the db tier's wall-clock —
# and it did that against tables that are almost always ALREADY EMPTY, because
# TRUNCATE's cost is per-RELATION (ACCESS EXCLUSIVE + relfilenode rewrite), not
# per-row.
#
# The replacement asks Postgres which tables actually hold rows and deletes only
# those. Measured on the test cluster: probe 3.9 ms, whole cleanup 4.1 ms on a
# clean DB / 7.2 ms after a test that wrote 3 rows — vs 845 ms. Rollback-based
# isolation (the usual answer) is NOT available here: the app makes 271 explicit
# ``.commit()`` calls across 71 files, which would break any enclosing
# transaction.
#
# Three things make it exact rather than approximate:
#
# 1. **The wipe set is DERIVED, not re-listed.** ``TRUNCATE ... CASCADE`` over
#    ``_PLANNER_TABLES`` today reaches 450 relations — the 105 listed plus 345
#    pulled in through inbound FKs. DELETE has no CASCADE, so the closure is
#    computed from ``pg_constraint`` at session start. Same wipe set as before,
#    with no second hand-maintained list to drift.
#
# 2. **Partitions collapse to their root.** ``public`` holds 1,478 tables but
#    only 167 non-partition roots; the rest are partitions of 12 parents.
#    ``EXISTS (SELECT 1 FROM parent)`` already short-circuits across every
#    partition, so probing partitions individually is 18x the planning cost for
#    the same answer (69.8 ms vs 3.9 ms measured). ``DELETE FROM parent``
#    likewise reaches every partition.
#
# 3. **Sequences are probed separately from rows.** DELETE has no RESTART
#    IDENTITY. A ROLLED-BACK insert advances a sequence permanently while
#    leaving the table empty (nextval is non-transactional — verified on the
#    test cluster), so a row-only probe would miss it and the next test would
#    see ids starting at 2. ``pg_sequences.last_value IS NOT NULL`` is exactly
#    "has been read since the last RESTART" and costs 0.8 ms.
#
# Any failure falls back to ``_truncate_planner_tables`` and invalidates the
# cached plan, so a test that creates its own FK-bearing table degrades to the
# old (correct, slow) path for one test instead of erroring.
# ---------------------------------------------------------------------------


class _CleanupPlan:
    """Session-cached derivation of what per-test cleanup must touch."""

    __slots__ = ("delete_order", "owned_sequences", "probe_sql")

    def __init__(
        self,
        delete_order: tuple[str, ...],
        probe_sql: sql.Composed,
        owned_sequences: frozenset[str],
    ) -> None:
        self.delete_order = delete_order
        self.probe_sql = probe_sql
        self.owned_sequences = owned_sequences


# Keyed by database name: one worker process only ever talks to one test DB,
# but keying makes a stale plan impossible if that ever stops being true.
_CLEANUP_PLANS: dict[str, _CleanupPlan] = {}

# Catalog queries. Every one resolves names via ``pg_class.relname`` rather than
# ``::regclass::text``: regclass rendering schema-qualifies and double-quotes
# whatever the current ``search_path`` requires, and those decorated strings
# would then be re-quoted by ``sql.Identifier`` into a name that matches nothing.
# ``pg_partition_root`` returns NULL for a non-partition, hence the COALESCE.

# Every ordinary/partitioned table in ``public``, collapsed to its partition root.
_ROOT_TABLES_SQL = """
SELECT DISTINCT root.relname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class root ON root.oid = COALESCE(pg_partition_root(c.oid), c.oid)
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
"""

# FK edges as (referencing_root, referenced_root). Both sides collapsed to the
# partition root so a constraint declared on a partition orders its parent.
_FK_EDGES_SQL = """
SELECT child.relname, parent.relname
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
JOIN pg_class child ON child.oid = COALESCE(pg_partition_root(c.conrelid), c.conrelid)
JOIN pg_class parent ON parent.oid = COALESCE(pg_partition_root(c.confrelid), c.confrelid)
WHERE c.contype = 'f' AND n.nspname = 'public'
"""

# Sequences owned by a table column (serial / GENERATED AS IDENTITY). These are
# precisely the ones ``TRUNCATE ... RESTART IDENTITY`` would have reset, so
# restricting to them keeps the new path's sequence behaviour identical.
# ``s.relname`` is the same string ``pg_sequences.sequencename`` reports.
_OWNED_SEQUENCES_SQL = """
SELECT s.relname, owner_root.relname
FROM pg_class s
JOIN pg_namespace n ON n.oid = s.relnamespace
JOIN pg_depend d
  ON d.classid = 'pg_class'::regclass
 AND d.objid = s.oid
 AND d.refclassid = 'pg_class'::regclass
 AND d.deptype IN ('a', 'i')
JOIN pg_class owner ON owner.oid = d.refobjid
JOIN pg_class owner_root ON owner_root.oid = COALESCE(pg_partition_root(owner.oid), owner.oid)
WHERE s.relkind = 'S' AND n.nspname = 'public'
"""


def _topological_delete_order(
    tables: set[str],
    referencing: dict[str, set[str]],
) -> tuple[str, ...]:
    """Order ``tables`` so every table precedes the tables it references.

    DELETE has no CASCADE, so a parent may only be emptied once nothing that
    references it still holds rows — children first, parents last. Kahn's
    algorithm over the reversed FK graph, restricted to ``tables``.

    Self-references are ignored (a row referencing its own table is removed by
    the same DELETE). A genuine multi-table FK cycle cannot be ordered; those
    tables are appended in a stable order and, if a DELETE among them then
    violates a constraint, the caller's fallback TRUNCATEs instead.
    """
    remaining = set(tables)
    order: list[str] = []
    while remaining:
        ready = sorted(
            t for t in remaining if all(child not in remaining for child in referencing.get(t, ()) if child != t)
        )
        if not ready:  # pragma: no cover — no FK cycle exists in the schema today
            order.extend(sorted(remaining))
            break
        order.extend(ready)
        remaining -= set(ready)
    return tuple(order)


def _build_cleanup_plan(conn: psycopg.Connection[tuple]) -> _CleanupPlan:
    """Derive the wipe set, delete order and owned sequences from the catalog."""
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(_ROOT_TABLES_SQL)
        roots = {row[0] for row in cur.fetchall()}
        cur.execute(_FK_EDGES_SQL)
        edges = [(row[0], row[1]) for row in cur.fetchall()]
        cur.execute(_OWNED_SEQUENCES_SQL)
        sequence_owners = [(row[0], row[1]) for row in cur.fetchall()]
    conn.rollback()

    referencing: dict[str, set[str]] = {}
    for child, parent in edges:
        if child in roots and parent in roots:
            referencing.setdefault(parent, set()).add(child)

    missing = sorted(t for t in _PLANNER_TABLES if t not in roots)
    if missing:
        raise RuntimeError(
            f"_PLANNER_TABLES names tables absent from the worker DB: {missing}. "
            f"A migration renamed or dropped them without updating the list."
        )

    # CASCADE closure: everything TRUNCATE would reach from the planner set.
    closure = set(_PLANNER_TABLES)
    stack = list(closure)
    while stack:
        table = stack.pop()
        for child in referencing.get(table, ()):
            if child not in closure:
                closure.add(child)
                stack.append(child)

    delete_order = _topological_delete_order(closure, referencing)
    assert set(delete_order) == closure, "delete order must cover the whole wipe set"

    probe_sql = sql.SQL(" UNION ALL ").join(
        sql.SQL("SELECT {name} WHERE EXISTS (SELECT 1 FROM {table})").format(
            name=sql.Literal(table),
            table=sql.Identifier(table),
        )
        for table in delete_order
    )
    owned = frozenset(seq for seq, owner in sequence_owners if owner in closure)
    return _CleanupPlan(delete_order, probe_sql, owned)


def _cleanup_plan(conn: psycopg.Connection[tuple]) -> _CleanupPlan:
    key = test_db_name()
    plan = _CLEANUP_PLANS.get(key)
    if plan is None:
        plan = _build_cleanup_plan(conn)
        _CLEANUP_PLANS[key] = plan
    return plan


# One long-lived connection per worker process does all per-test cleanup.
#
# This is a load-bearing performance decision, not tidiness (#1568). The probe is
# a ``UNION ALL ... EXISTS`` with one branch per NON-PARTITION ROOT of the wipe
# set — 139 branches for today's 450-relation closure, since 311 of those
# relations are partitions covered by their parent's branch. Planning it on a
# FRESH backend costs ~110 ms because an empty relcache/syscache must fault in
# every one of those relations' catalog entries. On a connection that has already
# run it the same query costs ~3.8 ms — warm catalog caches, plus psycopg3's
# automatic server-side prepare once ``prepare_threshold`` (5) executions pass.
# Opening a connection per cleanup pass, as the fixture used to, paid the cold
# price every single time.
#
# Lifetime is the worker session; ``_close_janitor_conn`` runs from the
# ``_worker_db_keepalive`` teardown. Keyed by database name so a plan and its
# connection can never disagree about which DB they refer to.
_JANITOR_CONNS: dict[str, psycopg.Connection[tuple]] = {}


def _janitor_conn() -> psycopg.Connection[tuple]:
    """Return the worker's long-lived cleanup connection, opening it if needed."""
    key = test_db_name()
    conn = _JANITOR_CONNS.get(key)
    if conn is not None and not conn.closed:
        return conn
    conn = psycopg.connect(test_database_url())
    _JANITOR_CONNS[key] = conn
    return conn


def _close_janitor_conn() -> None:
    """Close and forget the worker's cleanup connection (session teardown)."""
    conn = _JANITOR_CONNS.pop(test_db_name(), None)
    if conn is not None:
        try:
            conn.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass


def _reset_planner_tables(conn: psycopg.Connection[tuple]) -> None:
    """Empty every table the old TRUNCATE pass emptied — but only the dirty ones.

    See the block comment above for why this is shaped the way it is (#1568).
    Falls back to ``_truncate_planner_tables`` on any operational failure so a
    test that outgrows the cached plan still gets a clean database.

    ``assert_test_db`` runs INSIDE the try deliberately. Its wrong-database guard
    raises ``RuntimeError``, which is not a ``psycopg.Error`` and so still escapes
    uncaught — a connection pointed at the dev DB is never TRUNCATEd. But the same
    call raises ``psycopg.Error`` on a connection whose backend has died, and that
    must reach the fallback rather than abort cleanup.
    """
    try:
        assert_test_db(conn)
        plan = _cleanup_plan(conn)
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(plan.probe_sql)
            dirty = {row[0] for row in cur.fetchall()}
            for table in sorted(_TRUNCATE_BEFORE_DELETE & dirty):
                cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                dirty.remove(table)
            for table in plan.delete_order:
                if table in dirty:
                    cur.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table)))
            # DELETE has no RESTART IDENTITY; reset only sequences that have
            # actually been read (``last_value IS NOT NULL``), which includes
            # ones advanced by a rolled-back INSERT on a now-empty table.
            cur.execute("SELECT sequencename FROM pg_sequences WHERE schemaname = 'public' AND last_value IS NOT NULL")
            advanced = {row[0] for row in cur.fetchall()} & plan.owned_sequences
            for sequence in sorted(advanced):
                cur.execute(sql.SQL("ALTER SEQUENCE {} RESTART").format(sql.Identifier(sequence)))
        conn.commit()
    except psycopg.Error as exc:
        # A test that CREATEd its own FK-bearing table, or a schema change since
        # the plan was cached. Drop the plan so the next test rebuilds it, and
        # let TRUNCATE ... CASCADE — which needs no precomputed order — clean up.
        _CLEANUP_PLANS.pop(test_db_name(), None)
        try:
            conn.rollback()
        except psycopg.Error:
            pass
        fallback_conn = conn
        if conn.broken:
            # A dead backend cannot run the fallback either, and the whole point
            # of the fallback is that cleanup still happens. Discard it if it was
            # the janitor — otherwise the cache serves a corpse to every later
            # test — and TRUNCATE on a fresh backend instead.
            if _JANITOR_CONNS.get(test_db_name()) is conn:
                _JANITOR_CONNS.pop(test_db_name(), None)
            fallback_conn = _janitor_conn()
        warnings.warn(
            f"Fast per-test cleanup failed ({type(exc).__name__}: {exc}); "
            f"falling back to TRUNCATE and rebuilding the cleanup plan. If this "
            f"warning is not rare, the FK topology changed — see #1568.",
            stacklevel=2,
        )
        _truncate_planner_tables(fallback_conn)


def _assert_worker_relations_under_ceiling(conn: psycopg.Connection[tuple]) -> None:
    """Fail if the worker DB's relation count exceeds the ceiling.

    Tripwire (#1401): a test that ``CREATE``s relations without
    dropping them leaks into the session-reused worker DB (per-test
    cleanup empties rows — it never drops relations). This
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
        f"per-test cleanup only empties rows. The failing test is the "
        f"(or first) culprit: bound its relation creation and tear it "
        f"down via a registered finalizer. Do NOT raise this ceiling to "
        f"silence it. See #1401."
    )


@pytest.fixture
def ebull_test_conn() -> Iterator[psycopg.Connection[tuple]]:
    """Yield a fresh connection to the worker's private test DB.

    Cleaned before and after each test by ``_reset_planner_tables``
    (#1568) — same wipe set the old TRUNCATE pass produced, ~200x
    cheaper. Both passes go through ``_assert_test_db`` so the dev DB
    can never be wiped by a misconfigured connection.
    """
    if not test_db_available():
        pytest.skip("ebull_test DB unavailable")

    janitor = _janitor_conn()
    _reset_planner_tables(janitor)
    # #1444 — creation-time relation budget. The teardown tripwire
    # below is skipped by a ``kill -9`` (OOM / Ctrl-C), which is
    # exactly how a runaway test left ~6-10M-relfile worker DBs that
    # stalled crash recovery for hours (2026-06-02). Asserting at
    # SETUP too means the FIRST surviving test after a skipped
    # teardown fails fast and names the worker DB, bounding the
    # accumulation a single session can reach.
    _assert_worker_relations_under_ceiling(janitor)

    conn = psycopg.connect(test_database_url())
    try:
        yield conn
    finally:
        # Close the test's own connection FIRST: cleanup runs on the janitor,
        # and a test that left a transaction open would otherwise hold row
        # locks the janitor's DELETE would block on.
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
        _reset_planner_tables(_janitor_conn())
        # #1401 — tripwire in THIS test's teardown so a relation leak fails
        # the test that caused it and names the culprit.
        _assert_worker_relations_under_ceiling(_janitor_conn())


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
        # #1568 — the per-test cleanup connection has the same lifetime.
        _close_janitor_conn()
        if keepalive is not None:
            try:
                keepalive.close()
            except Exception:  # pragma: no cover - best-effort
                pass


__all__ = [
    "EBULL_SMOKE_LIFESPAN_LOCK",
    "EBULL_TEMPLATE_LOCK",
    "TEMPLATE_DB_NAME",
    "TemplateWorktreeMismatch",
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
