"""Shared pytest configuration for eBull tests.

Three responsibilities:

1. Auth bypass for the broad set of pre-existing API tests
   (``require_session_or_service_token`` no-op override). The
   dedicated auth tests in ``test_api_auth_session.py`` clear this
   override per-test to exercise the real dependency.
2. Per-invocation pytest infra (#893): build the
   ``ebull_test_template`` once in the controller, give every xdist
   worker a private DB derived from it, and route ``--basetemp``
   outside the repo so concurrent runs don't share locked tmp dirs.
3. Session-wide dev-DB-size tripwire (#1208 Sub 6): record
   ``pg_database_size('ebull')`` at session start, assert <1 MB growth
   at session end. Catches a test that opens a raw
   ``psycopg.connect(settings.database_url)`` outside the fixture and
   writes through it. Tripwire only — see the fixture docstring.
"""

from __future__ import annotations

import functools
import os
import pathlib
import shutil
import tempfile
from collections.abc import Iterator

# Skip lifespan catch-up in every TestClient(app) enter/exit cycle.
# Without this, each test that enters the FastAPI lifespan fires real
# overdue APScheduler jobs against the dev DB, which then block the
# shutdown(wait=True) path for hundreds of seconds per test. Gated at
# the start() call site in app/jobs/runtime.py so direct catch-up unit
# tests in tests/test_jobs_runtime.py::TestCatchUpOnBoot are unaffected.
# setdefault (not hard-set) lets a developer run
# EBULL_SKIP_CATCH_UP=0 pytest to reproduce catch-up bugs.
os.environ.setdefault("EBULL_SKIP_CATCH_UP", "1")

# Skip the #649A boot freshness sweep in every TestClient(app) enter
# cycle. Without this, every test that enters the FastAPI lifespan
# would dispatch a `scope='behind'` sync that holds the partial-
# unique-index gate; subsequent POST /sync scope='behind' tests in
# unrelated test modules would 409 against it.
os.environ.setdefault("EBULL_SKIP_BOOT_SWEEP", "1")

import psycopg  # noqa: E402
import pytest  # noqa: E402

from app.api.auth import require_session_or_service_token  # noqa: E402
from app.main import app  # noqa: E402
from tests.fixtures.ebull_test_db import (  # noqa: E402, F401
    _force_drop_invalid_test_dbs,  # noqa: E402 — #1455 session-start corpse sweep
    _run_id,  # noqa: E402
    build_template_if_stale,
    drop_worker_database,
)
from tests.fixtures.ebull_test_db import (
    _worker_db_keepalive as _worker_db_keepalive,  # noqa: F401 — autouse; #1208 Phase 2 Rail 1
)
from tests.fixtures.ebull_test_db import (
    ebull_test_conn as ebull_test_conn,
)


@pytest.fixture
def seeded_instrument_id(ebull_test_conn: psycopg.Connection[tuple]) -> int:
    """Insert a single fresh instruments row and return its instrument_id.

    Uses a fixed ID (1) — safe because ebull_test_conn TRUNCATEs
    instruments between every test (instruments is in _PLANNER_TABLES).
    """
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'PR12A', 'PR12 Test Co A', TRUE)"
    )
    ebull_test_conn.commit()
    return 1


@pytest.fixture
def two_seeded_instrument_ids(ebull_test_conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    """Insert two fresh instruments rows and return their instrument_ids.

    Uses fixed IDs (1, 2) — safe because ebull_test_conn TRUNCATEs
    instruments between every test (instruments is in _PLANNER_TABLES).
    """
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'PR12A', 'PR12 Test Co A', TRUE), "
        "       (2, 'PR12B', 'PR12 Test Co B', TRUE)"
    )
    ebull_test_conn.commit()
    return (1, 2)


def _noop_auth() -> None:  # pragma: no cover - trivial override
    return None


# Module-import-time install so non-fixtured tests see the bypass.
app.dependency_overrides[require_session_or_service_token] = _noop_auth


# Defense-in-depth (#655): re-assert the auth bypass at the start of
# every test. A test fixture elsewhere can call
# ``app.dependency_overrides.clear()`` and forget to restore — that
# wipes this module-global install and any subsequent test (notably
# the smoke test) hits real auth and 401s. Re-installing here makes
# the bypass robust against any other test that mutates the global
# override dict, regardless of test ordering.
@pytest.fixture(autouse=True)
def _reassert_auth_bypass() -> None:
    app.dependency_overrides[require_session_or_service_token] = _noop_auth


# #1208 Sub 6 — session-wide tripwire on dev DB growth.
#
# THIS IS A TRIPWIRE, NOT PROOF of no dev writes. Primary defense
# remains ``tests/fixtures/ebull_test_db.py::assert_test_db`` (rejects
# destructive ops against any DB not matching ``ebull_test_*``) +
# ``tests/smoke/test_no_settings_url_in_destructive_paths.py`` (static
# bug-shape grep). This fixture catches the residual case where a test
# opens a raw ``psycopg.connect(settings.database_url)`` outside the
# fixture path and writes through it — exactly the failure mode that
# almost certainly nuked the ``runtime_config`` singleton on 2026-05-18.
#
# Limits: misses deletes (size flat or shrinks), HOT updates (in-place
# page rewrites), and may false-positive on autovacuum / vacuum / stats
# work running concurrently. Additionally (#1455): when a live jobs
# process holds JOBS_PROCESS_LOCK_KEY on 'ebull', its legitimate writes
# are indistinguishable by size from a leak, so the assertion is skipped
# (warn-only) for that session. This is a conscious tradeoff — the
# exemption is coarse (jobs-running, not jobs-attributed), so a genuine
# leak that coincides with a running jobs process is downgraded to a
# warning rather than a failure. Acceptable because this is a secondary
# tripwire: the PRIMARY defenses (assert_test_db + the static grep smoke
# test above) still fail hard on any destructive op against a non-test DB.
# When this tripwire fires, grep the tests directory for raw
# ``psycopg.connect(settings.database_url)`` and route each use through
# ``tests/fixtures/ebull_test_db.py::test_database_url``.
# Never silence by raising the threshold — fix the offending test.
_DEV_DB_GROWTH_TOLERANCE_BYTES = 1_000_000


def _jobs_process_running() -> bool:
    """True iff a jobs process holds ``JOBS_PROCESS_LOCK_KEY`` on the dev DB.

    When the operator's local jobs process (``python -m app.jobs``) is alive
    during a long full-suite run, it legitimately writes to ``ebull``
    (heartbeat, job_runs, sync_runs, ingested rows) — tens of MB across a
    78-minute run. That growth is NOT a leaked ``psycopg.connect(
    settings.database_url)`` from a test, so the size tripwire must not fail
    on it (#1455).

    Detection reads ``pg_locks`` directly rather than calling
    ``probe_jobs_process_running`` (which momentarily *acquires* the fence):
    a passive tripwire must never perturb lock state — an acquire-probe has a
    sub-ms window where a cold-starting ``python -m app.jobs`` could lose the
    race for its own fence and refuse startup (Codex #1455). A ``bigint``
    advisory lock splits across ``pg_locks`` as ``classid = key>>32``,
    ``objid = key & 0xffffffff``, ``objsubid = 1``. Advisory locks are
    per-database, so we read on the same DB the jobs process fences.

    Best-effort: any error → False (fall through to the normal assertion
    rather than masking a real leak).
    """
    try:
        import psycopg

        from app.config import settings
        from app.jobs.locks import JOBS_PROCESS_LOCK_KEY

        # Decode assumes a non-negative key: Python's arbitrary-precision
        # >> on a negative int would yield a negative high half that the
        # mask silently "corrects". The key is a fixed positive constant;
        # assert it so a future sign change fails loud, not silently
        # mis-decoded (bot #1455 NITPICK).
        assert JOBS_PROCESS_LOCK_KEY >= 0, "JOBS_PROCESS_LOCK_KEY must be non-negative for pg_locks decode"
        classid = (JOBS_PROCESS_LOCK_KEY >> 32) & 0xFFFFFFFF
        objid = JOBS_PROCESS_LOCK_KEY & 0xFFFFFFFF
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            row = conn.execute(
                "SELECT 1 FROM pg_locks "
                "WHERE locktype = 'advisory' AND classid = %s AND objid = %s "
                "  AND objsubid = 1 AND granted "
                "LIMIT 1",
                (classid, objid),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _read_dev_db_size() -> int | None:
    """Read ``pg_database_size('ebull')`` once.

    Returns None if the dev DB is unreachable (CI, no Postgres, etc.) —
    in that case the tripwire is silently skipped because there's
    nothing to invariant-check.
    """
    try:
        import psycopg  # local import; conftest must load even without psycopg

        from app.config import settings

        dev_db_url = settings.database_url
        with psycopg.connect(dev_db_url, connect_timeout=2) as conn:
            row = conn.execute("SELECT pg_database_size('ebull')").fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])
    except Exception:
        return None


@pytest.fixture(scope="session", autouse=True)
def _dev_db_size_tripwire() -> Iterator[None]:
    if os.getenv("CI") == "true":
        yield
        return
    start_size = _read_dev_db_size()
    if start_size is None:
        yield
        return
    yield
    end_size = _read_dev_db_size()
    if end_size is None:
        return
    delta_bytes = end_size - start_size
    if delta_bytes >= _DEV_DB_GROWTH_TOLERANCE_BYTES and _jobs_process_running():
        # A live jobs process holds JOBS_PROCESS_LOCK_KEY on 'ebull' and is
        # the expected author of this growth (#1455). Skip the hard assertion
        # but WARN, so a genuine leak coinciding with a running jobs process
        # is still surfaced in the log rather than silently exempted.
        import warnings

        warnings.warn(
            f"dev DB 'ebull' grew by {delta_bytes} bytes during the session, "
            f"but a jobs process holds JOBS_PROCESS_LOCK_KEY on it — growth "
            f"attributed to the live jobs process; size tripwire skipped "
            f"(#1455). If no jobs process should be running, investigate.",
            stacklevel=2,
        )
        return
    assert delta_bytes < _DEV_DB_GROWTH_TOLERANCE_BYTES, (
        f"TRIPWIRE: dev DB 'ebull' grew by {delta_bytes} bytes during "
        f"the test session (tolerance {_DEV_DB_GROWTH_TOLERANCE_BYTES}). "
        "A test likely opened a raw psycopg.connect(settings.database_url) "
        "and wrote to ebull instead of ebull_test_*. Grep tests/ for "
        "`psycopg.connect(settings.database_url)` and route each use "
        "through `tests/fixtures/ebull_test_db.py::test_database_url`."
    )


def _is_xdist_worker(config: pytest.Config) -> bool:
    return hasattr(config, "workerinput")


def _set_basetemp(config: pytest.Config) -> pathlib.Path:
    """Route pytest tmp dirs outside the repo, partitioned by run id.

    Avoids the Windows-specific lock-leak smell from the legacy
    ``tmp_pytest/`` directory in repo root and lets concurrent
    invocations have non-overlapping tmp trees.
    """
    base = pathlib.Path(tempfile.gettempdir()) / "ebull_pytest" / _run_id()
    base.mkdir(parents=True, exist_ok=True)
    config.option.basetemp = str(base)
    return base


# ---------------------------------------------------------------------------
# DB-backed test auto-classification (test-gate fast tier)
# ---------------------------------------------------------------------------
#
# The pre-push gate runs only the fast tier (`-m "not db"`): pure-logic tests
# that need no Postgres. DB-backed integration tests are the slow, xdist-flaky
# majority (~280 files) and move OFF the per-push critical path — run them with
# `uv run pytest -m db` on demand / pre-merge. Rather than hand-annotate every
# file, classify at collection: a test is DB-backed if it pulls a real-DB
# fixture OR its module source references a real-DB entrypoint (raw
# psycopg.connect, the test-DB URL, run_migrations, or a TestClient lifespan
# that drives the dev DB).
_DB_FIXTURE_NAMES: frozenset[str] = frozenset(
    {
        "ebull_test_conn",
        "ebull_test_db",
        "db_conn",
        "test_pool",
        "test_conn",
        "seeded_instrument_id",
        "two_seeded_instrument_ids",
    }
)
_DB_SOURCE_MARKERS: tuple[str, ...] = (
    "ebull_test_conn",
    "test_database_url",
    "run_migrations",
    "psycopg.connect",
    "TestClient",
)


@functools.cache
def _module_source_touches_db(path: str) -> bool:
    try:
        source = pathlib.Path(path).read_text(encoding="utf-8")
    except OSError:
        return False
    return any(marker in source for marker in _DB_SOURCE_MARKERS)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-apply the ``db`` marker so the fast gate can deselect DB tests."""
    for item in items:
        if _DB_FIXTURE_NAMES & set(getattr(item, "fixturenames", ())):
            item.add_marker("db")
        elif _module_source_touches_db(str(item.path)):
            item.add_marker("db")


def pytest_configure(config: pytest.Config) -> None:
    """Build the test-DB template + set basetemp.

    Runs only in the xdist controller process. Workers inherit the
    template (fully migrated) and the run id (env propagation), so
    they never repeat this work.

    Errors building the template are not fatal: the per-test
    ``ebull_test_conn`` fixture skips cleanly if the DB stack is
    unavailable. We still log a warning via ``test_db_available``
    when a worker actually tries to use it.
    """
    # Registered in every process (controller + workers) so `-m db` /
    # `-m "not db"` selection is honoured regardless of run mode.
    config.addinivalue_line(
        "markers",
        "db: test needs a real Postgres connection (auto-applied at collection). "
        "Excluded from the fast pre-push gate; run the integration tier with `-m db`.",
    )

    if _is_xdist_worker(config):
        return

    # #1455 — force-drop datconnlimit=-2 corpses UNCONDITIONALLY at session
    # start, BEFORE build_template_if_stale opens its admin connection. A
    # SIGKILL'd / OOM'd worker (the #1444 18h-crash-loop signature) leaves a
    # -2 corpse that pg_database_size() hangs on, which both wedges
    # postgres_health-backed tests and causes collateral failures across
    # unrelated tests in the same run. build_template_if_stale runs its own
    # corpse sweep, but only AFTER acquiring the template lock + the orphan
    # sweep — and the whole call is best-effort: if any earlier step raises
    # against a degraded cluster, the exception is swallowed as a warning
    # below and the inner sweep never runs. Sweeping first, unconditionally,
    # un-degrades the cluster before any size query or template work.
    try:
        dropped = _force_drop_invalid_test_dbs()
        if dropped:
            import warnings

            warnings.warn(
                f"Swept {len(dropped)} datconnlimit=-2 test-DB corpse(s) at session start (#1455): {dropped}",
                stacklevel=2,
            )
    except Exception:  # pragma: no cover - best-effort; inner sweep retries
        pass

    try:
        build_template_if_stale()
    except AssertionError:
        # #1208 Phase 2 — the orphan sweep's ``_NEVER_DROP`` rail
        # raises ``AssertionError`` on a regex regression that could
        # target the operator dev DB or the test template. MUST
        # escape every outer handler so the failure is loud, not
        # demoted to a warning.
        raise
    except Exception as exc:  # pragma: no cover - best-effort
        # Don't crash the whole pytest invocation just because
        # Postgres is unreachable; tests that need the real DB will
        # skip individually via ``test_db_available``.
        import warnings

        warnings.warn(
            f"Could not build ebull_test_template: {type(exc).__name__}: {exc}",
            stacklevel=2,
        )

    _set_basetemp(config)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Drop this worker's private DB and prune its basetemp dir.

    Runs in every process — controller and workers. The controller
    has no private DB (worker_id == "main" only if no workers ran);
    the drop is a no-op when the DB doesn't exist.

    The template stays so the next invocation reuses it for free.
    """
    drop_worker_database()

    if _is_xdist_worker(session.config):
        return

    # Controller-only: best-effort cleanup of the run's basetemp tree.
    try:
        base = pathlib.Path(tempfile.gettempdir()) / "ebull_pytest" / _run_id()
        if base.exists():
            shutil.rmtree(base, ignore_errors=True)
    except Exception:  # pragma: no cover - best-effort
        pass
