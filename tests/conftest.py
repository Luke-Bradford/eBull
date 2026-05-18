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

import pytest  # noqa: E402

from app.api.auth import require_session_or_service_token  # noqa: E402
from app.main import app  # noqa: E402
from tests.fixtures.ebull_test_db import (  # noqa: E402, F401
    _run_id,  # noqa: E402
    build_template_if_stale,
    drop_worker_database,
)
from tests.fixtures.ebull_test_db import (
    ebull_test_conn as ebull_test_conn,
)


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
# work running concurrently. When this tripwire fires, grep the tests
# directory for raw ``psycopg.connect(settings.database_url)`` and
# route each use through ``tests/fixtures/ebull_test_db.py::test_database_url``.
# Never silence by raising the threshold — fix the offending test.
_DEV_DB_GROWTH_TOLERANCE_BYTES = 1_000_000


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
    if _is_xdist_worker(config):
        return

    try:
        build_template_if_stale()
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
