"""Shared pytest configuration for eBull API tests.

The protected routes use ``require_session_or_service_token`` (issue #98).
We install a no-op override on it so the broad set of pre-existing API
tests can hit protected endpoints without managing bearer tokens or
session cookies. The dedicated auth tests
(``test_api_auth_session.py``) clear this override per-test to exercise
the real dependency.
"""

from __future__ import annotations

import os

# Skip lifespan catch-up in every TestClient(app) enter/exit cycle.
# Without this, each test that enters the FastAPI lifespan fires real
# overdue APScheduler jobs against the dev DB, which then block the
# shutdown(wait=True) path for hundreds of seconds per test. Gated at
# the start() call site in app/jobs/runtime.py so direct catch-up unit
# tests in tests/test_jobs_runtime.py::TestCatchUpOnBoot are unaffected.
# setdefault (not hard-set) lets a developer run
# EBULL_SKIP_CATCH_UP=0 pytest to reproduce catch-up bugs.
os.environ.setdefault("EBULL_SKIP_CATCH_UP", "1")

import pytest  # noqa: E402

from app.api.auth import require_session_or_service_token  # noqa: E402
from app.main import app  # noqa: E402
from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401, E402


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
