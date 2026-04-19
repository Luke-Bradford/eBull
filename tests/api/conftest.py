"""Shared fixtures for tests/api/.

The module-global app object can accumulate stale dependency_overrides
from other test files that install them at import time (e.g.
test_api_audit.py's module-level setdefault). Every test in this
package that hits a real-DB endpoint must use the ``clean_client``
fixture so it starts with a clean slate.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.main import app


@pytest.fixture
def clean_client() -> Generator[TestClient, None, None]:
    """TestClient with auth bypassed and get_conn override cleared.

    Mirrors the pattern in tests/test_sync_orchestrator_api.py. Removing
    any stale get_conn override (e.g. the fallback mock installed at import
    time by test_api_audit.py) prevents mock connections from leaking into
    real-DB tests.

    The teardown deliberately re-installs the auth bypass (matching what
    tests/conftest.py installed at import time) so subsequent tests that
    depend on the no-op auth override (e.g. the smoke test) continue to
    work. We do not wipe all overrides — that would remove the no-op
    auth override and break tests that run after this fixture.
    """
    app.dependency_overrides.pop(get_conn, None)
    app.dependency_overrides[require_session_or_service_token] = lambda: None
    with TestClient(app) as client:
        yield client
    # Restore: remove any get_conn override this run may have left,
    # and ensure the auth bypass remains in place.
    app.dependency_overrides.pop(get_conn, None)
    app.dependency_overrides[require_session_or_service_token] = lambda: None
