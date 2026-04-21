"""Shared pytest configuration for eBull API tests.

The protected routes use ``require_session_or_service_token`` (issue #98).
We install a no-op override on it so the broad set of pre-existing API
tests can hit protected endpoints without managing bearer tokens or
session cookies. The dedicated auth tests
(``test_api_auth_session.py``) clear this override per-test to exercise
the real dependency.
"""

from __future__ import annotations

from app.api.auth import require_session_or_service_token
from app.main import app
from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401


def _noop_auth() -> None:  # pragma: no cover - trivial override
    return None


app.dependency_overrides[require_session_or_service_token] = _noop_auth
