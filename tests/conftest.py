"""Shared pytest configuration for eBull API tests.

Default ``require_auth`` to a no-op so the broad set of pre-existing API
tests do not have to manage bearer tokens. The dedicated auth test
(``test_api_auth.py``) clears this override per-test to exercise the real
auth dependency.
"""

from __future__ import annotations

from app.api.auth import require_auth
from app.main import app


def _noop_auth() -> None:  # pragma: no cover - trivial override
    return None


app.dependency_overrides[require_auth] = _noop_auth
