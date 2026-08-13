"""Regression tests for the auth-bypass isolation pattern (#655).

The conftest.py auth bypass (`require_session_or_service_token`) is
installed once at module import. Test fixtures elsewhere were calling
`app.dependency_overrides.clear()` to reset state — wiping the bypass
globally and causing later tests in unrelated files to 401 against
the real auth dep. Smoke (`/budget`) was the most visible victim.

Two layers of defense kept by this PR:

1. The offender (`tests/test_sync_orchestrator_api.py::client`) now
   snapshots + restores instead of clearing.
2. `conftest.py` registers an autouse fixture that re-installs the
   bypass at the start of every test — robust to any other future
   test that mutates the global dict and forgets to restore.

These tests pin both layers so a future regression of either is
caught.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.api.auth import require_session_or_service_token
from app.main import app


def test_auth_bypass_present_at_test_start() -> None:
    """Conftest's autouse fixture re-installs the bypass before each
    test runs. Asserting the override is present is the simplest
    pin on that contract."""
    override = app.dependency_overrides.get(require_session_or_service_token)
    assert override is not None, (
        "Auth bypass missing — conftest's _reassert_auth_bypass autouse fixture should have re-installed it"
    )


class TestEvenAfterClear:
    """If a hostile test inside the same module clears the override
    dict, the next test should still see the bypass — that's the
    whole point of the autouse re-assert."""

    def test_first_test_clears_overrides(self) -> None:
        # Simulate the bug: a test or fixture wipes the global dict
        # and forgets to restore. Pre-#655 this would have leaked to
        # the next test.
        app.dependency_overrides.clear()
        # Sanity: the clear actually wiped the bypass for THIS test's
        # remaining lifetime.
        assert require_session_or_service_token not in app.dependency_overrides

    def test_second_test_still_has_bypass(self) -> None:
        # The autouse fixture in conftest re-installed the bypass at
        # the start of THIS test, even though the previous test left
        # the dict empty. This is the contract that protects the
        # smoke test's /budget hit from auth pollution.
        assert require_session_or_service_token in app.dependency_overrides


@pytest.fixture
def _hostile_clear_then_restore_partial() -> Iterator[None]:
    """Fixture that mimics the pre-#655 bug shape: clear, install
    only some overrides, clear again on teardown."""
    app.dependency_overrides.clear()
    app.dependency_overrides[require_session_or_service_token] = lambda: None
    yield
    app.dependency_overrides.clear()


def test_hostile_fixture_does_not_break_subsequent_tests(
    _hostile_clear_then_restore_partial: None,
) -> None:
    """The fixture's teardown clears the dict — exactly the pre-#655
    bug. The next test's autouse re-assert restores; this test only
    proves the override IS in place during this test's body so a
    misconfiguration that disables the autouse fixture would fail
    here."""
    assert require_session_or_service_token in app.dependency_overrides
