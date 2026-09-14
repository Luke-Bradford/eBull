"""Regression tests for cross-module ``app.dependency_overrides`` isolation (#2224 cause 1).

``app.dependency_overrides`` is a plain dict on the process-global
``app.main.app``. Every API test module mutates that one dict, and three
dialects coexist:

1. ``setdefault(get_conn, _fallback_conn)`` at module import — 11 modules do
   this, so the first module imported on a worker wins the key.
2. ``[get_conn] = _fallback_conn`` in ``teardown_method`` — writes *that
   module's* fallback back onto the shared dict.
3. ``pop(get_conn, None)`` in teardown — removes the key entirely.

Only (3) is harmful, and it is harmful across module boundaries. (2) is
benign because every module's fallback is an equivalent empty mock, so a
foreign one serves fine. After (3) runs, the next test from a
``setdefault`` module that does *not* install its own override falls
through to the REAL ``get_conn``, whose pool was never started:

    tests/test_api_theses.py::TestGetThesisHistory::test_zero_limit_rejected
    E   assert 503 == 422
    WARNING app.db db pool absent (lifespan not started or torn down) — returning 503

That is #2224's "~2 failures per full run, different tests each time": under
xdist the victim is whichever ungrouped test happens to land after a popping
test on the same worker.

The fix is ``conftest._restore_dependency_overrides`` — snapshot before each
test, restore after — which is the #655 ``_reassert_auth_bypass`` pattern
adapted to a key that has no single correct value. These tests pin it.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest

from app.db import get_conn
from app.main import app

# Pin every test here to one worker. ``TestOverrideSurvivesAPop`` asserts an
# ORDERING contract: its second test is only meaningful if it runs after the
# first, in the same process. Unpinned, xdist's per-test distribution can put
# them on different workers and the assertion passes for the wrong reason.
pytestmark = pytest.mark.xdist_group("test_dependency_override_isolation")


def _fallback_conn() -> Iterator[MagicMock]:
    yield MagicMock()


# The exact shape the 11 API test modules use: one module-level install, at
# import time, under a key all of them share.
app.dependency_overrides.setdefault(get_conn, _fallback_conn)


def test_conn_override_present_at_test_start() -> None:
    """The baseline every ``setdefault`` module's non-overriding tests rely on."""
    assert get_conn in app.dependency_overrides, (
        "get_conn override missing at test start — conftest's "
        "_restore_dependency_overrides autouse fixture should have restored it"
    )


class TestOverrideSurvivesAPop:
    """A hostile test pops the shared key; the next test must not inherit that."""

    def test_first_test_pops_the_conn_override(self) -> None:
        # Dialect (3), as written in tests/test_api_auth_session.py:83 and
        # eight other modules.
        app.dependency_overrides.pop(get_conn, None)
        assert get_conn not in app.dependency_overrides

    def test_second_test_still_has_a_conn_override(self) -> None:
        # Restored by the autouse fixture's teardown, which runs AFTER the
        # previous test's own teardown_method. Without it this is the 503.
        assert get_conn in app.dependency_overrides


def _hostile_conn() -> Iterator[MagicMock]:  # pragma: no cover - never called
    yield MagicMock()


class TestForeignOverrideIsAlsoRestored:
    """Dialect (2) is benign in production but must still not escape its test.

    Identity — not membership — is what separates "restored" from "some other
    module happened to leave a value here", which is why this class exists
    alongside ``TestOverrideSurvivesAPop``.
    """

    def test_first_test_installs_a_foreign_override(self) -> None:
        app.dependency_overrides[get_conn] = _hostile_conn
        assert app.dependency_overrides[get_conn] is _hostile_conn

    def test_second_test_does_not_see_it(self) -> None:
        assert app.dependency_overrides.get(get_conn) is not _hostile_conn
