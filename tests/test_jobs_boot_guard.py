"""Tests for ``app.jobs.boot_guard`` (Stream A PR-A T1.8, #1233).

Two layers (per spec §20 + Test-lens IMPORTANT — pure-function refactor
keeps unit tests fast + table-driven; integration covers the wrapper's
DB-side breadcrumb + cleanup contract):

1. **Pure-function unit tests** for ``check_operator_exists``: no DB,
   mocked cursor; 3 cases over ``BootGuardOutcome``.

2. **Integration tests** against the real test DB
   (``ebull_test_conn`` fixture) for both the pure function (real
   SELECT) and the wrapper (``_check_operator_exists_with_cleanup``)
   — covers the breadcrumb-write contract + cleanup-on-fail contract.

The wrapper integration tests monkeypatch ``settings.database_url``
to the per-worker test DB so the wrapper's own ``psycopg.connect``
calls hit the right database.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.config import settings
from app.jobs.boot_guard import (
    OPERATOR_MISSING_ERROR_MESSAGE,
    BootGuardOutcome,
    check_operator_exists,
)
from tests.fixtures.ebull_test_db import test_database_url

# --------------------------------------------------------------------- #
# 1. Pure-function unit tests (mocked cursor, no DB)
# --------------------------------------------------------------------- #


class TestCheckOperatorExistsPureFunction:
    """Pure-function contract — closed-set ``BootGuardOutcome`` over the
    three input combinations. No DB required; uses a mocked cursor so
    the test runs in every CI environment."""

    def test_returns_operator_present_when_select_finds_a_row(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (1,)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        outcome = check_operator_exists(conn, skip_env_set=False)

        assert outcome is BootGuardOutcome.OPERATOR_PRESENT
        cur.execute.assert_called_once_with("SELECT 1 FROM operators LIMIT 1")

    def test_returns_operator_absent_when_select_finds_no_rows(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = None
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        outcome = check_operator_exists(conn, skip_env_set=False)

        assert outcome is BootGuardOutcome.OPERATOR_ABSENT
        cur.execute.assert_called_once_with("SELECT 1 FROM operators LIMIT 1")

    def test_returns_skipped_by_env_when_flag_true_and_skips_db(self) -> None:
        """Env-skip path MUST NOT touch the connection (cold-start path
        may run before the DB is reachable)."""
        conn = MagicMock()

        outcome = check_operator_exists(conn, skip_env_set=True)

        assert outcome is BootGuardOutcome.SKIPPED_BY_ENV
        conn.cursor.assert_not_called()


class TestBootGuardOutcomeIsClosedSet:
    """Pin the enum to its three members so a future refactor that
    silently adds a fourth member trips a test."""

    def test_enum_has_exactly_three_members(self) -> None:
        assert {m.name for m in BootGuardOutcome} == {
            "OPERATOR_PRESENT",
            "OPERATOR_ABSENT",
            "SKIPPED_BY_ENV",
        }

    def test_enum_values_are_unique_lowercase_strings(self) -> None:
        values = [m.value for m in BootGuardOutcome]
        assert len(set(values)) == len(values)
        assert all(isinstance(v, str) and v == v.lower() for v in values)


class TestOperatorMissingErrorMessage:
    """Error breadcrumb must be operator-actionable — pin substrings
    so a future "shorten the message" edit can't strip the actionable
    parts."""

    def test_mentions_auth_setup_endpoint(self) -> None:
        assert "/auth/setup" in OPERATOR_MISSING_ERROR_MESSAGE

    def test_mentions_skip_env_var_name_for_cold_start(self) -> None:
        assert "EBULL_JOBS_SKIP_OPERATOR_CHECK" in OPERATOR_MISSING_ERROR_MESSAGE

    def test_starts_with_operator_actionable_prefix(self) -> None:
        # "jobs boot blocked" lets the operator grep stderr for the marker.
        assert OPERATOR_MISSING_ERROR_MESSAGE.startswith("jobs boot blocked")


# --------------------------------------------------------------------- #
# 2. Integration tests — real DB via ``ebull_test_conn``.
# --------------------------------------------------------------------- #


pytestmark_integration = pytest.mark.integration


def _delete_all_operators(conn: psycopg.Connection[tuple]) -> None:
    """Helper — wipe operators (and the FK-cascading sessions) before
    a test that needs an empty table."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sessions")
        cur.execute("DELETE FROM operators")
    conn.commit()


def _insert_test_operator(conn: psycopg.Connection[tuple]) -> None:
    import uuid

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, %s)",
            (str(uuid.uuid4()), f"boot_guard_test_{uuid.uuid4().hex[:8]}", "x"),
        )
    conn.commit()


def _read_last_jobs_boot_error(conn: psycopg.Connection[tuple]) -> tuple[str | None, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_jobs_boot_error, last_jobs_boot_error_at FROM bootstrap_state WHERE id = 1",
        )
        row = cur.fetchone()
    return (None, None) if row is None else (row[0], row[1])


class TestCheckOperatorExistsAgainstRealDB:
    """The pure function against a real DB — verifies the SELECT query
    actually returns rows correctly from the live ``operators`` table."""

    @pytest.mark.integration
    def test_returns_present_with_seeded_operator(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        _delete_all_operators(ebull_test_conn)
        _insert_test_operator(ebull_test_conn)

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            outcome = check_operator_exists(guard_conn, skip_env_set=False)

        assert outcome is BootGuardOutcome.OPERATOR_PRESENT

    @pytest.mark.integration
    def test_returns_absent_with_empty_operators_table(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        _delete_all_operators(ebull_test_conn)

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            outcome = check_operator_exists(guard_conn, skip_env_set=False)

        assert outcome is BootGuardOutcome.OPERATOR_ABSENT


# --------------------------------------------------------------------- #
# Wrapper tests — drive ``_check_operator_exists_with_cleanup`` against
# the real test DB. Imported lazily to avoid pulling in __main__'s
# side-effectful imports at module-load time.
# --------------------------------------------------------------------- #


def _wrapper() -> Any:
    """Lazy import so the rest of the file can run without paying for
    ``app.jobs.__main__``'s import-time side effects (APScheduler,
    listener supervision, etc.).
    """
    from app.jobs.__main__ import _check_operator_exists_with_cleanup

    return _check_operator_exists_with_cleanup


class TestCheckOperatorExistsWithCleanupWrapper:
    """End-to-end wrapper contract:

    * env-skip path returns cleanly without touching DB or fence/pool.
    * operator-present path clears any stale breadcrumb.
    * operator-absent path persists the breadcrumb, closes fence + pool,
      and raises ``SystemExit(2)``.
    """

    @pytest.mark.integration
    def test_env_skip_does_not_touch_db_or_pool(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Env-skip path MUST NOT dial the DB — cold-start use case
        is documented as "DB may not be reachable yet". Verify the
        wrapper returns BEFORE any psycopg.connect call. Architect
        IMPORTANT — broken in v1 of PR-A (connect was unconditional)."""
        monkeypatch.setenv("EBULL_JOBS_SKIP_OPERATOR_CHECK", "1")
        connect_call_count = 0
        real_connect = psycopg.connect

        def _counting_connect(*args: Any, **kwargs: Any) -> Any:
            nonlocal connect_call_count
            connect_call_count += 1
            return real_connect(*args, **kwargs)

        monkeypatch.setattr(psycopg, "connect", _counting_connect)
        # Also patch through the import path used by the wrapper.
        from app.jobs import __main__ as jobs_main

        monkeypatch.setattr(jobs_main.psycopg, "connect", _counting_connect)

        fence_conn = MagicMock()
        pool = MagicMock()

        _wrapper()(fence_conn, pool)

        assert connect_call_count == 0, "env-skip path must not dial DB (cold-start contract)"
        fence_conn.close.assert_not_called()
        pool.close.assert_not_called()

    @pytest.mark.integration
    def test_operator_present_clears_stale_breadcrumb(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _delete_all_operators(ebull_test_conn)
        _insert_test_operator(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE bootstrap_state SET last_jobs_boot_error = %s WHERE id = 1",
                ("stale error from a prior failed boot",),
            )
        ebull_test_conn.commit()

        monkeypatch.delenv("EBULL_JOBS_SKIP_OPERATOR_CHECK", raising=False)
        monkeypatch.setattr(settings, "database_url", test_database_url())
        fence_conn = MagicMock()
        pool = MagicMock()

        _wrapper()(fence_conn, pool)

        message, at = _read_last_jobs_boot_error(ebull_test_conn)
        assert message is None, "successful boot must clear the stale breadcrumb message"
        assert at is None, "successful boot must clear the stale breadcrumb timestamp"
        fence_conn.close.assert_not_called()
        pool.close.assert_not_called()

    @pytest.mark.integration
    def test_operator_absent_persists_breadcrumb_and_exits_2(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _delete_all_operators(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE bootstrap_state SET last_jobs_boot_error = NULL WHERE id = 1")
        ebull_test_conn.commit()

        monkeypatch.delenv("EBULL_JOBS_SKIP_OPERATOR_CHECK", raising=False)
        monkeypatch.setattr(settings, "database_url", test_database_url())
        fence_conn = MagicMock()
        pool = MagicMock()

        with pytest.raises(SystemExit) as excinfo:
            _wrapper()(fence_conn, pool)

        assert excinfo.value.code == 2

        message, at = _read_last_jobs_boot_error(ebull_test_conn)
        assert message == OPERATOR_MISSING_ERROR_MESSAGE
        assert at is not None, "breadcrumb timestamp must be populated alongside message"

        fence_conn.close.assert_called_once()
        pool.close.assert_called_once()
