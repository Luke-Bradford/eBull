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
    is_cold_start_state,
    read_boot_gate_snapshot,
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


def _set_bootstrap_state_status(conn: psycopg.Connection[tuple], status: str) -> None:
    """Force ``bootstrap_state.status`` to a specific value before a test
    that needs to gate on cold-start vs post-install state.

    Issue #1363: ``is_cold_start_state`` returns ``True`` iff
    ``status='pending'``. The schema-seeded default is ``'pending'``, so
    tests that need to exercise the post-install hard-fail path MUST
    flip status to a non-pending value first.
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE bootstrap_state SET status = %s WHERE id = 1", (status,))
    conn.commit()


def _delete_all_operator_audit(conn: psycopg.Connection[tuple]) -> None:
    """Wipe ``operator_audit`` rows so the cold-start gate sees a true
    pre-setup state. Issue #1363 (Codex 2 P2 round 2): the cold-start
    branch fires only when ``NOT EXISTS(setup event in operator_audit)``
    — pollution from prior tests in the same session would mask the
    intended state.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM operator_audit")
    conn.commit()


def _insert_setup_audit_event(conn: psycopg.Connection[tuple]) -> None:
    """Insert a synthetic ``event_type='setup'`` row into
    ``operator_audit`` to simulate that ``/auth/setup`` has been run on
    this DB historically (even if the operator row was subsequently
    deleted). Issue #1363 Codex 2 P2 round 2: this is the load-bearing
    signal that distinguishes post-setup corruption from a true
    cold-start.
    """
    import uuid

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO operator_audit (event_type, target_operator_id, target_username) VALUES ('setup', %s, %s)",
            (str(uuid.uuid4()), f"synthetic_setup_{uuid.uuid4().hex[:8]}"),
        )
    conn.commit()


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
    def test_operator_absent_post_install_persists_breadcrumb_and_exits_2(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression: when ``bootstrap_state.status`` is non-pending
        (post-install) AND operators is empty (someone deleted the
        singleton row), the wrapper MUST hard-fail with breadcrumb +
        SystemExit(2). Issue #1363 cold-start tolerance must NOT
        weaken this path."""
        _delete_all_operators(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "complete")
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


# --------------------------------------------------------------------- #
# 3. Cold-start tolerance (#1363) — pure ``is_cold_start_state`` checks
#    + wrapper integration tests for the deferred-boot branch.
# --------------------------------------------------------------------- #


class TestIsColdStartStatePureFunction:
    """Pure-function contract for ``is_cold_start_state`` — mocked cursor,
    no DB. ``True`` only when ``bootstrap_state.status='pending'``."""

    def test_returns_true_when_status_is_pending(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = ("pending",)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert is_cold_start_state(conn) is True
        cur.execute.assert_called_once_with("SELECT status FROM bootstrap_state WHERE id = 1")

    @pytest.mark.parametrize("status", ["running", "complete", "partial_error", "cancelled"])
    def test_returns_false_for_any_non_pending_status(self, status: str) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (status,)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert is_cold_start_state(conn) is False

    def test_returns_false_when_singleton_row_missing(self) -> None:
        """Defensive: ``bootstrap_state`` singleton should be guaranteed
        by the earlier ``_ensure_bootstrap_state_singleton_with_cleanup``
        guard, but if it ever vanishes, fail closed (treat as
        post-install so the hard-fail path runs and surfaces the
        anomaly)."""
        cur = MagicMock()
        cur.fetchone.return_value = None
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert is_cold_start_state(conn) is False


class TestIsColdStartStateAgainstRealDB:
    """The pure function against a real DB — verifies the SELECT query
    actually reads ``bootstrap_state.status`` correctly."""

    @pytest.mark.integration
    def test_returns_true_for_pending(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        _set_bootstrap_state_status(ebull_test_conn, "pending")

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            assert is_cold_start_state(guard_conn) is True

    @pytest.mark.integration
    def test_returns_false_for_complete(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        _set_bootstrap_state_status(ebull_test_conn, "complete")

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            assert is_cold_start_state(guard_conn) is False


class TestColdStartToleranceWrapper:
    """Issue #1363 — when ``operators`` is empty AND
    ``bootstrap_state.status='pending'``, the wrapper MUST return
    cleanly without persisting a breadcrumb and without SystemExit.

    This is the cold-start window: ``/auth/setup`` has not been run
    yet, so operator absence is the expected initial state — the jobs
    process should boot alongside backend + frontend rather than crash
    and leave the operator with a dead worker after they complete
    setup via the UI."""

    @pytest.mark.integration
    def test_cold_start_returns_without_breadcrumb_or_exit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "pending")
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE bootstrap_state SET last_jobs_boot_error = NULL WHERE id = 1")
        ebull_test_conn.commit()

        monkeypatch.delenv("EBULL_JOBS_SKIP_OPERATOR_CHECK", raising=False)
        monkeypatch.setattr(settings, "database_url", test_database_url())
        fence_conn = MagicMock()
        pool = MagicMock()

        _wrapper()(fence_conn, pool)

        message, at = _read_last_jobs_boot_error(ebull_test_conn)
        assert message is None, "cold-start MUST NOT write a breadcrumb"
        assert at is None, "cold-start MUST NOT write a breadcrumb timestamp"
        fence_conn.close.assert_not_called()
        pool.close.assert_not_called()

    @pytest.mark.integration
    def test_cold_start_clears_matching_operator_missing_breadcrumb(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Codex 2 P2 fold: when a prior hard-fail boot wrote the
        ``OPERATOR_MISSING_ERROR_MESSAGE`` breadcrumb, and the system
        then transitioned to cold-start state (e.g. DB wipe), the
        cold-start branch MUST clear that specific stale message so
        ``/system/status`` does not lie about jobs being blocked while
        the jobs process is in fact running."""
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "pending")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE bootstrap_state "
                "SET last_jobs_boot_error = %s, last_jobs_boot_error_at = clock_timestamp() "
                "WHERE id = 1",
                (OPERATOR_MISSING_ERROR_MESSAGE,),
            )
        ebull_test_conn.commit()

        monkeypatch.delenv("EBULL_JOBS_SKIP_OPERATOR_CHECK", raising=False)
        monkeypatch.setattr(settings, "database_url", test_database_url())
        fence_conn = MagicMock()
        pool = MagicMock()

        _wrapper()(fence_conn, pool)

        message, at = _read_last_jobs_boot_error(ebull_test_conn)
        assert message is None, "cold-start MUST clear a matching OPERATOR_MISSING breadcrumb"
        assert at is None, "matching breadcrumb timestamp MUST be cleared with the message"
        fence_conn.close.assert_not_called()
        pool.close.assert_not_called()

    @pytest.mark.integration
    def test_cold_start_preserves_unrelated_breadcrumb(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Companion to the matching-clear test: a breadcrumb written
        by a different boot-failure cause (not the OPERATOR_MISSING
        message) MUST be preserved — the cold-start branch only knows
        about its own prior hard-fail artefact, so unrelated messages
        from future guards are left visible to ``/system/status``."""
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "pending")
        unrelated_message = "jobs boot blocked: some other guard fired"
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE bootstrap_state "
                "SET last_jobs_boot_error = %s, last_jobs_boot_error_at = clock_timestamp() "
                "WHERE id = 1",
                (unrelated_message,),
            )
        ebull_test_conn.commit()

        monkeypatch.delenv("EBULL_JOBS_SKIP_OPERATOR_CHECK", raising=False)
        monkeypatch.setattr(settings, "database_url", test_database_url())
        fence_conn = MagicMock()
        pool = MagicMock()

        _wrapper()(fence_conn, pool)

        message, _at = _read_last_jobs_boot_error(ebull_test_conn)
        assert message == unrelated_message, "cold-start branch MUST NOT clear breadcrumbs written by other guards"
        fence_conn.close.assert_not_called()
        pool.close.assert_not_called()


class TestReadBootGateSnapshotPureFunction:
    """Pure-function contract for ``read_boot_gate_snapshot`` — mocked
    cursor, no DB. Issue #1363 Codex 2 P2: returns ``(operator_present,
    is_cold_start)`` in a single statement-level snapshot.

    SQL returns ``(bool, bool)`` directly — the cold-start composition
    (``status='pending' AND NOT EXISTS setup audit``) is evaluated
    server-side. Pure-function tests just shape-check the projection."""

    def test_returns_both_true_when_operator_present_and_cold_start(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (True, True)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert read_boot_gate_snapshot(conn) == (True, True)

    def test_returns_operator_present_false_cold_start_true(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (False, True)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert read_boot_gate_snapshot(conn) == (False, True)

    def test_returns_operator_present_true_cold_start_false(self) -> None:
        cur = MagicMock()
        cur.fetchone.return_value = (True, False)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert read_boot_gate_snapshot(conn) == (True, False)

    def test_returns_false_false_when_bootstrap_state_missing(self) -> None:
        """Server-side COALESCE in the SQL collapses missing bootstrap
        state to ``''`` (≠ ``'pending'``) so the cold-start composition
        evaluates to False. Defensive fail-closed posture mirrors
        :func:`is_cold_start_state`."""
        cur = MagicMock()
        cur.fetchone.return_value = (False, False)
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        assert read_boot_gate_snapshot(conn) == (False, False)


class TestReadBootGateSnapshotAgainstRealDB:
    """Real-DB read of the combined snapshot. Each test resets the
    three sources of truth (operators / bootstrap_state.status /
    operator_audit) so the cold-start composition is deterministic."""

    @pytest.mark.integration
    def test_returns_present_and_cold_start_true_when_seeded_and_no_setup_audit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """``_insert_test_operator`` bypasses :func:`perform_setup` so no
        ``'setup'`` event is written to ``operator_audit`` — that
        combined with ``status='pending'`` is the cold-start composition
        even though an operator row happens to exist (the wrapper only
        consumes ``is_cold_start`` on the OPERATOR_ABSENT branch, but
        the projection is uniform)."""
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _insert_test_operator(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "pending")

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            assert read_boot_gate_snapshot(guard_conn) == (True, True)

    @pytest.mark.integration
    def test_returns_absent_and_post_install_when_status_complete(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "complete")

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            assert read_boot_gate_snapshot(guard_conn) == (False, False)

    @pytest.mark.integration
    def test_returns_absent_and_cold_start_false_when_setup_audit_present(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Codex 2 P2 round 2 fold: post-setup corruption case —
        ``operators`` empty AND ``bootstrap_state.status='pending'``
        BUT a prior ``'setup'`` audit row exists. This is NOT a
        cold-start; the missing operator row is corruption and MUST
        flow to the hard-fail path."""
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "pending")
        _insert_setup_audit_event(ebull_test_conn)

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            assert read_boot_gate_snapshot(guard_conn) == (False, False)


class TestPostSetupOperatorAbsenceStillHardFails:
    """Issue #1363 Codex 2 P2 round 2: the cold-start branch MUST NOT
    swallow a post-setup operator-absence. If ``/auth/setup`` has run
    historically (``operator_audit`` carries a ``'setup'`` event) AND
    the operator row was later deleted, the wrapper MUST hard-fail
    regardless of ``bootstrap_state.status``."""

    @pytest.mark.integration
    def test_post_setup_pending_status_with_no_operator_hard_fails(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _delete_all_operators(ebull_test_conn)
        _delete_all_operator_audit(ebull_test_conn)
        _set_bootstrap_state_status(ebull_test_conn, "pending")
        _insert_setup_audit_event(ebull_test_conn)
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
        assert message == OPERATOR_MISSING_ERROR_MESSAGE, (
            "post-setup operator absence MUST persist the hard-fail breadcrumb regardless of status='pending'"
        )
        assert at is not None
        fence_conn.close.assert_called_once()
        pool.close.assert_called_once()
