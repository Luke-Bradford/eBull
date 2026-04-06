"""
Tests for app.services.runtime_config.

Covers:
  - get_runtime_config: happy path, missing row -> RuntimeConfigCorrupt
  - update_runtime_config: partial update, atomicity, audit-row-per-changed-field,
    no-op vs change distinction, missing-row fail-closed, empty-patch ValueError
  - write_kill_switch_audit: row shape

Mock DB approach mirrors other service test files in this repo.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.runtime_config import (
    RuntimeConfig,
    RuntimeConfigCorrupt,
    get_runtime_config,
    update_runtime_config,
    write_kill_switch_audit,
)

_NOW = datetime(2026, 4, 7, 12, 0, 0, tzinfo=UTC)


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    conn = MagicMock()
    cursor_iter = iter(cursors)
    conn.cursor.side_effect = lambda **kwargs: next(cursor_iter)
    conn.execute.return_value = MagicMock(rowcount=1)
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    return conn


def _row(auto: bool = False, live: bool = False) -> dict[str, Any]:
    return {
        "enable_auto_trading": auto,
        "enable_live_trading": live,
        "updated_at": _NOW,
        "updated_by": "seed",
        "reason": "seed",
    }


# ---------------------------------------------------------------------------
# TestGetRuntimeConfig
# ---------------------------------------------------------------------------


class TestGetRuntimeConfig:
    def test_returns_runtime_config(self) -> None:
        conn = _make_conn([_make_cursor([_row(auto=True, live=False)])])
        rc = get_runtime_config(conn)
        assert isinstance(rc, RuntimeConfig)
        assert rc.enable_auto_trading is True
        assert rc.enable_live_trading is False
        assert rc.updated_by == "seed"

    def test_missing_row_raises_corrupt(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(RuntimeConfigCorrupt, match="missing"):
            get_runtime_config(conn)


# ---------------------------------------------------------------------------
# TestUpdateRuntimeConfig
# ---------------------------------------------------------------------------


class TestUpdateRuntimeConfig:
    def test_empty_patch_raises_value_error(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="at least one flag"):
            update_runtime_config(conn, updated_by="op", reason="r")

    def test_missing_row_raises_corrupt(self) -> None:
        conn = _make_conn([_make_cursor([])])  # SELECT FOR UPDATE returns nothing
        with pytest.raises(RuntimeConfigCorrupt, match="cannot update"):
            update_runtime_config(
                conn,
                updated_by="op",
                reason="r",
                enable_auto_trading=True,
            )

    def test_full_update_writes_two_audit_rows(self) -> None:
        # Both flags change: expect UPDATE + 2 audit INSERTs = 3 conn.execute calls
        conn = _make_conn([_make_cursor([_row(auto=False, live=False)])])
        updated = update_runtime_config(
            conn,
            updated_by="op",
            reason="enable everything",
            enable_auto_trading=True,
            enable_live_trading=True,
            now=_NOW,
        )
        assert updated.enable_auto_trading is True
        assert updated.enable_live_trading is True

        # 1 UPDATE + 2 audit INSERTs
        assert conn.execute.call_count == 3
        sqls = [c[0][0] for c in conn.execute.call_args_list]
        assert any("UPDATE runtime_config" in s for s in sqls)
        audit_calls = [c for c in conn.execute.call_args_list if "runtime_config_audit" in c[0][0]]
        assert len(audit_calls) == 2

        fields = {c[0][1]["field"] for c in audit_calls}
        assert fields == {"enable_auto_trading", "enable_live_trading"}

    def test_partial_update_only_writes_audit_for_changed_field(self) -> None:
        conn = _make_conn([_make_cursor([_row(auto=False, live=False)])])
        update_runtime_config(
            conn,
            updated_by="op",
            reason="auto on",
            enable_auto_trading=True,
            now=_NOW,
        )
        # 1 UPDATE + 1 audit row for enable_auto_trading
        assert conn.execute.call_count == 2
        audit_calls = [c for c in conn.execute.call_args_list if "runtime_config_audit" in c[0][0]]
        assert len(audit_calls) == 1
        assert audit_calls[0][0][1]["field"] == "enable_auto_trading"
        assert audit_calls[0][0][1]["old"] == "false"
        assert audit_calls[0][0][1]["new"] == "true"

    def test_no_op_update_writes_no_audit_row(self) -> None:
        # Patch sets enable_auto_trading=True but the row already has True.
        # The UPDATE still runs (stamps updated_at/updated_by/reason) but no
        # audit row is written because the value did not change.
        conn = _make_conn([_make_cursor([_row(auto=True, live=False)])])
        update_runtime_config(
            conn,
            updated_by="op",
            reason="noop",
            enable_auto_trading=True,
            now=_NOW,
        )
        # 1 UPDATE only — no audit insert
        assert conn.execute.call_count == 1
        assert "UPDATE runtime_config" in conn.execute.call_args_list[0][0][0]

    def test_atomic_via_transaction(self) -> None:
        conn = _make_conn([_make_cursor([_row(auto=False, live=False)])])
        update_runtime_config(
            conn,
            updated_by="op",
            reason="r",
            enable_live_trading=True,
            now=_NOW,
        )
        # Single transaction context wraps the SELECT FOR UPDATE + UPDATE + audit
        conn.transaction.assert_called_once()


# ---------------------------------------------------------------------------
# TestWriteKillSwitchAudit
# ---------------------------------------------------------------------------


class TestWriteKillSwitchAudit:
    def test_writes_audit_row_with_field_kill_switch(self) -> None:
        conn = _make_conn([])
        write_kill_switch_audit(
            conn,
            changed_by="ops",
            reason="emergency",
            old_active=False,
            new_active=True,
            now=_NOW,
        )
        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert "INSERT INTO runtime_config_audit" in sql
        assert params["field"] == "kill_switch"
        assert params["old"] == "false"
        assert params["new"] == "true"
        assert params["by"] == "ops"
        assert params["reason"] == "emergency"

    def test_old_active_none_serializes_to_null(self) -> None:
        conn = _make_conn([])
        write_kill_switch_audit(
            conn,
            changed_by="op",
            reason="bootstrap",
            old_active=None,
            new_active=True,
            now=_NOW,
        )
        params = conn.execute.call_args[0][1]
        assert params["old"] is None
        assert params["new"] == "true"
