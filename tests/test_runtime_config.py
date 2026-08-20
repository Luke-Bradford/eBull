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
    RuntimeConfigNoOp,
    get_runtime_config,
    is_local_llm_endpoint,
    local_llm_model_violation,
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


def _row(
    auto: bool = False,
    live: bool = False,
    currency: str = "USD",
    llm_provider: str = "openai_compatible",
    llm_base_url: str = "http://localhost:11434/v1",
    llm_model_writer: str = "qwen3:14b",
    llm_model_critic: str = "qwen3:14b",
) -> dict[str, Any]:
    return {
        "enable_auto_trading": auto,
        "enable_live_trading": live,
        "display_currency": currency,
        "llm_provider": llm_provider,
        "llm_base_url": llm_base_url,
        "llm_model_writer": llm_model_writer,
        "llm_model_critic": llm_model_critic,
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
        assert rc.display_currency == "USD"
        assert rc.updated_by == "seed"

    def test_returns_display_currency(self) -> None:
        conn = _make_conn([_make_cursor([_row(currency="GBP")])])
        rc = get_runtime_config(conn)
        assert rc.display_currency == "GBP"

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
        with pytest.raises(ValueError, match="at least one field"):
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
        # Two cursors: SELECT FOR UPDATE, then UPDATE ... RETURNING updated_at.
        conn = _make_conn(
            [
                _make_cursor([_row(auto=False, live=False)]),
                _make_cursor([{"updated_at": _NOW}]),
            ]
        )
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

        # UPDATE goes through the second cursor; conn.execute() is only used
        # for the two audit INSERTs.
        assert conn.execute.call_count == 2
        audit_calls = conn.execute.call_args_list
        assert all("runtime_config_audit" in c[0][0] for c in audit_calls)
        fields = {c[0][1]["field"] for c in audit_calls}
        assert fields == {"enable_auto_trading", "enable_live_trading"}

    def test_partial_update_only_writes_audit_for_changed_field(self) -> None:
        conn = _make_conn(
            [
                _make_cursor([_row(auto=False, live=False)]),
                _make_cursor([{"updated_at": _NOW}]),
            ]
        )
        update_runtime_config(
            conn,
            updated_by="op",
            reason="auto on",
            enable_auto_trading=True,
            now=_NOW,
        )
        # 1 audit row for enable_auto_trading (UPDATE goes via second cursor)
        assert conn.execute.call_count == 1
        audit_call = conn.execute.call_args_list[0]
        assert "runtime_config_audit" in audit_call[0][0]
        assert audit_call[0][1]["field"] == "enable_auto_trading"
        assert audit_call[0][1]["old"] == "false"
        assert audit_call[0][1]["new"] == "true"

    def test_no_op_update_raises(self) -> None:
        # Patch sets enable_auto_trading=True but the row already has True.
        # The patch is rejected so updated_at/updated_by/reason on the
        # singleton can never drift from the audit table.
        conn = _make_conn([_make_cursor([_row(auto=True, live=False)])])
        with pytest.raises(RuntimeConfigNoOp, match="not change"):
            update_runtime_config(
                conn,
                updated_by="op",
                reason="noop",
                enable_auto_trading=True,
                now=_NOW,
            )
        # No UPDATE issued
        assert conn.execute.call_count == 0

    def test_display_currency_update_writes_audit_row(self) -> None:
        conn = _make_conn(
            [
                _make_cursor([_row(auto=False, live=False, currency="USD")]),
                _make_cursor([{"updated_at": _NOW}]),
            ]
        )
        updated = update_runtime_config(
            conn,
            updated_by="op",
            reason="switch to GBP",
            display_currency="GBP",
            now=_NOW,
        )
        assert updated.display_currency == "GBP"
        assert updated.enable_auto_trading is False
        assert updated.enable_live_trading is False

        # 1 audit row for display_currency
        assert conn.execute.call_count == 1
        audit_call = conn.execute.call_args_list[0]
        assert "runtime_config_audit" in audit_call[0][0]
        assert audit_call[0][1]["field"] == "display_currency"
        assert audit_call[0][1]["old"] == "USD"
        assert audit_call[0][1]["new"] == "GBP"

    def test_display_currency_noop_raises(self) -> None:
        conn = _make_conn([_make_cursor([_row(currency="USD")])])
        with pytest.raises(RuntimeConfigNoOp, match="not change"):
            update_runtime_config(
                conn,
                updated_by="op",
                reason="noop",
                display_currency="USD",
                now=_NOW,
            )

    def test_llm_knob_update_writes_audit_rows(self) -> None:
        conn = _make_conn(
            [
                _make_cursor([_row()]),
                _make_cursor([{"updated_at": _NOW}]),
            ]
        )
        updated = update_runtime_config(
            conn,
            updated_by="op",
            reason="flip to anthropic",
            llm_provider="anthropic",
            llm_model_writer="claude-sonnet-4-6",
            now=_NOW,
        )
        assert updated.llm_provider == "anthropic"
        assert updated.llm_model_writer == "claude-sonnet-4-6"
        assert updated.llm_model_critic == "qwen3:14b"
        assert updated.llm_base_url == "http://localhost:11434/v1"

        assert conn.execute.call_count == 2
        fields = {c[0][1]["field"] for c in conn.execute.call_args_list}
        assert fields == {"llm_provider", "llm_model_writer"}

    def test_llm_split_knobs_audit_independently(self) -> None:
        # #1995: writer and critic changed together → one audit row EACH.
        # Both models must be in LOCAL_LLM_MODEL_ALLOWLIST (#2187) — _row()
        # points at localhost, so a fictional tag would now be rejected
        # before the audit path this test is about.
        conn = _make_conn(
            [
                _make_cursor([_row()]),
                _make_cursor([{"updated_at": _NOW}]),
            ]
        )
        updated = update_runtime_config(
            conn,
            updated_by="op",
            reason="deepseek writer, qwen critic",
            llm_model_writer="deepseek-r1:14b",
            llm_model_critic="qwen3:8b",
            now=_NOW,
        )
        assert updated.llm_model_writer == "deepseek-r1:14b"
        assert updated.llm_model_critic == "qwen3:8b"

        fields = {c[0][1]["field"] for c in conn.execute.call_args_list}
        assert fields == {"llm_model_writer", "llm_model_critic"}

    def test_llm_provider_invalid_value_raises(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="llm_provider must be one of"):
            update_runtime_config(conn, updated_by="op", reason="r", llm_provider="gemini")

    def test_llm_base_url_must_be_http(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="llm_base_url must start with"):
            update_runtime_config(conn, updated_by="op", reason="r", llm_base_url="localhost:11434/v1")

    def test_llm_model_writer_must_be_non_empty(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="llm_model_writer must be a non-empty"):
            update_runtime_config(conn, updated_by="op", reason="r", llm_model_writer="  ")

    def test_llm_model_critic_must_be_non_empty(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="llm_model_critic must be a non-empty"):
            update_runtime_config(conn, updated_by="op", reason="r", llm_model_critic="  ")

    def test_llm_noop_raises(self) -> None:
        conn = _make_conn([_make_cursor([_row()])])
        with pytest.raises(RuntimeConfigNoOp, match="not change"):
            update_runtime_config(
                conn,
                updated_by="op",
                reason="noop",
                llm_provider="openai_compatible",
                now=_NOW,
            )

    def test_atomic_via_transaction(self) -> None:
        conn = _make_conn(
            [
                _make_cursor([_row(auto=False, live=False)]),
                _make_cursor([{"updated_at": _NOW}]),
            ]
        )
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


# ---------------------------------------------------------------------------
# Local-model allow-list (#2187)
# ---------------------------------------------------------------------------


class TestIsLocalLlmEndpoint:
    @pytest.mark.parametrize(
        "base_url",
        [
            "http://localhost:11434/v1",
            "http://127.0.0.1:11434/v1",
            "http://LOCALHOST:11434/v1",
            "http://[::1]:11434/v1",
            "http://0.0.0.0:11434/v1",
            # Loopback spellings a string set would silently miss, each
            # one a bypass of the allow-list (Codex ckpt-2).
            "http://127.1:11434/v1",
            "http://2130706433:11434/v1",
            "http://127.0.0.53:11434/v1",
        ],
    )
    def test_local_hosts(self, base_url: str) -> None:
        assert is_local_llm_endpoint(base_url) is True

    @pytest.mark.parametrize(
        "base_url",
        [
            "https://api.openai.com/v1",
            "http://192.168.1.20:8000/v1",
            "http://llm.internal:11434/v1",
            # A host that merely CONTAINS "localhost" is remote.
            "https://localhost.example.com/v1",
            "",
        ],
    )
    def test_remote_or_unparseable_hosts(self, base_url: str) -> None:
        assert is_local_llm_endpoint(base_url) is False


class TestLocalLlmModelViolation:
    _LOCAL = "http://localhost:11434/v1"

    def test_allow_listed_local_model_passes(self) -> None:
        assert (
            local_llm_model_violation(
                provider="openai_compatible", base_url=self._LOCAL, model="qwen3:14b", field="llm_model_writer"
            )
            is None
        )

    def test_oversized_local_model_rejected(self) -> None:
        violation = local_llm_model_violation(
            provider="openai_compatible",
            base_url=self._LOCAL,
            model="mistral-small:latest",
            field="llm_model_writer",
        )
        assert violation is not None
        assert "llm_model_writer" in violation
        assert "mistral-small:latest" in violation

    def test_quantization_variant_is_not_admitted_by_family(self) -> None:
        # The whole point of exact matching: qwen3:14b is Q4_K_M at
        # ~9.3 GB, a q8_0 sibling is ~15 GB. A prefix/family match would
        # wave through the blob this list exists to exclude.
        assert (
            local_llm_model_violation(
                provider="openai_compatible", base_url=self._LOCAL, model="qwen3:14b-q8_0", field="llm_model_writer"
            )
            is not None
        )

    def test_remote_endpoint_exempt(self) -> None:
        assert (
            local_llm_model_violation(
                provider="openai_compatible",
                base_url="https://llm.example.com/v1",
                model="anything-at-all",
                field="llm_model_writer",
            )
            is None
        )

    def test_anthropic_provider_exempt(self) -> None:
        # Cloud model; base_url is irrelevant on that path and nothing is
        # resident locally.
        assert (
            local_llm_model_violation(
                provider="anthropic", base_url=self._LOCAL, model="claude-sonnet-4-6", field="llm_model_critic"
            )
            is None
        )


class TestUpdateRuntimeConfigAllowlist:
    def test_patching_to_oversized_local_model_rejected(self) -> None:
        conn = _make_conn([_make_cursor([_row()])])
        with pytest.raises(ValueError, match="allow-list"):
            update_runtime_config(conn, updated_by="op", reason="r", llm_model_writer="mistral-small:latest", now=_NOW)

    def test_moving_base_url_to_localhost_rechecks_unchanged_models(self) -> None:
        # The resulting-triple rule: only llm_base_url is patched, but it
        # newly subjects the untouched model columns to the local rule.
        conn = _make_conn(
            [_make_cursor([_row(llm_base_url="https://llm.example.com/v1", llm_model_writer="some-huge-remote-model")])]
        )
        with pytest.raises(ValueError, match="allow-list"):
            update_runtime_config(conn, updated_by="op", reason="r", llm_base_url="http://localhost:11434/v1", now=_NOW)
