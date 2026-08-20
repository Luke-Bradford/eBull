"""DB-tier tests for the #1919 PR-A SQL mechanisms (sql/218).

One integration test per genuinely-new SQL mechanism (test-quality
skill): thesis_runs insert/finish lifecycle + CHECK constraints, and the
runtime_config LLM-knob columns + audit-field CHECK extension.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.runtime_config import get_runtime_config, update_runtime_config
from app.services.thesis import (
    _finish_thesis_run_ok,
    _insert_thesis_run,
    _record_thesis_run_failure,
)


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed_instrument(conn, instrument_id: int = 9001) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, "THX", "Thesis Runs Test Co"),
    )
    conn.commit()
    return instrument_id


class TestThesisRunsLifecycle:
    def test_insert_running_then_finish_ok(self, conn) -> None:
        iid = _seed_instrument(conn)
        run_id = _insert_thesis_run(
            conn, iid, "manual", provider="openai_compatible", model="qwen3:14b", critic_model="qwen3:14b"
        )
        conn.commit()

        row = conn.execute(
            "SELECT status, trigger, provider, model, finished_at, thesis_id FROM thesis_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row == ("running", "manual", "openai_compatible", "qwen3:14b", None, None)

        # Minimal thesis row to link (audit columns nullable).
        thesis_row = conn.execute(
            """
            INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown,
                                model, provider, prompt_version)
            VALUES (%s, 1, 'value', 'watch', 'memo', 'qwen3:14b', 'openai_compatible', 'v1')
            RETURNING thesis_id
            """,
            (iid,),
        ).fetchone()
        with conn.transaction():
            _finish_thesis_run_ok(conn, run_id, int(thesis_row[0]))

        row = conn.execute(
            "SELECT status, thesis_id, finished_at IS NOT NULL FROM thesis_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row == ("ok", int(thesis_row[0]), True)

    def test_record_failure_writes_error(self, conn) -> None:
        iid = _seed_instrument(conn, 9002)
        run_id = _insert_thesis_run(
            conn, iid, "scheduled", provider="openai_compatible", model="qwen3:14b", critic_model="qwen3:14b"
        )
        conn.commit()

        _record_thesis_run_failure(conn, run_id, ValueError("Writer: unparseable JSON (finish_reason=length)"))

        row = conn.execute(
            "SELECT status, error, finished_at IS NOT NULL FROM thesis_runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row[0] == "failed"
        assert "finish_reason=length" in row[1]
        assert row[2] is True

    def test_trigger_check_rejects_unknown_value(self, conn) -> None:
        iid = _seed_instrument(conn, 9003)
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "INSERT INTO thesis_runs (instrument_id, trigger) VALUES (%s, 'cron')",
                (iid,),
            )
        conn.rollback()

    def test_status_check_rejects_unknown_value(self, conn) -> None:
        iid = _seed_instrument(conn, 9004)
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "INSERT INTO thesis_runs (instrument_id, trigger, status) VALUES (%s, 'manual', 'done')",
                (iid,),
            )
        conn.rollback()


class TestRuntimeConfigLlmKnobs:
    def test_defaults_are_local_first(self, conn) -> None:
        cfg = get_runtime_config(conn)
        assert cfg.llm_provider == "openai_compatible"
        assert cfg.llm_base_url == "http://localhost:11434/v1"
        assert cfg.llm_model_writer == "qwen3:14b"
        assert cfg.llm_model_critic == "qwen3:14b"

    def test_update_writes_audit_rows_per_changed_field(self, conn) -> None:
        updated = update_runtime_config(
            conn,
            updated_by="test",
            reason="flip to anthropic",
            llm_provider="anthropic",
            llm_model_writer="claude-sonnet-4-6",
        )
        assert updated.llm_provider == "anthropic"
        assert updated.llm_model_writer == "claude-sonnet-4-6"
        assert updated.llm_model_critic == "qwen3:14b"  # untouched (#1995 split)
        assert updated.llm_base_url == "http://localhost:11434/v1"  # untouched

        rows = conn.execute(
            """
            SELECT field, old_value, new_value FROM runtime_config_audit
            WHERE field IN ('llm_provider', 'llm_base_url', 'llm_model_writer', 'llm_model_critic')
            ORDER BY field
            """
        ).fetchall()
        assert ("llm_model_writer", "qwen3:14b", "claude-sonnet-4-6") in rows
        # critic untouched → no audit row for it (split knobs audit independently).
        assert not [r for r in rows if r[0] == "llm_model_critic"]
        assert ("llm_provider", "openai_compatible", "anthropic") in rows
        # base_url unchanged → no audit row for it.
        assert not [r for r in rows if r[0] == "llm_base_url"]

        # runtime_config is a singleton OUTSIDE the per-test truncate set —
        # restore the seed values so later tests (any worker order) see the
        # migration defaults.
        conn.execute(
            """
            UPDATE runtime_config
            SET llm_provider = 'openai_compatible', llm_model_writer = 'qwen3:14b'
            WHERE id = TRUE
            """
        )
        conn.execute(
            "DELETE FROM runtime_config_audit"
            " WHERE field IN ('llm_provider', 'llm_base_url', 'llm_model_writer', 'llm_model_critic')"
        )
        conn.commit()

    def test_provider_column_check_rejects_unknown(self, conn) -> None:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE runtime_config SET llm_provider = 'gemini' WHERE id = TRUE")
        conn.rollback()

    def test_service_validation_rejects_bad_values(self, conn) -> None:
        with pytest.raises(ValueError, match="llm_provider must be one of"):
            update_runtime_config(conn, updated_by="t", reason="r", llm_provider="gemini")
        with pytest.raises(ValueError, match="llm_base_url must start with"):
            update_runtime_config(conn, updated_by="t", reason="r", llm_base_url="localhost:11434")
        with pytest.raises(ValueError, match="non-empty"):
            update_runtime_config(conn, updated_by="t", reason="r", llm_model_writer="   ")
