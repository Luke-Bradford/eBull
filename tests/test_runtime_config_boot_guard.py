"""Tests for ``ensure_runtime_config_singleton`` (#1208 Sub 6).

Uses the real ``ebull_test_conn`` fixture and a separate autocommit
``psycopg.connect`` for the helper invocation — matches the boot-time
shape in ``app/main.py`` lifespan and ``app/jobs/__main__.py``.
"""

from __future__ import annotations

import psycopg
import psycopg.rows
import pytest

from app.services import runtime_config as rc_mod
from app.services.runtime_config import (
    BOOT_RECOVERY_CHANGED_BY,
    BOOT_RECOVERY_REASON,
    ensure_runtime_config_singleton,
)
from tests.fixtures.ebull_test_db import test_database_url


class TestEnsureRuntimeConfigSingleton:
    def test_noop_when_row_exists(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Template DB carries the seeded row; helper must be a quiet no-op."""
        caplog.set_level("WARNING", logger="app.services.runtime_config")
        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_runtime_config_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM runtime_config")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert not any("singleton vanished" in record.message for record in caplog.records)

    def test_reseeds_when_row_missing_with_audit_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Row dropped → helper re-seeds + writes three audit rows."""
        caplog.set_level("WARNING", logger="app.services.runtime_config")
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM runtime_config WHERE id = TRUE")
            cur.execute("DELETE FROM runtime_config_audit")
        ebull_test_conn.commit()

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_runtime_config_singleton(guard_conn)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT enable_auto_trading,
                       enable_live_trading,
                       display_currency,
                       updated_by,
                       reason
                FROM runtime_config
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert row["enable_auto_trading"] is False
        assert row["enable_live_trading"] is False
        assert row["display_currency"] == "GBP"
        assert row["updated_by"] == BOOT_RECOVERY_CHANGED_BY
        assert row["reason"] == BOOT_RECOVERY_REASON
        assert any("singleton vanished" in record.message for record in caplog.records)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT field, old_value, new_value, changed_by, reason
                FROM runtime_config_audit
                ORDER BY field
                """
            )
            audit_rows = cur.fetchall()
        assert [r["field"] for r in audit_rows] == [
            "display_currency",
            "enable_auto_trading",
            "enable_live_trading",
            "llm_base_url",
            "llm_model",
            "llm_provider",
        ]
        assert all(r["old_value"] is None for r in audit_rows)
        assert all(r["changed_by"] == BOOT_RECOVERY_CHANGED_BY for r in audit_rows)
        assert all(r["reason"] == BOOT_RECOVERY_REASON for r in audit_rows)
        by_field = {r["field"]: r["new_value"] for r in audit_rows}
        assert by_field == {
            "display_currency": "GBP",
            "llm_provider": "openai_compatible",
            "llm_base_url": "http://localhost:11434/v1",
            "llm_model": "qwen3:14b",
            "enable_auto_trading": "false",
            "enable_live_trading": "false",
        }

    def test_atomic_failure_rolls_back_seed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Audit-insert failure inside the helper rolls back the seed.

        Proves ``conn.transaction()`` opened a real BEGIN (not a
        SAVEPOINT under a phantom outer tx) and that the seed + audit
        rows land atomically.
        """
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM runtime_config WHERE id = TRUE")
            cur.execute("DELETE FROM runtime_config_audit")
        ebull_test_conn.commit()

        def _boom(
            conn: object,
            *,
            changed_at: object,
            changed_by: str,
            reason: str,
            field: str,
            old_value: str | None,
            new_value: str,
        ) -> None:
            raise RuntimeError("simulated audit insert failure")

        monkeypatch.setattr(rc_mod, "insert_runtime_config_audit_row", _boom)

        url = test_database_url()
        with pytest.raises(RuntimeError, match="simulated audit insert failure"):
            with psycopg.connect(url, autocommit=True) as guard_conn:
                ensure_runtime_config_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM runtime_config")
            count_row = cur.fetchone()
        assert count_row is not None
        assert count_row[0] == 0

    def test_raises_when_caller_is_not_autocommit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Helper must reject a non-autocommit conn (Codex 2 MEDIUM).

        Future caller passing a request-handler conn would have psycopg's
        implicit BEGIN already open, turning the helper's
        ``conn.transaction()`` into a SAVEPOINT under that outer tx and
        silently breaking the atomic-seed-plus-audit invariant.
        """
        # ebull_test_conn is autocommit=False by default. Reuse it
        # directly to exercise the guard.
        assert ebull_test_conn.autocommit is False
        with pytest.raises(RuntimeError, match="requires an autocommit connection"):
            ensure_runtime_config_singleton(ebull_test_conn)

    def test_raises_on_non_canonical_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Non-canonical row (id != TRUE) → fail loud, not silently re-insert.

        Simulates constraint corruption by dropping CHECK + inserting a
        FALSE row. Restores the schema in a try/finally so the worker
        DB stays usable for subsequent tests on the same worker.
        """
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM runtime_config")
            cur.execute("ALTER TABLE runtime_config DROP CONSTRAINT runtime_config_single_row")
            cur.execute(
                """
                INSERT INTO runtime_config
                    (id, enable_auto_trading, enable_live_trading,
                     updated_by, reason, display_currency)
                VALUES (FALSE, FALSE, FALSE,
                        'corrupt', 'corrupt', 'GBP')
                """
            )
        ebull_test_conn.commit()

        url = test_database_url()
        try:
            with pytest.raises(RuntimeError, match="singleton constraint violated"):
                with psycopg.connect(url, autocommit=True) as guard_conn:
                    ensure_runtime_config_singleton(guard_conn)
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute("DELETE FROM runtime_config")
                cur.execute("ALTER TABLE runtime_config ADD CONSTRAINT runtime_config_single_row CHECK (id = TRUE)")
                cur.execute(
                    """
                    INSERT INTO runtime_config
                        (id, enable_auto_trading, enable_live_trading,
                         updated_by, reason, display_currency)
                    VALUES (TRUE, FALSE, FALSE,
                            'test_restore', 'restore singleton', 'GBP')
                    """
                )
            ebull_test_conn.commit()
