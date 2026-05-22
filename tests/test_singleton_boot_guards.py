"""Tests for ``ensure_kill_switch_singleton`` + ``ensure_bootstrap_state_singleton`` (#1232).

Mirror of ``test_runtime_config_boot_guard.py`` (#1208 Sub 6). Uses the
real ``ebull_test_conn`` fixture and a separate autocommit
``psycopg.connect`` for the helper invocation — matches the boot-time
shape in ``app/main.py`` lifespan.
"""

from __future__ import annotations

import psycopg
import psycopg.rows
import pytest

from app.services.bootstrap_state import ensure_bootstrap_state_singleton
from app.services.budget import ensure_budget_config_singleton
from app.services.ops_monitor import ensure_kill_switch_singleton
from app.services.runtime_config import (
    BOOT_RECOVERY_CHANGED_BY,
    BOOT_RECOVERY_REASON,
)
from app.services.transaction_cost import ensure_transaction_cost_config_singleton
from tests.fixtures.ebull_test_db import test_database_url


class TestEnsureKillSwitchSingleton:
    def test_noop_when_row_exists(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Template DB carries the seeded row; helper must be a quiet no-op."""
        caplog.set_level("WARNING", logger="app.services.ops_monitor")
        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_kill_switch_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM kill_switch")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert not any("singleton vanished" in record.message for record in caplog.records)

    def test_reseeds_when_row_missing_with_audit_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Row dropped → helper re-seeds + writes one runtime_config_audit row."""
        caplog.set_level("WARNING", logger="app.services.ops_monitor")
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM kill_switch WHERE id = TRUE")
            cur.execute("DELETE FROM runtime_config_audit WHERE field = 'kill_switch'")
        ebull_test_conn.commit()

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_kill_switch_singleton(guard_conn)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT id, is_active, activated_at, activated_by, reason FROM kill_switch")
            row = cur.fetchone()
        assert row is not None
        assert row["id"] is True
        assert row["is_active"] is False
        assert row["activated_at"] is None
        assert row["activated_by"] is None
        assert row["reason"] is None
        assert any("singleton vanished" in record.message for record in caplog.records)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT field, old_value, new_value, changed_by, reason
                FROM runtime_config_audit
                WHERE field = 'kill_switch'
                """
            )
            audit_rows = cur.fetchall()
        assert len(audit_rows) == 1
        audit = audit_rows[0]
        assert audit["field"] == "kill_switch"
        assert audit["old_value"] is None
        assert audit["new_value"] == "false"
        assert audit["changed_by"] == BOOT_RECOVERY_CHANGED_BY
        assert audit["reason"] == BOOT_RECOVERY_REASON

    def test_atomic_failure_rolls_back_seed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Audit-insert failure rolls back the seed — proves real BEGIN not SAVEPOINT."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM kill_switch WHERE id = TRUE")
            cur.execute("DELETE FROM runtime_config_audit WHERE field = 'kill_switch'")
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

        # Patched at the import site in ops_monitor (where the helper looks
        # the symbol up).
        monkeypatch.setattr("app.services.ops_monitor.insert_runtime_config_audit_row", _boom)

        url = test_database_url()
        with pytest.raises(RuntimeError, match="simulated audit insert failure"):
            with psycopg.connect(url, autocommit=True) as guard_conn:
                ensure_kill_switch_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM kill_switch")
            count_row = cur.fetchone()
        assert count_row is not None
        assert count_row[0] == 0

    def test_raises_when_caller_is_not_autocommit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Helper must reject non-autocommit conn (SAVEPOINT-not-BEGIN guard)."""
        assert ebull_test_conn.autocommit is False
        with pytest.raises(RuntimeError, match="requires an autocommit connection"):
            ensure_kill_switch_singleton(ebull_test_conn)

    def test_raises_on_non_canonical_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Non-canonical row (id != TRUE) → fail loud, not silently re-insert."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM kill_switch")
            cur.execute("ALTER TABLE kill_switch DROP CONSTRAINT kill_switch_single_row")
            cur.execute("INSERT INTO kill_switch (id, is_active) VALUES (FALSE, FALSE)")
        ebull_test_conn.commit()

        url = test_database_url()
        try:
            with pytest.raises(RuntimeError, match="singleton constraint violated"):
                with psycopg.connect(url, autocommit=True) as guard_conn:
                    ensure_kill_switch_singleton(guard_conn)
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute("DELETE FROM kill_switch")
                cur.execute("ALTER TABLE kill_switch ADD CONSTRAINT kill_switch_single_row CHECK (id = TRUE)")
                cur.execute("INSERT INTO kill_switch (id, is_active) VALUES (TRUE, FALSE)")
            ebull_test_conn.commit()


class TestEnsureBootstrapStateSingleton:
    def test_noop_when_row_exists(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Template DB carries the seeded row; helper must be a quiet no-op."""
        caplog.set_level("WARNING", logger="app.services.bootstrap_state")
        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_bootstrap_state_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bootstrap_state")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert not any("singleton vanished" in record.message for record in caplog.records)

    def test_reseeds_when_row_missing(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Row dropped → helper re-seeds with status='pending' (column default)."""
        caplog.set_level("WARNING", logger="app.services.bootstrap_state")
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM bootstrap_state WHERE id = 1")
        ebull_test_conn.commit()

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_bootstrap_state_singleton(guard_conn)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT id, status, last_run_id, last_completed_at FROM bootstrap_state")
            row = cur.fetchone()
        assert row is not None
        assert row["id"] == 1
        assert row["status"] == "pending"
        assert row["last_run_id"] is None
        assert row["last_completed_at"] is None
        assert any("singleton vanished" in record.message for record in caplog.records)

    def test_raises_when_caller_is_not_autocommit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Helper must reject non-autocommit conn."""
        assert ebull_test_conn.autocommit is False
        with pytest.raises(RuntimeError, match="requires an autocommit connection"):
            ensure_bootstrap_state_singleton(ebull_test_conn)

    def test_raises_on_non_canonical_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        """Non-canonical row (id != 1) → fail loud."""
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM bootstrap_state")
            cur.execute('ALTER TABLE bootstrap_state DROP CONSTRAINT "bootstrap_state_id_check"')
            cur.execute("INSERT INTO bootstrap_state (id, status) VALUES (99, 'pending')")
        ebull_test_conn.commit()

        url = test_database_url()
        try:
            with pytest.raises(RuntimeError, match="singleton constraint violated"):
                with psycopg.connect(url, autocommit=True) as guard_conn:
                    ensure_bootstrap_state_singleton(guard_conn)
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute("DELETE FROM bootstrap_state")
                cur.execute('ALTER TABLE bootstrap_state ADD CONSTRAINT "bootstrap_state_id_check" CHECK (id = 1)')
                cur.execute("INSERT INTO bootstrap_state (id) VALUES (1)")
            ebull_test_conn.commit()


class TestEnsureBudgetConfigSingleton:
    def test_noop_when_row_exists(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Template DB carries the seeded row; helper must be a quiet no-op."""
        caplog.set_level("WARNING", logger="app.services.budget")
        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_budget_config_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM budget_config")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert not any("singleton vanished" in record.message for record in caplog.records)

    def test_reseeds_when_row_missing(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Row dropped → helper re-seeds with column defaults
        (cash_buffer_pct=0.05, cgt_scenario='higher')."""
        caplog.set_level("WARNING", logger="app.services.budget")
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM budget_config WHERE id = TRUE")
        ebull_test_conn.commit()

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_budget_config_singleton(guard_conn)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT id, cash_buffer_pct, cgt_scenario FROM budget_config")
            row = cur.fetchone()
        assert row is not None
        assert row["id"] is True
        assert str(row["cash_buffer_pct"]) == "0.0500"
        assert row["cgt_scenario"] == "higher"
        assert any("singleton vanished" in record.message for record in caplog.records)

    def test_raises_when_caller_is_not_autocommit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        assert ebull_test_conn.autocommit is False
        with pytest.raises(RuntimeError, match="requires an autocommit connection"):
            ensure_budget_config_singleton(ebull_test_conn)

    def test_raises_on_non_canonical_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM budget_config")
            cur.execute("ALTER TABLE budget_config DROP CONSTRAINT budget_config_single_row")
            cur.execute("INSERT INTO budget_config (id) VALUES (FALSE)")
        ebull_test_conn.commit()

        url = test_database_url()
        try:
            with pytest.raises(RuntimeError, match="singleton constraint violated"):
                with psycopg.connect(url, autocommit=True) as guard_conn:
                    ensure_budget_config_singleton(guard_conn)
        finally:
            with ebull_test_conn.cursor() as cur:
                cur.execute("DELETE FROM budget_config")
                cur.execute("ALTER TABLE budget_config ADD CONSTRAINT budget_config_single_row CHECK (id = true)")
                cur.execute("INSERT INTO budget_config (id) VALUES (TRUE)")
            ebull_test_conn.commit()


class TestEnsureTransactionCostConfigSingleton:
    def test_noop_when_row_exists(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level("WARNING", logger="app.services.transaction_cost")
        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_transaction_cost_config_singleton(guard_conn)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM transaction_cost_config")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
        assert not any("singleton vanished" in record.message for record in caplog.records)

    def test_reseeds_when_row_missing(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level("WARNING", logger="app.services.transaction_cost")
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM transaction_cost_config WHERE id = TRUE")
        ebull_test_conn.commit()

        url = test_database_url()
        with psycopg.connect(url, autocommit=True) as guard_conn:
            ensure_transaction_cost_config_singleton(guard_conn)

        with ebull_test_conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT id FROM transaction_cost_config")
            row = cur.fetchone()
        assert row is not None
        assert row["id"] is True
        assert any("singleton vanished" in record.message for record in caplog.records)

    def test_raises_when_caller_is_not_autocommit(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        assert ebull_test_conn.autocommit is False
        with pytest.raises(RuntimeError, match="requires an autocommit connection"):
            ensure_transaction_cost_config_singleton(ebull_test_conn)
