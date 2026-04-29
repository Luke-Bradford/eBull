"""Tests for `_boot_freshness_sweep` (#649A).

Covers:
- Helper dispatches `submit_sync(SyncScope.behind(), trigger='boot_sweep')`.
- `SyncAlreadyRunning` raised by the planner is swallowed (the
  high_frequency tick or a manual sync may have beaten us to the gate).
- Any other exception is logged but not propagated — boot must not
  fail because the recovery sweep failed.
- Migration 083 lets the orchestrator INSERT a row with
  trigger='boot_sweep' (the CHECK constraint accepts it).
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from unittest.mock import patch

import psycopg
import pytest

from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test Postgres not reachable",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[object]]:
    c: psycopg.Connection[object] = psycopg.connect(_test_database_url(), autocommit=True)
    try:
        yield c
    finally:
        c.close()


class TestBootSweepDispatch:
    def test_helper_calls_submit_sync_with_boot_sweep_trigger(self) -> None:
        """The helper must call submit_sync with the new trigger
        value so the audit trail in /sync/runs distinguishes
        recovery-on-startup from operator-clicked manual triggers."""
        from app.main import _boot_freshness_sweep

        captured: dict[str, object] = {}

        def fake_submit(scope: object, trigger: str) -> tuple[int, object]:
            captured["scope"] = scope
            captured["trigger"] = trigger
            return (1, object())

        with patch("app.services.sync_orchestrator.submit_sync", side_effect=fake_submit):
            asyncio.run(_boot_freshness_sweep())

        assert captured["trigger"] == "boot_sweep"
        # SyncScope.behind() returns a frozen dataclass; assert the
        # scope kind without coupling to the exact import path.
        scope = captured["scope"]
        assert getattr(scope, "kind", None) == "behind"

    def test_helper_swallows_sync_already_running(self) -> None:
        """The 5-minute high_frequency APScheduler tick may have
        beaten us to the gate. SyncAlreadyRunning is the expected
        outcome in that race; must not propagate."""
        from app.main import _boot_freshness_sweep
        from app.services.sync_orchestrator.types import SyncAlreadyRunning, SyncScope

        def fake_submit(scope: object, trigger: str) -> tuple[int, object]:
            raise SyncAlreadyRunning(SyncScope.behind(), active_sync_run_id=42)

        with patch("app.services.sync_orchestrator.submit_sync", side_effect=fake_submit):
            # Must not raise — best-effort recovery.
            asyncio.run(_boot_freshness_sweep())

    def test_helper_swallows_arbitrary_exceptions(self) -> None:
        """Any other failure (DB blip, classifier bug, etc.) must
        also be swallowed: the boot path must continue regardless of
        what the recovery sweep encounters."""
        from app.main import _boot_freshness_sweep

        def fake_submit(scope: object, trigger: str) -> tuple[int, object]:
            raise RuntimeError("unexpected planner failure")

        with patch("app.services.sync_orchestrator.submit_sync", side_effect=fake_submit):
            # Must not raise.
            asyncio.run(_boot_freshness_sweep())


class TestMigration083AcceptsBootSweepTrigger:
    """The trigger CHECK constraint on sync_runs must accept the new
    'boot_sweep' value. Without migration 083 the helper would
    INSERT a row that violated the prior {manual, scheduled,
    catch_up} check — the planner catches IntegrityError but the
    recovery sweep would silently never record a row."""

    def test_insert_with_boot_sweep_trigger_succeeds(self, conn: psycopg.Connection[object]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_runs
                    (scope, trigger, started_at, status, layers_planned)
                VALUES ('behind', 'boot_sweep', now(), 'complete', 0)
                RETURNING sync_run_id
                """,
            )
            row = cur.fetchone()
            assert row is not None
            sync_run_id = int(row[0])  # type: ignore[index]

        # Cleanup.
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sync_runs WHERE sync_run_id = %s", (sync_run_id,))

    def test_insert_with_unknown_trigger_still_rejected(self, conn: psycopg.Connection[object]) -> None:
        # Constraint must remain a positive allowlist — the new
        # value gets added but garbage values still fail.
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO sync_runs
                        (scope, trigger, started_at, status, layers_planned)
                    VALUES ('behind', 'not_a_real_trigger', now(), 'complete', 0)
                    """,
                )
