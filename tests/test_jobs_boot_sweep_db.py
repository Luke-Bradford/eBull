"""#3784: the boot sweep's holder lookup against a real ``sync_runs`` table."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.jobs.boot_sweep import _active_scope


def _bind_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config import settings as app_settings
    from tests.fixtures.ebull_test_db import test_database_url

    monkeypatch.setattr(app_settings, "database_url", test_database_url())


def _sync_run(conn: psycopg.Connection[Any], *, scope: str, status: str) -> int:
    row = conn.execute(
        "INSERT INTO sync_runs (scope, trigger, status, layers_planned) VALUES (%s, 'scheduled', %s, 1) "
        "RETURNING sync_run_id",
        (scope, status),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def test_the_lookup_reads_a_running_holders_scope_and_nothing_else(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _bind_settings_to_test_db(monkeypatch)
    running = _sync_run(ebull_test_conn, scope="high_frequency", status="running")
    assert _active_scope(running) == "high_frequency"
    ebull_test_conn.execute(
        "UPDATE sync_runs SET status = 'complete', finished_at = now() WHERE sync_run_id = %s", (running,)
    )
    ebull_test_conn.commit()
    assert _active_scope(running) is None  # finished between the conflict and the lookup
    assert _active_scope(None) is None
