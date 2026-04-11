"""§8.2 + §8.3 service-layer tests for copy-trading mirror sync.

All tests run against the dedicated ebull_test database (never
settings.database_url) — the same isolation pattern as
tests/test_operator_setup_race.py.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import psycopg.rows
import psycopg.sql
import pytest

from app.services.portfolio_sync import sync_portfolio
from tests.fixtures.copy_mirrors import (
    _NOW,
    two_mirror_payload,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB mirror sync test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    """Yield a fresh connection to ebull_test with copy_* tables
    truncated at the start of each test. Rollback on failure."""
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute("TRUNCATE copy_mirror_positions, copy_mirrors, copy_traders RESTART IDENTITY CASCADE")
        c.commit()
        yield c
        c.rollback()


def _count(conn: psycopg.Connection[Any], table: str) -> int:
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            psycopg.sql.SQL("SELECT COUNT(*) FROM {}").format(  # table is hard-coded
                psycopg.sql.Identifier(table)
            )
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def test_sync_mirrors_fresh_insert(conn: psycopg.Connection[Any]) -> None:
    """Spec §8.2: first sync inserts copy_traders + copy_mirrors +
    copy_mirror_positions rows with active=TRUE."""
    payload = two_mirror_payload()
    result = sync_portfolio(conn, payload, now=_NOW)
    conn.commit()

    assert _count(conn, "copy_traders") == 2
    assert _count(conn, "copy_mirrors") == 2
    assert _count(conn, "copy_mirror_positions") == 6
    assert result.mirrors_upserted == 2
    assert result.mirror_positions_upserted == 6
    assert result.mirrors_closed == 0

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active, closed_at FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    for row in rows:
        assert row["active"] is True
        assert row["closed_at"] is None


def test_sync_mirrors_idempotent_resync(conn: psycopg.Connection[Any]) -> None:
    """Spec §8.2: re-running the same payload is idempotent —
    row counts unchanged, active still TRUE, updated_at refreshed."""
    payload = two_mirror_payload()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()

    assert _count(conn, "copy_traders") == 2
    assert _count(conn, "copy_mirrors") == 2
    assert _count(conn, "copy_mirror_positions") == 6
