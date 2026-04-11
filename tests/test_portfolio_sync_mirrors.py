"""§8.2 + §8.3 service-layer tests for copy-trading mirror sync.

All tests run against the dedicated ebull_test database (never
settings.database_url) — the same isolation pattern as
tests/test_operator_setup_race.py.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from typing import Any

import psycopg
import psycopg.rows
import psycopg.sql
import pytest

from app.providers.implementations.etoro_broker import (
    PortfolioParseError,
    _parse_mirrors_payload,
)
from app.services.portfolio_sync import sync_portfolio
from tests.fixtures.copy_mirrors import (
    _NOW,
    parse_failure_payload,
    two_mirror_payload,
    two_mirror_seed_rows,
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


def test_sync_mirrors_evicts_closed_nested_positions(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.2: a nested position removed from the payload is
    DELETEd from copy_mirror_positions. Sibling positions in the
    same mirror and positions in other mirrors are untouched.
    copy_mirrors.active stays TRUE."""
    payload = two_mirror_payload()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()
    assert _count(conn, "copy_mirror_positions") == 6

    # Remove one nested position from the first mirror and re-sync.
    trimmed_positions = payload.mirrors[0].positions[1:]  # drop pos 1001
    trimmed_mirror = dataclasses.replace(payload.mirrors[0], positions=trimmed_positions)
    trimmed_payload = dataclasses.replace(
        payload,
        mirrors=(trimmed_mirror, payload.mirrors[1]),
    )
    sync_portfolio(conn, trimmed_payload, now=_NOW)
    conn.commit()

    # The removed row is gone, siblings remain.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT position_id FROM copy_mirror_positions
            WHERE mirror_id = %s ORDER BY position_id
            """,
            (payload.mirrors[0].mirror_id,),
        )
        remaining = [r["position_id"] for r in cur.fetchall()]
    assert remaining == [1002, 1003]

    # The other mirror is untouched.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (payload.mirrors[1].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["n"] == 3

    # The mirror row itself is still active.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active FROM copy_mirrors WHERE mirror_id = %s",
            (payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["active"] is True


def test_sync_mirrors_evicts_all_positions_when_mirror_empties(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.2: an empty positions[] evicts every nested row for
    that mirror (exploits Postgres `position_id <> ALL('{}')` === TRUE
    semantics)."""
    payload = two_mirror_payload()
    sync_portfolio(conn, payload, now=_NOW)
    conn.commit()

    empty_mirror = dataclasses.replace(payload.mirrors[0], positions=())
    emptied_payload = dataclasses.replace(
        payload,
        mirrors=(empty_mirror, payload.mirrors[1]),
    )
    sync_portfolio(conn, emptied_payload, now=_NOW)
    conn.commit()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["n"] == 0

    # Mirror B is untouched.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (payload.mirrors[1].mirror_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["n"] == 3


def test_sync_mirrors_partial_disappearance_soft_closes(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.4: a mirror that disappears from the payload
    (while other mirrors are still present) is soft-closed —
    active=FALSE, closed_at=%(now)s. Nested positions are RETAINED
    for audit."""
    two_mirror_seed_rows(conn)
    conn.commit()
    assert _count(conn, "copy_mirrors") == 2

    # Sync a payload that only contains the second mirror.
    full_payload = two_mirror_payload()
    partial_payload = dataclasses.replace(
        full_payload,
        mirrors=(full_payload.mirrors[1],),  # drop mirror A
    )
    result = sync_portfolio(conn, partial_payload, now=_NOW)
    conn.commit()

    # Mirror A: soft-closed, nested positions retained.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT active, closed_at FROM copy_mirrors
            WHERE mirror_id = %s
            """,
            (full_payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["active"] is False
    assert row["closed_at"] == _NOW

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM copy_mirror_positions WHERE mirror_id = %s",
            (full_payload.mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["n"] == 3  # retained for audit

    # Mirror B: still active.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active FROM copy_mirrors WHERE mirror_id = %s",
            (full_payload.mirrors[1].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["active"] is True

    assert result.mirrors_closed == 1


def test_sync_mirrors_recopy_resurrects_closed_mirror(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.4 / §1.2: if eToro reuses a previously-seen
    mirror_id, the ON CONFLICT DO UPDATE clause resets
    active=TRUE, closed_at=NULL so the mirror is live again."""
    two_mirror_seed_rows(conn)
    # Pre-close mirror A so it starts the test soft-closed.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE copy_mirrors
               SET active = FALSE,
                   closed_at = %(closed)s
             WHERE mirror_id = %(mid)s
            """,
            {
                "closed": _NOW,
                "mid": two_mirror_payload().mirrors[0].mirror_id,
            },
        )
    conn.commit()

    # Sync the full payload — mirror A re-appears.
    sync_portfolio(conn, two_mirror_payload(), now=_NOW)
    conn.commit()

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT active, closed_at FROM copy_mirrors WHERE mirror_id = %s",
            (two_mirror_payload().mirrors[0].mirror_id,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row["active"] is True
    assert row["closed_at"] is None


def test_sync_mirrors_total_disappearance_raises(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.4 asymmetry: if the payload mirrors[] is empty but
    local active mirrors exist, raise RuntimeError. Rows survive
    unchanged after the rollback."""
    two_mirror_seed_rows(conn)
    conn.commit()

    empty_payload = dataclasses.replace(two_mirror_payload(), mirrors=())

    with pytest.raises(RuntimeError, match="empty mirrors"):
        sync_portfolio(conn, empty_payload, now=_NOW)
    conn.rollback()

    # Both rows survive as active=TRUE.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert all(r["active"] is True for r in rows)


def test_sync_mirrors_parser_failure_aborts_before_eviction(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.3.3: if _parse_mirrors_payload raises
    PortfolioParseError, the sync transaction is rolled back before
    any upsert or eviction touches the DB. Seed rows survive
    unchanged — this is the regression test for the Codex v3
    finding V parse-and-soft-close hole."""
    two_mirror_seed_rows(conn)
    conn.commit()
    baseline_positions = _count(conn, "copy_mirror_positions")
    assert baseline_positions == 6

    raw_failure = parse_failure_payload()
    with pytest.raises(PortfolioParseError):
        # The failure fires inside the parser — callers of
        # sync_portfolio parse first, then call sync. In production
        # this is get_portfolio → sync_portfolio; in tests we
        # exercise the same ordering explicitly.
        _ = _parse_mirrors_payload(raw_failure)

    # sync_portfolio is never called — rows untouched.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert all(r["active"] is True for r in rows)
    assert _count(conn, "copy_mirror_positions") == baseline_positions


def test_sync_mirrors_known_mirror_top_level_parse_failure_aborts(
    conn: psycopg.Connection[Any],
) -> None:
    """Spec §2.2.2 / §2.3.3: a known mirrorID with a missing
    required top-level field raises PortfolioParseError, NOT
    log-and-skip. The outer _parse_mirrors_payload wraps the
    underlying KeyError. Without this, the sync would interpret
    the known mirror as disappeared and soft-close it — the hole
    Codex v3 finding V identified."""
    two_mirror_seed_rows(conn)
    conn.commit()

    bad_raw = parse_failure_payload()
    # Break the top-level field (not the nested one) this time.
    bad_raw[0]["positions"][0]["units"] = "1.0"  # fix the nested row
    del bad_raw[0]["availableAmount"]  # break the top-level row

    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload(bad_raw)
    assert "15712187" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, KeyError)

    # Seed rows are untouched — sync_portfolio never reached.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT active FROM copy_mirrors ORDER BY mirror_id")
        rows = cur.fetchall()
    assert len(rows) == 2
    assert all(r["active"] is True for r in rows)
