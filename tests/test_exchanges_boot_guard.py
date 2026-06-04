"""Tests for `ensure_exchanges_seeded` boot guard (#1270).

`exchanges` is reference data seeded once by sql/067; nothing reseeds it
after a clean-DB wipe, and an empty `exchanges` table silently empties
the us_equity issuer cohort → all issuer-side SEC ingest no-ops. The
boot guard reseeds the canonical rows when (and only when) the table is
empty.

Contract: like the `ensure_*_singleton` guards, it requires an
autocommit connection (opens its own real BEGIN). Tests mirror the
boot-time call shape with a fresh `psycopg.connect(..., autocommit=True)`
and use the `ebull_test_conn` fixture (same worker DB) for setup/asserts.
`exchanges` is NOT in the per-test TRUNCATE list; the template seeds it
with exactly the 8 canonical rows (instruments is empty at migration
time), so the empty→reseed tests self-restore to the original state.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from app.services.exchanges import _CANONICAL_EXCHANGE_SEED, ensure_exchanges_seeded
from tests.fixtures.ebull_test_db import (
    ebull_test_conn,  # noqa: F401 — fixture re-export
    test_database_url,
)


@pytest.fixture(autouse=True)
def _restore_exchanges(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[None]:
    """Snapshot + restore the FULL `exchanges` table around each test.

    `exchanges` is reference data NOT in the per-test TRUNCATE list, and
    these tests `DELETE FROM exchanges`. The reseed via `ensure_exchanges_
    seeded` only restores (exchange_id, country, asset_class) — NOT the
    migration-071 capability columns (filings/analyst/...) — so without a
    full restore, sibling tests on the same xdist worker (e.g.
    test_migration_071) see an `exchanges` table stripped of its capability
    data and fail. Snapshot every column before, restore after (#1445).
    """
    ebull_test_conn.rollback()
    ebull_test_conn.execute("DROP TABLE IF EXISTS _exchanges_backup")
    ebull_test_conn.execute("CREATE TEMP TABLE _exchanges_backup AS TABLE exchanges")
    ebull_test_conn.commit()
    yield
    ebull_test_conn.rollback()
    ebull_test_conn.execute("DELETE FROM exchanges")
    ebull_test_conn.execute("INSERT INTO exchanges SELECT * FROM _exchanges_backup")
    ebull_test_conn.execute("DROP TABLE _exchanges_backup")
    ebull_test_conn.commit()


def _rows(conn: psycopg.Connection[tuple]) -> set[tuple[str, str]]:
    conn.rollback()  # drop any stale snapshot so we read committed state
    return {(r[0], r[1]) for r in conn.execute("SELECT exchange_id, asset_class FROM exchanges").fetchall()}


def test_reseeds_canonical_rows_when_empty(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    ebull_test_conn.execute("DELETE FROM exchanges")
    ebull_test_conn.commit()

    with psycopg.connect(test_database_url(), autocommit=True) as guard_conn:
        ensure_exchanges_seeded(guard_conn)

    rows = _rows(ebull_test_conn)
    # All eight canonical rows present (= original template state restored).
    assert rows == {(eid, cls) for (eid, _country, cls) in _CANONICAL_EXCHANGE_SEED}
    assert {eid for (eid, cls) in rows if cls == "us_equity"} == {"2", "4", "5", "6", "7", "19", "20"}


def test_noop_when_table_already_populated(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # Migration-seeded → non-empty. Add a curated sentinel the guard must
    # NOT touch or duplicate.
    ebull_test_conn.execute(
        "INSERT INTO exchanges (exchange_id, country, asset_class) "
        "VALUES ('99', 'GB', 'uk_equity') ON CONFLICT (exchange_id) DO NOTHING"
    )
    ebull_test_conn.commit()
    try:
        before = _rows(ebull_test_conn)
        assert ("99", "uk_equity") in before

        with psycopg.connect(test_database_url(), autocommit=True) as guard_conn:
            ensure_exchanges_seeded(guard_conn)

        # No-op: guard fires only on an empty table; curated row intact.
        assert _rows(ebull_test_conn) == before
    finally:
        ebull_test_conn.execute("DELETE FROM exchanges WHERE exchange_id = '99'")
        ebull_test_conn.commit()


def test_idempotent_on_repeated_calls(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    ebull_test_conn.execute("DELETE FROM exchanges")
    ebull_test_conn.commit()

    with psycopg.connect(test_database_url(), autocommit=True) as guard_conn:
        ensure_exchanges_seeded(guard_conn)
        ensure_exchanges_seeded(guard_conn)  # second call: now non-empty → no-op

    assert len(_rows(ebull_test_conn)) == len(_CANONICAL_EXCHANGE_SEED)


def test_raises_when_caller_is_not_autocommit(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A non-autocommit conn would degrade the reseed BEGIN into a
    SAVEPOINT — fail loud at the boundary (mirrors the singleton guards)."""
    with pytest.raises(RuntimeError, match="autocommit"):
        ensure_exchanges_seeded(ebull_test_conn)
