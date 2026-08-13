"""PR12 PG >= 17 boot-time guard (#1233 spec §7).

Two cases:
- pg17_dev_db: the real test DB is PG17; the guard must not raise.
- simulated_pg16: a duck-typed FakeConn returns 160005 from
  ``current_setting('server_version_num')`` → guard must raise
  ``RuntimeError`` with the canonical "PG >= 17" message fragment.

No DB monkeypatch needed for the simulated case — the guard only
touches ``.cursor()``, so a minimal duck-type covers the contract.
"""

from __future__ import annotations

from typing import Any, cast

import psycopg
import pytest

from app.system.postgres_version_guard import (
    PG_MERGE_NOT_MATCHED_BY_SOURCE_MIN_VERSION_NUM,
    assert_postgres_min_version,
)


def test_guard_passes_on_pg17(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """Real PG17 dev DB — guard must not raise."""
    assert_postgres_min_version(ebull_test_conn)


def test_guard_fails_on_simulated_pg16() -> None:
    """Duck-typed FakeConn → 160005 (PG16.5) — guard raises RuntimeError."""

    class FakeCursor:
        def __enter__(self) -> FakeCursor:
            return self

        def __exit__(self, *a: object) -> bool:
            return False

        def execute(self, *a: object, **k: object) -> None:
            return None

        def fetchone(self) -> tuple[int]:
            return (160005,)

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

    with pytest.raises(RuntimeError, match="PG >= 17"):
        assert_postgres_min_version(cast(Any, FakeConn()))


def test_guard_fails_when_fetchone_returns_none() -> None:
    """`current_setting` should always return a row; defensive branch
    still tested so future refactors keep the message contract."""

    class FakeCursor:
        def __enter__(self) -> FakeCursor:
            return self

        def __exit__(self, *a: object) -> bool:
            return False

        def execute(self, *a: object, **k: object) -> None:
            return None

        def fetchone(self) -> None:
            return None

    class FakeConn:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

    with pytest.raises(RuntimeError, match="PG >= 17"):
        assert_postgres_min_version(cast(Any, FakeConn()))


def test_floor_constant_matches_pg17_zero() -> None:
    """Pin the constant so a typo (160000) cannot silently weaken
    the guard. PG 17.0 → 170000 per Postgres server_version_num encoding."""
    assert PG_MERGE_NOT_MATCHED_BY_SOURCE_MIN_VERSION_NUM == 170000
