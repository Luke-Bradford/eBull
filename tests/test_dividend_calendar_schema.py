"""Tests for dividend_calendar parser + schema drift fix (#644).

Covers:
- Migration 082 idempotently adds `last_parsed_at` to a pre-existing
  `dividend_events` table that may have been created without it (the
  CREATE TABLE IF NOT EXISTS in migration 054 short-circuited on some
  databases). Reapplying 082 a second time is a no-op.
- _parse_date contract regression coverage: returns None on the
  ValueError branch (out-of-range day) and the no-match branch.
  The exception clause `except KeyError, ValueError:` looks like
  Python-2 syntax but is valid Python 3.14 (PEP 758) and the project
  pins requires-python>=3.14; tests below pin the contract regardless
  of which form the file is in.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)


class TestParseDateContract:
    """Behavioural pin for `_parse_date`. Both the KeyError branch
    (unrecognised month token) and the ValueError branch (out-of-
    range day) must return None rather than propagate. Test pins
    the contract independent of the exception clause's syntactic
    form, which PEP 758 made flexible on Python 3.14+."""

    def test_parser_returns_date_on_canonical_long_form(self) -> None:
        from datetime import date

        from app.services.dividend_calendar import _parse_date

        # Happy path — proves the parser is wired up + the date()
        # call succeeds for a real input.
        assert _parse_date("April 1, 2026") == date(2026, 4, 1)

    def test_parser_returns_none_on_invalid_day(self) -> None:
        # Take the ValueError branch via date(year, month, day) where
        # day is out of range for the month. The pattern matches the
        # numeric form, the int()s succeed, then `date(2026, 2, 30)`
        # raises ValueError. Without the Python 3 except-tuple syntax
        # fix this branch would never have been reachable — the file
        # would have failed to even import.
        from app.services.dividend_calendar import _parse_date

        assert _parse_date("2/30/2026") is None

    def test_parser_returns_none_when_no_match(self) -> None:
        # Belt-and-suspenders: the `if m is None` guard above the
        # try/except handles unparseable input. Confirms the parser
        # is exception-safe for inputs that don't match _DATE_RE at
        # all.
        from app.services.dividend_calendar import _parse_date

        assert _parse_date("not a date at all") is None

    def test_parser_short_circuits_on_falsy_input(self) -> None:
        # Covers the `if not raw: return None` early-exit guard at
        # the top of `_parse_date`. The KeyError arm of the
        # except-clause is dead code in practice — `_DATE_RE`'s
        # `m1` group is built from `_MONTHS.keys()`, so a regex
        # match guarantees the lookup succeeds. Kept as defensive
        # belt-and-suspenders in the source; not test-worthy here.
        from app.services.dividend_calendar import _parse_date

        assert _parse_date("") is None
        assert _parse_date(None) is None


pytestmark_db = pytest.mark.skipif(
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


@pytestmark_db
class TestMigration082Idempotent:
    """Migration 082 must (a) add the column when missing and
    (b) be a no-op when already present. Both branches must hit on
    the same DB across reruns without raising."""

    def test_column_present_after_migration(self, conn: psycopg.Connection[object]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'dividend_events'
                  AND column_name = 'last_parsed_at'
                """
            )
            row = cur.fetchone()
        assert row is not None, "last_parsed_at column missing — migration 082 did not add it"
        assert row[1].startswith("timestamp")  # type: ignore[index]
        # NOT NULL with DEFAULT NOW() so existing rows backfill cleanly.
        assert row[2] == "NO"  # type: ignore[index]

    def test_replay_is_noop(self, conn: psycopg.Connection[object]) -> None:
        # Run the migration SQL a second time; it must not raise.
        sql_path = Path(__file__).resolve().parents[1] / "sql" / "082_dividend_events_last_parsed_at_backfill.sql"
        sql_text = sql_path.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql_text)  # type: ignore[call-overload]
        # Column still present + still NOT NULL.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT is_nullable FROM information_schema.columns
                WHERE table_name = 'dividend_events' AND column_name = 'last_parsed_at'
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "NO"  # type: ignore[index]
