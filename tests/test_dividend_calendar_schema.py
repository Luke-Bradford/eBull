"""Tests for dividend_calendar parser + schema drift fix (#644).

Covers:
- The Python 3 syntax fix for the `_parse_long_form_date` helper —
  the prior `except KeyError, ValueError:` was Python-2 syntax and
  raised SyntaxError when the path was actually exercised.
- Migration 082 idempotently adds `last_parsed_at` to a pre-existing
  `dividend_events` table that may have been created without it (the
  CREATE TABLE IF NOT EXISTS in migration 054 short-circuited on some
  databases). Reapplying 082 a second time is a no-op.
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


class TestParseDateSyntax:
    """Pure import + invocation. The Python-2 except syntax used to
    raise SyntaxError the first time the path was exercised on a
    Python 3 interpreter. Calling the parser with input that takes
    the failure branch proves the fix."""

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

    def test_parser_module_imports_cleanly_on_python3(self) -> None:
        # Belt-and-suspenders for the Python 2 syntax regression: if
        # the `except KeyError, ValueError:` line ever returns, the
        # module would fail to even import. This test passing means
        # the file is at least valid Python 3.
        import importlib

        import app.services.dividend_calendar as mod

        importlib.reload(mod)
        assert hasattr(mod, "_parse_date")


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
