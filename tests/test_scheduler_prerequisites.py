"""Unit tests for scheduler prerequisite functions.

Covers _has_scoreable_instruments which replaced _has_scores as the
prerequisite for morning_candidate_review (fix for #252 bootstrap
deadlock).

No live database — uses a mock connection with canned query results.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.workers.scheduler import _has_scoreable_instruments


def _mock_conn(exists_result: bool) -> MagicMock:
    """Build a mock psycopg Connection that returns *exists_result* for
    a ``SELECT EXISTS(...)`` query."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.execute.return_value = cursor
    cursor.fetchone.return_value = (exists_result,)
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


class TestHasScoreableInstruments:
    """Verify _has_scoreable_instruments prerequisite logic."""

    def test_passes_when_scoreable_instruments_exist(self) -> None:
        """Fundamentals exist, scores table empty — prerequisite should pass."""
        conn = _mock_conn(exists_result=True)
        result, reason = _has_scoreable_instruments(conn)
        assert result is True
        assert reason == ""

    def test_fails_when_no_scoreable_instruments(self) -> None:
        """No instruments with any data — prerequisite should fail."""
        conn = _mock_conn(exists_result=False)
        result, reason = _has_scoreable_instruments(conn)
        assert result is False
        assert reason == "no scoreable instruments"

    def test_query_checks_instruments_with_data(self) -> None:
        """Verify the SQL checks for theses, fundamentals, or price data."""
        conn = _mock_conn(exists_result=True)
        _has_scoreable_instruments(conn)

        cursor = conn.cursor.return_value.__enter__.return_value
        sql_arg = cursor.execute.call_args[0][0]
        # Access the raw template string from psycopg.sql.SQL — _obj is
        # the underlying str.  as_string() requires a real connection so
        # it returns a MagicMock on a mock conn (vacuous assertions).
        sql_text = sql_arg._obj
        assert isinstance(sql_text, str), "sql_arg._obj should be a plain string"

        # Must join coverage and check for at least one data source
        assert "coverage" in sql_text
        assert "is_tradable" in sql_text
        assert "theses" in sql_text
        assert "fundamentals_snapshot" in sql_text
        assert "price_daily" in sql_text
