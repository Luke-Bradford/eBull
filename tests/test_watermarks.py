"""Unit tests for app.services.watermarks (#269).

Mocks psycopg.Connection at the service boundary — the helper does
plain SELECT / INSERT ON CONFLICT, no driver-specific behaviour to
exercise live. The schema itself is covered by the migration and by
the smoke test that boots the app lifespan.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from psycopg.pq import TransactionStatus

from app.services.watermarks import Watermark, get_watermark, list_keys, set_watermark


def _mock_conn(
    fetchone_return=None,
    fetchall_return=None,
    transaction_status=TransactionStatus.INTRANS,
):
    """Build a MagicMock conn whose `execute().fetchone()` returns the
    given row and whose `execute().fetchall()` returns the given list.

    Each call to `execute()` returns a fresh cursor-like mock with both
    `fetchone` and `fetchall` wired — matches the psycopg3 pattern
    where `conn.execute(...)` returns a cursor you can immediately
    call `fetchone()`/`fetchall()` on. `transaction_status` simulates
    the `conn.info.transaction_status` check `set_watermark` uses to
    enforce its in-transaction invariant; default is INTRANS so tests
    that don't care about the guard behave like legitimate callers.
    """
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_return
    cursor.fetchall.return_value = fetchall_return or []
    conn.execute.return_value = cursor
    conn.info.transaction_status = transaction_status
    return conn


class TestGetWatermark:
    def test_returns_none_when_row_missing(self) -> None:
        conn = _mock_conn(fetchone_return=None)
        result = get_watermark(conn, "sec.tickers", "global")
        assert result is None

    def test_returns_watermark_dataclass_on_hit(self) -> None:
        now = datetime.now(UTC)
        watermark_at = datetime(2026, 4, 15, 20, 5, 57, tzinfo=UTC)
        conn = _mock_conn(
            fetchone_return=(
                "sec.tickers",
                "global",
                "Wed, 15 Apr 2026 20:05:57 GMT",
                watermark_at,
                now,
                "abc123",
            ),
        )
        result = get_watermark(conn, "sec.tickers", "global")
        assert result == Watermark(
            source="sec.tickers",
            key="global",
            watermark="Wed, 15 Apr 2026 20:05:57 GMT",
            watermark_at=watermark_at,
            fetched_at=now,
            response_hash="abc123",
        )

    def test_sends_parameterised_query(self) -> None:
        """Never interpolate source/key into the SQL — they can be
        attacker-controlled in principle (downstream bugs could route
        external identifiers into the source arg). Parameters only."""
        conn = _mock_conn(fetchone_return=None)
        get_watermark(conn, "sec.tickers", "'; DROP TABLE users --")
        call = conn.execute.call_args
        sql, params = call[0]
        assert "%s" in sql
        assert "DROP TABLE" not in sql  # never interpolated into SQL text
        assert params == ("sec.tickers", "'; DROP TABLE users --")


class TestSetWatermark:
    def test_upsert_sends_all_columns(self) -> None:
        conn = _mock_conn()
        now = datetime(2026, 4, 17, 12, 0, 0, tzinfo=UTC)
        set_watermark(
            conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0001140361-26-013192",
            watermark_at=now,
            response_hash="deadbeef",
        )
        sql, params = conn.execute.call_args[0]
        assert "INSERT INTO external_data_watermarks" in sql
        assert "ON CONFLICT (source, key) DO UPDATE" in sql
        assert params == (
            "sec.submissions",
            "0000320193",
            "0001140361-26-013192",
            now,
            "deadbeef",
        )

    def test_defaults_watermark_at_and_hash_to_none(self) -> None:
        conn = _mock_conn()
        set_watermark(
            conn,
            source="frankfurter.latest",
            key="global",
            watermark='"etag-value-123"',
        )
        _sql, params = conn.execute.call_args[0]
        assert params[3] is None
        assert params[4] is None

    @pytest.mark.parametrize(
        "status",
        [
            TransactionStatus.IDLE,
            TransactionStatus.UNKNOWN,
            TransactionStatus.INERROR,
            TransactionStatus.ACTIVE,
        ],
    )
    def test_refuses_to_write_outside_an_open_transaction(self, status: TransactionStatus) -> None:
        """Every non-INTRANS status must raise BEFORE execute is called.
        Autocommit connections report IDLE; closed/broken report UNKNOWN;
        aborted transactions report INERROR; a command mid-flight reports
        ACTIVE. None of them are safe targets for a watermark write —
        committing watermark ahead of data (IDLE/UNKNOWN) or against an
        aborted transaction (INERROR) would silently skip work on the
        next run or produce a cryptic downstream error."""
        conn = _mock_conn(transaction_status=status)
        with pytest.raises(RuntimeError, match="inside an open transaction"):
            set_watermark(
                conn,
                source="sec.submissions",
                key="0000320193",
                watermark="acc-1",
            )
        conn.execute.assert_not_called()

    def test_inerror_produces_dedicated_rollback_hint(self) -> None:
        """INERROR is the trickiest state — PostgreSQL would reject the
        write with 'current transaction is aborted' anyway, but the
        helpful cause ('rollback before retrying') beats the raw error."""
        conn = _mock_conn(transaction_status=TransactionStatus.INERROR)
        with pytest.raises(RuntimeError, match="rollback before retrying"):
            set_watermark(conn, source="s", key="k", watermark="v")


class TestListKeys:
    def test_returns_sorted_keys(self) -> None:
        conn = _mock_conn(
            fetchall_return=[("0000320193",), ("0000789019",), ("0001018724",)],
        )
        keys = list_keys(conn, "sec.submissions")
        assert keys == ["0000320193", "0000789019", "0001018724"]

    def test_returns_empty_list_when_source_has_no_entries(self) -> None:
        conn = _mock_conn(fetchall_return=[])
        assert list_keys(conn, "sec.submissions") == []
