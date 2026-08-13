"""Tests for app.api.coverage — admin coverage surface (#268 Chunk H).

Strategy: override ``get_conn`` with a MagicMock and stub
``conn.execute().fetchone()`` / ``conn.execute().fetchall()`` to
return canned row shapes. Exercises the HTTP shape, status gating
(only insufficient + structurally_young in the drill-down), and
the SQL order-by contract. conftest.py installs a no-op override
on the auth dependency globally.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app


def _mock_conn(fetchone_results: list[Any], fetchall_results: list[list[Any]]) -> MagicMock:
    """Build a MagicMock psycopg connection whose cursor().fetchone()
    / fetchall() cycles through the provided results in order.

    The handler opens ``conn.cursor(row_factory=...)`` as a context
    manager, runs ``cur.execute(sql)``, then calls ``fetchone`` or
    ``fetchall``. Mock matches that shape.
    """
    fo_iter = iter(fetchone_results)
    fa_iter = iter(fetchall_results)

    cur = MagicMock()
    cur.fetchone.side_effect = lambda: next(fo_iter, None)
    cur.fetchall.side_effect = lambda: next(fa_iter, [])
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _override_conn(conn: MagicMock) -> None:
    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen


def _clear() -> None:
    app.dependency_overrides.pop(get_conn, None)


client = TestClient(app)


# ---------------------------------------------------------------------
# GET /coverage/summary
# ---------------------------------------------------------------------


class TestCoverageSummary:
    def teardown_method(self) -> None:
        _clear()

    def test_returns_counts_by_status(self) -> None:
        # Columns: analysable, insufficient, fpi, no_primary_sec_cik,
        # structurally_young, unknown, null_rows, total_tradable.
        _override_conn(_mock_conn(fetchone_results=[(42, 5, 3, 7, 2, 1, 0, 60)], fetchall_results=[]))

        resp = client.get("/coverage/summary")

        assert resp.status_code == 200
        body = resp.json()
        assert body["analysable"] == 42
        assert body["insufficient"] == 5
        assert body["fpi"] == 3
        assert body["no_primary_sec_cik"] == 7
        assert body["structurally_young"] == 2
        assert body["unknown"] == 1
        assert body["null_rows"] == 0
        assert body["total_tradable"] == 60
        assert body["checked_at"] is not None

    def test_empty_universe_returns_zeros(self) -> None:
        _override_conn(_mock_conn(fetchone_results=[(0, 0, 0, 0, 0, 0, 0, 0)], fetchall_results=[]))

        resp = client.get("/coverage/summary")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_tradable"] == 0
        assert body["analysable"] == 0

    def test_fetchone_none_returns_all_zeros(self) -> None:
        """Degenerate empty-instruments aggregate returns a zero
        payload rather than 500."""
        _override_conn(_mock_conn(fetchone_results=[None], fetchall_results=[]))

        resp = client.get("/coverage/summary")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_tradable"] == 0


# ---------------------------------------------------------------------
# GET /coverage/insufficient
# ---------------------------------------------------------------------


class TestCoverageInsufficient:
    def teardown_method(self) -> None:
        _clear()

    def test_returns_drill_down_rows(self) -> None:
        rows = [
            (
                101,
                "ACME",
                "ACME Corp",
                "0000000101",
                "insufficient",
                2,
                datetime(2026, 4, 10, 4, 0, 0, tzinfo=UTC),
                "STILL_INSUFFICIENT_HTTP_ERROR",
                date(2024, 6, 15),
            ),
            (
                102,
                "NEWC",
                "New Co",
                "0000000102",
                "structurally_young",
                0,
                datetime(2026, 4, 8, 4, 0, 0, tzinfo=UTC),
                "STILL_INSUFFICIENT_STRUCTURALLY_YOUNG",
                date(2025, 10, 1),
            ),
        ]
        _override_conn(_mock_conn(fetchone_results=[], fetchall_results=[rows]))

        resp = client.get("/coverage/insufficient")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["rows"]) == 2

        first = body["rows"][0]
        assert first["instrument_id"] == 101
        assert first["symbol"] == "ACME"
        assert first["cik"] == "0000000101"
        assert first["filings_status"] == "insufficient"
        assert first["filings_backfill_attempts"] == 2
        assert first["filings_backfill_reason"] == "STILL_INSUFFICIENT_HTTP_ERROR"
        # date serialises as YYYY-MM-DD (pydantic default for
        # ``date | None``) — not an ISO datetime with time-of-day.
        assert first["earliest_sec_filing_date"] == "2024-06-15"

        second = body["rows"][1]
        assert second["filings_status"] == "structurally_young"
        assert second["filings_backfill_attempts"] == 0

    def test_empty_list(self) -> None:
        _override_conn(_mock_conn(fetchone_results=[], fetchall_results=[[]]))

        resp = client.get("/coverage/insufficient")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []

    def test_null_cik_and_earliest_date(self) -> None:
        """Rows without a primary SEC CIK or with zero filings
        return nullable fields as null rather than failing."""
        rows = [
            (
                201,
                "NOCIK",
                None,
                None,  # no CIK
                "insufficient",
                3,
                None,  # no last_at
                None,  # no reason
                None,  # no earliest filing
            ),
        ]
        _override_conn(_mock_conn(fetchone_results=[], fetchall_results=[rows]))

        resp = client.get("/coverage/insufficient")

        assert resp.status_code == 200
        body = resp.json()
        row = body["rows"][0]
        assert row["cik"] is None
        assert row["company_name"] is None
        assert row["filings_backfill_last_at"] is None
        assert row["filings_backfill_reason"] is None
        assert row["earliest_sec_filing_date"] is None
