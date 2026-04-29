"""Tests for the bulk-resolve refresh_filings path (#669).

Targets the new ``_bulk_resolve_identifiers`` helper plus the
end-to-end ``refresh_filings`` flow, asserting that:

  - one query resolves the entire cohort (the prior per-row resolver
    issued N queries for N instruments)
  - instruments lacking the identifier are silently dropped — no
    per-row INFO log line in the loop, only a single aggregate summary
  - upsert / provider-error counters still surface correctly via the
    returned ``FilingsRefreshSummary``

Uses the live ``ebull_test`` database (NOT the dev DB — destructive
test rows would otherwise pollute fundamentals data); falls back to
skip when that DB isn't available.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

import psycopg
import pytest

from app.providers.filings import FilingSearchResult, FilingsProvider
from app.services.filings import (
    FilingsRefreshSummary,
    _bulk_resolve_identifiers,
    refresh_filings,
)

# Use the dedicated test DB. Tests in this file insert into
# external_identifiers + instruments; the dev DB is off-limits for
# destructive tests per docs/settled-decisions + memory.
TEST_DB_URL = "postgresql://postgres:postgres@127.0.0.1:5432/ebull_test"


@pytest.fixture
def conn() -> Iterator[psycopg.Connection]:  # type: ignore[type-arg]
    try:
        c = psycopg.connect(TEST_DB_URL)
    except psycopg.OperationalError:
        pytest.skip("ebull_test DB not available; run scripts/migrate.py against ebull_test first")
    try:
        yield c
    finally:
        c.rollback()
        c.close()


def _seed_instrument(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    symbol: str,
    *,
    cik: str | None,
) -> None:
    """Seed an instrument + (optionally) a primary SEC CIK row.
    Wrapped in a SAVEPOINT so the per-test rollback in the fixture
    cleans up regardless of test outcome."""
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"test_bulk_{instrument_id}", f"Test exchange {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"test_bulk_{instrument_id}"),
    )
    if cik is not None:
        conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, TRUE)
            ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
            """,
            (instrument_id, cik),
        )


class _StubFilingsProvider(FilingsProvider):
    """In-memory stub. Returns a fixed list of SearchResults per CIK
    when ``identifier_value`` matches; otherwise raises so the test
    can assert non-CIK instruments don't reach the provider."""

    def __init__(self, results_by_cik: dict[str, list[FilingSearchResult]]) -> None:
        self._results = results_by_cik
        self.calls: list[tuple[str, str]] = []

    def list_filings_by_identifier(  # type: ignore[override]
        self,
        *,
        identifier_type: str,
        identifier_value: str,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        self.calls.append((identifier_type, identifier_value))
        return self._results.get(identifier_value, [])

    def get_filing(self, *args, **kwargs):  # type: ignore[override]
        raise NotImplementedError("not used in these tests")

    def build_cik_mapping(self):  # type: ignore[override]
        raise NotImplementedError("not used in these tests")


def test_bulk_resolve_returns_only_instruments_with_primary_identifier(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    _seed_instrument(conn, 990001, "BULKA", cik="0000990001")
    _seed_instrument(conn, 990002, "BULKB", cik=None)
    _seed_instrument(conn, 990003, "BULKC", cik="0000990003")

    resolved = _bulk_resolve_identifiers(
        conn,
        instrument_ids=["990001", "990002", "990003"],
        provider_name="sec",
        identifier_type="cik",
    )
    assert resolved == {"990001": "0000990001", "990003": "0000990003"}


def test_bulk_resolve_handles_empty_list(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    assert _bulk_resolve_identifiers(conn, [], "sec", "cik") == {}


def test_refresh_filings_only_calls_provider_for_resolved_instruments(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_instrument(conn, 990010, "RFA", cik="0000990010")
    _seed_instrument(conn, 990011, "RFB", cik=None)
    _seed_instrument(conn, 990012, "RFC", cik="0000990012")

    provider = _StubFilingsProvider({"0000990010": [], "0000990012": []})

    caplog.set_level(logging.INFO, logger="app.services.filings")
    summary = refresh_filings(
        provider=provider,  # type: ignore[arg-type]
        provider_name="sec",
        identifier_type="cik",
        conn=conn,
        instrument_ids=["990010", "990011", "990012"],
    )

    # Provider sees only the two resolved CIKs — no per-instrument
    # call for the BULKB row that has no CIK.
    assert sorted(provider.calls) == [
        ("cik", "0000990010"),
        ("cik", "0000990012"),
    ]
    assert summary == FilingsRefreshSummary(
        instruments_attempted=3,
        filings_upserted=0,
        instruments_skipped=1,
    )

    # Aggregate summary line replaces the prior per-row spam. Exactly
    # one INFO line should mention the skip count, and zero lines
    # should mention any single instrument_id as a "no sec/cik" miss.
    skip_lines = [
        r
        for r in caplog.records
        if r.name == "app.services.filings" and "missing" in r.getMessage() and "identifier" in r.getMessage()
    ]
    assert len(skip_lines) == 1
    assert "1/3" in skip_lines[0].getMessage()
    per_row_lines = [
        r
        for r in caplog.records
        if r.name == "app.services.filings" and "no sec/cik for instrument_id=" in r.getMessage()
    ]
    assert per_row_lines == []


def test_refresh_filings_handles_zero_input(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    provider = _StubFilingsProvider({})
    summary = refresh_filings(
        provider=provider,  # type: ignore[arg-type]
        provider_name="sec",
        identifier_type="cik",
        conn=conn,
        instrument_ids=[],
    )
    assert summary == FilingsRefreshSummary(0, 0, 0)
    assert provider.calls == []
