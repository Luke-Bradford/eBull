"""Tests for the force_refresh_fundamentals script (#674 follow-up).

Targets the symbol-resolution helper. The end-to-end refresh path
is exercised by the existing fundamentals tests; this file just
pins the resolver's contract: it must return CIK-having symbols
in caller order, list missing ones separately, and tolerate dups
+ casing variations.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from scripts.force_refresh_fundamentals import (
    ResolvedSymbol,
    resolve_symbols,
)

TEST_DB_URL = "postgresql://postgres:postgres@127.0.0.1:5432/ebull_test"


@pytest.fixture
def conn() -> Iterator[psycopg.Connection]:  # type: ignore[type-arg]
    try:
        c = psycopg.connect(TEST_DB_URL)
    except psycopg.OperationalError:
        pytest.skip("ebull_test DB not available")
    try:
        yield c
    finally:
        c.rollback()
        c.close()


def _seed(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    symbol: str,
    *,
    cik: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"frt_{instrument_id}", f"Test {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"frt_{instrument_id}"),
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


def test_resolve_returns_cik_having_symbols_and_separates_missing(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    _seed(conn, 970001, "FRTA", cik="0000970001")
    _seed(conn, 970002, "FRTB", cik=None)
    _seed(conn, 970003, "FRTC", cik="0000970003")

    resolved, missing = resolve_symbols(conn, ["FRTA", "FRTB", "FRTC", "NEVER"])

    assert resolved == [
        ResolvedSymbol(symbol="FRTA", instrument_id=970001, cik="0000970001"),
        ResolvedSymbol(symbol="FRTC", instrument_id=970003, cik="0000970003"),
    ]
    # FRTB lacks CIK, NEVER doesn't exist — both miss.
    assert sorted(missing) == ["FRTB", "NEVER"]


def test_resolve_is_case_insensitive(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    _seed(conn, 970010, "FRTUP", cik="0000970010")
    resolved, missing = resolve_symbols(conn, ["frtup"])
    assert len(resolved) == 1
    assert resolved[0].instrument_id == 970010
    assert missing == []


def test_resolve_preserves_duplicate_inputs(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    """The resolver itself is order-preserving and does NOT dedupe —
    every caller input slot resolves to a record. Dedupe happens in
    the script's ``main()`` before the expensive SEC fetch (so a
    typo like ``IEP IEP MPLX`` doesn't triple-fetch). Tested here so
    the resolver's contract is locked at the call-site interface."""
    _seed(conn, 970020, "FRTDUP", cik="0000970020")
    resolved, missing = resolve_symbols(conn, ["FRTDUP", "FRTDUP", "FRTDUP"])
    assert len(resolved) == 3
    assert {r.instrument_id for r in resolved} == {970020}
    assert missing == []


def test_resolve_handles_empty_input(
    conn: psycopg.Connection,  # type: ignore[type-arg]
) -> None:
    assert resolve_symbols(conn, []) == ([], [])
