"""Tests for the force_refresh_fundamentals script (#674 follow-up).

Targets the symbol-resolution helper. The end-to-end refresh path
is exercised by the existing fundamentals tests; this file just
pins the resolver's contract: it must return CIK-having symbols
in caller order, list missing ones separately, and tolerate dups
+ casing variations.

Uses the canonical ``ebull_test_conn`` fixture (auto-imported via
``tests/conftest.py``) so the test-DB URL is derived from
``settings.database_url`` rather than hardcoded — a misconfigured CI
environment fails visibly on connect rather than silently skipping
every assertion (PR #680 review).
"""

from __future__ import annotations

import psycopg

from scripts.force_refresh_fundamentals import (
    ResolvedSymbol,
    resolve_symbols,
)


def _seed(
    ebull_test_conn: psycopg.Connection[tuple],
    instrument_id: int,
    symbol: str,
    *,
    cik: str | None,
) -> None:
    ebull_test_conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"frt_{instrument_id}", f"Test {instrument_id}"),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"frt_{instrument_id}"),
    )
    if cik is not None:
        ebull_test_conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, TRUE)
            ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
                WHERE provider = 'sec' AND identifier_type = 'cik'
            DO NOTHING
            """,
            (instrument_id, cik),
        )


def test_resolve_returns_cik_having_symbols_and_separates_missing(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed(ebull_test_conn, 970001, "FRTA", cik="0000970001")
    _seed(ebull_test_conn, 970002, "FRTB", cik=None)
    _seed(ebull_test_conn, 970003, "FRTC", cik="0000970003")

    resolved, missing = resolve_symbols(ebull_test_conn, ["FRTA", "FRTB", "FRTC", "NEVER"])

    assert resolved == [
        ResolvedSymbol(symbol="FRTA", instrument_id=970001, cik="0000970001"),
        ResolvedSymbol(symbol="FRTC", instrument_id=970003, cik="0000970003"),
    ]
    # FRTB lacks CIK, NEVER doesn't exist — both miss.
    assert sorted(missing) == ["FRTB", "NEVER"]


def test_resolve_is_case_insensitive(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed(ebull_test_conn, 970010, "FRTUP", cik="0000970010")
    resolved, missing = resolve_symbols(ebull_test_conn, ["frtup"])
    assert len(resolved) == 1
    assert resolved[0].instrument_id == 970010
    assert missing == []


def test_resolve_preserves_duplicate_inputs(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The resolver itself is order-preserving and does NOT dedupe —
    every caller input slot resolves to a record. Dedupe happens in
    the script's ``main()`` before the expensive SEC fetch (so a
    typo like ``IEP IEP MPLX`` doesn't triple-fetch). Tested here so
    the resolver's contract is locked at the call-site interface."""
    _seed(ebull_test_conn, 970020, "FRTDUP", cik="0000970020")
    resolved, missing = resolve_symbols(ebull_test_conn, ["FRTDUP", "FRTDUP", "FRTDUP"])
    assert len(resolved) == 3
    assert {r.instrument_id for r in resolved} == {970020}
    assert missing == []


def test_resolve_handles_empty_input(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    assert resolve_symbols(ebull_test_conn, []) == ([], [])
