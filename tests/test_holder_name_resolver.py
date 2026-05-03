"""Verify the lifted ``app.services.holder_name_resolver`` returns the
same results as the original private helpers in
``app.services.def14a_drift`` so the two consumers (DEF 14A drift
detector + ownership rollup) cannot drift apart on match semantics.

Codex spec review caught the duplication risk on the v1 spec for
#789; this suite is the recurring guard.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services import def14a_drift
from app.services.holder_name_resolver import (
    normalise_name,
    resolve_holder_to_filer,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("John Doe", "john doe"),
        ("  John Doe  ", "john doe"),
        ("John Doe, CEO", "john doe"),
        ("John Doe - Director", "john doe"),
        ("John Doe — Director", "john doe"),
        ("John Doe – Director", "john doe"),
        ("ALL CAPS NAME", "all caps name"),
        ("", ""),
    ],
)
def test_normalise_name_matches_legacy(raw: str, expected: str) -> None:
    """Public + legacy private helper return the same result."""
    assert normalise_name(raw) == expected
    assert def14a_drift._normalise_name(raw) == expected


def test_resolver_matches_form4_filer(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Public ``resolve_holder_to_filer`` returns the same tuple as
    the legacy ``def14a_drift._resolve_holder_match`` for a Form 4
    match."""
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (789100, 'RES', 'Resolver Test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type, issuer_cik
        ) VALUES ('F4-RES-001', 789100, '4', '0000789100')
        ON CONFLICT (accession_number) DO NOTHING
        """,
    )
    conn.execute(
        """
        INSERT INTO insider_transactions (
            accession_number, txn_row_num, instrument_id, filer_cik, filer_name,
            txn_date, txn_code, shares, post_transaction_shares, is_derivative
        ) VALUES ('F4-RES-001', 1, 789100, '0001234567', 'Holder Name',
                  %s, 'P', 100, %s, FALSE)
        """,
        (date(2026, 3, 1), Decimal("250000")),
    )
    conn.commit()

    public = resolve_holder_to_filer(conn, instrument_id=789100, holder_name="Holder Name")
    legacy = def14a_drift._resolve_holder_match(conn, instrument_id=789100, holder_name="Holder Name")
    assert public == legacy
    assert public[0] is True
    assert public[1] == "0001234567"
    assert public[2] == Decimal("250000")


def test_resolver_returns_no_match_for_unknown_holder(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (789101, 'RESM', 'Resolver Test 2', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.commit()

    public = resolve_holder_to_filer(conn, instrument_id=789101, holder_name="Phantom Holder")
    legacy = def14a_drift._resolve_holder_match(conn, instrument_id=789101, holder_name="Phantom Holder")
    assert public == legacy == (False, None, None)
