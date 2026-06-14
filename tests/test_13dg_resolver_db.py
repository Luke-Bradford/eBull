"""DB-backed matrix for ``_resolve_issuer_to_instrument_id`` (#1628).

CUSIP-primary resolution (security-precise, settled #1102), deterministic
``sec`` > ``openfigi`` provider precedence, and the single-class-only CIK
fallback (multi-class / no-CIK / malformed-CIK → None — never guess the
share class, never fan out). Auto-marked ``db`` (pulls ``ebull_test_conn``).
"""

from __future__ import annotations

import psycopg

from app.services.blockholders import _resolve_issuer_to_instrument_id


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', 'US', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_extid(
    conn: psycopg.Connection[tuple],
    iid: int,
    provider: str,
    id_type: str,
    value: str,
) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, %s, %s, %s, FALSE)
        ON CONFLICT DO NOTHING
        """,
        (iid, provider, id_type, value),
    )


def test_resolve_issuer_to_instrument_id(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    for iid, sym in [(900001, "AAA"), (900002, "BBB"), (900003, "CCC"), (900004, "DDD")]:
        _seed_instrument(conn, iid, sym)

    # 1. CUSIP-primary (+ normalisation: lowercase/whitespace still resolves).
    _seed_extid(conn, 900001, "sec", "cusip", "111111111")
    assert _resolve_issuer_to_instrument_id(conn, cusip="111111111", cik=None) == 900001
    assert _resolve_issuer_to_instrument_id(conn, cusip="  111111111  ", cik=None) == 900001

    # 2. Provider precedence: same CUSIP under BOTH openfigi + sec -> sec wins.
    _seed_extid(conn, 900002, "openfigi", "cusip", "222222222")
    _seed_extid(conn, 900003, "sec", "cusip", "222222222")
    assert _resolve_issuer_to_instrument_id(conn, cusip="222222222", cik=None) == 900003

    # 3. Single-class CIK fallback when the CUSIP is absent / unresolved.
    _seed_extid(conn, 900004, "sec", "cik", "0000900004")
    assert _resolve_issuer_to_instrument_id(conn, cusip="", cik="0000900004") == 900004
    assert _resolve_issuer_to_instrument_id(conn, cusip="ZZZZZZZZZ", cik="0000900004") == 900004

    # 4. Multi-class CIK (2 instruments) -> None (never guess the class / fan out).
    _seed_extid(conn, 900001, "sec", "cik", "0000900099")
    _seed_extid(conn, 900002, "sec", "cik", "0000900099")
    assert _resolve_issuer_to_instrument_id(conn, cusip="", cik="0000900099") is None

    # 5. No CUSIP + no CIK -> None. Malformed CIK -> None (caught, not a crash).
    assert _resolve_issuer_to_instrument_id(conn, cusip="", cik=None) is None
    assert _resolve_issuer_to_instrument_id(conn, cusip="", cik="not-a-cik") is None
