"""Unit tests for ``app.services.manifest_parsers._fund_class_resolver`` (#1171).

Exercises the resolver + miss classifier per spec §7.4. Tests run against
the per-worker test DB fixture.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.manifest_parsers._fund_class_resolver import (
    ResolverMissReason,
    classify_resolver_miss,
    resolve_class_id_to_instrument,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    company_name: str = "Test Fund",
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, company_name),
    )


def _seed_directory(
    conn: psycopg.Connection[tuple],
    *,
    class_id: str,
    series_id: str = "S000000001",
    symbol: str | None = "VFIAX",
    trust_cik: str = "0000036405",
) -> None:
    conn.execute(
        """
        INSERT INTO cik_refresh_mf_directory (class_id, series_id, symbol, trust_cik)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (class_id) DO NOTHING
        """,
        (class_id, series_id, symbol, trust_cik),
    )


def _seed_class_id_ext(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    class_id: str,
) -> None:
    # PREVENTION: set is_primary=TRUE explicitly. Resolver filters on it; do
    # not rely on column DEFAULT (review-prevention-log entry from PR #1172).
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        )
        VALUES (%s, 'sec', 'class_id', %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (instrument_id, class_id),
    )


def test_resolve_hit_returns_instrument_id(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=1001, symbol="VFIAX")
    _seed_class_id_ext(ebull_test_conn, instrument_id=1001, class_id="C000000001")
    ebull_test_conn.commit()

    result = resolve_class_id_to_instrument(ebull_test_conn, "C000000001")
    assert result == 1001


def test_resolve_miss_returns_none(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    result = resolve_class_id_to_instrument(ebull_test_conn, "C000999999")
    assert result is None


def test_classify_pending_cik_refresh(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # Directory empty for this class_id.
    reason = classify_resolver_miss(ebull_test_conn, "C000999999")
    assert reason == ResolverMissReason.PENDING_CIK_REFRESH


def test_classify_ext_id_not_yet_written(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # Directory row + matching instrument, but no external_identifiers row.
    _seed_instrument(ebull_test_conn, iid=1002, symbol="VFINX")
    _seed_directory(ebull_test_conn, class_id="C000000002", symbol="VFINX")
    ebull_test_conn.commit()

    reason = classify_resolver_miss(ebull_test_conn, "C000000002")
    assert reason == ResolverMissReason.EXT_ID_NOT_YET_WRITTEN


def test_classify_instrument_not_in_universe(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # Directory row but symbol does not map to any instrument.
    _seed_directory(ebull_test_conn, class_id="C000000003", symbol="FXAIX_NOT_IN_UNIVERSE")
    ebull_test_conn.commit()

    reason = classify_resolver_miss(ebull_test_conn, "C000000003")
    assert reason == ResolverMissReason.INSTRUMENT_NOT_IN_UNIVERSE


def test_no_symbol_only_fallback(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Directory has class_id with matching symbol BUT no external_identifiers
    row → resolver MUST return None. No shortcut via symbol.

    This is the BLOCKING-4 guard: a stale symbol that happens to match an
    instruments row must NOT resolve. Caller's classify_resolver_miss returns
    EXT_ID_NOT_YET_WRITTEN (transient retry).
    """
    _seed_instrument(ebull_test_conn, iid=1003, symbol="VFINX")
    _seed_directory(ebull_test_conn, class_id="C000000004", symbol="VFINX")
    # Note: NO _seed_class_id_ext call.
    ebull_test_conn.commit()

    assert resolve_class_id_to_instrument(ebull_test_conn, "C000000004") is None
    assert classify_resolver_miss(ebull_test_conn, "C000000004") == ResolverMissReason.EXT_ID_NOT_YET_WRITTEN
