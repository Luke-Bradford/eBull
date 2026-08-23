from __future__ import annotations

import psycopg
import pytest

from app.services.strategies.validated_universe import STOCKS_TYPE_DESCRIPTION, load_validated_universe
from scripts.verify_2907_cost_by_size_band import _load_rows, classify_market_cap, classify_price
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration

_STOCKS_TYPE_ID = 2907
_PRICED_ID = 2907001
_UNPRICED_ID = 2907002


def _seed(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "INSERT INTO etoro_instrument_types (instrument_type_id, description) VALUES (%s, %s)",
        (_STOCKS_TYPE_ID, STOCKS_TYPE_DESCRIPTION),
    )
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('r6_2907', 'us_equity')")
    conn.execute(
        """
        INSERT INTO instruments
            (instrument_id, symbol, company_name, exchange, currency, is_tradable, instrument_type_id)
        VALUES
            (%s, 'R6COST1', 'R6 cost priced', 'r6_2907', 'USD', TRUE, %s),
            (%s, 'R6COST2', 'R6 cost unpriced', 'r6_2907', 'USD', TRUE, %s)
        """,
        (_PRICED_ID, _STOCKS_TYPE_ID, _UNPRICED_ID, _STOCKS_TYPE_ID),
    )
    conn.execute(
        """
        INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last)
        VALUES (%s, '2026-08-23T12:00:00Z', 49, 51, 50)
        """,
        (_PRICED_ID,),
    )


def test_database_census_conserves_every_validated_universe_id(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed(ebull_test_conn)
    instrument_ids = load_validated_universe(ebull_test_conn)
    rows = _load_rows(ebull_test_conn, instrument_ids)
    assert instrument_ids == (_PRICED_ID, _UNPRICED_ID)
    assert tuple(row.instrument_id for row in rows) == instrument_ids
    assert len(rows) == len({row.instrument_id for row in rows}) == 2
    assert [(classify_market_cap(row.market_cap_live), classify_price(row.last)) for row in rows] == [
        ("unknown_market_cap", "priced"),
        ("unknown_market_cap", "unpriced"),
    ]
