"""#3322: the broker-derived price currency survives the nightly venue-default upsert.

One integration test for the two new SQL mechanisms: the writer's UPDATE and the
``sync_universe`` preservation CASE.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg

from app.providers.market_data import InstrumentRecord, Quote
from app.services.fx import upsert_live_fx_rate
from app.services.instrument_price_currency import derive_instrument_price_currencies
from app.services.universe import sync_universe

_IID = 953322
NOW = datetime.now(UTC)


def _record(exchange: str) -> InstrumentRecord:
    return InstrumentRecord(
        provider_id=str(_IID),
        symbol="ZZ3322.L",
        company_name="ZZ3322 plc",
        exchange=exchange,
        currency=None,
        sector=None,
        industry=None,
        country=None,
        is_tradable=True,
        instrument_type_id=5,
    )


def _sync(conn: psycopg.Connection[tuple], exchange: str) -> None:
    provider = MagicMock()
    provider.get_tradable_instruments.return_value = [_record(exchange)]
    sync_universe(provider, conn)
    conn.commit()


def _state(conn: psycopg.Connection[tuple]) -> tuple[str | None, str, datetime | None]:
    row = conn.execute(
        "SELECT currency, currency_source, currency_derived_at FROM instruments WHERE instrument_id = %s",
        (_IID,),
    ).fetchone()
    assert row is not None
    return row[0], row[1], row[2]


def test_derived_gbx_survives_resync_and_resets_on_exchange_move(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    for ccy, rate in (("GBP", "0.74832"), ("EUR", "0.87237")):
        upsert_live_fx_rate(conn, from_currency="USD", to_currency=ccy, rate=Decimal(rate), quoted_at=NOW)
    # Own venue rows: the test must not depend on which curated exchanges the
    # template happens to carry.
    for exchange_id, ccy in (("x3322gbp", "GBP"), ("x3322eur", "EUR")):
        conn.execute(
            "INSERT INTO exchanges (exchange_id, currency) VALUES (%s, %s)"
            " ON CONFLICT (exchange_id) DO UPDATE SET currency = EXCLUDED.currency",
            (exchange_id, ccy),
        )
    conn.commit()

    _sync(conn, "x3322gbp")  # LSE-like venue: default GBP
    assert _state(conn)[:2] == ("GBP", "exchange")

    quoted_at = NOW - timedelta(minutes=5)
    provider = MagicMock()
    provider.get_quotes.return_value = [
        Quote(
            instrument_id=_IID,
            timestamp=quoted_at,
            bid=Decimal("5816"),
            ask=Decimal("5818"),
            last=None,
            conversion_rate=Decimal("0.013303"),
        )
    ]
    counts = derive_instrument_price_currencies(conn, provider, lambda: NOW)
    conn.commit()
    assert counts["derived"] >= 1
    assert _state(conn) == ("GBX", "broker_rate", quoted_at)

    _sync(conn, "x3322gbp")  # nightly re-sync on the same venue keeps the derived value
    assert _state(conn)[:2] == ("GBX", "broker_rate")

    _sync(conn, "x3322eur")  # an exchange move resets to the new venue default
    assert _state(conn) == ("EUR", "exchange", None)
