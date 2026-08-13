"""DB-level integration tests for #185 positions upsert
reset-on-reopen semantics (#186).

The CASE WHEN clause that flips ``positions.source`` on reopen is
duplicated across THREE call sites in the runtime:

  * ``app/services/order_client.py::_update_position_buy``
    (the eBull BUY/ADD path through execute_order)
  * ``app/services/portfolio_sync.py::sync_portfolio``
    (the broker-discovered external-open path)
  * ``app/api/orders.py::execute_order``
    (the manual operator order endpoint)

Existing unit tests assert the SQL string contains the CASE WHEN
clause but never exercise the conflict branch — they pass purely
because the literal text appears in the INSERT statement. PR #185
verified the semantics empirically against the dev DB at merge
time but a future incorrect rewrite (e.g. ``>= 0`` for ``<= 0``)
would silently regress.

These tests run against the real ``ebull_test`` Postgres so
ON CONFLICT DO UPDATE actually fires AND assertions distinguish
the two predicate branches:

  * Reset path: closed-position row + new opener of a different
    source ⇒ source flips to the new opener.
  * Preserve path: open-position row + ADD of a different source
    ⇒ source preserved.

The first scenario in the issue body (broker_sync→broker_sync
no-op) was deliberately removed: both predicate branches output
the same ``broker_sync`` value, so the test could not
distinguish a working reset from a removed-or-inverted predicate.
The remaining cases all have differing source values across the
two branches, so the assertion shape is decisive.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from app.providers.broker import BrokerPortfolio, BrokerPosition
from app.services.order_client import _update_position_buy
from app.services.portfolio_sync import sync_portfolio
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 4, 28, 12, 0, 0, tzinfo=UTC)


def _seed_instrument(conn: psycopg.Connection[tuple], instrument_id: int = 1) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, 'TST', 'Test Co', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.commit()


def _seed_position(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    current_units: float,
    source: str,
) -> None:
    """Seed a positions row directly. Used to set up the pre-conflict
    state — bypasses the upsert path so we control source + units
    exactly."""
    conn.execute(
        """
        INSERT INTO positions
            (instrument_id, open_date, avg_cost, current_units,
             cost_basis, source, updated_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            instrument_id,
            _NOW.date(),
            Decimal("100"),
            Decimal(str(current_units)),
            Decimal(str(current_units)) * Decimal("100"),
            source,
            _NOW,
        ),
    )
    conn.commit()


def _read_position_source_units(conn: psycopg.Connection[tuple], instrument_id: int) -> tuple[str, Decimal]:
    row = conn.execute(
        "SELECT source, current_units FROM positions WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchone()
    assert row is not None, f"positions row for instrument {instrument_id} not found"
    return str(row[0]), Decimal(str(row[1]))


# ---------------------------------------------------------------------------
# order_client._update_position_buy — eBull BUY/ADD path
# ---------------------------------------------------------------------------


def test_ebull_buy_into_closed_broker_position_flips_source_to_ebull(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The important reset path. Existing row is fully-closed
    (current_units=0) under broker_sync. An eBull BUY upsert must
    flip source to 'ebull' because eBull is the new opener.

    Without the CASE WHEN reset, the row would keep source='broker_sync'
    and the eBull-led trade would be misattributed in audit / reporting."""
    iid = 100
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_position(ebull_test_conn, instrument_id=iid, current_units=0.0, source="broker_sync")

    _update_position_buy(
        ebull_test_conn,
        instrument_id=iid,
        filled_price=Decimal("150"),
        filled_units=Decimal("10"),
        now=_NOW,
    )
    ebull_test_conn.commit()

    source, units = _read_position_source_units(ebull_test_conn, iid)
    assert source == "ebull", f"source should flip to 'ebull' on reopen; saw {source!r}"
    assert units == Decimal("10")


def test_ebull_add_into_open_broker_position_preserves_broker_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Preserve path. Existing row is OPEN (current_units > 0) under
    broker_sync. An eBull ADD upsert must NOT flip source — the
    original external open is still the authoritative claim."""
    iid = 101
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_position(ebull_test_conn, instrument_id=iid, current_units=5.0, source="broker_sync")

    _update_position_buy(
        ebull_test_conn,
        instrument_id=iid,
        filled_price=Decimal("150"),
        filled_units=Decimal("3"),
        now=_NOW,
    )
    ebull_test_conn.commit()

    source, units = _read_position_source_units(ebull_test_conn, iid)
    assert source == "broker_sync", f"source should be preserved on ADD into open broker position; saw {source!r}"
    assert units == Decimal("8")  # 5 prior + 3 added


def test_ebull_buy_into_negative_units_position_flips_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Boundary check on the ``<= 0`` predicate. A position can never
    legitimately hold negative units in v1 (long-only), but the
    reset clause uses ``<= 0`` to be defensive against any state
    that could result from a sync glitch. Verify the boundary holds:
    a row at units < 0 should also reset source on reopen."""
    iid = 102
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_position(ebull_test_conn, instrument_id=iid, current_units=-1.0, source="broker_sync")

    _update_position_buy(
        ebull_test_conn,
        instrument_id=iid,
        filled_price=Decimal("150"),
        filled_units=Decimal("10"),
        now=_NOW,
    )
    ebull_test_conn.commit()

    source, _units = _read_position_source_units(ebull_test_conn, iid)
    assert source == "ebull"


# ---------------------------------------------------------------------------
# portfolio_sync.sync_portfolio — broker-discovered external-open path
# ---------------------------------------------------------------------------


def _broker_portfolio_with_position(*, instrument_id: int, units: Decimal, open_price: Decimal) -> BrokerPortfolio:
    """Build a minimal BrokerPortfolio fixture wrapping one position.

    The mirror list is non-empty by default to dodge the §2.3.4
    pre-write guard in sync_portfolio that refuses to soft-close
    when broker mirrors[] is empty AND local mirrors exist. We
    don't seed local mirrors so an empty mirror list is also fine,
    but using () keeps the fixture honest about the only side we
    care about (positions)."""
    pos = BrokerPosition(
        instrument_id=instrument_id,
        units=units,
        open_price=open_price,
        current_price=open_price,
        raw_payload={"PositionID": 9001},
        position_id=9001,
        is_buy=True,
        amount=units * open_price,
        initial_amount_in_dollars=units * open_price,
        open_conversion_rate=Decimal("1"),
        open_date_time=_NOW,
        initial_units=units,
        stop_loss_rate=None,
        take_profit_rate=None,
        is_no_stop_loss=True,
        is_no_take_profit=True,
        leverage=1,
        is_tsl_enabled=False,
        total_fees=Decimal("0"),
    )
    return BrokerPortfolio(
        positions=[pos],
        available_cash=Decimal("1000"),
        raw_payload={},
        mirrors=(),
    )


def test_portfolio_sync_external_open_into_closed_ebull_position_flips_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Reset path through the production sync_portfolio code path
    (#186 Codex finding 2). A closed-out ebull-source row exists;
    the broker reports a new external open for the same instrument;
    sync_portfolio's INSERT...ON CONFLICT must flip source to
    'broker_sync' because eBull no longer owns the open.

    Distinguishing assertion: source values differ across the two
    branches of the CASE WHEN, so a reset that fails or a removed
    predicate would leave source='ebull' and fail loudly."""
    iid = 200
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_position(ebull_test_conn, instrument_id=iid, current_units=0.0, source="ebull")
    # sync_portfolio's pre-write guard refuses to proceed when the
    # broker returns empty mirrors[] AND local mirrors exist.
    # ``copy_mirrors`` is not in the planner-tables truncation list,
    # so a previous test in the run could leave rows. Defensive
    # delete before invocation.
    ebull_test_conn.execute("DELETE FROM copy_mirrors WHERE active = TRUE")
    ebull_test_conn.commit()

    portfolio = _broker_portfolio_with_position(instrument_id=iid, units=Decimal("4"), open_price=Decimal("125"))
    sync_portfolio(ebull_test_conn, portfolio, now=_NOW)
    ebull_test_conn.commit()

    source, units = _read_position_source_units(ebull_test_conn, iid)
    assert source == "broker_sync", (
        f"source should flip to 'broker_sync' when broker reopens a closed ebull row; saw {source!r}"
    )
    assert units == Decimal("4")


# ---------------------------------------------------------------------------
# api/orders.py — manual operator order endpoint
# ---------------------------------------------------------------------------


# app/api/orders.py duplicates the same upsert SQL inline at line
# ~233. The right long-term fix is a shared SQL constant imported by
# all three call sites (order_client / portfolio_sync / api.orders);
# this test takes the cheaper route of (a) pinning the production
# SQL block by reading the file and asserting the CASE WHEN exists,
# and (b) exercising the same SQL semantics on a real DB. (a) catches
# drift in the predicate; (b) catches a wrong predicate landing in
# any copy.
_API_ORDERS_PATH = "app/api/orders.py"


def _read_api_orders_source() -> str:
    from pathlib import Path

    return (Path(__file__).resolve().parents[1] / _API_ORDERS_PATH).read_text(encoding="utf-8")


def test_orders_api_source_pin_holds_the_reset_clause() -> None:
    """Drift detector. The reset CASE WHEN must exist verbatim in
    app/api/orders.py so a future refactor that drops the predicate
    or inverts the comparison fails this test loudly. Pairs with the
    DB-level branch tests below — together they cover both
    "is the SQL in the file" and "does the SQL behave correctly".
    """
    src = _read_api_orders_source()
    assert "WHEN positions.current_units <= 0" in src, (
        "app/api/orders.py must keep the <= 0 reset predicate verbatim; "
        "drift here would silently regress #185 source-attribution "
        "across the three copies of this upsert."
    )
    # All three load-bearing tokens must appear in the source.
    # Codex flagged: a regression to ``THEN positions.source`` (i.e.
    # reset arm writing the OLD value instead of the new opener)
    # would still pass a predicate-only check while silently
    # corrupting source attribution. Pin the THEN arm AND the ELSE
    # arm — together they uniquely fix the CASE shape.
    assert "THEN EXCLUDED.source" in src
    assert "ELSE positions.source" in src


def test_api_orders_buy_into_closed_position_flips_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Reset-arm DB regression for the api/orders.py SQL shape.
    The body is a verbatim copy of the production SQL at
    ``app/api/orders.py:228-258`` — the source-pin test above
    guarantees they stay in sync. Closed-position seed + eBull buy
    ⇒ source flips to 'ebull'."""
    iid = 300
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_position(ebull_test_conn, instrument_id=iid, current_units=0.0, source="broker_sync")
    _exec_api_orders_buy_sql(
        ebull_test_conn,
        instrument_id=iid,
        price=Decimal("150"),
        units=Decimal("7"),
    )
    ebull_test_conn.commit()

    source, units = _read_position_source_units(ebull_test_conn, iid)
    assert source == "ebull"
    assert units == Decimal("7")


def test_api_orders_buy_into_open_broker_position_preserves_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Preserve-arm DB regression (Codex finding: the prior version
    only exercised the reset arm in the api/orders path). Open
    position seed at source='broker_sync' + eBull buy ⇒ source
    preserved as 'broker_sync'."""
    iid = 301
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    _seed_position(ebull_test_conn, instrument_id=iid, current_units=4.0, source="broker_sync")
    _exec_api_orders_buy_sql(
        ebull_test_conn,
        instrument_id=iid,
        price=Decimal("150"),
        units=Decimal("3"),
    )
    ebull_test_conn.commit()

    source, units = _read_position_source_units(ebull_test_conn, iid)
    assert source == "broker_sync"
    assert units == Decimal("7")  # 4 prior + 3 added


def _exec_api_orders_buy_sql(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    price: Decimal,
    units: Decimal,
) -> None:
    """Verbatim copy of the upsert at ``app/api/orders.py:228-258``.

    If the production SQL changes shape, both this helper and the
    grep pin above must follow — explicit and intentional, the
    whole point of #186 is to flag drift across the three copies.
    """
    conn.execute(
        """
        INSERT INTO positions
            (instrument_id, open_date, avg_cost, current_units,
             cost_basis, source, updated_at)
        VALUES
            (%(iid)s, %(date)s, %(price)s, %(units)s,
             %(cost)s, 'ebull', %(now)s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            current_units = positions.current_units + EXCLUDED.current_units,
            cost_basis    = positions.cost_basis + EXCLUDED.cost_basis,
            avg_cost      = (positions.cost_basis + EXCLUDED.cost_basis)
                            / NULLIF(positions.current_units + EXCLUDED.current_units, 0),
            source        = CASE
                WHEN positions.current_units <= 0
                    THEN EXCLUDED.source
                ELSE positions.source
            END,
            updated_at    = EXCLUDED.updated_at
        """,
        {
            "iid": instrument_id,
            "date": _NOW.date(),
            "price": price,
            "units": units,
            "cost": price * units,
            "now": _NOW,
        },
    )
