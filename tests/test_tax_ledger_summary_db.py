"""``tax_year_summary`` reads gains and losses per statutory disposal (#3616). Auto-marked ``db``
(pulls ``ebull_test_conn``).

A statutory disposal is every disposal of one security on one day (TCGA 1992 s105(1)(a)). One sale identified
partly same-day at a loss and partly from the s104 pool at a gain is ONE disposal with one net result, and one
disposal lot with two same-day rows (two acquisition lots) is one same-day disposal.

``disposal_matches`` has no writer since slice 3d-2 moved matching to ``tax_matching``; the summary still reads it
until slice 3d-4, so the fixture writes the rows the retired matcher wrote for these lots.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg

from app.services.tax_ledger import tax_year_summary

_IID = 990_361_601


def _instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, %s, 'tax 3616 co', '4', 'USD', 'US', TRUE)
        """,
        (iid, symbol),
    )


def _disposal_lot(conn: psycopg.Connection[tuple], iid: int, at: datetime, units: str, gbp: str) -> int:
    # sql/485 refuses every insert into tax_lots; the fixture lifts the refusal for its own insert only.
    conn.execute("ALTER TABLE tax_lots DISABLE TRIGGER tax_lots_refuse_insert")
    row = conn.execute(
        """
        INSERT INTO tax_lots (instrument_id, event_time, event_type, direction, quantity, cost_or_proceeds,
                              original_currency, fx_rate_to_gbp, amount_gbp, tax_year)
        VALUES (%s, %s, 'EXIT', 'disposal', %s, %s, 'GBP', 1, %s, '2026/27')
        RETURNING tax_lot_id
        """,
        (iid, at, units, gbp, gbp),
    ).fetchone()
    conn.execute("ALTER TABLE tax_lots ENABLE TRIGGER tax_lots_refuse_insert")
    assert row is not None
    return int(row[0])


def _match(
    conn: psycopg.Connection[tuple],
    iid: int,
    lot: int,
    rule: str,
    units: str,
    cost: str,
    proceeds: str,
    day: date = date(2026, 9, 23),
) -> None:
    conn.execute(
        """
        INSERT INTO disposal_matches (instrument_id, disposal_tax_lot_id, matching_rule, matched_units,
                                      acquisition_cost_gbp, disposal_proceeds_gbp, gain_or_loss_gbp,
                                      disposal_uk_date, tax_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '2026/27')
        """,
        (iid, lot, rule, units, cost, proceeds, Decimal(proceeds) - Decimal(cost), day),
    )


def test_summary_nets_each_statutory_disposal_before_reading_its_sign(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _instrument(conn, _IID, "TAX3616")
    # 10 bought 2026-09-01 for £10; 5 for £100 and 5 for £200 on 2026-09-23; 15 sold that day for £150.
    lot = _disposal_lot(conn, _IID, datetime(2026, 9, 23, 10, tzinfo=UTC), "15", "150")
    # Same day: 10 units at the day's average £30 against proceeds of £10 each = -£200, one row per buy.
    _match(conn, _IID, lot, "same_day", "5", "150", "50")
    _match(conn, _IID, lot, "same_day", "5", "150", "50")
    # Pool: 5 units at £1 against £10 each = +£45.
    _match(conn, _IID, lot, "s104_pool", "5", "5", "50")

    summary = tax_year_summary(conn, "2026/27")
    assert summary.total_gains_gbp == Decimal("0")
    assert summary.total_losses_gbp == Decimal("-155")
    assert summary.net_gain_gbp == Decimal("-155")
    assert (summary.disposals_same_day, summary.disposals_s104) == (1, 1)

    # A second security with a plain £5,000 gain lifts the year over the exempt amount. The CGT scenario taxes
    # the net £1,845 above £3,000; the +£45 pool fragment of the first security's overall loss is not a gain.
    _instrument(conn, _IID + 1, "TAX3616B")
    lot_b = _disposal_lot(conn, _IID + 1, datetime(2026, 9, 24, 9, tzinfo=UTC), "10", "6000")
    _match(conn, _IID + 1, lot_b, "s104_pool", "10", "1000", "6000", date(2026, 9, 24))
    summary = tax_year_summary(conn, "2026/27")
    assert (summary.total_gains_gbp, summary.net_gain_gbp) == (Decimal("5000"), Decimal("4845"))
    assert summary.estimated_cgt_basic_scenario == Decimal("1845") * Decimal("0.18")
    assert summary.estimated_cgt_higher_scenario == Decimal("1845") * Decimal("0.24")
