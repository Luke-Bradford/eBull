"""``tax_year_summary`` reads gains and losses per statutory disposal (#3616). Auto-marked ``db``
(pulls ``ebull_test_conn``).

A statutory disposal is every disposal of one security on one day (TCGA 1992 s105(1)(a)). One sale identified
partly same-day at a loss and partly from the s104 pool at a gain is ONE disposal with one net result, and one
disposal lot with two same-day rows (two acquisition lots) is one same-day disposal.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg

from app.services.tax_ledger import run_disposal_matching, tax_year_summary

_IID = 990_361_601


def _lot(conn: psycopg.Connection[tuple], at: datetime, direction: str, units: str, gbp: str) -> None:
    conn.execute(
        """
        INSERT INTO tax_lots (instrument_id, event_time, event_type, direction, quantity, cost_or_proceeds,
                              original_currency, fx_rate_to_gbp, amount_gbp, tax_year)
        VALUES (%s, %s, %s, %s, %s, %s, 'GBP', 1, %s, '2026/27')
        """,
        (_IID, at, "BUY" if direction == "acquisition" else "EXIT", direction, units, gbp, gbp),
    )


def test_summary_nets_each_statutory_disposal_before_reading_its_sign(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, 'TAX3616', 'tax 3616 co', '4', 'USD', 'US', TRUE)
        """,
        (_IID,),
    )
    _lot(conn, datetime(2026, 9, 1, 9, tzinfo=UTC), "acquisition", "10", "10")
    _lot(conn, datetime(2026, 9, 23, 8, tzinfo=UTC), "acquisition", "5", "100")
    _lot(conn, datetime(2026, 9, 23, 9, tzinfo=UTC), "acquisition", "5", "200")
    _lot(conn, datetime(2026, 9, 23, 10, tzinfo=UTC), "disposal", "15", "150")

    run_disposal_matching(conn, instrument_id=_IID)
    rows = conn.execute(
        "SELECT matching_rule, count(*), sum(gain_or_loss_gbp) FROM disposal_matches WHERE instrument_id = %s "
        "GROUP BY 1 ORDER BY 1",
        (_IID,),
    ).fetchall()
    # Same day: 10 units at the day's average £30 against proceeds of £10 each = -£200, one row per buy.
    # Pool: 5 units at £1 against £10 each = +£45.
    assert rows == [("s104_pool", 1, Decimal("45.000000")), ("same_day", 2, Decimal("-200.000000"))]

    summary = tax_year_summary(conn, "2026/27")
    assert summary.total_gains_gbp == Decimal("0")
    assert summary.total_losses_gbp == Decimal("-155")
    assert summary.net_gain_gbp == Decimal("-155")
    assert (summary.disposals_same_day, summary.disposals_s104) == (1, 1)

    # A second security with a plain £5,000 gain lifts the year over the exempt amount. The CGT scenario taxes
    # the net £1,845 above £3,000; the +£45 pool fragment of the first security's overall loss is not a gain.
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, 'TAX3616B', 'tax 3616 b co', '4', 'USD', 'US', TRUE)
        """,
        (_IID + 1,),
    )
    for at, direction, units, gbp in (
        (datetime(2026, 9, 2, 9, tzinfo=UTC), "acquisition", "10", "1000"),
        (datetime(2026, 9, 24, 9, tzinfo=UTC), "disposal", "10", "6000"),
    ):
        conn.execute(
            """
            INSERT INTO tax_lots (instrument_id, event_time, event_type, direction, quantity, cost_or_proceeds,
                                  original_currency, fx_rate_to_gbp, amount_gbp, tax_year)
            VALUES (%s, %s, 'X', %s, %s, %s, 'GBP', 1, %s, '2026/27')
            """,
            (_IID + 1, at, direction, units, gbp, gbp),
        )
    run_disposal_matching(conn, instrument_id=_IID + 1)
    summary = tax_year_summary(conn, "2026/27")
    assert (summary.total_gains_gbp, summary.net_gain_gbp) == (Decimal("5000"), Decimal("4845"))
    assert summary.estimated_cgt_basic_scenario == Decimal("1845") * Decimal("0.18")
    assert summary.estimated_cgt_higher_scenario == Decimal("1845") * Decimal("0.24")
