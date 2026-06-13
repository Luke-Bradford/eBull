"""DB integration test for the EOD snapshot SQL mechanism (#1594 PR-A).

ONE file for the genuinely-new SQL: snapshot + per-position upsert
idempotency, the carry-forward dated-FX read, and the close-on/before-date
position read. The aggregation logic is pure-tested in
tests/test_portfolio_eod.py; the gap-range math in tests/test_fx_history.py.
Network (Frankfurter) is NOT exercised here — fx_rates_daily is seeded.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import fx_history
from app.services.fx_history import ensure_fx_history, load_fx_rates_for_date
from app.services.portfolio_eod import (
    EodEquity,
    PositionResult,
    _read_cash,
    _read_positions,
    _resolve_snapshot_date,
    _write_snapshot,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401  (fixture)

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[Any], iid: int, ccy: str = "USD") -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', %s, TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, f"SYM{iid}", f"Co {iid}", ccy),
    )


def _seed_position(conn: psycopg.Connection[Any], pid: int, iid: int, units: str) -> None:
    conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES (%s, %s, TRUE, %s, 100, 100, 10, 1, NOW(), '{}'::jsonb)
        ON CONFLICT (position_id) DO NOTHING
        """,
        (pid, iid, units),
    )


def _seed_price(conn: psycopg.Connection[Any], iid: int, d: date, close: str) -> None:
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s) "
        "ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close",
        (iid, d, Decimal(close)),
    )


def test_resolve_snapshot_date_is_max_held_price_date(ebull_test_conn: psycopg.Connection[Any]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_instrument(conn, 9001)
    _seed_position(conn, 5001, 9001, "2")
    _seed_price(conn, 9001, date(2025, 6, 10), "10")
    _seed_price(conn, 9001, date(2025, 6, 12), "11")
    assert _resolve_snapshot_date(conn, fallback=date(2020, 1, 1)) == date(2025, 6, 12)


def test_read_positions_carries_close_forward(ebull_test_conn: psycopg.Connection[Any]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_instrument(conn, 9002)
    _seed_position(conn, 5002, 9002, "3")
    _seed_price(conn, 9002, date(2025, 6, 9), "20")  # last close on/before the 11th
    rows = _read_positions(conn, date(2025, 6, 11))
    assert len(rows) == 1
    assert rows[0].close == Decimal("20")
    assert rows[0].native_ccy == "USD"
    assert rows[0].units == Decimal("3")


def test_read_positions_excludes_synthetic_ids(ebull_test_conn: psycopg.Connection[Any]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_instrument(conn, 9003)
    conn.execute(
        """
        INSERT INTO broker_positions (
            position_id, instrument_id, is_buy, units, amount,
            initial_amount_in_dollars, open_rate, open_conversion_rate,
            open_date_time, raw_payload
        ) VALUES (-777, %s, TRUE, 1, 1, 1, 1, 1, NOW(), '{}'::jsonb)
        """,
        (9003,),
    )
    assert _read_positions(conn, date(2025, 6, 11)) == []


def test_load_fx_rates_for_date_carry_forward(ebull_test_conn: psycopg.Connection[Any]) -> None:  # noqa: F811
    conn = ebull_test_conn
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO fx_rates_daily (rate_date, base_currency, quote_currency, rate) VALUES (%s,%s,%s,%s)",
            [
                (date(2025, 6, 6), "USD", "GBP", Decimal("0.70")),
                (date(2025, 6, 9), "USD", "GBP", Decimal("0.80")),
            ],
        )
    # Query the 11th (weekend gap after the 9th) → carries forward the 9th.
    rates, used = load_fx_rates_for_date(conn, date(2025, 6, 11))
    assert rates[("USD", "GBP")] == Decimal("0.80")
    assert used == date(2025, 6, 9)
    # Query the 7th → only the 6th is on/before.
    rates2, used2 = load_fx_rates_for_date(conn, date(2025, 6, 7))
    assert rates2[("USD", "GBP")] == Decimal("0.70")
    assert used2 == date(2025, 6, 6)


def test_ensure_fx_history_reports_rows_written(
    ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    monkeypatch,  # noqa: ANN001
) -> None:
    """ensure_fx_history returns the true count of newly-inserted rows.

    Pins the review-round-2 rebuttal: cur.rowcount after executemany +
    ON CONFLICT DO NOTHING IS the affected-row sum in psycopg3 (NOT -1), so
    the count is the honest "rows written" — non-zero on first load, zero on
    an idempotent re-run (len(rows) would wrongly report non-zero on re-run).
    Frankfurter HTTP is stubbed; only the DB write path is exercised.
    """
    conn = ebull_test_conn
    stub = {
        date(2025, 1, 1): {("USD", "GBP"): Decimal("0.80")},
        date(2025, 1, 2): {("USD", "GBP"): Decimal("0.81")},
        date(2025, 1, 3): {("USD", "GBP"): Decimal("0.82")},
    }
    monkeypatch.setattr(fx_history, "fetch_timeseries_rates", lambda *a, **k: stub)

    written = ensure_fx_history(conn, until=date(2025, 1, 3), targets=["GBP"], since=date(2025, 1, 1))
    conn.commit()
    assert written == 3  # rowcount reflects the 3 inserts, not 0 and not -1

    count = conn.execute("SELECT COUNT(*) FROM fx_rates_daily WHERE base_currency = 'USD'").fetchone()
    assert count is not None and count[0] == 3

    # Idempotent re-run: all 3 conflict → 0 newly written (len(rows) would say 3).
    rewritten = ensure_fx_history(conn, until=date(2025, 1, 3), targets=["GBP"], since=date(2025, 1, 1))
    conn.commit()
    assert rewritten == 0


def test_ensure_fx_history_backfills_a_missing_quote(
    ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    monkeypatch,  # noqa: ANN001
) -> None:
    """A target quote absent for a span must be backfilled even if another is present.

    Pins Codex ckpt-3 MEDIUM: coverage is per (base, quote), not per base. GBP
    already covers the span; EUR has zero rows → the gap detection must NOT
    treat the span as covered and skip EUR.
    """
    conn = ebull_test_conn
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO fx_rates_daily (rate_date, base_currency, quote_currency, rate) VALUES (%s,%s,%s,%s)",
            [
                (date(2025, 1, 1), "USD", "GBP", Decimal("0.80")),
                (date(2025, 1, 2), "USD", "GBP", Decimal("0.81")),
            ],
        )
    stub = {
        date(2025, 1, 1): {("USD", "GBP"): Decimal("0.80"), ("USD", "EUR"): Decimal("0.90")},
        date(2025, 1, 2): {("USD", "GBP"): Decimal("0.81"), ("USD", "EUR"): Decimal("0.91")},
    }
    monkeypatch.setattr(fx_history, "fetch_timeseries_rates", lambda *a, **k: stub)

    written = ensure_fx_history(conn, until=date(2025, 1, 2), targets=["EUR", "GBP"], since=date(2025, 1, 1))
    conn.commit()
    # GBP rows conflict (DO NOTHING); the 2 EUR rows are newly written.
    assert written == 2
    eur = conn.execute(
        "SELECT COUNT(*) FROM fx_rates_daily WHERE base_currency='USD' AND quote_currency='EUR'"
    ).fetchone()
    assert eur is not None and eur[0] == 2


def test_read_cash_sums_as_of_date(ebull_test_conn: psycopg.Connection[Any]) -> None:  # noqa: F811
    conn = ebull_test_conn
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO cash_ledger (event_time, amount, currency, event_type) VALUES (%s,%s,%s,'deposit')",
            [
                (datetime(2025, 6, 1, tzinfo=UTC), Decimal("100"), "USD"),
                (datetime(2025, 6, 5, tzinfo=UTC), Decimal("-30"), "USD"),
                (datetime(2025, 6, 20, tzinfo=UTC), Decimal("999"), "USD"),  # after the as-of date
            ],
        )
    rows = _read_cash(conn, date(2025, 6, 10))
    assert rows == [("USD", Decimal("70"))]


def test_write_snapshot_is_idempotent(ebull_test_conn: psycopg.Connection[Any]) -> None:  # noqa: F811
    conn = ebull_test_conn
    _seed_instrument(conn, 9004)
    _seed_position(conn, 5004, 9004, "2")
    d = date(2025, 6, 12)

    def _equity(total: str) -> EodEquity:
        return EodEquity(
            positions_value=Decimal(total),
            cash_value=Decimal("0"),
            total_value=Decimal(total),
            positions_total=1,
            positions_priced=1,
            positions_no_price=0,
            positions_no_fx=0,
            cash_no_fx_currencies=0,
            position_results=[PositionResult(5004, 9004, Decimal("2"), "USD", Decimal("10"), Decimal(total), "priced")],
        )

    with conn.transaction():
        _write_snapshot(conn, d, "GBP", date(2025, 6, 12), _equity("16.00"))
    # Re-run for the same date with a different value → overwrite, not duplicate.
    with conn.transaction():
        _write_snapshot(conn, d, "GBP", date(2025, 6, 12), _equity("18.00"))

    snap_row = conn.execute(
        "SELECT COUNT(*), MAX(total_value) FROM portfolio_eod_snapshots WHERE snapshot_date = %s", (d,)
    ).fetchone()
    pos_row = conn.execute(
        "SELECT COUNT(*) FROM portfolio_eod_position_snapshots WHERE snapshot_date = %s", (d,)
    ).fetchone()
    assert snap_row is not None and pos_row is not None
    assert snap_row[0] == 1
    assert pos_row[0] == 1
    assert snap_row[1] == Decimal("18.00")
