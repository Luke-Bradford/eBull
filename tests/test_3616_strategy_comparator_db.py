"""#3616 slices 1-2 — the comparator's close and rate lookups and their wiring into /strategies/wealth-history."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from app.api.strategies import get_strategy_wealth_history
from app.services.strategy_comparator import (
    COMPARATOR_SYMBOL,
    Close,
    FxRate,
    latest_valid_close,
    latest_valid_gbp_rate,
)
from app.services.strategy_control_plane import configure_paper_pool
from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION

_INSTRUMENT_ID = 2461601


def _instrument(conn: psycopg.Connection[Any], *, symbol: str = COMPARATOR_SYMBOL) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, currency, exchange, is_tradable)
        VALUES (%s, %s, 'iShares Core S&P 500 UCITS ETF', 'USD', '7', true)
        """,
        (_INSTRUMENT_ID, symbol),
    )


def _closes(conn: psycopg.Connection[Any], rows: list[tuple[str, str | None]]) -> None:
    for day, close in rows:
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, %s, %s::numeric)",
            (_INSTRUMENT_ID, day, close),
        )


def test_latest_valid_close_skips_invalid_rows_and_never_reads_the_future(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _instrument(ebull_test_conn)
    _closes(
        ebull_test_conn,
        [
            ("2026-09-01", "100"),
            ("2026-09-02", "0"),
            ("2026-09-03", None),
            ("2026-09-04", "NaN"),
            ("2026-09-05", "-1"),
            ("2026-09-08", "110"),
        ],
    )

    assert latest_valid_close(ebull_test_conn, _INSTRUMENT_ID, date(2026, 9, 7)) == Close(
        date(2026, 9, 1), Decimal("100")
    )
    assert latest_valid_close(ebull_test_conn, _INSTRUMENT_ID, date(2026, 9, 8)) == Close(
        date(2026, 9, 8), Decimal("110")
    )
    assert latest_valid_close(ebull_test_conn, _INSTRUMENT_ID, date(2026, 8, 31)) is None


def test_every_row_invalid_is_no_close(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    _instrument(ebull_test_conn)
    _closes(ebull_test_conn, [("2026-09-02", "0"), ("2026-09-03", "NaN"), ("2026-09-04", "-5")])
    assert latest_valid_close(ebull_test_conn, _INSTRUMENT_ID, date(2026, 9, 30)) is None


def _rates(conn: psycopg.Connection[Any], rows: list[tuple[str, str, str]]) -> None:
    for day, quote, rate in rows:
        conn.execute(
            """
            INSERT INTO fx_rates_daily (rate_date, base_currency, quote_currency, rate)
            VALUES (%s, 'USD', %s, %s::numeric)
            """,
            (day, quote, rate),
        )


def test_latest_valid_gbp_rate_is_pair_specific_skips_nan_and_never_reads_the_future(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _rates(
        ebull_test_conn,
        [
            ("2026-09-01", "GBP", "0.75"),
            # CHECK (rate > 0) admits NaN: 'NaN' > 0 is TRUE in Postgres.
            ("2026-09-03", "GBP", "NaN"),
            # A newer EUR row must not lend its date to GBP.
            ("2026-09-04", "EUR", "0.9"),
            ("2026-09-08", "GBP", "0.76"),
        ],
    )
    assert latest_valid_gbp_rate(ebull_test_conn, date(2026, 9, 7)) == FxRate(date(2026, 9, 1), Decimal("0.75"))
    assert latest_valid_gbp_rate(ebull_test_conn, date(2026, 9, 8)) == FxRate(date(2026, 9, 8), Decimal("0.76"))
    assert latest_valid_gbp_rate(ebull_test_conn, date(2026, 8, 31)) is None


def test_wealth_history_always_carries_the_comparator(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    response = get_strategy_wealth_history(days=365, conn=ebull_test_conn)
    comparator = response.comparator
    assert comparator.symbol == COMPARATOR_SYMBOL
    assert comparator.unavailable_reason == "comparator_instrument_missing"
    assert comparator.comparator_return is None and comparator.engine_excess is None
    assert comparator.hurdle.unavailable_reason == "mandate_unconfigured"
    assert comparator.gbp.unavailable_reason == "comparator_unavailable"
    assert comparator.gbp.fx_start_date is None and comparator.gbp.comparator_return is None
    assert response.benchmark_comparison_available is False


def test_wealth_history_comparator_and_hurdle_against_a_funded_pot(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _instrument(conn)
    # An unfunded base yesterday, funded today: S is the base date, not return_since.
    conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (
            snapshot_date, display_currency, total_value, positions_value, cash_value,
            positions_total, positions_priced, computed_at
        ) VALUES (CURRENT_DATE - 1, 'USD', 0, 0, 0, 0, 0, now()),
                 (CURRENT_DATE, 'USD', 0, 0, 0, 0, 0, now())
        """
    )
    conn.execute(
        """
        INSERT INTO price_daily (instrument_id, price_date, close)
        VALUES (%(id)s, CURRENT_DATE - 1, 100), (%(id)s, CURRENT_DATE, 102)
        """,
        {"id": _INSTRUMENT_ID},
    )
    conn.execute(
        """
        INSERT INTO fx_rates_daily (rate_date, base_currency, quote_currency, rate)
        VALUES (CURRENT_DATE - 1, 'USD', 'GBP', 0.8), (CURRENT_DATE, 'USD', 'GBP', 0.75)
        """
    )
    configure_paper_pool(
        conn,
        enabled=True,
        capital_limit=Decimal("1000"),
        risk_profile="growth",
        approval_mode="manual",
        max_concurrent_positions_override=None,
        changed_by="test",
        reason="fund the pot",
    )
    conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision, enabled, base_currency, core_instrument_id, core_target_pct,
            liquidity_reserve_pct, rebalance_band_pct, min_rebalance_amount,
            policy_version, changed_by, reason, mode
        ) VALUES (1, true, 'USD', %s, 50, 10, 5, 25, %s, 'test', 'test', %s)
        """,
        (_INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION, CORE_MANDATE_MODE),
    )
    today = conn.execute("SELECT CURRENT_DATE").fetchone()
    assert today is not None

    response = get_strategy_wealth_history(days=365, conn=conn)

    comparator = response.comparator
    assert response.total_return_available is True
    assert response.return_since == today[0]
    assert comparator.unavailable_reason is None
    assert comparator.window_end == today[0]
    assert comparator.window_start is not None and comparator.window_start < today[0]
    assert (comparator.start_close_date, comparator.end_close_date) == (comparator.window_start, today[0])
    assert comparator.comparator_return == Decimal("0.02")
    assert comparator.engine_excess == Decimal("-0.02")
    hurdle = comparator.hurdle
    assert (hurdle.core_target_weight, hurdle.core_weight, hurdle.active_weight) == (
        Decimal("0.5"),
        Decimal("0.35"),
        Decimal("0.3"),
    )
    assert hurdle.core_mandate_event_id is not None and hurdle.pool_event_id is not None
    multiplier = Decimal("0.65") / Decimal("0.3")
    assert hurdle.multiplier == multiplier
    assert hurdle.hurdle_return == Decimal("0.02") * multiplier
    assert hurdle.unavailable_reason is None
    # The refused S&P 500 TR index line stays refused beside an available comparator.
    assert response.benchmark_comparison_available is False
    assert {refusal.benchmark for refusal in response.benchmark_refusals} >= {"sp500_total_return"}
    gbp = comparator.gbp
    assert gbp.unavailable_reason is None
    assert (gbp.fx_start_date, gbp.fx_end_date) == (comparator.window_start, today[0])
    multiplier_gbp = Decimal("0.75") / Decimal("0.8")
    assert gbp.fx_multiplier == multiplier_gbp
    assert gbp.comparator_return == Decimal("1.02") * multiplier_gbp - 1
    assert gbp.engine_return == multiplier_gbp - 1
    assert gbp.engine_excess == multiplier_gbp * Decimal("-0.02")
    wire = response.model_dump(mode="json", by_alias=True)["comparator"]
    assert wire["return"] == "0.02"
    assert "comparator_return" not in wire


def test_a_matching_listing_wins_over_another_row_with_the_symbol(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # A lower-id, primary row in the wrong currency must not mask the USD/LSE class.
    ebull_test_conn.execute(
        """
        INSERT INTO instruments
            (instrument_id, symbol, company_name, currency, exchange, is_tradable, is_primary_listing)
        VALUES (2461600, %s, 'iShares Core S&P 500 UCITS ETF', 'GBP', '7', true, true)
        """,
        (COMPARATOR_SYMBOL,),
    )
    _instrument(ebull_test_conn)
    response = get_strategy_wealth_history(days=365, conn=ebull_test_conn)
    assert response.comparator.instrument_id == _INSTRUMENT_ID
    assert response.comparator.unavailable_reason == "pot_return_unavailable"
