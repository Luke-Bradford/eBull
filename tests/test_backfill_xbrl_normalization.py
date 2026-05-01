"""Tests for the XBRL normalization backfill script (#735).

Pins the cohort selector — the script's only piece of non-trivial
logic. The end-to-end normalization path is covered by the existing
fundamentals tests; this file just verifies the cohort SELECT picks
the right instruments under each ``--all-instruments`` flag setting,
and that the post-normalize verification probe correctly reports
unresolved migration-088 columns.

Uses the canonical ``ebull_test_conn`` fixture from
``tests/conftest.py`` so the test-DB URL is derived from
``settings.database_url`` rather than hardcoded.
"""

from __future__ import annotations

from datetime import date

import psycopg

from scripts.backfill_xbrl_normalization import (
    count_unprojected_088,
    select_cohort,
)


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    symbol: str,
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"bxn_{instrument_id}", f"Test {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"bxn_{instrument_id}"),
    )


def _seed_fact(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    concept: str,
    val: int = 100,
) -> None:
    conn.execute(
        """
        INSERT INTO financial_facts_raw
            (instrument_id, concept, unit, period_start, period_end, val,
             form_type, fiscal_year, fiscal_period, accession_number, filed_date)
        VALUES (%s, %s, 'shares', NULL, %s, %s, '10-K', 2024, 'FY', %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            concept,
            date(2024, 12, 31),
            val,
            f"acc-{instrument_id}-{concept}",
            date(2025, 2, 1),
        ),
    )


def _seed_period(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    *,
    treasury_shares: int | None = None,
    shares_authorized: int | None = None,
    shares_issued: int | None = None,
    retained_earnings: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO financial_periods
            (instrument_id, fiscal_year, fiscal_quarter, period_end_date,
             period_start_date, period_type, form_type, filed_date,
             reported_currency, source, source_ref,
             treasury_shares, shares_authorized, shares_issued, retained_earnings)
        VALUES (%s, 2024, 4, %s, %s, 'FY', '10-K', %s, 'USD', 'sec', %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            date(2024, 12, 31),
            date(2024, 1, 1),
            date(2025, 2, 1),
            f"acc-{instrument_id}-period",
            treasury_shares,
            shares_authorized,
            shares_issued,
            retained_earnings,
        ),
    )


def test_cohort_picks_issuers_with_any_unprojected_088_column(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # A: TreasuryStockShares in raw, treasury_shares NULL in canonical → IN cohort.
    _seed_instrument(ebull_test_conn, 9001, "AAA")
    _seed_fact(ebull_test_conn, 9001, "TreasuryStockShares", val=50)
    _seed_period(ebull_test_conn, 9001)

    # B: All 088 columns populated → EXCLUDED.
    _seed_instrument(ebull_test_conn, 9002, "BBB")
    _seed_fact(ebull_test_conn, 9002, "TreasuryStockShares", val=75)
    _seed_period(
        ebull_test_conn,
        9002,
        treasury_shares=75,
        shares_authorized=1_000_000,
        shares_issued=900_000,
        retained_earnings=500_000,
    )

    # C: Raw facts but no 088-relevant concepts → EXCLUDED.
    _seed_instrument(ebull_test_conn, 9003, "CCC")
    _seed_fact(ebull_test_conn, 9003, "Revenues", val=1000)
    _seed_period(ebull_test_conn, 9003)

    # D: TreasuryStockCommonShares (alias) → IN cohort.
    _seed_instrument(ebull_test_conn, 9004, "DDD")
    _seed_fact(ebull_test_conn, 9004, "TreasuryStockCommonShares", val=20)
    _seed_period(ebull_test_conn, 9004)

    # E: CommonStockSharesAuthorized in raw, shares_authorized NULL → IN cohort.
    # This is the case the original narrow cohort missed (codex review high).
    _seed_instrument(ebull_test_conn, 9005, "EEE")
    _seed_fact(ebull_test_conn, 9005, "CommonStockSharesAuthorized", val=2_000_000)
    _seed_period(ebull_test_conn, 9005)

    # F: RetainedEarningsAccumulatedDeficit in raw, retained_earnings NULL → IN cohort.
    _seed_instrument(ebull_test_conn, 9006, "FFF")
    _seed_fact(ebull_test_conn, 9006, "RetainedEarningsAccumulatedDeficit", val=100_000)
    _seed_period(ebull_test_conn, 9006)

    cohort = select_cohort(ebull_test_conn, only_unprojected_088=True, limit=None)

    assert 9001 in cohort
    assert 9002 not in cohort
    assert 9003 not in cohort
    assert 9004 in cohort
    assert 9005 in cohort
    assert 9006 in cohort


def test_cohort_all_instruments_picks_every_issuer_with_raw_facts(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, 9101, "EEE")
    _seed_fact(ebull_test_conn, 9101, "Revenues")

    _seed_instrument(ebull_test_conn, 9102, "FFF")
    _seed_fact(ebull_test_conn, 9102, "TreasuryStockShares")
    _seed_period(
        ebull_test_conn,
        9102,
        treasury_shares=999,
        shares_authorized=1,
        shares_issued=1,
        retained_earnings=1,
    )  # already populated — still in cohort under --all-instruments.

    _seed_instrument(ebull_test_conn, 9103, "GGG")
    # No raw facts → EXCLUDED even with --all-instruments.

    cohort = select_cohort(ebull_test_conn, only_unprojected_088=False, limit=None)

    assert 9101 in cohort
    assert 9102 in cohort
    assert 9103 not in cohort


def test_cohort_limit_caps_size(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    for iid in range(9201, 9206):
        _seed_instrument(ebull_test_conn, iid, f"L{iid}")
        _seed_fact(ebull_test_conn, iid, "TreasuryStockShares")
        _seed_period(ebull_test_conn, iid)

    cohort = select_cohort(ebull_test_conn, only_unprojected_088=True, limit=3)
    assert len(cohort) == 3
    assert cohort == [9201, 9202, 9203]


def test_count_unprojected_088_returns_zero_after_full_projection(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Issuer with raw treasury fact + canonical row populated.
    _seed_instrument(ebull_test_conn, 9301, "PPP")
    _seed_fact(ebull_test_conn, 9301, "TreasuryStockShares", val=50)
    _seed_period(
        ebull_test_conn,
        9301,
        treasury_shares=50,
        shares_authorized=1,
        shares_issued=1,
        retained_earnings=1,
    )
    assert count_unprojected_088(ebull_test_conn, [9301]) == 0


def test_count_unprojected_088_flags_partial_failure(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Issuer with raw retained-earnings fact but canonical column NULL —
    # simulates a normalize() that rolled back this instrument.
    _seed_instrument(ebull_test_conn, 9401, "QQQ")
    _seed_fact(ebull_test_conn, 9401, "RetainedEarningsAccumulatedDeficit", val=100)
    _seed_period(ebull_test_conn, 9401)  # all 088 columns NULL

    # Healthy issuer in same cohort — should not contribute to count.
    _seed_instrument(ebull_test_conn, 9402, "RRR")
    _seed_fact(ebull_test_conn, 9402, "TreasuryStockShares", val=10)
    _seed_period(
        ebull_test_conn,
        9402,
        treasury_shares=10,
        shares_authorized=1,
        shares_issued=1,
        retained_earnings=1,
    )

    assert count_unprojected_088(ebull_test_conn, [9401, 9402]) == 1


def test_count_unprojected_088_handles_empty_cohort(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Defensive: empty list short-circuits to 0 without hitting Postgres
    # with a `WHERE id = ANY('{}')` corner case.
    assert count_unprojected_088(ebull_test_conn, []) == 0
