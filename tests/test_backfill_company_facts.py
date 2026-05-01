"""Tests for the bulk SEC company-facts backfill script (#759).

Pins the cohort selector. The end-to-end refresh + normalize path is
covered by ``test_force_refresh_fundamentals`` and the existing
fundamentals tests; this file just verifies the cohort SELECT
correctly enumerates primary-SEC-CIK instruments and respects
``--start-from`` / ``--limit``.

Uses the canonical ``ebull_test_conn`` fixture from
``tests/conftest.py`` so the test-DB URL is derived from
``settings.database_url`` rather than hardcoded.
"""

from __future__ import annotations

import psycopg

from scripts.backfill_company_facts import (
    _exit_code_from_failures,
    count_facts_without_periods,
    select_all_primary_sec_instrument_ids,
    select_cohort,
)


def _seed(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    symbol: str,
    *,
    cik: str | None,
    is_primary_cik: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, country, asset_class)
        VALUES (%s, %s, 'US', 'us_equity')
        ON CONFLICT (exchange_id) DO NOTHING
        """,
        (f"bcf_{instrument_id}", f"Test {instrument_id}"),
    )
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id, symbol, f"Test {symbol}", f"bcf_{instrument_id}"),
    )
    if cik is not None:
        conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, %s)
            ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
            """,
            (instrument_id, cik, is_primary_cik),
        )


def test_cohort_picks_only_primary_sec_cik_instruments(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # A: has primary SEC CIK → IN cohort.
    _seed(ebull_test_conn, 9501, "BCF_AAA", cik="0000111111", is_primary_cik=True)

    # B: has secondary (is_primary=False) SEC CIK → EXCLUDED.
    # Operator-side rationale: secondary CIKs are historical / spin-off
    # holdovers; refreshing them would double-fetch the same issuer.
    _seed(ebull_test_conn, 9502, "BCF_BBB", cik="0000222222", is_primary_cik=False)

    # C: instrument with no external_identifiers row → EXCLUDED.
    _seed(ebull_test_conn, 9503, "BCF_CCC", cik=None)

    cohort = select_cohort(ebull_test_conn, start_from=0, limit=None)
    cohort_ids = {r.instrument_id for r in cohort}

    assert 9501 in cohort_ids
    assert 9502 not in cohort_ids
    assert 9503 not in cohort_ids


def test_cohort_start_from_skips_lower_ids(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    for iid in range(9601, 9606):
        _seed(ebull_test_conn, iid, f"BCF_S{iid}", cik=f"00000{iid:05d}")

    cohort = select_cohort(ebull_test_conn, start_from=9603, limit=None)
    cohort_ids = [r.instrument_id for r in cohort]

    # ``start_from`` is exclusive — IDs ≤ 9603 are skipped.
    assert 9601 not in cohort_ids
    assert 9603 not in cohort_ids
    assert 9604 in cohort_ids
    assert 9605 in cohort_ids


def test_cohort_limit_caps_size_within_seeded_range(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seeded = list(range(9701, 9706))
    for iid in seeded:
        _seed(ebull_test_conn, iid, f"BCF_L{iid}", cik=f"00001{iid:05d}")

    # Pin the LIMIT contract robustly even if the fixture's
    # per-test TRUNCATE stops working: rather than asserting the
    # cohort equals a positional slice of seeded ids (which would
    # break the moment IDs from a sibling test leak in), assert
    # ``limit`` truncates AND the truncated set is a subset of what
    # this test actually seeded. Reviewer PREVENTION pin on PR #760.
    cohort = select_cohort(ebull_test_conn, start_from=9700, limit=3)
    assert len(cohort) == 3
    cohort_ids = {r.instrument_id for r in cohort}
    assert cohort_ids.issubset(set(seeded))
    # ORDER BY instrument_id ASC — the three smallest seeded IDs.
    assert sorted(cohort_ids) == seeded[:3]


def test_cohort_start_from_combined_with_limit_advances_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    seeded = list(range(9751, 9756))
    for iid in seeded:
        _seed(ebull_test_conn, iid, f"BCF_W{iid}", cik=f"00002{iid:05d}")

    # ``start_from=9752`` skips ID 9751 + 9752; ``limit=2`` then caps
    # at 9753, 9754. Reviewer NITPICK pin: previously test name
    # implied this interaction but called ``start_from=0``.
    cohort = select_cohort(ebull_test_conn, start_from=9752, limit=2)
    assert len(cohort) == 2
    cohort_ids = {r.instrument_id for r in cohort}
    assert cohort_ids.issubset(set(seeded))
    assert sorted(cohort_ids) == [9753, 9754]


def test_cohort_returns_symbol_and_cik(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed(ebull_test_conn, 9801, "BCF_X", cik="0000777777")
    cohort = select_cohort(ebull_test_conn, start_from=0, limit=None)
    matched = [r for r in cohort if r.instrument_id == 9801]
    assert len(matched) == 1
    assert matched[0].symbol == "BCF_X"
    assert matched[0].cik == "0000777777"


def test_exit_code_from_failures_clean() -> None:
    assert _exit_code_from_failures(0, 10, normalize_failed=0) == 0


def test_exit_code_from_failures_total_fetch_failure() -> None:
    assert _exit_code_from_failures(10, 10, normalize_failed=0) == 1


def test_exit_code_from_failures_partial_fetch_failure() -> None:
    assert _exit_code_from_failures(3, 10, normalize_failed=0) == 2


def test_exit_code_from_failures_normalize_failure_alone_flips_to_2() -> None:
    # Codex review High #2 — normalize rollbacks must surface even
    # when every fetch succeeded.
    assert _exit_code_from_failures(0, 10, normalize_failed=2) == 2


def test_exit_code_from_failures_total_with_zero_cohort_is_clean() -> None:
    # Edge: empty cohort with zero failures must not trip the
    # "fetch_failed == fetch_total" branch.
    assert _exit_code_from_failures(0, 0, normalize_failed=0) == 0


def test_select_all_primary_returns_every_primary_cik(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed(ebull_test_conn, 9901, "BCF_AP_A", cik="0000900001", is_primary_cik=True)
    _seed(ebull_test_conn, 9902, "BCF_AP_B", cik="0000900002", is_primary_cik=True)
    _seed(ebull_test_conn, 9903, "BCF_AP_C", cik="0000900003", is_primary_cik=False)
    _seed(ebull_test_conn, 9904, "BCF_AP_D", cik=None)

    ids = select_all_primary_sec_instrument_ids(ebull_test_conn)
    assert 9901 in ids
    assert 9902 in ids
    assert 9903 not in ids
    assert 9904 not in ids


def test_count_facts_without_periods_flags_instruments_with_no_canonical(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    from datetime import date

    # A: has facts, no canonical row → counts as 1 (apparent rollback).
    _seed(ebull_test_conn, 9911, "BCF_PROBE_A", cik="0000910001")
    ebull_test_conn.execute(
        """
        INSERT INTO financial_facts_raw
            (instrument_id, concept, unit, period_start, period_end, val,
             form_type, fiscal_year, fiscal_period, accession_number, filed_date)
        VALUES (%s, 'Revenues', 'USD', NULL, %s, 1000, '10-K', 2024, 'FY', 'a-9911', %s)
        ON CONFLICT DO NOTHING
        """,
        (9911, date(2024, 12, 31), date(2025, 2, 1)),
    )

    # B: has facts AND canonical row → not counted.
    _seed(ebull_test_conn, 9912, "BCF_PROBE_B", cik="0000910002")
    ebull_test_conn.execute(
        """
        INSERT INTO financial_facts_raw
            (instrument_id, concept, unit, period_start, period_end, val,
             form_type, fiscal_year, fiscal_period, accession_number, filed_date)
        VALUES (%s, 'Revenues', 'USD', NULL, %s, 2000, '10-K', 2024, 'FY', 'a-9912', %s)
        ON CONFLICT DO NOTHING
        """,
        (9912, date(2024, 12, 31), date(2025, 2, 1)),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO financial_periods
            (instrument_id, fiscal_year, fiscal_quarter, period_end_date,
             period_start_date, period_type, form_type, filed_date,
             reported_currency, source, source_ref)
        VALUES (%s, 2024, 4, %s, %s, 'FY', '10-K', %s, 'USD', 'sec', 'a-9912')
        ON CONFLICT DO NOTHING
        """,
        (9912, date(2024, 12, 31), date(2024, 1, 1), date(2025, 2, 1)),
    )

    # C: no facts, no canonical row → not counted (nothing to normalize).
    _seed(ebull_test_conn, 9913, "BCF_PROBE_C", cik="0000910003")

    assert count_facts_without_periods(ebull_test_conn, [9911, 9912, 9913]) == 1


def test_count_facts_without_periods_handles_empty_cohort(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    assert count_facts_without_periods(ebull_test_conn, []) == 0
