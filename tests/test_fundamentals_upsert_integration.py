"""Integration tests for ``upsert_facts_for_instrument`` — ADR 0004.

Exercises the real Postgres identity index via ``ebull_test`` so the
Shape B (``executemany``) implementation is verified end-to-end:

- Identity branch with non-null ``period_start`` (duration fact).
- Identity branch with NULL ``period_start`` (instant / balance-sheet
  fact) — goes through ``COALESCE(period_start, '0001-01-01'::date)``.
- ``ON CONFLICT DO UPDATE WHERE IS DISTINCT FROM`` short-circuit for
  an unchanged re-upsert (idempotent path).
- Restatement — same identity tuple, mutated ``val`` — actually
  rewrites the row and increments ``upserted``.

These are the branches the bench [`scripts/bench_fundamentals_upsert.py`](../scripts/bench_fundamentals_upsert.py)
exercises against synthetic data; this test confirms the production
code path behaves the same against the real index.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.providers.fundamentals import XbrlFact
from app.services.fundamentals import (
    normalize_financial_periods,
    start_ingestion_run,
    upsert_facts_for_instrument,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 (fixture re-export)
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


_INSTRUMENT_ID = 1001


def _seed_instrument(conn: psycopg.Connection[tuple]) -> None:
    # Idempotent: the ``ebull_test_conn`` fixture truncates
    # ``instruments`` between tests, but an ON CONFLICT guard makes the
    # seed safe to re-run when the bench or another script has left a
    # stale ``TEST`` row behind.
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'TEST', 'Test Inc.', TRUE) "
        "ON CONFLICT (instrument_id) DO NOTHING",
        (_INSTRUMENT_ID,),
    )
    conn.commit()


def _duration_fact(accession: str, val: Decimal) -> XbrlFact:
    """Revenue-like fact with a non-null period_start."""
    return XbrlFact(
        concept="Revenues",
        taxonomy="us-gaap",
        unit="USD",
        period_start=date(2023, 1, 1),
        period_end=date(2023, 12, 31),
        val=val,
        frame="CY2023",
        accession_number=accession,
        form_type="10-K",
        filed_date=date(2024, 3, 15),
        fiscal_year=2023,
        fiscal_period="FY",
        decimals="-3",
    )


def _instant_fact(accession: str, val: Decimal) -> XbrlFact:
    """Balance-sheet-like fact with a NULL period_start."""
    return XbrlFact(
        concept="CashAndCashEquivalentsAtCarryingValue",
        taxonomy="us-gaap",
        unit="USD",
        period_start=None,
        period_end=date(2023, 12, 31),
        val=val,
        frame=None,
        accession_number=accession,
        form_type="10-K",
        filed_date=date(2024, 3, 15),
        fiscal_year=2023,
        fiscal_period="FY",
        decimals="-3",
    )


def test_seed_inserts_both_instant_and_duration_facts(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn)
    run_id = start_ingestion_run(
        ebull_test_conn,
        source="sec_edgar",
        endpoint="/test",
        instrument_count=1,
    )
    ebull_test_conn.commit()

    facts = [
        _duration_fact("acc-1", Decimal("100")),
        _instant_fact("acc-1", Decimal("50")),
    ]
    upserted, skipped = upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=facts,
        ingestion_run_id=run_id,
    )
    ebull_test_conn.commit()

    assert upserted == 2
    assert skipped == 0

    row = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = %s",
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert row is not None
    assert row[0] == 2


def test_reupsert_unchanged_is_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn)
    run_id = start_ingestion_run(
        ebull_test_conn,
        source="sec_edgar",
        endpoint="/test",
        instrument_count=1,
    )
    ebull_test_conn.commit()

    facts = [_duration_fact("acc-1", Decimal("100"))]
    upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=facts,
        ingestion_run_id=run_id,
    )
    ebull_test_conn.commit()

    # Re-upsert identical payload — the WHERE IS DISTINCT FROM filter
    # must short-circuit every row.
    upserted, skipped = upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=facts,
        ingestion_run_id=run_id,
    )
    ebull_test_conn.commit()

    assert upserted == 0
    assert skipped == 1


def test_restatement_rewrites_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn)
    run_id = start_ingestion_run(
        ebull_test_conn,
        source="sec_edgar",
        endpoint="/test",
        instrument_count=1,
    )
    ebull_test_conn.commit()

    upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=[_duration_fact("acc-1", Decimal("100"))],
        ingestion_run_id=run_id,
    )
    ebull_test_conn.commit()

    # Same identity tuple, mutated val — DO UPDATE WHERE matches.
    upserted, skipped = upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=[_duration_fact("acc-1", Decimal("200"))],
        ingestion_run_id=run_id,
    )
    ebull_test_conn.commit()

    assert upserted == 1
    assert skipped == 0

    row = ebull_test_conn.execute(
        "SELECT val FROM financial_facts_raw WHERE instrument_id = %s",
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("200.000000")


def test_batches_across_chunk_boundary(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Feed more than one chunk's worth of facts so the executemany
    # page_size (1000) is crossed at least once. Verifies aggregated
    # rowcount across chunks is correct against a real DB — the mock
    # tests cover the split path; this covers the real driver contract.
    _seed_instrument(ebull_test_conn)
    run_id = start_ingestion_run(
        ebull_test_conn,
        source="sec_edgar",
        endpoint="/test",
        instrument_count=1,
    )
    ebull_test_conn.commit()

    facts = [_duration_fact(f"acc-{i:05d}", Decimal(f"{100 + i}")) for i in range(1500)]
    upserted, skipped = upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=facts,
        ingestion_run_id=run_id,
    )
    ebull_test_conn.commit()

    assert upserted == 1500
    assert skipped == 0

    row = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = %s",
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert row is not None
    assert row[0] == 1500


def test_normalize_binds_frameless_annual_and_cleans_invalid_fy(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#1835 end-to-end through the real canonical pipeline.

    1. A 12-month annual revenue fact with ``frame=None`` binds the FY revenue
       (the old Frames-label YTD filter dropped every frameless duration fact,
       leaving FY revenue NULL for ~43% of annual facts).
    2. A stale STRUCTURALLY-INVALID FY row (``months_covered=3`` — a 3-month
       fact mislabeled fp=FY) is removed by the Phase-B2 cleanup.
    3. SAFETY: a valid ``months_covered=12`` annual row whose facts have aged
       out of retention-swept ``financial_facts_raw`` is PRESERVED — Phase-B2
       must not truncate durable canonical history.
    """
    _seed_instrument(ebull_test_conn)
    run_id = start_ingestion_run(
        ebull_test_conn,
        source="sec_edgar",
        endpoint="/test",
        instrument_count=1,
    )
    ebull_test_conn.commit()

    annual = XbrlFact(
        concept="Revenues",
        taxonomy="us-gaap",
        unit="USD",
        period_start=date(2023, 10, 1),
        period_end=date(2024, 9, 28),  # ~363 days — inside the [335,395] FY window
        val=Decimal("391035000000"),
        frame=None,  # #1835 — frameless annual must still bind
        accession_number="acc-fy24",
        form_type="10-K",
        filed_date=date(2024, 11, 1),
        fiscal_year=2024,
        fiscal_period="FY",
        decimals="-3",
    )
    cash = XbrlFact(
        concept="CashAndCashEquivalentsAtCarryingValue",
        taxonomy="us-gaap",
        unit="USD",
        period_start=None,
        period_end=date(2024, 9, 28),
        val=Decimal("30000000000"),
        frame=None,
        accession_number="acc-fy24",
        form_type="10-K",
        filed_date=date(2024, 11, 1),
        fiscal_year=2024,
        fiscal_period="FY",
        decimals="-3",
    )
    upsert_facts_for_instrument(
        ebull_test_conn,
        instrument_id=_INSTRUMENT_ID,
        facts=[annual, cash],
        ingestion_run_id=run_id,
    )

    # Stale mislabeled 3-month FY row (months_covered=3) — must be deleted.
    ebull_test_conn.execute(
        "INSERT INTO financial_periods (instrument_id, period_end_date, "
        "period_type, fiscal_year, fiscal_quarter, months_covered, source, "
        "source_ref, reported_currency, is_restated, is_derived, "
        "normalization_status) "
        "VALUES (%s, '2020-09-26', 'FY', 2020, NULL, 3, 'sec_edgar', 'stale', "
        "'USD', FALSE, FALSE, 'normalized')",
        (_INSTRUMENT_ID,),
    )
    # Valid annual row whose facts have aged out of raw (months_covered=12) —
    # must be PRESERVED (Phase-B2 must not truncate durable history).
    ebull_test_conn.execute(
        "INSERT INTO financial_periods (instrument_id, period_end_date, "
        "period_type, fiscal_year, fiscal_quarter, months_covered, revenue, "
        "source, source_ref, reported_currency, is_restated, is_derived, "
        "normalization_status) "
        "VALUES (%s, '2015-09-26', 'FY', 2015, NULL, 12, 233715000000, "
        "'sec_edgar', 'aged-out', 'USD', FALSE, FALSE, 'normalized')",
        (_INSTRUMENT_ID,),
    )
    ebull_test_conn.commit()

    normalize_financial_periods(ebull_test_conn, instrument_ids=[_INSTRUMENT_ID])
    ebull_test_conn.commit()

    bound = ebull_test_conn.execute(
        "SELECT revenue, months_covered FROM financial_periods "
        "WHERE instrument_id = %s AND period_type = 'FY' AND fiscal_year = 2024",
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert bound is not None
    assert bound[0] == Decimal("391035000000")
    assert bound[1] == 12

    invalid = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM financial_periods WHERE instrument_id = %s AND fiscal_year = 2020",
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert invalid is not None
    assert invalid[0] == 0  # mislabeled 3-month FY row removed

    preserved = ebull_test_conn.execute(
        "SELECT revenue FROM financial_periods WHERE instrument_id = %s AND fiscal_year = 2015 AND period_type = 'FY'",
        (_INSTRUMENT_ID,),
    ).fetchone()
    assert preserved is not None  # valid aged-out annual history NOT truncated
    assert preserved[0] == Decimal("233715000000")
