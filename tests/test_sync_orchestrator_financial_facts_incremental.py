"""End-to-end integration test for daily_financial_facts via the
scheduler entrypoint. Real DB, stubbed providers, full flow:

    plan_refresh -> execute_refresh -> normalization.

Run 1 (fresh install): cohort has no watermarks. Planner seeds every
covered CIK. Executor writes facts + both watermarks. Normalization
runs on touched instruments.

Run 2 (steady state): all master-index days 304. Planner returns
empty plan. Executor no-ops. Zero fetch_submissions + zero
extract_facts calls prove the short-circuit held.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from app.services.sec_incremental import LOOKBACK_DAYS
from app.services.watermarks import get_watermark
from tests.fixtures.ebull_test_db import (
    ebull_test_conn,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)
from tests.fixtures.sec_stubs import (
    StubFilingsProvider,
    StubFundamentalsProvider,
    sample_fact,
    submissions_json,
)

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


FIXTURE_MASTER = Path("tests/fixtures/sec/master_20260415.idx")
TODAY = date(2026, 4, 15)
CIK = "0000320193"
ACCESSION = "0000320193-26-000042"


def _seed_cohort(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'AAPL', 'APPLE INC', TRUE)"
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (1, 'sec', 'cik', %s, TRUE)",
        (CIK,),
    )
    conn.commit()


def _build_stubs_run_one() -> tuple[StubFilingsProvider, StubFundamentalsProvider]:
    master = FIXTURE_MASTER.read_bytes()
    filings = StubFilingsProvider(
        master_bodies={TODAY: master} | {TODAY - timedelta(days=i): None for i in range(1, LOOKBACK_DAYS)},
        submissions_by_cik={CIK: submissions_json(ACCESSION, form="10-Q")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={CIK: [sample_fact(ACCESSION)]},
    )
    return filings, fundamentals


def _build_stubs_run_two() -> tuple[StubFilingsProvider, StubFundamentalsProvider]:
    # Every lookback day returns None — master-index 304 or 404.
    filings = StubFilingsProvider(
        master_bodies={TODAY - timedelta(days=i): None for i in range(LOOKBACK_DAYS)},
    )
    fundamentals = StubFundamentalsProvider()
    return filings, fundamentals


def test_fresh_install_seeds_cohort_then_steady_state_is_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_cohort(ebull_test_conn)

    # ----- Run 1 -----
    filings_1, fundamentals_1 = _build_stubs_run_one()

    # The scheduler opens its own connection via psycopg.connect. Patch
    # it to return the test connection so the planner + executor share
    # the transactional scope the test assertions read from.
    fake_connect_cm = MagicMock()
    fake_connect_cm.__enter__.return_value = ebull_test_conn
    fake_connect_cm.__exit__.return_value = None

    fake_datetime = MagicMock()
    fake_datetime.now.return_value = datetime(TODAY.year, TODAY.month, TODAY.day, tzinfo=UTC)

    with (
        patch("app.workers.scheduler.SecFilingsProvider", return_value=filings_1),
        patch("app.workers.scheduler.SecFundamentalsProvider", return_value=fundamentals_1),
        patch("app.workers.scheduler.psycopg.connect", return_value=fake_connect_cm),
        patch("app.workers.scheduler.datetime", fake_datetime),
    ):
        from app.workers.scheduler import daily_financial_facts

        daily_financial_facts()

    # Seed results
    submissions_wm = get_watermark(ebull_test_conn, "sec.submissions", CIK)
    assert submissions_wm is not None
    assert submissions_wm.watermark == ACCESSION

    companyfacts_wm = get_watermark(ebull_test_conn, "sec.companyfacts", CIK)
    assert companyfacts_wm is not None
    assert companyfacts_wm.watermark == ACCESSION

    row_count = ebull_test_conn.execute("SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = 1").fetchone()
    assert row_count is not None and row_count[0] >= 1

    # A data_ingestion_runs row landed for this pass.
    status_row = ebull_test_conn.execute(
        "SELECT status FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1"
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "success"

    # Phase 2 (normalization) must have fired on the seeded instrument.
    periods_count = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM financial_periods_raw WHERE instrument_id = 1"
    ).fetchone()
    assert periods_count is not None and periods_count[0] >= 1

    # ----- Run 2 -----
    filings_2, fundamentals_2 = _build_stubs_run_two()
    fake_connect_cm_2 = MagicMock()
    fake_connect_cm_2.__enter__.return_value = ebull_test_conn
    fake_connect_cm_2.__exit__.return_value = None

    with (
        patch("app.workers.scheduler.SecFilingsProvider", return_value=filings_2),
        patch("app.workers.scheduler.SecFundamentalsProvider", return_value=fundamentals_2),
        patch("app.workers.scheduler.psycopg.connect", return_value=fake_connect_cm_2),
        patch("app.workers.scheduler.datetime", fake_datetime),
    ):
        from app.workers.scheduler import daily_financial_facts

        daily_financial_facts()

    # Planner fetched every lookback day's master-index (all 304).
    assert filings_2.fetch_master_calls == LOOKBACK_DAYS
    # Short-circuit: planner never called fetch_submissions or extract_facts.
    assert filings_2.fetch_submissions_calls == 0
    assert fundamentals_2.extract_calls == []
