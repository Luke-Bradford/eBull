"""Tests for app.services.sec_incremental.execute_refresh."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from logging import LogRecord
from typing import cast

import psycopg
import pytest

from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_edgar import MasterIndexEntry, SecFilingsProvider
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.fundamentals import (
    RefreshOutcome,
    RefreshPlan,
    execute_refresh,
)
from app.services.watermarks import get_watermark, set_watermark
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


@dataclass
class StubFilingsProvider:
    submissions_by_cik: dict[str, dict[str, object]] = field(default_factory=dict)
    fetch_calls: int = 0

    def fetch_submissions(self, cik: str) -> dict[str, object] | None:
        self.fetch_calls += 1
        return self.submissions_by_cik.get(cik)


@dataclass
class StubFundamentalsProvider:
    facts_by_cik: dict[str, list[XbrlFact]] = field(default_factory=dict)
    fail_on: set[str] = field(default_factory=set)
    extract_calls: list[str] = field(default_factory=list)

    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        self.extract_calls.append(cik)
        if cik in self.fail_on:
            raise RuntimeError(f"boom for {cik}")
        return self.facts_by_cik.get(cik, [])


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    cik: str,
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, symbol),
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', %s, TRUE)",
        (instrument_id, cik),
    )
    conn.commit()


def _sample_fact(accession: str) -> XbrlFact:
    return XbrlFact(
        concept="Revenues",
        taxonomy="us-gaap",
        unit="USD",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        val=Decimal("90000000000"),
        frame=None,
        accession_number=accession,
        form_type="10-Q",
        filed_date=date(2026, 4, 15),
        fiscal_year=2026,
        fiscal_period="Q1",
        decimals="-6",
    )


def _submissions_with_top(accession: str, form: str = "10-Q") -> dict[str, object]:
    return {
        "filings": {
            "recent": {
                "accessionNumber": [accession],
                "form": [form],
                "acceptedDate": ["2026-04-15T16:05:00.000Z"],
            }
        }
    }


def test_empty_plan_is_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    filings = StubFilingsProvider()
    fundamentals = StubFundamentalsProvider()

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=RefreshPlan(),
    )

    assert outcome == RefreshOutcome()
    assert filings.fetch_calls == 0
    assert fundamentals.extract_calls == []


def test_seed_writes_facts_and_both_watermarks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(seeds=["0000320193"])
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.seeded == 1
    assert outcome.failed == []

    submissions_wm = get_watermark(ebull_test_conn, "sec.submissions", "0000320193")
    assert submissions_wm is not None
    assert submissions_wm.watermark == "0000320193-26-000042"

    companyfacts_wm = get_watermark(ebull_test_conn, "sec.companyfacts", "0000320193")
    assert companyfacts_wm is not None
    assert companyfacts_wm.watermark == "0000320193-26-000042"

    count_row = ebull_test_conn.execute("SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = 1").fetchone()
    assert count_row is not None and count_row[0] >= 1

    status_row = ebull_test_conn.execute(
        "SELECT status FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1"
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "success"


def test_refresh_advances_both_watermarks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
        set_watermark(
            ebull_test_conn,
            source="sec.companyfacts",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    submissions_payload = _submissions_with_top("0000320193-26-000042")
    plan = RefreshPlan(
        refreshes=[("0000320193", "0000320193-26-000042")],
        submissions_by_cik={"0000320193": submissions_payload},
    )
    filings = StubFilingsProvider()  # body comes from the plan
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.refreshed == 1
    assert outcome.failed == []

    # Refresh path must NOT call fetch_submissions — the planner already
    # supplied both ``known_top_accession`` and ``known_submissions``
    # (#675 contract: planner-fed submissions are how the executor
    # avoids a second fetch AND still runs the items/entity-profile
    # extractions).
    assert filings.fetch_calls == 0

    submissions_wm = get_watermark(ebull_test_conn, "sec.submissions", "0000320193")
    assert submissions_wm is not None
    assert submissions_wm.watermark == "0000320193-26-000042"


def test_failure_does_not_advance_watermarks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    plan = RefreshPlan(refreshes=[("0000320193", "0000320193-26-000042")])
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(fail_on={"0000320193"})

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.failed == [("0000320193", "RuntimeError")]

    # Rollback of the failed CIK's transaction MUST leave the
    # watermark at its pre-refresh value.
    wm = get_watermark(ebull_test_conn, "sec.submissions", "0000320193")
    assert wm is not None
    assert wm.watermark == "0000320193-25-000108"

    status_row = ebull_test_conn.execute(
        "SELECT status FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1"
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "failed"


def test_one_failure_does_not_abort_siblings(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="A", cik="0000000001")
    _seed_instrument(ebull_test_conn, instrument_id=2, symbol="B", cik="0000000002")

    plan = RefreshPlan(seeds=["0000000001", "0000000002"])
    filings = StubFilingsProvider(
        submissions_by_cik={
            "0000000001": _submissions_with_top("0000000001-26-000001"),
            "0000000002": _submissions_with_top("0000000002-26-000001"),
        }
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000000002": [_sample_fact("0000000002-26-000001")]},
        fail_on={"0000000001"},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.seeded == 1
    assert outcome.failed == [("0000000001", "RuntimeError")]

    wm_failed = get_watermark(ebull_test_conn, "sec.submissions", "0000000001")
    assert wm_failed is None

    wm_success = get_watermark(ebull_test_conn, "sec.submissions", "0000000002")
    assert wm_success is not None
    assert wm_success.watermark == "0000000002-26-000001"

    status_row = ebull_test_conn.execute(
        "SELECT status FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1"
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "partial"


def test_submissions_only_advance_skips_companyfacts(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="MSFT", cik="0000789019")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000789019",
            watermark="older",
        )
    ebull_test_conn.commit()

    plan = RefreshPlan(
        submissions_only_advances=[("0000789019", "0000789019-26-000017")],
    )
    filings = StubFilingsProvider()  # fetch_submissions must NOT be called
    fundamentals = StubFundamentalsProvider()  # extract_facts must NOT be called

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.submissions_advanced == 1
    assert filings.fetch_calls == 0
    assert fundamentals.extract_calls == []

    wm = get_watermark(ebull_test_conn, "sec.submissions", "0000789019")
    assert wm is not None
    assert wm.watermark == "0000789019-26-000017"

    # No companyfacts watermark should have been written.
    facts_wm = get_watermark(ebull_test_conn, "sec.companyfacts", "0000789019")
    assert facts_wm is None


def test_all_failures_mark_run_status_failed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Audit-trail non-negotiable: start_ingestion_run is always paired
    with finish_ingestion_run. When every CIK in the plan fails, the
    run row must be marked status='failed' with an error message — not
    left in 'running'."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(seeds=["0000320193"])
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(fail_on={"0000320193"})

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.failed == [("0000320193", "RuntimeError")]

    status_row = ebull_test_conn.execute(
        "SELECT status, error FROM data_ingestion_runs ORDER BY ingestion_run_id DESC LIMIT 1"
    ).fetchone()
    assert status_row is not None
    assert status_row[0] == "failed"
    assert status_row[1] is not None and "1 CIKs failed" in status_row[1]


def test_failed_cik_withholds_master_index_watermark(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When a CIK that appeared in a day's master-index hits fails,
    that day's sec.master-index watermark must NOT advance. The next
    run must re-fetch (200 not 304), re-parse, and re-plan the failed
    CIK. Regression guard against Codex-found correctness bug: missed
    filings would be skipped forever once master-index was committed."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(
        refreshes=[("0000320193", "0000320193-26-000042")],
        pending_master_index_writes=[
            ("2026-04-15", "Wed, 15 Apr 2026 22:00:00 GMT", "abc123"),
        ],
        ciks_by_day={"2026-04-15": ["0000320193"]},
    )
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(fail_on={"0000320193"})

    execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    master_wm = get_watermark(ebull_test_conn, "sec.master-index", "2026-04-15")
    assert master_wm is None, (
        "Master-index watermark must not advance when a CIK in that day's "
        "hits failed — otherwise the next run would 304-skip and never retry"
    )


def test_transient_skip_withholds_master_index_watermark(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Regression guard: a seed-path CIK whose submissions.json is
    unavailable (transient provider glitch) must withhold the master-
    index watermark, same as an exception would. Otherwise next run
    304-skips the day and the transient failure becomes permanent.

    Seeds are the only path that still calls fetch_submissions in the
    executor; refresh path reuses the accession the planner already
    captured.
    """
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(
        seeds=["0000320193"],
        pending_master_index_writes=[
            ("2026-04-15", "Wed, 15 Apr 2026 22:00:00 GMT", "abc123"),
        ],
        ciks_by_day={"2026-04-15": ["0000320193"]},
    )
    # Provider returns None for submissions (transient skip path).
    filings = StubFilingsProvider(submissions_by_cik={})
    fundamentals = StubFundamentalsProvider()

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.seeded == 0
    assert outcome.refreshed == 0
    assert ("0000320193", "SubmissionsMissing") in outcome.failed

    master_wm = get_watermark(ebull_test_conn, "sec.master-index", "2026-04-15")
    assert master_wm is None, (
        "Transient submissions-unavailable skip must withhold the master-index watermark so the next run retries"
    )


def test_successful_ciks_commit_master_index_watermark(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Positive case: a day whose CIK hits all succeed has its
    master-index watermark committed, so the next run can 304-skip."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(
        seeds=["0000320193"],
        pending_master_index_writes=[
            ("2026-04-15", "Wed, 15 Apr 2026 22:00:00 GMT", "abc123"),
        ],
        ciks_by_day={"2026-04-15": ["0000320193"]},
    )
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.seeded == 1
    assert outcome.failed == []

    master_wm = get_watermark(ebull_test_conn, "sec.master-index", "2026-04-15")
    assert master_wm is not None
    assert master_wm.watermark == "Wed, 15 Apr 2026 22:00:00 GMT"
    assert master_wm.response_hash == "abc123"


def _mk_entry(accession: str, form: str, cik: str = "0000320193") -> MasterIndexEntry:
    return MasterIndexEntry(
        cik=cik,
        company_name="TEST CORP",
        form_type=form,
        date_filed="2026-04-15",
        accession_number=accession,
    )


def test_refresh_path_writes_filing_events(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """On the refresh path, each master-index entry for the CIK must
    be upserted into filing_events so downstream event-driven triggers
    see the new filing. #291 regression guard."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
        set_watermark(
            ebull_test_conn,
            source="sec.companyfacts",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    plan = RefreshPlan(
        refreshes=[("0000320193", "0000320193-26-000042")],
        new_filings_by_cik={
            "0000320193": [
                _mk_entry("0000320193-26-000042", "10-Q"),
                _mk_entry("0000320193-26-000043", "8-K"),
            ],
        },
    )
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.refreshed == 1

    rows = ebull_test_conn.execute(
        "SELECT filing_type, provider, provider_filing_id, primary_document_url "
        "FROM filing_events WHERE instrument_id = 1 ORDER BY provider_filing_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "10-Q"
    assert rows[0][1] == "sec"
    assert rows[0][2] == "0000320193-26-000042"
    assert rows[0][3] is not None and "edgar/data/320193" in rows[0][3]
    assert rows[1][0] == "8-K"
    assert rows[1][2] == "0000320193-26-000043"


def test_submissions_only_path_writes_filing_events(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """submissions_only_advances path (8-K only, no companyfacts)
    must still upsert the 8-K into filing_events — otherwise the
    cascade's event predicate in #273 never sees it."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000789019")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000789019",
            watermark="older",
        )
    ebull_test_conn.commit()

    plan = RefreshPlan(
        submissions_only_advances=[("0000789019", "0000789019-26-000017")],
        new_filings_by_cik={
            "0000789019": [_mk_entry("0000789019-26-000017", "8-K", cik="0000789019")],
        },
    )
    filings = StubFilingsProvider()
    fundamentals = StubFundamentalsProvider()

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.submissions_advanced == 1

    row = ebull_test_conn.execute(
        "SELECT filing_type, provider, provider_filing_id FROM filing_events WHERE instrument_id = 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "8-K"
    assert row[1] == "sec"
    assert row[2] == "0000789019-26-000017"


def test_refresh_path_preserves_existing_primary_document_url(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """When filing_events already has a row with a richer primary_document_url
    (e.g. from the submissions-based ingest path), the master-index upsert
    MUST NOT downgrade it to the generic index URL. #291 Codex P2."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    # Pre-existing filing_events row with specific primary doc URL
    # (simulating daily_research_refresh path).
    ebull_test_conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, source_url, primary_document_url
        ) VALUES (
            1, DATE '2026-04-15', '10-Q',
            'sec', '0000320193-26-000042',
            'https://www.sec.gov/Archives/edgar/data/320193/000032019326000042/aapl-20260330.htm',
            'https://www.sec.gov/Archives/edgar/data/320193/000032019326000042/aapl-20260330.htm'
        )
        """
    )
    ebull_test_conn.commit()
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
        set_watermark(
            ebull_test_conn,
            source="sec.companyfacts",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    plan = RefreshPlan(
        refreshes=[("0000320193", "0000320193-26-000042")],
        new_filings_by_cik={
            "0000320193": [_mk_entry("0000320193-26-000042", "10-Q")],
        },
    )
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    row = ebull_test_conn.execute(
        "SELECT primary_document_url FROM filing_events WHERE provider_filing_id = '0000320193-26-000042'"
    ).fetchone()
    assert row is not None
    # Original URL preserved — master-index upsert did NOT overwrite
    # with the generic ...-index.htm URL.
    assert row[0].endswith("aapl-20260330.htm"), f"master-index upsert downgraded primary_document_url to: {row[0]}"


def test_seed_path_does_not_write_filing_events(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Seeds have no master-index hit metadata (first sight) so they
    MUST NOT write filing_events. Historical backfill is the job of
    #268 Chunk E, not this executor."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(seeds=["0000320193"])
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    count = ebull_test_conn.execute("SELECT COUNT(*) FROM filing_events WHERE instrument_id = 1").fetchone()
    assert count is not None and count[0] == 0


# ---------------------------------------------------------------------------
# Per-CIK timing log (issue #418 observability signal)
# ---------------------------------------------------------------------------
#
# The per-CIK ``fundamentals.cik_timing`` log line is the signal #418
# uses to validate the ADR 0004 Shape B fix landed in production. The
# tests below pin its invariants:
#
# - Emits exactly once per CIK on the success path, carrying
#   ``outcome=success`` and the committed facts_upserted count.
# - Emits exactly once on the exception path, carrying
#   ``outcome=error_<ExceptionName>`` and ``facts_upserted=0`` — the
#   transaction rolled back, so no facts were actually committed
#   even if the upsert call completed before the later step raised.
# - Emits exactly once on early skip paths, carrying an explicit
#   ``outcome=skip_*`` tag.


def _timing_lines(caplog_records: Sequence[LogRecord]) -> list[str]:
    return [r.getMessage() for r in caplog_records if r.getMessage().startswith("fundamentals.cik_timing ")]


def test_timing_log_emitted_on_success(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(seeds=["0000320193"])
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    with caplog.at_level("INFO", logger="app.services.fundamentals"):
        execute_refresh(
            ebull_test_conn,
            filings_provider=cast(SecFilingsProvider, filings),
            fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
            plan=plan,
        )

    lines = _timing_lines(caplog.records)
    assert len(lines) == 1
    line = lines[0]
    assert "cik=0000320193" in line
    assert "mode=seed" in line
    assert "outcome=success" in line
    assert "facts_upserted=1" in line


def test_timing_log_emitted_on_exception_reports_zero_facts(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # ``fail_on`` causes the fundamentals provider to raise during
    # extract_facts, which is before the upsert runs — but the timing
    # log must still emit exactly once, with outcome=error_RuntimeError
    # and facts_upserted=0 (nothing was committed).
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    plan = RefreshPlan(refreshes=[("0000320193", "0000320193-26-000042")])
    filings = StubFilingsProvider(
        submissions_by_cik={"0000320193": _submissions_with_top("0000320193-26-000042")},
    )
    fundamentals = StubFundamentalsProvider(fail_on={"0000320193"})

    with caplog.at_level("INFO", logger="app.services.fundamentals"):
        execute_refresh(
            ebull_test_conn,
            filings_provider=cast(SecFilingsProvider, filings),
            fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
            plan=plan,
        )

    lines = _timing_lines(caplog.records)
    assert len(lines) == 1
    line = lines[0]
    assert "cik=0000320193" in line
    assert "mode=refresh" in line
    assert "outcome=error_RuntimeError" in line
    assert "facts_upserted=0" in line


def test_timing_log_emitted_on_instrument_missing_skip(
    ebull_test_conn: psycopg.Connection[tuple],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Plan-time drift: the CIK was in the plan but no instrument
    # resolves for it. _run_cik_upsert hits the InstrumentMissing
    # early return; the timing log must still emit once.
    plan = RefreshPlan(seeds=["0000999999"])
    filings = StubFilingsProvider()
    fundamentals = StubFundamentalsProvider()

    with caplog.at_level("INFO", logger="app.services.fundamentals"):
        execute_refresh(
            ebull_test_conn,
            filings_provider=cast(SecFilingsProvider, filings),
            fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
            plan=plan,
        )

    lines = _timing_lines(caplog.records)
    assert len(lines) == 1
    line = lines[0]
    assert "cik=0000999999" in line
    assert "outcome=skip_instrument_missing" in line
    assert "facts_upserted=0" in line


def test_cik_upsert_timing_row_persisted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Persist-path companion to the log-line tests above (#418): each
    # _run_cik_upsert exit must write a cik_upsert_timing row so the
    # admin UI can surface p50/p95 per-CIK without tailing logs.
    ebull_test_conn.execute("TRUNCATE cik_upsert_timing RESTART IDENTITY CASCADE")
    ebull_test_conn.commit()

    plan = RefreshPlan(seeds=["0000999998"])
    filings = StubFilingsProvider()
    fundamentals = StubFundamentalsProvider()

    execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    rows = ebull_test_conn.execute(
        "SELECT cik, mode, outcome, facts_upserted, seconds FROM cik_upsert_timing ORDER BY timing_id"
    ).fetchall()
    assert len(rows) == 1
    cik, mode, outcome, facts, seconds = rows[0]
    assert cik == "0000999998"
    assert mode == "seed"
    assert outcome == "skip_instrument_missing"
    assert facts == 0
    assert float(seconds) >= 0


# ---------------------------------------------------------------------------
# #675: items[] + entity-profile must apply on the planner-driven refresh
# path. Pre-fix the planner discarded the submissions body and the executor's
# extractions sat inside a seed-only ``else`` branch — so 480k+ existing 8-K
# rows had ``items=NULL`` and entity-profile rows went stale silently.
# ---------------------------------------------------------------------------


def _submissions_with_items(
    accession: str,
    form: str,
    items_csv: str,
    primary_doc: str = "doc.htm",
) -> dict[str, object]:
    """submissions.json shape carrying enough fields for both
    ``parse_8k_items_by_accession`` and ``parse_entity_profile`` to
    return a non-empty result."""
    return {
        "name": "TEST CORP",
        "sic": "1234",
        "sicDescription": "Test Industry",
        "exchanges": ["NASDAQ"],
        "tickers": ["TEST"],
        "addresses": {"business": {}, "mailing": {}},
        "formerNames": [],
        "filings": {
            "recent": {
                "accessionNumber": [accession],
                "form": [form],
                "items": [items_csv],
                "filingDate": ["2026-04-15"],
                "primaryDocument": [primary_doc],
                "reportDate": [""],
                "acceptedDate": ["2026-04-15T09:00:00.000Z"],
            }
        },
    }


def test_refresh_path_applies_8k_items_from_planner_submissions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Regression guard for #675: when the planner passes through the
    submissions body via ``RefreshPlan.submissions_by_cik``, the
    executor's refresh path MUST apply 8-K ``items[]`` to the
    matching ``filing_events`` row. Pre-fix this UPDATE never ran on
    the refresh path because the extraction lived inside a seed-only
    ``else`` branch."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
        set_watermark(
            ebull_test_conn,
            source="sec.companyfacts",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    accession_8k = "0000320193-26-000099"
    submissions = _submissions_with_items(accession_8k, "8-K", "1.01,8.01")

    plan = RefreshPlan(
        refreshes=[("0000320193", "0000320193-26-000042")],
        submissions_by_cik={"0000320193": submissions},
        new_filings_by_cik={
            "0000320193": [
                _mk_entry("0000320193-26-000042", "10-Q"),
                _mk_entry(accession_8k, "8-K"),
            ],
        },
    )
    filings = StubFilingsProvider()  # no fetch — known_submissions provided
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.refreshed == 1
    assert outcome.failed == []
    # Planner-fed submissions → no provider fetch expected.
    assert filings.fetch_calls == 0

    items_row = ebull_test_conn.execute(
        "SELECT items FROM filing_events WHERE provider_filing_id = %s",
        (accession_8k,),
    ).fetchone()
    assert items_row is not None
    assert items_row[0] == ["1.01", "8.01"]


def test_refresh_path_applies_entity_profile_from_planner_submissions(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Companion to the items test: the entity-profile upsert (#427)
    sat in the same seed-only else branch and was equally affected
    by #675. Refresh path with planner-fed submissions must upsert
    the profile too."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
        set_watermark(
            ebull_test_conn,
            source="sec.companyfacts",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    ebull_test_conn.commit()

    submissions = _submissions_with_items("0000320193-26-000042", "10-Q", "")

    plan = RefreshPlan(
        refreshes=[("0000320193", "0000320193-26-000042")],
        submissions_by_cik={"0000320193": submissions},
    )
    filings = StubFilingsProvider()
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
    )

    execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    profile = ebull_test_conn.execute(
        "SELECT sic, sic_description FROM instrument_sec_profile WHERE instrument_id = 1"
    ).fetchone()
    assert profile is not None
    assert profile[0] == "1234"
    assert profile[1] == "Test Industry"


def test_submissions_only_path_applies_8k_items(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """submissions_only_advances path (8-K only, no companyfacts)
    must also apply items[] using the planner-fed submissions body
    so ``dividend_calendar`` selectors that filter on
    ``'8.01' = ANY(items)`` see today's filing."""
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000789019")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000789019",
            watermark="older",
        )
    ebull_test_conn.commit()

    accession = "0000789019-26-000017"
    submissions = _submissions_with_items(accession, "8-K", "8.01")

    plan = RefreshPlan(
        submissions_only_advances=[("0000789019", accession)],
        submissions_by_cik={"0000789019": submissions},
        new_filings_by_cik={
            "0000789019": [_mk_entry(accession, "8-K", cik="0000789019")],
        },
    )
    filings = StubFilingsProvider()
    fundamentals = StubFundamentalsProvider()

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.submissions_advanced == 1
    assert filings.fetch_calls == 0

    items_row = ebull_test_conn.execute(
        "SELECT items FROM filing_events WHERE provider_filing_id = %s",
        (accession,),
    ).fetchone()
    assert items_row is not None
    assert items_row[0] == ["8.01"]
