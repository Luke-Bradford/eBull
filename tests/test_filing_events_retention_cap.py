"""filing_events 10y retention cap — #1233 §4.2 PR3.

Pins the three contracts:

1. ``filing_events_retention_cutoff`` is ``now - 10y`` (rolling).
2. ``filing_within_retention`` is inclusive at the boundary.
3. Each of the three writer chokepoints — ``_upsert_filing``,
   ``_upsert_filing_event``, and
   ``fundamentals._upsert_filing_from_master_index`` — rejects rows
   whose ``filing_date`` falls before the cutoff and accepts rows on
   or after the boundary.

Boundary contract matters because the rolling window means a filing
"on the boundary" exists for exactly one day per year before falling
off. An exclusive boundary would create a 1-day blip where the
filing visibly drops + re-appears.

Existing rows are not deleted by the cap (#1233 §6.3 is the only
purge event). These tests therefore assert *insert* behaviour only;
a pre-cap row already in the table stays put.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from app.providers.filings import FilingEvent, FilingSearchResult
from app.services.filings import (
    FILING_EVENTS_RETENTION_YEARS,
    _upsert_filing,
    _upsert_filing_event,
    filing_events_retention_cutoff,
    filing_within_retention,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

# ---------------------------------------------------------------------------
# Pure helper contracts
# ---------------------------------------------------------------------------


class TestRetentionCutoff:
    def test_constant_is_10_years(self) -> None:
        assert FILING_EVENTS_RETENTION_YEARS == 10

    def test_cutoff_is_now_minus_10_calendar_years(self) -> None:
        """Calendar-year subtraction, NOT 365*10 days — leap days
        would otherwise drift the boundary by ~2-3 days per decade
        (Codex 1a #1)."""
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = filing_events_retention_cutoff(now=ref)
        # 10 calendar years before 2026-05-20 is exactly 2016-05-20.
        assert cutoff == date(2016, 5, 20)

    def test_cutoff_feb_29_anchors_to_feb_28_in_non_leap_target(self) -> None:
        """today=Feb 29, target year non-leap → cutoff anchors to
        Feb 28. Conservative: one day earlier, not later, so a
        filing at Feb 28 in the target year stays inside the window."""
        ref = datetime(2024, 2, 29, 12, 0, 0, tzinfo=UTC)  # 2024 is leap
        cutoff = filing_events_retention_cutoff(now=ref)
        # 2024 - 10 = 2014 (not a leap year) → Feb 28.
        assert cutoff == date(2014, 2, 28)

    def test_cutoff_returns_date_not_datetime(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        result = filing_events_retention_cutoff(now=ref)
        # ``filing_events.filing_date`` is a DATE column; the cutoff
        # must be ``date`` so equality / comparison stays type-pure.
        assert isinstance(result, date)
        assert not isinstance(result, datetime)


class TestFilingWithinRetention:
    def test_at_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = filing_events_retention_cutoff(now=ref)
        # The boundary date itself is INSIDE the window.
        assert filing_within_retention(cutoff, now=ref) is True

    def test_one_day_before_boundary_rejected(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = filing_events_retention_cutoff(now=ref)
        assert filing_within_retention(cutoff - timedelta(days=1), now=ref) is False

    def test_one_day_after_boundary_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = filing_events_retention_cutoff(now=ref)
        assert filing_within_retention(cutoff + timedelta(days=1), now=ref) is True

    def test_today_accepted(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        assert filing_within_retention(ref.date(), now=ref) is True

    def test_future_filing_accepted(self) -> None:
        """A future-dated filing (operator clock skew, weird provider)
        is still inside the window — the cap is a lower bound, not a
        range. Don't drop on the upper side."""
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        assert filing_within_retention(ref.date() + timedelta(days=1), now=ref) is True

    def test_ancient_filing_rejected(self) -> None:
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        ancient = date(1993, 1, 1)
        assert filing_within_retention(ancient, now=ref) is False


# ---------------------------------------------------------------------------
# Writer-level integration
# ---------------------------------------------------------------------------


pytestmark_integration = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], instrument_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            (instrument_id, f"FE{instrument_id}", f"Test FE{instrument_id}"),
        )


def _make_search_result(
    *,
    accession: str,
    filed_at: datetime,
    filing_type: str = "10-K",
) -> FilingSearchResult:
    return FilingSearchResult(
        provider_filing_id=accession,
        symbol="FE-TEST",
        filed_at=filed_at,
        filing_type=filing_type,
        period_of_report=None,
        primary_document_url=f"https://www.sec.gov/Archives/{accession}.htm",
    )


def _read_filing(conn: psycopg.Connection[tuple], *, instrument_id: int, accession: str) -> tuple | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT filing_date, filing_type FROM filing_events
            WHERE provider = 'sec' AND provider_filing_id = %s AND instrument_id = %s
            """,
            (accession, instrument_id),
        )
        return cur.fetchone()


@pytest.mark.integration
class TestUpsertFilingRespectsCutoff:
    def test_recent_filing_inserted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, 540001)
        ref = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        result = _make_search_result(accession="0000000001-FE-A", filed_at=ref)
        _upsert_filing(ebull_test_conn, "540001", "sec", result)
        ebull_test_conn.commit()
        row = _read_filing(ebull_test_conn, instrument_id=540001, accession="0000000001-FE-A")
        assert row is not None
        assert row[1] == "10-K"

    def test_pre_cutoff_filing_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A filing dated 15 years ago must be silently dropped — no
        row in filing_events, no exception."""
        _seed_instrument(ebull_test_conn, 540002)
        # 15 years pre-now → outside the 10y window.
        ancient = datetime.now(tz=UTC) - timedelta(days=365 * 15)
        result = _make_search_result(accession="0000000002-FE-B", filed_at=ancient)
        _upsert_filing(ebull_test_conn, "540002", "sec", result)
        ebull_test_conn.commit()
        assert _read_filing(ebull_test_conn, instrument_id=540002, accession="0000000002-FE-B") is None

    def test_boundary_filing_inserted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A filing exactly at the boundary (10y - 0 days) is INSIDE
        the window (inclusive boundary, pinned by
        ``test_at_boundary_accepted`` above and reasserted at the
        writer-level)."""
        _seed_instrument(ebull_test_conn, 540003)
        boundary_date = filing_events_retention_cutoff()
        # Convert back to datetime for the writer signature.
        boundary_dt = datetime.combine(boundary_date, datetime.min.time(), tzinfo=UTC)
        result = _make_search_result(accession="0000000003-FE-C", filed_at=boundary_dt)
        _upsert_filing(ebull_test_conn, "540003", "sec", result)
        ebull_test_conn.commit()
        row = _read_filing(ebull_test_conn, instrument_id=540003, accession="0000000003-FE-C")
        assert row is not None


@pytest.mark.integration
class TestUpsertFilingEventRespectsCutoff:
    """The richer ``_upsert_filing_event`` mirror of the same gate."""

    def test_pre_cutoff_event_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, 540004)
        ancient = datetime.now(tz=UTC) - timedelta(days=365 * 12)
        event = FilingEvent(
            provider_filing_id="0000000004-FE-D",
            symbol="FE-TEST",
            filed_at=ancient,
            filing_type="8-K",
            period_of_report=None,
            primary_document_url="https://www.sec.gov/Archives/x.htm",
            extracted_summary=None,
            red_flag_score=None,
            raw_payload={},
        )
        _upsert_filing_event(ebull_test_conn, "540004", "sec", event)
        ebull_test_conn.commit()
        assert _read_filing(ebull_test_conn, instrument_id=540004, accession="0000000004-FE-D") is None

    def test_recent_event_inserted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, 540005)
        event = FilingEvent(
            provider_filing_id="0000000005-FE-E",
            symbol="FE-TEST",
            filed_at=datetime.now(tz=UTC),
            filing_type="8-K",
            period_of_report=None,
            primary_document_url="https://www.sec.gov/Archives/y.htm",
            extracted_summary=None,
            red_flag_score=None,
            raw_payload={},
        )
        _upsert_filing_event(ebull_test_conn, "540005", "sec", event)
        ebull_test_conn.commit()
        row = _read_filing(ebull_test_conn, instrument_id=540005, accession="0000000005-FE-E")
        assert row is not None
        assert row[1] == "8-K"


@pytest.mark.integration
class TestMasterIndexUpsertRespectsCutoff:
    """Third chokepoint —
    ``app/services/fundamentals.py::_upsert_filing_from_master_index``.

    Imports lazily because the function is private + the module is
    heavy. The test pins that the gate fires before the INSERT
    executes."""

    def test_pre_cutoff_master_index_entry_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.providers.implementations.sec_edgar import MasterIndexEntry
        from app.services.fundamentals import _upsert_filing_from_master_index

        _seed_instrument(ebull_test_conn, 540006)
        # 11 years pre-now → outside the 10y window.
        ancient_date = (datetime.now(tz=UTC) - timedelta(days=365 * 11)).date().isoformat()
        entry = MasterIndexEntry(
            cik="0000000540",
            company_name="Test FE540006",
            form_type="10-K",
            date_filed=ancient_date,
            accession_number="0000540006-99-000001",
        )
        _upsert_filing_from_master_index(
            ebull_test_conn,
            instrument_id=540006,
            entry=entry,
            symbol="FE540006",
        )
        ebull_test_conn.commit()
        assert (
            _read_filing(
                ebull_test_conn,
                instrument_id=540006,
                accession="0000540006-99-000001",
            )
            is None
        )

    def test_recent_master_index_entry_inserted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.providers.implementations.sec_edgar import MasterIndexEntry
        from app.services.fundamentals import _upsert_filing_from_master_index

        _seed_instrument(ebull_test_conn, 540007)
        recent_date = (datetime.now(tz=UTC) - timedelta(days=30)).date().isoformat()
        entry = MasterIndexEntry(
            cik="0000000540",
            company_name="Test FE540007",
            form_type="10-Q",
            date_filed=recent_date,
            accession_number="0000540007-26-000001",
        )
        _upsert_filing_from_master_index(
            ebull_test_conn,
            instrument_id=540007,
            entry=entry,
            symbol="FE540007",
        )
        ebull_test_conn.commit()
        row = _read_filing(
            ebull_test_conn,
            instrument_id=540007,
            accession="0000540007-26-000001",
        )
        assert row is not None
        assert row[1] == "10-Q"


def test_no_writer_bypasses_the_gate() -> None:
    """Lint-style check: enumerate every ``INSERT INTO filing_events``
    site in app/ and confirm each is preceded by a
    ``filing_within_retention`` guard (or imports the helper and
    invokes it). #1233 §4.2 — caps must apply to every writer.

    This is a defensive guard: a future PR that adds a fourth writer
    without the gate will fail this test, prompting the developer to
    either route through the existing chokepoints or add the gate
    explicitly."""
    import subprocess

    proc = subprocess.run(
        ["grep", "-rln", "INSERT INTO filing_events", "app/"],
        capture_output=True,
        text=True,
        check=False,
    )
    writer_files = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    # Allow a deny-list of files known not to need the gate (e.g.
    # SQL migrations, documentation generators). Currently empty —
    # every writer must invoke ``filing_within_retention``.
    deny_list: set[str] = set()
    bypassed: list[str] = []
    for f in writer_files:
        if f in deny_list:
            continue
        # ``errors="replace"`` so a binary blob (e.g. a stray
        # non-UTF8 fixture surfaced via grep) doesn't crash the lint
        # — we only care about the ASCII helper name.
        text = open(f, encoding="utf-8", errors="replace").read()
        if "filing_within_retention" not in text:
            bypassed.append(f)
    assert not bypassed, (
        f"INSERT INTO filing_events sites without filing_within_retention gate: {bypassed}. "
        "Add the gate or route through app/services/filings.py chokepoints (#1233 §4.2)."
    )
