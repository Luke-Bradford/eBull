"""#1347 — S17 + S18 bootstrap recency-bound cohort.

Pins:
1. ``bootstrap_filings_recency_floor`` — UTC-normalised, naive rejected,
   13-month (396d) offset. Pure unit (no DB).
2. ``discover_pending_def14a(min_filing_date=...)`` — excludes pre-floor
   accessions, includes the boundary (``>=``), ``None`` is unbounded.
3. ``ingest_business_summaries(min_filing_date=...)`` — excludes an
   instrument whose latest 10-K is staler than the floor; includes
   within-floor; ``None`` is unbounded.

The chunker-level gate (``progress_ctx``-derived floor) is exercised by the
existing bootstrap tests; here we pin the value-agnostic selectors + the pure
floor helper, which is where the correctness lives.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import psycopg
import pytest

from app.services.business_summary import ingest_business_summaries
from app.services.def14a_ingest import discover_pending_def14a
from app.services.filings import (
    BOOTSTRAP_FILINGS_RECENCY_DAYS,
    bootstrap_filings_recency_floor,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# ---------------------------------------------------------------------------
# Pure helper — no DB
# ---------------------------------------------------------------------------


class TestBootstrapFilingsRecencyFloor:
    def test_offset_is_396_days(self) -> None:
        now = datetime(2026, 5, 29, 12, 0, tzinfo=UTC)
        floor = bootstrap_filings_recency_floor(now)
        assert (now.date() - floor).days == BOOTSTRAP_FILINGS_RECENCY_DAYS == 396

    def test_naive_now_rejected(self) -> None:
        with pytest.raises(ValueError, match="tz-aware"):
            bootstrap_filings_recency_floor(datetime(2026, 5, 29, 12, 0))  # noqa: DTZ001 — intentional naive

    def test_non_utc_now_normalised_to_utc_calendar_day(self) -> None:
        # 23:00 at UTC-5 is 04:00 the NEXT calendar day in UTC. The floor
        # must anchor to the UTC day (2026-05-30), not the local day.
        est = timezone(timedelta(hours=-5))
        now = datetime(2026, 5, 29, 23, 0, tzinfo=est)
        floor = bootstrap_filings_recency_floor(now)
        assert floor == date(2026, 5, 30) - timedelta(days=BOOTSTRAP_FILINGS_RECENCY_DAYS)


# ---------------------------------------------------------------------------
# Integration — selectors against the dev DB
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
        VALUES (%s, %s, %s, TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_profile(conn: psycopg.Connection[tuple], *, instrument_id: int, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik)
        VALUES (%s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
        """,
        (instrument_id, cik),
    )


def _seed_filing_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    filing_date: date,
    filing_type: str = "DEF 14A",
) -> None:
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, %s, %s, 'sec', %s, 'https://example.test/doc.htm')
        ON CONFLICT (provider, provider_filing_id, instrument_id) DO NOTHING
        """,
        (instrument_id, filing_date, filing_type, accession),
    )


_FLOOR = date(2025, 1, 1)


class TestDef14aDiscoverRecencyBound:
    def test_floor_excludes_pre_within_cap(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Two DEF 14A — both within the latest-2-per-filer cap, so the cap
        drops neither. The floor is the ONLY discriminator: with the floor,
        only the post-floor accession survives; with ``None``, both do."""
        conn = ebull_test_conn
        iid = 1_347_001
        _seed_instrument(conn, iid=iid, symbol="RECA")
        _seed_profile(conn, instrument_id=iid, cik="0001347001")
        _seed_filing_event(conn, instrument_id=iid, accession="REC-PRE", filing_date=date(2024, 1, 1))
        _seed_filing_event(conn, instrument_id=iid, accession="REC-POST", filing_date=date(2026, 1, 1))
        conn.commit()

        ours = {"REC-PRE", "REC-POST"}
        bounded = {
            r.accession_number
            for r in discover_pending_def14a(conn, min_filing_date=_FLOOR, limit=100)
            if r.accession_number in ours
        }
        unbounded = {
            r.accession_number
            for r in discover_pending_def14a(conn, min_filing_date=None, limit=100)
            if r.accession_number in ours
        }
        assert bounded == {"REC-POST"}  # floor excludes pre, NOT the cap
        assert unbounded == ours  # both within cap → unbounded returns both

    def test_boundary_is_inclusive(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A filing dated exactly on the floor is retained (``>=``)."""
        conn = ebull_test_conn
        iid = 1_347_002
        _seed_instrument(conn, iid=iid, symbol="RECB")
        _seed_profile(conn, instrument_id=iid, cik="0001347002")
        _seed_filing_event(conn, instrument_id=iid, accession="REC-BND", filing_date=_FLOOR)
        conn.commit()

        bounded = {
            r.accession_number
            for r in discover_pending_def14a(conn, min_filing_date=_FLOOR, limit=100)
            if r.accession_number == "REC-BND"
        }
        assert bounded == {"REC-BND"}


class TestBusinessSummaryRecencyBound:
    class _NullFetcher:
        """Returns no body — we assert on candidate selection
        (``filings_scanned``), not on parse output."""

        def fetch_document_text(self, absolute_url: str) -> str | None:
            return None

    def _seed_10k(
        self,
        conn: psycopg.Connection[tuple],
        *,
        instrument_id: int,
        accession: str,
        filing_date: date,
    ) -> None:
        conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type,
                provider, provider_filing_id, primary_document_url
            ) VALUES (%s, %s, '10-K', 'sec', %s, 'https://example.test/10k.htm')
            ON CONFLICT (provider, provider_filing_id, instrument_id) DO NOTHING
            """,
            (instrument_id, filing_date, accession),
        )

    def test_stale_latest_10k_excluded_by_floor(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 1_347_010
        _seed_instrument(conn, iid=iid, symbol="BSA")
        self._seed_10k(conn, instrument_id=iid, accession="BSA-STALE", filing_date=date(2023, 1, 1))
        conn.commit()

        result = ingest_business_summaries(
            conn,
            self._NullFetcher(),
            min_filing_date=_FLOOR,
        )
        scanned = {
            iid
            for (iid,) in conn.execute(
                "SELECT instrument_id FROM filing_events WHERE provider_filing_id = 'BSA-STALE'"
            ).fetchall()
        }
        # Stale-latest instrument is in filing_events but excluded from the
        # bounded cohort → not scanned.
        assert iid in scanned  # sanity: row exists
        assert result.filings_scanned == 0

    def test_within_floor_10k_scanned(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 1_347_011
        _seed_instrument(conn, iid=iid, symbol="BSB")
        self._seed_10k(conn, instrument_id=iid, accession="BSB-FRESH", filing_date=date(2026, 2, 1))
        conn.commit()

        result = ingest_business_summaries(
            conn,
            self._NullFetcher(),
            min_filing_date=_FLOOR,
        )
        assert result.filings_scanned == 1

    def test_none_is_unbounded(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        iid = 1_347_012
        _seed_instrument(conn, iid=iid, symbol="BSC")
        self._seed_10k(conn, instrument_id=iid, accession="BSC-OLD", filing_date=date(2019, 6, 1))
        conn.commit()

        result = ingest_business_summaries(
            conn,
            self._NullFetcher(),
            min_filing_date=None,
        )
        assert result.filings_scanned == 1
