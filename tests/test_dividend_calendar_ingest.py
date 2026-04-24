"""Integration tests for ``ingest_dividend_events`` (#434).

Runs against the real ``ebull_test`` DB via the ``ebull_test_conn``
fixture so the migration, FK cascade, UNIQUE constraint, and JOIN
logic are all exercised. The HTTP fetch is stubbed with a callable
that returns canned text per URL — the parser path is pure-python
and covered by ``test_dividend_calendar.py`` separately.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import psycopg
import pytest

from app.services.dividend_calendar import (
    DividendAnnouncement,
    IngestResult,
    ingest_dividend_events,
    upsert_dividend_event,
)

pytestmark = pytest.mark.integration


class _StubFetcher:
    """In-memory fetcher — maps URL → body (or ``None`` for 404)."""

    def __init__(self, by_url: dict[str, str | None]) -> None:
        self._by_url = by_url
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._by_url.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], symbol: str = "KO") -> int:
    # ``instrument_id`` is a caller-assigned BIGINT (broker IDs pass through),
    # so the test picks a stable synthetic value rather than relying on a
    # sequence default.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name)
            VALUES (%s, %s, %s)
            RETURNING instrument_id
            """,
            (42, symbol, "Test Co"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str,
    items: list[str] | None = None,
) -> None:
    """Insert one SEC 8-K filing row with items[] set (default
    ARRAY['8.01'] so the ingester will pick it up)."""
    items = items if items is not None else ["8.01"]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url, items)
            VALUES (%s, CURRENT_DATE, '8-K', 'sec', %s, %s, %s)
            """,
            (instrument_id, accession, url, items),
        )
    conn.commit()


def _count_events(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dividend_events")
        row = cur.fetchone()
        assert row is not None
        return int(row[0])


# Canonical announcement text — covered by parser tests, here it's
# fixture material.
_COKE_ANNOUNCEMENT = (
    "On February 15, 2024, the Board of Directors of The Coca-Cola "
    "Company declared a regular quarterly cash dividend of $0.485 "
    "per share, payable on April 1, 2024, to shareholders of record "
    "as of March 15, 2024. The ex-dividend date is March 14, 2024."
)


class TestIngestDividendEvents:
    def test_happy_path_inserts_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """One 8-K + 8.01 + valid doc → one dividend_events row."""
        inst_id = _seed_instrument(ebull_test_conn)
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession="0000021344-24-000005",
            url="https://www.sec.gov/Archives/fake-coke.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/fake-coke.htm": _COKE_ANNOUNCEMENT})

        result = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert isinstance(result, IngestResult)
        assert result.filings_scanned == 1
        assert result.rows_inserted == 1
        assert result.rows_updated == 0
        assert result.parse_misses == 0
        assert result.fetch_errors == 0
        assert _count_events(ebull_test_conn) == 1

    def test_rerun_is_idempotent(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A second pass over the same filing does NOT re-ingest —
        the JOIN-guard on dividend_events excludes already-processed
        filings. Counters show 0 scanned, 0 inserted."""
        inst_id = _seed_instrument(ebull_test_conn)
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession="0000021344-24-000005",
            url="https://www.sec.gov/Archives/fake-coke.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/fake-coke.htm": _COKE_ANNOUNCEMENT})

        ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        second = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert second.filings_scanned == 0
        assert second.rows_inserted == 0
        assert _count_events(ebull_test_conn) == 1

    def test_non_dividend_8k_is_parse_miss_with_tombstone(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A buyback / JV 8-K carrying 8.01 parses as None → parse_miss
        counter increments AND a tombstone row is written (all NULL
        dates + NULL amount). The tombstone is what bounds re-fetch
        cadence to weekly via the partial-row TTL — without it, the
        LEFT JOIN would re-fetch the same miss on every daily run.
        Codex PR #446 review WARNING."""
        inst_id = _seed_instrument(ebull_test_conn)
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession="0000021344-24-000006",
            url="https://www.sec.gov/Archives/fake-jv.htm",
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/fake-jv.htm": (
                    "On March 1, 2024, the Company entered into a joint "
                    "venture agreement with XYZ Corp regarding the "
                    "European market."
                )
            }
        )

        result = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_scanned == 1
        assert result.parse_misses == 1
        assert _count_events(ebull_test_conn) == 1  # tombstone row

        # Second immediate pass: TTL is fresh, so the filing is NOT
        # re-scanned. Counters reflect zero work.
        second_fetcher = _StubFetcher(
            {
                "https://www.sec.gov/Archives/fake-jv.htm": (
                    "On March 1, 2024, the Company entered into a joint venture..."
                )
            }
        )
        second = ingest_dividend_events(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_fetch_404_writes_tombstone(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Withdrawn filing (provider returns None) → fetch_errors
        increments AND tombstone row written. Caps retry cadence at
        weekly via the TTL, not daily. Codex PR #446 WARNING."""
        inst_id = _seed_instrument(ebull_test_conn)
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession="0000021344-24-000007",
            url="https://www.sec.gov/Archives/gone.htm",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/gone.htm": None})

        result = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_scanned == 1
        assert result.fetch_errors == 1
        assert _count_events(ebull_test_conn) == 1  # tombstone row

        # Second immediate pass must NOT re-fetch.
        second_fetcher = _StubFetcher({"https://www.sec.gov/Archives/gone.htm": None})
        second = ingest_dividend_events(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_filing_without_items_is_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Pre-#431 filings have items IS NULL and must be ignored —
        the items-based gate is the only idempotency key we have for
        non-8.01 noise."""
        inst_id = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO filing_events
                    (instrument_id, filing_date, filing_type, provider,
                     provider_filing_id, primary_document_url, items)
                VALUES (%s, CURRENT_DATE, '8-K', 'sec',
                        '0000021344-24-000099',
                        'https://www.sec.gov/Archives/unknown-items.htm',
                        NULL)
                """,
                (inst_id,),
            )
        ebull_test_conn.commit()
        fetcher = _StubFetcher({})

        result = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_scanned == 0
        assert fetcher.calls == []

    def test_partial_row_is_reparsed_after_ttl(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex #434 H1 regression — a row parsed as partial (e.g. amount
        only, no calendar dates) must be revisited on a later run so a
        regex improvement can backfill the dates. Gated by a 7-day TTL
        on ``last_parsed_at`` so stable partials don't hammer SEC daily."""
        inst_id = _seed_instrument(ebull_test_conn)
        accession = "0000021344-24-000020"
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession=accession,
            url="https://www.sec.gov/Archives/partial.htm",
        )
        # First pass: partial parse (amount only, no calendar dates).
        partial = (
            "The Board declared a quarterly cash dividend of $0.25 "
            "per share. Details regarding the record date and payment "
            "date will be disclosed in a subsequent filing."
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/partial.htm": partial})
        first = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert first.rows_inserted == 1

        # Simulate ≥ 7 days passing — move last_parsed_at back in time.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE dividend_events SET last_parsed_at = NOW() - INTERVAL '8 days' "
                "WHERE instrument_id=%s AND source_accession=%s",
                (inst_id, accession),
            )
        ebull_test_conn.commit()

        # Second pass: the parser has "improved" — now returns full
        # calendar. Re-parse picks up the same accession and UPDATEs.
        full = (
            "On February 15, 2024, the Board declared a quarterly cash "
            "dividend of $0.25 per share, payable on April 1, 2024, to "
            "shareholders of record as of March 15, 2024."
        )
        better_fetcher = _StubFetcher({"https://www.sec.gov/Archives/partial.htm": full})
        second = ingest_dividend_events(ebull_test_conn, cast("object", better_fetcher))  # type: ignore[arg-type]

        assert second.filings_scanned == 1
        assert second.rows_inserted == 0
        assert second.rows_updated == 1
        # Dates now populated.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT pay_date, record_date FROM dividend_events WHERE instrument_id=%s AND source_accession=%s",
                (inst_id, accession),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None
            assert row[1] is not None

    def test_fresh_partial_within_ttl_not_reparsed(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A partial row parsed < 7 days ago must NOT be re-fetched —
        otherwise stable partials hammer SEC every daily run."""
        inst_id = _seed_instrument(ebull_test_conn)
        accession = "0000021344-24-000021"
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession=accession,
            url="https://www.sec.gov/Archives/fresh-partial.htm",
        )
        partial = (
            "The Board declared a quarterly cash dividend of $0.25 "
            "per share. Details regarding the record date will follow."
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/fresh-partial.htm": partial})
        ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        # Second pass immediately — last_parsed_at < 7 days old.
        second_fetcher = _StubFetcher({"https://www.sec.gov/Archives/fresh-partial.htm": partial})
        second = ingest_dividend_events(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]

        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_filing_with_different_items_is_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """8-K with items=['1.01','9.01'] (no 8.01) must not even be
        fetched."""
        inst_id = _seed_instrument(ebull_test_conn)
        _seed_filing(
            ebull_test_conn,
            instrument_id=inst_id,
            accession="0000021344-24-000008",
            url="https://www.sec.gov/Archives/other.htm",
            items=["1.01", "9.01"],
        )
        fetcher = _StubFetcher({})

        result = ingest_dividend_events(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_scanned == 0
        assert fetcher.calls == []


class TestUpsertDividendEvent:
    def test_update_path_flips_insert_to_false(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Re-calling upsert for the same (instrument_id, accession)
        returns False (UPDATE, not INSERT)."""
        inst_id = _seed_instrument(ebull_test_conn)
        a1 = DividendAnnouncement(dps_declared="0.25")
        a2 = DividendAnnouncement(dps_declared="0.30")

        inserted = upsert_dividend_event(
            ebull_test_conn,
            instrument_id=inst_id,
            source_accession="A1",
            announcement=a1,
        )
        ebull_test_conn.commit()
        updated = upsert_dividend_event(
            ebull_test_conn,
            instrument_id=inst_id,
            source_accession="A1",
            announcement=a2,
        )
        ebull_test_conn.commit()

        assert inserted is True
        assert updated is False
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT dps_declared FROM dividend_events WHERE instrument_id=%s AND source_accession=%s",
                (inst_id, "A1"),
            )
            row = cur.fetchone()
            assert row is not None
            assert str(row[0]) == "0.300000"


# Keep the type-checker happy re the ``cast`` above — the actual
# fixture is shaped correctly but pyright doesn't infer Protocol
# conformance for tuple-row cursors cleanly.
_ = Callable  # noqa: F401 — silence "unused import" under -Wunused.
