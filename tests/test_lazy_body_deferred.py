"""Tests for #1343 lazy-on-click body deferral.

Covers the defer/seed/fill path against ``ebull_test``:

- metadata-only seed writes ``body_deferred=TRUE`` + body='' + ZERO
  sections / empty item bodies (no fetch),
- ``get_parse_status`` surfaces ``'deferred'`` (NOT ``parse_failed``),
- the bootstrap gate seeds WITHOUT fetching — the fetch stub RAISES, so a
  fetch-then-defer regression trips (committee IMPORTANT-1),
- a recorded parse attempt clears ``body_deferred`` so the panel stops
  re-triggering the lazy fetch (prevention-log §1265),
- ``fetch_*_body_now`` early-returns ``not_deferred`` without fetching.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test Postgres not reachable",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    c: psycopg.Connection[Any] = psycopg.connect(_test_database_url(), autocommit=True)
    try:
        yield c
    finally:
        c.close()


class _RaisingFetcher:
    """``fetch_document_text`` that fails the test if called.

    Proves the bootstrap metadata-only path issues ZERO HTTP — a stub
    that merely *returned* a body would still pass even on a
    fetch-then-defer regression (committee IMPORTANT-1)."""

    def fetch_document_text(self, absolute_url: str) -> str | None:
        raise AssertionError(f"fetch_document_text must NOT be called for a deferred/metadata path: {absolute_url}")


def _seed_instrument(conn: psycopg.Connection[Any]) -> int:
    sym = f"TEST_{uuid4().hex[:8]}"
    iid = 10**12 + int(uuid4().hex[:8], 16)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments
                (instrument_id, symbol, company_name, currency, is_tradable, is_primary_listing)
            VALUES (%s, %s, %s, 'USD', TRUE, TRUE)
            RETURNING instrument_id
            """,
            (iid, sym, f"Test Instrument {sym}"),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])  # type: ignore[index]


def _seed_filing_event(
    conn: psycopg.Connection[Any],
    iid: int,
    *,
    form: str,
    accession: str,
    filing_date: Any,
    report_date: Any | None = None,
    items: list[str] | None = None,
    url: str = "https://sec.example/doc.htm",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider, provider_filing_id,
                 primary_document_url, report_date, items)
            VALUES (%s, %s, %s, 'sec', %s, %s, %s, %s)
            """,
            (iid, filing_date, form, accession, url, report_date, items),
        )


def _cleanup(conn: psycopg.Connection[Any], iid: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM eight_k_items WHERE accession_number IN "
            "(SELECT accession_number FROM eight_k_filings WHERE instrument_id = %s)",
            (iid,),
        )
        cur.execute("DELETE FROM eight_k_filings WHERE instrument_id = %s", (iid,))
        cur.execute("DELETE FROM instrument_business_summary_sections WHERE instrument_id = %s", (iid,))
        cur.execute("DELETE FROM instrument_business_summary WHERE instrument_id = %s", (iid,))
        cur.execute("DELETE FROM filing_events WHERE instrument_id = %s", (iid,))
        cur.execute("DELETE FROM instruments WHERE instrument_id = %s", (iid,))


def test_seed_business_summary_metadata_writes_deferred_no_sections(conn: psycopg.Connection[Any]) -> None:
    from app.services.business_summary import get_parse_status, seed_business_summary_metadata

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        assert (
            seed_business_summary_metadata(conn, instrument_id=iid, source_accession=acc, filed_at=datetime.now(tz=UTC))
            is True
        )
        with conn.cursor() as cur:
            cur.execute("SELECT body, body_deferred FROM instrument_business_summary WHERE instrument_id=%s", (iid,))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "" and bool(row[1]) is True
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM instrument_business_summary_sections WHERE instrument_id=%s", (iid,))
            count_row = cur.fetchone()
        assert count_row is not None and int(count_row[0]) == 0
        # Classifies 'deferred', NOT parse_failed (committee I1).
        ps = get_parse_status(conn, instrument_id=iid)
        assert ps is not None and ps.state == "deferred"
        # ON CONFLICT DO NOTHING — re-seed is a no-op, never clobbers.
        assert (
            seed_business_summary_metadata(conn, instrument_id=iid, source_accession=acc, filed_at=datetime.now(tz=UTC))
            is False
        )
    finally:
        _cleanup(conn, iid)


def test_record_parse_attempt_clears_body_deferred(conn: psycopg.Connection[Any]) -> None:
    from app.services.business_summary import get_parse_status, record_parse_attempt, seed_business_summary_metadata

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        seed_business_summary_metadata(conn, instrument_id=iid, source_accession=acc, filed_at=datetime.now(tz=UTC))
        record_parse_attempt(conn, instrument_id=iid, source_accession=acc, reason="no_item_1_marker")
        with conn.cursor() as cur:
            cur.execute("SELECT body_deferred FROM instrument_business_summary WHERE instrument_id=%s", (iid,))
            row = cur.fetchone()
        assert row is not None and bool(row[0]) is False
        # No longer 'deferred' → the panel won't re-trigger the lazy fetch (§1265).
        ps = get_parse_status(conn, instrument_id=iid)
        assert ps is not None and ps.state != "deferred"
    finally:
        _cleanup(conn, iid)


def test_ingest_business_summaries_metadata_only_no_fetch(conn: psycopg.Connection[Any]) -> None:
    from app.services.business_summary import ingest_business_summaries

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        _seed_filing_event(conn, iid, form="10-K", accession=acc, filing_date=datetime.now(tz=UTC).date())
        # metadata_only=True → seeds a deferred placeholder, NEVER fetches.
        result = ingest_business_summaries(conn, _RaisingFetcher(), limit=10, metadata_only=True)
        assert result.rows_inserted >= 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT body, body_deferred, source_accession FROM instrument_business_summary WHERE instrument_id=%s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "" and bool(row[1]) is True and str(row[2]) == acc
    finally:
        _cleanup(conn, iid)


def test_ingest_8k_events_metadata_only_no_fetch(
    conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.services.eight_k_events as ek

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        rd = datetime.now(tz=UTC).date()
        _seed_filing_event(conn, iid, form="8-K", accession=acc, filing_date=rd, report_date=rd, items=["1.01", "9.01"])
        # Under an orchestrated bootstrap, resolve_progress_context() is
        # non-None → metadata-only seed. Patch it so the gate fires
        # without standing up a real bootstrap run.
        monkeypatch.setattr(ek, "resolve_progress_context", lambda: object())
        result = ek.ingest_8k_events(conn, _RaisingFetcher(), limit=10)
        assert result.filings_parsed >= 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT body_deferred, date_of_report, is_tombstone FROM eight_k_filings WHERE accession_number=%s",
                (acc,),
            )
            row = cur.fetchone()
        assert row is not None
        assert bool(row[0]) is True and row[1] == rd and bool(row[2]) is False
        with conn.cursor() as cur:
            cur.execute(
                "SELECT item_code, body FROM eight_k_items WHERE accession_number=%s ORDER BY item_order",
                (acc,),
            )
            items = cur.fetchall()
        assert [str(c) for c, _ in items] == ["1.01", "9.01"]
        assert all(b == "" for _, b in items)  # bodies deferred to first view
    finally:
        _cleanup(conn, iid)


def test_seed_eight_k_metadata_dedupes_and_no_clobber(conn: psycopg.Connection[Any]) -> None:
    from app.services.eight_k_events import seed_eight_k_metadata

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        rd = datetime.now(tz=UTC).date()
        ok = seed_eight_k_metadata(
            conn,
            instrument_id=iid,
            accession_number=acc,
            document_type="8-K",
            is_amendment=False,
            date_of_report=rd,
            primary_document_url="https://sec.example/8k.htm",
            known_items=("2.02", "2.02", "7.01"),
        )
        assert ok is True
        with conn.cursor() as cur:
            cur.execute("SELECT item_code FROM eight_k_items WHERE accession_number=%s ORDER BY item_order", (acc,))
            codes = [str(r[0]) for r in cur.fetchall()]
        assert codes == ["2.02", "7.01"]  # deduped, source-order preserved
        # ON CONFLICT DO NOTHING — re-seed never clobbers a fetched filing.
        assert (
            seed_eight_k_metadata(
                conn,
                instrument_id=iid,
                accession_number=acc,
                document_type="8-K",
                is_amendment=False,
                date_of_report=rd,
                primary_document_url="x",
                known_items=(),
            )
            is False
        )
    finally:
        _cleanup(conn, iid)


def test_fetch_business_summary_body_now_not_deferred_skips_fetch(conn: psycopg.Connection[Any]) -> None:
    from app.services.business_summary import fetch_business_summary_body_now, upsert_business_summary

    iid = _seed_instrument(conn)
    try:
        # A real (non-deferred) body → the lazy fill must early-return
        # without touching the fetcher.
        upsert_business_summary(
            conn, instrument_id=iid, body="Real Item 1 body.", source_accession="acc-1", filed_at=datetime.now(tz=UTC)
        )
        outcome = fetch_business_summary_body_now(conn, _RaisingFetcher(), instrument_id=iid)
        assert outcome == "not_deferred"
    finally:
        _cleanup(conn, iid)


def test_lazy_fill_transient_error_leaves_row_deferred(conn: psycopg.Connection[Any]) -> None:
    """A transient fetch failure must NOT exit the deferred state.

    Codex ckpt2 BLOCKING-1: recording a parse attempt on a transient
    error would clear ``body_deferred`` and stop the panel ever retrying.
    A transient error must propagate (→ 503) and leave the row deferred."""
    from app.services.business_summary import fetch_business_summary_body_now, seed_business_summary_metadata

    class _Transient:
        def fetch_document_text(self, absolute_url: str) -> str | None:
            raise RuntimeError("simulated transient fetch failure (5xx)")

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        seed_business_summary_metadata(conn, instrument_id=iid, source_accession=acc, filed_at=datetime.now(tz=UTC))
        _seed_filing_event(conn, iid, form="10-K", accession=acc, filing_date=datetime.now(tz=UTC).date())
        with pytest.raises(RuntimeError):
            fetch_business_summary_body_now(conn, _Transient(), instrument_id=iid)
        with conn.cursor() as cur:
            cur.execute("SELECT body_deferred FROM instrument_business_summary WHERE instrument_id=%s", (iid,))
            row = cur.fetchone()
        assert row is not None and bool(row[0]) is True  # still deferred → next view retries
    finally:
        _cleanup(conn, iid)


def test_lazy_8k_no_source_tombstones_and_exits_deferred(conn: psycopg.Connection[Any]) -> None:
    """A deferred 8-K with no primary_document_url must EXIT deferred.

    Bot review BLOCKING: without tombstoning, the row stays
    body_deferred=TRUE and is re-attempted on every detail open (silent
    infinite-defer, no backoff)."""
    from app.services.eight_k_events import fetch_eight_k_body_now, seed_eight_k_metadata

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        seed_eight_k_metadata(
            conn,
            instrument_id=iid,
            accession_number=acc,
            document_type="8-K",
            is_amendment=False,
            date_of_report=datetime.now(tz=UTC).date(),
            primary_document_url="",  # malformed: no fetchable URL
            known_items=("1.01",),
        )
        outcome = fetch_eight_k_body_now(conn, _RaisingFetcher(), accession_number=acc)
        assert outcome == "no_source"
        with conn.cursor() as cur:
            cur.execute("SELECT is_tombstone, body_deferred FROM eight_k_filings WHERE accession_number=%s", (acc,))
            row = cur.fetchone()
        assert row is not None
        assert bool(row[0]) is True and bool(row[1]) is False  # tombstoned, exited deferred
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM eight_k_items WHERE accession_number=%s", (acc,))
            count_row = cur.fetchone()
        assert count_row is not None and int(count_row[0]) == 0  # seeded items dropped
    finally:
        _cleanup(conn, iid)
