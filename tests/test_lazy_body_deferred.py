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
from psycopg_pool import ConnectionPool

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


@pytest.fixture
def pool() -> Iterator[ConnectionPool[psycopg.Connection[Any]]]:
    """Real pool against ``ebull_test`` for the lazy-fill services (#1472
    PR2: ``fetch_*_body_now`` now borrow + release short-lived pool conns
    around the SEC fetch instead of taking a caller-supplied conn). Writes
    commit on each ``with pool.connection()`` exit, so the autocommit
    ``conn`` fixture sees them when asserting."""
    p: ConnectionPool[psycopg.Connection[Any]] = ConnectionPool(_test_database_url(), min_size=1, max_size=2, open=True)
    try:
        yield p
    finally:
        p.close()


class _RaisingFetcher:
    """``fetch_document_text`` that fails the test if called.

    Proves the bootstrap metadata-only path issues ZERO HTTP — a stub
    that merely *returned* a body would still pass even on a
    fetch-then-defer regression (committee IMPORTANT-1)."""

    def fetch_document_text(self, absolute_url: str) -> str | None:
        raise AssertionError(f"fetch_document_text must NOT be called for a deferred/metadata path: {absolute_url}")


# A minimal-but-parseable 8-K body (cover page + Item 1.01 + signature),
# mirroring the proven shape in tests/test_eight_k_events.py so the lazy
# fill's parse → upsert path produces a real item body.
_FILLABLE_8K = """
<html><body>
<p>FORM 8-K</p>
<p>Date of Report (Date of earliest event reported): March 15, 2026</p>
<p>(Exact name of registrant as specified in its charter) APEX INDUSTRIES INC.</p>
<p>Item 1.01. Entry into a Material Definitive Agreement.</p>
<p>On March 14, 2026, the Company entered into a credit agreement with Acme Bank.</p>
<p>SIGNATURE</p>
<p>By: /s/ Jane Smith</p>
</body></html>
"""


class _FixedFetcher:
    """``fetch_document_text`` returning a fixed parseable body."""

    def __init__(self, body: str) -> None:
        self._body = body

    def fetch_document_text(self, absolute_url: str) -> str | None:
        return self._body


# A canonical 10-K Item 1 layout (TOC → body heading → Item 1A end marker),
# mirroring the proven fixture in tests/test_business_summary.py so
# extract_business_section returns a real body.
_FILLABLE_10K = """
<html><body>
<h2>Table of Contents</h2>
<p>Item 1. Business .... 3</p>
<p>Item 1A. Risk Factors ... 10</p>
<h2>Item 1. Business</h2>
<p>The Company is a global manufacturer of specialty materials used in
   aerospace and automotive end markets.</p>
<p>We operate through four segments: Industrial, Safety, Transportation,
   and Consumer.</p>
<h2>Item 1A. Risk Factors</h2>
<p>The following factors may affect our results.</p>
</body></html>
"""


class _DriftingFetcher:
    """Simulate a concurrent reseed during the fetch window: when the body
    is fetched, advance the row's ``source_accession`` to a NEW (still
    deferred) accession on a side connection, then return a body for the
    OLD one. Drives the phase-3 drift guard (#1492 Codex ckpt2)."""

    def __init__(self, conn: psycopg.Connection[Any], instrument_id: int, new_accession: str, body: str) -> None:
        self._conn = conn
        self._iid = instrument_id
        self._new = new_accession
        self._body = body

    def fetch_document_text(self, absolute_url: str) -> str | None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE instrument_business_summary SET source_accession = %s WHERE instrument_id = %s",
                (self._new, self._iid),
            )
        return self._body


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


def test_fetch_business_summary_body_now_not_deferred_skips_fetch(
    conn: psycopg.Connection[Any], pool: ConnectionPool[psycopg.Connection[Any]]
) -> None:
    from app.services.business_summary import fetch_business_summary_body_now, upsert_business_summary

    iid = _seed_instrument(conn)
    try:
        # A real (non-deferred) body → the lazy fill must early-return
        # without touching the fetcher.
        upsert_business_summary(
            conn, instrument_id=iid, body="Real Item 1 body.", source_accession="acc-1", filed_at=datetime.now(tz=UTC)
        )
        outcome = fetch_business_summary_body_now(pool, _RaisingFetcher(), instrument_id=iid)
        assert outcome == "not_deferred"
    finally:
        _cleanup(conn, iid)


def test_lazy_fill_transient_error_leaves_row_deferred(
    conn: psycopg.Connection[Any], pool: ConnectionPool[psycopg.Connection[Any]]
) -> None:
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
            fetch_business_summary_body_now(pool, _Transient(), instrument_id=iid)
        with conn.cursor() as cur:
            cur.execute("SELECT body_deferred FROM instrument_business_summary WHERE instrument_id=%s", (iid,))
            row = cur.fetchone()
        assert row is not None and bool(row[0]) is True  # still deferred → next view retries
    finally:
        _cleanup(conn, iid)


def test_lazy_8k_no_source_tombstones_and_exits_deferred(
    conn: psycopg.Connection[Any], pool: ConnectionPool[psycopg.Connection[Any]]
) -> None:
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
        outcome = fetch_eight_k_body_now(pool, _RaisingFetcher(), accession_number=acc)
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


def test_lazy_8k_filled_writes_body_and_clears_deferred(
    conn: psycopg.Connection[Any], pool: ConnectionPool[psycopg.Connection[Any]]
) -> None:
    """Happy path: a deferred 8-K with a fetchable URL → the fetched body
    is parsed + cached and ``body_deferred`` is cleared.

    Exercises the #1472 fetch-first write path (phase 3: re-borrow →
    ``pg_advisory_xact_lock`` → re-check → upsert + best-effort manifest),
    which the not_deferred / transient / no_source cases never reach. The
    pool write commits, so the autocommit ``conn`` fixture sees it."""
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
            primary_document_url="https://sec.example/8k.htm",
            known_items=("1.01",),
        )
        _seed_filing_event(
            conn, iid, form="8-K", accession=acc, filing_date=datetime.now(tz=UTC).date(), items=["1.01"]
        )
        outcome = fetch_eight_k_body_now(pool, _FixedFetcher(_FILLABLE_8K), accession_number=acc)
        assert outcome == "filled"
        with conn.cursor() as cur:
            cur.execute("SELECT body_deferred, is_tombstone FROM eight_k_filings WHERE accession_number=%s", (acc,))
            row = cur.fetchone()
        assert row is not None
        assert bool(row[0]) is False  # body_deferred cleared
        assert bool(row[1]) is False  # NOT a tombstone — a real fill
        with conn.cursor() as cur:
            cur.execute(
                "SELECT body FROM eight_k_items WHERE accession_number=%s AND item_code='1.01'",
                (acc,),
            )
            item = cur.fetchone()
        assert item is not None and "credit agreement" in item[0]  # parsed item body cached
    finally:
        _cleanup(conn, iid)


def test_lazy_10k_filled_writes_body_and_clears_deferred(
    conn: psycopg.Connection[Any], pool: ConnectionPool[psycopg.Connection[Any]]
) -> None:
    """Happy path for the 10-K Item 1 lazy fill: deferred row + fetchable
    URL → extracted body cached, ``body_deferred`` cleared. Exercises the
    business-summary phase-3 write path (drift guard → upsert → sections →
    best-effort manifest), which the not_deferred / transient cases skip."""
    from app.services.business_summary import fetch_business_summary_body_now, seed_business_summary_metadata

    iid = _seed_instrument(conn)
    acc = f"000-{uuid4().hex[:12]}"
    try:
        seed_business_summary_metadata(conn, instrument_id=iid, source_accession=acc, filed_at=datetime.now(tz=UTC))
        _seed_filing_event(conn, iid, form="10-K", accession=acc, filing_date=datetime.now(tz=UTC).date())
        outcome = fetch_business_summary_body_now(pool, _FixedFetcher(_FILLABLE_10K), instrument_id=iid)
        assert outcome == "filled"
        with conn.cursor() as cur:
            cur.execute("SELECT body, body_deferred FROM instrument_business_summary WHERE instrument_id=%s", (iid,))
            row = cur.fetchone()
        assert row is not None
        assert bool(row[1]) is False  # body_deferred cleared
        assert "global manufacturer" in row[0]  # parsed Item 1 body cached
    finally:
        _cleanup(conn, iid)


def test_lazy_10k_accession_drift_returns_already_without_clobbering(
    conn: psycopg.Connection[Any], pool: ConnectionPool[psycopg.Connection[Any]]
) -> None:
    """#1492 Codex ckpt2 drift guard: if the row is reseeded to a NEWER
    still-deferred 10-K during the fetch window, phase 3 must detect the
    ``source_accession`` drift and return ``already`` WITHOUT clearing the
    new accession's deferred state (a stale failure-path
    ``record_parse_attempt`` would otherwise exit it via the ungated
    clear)."""
    from app.services.business_summary import fetch_business_summary_body_now, seed_business_summary_metadata

    iid = _seed_instrument(conn)
    acc_old = f"000-{uuid4().hex[:12]}"
    acc_new = f"000-{uuid4().hex[:12]}"
    try:
        seed_business_summary_metadata(conn, instrument_id=iid, source_accession=acc_old, filed_at=datetime.now(tz=UTC))
        _seed_filing_event(conn, iid, form="10-K", accession=acc_old, filing_date=datetime.now(tz=UTC).date())
        fetcher = _DriftingFetcher(conn, iid, acc_new, _FILLABLE_10K)
        outcome = fetch_business_summary_body_now(pool, fetcher, instrument_id=iid)
        assert outcome == "already"  # drift detected → leave the row alone
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_accession, body_deferred FROM instrument_business_summary WHERE instrument_id=%s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert str(row[0]) == acc_new  # reseeded accession untouched
        assert bool(row[1]) is True  # STILL deferred → acc_new can lazy-fill later
    finally:
        _cleanup(conn, iid)
