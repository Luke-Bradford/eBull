"""Tests for the 8-K manifest-worker parser adapter (#873).

The parser wraps the existing ``eight_k_events`` pure-parser +
table-writer so the manifest worker can drive 8-K ingest one
accession at a time. Tests cover:

- Happy path: HTML fetch → store_raw → parse → upsert → ParseOutcome.
- Tombstone: fetch returns empty body OR parser returns None.
- Failure: fetch raises (transient — worker retries).
- Raw-payload invariant: the worker rejects a parsed outcome with
  raw_status='absent' (pinned by registering with
  ``requires_raw_payload=True``).

The fetch boundary is monkeypatched at the
``SecFilingsProvider.fetch_document_text`` level so tests run without
touching the SEC.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_pending_8k(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    primary_doc_url: str = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/aapl-8k.htm",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik="0000320193",
        form="8-K",
        source="sec_8k",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 5, 11, tzinfo=UTC),
        primary_document_url=primary_doc_url,
    )


# A minimal 8-K HTML body the parser will accept — declares
# Form 8-K, a single numbered item, and a signature block. The
# parser returns None if it can't find the ``8-K`` marker token,
# so we include it explicitly in the body.
_FAKE_8K_HTML = """
<html><body>
<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>
<p>FORM 8-K</p>
<p>Date of Report (Date of earliest event reported): May 11, 2026</p>
<p>APPLE INC.</p>
<p>Item 8.01 Other Events</p>
<p>Apple announced a partnership.</p>
<p>SIGNATURE</p>
<p>By: /s/ Luca Maestri</p>
<p>Name: Luca Maestri</p>
<p>Title: Chief Financial Officer</p>
<p>Date: May 11, 2026</p>
</body></html>
"""


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    """Wipe the worker parser registry before each test, then call
    ``register_all_parsers()`` so every production parser
    re-registers cleanly. ``importlib.reload`` would only re-run
    the package ``__init__.py``, not the per-source submodules
    Python has already cached — the explicit ``register_all_parsers``
    is what makes test isolation work."""
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def test_happy_path_parses_and_stores_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest worker drains an 8-K pending row when the registered
    parser fetches → store_raw → parse → upsert successfully."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=8730001, symbol="AAPL")
    _seed_pending_8k(ebull_test_conn, accession="0000320193-26-000001", instrument_id=8730001)
    ebull_test_conn.commit()

    # Patch SecFilingsProvider so no real HTTP fires.
    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_8K_HTML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_8k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    assert stats.skipped_no_parser == 0

    row = get_manifest_row(ebull_test_conn, "0000320193-26-000001")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    # eight_k_filings row exists.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id, document_type, is_tombstone FROM eight_k_filings "
            "WHERE accession_number = '0000320193-26-000001'"
        )
        row8k = cur.fetchone()
    assert row8k is not None
    assert row8k[0] == 8730001
    assert row8k[1] == "8-K"
    assert row8k[2] is False

    # filing_raw_documents has the body so a future re-wash can reparse.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT byte_count FROM filing_raw_documents
            WHERE accession_number = '0000320193-26-000001'
              AND document_kind = 'primary_doc'
            """
        )
        raw = cur.fetchone()
    assert raw is not None
    assert raw[0] > 0


def test_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the fetch returns empty / non-200 body, the manifest row
    transitions to ``tombstoned`` and an entity-level tombstone is
    written to eight_k_filings (matching legacy semantics)."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=8730002, symbol="DEAD")
    _seed_pending_8k(ebull_test_conn, accession="0000999999-26-000001", instrument_id=8730002)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: None,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_8k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0000999999-26-000001")
    assert row is not None and row.ingest_status == "tombstoned"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT is_tombstone FROM eight_k_filings WHERE accession_number = '0000999999-26-000001'")
        row8k = cur.fetchone()
    assert row8k is not None and row8k[0] is True


def test_fetch_exception_marks_failed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the fetch raises, the manifest row transitions to
    ``failed`` so the worker retries on its backoff schedule."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=8730003, symbol="TRAN")
    _seed_pending_8k(ebull_test_conn, accession="0000777777-26-000001", instrument_id=8730003)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_8k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0000777777-26-000001")
    assert row is not None and row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    # Codex pre-push round 2: failed outcomes from this parser MUST
    # carry next_retry_at = now + 1h so the worker honours the
    # standard backoff. Without the explicit set in
    # ``_failed_outcome``, the worker would retry immediately,
    # hammering SEC on every tick.
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    # 1h backoff ± slack for clock drift across the test.
    assert 3300 < delta < 3900


def test_parse_phase_exception_preserves_stored_raw_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push round 2 BLOCKING: if the labels-load or
    ``parse_8k_filing`` raises AFTER ``store_raw`` committed, the
    parser MUST return ``_failed_outcome(raw_status='stored')`` so
    the manifest reflects actual raw state. Otherwise the manifest
    would say ``raw_status='absent'`` while the raw row physically
    exists — permanent split between the two tables, plus store_raw
    unique-conflict churn on every retry."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import eight_k as parser_module

    _seed_instrument(ebull_test_conn, iid=8730004, symbol="SPLIT")
    _seed_pending_8k(ebull_test_conn, accession="0000111111-26-000001", instrument_id=8730004)
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_8K_HTML,
    )

    def _raising_parse(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic parser crash")

    monkeypatch.setattr(parser_module, "parse_8k_filing", _raising_parse)

    stats = run_manifest_worker(ebull_test_conn, source="sec_8k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0000111111-26-000001")
    assert row is not None
    assert row.ingest_status == "failed"
    # Critical: raw_status=stored because store_raw ran BEFORE the
    # parse exception. Without the fix this would be 'absent' and
    # diverge from the filing_raw_documents row that physically exists.
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM filing_raw_documents WHERE accession_number = '0000111111-26-000001'")
        assert cur.fetchone() is not None


def test_deterministic_upsert_exception_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: a non-transient upsert exception
    (``IntegrityError``-shape constraint violation, or any non-DB
    Python exception) must tombstone the manifest row + the
    ``eight_k_filings`` typed table — re-fetching the same dead HTML
    every hour on a deterministic bug wastes SEC fair-use budget."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import eight_k as parser_module

    _seed_instrument(ebull_test_conn, iid=8730050, symbol="UFAIL")
    _seed_pending_8k(ebull_test_conn, accession="0000888888-26-000050", instrument_id=8730050)
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_8K_HTML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic 8-K upsert constraint violation")

    monkeypatch.setattr(parser_module, "upsert_8k_filing", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_8k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    assert stats.failed == 0
    row = get_manifest_row(ebull_test_conn, "0000888888-26-000050")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    # Class name embedded so the backfill can discriminate.
    assert row.error is not None
    assert "RuntimeError" in row.error
    assert "upsert error" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT is_tombstone FROM eight_k_filings WHERE accession_number = '0000888888-26-000050'")
        f = cur.fetchone()
    assert f is not None and f[0] is True


def test_transient_upsert_exception_retries(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: a transient psycopg ``OperationalError``
    (SerializationFailure / DeadlockDetected / connection drop) on
    the upsert must keep the manifest row in ``failed`` with a 1h
    backoff so the worker retries on the next tick — the parsed XML
    isn't the problem, the DB-side state is."""
    import psycopg.errors

    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import eight_k as parser_module

    _seed_instrument(ebull_test_conn, iid=8730051, symbol="UTRAN")
    _seed_pending_8k(ebull_test_conn, accession="0000888888-26-000051", instrument_id=8730051)
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_8K_HTML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.SerializationFailure("synthetic serialisation failure")

    monkeypatch.setattr(parser_module, "upsert_8k_filing", _raising_upsert)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_8k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, "0000888888-26-000051")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None
    assert "SerializationFailure" in row.error
    # 1h backoff respected.
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    assert 3300 < delta < 3900

    # No typed-table tombstone written — transient must keep the
    # accession alive for retry. Without this guard a deadlock would
    # tombstone the row + skip every future retry attempt.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM eight_k_filings WHERE accession_number = '0000888888-26-000051'")
        assert cur.fetchone() is None


def test_parser_registered_via_register_all() -> None:
    """``register_all_parsers()`` populates the worker registry with
    every production parser. Pins the architecture invariant that the
    package's public registration function is the SINGLE place that
    wires parsers into the worker."""
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parsers import register_all_parsers

    # Autouse fixture above already registered; sanity-check.
    assert "sec_8k" in registered_parser_sources()

    clear_registered_parsers()
    assert "sec_8k" not in registered_parser_sources()

    register_all_parsers()
    assert "sec_8k" in registered_parser_sources()
