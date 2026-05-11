"""Tests for the DEF 14A manifest-worker parser adapter (#873).

The parser wraps the existing ``def14a_ingest`` pure-parser +
table-writer helpers so the manifest worker can drive DEF 14A
ingest one accession at a time. Tests cover:

- Happy path: HTML fetch → store_raw → parse → upsert beneficial
  holdings + write-through observations → ParseOutcome(parsed).
- Tombstone on empty fetch: 404 / empty body returns tombstoned +
  records a ``failed`` ingest-log row.
- Tombstone on no-table: parser identifies no beneficial-ownership
  table (notice-only proxy) → tombstoned + ``partial`` log row +
  raw_status=stored.
- Fetch raises: returns failed with 1h backoff (worker retries).
- Parse-phase exception AFTER store_raw: returns failed with
  raw_status='stored' so the manifest matches filing_raw_documents
  state (mirrors the 8-K Codex round 2 BLOCKING).
- Registration: ``register_all_parsers`` wires sec_def14a into the
  worker's parser registry.

The fetch boundary is monkeypatched at
``SecFilingsProvider.fetch_document_text`` level so tests run
without touching SEC.
"""

from __future__ import annotations

from datetime import UTC, datetime
from textwrap import dedent

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik)
        VALUES (%s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
        """,
        (iid, cik),
    )


def _seed_pending_def14a(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    cik: str = "0000320193",
    form: str = "DEF 14A",
    primary_doc_url: str = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000010/def14a.htm",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_def14a",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 5, 11, tzinfo=UTC),
        primary_document_url=primary_doc_url,
    )


# A minimal DEF 14A body the parser will accept — section heading
# + beneficial-ownership table with headers the scorer recognises.
# Pattern copied from tests/test_sec_def14a_parser.py so this is
# guaranteed-parseable upstream.
_FAKE_DEF14A_HTML = dedent("""
<!DOCTYPE html>
<html><head><title>Proxy Statement</title></head>
<body>
<h1>Notice of Annual Meeting</h1>
<p>Some preamble prose.</p>

<h2>Security Ownership of Certain Beneficial Owners and Management</h2>
<p>The following table sets forth the beneficial ownership as of March 1, 2026.</p>
<table>
  <tr>
    <th>Name and Address of Beneficial Owner</th>
    <th>Number of Shares Beneficially Owned</th>
    <th>Percent of Class</th>
  </tr>
  <tr><td>John Doe, CEO</td><td>1,500,000</td><td>5.5%</td></tr>
  <tr><td>Vanguard Group, Inc.</td><td>3,000,000</td><td>11.0%</td></tr>
</table>
<p>Footnotes:</p>
<ol><li>Includes options exercisable within 60 days.</li></ol>
</body></html>
""")

# A DEF 14A body the parser will accept as a notice-only proxy —
# heading present but no table the scorer recognises as a
# beneficial-ownership table. Forces parser.rows == [] so the
# adapter exercises the no-rows tombstone path.
_NOTICE_ONLY_DEF14A_HTML = dedent("""
<!DOCTYPE html>
<html><body>
<h2>Notice of Annual Meeting</h2>
<p>The annual meeting will ratify the auditor. No governance changes.</p>
<table>
  <tr><th>Date</th><th>Time</th></tr>
  <tr><td>April 1, 2026</td><td>10:00 AM</td></tr>
</table>
</body></html>
""")


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    """Wipe the worker parser registry before each test, then call
    ``register_all_parsers()`` so every production parser
    re-registers cleanly. Mirrors the 8-K test fixture so test
    isolation works the same way across parser test modules."""
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def test_happy_path_parses_and_stores_raw_and_holdings(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest worker drains a DEF 14A pending row when the
    registered parser fetches → store_raw → parse → upsert
    holdings + observations → log success."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8740001
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL", cik="0000320193")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000320193-26-000010",
        instrument_id=iid,
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_DEF14A_HTML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    assert stats.skipped_no_parser == 0

    row = get_manifest_row(ebull_test_conn, "0000320193-26-000010")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"
    assert row.parser_version == "def14a-v1"

    # def14a_beneficial_holdings rows exist for the parsed table.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM def14a_beneficial_holdings "
            "WHERE accession_number = '0000320193-26-000010' AND instrument_id = %s",
            (iid,),
        )
        count_row = cur.fetchone()
    assert count_row is not None and count_row[0] >= 2

    # def14a_ingest_log records success so legacy discovery skips this.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, rows_inserted FROM def14a_ingest_log WHERE accession_number = '0000320193-26-000010'"
        )
        log_row = cur.fetchone()
    assert log_row is not None
    assert log_row[0] == "success"
    assert log_row[1] >= 2

    # filing_raw_documents has the body so a re-wash can reparse.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT byte_count FROM filing_raw_documents
            WHERE accession_number = '0000320193-26-000010'
              AND document_kind = 'def14a_body'
            """
        )
        raw = cur.fetchone()
    assert raw is not None
    assert raw[0] > 0


def test_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty/404 body → manifest row tombstoned + ingest-log
    records ``failed`` so the legacy discovery filter skips it."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8740002
    _seed_instrument(ebull_test_conn, iid=iid, symbol="DEAD", cik="0000999999")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000999999-26-000020",
        instrument_id=iid,
        cik="0000999999",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: None,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0000999999-26-000020")
    assert row is not None and row.ingest_status == "tombstoned"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT status FROM def14a_ingest_log WHERE accession_number = '0000999999-26-000020'")
        log_row = cur.fetchone()
    assert log_row is not None and log_row[0] == "failed"


def test_no_table_tombstones_with_stored_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Notice-only proxy (heading present, no recognisable table) →
    manifest row tombstoned + raw_status='stored' (body is on disk)
    + log row status='partial'. Mirrors legacy 'partial' bucket so
    operator dashboard counts converge."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8740003
    _seed_instrument(ebull_test_conn, iid=iid, symbol="NOTC", cik="0000111111")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000111111-26-000030",
        instrument_id=iid,
        cik="0000111111",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _NOTICE_ONLY_DEF14A_HTML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0000111111-26-000030")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    # Body persisted before parse — raw row exists; manifest reflects.
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT status FROM def14a_ingest_log WHERE accession_number = '0000111111-26-000030'")
        log_row = cur.fetchone()
    assert log_row is not None and log_row[0] == "partial"


def test_fetch_exception_marks_failed_with_backoff(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch raise → manifest row failed + next_retry_at = now+1h.
    Without the explicit backoff in ``_failed_outcome``, the worker
    would retry the row on every tick and hammer SEC."""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8740004
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TRAN", cik="0000222222")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000222222-26-000040",
        instrument_id=iid,
        cik="0000222222",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0000222222-26-000040")
    assert row is not None and row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    # 1h backoff ± slack for test wall-clock drift.
    assert 3300 < delta < 3900


def test_parse_phase_exception_preserves_stored_raw_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the parser raises AFTER store_raw committed inside its
    savepoint, the parser MUST return raw_status='stored' so the
    manifest matches filing_raw_documents. Otherwise the manifest
    says raw_status='absent' while the raw row exists — permanent
    split between tables, plus store_raw unique-conflict churn on
    every retry."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import def14a as parser_module

    iid = 8740005
    _seed_instrument(ebull_test_conn, iid=iid, symbol="SPLIT", cik="0000333333")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000333333-26-000050",
        instrument_id=iid,
        cik="0000333333",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_DEF14A_HTML,
    )

    def _raising_parse(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic parser crash")

    monkeypatch.setattr(parser_module, "parse_beneficial_ownership_table", _raising_parse)

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, "0000333333-26-000050")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM filing_raw_documents WHERE accession_number = '0000333333-26-000050'")
        assert cur.fetchone() is not None


def test_pre_14a_tombstones_to_match_legacy(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """PRE 14A (preliminary proxy) routes to sec_def14a in the
    manifest but the legacy ingester excludes it. The parser
    tombstones PRE 14A to keep dual-path accounting consistent
    during cutover — no body fetch, no holdings write. (Codex
    pre-push P1.)"""
    import app.services.manifest_parsers  # noqa: F401 — register

    iid = 8740006
    _seed_instrument(ebull_test_conn, iid=iid, symbol="PRE", cik="0000444444")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000444444-26-000060",
        instrument_id=iid,
        cik="0000444444",
        form="PRE 14A",
    )
    ebull_test_conn.commit()

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, "0000444444-26-000060")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.error is not None and "PRE 14A" in row.error
    # No raw fetched, no holdings, no ingest-log row.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM filing_raw_documents WHERE accession_number = '0000444444-26-000060'")
        raw_count = cur.fetchone()
    assert raw_count is not None and raw_count[0] == 0
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM def14a_beneficial_holdings WHERE accession_number = '0000444444-26-000060'")
        holdings_count = cur.fetchone()
    assert holdings_count is not None and holdings_count[0] == 0
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM def14a_ingest_log WHERE accession_number = '0000444444-26-000060'")
        log_count = cur.fetchone()
    assert log_count is not None and log_count[0] == 0


def test_siblings_resolution_failure_preserves_stored_raw_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push BLOCKING (original): if siblings resolution raises
    AFTER ``store_raw`` committed in its savepoint, the parser MUST
    NOT mark the manifest ``raw_status='absent'`` — the raw body
    physically exists in ``filing_raw_documents``. Manifest must
    reflect that state regardless of which terminal outcome the
    parser returns.

    PR #1131 update: a synthetic ``RuntimeError`` from
    ``_resolve_siblings`` is now classified as deterministic by
    ``is_transient_upsert_error`` (non-DB Python exception → never
    self-fixes on retry), so the row tombstones with
    raw_status='stored' instead of staying ``failed``. The invariant
    under test — raw_status reflects ground truth — still holds; the
    only behavioural delta is the terminal manifest state."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import def14a as parser_module

    iid = 8740007
    _seed_instrument(ebull_test_conn, iid=iid, symbol="SIB", cik="0000555555")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000555555-26-000070",
        instrument_id=iid,
        cik="0000555555",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_DEF14A_HTML,
    )

    def _raising_siblings(conn, *, instrument_id, issuer_cik):  # noqa: ARG001
        raise RuntimeError("synthetic siblings DB error")

    monkeypatch.setattr(parser_module, "_resolve_siblings", _raising_siblings)

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    # PR #1131: deterministic exception → tombstoned (was failed).
    assert stats.tombstoned == 1
    assert stats.failed == 0
    row = get_manifest_row(ebull_test_conn, "0000555555-26-000070")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    # Invariant under test: raw_status=stored because store_raw ran
    # BEFORE the siblings raise. The fix moves _resolve_siblings INTO
    # the same try block whose except returns raw_status='stored'.
    assert row.raw_status == "stored"
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM filing_raw_documents WHERE accession_number = '0000555555-26-000070'")
        assert cur.fetchone() is not None


def test_deterministic_upsert_exception_tombstones_with_log_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: deterministic upsert exception tombstones the
    manifest + writes a ``def14a_ingest_log`` row with status='failed'
    (mirrors the existing empty-body / no-rows audit-log pattern)."""
    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import def14a as parser_module

    iid = 8740090
    _seed_instrument(ebull_test_conn, iid=iid, symbol="UFAIL", cik="0000666666")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000666666-26-000090",
        instrument_id=iid,
        cik="0000666666",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_DEF14A_HTML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic DEF 14A upsert violation")

    monkeypatch.setattr(parser_module, "_upsert_holding", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    assert stats.failed == 0
    row = get_manifest_row(ebull_test_conn, "0000666666-26-000090")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    assert row.error is not None
    assert "RuntimeError" in row.error

    # Ingest-log row pinned at status='failed' so audit-trail
    # accounting matches legacy semantics.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT status, error FROM def14a_ingest_log WHERE accession_number = '0000666666-26-000090'")
        log = cur.fetchone()
    assert log is not None
    assert log[0] == "failed"
    assert log[1] is not None and "RuntimeError" in log[1]


def test_transient_upsert_exception_retries(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: an ``OperationalError`` on the upsert phase keeps the
    manifest in ``failed`` with a 1h backoff — no log-row write, no
    tombstone — so the next retry sees a clean slate."""
    import psycopg.errors

    from app.providers.implementations import sec_edgar
    from app.services.manifest_parsers import def14a as parser_module

    iid = 8740091
    _seed_instrument(ebull_test_conn, iid=iid, symbol="UTRAN", cik="0000666667")
    _seed_pending_def14a(
        ebull_test_conn,
        accession="0000666667-26-000091",
        instrument_id=iid,
        cik="0000666667",
    )
    ebull_test_conn.commit()

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_DEF14A_HTML,
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.DeadlockDetected("synthetic deadlock")

    monkeypatch.setattr(parser_module, "_upsert_holding", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_def14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, "0000666667-26-000091")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None
    assert "DeadlockDetected" in row.error

    # No ingest-log entry written — transient must keep the retry path
    # clean so a deterministic-resolution later doesn't see a stale
    # 'failed' marker on this accession.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM def14a_ingest_log WHERE accession_number = '0000666667-26-000091'")
        assert cur.fetchone() is None


def test_parser_registered_via_register_all() -> None:
    """``register_all_parsers()`` populates the worker registry with
    sec_def14a alongside sec_8k. Pins the architecture invariant
    that the package's public registration function is the SINGLE
    place that wires parsers into the worker."""
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parsers import register_all_parsers

    assert "sec_def14a" in registered_parser_sources()

    clear_registered_parsers()
    assert "sec_def14a" not in registered_parser_sources()

    register_all_parsers()
    assert "sec_def14a" in registered_parser_sources()
