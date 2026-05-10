"""Tests for the manifest-parser audit (#935 §5)."""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from app.services.manifest_parser_audit import compute_manifest_parser_audit
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_aapl(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (935001, 'AAPL', 'Apple', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.commit()


def _seed_manifest_row(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    form: str,
    source: str,
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik="0000320193",
        form=form,
        source=source,  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id="935001",
        instrument_id=935001,
        filed_at=datetime(2026, 5, 11, tzinfo=UTC),
    )


def test_audit_lists_every_known_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The audit returns one row per ``ManifestSource`` even when the
    table is empty — sources with zero rows are still operator-visible
    so a "wired but unused" lane shows up."""
    report = compute_manifest_parser_audit(ebull_test_conn, registered_sources=frozenset({"sec_form4", "sec_8k"}))

    sources = {r.source for r in report.sources}
    assert "sec_form4" in sources
    assert "sec_8k" in sources
    assert "sec_def14a" in sources  # known but not registered
    # No manifest rows seeded; every source has zero counts.
    for r in report.sources:
        assert r.rows_pending == 0
        assert r.rows_fetched == 0
        assert r.rows_parsed == 0
        assert r.rows_failed == 0
        assert r.rows_tombstoned == 0


def test_stuck_no_parser_counts_pending_and_fetched_on_unregistered_source(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A source with no registered parser whose manifest rows are
    pending/fetched reports them under ``stuck_no_parser``.
    Parsed / tombstoned / failed are terminal-ish; they don't
    contribute to the stuck count."""
    conn = ebull_test_conn
    _seed_aapl(conn)
    _seed_manifest_row(conn, accession="0000320193-26-000001", form="DEF 14A", source="sec_def14a")
    # A second pending row on the same source.
    _seed_manifest_row(conn, accession="0000320193-26-000002", form="DEF 14A", source="sec_def14a")
    conn.commit()

    # sec_def14a not in registered_sources → both rows count as stuck.
    report = compute_manifest_parser_audit(conn, registered_sources=frozenset({"sec_form4"}))
    def14a = next(r for r in report.sources if r.source == "sec_def14a")
    assert def14a.has_registered_parser is False
    assert def14a.rows_pending == 2
    assert def14a.stuck_no_parser == 2
    assert report.total_stuck_no_parser == 2


def test_registered_parser_zeroes_stuck_count(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """If the source HAS a registered parser, ``stuck_no_parser`` is
    zero regardless of pending row count — the worker will pick them
    up on its next tick."""
    conn = ebull_test_conn
    _seed_aapl(conn)
    _seed_manifest_row(conn, accession="0000320193-26-000010", form="8-K", source="sec_8k")
    conn.commit()

    report = compute_manifest_parser_audit(conn, registered_sources=frozenset({"sec_8k"}))
    eight_k = next(r for r in report.sources if r.source == "sec_8k")
    assert eight_k.has_registered_parser is True
    assert eight_k.rows_pending == 1
    assert eight_k.stuck_no_parser == 0
    assert report.total_stuck_no_parser == 0


def test_fetched_and_failed_rows_count_as_stuck_too(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Codex pre-push round 1: ``stuck_no_parser`` covers pending +
    fetched + failed. The worker's ``iter_retryable`` scans failed
    rows past their backoff, and the worker's ``iter_pending`` covers
    both pending and fetched (a row can sit in ``fetched`` if the
    fetcher landed the body but the parser hadn't run yet). Excluding
    either would under-report the worker's actual drop rate."""
    conn = ebull_test_conn
    _seed_aapl(conn)
    # One pending row, one fetched, one failed — all on the same
    # unregistered source.
    _seed_manifest_row(conn, accession="0000320193-26-000030", form="DEF 14A", source="sec_def14a")
    _seed_manifest_row(conn, accession="0000320193-26-000031", form="DEF 14A", source="sec_def14a")
    _seed_manifest_row(conn, accession="0000320193-26-000032", form="DEF 14A", source="sec_def14a")
    conn.execute(
        """
        UPDATE sec_filing_manifest
           SET ingest_status = 'fetched'
         WHERE accession_number = '0000320193-26-000031'
        """,
    )
    conn.execute(
        """
        UPDATE sec_filing_manifest
           SET ingest_status = 'failed'
         WHERE accession_number = '0000320193-26-000032'
        """,
    )
    conn.commit()

    report = compute_manifest_parser_audit(conn, registered_sources=frozenset())
    def14a = next(r for r in report.sources if r.source == "sec_def14a")
    assert def14a.rows_pending == 1
    assert def14a.rows_fetched == 1
    assert def14a.rows_failed == 1
    # Pending + fetched + failed all count toward stuck.
    assert def14a.stuck_no_parser == 3


def test_total_stuck_aggregates_across_sources(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``total_stuck_no_parser`` sums per-source stuck counts across
    every unregistered source."""
    conn = ebull_test_conn
    _seed_aapl(conn)
    _seed_manifest_row(conn, accession="0000320193-26-000020", form="DEF 14A", source="sec_def14a")
    _seed_manifest_row(conn, accession="0000320193-26-000021", form="N-PORT", source="sec_n_port")
    conn.commit()

    # Neither source has a registered parser; both pending rows count.
    report = compute_manifest_parser_audit(conn, registered_sources=frozenset())
    assert report.total_stuck_no_parser == 2
