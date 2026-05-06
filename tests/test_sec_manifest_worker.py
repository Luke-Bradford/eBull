"""Tests for the manifest-driven SEC worker (#869).

Covers:

- Pluggable parser registry: register / dispatch by source
- Worker iterates pending + retryable rows
- Outcome → state transition contract
- Parser exception → failed transition with backoff
- Skip rows whose source has no registered parser
- Per-source filter narrows the iteration
- WorkerStats summary
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    ParseOutcome,
    clear_registered_parsers,
    register_parser,
    run_manifest_worker,
)
from app.services.sec_manifest import (
    ManifestRow,
    get_manifest_row,
    record_manifest_entry,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _clear_parsers() -> None:
    clear_registered_parsers()


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_pending(conn: psycopg.Connection[tuple], *, accession: str, source: str = "sec_form4") -> None:
    _seed_instrument(conn, iid=1, symbol="X")
    record_manifest_entry(
        conn,
        accession,
        cik="0000000001",
        form="4",
        source=source,  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id="1",
        instrument_id=1,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


class TestParserRegistry:
    def test_unregistered_source_skips_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()
        # No parser registered
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.rows_processed == 1
        assert stats.parsed == 0
        assert stats.skipped_no_parser == 1
        # #940: per-source breakdown exposes which sources lack parsers.
        assert stats.skipped_no_parser_by_source == {"sec_form4": 1}

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "pending"  # untouched

    def test_unregistered_source_emits_warning_with_breakdown(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # #940: skipped no-parser rows must surface at WARNING level
        # with the per-source breakdown so an operator running the
        # worker sees real work being dropped without grepping debug.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-2", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-3", source="sec_def14a")
        ebull_test_conn.commit()

        with caplog.at_level("WARNING", logger="app.jobs.sec_manifest_worker"):
            stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=10)
        ebull_test_conn.commit()
        assert stats.skipped_no_parser == 3
        assert stats.skipped_no_parser_by_source == {"sec_form4": 2, "sec_def14a": 1}

        warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert warnings, "expected a WARNING log line for skipped no-parser rows"
        msg = warnings[0].getMessage()
        assert "skipped 3 row(s)" in msg
        assert "sec_form4" in msg
        assert "sec_def14a" in msg

    def test_no_skip_no_warning(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # When every source has a parser, the warning is not emitted —
        # operators should only see the message when something is
        # actually dropped.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)

        with caplog.at_level("WARNING", logger="app.jobs.sec_manifest_worker"):
            stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.skipped_no_parser == 0
        assert stats.skipped_no_parser_by_source == {}
        warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert not warnings, f"unexpected WARNING(s): {[w.getMessage() for w in warnings]}"

    def test_registered_parser_drives_parsed_transition(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def fake_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="stored")

        register_parser("sec_form4", fake_parser)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.skipped_no_parser == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.parser_version == "v1"
        assert row.raw_status == "stored"

    def test_parser_exception_marks_failed_with_backoff(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def crashing_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            raise RuntimeError("HTTP 503 from SEC")

        register_parser("sec_form4", crashing_parser)

        now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10, now=now)
        ebull_test_conn.commit()
        assert stats.failed == 1

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "failed"
        assert row.error is not None
        assert "RuntimeError" in row.error
        assert "HTTP 503" in row.error
        # 1h default backoff
        assert row.next_retry_at == datetime(2026, 1, 1, 13, 0, tzinfo=UTC)

    def test_parser_outcome_failed_uses_provided_retry(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        custom_retry = datetime(2026, 6, 1, tzinfo=UTC)

        def soft_failing_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="failed", error="parse miss", next_retry_at=custom_retry)

        register_parser("sec_form4", soft_failing_parser)
        run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "failed"
        assert row.next_retry_at == custom_retry

    def test_payload_backed_parser_rejects_parsed_with_absent_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #938 audit invariant: a parser registered with
        # ``requires_raw_payload=True`` must not transition a row to
        # ``parsed`` while ``raw_status='absent'``. The worker
        # converts the outcome to a ``failed`` transition with a
        # descriptive error so the row remains auditable + retryable.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser_drops_raw(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="absent")

        register_parser("sec_form4", parser_drops_raw, requires_raw_payload=True)

        now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10, now=now)
        ebull_test_conn.commit()
        assert stats.parsed == 0
        assert stats.failed == 1
        assert stats.raw_payload_violations == 1

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "failed"
        assert row.error is not None
        assert "raw payload missing" in row.error
        # 1h backoff so the retry path eventually re-fires the parser.
        assert row.next_retry_at == datetime(2026, 1, 1, 13, 0, tzinfo=UTC)

    def test_payload_backed_parser_accepts_parsed_with_stored_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Same flag, valid raw_status -> normal parsed transition.
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser_persists_raw(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="stored")

        register_parser("sec_form4", parser_persists_raw, requires_raw_payload=True)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.raw_payload_violations == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "stored"

    def test_payload_backed_parser_accepts_parsed_when_row_already_has_stored_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex pre-push regression: ``transition_status(..., parsed,
        # raw_status=None)`` preserves the row's existing
        # ``raw_status`` column. The retry / rebuild flow may re-run a
        # parser whose raw body is already on disk — the parser
        # returns ``ParseOutcome(status='parsed', raw_status=None)``
        # because it has nothing new to write. The worker must check
        # the row's effective raw_status (``outcome.raw_status or
        # row.raw_status``), not just the outcome.
        from app.services.sec_manifest import transition_status

        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        # Pre-stamp ``raw_status='stored'`` while keeping
        # ``ingest_status='pending'`` so the worker picks the row up
        # via ``iter_pending`` AND finds existing raw evidence. Models
        # a rebuild flow: body stored on a prior pass, parsed reset to
        # pending for re-parse, parser doesn't restamp raw.
        transition_status(
            ebull_test_conn,
            "ACC-1",
            ingest_status="pending",
            raw_status="stored",
        )
        ebull_test_conn.commit()

        def parser_no_restamp(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v2", raw_status=None)

        register_parser("sec_form4", parser_no_restamp, requires_raw_payload=True)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.failed == 0
        assert stats.raw_payload_violations == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "stored"  # preserved across the parsed transition

    def test_payload_backed_parser_accepts_parsed_with_compacted_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # ``compacted`` is a valid post-storage state (raw bytes
        # written, then compacted into the per-quarter archive). The
        # invariant is "evidence on disk somewhere", not "literally
        # ``stored``".
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def parser_compacts_raw(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="compacted")

        register_parser("sec_form4", parser_compacts_raw, requires_raw_payload=True)

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.raw_payload_violations == 0

    def test_non_payload_parser_allows_parsed_with_absent_raw(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Default ``requires_raw_payload=False`` preserves backward
        # compatibility: synthesised / non-payload parsers can mark
        # rows ``parsed`` without a raw body. Used for sources where
        # the manifest row IS the truth (e.g. heartbeat-style entries).
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def synthesised_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed", parser_version="v1", raw_status="absent")

        register_parser("sec_form4", synthesised_parser)  # default flag = False

        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1
        assert stats.failed == 0
        assert stats.raw_payload_violations == 0

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "parsed"
        assert row.raw_status == "absent"

    def test_tombstoned_outcome_clears_retry(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        ebull_test_conn.commit()

        def tombstoning_parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="tombstoned", error="not on file")

        register_parser("sec_form4", tombstoning_parser)
        run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()

        row = get_manifest_row(ebull_test_conn, "ACC-1")
        assert row is not None
        assert row.ingest_status == "tombstoned"
        assert row.error == "not on file"
        assert row.next_retry_at is None


class TestSourceFilter:
    def test_source_filter_narrows_dispatch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-FORM4", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-DEF14A", source="sec_def14a")
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        register_parser("sec_def14a", parser)

        # Only drain form4 source
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1

        form4_row = get_manifest_row(ebull_test_conn, "ACC-FORM4")
        def14a_row = get_manifest_row(ebull_test_conn, "ACC-DEF14A")
        assert form4_row is not None
        assert def14a_row is not None
        assert form4_row.ingest_status == "parsed"
        assert def14a_row.ingest_status == "pending"

    def test_no_source_filter_drains_all(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_pending(ebull_test_conn, accession="ACC-FORM4", source="sec_form4")
        _seed_pending(ebull_test_conn, accession="ACC-DEF14A", source="sec_def14a")
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        register_parser("sec_def14a", parser)

        stats = run_manifest_worker(ebull_test_conn, source=None, max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 2


class TestRetryablePath:
    def test_failed_rows_past_retry_eligible(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.sec_manifest import transition_status

        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        # Manually mark failed with retry in past
        transition_status(
            ebull_test_conn,
            "ACC-1",
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 1

    def test_failed_rows_with_future_retry_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.sec_manifest import transition_status

        _seed_pending(ebull_test_conn, accession="ACC-1", source="sec_form4")
        transition_status(
            ebull_test_conn,
            "ACC-1",
            ingest_status="failed",
            error="x",
            next_retry_at=datetime(2099, 1, 1, tzinfo=UTC),
        )
        ebull_test_conn.commit()

        def parser(conn: psycopg.Connection, row: ManifestRow) -> ParseOutcome:
            return ParseOutcome(status="parsed")

        register_parser("sec_form4", parser)
        stats = run_manifest_worker(ebull_test_conn, source="sec_form4", max_rows=10)
        ebull_test_conn.commit()
        assert stats.parsed == 0
        assert stats.rows_processed == 0
