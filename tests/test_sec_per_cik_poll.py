"""Tests for per-CIK scheduled polling (#870)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_per_cik_poll import run_per_cik_poll
from app.services.data_freshness import get_freshness_row, record_poll_outcome
from app.services.sec_manifest import get_manifest_row
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def _seed_aapl(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
        """
    )
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1701, '0000320193')
        """
    )
    conn.commit()


def _aapl_submissions_recent() -> dict:
    return {
        "cik": "320193",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-26-000099"],
                "filingDate": ["2026-04-30"],
                "form": ["8-K"],
                "acceptanceDateTime": ["2026-04-30T16:00:00.000Z"],
                "primaryDocument": ["item502.htm"],
            },
            "files": [],
        },
    }


def _fake_get(status: int, payload: dict | bytes):
    body = json.dumps(payload).encode("utf-8") if isinstance(payload, dict) else payload

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return status, body

    return _impl


class TestPerCikPoll:
    def test_due_subject_polled_and_manifest_recorded(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        # Seed scheduler row past expected_next_at
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_8k",
            outcome="current",
            last_known_filing_id="0000320193-25-000001",
            last_known_filed_at=datetime(2025, 1, 1, tzinfo=UTC),
            cik="0000320193",
            instrument_id=1701,
        )
        # Force expected_next_at into the past
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01' WHERE source = 'sec_8k'")
        ebull_test_conn.commit()

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(200, _aapl_submissions_recent()),
            source="sec_8k",
        )
        ebull_test_conn.commit()

        assert stats.subjects_polled == 1
        assert stats.new_filings_recorded == 1

        # Verify manifest row exists
        row = get_manifest_row(ebull_test_conn, "0000320193-26-000099")
        assert row is not None
        assert row.source == "sec_8k"
        assert row.ingest_status == "pending"

        # Verify scheduler watermark advanced
        sched = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_8k")
        assert sched is not None
        assert sched.last_known_filing_id == "0000320193-26-000099"
        assert sched.last_polled_outcome == "new_data"

    def test_no_new_data_advances_expected_next_at(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_8k",
            outcome="current",
            last_known_filing_id="0000320193-26-000099",
            last_known_filed_at=datetime(2026, 4, 30, tzinfo=UTC),
            cik="0000320193",
            instrument_id=1701,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01' WHERE source = 'sec_8k'")
        ebull_test_conn.commit()

        # Same recent payload — watermark already at top, so no new
        run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(200, _aapl_submissions_recent()),
            source="sec_8k",
        )
        ebull_test_conn.commit()

        sched = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_8k")
        assert sched is not None
        assert sched.last_polled_outcome == "current"
        # expected_next_at should now be in the future (NOT 2024)
        assert sched.expected_next_at is not None
        assert sched.expected_next_at > datetime(2025, 1, 1, tzinfo=UTC)

    def test_poll_error_records_error_outcome(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_8k",
            outcome="current",
            last_known_filing_id="ACC-x",
            last_known_filed_at=datetime(2026, 1, 1, tzinfo=UTC),
            cik="0000320193",
            instrument_id=1701,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01' WHERE source = 'sec_8k'")
        ebull_test_conn.commit()

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(503, b""),
            source="sec_8k",
        )
        ebull_test_conn.commit()

        assert stats.poll_errors == 1
        sched = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_8k")
        assert sched is not None
        assert sched.state == "error"


class TestG13RecheckPath:
    """#1155 G13 — recheck reader path (never_filed / error rows)."""

    def test_never_filed_stays_in_recheck_queue_after_empty_poll(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """#1155 G13 — Codex round 2 finding: a 'never_filed' row that
        polls successfully with no new filings must STAY 'never_filed'
        with an advanced next_recheck_at, NOT transition to 'current'.
        Otherwise the recheck path is defeated on the first poll.
        """
        _seed_aapl(ebull_test_conn)
        # Seed a never_filed row past its next_recheck_at
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_def14a",
            outcome="never",
            cik="0000320193",
            instrument_id=1701,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE data_freshness_index SET next_recheck_at = '2024-01-01' WHERE source = 'sec_def14a'")
        ebull_test_conn.commit()

        # Payload returns a single 8-K — NOT a def14a accession, so the
        # source filter at check_freshness sees no new sec_def14a rows.
        run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(200, _aapl_submissions_recent()),
            source="sec_def14a",
        )
        ebull_test_conn.commit()

        sched = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_def14a")
        assert sched is not None
        # CRITICAL: state stays 'never_filed', does NOT transition to 'current'
        assert sched.state == "never_filed"
        assert sched.last_polled_outcome == "never"
        # next_recheck_at advanced to a future time
        assert sched.next_recheck_at is not None
        assert sched.next_recheck_at > datetime(2025, 1, 1, tzinfo=UTC)

    def test_recheck_subject_drained_alongside_poll(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Seed one 'error'-state subject + one 'current' subject past
        expected_next_at. Both should be probed in one tick; stats
        differentiate via the recheck_* counters.
        """
        _seed_aapl(ebull_test_conn)

        # Subject 1: 'current' state past expected_next_at (poll path)
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_8k",
            outcome="current",
            last_known_filing_id="0000320193-25-000001",
            last_known_filed_at=datetime(2025, 1, 1, tzinfo=UTC),
            cik="0000320193",
            instrument_id=1701,
        )
        # Subject 2: 'error' state past next_recheck_at (recheck path)
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_def14a",
            outcome="error",
            error="prior 503",
            cik="0000320193",
            instrument_id=1701,
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01' WHERE source = 'sec_8k'")
            cur.execute("UPDATE data_freshness_index SET next_recheck_at = '2024-01-01' WHERE source = 'sec_def14a'")
        ebull_test_conn.commit()

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(200, _aapl_submissions_recent()),
            # No source filter — both readers see their respective rows
        )
        ebull_test_conn.commit()

        # Poll path probed sec_8k subject
        assert stats.subjects_polled == 1
        # Recheck path probed sec_def14a subject
        assert stats.recheck_subjects_polled == 1
        # Total error count is 0 — both probes returned 200
        assert stats.poll_errors == 0

    def test_budget_split_2_3_poll_1_3_recheck(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``max_subjects=100`` should split ``poll=66, recheck=34``.
        Verified by passing distinct iterators via monkeypatch.
        """
        import app.jobs.sec_per_cik_poll as poll_mod

        captured: dict[str, int] = {}

        def _spy_poll(conn, *, source, limit, now=None):  # noqa: ARG001
            captured["poll"] = limit
            return iter([])

        def _spy_recheck(conn, *, source, limit, now=None):  # noqa: ARG001
            captured["recheck"] = limit
            return iter([])

        original_poll = poll_mod.subjects_due_for_poll
        original_recheck = poll_mod.subjects_due_for_recheck
        poll_mod.subjects_due_for_poll = _spy_poll  # type: ignore[assignment]
        poll_mod.subjects_due_for_recheck = _spy_recheck  # type: ignore[assignment]
        try:
            run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, {}), max_subjects=100)
        finally:
            poll_mod.subjects_due_for_poll = original_poll  # type: ignore[assignment]
            poll_mod.subjects_due_for_recheck = original_recheck  # type: ignore[assignment]

        # 100 * 2 // 3 = 66; 100 - 66 = 34
        assert captured == {"poll": 66, "recheck": 34}

    def test_budget_split_degenerate_max_subjects_1(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``max_subjects=1`` should produce ``poll=0, recheck=1``
        (no floor); total=1, never 2.
        """
        import app.jobs.sec_per_cik_poll as poll_mod

        poll_limits: list[int] = []
        recheck_limits: list[int] = []

        def _spy_poll(conn, *, source, limit, now=None):  # noqa: ARG001
            poll_limits.append(limit)
            return iter([])

        def _spy_recheck(conn, *, source, limit, now=None):  # noqa: ARG001
            recheck_limits.append(limit)
            return iter([])

        original_poll = poll_mod.subjects_due_for_poll
        original_recheck = poll_mod.subjects_due_for_recheck
        poll_mod.subjects_due_for_poll = _spy_poll  # type: ignore[assignment]
        poll_mod.subjects_due_for_recheck = _spy_recheck  # type: ignore[assignment]
        try:
            run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, {}), max_subjects=1)
        finally:
            poll_mod.subjects_due_for_poll = original_poll  # type: ignore[assignment]
            poll_mod.subjects_due_for_recheck = original_recheck  # type: ignore[assignment]

        # poll_budget=0 means the reader is SKIPPED entirely (not called
        # with limit=0). recheck_budget=1 means the reader IS called.
        assert poll_limits == []
        assert recheck_limits == [1]
