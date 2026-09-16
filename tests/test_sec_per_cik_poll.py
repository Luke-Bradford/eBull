"""Tests for per-CIK scheduled polling (#870)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_per_cik_poll import run_per_cik_poll
from app.services.data_freshness import (
    ciks_due_for_poll,
    get_freshness_row,
    record_poll_outcome,
)
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


def _seed_msft(conn: psycopg.Connection[tuple]) -> None:
    """A SECOND CIK, so a batching test can tell per-CIK from per-subject."""
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1702, 'MSFT', 'Microsoft', '4', 'USD', TRUE)
        """
    )
    conn.execute("INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1702, '0000789019')")
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


def _aapl_mixed_source_recent() -> dict:
    """One entity-wide response carrying TWO manifest sources.

    This is the shape the whole change turns on: SEC serves every form of a
    CIK in one document, so a poll for ``sec_8k`` and a poll for ``sec_form4``
    on the same CIK were fetching identical bytes twice.
    """
    return {
        "cik": "320193",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-26-000099", "0000320193-26-000098"],
                "filingDate": ["2026-04-30", "2026-04-29"],
                "form": ["8-K", "4"],
                "acceptanceDateTime": ["2026-04-30T16:00:00.000Z", "2026-04-29T16:00:00.000Z"],
                "primaryDocument": ["item502.htm", "ownership.xml"],
            },
            "files": [],
        },
    }


def _fake_get(status: int, payload: dict | bytes):
    body = json.dumps(payload).encode("utf-8") if isinstance(payload, dict) else payload

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return status, body

    return _impl


def _counting_get(status: int, payload: dict | bytes, urls: list[str]):
    """``_fake_get`` that records every URL it is asked for.

    The batching claim is a claim about FETCH COUNT, so it has to be asserted
    on the fetches themselves — a stats counter counts subjects and would show
    the same number either way.
    """
    body = json.dumps(payload).encode("utf-8") if isinstance(payload, dict) else payload

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        urls.append(url)
        return status, body

    return _impl


def _make_due(conn: psycopg.Connection[tuple], source: str, *, cik: str, instrument_id: int, subject_id: str) -> None:
    """Seed one 'current' scheduler row and force it past its deadline."""
    record_poll_outcome(
        conn,
        subject_type="issuer",
        subject_id=subject_id,
        source=source,  # type: ignore[arg-type]
        outcome="current",
        last_known_filing_id=f"SEED-{source}",
        last_known_filed_at=datetime(2025, 1, 1, tzinfo=UTC),
        cik=cik,
        instrument_id=instrument_id,
    )
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE data_freshness_index SET expected_next_at = '2024-01-01' WHERE source = %s AND subject_id = %s",
            (source, subject_id),
        )
    conn.commit()


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
        """``max_ciks=100`` should split ``poll=66, recheck=34``.

        The unit is CIKs since #3109; the 2/3-1/3 split itself is unchanged.
        """
        import app.jobs.sec_per_cik_poll as poll_mod

        captured: dict[str, int] = {}

        def _spy_poll(conn, *, source, limit, now=None):  # noqa: ARG001
            captured["poll"] = limit
            return []

        def _spy_recheck(conn, *, source, limit, now=None):  # noqa: ARG001
            captured["recheck"] = limit
            return []

        original_poll = poll_mod.ciks_due_for_poll
        original_recheck = poll_mod.ciks_due_for_recheck
        poll_mod.ciks_due_for_poll = _spy_poll  # type: ignore[assignment]
        poll_mod.ciks_due_for_recheck = _spy_recheck  # type: ignore[assignment]
        try:
            run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, {}), max_ciks=100)
        finally:
            poll_mod.ciks_due_for_poll = original_poll  # type: ignore[assignment]
            poll_mod.ciks_due_for_recheck = original_recheck  # type: ignore[assignment]

        # 100 * 2 // 3 = 66; 100 - 66 = 34
        assert captured == {"poll": 66, "recheck": 34}

    def test_budget_split_degenerate_max_ciks_1(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``max_ciks=1`` should produce ``poll=0, recheck=1``
        (no floor); total=1, never 2.
        """
        import app.jobs.sec_per_cik_poll as poll_mod

        poll_limits: list[int] = []
        recheck_limits: list[int] = []

        def _spy_poll(conn, *, source, limit, now=None):  # noqa: ARG001
            poll_limits.append(limit)
            return []

        def _spy_recheck(conn, *, source, limit, now=None):  # noqa: ARG001
            recheck_limits.append(limit)
            return []

        original_poll = poll_mod.ciks_due_for_poll
        original_recheck = poll_mod.ciks_due_for_recheck
        poll_mod.ciks_due_for_poll = _spy_poll  # type: ignore[assignment]
        poll_mod.ciks_due_for_recheck = _spy_recheck  # type: ignore[assignment]
        try:
            run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, {}), max_ciks=1)
        finally:
            poll_mod.ciks_due_for_poll = original_poll  # type: ignore[assignment]
            poll_mod.ciks_due_for_recheck = original_recheck  # type: ignore[assignment]

        # poll_budget=0 means the reader is SKIPPED entirely (not called
        # with limit=0). recheck_budget=1 means the reader IS called.
        assert poll_limits == []
        assert recheck_limits == [1]


class TestMultiSourceBatching:
    """#3109 — one fetch per CIK, not one per (subject, source) triple."""

    def test_two_due_sources_on_one_cik_cost_ONE_fetch(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The change, stated as an assertion on the wire.

        Before #3109 this was two identical GETs of the same entity-wide
        document, one of which threw away the 8-K and one the Form 4.
        """
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")

        urls: list[str] = []
        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get=_counting_get(200, _aapl_mixed_source_recent(), urls),
        )
        ebull_test_conn.commit()

        assert len(urls) == 1, f"expected one fetch for one CIK, got {urls}"
        assert stats.subjects_polled == 2
        # Each subject took ITS OWN source out of the shared response.
        assert stats.new_filings_recorded == 2
        eightk = get_manifest_row(ebull_test_conn, "0000320193-26-000099")
        form4 = get_manifest_row(ebull_test_conn, "0000320193-26-000098")
        assert eightk is not None and eightk.source == "sec_8k"
        assert form4 is not None and form4.source == "sec_form4"

    def test_each_subject_gets_its_own_source_filter_and_watermark(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A shared response must not leak one source's rows into another's
        scheduler row."""
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")

        run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, _aapl_mixed_source_recent()))
        ebull_test_conn.commit()

        eightk = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_8k")
        form4 = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_form4")
        assert eightk is not None and eightk.last_known_filing_id == "0000320193-26-000099"
        assert form4 is not None and form4.last_known_filing_id == "0000320193-26-000098"
        assert eightk.new_filings_since == 1
        assert form4.new_filings_since == 1

    def test_distinct_ciks_still_cost_one_fetch_EACH(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Guards against the batching collapsing across CIKs — the failure
        that would silently poll one entity and mark another current."""
        _seed_aapl(ebull_test_conn)
        _seed_msft(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_8k", cik="0000789019", instrument_id=1702, subject_id="1702")

        urls: list[str] = []
        run_per_cik_poll(ebull_test_conn, http_get=_counting_get(200, _aapl_submissions_recent(), urls))
        ebull_test_conn.commit()

        assert len(urls) == 2
        assert {u.rsplit("/", 1)[-1] for u in urls} == {"CIK0000320193.json", "CIK0000789019.json"}

    def test_a_fetch_failure_errors_EVERY_subject_of_that_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The accepted regression in failure granularity, pinned so it is a
        decision rather than a surprise."""
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")

        stats = run_per_cik_poll(ebull_test_conn, http_get=_fake_get(503, b""))
        ebull_test_conn.commit()

        assert stats.poll_errors == 2
        for source in ("sec_8k", "sec_form4"):
            row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source=source)
            assert row is not None and row.state == "error", source

    def test_source_scoping_still_selects_only_that_source(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``source=`` is applied BEFORE ranking, so a scoped call neither
        processes other sources nor spends its CIK budget on them."""
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")

        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(200, _aapl_mixed_source_recent()),
            source="sec_8k",
        )
        ebull_test_conn.commit()

        assert stats.subjects_polled == 1
        form4 = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_form4")
        assert form4 is not None
        assert form4.last_known_filing_id == "SEED-sec_form4", "unscoped source was processed"


class TestCikSelectorInvariants:
    """#3109 — the CIK-first selector's ranking and eligibility rules."""

    def test_finra_singleton_rows_are_never_selected(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """⚠⚠ The old ``cik is None`` guard never protected these.

        The FINRA universe rows carry the literal strings ``FINRA_REGSHO`` /
        ``FINRA_SI`` as their cik, not NULL — so they were eligible, and a
        poll would have fetched ``.../CIKFINRA_SI.json``, taken the 404
        branch, and written ``outcome='current'``. Asserted by row identity
        because this is a NARROWING gate: what it rejects is the contract.
        """
        record_poll_outcome(
            ebull_test_conn,
            subject_type="finra_universe",
            subject_id="FINRA_SI",
            source="finra_short_interest",
            outcome="current",
            last_known_filing_id="SEED",
            last_known_filed_at=datetime(2025, 1, 1, tzinfo=UTC),
            cik="FINRA_SI",
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01'")
        ebull_test_conn.commit()

        urls: list[str] = []
        stats = run_per_cik_poll(ebull_test_conn, http_get=_counting_get(404, b"", urls))
        ebull_test_conn.commit()

        assert urls == [], "a non-numeric cik reached the SEC fetch path"
        assert stats.subjects_polled == 0
        row = get_freshness_row(
            ebull_test_conn,
            subject_type="finra_universe",
            subject_id="FINRA_SI",
            source="finra_short_interest",
        )
        assert row is not None
        assert row.last_polled_outcome == "current", "the row was touched by a poll that cannot see it"

    def test_a_null_deadline_ranks_its_whole_cik_first(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """⚠⚠ ``MIN`` ignores NULL, so ranking a group on a bare
        ``MIN(expected_next_at)`` would push a CIK holding a NULL deadline
        BEHIND an all-NULL one — inverting the row readers' NULLS FIRST.
        The COALESCE-inside-MIN is what this pins.
        """
        _seed_aapl(ebull_test_conn)
        _seed_msft(ebull_test_conn)
        # AAPL: one NULL deadline + one recent one. MSFT: an old deadline.
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_8k", cik="0000789019", instrument_id=1702, subject_id="1702")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = NULL"
                " WHERE subject_id = '1701' AND source = 'sec_form4'"
            )
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = '2020-01-01'"
                " WHERE subject_id = '1701' AND source = 'sec_8k'"
            )
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2021-01-01' WHERE subject_id = '1702'")
        ebull_test_conn.commit()

        batches = ciks_due_for_poll(ebull_test_conn, limit=1)
        assert len(batches) == 1
        assert {r.cik for r in batches[0]} == {"0000320193"}
        # ...and the whole CIK comes with it, not just the NULL row.
        assert {r.source for r in batches[0]} == {"sec_8k", "sec_form4"}

    def test_budget_counts_ciks_not_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``limit=1`` returns ONE CIK with ALL its due rows — the unit change
        the 1.92x claim rests on."""
        _seed_aapl(ebull_test_conn)
        _seed_msft(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_10k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_8k", cik="0000789019", instrument_id=1702, subject_id="1702")

        batches = ciks_due_for_poll(ebull_test_conn, limit=1)
        assert len(batches) == 1
        assert len(batches[0]) == 3
        assert all(r.cik == batches[0][0].cik for r in batches[0])

    def test_batches_are_contiguous_and_single_cik(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        _seed_msft(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_8k", cik="0000789019", instrument_id=1702, subject_id="1702")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")

        batches = ciks_due_for_poll(ebull_test_conn, limit=10)
        assert [len({r.cik for r in b}) for b in batches] == [1] * len(batches)
        assert sum(len(b) for b in batches) == 3
        assert len(batches) == 2
