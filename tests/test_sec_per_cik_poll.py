"""Tests for per-CIK scheduled polling (#870)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_per_cik_poll import run_per_cik_poll
from app.services.data_freshness import (
    FreshnessRow,
    ciks_due_for_poll,
    get_freshness_row,
    record_poll_outcome,
)
from app.services.sec_manifest import get_manifest_row
from app.services.watermarks import get_watermark, set_watermark
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def _freshness_row(*, cik: str, source: str = "sec_8k", state: str = "error") -> FreshnessRow:
    """A minimal in-memory scheduler row, for tests that only need the SHAPE.

    Used where a lane reader is stubbed out and the job under test just counts
    what came back — no DB round-trip, so nothing here is a claim about what
    the schema would store.

    ⚠ ``institutional_filer``, not ``issuer``: the job DOES write an outcome
    for whatever a stubbed reader hands it, and an issuer row carries an
    ``instrument_id`` FK to ``instruments``. A synthetic issuer row therefore
    fails on the foreign key rather than on the thing under test.
    """
    now = datetime.now(tz=UTC)
    return FreshnessRow(
        subject_type="institutional_filer",
        subject_id=cik,
        source=source,  # type: ignore[arg-type]
        cik=cik.zfill(10),
        instrument_id=None,
        last_known_filing_id=None,
        last_known_filed_at=None,
        last_polled_at=None,
        last_polled_outcome="never",
        new_filings_since=0,
        expected_next_at=None,
        next_recheck_at=None,
        next_poll_at=now,
        state=state,  # type: ignore[arg-type]
    )


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
            "UPDATE data_freshness_index SET expected_next_at = '2024-01-01',"
            " next_poll_at = '2024-01-01' WHERE source = %s AND subject_id = %s",
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
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = '2024-01-01',"
                " next_poll_at = '2024-01-01' WHERE source = 'sec_8k'"
            )
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
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = '2024-01-01',"
                " next_poll_at = '2024-01-01' WHERE source = 'sec_8k'"
            )
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
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = '2024-01-01',"
                " next_poll_at = '2024-01-01' WHERE source = 'sec_8k'"
            )
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
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = '2024-01-01',"
                " next_poll_at = '2024-01-01' WHERE source = 'sec_8k'"
            )
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
        """``max_ciks=100`` with an EMPTY recheck lane gives poll the lot.

        The unit is CIKs since #3109. The 2/3-1/3 split survives as the
        recheck lane's CAP, but it is selected FIRST and the poll lane takes
        the residual — so an empty recheck lane (its state on dev: 0 rows)
        yields ``poll=100`` rather than burning 34 slots on nothing.

        ⚠ The spies here return ``[]``, which IS the empty-recheck case. The
        recheck-full case is a separate test below; asserting only this one
        would let a regression that ignores ``len(recheck_due)`` pass.
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

        # recheck cap = 100 - (100 * 2 // 3) = 34; it returned nothing,
        # so the poll lane gets the whole 100.
        assert captured == {"poll": 100, "recheck": 34}

    def test_budget_rollover_recheck_full_preserves_g13_split(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A FULL recheck lane still reserves its #1155 G13 share.

        The rollover must not be able to cannibalise the guaranteed recheck
        throughput — it only reclaims what that lane leaves unspent. With 34
        recheck CIKs returned, the poll lane gets exactly the historical 66.
        """
        import app.jobs.sec_per_cik_poll as poll_mod

        captured: dict[str, int] = {}
        # 34 single-row batches: the recheck lane spending its whole cap.
        full_recheck = [[_freshness_row(cik=str(900000 + i))] for i in range(34)]

        def _spy_poll(conn, *, source, limit, now=None):  # noqa: ARG001
            captured["poll"] = limit
            return []

        def _spy_recheck(conn, *, source, limit, now=None):  # noqa: ARG001
            captured["recheck"] = limit
            return full_recheck

        original_poll = poll_mod.ciks_due_for_poll
        original_recheck = poll_mod.ciks_due_for_recheck
        poll_mod.ciks_due_for_poll = _spy_poll  # type: ignore[assignment]
        poll_mod.ciks_due_for_recheck = _spy_recheck  # type: ignore[assignment]
        try:
            run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, {}), max_ciks=100)
        finally:
            poll_mod.ciks_due_for_poll = original_poll  # type: ignore[assignment]
            poll_mod.ciks_due_for_recheck = original_recheck  # type: ignore[assignment]

        assert captured == {"poll": 66, "recheck": 34}

    def test_budget_split_degenerate_max_ciks_1(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``max_ciks=1`` stays bounded at a total of 1 fetch, never 2.

        recheck cap = 1 - (1 * 2 // 3) = 1. The spy returns nothing, so the
        rollover hands the residual 1 to the poll lane. Before #3109 the poll
        lane was hard-wired to ``1 * 2 // 3 = 0`` and was skipped entirely;
        the important invariant — poll + recheck never exceeds ``max_ciks`` —
        is unchanged.
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

        assert recheck_limits == [1]
        assert poll_limits == [1]
        # The invariant that actually matters: total budget is never exceeded.
        assert sum(recheck_limits) + sum(poll_limits) <= 1 + 1  # cap + residual, one fetch each at most

    def test_budget_degenerate_max_ciks_zero_polls_nothing(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``max_ciks=0`` must select nothing at all, in either lane.

        The rollover computes ``poll_budget = max_ciks - len(recheck_due)``,
        which is 0 here — the guard is ``> 0``, so neither reader is called.
        A negative budget is unreachable for the same reason: ``len(recheck_due)``
        is capped by the limit passed to it.
        """
        import app.jobs.sec_per_cik_poll as poll_mod

        calls: list[str] = []

        def _spy_poll(conn, *, source, limit, now=None):  # noqa: ARG001
            calls.append("poll")
            return []

        def _spy_recheck(conn, *, source, limit, now=None):  # noqa: ARG001
            calls.append("recheck")
            return []

        original_poll = poll_mod.ciks_due_for_poll
        original_recheck = poll_mod.ciks_due_for_recheck
        poll_mod.ciks_due_for_poll = _spy_poll  # type: ignore[assignment]
        poll_mod.ciks_due_for_recheck = _spy_recheck  # type: ignore[assignment]
        try:
            run_per_cik_poll(ebull_test_conn, http_get=_fake_get(200, {}), max_ciks=0)
        finally:
            poll_mod.ciks_due_for_poll = original_poll  # type: ignore[assignment]
            poll_mod.ciks_due_for_recheck = original_recheck  # type: ignore[assignment]

        assert calls == []


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
            cur.execute("UPDATE data_freshness_index SET expected_next_at = '2024-01-01', next_poll_at = '2024-01-01'")
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

    def test_a_cik_whose_deadlines_are_all_null_ranks_FIRST(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """⚠⚠ Two NULL traps compose here, and the naive query hits both.

        ``MIN`` ignores NULLs, so a group whose deadlines are ALL NULL gets a
        NULL rank key — and ``ORDER BY key ASC`` defaults to NULLS **LAST**.
        So the most urgent CIK (never polled, no prediction) would sort behind
        every dated one, exactly inverting the row readers' explicit
        ``NULLS FIRST``. Folding the NULL into the key with
        ``COALESCE(..., '-infinity')`` fixes both at once.

        ⚠ An earlier version of this test gave the NULL CIK a second, OLD
        deadline as well — which does NOT discriminate, because ``MIN`` then
        returns that old date and the CIK ranks first either way. The probe
        (bare ``MIN``) survived it.
        """
        _seed_aapl(ebull_test_conn)
        _seed_msft(ebull_test_conn)
        # AAPL: deadlines ALL NULL. MSFT: an old, dated deadline.
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_8k", cik="0000789019", instrument_id=1702, subject_id="1702")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = NULL,"
                " next_poll_at = '2019-01-01' WHERE subject_id = '1701'"
            )
            cur.execute(
                "UPDATE data_freshness_index SET expected_next_at = '2020-01-01',"
                " next_poll_at = '2020-01-01' WHERE subject_id = '1702'"
            )
        ebull_test_conn.commit()

        batches = ciks_due_for_poll(ebull_test_conn, limit=1)
        assert len(batches) == 1
        assert {r.cik for r in batches[0]} == {"0000320193"}, "a dated CIK outranked an unpolled one"
        # ...and the whole CIK comes with it, not just one of its rows.
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


def _fake_get_with_meta(status: int, payload: dict | bytes, last_modified: str | None, seen: list[dict]):
    body = json.dumps(payload).encode("utf-8") if isinstance(payload, dict) else payload

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes, str | None]:
        seen.append(dict(headers))
        return status, body, last_modified

    return _impl


class TestConditionalGetGuards:
    """#3109 — the two ways a response could certify a subject nobody read.

    ⚠ This whole path is inert in production: #3110 measured that
    ``data.sec.gov`` returns no ``Last-Modified`` on these responses, and no
    validator has ever been stored. It is kept correct because correctness
    does not depend on whether it fires.
    """

    def test_unsolicited_304_is_an_error_not_a_success(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """No ``If-Modified-Since`` was sent, so a 304 cannot mean "unchanged
        since your watermark" — there was no watermark. Treating it as success
        would mark every batched subject ``current`` off a payload nobody read.
        """
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")

        seen: list[dict] = []
        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=_fake_get_with_meta(304, b"", None, seen),
        )
        ebull_test_conn.commit()

        assert seen and "If-Modified-Since" not in seen[0], "test setup sent a conditional request"
        assert stats.poll_errors == 1
        row = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_8k")
        assert row is not None
        assert row.state == "error", "an unsolicited 304 certified the subject as current"

    def test_a_404_does_not_persist_a_validator(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The validator write is gated on "every discovered filing was
        recorded", which on a 404 is trivially true at ``0 == 0``. Carrying the
        header off a "no such CIK" response let it install a validator that the
        next tick would send as ``If-Modified-Since``.
        """
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")

        seen: list[dict] = []
        run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=_fake_get_with_meta(404, b"", "Wed, 01 Apr 2026 00:00:00 GMT", seen),
        )
        ebull_test_conn.commit()

        stored = get_watermark(ebull_test_conn, "sec.last_modified.per_cik_poll", "0000320193:sec_8k")
        assert stored is None or stored.watermark is None, f"a 404 stored a validator: {stored}"

    def test_a_multi_subject_batch_never_sends_if_modified_since(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """⚠⚠ The ``<cik>:<source>`` watermark key is SHARED by sibling
        subjects (963 duplicate ``(cik, source)`` pairs measured 2026-09-16),
        so no batch-level agreement check can establish that each batched
        subject was processed at that validator. Only a single-subject batch —
        which is byte-identical to the pre-batching probe — may send one.
        """
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")
        # Plant a validator that a single-subject batch WOULD have sent.
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source="sec.last_modified.per_cik_poll",
                key="0000320193:sec_8k",
                watermark="Wed, 01 Apr 2026 00:00:00 GMT",
                watermark_at=None,
            )
        ebull_test_conn.commit()

        seen: list[dict] = []
        run_per_cik_poll(
            ebull_test_conn,
            http_get_with_meta=_fake_get_with_meta(200, _aapl_mixed_source_recent(), None, seen),
        )
        ebull_test_conn.commit()

        assert len(seen) == 1, "batching collapsed or split unexpectedly"
        assert "If-Modified-Since" not in seen[0], "a multi-subject batch sent a shared-key validator"


class TestMixedPaddingCik:
    """#3109 — padding variants of one CIK must not abort the run.

    ``data_freshness_index.cik`` has no format constraint, so ``320193`` and
    ``0000320193`` can both exist. They address the SAME submissions URL, so
    the selector groups them — but if each row kept its stored string the
    batch would disagree with itself and ``_probe_cik``'s one-CIK assertion
    would abort the whole poll before anything was fetched.

    Measured 0 variants on the dev corpus 2026-09-16 — latent, not live.
    Found by Codex checkpoint 2.
    """

    def test_padding_variants_form_one_batch_and_do_not_abort(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_form4", cik="0000320193", instrument_id=1701, subject_id="1701")
        # Strip the padding off ONE of them, behind the scheduler's back.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE data_freshness_index SET cik = '320193' WHERE subject_id = '1701' AND source = 'sec_form4'"
            )
        ebull_test_conn.commit()

        batches = ciks_due_for_poll(ebull_test_conn, limit=10)
        assert len(batches) == 1, "padding variants split into separate batches"
        assert {r.cik for r in batches[0]} == {"0000320193"}, "a row kept its unpadded cik"

        urls: list[str] = []
        stats = run_per_cik_poll(
            ebull_test_conn,
            http_get=_counting_get(200, _aapl_mixed_source_recent(), urls),
        )
        ebull_test_conn.commit()

        assert len(urls) == 1
        assert urls[0].endswith("CIK0000320193.json")
        assert stats.subjects_polled == 2
        assert stats.poll_errors == 0


class TestPollRotation:
    """#3109 slices 1-3 — the queue must ROTATE, not re-serve the same head.

    Before this, eligibility was keyed on ``expected_next_at`` — a deadline
    derived from ``last_known_filed_at``. A filer last seen in 1994 therefore
    had a permanently-elapsed deadline, won ``ORDER BY ... ASC`` on every
    hourly tick, and starved the rest: measured on dev 2026-09-16, **95 of
    55,775 rows had ever been polled** since 2026-06-05, and re-running the
    live selector returned every CIK the previous run had just polled
    (``polled_only = 0``).
    """

    def test_two_cycles_reach_DIFFERENT_ciks(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """The headline acceptance criterion, with the population ABOVE budget.

        ⚠ Disjointness alone is not the assertion. An empty second run is
        trivially disjoint from the first, so this asserts run 2 is NON-EMPTY
        as well — that pairing is the whole test. Budget is 1 CIK per lane
        against 2 due CIKs, so a non-rotating queue re-serves CIK 1 twice and
        the disjointness assertion fires.
        """
        _seed_aapl(ebull_test_conn)
        _seed_msft(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")
        _make_due(ebull_test_conn, "sec_8k", cik="0000789019", instrument_id=1702, subject_id="1702")

        first: list[str] = []
        run_per_cik_poll(
            ebull_test_conn,
            http_get=_counting_get(200, _aapl_submissions_recent(), first),
            max_ciks=1,
        )
        ebull_test_conn.commit()

        second: list[str] = []
        run_per_cik_poll(
            ebull_test_conn,
            http_get=_counting_get(200, _aapl_submissions_recent(), second),
            max_ciks=1,
        )
        ebull_test_conn.commit()

        assert len(first) == 1, "budget of 1 CIK should buy exactly one fetch"
        assert len(second) == 1, "second cycle reached NOTHING — the queue drained to empty"
        assert first != second, "second cycle re-served the same CIK: the head is still pinned"

    def test_polled_row_is_EXCLUDED_not_merely_reordered(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A 1994-dated filer must leave the queue after one poll.

        This is the ticket's defect in its narrowest form: the row's
        ``expected_next_at`` stays in 1994 (correctly — that IS when it last
        filed), but it must no longer be selectable.
        """
        _seed_aapl(ebull_test_conn)
        record_poll_outcome(
            ebull_test_conn,
            subject_type="issuer",
            subject_id="1701",
            source="sec_8k",
            outcome="current",
            last_known_filing_id="ANCIENT",
            last_known_filed_at=datetime(1994, 3, 30, tzinfo=UTC),
            cik="0000320193",
            instrument_id=1701,
        )
        ebull_test_conn.commit()

        # Precondition: the derived deadline really is still in the past.
        sched = get_freshness_row(ebull_test_conn, subject_type="issuer", subject_id="1701", source="sec_8k")
        assert sched is not None
        assert sched.expected_next_at is not None
        assert sched.expected_next_at < datetime.now(tz=UTC), "test premise broken: row is not overdue"

        # ...and yet it is NOT eligible, because it was just polled.
        assert ciks_due_for_poll(ebull_test_conn, limit=10) == []
        assert sched.next_poll_at > datetime.now(tz=UTC)

    def test_200_empty_advances_eligibility(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A poll that discovers nothing still spent a request, so it must pay.

        ⚠ Read back through SQL, not from the in-memory object the writer
        returned — the claim is about what PERSISTS.
        """
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")

        run_per_cik_poll(
            ebull_test_conn,
            http_get=_fake_get(200, {"cik": "320193", "filings": {"recent": {}, "files": []}}),
            source="sec_8k",
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT next_poll_at > now() FROM data_freshness_index WHERE subject_id = '1701' AND source = 'sec_8k'"
            )
            row = cur.fetchone()
        assert row == (True,), "an empty 200 left the row immediately due again"

    def test_error_gets_a_FINITE_recheck_deadline(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A failed poll must not pin the recheck lane (#3109).

        ``_record_subject_error`` supplied no ``next_recheck_at``, so the
        UPSERT wrote NULL — and ``ciks_due_for_recheck`` treats NULL as
        immediately due. That is the identical starvation defect in the other
        lane, and it is why the poll lane's budget rollover could not assume
        the recheck lane ever empties.
        """
        _seed_aapl(ebull_test_conn)
        _make_due(ebull_test_conn, "sec_8k", cik="0000320193", instrument_id=1701, subject_id="1701")

        run_per_cik_poll(ebull_test_conn, http_get=_fake_get(500, b"boom"), source="sec_8k")
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT state, next_recheck_at IS NULL, next_recheck_at > now()"
                " FROM data_freshness_index WHERE subject_id = '1701' AND source = 'sec_8k'"
            )
            row = cur.fetchone()
        assert row == ("error", False, True), "errored row is still immediately due forever"
