"""#3518 — ``periodic_report_sections`` against real SQL: every sql/441 CHECK and trigger, the selector's target
rule on seeded events, a run end to end, and per-accession atomicity (spec §3, §4.1, §7)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg
import pytest
from psycopg import sql

from app.services import periodic_report_sections as prs
from app.services.mdna_extraction import ParseOutcome, extractor_id
from app.services.periodic_report_sections import (
    AccessionWork,
    RowOutcome,
    SectionsRunResult,
    Target,
    run_periodic_report_sections,
    select_worklist,
    write_accession_rows,
)
from scripts.invalidate_periodic_report_section import invalidate

Conn = psycopg.Connection[Any]
SHA = "a" * 64
FAR = datetime(2020, 1, 1, tzinfo=UTC)


def _instrument(conn: Conn, iid: int, *, tradable: bool = True, exchange: str = "us3518") -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange) "
        "VALUES (%s, %s, %s, %s, %s)",
        (iid, f"S{iid}", f"S{iid} Inc", tradable, exchange),
    )


def _exchanges(conn: Conn) -> None:
    conn.execute(
        "INSERT INTO exchanges (exchange_id, asset_class) VALUES ('us3518', 'us_equity'), ('xx3518', 'crypto')"
    )


def _insert(conn: Conn, **overrides: Any) -> int:
    row: dict[str, Any] = {
        "instrument_id": 1,
        "accession_number": "0000000001-26-000001",
        "section_id": "10-Q:Part I, Item 2",
        "extractor": "ext",
        "status": "extracted",
        "body": "Discussion and Analysis",
        "full_chars": 23,
        "detail": None,
        "retryable": False,
        "invalidates_row_id": None,
        "source_url": "https://www.sec.gov/x.htm",
        "source_text_sha256": SHA,
        "source_chars": 100,
    }
    row.update(overrides)
    query = sql.SQL("INSERT INTO periodic_report_sections ({cols}) VALUES ({vals}) RETURNING row_id").format(
        cols=sql.SQL(", ").join(sql.Identifier(k) for k in row),
        vals=sql.SQL(", ").join(sql.Placeholder(k) for k in row),
    )
    got = conn.execute(query, row).fetchone()
    assert got is not None
    return int(got[0])


# --- CHECKs ----------------------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "overrides",
    [
        {"section_id": "10-K:Item 1"},
        {"status": "unknown"},
        {"body": None},  # extracted without a body
        {"body": "   "},
        {"full_chars": None},
        {"full_chars": 0},
        {"detail": "x"},  # detail on an extracted row
        {"retryable": True},  # retryable extracted row
        {"source_url": None},
        {"source_text_sha256": None, "source_chars": None},  # extracted without provenance
        {"source_text_sha256": "ABC"},
        {"source_chars": -1},
        {"source_text_sha256": None},  # hash and chars travel together
        {"status": "item_absent", "detail": "none"},  # body / full_chars on a non-extracted row
        {"status": "fetch_failed", "body": None, "full_chars": None, "detail": None},  # failure without detail
        {"status": "item_absent", "body": None, "full_chars": None, "detail": "none", "retryable": True},
        {
            "status": "fetch_failed",
            "body": None,
            "full_chars": None,
            "detail": "empty",
            "source_url": None,
            "source_text_sha256": None,
            "source_chars": None,
        },  # a URL is required unless no_url
        {"status": "item_absent", "body": None, "full_chars": None, "detail": "none", "invalidates_row_id": 1},
    ],
)
def test_checks_reject(ebull_test_conn: Conn, overrides: dict[str, Any]) -> None:
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    with pytest.raises((psycopg.errors.CheckViolation, psycopg.errors.ForeignKeyViolation)):
        _insert(ebull_test_conn, **overrides)


def test_valid_failure_rows_are_accepted(ebull_test_conn: Conn) -> None:
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    none = {"body": None, "full_chars": None, "source_text_sha256": None, "source_chars": None}
    _insert(ebull_test_conn, status="fetch_failed", detail="no_url", retryable=True, **{**none, "source_url": None})
    _insert(ebull_test_conn, status="fetch_failed", detail="HTTPStatusError:503", retryable=True, **none)
    _insert(ebull_test_conn, status="parse_failed", detail="caption_absent", body=None, full_chars=None)
    _insert(ebull_test_conn, status="item_absent", detail="none", body=None, full_chars=None)


# --- append-only + invalidation --------------------------------------------------------------------------------
def test_update_delete_truncate_are_refused(ebull_test_conn: Conn) -> None:
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    rid = _insert(ebull_test_conn)
    ebull_test_conn.commit()
    for stmt in (
        "UPDATE periodic_report_sections SET extractor = 'y'",
        "DELETE FROM periodic_report_sections",
        "TRUNCATE periodic_report_sections",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            ebull_test_conn.execute(stmt)
        ebull_test_conn.rollback()
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM periodic_report_sections WHERE row_id = %s", (rid,)
    ).fetchone() == (1,)


def test_fetched_at_is_the_insert_clock_whatever_the_writer_supplies(ebull_test_conn: Conn) -> None:
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    rid = _insert(ebull_test_conn, fetched_at=FAR)
    got = ebull_test_conn.execute(
        "SELECT fetched_at > now() - interval '1 minute' FROM periodic_report_sections WHERE row_id = %s", (rid,)
    ).fetchone()
    assert got == (True,)


def test_invalidation_integrity(ebull_test_conn: Conn) -> None:
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    _instrument(ebull_test_conn, 2)
    target = _insert(ebull_test_conn)
    other = _insert(ebull_test_conn, instrument_id=2)
    ebull_test_conn.commit()
    inv: dict[str, Any] = {
        "status": "invalidated",
        "body": None,
        "full_chars": None,
        "detail": "reason",
        "source_url": None,
        "source_text_sha256": None,
        "source_chars": None,
    }
    for bad, match in (
        ({"invalidates_row_id": other}, "different identity"),
        ({"invalidates_row_id": target, "extractor": "other"}, "different identity"),
    ):
        with pytest.raises(psycopg.errors.RaiseException, match=match):
            _insert(ebull_test_conn, **{**inv, **bad})
        ebull_test_conn.rollback()
    first = invalidate(ebull_test_conn, target, "  transient error page  ")
    ebull_test_conn.commit()
    assert first["invalidates_row_id"] == target
    detail = ebull_test_conn.execute(
        "SELECT detail FROM periodic_report_sections WHERE row_id = %s", (first["row_id"],)
    ).fetchone()
    assert detail == ("transient error page",)
    with pytest.raises(psycopg.errors.UniqueViolation):
        invalidate(ebull_test_conn, target, "again")
    ebull_test_conn.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="itself an invalidation"):
        invalidate(ebull_test_conn, first["row_id"], "chain")
    ebull_test_conn.rollback()
    with pytest.raises(LookupError):
        invalidate(ebull_test_conn, 999_999, "missing")
    ebull_test_conn.rollback()
    # self-reference: the identity value is assigned before the BEFORE trigger runs
    nxt = ebull_test_conn.execute("SELECT pg_get_serial_sequence('periodic_report_sections', 'row_id')").fetchone()
    assert nxt is not None
    seq = ebull_test_conn.execute(
        sql.SQL("SELECT last_value + 1 FROM {}").format(sql.Identifier(*str(nxt[0]).split(".")))
    ).fetchone()
    assert seq is not None
    with pytest.raises(psycopg.errors.RaiseException, match="cannot invalidate itself"):
        _insert(ebull_test_conn, **{**inv, "invalidates_row_id": int(seq[0])})


# --- selector --------------------------------------------------------------------------------------------------
def _event(
    conn: Conn,
    iid: int,
    acc: str,
    form: str,
    report: date | None,
    *,
    filed: date = date(2026, 8, 1),
    created: datetime | None = None,
    provider: str = "sec",
    url: str | None = "default",
) -> None:
    conn.execute(
        "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, "
        "report_date, primary_document_url, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            iid,
            filed,
            form,
            provider,
            acc,
            report,
            f"https://www.sec.gov/{acc}.htm" if url == "default" else url,
            created or datetime(2026, 8, 1, tzinfo=UTC),
        ),
    )


def _seed_world(conn: Conn) -> None:
    _exchanges(conn)
    for iid in range(1, 11):
        _instrument(conn, iid, tradable=iid != 5, exchange="xx3518" if iid == 9 else "us3518")
    q2, q1 = date(2026, 6, 30), date(2026, 3, 31)
    _event(conn, 1, "A-Q2", "10-Q", q2)  # target
    _event(conn, 1, "A-K", "10-K", date(2025, 12, 31))
    _event(conn, 2, "B-Q2", "10-Q", q2)  # two original rows for one accession -> excluded
    _event(conn, 2, "B-Q2", "10-QT", q2, provider="sec_alt")
    _event(conn, 2, "B-Q1", "10-Q", q1)  # -> target
    _event(conn, 3, "C-Q2A", "10-Q/A", q2)  # amendment: never a target
    _event(conn, 3, "C-Q1", "10-Q", q1)  # -> target
    _event(conn, 4, "D-Q2", "10-Q", q2, created=datetime(2099, 1, 1, tzinfo=UTC))  # created after as_of
    _event(conn, 4, "D-Q1", "10-KT", q1)  # -> target (transition report, 10-K family)
    _event(conn, 5, "E-Q2", "10-Q", q2)  # not tradable: out of scope
    # 6: in scope, no periodic event -> no_target
    _event(conn, 7, "G-FUT", "10-Q", date(2099, 3, 31))  # report date after today: never a target
    _event(conn, 8, "H-Q2", "10-Q", q2, filed=date(2026, 8, 5))  # shared accession, consistent
    _event(conn, 10, "H-Q2", "10-Q", q2, filed=date(2026, 8, 5))
    _event(conn, 9, "I-Q2", "10-Q", q2)  # non-US-equity exchange: out of scope
    _event(conn, 1, "A-Q2-8K", "8-K", q2)  # not a periodic form


def test_selector_applies_the_target_rule(ebull_test_conn: Conn) -> None:
    _seed_world(ebull_test_conn)
    ebull_test_conn.commit()
    result = SectionsRunResult(extractor="ext")
    wl = select_worklist(ebull_test_conn, "ext", result)
    got = {t.instrument_id: (w.accession, t.section_id) for w in wl.work for t in w.due}
    assert got == {
        1: ("A-Q2", "10-Q:Part I, Item 2"),
        2: ("B-Q1", "10-Q:Part I, Item 2"),
        3: ("C-Q1", "10-Q:Part I, Item 2"),
        4: ("D-Q1", "10-K:Item 7"),
        8: ("H-Q2", "10-Q:Part I, Item 2"),
        10: ("H-Q2", "10-Q:Part I, Item 2"),
    }
    assert (result.in_scope, result.no_target, result.due, result.accessions_due) == (8, 2, 6, 5)
    assert wl.work[0].accession == "H-Q2"  # newest filing first


def test_run_end_to_end_then_nothing_is_due(ebull_test_conn: Conn) -> None:
    _seed_world(ebull_test_conn)
    ebull_test_conn.commit()

    class Source:
        def fetch_document_text(self, absolute_url: str) -> str | None:
            if "B-Q1" in absolute_url:
                return None  # 404: terminal
            if "C-Q1" in absolute_url:
                return ""  # empty 200: retryable
            return "<html>doc</html>"

    class Parser:
        def parse(self, family: str, html: str, accession: str, source_url: str) -> ParseOutcome:
            if accession == "D-Q1":
                return ParseOutcome("parse_failed", detail="caption_absent")
            return ParseOutcome("extracted", body=" Discussion and Analysis ", full_chars=23)

    first = run_periodic_report_sections(ebull_test_conn, Source(), Parser())
    assert first.rows_written == 6 and first.fetched == 5
    assert first.outcomes == {
        "extracted/-": 3,
        "fetch_failed/missing": 1,
        "fetch_failed/empty": 1,
        "parse_failed/caption_absent": 1,
    }
    rows = ebull_test_conn.execute(
        "SELECT instrument_id, status, extractor, source_chars FROM periodic_report_sections ORDER BY instrument_id"
    ).fetchall()
    assert [(r[0], r[1]) for r in rows] == [
        (1, "extracted"),
        (2, "fetch_failed"),
        (3, "fetch_failed"),
        (4, "parse_failed"),
        (8, "extracted"),
        (10, "extracted"),
    ]
    assert {r[2] for r in rows} == {extractor_id()}

    second = run_periodic_report_sections(ebull_test_conn, Source(), Parser())
    assert (second.rows_written, second.not_due_terminal, second.not_due_backoff, second.due) == (0, 5, 1, 0)


def test_one_accession_commits_all_its_rows_or_none(ebull_test_conn: Conn) -> None:
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    ebull_test_conn.commit()
    due = (
        Target(instrument_id=1, accession="X", form="10-Q", filing_date=date(2026, 8, 1), url="u"),
        Target(instrument_id=424_242, accession="X", form="10-Q", filing_date=date(2026, 8, 1), url="u"),  # FK fails
    )
    work = AccessionWork(
        accession="X", family="10-Q", url="u", due=due, all_never=True, max_filing_date=date(2026, 8, 1)
    )
    outcome = RowOutcome("fetch_failed", retryable=True, detail="empty", source_url="u")
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        write_accession_rows(ebull_test_conn, work, outcome, "ext")
    ebull_test_conn.rollback()
    assert ebull_test_conn.execute("SELECT count(*) FROM periodic_report_sections").fetchone() == (0,)


def test_backoff_reads_the_stored_clock(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    """A retryable row written now is not due now, and is due once the as-of passes its one-day backoff."""
    _exchanges(ebull_test_conn)
    _instrument(ebull_test_conn, 1)
    _event(ebull_test_conn, 1, "A", "10-Q", date(2026, 6, 30))
    _insert(
        ebull_test_conn,
        accession_number="A",
        extractor="ext",
        status="fetch_failed",
        body=None,
        full_chars=None,
        detail="empty",
        retryable=True,
        source_text_sha256=None,
        source_chars=None,
    )
    ebull_test_conn.commit()
    result = SectionsRunResult(extractor="ext")
    assert select_worklist(ebull_test_conn, "ext", result).work == [] and result.not_due_backoff == 1
    real = prs.due_state
    monkeypatch.setattr(prs, "due_state", lambda rows, as_of: real(rows, as_of + timedelta(days=1)))
    result = SectionsRunResult(extractor="ext")
    assert [w.accession for w in select_worklist(ebull_test_conn, "ext", result).work] == ["A"]
