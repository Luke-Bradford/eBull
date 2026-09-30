"""#3518 — producer policy without a database: due state, attempt counting, backoff, accession grouping and
order, the §4.2 fetch table, and the run's caps (spec §4, §7)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest

from app.services import periodic_report_sections as prs
from app.services.mdna_extraction import ParseOutcome
from app.services.periodic_report_sections import (
    MAX_SOURCE_CHARS,
    AccessionWork,
    DueState,
    Fetched,
    HistoryRow,
    RowOutcome,
    SectionsRunResult,
    Target,
    Worklist,
    attempt_count,
    backoff_days,
    build_worklist,
    due_state,
    effective_history,
    fetch_primary_document,
    run_periodic_report_sections,
)

NOW = datetime(2026, 9, 30, 22, 30, tzinfo=UTC)


def _row(
    row_id: int,
    status: str,
    *,
    retryable: bool = False,
    invalidates: int | None = None,
    days_ago: float = 10,
) -> HistoryRow:
    return HistoryRow(
        row_id=row_id,
        status=status,  # type: ignore[arg-type]
        retryable=retryable,
        invalidates_row_id=invalidates,
        fetched_at=NOW - timedelta(days=days_ago),
    )


# --- due state -------------------------------------------------------------------------------------------------
def test_backoff_schedule() -> None:
    assert [backoff_days(n) for n in range(1, 9)] == [1, 2, 4, 8, 16, 30, 30, 30]
    assert backoff_days(0) == 1


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([], "never"),
        ([_row(1, "extracted")], "terminal"),
        ([_row(1, "item_absent")], "terminal"),
        ([_row(1, "parse_failed")], "terminal"),  # caption_absent / accessor exception
        ([_row(1, "fetch_failed")], "terminal"),  # missing (404/410)
        ([_row(1, "fetch_failed", retryable=True, days_ago=0.5)], "backoff"),
        ([_row(1, "fetch_failed", retryable=True, days_ago=1)], "retry_due"),
        # two consecutive retryable rows -> attempt 2 -> 2 days
        ([_row(1, "fetch_failed", retryable=True), _row(2, "parse_failed", retryable=True, days_ago=1.5)], "backoff"),
        ([_row(1, "fetch_failed", retryable=True), _row(2, "parse_failed", retryable=True, days_ago=2)], "retry_due"),
        # a retryable row after a terminal one counts from 1 again
        ([_row(1, "item_absent"), _row(2, "fetch_failed", retryable=True, days_ago=1)], "retry_due"),
        # due state reads the LATEST effective row (the reader, not this, prefers an earlier success)
        ([_row(1, "extracted"), _row(2, "fetch_failed", retryable=True, days_ago=0.1)], "backoff"),
        # every attempt invalidated -> due again (the operator's repair path)
        ([_row(1, "extracted"), _row(2, "invalidated", invalidates=1)], "never"),
        # invalidating the latest row exposes the one before it
        ([_row(1, "item_absent"), _row(2, "extracted"), _row(3, "invalidated", invalidates=2)], "terminal"),
    ],
)
def test_due_state(rows: list[HistoryRow], expected: DueState) -> None:
    assert due_state(rows, NOW) == expected


def test_attempt_count_resets_at_an_invalidation_and_at_a_terminal_row() -> None:
    retry = dict(retryable=True)
    assert attempt_count([]) == 0
    assert attempt_count([_row(1, "fetch_failed", **retry), _row(2, "parse_failed", **retry)]) == 2
    assert (
        attempt_count([_row(1, "fetch_failed", **retry), _row(2, "item_absent"), _row(3, "fetch_failed", **retry)]) == 1
    )
    assert (
        attempt_count(
            [_row(1, "fetch_failed", **retry), _row(2, "invalidated", invalidates=1), _row(3, "fetch_failed", **retry)]
        )
        == 1
    )
    # order is by row_id, not by list position
    assert (
        attempt_count([_row(3, "fetch_failed", **retry), _row(1, "extracted"), _row(2, "fetch_failed", **retry)]) == 2
    )


def test_effective_history_drops_invalidations_and_their_targets() -> None:
    rows = [_row(3, "invalidated", invalidates=1), _row(2, "item_absent"), _row(1, "extracted")]
    assert [r.row_id for r in effective_history(rows)] == [2]


# --- worklist --------------------------------------------------------------------------------------------------
def _t(iid: int, acc: str, *, form: str = "10-Q", filed: int = 1, url: str | None = "u") -> Target:
    return Target(instrument_id=iid, accession=acc, form=form, filing_date=date(2026, 9, filed), url=url)


def test_worklist_groups_due_instruments_and_skips_inconsistent_accessions() -> None:
    targets = [
        _t(1, "A"),
        _t(2, "A"),  # same accession, not due: gets no row, still counted for consistency
        _t(3, "B", url="u1"),
        _t(4, "B", url="u2"),  # inconsistent URL
        _t(5, "C", form="10-QT"),
        _t(6, "C", form="10-K"),  # inconsistent family
        _t(7, "D", form="10-KT"),
        _t(8, "E"),  # not due at all
    ]
    states: dict[int, DueState] = {
        1: "never",
        2: "terminal",
        3: "never",
        4: "never",
        5: "never",
        6: "backoff",
        7: "retry_due",
        8: "backoff",
    }
    wl = build_worklist(targets, states)
    assert wl.inconsistent == ["B", "C"]
    by_acc = {w.accession: w for w in wl.work}
    assert set(by_acc) == {"A", "D"}
    assert [t.instrument_id for t in by_acc["A"].due] == [1]
    assert by_acc["D"].family == "10-K" and by_acc["D"].due[0].section_id == "10-K:Item 7"


def test_worklist_order_never_attempted_first_then_newest_filing_then_accession() -> None:
    targets = [
        _t(1, "R-old", filed=1),
        _t(2, "N-old", filed=2),
        _t(3, "N-new-a", filed=9),
        _t(4, "N-new-b", filed=9),
        _t(5, "R-new", filed=20),
        _t(6, "MIX", filed=25),
        _t(7, "MIX", filed=3),
    ]
    states: dict[int, DueState] = {
        1: "retry_due",
        2: "never",
        3: "never",
        4: "never",
        5: "retry_due",
        6: "never",
        7: "retry_due",  # one due instrument has history -> the accession is not all-never
    }
    order = [w.accession for w in build_worklist(targets, states).work]
    assert order == ["N-new-b", "N-new-a", "N-old", "MIX", "R-new", "R-old"]


# --- fetch table (spec §4.2) -----------------------------------------------------------------------------------
def _raises(exc: Exception) -> Callable[[str], str | None]:
    def f(_url: str) -> str | None:
        raise exc

    return f


def _status_error(code: int) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "https://www.sec.gov/x")
    return httpx.HTTPStatusError("boom", request=req, response=httpx.Response(code, request=req))


@pytest.mark.parametrize(
    ("fetch", "url", "expected"),
    [
        (lambda _u: "never called", None, RowOutcome("fetch_failed", retryable=True, detail="no_url")),
        (lambda _u: None, "u", RowOutcome("fetch_failed", retryable=False, detail="missing", source_url="u")),
        (lambda _u: "", "u", RowOutcome("fetch_failed", retryable=True, detail="empty", source_url="u")),
        (lambda _u: " \n\t", "u", RowOutcome("fetch_failed", retryable=True, detail="empty", source_url="u")),
        (
            _raises(_status_error(429)),
            "u",
            RowOutcome("fetch_failed", retryable=True, detail="HTTPStatusError:429", source_url="u"),
        ),
        (
            _raises(_status_error(503)),
            "u",
            RowOutcome("fetch_failed", retryable=True, detail="HTTPStatusError:503", source_url="u"),
        ),
        (
            _raises(_status_error(403)),
            "u",
            RowOutcome("fetch_failed", retryable=True, detail="HTTPStatusError:403", source_url="u"),
        ),
        (
            _raises(httpx.ReadTimeout("slow")),
            "u",
            RowOutcome("fetch_failed", retryable=True, detail="ReadTimeout", source_url="u"),
        ),
        (
            _raises(httpx.ConnectError("down")),
            "u",
            RowOutcome("fetch_failed", retryable=True, detail="ConnectError", source_url="u"),
        ),
    ],
)
def test_fetch_table(fetch: Callable[[str], str | None], url: str | None, expected: RowOutcome) -> None:
    assert fetch_primary_document(fetch, url) == Fetched(outcome=expected)


def test_fetch_non_http_exception_fails_the_run() -> None:
    with pytest.raises(KeyError):
        fetch_primary_document(_raises(KeyError("bug")), "u")


def test_fetch_too_large_is_terminal_and_carries_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prs, "MAX_SOURCE_CHARS", 10)
    out = fetch_primary_document(lambda _u: "x" * 11, "u").outcome
    assert out is not None
    assert (out.status, out.detail, out.retryable, out.source_chars) == ("parse_failed", "too_large", False, 11)
    assert out.source_text_sha256 is not None and len(out.source_text_sha256) == 64
    assert fetch_primary_document(lambda _u: "x" * 10, "u") == Fetched(html="x" * 10)
    assert MAX_SOURCE_CHARS == 64_000_000


def test_provenance_hashes_the_utf8_of_the_decoded_text() -> None:
    import hashlib

    out = prs.parsed_outcome(ParseOutcome("item_absent", detail="none"), "u", "é")
    assert out.source_text_sha256 == hashlib.sha256("é".encode()).hexdigest()
    assert out.source_chars == 1


# --- the run: caps and outcome routing (DB calls replaced) ----------------------------------------------------
class _Source:
    def __init__(self, bodies: dict[str, str | None]) -> None:
        self.bodies = bodies
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self.bodies[absolute_url]


class _Parser:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def parse(self, family: str, html: str, accession: str, source_url: str) -> ParseOutcome:
        self.calls.append(accession)
        return ParseOutcome("extracted", body="Discussion and Analysis", full_chars=23)


def _work(acc: str, n_due: int = 1, url: str | None = None) -> AccessionWork:
    due = tuple(_t(i, acc) for i in range(n_due))
    return AccessionWork(
        accession=acc, family="10-Q", url=url or f"url-{acc}", due=due, all_never=True, max_filing_date=date(2026, 9, 1)
    )


def _patch_db(monkeypatch: pytest.MonkeyPatch, work: list[AccessionWork]) -> list[tuple[str, RowOutcome]]:
    written: list[tuple[str, RowOutcome]] = []

    def select(_conn: Any, _extractor: str, result: SectionsRunResult) -> Worklist:
        return Worklist(work=work, inconsistent=[])

    def write(_conn: Any, w: AccessionWork, outcome: RowOutcome, _extractor: str) -> int:
        written.append((w.accession, outcome))
        return len(w.due)

    monkeypatch.setattr(prs, "select_worklist", select)
    monkeypatch.setattr(prs, "write_accession_rows", write)
    return written


def test_run_caps_accessions_and_counts_the_rest_deferred(monkeypatch: pytest.MonkeyPatch) -> None:
    work = [_work("A", 2), _work("B"), _work("C")]
    written = _patch_db(monkeypatch, work)
    source = _Source({"url-A": "<html/>", "url-B": None, "url-C": "<html/>"})
    parser = _Parser()
    result = run_periodic_report_sections(None, source, parser, max_accessions=2)  # type: ignore[arg-type]
    assert [a for a, _ in written] == ["A", "B"]
    assert (result.fetched, result.deferred_by_cap, result.rows_written) == (2, 1, 3)
    assert parser.calls == ["A"]  # B was a 404: no parse
    assert result.outcomes == {"extracted/-": 2, "fetch_failed/missing": 1}
    assert written[0][1].source_url == "url-A" and written[0][1].source_chars == len("<html/>")


def test_run_stops_starting_accessions_after_the_time_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    written = _patch_db(monkeypatch, [_work("A"), _work("B")])
    ticks = iter([0.0, 0.0, 100.0])
    result = run_periodic_report_sections(
        None,  # type: ignore[arg-type]
        _Source({"url-A": "<html/>", "url-B": "<html/>"}),
        _Parser(),
        max_run_seconds=50,
        clock=lambda: next(ticks),
    )
    assert [a for a, _ in written] == ["A"]
    assert result.deferred_by_cap == 1


def test_run_writes_no_url_without_fetching(monkeypatch: pytest.MonkeyPatch) -> None:
    w = AccessionWork(
        accession="A", family="10-K", url=None, due=(_t(1, "A"),), all_never=True, max_filing_date=date(2026, 9, 1)
    )
    written = _patch_db(monkeypatch, [w])
    source = _Source({})
    result = run_periodic_report_sections(None, source, _Parser())  # type: ignore[arg-type]
    assert source.calls == [] and result.fetched == 0
    assert written == [("A", RowOutcome("fetch_failed", retryable=True, detail="no_url"))]
