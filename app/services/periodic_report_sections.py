"""``periodic_report_sections`` producer (#3518): selection, due state, fetch outcomes and the run.

Spec: ``docs/proposals/etl/2026-09-30-3518-periodic-report-sections.md`` §4-§5. The text a row carries is decided
by ``app/services/mdna_extraction.py`` (hashed); this module decides WHICH reports are fetched, when a failed one
is retried, and how a fetch outcome becomes a row.

Target per instrument (spec §4.1, fund-v1 §4's rule at the job's as-of): the newest original ``10-K`` / ``10-KT``
/ ``10-Q`` / ``10-QT`` event with ``created_at <= as_of``, exactly one such row per (instrument, accession),
``report_date`` set and ``<=`` the as-of's UTC date, ordered by (report_date, filing_date, accession) descending.
Only the CURRENT target is ever extracted: fund-v1 runs live and never reads a past session's target.

Knowledge time: ``fetched_at`` is the INSERT's ``clock_timestamp()`` (sql/441 trigger); each accession commits in
its own transaction a few statements later.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Final, Literal, Protocol

import httpx
import psycopg
from psycopg.rows import dict_row

from app.services.mdna_extraction import FORM_FAMILY, SECTION_ID, ParseOutcome, extractor_id

logger = logging.getLogger(__name__)

# --- bounds (spec §4.4, by construction; the measurements behind them and their reproducing queries / census
# command are in spec §8.1 and §8.3) ------------------------------------------------------------------------------
MAX_ACCESSIONS_PER_RUN: Final = 500  # distinct accessions fetched per run
MAX_RUN_SECONDS: Final = 2_700  # no new accession after 45 minutes from the job's own start
MAX_SOURCE_CHARS: Final = 64_000_000  # decoded characters; larger -> parse_failed / too_large
#: attempt 1 -> 1 d, 2 -> 2 d, ... >= 6 -> 30 d. Not business_summary's 1/7/30/365: a one-year quarantine would
#: silence a live trial's target.
BACKOFF_DAYS: Final = (1, 2, 4, 8, 16, 30)

ORIGINAL_FORMS: Final = tuple(FORM_FAMILY)

Status = Literal["extracted", "item_absent", "fetch_failed", "parse_failed", "invalidated"]


# --- due state (spec §4.1 / §4.4) ------------------------------------------------------------------------------
@dataclass(frozen=True)
class HistoryRow:
    """One stored row of an identity under the CURRENT extractor."""

    row_id: int
    status: Status
    retryable: bool
    invalidates_row_id: int | None
    fetched_at: datetime


def effective_history(rows: Iterable[HistoryRow]) -> list[HistoryRow]:
    """The identity's rows minus invalidation rows and the rows they invalidate, in ``row_id`` order."""
    rows = list(rows)
    withdrawn = {r.invalidates_row_id for r in rows if r.status == "invalidated"}
    return sorted((r for r in rows if r.status != "invalidated" and r.row_id not in withdrawn), key=lambda r: r.row_id)


def attempt_count(rows: Iterable[HistoryRow]) -> int:
    """Consecutive retryable rows counting back from the latest row, stopping at a non-retryable, ``extracted``
    or ``invalidated`` row (an invalidation resets the count)."""
    n = 0
    for r in sorted(rows, key=lambda r: r.row_id, reverse=True):
        if r.status == "invalidated" or not r.retryable:
            break
        n += 1
    return n


def backoff_days(attempt: int) -> int:
    return BACKOFF_DAYS[min(max(attempt, 1), len(BACKOFF_DAYS)) - 1]


DueState = Literal["never", "retry_due", "terminal", "backoff"]


def due_state(rows: Iterable[HistoryRow], as_of: datetime) -> DueState:
    """``never`` / ``retry_due`` are due; ``terminal`` / ``backoff`` are not.

    Due when the effective history is empty (never attempted, or every attempt invalidated — the operator's
    repair path), or its latest row is retryable and the backoff for the attempt count has elapsed since it.
    """
    rows = list(rows)
    effective = effective_history(rows)
    if not effective:
        return "never"
    latest = effective[-1]
    if not latest.retryable:
        return "terminal"
    if as_of - latest.fetched_at >= timedelta(days=backoff_days(attempt_count(rows))):
        return "retry_due"
    return "backoff"


# --- worklist (spec §4.1) --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Target:
    instrument_id: int
    accession: str
    form: str
    filing_date: date
    url: str | None

    @property
    def family(self) -> str:
        return FORM_FAMILY[self.form]

    @property
    def section_id(self) -> str:
        return SECTION_ID[self.family]


@dataclass(frozen=True)
class AccessionWork:
    accession: str
    family: str
    url: str | None
    due: tuple[Target, ...]  # due instruments only; each gets one row
    all_never: bool  # every due instrument has an empty effective history
    max_filing_date: date


@dataclass
class Worklist:
    work: list[AccessionWork]
    inconsistent: list[str]


def build_worklist(targets: Sequence[Target], states: dict[int, DueState]) -> Worklist:
    """Group due instruments by accession, skip accessions whose instruments (due or not) disagree on form family
    or URL, and order: all-never-attempted first, then newest filing first, then accession."""
    by_acc: dict[str, list[Target]] = {}
    for t in targets:
        by_acc.setdefault(t.accession, []).append(t)
    work: list[AccessionWork] = []
    inconsistent: list[str] = []
    for acc, ts in by_acc.items():
        due = tuple(t for t in ts if states[t.instrument_id] in ("never", "retry_due"))
        if not due:
            continue
        if len({(t.family, t.url) for t in ts}) > 1:
            inconsistent.append(acc)
            continue
        work.append(
            AccessionWork(
                accession=acc,
                family=ts[0].family,
                url=ts[0].url,
                due=due,
                all_never=all(states[t.instrument_id] == "never" for t in due),
                max_filing_date=max(t.filing_date for t in ts),
            )
        )
    # Newest-first serves the live trial; a retry-exhausted backlog cannot starve new reports.
    work.sort(key=lambda w: w.accession, reverse=True)
    work.sort(key=lambda w: w.max_filing_date, reverse=True)
    work.sort(key=lambda w: not w.all_never)
    return Worklist(work=work, inconsistent=sorted(inconsistent))


# --- fetch outcome (spec §4.2) ---------------------------------------------------------------------------------
@dataclass(frozen=True)
class RowOutcome:
    status: Status
    retryable: bool
    detail: str | None = None
    body: str | None = None
    full_chars: int | None = None
    source_url: str | None = None
    source_text_sha256: str | None = None
    source_chars: int | None = None


@dataclass(frozen=True)
class Fetched:
    """Either a document to parse (``html``) or a terminal row outcome for the accession (``outcome``)."""

    html: str | None = None
    outcome: RowOutcome | None = None


def fetch_primary_document(fetch: Callable[[str], str | None], url: str | None) -> Fetched:
    """Apply the §4.2 table. Only ``httpx.HTTPError`` is caught: anything else is a code or config defect and
    fails the run (prevention log #1698: a transient is never recorded as terminal)."""
    if url is None:
        return Fetched(outcome=RowOutcome("fetch_failed", retryable=True, detail="no_url"))
    try:
        html = fetch(url)
    except httpx.HTTPError as exc:
        status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
        detail = type(exc).__name__ + (f":{status}" if status is not None else "")
        return Fetched(outcome=RowOutcome("fetch_failed", retryable=True, detail=detail, source_url=url))
    if html is None:
        return Fetched(outcome=RowOutcome("fetch_failed", retryable=False, detail="missing", source_url=url))
    if not html.strip():
        return Fetched(outcome=RowOutcome("fetch_failed", retryable=True, detail="empty", source_url=url))
    if len(html) > MAX_SOURCE_CHARS:
        return Fetched(
            outcome=RowOutcome("parse_failed", retryable=False, detail="too_large", source_url=url, **_provenance(html))
        )
    return Fetched(html=html)


def _provenance(html: str) -> dict[str, Any]:
    return {"source_text_sha256": hashlib.sha256(html.encode("utf-8")).hexdigest(), "source_chars": len(html)}


def parsed_outcome(parsed: ParseOutcome, url: str, html: str) -> RowOutcome:
    return RowOutcome(
        parsed.status,
        retryable=parsed.retryable,
        detail=parsed.detail,
        body=parsed.body,
        full_chars=parsed.full_chars,
        source_url=url,
        **_provenance(html),
    )


# --- the run (spec §5) -----------------------------------------------------------------------------------------
class DocumentSource(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


class Parser(Protocol):
    def parse(self, family: str, html: str, accession: str, source_url: str) -> ParseOutcome: ...


@dataclass
class SectionsRunResult:
    extractor: str
    as_of: datetime | None = None
    # instrument units
    in_scope: int = 0
    no_target: int = 0
    not_due_terminal: int = 0
    not_due_backoff: int = 0
    due: int = 0
    # accession units
    accessions_due: int = 0
    inconsistent_accession: int = 0
    deferred_by_cap: int = 0
    fetched: int = 0
    # rows
    outcomes: Counter[str] = field(default_factory=Counter)
    rows_written: int = 0

    def digest(self) -> str:
        outcomes = ",".join(f"{k}={v}" for k, v in sorted(self.outcomes.items()))
        return (
            f"extractor={self.extractor} in_scope={self.in_scope} no_target={self.no_target} "
            f"not_due_terminal={self.not_due_terminal} not_due_backoff={self.not_due_backoff} due={self.due} "
            f"accessions_due={self.accessions_due} inconsistent_accession={self.inconsistent_accession} "
            f"deferred_by_cap={self.deferred_by_cap} fetched={self.fetched} rows_written={self.rows_written} "
            f"outcomes[{outcomes}]"
        )


_SCOPE_CTE: Final = """
    scope AS (
        SELECT i.instrument_id
          FROM instruments i
          JOIN exchanges e ON e.exchange_id = i.exchange
         WHERE i.is_tradable AND e.asset_class = 'us_equity'
    )
"""

_IN_SCOPE_SQL: Final = "WITH" + _SCOPE_CTE + "SELECT count(*) AS n FROM scope"

# The census rule (scripts/measure_3518_mdna_extraction_census.py::_targets) over the producer's scope: an
# accession with more than one original row for the instrument is excluded and the next-newest report is taken.
_TARGETS_SQL: Final = (
    "WITH"
    + _SCOPE_CTE
    + """,
    ev AS (
        SELECT fe.instrument_id, fe.provider_filing_id AS acc, min(fe.filing_type) AS form,
               min(fe.filing_date) AS filing_date, min(fe.report_date) AS report_date,
               min(fe.primary_document_url) AS url, count(*) AS n
          FROM filing_events fe
          JOIN scope USING (instrument_id)
         WHERE fe.filing_type = ANY(%(forms)s) AND fe.created_at <= %(as_of)s
         GROUP BY 1, 2
    )
    SELECT DISTINCT ON (ev.instrument_id) ev.instrument_id, ev.acc, ev.form, ev.filing_date, ev.url
      FROM ev
     WHERE ev.n = 1 AND ev.report_date IS NOT NULL
       AND ev.report_date <= (%(as_of)s AT TIME ZONE 'UTC')::date
     ORDER BY ev.instrument_id, ev.report_date DESC, ev.filing_date DESC, ev.acc DESC
"""
)

_HISTORY_SQL: Final = """
    SELECT p.instrument_id, p.row_id, p.status, p.retryable, p.invalidates_row_id, p.fetched_at
      FROM periodic_report_sections p
      JOIN unnest(%(ids)s::bigint[], %(accs)s::text[], %(sections)s::text[]) AS t(instrument_id, acc, section_id)
        ON p.instrument_id = t.instrument_id AND p.accession_number = t.acc AND p.section_id = t.section_id
     WHERE p.extractor = %(extractor)s
"""

_INSERT_SQL: Final = """
    INSERT INTO periodic_report_sections
        (instrument_id, accession_number, section_id, extractor, status, body, full_chars, detail, retryable,
         source_url, source_text_sha256, source_chars)
    VALUES (%(instrument_id)s, %(accession_number)s, %(section_id)s, %(extractor)s, %(status)s, %(body)s,
            %(full_chars)s, %(detail)s, %(retryable)s, %(source_url)s, %(source_text_sha256)s, %(source_chars)s)
"""


def select_worklist(conn: psycopg.Connection[Any], extractor: str, result: SectionsRunResult) -> Worklist:
    """One REPEATABLE READ transaction materialises targets, due state and accession groups, then commits."""
    conn.commit()
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT now() AS as_of")
        as_of: datetime = cur.fetchone()["as_of"]  # type: ignore[index]
        cur.execute(_IN_SCOPE_SQL)
        result.as_of = as_of
        result.in_scope = int(cur.fetchone()["n"])  # type: ignore[index]
        cur.execute(_TARGETS_SQL, {"forms": list(ORIGINAL_FORMS), "as_of": as_of})
        rows = cur.fetchall()
        targets = [
            Target(
                instrument_id=int(r["instrument_id"]),
                accession=str(r["acc"]),
                form=str(r["form"]),
                filing_date=r["filing_date"],
                url=r["url"],
            )
            for r in rows
        ]
        result.no_target = result.in_scope - len(targets)
        cur.execute(
            _HISTORY_SQL,
            {
                "ids": [t.instrument_id for t in targets],
                "accs": [t.accession for t in targets],
                "sections": [t.section_id for t in targets],
                "extractor": extractor,
            },
        )
        history: dict[int, list[HistoryRow]] = {}
        for h in cur.fetchall():
            history.setdefault(int(h["instrument_id"]), []).append(
                HistoryRow(
                    row_id=int(h["row_id"]),
                    status=h["status"],
                    retryable=bool(h["retryable"]),
                    invalidates_row_id=h["invalidates_row_id"],
                    fetched_at=h["fetched_at"],
                )
            )
    conn.commit()

    states: dict[int, DueState] = {t.instrument_id: due_state(history.get(t.instrument_id, []), as_of) for t in targets}
    tally = Counter(states.values())
    result.not_due_terminal = tally["terminal"]
    result.not_due_backoff = tally["backoff"]
    result.due = tally["never"] + tally["retry_due"]
    worklist = build_worklist(targets, states)
    result.accessions_due = len(worklist.work) + len(worklist.inconsistent)
    result.inconsistent_accession = len(worklist.inconsistent)
    return worklist


def write_accession_rows(
    conn: psycopg.Connection[Any], work: AccessionWork, outcome: RowOutcome, extractor: str
) -> int:
    """All rows of one accession in ONE transaction: one row per due instrument."""
    with conn.transaction(), conn.cursor() as cur:
        for t in work.due:
            cur.execute(
                _INSERT_SQL,
                {
                    "instrument_id": t.instrument_id,
                    "accession_number": work.accession,
                    "section_id": t.section_id,
                    "extractor": extractor,
                    "status": outcome.status,
                    "body": outcome.body,
                    "full_chars": outcome.full_chars,
                    "detail": outcome.detail,
                    "retryable": outcome.retryable,
                    "source_url": outcome.source_url,
                    "source_text_sha256": outcome.source_text_sha256,
                    "source_chars": outcome.source_chars,
                },
            )
    conn.commit()
    return len(work.due)


def run_periodic_report_sections(
    conn: psycopg.Connection[Any],
    source: DocumentSource,
    parser: Parser,
    *,
    max_accessions: int = MAX_ACCESSIONS_PER_RUN,
    max_run_seconds: float = MAX_RUN_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> SectionsRunResult:
    """One producer run. ``source`` is the ``SecFilingsProvider`` (its HTTP layer waits on the process's
    ``sec_rate_gate``); ``parser`` is a :class:`~app.services.mdna_parse_worker.ParseWorker`."""
    started = clock()
    extractor = extractor_id()
    result = SectionsRunResult(extractor=extractor)
    worklist = select_worklist(conn, extractor, result)
    logger.info("sec_periodic_report_sections: extractor %s, as_of %s", extractor, result.as_of)
    processed = 0
    for work in worklist.work:
        if processed >= max_accessions or clock() - started >= max_run_seconds:
            result.deferred_by_cap += 1
            continue
        processed += 1
        fetched = fetch_primary_document(source.fetch_document_text, work.url)
        if work.url is not None:
            result.fetched += 1
        if fetched.outcome is not None:
            outcome = fetched.outcome
        else:
            assert fetched.html is not None and work.url is not None
            outcome = parsed_outcome(
                parser.parse(work.family, fetched.html, work.accession, work.url), work.url, fetched.html
            )
        del fetched
        written = write_accession_rows(conn, work, outcome, extractor)
        result.rows_written += written
        result.outcomes[f"{outcome.status}/{outcome.detail or '-'}"] += written
    return result
