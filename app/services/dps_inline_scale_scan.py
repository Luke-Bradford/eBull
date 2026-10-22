"""``sec_dps_inline_scale_scan``: the inline ``scale`` of every DPS occurrence, per accession (#3947).

Spec: ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §The scan. Tables: sql/514.

* **Population:** accessions holding a ``CommonStockDividendsPerShareDeclared`` fact (any unit) in
  ``financial_facts_raw``, filed on or after 2019-06-15 (the first inline XBRL phase-in; spec policy 6).
* **Eligible:** ``parser_version`` is not the current one, and either ``attempt_version`` is not the current one
  (never attempted at this version) or fewer than :data:`MAX_ATTEMPTS` failures with the backoff elapsed.
* **Attempt:** the ``index.json`` listing, then every ``.htm``/``.html``/``.xhtml`` file of it except the EDGAR
  Renderer pages ``FilingSummary.xml`` names, through the provider's shared SEC limiter; the bodies are parsed
  (:mod:`app.services.inline_xbrl_dps`) and discarded.
* **Success** replaces the accession's occurrences and active generation in one transaction, and enqueues the
  accession's instruments in ``sec_inline_scale_renormalise`` when the old or new generation holds a positive-scale
  or an unresolved occurrence. **Failure** changes only the work state: ``attempts`` + 1, the next attempt
  ``2^attempts`` hours later, ``last_error``.
* **Publication is conditional** on the work state read when the run selected the accession (``attempt_version``,
  ``attempts``), re-read under a row lock; a mismatch is counted ``superseded`` and nothing is written. The job's
  lane lock already keeps two scans from overlapping, so this guards a stale run, not a race.
* **Order:** filings from the last :data:`RECENT_DAYS` days first, then the backlog oldest first. No accession is
  started after :data:`START_CAP`.

* **Drain first** (only when the caller passes ``drain``, i.e. ``EBULL_DPS_INLINE_SCALE_ENABLED`` is on): queued
  instruments in ``revision`` order, none started after half of :data:`START_CAP`. Each is normalised, then its row
  deleted only at the revision read, so work queued meanwhile stays queued; a failure counts in ``failures`` and the
  row stays for the next run.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Final, Protocol

import psycopg

from app.services.filing_documents import parse_filing_index
from app.services.inline_xbrl_dps import (
    CONCEPT,
    INLINE_SUFFIXES,
    DocumentSetError,
    Occurrence,
    parse_document_set,
    rendered_report_files,
)
from app.services.sync_orchestrator.progress import report_progress

logger = logging.getLogger(__name__)

PARSER_VERSION: Final = "dps-inline-scale-v1"
SCOPE_START: Final = date(2019, 6, 15)
MAX_ATTEMPTS: Final = 5
RECENT_DAYS: Final = 7
START_CAP: Final = timedelta(minutes=45)
FILING_SUMMARY: Final = "FilingSummary.xml"


class ScanProvider(Protocol):
    def fetch_filing_index(
        self, provider_filing_id: str, *, issuer_cik: str | None = None
    ) -> dict[str, object] | None: ...

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass(frozen=True)
class Target:
    accession_number: str
    filed_date: date
    issuer_cik: str | None
    instrument_ids: tuple[int, ...]
    seen_attempt_version: str | None
    seen_attempts: int | None


@dataclass
class ScanResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    superseded: int = 0
    deferred: int = 0
    documents_fetched: int = 0
    instruments_enqueued: int = 0
    work_state: dict[str, int] = field(default_factory=dict)
    queue_length: int = 0
    drain: DrainResult | None = None

    def digest(self) -> str:
        state = ", ".join(f"{k} {v}" for k, v in self.work_state.items())
        drained = "drain off" if self.drain is None else self.drain.digest()
        return (
            f"{drained}; "
            f"attempted {self.attempted} (ok {self.succeeded}, failed {self.failed}, superseded {self.superseded}),"
            f" deferred {self.deferred}, documents {self.documents_fetched},"
            f" enqueued {self.instruments_enqueued}; population: {state}; queue {self.queue_length}"
        )


@dataclass
class DrainResult:
    normalised: int = 0
    failed: int = 0
    requeued: int = 0
    deferred: int = 0

    def digest(self) -> str:
        return (
            f"drained {self.normalised} (failed {self.failed}, re-queued meanwhile {self.requeued}),"
            f" drain deferred {self.deferred}"
        )


_POPULATION_SQL: Final = """
WITH pop AS (
    SELECT f.accession_number,
           max(f.filed_date) AS filed_date,
           array_agg(DISTINCT f.instrument_id ORDER BY f.instrument_id) AS instrument_ids
    FROM financial_facts_raw f
    WHERE f.concept = %(concept)s AND f.filed_date >= %(scope_start)s
    GROUP BY f.accession_number
)
"""


def select_targets(conn: psycopg.Connection[tuple], *, today: date) -> list[Target]:
    """Every eligible accession, in scan order."""
    with conn.transaction():
        rows = conn.execute(
            _POPULATION_SQL
            + """
            SELECT p.accession_number, p.filed_date, p.instrument_ids,
                   -- The archive lives under the issuer CIK (#736): the manifest's, else the instrument's primary.
                   COALESCE(m.cik, (
                       SELECT ei.identifier_value FROM external_identifiers ei
                       WHERE ei.instrument_id = ANY (p.instrument_ids)
                         AND ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary
                       ORDER BY ei.instrument_id LIMIT 1)),
                   s.attempt_version, s.attempts
            FROM pop p
            LEFT JOIN sec_filing_manifest m USING (accession_number)
            LEFT JOIN sec_inline_scale_scan s USING (accession_number)
            WHERE s.parser_version IS DISTINCT FROM %(version)s
              AND (s.attempt_version IS DISTINCT FROM %(version)s
                   OR (s.attempts < %(max_attempts)s AND s.next_attempt_at <= now()))
            ORDER BY (p.filed_date >= %(recent_from)s) DESC, p.filed_date, p.accession_number
            """,
            {
                "concept": CONCEPT,
                "scope_start": SCOPE_START,
                "version": PARSER_VERSION,
                "max_attempts": MAX_ATTEMPTS,
                "recent_from": today - timedelta(days=RECENT_DAYS),
            },
        ).fetchall()
    return [Target(r[0], r[1], r[3], tuple(r[2]), r[4], r[5]) for r in rows]


def fetch_document_set(provider: ScanProvider, target: Target) -> list[tuple[str, str]]:
    """``(name, text)`` of every HTML file the accession's ``index.json`` lists, in listing order.

    Raises :class:`DocumentSetError` on a missing CIK, a 404 listing or document, or a listing with no entries;
    other HTTP errors propagate from the provider. Either way the attempt fails.
    """
    if target.issuer_cik is None:
        raise DocumentSetError("no_issuer_cik")
    index = provider.fetch_filing_index(target.accession_number, issuer_cik=target.issuer_cik)
    if index is None:
        raise DocumentSetError("index_not_found")
    listing = parse_filing_index(index, accession_number=target.accession_number, cik=target.issuer_cik)
    if not listing:
        raise DocumentSetError("index_unparseable")
    urls = {entry.document_name: entry.document_url for entry in listing}
    rendered: frozenset[str] = frozenset()
    if FILING_SUMMARY in urls:
        summary = provider.fetch_document_text(urls[FILING_SUMMARY])
        rendered = rendered_report_files(summary) if summary is not None else frozenset()
    documents: list[tuple[str, str]] = []
    for entry in listing:
        if not entry.document_name.lower().endswith(INLINE_SUFFIXES) or entry.document_name in rendered:
            continue
        text = provider.fetch_document_text(entry.document_url)
        if text is None:
            raise DocumentSetError(f"document_not_found:{entry.document_name}")
        documents.append((entry.document_name, text))
    return documents


def _locked_state_matches(cur: psycopg.Cursor[tuple], target: Target) -> bool:
    row = cur.execute(
        "SELECT attempt_version, attempts FROM sec_inline_scale_scan WHERE accession_number = %s FOR UPDATE",
        (target.accession_number,),
    ).fetchone()
    seen = (target.seen_attempt_version, target.seen_attempts)
    return (row is None and seen == (None, None)) or (row is not None and (row[0], row[1]) == seen)


def _needs_renormalise(occurrences: Sequence[Occurrence]) -> bool:
    return any(o.unresolved is not None or (o.scale is not None and o.scale > 0) for o in occurrences)


def publish_success(
    conn: psycopg.Connection[tuple],
    target: Target,
    occurrences: Sequence[Occurrence] | None,
    documents: int,
) -> int | None:
    """Replace the accession's active generation. ``None`` when superseded; else the instruments enqueued."""
    with conn.transaction(), conn.cursor() as cur:
        if not _locked_state_matches(cur, target):
            return None
        old = cur.execute(
            "SELECT EXISTS (SELECT 1 FROM sec_inline_dps_occurrence"
            " WHERE accession_number = %s AND (scale > 0 OR unresolved IS NOT NULL))",
            (target.accession_number,),
        ).fetchone()
        cur.execute(
            """
            INSERT INTO sec_inline_scale_scan (accession_number, outcome, documents, parser_version, scanned_at,
                                               attempt_version, attempts, next_attempt_at, last_error)
            VALUES (%(a)s, %(outcome)s, %(documents)s, %(version)s, now(), %(version)s, 0, NULL, NULL)
            ON CONFLICT (accession_number) DO UPDATE SET
                outcome = EXCLUDED.outcome, documents = EXCLUDED.documents,
                parser_version = EXCLUDED.parser_version, scanned_at = EXCLUDED.scanned_at,
                attempt_version = EXCLUDED.attempt_version, attempts = 0, next_attempt_at = NULL, last_error = NULL
            """,
            {
                "a": target.accession_number,
                "outcome": "not_inline" if occurrences is None else "inline",
                "documents": documents,
                "version": PARSER_VERSION,
            },
        )
        cur.execute("DELETE FROM sec_inline_dps_occurrence WHERE accession_number = %s", (target.accession_number,))
        cur.executemany(
            """
            INSERT INTO sec_inline_dps_occurrence (
                accession_number, document, element_ordinal, fact_id, context_ref_raw, unit_ref_raw, scale_raw,
                sign_raw, format_raw, decimals_raw, display_text, entity_cik, period_start, period_end,
                dimensional, unit, scale, instance_value, unresolved)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    target.accession_number, o.document, o.element_ordinal, o.fact_id, o.context_ref_raw,
                    o.unit_ref_raw, o.scale_raw, o.sign_raw, o.format_raw, o.decimals_raw, o.display_text,
                    o.entity_cik, o.period_start, o.period_end, o.dimensional, o.unit, o.scale, o.instance_value,
                    o.unresolved,
                )
                for o in occurrences or ()
            ],
        )  # fmt: skip
        if not ((old is not None and old[0]) or _needs_renormalise(occurrences or ())):
            return 0
        # Always the new revision: work queued while a drain holds an older one stays queued.
        cur.execute(
            """
            INSERT INTO sec_inline_scale_renormalise (instrument_id, revision)
            SELECT i, nextval('sec_inline_scale_renormalise_revision') FROM unnest(%s::bigint[]) AS i
            ON CONFLICT (instrument_id) DO UPDATE SET revision = EXCLUDED.revision
            """,
            (list(target.instrument_ids),),
        )
        return len(target.instrument_ids)


def record_failure(conn: psycopg.Connection[tuple], target: Target, error: str) -> bool:
    """Count a failed attempt; the active generation is untouched. False when superseded."""
    attempts = (target.seen_attempts or 0) + 1 if target.seen_attempt_version == PARSER_VERSION else 1
    with conn.transaction(), conn.cursor() as cur:
        if not _locked_state_matches(cur, target):
            return False
        cur.execute(
            """
            INSERT INTO sec_inline_scale_scan (accession_number, attempt_version, attempts, next_attempt_at,
                                               last_error)
            VALUES (%(a)s, %(version)s, %(attempts)s, now() + make_interval(hours => %(hours)s), %(error)s)
            ON CONFLICT (accession_number) DO UPDATE SET
                attempt_version = EXCLUDED.attempt_version, attempts = EXCLUDED.attempts,
                next_attempt_at = EXCLUDED.next_attempt_at, last_error = EXCLUDED.last_error
            """,
            {
                "a": target.accession_number,
                "version": PARSER_VERSION,
                "attempts": attempts,
                "hours": 2**attempts,
                "error": error[:2000],
            },
        )
    return True


def work_state(conn: psycopg.Connection[tuple]) -> tuple[dict[str, int], int]:
    """Population accessions by work state at the current version, and the queue length."""
    with conn.transaction():
        row = conn.execute(
            _POPULATION_SQL
            + """
            SELECT count(*) FILTER (WHERE s.parser_version = %(version)s),
                   count(*) FILTER (WHERE s.parser_version IS DISTINCT FROM %(version)s
                                      AND s.attempt_version = %(version)s AND s.attempts >= %(max_attempts)s),
                   count(*) FILTER (WHERE s.parser_version IS DISTINCT FROM %(version)s
                                      AND s.attempt_version = %(version)s
                                      AND s.attempts BETWEEN 1 AND %(max_attempts)s - 1),
                   count(*),
                   (SELECT count(*) FROM sec_inline_scale_renormalise)
            FROM pop p LEFT JOIN sec_inline_scale_scan s USING (accession_number)
            """,
            {
                "concept": CONCEPT,
                "scope_start": SCOPE_START,
                "version": PARSER_VERSION,
                "max_attempts": MAX_ATTEMPTS,
            },
        ).fetchone()
    assert row is not None
    done, exhausted, retrying, total, queue = row
    pending = total - done - exhausted - retrying
    return {"done": done, "pending": pending, "retrying": retrying, "exhausted": exhausted}, queue


def run_dps_inline_scale_scan(
    conn: psycopg.Connection[tuple],
    provider: ScanProvider,
    *,
    today: date | None = None,
    clock: Callable[[], float] = time.monotonic,
    start_cap: timedelta = START_CAP,
    drain: bool = False,
) -> ScanResult:
    """One run: the drain (when ``drain``), then every eligible accession in order until :data:`START_CAP` of starts,
    then the work-state report."""
    result = ScanResult()
    deadline = clock() + start_cap.total_seconds()
    if drain:
        result.drain = drain_renormalise_queue(conn, admit_until=clock() + start_cap.total_seconds() / 2, clock=clock)
    targets = select_targets(conn, today=today or datetime.now(UTC).date())
    for index, target in enumerate(targets):
        if clock() >= deadline:
            result.deferred = len(targets) - index
            break
        result.attempted += 1
        # The heartbeat `_tracked_job` reads: without it a healthy 45-minute run reads `mid_flight_stuck` and is not
        # protected from a dev reload.
        report_progress(index, len(targets))
        try:
            documents = fetch_document_set(provider, target)
            result.documents_fetched += len(documents)
            occurrences = parse_document_set(documents)
        except Exception as exc:  # noqa: BLE001 — any fetch/parse failure is one counted attempt, bounded by MAX_ATTEMPTS
            logger.info("dps inline scale scan: %s failed: %r", target.accession_number, exc)
            if record_failure(conn, target, repr(exc)):
                result.failed += 1
            else:
                result.superseded += 1
            continue
        enqueued = publish_success(conn, target, occurrences, len(documents))
        if enqueued is None:
            result.superseded += 1
        else:
            result.succeeded += 1
            result.instruments_enqueued += enqueued
    report_progress(result.attempted, len(targets), force=True)
    result.work_state, result.queue_length = work_state(conn)
    return result


def drain_renormalise_queue(
    conn: psycopg.Connection[tuple],
    *,
    admit_until: float | None,
    clock: Callable[[], float] = time.monotonic,
) -> DrainResult:
    """Normalise the queued instruments in ``revision`` order; none is started at or after ``admit_until``.

    ``admit_until=None`` drains every row read at the start (the spec's uncapped enablement run). The delete is
    conditional on the revision read: a scan that re-queued the instrument meanwhile took a new revision, and that
    row stays for the next drain.

    Refuses with the flag off (draining would re-normalise without the decisions and then discard the queue), and on
    a connection inside a transaction (each instrument must commit on its own, so its lock is released at once).
    """
    from app.config import settings
    from app.services.fundamentals import normalize_financial_periods  # cycle: fundamentals imports this module

    if not settings.dps_inline_scale_enabled:
        raise RuntimeError("EBULL_DPS_INLINE_SCALE_ENABLED is off: draining would discard the queue uncorrected")
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("drain_renormalise_queue needs a connection outside a transaction")
    with conn.transaction():
        queued = conn.execute(
            "SELECT instrument_id, revision FROM sec_inline_scale_renormalise ORDER BY revision"
        ).fetchall()
    result = DrainResult()
    for index, (instrument_id, revision) in enumerate(queued):
        if admit_until is not None and clock() >= admit_until:
            result.deferred = len(queued) - index
            break
        report_progress(index, len(queued))
        summary = normalize_financial_periods(conn, [instrument_id], dps_inline_scale=True)
        with conn.transaction():
            if summary.instruments_failed:
                conn.execute(
                    "UPDATE sec_inline_scale_renormalise SET failures = failures + 1 WHERE instrument_id = %s",
                    (instrument_id,),
                )
                result.failed += 1
                continue
            deleted = conn.execute(
                "DELETE FROM sec_inline_scale_renormalise WHERE instrument_id = %s AND revision = %s",
                (instrument_id, revision),
            ).rowcount
        result.normalised += 1
        result.requeued += int(deleted == 0)
    return result
