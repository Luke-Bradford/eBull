"""``sec_dps_inline_scale_scan``: the inline ``scale`` of every DPS occurrence, per accession (#3947).

Spec: ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §The scan, as Amendment 1 overrides it.
Tables: sql/514, sql/515.

* **Population (A1):** ``sec_inline_scale_population``, insert-only (accession, instrument) pairs holding
  ``CommonStockDividendsPerShareDeclared``. Written by every companyfacts write into raw
  (:func:`record_population`, from ``upsert_facts_for_instrument``) and by each run's seed (:func:`seed_population`)
  from raw and from the ``source_ref`` of ``sec_edgar`` canonical rows. Raw is a retention window, so the table
  outlives it. An accession is in scope when its rows are dated on or after 2019-06-15 (spec policy 6); undated
  accessions are counted, never scanned. Class ``raw``: the accession holds the concept in raw now; else ``history``.
* **Discovery (A1 (3)):** each run fetches companyfacts for population instruments with no ``discovered_at`` and
  writes it through ``upsert_facts_for_instrument(complete_payload=True)``; a failure backs off ``2^attempts`` hours,
  and :data:`MAX_ATTEMPTS` failures exhaust it.
* **Eligible:** ``parser_version`` is not the current one, and either ``attempt_version`` is not the current one
  (never attempted at this version) or fewer than :data:`MAX_ATTEMPTS` failures with the backoff elapsed.
* **Attempt:** the ``index.json`` listing, then every ``.htm``/``.html``/``.xhtml`` file of it except the EDGAR
  Renderer pages ``FilingSummary.xml`` names, through the provider's shared SEC limiter; the bodies are parsed
  (:mod:`app.services.inline_xbrl_dps`) and discarded.
* **Success** replaces the accession's occurrences and active generation in one transaction, and enqueues the
  accession's instruments in ``sec_inline_scale_renormalise`` when the old or new generation holds a positive-scale
  or an unresolved occurrence, or, failing that, those instruments that are candidates (A4: any of their population
  accessions' active generation holds one). **Failure** changes only the work state: ``attempts`` + 1, the next
  attempt ``2^attempts`` hours later, ``last_error``.
* **Publication is conditional** on the work state read when the run selected the accession (``attempt_version``,
  ``attempts``), re-read under a row lock; a mismatch is counted ``superseded`` and nothing is written. The job's
  lane lock already keeps two scans from overlapping, so this guards a stale run, not a race.
* **Order (A1):** the drain, the seed, discovery, then accessions: filings from the last :data:`RECENT_DAYS` days,
  the ``raw`` backlog, ``history`` accessions of candidate instruments, the remaining ``history``; oldest first within
  each. No accession is started after :data:`START_CAP`; the drain and discovery admit none after half of it.

* **Drain first** (only when the caller passes ``drain``, i.e. ``EBULL_DPS_INLINE_SCALE_ENABLED`` is on): queued
  instruments in ``revision`` order. Each is refilled from companyfacts and normalised in one locked transaction
  (:func:`refill_and_normalise`, A2), then marked ``drained_revision = revision`` only at the revision read (A4: the
  row stays as the ledger), so work queued meanwhile stays queued; a failure counts in ``failures`` and the row stays
  queued for the next run.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Final, Protocol

import psycopg

from app.providers.fundamentals import XbrlFact
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


class FactsProvider(Protocol):
    """The companyfacts fetch the daily path uses (``SecFundamentalsProvider``)."""

    def extract_facts_and_catalog(self, symbol: str, cik: str) -> tuple[Sequence[XbrlFact], Sequence[object]]: ...


@dataclass(frozen=True)
class Target:
    accession_number: str
    filed_date: date
    issuer_cik: str | None
    instrument_ids: tuple[int, ...]
    seen_attempt_version: str | None
    seen_attempts: int | None


@dataclass
class WorkState:
    """The run note's coverage (A1): accessions by class and work state, undated accessions, discovery of
    population instruments, and the queue (instruments with ``drained_revision`` behind ``revision``)."""

    classes: dict[str, dict[str, int]] = field(default_factory=dict)
    undated: int = 0
    discovery: dict[str, int] = field(default_factory=dict)
    queue: int = 0

    def incomplete(self) -> bool:
        """Any scope accession still pending or retrying, in either class."""
        return any(c["pending"] or c["retrying"] for name, c in self.classes.items() if name in ("raw", "history"))

    def digest(self) -> str:
        def counts(d: Mapping[str, int]) -> str:
            return ", ".join(f"{k} {v}" for k, v in d.items())

        classes = "; ".join(f"{name}: {counts(c)}" for name, c in self.classes.items())
        return f"{classes}; undated {self.undated}; discovery: {counts(self.discovery)}; queue {self.queue}"


@dataclass
class ScanResult:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    superseded: int = 0
    deferred: int = 0
    documents_fetched: int = 0
    instruments_enqueued: int = 0
    seeded: int = 0
    work_state: WorkState = field(default_factory=WorkState)
    drain: DrainResult | None = None
    discovery: DiscoveryResult | None = None

    def digest(self) -> str:
        drained = "drain off" if self.drain is None else self.drain.digest()
        discovered = "discovery none" if self.discovery is None else self.discovery.digest()
        return (
            f"{drained}; seeded {self.seeded}; {discovered}; "
            f"attempted {self.attempted} (ok {self.succeeded}, failed {self.failed}, superseded {self.superseded}),"
            f" deferred {self.deferred}, documents {self.documents_fetched},"
            f" enqueued {self.instruments_enqueued}; population: {self.work_state.digest()}"
        )


@dataclass
class DrainResult:
    normalised: int = 0
    failed: int = 0  # every failure, fetch failures included
    fetch_failed: int = 0
    requeued: int = 0
    deferred: int = 0

    def digest(self) -> str:
        return (
            f"drained {self.normalised} (failed {self.failed}, of which fetch {self.fetch_failed};"
            f" re-queued meanwhile {self.requeued}), drain deferred {self.deferred}"
        )


@dataclass
class DiscoveryResult:
    discovered: int = 0
    failed: int = 0
    deferred: int = 0
    write_failed: int = 0

    def digest(self) -> str:
        return (
            f"discovered {self.discovered} (failed {self.failed}, write failed {self.write_failed}),"
            f" discovery deferred {self.deferred}"
        )


# A4 candidate: a population instrument with an accession whose active generation holds a positive-scale or an
# unresolved occurrence. Occurrence rows exist only for an accession's active generation (publication replaces them).
# `{iid}` is a column expression of the enclosing query, never input.
_CANDIDATE_SQL: Final = """
EXISTS (SELECT 1 FROM sec_inline_scale_population cp
        JOIN sec_inline_dps_occurrence co ON co.accession_number = cp.accession_number
        WHERE cp.instrument_id = {iid} AND (co.scale > 0 OR co.unresolved IS NOT NULL))
"""

# Enqueue `iids` (sorted, so concurrent enqueuers lock in one order): all of them when `all`, else the candidates.
# Always a fresh revision: work queued while a drain holds an older one stays queued, and a drained row (ledger)
# is queued again.
_ENQUEUE_SQL: Final = (
    """
    INSERT INTO sec_inline_scale_renormalise (instrument_id, revision)
    SELECT i, nextval('sec_inline_scale_renormalise_revision')
    FROM unnest(%(iids)s::bigint[]) AS i
    WHERE %(all)s OR """
    + _CANDIDATE_SQL.format(iid="i")
    + """
    ORDER BY i
    ON CONFLICT (instrument_id) DO UPDATE SET revision = EXCLUDED.revision
    """
)


def _enqueue(cur: psycopg.Cursor[tuple], instrument_ids: Sequence[int], *, all_: bool) -> int:
    if not instrument_ids:
        return 0
    cur.execute(_ENQUEUE_SQL, {"iids": sorted(set(instrument_ids)), "all": all_})
    return cur.rowcount


def _enqueue_new_membership(cur: psycopg.Cursor[tuple], pairs: Sequence[tuple[str, int]]) -> int:
    """A1 (1): a new (accession, instrument) pair whose accession already has an active generation runs the A4
    predicate for that instrument, so an already-scanned accession still reaches a newly attached instrument. For a
    new pair the accession's own generation is one of the instrument's, so the predicate is candidacy."""
    if not pairs:
        return 0
    rows = cur.execute(
        "SELECT DISTINCT n.iid FROM unnest(%s::text[], %s::bigint[]) AS n(acc, iid)"
        " JOIN sec_inline_scale_scan s ON s.accession_number = n.acc WHERE s.outcome IS NOT NULL",
        ([a for a, _ in pairs], [i for _, i in pairs]),
    ).fetchall()
    return _enqueue(cur, [r[0] for r in rows], all_=False)


def record_population(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accessions: Mapping[str, date | None],
    complete_payload: bool,
) -> int:
    """A1 (1), in the caller's transaction: record the instrument's accessions holding the concept (filling a NULL
    date once one is known), enqueue on new membership, and set ``discovered_at`` when the caller wrote the CIK's
    whole companyfacts payload. Returns the instruments enqueued (0 or 1)."""
    enqueued = 0
    if not (accessions or complete_payload):
        return enqueued
    with conn.cursor() as cur:
        if accessions:
            accs = sorted(accessions)
            dates = [accessions[a] for a in accs]
            new = cur.execute(
                """
                INSERT INTO sec_inline_scale_population (accession_number, instrument_id, filed_date)
                SELECT a, %(iid)s, d FROM unnest(%(accs)s::text[], %(dates)s::date[]) AS t(a, d) ORDER BY a
                ON CONFLICT (accession_number, instrument_id) DO NOTHING
                RETURNING accession_number
                """,
                {"iid": instrument_id, "accs": accs, "dates": dates},
            ).fetchall()
            cur.execute(
                """
                UPDATE sec_inline_scale_population p SET filed_date = t.d
                FROM unnest(%(accs)s::text[], %(dates)s::date[]) AS t(a, d)
                WHERE p.accession_number = t.a AND p.instrument_id = %(iid)s
                  AND p.filed_date IS NULL AND t.d IS NOT NULL
                """,
                {"iid": instrument_id, "accs": accs, "dates": dates},
            )
            enqueued = _enqueue_new_membership(cur, [(r[0], instrument_id) for r in new])
        if complete_payload:
            cur.execute(
                """
                INSERT INTO sec_inline_scale_discovery (instrument_id, discovered_at) VALUES (%s, now())
                ON CONFLICT (instrument_id) DO UPDATE SET discovered_at = EXCLUDED.discovered_at
                """,
                (instrument_id,),
            )
    return enqueued


# A1 (2): pairs from durable records — raw holding the concept, and the `source_ref` (comma-separated accessions) of
# `sec_edgar` canonical rows holding `dps_declared`.
_SEED_SQL: Final = """
SELECT accession_number, instrument_id FROM financial_facts_raw WHERE concept = %(concept)s
UNION
SELECT trim(a), p.instrument_id
FROM financial_periods p, unnest(string_to_array(p.source_ref, ',')) a
WHERE p.source = 'sec_edgar' AND p.dps_declared IS NOT NULL AND trim(a) <> ''
ORDER BY 1, 2
"""

# Every undated member, seeded or not, takes a date once one is known: raw (any concept), else the manifest, else the
# earliest `sec` filing event.
_DATE_UNDATED_SQL: Final = """
UPDATE sec_inline_scale_population p SET filed_date = d.filed
FROM (
    SELECT u.accession_number, u.instrument_id, COALESCE(
        (SELECT max(r.filed_date) FROM financial_facts_raw r
         WHERE r.instrument_id = u.instrument_id AND r.accession_number = u.accession_number),
        (SELECT m.filed_at::date FROM sec_filing_manifest m WHERE m.accession_number = u.accession_number),
        (SELECT min(e.filing_date) FROM filing_events e
         WHERE e.provider = 'sec' AND e.provider_filing_id = u.accession_number)) AS filed
    FROM sec_inline_scale_population u WHERE u.filed_date IS NULL
) d
WHERE p.accession_number = d.accession_number AND p.instrument_id = d.instrument_id AND d.filed IS NOT NULL
"""


def seed_population(conn: psycopg.Connection[tuple]) -> int:
    """A1 (2), one transaction: insert the durable-record pairs not yet in the population, date every undated member
    whose date is now known, enqueue on new membership, and give every population instrument a discovery row.
    Returns the pairs inserted."""
    with conn.transaction(), conn.cursor() as cur:
        rows = cur.execute(_SEED_SQL, {"concept": CONCEPT}).fetchall()
        new = cur.execute(
            """
            INSERT INTO sec_inline_scale_population (accession_number, instrument_id)
            SELECT a, i FROM unnest(%s::text[], %s::bigint[]) AS t(a, i) ORDER BY a, i
            ON CONFLICT (accession_number, instrument_id) DO NOTHING
            RETURNING accession_number, instrument_id
            """,
            ([r[0] for r in rows], [r[1] for r in rows]),
        ).fetchall()
        cur.execute(_DATE_UNDATED_SQL)
        _enqueue_new_membership(cur, [(r[0], r[1]) for r in new])
        cur.execute(
            "INSERT INTO sec_inline_scale_discovery (instrument_id)"
            " SELECT DISTINCT instrument_id FROM sec_inline_scale_population ORDER BY 1 ON CONFLICT DO NOTHING"
        )
    return len(new)


# The scope population: accessions dated on or after the scope start, with every population instrument, the class
# (`in_raw`: the concept is in raw now) and whether any of its instruments is a candidate (A4).
_POPULATION_SQL: Final = (
    """
WITH members AS (
    SELECT p.accession_number, max(p.filed_date) AS filed_date,
           array_agg(p.instrument_id ORDER BY p.instrument_id) AS instrument_ids
    FROM sec_inline_scale_population p
    GROUP BY p.accession_number
    HAVING max(p.filed_date) >= %(scope_start)s
),
pop AS (
    SELECT m.*,
           EXISTS (SELECT 1 FROM financial_facts_raw f
                   WHERE f.instrument_id = ANY (m.instrument_ids) AND f.accession_number = m.accession_number
                     AND f.concept = %(concept)s) AS in_raw,
           EXISTS (SELECT 1 FROM unnest(m.instrument_ids) AS mi(iid) WHERE """
    + _CANDIDATE_SQL.format(iid="mi.iid")
    + """) AS candidate
    FROM members m
)
"""
)


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
            ORDER BY CASE WHEN p.filed_date >= %(recent_from)s THEN 0 WHEN p.in_raw THEN 1
                          WHEN p.candidate THEN 2 ELSE 3 END,
                     p.filed_date, p.accession_number
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
        # A4: every instrument when either generation holds a positive-scale or unresolved occurrence; else the
        # candidates, since a rescan can flip one of their leaves from unverified to verified and unlock a group.
        changed = bool(old is not None and old[0]) or _needs_renormalise(occurrences or ())
        return _enqueue(cur, target.instrument_ids, all_=changed)


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


def work_state(conn: psycopg.Connection[tuple]) -> WorkState:
    """The run note's coverage: scope accessions by class (``raw``, ``history``, and the ``history`` accessions of
    candidate instruments as ``history_candidate``, a subset) and work state at the current version; the undated
    accessions; discovery of population instruments; the queue length."""
    params = {"concept": CONCEPT, "scope_start": SCOPE_START, "version": PARSER_VERSION, "max_attempts": MAX_ATTEMPTS}
    with conn.transaction():
        rows = conn.execute(
            _POPULATION_SQL
            + """
            , classed AS (
                SELECT CASE WHEN p.in_raw THEN 'raw' ELSE 'history' END AS class, p.candidate,
                       s.parser_version, s.attempt_version, s.attempts
                FROM pop p LEFT JOIN sec_inline_scale_scan s USING (accession_number)
            ), tagged AS (
                SELECT class, parser_version, attempt_version, attempts FROM classed
                UNION ALL
                SELECT 'history_candidate', parser_version, attempt_version, attempts FROM classed
                WHERE class = 'history' AND candidate
            )
            SELECT class,
                   count(*) FILTER (WHERE parser_version = %(version)s),
                   count(*) FILTER (WHERE parser_version IS DISTINCT FROM %(version)s
                                      AND attempt_version = %(version)s AND attempts >= %(max_attempts)s),
                   count(*) FILTER (WHERE parser_version IS DISTINCT FROM %(version)s
                                      AND attempt_version = %(version)s
                                      AND attempts BETWEEN 1 AND %(max_attempts)s - 1),
                   count(*)
            FROM tagged GROUP BY class
            """,
            params,
        ).fetchall()
        undated, queue = conn.execute(
            """
            SELECT (SELECT count(*) FROM (SELECT 1 FROM sec_inline_scale_population GROUP BY accession_number
                                          HAVING max(filed_date) IS NULL) u),
                   (SELECT count(*) FROM sec_inline_scale_renormalise
                    WHERE drained_revision IS DISTINCT FROM revision)
            """
        ).fetchone() or (0, 0)
        discovery = conn.execute(
            """
            SELECT count(*) FILTER (WHERE d.discovered_at IS NOT NULL),
                   count(*) FILTER (WHERE d.discovered_at IS NULL AND COALESCE(d.attempts, 0) = 0),
                   count(*) FILTER (WHERE d.discovered_at IS NULL AND d.attempts BETWEEN 1 AND %(max_attempts)s - 1),
                   count(*) FILTER (WHERE d.discovered_at IS NULL AND d.attempts >= %(max_attempts)s)
            FROM (SELECT DISTINCT instrument_id FROM sec_inline_scale_population) p
            LEFT JOIN sec_inline_scale_discovery d USING (instrument_id)
            """,
            params,
        ).fetchone() or (0, 0, 0, 0)
    state = WorkState(undated=undated, queue=queue)
    by_class = {r[0]: r[1:] for r in rows}
    for name in ("raw", "history", "history_candidate"):
        done, exhausted, retrying, total = by_class.get(name, (0, 0, 0, 0))
        pending = total - done - exhausted - retrying
        state.classes[name] = {"done": done, "pending": pending, "retrying": retrying, "exhausted": exhausted}
    state.discovery = dict(zip(("discovered", "pending", "retrying", "exhausted"), discovery, strict=True))
    return state


def run_discovery(
    conn: psycopg.Connection[tuple],
    facts_provider: FactsProvider,
    *,
    admit_until: float,
    clock: Callable[[], float] = time.monotonic,
) -> DiscoveryResult:
    """A1 (3): fetch companyfacts for each due population instrument with no ``discovered_at`` (instrument order) and
    write it through ``upsert_facts_for_instrument(complete_payload=True)``, one transaction each; none is started at
    or after ``admit_until``. An empty payload (no companyfacts at SEC) still discovers: there is nothing to capture.
    A fetch or parse failure backs off ``2^attempts`` hours; :data:`MAX_ATTEMPTS` failures exhaust it. A write failure
    is counted and leaves the discovery row unchanged, so it burns no attempt and the run continues."""
    from app.services.fundamentals import (  # cycle: fundamentals imports this module
        finish_ingestion_run,
        start_ingestion_run,
        upsert_facts_for_instrument,
    )

    result = DiscoveryResult()
    with conn.transaction():
        due = conn.execute(
            """
            SELECT d.instrument_id, i.symbol,
                   (SELECT ei.identifier_value FROM external_identifiers ei
                    WHERE ei.instrument_id = d.instrument_id AND ei.provider = 'sec' AND ei.identifier_type = 'cik'
                      AND ei.is_primary LIMIT 1)
            FROM sec_inline_scale_discovery d JOIN instruments i USING (instrument_id)
            WHERE d.discovered_at IS NULL AND d.attempts < %(max_attempts)s
              AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= now())
              AND EXISTS (SELECT 1 FROM sec_inline_scale_population p WHERE p.instrument_id = d.instrument_id)
            ORDER BY d.instrument_id
            """,
            {"max_attempts": MAX_ATTEMPTS},
        ).fetchall()
    if not due:
        return result
    with conn.transaction():
        run_id = start_ingestion_run(
            conn, source="sec_edgar", endpoint="/api/xbrl/companyfacts", instrument_count=len(due)
        )
    upserted = 0
    error: str | None = None
    try:
        for index, (instrument_id, symbol, cik) in enumerate(due):
            if clock() >= admit_until:
                result.deferred = len(due) - index
                break
            report_progress(index, len(due))
            try:
                if cik is None:
                    raise LookupError("no_primary_sec_cik")
                facts, _catalog = facts_provider.extract_facts_and_catalog(symbol, cik)
            except Exception as exc:  # noqa: BLE001 — a fetch/parse failure is one counted attempt, bounded by MAX_ATTEMPTS
                logger.info("dps inline scale discovery: instrument %s failed: %r", instrument_id, exc)
                with conn.transaction():
                    conn.execute(
                        """
                        UPDATE sec_inline_scale_discovery
                        SET attempts = attempts + 1,
                            next_attempt_at = now() + make_interval(hours => (2 ^ (attempts + 1))::int),
                            last_error = %s
                        WHERE instrument_id = %s
                        """,
                        (repr(exc)[:2000], instrument_id),
                    )
                result.failed += 1
                continue
            # A write error is not the instrument's attempt to burn: it is counted, the discovery row is left as it was
            # (the instrument is due again next run), and the run carries on to the scan.
            try:
                with conn.transaction():
                    upserted += upsert_facts_for_instrument(
                        conn, instrument_id=instrument_id, facts=facts, ingestion_run_id=run_id, complete_payload=True
                    )[0]
            except psycopg.Error as exc:
                logger.warning("dps inline scale discovery: instrument %s write failed: %r", instrument_id, exc)
                result.write_failed += 1
                continue
            result.discovered += 1
    except BaseException as exc:
        error = repr(exc)[:2000]
        raise
    finally:
        failed = result.failed + result.write_failed
        with conn.transaction():
            finish_ingestion_run(
                conn,
                run_id=run_id,
                status="failed"
                if error
                else "success"
                if failed == 0
                else ("partial" if result.discovered else "failed"),
                rows_upserted=upserted,
                error=error,
            )
    return result


def run_dps_inline_scale_scan(
    conn: psycopg.Connection[tuple],
    provider: ScanProvider,
    *,
    facts_provider: FactsProvider,
    today: date | None = None,
    clock: Callable[[], float] = time.monotonic,
    start_cap: timedelta = START_CAP,
    drain: bool = False,
) -> ScanResult:
    """One run: the drain (when ``drain``), the population seed, discovery, then every eligible accession in order
    until :data:`START_CAP` of starts, then the work-state report. The drain and discovery admit nothing after half
    of the cap.

    ``drain`` is the caller's one read of ``EBULL_DPS_INLINE_SCALE_ENABLED`` (the job passes it), handed to the drain
    as ``enabled`` so a run never reads the flag twice.

    Refuses a connection with an open transaction: every network fetch (companyfacts, filing documents) runs between
    the run's own short transactions, never inside one."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("run_dps_inline_scale_scan needs a connection outside a transaction")
    result = ScanResult()
    start = clock()
    deadline = start + start_cap.total_seconds()
    half = start + start_cap.total_seconds() / 2
    if drain:
        result.drain = drain_renormalise_queue(
            conn, facts_provider=facts_provider, admit_until=half, clock=clock, enabled=True
        )
    result.seeded = seed_population(conn)
    result.discovery = run_discovery(conn, facts_provider, admit_until=half, clock=clock)
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
    result.work_state = work_state(conn)
    return result


class RefillFailed(Exception):
    """``refill_and_normalise`` could not refill or normalise; the caller's transaction must roll back."""


def fetch_companyfacts(
    conn: psycopg.Connection[tuple], facts_provider: FactsProvider, instrument_id: int
) -> Sequence[XbrlFact]:
    """Amendment 1 A2 step 1: the instrument's whole companyfacts payload, from its primary SEC CIK as the daily path
    fetches it. Run it outside any transaction: no SEC request may hold a lock. Raises on a fetch or parse failure; an
    empty payload (no companyfacts at SEC) is returned as empty."""
    with conn.transaction():
        row = conn.execute(
            """
            SELECT i.symbol,
                   (SELECT ei.identifier_value FROM external_identifiers ei
                    WHERE ei.instrument_id = i.instrument_id AND ei.provider = 'sec' AND ei.identifier_type = 'cik'
                      AND ei.is_primary LIMIT 1)
            FROM instruments i WHERE i.instrument_id = %s
            """,
            (instrument_id,),
        ).fetchone()
    if row is None or row[1] is None:
        raise LookupError("no_primary_sec_cik")
    facts, _catalog = facts_provider.extract_facts_and_catalog(row[0], row[1])
    return facts


def refill_and_normalise(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    facts: Sequence[XbrlFact],
    *,
    ingestion_run_id: int,
    dps_inline_scale: bool,
) -> int:
    """Amendment 1 A2 step 2, inside the caller's open transaction: take the instrument's normaliser lock, upsert the
    prefetched companyfacts payload (which records the population, A1 (1)), then normalise over the refilled raw.

    The lock is held to the end of the caller's transaction, and the retention sweep takes the same lock, so no sweep
    removes facts between the upsert and the normalisation. Raises :class:`RefillFailed` when the normalisation
    fails, so the caller rolls the upsert back with it: only a successful refill and normalisation may dequeue.
    Returns the facts upserted."""
    from app.services.fundamentals import (  # cycle: fundamentals imports this module
        lock_instrument_for_normalize,
        normalize_financial_periods,
        upsert_facts_for_instrument,
    )

    if conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("refill_and_normalise runs inside the caller's transaction")
    lock_instrument_for_normalize(conn, instrument_id)
    upserted, _ = upsert_facts_for_instrument(
        conn, instrument_id=instrument_id, facts=facts, ingestion_run_id=ingestion_run_id, complete_payload=True
    )
    summary = normalize_financial_periods(conn, [instrument_id], dps_inline_scale=dps_inline_scale)
    if summary.instruments_failed:
        raise RefillFailed(f"normalisation of instrument {instrument_id} failed")
    return upserted


def drain_renormalise_queue(
    conn: psycopg.Connection[tuple],
    *,
    facts_provider: FactsProvider,
    admit_until: float | None,
    clock: Callable[[], float] = time.monotonic,
    enabled: bool | None = None,
) -> DrainResult:
    """Refill and normalise the queued instruments in ``revision`` order; none is started at or after ``admit_until``.

    Each instrument (Amendment 1 A2): fetch its companyfacts outside any transaction, then one transaction runs
    :func:`refill_and_normalise` and marks the row drained. A fetch failure, a write error or a failed normalisation
    rolls that transaction back, increments ``failures`` and leaves the row queued.

    ``admit_until=None`` drains every queued row read at the start (the spec's uncapped enablement run). A drained
    row is kept as the ledger (A4): ``drained_revision`` is set to the revision read, conditioned on it, so a scan
    that re-queued the instrument meanwhile took a new revision and the row stays queued for the next drain.

    Refuses when ``enabled`` is false (``None`` reads ``EBULL_DPS_INLINE_SCALE_ENABLED`` once): with the flag off,
    every other normaliser caller reads stored values, so a drain would correct instruments the next normalisation
    reverts. Refuses on a connection with an open transaction: each instrument's normalisation and ledger mark must
    commit on their own, so its advisory lock is released at once. An autocommit connection, or an idle one without
    autocommit, both qualify.
    """
    from app.config import settings
    from app.services.fundamentals import (  # cycle: fundamentals imports this module
        finish_ingestion_run,
        start_ingestion_run,
    )

    if not (settings.dps_inline_scale_enabled if enabled is None else enabled):
        raise RuntimeError("EBULL_DPS_INLINE_SCALE_ENABLED is off: draining would discard the queue uncorrected")
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("drain_renormalise_queue needs a connection outside a transaction")
    with conn.transaction():
        queued = conn.execute(
            "SELECT instrument_id, revision FROM sec_inline_scale_renormalise"
            " WHERE drained_revision IS DISTINCT FROM revision ORDER BY revision"
        ).fetchall()
    result = DrainResult()
    upserted = 0
    if not queued:
        return result
    with conn.transaction():
        run_id = start_ingestion_run(
            conn, source="sec_edgar", endpoint="/api/xbrl/companyfacts", instrument_count=len(queued)
        )
    error: str | None = None
    try:
        for index, (instrument_id, revision) in enumerate(queued):
            if admit_until is not None and clock() >= admit_until:
                result.deferred = len(queued) - index
                break
            report_progress(index, len(queued))
            try:
                facts = fetch_companyfacts(conn, facts_provider, instrument_id)
            except Exception as exc:  # noqa: BLE001 — a fetch/parse failure keeps the row; the next drain retries it
                logger.info("dps inline scale drain: instrument %s fetch failed: %r", instrument_id, exc)
                _count_drain_failure(conn, instrument_id)
                result.failed += 1
                result.fetch_failed += 1
                continue
            try:
                with conn.transaction():
                    written = refill_and_normalise(
                        conn, instrument_id, facts, ingestion_run_id=run_id, dps_inline_scale=True
                    )
                    marked = conn.execute(
                        "UPDATE sec_inline_scale_renormalise SET drained_revision = revision"
                        " WHERE instrument_id = %s AND revision = %s",
                        (instrument_id, revision),
                    ).rowcount
            except (RefillFailed, psycopg.Error) as exc:
                logger.warning("dps inline scale drain: instrument %s refill failed: %r", instrument_id, exc)
                _count_drain_failure(conn, instrument_id)
                result.failed += 1
                continue
            upserted += written
            result.normalised += 1
            result.requeued += int(marked == 0)
    except BaseException as exc:
        error = repr(exc)[:2000]
        raise
    finally:
        with conn.transaction():
            finish_ingestion_run(
                conn,
                run_id=run_id,
                status="failed"
                if error
                else "success"
                if result.failed == 0
                else ("partial" if result.normalised else "failed"),
                rows_upserted=upserted,
                error=error,
            )
    return result


def _count_drain_failure(conn: psycopg.Connection[tuple], instrument_id: int) -> None:
    with conn.transaction():
        conn.execute(
            "UPDATE sec_inline_scale_renormalise SET failures = failures + 1 WHERE instrument_id = %s",
            (instrument_id,),
        )
