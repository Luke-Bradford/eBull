"""SEC filing manifest — single source of truth for "is filing X on file?".

Issue #864 / spec §"sec_filing_manifest"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

#1131 backfill: :func:`tombstone_stale_failed_upserts` walks the
manifest looking for rows stuck in ``failed`` with a pre-#1131-shape
``upsert error:...`` message and promotes them to ``tombstoned`` once
they have been failing continuously for longer than ``age_hours``. The
sweep stops the legacy retry loop that hammered SEC every hour for
deterministic constraint violations on Form 4 / 8-K / 13D/G / DEF 14A
ingest before the manifest parsers learned to discriminate transient
errors from deterministic ones (see
``app/services/manifest_parsers/_classify.py``).

The manifest replaces the per-source bespoke joins against
``def14a_ingest_log`` / ``institutional_holdings_ingest_log`` /
``insider_filings.is_tombstone`` / ``unresolved_13f_cusips`` with one
canonical accession-level table.

Lifecycle (state machine on ``ingest_status``):

    pending  ─► fetched   (worker downloads body)
    fetched  ─► parsed    (parser succeeded, typed-table rows recorded)
    fetched  ─► tombstoned (intentionally not parseable: not-on-file,
                            no-table, partial — operator decided not to
                            chase further)
    fetched  ─► failed    (fetch / parse error; retryable per backoff)
    failed   ─► pending   (retry window opened; worker picks up via
                            iter_retryable)
    parsed   ─► pending   (rebuild flips back; preserves history)

Transitions are validated in ``transition_status`` so a bad call site
trips loudly instead of silently corrupting state.

Source naming uses the spec's ``sec_*`` / ``finra_*`` convention which
is COARSER than ``form`` (13D + 13D/A both map to ``sec_13d``) and
DIFFERENT from the legacy ``OwnershipSource`` enum in
``ownership_observations`` (``form4`` vs ``sec_form4``). The manifest
is the boundary at which the new naming is enforced; downstream
parsers translate to the legacy enum where they write observation rows
during the migration window.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg import sql

logger = logging.getLogger(__name__)


# #1131 backfill heuristic + retry-loop discriminator. Old-format
# manifest ``error`` strings from pre-#1131 parsers stored just
# ``"upsert error: <message>"`` without the exception class name. New-
# format strings carry ``"upsert error: <ExceptionClass>: <message>"``
# (or ``"upsert+tombstone error: <Class>: ..."`` /
# ``"upsert+log error: <Class>: ..."``). The sweep skips
# transient-shaped failures (``OperationalError`` /
# ``SerializationFailure`` / ``DeadlockDetected``) by *anchored prefix*
# match — Codex pre-push round 1: a substring-anywhere match would
# skip deterministic rows whose error message happened to mention the
# token (e.g. ``upsert error: CheckViolation: value "OperationalError"
# violates ...``). Mixed-format rows are tombstoned by age.
_BACKFILL_TRANSIENT_CLASS_TOKENS: tuple[str, ...] = (
    "OperationalError",
    "SerializationFailure",
    "DeadlockDetected",
)
_BACKFILL_ERROR_PREFIXES: tuple[str, ...] = (
    "upsert error: ",
    "upsert+tombstone error: ",
    "upsert+log error: ",
)
_BACKFILL_DEFAULT_AGE = timedelta(hours=24)
_BACKFILL_DEFAULT_LIMIT = 1000


def _error_has_transient_class_prefix(error_text: str) -> bool:
    """Return True iff ``error_text`` starts with one of the upsert
    prefixes immediately followed by a transient-class token.

    Anchored match per Codex pre-push round 1 — a plain
    ``token in error_text`` lookup would skip a deterministic row whose
    error message happened to *quote* the token (e.g. a CHECK
    constraint message referencing a column value that includes the
    literal string ``"OperationalError"``).
    """
    for prefix in _BACKFILL_ERROR_PREFIXES:
        if not error_text.startswith(prefix):
            continue
        tail = error_text[len(prefix) :]
        for token in _BACKFILL_TRANSIENT_CLASS_TOKENS:
            if tail.startswith(f"{token}:"):
                return True
    return False


ManifestSource = Literal[
    "sec_form3",
    "sec_form4",
    "sec_form5",
    "sec_13d",
    "sec_13g",
    "sec_13f_hr",
    "sec_def14a",
    "sec_n_port",
    "sec_n_csr",
    "sec_10k",
    "sec_10q",
    "sec_8k",
    "sec_xbrl_facts",
    "finra_short_interest",
]

ManifestSubjectType = Literal[
    "issuer",
    "institutional_filer",
    "blockholder_filer",
    "fund_series",
    "finra_universe",
]

IngestStatus = Literal["pending", "fetched", "parsed", "tombstoned", "failed"]
RawStatus = Literal["absent", "stored", "compacted"]


# Allowed transitions for ``transition_status``. Pinned here so a
# misbehaving caller (e.g. the worker dispatching to the wrong parser
# variant) trips a ValueError instead of leaving the manifest in an
# undefined state.
_ALLOWED_TRANSITIONS: dict[IngestStatus, frozenset[IngestStatus]] = {
    # ``pending`` self-loop: re-discovery (Atom + daily-index converge)
    # bumps last_attempted_at without state change. Other transitions
    # cover both the steady-state worker flow (pending -> fetched ->
    # parsed) and the backfill / write-through path (pending -> parsed
    # direct, when the body either lives in filing_raw_documents already
    # or was never separately stored).
    "pending": frozenset({"pending", "fetched", "parsed", "failed", "tombstoned"}),
    # ``fetched`` is transient — must transition out, no self-loop.
    "fetched": frozenset({"parsed", "tombstoned", "failed"}),
    # ``failed`` self-loop: a worker re-fetch that fails again should
    # update the error/retry without raising. Without the self-loop
    # in the allowed set, the second call would silently no-op (Claude
    # bot review #879 WARNING).
    "failed": frozenset({"pending", "fetched", "parsed", "tombstoned", "failed"}),
    # ``parsed`` -> ``pending`` is the only legal exit (rebuild path
    # in #872). No self-loop — re-parses must go through ``pending``
    # so the rewash gate is explicit.
    "parsed": frozenset({"pending"}),
    # ``tombstoned`` is terminal under normal flow; rebuild can
    # resurrect it back to pending for an explicit operator retry.
    # No self-loop.
    "tombstoned": frozenset({"pending"}),
}


@dataclass(frozen=True)
class ManifestRow:
    """Public dataclass mirroring one ``sec_filing_manifest`` row.

    Used by the round-trip helpers and tests; ``record_manifest_entry``
    takes discrete kwargs to keep the call site readable from the
    Atom-feed / submissions.json / daily-index discovery paths."""

    accession_number: str
    cik: str
    form: str
    source: ManifestSource
    subject_type: ManifestSubjectType
    subject_id: str
    instrument_id: int | None
    filed_at: datetime
    accepted_at: datetime | None
    primary_document_url: str | None
    is_amendment: bool
    amends_accession: str | None
    ingest_status: IngestStatus
    parser_version: str | None
    raw_status: RawStatus
    last_attempted_at: datetime | None
    next_retry_at: datetime | None
    error: str | None


def record_manifest_entry(
    conn: psycopg.Connection[Any],
    accession_number: str,
    *,
    cik: str,
    form: str,
    source: ManifestSource,
    subject_type: ManifestSubjectType,
    subject_id: str,
    instrument_id: int | None,
    filed_at: datetime,
    accepted_at: datetime | None = None,
    primary_document_url: str | None = None,
    is_amendment: bool = False,
    amends_accession: str | None = None,
) -> None:
    """UPSERT one manifest row keyed by ``accession_number``.

    Idempotent on re-discovery (Atom + daily-index + submissions.json
    all converge on the same accession). On conflict, refreshes the
    metadata fields without touching ``ingest_status`` /
    ``parser_version`` / ``raw_status`` / retry state — those are
    owned by ``transition_status``. ``updated_at`` is touched by the
    table trigger.

    Validates the issuer-vs-instrument cross-check at the call site:
    issuer-scoped rows MUST have ``instrument_id`` set; non-issuer rows
    MUST have ``instrument_id=None``. The DB CHECK constraint catches
    violations too; raising here gives a clearer error trail."""
    if subject_type == "issuer":
        if instrument_id is None:
            raise ValueError(
                f"record_manifest_entry: subject_type='issuer' requires instrument_id (accession={accession_number})"
            )
    else:
        if instrument_id is not None:
            raise ValueError(
                f"record_manifest_entry: subject_type={subject_type!r} must have instrument_id=None"
                f" (accession={accession_number})"
            )
    if not cik or not cik.strip():
        raise ValueError(f"record_manifest_entry: cik is required (accession={accession_number})")
    if not subject_id or not subject_id.strip():
        raise ValueError(f"record_manifest_entry: subject_id is required (accession={accession_number})")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_filing_manifest (
                accession_number, cik, form, source,
                subject_type, subject_id, instrument_id,
                filed_at, accepted_at, primary_document_url,
                is_amendment, amends_accession
            ) VALUES (
                %(accession)s, %(cik)s, %(form)s, %(source)s,
                %(stype)s, %(sid)s, %(iid)s,
                %(filed_at)s, %(accepted_at)s, %(url)s,
                %(is_amend)s, %(amends)s
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                cik = EXCLUDED.cik,
                form = EXCLUDED.form,
                source = EXCLUDED.source,
                subject_type = EXCLUDED.subject_type,
                subject_id = EXCLUDED.subject_id,
                instrument_id = EXCLUDED.instrument_id,
                filed_at = EXCLUDED.filed_at,
                accepted_at = COALESCE(EXCLUDED.accepted_at, sec_filing_manifest.accepted_at),
                primary_document_url = COALESCE(
                    EXCLUDED.primary_document_url, sec_filing_manifest.primary_document_url
                ),
                is_amendment = EXCLUDED.is_amendment,
                amends_accession = COALESCE(
                    EXCLUDED.amends_accession, sec_filing_manifest.amends_accession
                )
            """,
            {
                "accession": accession_number,
                "cik": cik.strip(),
                "form": form,
                "source": source,
                "stype": subject_type,
                "sid": subject_id.strip(),
                "iid": instrument_id,
                "filed_at": filed_at,
                "accepted_at": accepted_at,
                "url": primary_document_url,
                "is_amend": is_amendment,
                "amends": amends_accession,
            },
        )

    # #956: every manifest discovery write also seeds / updates the
    # scheduler row for its (subject_type, subject_id, source) triple.
    # Pre-#956 only the first-install drain (#937 / PR #957) called
    # ``seed_scheduler_from_manifest``; Atom fast-lane / daily-index
    # reconcile / per-CIK poll / targeted rebuild left new triples
    # scheduler-invisible until the next full bulk seed. Lazy import
    # to avoid a circular dependency — ``data_freshness`` imports
    # ``ManifestSource`` from this module.
    from app.services.data_freshness import seed_freshness_for_manifest_row

    seed_freshness_for_manifest_row(
        conn,
        subject_type=subject_type,
        subject_id=subject_id.strip(),
        source=source,
        cik=cik.strip(),
        instrument_id=instrument_id,
        accession_number=accession_number,
        filed_at=filed_at,
    )


def transition_status(
    conn: psycopg.Connection[Any],
    accession_number: str,
    *,
    ingest_status: IngestStatus,
    parser_version: str | None = None,
    error: str | None = None,
    raw_status: RawStatus | None = None,
    last_attempted_at: datetime | None = None,
    next_retry_at: datetime | None = None,
) -> None:
    """Atomic state-machine transition for one manifest row.

    Reads the current ``ingest_status`` then validates the transition
    against ``_ALLOWED_TRANSITIONS`` inside the same transaction so a
    concurrent transition can't sneak in between the read and the
    write. Raises ``ValueError`` on illegal transitions.

    ``parser_version`` is only stamped on transitions to ``parsed`` —
    a worker re-fetching after ``failed`` shouldn't blank out the
    parser version of the previous successful parse.

    ``error`` is cleared (set NULL) on success transitions and
    populated on ``failed`` / ``tombstoned``.

    ``last_attempted_at`` defaults to NOW() on every transition; the
    caller can override for backfill / fixture cases.

    ``next_retry_at`` is only meaningful on ``failed`` transitions —
    the worker reads ``WHERE ingest_status='failed' AND
    (next_retry_at IS NULL OR next_retry_at <= NOW())``.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT ingest_status, raw_status FROM sec_filing_manifest WHERE accession_number = %s FOR UPDATE",
            (accession_number,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"transition_status: manifest row missing for accession={accession_number}")
        current_status: IngestStatus = row[0]
        current_raw_status: RawStatus = row[1]
        allowed = _ALLOWED_TRANSITIONS[current_status]
        # Claude bot review on PR #879 (WARNING): same-status no-op
        # was previously short-circuited unconditionally; that masked
        # double-error writes (e.g. worker calls failed->failed twice
        # by accident, second call silently succeeds without going
        # through the validation path). Treat self-transition as a
        # legal-only-when-explicitly-allowed case.
        if ingest_status not in allowed:
            raise ValueError(
                f"transition_status: illegal transition {current_status!r} -> {ingest_status!r} "
                f"(accession={accession_number})"
            )

        # Codex pre-push catch on #948: reject evidence downgrades.
        # Once a row has ``raw_status in ('stored', 'compacted')`` we
        # never silently flip it back to ``'absent'`` — that would
        # break the #938 audit invariant for payload-backed parsers.
        # Callers that genuinely need to drop evidence (e.g. a
        # compaction job that loses bytes) should add a dedicated
        # state, not piggyback on ``transition_status``.
        if raw_status == "absent" and current_raw_status in ("stored", "compacted"):
            raise ValueError(
                f"transition_status: evidence downgrade rejected — "
                f"raw_status={current_raw_status!r} cannot transition to 'absent' "
                f"(accession={accession_number}, ingest_status={ingest_status!r})"
            )

        # Build SET clause dynamically so we only touch fields the
        # caller asked about, plus the always-bump ``last_attempted_at``.
        # Use ``psycopg.sql.SQL`` composition for the dynamic UPDATE so
        # static analysis (pyright LiteralString) passes; the clause
        # tokens are module-local constants — never user input — but
        # composing through ``sql.SQL`` keeps the type safety habit.
        set_clauses: list[sql.Composable] = [
            sql.SQL("ingest_status = %(status)s"),
            sql.SQL("last_attempted_at = COALESCE(%(attempt)s, NOW())"),
        ]
        params: dict[str, Any] = {
            "accession": accession_number,
            "status": ingest_status,
            "attempt": last_attempted_at,
        }

        if ingest_status == "parsed":
            # Clear error on success; stamp parser_version when given.
            set_clauses.append(sql.SQL("error = NULL"))
            if parser_version is not None:
                set_clauses.append(sql.SQL("parser_version = %(parser_version)s"))
                params["parser_version"] = parser_version
            # parsed implies the body has been fetched + stored
            if raw_status is not None:
                set_clauses.append(sql.SQL("raw_status = %(raw_status)s"))
                params["raw_status"] = raw_status
            set_clauses.append(sql.SQL("next_retry_at = NULL"))
        elif ingest_status == "fetched":
            if raw_status is not None:
                set_clauses.append(sql.SQL("raw_status = %(raw_status)s"))
                params["raw_status"] = raw_status
            set_clauses.append(sql.SQL("error = NULL"))
            set_clauses.append(sql.SQL("next_retry_at = NULL"))
        elif ingest_status == "failed":
            set_clauses.append(sql.SQL("error = %(error)s"))
            params["error"] = error
            set_clauses.append(sql.SQL("next_retry_at = %(next_retry)s"))
            params["next_retry"] = next_retry_at
            # PR #1126 Codex round 2 — a parser can store raw BEFORE
            # the parse phase fails. Without honouring raw_status on
            # the failed branch, the manifest column stays 'absent'
            # while the raw body physically exists in
            # ``filing_raw_documents``. That split breaks the audit
            # invariant (raw_status reflects the table) and causes
            # the worker to retry with `outcome.raw_status='absent'`
            # forever — store_raw re-fires on every retry and the
            # row never escapes the `failed` state.
            if raw_status is not None:
                set_clauses.append(sql.SQL("raw_status = %(raw_status)s"))
                params["raw_status"] = raw_status
        elif ingest_status == "tombstoned":
            set_clauses.append(sql.SQL("error = %(error)s"))
            params["error"] = error
            set_clauses.append(sql.SQL("next_retry_at = NULL"))
            # A tombstoned row may still hold body bytes from an
            # earlier fetch (parser failed on a malformed payload).
            # Allow the caller to update ``raw_status`` rather than
            # silently dropping it. (#948.)
            if raw_status is not None:
                set_clauses.append(sql.SQL("raw_status = %(raw_status)s"))
                params["raw_status"] = raw_status
        elif ingest_status == "pending":
            # Rebuild path: clear retry state; keep parser_version so
            # the rewash detector can compare against current.
            set_clauses.append(sql.SQL("error = NULL"))
            set_clauses.append(sql.SQL("next_retry_at = NULL"))
            # Atom re-discovery / rebuild may want to flag that the
            # body was retroactively persisted in a parallel job.
            # Allow ``raw_status`` updates rather than silently
            # dropping them. (#948.)
            if raw_status is not None:
                set_clauses.append(sql.SQL("raw_status = %(raw_status)s"))
                params["raw_status"] = raw_status

        update_query = sql.SQL(
            "UPDATE sec_filing_manifest SET {set_clause} WHERE accession_number = %(accession)s"
        ).format(set_clause=sql.SQL(", ").join(set_clauses))
        cur.execute(update_query, params)


def get_manifest_row(
    conn: psycopg.Connection[Any],
    accession_number: str,
) -> ManifestRow | None:
    """Fetch one manifest row by accession; returns None if absent."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, cik, form, source,
                   subject_type, subject_id, instrument_id,
                   filed_at, accepted_at, primary_document_url,
                   is_amendment, amends_accession,
                   ingest_status, parser_version, raw_status,
                   last_attempted_at, next_retry_at, error
            FROM sec_filing_manifest
            WHERE accession_number = %s
            """,
            (accession_number,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ManifestRow(**row)


def iter_pending(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
) -> Iterator[ManifestRow]:
    """Yield manifest rows ready for the worker to fetch + parse.

    Ordering: ``filed_at ASC`` so the oldest backlog drains first; the
    worker can ``LIMIT`` to bound per-tick work and rely on stable
    ordering across runs. ``source=None`` returns rows for every
    source (useful for the manifest worker that dispatches per-source
    internally)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if source is None:
            cur.execute(
                """
                SELECT accession_number, cik, form, source,
                       subject_type, subject_id, instrument_id,
                       filed_at, accepted_at, primary_document_url,
                       is_amendment, amends_accession,
                       ingest_status, parser_version, raw_status,
                       last_attempted_at, next_retry_at, error
                FROM sec_filing_manifest
                WHERE ingest_status = 'pending'
                ORDER BY filed_at ASC
                LIMIT %s
                """,
                (limit,),
            )
        else:
            cur.execute(
                """
                SELECT accession_number, cik, form, source,
                       subject_type, subject_id, instrument_id,
                       filed_at, accepted_at, primary_document_url,
                       is_amendment, amends_accession,
                       ingest_status, parser_version, raw_status,
                       last_attempted_at, next_retry_at, error
                FROM sec_filing_manifest
                WHERE ingest_status = 'pending' AND source = %s
                ORDER BY filed_at ASC
                LIMIT %s
                """,
                (source, limit),
            )
        for row in cur.fetchall():
            yield ManifestRow(**row)


def iter_retryable(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    limit: int = 100,
) -> Iterator[ManifestRow]:
    """Yield ``failed`` manifest rows whose backoff has elapsed.

    Predicate: ``ingest_status='failed' AND (next_retry_at IS NULL OR
    next_retry_at <= NOW())``. ``NULL`` retry time means no backoff
    set — eligible immediately. Worker should still respect a per-tick
    rate budget."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if source is None:
            cur.execute(
                """
                SELECT accession_number, cik, form, source,
                       subject_type, subject_id, instrument_id,
                       filed_at, accepted_at, primary_document_url,
                       is_amendment, amends_accession,
                       ingest_status, parser_version, raw_status,
                       last_attempted_at, next_retry_at, error
                FROM sec_filing_manifest
                WHERE ingest_status = 'failed'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC
                LIMIT %s
                """,
                (limit,),
            )
        else:
            cur.execute(
                """
                SELECT accession_number, cik, form, source,
                       subject_type, subject_id, instrument_id,
                       filed_at, accepted_at, primary_document_url,
                       is_amendment, amends_accession,
                       ingest_status, parser_version, raw_status,
                       last_attempted_at, next_retry_at, error
                FROM sec_filing_manifest
                WHERE ingest_status = 'failed'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                  AND source = %s
                ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC
                LIMIT %s
                """,
                (source, limit),
            )
        for row in cur.fetchall():
            yield ManifestRow(**row)


@dataclass(frozen=True)
class StaleFailedUpsertSweepResult:
    """Summary of one :func:`tombstone_stale_failed_upserts` invocation."""

    rows_scanned: int
    rows_tombstoned: int
    rows_skipped_transient: int
    rows_skipped_race: int


def tombstone_stale_failed_upserts(
    conn: psycopg.Connection[Any],
    *,
    age: timedelta = _BACKFILL_DEFAULT_AGE,
    limit: int = _BACKFILL_DEFAULT_LIMIT,
) -> StaleFailedUpsertSweepResult:
    """One-shot backfill (#1131): tombstone manifest rows that have
    been *idle* in ``ingest_status='failed'`` with an upsert-error
    shape for at least ``age``.

    Why: pre-#1131 every per-source manifest parser treated an upsert
    exception the same way — ``failed`` with a 1h backoff retry. A
    deterministic constraint violation on the typed-table upsert (bad
    date past a CHECK, malformed enum, FK miss) hammered SEC every
    hour for the same dead XML forever. PR #1131 split transient from
    deterministic at the *parser* level; this sweep is a one-shot
    accelerator that promotes the pre-#1131 retry-stuck rows to
    ``tombstoned`` so the worker stops re-fetching them while the new
    parser code catches up.

    Heuristic (Codex pre-push round 1 — precise semantics): a row is
    eligible iff ``last_attempted_at < NOW() - age`` AND the
    ``error`` column starts with one of the upsert-shape prefixes AND
    does NOT begin with a transient-class prefix
    (``upsert error: OperationalError:`` etc.). That selects rows
    that have not been retried for ``age`` — NOT rows that retry
    every hour and never escape ``failed``. The worker advances
    ``last_attempted_at`` on every retry, so an actively-looping row
    stays *young*. Once #1131's parser-level discrimination ships,
    actively-looping rows will tombstone themselves on the very next
    retry; this sweep handles the remaining backlog that the worker
    is not actively processing (next_retry_at in the future, no
    parser registered for that source, etc.).

    Scope: ``error`` matches the anchored prefixes
    ``"upsert error: "`` / ``"upsert+tombstone error: "`` /
    ``"upsert+log error: "``. Fetch errors, parse errors, and
    ingest-log failures stay in ``failed`` because they may genuinely
    recover.

    Race-safety (Codex pre-push round 1): the sweep is two-phase —
    sample candidates with a non-locking SELECT, then per-row
    conditional UPDATE that *only* fires when ``ingest_status`` is
    still ``failed`` AND ``last_attempted_at`` matches the sampled
    value. If a concurrent manifest worker advanced
    ``last_attempted_at`` (succeeded, failed again, or tombstoned the
    row itself) between sample and update, the conditional UPDATE
    matches zero rows and the sweep records a ``rows_skipped_race``
    instead of overwriting the worker's progress. This is the minimal
    fix that doesn't require worker-side row-locking (out of scope);
    the residual race window (worker holds the accession but has not
    yet transitioned, sweep wins) is small enough that the operator
    can recover by re-running the sweep.

    Per-row commit so a connection drop mid-loop preserves earlier
    progress. Returns counters so the caller (the job runner) can
    record progress.
    """
    cutoff = datetime.now(tz=UTC) - age
    rows_scanned = 0
    rows_tombstoned = 0
    rows_skipped_transient = 0
    rows_skipped_race = 0

    # Pull the candidate batch up-front (snapshot read). ``LIMIT``
    # bounds memory. The per-row conditional UPDATE below carries the
    # sampled ``last_attempted_at`` so a worker advance between sample
    # and update is detected.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number, error, last_attempted_at
            FROM sec_filing_manifest
            WHERE ingest_status = 'failed'
              AND (
                  error LIKE 'upsert error:%%'
                  OR error LIKE 'upsert+tombstone error:%%'
                  OR error LIKE 'upsert+log error:%%'
              )
              AND last_attempted_at < %s
            ORDER BY last_attempted_at ASC
            LIMIT %s
            """,
            (cutoff, limit),
        )
        candidates: list[tuple[str, str, datetime | None]] = [
            (str(row[0]), str(row[1] or ""), row[2]) for row in cur.fetchall()
        ]

    # PR #1132 review BLOCKING: the SELECT above opened an implicit
    # psycopg3 transaction that ``conn.cursor()`` does not commit. If
    # we entered the per-row ``with conn.transaction():`` loop while
    # that tx was still open, every iteration would issue SAVEPOINT
    # (not BEGIN) and on exit RELEASE SAVEPOINT (not COMMIT) — all
    # row updates would be buffered until the outer caller commits,
    # so a connection drop mid-loop would lose every in-flight
    # tombstone. Commit here to close the implicit tx so each
    # subsequent ``with conn.transaction():`` starts a top-level
    # transaction that commits on success.
    conn.commit()

    for accession, error_text, sampled_attempted_at in candidates:
        rows_scanned += 1
        if _error_has_transient_class_prefix(error_text):
            # Post-#1131 transient-shape row — leave alone, the
            # operator can investigate manually. Sweeping these would
            # mask genuine DB-side issues that retry would resolve.
            rows_skipped_transient += 1
            continue
        # Conditional UPDATE: the WHERE clause carries the sampled
        # ``last_attempted_at`` so a concurrent worker tick that
        # advanced the timestamp (or moved the row out of ``failed``
        # entirely) means our UPDATE matches zero rows. This bypasses
        # ``transition_status`` but the equivalent state-machine move
        # (``failed`` -> ``tombstoned``) is explicitly allowed per
        # ``_ALLOWED_TRANSITIONS`` so the state machine stays honest.
        # raw_status is intentionally NOT touched — the existing value
        # carries forward (the #948 evidence-downgrade invariant is
        # not at risk because we never write 'absent').
        new_error = f"#1131 backfill: stale failed upsert (orig: {error_text})"
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE sec_filing_manifest
                    SET ingest_status = 'tombstoned',
                        error = %(new_error)s,
                        next_retry_at = NULL,
                        last_attempted_at = NOW()
                    WHERE accession_number = %(accession)s
                      AND ingest_status = 'failed'
                      AND last_attempted_at IS NOT DISTINCT FROM %(expected_attempted_at)s
                    RETURNING accession_number
                    """,
                    {
                        "new_error": new_error,
                        "accession": accession,
                        "expected_attempted_at": sampled_attempted_at,
                    },
                )
                won_race = cur.fetchone() is not None
            if won_race:
                rows_tombstoned += 1
            else:
                rows_skipped_race += 1
        except Exception:  # noqa: BLE001 — per-row failure must not abort the sweep
            logger.exception(
                "tombstone_stale_failed_upserts: UPDATE failed accession=%s",
                accession,
            )
            rows_skipped_race += 1

    logger.info(
        "tombstone_stale_failed_upserts: scanned=%d tombstoned=%d skipped_transient=%d skipped_race=%d age=%s",
        rows_scanned,
        rows_tombstoned,
        rows_skipped_transient,
        rows_skipped_race,
        age,
    )
    return StaleFailedUpsertSweepResult(
        rows_scanned=rows_scanned,
        rows_tombstoned=rows_tombstoned,
        rows_skipped_transient=rows_skipped_transient,
        rows_skipped_race=rows_skipped_race,
    )


# Form-code → source mapping used by the discovery paths (Atom feed,
# submissions.json, daily-index reader). Pinned here so the same
# mapping is used everywhere — adding a new form means one edit, not
# a sweep across providers.
_FORM_TO_SOURCE: dict[str, ManifestSource] = {
    # Insider section 16
    "3": "sec_form3",
    "3/A": "sec_form3",
    "4": "sec_form4",
    "4/A": "sec_form4",
    "5": "sec_form5",
    "5/A": "sec_form5",
    # Beneficial owner — SEC publishes both ``SC 13D`` (filer-facing
    # short form) and ``SCHEDULE 13D`` (DB normalised form used by the
    # legacy ``blockholder_filings`` table). Map both so the backfill
    # path doesn't drop real rows.
    "SC 13D": "sec_13d",
    "SC 13D/A": "sec_13d",
    "SC 13G": "sec_13g",
    "SC 13G/A": "sec_13g",
    "SCHEDULE 13D": "sec_13d",
    "SCHEDULE 13D/A": "sec_13d",
    "SCHEDULE 13G": "sec_13g",
    "SCHEDULE 13G/A": "sec_13g",
    # Institutional manager
    "13F-HR": "sec_13f_hr",
    "13F-HR/A": "sec_13f_hr",
    # Proxy. ``DEFM14A`` is the merger-related proxy variant; the
    # existing ``app/services/def14a_ingest.py`` treats it as ingestible
    # so the manifest must too (Codex pre-push review #866).
    # ``DEFA14A`` (additional definitive proxy) and ``DEFR14A``
    # (revised definitive proxy) are amendment-style proxies; mapped
    # to ``sec_def14a`` so discovery does not silently skip them
    # (Codex pre-push review #939).
    "DEF 14A": "sec_def14a",
    "DEFA14A": "sec_def14a",
    "DEFM14A": "sec_def14a",
    "DEFR14A": "sec_def14a",
    "PRE 14A": "sec_def14a",
    # Fund (Phase 3). SEC EDGAR submissions API uses both ``NPORT-P`` /
    # ``NPORT-P/A`` (current spelling, no internal dash, "-P" suffix
    # marking the public-quarterly version) and ``N-PORT`` / ``N-PORT/A``
    # (legacy spelling) on the same form-type field. #917 maps both so
    # the manifest classifies regardless of which spelling SEC returns
    # for a given accession (Codex pre-impl review finding #1).
    "N-PORT": "sec_n_port",
    "N-PORT/A": "sec_n_port",
    "NPORT-P": "sec_n_port",
    "NPORT-P/A": "sec_n_port",
    "N-CSR": "sec_n_csr",
    "N-CSR/A": "sec_n_csr",
    # #1171 — N-CSRS (semi-annual) was previously absent so manifest writes
    # silently dropped them. Real fund-metadata parser handles both variants.
    "N-CSRS": "sec_n_csr",
    "N-CSRS/A": "sec_n_csr",
    # Periodic
    "10-K": "sec_10k",
    "10-K/A": "sec_10k",
    "10-Q": "sec_10q",
    "10-Q/A": "sec_10q",
    "8-K": "sec_8k",
    "8-K/A": "sec_8k",
}


def map_form_to_source(form: str) -> ManifestSource | None:
    """Map an SEC form code to the manifest's ``source`` enum value.

    Returns ``None`` for unsupported forms (e.g. ``S-1``, ``424B5``,
    ``CORRESP``) — the discovery paths skip these. Matching is exact;
    callers must canonicalise spacing first (SEC sometimes emits
    ``13F-HR`` and sometimes ``13F-HR  `` with trailing whitespace)."""
    return _FORM_TO_SOURCE.get(form.strip())


# SEC amendment forms that DON'T carry the standard ``/A`` suffix.
# DEFA14A is the additional-proxy amendment of DEF 14A; DEFR14A is
# the proxy revision. Claude bot review on PR #878 caught this gap —
# discovery callers that derive ``is_amendment`` from
# ``is_amendment_form(form)`` would have left these rows with
# ``is_amendment=False`` and an empty amendment chain.
_NON_SUFFIX_AMENDMENT_FORMS: frozenset[str] = frozenset(
    {
        "DEFA14A",  # additional definitive proxy
        "DEFR14A",  # revised definitive proxy
    }
)


def is_amendment_form(form: str) -> bool:
    """True when the form code is an amendment of an earlier filing.

    Most SEC amendments are signalled by the ``/A`` suffix
    (``13F-HR/A``, ``SC 13D/A``, ``4/A``, ``DEF 14A/A``). A handful of
    proxy variants — ``DEFA14A`` / ``DEFR14A`` — encode the amendment
    semantics in the form code itself without a suffix; we explicitly
    list those."""
    canonical = form.strip()
    if canonical.endswith("/A"):
        return True
    return canonical in _NON_SUFFIX_AMENDMENT_FORMS
