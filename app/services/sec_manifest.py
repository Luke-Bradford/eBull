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
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal

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
    "finra_regsho_daily",
    "sec_nt",
    "sec_pre14a",
    "sec_424b",
]

# Sources intentionally absent from ``_FORM_TO_SOURCE``:
#  - FINRA sources: caller-owned ScheduledJob path, not SEC form discovery.
#  - sec_xbrl_facts: bulk Companyfacts JSON ingest (no Atom/daily-index
#    discovery); synth no-op manifest rows written by
#    ``sec_companyfacts_ingest`` directly. See
#    ``docs/etl/sources/sec_xbrl_facts.md`` §6.
#
# Final-committee Architect IMP-2 fold (2026-05-24): moved here from
# ``tests/smoke/test_etl_source_to_sink.py`` so production is the
# authoritative source of "intentionally not form-discoverable" sources.
# Smoke test imports this constant; lint script can grep it.
FORM_MAPPING_EXEMPT: frozenset[str] = frozenset({"finra_short_interest", "finra_regsho_daily", "sec_xbrl_facts"})

ManifestSubjectType = Literal[
    "issuer",
    "institutional_filer",
    "blockholder_filer",
    "fund_series",
    "finra_universe",
]

# Format guards for SEC-sourced manifest rows (#1460, deferred from
# #1433 item 3). Scoped to ``source.startswith("sec_")`` because FINRA
# sources write synthetic identifiers (cik="FINRA_SI",
# accession="FINRA_SI_20260430" / "FINRA_REGSHO_CNMS_*") that a
# blanket SEC regex would reject in production. Mirrors the sql/134
# ownership-table CHECKs; verified-clean baseline 2026-06-04: all
# 2,339,524 dev manifest rows across 12 sec_* sources conform.
_MANIFEST_CIK_RE: Final = re.compile(r"^[0-9]{10}$")
_MANIFEST_ACCESSION_RE: Final = re.compile(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")

IngestStatus = Literal["pending", "fetched", "parsed", "tombstoned", "failed", "deferred"]
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
    "pending": frozenset({"pending", "fetched", "parsed", "failed", "tombstoned", "deferred"}),
    # ``fetched`` is transient — must transition out, no self-loop.
    "fetched": frozenset({"parsed", "tombstoned", "failed"}),
    # ``failed`` self-loop: a worker re-fetch that fails again should
    # update the error/retry without raising. Without the self-loop
    # in the allowed set, the second call would silently no-op (Claude
    # bot review #879 WARNING).
    "failed": frozenset({"pending", "fetched", "parsed", "tombstoned", "failed", "deferred"}),
    # ``parsed`` -> ``pending`` is the only legal exit (rebuild path
    # in #872). No self-loop — re-parses must go through ``pending``
    # so the rewash gate is explicit.
    "parsed": frozenset({"pending"}),
    # ``tombstoned`` is terminal under normal flow; rebuild can
    # resurrect it back to pending for an explicit operator retry.
    # No self-loop.
    "tombstoned": frozenset({"pending"}),
    # ``deferred`` (#1343) — metadata seeded at bootstrap, 10-K/8-K body
    # fetch deferred to first user view. Exits: ``pending`` (operator
    # force-drain re-queues for eager fetch), ``parsed`` (lazy fill
    # succeeded), ``tombstoned`` (lazy fill hit a deterministic failure).
    # No self-loop. No ``failed`` — a transient lazy failure leaves the
    # row ``deferred`` for the next click; backoff lives in the typed
    # table (instrument_business_summary / eight_k_filings), not here.
    "deferred": frozenset({"pending", "parsed", "tombstoned"}),
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
    initial_ingest_status: IngestStatus = "pending",
) -> None:
    """UPSERT one manifest row keyed by ``accession_number``.

    Idempotent on re-discovery (Atom + daily-index + submissions.json
    all converge on the same accession). On conflict, refreshes the
    metadata fields without touching ``ingest_status`` /
    ``parser_version`` / ``raw_status`` / retry state — those are
    owned by ``transition_status``. ``updated_at`` is touched by the
    table trigger.

    ``initial_ingest_status`` (#1343) sets the lifecycle state ONLY on
    INSERT (default ``'pending'``); it is NOT applied on conflict, so a
    re-discovery never overwrites a live row's state. S16 passes
    ``'deferred'`` for sec_10k/sec_8k so the post-bootstrap worker skips
    the body backlog (bodies fill lazily on first view).

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
    if source.startswith("sec_"):
        # Format guard (#1460): accession is matched RAW because the
        # INSERT binds it verbatim as the PK (the anchored regex rejects
        # surrounding whitespace); cik is matched stripped because the
        # INSERT binds ``cik.strip()``.
        if not _MANIFEST_ACCESSION_RE.fullmatch(accession_number):
            raise ValueError(
                f"record_manifest_entry: malformed SEC accession number {accession_number!r}"
                f" for source={source!r} (expected NNNNNNNNNN-NN-NNNNNN)"
            )
        if not _MANIFEST_CIK_RE.fullmatch(cik.strip()):
            raise ValueError(
                f"record_manifest_entry: malformed SEC cik {cik!r} for source={source!r}"
                f" (expected 10 digits; accession={accession_number})"
            )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sec_filing_manifest (
                accession_number, cik, form, source,
                subject_type, subject_id, instrument_id,
                filed_at, accepted_at, primary_document_url,
                is_amendment, amends_accession, ingest_status
            ) VALUES (
                %(accession)s, %(cik)s, %(form)s, %(source)s,
                %(stype)s, %(sid)s, %(iid)s,
                %(filed_at)s, %(accepted_at)s, %(url)s,
                %(is_amend)s, %(amends)s, %(ingest_status)s
            )
            -- ``ingest_status`` is applied ONLY on INSERT; the ON CONFLICT
            -- SET below deliberately omits it so a re-discovery never
            -- clobbers a live row's lifecycle state (owned by
            -- ``transition_status``). #1343 S16 passes
            -- ``initial_ingest_status='deferred'`` for sec_10k/sec_8k.
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
                "ingest_status": initial_ingest_status,
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
        # #1686 — idempotent terminal-tombstone no-op for the concurrent
        # double-tombstone race. The bulk pre-retention sweep
        # (``manifest_pre_retention_sweep``) and the worker apply the
        # IDENTICAL pre-fetch retention gate, so both can tombstone the
        # same pending row: the sweep wins (pending->tombstoned commits),
        # then the worker's parser returns ``status='tombstoned'`` and
        # calls here with ``current='tombstoned'``. ``tombstoned`` is
        # otherwise terminal (``_ALLOWED_TRANSITIONS['tombstoned']={pending}``),
        # so without this guard the redundant transition raises and aborts
        # the whole worker tick. Return as a NO-OP (do NOT re-stamp
        # last_attempted_at/error) — a concurrent writer reaching the same
        # terminal state is benign. This is the ONLY self-transition
        # exemption to the #879 "explicitly-allowed" rule, and because it
        # writes nothing it cannot mask a double-error write (the #879
        # failure mode the rule guards against).
        if current_status == "tombstoned" and ingest_status == "tombstoned":
            return
        # #1591 Part 2 — idempotent parsed-on-parsed no-op for the SAME
        # concurrent-drainer race, now reachable because the prefetch
        # (``_prefetch_then_dispatch``, extended to the per-source rebuild +
        # 10-K/8-K) widens the window between row-select and transition.
        # Drainer A commits ``pending -> parsed``; drainer B, which read the
        # row as pending before A committed, finishes its (idempotent) parse
        # and calls here with ``current='parsed'``. ``parsed`` is otherwise
        # terminal (``_ALLOWED_TRANSITIONS['parsed']={pending}`` — re-parses
        # must go through pending), so without this the redundant transition
        # raises and aborts B's whole tick. Benign NO-OP: writes nothing (same
        # #879-safe property as the tombstoned case above) and cannot mask a
        # single-drainer bug — that path always goes ``pending -> parsed``, so
        # ``parsed -> parsed`` is unreachable without a concurrent writer.
        if current_status == "parsed" and ingest_status == "parsed":
            return
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
        # #1233 PR-5a — use ``clock_timestamp()`` rather than ``NOW()``
        # (= ``transaction_timestamp()``, fixed at tx start). The
        # bootstrap manifest reset prelude
        # (``app/services/bootstrap_orchestrator.py::reset_manifest_for_run``)
        # filters ``last_attempted_at < bootstrap_runs.triggered_at`` to
        # protect concurrent live cron writes. ``NOW()`` would mis-stamp
        # a long-running worker transaction's commit with its tx-start
        # time — a worker tx begun before ``triggered_at`` but
        # committing AFTER it would write a stale ``last_attempted_at``
        # that survives the reset predicate and gets erroneously
        # flipped to ``pending``. ``clock_timestamp()`` evaluates at
        # statement time, so the stamp reflects when the failure was
        # actually recorded.
        set_clauses: list[sql.Composable] = [
            sql.SQL("ingest_status = %(status)s"),
            sql.SQL("last_attempted_at = COALESCE(%(attempt)s, clock_timestamp())"),
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
        elif ingest_status == "deferred":
            # #1343 — metadata seeded, body fetch deferred to first user
            # view. Clear any prior failure state; raw_status stays
            # 'absent' (no body fetched). The evidence-downgrade guard
            # above already prevents clobbering a previously-stored body,
            # so a deferred transition never loses raw bytes.
            set_clauses.append(sql.SQL("error = NULL"))
            set_clauses.append(sql.SQL("next_retry_at = NULL"))

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
                ORDER BY filed_at ASC, accession_number ASC
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
                ORDER BY filed_at ASC, accession_number ASC
                LIMIT %s
                """,
                (source, limit),
            )
        for row in cur.fetchall():
            yield ManifestRow(**row)


def iter_pending_recent(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource,
    since: datetime,
    limit: int = 30,
) -> Iterator[ManifestRow]:
    """Yield the NEWEST pending rows for one source filed on/after ``since``.

    #1685 — the recent-first slice. ``filed_at DESC`` (newest first), floored at
    ``since`` (the worker passes a 90-day window), so every source keeps its
    recent filings fresh regardless of the oldest-first historical backlog the
    main drain (``iter_pending``) works through. Served by the partial index
    ``idx_manifest_recent (source, filed_at DESC, accession_number DESC) WHERE
    ingest_status='pending'`` (sql/204) so a tick does not scan the ~1.46M-row
    pending backlog. Source-scoped only — the worker allocates a per-source
    recent quota via ``compute_quotas``."""
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
            WHERE ingest_status = 'pending' AND source = %s AND filed_at >= %s
            ORDER BY filed_at DESC, accession_number DESC
            LIMIT %s
            """,
            (source, since, limit),
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
                ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC,
                         accession_number ASC
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
                ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC,
                         accession_number ASC
                LIMIT %s
                """,
                (source, limit),
            )
        for row in cur.fetchall():
            yield ManifestRow(**row)


def iter_pending_topup(
    conn: psycopg.Connection[Any],
    *,
    sources: Sequence[ManifestSource],
    exclude_accessions: Sequence[str],
    limit: int,
) -> Iterator[ManifestRow]:
    """Global oldest-pending top-up, scoped to registered sources +
    excluding accessions already picked in Phase A (#1179).

    Used by ``run_manifest_worker`` Phase B to fill leftover budget
    after per-source quotas. Both array params are explicitly cast
    ``%s::text[]`` so psycopg3 type inference handles empty lists.
    Empty ``sources`` short-circuits without firing SQL.
    """
    if not sources:
        return
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
            WHERE ingest_status = 'pending'
              AND source = ANY(%s::text[])
              AND accession_number != ALL(%s::text[])
            ORDER BY filed_at ASC, accession_number ASC
            LIMIT %s
            """,
            (list(sources), list(exclude_accessions), limit),
        )
        for row in cur.fetchall():
            yield ManifestRow(**row)


def iter_retryable_topup(
    conn: psycopg.Connection[Any],
    *,
    sources: Sequence[ManifestSource],
    exclude_accessions: Sequence[str],
    limit: int,
) -> Iterator[ManifestRow]:
    """Global oldest-retryable top-up, mirroring
    :func:`iter_pending_topup` (#1179).

    Predicate matches :func:`iter_retryable`:
    ``ingest_status='failed' AND (next_retry_at IS NULL OR
    next_retry_at <= NOW())``.
    """
    if not sources:
        return
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
            WHERE ingest_status = 'failed'
              AND (next_retry_at IS NULL OR next_retry_at <= NOW())
              AND source = ANY(%s::text[])
              AND accession_number != ALL(%s::text[])
            ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC,
                     accession_number ASC
            LIMIT %s
            """,
            (list(sources), list(exclude_accessions), limit),
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
    # ``PRE 14A`` / ``PRER14A`` (preliminary proxy) are deliberately NOT
    # mapped to ``sec_def14a`` (#1320). A PRE 14A is a pre-finalisation
    # draft whose ownership figures we never historically counted — the
    # definitive DEF 14A that follows is what the ownership pipeline
    # ingests. Mapping it there routed 6k+ drafts into the sec_def14a
    # manifest namespace, which the parser then tombstoned pre-fetch
    # (wasted worker cycles + polluted coverage). The parser PRE-14A
    # tombstone branch (manifest_parsers/def14a.py) stays as
    # defense-in-depth for any PRE row that reaches the worker via a
    # legacy/manual seed.
    #
    # #1892 (#1015 item 3) maps them instead to the wholly separate
    # ``sec_pre14a`` source below — a meeting-agenda proposal-signal
    # parser, not an ownership source. #1320's concern is unaffected.
    "PRE 14A": "sec_pre14a",
    "PRER14A": "sec_pre14a",
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
    # Late-filing notices (Form 12b-25). #1015 — NT 10-K / NT 10-Q upgraded
    # from metadata-only to PARSE+RAW. The ``/A`` variants (2 rows total) and
    # NT 20-F (foreign deadline regime) stay metadata-only, out of scope.
    "NT 10-K": "sec_nt",
    "NT 10-Q": "sec_nt",
    # 424B prospectuses (#1816). The subtype is a Rule 424(b) filing-trigger
    # bucket, not a taxonomy — economic facts come from the parsed Item
    # 501(b)(3) cover. 424B2 is volume-gated (#1975): mapped here, but the
    # parser's pre-fetch gate tombstones B2 rows for filers with >100 lifetime
    # B2 filings (bank/ETN structured-note factories — a fetch-cost bound, not
    # a classification). 424B8 stays unmapped: it duplicates the underlying
    # 424(b) paragraph's filing.
    "424B1": "sec_424b",
    "424B2": "sec_424b",
    "424B3": "sec_424b",
    "424B4": "sec_424b",
    "424B5": "sec_424b",
    "424B7": "sec_424b",
}


def map_form_to_source(form: str) -> ManifestSource | None:
    """Map an SEC form code to the manifest's ``source`` enum value.

    Returns ``None`` for unsupported forms (e.g. ``S-1``, ``424B8``,
    ``CORRESP``) — the discovery paths skip these. Matching is exact;
    callers must canonicalise spacing first (SEC sometimes emits
    ``13F-HR`` and sometimes ``13F-HR  `` with trailing whitespace).

    The intentionally-unmapped forms and the rationale for each (operator
    FAQ: "where is 6-K?") live in ``docs/etl/sources/README.md`` §"Forms NOT
    ingested by the manifest + why". Most are recorded as a metadata-only
    ``filing_events`` row — see ``SEC_METADATA_ONLY`` in
    ``app/services/filings.py`` — which is a SEPARATE taxonomy from this map
    (a form can be in both: ``5``/``5/A`` are metadata-only at the
    ``filing_events`` tier yet mapped here to ``sec_form5``). ``13F-NT`` is
    deliberately unmapped (notice-only, no holdings) — see
    ``docs/review-prevention-log.md`` for the stale-parent double-count gap
    that drop creates."""
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


# Per-cohort form allow-list for the bulk submissions.zip filer-writer
# (#1337 P1 — see docs/proposals/etl/bulk-first-bootstrap.md §4). The
# bulk path emits ``sec_filing_manifest`` rows for filer-cohort CIKs
# (institutional_filer + blockholder_filer) by parsing the same
# submissions.json payload S8 reads for issuers. A filer CIK files many
# form types; we write a manifest row ONLY for the forms that define its
# cohort — otherwise an institutional_filer that also files (say) a
# stray non-13F form would get a misclassified ``institutional_filer``
# row. Values verified against ``_FORM_TO_SOURCE`` above:
#  - institutional_filer: 13F-HR / 13F-HR/A are the only 13F forms
#    ``_FORM_TO_SOURCE`` maps (13F-NT notice-only is intentionally absent
#    — the HTTP path drops it too).
#  - blockholder_filer: both the short (``SC 13D``) and legacy long-form
#    (``SCHEDULE 13D``) variants, original + amendment.
# A CIK that is BOTH an issuer AND a filer hits both writers via the
# multimap in ``sec_submissions_ingest._load_known_cik_subjects`` — the
# issuer path picks up the 10-K, the filer path picks up the 13F.
_FILER_COHORT_FORMS: dict[ManifestSubjectType, frozenset[str]] = {
    "institutional_filer": frozenset({"13F-HR", "13F-HR/A"}),
    "blockholder_filer": frozenset(
        {
            "SC 13D",
            "SC 13D/A",
            "SC 13G",
            "SC 13G/A",
            "SCHEDULE 13D",
            "SCHEDULE 13D/A",
            "SCHEDULE 13G",
            "SCHEDULE 13G/A",
        }
    ),
}
