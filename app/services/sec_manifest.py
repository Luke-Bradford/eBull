"""SEC filing manifest — single source of truth for "is filing X on file?".

Issue #864 / spec §"sec_filing_manifest"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

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
from datetime import datetime
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg import sql

logger = logging.getLogger(__name__)


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
            "SELECT ingest_status FROM sec_filing_manifest WHERE accession_number = %s FOR UPDATE",
            (accession_number,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"transition_status: manifest row missing for accession={accession_number}")
        current_status: IngestStatus = row[0]
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
        elif ingest_status == "tombstoned":
            set_clauses.append(sql.SQL("error = %(error)s"))
            params["error"] = error
            set_clauses.append(sql.SQL("next_retry_at = NULL"))
        elif ingest_status == "pending":
            # Rebuild path: clear retry state; keep parser_version so
            # the rewash detector can compare against current.
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
    "DEF 14A": "sec_def14a",
    "DEFA14A": "sec_def14a",
    "DEFM14A": "sec_def14a",
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
