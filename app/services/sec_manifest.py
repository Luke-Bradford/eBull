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
    # ``pending`` -> ``parsed`` is allowed for the backfill + write-
    # through paths where we record an UPSERT into the typed table
    # without a separate "fetched" hop (the body either lives in
    # filing_raw_documents already or was never separately stored).
    # Steady-state worker flow is ``pending`` -> ``fetched`` -> ``parsed``.
    "pending": frozenset({"fetched", "parsed", "failed", "tombstoned"}),
    "fetched": frozenset({"parsed", "tombstoned", "failed"}),
    # ``failed`` -> ``pending`` is the retry-after-backoff path the
    # worker uses; ``failed`` -> ``tombstoned`` is the "give up after
    # N retries" path; ``failed`` -> ``parsed`` covers the case where
    # a worker re-fetches and parses in one step after a transient
    # error.
    "failed": frozenset({"pending", "fetched", "parsed", "tombstoned"}),
    # ``parsed`` -> ``pending`` is the rebuild path (#872). We keep the
    # accession history; the worker picks the row up again next pass.
    "parsed": frozenset({"pending"}),
    # Tombstoned is terminal under normal flow; rebuild can resurrect
    # it back to pending for an explicit operator-driven retry.
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
        if ingest_status != current_status and ingest_status not in allowed:
            raise ValueError(
                f"transition_status: illegal transition {current_status!r} -> {ingest_status!r} "
                f"(accession={accession_number})"
            )

        # Build SET clause dynamically so we only touch fields the
        # caller asked about, plus the always-bump ``last_attempted_at``.
        set_clauses = ["ingest_status = %(status)s", "last_attempted_at = COALESCE(%(attempt)s, NOW())"]
        params: dict[str, Any] = {
            "accession": accession_number,
            "status": ingest_status,
            "attempt": last_attempted_at,
        }

        if ingest_status == "parsed":
            # Clear error on success; stamp parser_version when given.
            set_clauses.append("error = NULL")
            if parser_version is not None:
                set_clauses.append("parser_version = %(parser_version)s")
                params["parser_version"] = parser_version
            # parsed implies the body has been fetched + stored
            if raw_status is not None:
                set_clauses.append("raw_status = %(raw_status)s")
                params["raw_status"] = raw_status
            set_clauses.append("next_retry_at = NULL")
        elif ingest_status == "fetched":
            if raw_status is not None:
                set_clauses.append("raw_status = %(raw_status)s")
                params["raw_status"] = raw_status
            set_clauses.append("error = NULL")
            set_clauses.append("next_retry_at = NULL")
        elif ingest_status == "failed":
            set_clauses.append("error = %(error)s")
            params["error"] = error
            set_clauses.append("next_retry_at = %(next_retry)s")
            params["next_retry"] = next_retry_at
        elif ingest_status == "tombstoned":
            set_clauses.append("error = %(error)s")
            params["error"] = error
            set_clauses.append("next_retry_at = NULL")
        elif ingest_status == "pending":
            # Rebuild path: clear retry state; keep parser_version so
            # the rewash detector can compare against current.
            set_clauses.append("error = NULL")
            set_clauses.append("next_retry_at = NULL")

        cur.execute(
            f"UPDATE sec_filing_manifest SET {', '.join(set_clauses)} WHERE accession_number = %(accession)s",
            params,
        )


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
    where = "ingest_status = 'pending'"
    params: list[Any] = []
    if source is not None:
        where += " AND source = %s"
        params.append(source)
    params.append(limit)

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT accession_number, cik, form, source,
                   subject_type, subject_id, instrument_id,
                   filed_at, accepted_at, primary_document_url,
                   is_amendment, amends_accession,
                   ingest_status, parser_version, raw_status,
                   last_attempted_at, next_retry_at, error
            FROM sec_filing_manifest
            WHERE {where}
            ORDER BY filed_at ASC
            LIMIT %s
            """,
            params,
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
    where = "ingest_status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= NOW())"
    params: list[Any] = []
    if source is not None:
        where += " AND source = %s"
        params.append(source)
    params.append(limit)

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT accession_number, cik, form, source,
                   subject_type, subject_id, instrument_id,
                   filed_at, accepted_at, primary_document_url,
                   is_amendment, amends_accession,
                   ingest_status, parser_version, raw_status,
                   last_attempted_at, next_retry_at, error
            FROM sec_filing_manifest
            WHERE {where}
            ORDER BY COALESCE(next_retry_at, last_attempted_at, filed_at) ASC
            LIMIT %s
            """,
            params,
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
    # Proxy
    "DEF 14A": "sec_def14a",
    "DEFA14A": "sec_def14a",
    "PRE 14A": "sec_def14a",
    # Fund (Phase 3)
    "N-PORT": "sec_n_port",
    "N-PORT/A": "sec_n_port",
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


def is_amendment_form(form: str) -> bool:
    """True when the form code carries the ``/A`` amendment suffix."""
    return form.strip().endswith("/A")
