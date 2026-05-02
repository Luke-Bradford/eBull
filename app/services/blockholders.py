"""SEC Schedule 13D / 13G blockholder ingester (#766 PR 2 of 3).

Walks the operator-curated ``blockholder_filer_seeds`` list and, for
each active filer:

  1. Fetches ``data.sec.gov/submissions/CIK{cik}.json`` to discover
     SC 13D / SC 13D/A / SC 13G / SC 13G/A accessions filed by that
     CIK. The submissions index uses ``SC 13D`` / ``SC 13D/A`` /
     ``SC 13G`` / ``SC 13G/A`` form labels; the canonical
     ``SCHEDULE 13D`` / ``SCHEDULE 13G`` strings come from the parsed
     primary_doc.xml itself, not the index.
  2. For each accession not yet present in
     ``blockholder_filings_ingest_log``, fetches the per-filing
     ``primary_doc.xml`` directly. Unlike 13F-HR the 13D/G archive
     has no separate infotable — every canonical field (issuer
     CUSIP, ownership block, reporter list) lives in primary_doc.
     The ``index.json`` walk is therefore unnecessary.
  3. Parses primary_doc.xml via :mod:`app.providers.implementations.
     sec_13dg`. A single accession yields 1..N reporting persons
     (joint filings), and the ingester writes one
     ``blockholder_filings`` row per reporter.
  4. Resolves the issuer CUSIP to an ``instrument_id`` via
     ``external_identifiers``. Filings whose CUSIP is unknown still
     write rows (with ``instrument_id IS NULL``) so the audit trail
     stays intact and the PR 3 reader can re-attempt resolution
     once the #740 backfill closes the gap.
  5. Upserts the primary filer + every reporter row inside one
     transaction. Idempotent re-ingest of the same accession is
     guaranteed by the partial UNIQUE INDEX from migration 095.

The ingester is the only DB-touching half of the pipeline; the
parser stays pure. The HTTP fetch routes through the bounded-
concurrency client added in #728 so concurrent filer ingests share
the SEC fair-use rate budget.

Tombstones: a filing whose primary_doc.xml fetch 404s is recorded in
``blockholder_filings_ingest_log`` with ``status='failed'`` plus the
accession number in ``error``. The next run sees the accession is
still missing and skips it — short-lived 404s heal naturally;
persistent failures show up in the ops monitor (#13). To force a
retry the operator deletes the log row.

The amendment-chain aggregator (:func:`latest_blockholder_positions`)
is exposed here so PR 3's reader endpoint can call it without
re-implementing the SEC supersession semantics.
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET  # noqa: S405 — only used to catch ET.ParseError; no untrusted input parsed here.
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol

import psycopg
import psycopg.rows

from app.providers.implementations.sec_13dg import (
    BlockholderFiling,
    BlockholderReportingPerson,
    parse_primary_doc,
)
from app.services.fundamentals import finish_ingestion_run, start_ingestion_run

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider contract
# ---------------------------------------------------------------------------


class SecArchiveFetcher(Protocol):
    """Subset of the SEC EDGAR provider this ingester relies on.

    Decoupled to keep the service unit-testable with an in-memory
    fake. The production binding is :class:`app.providers.
    implementations.sec_edgar.SecEdgarProvider`.
    """

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


# SEC submissions JSON uses BOTH form-label conventions in
# practice. Pre-2024-12-19 (BOM rule effective date) filings show
# the short ``SC 13D`` / ``SC 13D/A`` form; post-2024-12-19
# filings — which carry the structured primary_doc.xml the parser
# in #766 PR 1 consumes — show the long ``SCHEDULE 13D`` /
# ``SCHEDULE 13D/A`` form. The filter accepts both so we don't
# silently miss every modern (post-BOM) filing — verified against
# Carl Icahn (CIK 0000921669) submissions JSON, which has
# ``SCHEDULE 13D/A`` for 2025+ entries.
_SUBMISSIONS_INDEX_FORMS: frozenset[str] = frozenset(
    (
        "SC 13D",
        "SC 13D/A",
        "SC 13G",
        "SC 13G/A",
        "SCHEDULE 13D",
        "SCHEDULE 13D/A",
        "SCHEDULE 13G",
        "SCHEDULE 13G/A",
    )
)


@dataclass(frozen=True)
class AccessionRef:
    """One discovered 13D / 13G accession to ingest."""

    accession_number: str
    filing_type: str  # one of _SUBMISSIONS_INDEX_FORMS values
    filed_at: datetime | None


@dataclass(frozen=True)
class IngestSummary:
    """Per-filer rollup of one ingest pass.

    ``submissions_fetch_failed`` is set when the per-CIK
    submissions JSON itself is unreachable. The batch wrapper uses
    this signal to downgrade ``data_ingestion_runs.status`` to
    ``partial`` so a stale seed entry (CIK renamed, EDGAR archive
    moved, fair-use throttling) does not silently masquerade as
    ``success`` with zero rows. Codex pre-push review caught this on
    PR review.
    """

    filer_cik: str
    accessions_seen: int
    accessions_ingested: int
    accessions_failed: int
    rows_inserted: int  # one row per reporter; sums across accessions
    rows_skipped_no_cusip: int
    submissions_fetch_failed: bool = False
    first_error: str | None = None


@dataclass(frozen=True)
class BlockholderPosition:
    """Aggregator row: latest non-superseded filing per reporter on
    one issuer. Returned by :func:`latest_blockholder_positions`.

    ``reporter_identity`` matches the schema's hot-path index key:
    the reporter CIK when present, the reporter name when not. Joint
    filers under one CIK collapse to a single position; co-reporters
    on a joint filing with different CIKs (or names) each get their
    own position so the consumer can dedupe via ``member_of_group``
    if needed.
    """

    reporter_identity: str
    reporter_cik: str | None
    reporter_name: str
    issuer_cik: str
    instrument_id: int | None
    submission_type: str
    status: str
    aggregate_amount_owned: Decimal | None
    percent_of_class: Decimal | None
    member_of_group: str | None
    type_of_reporting_person: str | None
    accession_number: str
    date_of_event: date | None
    filed_at: datetime | None


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{filename}"


def _zero_pad_cik(cik: str | int) -> str:
    return str(int(str(cik).strip())).zfill(10)


def _accession_no_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")


def _submissions_url(cik: str) -> str:
    return _SUBMISSIONS_URL.format(cik=_zero_pad_cik(cik))


def _archive_file_url(cik: str, accession_number: str, filename: str) -> str:
    return _ARCHIVE_URL.format(
        cik_int=int(_zero_pad_cik(cik)),
        accn_no_dashes=_accession_no_dashes(accession_number),
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Submissions index walker
# ---------------------------------------------------------------------------


def parse_submissions_index(payload: str) -> list[AccessionRef] | None:
    """Walk ``data.sec.gov/submissions/CIK{cik}.json`` and emit one
    :class:`AccessionRef` per SC 13D / SC 13G row.

    Returns ``None`` when the payload is not valid JSON — the
    ingester treats that the same as a 404 / transport failure so a
    malformed 200-body cannot silently masquerade as "no recent
    filings" (which would emit a clean ``success`` audit record for a
    filer whose seed entry has actually gone stale). Codex pre-push
    review caught this on PR review.

    Returns ``[]`` when the payload is valid JSON but carries no
    13D/G accessions in the ``recent`` array — that is a legitimate
    "filer has nothing on file in this form family" outcome.

    Mirrors the 13F-HR equivalent in ``app/services/
    institutional_holdings.py`` but filters on the four 13D/G
    submission-form labels instead of 13F-HR. ``period_of_report``
    has no analogue on 13D/G (the form covers a single event, not a
    fiscal quarter), so the ref carries only the accession + form +
    filing-date timestamp; the canonical event date comes from the
    parser's ``date_of_event`` later.

    Older-history shards referenced via ``files`` are out of scope —
    the recent array holds the latest ~1,000 filings per CIK, far
    more than the operator-curated seed list will need.
    """
    try:
        data: dict[str, Any] = json.loads(payload)
    except json.JSONDecodeError:
        logger.warning("13D/G submissions index payload is not valid JSON")
        return None

    filings = data.get("filings", {})
    recent = filings.get("recent", {})
    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])

    out: list[AccessionRef] = []
    for i, accession in enumerate(accessions):
        if i >= len(forms):
            break
        form = str(forms[i]).strip()
        if form not in _SUBMISSIONS_INDEX_FORMS:
            continue
        filed_at = _safe_iso_datetime(filing_dates[i] if i < len(filing_dates) else "")
        out.append(
            AccessionRef(
                accession_number=str(accession),
                filing_type=form,
                filed_at=filed_at,
            )
        )
    return out


def _safe_iso_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _safe_iso_datetime(text: str | None) -> datetime | None:
    """Coerce ``YYYY-MM-DD`` to a UTC tz-aware ``datetime``. The 13F
    ingester carries the same helper for the same reason: ``filed_at``
    is TIMESTAMPTZ and a naive datetime would drift to the server's
    local zone on write."""
    parsed = _safe_iso_date(text)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _list_active_filer_seeds(conn: psycopg.Connection[tuple]) -> list[str]:
    cur = conn.execute("SELECT cik FROM blockholder_filer_seeds WHERE active = TRUE ORDER BY cik")
    return [_zero_pad_cik(row[0]) for row in cur.fetchall()]


def _existing_accessions_for_filer(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
) -> set[str]:
    """Return every accession_number this filer has already had an
    ingest attempt for — success / partial / failed all count.

    Reads from ``blockholder_filings_ingest_log`` rather than
    ``blockholder_filings.accession_number`` because:

      * A failed accession (404, parse error) writes no filing row
        but must still be skipped by the next run to avoid
        re-fetching forever.
      * A partial accession (issuer CUSIP unresolved) DOES write
        filing rows with ``instrument_id IS NULL``, but the log
        carries the canonical "have we attempted this?" answer.

    To force a retry, the operator deletes the log row.
    """
    cur = conn.execute(
        "SELECT accession_number FROM blockholder_filings_ingest_log WHERE filer_cik = %(cik)s",
        {"cik": filer_cik},
    )
    return {row[0] for row in cur.fetchall()}


def _record_ingest_attempt(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    accession_number: str,
    submission_type: str | None,
    status: str,
    rows_inserted: int = 0,
    rows_skipped: int = 0,
    error: str | None = None,
) -> None:
    """Idempotent upsert into ``blockholder_filings_ingest_log``.

    Status is one of ``'success'`` / ``'partial'`` / ``'failed'``.
    Re-recording the same accession overwrites the prior attempt so a
    follow-up successful run can promote a failed/partial accession
    to success.
    """
    conn.execute(
        """
        INSERT INTO blockholder_filings_ingest_log (
            accession_number, filer_cik, submission_type,
            status, rows_inserted, rows_skipped, error
        ) VALUES (
            %(accession_number)s, %(filer_cik)s, %(submission_type)s,
            %(status)s, %(rows_inserted)s, %(rows_skipped)s, %(error)s
        )
        ON CONFLICT (accession_number) DO UPDATE SET
            status = EXCLUDED.status,
            rows_inserted = EXCLUDED.rows_inserted,
            rows_skipped = EXCLUDED.rows_skipped,
            error = EXCLUDED.error,
            fetched_at = NOW()
        """,
        {
            "accession_number": accession_number,
            "filer_cik": filer_cik,
            "submission_type": submission_type,
            "status": status,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "error": error,
        },
    )


def _resolve_cusip_to_instrument_id(
    conn: psycopg.Connection[tuple],
    cusip: str,
) -> int | None:
    """Look up the instrument_id mapped to a CUSIP via
    external_identifiers. Same lookup shape as the 13F-HR ingester —
    the CUSIP backfill (#740) populates these rows."""
    cur = conn.execute(
        """
        SELECT instrument_id
        FROM external_identifiers
        WHERE provider = 'sec'
          AND identifier_type = 'cusip'
          AND identifier_value = %(cusip)s
        ORDER BY is_primary DESC, external_identifier_id ASC
        LIMIT 1
        """,
        {"cusip": cusip.strip().upper()},
    )
    row = cur.fetchone()
    return int(row[0]) if row is not None else None


def seed_filer(
    conn: psycopg.Connection[tuple],
    *,
    cik: str | int,
    label: str,
    notes: str | None = None,
    active: bool = True,
) -> None:
    """Idempotent helper for adding a filer to the curated seed list.

    Used by tests + an operator-side script. The admin UI in PR 3
    will call the same helper via an API endpoint.
    """
    conn.execute(
        """
        INSERT INTO blockholder_filer_seeds (cik, label, active, notes)
        VALUES (%(cik)s, %(label)s, %(active)s, %(notes)s)
        ON CONFLICT (cik) DO UPDATE SET
            label = EXCLUDED.label,
            active = EXCLUDED.active,
            notes = COALESCE(EXCLUDED.notes, blockholder_filer_seeds.notes)
        """,
        {
            "cik": _zero_pad_cik(cik),
            "label": label,
            "active": active,
            "notes": notes,
        },
    )


def _upsert_filer(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    name: str,
) -> int:
    """Insert / update a ``blockholder_filers`` row. Returns filer_id.

    Unlike the 13F-HR ``institutional_filers`` row this carries no
    derived ``filer_type`` — 13D/G has no analogue (the active /
    passive split lives on each filing row, derived from
    submission_type by the parser). The filer record is just an
    EDGAR-CIK-to-name map for audit + the operator UI.
    """
    cur = conn.execute(
        """
        INSERT INTO blockholder_filers (cik, name)
        VALUES (%(cik)s, %(name)s)
        ON CONFLICT (cik) DO UPDATE SET
            name = EXCLUDED.name,
            fetched_at = NOW()
        RETURNING filer_id
        """,
        {"cik": cik, "name": name},
    )
    row = cur.fetchone()
    assert row is not None, "filer upsert RETURNING produced no row"
    return int(row[0])


def _upsert_filing_row(
    conn: psycopg.Connection[tuple],
    *,
    filer_id: int,
    accession_number: str,
    submission_type: str,
    status: str,
    instrument_id: int | None,
    issuer_cik: str,
    issuer_cusip: str,
    securities_class_title: str | None,
    date_of_event: date | None,
    filed_at: datetime | None,
    person: BlockholderReportingPerson,
) -> bool:
    """Per-reporter upsert. Returns True on insert, False on
    re-ingest of the same ``(accession, reporter_cik COALESCE '',
    reporter_name)`` tuple — the partial UNIQUE INDEX from migration
    095 backstops re-runs.
    """
    cur = conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip, securities_class_title,
            reporter_cik, reporter_no_cik, reporter_name,
            member_of_group, type_of_reporting_person, citizenship,
            sole_voting_power, shared_voting_power,
            sole_dispositive_power, shared_dispositive_power,
            aggregate_amount_owned, percent_of_class,
            date_of_event, filed_at
        ) VALUES (
            %(filer_id)s, %(accession_number)s, %(submission_type)s, %(status)s,
            %(instrument_id)s, %(issuer_cik)s, %(issuer_cusip)s, %(securities_class_title)s,
            %(reporter_cik)s, %(reporter_no_cik)s, %(reporter_name)s,
            %(member_of_group)s, %(type_of_reporting_person)s, %(citizenship)s,
            %(sole_voting_power)s, %(shared_voting_power)s,
            %(sole_dispositive_power)s, %(shared_dispositive_power)s,
            %(aggregate_amount_owned)s, %(percent_of_class)s,
            %(date_of_event)s, %(filed_at)s
        )
        ON CONFLICT DO NOTHING
        """,
        {
            "filer_id": filer_id,
            "accession_number": accession_number,
            "submission_type": submission_type,
            "status": status,
            "instrument_id": instrument_id,
            "issuer_cik": issuer_cik,
            "issuer_cusip": issuer_cusip,
            "securities_class_title": securities_class_title,
            "reporter_cik": person.cik,
            "reporter_no_cik": person.no_cik,
            "reporter_name": person.name,
            "member_of_group": person.member_of_group,
            "type_of_reporting_person": person.type_of_reporting_person,
            "citizenship": person.citizenship,
            "sole_voting_power": person.sole_voting_power,
            "shared_voting_power": person.shared_voting_power,
            "sole_dispositive_power": person.sole_dispositive_power,
            "shared_dispositive_power": person.shared_dispositive_power,
            "aggregate_amount_owned": person.aggregate_amount_owned,
            "percent_of_class": person.percent_of_class,
            "date_of_event": date_of_event,
            "filed_at": filed_at,
        },
    )
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Internal mutable helper
# ---------------------------------------------------------------------------


@dataclass
class _MutableSummary:
    cik: str
    accessions_seen: int = 0
    accessions_ingested: int = 0
    accessions_failed: int = 0
    rows_inserted: int = 0
    rows_skipped_no_cusip: int = 0
    submissions_fetch_failed: bool = False
    first_error: str | None = None

    def to_immutable(self) -> IngestSummary:
        return IngestSummary(
            filer_cik=self.cik,
            accessions_seen=self.accessions_seen,
            accessions_ingested=self.accessions_ingested,
            accessions_failed=self.accessions_failed,
            rows_inserted=self.rows_inserted,
            rows_skipped_no_cusip=self.rows_skipped_no_cusip,
            submissions_fetch_failed=self.submissions_fetch_failed,
            first_error=self.first_error,
        )


@dataclass(frozen=True)
class _AccessionOutcome:
    status: str
    rows_inserted: int
    rows_skipped_no_cusip: int
    error: str | None
    submission_type: str | None  # None when the parse never produced one

    @property
    def ingested(self) -> bool:
        return self.status in ("success", "partial")


# ---------------------------------------------------------------------------
# Ingest core
# ---------------------------------------------------------------------------


def _ingest_single_accession(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    filer_cik: str,
    ref: AccessionRef,
) -> _AccessionOutcome:
    """Per-accession driver. Never raises — every fetch / parse
    failure resolves to an ``_AccessionOutcome`` with status='failed'
    so a single malformed accession does not abort the filer batch.

    13D/G accessions only have one canonical XML attachment
    (primary_doc.xml). No archive index walk is necessary.
    """
    primary_url = _archive_file_url(filer_cik, ref.accession_number, "primary_doc.xml")

    primary_xml = sec.fetch_document_text(primary_url)
    if primary_xml is None:
        logger.info(
            "13D/G ingest: primary_doc.xml 404/error for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            rows_inserted=0,
            rows_skipped_no_cusip=0,
            error="primary_doc.xml fetch failed",
            submission_type=None,
        )

    try:
        filing: BlockholderFiling = parse_primary_doc(primary_xml)
    except (ValueError, ET.ParseError) as exc:
        # ``ET.ParseError`` covers malformed XML (truncated download,
        # mid-byte cutoff, accidental HTML error page returned with a
        # 200). ``ValueError`` covers parser-side schema errors
        # (missing required field, unrecognised submissionType).
        # Both must tombstone so the batch loop does not roll back
        # the whole filer's transaction. Codex pre-push review caught
        # the missing ParseError handler.
        logger.exception(
            "13D/G ingest: primary_doc.xml parse failed for cik=%s accession=%s",
            filer_cik,
            ref.accession_number,
        )
        return _AccessionOutcome(
            status="failed",
            rows_inserted=0,
            rows_skipped_no_cusip=0,
            error=f"primary_doc.xml parse failed: {exc}",
            submission_type=None,
        )

    instrument_id = _resolve_cusip_to_instrument_id(conn, filing.issuer_cusip)
    skipped_no_cusip = 0 if instrument_id is not None else len(filing.reporting_persons)

    # Resolve the *filer's* canonical name. ``filing.issuer_name`` is
    # the issuer (the company being filed against), not the filer.
    # The filer's name is not carried directly in primary_doc.xml —
    # the SEC schema records only the filer's CIK in
    # ``<filerCredentials>``. We pick:
    #   1. A reporting-person row whose CIK matches the primary
    #      filer's CIK — covers the common case where the filer is
    #      also one of the reporters on its own filing.
    #   2. Otherwise the first reporting person — the filer is then
    #      a service company (transfer agent, edgar agent) acting on
    #      behalf of the reporters; the first reporter's name is the
    #      most useful operator-facing label.
    #   3. Otherwise a placeholder. Should not occur — the parser
    #      already raises ValueError when reporting_persons is empty.
    filer_name = next(
        (p.name for p in filing.reporting_persons if p.cik == filing.primary_filer_cik),
        filing.reporting_persons[0].name if filing.reporting_persons else f"CIK {filing.primary_filer_cik}",
    )
    filer_id = _upsert_filer(conn, cik=filing.primary_filer_cik, name=filer_name)

    inserted = 0
    for person in filing.reporting_persons:
        if _upsert_filing_row(
            conn,
            filer_id=filer_id,
            accession_number=ref.accession_number,
            submission_type=filing.submission_type,
            status=filing.status,
            instrument_id=instrument_id,
            issuer_cik=filing.issuer_cik,
            issuer_cusip=filing.issuer_cusip,
            securities_class_title=filing.securities_class_title,
            date_of_event=filing.date_of_event,
            filed_at=filing.filed_at or ref.filed_at,
            person=person,
        ):
            inserted += 1

    if instrument_id is None:
        # Issuer CUSIP unresolved — every reporter row writes with
        # ``instrument_id IS NULL``. Mark the accession ``partial`` so
        # the operator sees the gap on the ops monitor and the audit
        # trail tracks why the rows are unjoinable to ``instruments``.
        return _AccessionOutcome(
            status="partial",
            rows_inserted=inserted,
            rows_skipped_no_cusip=skipped_no_cusip,
            error=f"issuer CUSIP {filing.issuer_cusip!r} unresolved (gated by #740 backfill)",
            submission_type=filing.submission_type,
        )

    return _AccessionOutcome(
        status="success",
        rows_inserted=inserted,
        rows_skipped_no_cusip=0,
        error=None,
        submission_type=filing.submission_type,
    )


def ingest_filer_blockholders(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
    *,
    filer_cik: str,
) -> IngestSummary:
    """Fetch + parse + upsert every pending 13D/G filing for one filer.

    ``filer_cik`` is normalised to 10-digit padded form on entry.
    The function commits no transactions itself — the caller (test
    code or :func:`ingest_all_active_filers`) decides commit cadence.
    """
    cik = _zero_pad_cik(filer_cik)
    summary = _MutableSummary(cik=cik)

    submissions_payload = sec.fetch_document_text(_submissions_url(cik))
    if submissions_payload is None:
        logger.warning("13D/G ingest: submissions JSON 404/error for cik=%s", cik)
        summary.submissions_fetch_failed = True
        summary.first_error = "submissions JSON 404/error"
        return summary.to_immutable()

    pending_accessions = parse_submissions_index(submissions_payload)
    if pending_accessions is None:
        # Malformed 200-body — treated the same as a 404 so a stale
        # CIK whose archive returns garbage (or HTML) does not
        # silently masquerade as "no filings = success".
        logger.warning("13D/G ingest: submissions JSON malformed for cik=%s", cik)
        summary.submissions_fetch_failed = True
        summary.first_error = "submissions JSON malformed"
        return summary.to_immutable()
    summary.accessions_seen = len(pending_accessions)

    already_ingested = _existing_accessions_for_filer(conn, filer_cik=cik)

    for ref in pending_accessions:
        if ref.accession_number in already_ingested:
            continue
        outcome = _ingest_single_accession(conn, sec, filer_cik=cik, ref=ref)
        _record_ingest_attempt(
            conn,
            filer_cik=cik,
            accession_number=ref.accession_number,
            submission_type=outcome.submission_type or ref.filing_type,
            status=outcome.status,
            rows_inserted=outcome.rows_inserted,
            rows_skipped=outcome.rows_skipped_no_cusip,
            error=outcome.error,
        )
        if outcome.ingested:
            summary.accessions_ingested += 1
        else:
            summary.accessions_failed += 1
            if outcome.error and summary.first_error is None:
                summary.first_error = f"{ref.accession_number}: {outcome.error}"
        summary.rows_inserted += outcome.rows_inserted
        summary.rows_skipped_no_cusip += outcome.rows_skipped_no_cusip

    return summary.to_immutable()


def ingest_all_active_filers(
    conn: psycopg.Connection[tuple],
    sec: SecArchiveFetcher,
) -> list[IngestSummary]:
    """Walk every active row in ``blockholder_filer_seeds`` and ingest."""
    seeds = _list_active_filer_seeds(conn)
    if not seeds:
        logger.info("13D/G ingest: no active filer seeds; nothing to do")
        return []

    run_id = start_ingestion_run(
        conn,
        source="sec_edgar_13dg",
        endpoint="/Archives/edgar/data/{cik}/{accession}/primary_doc.xml",
        instrument_count=len(seeds),
    )
    conn.commit()

    rows_upserted = 0
    rows_skipped = 0
    summaries: list[IngestSummary] = []
    crash_error: str | None = None
    accession_failures = 0
    submissions_failures = 0
    first_filer_error: str | None = None
    try:
        for cik in seeds:
            try:
                summary = ingest_filer_blockholders(conn, sec, filer_cik=cik)
            except Exception as exc:  # noqa: BLE001 — per-filer crash must not abort the batch
                logger.exception("13D/G ingest: filer %s raised; continuing batch", cik)
                crash_error = f"{cik}: {exc}"
                conn.rollback()
                continue
            conn.commit()
            summaries.append(summary)
            rows_upserted += summary.rows_inserted
            rows_skipped += summary.rows_skipped_no_cusip
            accession_failures += summary.accessions_failed
            if summary.submissions_fetch_failed:
                submissions_failures += 1
            if summary.first_error and first_filer_error is None:
                first_filer_error = f"{cik} {summary.first_error}"
    finally:
        # Status precedence:
        #   * any per-filer crash + zero summaries -> failed
        #   * any per-filer crash with summaries  -> partial
        #   * any submissions-fetch failure        -> partial
        #     (a curated seed is silently invisible without this)
        #   * any per-accession failure            -> partial
        #   * any unresolved-CUSIP skip            -> partial
        #   * else                                 -> success
        if crash_error and not summaries:
            status = "failed"
        elif crash_error or accession_failures > 0 or rows_skipped > 0 or submissions_failures > 0:
            status = "partial"
        else:
            status = "success"
        error_parts: list[str] = []
        if crash_error:
            error_parts.append(f"crash: {crash_error}")
        if submissions_failures > 0:
            error_parts.append(f"{submissions_failures} filer submissions fetch failed")
        if first_filer_error:
            error_parts.append(f"first: {first_filer_error}")
        if rows_skipped > 0 and not error_parts:
            error_parts.append(f"{rows_skipped} reporter rows skipped — issuer CUSIPs unresolved (#740)")
        finish_ingestion_run(
            conn,
            run_id=run_id,
            status=status,
            rows_upserted=rows_upserted,
            rows_skipped=rows_skipped,
            error="; ".join(error_parts) or None,
        )
        conn.commit()

    return summaries


# ---------------------------------------------------------------------------
# Amendment-chain aggregator
# ---------------------------------------------------------------------------


def latest_blockholder_positions(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
) -> list[BlockholderPosition]:
    """Return the latest non-superseded blockholder filing per
    ``(reporter_identity, issuer_cik)`` for one instrument.

    Reporter identity is ``COALESCE(reporter_cik, reporter_name)``,
    matching the schema's hot-path index. The aggregator picks the
    row with the latest ``filed_at`` per identity regardless of form
    type, so a 13D filed after a prior 13G/A by the same reporter on
    the same issuer correctly supersedes the 13G chain (passive →
    active conversion).

    Ordering: ``filed_at DESC NULLS LAST`` so a row with NULL filed_at
    (parser failed to extract a signature date) never wins against a
    row with a real filed_at.

    The PR 3 reader endpoint calls this directly. Exposed as a
    service function rather than an ORM-style method so unit tests
    can call it without going through the HTTP layer.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (
                COALESCE(reporter_cik, reporter_name),
                issuer_cik
            )
                COALESCE(reporter_cik, reporter_name) AS reporter_identity,
                reporter_cik,
                reporter_name,
                issuer_cik,
                instrument_id,
                submission_type,
                status,
                aggregate_amount_owned,
                percent_of_class,
                member_of_group,
                type_of_reporting_person,
                accession_number,
                date_of_event,
                filed_at
            FROM blockholder_filings
            WHERE instrument_id = %(instrument_id)s
            ORDER BY
                COALESCE(reporter_cik, reporter_name),
                issuer_cik,
                filed_at DESC NULLS LAST,
                accession_number DESC
            """,
            {"instrument_id": instrument_id},
        )
        rows = cur.fetchall()

    return [
        BlockholderPosition(
            reporter_identity=row["reporter_identity"],
            reporter_cik=row["reporter_cik"],
            reporter_name=row["reporter_name"],
            issuer_cik=row["issuer_cik"],
            instrument_id=row["instrument_id"],
            submission_type=row["submission_type"],
            status=row["status"],
            aggregate_amount_owned=row["aggregate_amount_owned"],
            percent_of_class=row["percent_of_class"],
            member_of_group=row["member_of_group"],
            type_of_reporting_person=row["type_of_reporting_person"],
            accession_number=row["accession_number"],
            date_of_event=row["date_of_event"],
            filed_at=row["filed_at"],
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Iterator (exposed for ad-hoc reporting / debug)
# ---------------------------------------------------------------------------


def iter_filer_filings(
    conn: psycopg.Connection[tuple],
    *,
    filer_cik: str,
    limit: int = 1000,
) -> Iterator[dict[str, Any]]:
    """Yield the most recent filings for one primary filer."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT bf.accession_number, bf.submission_type, bf.status,
                   bf.reporter_name, bf.reporter_cik, bf.aggregate_amount_owned,
                   bf.percent_of_class, bf.date_of_event, bf.filed_at,
                   bf.issuer_cusip, i.symbol, i.company_name
            FROM blockholder_filings bf
            JOIN blockholder_filers f USING (filer_id)
            LEFT JOIN instruments i ON i.instrument_id = bf.instrument_id
            WHERE f.cik = %(cik)s
            ORDER BY bf.filed_at DESC NULLS LAST, bf.accession_number DESC
            LIMIT %(limit)s
            """,
            {"cik": _zero_pad_cik(filer_cik), "limit": limit},
        )
        for row in cur.fetchall():
            yield dict(row)
