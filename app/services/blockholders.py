"""SEC Schedule 13D / 13G blockholder shared helpers (#766 PR 2 of 3).

Historical seed-walking entrypoints (``ingest_all_active_filers``,
``ingest_filer_blockholders``, ``_list_active_filer_seeds``,
``seed_filer``) were retired in #1233 PR11 once the manifest-worker
path (:mod:`app.services.manifest_parsers.sec_13dg`) became the sole
production write path. The operator-curated ``blockholder_filer_seeds``
table was dropped in the same epic (sql/161). Discovery of new SC
13D/G accessions rides the legacy daily-index path
(``app/services/filings_history.py`` → ``sec_filing_manifest``) — the
v8 empirical pivot 2026-05-21 abandoned the universe-issuer-CIK
discovery layer after smoke against AAPL/GME/MSFT/JPM/HD showed that
``efts.sec.gov/LATEST/search-index`` post-2024-12-18 indexes SC 13D/G
by FILER CIK only, not SUBJECT CIK.

This module now exposes the surviving shared substrate consumed by
the manifest parser, the rewash pipeline, and the PR 3 reader
endpoint:

  * Retention helpers (:func:`blockholders_retention_cutoff`,
    :func:`blockholders_within_retention`) — the canonical
    cutoff used at every 13D/G writer chokepoint per spec §3.2.
  * Public dataclasses (:class:`AccessionRef`,
    :class:`BlockholderPosition`).
  * Lower-level DB helpers (:func:`_upsert_filer`,
    :func:`_upsert_filing_row`,
    :func:`_record_13dg_observation_for_filing`,
    :func:`_resolve_cusip_to_instrument_id`,
    :func:`_record_ingest_attempt`).
  * URL builders (:func:`_archive_file_url`,
    :func:`_submissions_url`).
  * Submissions-index parser (:func:`parse_submissions_index`).
  * Amendment-chain aggregator (:func:`latest_blockholder_positions`)
    consumed by the PR 3 reader endpoint.

Tombstones: a filing whose primary_doc.xml fetch 404s is recorded in
``blockholder_filings_ingest_log`` with ``status='failed'`` plus the
accession number in ``error``. The next run sees the accession is
still missing and skips it — short-lived 404s heal naturally;
persistent failures show up in the ops monitor (#13). To force a
retry the operator deletes the log row.
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
from app.services import raw_filings
from app.services.ownership_observations import (
    record_blockholder_observation,
    refresh_blockholders_current,
)
from app.services.sec_identity import siblings_for_issuer_cik

# v2 (#1628): issuer CUSIP is now extracted from the unified mandate
# schema (``<issuerCusips>/<issuerCusipNumber>``) — edgartools 5.30.2 +
# the legacy extractors read the stale flat ``<issuerCUSIP>`` and
# returned an empty CUSIP, so CUSIP-only resolution never resolved and
# blockholders never populated. The bump flips sec_13d/sec_13g manifest
# rows back to ``pending`` so they re-drain through the fixed extractor.
_PARSER_VERSION_13DG = "13dg-primary-v2"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Retention helpers (#1233 PR11, spec §3.2)
# ---------------------------------------------------------------------------
#
# Schedule 13D / 13G ingest is capped at the more-recent of (today − 3y) and
# the SEC Schedule 13 structured-XML mandate effective date (2024-12-18, SEC
# EDGAR Release 23.4). Filings made BEFORE the mandate are HTML-only and not
# parseable by ``edgartools.beneficial_ownership.schedule13`` (skill_edgartools
# G11) or by any extant library in this repo. PR11 honours "100% complete"
# by capping retention at the floor so every filing inside the window is
# guaranteed parseable. Once ``today − 3y >= 2024-12-18`` (i.e. on / after
# 2027-12-18) the function reverts to plain ``today − 3y``.
#
# Calendar-day granularity: the helper returns ``date`` (NOT ``datetime``).
# The SEC mandate is a calendar-day effective date — comparing as ``date``
# avoids timezone ambiguity at the day boundary, and the discovery query
# param ``efts.sec.gov/.../search-index?&startdt=YYYY-MM-DD`` expects an
# ISO calendar date too. Returning ``datetime`` would force every consumer
# to ``.date()`` at the call site and risk timezone-driven off-by-one drift.
INSIDER_BLOCKHOLDERS_RETENTION_YEARS = 3

# SEC EDGAR Release 23.4 effective date — Schedule 13 XBRL mandate.
SEC_SCHEDULE_13_XML_MANDATE_DATE = date(2024, 12, 18)


def blockholders_retention_cutoff() -> date:
    """Inclusive lower bound on ``filed_at`` for Schedule 13D/13G ingest.

    Returns ``max(today − 3y, SEC_SCHEDULE_13_XML_MANDATE_DATE)`` as a
    ``date`` (calendar-day granularity; see module-level rationale). The
    XML-mandate floor dominates while ``today − 3y`` is still earlier
    than 2024-12-18, and degrades to the plain 3-year rolling floor once
    the rolling boundary catches up.
    """
    today = datetime.now(tz=UTC).date()
    # Calendar-exact 3-year subtraction (not 365×3=1095 days) so the
    # floor doesn't drift by ±1 day across leap-year boundaries — bot
    # NITPICK 2026-05-21. Feb 29 today → Feb 28 floor three years prior
    # (no Feb 29 in non-leap-year fallback).
    try:
        three_year_floor = today.replace(year=today.year - INSIDER_BLOCKHOLDERS_RETENTION_YEARS)
    except ValueError:
        three_year_floor = today.replace(year=today.year - INSIDER_BLOCKHOLDERS_RETENTION_YEARS, day=28)
    return max(three_year_floor, SEC_SCHEDULE_13_XML_MANDATE_DATE)


def blockholders_within_retention(filed_at: datetime | None) -> bool:
    """Inclusive retention predicate used by every 13D/G writer chokepoint.

    ``filed_at is None`` resolves to ``False`` (defensive — a row missing
    the canonical filing timestamp cannot be safely placed inside the
    retention window). Otherwise returns
    ``filed_at.date() >= blockholders_retention_cutoff()`` so a filing
    timestamped exactly at the cutoff midnight UTC is retained.
    """
    if filed_at is None:
        return False
    return filed_at.date() >= blockholders_retention_cutoff()


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


def _resolve_issuer_to_instrument_id(
    conn: psycopg.Connection[tuple],
    *,
    cusip: str | None,
    cik: str | None,
) -> int | None:
    """Resolve a 13D/G subject company to an instrument_id (#1628).

    CUSIP is the security-precise key — it disambiguates share-class
    siblings (GOOG/GOOGL share an issuer CIK but not a CUSIP; settled
    "CIK = entity, CUSIP = security" #1102). CIK is a single-class-only
    fallback for the coverage gap (we hold more ``(sec, cik)`` mappings
    than ``(sec, cusip)`` ones). Order:

      1. CUSIP -> ``external_identifiers`` (provider ``sec`` | ``openfigi``,
         ``sec`` canonical first via the CASE-ordered tiebreak). Precise;
         safe for multi-class issuers.
      2. else, if CIK maps to EXACTLY ONE instrument -> that instrument
         (single-class issuer — safe + broader coverage). A multi-class
         CIK (or zero siblings, or a malformed CIK) with an unresolved
         CUSIP stays ``None``: we never guess the share class and never
         fan out (a 5%+ holder owns ONE class). The caller still writes
         the ``blockholder_filings`` audit row with ``instrument_id``
         NULL (status ``partial``).
    """
    if cusip:
        cur = conn.execute(
            """
            SELECT instrument_id
            FROM external_identifiers
            WHERE provider IN ('sec', 'openfigi')
              AND identifier_type = 'cusip'
              AND identifier_value = %(cusip)s
            ORDER BY CASE provider WHEN 'sec' THEN 0 ELSE 1 END,
                     is_primary DESC,
                     external_identifier_id ASC
            LIMIT 1
            """,
            {"cusip": cusip.strip().upper()},
        )
        row = cur.fetchone()
        if row is not None:
            return int(row[0])
    if cik:
        try:
            siblings = siblings_for_issuer_cik(conn, cik)
        except ValueError:
            return None  # malformed CIK — leave unresolved (audit row still written)
        if len(siblings) == 1:
            return siblings[0]
    return None


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
    batch_run_id: Any,
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
    # Persist raw body BEFORE parsing — re-wash workflows depend on
    # this row even if parsing fails. Operator audit 2026-05-03 +
    # PR #808 contract. Commit immediately so a later per-filer
    # exception that triggers ``conn.rollback()`` upstream cannot
    # take this row down with it (Codex pre-push review).
    raw_filings.store_raw(
        conn,
        accession_number=ref.accession_number,
        document_kind="primary_doc_13dg",
        payload=primary_xml,
        parser_version=_PARSER_VERSION_13DG,
        source_url=primary_url,
    )
    conn.commit()

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

    instrument_id = _resolve_issuer_to_instrument_id(conn, cusip=filing.issuer_cusip, cik=filing.issuer_cik)
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
        # Skip observation write-through: ownership_blockholders_observations
        # requires a non-null instrument_id (CHECK constraint on
        # ``subject_type='issuer'``-equivalent for this table).
        return _AccessionOutcome(
            status="partial",
            rows_inserted=inserted,
            rows_skipped_no_cusip=skipped_no_cusip,
            error=f"issuer CUSIP {filing.issuer_cusip!r} unresolved (gated by #740 backfill)",
            submission_type=filing.submission_type,
        )

    # Write-through observation + refresh _current (#890 / spec
    # §"Eliminate periodic re-scan jobs"). Replaces the legacy nightly
    # ownership_observations_sync.sync_blockholders read-from-typed-
    # tables path. One observation per (accession, primary filer) per
    # the SEC convention that joint reporters claim the same beneficial
    # figure; pick the row with the highest aggregate_amount_owned to
    # match the legacy DISTINCT ON ... ORDER BY ... DESC NULLS LAST.
    _record_13dg_observation_for_filing(
        conn,
        instrument_id=instrument_id,
        accession_number=ref.accession_number,
        primary_document_url=primary_url,
        filing=filing,
        filer_name=filer_name,
        ref=ref,
        run_id=batch_run_id,
    )
    refresh_blockholders_current(conn, instrument_id=instrument_id)

    return _AccessionOutcome(
        status="success",
        rows_inserted=inserted,
        rows_skipped_no_cusip=0,
        error=None,
        submission_type=filing.submission_type,
    )


def _record_13dg_observation_for_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    filing: BlockholderFiling,
    filer_name: str,
    ref: AccessionRef,
    run_id: Any,
) -> None:
    """Record one ``ownership_blockholders_observations`` row for one
    13D/G accession.

    Mirrors the legacy batch-sync rule in
    ``ownership_observations_sync.sync_blockholders``:

      - Identity: PRIMARY filer's CIK (``filing.primary_filer_cik``),
        NEVER the per-row reporter_cik. Joint reporters on the same
        accession collapse to one observation per the SEC convention
        that joint filers claim the same beneficial figure on the
        cover page (#837 lesson).
      - Picks the reporting_persons row with the highest
        ``aggregate_amount_owned`` (DESC NULLS LAST) — matches the
        legacy ``DISTINCT ON (accession, filer_id) ORDER BY ...``.
      - Source enum: ``'13d'`` for SCHEDULE 13D family, ``'13g'`` for
        SCHEDULE 13G.
      - Filter: ``aggregate_amount_owned IS NOT NULL`` AND
        ``filed_at IS NOT NULL`` — both required by the observation
        contract.
    """
    if not filing.reporting_persons:
        return
    filed_at = filing.filed_at or ref.filed_at
    if filed_at is None:
        return
    # Match legacy DISTINCT ON ... ORDER BY aggregate_amount_owned
    # DESC NULLS LAST. ``key=lambda`` with ``-Decimal`` would crash on
    # NULL; use a sentinel that floats NULLs to the bottom.
    chosen = max(
        filing.reporting_persons,
        key=lambda p: (p.aggregate_amount_owned is not None, p.aggregate_amount_owned or Decimal(0)),
    )
    if chosen.aggregate_amount_owned is None:
        return
    stype = filing.submission_type
    source = "13d" if stype.startswith("SCHEDULE 13D") else "13g"
    record_blockholder_observation(
        conn,
        instrument_id=instrument_id,
        reporter_cik=filing.primary_filer_cik,
        reporter_name=filer_name,
        ownership_nature="beneficial",
        submission_type=stype,
        status_flag=filing.status,
        source=source,  # type: ignore[arg-type]
        source_document_id=accession_number,
        source_accession=accession_number,
        source_field=None,
        source_url=primary_document_url or None,
        filed_at=filed_at,
        period_start=None,
        period_end=filed_at.date(),
        ingest_run_id=run_id,
        aggregate_amount_owned=chosen.aggregate_amount_owned,
        percent_of_class=chosen.percent_of_class,
    )


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
