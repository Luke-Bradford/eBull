"""C4 — bulk Insider Transactions Data Sets ingester (#1024).

Reads cached Insider Transactions Data Sets ZIPs (downloaded by
Phase A3, #1021) and writes ``ownership_insiders_observations`` rows
for every Form 3/4/5 filing whose issuer CIK resolves to a universe
instrument.

Each ZIP (``<YYYY>q<N>_form345.zip``) contains TSVs:

  - ``SUBMISSION.tsv`` — one row per filing.
  - ``REPORTING_OWNER.tsv`` — one row per insider per filing.
  - ``NON_DERIV_TRANS.tsv`` — non-derivative transactions.
  - ``NON_DERIV_HOLDING.tsv`` — post-transaction holdings (the
    canonical "shares-owned-following-transaction" figure).
  - ``DERIV_TRANS.tsv`` / ``DERIV_HOLDING.tsv`` — derivatives,
    not used here (existing per-filing parser also only writes
    non-derivative shares-owned).

Replaces S9 (`sec_insider_transactions_backfill`) + S10
(`sec_form3_ingest`) on a fresh install. The bulk archive covers
Forms 3, 4, AND 5; the per-filing path covered only 3 + 4. Form 5
(annual catch-up) is a free upgrade.

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md

PR-3 (#1233 v3 §7) — per-archive COPY refactor. The target table
``ownership_insiders_observations.holder_identity_key`` is a
GENERATED STORED column derived from ``holder_cik`` / ``holder_name``;
the staging table omits it + INSERT...SELECT lets the target
re-generate the column on insert (the unique index on the generated
column still drives ON CONFLICT inference because PG materialises
the generated value before index lookup).
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import psycopg

from app.services.insider_transactions import form4_retention_cutoff, form5_retention_cutoff

logger = logging.getLogger(__name__)


@dataclass
class InsiderIngestResult:
    """Per-archive ingest outcome."""

    submissions_seen: int = 0
    rows_written: int = 0
    rows_skipped_unresolved_cik: int = 0
    rows_skipped_orphan_owner: int = 0
    rows_skipped_bad_data: int = 0
    # PR4 (#1233 §4.3) — Form 4 / 4-A rows whose ``FILING_DATE`` falls
    # before the 3y retention cap. Counted separately from
    # ``rows_skipped_bad_data`` so operator-visible logging can
    # distinguish deliberate retention skips from malformed input.
    # PR10b (#1233 §4.4) extends the same counter to Form 5 / 5-A
    # rows outside the 18-month cap. Form 3 rows are still unbounded
    # (per §4.4 — Form 3 is read-side latest-per-pair).
    rows_skipped_retention: int = 0
    parse_errors: int = 0
    touched_instrument_ids: set[int] = field(default_factory=set)


# ---------------------------------------------------------------------------
# CIK → instrument lookup
# ---------------------------------------------------------------------------


def _load_cik_to_instrument(conn: psycopg.Connection[Any]) -> dict[str, list[int]]:
    """Return ``{cik_padded: [instrument_id, ...]}`` for every CIK-mapped instrument.

    Multimap shape — share-class siblings (GOOG/GOOGL, BRK.A/BRK.B) co-bind
    a single SEC CIK per #1102. Collapsing to ``dict[str, int]`` would
    silently drop one sibling on every bulk run, leaving it without
    insider observations.
    """
    out: dict[str, list[int]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, identifier_value
            FROM external_identifiers
            WHERE provider = 'sec' AND identifier_type = 'cik'
            """,
        )
        for row in cur.fetchall():
            instrument_id, identifier = row
            cik = str(identifier).zfill(10)
            out.setdefault(cik, []).append(int(instrument_id))
    return out


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


_FALLBACK_DATE_FORMATS = (
    "%d-%b-%Y",  # 14-Nov-2025 — SEC Insider readme documents this for FILING_DATE
    "%d-%b-%y",
    "%d-%B-%Y",  # 14-November-2025
    "%Y-%m-%d",  # ISO defensive fallback
)


def _parse_filing_date(value: str | None) -> datetime | None:
    """Parse a FILING_DATE that may be ISO or ``DD-MON-YYYY``.

    SEC Insider Transactions Data Sets readme documents
    ``FILING_DATE`` as ``DD-MON-YYYY`` (e.g. ``14-NOV-2025``). The
    dataset has used both upper and mixed-case month abbreviations
    historically, and some quarters publish ISO; accept all three.
    """
    if not value:
        return None
    text = value.strip()
    try:
        return datetime.fromisoformat(text).replace(tzinfo=UTC)
    except ValueError:
        pass
    for fmt in _FALLBACK_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    titled = text.title()
    for fmt in _FALLBACK_DATE_FORMATS:
        try:
            return datetime.strptime(titled, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    for fmt in _FALLBACK_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    titled = text.title()
    for fmt in _FALLBACK_DATE_FORMATS:
        try:
            return datetime.strptime(titled, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None or not value.strip():
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as _exc:
        del _exc
        return None


_OFFICER_DIRECTOR_FLAGS = {"OFFICER", "DIRECTOR", "ISOFFICER", "ISDIRECTOR"}
_TEN_PERCENT_FLAGS = {"TENPERCENTOWNER", "TEN_PERCENT_OWNER", "10PERCENTOWNER"}


def _map_relationship(row: dict[str, str]) -> Literal["direct", "indirect", "beneficial"]:
    """Map REPORTING_OWNER's relationship flags to ownership_nature.

    Spec: officer/director → 'direct'; ten-percent owner → 'beneficial'.
    Default 'direct' so a row with no relationship flag still lands
    in the schema CHECK enum.
    """
    flags = " ".join(
        (row.get(k) or "").upper()
        for k in (
            "RPTOWNER_RELATIONSHIP",
            "IS_OFFICER",
            "IS_DIRECTOR",
            "IS_TEN_PERCENT_OWNER",
            "IS_OTHER",
        )
    )
    if any(token in flags for token in _OFFICER_DIRECTOR_FLAGS):
        return "direct"
    if any(token in flags for token in _TEN_PERCENT_FLAGS):
        return "beneficial"
    return "direct"


def _map_form_to_source(form: str) -> Literal["form3", "form4"]:
    """Map dataset's FORM column to the observations source enum.

    Form 3 / 3-A → ``form3``. Form 4 / 4-A and Form 5 / 5-A → ``form4``
    (Form 5 is the annual catch-up of the same Form-4 transaction
    universe; the existing source-priority chain treats it as form4).
    """
    upper = (form or "").strip().upper()
    if upper.startswith("3"):
        return "form3"
    return "form4"


# ---------------------------------------------------------------------------
# TSV helpers
# ---------------------------------------------------------------------------


def _open_tsv(zf: zipfile.ZipFile, *candidate_names: str) -> list[dict[str, str]]:
    """Open the first matching TSV from a list of candidate filenames.

    SEC Insider Transactions Data Sets historically publish under
    both ``REPORTING_OWNER.tsv`` and ``REPORTINGOWNER.tsv`` (and same
    fork for the holding/trans tables). The Insider readme PDF
    documents the underscore-free form as primary; older quarters
    used the underscored form. Accept either by passing both names.
    """
    available = zf.namelist()
    for name in candidate_names:
        if name in available:
            with zf.open(name) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
                return list(csv.DictReader(text, delimiter="\t"))
    # Try suffix-match (some archives nest under a directory).
    for name in candidate_names:
        nested = [n for n in available if n.endswith("/" + name)]
        if nested:
            with zf.open(nested[0]) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
                return list(csv.DictReader(text, delimiter="\t"))
    return []


def _iter_tsv(zf: zipfile.ZipFile, *candidate_names: str) -> Iterator[dict[str, str]]:
    """Stream rows from a TSV (used for the large transaction / holding
    tables which can hold millions of rows per quarter).
    """
    available = zf.namelist()
    target: str | None = None
    for name in candidate_names:
        if name in available:
            target = name
            break
    if target is None:
        for name in candidate_names:
            nested = [n for n in available if n.endswith("/" + name)]
            if nested:
                target = nested[0]
                break
    if target is None:
        return
    with zf.open(target) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        yield from csv.DictReader(text, delimiter="\t")


# ---------------------------------------------------------------------------
# PR-3 — per-archive staging table lifecycle
# ---------------------------------------------------------------------------


# ``holder_identity_key`` is a GENERATED STORED column on the target
# table — omit from both the COPY column list and the staging schema.
# The INSERT...SELECT into the target re-derives it from
# ``(holder_cik, holder_name)`` so ON CONFLICT inference on the
# generated key works unchanged.
_STG_COPY_COLUMNS = (
    "instrument_id",
    "holder_cik",
    "holder_name",
    "ownership_nature",
    "source",
    "source_document_id",
    "source_accession",
    "source_field",
    "source_url",
    "filed_at",
    "period_start",
    "period_end",
    "ingest_run_id",
    "shares",
)


_CREATE_STG_SQL = """
CREATE TEMP TABLE _stg_insider (
    instrument_id      BIGINT,
    holder_cik         TEXT,
    holder_name        TEXT,
    ownership_nature   TEXT,
    source             TEXT,
    source_document_id TEXT,
    source_accession   TEXT,
    source_field       TEXT,
    source_url         TEXT,
    filed_at           TIMESTAMPTZ,
    period_start       DATE,
    period_end         DATE,
    ingest_run_id      UUID,
    shares             NUMERIC(24, 4)
) ON COMMIT DROP
"""


# Drain into observations table. ON CONFLICT key uses
# ``holder_identity_key`` (GENERATED STORED on the target) so the
# INSERT column list omits it; PG derives the key from the inserted
# ``(holder_cik, holder_name)`` and consults the unique index built
# on the generated column.
#
# DISTINCT ON dedupes staging rows BEFORE the INSERT — without this,
# two staging rows that share a conflict tuple (e.g. an archive with
# an amendment to a prior filing under the same accession) trigger
# PG's ``cardinality_violation``. The DISTINCT ON expression
# materialises the same identity-key formula the target table uses
# in its GENERATED STORED column. ``ctid DESC`` preserves the
# last-write-wins semantic of the legacy per-row INSERT path.
_INSERT_FROM_STG_SQL = """
INSERT INTO ownership_insiders_observations (
    instrument_id, holder_cik, holder_name, ownership_nature,
    source, source_document_id, source_accession, source_field, source_url,
    filed_at, period_start, period_end, ingest_run_id, shares
)
SELECT DISTINCT ON (
    instrument_id,
    CASE
        WHEN holder_cik IS NOT NULL AND length(trim(holder_cik)) > 0
            THEN 'CIK:' || trim(holder_cik)
        ELSE 'NAME:' || lower(trim(holder_name))
    END,
    ownership_nature, source, source_document_id, period_end
)
    instrument_id, holder_cik, holder_name, ownership_nature,
    source, source_document_id, source_accession, source_field, source_url,
    filed_at, period_start, period_end, ingest_run_id, shares
FROM _stg_insider
ORDER BY
    instrument_id,
    CASE
        WHEN holder_cik IS NOT NULL AND length(trim(holder_cik)) > 0
            THEN 'CIK:' || trim(holder_cik)
        ELSE 'NAME:' || lower(trim(holder_name))
    END,
    ownership_nature, source, source_document_id, period_end,
    ctid DESC
ON CONFLICT (
    instrument_id, holder_identity_key, ownership_nature, source,
    source_document_id, period_end
)
DO UPDATE SET
    holder_name = EXCLUDED.holder_name,
    source_accession = EXCLUDED.source_accession,
    source_field = EXCLUDED.source_field,
    source_url = EXCLUDED.source_url,
    filed_at = EXCLUDED.filed_at,
    period_start = EXCLUDED.period_start,
    shares = EXCLUDED.shares,
    ingest_run_id = EXCLUDED.ingest_run_id,
    ingested_at = clock_timestamp()
"""


def _stage_owners(
    *,
    copy: Any,
    owner_list: list[dict[str, str]],
    instrument_id: int,
    issuer_cik: str,
    accn: str,
    source: Literal["form3", "form4"],
    source_document_id: str,
    source_field: str | None,
    filed_at: datetime,
    period_end: date,
    ingest_run_id: UUID,
    shares: Decimal | None,
    result: InsiderIngestResult,
) -> None:
    """Write one staging row per reporting owner on the input row.

    Multi-owner filings (joint Form 4) produce N observation rows;
    the schema PK includes ``holder_identity_key`` so distinct owners
    coexist within the same accession+nature+period+document_id.
    Mirrors the legacy ``_write_for_owners`` semantics, but stages
    into ``_stg_insider`` via ``copy.write_row`` instead of executing
    a per-row INSERT.
    """
    accession_no_dashes = accn.replace("-", "")
    source_url = f"https://www.sec.gov/Archives/edgar/data/{int(issuer_cik)}/{accession_no_dashes}/"
    for owner in owner_list:
        holder_cik_raw = owner.get("RPTOWNERCIK") or ""
        holder_cik = str(holder_cik_raw).strip().zfill(10) if holder_cik_raw.strip() else None
        holder_name = (owner.get("RPTOWNERNAME") or "").strip()
        if not holder_name:
            result.rows_skipped_bad_data += 1
            continue
        ownership_nature = _map_relationship(owner)

        copy.write_row(
            (
                instrument_id,
                holder_cik,
                holder_name,
                ownership_nature,
                source,
                source_document_id,
                accn,
                source_field,
                source_url,
                filed_at,
                None,
                period_end,
                str(ingest_run_id),
                shares,
            )
        )
        result.touched_instrument_ids.add(instrument_id)


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def ingest_insider_dataset_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    cik_to_instrument: dict[str, list[int]] | None = None,
    ingest_run_id: UUID | None = None,
) -> InsiderIngestResult:
    """Walk one Insider Transactions Data Set ZIP and append observations.

    The dataset's tables join on ``ACCESSION_NUMBER`` and (for the
    holding tables) on ``REPORTING_OWNER`` per filing. The primary
    loop iterates the post-transaction NON_DERIV_HOLDING rows so each
    observation row carries the canonical ``shares-owned-following-transaction``
    figure (matches the existing per-filing Form 4 ingester).

    Returns telemetry suitable for stage reporting.

    PR-3 per-archive lifecycle:
      1. CREATE TEMP TABLE _stg_insider ON COMMIT DROP.
      2. Pre-validate every transaction + holding row in Python.
      3. Cursor-level COPY streams validated rows into staging
         (PG17 ON_ERROR ignore + LOG_VERBOSITY verbose).
      4. INSERT...SELECT...ON CONFLICT drains staging into
         ``ownership_insiders_observations`` (the target's GENERATED
         STORED ``holder_identity_key`` is auto-derived; ON CONFLICT
         inference on the generated key works unchanged).
      5. Orchestrator commits → staging drops via ON COMMIT DROP.
    """
    if cik_to_instrument is None:
        cik_to_instrument = _load_cik_to_instrument(conn)
    if ingest_run_id is None:
        ingest_run_id = uuid4()

    result = InsiderIngestResult()

    # PR4 (#1233 §4.3) — Form 4 / 4-A 3y retention cap, archive scope.
    # PR10b (#1233 §4.4) — Form 5 / 5-A 18-month retention cap, same
    # archive scope. Anchor each cutoff to a single instant for the
    # whole archive so a long walk crossing midnight UTC uses one
    # boundary. Form 3 rows are NOT gated here — Form 3 is read-side
    # latest-per-pair via ``list_baseline_only_insider_holdings``.
    retention_cutoff = form4_retention_cutoff()
    retention_cutoff_form5 = form5_retention_cutoff()

    # PR-3: CREATE TEMP TABLE before opening the COPY context.
    with conn.cursor() as cur:
        cur.execute(_CREATE_STG_SQL)

    with zipfile.ZipFile(archive_path) as zf:
        submissions = _open_tsv(zf, "SUBMISSION.tsv")
        owners = _open_tsv(zf, "REPORTINGOWNER.tsv", "REPORTING_OWNER.tsv")
        holdings = _open_tsv(zf, "NONDERIV_HOLDING.tsv", "NON_DERIV_HOLDING.tsv")
        transactions = _open_tsv(zf, "NONDERIV_TRANS.tsv", "NON_DERIV_TRANS.tsv")
        result.submissions_seen = len(submissions)

        sub_by_accn: dict[str, dict[str, str]] = {
            r["ACCESSION_NUMBER"]: r for r in submissions if "ACCESSION_NUMBER" in r
        }
        owners_by_accn: dict[str, list[dict[str, str]]] = {}
        for owner in owners:
            accn = owner.get("ACCESSION_NUMBER", "").strip()
            if accn:
                owners_by_accn.setdefault(accn, []).append(owner)

        accessions_with_transactions: set[str] = set()
        copy_attempted = 0

        copy_sql = (
            "COPY _stg_insider ("
            + ", ".join(_STG_COPY_COLUMNS)
            + ") FROM STDIN WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)"
        )

        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
            # ─── Primary write path: NONDERIV_TRANS ─────────────────
            # Each transaction carries its own SHRS_OWND_FOLWNG_TRANS
            # (post-transaction shares-owned) per the SEC Insider
            # readme. Write parity with the existing per-filing Form 4
            # ingester requires the primary loop to iterate
            # transactions, not just holdings.
            for trans in transactions:
                accn = trans.get("ACCESSION_NUMBER", "").strip()
                sub = sub_by_accn.get(accn)
                if sub is None:
                    result.rows_skipped_orphan_owner += 1
                    continue
                owner_list = owners_by_accn.get(accn, [])
                if not owner_list:
                    result.rows_skipped_orphan_owner += 1
                    continue
                accessions_with_transactions.add(accn)

                issuer_cik_raw = sub.get("ISSUERCIK") or sub.get("ISSUER_CIK") or ""
                issuer_cik = str(issuer_cik_raw).strip().zfill(10) if issuer_cik_raw.strip() else ""
                matched_instruments = cik_to_instrument.get(issuer_cik, []) if issuer_cik else []
                if not matched_instruments:
                    result.rows_skipped_unresolved_cik += 1
                    continue

                form = sub.get("DOCUMENT_TYPE") or sub.get("FORM_TYPE") or sub.get("FORM") or ""
                source = _map_form_to_source(form)

                filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))
                if filed_at is None:
                    result.rows_skipped_bad_data += 1
                    continue

                form_upper = (form or "").strip().upper()
                if form_upper.startswith("4") and filed_at.date() < retention_cutoff:
                    result.rows_skipped_retention += 1
                    continue
                if form_upper.startswith("5") and filed_at.date() < retention_cutoff_form5:
                    result.rows_skipped_retention += 1
                    continue

                period_end = _parse_iso_date(trans.get("TRANS_DATE")) or _parse_iso_date(sub.get("PERIOD_OF_REPORT"))
                if period_end is None:
                    result.rows_skipped_bad_data += 1
                    continue

                shares = _parse_decimal(trans.get("SHRS_OWND_FOLWNG_TRANS"))

                trans_sk = (trans.get("NONDERIV_TRANS_SK") or trans.get("NON_DERIV_TRANS_SK") or "").strip() or "0"
                source_document_id = f"{accn}:NDT:{trans_sk}"

                # Fan-out across share-class siblings on the same CIK (#1117).
                for instrument_id in matched_instruments:
                    pre_count = copy_attempted
                    _stage_owners(
                        copy=copy,
                        owner_list=owner_list,
                        instrument_id=instrument_id,
                        issuer_cik=issuer_cik,
                        accn=accn,
                        source=source,
                        source_document_id=source_document_id,
                        source_field=trans_sk if trans_sk != "0" else None,
                        filed_at=filed_at,
                        period_end=period_end,
                        ingest_run_id=ingest_run_id,
                        shares=shares,
                        result=result,
                    )
                    # _stage_owners has already counted bad-data
                    # per-owner skips on ``result``; here we only
                    # need to advance ``copy_attempted`` by the number
                    # of rows actually staged for this owner-list.
                    # Each successful owner contributes one row;
                    # bad-data skips contributed zero.
                    copy_attempted = pre_count + (sum(1 for o in owner_list if (o.get("RPTOWNERNAME") or "").strip()))

            # ─── Secondary write path: NONDERIV_HOLDING ─────────────
            # For accessions WITHOUT transactions (Form 3 initial-holdings
            # statements), the holdings rows are the only signal.
            for holding in holdings:
                accn = holding.get("ACCESSION_NUMBER", "").strip()
                sub = sub_by_accn.get(accn)
                if sub is None:
                    result.rows_skipped_orphan_owner += 1
                    continue
                if accn in accessions_with_transactions:
                    continue  # already covered by primary path
                owner_list = owners_by_accn.get(accn, [])
                if not owner_list:
                    result.rows_skipped_orphan_owner += 1
                    continue

                issuer_cik_raw = sub.get("ISSUERCIK") or sub.get("ISSUER_CIK") or ""
                issuer_cik = str(issuer_cik_raw).strip().zfill(10) if issuer_cik_raw.strip() else ""
                matched_instruments = cik_to_instrument.get(issuer_cik, []) if issuer_cik else []
                if not matched_instruments:
                    result.rows_skipped_unresolved_cik += 1
                    continue

                form = sub.get("DOCUMENT_TYPE") or sub.get("FORM_TYPE") or sub.get("FORM") or ""
                source = _map_form_to_source(form)

                filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))
                if filed_at is None:
                    result.rows_skipped_bad_data += 1
                    continue

                # Form 3 initial-holdings statements typically land in
                # THIS loop (no NONDERIV_TRANS rows); the
                # ``startswith("3")`` case is intentionally unguarded —
                # Form 3 is read-side latest-per-pair, not ingest-capped.
                form_upper = (form or "").strip().upper()
                if form_upper.startswith("4") and filed_at.date() < retention_cutoff:
                    result.rows_skipped_retention += 1
                    continue
                if form_upper.startswith("5") and filed_at.date() < retention_cutoff_form5:
                    result.rows_skipped_retention += 1
                    continue

                period_end = _parse_iso_date(sub.get("PERIOD_OF_REPORT"))
                if period_end is None:
                    result.rows_skipped_bad_data += 1
                    continue

                shares = _parse_decimal(holding.get("SHRS_OWND_FOLWNG_TRANS"))
                holding_sk = (
                    holding.get("NONDERIV_HOLDING_SK") or holding.get("NON_DERIV_HOLDING_SK") or ""
                ).strip() or "0"
                source_document_id = f"{accn}:NDH:{holding_sk}"

                # Fan-out across share-class siblings on the same CIK (#1117).
                for instrument_id in matched_instruments:
                    pre_count = copy_attempted
                    _stage_owners(
                        copy=copy,
                        owner_list=owner_list,
                        instrument_id=instrument_id,
                        issuer_cik=issuer_cik,
                        accn=accn,
                        source=source,
                        source_document_id=source_document_id,
                        source_field=holding_sk if holding_sk != "0" else None,
                        filed_at=filed_at,
                        period_end=period_end,
                        ingest_run_id=ingest_run_id,
                        shares=shares,
                        result=result,
                    )
                    copy_attempted = pre_count + (sum(1 for o in owner_list if (o.get("RPTOWNERNAME") or "").strip()))

    # Drain staging into the partitioned observations table.
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM _stg_insider")
        row = cur.fetchone()
        accepted_via_copy = int(row[0]) if row else 0
        skipped_by_copy = copy_attempted - accepted_via_copy
        if skipped_by_copy > 0:
            result.rows_skipped_bad_data += skipped_by_copy
        cur.execute(_INSERT_FROM_STG_SQL)
        if cur.rowcount >= 0:
            result.rows_written = cur.rowcount
        else:
            result.rows_written = accepted_via_copy
    return result
