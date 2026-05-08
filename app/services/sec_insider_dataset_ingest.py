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
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import psycopg

from app.services.ownership_observations import record_insider_observation

logger = logging.getLogger(__name__)


@dataclass
class InsiderIngestResult:
    """Per-archive ingest outcome."""

    submissions_seen: int = 0
    rows_written: int = 0
    rows_skipped_unresolved_cik: int = 0
    rows_skipped_orphan_owner: int = 0
    rows_skipped_bad_data: int = 0
    parse_errors: int = 0


# ---------------------------------------------------------------------------
# CIK → instrument lookup
# ---------------------------------------------------------------------------


def _load_cik_to_instrument(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Return ``{cik_padded: instrument_id}`` for every CIK-mapped instrument."""
    out: dict[str, int] = {}
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
            out[cik] = int(instrument_id)
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
    # SEC publishes ``DD-NOV-YYYY`` — strptime ``%b`` is locale-aware
    # and recognises ``Nov`` but not always upper-case ``NOV``. Try
    # title-casing as a last resort.
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


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def ingest_insider_dataset_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    cik_to_instrument: dict[str, int] | None = None,
    ingest_run_id: UUID | None = None,
) -> InsiderIngestResult:
    """Walk one Insider Transactions Data Set ZIP and append observations.

    The dataset's tables join on ``ACCESSION_NUMBER`` and (for the
    holding tables) on ``REPORTING_OWNER`` per filing. The primary
    loop iterates the post-transaction NON_DERIV_HOLDING rows so each
    observation row carries the canonical ``shares-owned-following-transaction``
    figure (matches the existing per-filing Form 4 ingester).

    Returns telemetry suitable for stage reporting.
    """
    if cik_to_instrument is None:
        cik_to_instrument = _load_cik_to_instrument(conn)
    if ingest_run_id is None:
        ingest_run_id = uuid4()

    result = InsiderIngestResult()

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

        # ─── Primary write path: NONDERIV_TRANS ─────────────────
        # Each transaction carries its own SHRS_OWND_FOLWNG_TRANS
        # (post-transaction shares-owned) per the SEC Insider readme.
        # Codex pre-push round 2 finding: write parity with the
        # existing per-filing Form 4 ingester requires the primary
        # loop to iterate transactions, not just holdings (most
        # Form 4 filings have transactions but no NONDERIV_HOLDING
        # row).
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
            instrument_id = cik_to_instrument.get(issuer_cik) if issuer_cik else None
            if instrument_id is None:
                result.rows_skipped_unresolved_cik += 1
                continue

            form = sub.get("DOCUMENT_TYPE") or sub.get("FORM_TYPE") or sub.get("FORM") or ""
            source = _map_form_to_source(form)

            filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))
            if filed_at is None:
                result.rows_skipped_bad_data += 1
                continue

            period_end = _parse_iso_date(trans.get("TRANS_DATE")) or _parse_iso_date(sub.get("PERIOD_OF_REPORT"))
            if period_end is None:
                result.rows_skipped_bad_data += 1
                continue

            shares = _parse_decimal(trans.get("SHRS_OWND_FOLWNG_TRANS"))

            trans_sk = (trans.get("NONDERIV_TRANS_SK") or trans.get("NON_DERIV_TRANS_SK") or "").strip() or "0"
            source_document_id = f"{accn}:NDT:{trans_sk}"

            _write_for_owners(
                conn,
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
            instrument_id = cik_to_instrument.get(issuer_cik) if issuer_cik else None
            if instrument_id is None:
                result.rows_skipped_unresolved_cik += 1
                continue

            form = sub.get("DOCUMENT_TYPE") or sub.get("FORM_TYPE") or sub.get("FORM") or ""
            source = _map_form_to_source(form)

            filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))
            if filed_at is None:
                result.rows_skipped_bad_data += 1
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

            _write_for_owners(
                conn,
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
    return result


def _write_for_owners(
    conn: psycopg.Connection[Any],
    *,
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
    """Write one observation per reporting owner on this row.

    Multi-owner filings (joint Form 4) produce N observation rows;
    the schema PK includes ``holder_identity_key`` so distinct owners
    coexist within the same accession+nature+period+document_id.
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

        try:
            record_insider_observation(
                conn,
                instrument_id=instrument_id,
                holder_cik=holder_cik,
                holder_name=holder_name,
                ownership_nature=ownership_nature,
                source=source,
                source_document_id=source_document_id,
                source_accession=accn,
                source_field=source_field,
                source_url=source_url,
                filed_at=filed_at,
                period_start=None,
                period_end=period_end,
                ingest_run_id=ingest_run_id,
                shares=shares,
            )
            result.rows_written += 1
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "insider ingest: record failed for %s/%s: %s",
                accn,
                holder_name,
                exc,
            )
            result.parse_errors += 1
