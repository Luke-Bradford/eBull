"""C3 — bulk Form 13F Data Sets ingester (#1023).

Reads cached Form 13F Data Sets ZIPs (downloaded by Phase A3, #1021)
and writes ``ownership_institutions_observations`` rows for every
holding whose CUSIP resolves to a universe instrument.

Each ZIP contains TSVs:

  - ``SUBMISSION.tsv`` — one row per filing
    (CIK, accession, filing date, period of report).
  - ``COVERPAGE.tsv`` — one row per filing
    (filer name, total holdings value, etc).
  - ``INFOTABLE.tsv`` — one row per holding
    (CUSIP, value, shares, type, voting authority, PUT/CALL).

Replaces S13 (`sec_13f_quarterly_sweep`) entirely on a fresh install.
The bulk archive covers 100% of 13F filers — no top-N cohort cuts.

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import psycopg

from app.services.ownership_observations import record_institution_observation

logger = logging.getLogger(__name__)


@dataclass
class Form13FIngestResult:
    """Per-archive ingest outcome."""

    submissions_seen: int = 0
    coverpage_seen: int = 0
    infotable_seen: int = 0
    rows_written: int = 0
    rows_skipped_unresolved_cusip: int = 0
    rows_skipped_orphan_accession: int = 0
    rows_skipped_bad_data: int = 0
    parse_errors: int = 0
    touched_instrument_ids: set[int] = field(default_factory=set)


def _load_cusip_map(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Preload all SEC CUSIP → instrument_id mappings into a dict.

    13F + N-PORT INFOTABLE rows can number in the millions per
    archive; doing one indexed DB query per row is the dominant
    cost of the bulk ingest. Loading the entire map once at the
    top of ingest_*_dataset_archive collapses millions of round
    trips into one SELECT. CUSIP universe is bounded (~13k SEC
    Form 13F securities list rows + ~1500 universe instruments),
    so the dict fits comfortably in memory.

    Codex sweep BLOCKING for #1020.
    """
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value, instrument_id
            FROM external_identifiers
            WHERE provider = 'sec' AND identifier_type = 'cusip'
            ORDER BY is_primary DESC, external_identifier_id ASC
            """,
        )
        for row in cur.fetchall():
            cusip, instrument_id = row
            key = str(cusip).strip().upper()
            # First (highest priority) wins per (CUSIP) — match the
            # `ORDER BY is_primary DESC` shape of the per-row query
            # this replaces.
            out.setdefault(key, int(instrument_id))
    return out


def _parse_filing_date(value: str | None) -> datetime | None:
    """Parse a filing-date that may be ISO or SEC's ``DD-MMM-YYYY``.

    Real-world 13F dataset (verified 2026-05-08 against
    form13f_01dec2025-28feb2026.zip) emits ``FILING_DATE`` as
    ``31-DEC-2025``, NOT ISO. Without the fallback every 13F holding
    is rejected as bad_data — verified with a probe ingest that
    produced 0 rows_written.
    """
    if not value:
        return None
    text = value.strip()
    try:
        return datetime.fromisoformat(text).replace(tzinfo=UTC)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text[:10]).replace(tzinfo=UTC)
    except ValueError:
        pass
    for fmt in ("%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    titled = text.title()
    for fmt in ("%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(titled, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _parse_period_end(value: str | None) -> date | None:
    if not value:
        return None
    # Dataset uses ``DD-MMM-YYYY`` for some columns and ISO for others;
    # try ISO first then fall back.
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        for fmt in ("%d-%b-%Y", "%d-%b-%y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None or not value.strip():
        return None
    try:
        return Decimal(str(value).strip())
    # Bind ``as _`` so ruff format on Python 3.14 keeps the tuple
    # parens (PEP 758 unparenthesised except handlers strip them
    # otherwise — Codex / older parsers reject the bare form).
    except (InvalidOperation, ValueError) as _exc:
        del _exc
        return None


def _map_putcall(raw: str | None) -> Literal["EQUITY", "PUT", "CALL"]:
    """Map dataset's PUTCALL column to the schema's exposure_kind enum."""
    if not raw:
        return "EQUITY"
    upper = raw.strip().upper()
    if upper == "PUT":
        return "PUT"
    if upper == "CALL":
        return "CALL"
    return "EQUITY"


def _map_voting_authority(row: dict[str, str]) -> str | None:
    """Pick the highest-priority non-zero voting authority column.

    Dataset publishes three parallel columns (SOLE / SHARED / NONE);
    the row's "primary" voting flavour is the first that's non-zero.

    Column-name resilience: the SEC dataset has historically used
    both ``VOTING_AUTH_<KIND>`` and ``VOTING_AUTHORITY_<KIND>`` in
    different publication runs; this helper accepts either spelling.
    """

    def _read(*candidates: str) -> Decimal | None:
        for key in candidates:
            value = _parse_decimal(row.get(key))
            if value is not None:
                return value
        return None

    sole = _read("VOTING_AUTH_SOLE", "VOTING_AUTHORITY_SOLE")
    shared = _read("VOTING_AUTH_SHARED", "VOTING_AUTHORITY_SHARED")
    none_v = _read("VOTING_AUTH_NONE", "VOTING_AUTHORITY_NONE")
    if sole and sole > 0:
        return "SOLE"
    if shared and shared > 0:
        return "SHARED"
    if none_v and none_v > 0:
        return "NONE"
    return None


def _open_tsv(zf: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    """Read a TSV from the archive into a list of dicts.

    The 13F datasets are typically <100 MB so loading SUBMISSION.tsv
    + COVERPAGE.tsv into memory is acceptable. INFOTABLE can be 30M+
    rows so the caller iterates that one streamingly.
    """
    if name not in zf.namelist():
        # Some archives bundle CSVs at top-level and others nest under
        # a directory. Try to fall back.
        candidates = [n for n in zf.namelist() if n.endswith("/" + name) or n == name]
        if not candidates:
            return []
        name = candidates[0]
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        return list(csv.DictReader(text, delimiter="\t"))


def _iter_tsv(zf: zipfile.ZipFile, name: str):
    """Yield rows of a TSV one at a time (used for INFOTABLE)."""
    if name not in zf.namelist():
        candidates = [n for n in zf.namelist() if n.endswith("/" + name) or n == name]
        if not candidates:
            return
        name = candidates[0]
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        yield from csv.DictReader(text, delimiter="\t")


def ingest_13f_dataset_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    ingest_run_id: UUID | None = None,
) -> Form13FIngestResult:
    """Walk one Form 13F Data Set ZIP and append observations.

    The 13F dataset's three TSVs join on ``ACCESSION_NUMBER``. The
    primary loop iterates ``INFOTABLE.tsv`` (one row per holding) and
    looks up the matching SUBMISSION + COVERPAGE row by accession.

    Returns telemetry suitable for stage reporting. Per-row failures
    (unresolved CUSIP, bad period_end, etc) are counted on the result
    rather than raised.
    """
    if ingest_run_id is None:
        ingest_run_id = uuid4()

    result = Form13FIngestResult()
    # Preload CUSIP → instrument map once. Per-row DB lookup would
    # otherwise dominate cost on multi-million-row INFOTABLE.tsv
    # (Codex sweep BLOCKING).
    cusip_map = _load_cusip_map(conn)

    with zipfile.ZipFile(archive_path) as zf:
        submissions = _open_tsv(zf, "SUBMISSION.tsv")
        coverpages = _open_tsv(zf, "COVERPAGE.tsv")
        result.submissions_seen = len(submissions)
        result.coverpage_seen = len(coverpages)

        sub_by_accession = {row["ACCESSION_NUMBER"]: row for row in submissions if "ACCESSION_NUMBER" in row}
        cover_by_accession = {row["ACCESSION_NUMBER"]: row for row in coverpages if "ACCESSION_NUMBER" in row}

        for row in _iter_tsv(zf, "INFOTABLE.tsv"):
            result.infotable_seen += 1
            accession = row.get("ACCESSION_NUMBER", "").strip()
            if not accession:
                result.rows_skipped_orphan_accession += 1
                continue
            sub = sub_by_accession.get(accession)
            cover = cover_by_accession.get(accession)
            if sub is None or cover is None:
                result.rows_skipped_orphan_accession += 1
                continue

            cusip = (row.get("CUSIP") or "").strip().upper()
            if not cusip:
                result.rows_skipped_bad_data += 1
                continue

            instrument_id = cusip_map.get(cusip)
            if instrument_id is None:
                result.rows_skipped_unresolved_cusip += 1
                continue

            filer_cik = str(sub.get("CIK") or "").strip()
            if not filer_cik:
                result.rows_skipped_bad_data += 1
                continue
            filer_cik = filer_cik.zfill(10)
            filer_name = (cover.get("FILINGMANAGER_NAME") or "").strip()
            if not filer_name:
                # Schema requires NOT NULL filer_name; fall back to
                # the CIK to keep the row instead of dropping it.
                filer_name = f"CIK{filer_cik}"

            filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))
            period_end = _parse_period_end(cover.get("REPORTCALENDARORQUARTER"))
            if filed_at is None or period_end is None:
                result.rows_skipped_bad_data += 1
                continue

            # SSHPRNAMT carries shares OR principal-amount depending on
            # SSHPRNAMTTYPE (SH | PRN). PRN rows hold bond principal in
            # dollars, NOT shares — must skip to avoid silent
            # corruption (PR #1054 finding: 20k PRN rows in 2026Q1).
            shprn_type = (row.get("SSHPRNAMTTYPE") or "").strip().upper()
            if shprn_type and shprn_type != "SH":
                result.rows_skipped_bad_data += 1
                continue
            shares = _parse_decimal(row.get("SSHPRNAMT"))
            # VALUE column unit changed 2023-01-03 — pre-cutover it
            # was reported in $thousands, post-cutover in $dollars
            # (SEC metadata FORM13F_metadata.json:
            # "Starting on January 3, 2023, market value is reported
            # rounded to the nearest dollar.  Previously, market value
            # was reported in thousands."). Discriminate on FILED_AT
            # (when the filer reported), NOT period_end — a 2022Q4
            # restatement filed in March 2023 reports in dollars even
            # though period_end is pre-cutover. Codex pre-push MEDIUM
            # for #1054.
            _VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)
            value_raw = _parse_decimal(row.get("VALUE"))
            if value_raw is None:
                market_value_usd = None
            elif filed_at.date() >= _VALUE_DOLLARS_CUTOVER:
                market_value_usd = value_raw
            else:
                market_value_usd = value_raw * Decimal("1000")

            voting_authority = _map_voting_authority(row)
            exposure_kind = _map_putcall(row.get("PUTCALL"))

            accession_no_dashes = accession.replace("-", "")
            source_url = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession_no_dashes}/"
            # Per-row savepoint: a CHECK-constraint violation on one
            # malformed dataset row would otherwise put psycopg into
            # ``InFailedSqlTransaction`` for every subsequent
            # ``_resolve_cusip`` / ``record_institution_observation``
            # call. Wrapping each write in ``conn.transaction()``
            # rolls back the bad row cleanly so the loop can keep
            # processing. Codex review BLOCKING for PR #1031.
            try:
                with conn.transaction():
                    record_institution_observation(
                        conn,
                        instrument_id=instrument_id,
                        filer_cik=filer_cik,
                        filer_name=filer_name,
                        # Spec maps 13F filers to ``filer_type='INV'``
                        # (investment manager) by default. The schema
                        # CHECK accepts ETF/INV/INS/BD/OTHER. INV is
                        # the right default for typical 13F-HR filers.
                        filer_type="INV",
                        ownership_nature="economic",
                        source="13f",
                        source_document_id=accession,
                        source_accession=accession,
                        source_field=None,
                        source_url=source_url,
                        filed_at=filed_at,
                        period_start=None,
                        period_end=period_end,
                        ingest_run_id=ingest_run_id,
                        shares=shares,
                        market_value_usd=market_value_usd,
                        voting_authority=voting_authority,
                        exposure_kind=exposure_kind,
                    )
                result.rows_written += 1
                result.touched_instrument_ids.add(instrument_id)
            except Exception as exc:  # noqa: BLE001
                # Record-level write failure rolled back via the
                # savepoint; loop continues with a clean transaction.
                logger.debug(
                    "13F ingest: record_institution_observation failed for %s/%s: %s",
                    accession,
                    cusip,
                    exc,
                )
                result.parse_errors += 1
    return result
