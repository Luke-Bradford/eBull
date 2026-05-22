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

PR-3 (#1233 v3 §7) — per-archive COPY refactor (was per-row INSERT +
``with conn.transaction()`` savepoint, ~1500 rows/s ceiling). New shape
per archive:

    CREATE TEMP TABLE _stg_13f (...) ON COMMIT DROP;
    -- Python pre-validates every row + accumulates buffer
    cursor-level COPY _stg_13f ... FROM STDIN
                       WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)
    INSERT INTO ownership_institutions_observations
        SELECT ... FROM _stg_13f
        ON CONFLICT (...) DO UPDATE SET ...
    -- orchestrator commits → TEMP drops via ON COMMIT DROP

Cancel observation cost: per-row checkpoint (ms) → per-archive
checkpoint (10-60s on multi-million-row archives). Documented in
``.claude/skills/data-engineer/SKILL.md`` §3.5 and the spec §7.
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

from app.services.cusip_resolver import record_unresolved_cusip_from_bulk
from app.services.institutional_holdings import thirteen_f_retention_cutoff

logger = logging.getLogger(__name__)


# SEC FORM13F_metadata.json column description: "Starting on
# January 3, 2023, market value is reported rounded to the nearest
# dollar.  Previously, market value was reported in thousands."
# Hoisted to module level so the cutover constant isn't re-built
# on every INFOTABLE row. Codex pre-push NITPICK for #1054.
_VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)


# Flush unresolved-CUSIP buffer every N rows to the PR-1a helper. The
# helper inserts with ON CONFLICT DO NOTHING into the bulk-path partial
# UNIQUE index on (cusip, filer_cik, period_end, source) so flushing
# in chunks bounds in-memory buffer growth without changing semantics.
_UNRESOLVED_FLUSH_BATCH = 1000


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
    rows_skipped_retention: int = 0  # PR6 #1233 §4.5
    parse_errors: int = 0
    touched_instrument_ids: set[int] = field(default_factory=set)


def _load_cusip_map(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Preload all CUSIP → instrument_id mappings (SEC + OpenFIGI) into a dict.

    13F + N-PORT INFOTABLE rows can number in the millions per
    archive; doing one indexed DB query per row is the dominant
    cost of the bulk ingest. Loading the entire map once at the
    top of ingest_*_dataset_archive collapses millions of round
    trips into one SELECT. CUSIP universe is bounded (~13k SEC
    Form 13F securities list rows + ~1500 universe instruments +
    OpenFIGI promotions), so the dict fits comfortably in memory.

    Codex sweep BLOCKING for #1020.

    Provider widening (#1233 PR-1b): the WHERE filter now reads
    ``provider IN ('sec', 'openfigi')`` so post-bulk-sweep OpenFIGI
    promotions become visible to the next bulk ingest pass without
    a schema-level UNION view. The SEC-curated mappings remain
    authoritative — ``ORDER BY is_primary DESC, external_identifier_id ASC``
    means a SEC ``is_primary=TRUE`` row wins over an OpenFIGI
    ``is_primary=FALSE`` row when both exist for the same CUSIP
    (the OpenFIGI sweep deliberately writes ``is_primary=FALSE``;
    see ``app/services/cusip_resolver.py::_promote_openfigi_mapping``).
    The CUSIP-uniqueness constraint
    (``uq_external_identifiers_provider_value`` on
    ``(provider, identifier_type, identifier_value)``) means at most
    one row per (provider, cusip), so the dedup happens at the row
    level — ``setdefault`` keeps the first-seen mapping per CUSIP
    after the ORDER BY.
    """
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_value, instrument_id
            FROM external_identifiers
            WHERE provider IN ('sec', 'openfigi') AND identifier_type = 'cusip'
            ORDER BY is_primary DESC, external_identifier_id ASC
            """,
        )
        for row in cur.fetchall():
            cusip, instrument_id = row
            key = str(cusip).strip().upper()
            # First (highest priority) wins per (CUSIP) — match the
            # `ORDER BY is_primary DESC` shape of the per-row query
            # this replaces. SEC primary > SEC non-primary > OpenFIGI
            # non-primary by construction (OpenFIGI sweep never writes
            # is_primary=TRUE).
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


def _iter_tsv(zf: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    """Yield rows of a TSV one at a time (used for INFOTABLE)."""
    if name not in zf.namelist():
        candidates = [n for n in zf.namelist() if n.endswith("/" + name) or n == name]
        if not candidates:
            return
        name = candidates[0]
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        yield from csv.DictReader(text, delimiter="\t")


# ---------------------------------------------------------------------------
# PR-3 — per-archive staging table lifecycle
# ---------------------------------------------------------------------------


# Column order MUST match the CREATE TEMP TABLE shape exactly. The
# INSERT...SELECT below uses positional projection so any drift here
# silently misaligns columns. scripts/check_bulk_ingest_copy_pattern.sh
# pins the COPY column list shape in the codebase.
_STG_COPY_COLUMNS = (
    "instrument_id",
    "filer_cik",
    "filer_name",
    "filer_type",
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
    "market_value_usd",
    "voting_authority",
    "exposure_kind",
)


# CREATE TEMP TABLE shape mirrors ownership_institutions_observations
# minus the partition/CHECK/PK (staging is unconstrained — DB-side
# CHECKs fire on the INSERT...SELECT into the target). Same column
# types so COPY parses identically. ON COMMIT DROP releases the
# staging table when the orchestrator commits the per-archive tx,
# matching the spec invariant "TEMP table dies with the per-archive
# transaction so the next iteration starts clean."
_CREATE_STG_SQL = """
CREATE TEMP TABLE _stg_13f (
    instrument_id      BIGINT,
    filer_cik          TEXT,
    filer_name         TEXT,
    filer_type         TEXT,
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
    shares             NUMERIC(24, 4),
    market_value_usd   NUMERIC(20, 2),
    voting_authority   TEXT,
    exposure_kind      TEXT
) ON COMMIT DROP
"""


# Drain into observations table with ON CONFLICT shape matching the
# legacy ``record_institution_observation`` semantics 1:1. Conflict
# key + UPDATE SET clause copied verbatim from
# ``ownership_observations.py:record_institution_observation``.
#
# DISTINCT ON dedupes the staging rows per conflict key BEFORE the
# INSERT — without this, two staging rows that share a conflict tuple
# (e.g. an archive containing an INFOTABLE row + its amendment under
# the same accession) trigger PG's ``cardinality_violation: ON
# CONFLICT DO UPDATE command cannot affect row a second time``. The
# legacy per-row INSERT path serialised the writes so the second one
# would have UPDATEd the first; the bulk path keeps that "last write
# wins" semantic by ordering DISTINCT ON by ``ctid DESC`` (the
# implicit physical row order — COPY appends so ctid grows
# monotonically, picking the LAST staged row per group).
_INSERT_FROM_STG_SQL = """
INSERT INTO ownership_institutions_observations (
    instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
    source, source_document_id, source_accession, source_field, source_url,
    filed_at, period_start, period_end, ingest_run_id,
    shares, market_value_usd, voting_authority, exposure_kind
)
SELECT DISTINCT ON (
    instrument_id, filer_cik, ownership_nature, period_end,
    source_document_id, exposure_kind
)
    instrument_id, filer_cik, filer_name, filer_type, ownership_nature,
    source, source_document_id, source_accession, source_field, source_url,
    filed_at, period_start, period_end, ingest_run_id,
    shares, market_value_usd, voting_authority, exposure_kind
FROM _stg_13f
ORDER BY
    instrument_id, filer_cik, ownership_nature, period_end,
    source_document_id, exposure_kind,
    ctid DESC
ON CONFLICT (
    instrument_id, filer_cik, ownership_nature, period_end,
    source_document_id, exposure_kind
)
DO UPDATE SET
    filer_name = EXCLUDED.filer_name,
    filer_type = EXCLUDED.filer_type,
    source_accession = EXCLUDED.source_accession,
    source_field = EXCLUDED.source_field,
    source_url = EXCLUDED.source_url,
    filed_at = EXCLUDED.filed_at,
    period_start = EXCLUDED.period_start,
    shares = EXCLUDED.shares,
    market_value_usd = EXCLUDED.market_value_usd,
    voting_authority = EXCLUDED.voting_authority,
    ingest_run_id = EXCLUDED.ingest_run_id,
    ingested_at = clock_timestamp()
"""


def _build_copy_row(
    *,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    filer_type: str,
    ownership_nature: str,
    source: str,
    source_document_id: str,
    source_accession: str | None,
    source_field: str | None,
    source_url: str | None,
    filed_at: datetime,
    period_start: date | None,
    period_end: date,
    ingest_run_id: UUID,
    shares: Decimal | None,
    market_value_usd: Decimal | None,
    voting_authority: str | None,
    exposure_kind: str,
) -> tuple[Any, ...]:
    """Return one staged row in the column order _STG_COPY_COLUMNS expects.

    The wrapper exists to keep the per-row tuple build at the
    ingester's INFOTABLE loop terse and the column order single-sourced
    against _STG_COPY_COLUMNS.
    """
    return (
        instrument_id,
        filer_cik,
        filer_name,
        filer_type,
        ownership_nature,
        source,
        source_document_id,
        source_accession,
        source_field,
        source_url,
        filed_at,
        period_start,
        period_end,
        str(ingest_run_id),
        shares,
        market_value_usd,
        voting_authority,
        exposure_kind,
    )


def _flush_unresolved_buffer(
    conn: psycopg.Connection[Any],
    *,
    buffer: list[tuple[str, str, date]],
    source: Literal["bulk_13f_dataset", "bulk_nport_dataset"],
    result: Form13FIngestResult,
) -> None:
    """Drain accumulated unresolved CUSIPs via the PR-1a helper.

    Each write under its own SAVEPOINT (``with conn.transaction()``)
    so a DB error (e.g. CHECK violation on the bulk-path schema)
    does NOT leave the enclosing per-archive tx in
    ``InFailedSqlTransaction`` and abort the staging drain. Failure
    to record an unresolved CUSIP is logged + counted as a
    ``parse_error`` but does NOT abort the archive — the unresolved
    table is a hint for the PR-1b OpenFIGI sweep, not a source of
    truth. Caller is responsible for clearing the buffer post-flush.

    Lint note: the savepoint is OUTSIDE the cur.copy() block body
    (flush runs post-COPY) so the bulk-ingest lint guard at
    scripts/check_bulk_ingest_copy_pattern.sh accepts it.
    """
    for cusip, filer_cik, period_end in buffer:
        try:
            with conn.transaction():
                record_unresolved_cusip_from_bulk(
                    conn,
                    cusip=cusip,
                    filer_cik=filer_cik,
                    period_end=period_end,
                    source=source,
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "13F ingest: record_unresolved_cusip_from_bulk failed for cusip=%s filer=%s period=%s: %s",
                cusip,
                filer_cik,
                period_end,
                exc,
            )
            result.parse_errors += 1


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

    PR-3: per-archive lifecycle —

      1. CREATE TEMP TABLE _stg_13f (...) ON COMMIT DROP.
      2. Python pre-validates every INFOTABLE row, mirroring the
         legacy per-row gates (CUSIP map, retention, PRN-vs-SH,
         value-cutover).
      3. Validated rows are COPY'd into ``_stg_13f`` via
         cursor-level COPY (`... ON_ERROR ignore, LOG_VERBOSITY verbose`) so
         residual schema-drift on a single row skips that row + logs a
         NOTICE rather than aborting the bulk write.
      4. Single INSERT...SELECT...ON CONFLICT drains staging into
         ``ownership_institutions_observations`` preserving the legacy
         UPSERT semantics (conflict key + UPDATE SET copied from
         ``record_institution_observation``).
      5. Caller (orchestrator) commits → ``_stg_13f`` drops via
         ON COMMIT DROP, leaving a clean transaction boundary for the
         next archive.

    Cancel observation cost: per-row INSERT used to checkpoint cancel
    at sub-second latency. COPY drains atomically per archive, so
    cancel observed only at archive boundary (10-60s on
    multi-million-row archives). Acceptable trade-off documented in
    spec §7 + skill §3.5.
    """
    if ingest_run_id is None:
        ingest_run_id = uuid4()

    result = Form13FIngestResult()
    # Preload CUSIP → instrument map once. Per-row DB lookup would
    # otherwise dominate cost on multi-million-row INFOTABLE.tsv
    # (Codex sweep BLOCKING).
    cusip_map = _load_cusip_map(conn)

    # PR6 #1233 §4.5 — archive-level retention cutoff. Resolve once
    # OUTSIDE the per-row INFOTABLE loop so a multi-million-row drain
    # doesn't re-evaluate ``date.today()`` per row (and so a sentinel
    # mid-drain ``today`` boundary roll doesn't admit some rows and
    # reject others within the same archive).
    retention_cutoff = thirteen_f_retention_cutoff()

    # PR-3: CREATE TEMP TABLE _stg_13f ON COMMIT DROP — MUST live
    # inside the per-archive tx (the orchestrator opens a fresh conn
    # per archive and commits after this function returns). Drops
    # automatically when the orchestrator commits.
    with conn.cursor() as cur:
        cur.execute(_CREATE_STG_SQL)

    unresolved_buffer: list[tuple[str, str, date]] = []

    with zipfile.ZipFile(archive_path) as zf:
        submissions = _open_tsv(zf, "SUBMISSION.tsv")
        coverpages = _open_tsv(zf, "COVERPAGE.tsv")
        result.submissions_seen = len(submissions)
        result.coverpage_seen = len(coverpages)

        sub_by_accession = {row["ACCESSION_NUMBER"]: row for row in submissions if "ACCESSION_NUMBER" in row}
        cover_by_accession = {row["ACCESSION_NUMBER"]: row for row in coverpages if "ACCESSION_NUMBER" in row}

        # Stream INFOTABLE → pre-validate → COPY into _stg_13f. Single
        # cursor.copy() context spans every validated row in the
        # archive so the wire-level COPY drains in one pass. PG17
        # ON_ERROR ignore + LOG_VERBOSITY verbose skip residual
        # schema-drift rows + emit NOTICEs.
        copy_sql = (
            "COPY _stg_13f ("
            + ", ".join(_STG_COPY_COLUMNS)
            + ") FROM STDIN WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)"
        )
        copy_attempted = 0
        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
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

                filer_cik_raw = str(sub.get("CIK") or "").strip()
                if not filer_cik_raw:
                    result.rows_skipped_bad_data += 1
                    continue
                filer_cik = filer_cik_raw.zfill(10)

                period_end = _parse_period_end(cover.get("REPORTCALENDARORQUARTER"))
                filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))

                instrument_id = cusip_map.get(cusip)
                if instrument_id is None:
                    result.rows_skipped_unresolved_cusip += 1
                    # PR-1a — record (cusip, filer, period) so the
                    # PR-1b OpenFIGI sweep can rewash. Skip if period
                    # parse failed (bulk-path index requires period).
                    if period_end is not None:
                        unresolved_buffer.append((cusip, filer_cik, period_end))
                    continue

                filer_name = (cover.get("FILINGMANAGER_NAME") or "").strip()
                if not filer_name:
                    # Schema requires NOT NULL filer_name; fall back to
                    # the CIK to keep the row instead of dropping it.
                    filer_name = f"CIK{filer_cik}"

                if filed_at is None or period_end is None:
                    result.rows_skipped_bad_data += 1
                    continue

                # PR6 #1233 §4.5 — per-row retention gate. Bulk dataset
                # archives can span 30+ years; the per-row check honours
                # the calendar-quarter cap regardless of the archive's
                # nominal coverage window.
                if period_end < retention_cutoff:
                    result.rows_skipped_retention += 1
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
                # was reported in $thousands, post-cutover in $dollars.
                # See _VALUE_DOLLARS_CUTOVER constant at module top.
                # Discriminate on FILED_AT (when the filer reported), NOT
                # period_end — a 2022Q4 restatement filed in March 2023
                # reports in dollars even though period_end is pre-cutover.
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

                copy.write_row(
                    _build_copy_row(
                        instrument_id=instrument_id,
                        filer_cik=filer_cik,
                        filer_name=filer_name,
                        # Spec maps 13F filers to ``filer_type='INV'``
                        # (investment manager) by default. The schema
                        # CHECK accepts ETF/INV/INS/BD/OTHER. INV is
                        # the right default for typical 13F-HR filers.
                        filer_type="INV",
                        # ``ownership_nature`` for 13F-HR: pass
                        # ``'economic'`` for the full reported
                        # position. Mapping pinned in
                        # ``record_institution_observation`` docstring.
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
                )
                copy_attempted += 1
                result.touched_instrument_ids.add(instrument_id)

    # Flush accumulated unresolved CUSIPs after the COPY context
    # closes (a COPY context exclusively owns the cursor so we cannot
    # interleave normal statements; flushing post-stream is the
    # simplest correct shape).
    if unresolved_buffer:
        # Chunk the flush so a 5M-row archive with a 100k-unresolved
        # tail doesn't block on one giant statement series. The PR-1a
        # helper issues one INSERT per call so this is just a control-
        # flow guard against unbounded buffer growth — semantically the
        # same as flushing the whole list at once.
        for start in range(0, len(unresolved_buffer), _UNRESOLVED_FLUSH_BATCH):
            chunk = unresolved_buffer[start : start + _UNRESOLVED_FLUSH_BATCH]
            _flush_unresolved_buffer(
                conn,
                buffer=chunk,
                source="bulk_13f_dataset",
                result=result,
            )
        unresolved_buffer.clear()

    # Drain staging into the partitioned observations table.
    #
    # ON_ERROR ignore at COPY time silently drops rows that fail
    # wire-level parse — those rows never reach _stg_13f. A row that
    # later violates a target-table CHECK constraint at INSERT raises;
    # the orchestrator records the archive as failed and rolls back
    # cleanly via the per-archive boundary.
    accepted_via_copy = 0
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM _stg_13f")
        row = cur.fetchone()
        accepted_via_copy = int(row[0]) if row else 0
        # Surface PG17 ON_ERROR-skipped count as rows_skipped_bad_data
        # so operator-visible telemetry stays consistent with the
        # legacy per-row path (where a bad row landed in
        # parse_errors). Bad-data accounting is the more honest bucket
        # because ON_ERROR ignore = pre-validated row hit a type-cast
        # issue at COPY time, not a schema CHECK violation.
        skipped_by_copy = copy_attempted - accepted_via_copy
        if skipped_by_copy > 0:
            result.rows_skipped_bad_data += skipped_by_copy
        cur.execute(_INSERT_FROM_STG_SQL)
        # cur.rowcount counts inserts + updates (ON CONFLICT DO UPDATE).
        # Both paths represent successful writes from the operator's
        # perspective, so attribute both to rows_written.
        if cur.rowcount >= 0:
            result.rows_written = cur.rowcount
        else:
            # Driver couldn't tag the result. Fall back to staged
            # count so the count is a lower bound rather than 0.
            result.rows_written = accepted_via_copy
    return result
