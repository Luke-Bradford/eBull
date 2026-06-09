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
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import psycopg

from app.services.cusip_resolver import (
    delete_resolved_bulk_markers,
    flush_unresolved_cusips_bulk,
    in_window_bulk_markers_exist,
    load_bulk_cusip_map,
    reconcile_survived_markers,
)
from app.services.institutional_holdings import thirteen_f_retention_cutoff
from app.services.ownership_observations import period_end_within_bounds

logger = logging.getLogger(__name__)


# SEC FORM13F_metadata.json column description: "Starting on
# January 3, 2023, market value is reported rounded to the nearest
# dollar.  Previously, market value was reported in thousands."
# Hoisted to module level so the cutover constant isn't re-built
# on every INFOTABLE row. Codex pre-push NITPICK for #1054.
_VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)


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
    resolved_markers_deleted: int = 0  # #1399 — inline-deleted bulk markers
    figi_identifiers_seen: int = 0  # #1302 — distinct FIGIs collected this archive
    figi_identifiers_written: int = 0  # #1302 — newly inserted external_identifiers rows
    touched_instrument_ids: set[int] = field(default_factory=set)


# #1302 — the 13F INFOTABLE gained a ``FIGI`` column on 2023-01-03 (NOT
# ``LEI`` — empirically verified against the published dataset header; there
# is no LEI anywhere in the 13F structured data). FIGI is the 12-char
# OpenFIGI/Bloomberg global security identifier; ISO/BBG form is uppercase
# alphanumeric exactly 12 long. Reject anything else (empty / malformed) so
# only clean values reach external_identifiers.
_FIGI_RE = re.compile(r"^[A-Z0-9]{12}$")


# Persist the distinct CUSIP-derived FIGI -> instrument mappings at INSTRUMENT
# grain (NOT per holding-row): FIGI identifies the security, so storing it
# per (filer, period) observation would denormalise the same value across
# millions of rows of an already-bloated partitioned table (#1219/#1349). The
# settled-decisions home for a provider-native security identifier is
# ``external_identifiers``. Bounded by distinct securities held in the archive
# (~thousands). ``DO NOTHING`` never clobbers a curated/prior mapping; a FIGI
# already bound to a DIFFERENT instrument is a data anomaly left for audit,
# not silently rebound. The ON CONFLICT predicate targets the non-CIK partial
# unique index ``uq_external_identifiers_provider_value_non_cik`` (#1102).
_FIGI_UPSERT_SQL = """
INSERT INTO external_identifiers (
    instrument_id, provider, identifier_type, identifier_value, is_primary
) VALUES (%(iid)s, 'sec', 'figi', %(figi)s, FALSE)
ON CONFLICT (provider, identifier_type, identifier_value)
    WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
DO NOTHING
"""


def _persist_figi_external_identifiers(
    conn: psycopg.Connection[Any],
    figi_to_instrument: dict[str, int],
    *,
    result: Form13FIngestResult,
) -> None:
    """Batch-upsert collected FIGI -> instrument mappings (#1302).

    Idempotent + clobber-safe (DO NOTHING). Runs in the caller's per-archive
    transaction so it commits/rolls back atomically with the holdings drain.
    """
    if not figi_to_instrument:
        return
    result.figi_identifiers_seen += len(figi_to_instrument)
    params = [{"iid": iid, "figi": figi} for figi, iid in figi_to_instrument.items()]
    with conn.cursor() as cur:
        cur.executemany(_FIGI_UPSERT_SQL, params)
        if cur.rowcount and cur.rowcount > 0:
            result.figi_identifiers_written += cur.rowcount


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
    """Drain accumulated unresolved CUSIPs via the PR-1295 COPY helper.

    Pre-#1295: per-row INSERT + SAVEPOINT loop (~1k rows/s, dominated
    Phase C wall-clock when the unresolved set hit 2M+).
    Post-#1295: one COPY + INSERT...SELECT...ON CONFLICT pass via
    :func:`cusip_resolver.flush_unresolved_cusips_bulk`. Same
    idempotency on the bulk partial UNIQUE INDEX.

    Failure isolation: the helper is wrapped in ONE savepoint
    (``with conn.transaction():``) so a CHECK / FK / OOM raise
    inside it rolls back to the savepoint without poisoning the
    outer archive tx. This preserves the pre-#1295 contract that
    "the unresolved table is a hint for the PR-1b OpenFIGI sweep,
    not a source of truth — a flush failure must not abort the
    archive's observation writes". One savepoint per flush is the
    cheapest way to keep that invariant under the new single-call
    shape.

    Lint note: the savepoint lives OUTSIDE the main observations
    ``cur.copy()`` block (the flush runs post-stream), so the
    bulk-ingest lint guard at
    scripts/check_bulk_ingest_copy_pattern.sh invariant C.1 is
    satisfied (the awk walker only scans inside the COPY-cursor
    body).
    """
    if not buffer:
        return
    try:
        with conn.transaction():
            flush_unresolved_cusips_bulk(conn, buffer, source=source)
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "13F ingest: flush_unresolved_cusips_bulk failed (chunk=%d, source=%s): %s",
            len(buffer),
            source,
            exc,
        )
        result.parse_errors += 1


def _delete_resolved_markers(
    conn: psycopg.Connection[Any],
    markers: set[tuple[str, str, date]],
    *,
    result: Form13FIngestResult,
) -> None:
    """Delete bulk markers a prior run recorded for CUSIPs resolved this run.

    Mirror of :func:`_flush_unresolved_buffer`'s failure isolation: one
    savepoint (``with conn.transaction():``) so a raise inside the
    delete rolls back to the savepoint without poisoning the outer
    archive tx. A delete failure is non-fatal — a redundant marker is
    harmless (retention purge / a later run reclaims it). Records the
    deleted count only on a clean commit (#1399).
    """
    if not markers:
        return
    try:
        with conn.transaction():
            deleted = delete_resolved_bulk_markers(conn, markers, source="bulk_13f_dataset")
        result.resolved_markers_deleted += deleted
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "13F ingest: delete_resolved_bulk_markers failed (markers=%d): %s",
            len(markers),
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
    cusip_map = load_bulk_cusip_map(conn)

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

    # #1399 — collect (cusip, filer, period) for CUSIPs that resolve
    # this run so their now-redundant bulk markers (written by an
    # earlier run when the CUSIP was unmapped) can be inline-deleted
    # post-stream. SET, not list: many holding rows share one tuple
    # (multiple exposure_kind / fund series) — dedup bounds memory to
    # distinct positions and kills duplicate-temp churn (Codex). Gated:
    # only collect when an in-window bulk marker actually exists, so
    # most steady-state runs add zero overhead.
    collect_resolved = in_window_bulk_markers_exist(conn, "bulk_13f_dataset", retention_cutoff)
    # (cusip, filer_cik, period_end, instrument_id). instrument_id is
    # carried so the post-drain reconcile can confirm the observation
    # actually survived the ``ON_ERROR ignore`` COPY into _stg_13f
    # before the marker is deleted (Codex ckpt-2 HIGH — a wire-skipped
    # row must not delete a marker for an obs that never landed).
    materialised_markers: set[tuple[str, str, date, int]] = set()

    # #1302 — distinct FIGI -> instrument mappings collected across the
    # archive; batch-upserted into external_identifiers after the drain.
    figi_to_instrument: dict[str, int] = {}

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
                # #1433 — reject a NULL or out-of-[1900,2100) period_end
                # before it reaches the partitioned table. Mirrors the
                # #1218 XBRL guard: a year-6016 / pre-1900 value would land
                # in the DEFAULT partition and silently skew every
                # period-bounded institutional rollup. A 13F-HR with no
                # parseable cover period has nothing to rewash either, so
                # this also keeps it out of the unresolved-CUSIP buffer.
                if not period_end_within_bounds(period_end):
                    result.rows_skipped_bad_data += 1
                    continue
                filed_at = _parse_filing_date(sub.get("FILING_DATE") or sub.get("DATE_FILED"))

                instrument_id = cusip_map.get(cusip)
                if instrument_id is None:
                    result.rows_skipped_unresolved_cusip += 1
                    # PR-1a — record (cusip, filer, period) so the
                    # PR-1b OpenFIGI sweep can rewash. period_end is
                    # guaranteed in-window (non-None) by the #1433 guard
                    # above, so the bulk-path index always has a period.
                    unresolved_buffer.append((cusip, filer_cik, period_end))
                    continue

                # #1302 — capture the security's FIGI (12-char OpenFIGI id,
                # new INFOTABLE column 2023-01-03) against the instrument it
                # resolved to. Collected before the share/retention gates
                # below: the FIGI<->instrument identity holds regardless of
                # whether THIS holding row is a valid current position.
                figi = (row.get("FIGI") or "").strip().upper()
                if figi and _FIGI_RE.match(figi):
                    figi_to_instrument.setdefault(figi, instrument_id)

                filer_name = (cover.get("FILINGMANAGER_NAME") or "").strip()
                if not filer_name:
                    # Schema requires NOT NULL filer_name; fall back to
                    # the CIK to keep the row instead of dropping it.
                    filer_name = f"CIK{filer_cik}"

                # period_end already validated in-window above (#1433); only
                # filed_at remains to null-check here.
                if filed_at is None:
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
                # #1433 — an SH-type 13F holding must carry a positive share
                # count. NULL / 0 / negative SSHPRNAMT is malformed (the
                # schema column is nullable, sql/114, so the guard lives
                # here at parse) and would otherwise be summed into the
                # institutional ownership rollup as a phantom position.
                if shares is None or shares <= 0:
                    result.rows_skipped_bad_data += 1
                    continue
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

                # #1399 — same (cusip, filer_cik, period_end) expressions
                # the unresolved buffer uses above (:604); grain matches
                # the bulk marker by construction. instrument_id carried
                # for the post-drain survival reconcile.
                if collect_resolved:
                    materialised_markers.add((cusip, filer_cik, period_end, instrument_id))

    # Flush accumulated unresolved CUSIPs after the COPY context
    # closes (a COPY context exclusively owns the cursor so we cannot
    # interleave normal statements; flushing post-stream is the
    # simplest correct shape). #1295: a single COPY pass handles
    # millions of triples — no per-chunk loop needed.
    if unresolved_buffer:
        _flush_unresolved_buffer(
            conn,
            buffer=unresolved_buffer,
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

    # #1302 — persist the archive's distinct FIGI -> instrument mappings.
    # Same per-archive transaction as the drain, so it commits/rolls back
    # atomically with the holdings.
    _persist_figi_external_identifiers(conn, figi_to_instrument, result=result)

    # #1399 — delete bulk markers a prior run recorded for CUSIPs that
    # resolved this run. Reconcile against _stg_13f FIRST: only the
    # rows that survived the ``ON_ERROR ignore`` COPY are present there,
    # so a wire-skipped holding cannot delete a marker for an obs that
    # never materialised (Codex ckpt-2 HIGH). Match on the obs grain
    # (instrument_id, filer_cik, period_end); delete on the marker grain
    # (cusip, filer_cik, period_end). Savepoint-isolated; same tx as the
    # drain so the delete and the obs writes commit/rollback together.
    if materialised_markers:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT instrument_id, filer_cik, period_end FROM _stg_13f")
            survived = {(r[0], r[1], r[2]) for r in cur.fetchall()}
        to_delete = reconcile_survived_markers(materialised_markers, survived)
        if to_delete:
            _delete_resolved_markers(conn, to_delete, result=result)
        materialised_markers.clear()
    return result
