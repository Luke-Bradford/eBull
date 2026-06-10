"""C5 — bulk Form N-PORT Data Sets ingester (#1025).

Reads cached Form N-PORT Data Sets ZIPs (downloaded by Phase A3,
#1021) and writes ``ownership_funds_observations`` rows for every
equity-common, Long, positive-share fund holding whose CUSIP
resolves to a universe instrument.

Each ZIP (``<YYYY>q<N>_nport.zip``) contains TSVs flattened from the
SEC NPORT-P XML submissions. The relevant tables are:

  - ``SUBMISSION.tsv`` — accession-level metadata (FILING_DATE,
    REPORT_DATE = period_end).
  - ``REGISTRANT.tsv`` — registrant-level metadata (CIK = filer CIK).
  - ``FUND_REPORTED_INFO.tsv`` — per-fund metadata
    (SERIES_ID = the ``S0000xxxxx`` SEC series identifier,
    SERIES_NAME = human-readable fund name).
  - ``FUND_REPORTED_HOLDING.tsv`` — per-holding rows
    (CUSIP, BALANCE = shares, PAYOFF_PROFILE, ASSET_CAT, HOLDING_ID).

Replaces S14 (`sec_n_port_ingest`) entirely on a fresh install. The
existing per-filing parser remains the daily-incremental path; the
bulk dataset is the first-install seed.

Schema reference: SEC nport_readme.pdf (data sets readme).
Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md

PR-3 (#1233 v3 §7) — per-archive COPY refactor (was per-row INSERT +
``with conn.transaction()`` savepoint, ~1500 rows/s ceiling). Mirrors
the 13F shape; series upserts happen ONCE per archive in a pre-pass
so the COPY hot loop is free of per-row savepoints.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, Literal
from uuid import UUID, uuid4

import psycopg

from app.services.cusip_resolver import (
    flush_unresolved_cusips_bulk,
    load_bulk_cusip_map,
)
from app.services.n_port_ingest import n_port_retention_cutoff
from app.services.ownership_observations import period_end_within_bounds, upsert_sec_fund_series

logger = logging.getLogger(__name__)


# Precision of ``ownership_funds_observations.shares`` (sql/123:84 —
# NUMERIC(24, 4) NOT NULL CHECK (shares > 0)). The pre-validation gate
# must quantise BALANCE to this scale BEFORE the > 0 predicate, because
# a fractional-share holding like 0.00005 passes ``> 0`` but truncates
# to 0.0000 on COPY into the staging table → trips the strict CHECK on
# the drain INSERT. Pre-PR-3 per-row INSERT masked this via per-row
# SAVEPOINT; the COPY-batched path cannot.
_BALANCE_QUANTUM: Final = Decimal("0.0001")


@dataclass
class NPortIngestResult:
    """Per-archive ingest outcome."""

    submissions_seen: int = 0
    holdings_seen: int = 0
    rows_written: int = 0
    rows_skipped_unresolved_cusip: int = 0
    rows_skipped_orphan_accession: int = 0
    rows_skipped_non_equity: int = 0
    rows_skipped_non_long: int = 0
    rows_skipped_non_share_units: int = 0
    rows_skipped_non_positive_shares: int = 0
    rows_skipped_missing_series: int = 0
    rows_skipped_bad_data: int = 0
    rows_skipped_retention: int = 0  # PR7 #1233 §4.6
    parse_errors: int = 0
    # #1340 — distinct accessions seeded into n_port_ingest_log so the per-CIK
    # HTTP sweep (S23) skips bulk-loaded filings.
    ingest_log_rows_seeded: int = 0
    touched_instrument_ids: set[int] = field(default_factory=set)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _parse_filing_date(value: str | None) -> datetime | None:
    """Parse a filing-date that may be ISO or SEC's ``DD-MMM-YYYY``.

    Real-world N-PORT dataset (verified 2026-05-08 against
    nport_2026q1.zip) emits ``FILING_DATE`` as ``25-FEB-2026``.
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


def _parse_iso_date(value: str | None) -> date | None:
    """Parse a date that may be ISO or SEC's ``DD-MMM-YYYY`` format.

    Real-world SEC N-PORT dataset (verified 2026-05-08 against
    nport_2026q1.zip) emits ``REPORT_DATE`` and ``REPORT_ENDING_PERIOD``
    as ``31-DEC-2025``, NOT ISO. Without the fallback every N-PORT
    holding gets skipped as bad_data — verified with a probe ingest
    that produced 0 rows_written.
    """
    if not value:
        return None
    text = value.strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    for fmt in ("%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    # `%b` is locale-aware; SEC uses uppercase MMM (DEC, JAN, …).
    # Many locales accept it but title-case as fallback.
    titled = text.title()
    for fmt in ("%d-%b-%Y", "%d-%b-%y"):
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


def _open_tsv(zf: zipfile.ZipFile, *candidate_names: str) -> list[dict[str, str]]:
    """Open the first matching TSV from a list of candidate filenames."""
    available = zf.namelist()
    for name in candidate_names:
        if name in available:
            with zf.open(name) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
                return list(csv.DictReader(text, delimiter="\t"))
    for name in candidate_names:
        nested = [n for n in available if n.endswith("/" + name)]
        if nested:
            with zf.open(nested[0]) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
                return list(csv.DictReader(text, delimiter="\t"))
    return []


def _iter_tsv(zf: zipfile.ZipFile, *candidate_names: str) -> Iterator[dict[str, str]]:
    """Stream rows from a TSV — used for FUND_REPORTED_HOLDING which
    can be very large (millions of holdings per quarter).
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


def _flush_unresolved_buffer(
    conn: psycopg.Connection[Any],
    buffer: list[tuple[str, str, date]],
    *,
    source: Literal["bulk_13f_dataset", "bulk_nport_dataset"],
    cutoff: date,
    result: NPortIngestResult,
) -> None:
    """Drain ``buffer`` into ``unresolved_13f_cusips`` via the PR-1295
    COPY helper :func:`cusip_resolver.flush_unresolved_cusips_bulk`.

    Pre-#1295: per-row INSERT + SAVEPOINT loop. Post-#1295: one COPY
    + aggregated INSERT...SELECT...ON CONFLICT pass onto the
    per-(cusip, source) bulk partial UNIQUE INDEX (sql/189, #1349).
    ``cutoff`` is the per-source retention floor passed through to the
    helper's writer-side gate (a no-op here — the N-PORT walk gates
    retention before buffering).

    Failure isolation: ONE savepoint (``with conn.transaction():``)
    wraps the helper call so a raise inside the helper rolls back
    to the savepoint and the outer archive tx survives. Preserves
    the pre-#1295 contract that the unresolved-CUSIP buffer is a
    sweep hint, not a source of truth — a flush failure must not
    abort the observation writes that already landed in
    ``_stg_nport``.

    Lint note: the savepoint sits AFTER the observations
    ``cur.copy()`` block has closed, so the bulk-ingest lint guard
    at scripts/check_bulk_ingest_copy_pattern.sh invariant C.1
    (awk walker scoped to the cur.copy(...) body) is satisfied.
    """
    if not buffer:
        return
    try:
        with conn.transaction():
            flush_unresolved_cusips_bulk(conn, buffer, source=source, cutoff=cutoff)
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "nport ingest: flush_unresolved_cusips_bulk failed (chunk=%d, source=%s): %s",
            len(buffer),
            source,
            exc,
        )
        result.parse_errors += 1


# ---------------------------------------------------------------------------
# PR-3 — per-archive staging table lifecycle
# ---------------------------------------------------------------------------


_STG_COPY_COLUMNS = (
    "instrument_id",
    "fund_series_id",
    "fund_series_name",
    "fund_filer_cik",
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
    "payoff_profile",
    "asset_category",
)


# CREATE TEMP TABLE shape mirrors ownership_funds_observations minus
# the partition / CHECK / PK. ON COMMIT DROP releases the staging
# table when the orchestrator commits the per-archive tx, matching
# the spec invariant.
_CREATE_STG_SQL = """
CREATE TEMP TABLE _stg_nport (
    instrument_id      BIGINT,
    fund_series_id     TEXT,
    fund_series_name   TEXT,
    fund_filer_cik     TEXT,
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
    payoff_profile     TEXT,
    asset_category     TEXT
) ON COMMIT DROP
"""


# Drain into observations table with ON CONFLICT shape matching the
# legacy ``record_fund_observation`` semantics 1:1. Conflict key +
# UPDATE SET clause copied verbatim from ``ownership_observations.py``.
# DISTINCT ON dedupes staging rows per conflict key BEFORE the
# INSERT — see ``sec_13f_dataset_ingest._INSERT_FROM_STG_SQL`` for
# the cardinality-violation rationale (ctid DESC preserves
# last-write-wins semantics).
_INSERT_FROM_STG_SQL = """
INSERT INTO ownership_funds_observations (
    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
    ownership_nature,
    source, source_document_id, source_accession, source_field, source_url,
    filed_at, period_start, period_end, ingest_run_id,
    shares, market_value_usd, payoff_profile, asset_category
)
SELECT DISTINCT ON (instrument_id, fund_series_id, period_end, source_document_id)
    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
    ownership_nature,
    source, source_document_id, source_accession, source_field, source_url,
    filed_at, period_start, period_end, ingest_run_id,
    shares, market_value_usd, payoff_profile, asset_category
FROM _stg_nport
ORDER BY
    instrument_id, fund_series_id, period_end, source_document_id,
    ctid DESC
ON CONFLICT (instrument_id, fund_series_id, period_end, source_document_id)
DO UPDATE SET
    fund_series_name = EXCLUDED.fund_series_name,
    fund_filer_cik = EXCLUDED.fund_filer_cik,
    source_accession = EXCLUDED.source_accession,
    source_field = EXCLUDED.source_field,
    source_url = EXCLUDED.source_url,
    filed_at = EXCLUDED.filed_at,
    period_start = EXCLUDED.period_start,
    shares = EXCLUDED.shares,
    market_value_usd = EXCLUDED.market_value_usd,
    payoff_profile = EXCLUDED.payoff_profile,
    asset_category = EXCLUDED.asset_category,
    ingest_run_id = EXCLUDED.ingest_run_id,
    ingested_at = clock_timestamp()
"""


# #1340 — seed ``n_port_ingest_log`` from the same staging table so the per-CIK
# HTTP sweep (``sec_n_port_ingest`` / S23) skips every accession the bulk path
# already loaded (its ``_existing_accessions_for_fund_filer`` reads this log
# regardless of status). Set-based, one statement, no per-row round-trips.
# Grouped by accession (the log PK); ``MAX(fund_series_id)`` picks one of the
# accession's series for the informational column — every staged value already
# satisfies the ``^S[0-9]{9}$`` CHECK (it passed the observations drain above).
# ``status='success'`` is honest for a SEEDED accession: every seeded
# accession had all its in-universe holdings resolved (recoverable-miss
# accessions are excluded via ``%(unresolved_accns)s``), so S23 re-fetching
# it would yield nothing new. ``COUNT(*)`` is the staged-row count for the
# accession (an informational figure; the DISTINCT-ON drain may collapse a
# handful of duplicate holding-ids, so it is an upper bound, not an exact
# landed count). Runs BEFORE commit while ``_stg_nport`` (ON COMMIT DROP)
# is alive, and BEFORE ``series_upsert_buffer.clear()``.
_SEED_NPORT_INGEST_LOG_SQL = """
INSERT INTO n_port_ingest_log (
    accession_number, filer_cik, fund_series_id, period_of_report,
    status, holdings_inserted, holdings_skipped, error
)
SELECT
    source_accession,
    MAX(fund_filer_cik),
    MAX(fund_series_id),
    MAX(period_end),
    'success',
    COUNT(*),
    0,
    NULL
FROM _stg_nport
WHERE source_accession IS NOT NULL
  AND source_accession <> ALL(%(unresolved_accns)s::text[])
GROUP BY source_accession
ON CONFLICT (accession_number) DO UPDATE SET
    filer_cik = EXCLUDED.filer_cik,
    fund_series_id = EXCLUDED.fund_series_id,
    period_of_report = EXCLUDED.period_of_report,
    status = EXCLUDED.status,
    holdings_inserted = EXCLUDED.holdings_inserted,
    holdings_skipped = EXCLUDED.holdings_skipped,
    error = EXCLUDED.error,
    fetched_at = NOW()
"""


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


def ingest_nport_dataset_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    ingest_run_id: UUID | None = None,
) -> NPortIngestResult:
    """Walk one Form N-PORT Data Set ZIP and append observations.

    The four relevant TSVs join on ``ACCESSION_NUMBER``. The primary
    loop iterates ``FUND_REPORTED_HOLDING.tsv`` (one row per fund
    holding) and looks up SUBMISSION + REGISTRANT + FUND_REPORTED_INFO
    rows by accession.

    Filters at the write boundary (matches the existing per-filing
    ingester semantics + the schema's CHECK constraints which are the
    second-line guard):

      - ``ASSET_CAT == 'EC'`` (equity-common only).
      - ``PAYOFF_PROFILE == 'Long'`` (no shorts in ownership pie).
      - ``BALANCE > 0`` (positive shares only).
      - Holdings missing a SERIES_ID at the FUND_REPORTED_INFO level
        are skipped — refusing to synthesise a fund-series identity
        per the existing per-filing parser's posture.

    PR-3 per-archive lifecycle:
      1. CREATE TEMP TABLE _stg_nport ON COMMIT DROP.
      2. Stream FUND_REPORTED_HOLDING through Python pre-validation;
         buffer qualifying ``(accn, series_id, name, filer, period_end)``
         tuples for the series upsert; write the COPY row into
         staging in the same pass.
      3. AFTER the COPY context closes, upsert every qualifying
         series under its own savepoint. Legacy semantic preserved:
         a series is only upserted if at least ONE holding under that
         filing passed every gate (asset_cat / payoff / unit /
         balance / CUSIP). ``seen_series`` is keyed by
         ``(accn, series_id)`` so a series re-encountered under a
         different accession STILL upserts (legacy semantic — feeds
         the helper's GREATEST monotonic advance on
         ``last_seen_period_end``).
      4. INSERT...SELECT...ON CONFLICT drains staging into target.
      5. Orchestrator commits → staging drops via ON COMMIT DROP.

    Returns telemetry suitable for stage reporting.
    """
    if ingest_run_id is None:
        ingest_run_id = uuid4()
    result = NPortIngestResult()
    cusip_map = load_bulk_cusip_map(conn)

    # #1233 PR-1a — buffer of (cusip, filer_cik, period_end) triples
    # for every unresolved-CUSIP holding. Mirrors the 13F path.
    unresolved_buffer: list[tuple[str, str, date]] = []

    # #1340 — accessions with at least one holding whose CUSIP is valid but
    # NOT YET in the cusip_map (buffered above for the S13 OpenFIGI sweep).
    # These are RECOVERABLE: a later resolution could ingest the held-back
    # holdings, so S23's per-CIK sweep must keep them re-fetchable. We
    # therefore EXCLUDE these accessions from the n_port_ingest_log seed
    # below — only fully-resolved accessions are marked done. (Empty-CUSIP
    # holdings are permanently unresolvable for everyone, so accessions that
    # only lose empty-CUSIP rows stay eligible to seed.)
    unresolved_accns: set[str] = set()

    # PR-3: CREATE TEMP TABLE before opening the COPY context.
    with conn.cursor() as cur:
        cur.execute(_CREATE_STG_SQL)

    with zipfile.ZipFile(archive_path) as zf:
        submissions = _open_tsv(zf, "SUBMISSION.tsv")
        registrants = _open_tsv(zf, "REGISTRANT.tsv")
        fund_info = _open_tsv(zf, "FUND_REPORTED_INFO.tsv")
        result.submissions_seen = len(submissions)

        sub_by_accn: dict[str, dict[str, str]] = {
            r["ACCESSION_NUMBER"]: r for r in submissions if "ACCESSION_NUMBER" in r
        }
        reg_by_accn: dict[str, dict[str, str]] = {
            r["ACCESSION_NUMBER"]: r for r in registrants if "ACCESSION_NUMBER" in r
        }
        fund_by_accn: dict[str, dict[str, str]] = {
            r["ACCESSION_NUMBER"]: r for r in fund_info if "ACCESSION_NUMBER" in r
        }

        # PR7 #1233 §4.6 — 8-quarter (24-month) retention cap. Cutoff
        # resolved ONCE per archive to avoid date-rollover during a
        # multi-million-row drain. Gate fires BEFORE any per-row work
        # (CUSIP map lookup, series upsert, observation write) so
        # pre-cap rows don't pay for downstream filters and the
        # ``rows_skipped_retention`` counter is unconfounded with
        # filter / unresolved-CUSIP buckets. Codex 1a WARN 3.
        retention_cutoff = n_port_retention_cutoff()

        # PR-3: series upsert deferred to post-COPY. Legacy semantic
        # preserved by buffering ``(accn, series_id, name, filer,
        # period_end)`` tuples for every accession+series that has
        # at least one fully-gated holding — the COPY context can't
        # share its cursor with non-COPY statements, so we collect
        # in-band and drain after the COPY block closes. ``seen_series``
        # is keyed by ``(accn, series_id)`` to mirror the legacy
        # ``seen_series`` set (Codex 2 HIGH on pre-pass `series_id`-only
        # dedup: same series under a NEW accession in the same archive
        # must still upsert so ``GREATEST(last_seen_period_end, …)``
        # advances).
        seen_series: set[tuple[str, str]] = set()
        series_upsert_buffer: list[tuple[str, str, str, str, date]] = []
        # Tracks series whose upsert FAILED so subsequent holdings
        # under the same series in the SAME archive don't keep
        # re-trying. Set is populated post-COPY when we drain
        # series_upsert_buffer; the per-holding write does NOT
        # short-circuit on this because the legacy path retried per
        # holding until the series was marked seen — there's no clean
        # "skip" channel during the COPY stream. The drain happens
        # AFTER the staging drain so a failed series doesn't roll back
        # the staged observations.
        series_failed: set[str] = set()

        # Main per-holding loop. Pre-validate then COPY into staging.
        copy_sql = (
            "COPY _stg_nport ("
            + ", ".join(_STG_COPY_COLUMNS)
            + ") FROM STDIN WITH (FORMAT text, ON_ERROR ignore, LOG_VERBOSITY verbose)"
        )
        copy_attempted = 0
        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for holding in _iter_tsv(zf, "FUND_REPORTED_HOLDING.tsv"):
                result.holdings_seen += 1
                accn = holding.get("ACCESSION_NUMBER", "").strip()
                if not accn:
                    result.rows_skipped_orphan_accession += 1
                    continue
                sub = sub_by_accn.get(accn)
                if sub is None:
                    result.rows_skipped_orphan_accession += 1
                    continue

                # PR7 §4.6 retention gate (EARLY) — same shape as
                # legacy.
                period_end_early = _parse_iso_date(sub.get("REPORT_DATE")) or _parse_iso_date(
                    sub.get("REPORT_ENDING_PERIOD")
                )
                # #1433 — reject a NULL or out-of-[1900,2100) period_end
                # BEFORE the retention gate and the unresolved-CUSIP buffer,
                # so a junk year-6016 with an unresolved CUSIP never leaks
                # into unresolved markers, and a pre-1900 date is counted as
                # bad_data rather than masked as retention (mirrors the #1218
                # XBRL guard; shared bound via period_end_within_bounds).
                # period_end_early == the staged period_end (same
                # REPORT_DATE / REPORT_ENDING_PERIOD expression below).
                if not period_end_within_bounds(period_end_early):
                    result.rows_skipped_bad_data += 1
                    continue
                if period_end_early < retention_cutoff:
                    result.rows_skipped_retention += 1
                    continue

                reg = reg_by_accn.get(accn)
                fund = fund_by_accn.get(accn)
                if reg is None or fund is None:
                    result.rows_skipped_orphan_accession += 1
                    continue

                # ─── filter at write boundary ───────────────────────
                asset_cat = (holding.get("ASSET_CAT") or "").strip()
                if asset_cat != "EC":
                    result.rows_skipped_non_equity += 1
                    continue
                payoff = (holding.get("PAYOFF_PROFILE") or "").strip()
                if payoff != "Long":
                    result.rows_skipped_non_long += 1
                    continue
                unit = (holding.get("UNIT") or "").strip().upper()
                if unit != "NS":
                    result.rows_skipped_non_share_units += 1
                    continue
                balance = _parse_decimal(holding.get("BALANCE"))
                if balance is None or balance <= 0:
                    result.rows_skipped_non_positive_shares += 1
                    continue
                # PR-3 regression guard: ``ownership_funds_observations.shares``
                # is NUMERIC(24, 4) with a strict ``CHECK (shares > 0)``
                # (sql/123:84). A fractional-share holding like 0.00005
                # passes the ``> 0`` predicate above but quantises to
                # 0.0000 on COPY into staging, then trips the CHECK on
                # the INSERT...SELECT drain — aborting the entire archive.
                # Pre-PR-3 per-row INSERT path masked this via per-row
                # SAVEPOINT (counted as bad_data, loop continued); the
                # COPY-batched path cannot. Quantise here at the same
                # precision the column will store, and reject the row
                # if it underflows to zero.
                # ROUND_HALF_EVEN matches Postgres NUMERIC coercion
                # semantics exactly — what PG would do on INSERT, we do
                # here so the pre-validation outcome lines up with the
                # CHECK constraint outcome. ``0.00005 → 0.0000`` (tie
                # to even, reject); ``0.00006 → 0.0001`` (accept);
                # ``0.00004 → 0.0000`` (reject). Codex 2 finding on
                # the fix branch — implicit rounding mode was a
                # forensic gap.
                balance_q = balance.quantize(_BALANCE_QUANTUM, rounding=ROUND_HALF_EVEN)
                if balance_q <= 0:
                    result.rows_skipped_non_positive_shares += 1
                    continue
                balance = balance_q

                cusip = (holding.get("ISSUER_CUSIP") or "").strip().upper()
                if not cusip:
                    result.rows_skipped_unresolved_cusip += 1
                    continue
                instrument_id = cusip_map.get(cusip)
                if instrument_id is None:
                    filer_cik_raw_buf = (reg.get("CIK") or "").strip()
                    # period_end_early guaranteed non-None by the #1433 guard.
                    if filer_cik_raw_buf:
                        filer_cik_buf = filer_cik_raw_buf.zfill(10)
                        unresolved_buffer.append((cusip, filer_cik_buf, period_end_early))
                    # #1340 — recoverable miss: hold this accession back from
                    # the n_port_ingest_log seed so S23 can re-fetch it after
                    # the CUSIP resolves.
                    unresolved_accns.add(accn)
                    result.rows_skipped_unresolved_cusip += 1
                    continue

                # ─── series identity ────────────────────────────────
                series_id = (fund.get("SERIES_ID") or "").strip()
                series_name = (fund.get("SERIES_NAME") or "").strip()
                if not series_id:
                    result.rows_skipped_missing_series += 1
                    continue

                filer_cik_raw = (reg.get("CIK") or "").strip()
                if not filer_cik_raw:
                    result.rows_skipped_bad_data += 1
                    continue
                filer_cik = filer_cik_raw.zfill(10)

                filed_at = _parse_filing_date(sub.get("FILING_DATE"))
                # period_end == period_end_early (same REPORT_DATE /
                # REPORT_ENDING_PERIOD), already validated in-window by the
                # #1433 guard above — reuse it (non-None) and only null-check
                # filed_at here.
                period_end = period_end_early
                if filed_at is None:
                    result.rows_skipped_bad_data += 1
                    continue

                # Series upsert is deferred to post-COPY. Buffer this
                # (accn, series_id) tuple iff this is the first
                # qualifying holding for the pair. Legacy semantic:
                # one upsert per (accn, series_id) — re-encountering
                # the same series under a different accession in the
                # same archive STILL upserts so ``GREATEST(...)``
                # advances ``last_seen_period_end``.
                series_key = (accn, series_id)
                if series_key not in seen_series:
                    seen_series.add(series_key)
                    series_upsert_buffer.append(
                        (
                            accn,
                            series_id,
                            series_name or f"Series {series_id}",
                            filer_cik,
                            period_end,
                        )
                    )

                holding_id = (holding.get("HOLDING_ID") or "0").strip() or "0"
                source_document_id = f"{accn}:{holding_id}"
                accession_no_dashes = accn.replace("-", "")
                source_url = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession_no_dashes}/"

                # CURRENCY_VALUE in local currency; only treat USD
                # as canonical USD per legacy posture.
                currency_code = (holding.get("CURRENCY_CODE") or "").strip().upper()
                if currency_code == "USD":
                    market_value_usd = _parse_decimal(holding.get("CURRENCY_VALUE"))
                else:
                    market_value_usd = None

                copy.write_row(
                    (
                        instrument_id,
                        series_id,
                        series_name or f"Series {series_id}",
                        filer_cik,
                        "economic",
                        "nport",
                        source_document_id,
                        accn,
                        holding_id,
                        source_url,
                        filed_at,
                        None,
                        period_end,
                        str(ingest_run_id),
                        balance,
                        market_value_usd,
                        payoff,
                        asset_cat,
                    )
                )
                copy_attempted += 1
                result.touched_instrument_ids.add(instrument_id)

    # Drain staging into the partitioned observations table FIRST,
    # before the series upsert + unresolved-CUSIP flushes. Rationale:
    # the staging drain is the observation write-through; a series
    # upsert failure (rare; would require a malformed series_id) must
    # NOT roll back the legitimate observation rows. Counter-legacy:
    # the per-row savepoint pattern lost holdings under failed
    # series upserts. Documented refinement; the observation row
    # carries ``fund_series_name`` inline so a missing reference-table
    # row does not break downstream rollups.
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM _stg_nport")
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

    # Post-COPY series upserts. Each upsert under its own savepoint
    # (legitimate use — this is OUTSIDE the cur.copy() block body,
    # so the bulk-ingest lint guard at
    # scripts/check_bulk_ingest_copy_pattern.sh accepts it). A
    # malformed series_id raises ValueError inside the helper; the
    # savepoint rolls it back cleanly and the loop continues.
    for accn, series_id, series_name, filer_cik, period_end in series_upsert_buffer:
        try:
            with conn.transaction():
                upsert_sec_fund_series(
                    conn,
                    fund_series_id=series_id,
                    fund_series_name=series_name,
                    fund_filer_cik=filer_cik,
                    last_seen_period_end=period_end,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "nport ingest: upsert_sec_fund_series failed for accn=%s series=%s: %s",
                accn,
                series_id,
                exc,
            )
            series_failed.add(series_id)
            result.parse_errors += 1

    # #1340 — seed n_port_ingest_log from staging so S23's per-CIK HTTP sweep
    # skips the accessions this bulk path FULLY loaded. Accessions with any
    # recoverable (valid-but-unmapped) CUSIP are held back so S23 can still
    # re-fetch them after resolution. Must run before commit (while _stg_nport
    # is alive) and before the buffer clear below.
    with conn.cursor() as cur:
        cur.execute(
            _SEED_NPORT_INGEST_LOG_SQL,
            {"unresolved_accns": sorted(unresolved_accns)},
        )
        result.ingest_log_rows_seeded = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    series_upsert_buffer.clear()

    # Flush accumulated unresolved CUSIPs AFTER the staging drain so
    # a flush failure cannot roll it back. #1295: single COPY pass
    # handles the whole buffer; no per-chunk loop needed.
    if unresolved_buffer:
        _flush_unresolved_buffer(
            conn,
            unresolved_buffer,
            source="bulk_nport_dataset",
            cutoff=retention_cutoff,
            result=result,
        )
        unresolved_buffer.clear()

    # #1349 — the #1399 inline marker-delete that used to follow here is
    # gone: with per-(cusip, source) marker grain, mapped-CUSIP hygiene
    # is owned by ``sweep_bulk_cusips_resolved_via_extid`` in the
    # cadenced ``cusip_resolver_post_bulk_sweep`` job.
    return result


# Alias to match the spec stage name ``sec_nport_ingest_from_dataset``
# so orchestrator/admin code can import either form. Codex pre-push
# round 1, finding 1.
sec_nport_ingest_from_dataset = ingest_nport_dataset_archive
