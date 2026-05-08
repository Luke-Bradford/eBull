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
from typing import Any
from uuid import UUID, uuid4

import psycopg

from app.services.ownership_observations import (
    record_fund_observation,
    upsert_sec_fund_series,
)

logger = logging.getLogger(__name__)


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
    parse_errors: int = 0
    touched_instrument_ids: set[int] = field(default_factory=set)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _parse_filing_date(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    try:
        return datetime.fromisoformat(text).replace(tzinfo=UTC)
    except ValueError:
        try:
            return datetime.fromisoformat(text[:10]).replace(tzinfo=UTC)
        except ValueError:
            return None


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None or not value.strip():
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as _exc:
        del _exc
        return None


def _load_cusip_map(conn: psycopg.Connection[Any]) -> dict[str, int]:
    """Preload the SEC CUSIP → instrument_id map (perf, Codex sweep BLOCKING).

    See ``sec_13f_dataset_ingest._load_cusip_map`` for rationale —
    same pattern duplicated here to keep ingester modules independent.
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
            out.setdefault(str(cusip).strip().upper(), int(instrument_id))
    return out


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


def _iter_tsv(zf: zipfile.ZipFile, *candidate_names: str):
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

    Returns telemetry suitable for stage reporting.
    """
    if ingest_run_id is None:
        ingest_run_id = uuid4()
    result = NPortIngestResult()
    cusip_map = _load_cusip_map(conn)

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

        # Track the (series_id, series_name, filer_cik, period_end)
        # triples we have already upserted into sec_fund_series so
        # we don't re-issue the upsert per holding.
        seen_series: set[str] = set()

        for holding in _iter_tsv(zf, "FUND_REPORTED_HOLDING.tsv"):
            result.holdings_seen += 1
            accn = holding.get("ACCESSION_NUMBER", "").strip()
            if not accn:
                result.rows_skipped_orphan_accession += 1
                continue
            sub = sub_by_accn.get(accn)
            reg = reg_by_accn.get(accn)
            fund = fund_by_accn.get(accn)
            if sub is None or reg is None or fund is None:
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
            # UNIT='NS' (number of shares) guard. A Long EC convertible-bond
            # holding can report BALANCE in 'PA' (principal amount); passing
            # that as ``shares`` would silently land non-share balances in
            # the ownership pie. Existing XML ingester applies the same
            # guard at app/services/n_port_ingest.py:886. Codex pre-push
            # round 1, finding 2.
            unit = (holding.get("UNIT") or "").strip().upper()
            if unit != "NS":
                result.rows_skipped_non_share_units += 1
                continue
            balance = _parse_decimal(holding.get("BALANCE"))
            if balance is None or balance <= 0:
                result.rows_skipped_non_positive_shares += 1
                continue

            cusip = (holding.get("ISSUER_CUSIP") or "").strip().upper()
            if not cusip:
                result.rows_skipped_unresolved_cusip += 1
                continue
            instrument_id = cusip_map.get(cusip)
            if instrument_id is None:
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
            period_end = _parse_iso_date(sub.get("REPORT_DATE")) or _parse_iso_date(sub.get("REPORT_ENDING_PERIOD"))
            if filed_at is None or period_end is None:
                result.rows_skipped_bad_data += 1
                continue

            holding_id = (holding.get("HOLDING_ID") or "0").strip() or "0"
            source_document_id = f"{accn}:{holding_id}"
            accession_no_dashes = accn.replace("-", "")
            source_url = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession_no_dashes}/"

            # Upsert series reference once per (accession, series_id).
            series_key = f"{accn}:{series_id}"
            if series_key not in seen_series:
                # Per-series savepoint: a CHECK violation on the
                # series-id regex would otherwise abort the
                # transaction. Wrap so we can record the failure and
                # continue cleanly.
                try:
                    with conn.transaction():
                        upsert_sec_fund_series(
                            conn,
                            fund_series_id=series_id,
                            fund_series_name=series_name or f"Series {series_id}",
                            fund_filer_cik=filer_cik,
                            last_seen_period_end=period_end,
                        )
                except Exception as exc:  # noqa: BLE001
                    # PR review WARNING (#1033): mark the series as
                    # SEEN even on failure so subsequent holdings
                    # under the same accession+series do not retry
                    # the failing upsert (each retry was incrementing
                    # parse_errors and silently discarding the
                    # holding). Log at WARNING so the first occurrence
                    # surfaces in production logs.
                    logger.warning(
                        "nport ingest: upsert_sec_fund_series failed for accn=%s series=%s: %s",
                        accn,
                        series_id,
                        exc,
                    )
                    seen_series.add(series_key)
                    result.parse_errors += 1
                    continue
                seen_series.add(series_key)

            # ─── currency_value (market_value_usd) ─────────────
            # CURRENCY_VALUE is the value column; the dataset stores
            # it in local currency and CURRENCY_CODE indicates which.
            # Treat USD-denominated rows as canonical USD, and leave
            # the column NULL for foreign rows rather than apply an
            # ad-hoc fx conversion that the schema doesn't ask for.
            currency_code = (holding.get("CURRENCY_CODE") or "").strip().upper()
            if currency_code == "USD":
                market_value_usd = _parse_decimal(holding.get("CURRENCY_VALUE"))
            else:
                market_value_usd = None

            # Per-row savepoint: a CHECK violation on one malformed
            # holding row would otherwise put psycopg into
            # ``InFailedSqlTransaction`` for every subsequent
            # ``record_fund_observation`` call. Wrapping each write
            # in ``conn.transaction()`` rolls back the bad row cleanly
            # so the loop keeps processing. Codex pre-push BLOCKING
            # for #1020.
            try:
                with conn.transaction():
                    record_fund_observation(
                        conn,
                        instrument_id=instrument_id,
                        fund_series_id=series_id,
                        fund_series_name=series_name or f"Series {series_id}",
                        fund_filer_cik=filer_cik,
                        source_document_id=source_document_id,
                        source_accession=accn,
                        source_field=holding_id,
                        source_url=source_url,
                        filed_at=filed_at,
                        period_start=None,
                        period_end=period_end,
                        ingest_run_id=ingest_run_id,
                        shares=balance,
                        market_value_usd=market_value_usd,
                        payoff_profile=payoff,
                        asset_category=asset_cat,
                    )
                result.rows_written += 1
                result.touched_instrument_ids.add(instrument_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "nport ingest: record_fund_observation failed for %s/%s: %s",
                    accn,
                    cusip,
                    exc,
                )
                result.parse_errors += 1

    return result


# Alias to match the spec stage name ``sec_nport_ingest_from_dataset``
# so orchestrator/admin code can import either form. Codex pre-push
# round 1, finding 1.
sec_nport_ingest_from_dataset = ingest_nport_dataset_archive
