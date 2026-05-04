"""Sync legacy typed-table rows into the new observations + _current
shape (#840.E-prep).

The Phase 1 schema unification (#840) introduces immutable
``ownership_*_observations`` + materialised ``_current`` tables.
Sub-PRs A-D (#851 / #852 / #853 / #854 / #855) shipped the schema and
the helper functions but did not touch the legacy ingesters. This
module is the bridge: a periodic / on-demand sync that re-reads
recent legacy typed-table rows (insider_transactions,
insider_initial_holdings, blockholder_filings, institutional_holdings,
def14a_beneficial_holdings, financial_periods.treasury_shares) and
mirrors them into the new tables via the ``record_*_observation`` +
``refresh_*_current`` API.

Why a periodic sync rather than write-through wired into every
ingester:

- Five ingesters across five service modules + the fundamentals
  normaliser. Touching every call site is a wide blast radius for
  one PR.
- Idempotency is already guaranteed by ``record_*``'s ON CONFLICT
  DO UPDATE on the natural key, so re-running the sync is cheap.
- The lag (hours, not days) is acceptable for v1; live write-through
  can be retrofitted post-#840.E if the operator wants.

The sync is also what runs as a one-shot bootstrap to retro-populate
observations from the existing legacy data the day this lands.

Each ``sync_<category>(conn, *, since=None)`` function:
  1. SELECTs eligible legacy rows (optionally filtered by ``since``).
  2. Resolves identity to the new model (legacy ``filer_id`` → cik
     for institutions; primary filer cik for blockholders;
     normalised holder name for DEF 14A; etc.).
  3. Calls ``record_<category>_observation`` for each row.
  4. Refreshes ``_current`` for every touched ``instrument_id``.
  5. Returns counts + unresolved-orphan list.

Per Codex plan-review finding #2, orphan rows (filer_id with no
``institutional_filers`` parent) are skipped + logged loudly rather
than dropping silently.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows

from app.services.ownership_observations import (
    record_blockholder_observation,
    record_def14a_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
    refresh_blockholders_current,
    refresh_def14a_current,
    refresh_insiders_current,
    refresh_institutions_current,
    refresh_treasury_current,
    resolve_filer_cik_or_raise,
)

logger = logging.getLogger(__name__)


@dataclass
class SyncSummary:
    """Per-category sync rollup. ``orphans`` lists legacy rows whose
    parent identity could not be resolved (filer_id with no filer row,
    blank reporter_cik, etc.) — operator-facing audit signal."""

    rows_scanned: int = 0
    observations_recorded: int = 0
    instruments_refreshed: int = 0
    orphans: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _refresh_for_instruments(
    conn: psycopg.Connection[Any],
    *,
    instrument_ids: Iterable[int],
    refresh_fn: Any,
    summary: SyncSummary,
) -> int:
    """Refresh ``_current`` for every touched instrument.

    Codex review for #840.E-prep: refresh failures must surface in
    the operator-visible summary, not get logged-and-swallowed —
    otherwise sync_all reports success while ``_current`` stays stale
    for the failed instruments and the next rollup read returns
    inconsistent data. Append to ``summary.orphans`` so the run
    status reflects the failure."""
    n = 0
    for iid in sorted(set(instrument_ids)):
        try:
            refresh_fn(conn, instrument_id=iid)
            n += 1
        except Exception as exc:
            logger.exception("ownership_observations_sync: refresh failed for instrument_id=%d", iid)
            summary.orphans.append(f"refresh failed instrument_id={iid}: {exc}")
    return n


# ---------------------------------------------------------------------------
# Insiders (Form 4 + Form 3)
# ---------------------------------------------------------------------------


def sync_insiders(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit: int | None = None,
) -> SyncSummary:
    """Mirror ``insider_transactions`` + ``insider_initial_holdings``
    into ``ownership_insiders_observations`` and refresh ``_current``
    for every touched instrument.

    Form 4 transactions map to ``source='form4'``,
    ``ownership_nature='direct'`` (insider_transactions records direct
    holdings via ``post_transaction_shares``). Indirect via family
    trusts / control entities is captured separately on the form via
    the ``direct_indirect`` column on ``insider_initial_holdings``;
    Form 4's transaction-level indirect lives on the per-transaction
    ``direct_indirect`` field (when present) — for the v1 sync we
    treat any transaction without an explicit indirect tag as
    ``direct``."""
    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    # Form 4 — read insider_transactions joined to insider_filings for
    # filed_at + url. Group by accession + filer to capture latest
    # post_transaction_shares per holding.
    where = "WHERE it.post_transaction_shares IS NOT NULL AND it.is_derivative = FALSE"
    params: dict[str, Any] = {}
    if since is not None:
        where += " AND it.txn_date >= %(since)s"
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    # Codex review for #840.E-prep:
    # 1. ORDER BY adds ``txn_date DESC`` so the latest balance per
    #    holding wins, not just the XML-last row.
    # 2. DISTINCT ON keys on (accession, filer, direct_indirect) so
    #    direct + indirect splits for the same filer/accession produce
    #    SEPARATE observations (two-axis identity preserved).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (it.accession_number, it.filer_cik, it.filer_name, it.direct_indirect)
                it.instrument_id, it.accession_number,
                it.filer_cik, it.filer_name, it.direct_indirect,
                it.txn_date, it.post_transaction_shares,
                f.primary_document_url
            FROM insider_transactions it
            JOIN insider_filings f USING (accession_number)
            {where}
            ORDER BY it.accession_number, it.filer_cik, it.filer_name, it.direct_indirect,
                     it.txn_date DESC NULLS LAST, it.txn_row_num DESC
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        cik = row["filer_cik"]
        if cik is not None:
            cik = str(cik).strip() or None
        name = str(row["filer_name"] or "").strip()
        if not name and cik is None:
            summary.orphans.append(f"insider_transactions accession={row['accession_number']} (no cik or name)")
            continue
        nature = "indirect" if (row["direct_indirect"] == "I") else "direct"
        try:
            record_insider_observation(
                conn,
                instrument_id=int(row["instrument_id"]),
                holder_cik=cik,
                holder_name=name or (cik or "UNKNOWN"),
                ownership_nature=nature,  # type: ignore[arg-type]
                source="form4",
                source_document_id=str(row["accession_number"]),
                source_accession=str(row["accession_number"]),
                source_field=None,
                source_url=str(row["primary_document_url"]) if row["primary_document_url"] else None,
                filed_at=datetime.combine(row["txn_date"], datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=row["txn_date"],
                ingest_run_id=run_id,
                shares=Decimal(row["post_transaction_shares"]),
            )
            summary.observations_recorded += 1
            instruments_touched.add(int(row["instrument_id"]))
        except Exception as exc:
            summary.orphans.append(f"insider_transactions accession={row['accession_number']}: {exc}")

    # Form 3 — initial holdings baseline. Direct vs indirect tag lives
    # on insider_initial_holdings.direct_indirect ('D'/'I'). Map 'I'
    # to ``ownership_nature='indirect'``.
    where = "WHERE iih.shares IS NOT NULL AND iih.is_derivative = FALSE"
    if since is not None:
        where += " AND iih.as_of_date >= %(since)s"

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT iih.instrument_id, iih.accession_number,
                   iih.filer_cik, iih.filer_name,
                   iih.shares, iih.as_of_date, iih.direct_indirect,
                   f.primary_document_url
            FROM insider_initial_holdings iih
            JOIN insider_filings f USING (accession_number)
            {where}
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        cik = row["filer_cik"]
        if cik is not None:
            cik = str(cik).strip() or None
        name = str(row["filer_name"] or "").strip()
        if not name and cik is None:
            summary.orphans.append(f"insider_initial_holdings accession={row['accession_number']} (no cik or name)")
            continue
        nature = "indirect" if (row["direct_indirect"] == "I") else "direct"
        try:
            record_insider_observation(
                conn,
                instrument_id=int(row["instrument_id"]),
                holder_cik=cik,
                holder_name=name or (cik or "UNKNOWN"),
                ownership_nature=nature,  # type: ignore[arg-type]
                source="form3",
                source_document_id=str(row["accession_number"]),
                source_accession=str(row["accession_number"]),
                source_field=None,
                source_url=str(row["primary_document_url"]) if row["primary_document_url"] else None,
                filed_at=datetime.combine(row["as_of_date"], datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=row["as_of_date"],
                ingest_run_id=run_id,
                shares=Decimal(row["shares"]),
            )
            summary.observations_recorded += 1
            instruments_touched.add(int(row["instrument_id"]))
        except Exception as exc:
            summary.orphans.append(f"insider_initial_holdings accession={row['accession_number']}: {exc}")

    summary.instruments_refreshed = _refresh_for_instruments(
        conn, instrument_ids=instruments_touched, refresh_fn=refresh_insiders_current, summary=summary
    )
    return summary


# ---------------------------------------------------------------------------
# Institutions (13F-HR)
# ---------------------------------------------------------------------------


def sync_institutions(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit: int | None = None,
) -> SyncSummary:
    """Mirror ``institutional_holdings`` (joined to
    ``institutional_filers`` for cik) into
    ``ownership_institutions_observations`` and refresh ``_current``.

    Per Codex plan-review finding #2: filer_id → cik resolution is
    explicit; orphans (filer_id without parent row) are logged and
    skipped, never silently dropped. ``ownership_nature='economic'``
    for the equity slice; ``exposure_kind`` mirrors ``is_put_call``."""
    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    where = "WHERE 1=1"
    params: dict[str, Any] = {}
    if since is not None:
        where += " AND ih.period_of_report >= %(since)s"
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT ih.instrument_id, ih.filer_id, ih.accession_number,
                   ih.period_of_report, ih.shares, ih.market_value_usd,
                   ih.voting_authority, ih.is_put_call, ih.filed_at,
                   f.cik, f.name AS filer_name, f.filer_type
            FROM institutional_holdings ih
            JOIN institutional_filers f ON f.filer_id = ih.filer_id
            {where}
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        cik = str(row["cik"] or "").strip()
        if not cik:
            summary.orphans.append(f"institutional_holdings filer_id={row['filer_id']} (blank cik)")
            continue
        exposure = "EQUITY"
        if row["is_put_call"] in ("PUT", "CALL"):
            exposure = str(row["is_put_call"])
        try:
            record_institution_observation(
                conn,
                instrument_id=int(row["instrument_id"]),
                filer_cik=cik,
                filer_name=str(row["filer_name"]),
                filer_type=str(row["filer_type"]) if row["filer_type"] else None,
                ownership_nature="economic",
                source="13f",
                source_document_id=str(row["accession_number"]),
                source_accession=str(row["accession_number"]),
                source_field=None,
                source_url=None,
                filed_at=row["filed_at"] or datetime.combine(row["period_of_report"], datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=row["period_of_report"],
                ingest_run_id=run_id,
                shares=Decimal(row["shares"]),
                # Codex review: ``is not None`` so a legitimate zero
                # value isn't dropped via truthiness.
                market_value_usd=(Decimal(row["market_value_usd"]) if row["market_value_usd"] is not None else None),
                voting_authority=str(row["voting_authority"]) if row["voting_authority"] else None,
                exposure_kind=exposure,  # type: ignore[arg-type]
            )
            summary.observations_recorded += 1
            instruments_touched.add(int(row["instrument_id"]))
        except Exception as exc:
            summary.orphans.append(f"institutional_holdings accession={row['accession_number']}: {exc}")

    summary.instruments_refreshed = _refresh_for_instruments(
        conn, instrument_ids=instruments_touched, refresh_fn=refresh_institutions_current, summary=summary
    )
    return summary


# ---------------------------------------------------------------------------
# Blockholders (13D/G)
# ---------------------------------------------------------------------------


def sync_blockholders(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit: int | None = None,
) -> SyncSummary:
    """Mirror ``blockholder_filings`` (joined to ``blockholder_filers``
    for primary cik) into ``ownership_blockholders_observations`` and
    refresh ``_current``.

    Per #837 lesson + Codex plan review: identity is the PRIMARY filer
    (filer_id → cik), never the per-row reporter_cik. Joint reporters
    on the same accession collapse via ``DISTINCT ON (accession_number,
    filer_id)`` so each accession contributes one observation per
    primary filer."""
    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    # Codex review: filed_at is nullable on the legacy table but
    # required on observations. Filter NULLs out at the query level
    # rather than discovering the orphan per-row in the catch-all
    # exception handler — keeps the orphans list focused on real
    # identity gaps.
    where = "WHERE bf.aggregate_amount_owned IS NOT NULL AND bf.filed_at IS NOT NULL"
    params: dict[str, Any] = {}
    if since is not None:
        where += " AND bf.filed_at >= %(since)s"
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    # DISTINCT ON keeps one row per (accession, primary filer) — joint
    # reporter dimension collapses per the SEC convention that joint
    # filers claim the same beneficial figure on the cover page.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (bf.accession_number, bf.filer_id)
                   bf.instrument_id, bf.accession_number,
                   bf.submission_type, bf.status, bf.filed_at,
                   bf.aggregate_amount_owned, bf.percent_of_class,
                   f.cik, f.name AS filer_name
            FROM blockholder_filings bf
            JOIN blockholder_filers f ON f.filer_id = bf.filer_id
            {where}
            ORDER BY bf.accession_number, bf.filer_id,
                     bf.aggregate_amount_owned DESC NULLS LAST
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        cik = str(row["cik"] or "").strip()
        if not cik or row["instrument_id"] is None:
            summary.orphans.append(f"blockholder_filings accession={row['accession_number']} (blank cik or instrument)")
            continue
        # Map submission_type to source tag.
        stype = str(row["submission_type"])
        source = "13d" if stype.startswith("SCHEDULE 13D") else "13g"
        try:
            record_blockholder_observation(
                conn,
                instrument_id=int(row["instrument_id"]),
                reporter_cik=cik,
                reporter_name=str(row["filer_name"]),
                ownership_nature="beneficial",
                submission_type=stype,
                status_flag=str(row["status"]) if row["status"] else None,
                source=source,  # type: ignore[arg-type]
                source_document_id=str(row["accession_number"]),
                source_accession=str(row["accession_number"]),
                source_field=None,
                source_url=None,
                filed_at=row["filed_at"],
                period_start=None,
                # filed_at is required at the query level (Codex review)
                # so .date() is always safe — no fallback to today().
                period_end=row["filed_at"].date(),
                ingest_run_id=run_id,
                aggregate_amount_owned=Decimal(row["aggregate_amount_owned"]),
                # ``is not None`` so a zero value isn't dropped via truthiness.
                percent_of_class=(Decimal(row["percent_of_class"]) if row["percent_of_class"] is not None else None),
            )
            summary.observations_recorded += 1
            instruments_touched.add(int(row["instrument_id"]))
        except Exception as exc:
            summary.orphans.append(f"blockholder_filings accession={row['accession_number']}: {exc}")

    summary.instruments_refreshed = _refresh_for_instruments(
        conn, instrument_ids=instruments_touched, refresh_fn=refresh_blockholders_current, summary=summary
    )
    return summary


# ---------------------------------------------------------------------------
# Treasury
# ---------------------------------------------------------------------------


def sync_treasury(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit: int | None = None,
) -> SyncSummary:
    """Mirror ``financial_periods.treasury_shares`` into
    ``ownership_treasury_observations`` and refresh ``_current``.

    Source = ``'xbrl_dei'`` (the canonical XBRL DEI / us-gaap concept
    extraction path). Synthetic ``source_document_id`` is
    ``f'{instrument_id}|{period_end}'`` since financial_periods doesn't
    carry an accession on the canonical row — the original facts in
    ``financial_facts_raw`` do, but joining there per row is heavy
    for the sync. The synthetic id is stable so re-runs are
    idempotent."""
    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    where = (
        "WHERE fp.treasury_shares IS NOT NULL AND fp.superseded_at IS NULL AND fp.period_type IN ('Q1','Q2','Q3','Q4')"
    )
    params: dict[str, Any] = {}
    if since is not None:
        where += " AND fp.period_end_date >= %(since)s"
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT fp.instrument_id, fp.period_end_date, fp.treasury_shares,
                   fp.filed_date
            FROM financial_periods fp
            {where}
            ORDER BY fp.period_end_date DESC
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        iid = int(row["instrument_id"])
        period_end = row["period_end_date"]
        synthetic_doc_id = f"{iid}|{period_end.isoformat()}"
        try:
            record_treasury_observation(
                conn,
                instrument_id=iid,
                source="xbrl_dei",
                source_document_id=synthetic_doc_id,
                source_accession=None,
                source_field="TreasuryStockShares",
                source_url=None,
                filed_at=row["filed_date"] or datetime.combine(period_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=period_end,
                ingest_run_id=run_id,
                treasury_shares=Decimal(row["treasury_shares"]),
            )
            summary.observations_recorded += 1
            instruments_touched.add(iid)
        except Exception as exc:
            summary.orphans.append(f"treasury instrument_id={iid} period={period_end}: {exc}")

    summary.instruments_refreshed = _refresh_for_instruments(
        conn, instrument_ids=instruments_touched, refresh_fn=refresh_treasury_current, summary=summary
    )
    return summary


# ---------------------------------------------------------------------------
# DEF 14A
# ---------------------------------------------------------------------------


def sync_def14a(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit: int | None = None,
) -> SyncSummary:
    """Mirror ``def14a_beneficial_holdings`` into
    ``ownership_def14a_observations`` and refresh ``_current``.

    ``ownership_nature`` defaults to ``'beneficial'`` (DEF 14A's
    canonical table reports beneficial ownership per Rule 13d-3)."""
    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    # Bot review for #840.E-prep: defensive guard against a NULL
    # fetched_at falling through to .date() below. Schema declares
    # fetched_at NOT NULL so this should never be reachable, but
    # query-level filter is the cheapest belt-and-braces.
    where = (
        "WHERE d14.shares IS NOT NULL AND d14.instrument_id IS NOT NULL "
        "AND (d14.as_of_date IS NOT NULL OR d14.fetched_at IS NOT NULL)"
    )
    params: dict[str, Any] = {}
    if since is not None:
        where += " AND d14.as_of_date >= %(since)s"
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT d14.instrument_id, d14.accession_number, d14.holder_name,
                   d14.holder_role, d14.shares, d14.percent_of_class,
                   d14.as_of_date, d14.fetched_at
            FROM def14a_beneficial_holdings d14
            {where}
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        iid = int(row["instrument_id"])
        as_of = row["as_of_date"] or row["fetched_at"].date()
        try:
            record_def14a_observation(
                conn,
                instrument_id=iid,
                holder_name=str(row["holder_name"]),
                holder_role=str(row["holder_role"]) if row["holder_role"] else None,
                ownership_nature="beneficial",
                source="def14a",
                source_document_id=str(row["accession_number"]),
                source_accession=str(row["accession_number"]),
                source_field=None,
                source_url=None,
                filed_at=row["fetched_at"] or datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=as_of,
                ingest_run_id=run_id,
                shares=Decimal(row["shares"]),
                # Codex review: ``is not None`` so a zero percent
                # isn't dropped via truthiness.
                percent_of_class=(Decimal(row["percent_of_class"]) if row["percent_of_class"] is not None else None),
            )
            summary.observations_recorded += 1
            instruments_touched.add(iid)
        except Exception as exc:
            summary.orphans.append(f"def14a accession={row['accession_number']} holder={row['holder_name']}: {exc}")

    summary.instruments_refreshed = _refresh_for_instruments(
        conn, instrument_ids=instruments_touched, refresh_fn=refresh_def14a_current, summary=summary
    )
    return summary


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


@dataclass
class SyncAllResult:
    insiders: SyncSummary
    institutions: SyncSummary
    blockholders: SyncSummary
    treasury: SyncSummary
    def14a: SyncSummary

    @property
    def total_observations_recorded(self) -> int:
        return (
            self.insiders.observations_recorded
            + self.institutions.observations_recorded
            + self.blockholders.observations_recorded
            + self.treasury.observations_recorded
            + self.def14a.observations_recorded
        )


def sync_all(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit_per_category: int | None = None,
) -> SyncAllResult:
    """Run every category sync. Caller commits between categories
    (each ``sync_*`` commits its own observations via the underlying
    helpers; ``refresh_*_current`` runs inside its own transaction
    via the ``conn.transaction()`` wrap)."""
    insiders = sync_insiders(conn, since=since, limit=limit_per_category)
    conn.commit()
    institutions = sync_institutions(conn, since=since, limit=limit_per_category)
    conn.commit()
    blockholders = sync_blockholders(conn, since=since, limit=limit_per_category)
    conn.commit()
    treasury = sync_treasury(conn, since=since, limit=limit_per_category)
    conn.commit()
    def14a = sync_def14a(conn, since=since, limit=limit_per_category)
    conn.commit()
    result = SyncAllResult(
        insiders=insiders,
        institutions=institutions,
        blockholders=blockholders,
        treasury=treasury,
        def14a=def14a,
    )
    logger.info(
        "ownership_observations_sync.sync_all: total_observations=%d "
        "insiders=%d institutions=%d blockholders=%d treasury=%d def14a=%d",
        result.total_observations_recorded,
        insiders.observations_recorded,
        institutions.observations_recorded,
        blockholders.observations_recorded,
        treasury.observations_recorded,
        def14a.observations_recorded,
    )
    return result


__all__ = [
    "SyncAllResult",
    "SyncSummary",
    "resolve_filer_cik_or_raise",
    "sync_all",
    "sync_blockholders",
    "sync_def14a",
    "sync_insiders",
    "sync_institutions",
    "sync_treasury",
]
