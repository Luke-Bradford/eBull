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

from app.providers.implementations.sec_def14a import extract_plan_name_and_trustee, is_esop_plan
from app.services.ownership_observations import (
    record_blockholder_observation,
    record_def14a_observation,
    record_esop_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
    refresh_blockholders_current,
    refresh_blockholders_current_batch,
    refresh_current_with_batch_fallback,
    refresh_def14a_current,
    refresh_def14a_current_batch,
    refresh_esop_current,
    refresh_esop_current_batch,
    refresh_insiders_current,
    refresh_insiders_current_batch,
    refresh_institutions_current,
    refresh_institutions_current_batch,
    refresh_treasury_current,
    refresh_treasury_current_batch,
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
    refresh_batch_fn: Any,
    refresh_one_fn: Any,
    summary: SyncSummary,
) -> int:
    """Refresh ``_current`` for every touched instrument via the batch
    MERGE writer (#1345 PR-B), falling back to the per-instrument writer
    if the atomic batch fails.

    Codex review for #840.E-prep: refresh failures must surface in the
    operator-visible summary, not get logged-and-swallowed — otherwise
    sync_all reports success while ``_current`` stays stale for the
    failed instruments and the next rollup read returns inconsistent
    data. The shared helper returns the ids that failed even the
    per-instrument fallback; map each onto ``summary.orphans`` so the
    run status reflects the failure (same contract as the old
    per-instrument loop)."""
    refreshed, failures = refresh_current_with_batch_fallback(
        conn,
        instrument_ids=instrument_ids,
        refresh_batch_fn=refresh_batch_fn,
        refresh_one_fn=refresh_one_fn,
    )
    for iid, exc in failures:
        summary.orphans.append(f"refresh failed instrument_id={iid}: {exc}")
    return refreshed


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
    #
    # PR10b (#1233 §4.4) — Form 5 18-month retention cap chokepoint.
    # ``sync_insiders`` is a steady-state observations writer: it
    # mirrors typed-table rows into the observations layer, so a
    # post-cap re-run would re-write pre-cap Form 5 observations
    # without the gate. The LEFT JOIN to ``filing_events`` (canonical
    # source of ``filing_date`` outside the typed tables) lets each
    # retention-capped branch reference ``fe.filing_date >= cutoff``.
    # The JOIN is clamped to ``fe.instrument_id = it.instrument_id``
    # so a sibling share-class's ``filing_events`` row cannot satisfy
    # the gate for the row's own instrument (#1247 Codex 2 — per
    # data-engineer skill §Q15, ``filing_events`` is fanned out across
    # share-class siblings sharing a CIK; without the instrument
    # clamp, sibling-A's row would gate sibling-B's transaction).
    #
    # #1247 (PR4 follow-up) — Form 4 3y retention cap also gated here.
    # Same strict shape as Form 5: ``fe.filing_date IS NOT NULL AND
    # fe.filing_date >= form4_cutoff``. Closes the gap PR4 left for
    # sync_insiders. Pre-cap Form 4 rows lacking a ``filing_events``
    # row are excluded because we cannot prove their retention status
    # — fail-closed for unattributed legacy rows is the conservative
    # interpretation of §6.3 ingest-side capping. The prior PR10b
    # carve-out ("Form 4 rows without ``filing_events`` still sync")
    # is retired; the matching legacy-test
    # ``test_form4_without_filing_events_row_still_syncs`` is updated
    # to assert the new contract.
    #
    # Form 3 ('3'/'3/A') is read-side latest-per-pair (per PR10b
    # §4.4); no ingest-side retention cap, so it stays in the
    # unconditional branch.
    from app.services.insider_transactions import (
        form4_retention_cutoff,
        form5_retention_cutoff,
    )

    where = "WHERE it.post_transaction_shares IS NOT NULL AND it.is_derivative = FALSE"
    params: dict[str, Any] = {
        "form4_cutoff": form4_retention_cutoff(),
        "form5_cutoff": form5_retention_cutoff(),
    }
    if since is not None:
        where += " AND it.txn_date >= %(since)s"
        params["since"] = since
    # Retention gates resolve the SEC filing date manifest-first
    # (#899 Codex ckpt-2): a manifest-only accession (no filing_events
    # row) is provably in/out of retention via m.filed_at — gating on
    # fe.filing_date alone wrongly excluded it. Rows with NEITHER
    # source stay excluded (fail-closed for unattributed legacy rows,
    # #1247).
    where += (
        " AND ("
        "  f.document_type IN ('3','3/A')"
        "  OR ("
        "    f.document_type IN ('4','4/A')"
        "    AND COALESCE((m.filed_at AT TIME ZONE 'UTC')::date, fe.filing_date) IS NOT NULL"
        "    AND COALESCE((m.filed_at AT TIME ZONE 'UTC')::date, fe.filing_date) >= %(form4_cutoff)s"
        "  )"
        "  OR ("
        "    f.document_type IN ('5','5/A')"
        "    AND COALESCE((m.filed_at AT TIME ZONE 'UTC')::date, fe.filing_date) IS NOT NULL"
        "    AND COALESCE((m.filed_at AT TIME ZONE 'UTC')::date, fe.filing_date) >= %(form5_cutoff)s"
        "  )"
        ")"
    )
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
                f.primary_document_url,
                COALESCE(m.filed_at, fe.filing_date::timestamp AT TIME ZONE 'UTC') AS sec_filed_at
            FROM insider_transactions it
            JOIN insider_filings f USING (accession_number)
            LEFT JOIN filing_events fe
              ON fe.provider_filing_id = it.accession_number
             AND fe.provider = 'sec'
             AND fe.instrument_id = it.instrument_id
            LEFT JOIN sec_filing_manifest m
              ON m.accession_number = it.accession_number
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
                # #899 — SEC filing timestamp (manifest → filing_events);
                # txn_date fallback only for rows with neither.
                filed_at=row["sec_filed_at"]
                if row["sec_filed_at"] is not None
                else datetime.combine(row["txn_date"], datetime.min.time(), tzinfo=UTC),
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
                   f.primary_document_url,
                   COALESCE(m.filed_at, fe.filing_date::timestamp AT TIME ZONE 'UTC') AS sec_filed_at
            FROM insider_initial_holdings iih
            JOIN insider_filings f USING (accession_number)
            LEFT JOIN filing_events fe
              ON fe.provider_filing_id = iih.accession_number
             AND fe.provider = 'sec'
             AND fe.instrument_id = iih.instrument_id
            LEFT JOIN sec_filing_manifest m
              ON m.accession_number = iih.accession_number
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
                # #899 — SEC filing timestamp (manifest → filing_events);
                # as_of_date fallback only for rows with neither.
                filed_at=row["sec_filed_at"]
                if row["sec_filed_at"] is not None
                else datetime.combine(row["as_of_date"], datetime.min.time(), tzinfo=UTC),
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
        conn,
        instrument_ids=instruments_touched,
        refresh_batch_fn=refresh_insiders_current_batch,
        refresh_one_fn=refresh_insiders_current,
        summary=summary,
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
    for the equity slice; ``exposure_kind`` mirrors ``is_put_call``.

    PR6 #1233 §4.5 — the 8-quarter retention cap is enforced as a SQL
    predicate so this scheduled repair sweep cannot repopulate pre-cap
    observations from any pre-cap ``institutional_holdings`` rows that
    still exist in the dev DB pre-pre-wipe. ``since`` is the caller's
    optional ADDITIONAL floor (incremental repair); the cap is the
    intrinsic floor."""
    from app.services.institutional_holdings import thirteen_f_retention_cutoff

    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    retention_cutoff = thirteen_f_retention_cutoff()
    where = "WHERE ih.period_of_report >= %(retention_cutoff)s"
    params: dict[str, Any] = {"retention_cutoff": retention_cutoff}
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
        conn,
        instrument_ids=instruments_touched,
        refresh_batch_fn=refresh_institutions_current_batch,
        refresh_one_fn=refresh_institutions_current,
        summary=summary,
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
    """Mirror ``blockholder_filings`` into
    ``ownership_blockholders_observations`` and refresh ``_current``.

    Identity is the per-row ``reporter_cik`` of the largest-aggregate
    reporting person (the disclosing person's own CIK), NOT
    ``blockholder_filers.cik`` — post-#1628 that filer row carries the
    subject/issuer CIK on the drain path (#1638). Joint reporters on the
    same accession collapse via ``DISTINCT ON (accession_number)``
    ordered by aggregate so each accession contributes one observation
    (#837). Rows whose ``reporter_cik`` is NULL (13G covers omit
    per-reporter CIKs; the rare multi-party 13D whose largest reporter is
    CIK-less) are SKIPPED here — the canonical write-through path
    (``_record_13dg_observation_for_filing``) populates them with the
    document filer-of-record fallback, which the typed tables cannot
    reconstruct. This legacy mirror runs only via the manual one-shot
    ``ownership_observations_backfill``; the daily job is the repair
    sweep.

    PR11 #1233 §3.2 chokepoint C — the 3y retention cap is enforced as
    a SQL predicate on the raw chain's own ``bf.filed_at`` column so
    this steady-state observations writer cannot repopulate pre-cap
    observations from any pre-cap ``blockholder_filings`` rows that
    still exist in the dev DB pre-pre-wipe. The gate uses
    ``bf.filed_at`` directly (NOT a ``LEFT JOIN filing_events ...
    WHERE fe.filing_date >= cutoff`` predicate) because such a predicate
    null-rejects rows missing a ``filing_events`` entry — the Codex 1a
    HIGH #4 / Codex 1b PR10b lesson for this category of cap. ``since``
    is the caller's optional ADDITIONAL floor (incremental repair); the
    cap is the intrinsic floor."""
    from app.services.blockholders import blockholders_retention_cutoff

    summary = SyncSummary()
    instruments_touched: set[int] = set()
    run_id = uuid4()

    # Codex review: filed_at is nullable on the legacy table but
    # required on observations. Filter NULLs out at the query level
    # rather than discovering the orphan per-row in the catch-all
    # exception handler — keeps the orphans list focused on real
    # identity gaps.
    where = (
        "WHERE bf.aggregate_amount_owned IS NOT NULL "
        "AND bf.filed_at IS NOT NULL "
        "AND bf.filed_at >= %(retention_cutoff)s"
    )
    params: dict[str, Any] = {"retention_cutoff": blockholders_retention_cutoff()}
    if since is not None:
        where += " AND bf.filed_at >= %(since)s"
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    # DISTINCT ON keeps the largest-aggregate reporting person per
    # accession — joint reporters collapse to one observation per the SEC
    # convention that joint filers claim the same beneficial figure (#837).
    # Identity is the per-row ``reporter_cik`` (the disclosing person's
    # own CIK), NOT ``blockholder_filers.cik`` which post-#1628 is the
    # subject/issuer on the drain path (#1638).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (bf.accession_number)
                   bf.instrument_id, bf.accession_number,
                   bf.submission_type, bf.status, bf.filed_at,
                   bf.aggregate_amount_owned, bf.percent_of_class,
                   bf.reporter_cik, bf.reporter_name
            FROM blockholder_filings bf
            {where}
            ORDER BY bf.accession_number,
                     bf.aggregate_amount_owned DESC NULLS LAST,
                     -- Deterministic tie-breaker matching the write-through
                     -- resolver, whose max() returns the FIRST equal-aggregate
                     -- reporter in XML order. _upsert_filing_row inserts
                     -- reporters in XML order, so the lowest filing_id is the
                     -- first XML reporter — both paths pick the same CIK on a
                     -- tie, so no second observation under a different natural
                     -- key (Codex ckpt-2).
                     bf.filing_id ASC
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    for row in rows:
        summary.rows_scanned += 1
        reporter_cik = str(row["reporter_cik"] or "").strip()
        if not reporter_cik or row["instrument_id"] is None:
            # NULL per-reporter CIK = a 13G cover (or the rare multi-party
            # 13D whose largest reporter is CIK-less). The typed tables
            # carry no filer-of-record fallback, so defer to the canonical
            # write-through populator rather than write a wrong CIK (#1638).
            summary.orphans.append(
                f"blockholder_filings accession={row['accession_number']} (no per-reporter cik or instrument)"
            )
            continue
        # Map submission_type to source tag.
        stype = str(row["submission_type"])
        source = "13d" if stype.startswith("SCHEDULE 13D") else "13g"
        try:
            record_blockholder_observation(
                conn,
                instrument_id=int(row["instrument_id"]),
                reporter_cik=reporter_cik,
                reporter_name=str(row["reporter_name"]),
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
        conn,
        instrument_ids=instruments_touched,
        refresh_batch_fn=refresh_blockholders_current_batch,
        refresh_one_fn=refresh_blockholders_current,
        summary=summary,
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
        conn,
        instrument_ids=instruments_touched,
        refresh_batch_fn=refresh_treasury_current_batch,
        refresh_one_fn=refresh_treasury_current,
        summary=summary,
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

    esop_instruments_touched: set[int] = set()
    for row in rows:
        summary.rows_scanned += 1
        iid = int(row["instrument_id"])
        as_of = row["as_of_date"] or row["fetched_at"].date()
        # ESOP rows route to ownership_esop_observations, NOT the
        # general def14a observations path. The legacy ingest before
        # #843 tagged ESOP rows with the section-context role
        # ('principal' typically) — role check alone misses them.
        # Run the same name-pattern detection the parser uses so a
        # bootstrap / repair over pre-#843 def14a_beneficial_holdings
        # rows correctly routes legacy plans into the ESOP slice
        # rather than dropping them on the floor (refresh_def14a_current
        # filters them by name regex). Codex pre-push review #843
        # rounds 2 + 4 caught this.
        if str(row.get("holder_role") or "") == "esop" or is_esop_plan(str(row.get("holder_name") or "")):
            try:
                plan_name, trustee_name = extract_plan_name_and_trustee(str(row["holder_name"]))
                if not plan_name:
                    continue
                record_esop_observation(
                    conn,
                    instrument_id=iid,
                    plan_name=plan_name,
                    plan_trustee_name=trustee_name,
                    plan_trustee_cik=None,
                    source_document_id=str(row["accession_number"]),
                    source_accession=str(row["accession_number"]),
                    source_field=None,
                    source_url=None,
                    filed_at=row["fetched_at"] or datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
                    period_start=None,
                    period_end=as_of,
                    ingest_run_id=run_id,
                    shares=Decimal(row["shares"]),
                    percent_of_class=(
                        Decimal(row["percent_of_class"]) if row["percent_of_class"] is not None else None
                    ),
                )
                summary.observations_recorded += 1
                esop_instruments_touched.add(iid)
            except Exception as exc:
                summary.orphans.append(
                    f"def14a-esop accession={row['accession_number']} holder={row['holder_name']}: {exc}"
                )
            continue
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
        conn,
        instrument_ids=instruments_touched,
        refresh_batch_fn=refresh_def14a_current_batch,
        refresh_one_fn=refresh_def14a_current,
        summary=summary,
    )
    if esop_instruments_touched:
        summary.instruments_refreshed += _refresh_for_instruments(
            conn,
            instrument_ids=esop_instruments_touched,
            refresh_batch_fn=refresh_esop_current_batch,
            refresh_one_fn=refresh_esop_current,
            summary=summary,
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
    """Run every legacy-mirror category sync (one-shot backfill).

    Dispatches exactly **5 categories**: insiders, institutions,
    blockholders, treasury, def14a. These are the categories with
    legacy typed source tables to mirror into ``ownership_*_observations``.

    **Asymmetric with the daily drift-repair sweep** at
    :mod:`app.jobs.ownership_observations_repair` (``_CATEGORIES`` lists
    **7**: the 5 here + ``funds`` + ``esop``). The asymmetry is by-design:

    * **Funds** has no legacy mirror source — fund holdings land via
      NPORT manifest-worker write-through (``sec_n_port.py`` parser →
      ``refresh_funds_current``) and via the bulk-dataset ingest path
      (``sec_bulk_orchestrator_jobs.py``). There is no legacy
      ``fund_holdings`` table to read from.
    * **ESOP** rows ARE processed here, but transitively inside
      ``sync_def14a`` (lines 691-769) — DEF 14A bene-table rows flagged
      as ESOP route into ``ownership_esop_observations`` + call
      ``refresh_esop_current``. There is no separate ``sync_esop``
      entry because ESOP shares the DEF 14A source.

    The daily 03:30 UTC ``JOB_OWNERSHIP_OBSERVATIONS_SYNC`` (which calls
    ``run_observations_repair_sweep``, NOT ``sync_all``) is the
    integrity floor for all 7 categories — drift between observations
    and ``_current`` for any category, including funds + esop, is
    detected and repaired within 24h regardless of which dispatch path
    populated the observations.

    Caller commits between categories (each ``sync_*`` commits its own
    observations via the underlying helpers; ``refresh_*_current`` runs
    inside its own transaction via the ``conn.transaction()`` wrap).

    See ``.claude/skills/data-engineer/SKILL.md`` §write-through for
    the canonical statement of these invariants.
    """
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
