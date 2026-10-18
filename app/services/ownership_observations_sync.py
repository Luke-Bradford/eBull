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
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import partial
from itertools import groupby
from typing import Any, Final
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


@dataclass(frozen=True)
class _StoredForm3Line:
    """A stored ``insider_initial_holdings`` line, shaped for ``form3_observation_shares``."""

    row: dict[str, Any]
    row_num: int | None
    is_derivative: bool
    filer_cik: str | None
    security_title: str | None
    shares: Decimal | None
    direct_indirect: str | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> _StoredForm3Line:
        cik = row["filer_cik"]
        return cls(
            row=row,
            row_num=row["row_num"],
            is_derivative=bool(row["is_derivative"]),
            filer_cik=(str(cik).strip() or None) if cik is not None else None,
            security_title=row["security_title"],
            shares=Decimal(row["shares"]) if row["shares"] is not None else None,
            direct_indirect=row["direct_indirect"],
        )


@dataclass(frozen=True)
class _StoredForm4Line:
    """A stored ``insider_transactions`` line, shaped for ``form4_observation_shares``."""

    row: dict[str, Any]
    txn_row_num: int
    is_derivative: bool
    security_title: str | None
    txn_date: date
    txn_date_invalid: bool
    shares: Decimal | None
    acquired_disposed_code: str | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> _StoredForm4Line:
        return cls(
            row=row,
            txn_row_num=int(row["txn_row_num"]),
            is_derivative=False,  # the line query reads non-derivative lines only
            security_title=row["security_title"],
            txn_date=row["txn_date"],
            txn_date_invalid=bool(row["txn_date_invalid"]),
            shares=Decimal(row["shares"]) if row["shares"] is not None else None,
            acquired_disposed_code=row["acquired_disposed_code"],
            post_transaction_shares=(
                Decimal(row["post_transaction_shares"]) if row["post_transaction_shares"] is not None else None
            ),
            direct_indirect=row["direct_indirect"],
            nature_of_ownership=row["nature_of_ownership"],
        )


@dataclass(frozen=True)
class Form4Observation:
    """One ``form4``-source observation as ``sync_insiders`` derives it, before it is recorded.
    ``verdict`` is :func:`form4_observation_shares`'s for an ``I`` observation of a Form 4/4A, else
    None; ``latest_line_shares`` is the latest eligible line's balance (the pre-#3227 value)."""

    instrument_id: int
    accession_number: str
    holder_cik: str | None
    holder_name: str
    ownership_nature: str
    source_url: str | None
    sec_filed_at: datetime | None
    period_end: date
    shares: Decimal
    latest_line_shares: Decimal
    verdict: str | None
    # ``transaction``, or ``holding`` for a holding-only group (#3227, ``form4_holding_observations``;
    # ``verdict`` is then its verdict and ``latest_line_shares`` equals ``shares``).
    kind: str = "transaction"
    # A transaction observation's ``form4_fold_shares`` verdict (#3227 mixed groups); ``shares`` is
    # its folded value only when this is ``sums``. None on a holding-only observation.
    fold_verdict: str | None = None


@dataclass(frozen=True)
class _StoredForm4Holding:
    """A stored ``insider_form4_holdings`` line, shaped for ``form4_holding_observations`` and
    ``form4_fold_shares``."""

    row: dict[str, Any]
    is_derivative: bool
    security_title: str | None
    shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> _StoredForm4Holding:
        return cls(
            row=row,
            is_derivative=False,  # the holding query reads non-derivative lines only
            security_title=row["security_title"],
            shares=Decimal(row["shares"]) if row["shares"] is not None else None,
            direct_indirect=row["direct_indirect"],
            nature_of_ownership=row["nature_of_ownership"],
        )


# A unit is one filer's lines in one accession on one instrument (#3227). The CIK is stripped as
# ``btrim`` strips; a null or blank CIK keeps the pre-#3227 grouping, which also split on the name.
# Units come from the transaction lines ({where}) and, for Form 4/4A, from the non-derivative
# holding lines of an accession with a manifest ``filed_at`` in retention ({holding_where}) — the
# gates ``form4_holding_observations`` applies, which have no ``filing_events`` fallback.
_FORM4_UNITS_SQL: Final = """
SELECT instrument_id, accession_number, unit_cik, unit_name, document_type, primary_document_url,
       signature_date, period_of_report,
       min(sec_filed_at) AS sec_filed_at, max(manifest_filed_at) AS manifest_filed_at,
       bool_or(from_txn) AS from_txn
  FROM (
    SELECT it.instrument_id, it.accession_number,
           CASE WHEN nullif(btrim(it.filer_cik), '') IS NULL THEN it.filer_cik ELSE btrim(it.filer_cik) END AS unit_cik,
           CASE WHEN nullif(btrim(it.filer_cik), '') IS NULL THEN it.filer_name END AS unit_name,
           f.document_type, f.primary_document_url, f.signature_date, f.period_of_report,
           COALESCE(m.filed_at, fe.filing_date::timestamp AT TIME ZONE 'UTC') AS sec_filed_at,
           m.filed_at AS manifest_filed_at, TRUE AS from_txn
      FROM insider_transactions it
      JOIN insider_filings f USING (accession_number)
      LEFT JOIN filing_events fe
        ON fe.provider_filing_id = it.accession_number
       AND fe.provider = 'sec'
       AND fe.instrument_id = it.instrument_id
      LEFT JOIN sec_filing_manifest m
        ON m.accession_number = it.accession_number
     {where}
    UNION ALL
    SELECT h.instrument_id, h.accession_number,
           CASE WHEN nullif(btrim(h.filer_cik), '') IS NULL THEN h.filer_cik ELSE btrim(h.filer_cik) END,
           CASE WHEN nullif(btrim(h.filer_cik), '') IS NULL THEN h.filer_name END,
           f.document_type, f.primary_document_url, f.signature_date, f.period_of_report,
           m.filed_at, m.filed_at, FALSE
      FROM insider_form4_holdings h
      JOIN insider_filings f USING (accession_number)
      JOIN sec_filing_manifest m ON m.accession_number = h.accession_number
     {holding_where}
  ) u
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
 ORDER BY 1, 2, 3, 4
 {limit}
"""

_FORM4_HOLDINGS_SQL: Final = """
SELECT h.instrument_id, h.accession_number, h.row_num, h.filer_cik, h.filer_name,
       h.security_title, h.shares, h.direct_indirect, h.nature_of_ownership
  FROM insider_form4_holdings h
 WHERE h.accession_number = ANY(%(accs)s::text[]) AND NOT h.is_derivative
 ORDER BY h.instrument_id, h.accession_number, h.row_num
"""

_FORM4_LINES_SQL: Final = """
SELECT it.instrument_id, it.accession_number, it.txn_row_num, it.filer_cik, it.filer_name,
       it.security_title, it.txn_date, it.txn_date_invalid, it.shares, it.acquired_disposed_code,
       it.post_transaction_shares, it.direct_indirect, it.nature_of_ownership
  FROM insider_transactions it
 WHERE it.accession_number = ANY(%(accs)s::text[]) AND NOT it.is_derivative
 ORDER BY it.instrument_id, it.accession_number, it.txn_row_num
"""

_FORM4_OWNERS_SQL: Final = """
SELECT accession_number, array_agg(filer_cik) FROM insider_filers
 WHERE accession_number = ANY(%(accs)s::text[]) GROUP BY 1
"""

_FORM4_ACCESSION_CHUNK = 2000


def _form4_unit_key(row: dict[str, Any]) -> tuple[int, str, str | None, str | None]:
    """The same key ``_FORM4_UNITS_SQL`` selects, from a line row."""
    cik = row["filer_cik"]
    stripped = cik.strip(" ") if cik is not None else ""
    if stripped:
        return (int(row["instrument_id"]), row["accession_number"], stripped, None)
    return (int(row["instrument_id"]), row["accession_number"], cik, row["filer_name"])


def derive_form4_observations(
    conn: psycopg.Connection[Any],
    *,
    since: date | None = None,
    limit: int | None = None,
) -> Iterator[Form4Observation]:
    """Every ``form4``-source observation ``sync_insiders`` writes from ``insider_transactions``.

    One observation per ``(unit, direct_indirect)``, holding the unit's latest eligible line
    (``txn_date DESC, txn_row_num DESC``), where an eligible line is non-derivative, has a balance
    and a valid date. An ``I`` observation of a Form 4/4A instead holds its vehicles' sum where
    :func:`form4_observation_shares` allows it, and a ``D`` or ``I`` one whose unit has holding
    lines of that D/I folds them in where :func:`form4_fold_shares` allows it (#3227).

    Units are selected FIRST — the units with an eligible line in the retention window (and dated
    on or after ``since``), ``limit`` of them — and then every non-derivative line of those units
    is read. A ``since`` on line dates would cut a filing's earlier lines and write a partial sum,
    and the helper's completeness condition must see the lines this writer does not keep.

    Retention (#1233 §4.4, #1247): Form 4 3y and Form 5 18-month caps, the filing date resolved
    manifest-first (#899 Codex ckpt-2) and ``filing_events`` clamped to the row's own instrument
    (data-engineer §Q15 — siblings share a CIK). Rows with neither date source stay excluded
    (fail-closed for unattributed legacy rows). Form 3 rows here are uncapped.
    """
    from app.services.insider_transactions import (
        FORM4_HOLDING_NATURE,
        FORM4_VEHICLE_SUM_DOCUMENT_TYPES,
        FORM4_VEHICLE_SUMS,
        form4_earlier_balances,
        form4_fold_shares,
        form4_holding_observations,
        form4_observation_shares,
        form4_retention_cutoff,
        form5_retention_cutoff,
    )

    # #1687 — exclude future-dated txn_date typos so the latest-per-group
    # re-derivation never picks an impossible date as the current holding.
    where = "WHERE it.post_transaction_shares IS NOT NULL AND it.is_derivative = FALSE AND NOT it.txn_date_invalid"
    params: dict[str, Any] = {
        "form4_cutoff": form4_retention_cutoff(),
        "form5_cutoff": form5_retention_cutoff(),
    }
    if since is not None:
        where += " AND it.txn_date >= %(since)s"
        params["since"] = since
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
    # #3227 — holding units. ``since`` applies to the holding's ``period_end``
    # (``form4_holding_as_of``), exactly in Python below; this prefilter only skips accessions whose
    # later candidate date is already before it (``GREATEST`` ignores a NULL).
    holding_where = (
        "WHERE NOT h.is_derivative AND f.document_type IN ('4','4/A')"
        " AND (m.filed_at AT TIME ZONE 'UTC')::date >= %(form4_cutoff)s"
    )
    if since is not None:
        holding_where += " AND GREATEST(f.signature_date, f.period_of_report) >= %(since)s"
    if limit is not None:
        params["lim"] = limit
    sql = _FORM4_UNITS_SQL.format(
        where=where, holding_where=holding_where, limit="LIMIT %(lim)s" if limit is not None else ""
    )
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        units = {(int(u["instrument_id"]), u["accession_number"], u["unit_cik"], u["unit_name"]): u for u in cur}

    accessions = list(dict.fromkeys(key[1] for key in units))
    for i in range(0, len(accessions), _FORM4_ACCESSION_CHUNK):
        chunk = accessions[i : i + _FORM4_ACCESSION_CHUNK]
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_FORM4_LINES_SQL, {"accs": chunk})
            line_rows = cur.fetchall()
            cur.execute(_FORM4_HOLDINGS_SQL, {"accs": chunk})
            holding_rows = cur.fetchall()
            cur.execute(_FORM4_OWNERS_SQL, {"accs": chunk})
            owners = {r["accession_number"]: list(r["array_agg"]) for r in cur.fetchall()}
        # A dict, not ``groupby``: two filers' lines can interleave by ``txn_row_num``.
        by_unit: dict[tuple[int, str, str | None, str | None], list[_StoredForm4Line]] = {}
        for r in line_rows:
            by_unit.setdefault(_form4_unit_key(r), []).append(_StoredForm4Line.from_row(r))
        holdings_by_unit: dict[tuple[int, str, str | None, str | None], list[_StoredForm4Holding]] = {}
        for r in holding_rows:
            holdings_by_unit.setdefault(_form4_unit_key(r), []).append(_StoredForm4Holding.from_row(r))
        for key, held in holdings_by_unit.items():
            unit = units.get(key)
            if unit is None or key[3] is not None:
                continue
            groups = form4_holding_observations(
                held,
                by_unit.get(key, []),
                document_type=unit["document_type"],
                owner_ciks=owners.get(key[1], []),
                signature_date=unit["signature_date"],
                period_of_report=unit["period_of_report"],
                manifest_filed_at=unit["manifest_filed_at"],
            )
            for g in groups:
                if g.shares is None or g.period_end is None or (since is not None and g.period_end < since):
                    continue
                yield Form4Observation(
                    instrument_id=key[0],
                    accession_number=key[1],
                    holder_cik=key[2],
                    holder_name=str(held[0].row["filer_name"] or "").strip(),
                    ownership_nature=FORM4_HOLDING_NATURE[g.direct_indirect],
                    source_url=str(unit["primary_document_url"]) if unit["primary_document_url"] else None,
                    sec_filed_at=unit["manifest_filed_at"],
                    period_end=g.period_end,
                    shares=g.shares,
                    latest_line_shares=g.shares,
                    verdict=g.verdict,
                    kind="holding",
                )
        for key, lines in by_unit.items():
            unit = units.get(key)
            if unit is None or not unit["from_txn"]:
                continue
            latest: dict[str | None, _StoredForm4Line] = {}
            for ln in lines:
                if ln.post_transaction_shares is None or ln.txn_date_invalid:
                    continue
                prior = latest.get(ln.direct_indirect)
                # ``txn_date`` is NOT NULL (sql/056_insider_transactions.sql, never relaxed), so this
                # tuple order is the old ``txn_date DESC NULLS LAST, txn_row_num DESC`` exactly.
                if prior is None or (ln.txn_date, ln.txn_row_num) > (prior.txn_date, prior.txn_row_num):
                    latest[ln.direct_indirect] = ln
            holder_cik = (key[2] or "").strip() or None
            for direct_indirect, ln in latest.items():
                assert ln.post_transaction_shares is not None
                shares, verdict = ln.post_transaction_shares, None
                if direct_indirect == "I" and unit["document_type"] in FORM4_VEHICLE_SUM_DOCUMENT_TYPES:
                    verdict, vehicle_sum = form4_observation_shares(
                        lines,
                        document_type=unit["document_type"],
                        filer_cik=holder_cik if key[3] is None else None,
                        owner_ciks=owners.get(key[1], []),
                        earlier_balances=partial(
                            form4_earlier_balances,
                            conn,
                            instrument_id=key[0],
                            accession_number=key[1],
                            filer_cik=holder_cik,
                        ),
                    )
                    if verdict == FORM4_VEHICLE_SUMS and vehicle_sum is not None:
                        shares = vehicle_sum
                fold_verdict: str | None = None
                if direct_indirect in ("D", "I"):
                    fold_verdict, folded = form4_fold_shares(
                        lines,
                        holdings_by_unit.get(key, []),
                        direct_indirect,
                        document_type=unit["document_type"],
                        filer_cik=holder_cik if key[3] is None else None,
                        owner_ciks=owners.get(key[1], []),
                        earlier_balances=partial(
                            form4_earlier_balances,
                            conn,
                            instrument_id=key[0],
                            accession_number=key[1],
                            filer_cik=holder_cik,
                        ),
                    )
                    if fold_verdict == FORM4_VEHICLE_SUMS and folded is not None:
                        shares = folded
                yield Form4Observation(
                    instrument_id=key[0],
                    accession_number=key[1],
                    holder_cik=holder_cik,
                    holder_name=str(ln.row["filer_name"] or "").strip(),
                    ownership_nature="indirect" if direct_indirect == "I" else "direct",
                    source_url=str(unit["primary_document_url"]) if unit["primary_document_url"] else None,
                    sec_filed_at=unit["sec_filed_at"],
                    period_end=ln.txn_date,
                    shares=shares,
                    latest_line_shares=ln.post_transaction_shares,
                    verdict=verdict,
                    fold_verdict=fold_verdict,
                )


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

    # Form 4 — one observation per (filer, D/I) per accession, derived by
    # ``derive_form4_observations`` (retention gates, units, #3227 vehicle sum).
    for obs in derive_form4_observations(conn, since=since, limit=limit):
        summary.rows_scanned += 1
        if not obs.holder_name and obs.holder_cik is None:
            summary.orphans.append(f"insider_transactions accession={obs.accession_number} (no cik or name)")
            continue
        try:
            record_insider_observation(
                conn,
                instrument_id=obs.instrument_id,
                holder_cik=obs.holder_cik,
                holder_name=obs.holder_name or (obs.holder_cik or "UNKNOWN"),
                ownership_nature=obs.ownership_nature,  # type: ignore[arg-type]
                source="form4",
                source_document_id=obs.accession_number,
                source_accession=obs.accession_number,
                source_field=None,
                source_url=obs.source_url,
                # #899 — SEC filing timestamp (manifest → filing_events);
                # txn_date fallback only for rows with neither.
                filed_at=obs.sec_filed_at
                if obs.sec_filed_at is not None
                else datetime.combine(obs.period_end, datetime.min.time(), tzinfo=UTC),
                period_start=None,
                period_end=obs.period_end,
                ingest_run_id=run_id,
                shares=obs.shares,
            )
            summary.observations_recorded += 1
            instruments_touched.add(obs.instrument_id)
        except Exception as exc:
            summary.orphans.append(f"insider_transactions accession={obs.accession_number}: {exc}")

    params: dict[str, Any] = {}
    if since is not None:
        params["since"] = since
    if limit is not None:
        params["lim"] = limit
    limit_sql = "LIMIT %(lim)s" if limit is not None else ""

    # Form 3 — initial holdings baseline. Direct vs indirect tag lives
    # on insider_initial_holdings.direct_indirect ('D'/'I'). Map 'I'
    # to ``ownership_nature='indirect'``.
    where = "WHERE iih.shares IS NOT NULL AND iih.is_derivative = FALSE"
    if since is not None:
        where += " AND iih.as_of_date >= %(since)s"

    # ``owner_count`` uses set semantics, like the live writer's ``len({f.filer_cik for f in
    # parsed.filers})``: a DISTINCT subselect counts a NULL as one value, where ``count(DISTINCT
    # col)`` would skip it. ``insider_filers.filer_cik`` is NOT NULL today (sql/057); this keeps the
    # two writers equal without relying on that.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT iih.instrument_id, iih.accession_number, iih.row_num,
                   iih.filer_cik, iih.filer_name, iih.is_derivative,
                   iih.security_title,
                   iih.shares, iih.as_of_date, iih.direct_indirect,
                   (SELECT count(*) FROM (SELECT DISTINCT x.filer_cik FROM insider_filers x
                     WHERE x.accession_number = iih.accession_number) d) AS owner_count,
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
            ORDER BY iih.instrument_id, iih.accession_number, iih.row_num
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    # #3227 — one observation per (filer, D/I) per accession, its shares from the SAME rule the
    # live XML writer applies (``form3_observation_shares``). Iterating the lines one by one let the
    # ON CONFLICT upsert keep whichever line came last in an unordered scan, and this weekly job
    # then overwrote the live writer's value with it.
    from app.services.insider_form3_ingest import form3_observation_lines, form3_observation_shares

    # ``limit`` caps ROWS, so a capped scan can end part-way through an accession, and a partial
    # group would write a partial sum. Drop the trailing accession whenever the cap was reached; it
    # is the only one that can be cut, and an uncapped run (the scheduled one) never trims.
    summary.rows_scanned += len(rows)  # counted before the trim: every fetched row was scanned
    if limit is not None and rows and len(rows) >= limit:
        tail = (rows[-1]["instrument_id"], rows[-1]["accession_number"])
        rows = [r for r in rows if (r["instrument_id"], r["accession_number"]) != tail]

    lines = [_StoredForm3Line.from_row(row) for row in rows]
    for _, acc_lines in groupby(lines, key=lambda line: (line.row["instrument_id"], line.row["accession_number"])):
        group = list(acc_lines)
        shares_by_key = form3_observation_shares(group, owner_count=int(group[0].row["owner_count"]))
        # Same filter as the helper, so the metadata row is the line whose shares it would keep. The
        # SELECT already drops derivative and shareless rows; this keeps the pairing true by construction.
        last_by_key = {(line.filer_cik, line.direct_indirect): line.row for line in form3_observation_lines(group)}
        for key, shares in shares_by_key.items():
            row = last_by_key[key]
            cik = key[0]
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
                    shares=shares,
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
