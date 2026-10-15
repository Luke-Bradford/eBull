"""Reads one effective matching run for the tax view (#3616 slice 3d-4a, Designs 1 and 6).

Every read is keyed by the run id ``resolve_tax_run`` returns, inside the caller's REPEATABLE READ snapshot. A sealed
run's rows cannot change (the sql/489 and sql/490 seal triggers), so the rows agree with the selection. Nothing here
writes, commits or changes the connection's isolation level.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.tax_matching import TaxRunResolution, require_snapshot, resolve_tax_run
from app.services.tax_year_view import (
    EvidenceSubject,
    FundDisposal,
    FundSecurity,
    RunRows,
    ShareDisposal,
    WithheldSecurity,
)


@dataclass(frozen=True)
class Identification:
    seq: int
    rule: str
    acquisition_uk_date: date | None
    acquisition_lot_keys: tuple[str, ...]
    units: Decimal
    cost_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ShareDisposalDetail:
    row: ShareDisposal
    lot_keys: tuple[str, ...]
    units: Decimal
    proceeds_gbp: Decimal | None
    cost_gbp: Decimal | None
    identifications: tuple[Identification, ...]


@dataclass(frozen=True)
class FundDisposalDetail:
    row: FundDisposal
    executed_at: datetime
    units: Decimal
    proceeds_gbp: Decimal | None
    cost_gbp: Decimal | None
    computed_gain_gbp: Decimal | None
    identifications: tuple[Identification, ...]


@dataclass(frozen=True)
class FundBalance:
    lot_key: str
    units_left: Decimal
    cost_left_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class Holding:
    tax_security_id: int
    symbols: tuple[str, ...]
    holding_basis: str
    units: Decimal | None
    cost_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]
    window_open_since: date | None
    fund_balances: tuple[FundBalance, ...]


@dataclass(frozen=True)
class TaxRunData:
    matching_run_id: int
    evidence_run_id: int
    matching_policy_id: str
    class_policy_id: str
    character_policy_id: str | None
    rows: RunRows
    symbols: dict[int, tuple[str, ...]]
    share_details: tuple[ShareDisposalDetail, ...]
    fund_details: tuple[FundDisposalDetail, ...]
    holdings: tuple[Holding, ...]


def _t(values: Any) -> tuple[str, ...]:
    return tuple(values or ())


Cursor = psycopg.Cursor[dict[str, Any]]
Params = dict[str, int]


def _symbols(cur: Cursor, params: Params) -> dict[int, tuple[str, ...]]:
    """Through ``instrument_ids``: an unestablished identity stores no ``tax_security_id`` on its instrument row."""
    rows = cur.execute(
        """
        SELECT s.tax_security_id, array_agg(DISTINCT i.symbol ORDER BY i.symbol) AS symbols
        FROM tax_matching_securities s
        JOIN tax_matching_instruments i
          ON i.matching_run_id = s.matching_run_id AND i.instrument_id = ANY (s.instrument_ids)
        WHERE s.matching_run_id = %(run)s AND i.symbol IS NOT NULL
        GROUP BY s.tax_security_id
        """,
        params,
    ).fetchall()
    return {r["tax_security_id"]: tuple(r["symbols"]) for r in rows}


def _withheld(cur: Cursor, params: Params, symbols: dict[int, tuple[str, ...]]) -> tuple[WithheldSecurity, ...]:
    """Each withheld security with its own dependency reasons and the run-level ones: an ``unplaceable`` dependency
    has no security and withholds every security (``withheld_all``)."""
    own: dict[int, set[str]] = {}
    run_level: set[str] = set()
    for r in cur.execute(
        "SELECT DISTINCT tax_security_id, reason FROM tax_matching_dependencies WHERE matching_run_id = %(run)s",
        params,
    ).fetchall():
        if r["tax_security_id"] is None:
            run_level.add(r["reason"])
        else:
            own.setdefault(r["tax_security_id"], set()).add(r["reason"])
    ids = [
        r["tax_security_id"]
        for r in cur.execute(
            "SELECT tax_security_id FROM tax_matching_securities "
            "WHERE matching_run_id = %(run)s AND status = 'withheld' ORDER BY tax_security_id",
            params,
        ).fetchall()
    ]
    return tuple(
        WithheldSecurity(sid, symbols.get(sid, ()), tuple(sorted(own.get(sid, set()) | run_level))) for sid in ids
    )


def _fund_securities(cur: Cursor, params: Params, symbols: dict[int, tuple[str, ...]]) -> tuple[FundSecurity, ...]:
    rows = cur.execute(
        "SELECT tax_security_id FROM tax_matching_characters "
        "WHERE matching_run_id = %(run)s AND character = 'non_reporting_fund' ORDER BY tax_security_id",
        params,
    ).fetchall()
    return tuple(FundSecurity(r["tax_security_id"], symbols.get(r["tax_security_id"], ())) for r in rows)


def _identification(r: dict[str, Any], lot_keys: tuple[str, ...]) -> Identification:
    return Identification(
        r["seq"],
        r["rule"],
        r["acquisition_uk_date"],
        lot_keys,
        r["units"],
        r["cost_gbp"],
        _t(r["missing_lot_keys"]),
        _t(r["reasons"]),
    )


def _share_details(cur: Cursor, params: Params) -> tuple[ShareDisposalDetail, ...]:
    identifications: dict[tuple[int, date], list[Identification]] = {}
    for r in cur.execute(
        """
        SELECT tax_security_id, disposal_uk_date, seq, rule, acquisition_uk_date, acquisition_lot_keys, units,
               cost_gbp, missing_lot_keys, reasons
        FROM tax_identifications WHERE matching_run_id = %(run)s
        ORDER BY tax_security_id, disposal_uk_date, seq
        """,
        params,
    ).fetchall():
        key = (r["tax_security_id"], r["disposal_uk_date"])
        identifications.setdefault(key, []).append(_identification(r, _t(r["acquisition_lot_keys"])))
    rows = cur.execute(
        """
        SELECT tax_security_id, uk_date, tax_year, lot_keys, units, proceeds_gbp, cost_gbp, gain_gbp, status,
               missing_lot_keys, reasons
        FROM tax_statutory_disposals WHERE matching_run_id = %(run)s ORDER BY uk_date, tax_security_id
        """,
        params,
    ).fetchall()
    return tuple(
        ShareDisposalDetail(
            ShareDisposal(
                r["tax_security_id"],
                r["uk_date"],
                r["tax_year"],
                r["status"],
                r["gain_gbp"],
                _t(r["missing_lot_keys"]),
                _t(r["reasons"]),
            ),
            _t(r["lot_keys"]),
            r["units"],
            r["proceeds_gbp"],
            r["cost_gbp"],
            tuple(identifications.get((r["tax_security_id"], r["uk_date"]), ())),
        )
        for r in rows
    )


def _fund_details(cur: Cursor, params: Params) -> tuple[FundDisposalDetail, ...]:
    identifications: dict[tuple[int, str], list[Identification]] = {}
    for r in cur.execute(
        """
        SELECT tax_security_id, disposal_lot_key, seq, rule, acquisition_uk_date, acquisition_lot_key, units,
               cost_gbp, missing_lot_keys, reasons
        FROM tax_fund_identifications WHERE matching_run_id = %(run)s
        ORDER BY tax_security_id, disposal_lot_key, seq
        """,
        params,
    ).fetchall():
        key = (r["tax_security_id"], r["disposal_lot_key"])
        identifications.setdefault(key, []).append(_identification(r, (r["acquisition_lot_key"],)))
    rows = cur.execute(
        """
        SELECT tax_security_id, lot_key, seq, executed_at, uk_date, tax_year, units, proceeds_gbp, cost_gbp,
               computed_gain_gbp, basic_gain_gbp, offshore_income_gain_gbp, chargeable_gain_gbp, status,
               gain_character, missing_lot_keys, reasons
        FROM tax_fund_disposals WHERE matching_run_id = %(run)s ORDER BY uk_date, tax_security_id, seq
        """,
        params,
    ).fetchall()
    return tuple(
        FundDisposalDetail(
            FundDisposal(
                r["tax_security_id"],
                r["seq"],
                r["lot_key"],
                r["uk_date"],
                r["tax_year"],
                r["status"],
                r["gain_character"],
                r["basic_gain_gbp"],
                r["offshore_income_gain_gbp"],
                r["chargeable_gain_gbp"],
                _t(r["missing_lot_keys"]),
                _t(r["reasons"]),
            ),
            r["executed_at"],
            r["units"],
            r["proceeds_gbp"],
            r["cost_gbp"],
            r["computed_gain_gbp"],
            tuple(identifications.get((r["tax_security_id"], r["lot_key"]), ())),
        )
        for r in rows
    )


def _holdings(cur: Cursor, params: Params, symbols: dict[int, tuple[str, ...]]) -> tuple[Holding, ...]:
    balances: dict[int, list[FundBalance]] = {}
    for r in cur.execute(
        """
        SELECT tax_security_id, lot_key, units_left, cost_left_gbp, missing_lot_keys, reasons
        FROM tax_fund_acquisition_balances WHERE matching_run_id = %(run)s ORDER BY tax_security_id, lot_key
        """,
        params,
    ).fetchall():
        balances.setdefault(r["tax_security_id"], []).append(
            FundBalance(r["lot_key"], r["units_left"], r["cost_left_gbp"], _t(r["missing_lot_keys"]), _t(r["reasons"]))
        )
    rows = cur.execute(
        """
        SELECT tax_security_id, holding_basis, pool_units, pool_cost_gbp, pool_missing_lot_keys, pool_reasons,
               window_open_since
        FROM tax_matching_securities
        WHERE matching_run_id = %(run)s AND status = 'computed' ORDER BY tax_security_id
        """,
        params,
    ).fetchall()
    return tuple(
        Holding(
            r["tax_security_id"],
            symbols.get(r["tax_security_id"], ()),
            r["holding_basis"],
            r["pool_units"],
            r["pool_cost_gbp"],
            _t(r["pool_missing_lot_keys"]),
            _t(r["pool_reasons"]),
            r["window_open_since"],
            tuple(balances.get(r["tax_security_id"], ())),
        )
        for r in rows
    )


def _subjects(cur: Cursor, params: Params) -> tuple[EvidenceSubject, ...]:
    rows = cur.execute(
        """
        SELECT subject, outcome, product, reasons,
               (first_event_at AT TIME ZONE 'Europe/London')::date AS first_uk_date,
               (last_event_at AT TIME ZONE 'Europe/London')::date AS last_uk_date
        FROM tax_evidence_subjects
        WHERE run_id = %(evidence)s AND outcome IN ('unresolved', 'excluded_non_share_product')
        ORDER BY subject
        """,
        params,
    ).fetchall()
    return tuple(
        EvidenceSubject(
            r["subject"], r["outcome"], r["product"], _t(r["reasons"]), r["first_uk_date"], r["last_uk_date"]
        )
        for r in rows
    )


def read_tax_run(conn: psycopg.Connection[Any], resolution: TaxRunResolution) -> TaxRunData | None:
    """The resolved run's rows, or None when the resolution names no run."""
    require_snapshot(conn)
    if resolution.run is None:
        return None
    run_id = resolution.run.matching_run_id
    params: Params = {"run": run_id, "evidence": resolution.run.evidence_run_id}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        meta = cur.execute(
            "SELECT matching_policy_id, class_policy_id, character_policy_id FROM tax_matching_runs "
            "WHERE matching_run_id = %(run)s",
            params,
        ).fetchone()
        if meta is None:
            raise RuntimeError(f"tax matching run {run_id} vanished inside the snapshot")
        symbols = _symbols(cur, params)
        share_details = _share_details(cur, params)
        fund_details = _fund_details(cur, params)
        rows = RunRows(
            share_disposals=tuple(d.row for d in share_details),
            fund_disposals=tuple(d.row for d in fund_details),
            withheld=_withheld(cur, params, symbols),
            subjects=_subjects(cur, params),
            non_reporting_funds=_fund_securities(cur, params, symbols),
        )
        holdings = _holdings(cur, params, symbols)
    return TaxRunData(
        matching_run_id=run_id,
        evidence_run_id=resolution.run.evidence_run_id,
        matching_policy_id=meta["matching_policy_id"],
        class_policy_id=meta["class_policy_id"],
        character_policy_id=meta["character_policy_id"],
        rows=rows,
        symbols=symbols,
        share_details=share_details,
        fund_details=fund_details,
        holdings=holdings,
    )


def resolve_and_read(conn: psycopg.Connection[Any]) -> tuple[TaxRunResolution, TaxRunData | None]:
    """Resolve the effective run and read it, in the caller's snapshot."""
    resolution = resolve_tax_run(conn)
    return resolution, read_tax_run(conn, resolution)
