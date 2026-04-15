"""
Budget service — types, config, and capital events.

Pure service module that computes budget state on the fly from DB rows.
No caching, no singletons in memory — every call reads current state.

Capital events (``capital_events`` table):
  ``amount`` is always positive; ``event_type`` carries the directional
  semantics (injection = cash in, withdrawal = cash out, tax_provision =
  reserved for CGT, tax_release = released back from CGT reserve).

Budget config (``budget_config`` table):
  Singleton row (``id = TRUE``, same pattern as ``runtime_config``) holding
  operator-level preferences: ``cash_buffer_pct`` and ``cgt_scenario``.
  Every mutation writes one ``budget_config_audit`` row per changed field
  inside the same transaction as the UPDATE.

Issue: #203
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg import sql

from app.services.tax_ledger import ANNUAL_EXEMPT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

CapitalEventType = Literal["injection", "withdrawal", "tax_provision", "tax_release"]
CapitalEventSource = Literal["operator", "system", "broker_sync"]
CgtScenario = Literal["basic", "higher"]

_ZERO = Decimal("0")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class BudgetConfigCorrupt(RuntimeError):
    """Raised when the budget_config singleton row is missing.

    Callers on safety-critical paths must catch this and fail closed —
    never default to permissive budget assumptions.
    """


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetConfig:
    cash_buffer_pct: Decimal
    cgt_scenario: str
    updated_at: datetime
    updated_by: str
    reason: str


@dataclass(frozen=True)
class BudgetState:
    cash_balance: Decimal | None
    deployed_capital: Decimal
    mirror_equity: Decimal
    working_budget: Decimal | None
    estimated_tax_gbp: Decimal
    estimated_tax_usd: Decimal
    gbp_usd_rate: Decimal | None
    cash_buffer_reserve: Decimal
    available_for_deployment: Decimal | None
    cash_buffer_pct: Decimal
    cgt_scenario: str
    tax_year: str


@dataclass(frozen=True)
class CapitalEvent:
    event_id: int
    event_time: datetime
    event_type: str
    amount: Decimal
    currency: str
    source: str
    note: str | None
    created_by: str | None


# ---------------------------------------------------------------------------
# Budget config queries
# ---------------------------------------------------------------------------


def get_budget_config(conn: psycopg.Connection[Any]) -> BudgetConfig:
    """Load the singleton budget_config row.

    Raises BudgetConfigCorrupt if the row is missing — every caller on a
    safety-critical path must treat this as fail-closed.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cash_buffer_pct,
                   cgt_scenario,
                   updated_at,
                   updated_by,
                   reason
            FROM budget_config
            WHERE id = TRUE
            """
        )
        row = cur.fetchone()

    if row is None:
        raise BudgetConfigCorrupt("budget_config singleton row missing — configuration corrupt")

    return BudgetConfig(
        cash_buffer_pct=Decimal(str(row["cash_buffer_pct"])),
        cgt_scenario=str(row["cgt_scenario"]),
        updated_at=row["updated_at"],
        updated_by=str(row["updated_by"]),
        reason=str(row["reason"]),
    )


def update_budget_config(
    conn: psycopg.Connection[Any],
    *,
    cash_buffer_pct: Decimal | None = None,
    cgt_scenario: str | None = None,
    updated_by: str,
    reason: str,
) -> BudgetConfig:
    """Atomically update the budget_config singleton.

    Only fields passed as non-None are changed (partial update semantics).
    Writes one audit row per changed field, in the same transaction as the
    UPDATE.  The pre-update row is read inside the transaction so the audit
    ``old_value`` cannot race a concurrent writer.

    This function accepts a caller connection.  It must NOT call
    ``conn.commit()`` — only uses ``conn.transaction()`` for savepoints.
    The caller owns the commit.

    Raises ValueError if no fields are provided or if provided values
    match the current row (no-op patch).
    Raises BudgetConfigCorrupt if the singleton row is missing.
    """
    if cash_buffer_pct is None and cgt_scenario is None:
        raise ValueError("at least one of cash_buffer_pct or cgt_scenario must be provided")

    # prevention: audit reads outside the write transaction
    # prevention: read-then-write cap enforcement outside transaction
    # All reads + UPDATE + audit writes happen inside one conn.transaction() block.
    with conn.transaction():
        # Read current values inside the transaction.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT cash_buffer_pct,
                       cgt_scenario,
                       updated_at,
                       updated_by,
                       reason
                FROM budget_config
                WHERE id = TRUE
                FOR UPDATE
                """
            )
            current = cur.fetchone()

        if current is None:
            raise BudgetConfigCorrupt("budget_config singleton row missing — cannot update; configuration corrupt")

        old_buffer = Decimal(str(current["cash_buffer_pct"]))
        old_scenario = str(current["cgt_scenario"])

        # Track which fields actually changed.
        changes: dict[str, tuple[str, str]] = {}
        if cash_buffer_pct is not None and cash_buffer_pct != old_buffer:
            changes["cash_buffer_pct"] = (str(old_buffer), str(cash_buffer_pct))
        if cgt_scenario is not None and cgt_scenario != old_scenario:
            changes["cgt_scenario"] = (old_scenario, cgt_scenario)

        if not changes:
            raise ValueError("no fields changed")

        # Build dynamic SET clause using psycopg.sql for type-safe composition.
        # Column names come from a fixed set in code (never user input).
        set_parts: list[sql.Composable] = []
        params: dict[str, Any] = {
            "by": updated_by,
            "reason": reason,
        }
        if "cash_buffer_pct" in changes:
            set_parts.append(sql.SQL("cash_buffer_pct = {buffer}").format(buffer=sql.Placeholder("buffer")))
            params["buffer"] = cash_buffer_pct
        if "cgt_scenario" in changes:
            set_parts.append(sql.SQL("cgt_scenario = {scenario}").format(scenario=sql.Placeholder("scenario")))
            params["scenario"] = cgt_scenario

        set_parts.append(sql.SQL("updated_at = NOW()"))
        set_parts.append(sql.SQL("updated_by = {by}").format(by=sql.Placeholder("by")))
        set_parts.append(sql.SQL("reason = {reason}").format(reason=sql.Placeholder("reason")))

        query = sql.SQL(
            "UPDATE budget_config SET {sets} WHERE id = TRUE"
            " RETURNING cash_buffer_pct, cgt_scenario, updated_at, updated_by, reason"
        ).format(sets=sql.SQL(", ").join(set_parts))

        with conn.cursor(row_factory=psycopg.rows.dict_row) as upd_cur:
            result = upd_cur.execute(query, params)
            # prevention: single-row UPDATE silent no-op on missing row
            if result.rowcount == 0:
                raise BudgetConfigCorrupt("budget_config UPDATE affected 0 rows — singleton vanished")
            updated_row = upd_cur.fetchone()

        if updated_row is None:
            raise RuntimeError("RETURNING produced no row despite rowcount > 0")

        # Write one audit row per changed field.
        for field, (old_val, new_val) in changes.items():
            conn.execute(
                """
                INSERT INTO budget_config_audit
                    (changed_at, changed_by, field, old_value, new_value, reason)
                VALUES
                    (NOW(), %(by)s, %(field)s, %(old)s, %(new)s, %(reason)s)
                """,
                {
                    "by": updated_by,
                    "field": field,
                    "old": old_val,
                    "new": new_val,
                    "reason": reason,
                },
            )

    logger.info(
        "budget_config updated by=%s reason=%s changes=%s",
        updated_by,
        reason,
        list(changes.keys()),
    )

    return BudgetConfig(
        cash_buffer_pct=Decimal(str(updated_row["cash_buffer_pct"])),
        cgt_scenario=str(updated_row["cgt_scenario"]),
        updated_at=updated_row["updated_at"],
        updated_by=str(updated_row["updated_by"]),
        reason=str(updated_row["reason"]),
    )


# ---------------------------------------------------------------------------
# Capital event queries
# ---------------------------------------------------------------------------


def record_capital_event(
    conn: psycopg.Connection[Any],
    *,
    event_type: CapitalEventType,
    amount: Decimal,
    currency: str,
    source: CapitalEventSource,
    note: str | None,
    created_by: str | None,
) -> CapitalEvent:
    """Insert a capital event and return the persisted row.

    ``amount`` must be positive — the sign is carried by ``event_type``.

    Raises ValueError if amount <= 0.
    Raises RuntimeError if RETURNING produces no row (invariant violation).
    """
    if amount <= _ZERO:
        raise ValueError("amount must be positive")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO capital_events
                (event_type, amount, currency, source, note, created_by)
            VALUES
                (%(type)s, %(amount)s, %(currency)s, %(source)s, %(note)s, %(by)s)
            RETURNING event_id, event_time, event_type, amount, currency,
                      source, note, created_by
            """,
            {
                "type": event_type,
                "amount": amount,
                "currency": currency,
                "source": source,
                "note": note,
                "by": created_by,
            },
        )
        row = cur.fetchone()

    # prevention: assert as runtime guard
    if row is None:
        raise RuntimeError("INSERT INTO capital_events RETURNING produced no row")

    return CapitalEvent(
        event_id=int(row["event_id"]),
        event_time=row["event_time"],
        event_type=str(row["event_type"]),
        amount=Decimal(str(row["amount"])),
        currency=str(row["currency"]),
        source=str(row["source"]),
        note=row["note"],
        created_by=row["created_by"],
    )


def list_capital_events(
    conn: psycopg.Connection[Any],
    *,
    limit: int = 50,
    offset: int = 0,
) -> list[CapitalEvent]:
    """Return capital events ordered by event_time descending.

    Supports pagination via limit/offset.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT event_id, event_time, event_type, amount, currency,
                   source, note, created_by
            FROM capital_events
            ORDER BY event_time DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": limit, "offset": offset},
        )
        rows = cur.fetchall()

    return [
        CapitalEvent(
            event_id=int(r["event_id"]),
            event_time=r["event_time"],
            event_type=str(r["event_type"]),
            amount=Decimal(str(r["amount"])),
            currency=str(r["currency"]),
            source=str(r["source"]),
            note=r["note"],
            created_by=r["created_by"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Budget state computation
# ---------------------------------------------------------------------------


def _current_uk_tax_year() -> str:
    """Return the current UK tax year as ``"YYYY/YY"``.

    The UK tax year runs 6 April to 5 April.
    E.g. on 2026-04-15 the tax year is ``"2026/27"`` (past 6 April);
    on 2025-04-05 the tax year is ``"2024/25"`` (on or before 5 April).
    """
    now = datetime.now(tz=UTC)
    # Tax year starts on 6 April. Dates up to and including 5 April
    # belong to the tax year that started the previous calendar year.
    if now.month < 4 or (now.month == 4 and now.day <= 5):
        start_year = now.year - 1
    else:
        start_year = now.year
    end_year_short = str(start_year + 1)[-2:]
    return f"{start_year}/{end_year_short}"


def _load_cash_balance(conn: psycopg.Connection[Any]) -> Decimal | None:
    """Load total cash balance from ``cash_ledger``.

    Aggregate always returns one row; the column is NULL when the table
    is empty (prevention: dead-code None-guard on aggregate fetchone).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT SUM(amount) AS balance FROM cash_ledger")
        row = cur.fetchone()

    # prevention: aggregate fetchone always returns a row; None-check is
    # on the column value, not the row itself.
    if row is None or row["balance"] is None:
        return None
    return Decimal(str(row["balance"]))


def _load_deployed_capital(conn: psycopg.Connection[Any]) -> Decimal:
    """Load total deployed capital from open positions.

    CRITICAL: ``WHERE current_units > 0`` — prevention log says
    zero-unit positions inflate AUM via cost_basis fallback.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(cost_basis), 0) AS deployed
            FROM positions
            WHERE current_units > 0
            """
        )
        row = cur.fetchone()

    # Aggregate with COALESCE always returns a non-None value.
    if row is None:
        return _ZERO  # defensive; should never happen
    return Decimal(str(row["deployed"]))


def _load_mirror_equity(conn: psycopg.Connection[Any]) -> Decimal:
    """Load total equity held in active copy mirrors.

    Mirror equity = active mirrors' available_amount + their positions'
    current_value.  Returns ``Decimal("0")`` if no mirrors exist.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(
                (SELECT SUM(cm.available_amount)
                 FROM copy_mirrors cm
                 WHERE cm.status = 'active')
                +
                (SELECT COALESCE(SUM(cmp.current_value), 0)
                 FROM copy_mirror_positions cmp
                 JOIN copy_mirrors cm2 ON cm2.mirror_id = cmp.mirror_id
                 WHERE cm2.status = 'active'),
                0
            ) AS mirror_equity
            """
        )
        row = cur.fetchone()

    if row is None:
        return _ZERO  # defensive; should never happen
    return Decimal(str(row["mirror_equity"]))


def _load_tax_estimates(
    conn: psycopg.Connection[Any],
    tax_year: str,
) -> tuple[Decimal, Decimal]:
    """Return ``(basic_estimate_gbp, higher_estimate_gbp)`` for ``tax_year``.

    Reads ``disposal_matches`` and applies current-year CGT rates
    (basic=18%, higher=24%) after the annual exempt amount.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN gain_or_loss_gbp > 0
                                  THEN gain_or_loss_gbp ELSE 0 END), 0) AS total_gains,
                COALESCE(SUM(gain_or_loss_gbp), 0) AS net_gain
            FROM disposal_matches
            WHERE tax_year = %(ty)s
            """,
            {"ty": tax_year},
        )
        row = cur.fetchone()

    if row is None:
        return (_ZERO, _ZERO)  # defensive

    total_gains = Decimal(str(row["total_gains"]))
    net_gain = Decimal(str(row["net_gain"]))

    taxable_net = max(net_gain - ANNUAL_EXEMPT, _ZERO)
    if total_gains <= _ZERO or taxable_net <= _ZERO:
        return (_ZERO, _ZERO)

    basic_rate = Decimal("0.18")
    higher_rate = Decimal("0.24")

    # scale = proportion of total gains that is taxable after exempt amount
    # units: taxable_net (GBP) / total_gains (GBP) = dimensionless ratio
    scale = taxable_net / total_gains
    _TWO_DP = Decimal("0.01")
    basic_est = (total_gains * basic_rate * scale).quantize(_TWO_DP)
    higher_est = (total_gains * higher_rate * scale).quantize(_TWO_DP)
    return (basic_est, higher_est)


def _load_gbp_usd_rate(conn: psycopg.Connection[Any]) -> Decimal | None:
    """Load the GBP -> USD exchange rate from ``live_fx_rates``.

    Returns None if no rate is found.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT rate
            FROM live_fx_rates
            WHERE from_currency = 'GBP' AND to_currency = 'USD'
            """
        )
        row = cur.fetchone()

    if row is None:
        return None
    return Decimal(str(row["rate"]))


def compute_budget_state(conn: psycopg.Connection[Any]) -> BudgetState:
    """Compute a full budget state snapshot from current DB state.

    Reads from budget_config, cash_ledger, positions, copy_mirrors,
    copy_mirror_positions, disposal_matches, and live_fx_rates.
    """
    config = get_budget_config(conn)
    tax_year = _current_uk_tax_year()

    cash_balance = _load_cash_balance(conn)
    deployed_capital = _load_deployed_capital(conn)
    mirror_equity = _load_mirror_equity(conn)

    # working_budget = cash + deployed + mirrors (None if cash is unknown)
    if cash_balance is not None:
        working_budget = cash_balance + deployed_capital + mirror_equity
    else:
        working_budget = None

    # Tax estimates
    basic_est, higher_est = _load_tax_estimates(conn, tax_year)
    if config.cgt_scenario == "basic":
        estimated_tax_gbp = basic_est
    else:
        estimated_tax_gbp = higher_est

    # FX conversion
    gbp_usd_rate = _load_gbp_usd_rate(conn)
    if gbp_usd_rate is not None:
        estimated_tax_usd = estimated_tax_gbp * gbp_usd_rate
    else:
        if estimated_tax_gbp > _ZERO:
            logger.warning(
                "No GBP->USD rate available; using 0 for tax_usd (tax_gbp=%s)",
                estimated_tax_gbp,
            )
        estimated_tax_usd = _ZERO

    # Cash buffer reserve
    if working_budget is not None:
        cash_buffer_reserve = working_budget * config.cash_buffer_pct
    else:
        cash_buffer_reserve = _ZERO

    # Available for deployment
    if cash_balance is not None:
        available_for_deployment = cash_balance - estimated_tax_usd - cash_buffer_reserve
    else:
        available_for_deployment = None

    return BudgetState(
        cash_balance=cash_balance,
        deployed_capital=deployed_capital,
        mirror_equity=mirror_equity,
        working_budget=working_budget,
        estimated_tax_gbp=estimated_tax_gbp,
        estimated_tax_usd=estimated_tax_usd,
        gbp_usd_rate=gbp_usd_rate,
        cash_buffer_reserve=cash_buffer_reserve,
        available_for_deployment=available_for_deployment,
        cash_buffer_pct=config.cash_buffer_pct,
        cgt_scenario=config.cgt_scenario,
        tax_year=tax_year,
    )
