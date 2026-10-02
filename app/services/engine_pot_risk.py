"""Drawdown of the engine pot -- its own book, never the demo account (#3541 slice 1).

Spec: ``docs/proposals/execution/2026-10-02-3541-engine-pot-drawdown.md``.

The demo account also holds the operator's own positions, copy mirrors and supervisor
experiments, so a high-water mark on account equity let an operator GME move pass or fail an
engine entry. The pot is measured here instead:

``NAV = principal + realised + unrealised`` over exact-owned positions of eligible trades --
the population ``strategy_wealth`` and the #3540 bridge already use.

Drawdown is the peak-to-trough decline of a time-weighted NAV index, sub-periods linked per
GIPS 2020 2.A.24(f). The NAV separates exactly into principal and P&L, so a sub-period's P&L is
exact; only WHEN a principal flow landed inside it is unobserved. The 2.A.24(e) flow-timing
policy here is the fail-closed bound: the smaller of the start- and end-of-span bases (see
``link``). Membership is point-in-time at the snapshot (claimed / released timestamps).

Slice 3 applies the same index to each paper deployment's own book (principal = its
``capital_limit`` in force), for the live gate's paper-period drawdown
(``docs/proposals/execution/2026-10-02-3541-deployment-nav-risk.md``).

⚠ Fees are a memo, as in #3540: ``totalFees`` inclusion in ``pnL`` is undocumented and every
stored value is 0, so they are not a term here either.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerDirectPositionInvestment
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.strategy_core_arc_sql import core_arm_joins, core_arm_present
from app.services.strategy_engine_capital import EngineCapitalObservationError

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")

PotRiskRefusal = Literal["engine_pot_risk_stale", "engine_pot_epoch_mismatch"]
"""Refusals of the risk STATE, as distinct from ``EngineCapitalRefusal`` (the observation)."""


@dataclass(frozen=True)
class PotNav:
    principal: Decimal
    epoch_started_at: datetime
    realised: Decimal
    unrealised: Decimal
    observed_at: datetime

    @property
    def pnl(self) -> Decimal:
        return self.realised + self.unrealised

    @property
    def nav(self) -> Decimal:
        return self.principal + self.pnl


@dataclass(frozen=True)
class PotRiskState:
    epoch_started_at: datetime
    nav_index: Decimal
    index_high_water: Decimal
    last_nav: Decimal
    last_pnl: Decimal
    last_drawdown_pct: Decimal
    observed_at: datetime


def link(state: PotRiskState | None, pot: PotNav) -> PotRiskState:
    """Link one observation onto the index. Pure.

    ``state`` is ``None`` for the first observation, which is the base: index 1, drawdown 0.
    """
    if state is None:
        return PotRiskState(pot.epoch_started_at, _ONE, _ONE, pot.nav, pot.pnl, _ZERO, pot.observed_at)
    # The P&L between two observations is exact, but WHEN a principal flow landed inside the
    # span is not observed, and the pot's return depends on it (withdrawing half before a
    # loss doubles the loss's percentage). The start-of-span and end-of-span conventions
    # bound it; the SMALLER base is taken -- larger on a loss, and on a gain it raises the
    # high water, so every later drawdown is measured from a higher peak. Fail-closed.
    flow = pot.principal - (state.last_nav - state.last_pnl)
    base = min(state.last_nav, state.last_nav + flow)
    if base <= 0:
        raise EngineCapitalObservationError(
            "engine pot NAV is not positive across a principal change", "engine_capital_population_incomplete"
        )
    index = state.nav_index * (_ONE + (pot.pnl - state.last_pnl) / base)
    if not index.is_finite() or index <= 0:
        raise EngineCapitalObservationError(
            "engine pot NAV index is not positive", "engine_capital_population_incomplete"
        )
    high_water = max(state.index_high_water, index)
    drawdown = (high_water - index) / high_water * _HUNDRED
    return PotRiskState(pot.epoch_started_at, index, high_water, pot.nav, pot.pnl, drawdown, pot.observed_at)


def _snapshot_positions(snapshot: BrokerAccountRiskSnapshot) -> dict[int, BrokerDirectPositionInvestment]:
    if snapshot.observed_at.tzinfo is None:
        raise EngineCapitalObservationError("broker snapshot time is naive", "engine_capital_snapshot_unusable")
    currency = (
        None
        if snapshot.account_currency_id is None
        else DOCUMENTED_ACCOUNT_CURRENCIES.get(snapshot.account_currency_id)
    )
    if currency != "USD":
        raise EngineCapitalObservationError(
            "broker account currency is not observed as USD", "engine_capital_snapshot_unusable"
        )
    positions = {row.position_id: row for row in snapshot.direct_positions}
    if len(positions) != len(snapshot.direct_positions):
        raise EngineCapitalObservationError(
            "broker snapshot repeats a direct position id", "engine_capital_snapshot_unusable"
        )
    return positions


def book_membership(rows: list[tuple[Any, ...]], at: datetime) -> tuple[dict[int, int], set[int]]:
    """``(held position -> instrument, released positions)`` of the book at ``at``.

    ``rows`` are ``engine_book_rows``'s. Raises for a trade that reached the broker with no
    ownership row.
    """
    # Membership is POINT-IN-TIME at the snapshot: a row claimed after it is not yet in the
    # book, a row released after it is still held in it. Every close counts only up to the
    # snapshot, so the book is priced as of one instant. `broker_position_id` is UNIQUE
    # (sql/281), so one position is never both held and released, nor held twice.
    held: dict[int, int] = {}
    released: set[int] = set()
    for trade_id, status, instrument_id, position_id, claimed_at, released_at in rows:
        if position_id is None:
            # The #3540 `trade_without_ownership` census, on this population: a trade that
            # reached the broker with no ownership row has P&L nobody can attribute.
            if status in ("open", "closing", "closed"):
                raise EngineCapitalObservationError(
                    f"strategy trade {trade_id} has no exact ownership", "engine_capital_population_incomplete"
                )
            continue
        if claimed_at > at:
            continue
        if released_at is not None and released_at <= at:
            released.add(int(position_id))
        else:
            held[int(position_id)] = int(instrument_id)
    return held, released


def _price_book(
    conn: psycopg.Connection[Any],
    positions: dict[int, BrokerDirectPositionInvestment],
    rows: list[tuple[Any, ...]],
    at: datetime,
) -> tuple[Decimal, Decimal]:
    """``(realised, unrealised)`` of an exact-owned book at ``at``.

    ``rows`` are ``(trade_id, status, instrument_id, position_id, claimed_at, released_at)``,
    one per ownership row (NULLs when a trade has none).
    """
    held, released = book_membership(rows, at)

    unrealised = _ZERO
    for position_id, instrument_id in held.items():
        row = positions.get(position_id)
        if row is None:
            raise EngineCapitalObservationError(
                f"owned position {position_id} is absent from broker snapshot",
                "engine_capital_ownership_unwitnessed",
            )
        if row.instrument_id != instrument_id:
            raise EngineCapitalObservationError(
                f"owned position {position_id} belongs to another instrument",
                "engine_capital_ownership_mismatched",
            )
        if not row.unrealized_pnl.is_finite():
            raise EngineCapitalObservationError(
                f"owned position {position_id} unrealised P&L is invalid",
                "engine_capital_ownership_mismatched",
            )
        unrealised += row.unrealized_pnl

    # A held position's partial closes are realised; a close after the snapshot is still
    # inside that position's unrealised.
    closes = conn.execute(
        """
        SELECT ids.position_id,
               count(event.event_id) AS close_count,
               count(event.event_id) FILTER (WHERE event.realized_pnl_usd IS NULL) AS missing_pnl,
               COALESCE(sum(event.realized_pnl_usd),0) AS realised
        FROM unnest(%s::bigint[]) AS ids(position_id)
        LEFT JOIN trade_events event
          ON event.position_id=ids.position_id AND event.event_kind='close' AND event.executed_at <= %s
        GROUP BY ids.position_id
        """,
        ([*held, *released], at),
    ).fetchall()
    realised = _ZERO
    for position_id, close_count, missing_pnl, amount in closes:
        if int(missing_pnl):
            raise EngineCapitalObservationError(
                f"owned position {position_id} has a close without realised P&L",
                "engine_capital_population_incomplete",
            )
        if int(close_count) == 0 and int(position_id) in released:
            raise EngineCapitalObservationError(
                f"released position {position_id} has no close event by the snapshot",
                "engine_capital_population_incomplete",
            )
        realised += Decimal(str(amount))
    return realised, unrealised


def observe_pot_nav(conn: psycopg.Connection[Any], snapshot: BrokerAccountRiskSnapshot) -> PotNav:
    """Read principal and the exact-owned population, priced by ``snapshot``.

    Runs in the caller's transaction. Raises ``EngineCapitalObservationError`` for every
    population or snapshot defect; nothing is coerced to zero.
    """
    positions = _snapshot_positions(snapshot)
    pool = conn.execute(
        """
        SELECT latest.capital_limit, first_event.changed_at
        FROM LATERAL (
            SELECT capital_limit FROM strategy_paper_pool_events
            ORDER BY strategy_paper_pool_event_id DESC LIMIT 1
        ) latest
        CROSS JOIN LATERAL (
            SELECT changed_at FROM strategy_paper_pool_events
            ORDER BY strategy_paper_pool_event_id LIMIT 1
        ) first_event
        """
    ).fetchone()
    if pool is None:
        raise EngineCapitalObservationError("no paper pool has been assigned", "engine_capital_population_incomplete")
    principal = Decimal(str(pool[0]))
    epoch: datetime = pool[1]

    rows = engine_book_rows(conn, epoch)
    realised, unrealised = _price_book(conn, positions, rows, snapshot.observed_at)
    pot = PotNav(principal, epoch, realised, unrealised, snapshot.observed_at)
    if not all(value.is_finite() for value in (principal, realised, unrealised)) or pot.nav <= 0:
        raise EngineCapitalObservationError("engine pot NAV is not positive", "engine_capital_population_incomplete")
    return pot


def engine_book_rows(conn: psycopg.Connection[Any], epoch: datetime) -> list[tuple[Any, ...]]:
    """``(trade_id, status, instrument_id, position_id, claimed_at, released_at)`` of the engine book.

    Eligibility is `_load_realised_delta`'s: an allocated paper-deployment trade or a core-arm
    trade, created at or after the pot epoch. One row per ownership row (NULL when none).
    """
    return conn.execute(
        f"""
        SELECT trade.strategy_trade_id,trade.status,trade.instrument_id,
               ownership.broker_position_id,ownership.claimed_at,ownership.released_at
        FROM strategy_trades trade
        LEFT JOIN strategy_funding_decisions funding
          ON funding.funding_decision_id=trade.funding_decision_id
         AND funding.verdict='allocated'
        LEFT JOIN strategy_deployments deployment
          ON deployment.deployment_id=funding.deployment_id
         AND deployment.mode='paper'
        {core_arm_joins("trade")}
        LEFT JOIN strategy_position_ownership ownership
          ON ownership.strategy_trade_id=trade.strategy_trade_id
        WHERE trade.created_at >= %s
          AND (deployment.deployment_id IS NOT NULL OR {core_arm_present("trade")})
        ORDER BY trade.strategy_trade_id
        """,
        (epoch,),
    ).fetchall()


def _load_state(conn: psycopg.Connection[Any], *, for_update: bool) -> PotRiskState | None:
    row = conn.execute(
        "SELECT epoch_started_at,nav_index,index_high_water,last_nav,last_pnl,last_drawdown_pct,observed_at "
        "FROM strategy_engine_pot_risk_state WHERE id" + (" FOR UPDATE" if for_update else "")
    ).fetchone()
    if row is None:
        return None
    index, high_water, nav, pnl, drawdown = (Decimal(str(value)) for value in row[1:6])
    return PotRiskState(row[0], index, high_water, nav, pnl, drawdown, row[6])


def _check(state: PotRiskState | None, pot: PotNav) -> PotRiskRefusal | None:
    if state is None:
        return None
    if state.epoch_started_at != pot.epoch_started_at:
        return "engine_pot_epoch_mismatch"
    if pot.observed_at < state.observed_at:
        return "engine_pot_risk_stale"
    return None


def advance_pot_drawdown(conn: psycopg.Connection[Any], pot: PotNav) -> Decimal | PotRiskRefusal:
    """Link ``pot`` into the stored index and return its drawdown percent.

    The caller holds the transaction. The bootstrap insert precedes the row lock so two
    concurrent first observations cannot both write a base: the loser links against the
    winner, and an identical P&L links at a zero return.
    """
    base = link(None, pot)
    conn.execute(
        """
        INSERT INTO strategy_engine_pot_risk_state (
            id,epoch_started_at,nav_index,index_high_water,last_nav,last_pnl,last_drawdown_pct,observed_at
        ) VALUES (true,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
        """,
        (
            base.epoch_started_at,
            base.nav_index,
            base.index_high_water,
            base.last_nav,
            base.last_pnl,
            base.last_drawdown_pct,
            base.observed_at,
        ),
    )
    state = _load_state(conn, for_update=True)
    refusal = _check(state, pot)
    if refusal is not None:
        return refusal
    linked = link(state, pot)
    conn.execute(
        """
        UPDATE strategy_engine_pot_risk_state
        SET nav_index=%s,index_high_water=%s,last_nav=%s,last_pnl=%s,last_drawdown_pct=%s,observed_at=%s
        WHERE id
        """,
        (
            linked.nav_index,
            linked.index_high_water,
            linked.last_nav,
            linked.last_pnl,
            linked.last_drawdown_pct,
            linked.observed_at,
        ),
    )
    return linked.last_drawdown_pct


def preview_pot_drawdown(conn: psycopg.Connection[Any], pot: PotNav) -> Decimal | PotRiskRefusal:
    """``advance_pot_drawdown``'s arithmetic and refusals, without writing."""
    state = _load_state(conn, for_update=False)
    refusal = _check(state, pot)
    if refusal is not None:
        return refusal
    return link(state, pot).last_drawdown_pct


def load_stored_pot_drawdown(conn: psycopg.Connection[Any]) -> Decimal | None:
    """The last recorded drawdown, for a reader whose own observation is older than it."""
    state = _load_state(conn, for_update=False)
    return None if state is None else state.last_drawdown_pct


def observe_deployment_nav(
    conn: psycopg.Connection[Any], snapshot: BrokerAccountRiskSnapshot, deployment_id: int
) -> PotNav:
    """One paper deployment's own book, priced by ``snapshot`` (#3541 slice 3).

    Principal is the deployment's ``capital_limit``; the population is every trade allocated
    to it. Same refusals as ``observe_pot_nav``.
    """
    positions = _snapshot_positions(snapshot)
    # Principal is the CURRENT capital limit, as the pot's is its latest pool event. A revision
    # landing between the snapshot and this read is linked into the span ending at the
    # snapshot; `link`'s flow bound already assumes nothing about when inside a span a flow
    # landed, and takes the smaller base, so the mis-timing is fail-closed.
    capital = conn.execute(
        "SELECT capital_limit, created_at FROM strategy_deployments WHERE deployment_id=%s AND mode='paper'",
        (deployment_id,),
    ).fetchone()
    if capital is None:
        raise EngineCapitalObservationError(
            f"paper deployment {deployment_id} does not exist", "engine_capital_population_incomplete"
        )
    principal = Decimal(str(capital[0]))
    if not principal > 0:
        raise EngineCapitalObservationError(
            f"paper deployment {deployment_id} has no capital", "engine_capital_population_incomplete"
        )
    rows = conn.execute(
        """
        SELECT trade.strategy_trade_id,trade.status,trade.instrument_id,
               ownership.broker_position_id,ownership.claimed_at,ownership.released_at
        FROM strategy_trades trade
        JOIN strategy_funding_decisions funding
          ON funding.funding_decision_id=trade.funding_decision_id
         AND funding.verdict='allocated'
        LEFT JOIN strategy_position_ownership ownership
          ON ownership.strategy_trade_id=trade.strategy_trade_id
        WHERE funding.deployment_id=%s
        ORDER BY trade.strategy_trade_id
        """,
        (deployment_id,),
    ).fetchall()
    realised, unrealised = _price_book(conn, positions, rows, snapshot.observed_at)
    nav = PotNav(principal, capital[1], realised, unrealised, snapshot.observed_at)
    if not all(value.is_finite() for value in (principal, realised, unrealised)) or nav.nav <= 0:
        raise EngineCapitalObservationError(
            f"paper deployment {deployment_id} NAV is not positive", "engine_capital_population_incomplete"
        )
    return nav


def advance_deployment_drawdown(conn: psycopg.Connection[Any], deployment_id: int, nav: PotNav) -> Decimal | None:
    """Link ``nav`` into the deployment's index; return its drawdown, or ``None`` when stale.

    Clears ``last_refusal``. Bootstrap-then-lock, as ``advance_pot_drawdown``. The base is the
    deployment when FIRST FUNDED -- that revision's capital and zero P&L, at ``created_at``
    (``epoch_started_at``) -- so P&L earned before the first observation is linked, and a
    capital revision before it is linked as a flow, rather than either being absorbed.
    The currency is USD by constraint (sql/338), the unit the broker P&L is validated in.
    """
    funded = conn.execute(
        "SELECT capital_limit FROM strategy_deployment_events WHERE deployment_id=%s AND capital_limit > 0 "
        "ORDER BY revision LIMIT 1",
        (deployment_id,),
    ).fetchone()
    if funded is None:
        raise EngineCapitalObservationError(
            f"paper deployment {deployment_id} has no funded capital revision", "engine_capital_population_incomplete"
        )
    base = link(
        None,
        replace(
            nav,
            principal=Decimal(str(funded[0])),
            realised=_ZERO,
            unrealised=_ZERO,
            observed_at=nav.epoch_started_at,
        ),
    )
    conn.execute(
        """
        INSERT INTO strategy_deployment_nav_risk_state (
            deployment_id,nav_index,index_high_water,last_nav,last_pnl,last_drawdown_pct,max_drawdown_pct,
            observed_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (deployment_id) DO NOTHING
        """,
        (
            deployment_id,
            base.nav_index,
            base.index_high_water,
            base.last_nav,
            base.last_pnl,
            base.last_drawdown_pct,
            base.last_drawdown_pct,
            base.observed_at,
        ),
    )
    row = conn.execute(
        "SELECT nav_index,index_high_water,last_nav,last_pnl,last_drawdown_pct,observed_at "
        "FROM strategy_deployment_nav_risk_state WHERE deployment_id=%s FOR UPDATE",
        (deployment_id,),
    ).fetchone()
    assert row is not None
    if nav.observed_at < row[5]:
        return None
    index, high_water, last_nav, last_pnl, drawdown = (Decimal(str(value)) for value in row[0:5])
    linked = link(PotRiskState(nav.epoch_started_at, index, high_water, last_nav, last_pnl, drawdown, row[5]), nav)
    conn.execute(
        """
        UPDATE strategy_deployment_nav_risk_state
        SET nav_index=%s,index_high_water=%s,last_nav=%s,last_pnl=%s,last_drawdown_pct=%s,
            max_drawdown_pct=GREATEST(max_drawdown_pct,%s),last_refusal=NULL,observed_at=%s
        WHERE deployment_id=%s
        """,
        (
            linked.nav_index,
            linked.index_high_water,
            linked.last_nav,
            linked.last_pnl,
            linked.last_drawdown_pct,
            linked.last_drawdown_pct,
            linked.observed_at,
            deployment_id,
        ),
    )
    return linked.last_drawdown_pct


def record_deployment_refusal(conn: psycopg.Connection[Any], deployment_id: int, reason: str) -> None:
    """Mark the deployment's drawdown unobservable until its next successful advance.

    The live gate reads a refused row as no evidence. With no row yet there is nothing to
    mark: the gate already has no evidence.
    """
    conn.execute(
        "UPDATE strategy_deployment_nav_risk_state SET last_refusal=%s WHERE deployment_id=%s",
        (reason[:1000], deployment_id),
    )


__all__ = [
    "PotNav",
    "PotRiskRefusal",
    "PotRiskState",
    "advance_deployment_drawdown",
    "advance_pot_drawdown",
    "link",
    "load_stored_pot_drawdown",
    "observe_deployment_nav",
    "observe_pot_nav",
    "preview_pot_drawdown",
    "record_deployment_refusal",
]
