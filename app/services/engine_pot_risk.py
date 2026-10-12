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
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal, NoReturn
from zoneinfo import ZoneInfo

import psycopg

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerDirectPositionInvestment
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.market_calendar import latest_completed_us_session
from app.services.strategy_core_arc_sql import core_arm_joins, core_arm_present
from app.services.strategy_engine_capital import EngineCapitalObservationError, EngineCapitalRefusal

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")
_HALF_STORED_UNIT = Decimal("5E-9")  # half the last place of `trade_events.units` NUMERIC(20,8), sql/194

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


# eToro books a PARTIAL close under a NEW position id that shares the entry order's
# `orderId` and that nobody owns (`broker_closed_release`, attended 2026-09-23). The
# predicate is `ai_trial_readout._CLOSE_ROWS_SQL`'s. `orders.broker_order_ref` is not unique by
# constraint, so `_price_book` refuses a slice matched to two trades rather than count it twice.
_SIBLING_CLOSES_SQL = """
    SELECT t.trade_id, count(ev.event_id), COALESCE(sum(ev.units),0),
           count(ev.event_id) FILTER (WHERE ev.realized_pnl_usd IS NULL), COALESCE(sum(ev.realized_pnl_usd),0),
           array_agg(ev.event_id)
    FROM unnest(%s::bigint[]) AS t(trade_id)
    JOIN trade_events ev ON ev.event_kind = 'close' AND ev.executed_at <= %s
     AND NOT EXISTS (SELECT 1 FROM strategy_position_ownership other WHERE other.broker_position_id = ev.position_id)
     AND EXISTS (
         SELECT 1 FROM strategy_trade_orders sto
         JOIN orders o ON o.order_id = sto.order_id
         WHERE sto.strategy_trade_id = t.trade_id AND sto.purpose = 'entry'
           AND (ev.order_id::text = o.broker_order_ref OR ev.raw_payload ->> 'orderId' = o.broker_order_ref)
     )
    GROUP BY t.trade_id
"""


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
    members = [*held, *released]
    ledger = conn.execute(
        """
        SELECT ids.position_id,
               sum(event.units) FILTER (WHERE event.event_kind='open') AS open_units,
               count(event.event_id) FILTER (WHERE event.event_kind='close') AS close_count,
               COALESCE(sum(event.units) FILTER (WHERE event.event_kind='close'),0) AS closed_units,
               count(event.event_id) FILTER (
                   WHERE event.event_kind='close' AND event.realized_pnl_usd IS NULL
               ) AS missing_pnl,
               COALESCE(sum(event.realized_pnl_usd) FILTER (WHERE event.event_kind='close'),0) AS realised
        FROM unnest(%s::bigint[]) AS ids(position_id)
        LEFT JOIN trade_events event
          ON event.position_id=ids.position_id AND event.executed_at <= %s
        GROUP BY ids.position_id
        """,
        (members, at),
    ).fetchall()
    # Every member came from one of these rows (`book_membership`), and `broker_position_id` is
    # UNIQUE (sql/281), so each member has exactly one trade.
    trade_of = {
        int(position_id): int(trade_id)
        for trade_id, _status, _instrument_id, position_id, _claimed_at, _released_at in rows
        if position_id is not None and int(position_id) in members
    }
    siblings: dict[int, tuple[int, Decimal, int, Decimal]] = {}
    sliced: set[int] = set()
    for trade_id, count, units, missing, amount, event_ids in conn.execute(
        _SIBLING_CLOSES_SQL, (sorted(set(trade_of.values())), at)
    ).fetchall():
        if sliced & set(event_ids):
            raise EngineCapitalObservationError(
                f"a close slice matches strategy trade {trade_id} and another trade",
                "engine_capital_population_incomplete",
            )
        sliced.update(event_ids)
        siblings[int(trade_id)] = (int(count), Decimal(str(units)), int(missing), Decimal(str(amount)))

    realised = _ZERO
    by_trade: dict[int, list[tuple[int, Decimal | None, int, Decimal]]] = {}
    for position_id, open_units, close_count, closed_units, missing_pnl, amount in ledger:
        realised += _closes_realised(f"owned position {position_id}", int(missing_pnl), amount)
        by_trade.setdefault(trade_of[int(position_id)], []).append(
            (
                int(position_id),
                None if open_units is None else Decimal(str(open_units)),
                int(close_count),
                Decimal(str(closed_units)),
            )
        )
    for trade_id, ledgers in by_trade.items():
        sibling_count, sibling_units, sibling_missing, sibling_amount = siblings.get(trade_id, (0, _ZERO, 0, _ZERO))
        realised += _closes_realised(f"strategy trade {trade_id} close slice", sibling_missing, sibling_amount)
        _check_units_conserved(trade_id, ledgers, sibling_count, sibling_units, {pid: positions[pid] for pid in held})
    return realised, unrealised


def _closes_realised(label: str, missing_pnl: int, amount: Any) -> Decimal:
    if missing_pnl:
        raise EngineCapitalObservationError(
            f"{label} has a close without realised P&L", "engine_capital_population_incomplete"
        )
    value = Decimal(str(amount))
    if not value.is_finite():
        # Two aggregates of opposite infinite sign would otherwise raise
        # `decimal.InvalidOperation` in the sum and abort the whole paper cycle (#3612).
        raise EngineCapitalObservationError(
            f"{label} realised P&L is not finite", "engine_capital_population_incomplete"
        )
    return value


def _check_units_conserved(
    trade_id: int,
    ledgers: list[tuple[int, Decimal | None, int, Decimal]],
    sibling_count: int,
    sibling_units: Decimal,
    held: dict[int, BrokerDirectPositionInvestment],
) -> None:
    """Refuse a NAV priced on an incomplete ledger (#3612).

    ``ledgers`` are ``(position_id, open units, close count, closed units)`` of the trade's
    book positions; ``held`` maps the book's held positions to their broker rows.

    Without this a released position passed with any close history, so a recorded partial
    followed by an unrecorded final close left that close's P&L out; and a held position whose
    partial close was unrecorded had its units and mark reduced with no realised P&L beside
    them. Both NAV readers link what they price into a drawdown index. The ledger's units
    contract (sql/194): the open carries the ORIGINAL size and each close its slice. So each
    position's ledger balance (open less its own closes) is at least its broker units (zero
    once released), and the trade's shortfalls sum to the unowned partial-close slices under its
    entry order. With no slice, every position therefore reconciles on its own, and opposite
    discrepancies on two positions of one trade (sql/282) cannot cancel.
    """
    # The ledger stores units at NUMERIC(20,8), so each stored value is within half an
    # eighth-place unit of what the broker reported; the broker's own units are unrounded.
    shortfall = _ZERO
    stored_values = sibling_count
    for position_id, open_units, close_count, closed_units in ledgers:
        row = held.get(position_id)
        if open_units is None:
            # The open is ingested by the next portfolio sync, after the ownership claim
            # (measured 3s to 11 minutes on every engine position, 2026-10-12). Until then the
            # position is whole only if nothing has closed: no recorded close or slice, and the
            # broker's `isPartiallyAltered` -- documented as "whether this position was
            # partially closed" -- is false. The rule `ranking_pot_loss` uses for a fresh fill.
            if row is not None and close_count == 0 and sibling_count == 0 and row.is_partially_altered is False:
                continue
            raise EngineCapitalObservationError(
                f"owned position {position_id} has no recorded open by the snapshot",
                "engine_capital_population_incomplete",
            )
        broker = _ZERO if row is None else row.units
        gap = open_units - closed_units - broker
        stored_values += 1 + close_count
        if not gap.is_finite() or gap < -(1 + close_count) * _HALF_STORED_UNIT:
            raise EngineCapitalObservationError(
                f"owned position {position_id} ledger leaves {open_units - closed_units} units, the broker {broker}",
                "engine_capital_population_incomplete",
            )
        shortfall += gap
    if abs(shortfall - sibling_units) > stored_values * _HALF_STORED_UNIT:
        raise EngineCapitalObservationError(
            f"strategy trade {trade_id} ledger is short {shortfall} units against {sibling_units} in close slices",
            "engine_capital_population_incomplete",
        )


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
    return conn.execute(_ENGINE_BOOK_SQL + " ORDER BY trade.strategy_trade_id", {"epoch": epoch}).fetchall()


# `engine_book_rows`' population, also read inside `observe_daily_marked_pnl`'s one statement.
_ENGINE_BOOK_SQL = f"""
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
        WHERE trade.created_at >= %(epoch)s
          AND (deployment.deployment_id IS NOT NULL OR {core_arm_present("trade")})
"""


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


# ---------------------------------------------------------------------------
# #3612 slice 2: daily loss on marked P&L
# Spec: docs/proposals/risk/2026-10-12-3612-marked-daily-loss.md
# ---------------------------------------------------------------------------

_NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class DailyMark:
    """One position's broker terms in a snapshot: units, unrealised P&L, mark time, ``totalFees``."""

    units: Decimal
    pnl: Decimal
    at: datetime | None
    fees: Decimal | None


@dataclass(frozen=True)
class DailyAnchor:
    """The session-close anchor S (sql/482): its receipt time, currency and declared position count."""

    observed_at: datetime
    account_currency_id: int
    currency: str | None
    declared_positions: int
    marks: dict[int, DailyMark]


@dataclass(frozen=True)
class LedgerEvent:
    kind: str
    executed_at: datetime
    units: Decimal


def daily_anchor_session(observed_at: datetime) -> date:
    """P: the latest NYSE session completed before D, ``observed_at``'s New York date, began."""
    day_start = datetime.combine(observed_at.astimezone(_NEW_YORK).date(), time.min, tzinfo=_NEW_YORK)
    return latest_completed_us_session(day_start)


def _refuse(message: str, code: EngineCapitalRefusal) -> NoReturn:
    raise EngineCapitalObservationError(message, code)


def marked_daily_pnl(
    *,
    anchor: DailyAnchor | None,
    live: dict[int, DailyMark],
    book: list[tuple[Any, ...]],
    events: dict[int, list[LedgerEvent]],
    observed_at: datetime,
    decided_at: datetime,
    max_mark_age_seconds: int,
) -> Decimal:
    """The engine book's broker ΔU since S, or a refusal (spec Design 3 and 4). Pure.

    ``book`` is ``engine_book_rows``' rows, every ownership row whatever its claim time.
    ``events`` holds each book position's ``trade_events`` up to ``decided_at``.
    """
    if anchor is None:
        _refuse("no session-close anchor for the prior session", "engine_pot_daily_anchor_missing")
    if anchor.currency != "USD" or DOCUMENTED_ACCOUNT_CURRENCIES.get(anchor.account_currency_id) != "USD":
        _refuse("session-close anchor is not a USD account", "engine_capital_snapshot_unusable")
    if len(anchor.marks) != anchor.declared_positions:
        _refuse(
            f"session-close anchor holds {len(anchor.marks)} marks for {anchor.declared_positions} positions",
            "engine_pot_daily_anchor_incomplete",
        )
    for position_id, mark in anchor.marks.items():
        values = (mark.units, mark.pnl, mark.fees)
        if mark.at is None or not all(value is not None and value.is_finite() for value in values):
            _refuse(f"anchor mark of position {position_id} is incomplete", "engine_pot_daily_anchor_incomplete")
    anchor_at = anchor.observed_at

    claimed: dict[int, datetime] = {}
    for trade_id, status, _instrument_id, position_id, claimed_at, _released_at in book:
        if position_id is None:
            if status in ("open", "closing", "closed"):
                _refuse(f"strategy trade {trade_id} has no exact ownership", "engine_capital_population_incomplete")
            continue
        claimed[int(position_id)] = claimed_at

    for position_id in claimed:
        for event in events.get(position_id, []):
            if not event.units.is_finite():
                _refuse(f"position {position_id} has a non-finite ledger event", "engine_pot_daily_event_unwitnessed")
            # The veto runs through the decision, and over positions claimed after the snapshot too.
            if event.kind == "close" and anchor_at < event.executed_at <= decided_at:
                _refuse(f"position {position_id} closed after the anchor", "engine_pot_daily_close_in_window")

    population = {pid: at for pid, at in claimed.items() if at <= observed_at}
    oldest_mark = decided_at - timedelta(seconds=max_mark_age_seconds)
    for position_id in population:
        mark = live.get(position_id)
        if mark is None:
            continue
        if mark.at is None or mark.at > observed_at or mark.at < oldest_mark:
            _refuse(f"position {position_id} broker mark time {mark.at} is unusable", "engine_pot_daily_mark_stale")
        anchor_mark = anchor.marks.get(position_id)
        if anchor_mark is not None and anchor_mark.at is not None and not mark.at > anchor_mark.at:
            _refuse(f"position {position_id} broker mark is not after its anchor", "engine_pot_daily_mark_stale")

    for position_id in population:
        ledger = events.get(position_id, [])
        for snapshot_marks, receipt in ((anchor.marks, anchor_at), (live, observed_at)):
            mark = snapshot_marks.get(position_id)
            if mark is None:
                continue
            assert mark.at is not None  # checked above for both snapshots
            if any(mark.at <= event.executed_at <= receipt for event in ledger):
                _refuse(
                    f"position {position_id} has an event between mark and receipt",
                    "engine_pot_daily_event_unwitnessed",
                )
            if _ledger_units(ledger, receipt) != mark.units:
                _refuse(f"position {position_id} units disagree with its ledger", "engine_pot_daily_event_unwitnessed")

    daily = _ZERO
    for position_id, claimed_at in population.items():
        anchor_mark, mark = anchor.marks.get(position_id), live.get(position_id)
        in_window = [event for event in events.get(position_id, []) if anchor_at < event.executed_at <= observed_at]
        if anchor_mark is not None and mark is not None:
            if in_window or mark.units != anchor_mark.units:
                _refuse(f"held position {position_id} changed without a witness", "engine_pot_daily_event_unwitnessed")
            if mark.fees is None or not mark.fees.is_finite() or mark.fees != anchor_mark.fees:
                _refuse(f"held position {position_id} fees changed", "engine_pot_daily_accounting_unverified")
            change = mark.pnl - anchor_mark.pnl
        elif mark is not None:
            opens = [event for event in in_window if event.kind == "open"]
            if len(in_window) != 1 or len(opens) != 1 or opens[0].units != mark.units:
                _refuse(f"opened position {position_id} has no matching open", "engine_pot_daily_event_unwitnessed")
            if mark.fees is None or mark.fees != _ZERO:
                _refuse(f"opened position {position_id} carries fees", "engine_pot_daily_accounting_unverified")
            change = mark.pnl
        elif anchor_mark is not None:
            _refuse(f"position {position_id} left the book with no close", "engine_pot_daily_event_unwitnessed")
        else:
            if in_window or anchor_at < claimed_at <= observed_at:
                _refuse(f"position {position_id} changed with no mark", "engine_pot_daily_event_unwitnessed")
            continue
        if not change.is_finite():
            _refuse(f"position {position_id} broker P&L is not finite", "engine_capital_snapshot_unusable")
        daily += change
    return daily


def _ledger_units(ledger: list[LedgerEvent], cut: datetime) -> Decimal | None:
    """The open's units less every close executed at or before ``cut``, or ``None`` with no single open."""
    opens = [event for event in ledger if event.kind == "open" and event.executed_at <= cut]
    if len(opens) != 1:
        return None
    closed = sum((event.units for event in ledger if event.kind == "close" and event.executed_at <= cut), _ZERO)
    return opens[0].units - closed


_DAILY_MARKED_SQL = f"""
WITH book AS ({_ENGINE_BOOK_SQL})
SELECT
    (SELECT json_build_array(observed_at, account_currency_id, currency,
                             official_direct_long_positions + official_direct_short_positions)
       FROM broker_account_session_close_snapshots
      WHERE environment = %(environment)s AND session_date = %(session)s),
    (SELECT COALESCE(json_agg(json_build_array(position_id, units::text, unrealized_pnl::text,
                                               pnl_timestamp, total_fees::text)), '[]'::json)
       FROM broker_account_session_close_marks
      WHERE environment = %(environment)s AND session_date = %(session)s),
    (SELECT COALESCE(json_agg(json_build_array(strategy_trade_id, status, instrument_id, broker_position_id,
                                               claimed_at, released_at) ORDER BY strategy_trade_id), '[]'::json)
       FROM book),
    (SELECT COALESCE(json_agg(json_build_array(e.position_id, e.event_kind, e.executed_at, e.units::text)
                              ORDER BY e.executed_at, e.event_id), '[]'::json)
       FROM trade_events e
      WHERE e.position_id IN (SELECT broker_position_id FROM book WHERE broker_position_id IS NOT NULL)
        AND e.executed_at <= %(until)s)
"""


def _at(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


def _anchor_mark(row: list[Any]) -> DailyMark:
    """Decode one sql/482 child. Its columns are NOT NULL; a NULL still refuses rather than raising."""
    position_id, units, pnl, at, fees = row
    if units is None or pnl is None:
        _refuse(f"anchor mark of position {position_id} is incomplete", "engine_pot_daily_anchor_incomplete")
    return DailyMark(Decimal(units), Decimal(pnl), _at(at), None if fees is None else Decimal(fees))


def observe_daily_marked_pnl(
    conn: psycopg.Connection[Any],
    snapshot: BrokerAccountRiskSnapshot,
    *,
    decided_at: datetime,
    max_mark_age_seconds: int,
) -> Decimal:
    """The engine book's marked P&L since the prior session's close anchor (#3612 slice 2).

    One statement reads the anchor, the book and its ledger, so they share one snapshot even
    in READ COMMITTED. Raises ``EngineCapitalObservationError`` for every refusal.
    """
    positions = _snapshot_positions(snapshot)
    epoch_row = conn.execute(
        "SELECT changed_at FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id LIMIT 1"
    ).fetchone()
    if epoch_row is None:
        _refuse("no paper pool has been assigned", "engine_capital_population_incomplete")
    row = conn.execute(
        _DAILY_MARKED_SQL,
        {
            "epoch": epoch_row[0],
            "environment": "demo",
            "session": daily_anchor_session(snapshot.observed_at),
            "until": max(decided_at, snapshot.observed_at),
        },
    ).fetchone()
    assert row is not None  # scalar subqueries always return one row
    anchor_json, marks_json, book_json, events_json = row
    anchor = (
        None
        if anchor_json is None
        else DailyAnchor(
            observed_at=datetime.fromisoformat(anchor_json[0]),
            account_currency_id=int(anchor_json[1]),
            currency=anchor_json[2],
            declared_positions=int(anchor_json[3]),
            marks={int(row[0]): _anchor_mark(row) for row in marks_json},
        )
    )
    events: dict[int, list[LedgerEvent]] = {}
    for pid, kind, at, units in events_json:
        events.setdefault(int(pid), []).append(LedgerEvent(kind, datetime.fromisoformat(at), Decimal(units)))
    book = [
        (trade_id, status, instrument_id, pid, _at(claimed_at), _at(released_at))
        for trade_id, status, instrument_id, pid, claimed_at, released_at in book_json
    ]
    live = {
        pid: DailyMark(position.units, position.unrealized_pnl, position.pnl_timestamp, position.total_fees)
        for pid, position in positions.items()
    }
    return marked_daily_pnl(
        anchor=anchor,
        live=live,
        book=book,
        events=events,
        observed_at=snapshot.observed_at,
        decided_at=decided_at,
        max_mark_age_seconds=max_mark_age_seconds,
    )
