"""Engine-book NAV/P&L bridge between consecutive broker snapshots (#3540, gap register P1).

Spec: ``docs/proposals/execution/2026-10-01-3540-engine-nav-bridge.md``.

For each interval ``(observed_at[t-1], observed_at[t]]`` the pot NAV change of the exact-owned
book is stated as named components, and every broker figure the statement uses is checked
against a recomputation from the broker's own rates. Two statuses, never conflated:

- ``closed``: the statement is complete -- every owned position is accounted for at both
  endpoints and every figure it sums is present. There is no independent broker NAV for the
  engine pot (the account also holds non-engine positions), so this proves completeness only.
- ``validated``: closed, and every check is within its bound, nothing is unrecomputable, and
  the fee memo is ``fees_zero``.

An unknown is a named state with amount ``None``; nothing is coerced to zero.

⚠ Fees and distributions are a MEMO, not a term. ``totalFees`` carries both as one signed
number, and whether the broker's ``pnL`` / ``netProfit`` already include it is undocumented and
has never been observable (every stored value is 0 -- see the spec for the queries). The bridge
therefore names a nonzero value ``fees_nonzero_inclusion_unresolved`` instead of choosing a
treatment, which would either double-charge or omit it.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

_ZERO = Decimal("0")
_HALF_CENT = Decimal("0.005")
_USD_CURRENCY_ID = 1

ResidualCode = Literal[
    "mark_pnl",
    "close_netprofit",
    "close_open_rate",
    "open_investment",
    "units_conserved",
]
StateCode = Literal[
    "snapshot_missing",
    "principal_unobserved",
    "open_event_missing",
    "close_netprofit_missing",
    "close_units_missing",
    "units_over_closed",
    "owned_position_unmarked",
    "closed_position_still_marked",
    "mark_incomplete",
    "position_altered",
    "not_recomputable",
    "trade_without_ownership",
]
FeesMemo = Literal["fees_zero", "fees_unobserved", "fees_nonzero_inclusion_unresolved"]

# States that make the statement incomplete. The rest only block validation.
_BREAKS_CLOSURE: frozenset[StateCode] = frozenset(
    {
        "snapshot_missing",
        "principal_unobserved",
        "open_event_missing",
        "close_netprofit_missing",
        "close_units_missing",
        "units_over_closed",
        "owned_position_unmarked",
        "closed_position_still_marked",
        "mark_incomplete",
        "trade_without_ownership",
    }
)


@dataclass(frozen=True)
class Snapshot:
    snapshot_date: date
    observed_at: datetime


@dataclass(frozen=True)
class Mark:
    position_id: int
    is_buy: bool | None
    units: Decimal | None
    pnl: Decimal | None
    close_rate: Decimal | None
    conversion_rate: Decimal | None
    asset_currency_id: int | None
    is_partially_altered: bool | None
    total_fees: Decimal | None


@dataclass(frozen=True)
class OpenEvent:
    executed_at: datetime
    units: Decimal
    #: ``trade_events.price`` is nullable (missing or non-positive broker rate at ingest);
    #: None makes every check that needs it ``not_recomputable``.
    price: Decimal | None
    investment_usd: Decimal | None


@dataclass(frozen=True)
class CloseEvent:
    executed_at: datetime
    units: Decimal | None
    net_profit: Decimal | None
    fees_usd: Decimal | None
    open_rate: Decimal | None
    close_rate: Decimal | None
    is_buy: bool | None


@dataclass(frozen=True)
class OwnedPosition:
    position_id: int
    core_linked: bool
    #: Earliest ownership claim. From then on the position must have an open event.
    claimed_at: datetime | None
    open: OpenEvent | None
    closes: tuple[CloseEvent, ...]
    leverage: int | None
    asset_currency_id: int | None
    #: ``broker_positions_closed.total_fees`` -- the counter's final value; None when the
    #: position is not (yet) in that table.
    closed_total_fees: Decimal | None


@dataclass(frozen=True)
class PrincipalEvent:
    changed_at: datetime
    event_id: int
    capital_limit: Decimal | None


@dataclass(frozen=True)
class Residual:
    code: ResidualCode
    position_id: int
    broker: Decimal
    recomputed: Decimal
    bound: Decimal

    @property
    def amount(self) -> Decimal:
        """Signed: broker figure minus recomputation."""
        return self.broker - self.recomputed

    @property
    def exceeds(self) -> bool:
        return abs(self.amount) > self.bound


@dataclass(frozen=True)
class State:
    code: StateCode
    position_id: int | None = None


@dataclass(frozen=True)
class BridgeInterval:
    opened_at: datetime
    closed_at: datetime
    days: int
    opening_nav: Decimal | None
    flows: Decimal | None
    realised: Decimal | None
    unrealised_released: Decimal | None
    unrealised_opened: Decimal | None
    unrealised_continuing: Decimal | None
    fx_effect: Decimal | None
    price_effect: Decimal | None
    closing_nav: Decimal | None
    fees_memo: FeesMemo
    #: Every fee observation the memo read: (position_id, value or None when unobserved).
    fees_detail: tuple[tuple[int, Decimal | None], ...]
    residuals: tuple[Residual, ...]
    states: tuple[State, ...]
    closed: bool
    validated: bool


@dataclass(frozen=True)
class BridgeCensus:
    """Book-level facts reported once per run."""

    owned_positions: int
    non_core_owned_positions: int
    trades_without_ownership: int


@dataclass
class _Endpoint:
    snapshot: Snapshot
    principal: Decimal | None
    members: set[int] = field(default_factory=set)
    marks: dict[int, Mark] = field(default_factory=dict)
    states: list[State] = field(default_factory=list)


def rate_quantum(rate: Decimal) -> Decimal:
    """The rate's rounding quantum, by construction: 10^-max(2, decimals as received).

    JSON numbers lose trailing zeros, so 1 dp (``762.8``) is indistinguishable from 2 dp; a
    finer observed precision is taken as the quantum. A true quantum coarser than 0.01 makes
    the bound too tight, so a check fails LOUD rather than hiding a residual.
    """
    exponent = rate.normalize().as_tuple().exponent
    decimals = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    return Decimal(1).scaleb(-max(2, decimals))


def _sign(is_buy: bool) -> Decimal:
    return Decimal(1) if is_buy else Decimal(-1)


def _principal_at(events: Sequence[PrincipalEvent], instant: datetime) -> Decimal | None:
    eligible = [event for event in events if event.changed_at <= instant]
    if not eligible:
        return None
    return max(eligible, key=lambda event: (event.changed_at, event.event_id)).capital_limit


def _closed_units(position: OwnedPosition, instant: datetime) -> Decimal:
    return sum((close.units or _ZERO for close in position.closes if close.executed_at <= instant), _ZERO)


def _is_member(position: OwnedPosition, instant: datetime) -> bool:
    if position.open is None or position.open.executed_at > instant:
        return False
    return _closed_units(position, instant) < position.open.units


def _mark_complete(mark: Mark) -> bool:
    return None not in (
        mark.is_buy,
        mark.units,
        mark.pnl,
        mark.close_rate,
        mark.conversion_rate,
        mark.asset_currency_id,
        mark.is_partially_altered,
    )


def _endpoint(
    snapshot: Snapshot,
    marks: Mapping[int, Mark],
    positions: Sequence[OwnedPosition],
    principal_events: Sequence[PrincipalEvent],
) -> _Endpoint:
    instant = snapshot.observed_at
    point = _Endpoint(snapshot=snapshot, principal=_principal_at(principal_events, instant))
    if point.principal is None:
        point.states.append(State("principal_unobserved"))
    for position in positions:
        pid = position.position_id
        mark = marks.get(pid)
        closes_so_far = [close for close in position.closes if close.executed_at <= instant]
        # Every close that reaches this endpoint's realised figure must carry its profit and
        # its units, however long ago it happened -- otherwise the NAV is silently None.
        for close in closes_so_far:
            if close.net_profit is None:
                point.states.append(State("close_netprofit_missing", pid))
            if close.units is None:
                point.states.append(State("close_units_missing", pid))
        if position.open is None:
            claimed = position.claimed_at is not None and position.claimed_at <= instant
            if claimed or mark is not None or closes_so_far:
                point.states.append(State("open_event_missing", pid))
            continue
        if position.open.executed_at <= instant and _closed_units(position, instant) > position.open.units:
            point.states.append(State("units_over_closed", pid))
            continue
        if _is_member(position, instant):
            point.members.add(pid)
            if mark is None:
                point.states.append(State("owned_position_unmarked", pid))
            elif not _mark_complete(mark):
                point.states.append(State("mark_incomplete", pid))
            else:
                point.marks[pid] = mark
        elif mark is not None:
            point.states.append(State("closed_position_still_marked", pid))
    return point


def _closing_nav(point: _Endpoint, positions: Sequence[OwnedPosition]) -> Decimal | None:
    if point.principal is None or any(state.code in _BREAKS_CLOSURE for state in point.states):
        return None
    realised = _ZERO
    for position in positions:
        for close in position.closes:
            if close.executed_at <= point.snapshot.observed_at:
                if close.net_profit is None:  # already named by `_endpoint`
                    return None
                realised += close.net_profit
    unrealised = sum((point.marks[pid].pnl or _ZERO for pid in point.members), _ZERO)
    return point.principal + realised + unrealised


def _recomputable(position: OwnedPosition, currency_id: int | None) -> bool:
    return (
        currency_id == _USD_CURRENCY_ID
        and position.leverage == 1
        and position.open is not None
        and position.open.price is not None
    )


def _mark_checks(marks: Mapping[int, Mark], by_id: Mapping[int, OwnedPosition]) -> tuple[list[Residual], list[State]]:
    residuals: list[Residual] = []
    states: list[State] = []
    for pid in sorted(marks):
        mark, position = marks[pid], by_id[pid]
        if mark.is_partially_altered:
            states.append(State("position_altered", pid))
        # The recomputation has no conversion term, so a USD row must also carry conv = 1.
        if not _recomputable(position, mark.asset_currency_id) or mark.conversion_rate != 1:
            states.append(State("not_recomputable", pid))
            continue
        assert position.open is not None and position.open.price is not None
        assert mark.units is not None and mark.close_rate is not None
        assert mark.pnl is not None and mark.is_buy is not None
        open_rate = position.open.price
        residuals.append(
            Residual(
                "mark_pnl",
                pid,
                broker=mark.pnl,
                recomputed=_sign(mark.is_buy) * mark.units * (mark.close_rate - open_rate),
                bound=_HALF_CENT + mark.units * (rate_quantum(mark.close_rate) + rate_quantum(open_rate)) / 2,
            )
        )
    return residuals, states


def _close_checks(pairs: Sequence[tuple[OwnedPosition, CloseEvent]]) -> tuple[list[Residual], list[State]]:
    residuals: list[Residual] = []
    states: list[State] = []
    for position, close in pairs:
        pid = position.position_id
        if (
            not _recomputable(position, position.asset_currency_id)
            or close.net_profit is None
            or close.units is None
            or close.open_rate is None
            or close.close_rate is None
            or close.is_buy is None
        ):
            states.append(State("not_recomputable", pid))
            continue
        assert position.open is not None and position.open.price is not None
        residuals.append(
            Residual("close_open_rate", pid, broker=close.open_rate, recomputed=position.open.price, bound=_ZERO)
        )
        residuals.append(
            Residual(
                "close_netprofit",
                pid,
                broker=close.net_profit,
                recomputed=_sign(close.is_buy) * close.units * (close.close_rate - close.open_rate),
                bound=_HALF_CENT + close.units * (rate_quantum(close.close_rate) + rate_quantum(close.open_rate)) / 2,
            )
        )
    return residuals, states


def _open_checks(
    positions: Sequence[OwnedPosition], after: datetime | None, until: datetime
) -> tuple[list[Residual], list[State]]:
    residuals: list[Residual] = []
    states: list[State] = []
    for position in positions:
        event = position.open
        if event is None or event.executed_at > until or (after is not None and event.executed_at <= after):
            continue
        if event.investment_usd is None or not _recomputable(position, position.asset_currency_id):
            states.append(State("not_recomputable", position.position_id))
            continue
        assert event.price is not None
        residuals.append(
            Residual(
                "open_investment",
                position.position_id,
                broker=event.investment_usd,
                recomputed=event.units * event.price,
                bound=_HALF_CENT + event.units * rate_quantum(event.price) / 2,
            )
        )
    return residuals, states


def bridge_intervals(
    snapshots: Sequence[Snapshot],
    marks_by_date: Mapping[date, Mapping[int, Mark]],
    positions: Sequence[OwnedPosition],
    principal_events: Sequence[PrincipalEvent],
    *,
    trades_without_ownership: int = 0,
) -> list[BridgeInterval]:
    """One ``BridgeInterval`` per consecutive snapshot pair, oldest first."""
    ordered = sorted(snapshots, key=lambda snapshot: snapshot.observed_at)
    by_id = {position.position_id: position for position in positions}
    endpoints = [
        _endpoint(snapshot, marks_by_date.get(snapshot.snapshot_date, {}), positions, principal_events)
        for snapshot in ordered
    ]
    out: list[BridgeInterval] = []
    for index, (start, end) in enumerate(zip(endpoints, endpoints[1:], strict=False)):
        # The first interval's opening NAV rests on figures no earlier interval checked (the
        # opening marks and every close before it), so it checks them too.
        out.append(_interval(start, end, by_id, positions, trades_without_ownership, check_opening=index == 0))
    return out


def _interval(
    start: _Endpoint,
    end: _Endpoint,
    by_id: Mapping[int, OwnedPosition],
    positions: Sequence[OwnedPosition],
    trades_without_ownership: int,
    *,
    check_opening: bool,
) -> BridgeInterval:
    s0, s1 = start.snapshot.observed_at, end.snapshot.observed_at
    states: list[State] = list(dict.fromkeys([*start.states, *end.states]))
    if trades_without_ownership:
        states.append(State("trade_without_ownership"))

    closes_in = [
        (position, close) for position in positions for close in position.closes if s0 < close.executed_at <= s1
    ]

    opening = _closing_nav(start, positions)
    closing = _closing_nav(end, positions)
    closed = opening is not None and closing is not None and not any(s.code in _BREAKS_CLOSURE for s in states)

    flows = realised = released = opened = continuing = fx = price = None
    if closed:
        assert start.principal is not None and end.principal is not None
        flows = end.principal - start.principal
        realised = sum((close.net_profit or _ZERO for _, close in closes_in), _ZERO)
        # (closure guarantees every net_profit here is present -- `_endpoint` names a None)
        released = -sum((start.marks[pid].pnl or _ZERO for pid in start.members - end.members), _ZERO)
        opened = sum((end.marks[pid].pnl or _ZERO for pid in end.members - start.members), _ZERO)
        continuing = _ZERO
        fx = _ZERO
        for pid in start.members & end.members:
            before, after = start.marks[pid], end.marks[pid]
            assert before.pnl is not None and after.pnl is not None
            continuing += after.pnl - before.pnl
            assert after.is_buy is not None and after.units is not None and after.close_rate is not None
            assert after.conversion_rate is not None and before.conversion_rate is not None
            fx += (
                _sign(after.is_buy) * after.units * after.close_rate * (after.conversion_rate - before.conversion_rate)
            )
        price = continuing - fx

    # Each snapshot is checked once, as a closing endpoint; the first interval also checks
    # what its opening NAV rests on.
    residuals, check_states = _mark_checks(end.marks, by_id)
    states.extend(check_states)
    for pid in sorted(end.marks.keys() & start.marks.keys()):
        before_units, after_units = start.marks[pid].units, end.marks[pid].units
        assert before_units is not None and after_units is not None
        closed_units = sum((c.units or _ZERO for p, c in closes_in if p.position_id == pid), _ZERO)
        residuals.append(
            Residual("units_conserved", pid, broker=after_units, recomputed=before_units - closed_units, bound=_ZERO)
        )
    close_pairs = list(closes_in)
    if check_opening:
        opening_residuals, opening_states = _mark_checks(start.marks, by_id)
        residuals.extend(opening_residuals)
        states.extend(opening_states)
        close_pairs = [
            (position, close) for position in positions for close in position.closes if close.executed_at <= s1
        ]
    for check in (_close_checks(close_pairs), _open_checks(positions, None if check_opening else s0, s1)):
        residuals.extend(check[0])
        states.extend(check[1])

    fees_memo, fees_detail = _fees(start, end, closes_in, positions, s0, s1)
    states = list(dict.fromkeys(states))
    validated = (
        closed
        and fees_memo == "fees_zero"
        and not any(residual.exceeds for residual in residuals)
        and not any(state.code in ("not_recomputable", "position_altered") for state in states)
    )
    return BridgeInterval(
        opened_at=s0,
        closed_at=s1,
        days=(end.snapshot.snapshot_date - start.snapshot.snapshot_date).days,
        opening_nav=opening if closed else None,
        flows=flows,
        realised=realised,
        unrealised_released=released,
        unrealised_opened=opened,
        unrealised_continuing=continuing,
        fx_effect=fx,
        price_effect=price,
        closing_nav=closing if closed else None,
        fees_memo=fees_memo,
        fees_detail=fees_detail,
        residuals=tuple(residuals),
        states=tuple(states),
        closed=closed,
        validated=validated,
    )


def _fees(
    start: _Endpoint,
    end: _Endpoint,
    closes_in: Sequence[tuple[OwnedPosition, CloseEvent]],
    positions: Sequence[OwnedPosition],
    s0: datetime,
    s1: datetime,
) -> tuple[FeesMemo, tuple[tuple[int, Decimal | None], ...]]:
    """Every fee observation relevant to the interval, and the memo they imply."""
    detail: list[tuple[int, Decimal | None]] = []
    for point in (start, end):
        for pid in sorted(point.members):
            mark = point.marks.get(pid)
            detail.append((pid, mark.total_fees if mark is not None else None))
    for position, close in closes_in:
        detail.append((position.position_id, close.fees_usd))
    for position in positions:
        if any(s0 < close.executed_at <= s1 for close in position.closes) and not _is_member(position, s1):
            detail.append((position.position_id, position.closed_total_fees))
    if any(value is None for _, value in detail):
        return "fees_unobserved", tuple(detail)
    if any(value != 0 for _, value in detail):
        return "fees_nonzero_inclusion_unresolved", tuple(detail)
    return "fees_zero", tuple(detail)


# ---------------------------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------------------------


def _dec(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def load_bridge(
    conn: psycopg.Connection[Any], *, sessions: int, environment: str = "demo"
) -> tuple[list[BridgeInterval], BridgeCensus]:
    """Read everything the bridge needs and compute it. Caller owns the transaction; run it
    under ``REPEATABLE READ`` so every query sees one snapshot generation."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_date, observed_at FROM broker_account_equity_snapshots
            WHERE environment = %(env)s ORDER BY snapshot_date DESC LIMIT %(n)s
            """,
            {"env": environment, "n": sessions + 1},
        )
        snapshots = [Snapshot(row["snapshot_date"], row["observed_at"]) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT own.broker_position_id AS position_id,
                   bool_or(t.core_rebalance_intent_id IS NOT NULL) AS core_linked,
                   min(own.claimed_at) AS claimed_at
            FROM strategy_position_ownership own
            JOIN strategy_trades t ON t.strategy_trade_id = own.strategy_trade_id
            GROUP BY own.broker_position_id
            """
        )
        owned = {int(row["position_id"]): (bool(row["core_linked"]), row["claimed_at"]) for row in cur.fetchall()}
        ids = list(owned)

        cur.execute(
            """
            SELECT count(*) AS n FROM strategy_trades t
            WHERE t.status IN ('open', 'closing', 'closed')
              AND NOT EXISTS (
                  SELECT 1 FROM strategy_position_ownership own
                  WHERE own.strategy_trade_id = t.strategy_trade_id
              )
            """
        )
        census_row = cur.fetchone()
        trades_without_ownership = int(census_row["n"]) if census_row else 0

        # ⚠ No environment predicate, deliberately: `trade_events` has no environment column.
        # The population is the ownership set, keyed by the broker's globally assigned position
        # id -- the same join `strategy_monitoring._OWNED_LIFECYCLE_SQL` and `strategy_wealth` use.
        cur.execute(
            """
            SELECT position_id, event_kind, executed_at, units, price, investment_usd,
                   realized_pnl_usd, fees_usd,
                   raw_payload->>'openRate' AS open_rate,
                   raw_payload->>'closeRate' AS close_rate,
                   raw_payload->>'isBuy' AS is_buy
            FROM trade_events
            WHERE position_id = ANY(%(ids)s) AND event_kind IN ('open', 'close')
            ORDER BY executed_at, event_id
            """,
            {"ids": ids},
        )
        events = list(cur.fetchall())

        cur.execute(
            """
            SELECT position_id, leverage, NULL::numeric AS closed_total_fees FROM broker_positions
            WHERE position_id = ANY(%(ids)s)
            UNION ALL
            SELECT position_id, leverage, total_fees FROM broker_positions_closed
            WHERE position_id = ANY(%(ids)s)
            """,
            {"ids": ids},
        )
        broker_rows: dict[int, dict[str, Any]] = {}
        for row in cur.fetchall():
            previous = broker_rows.get(int(row["position_id"]))
            # A closed row wins over a stale open row for the final fee counter.
            if previous is None or row["closed_total_fees"] is not None:
                broker_rows[int(row["position_id"])] = row

        cur.execute(
            """
            SELECT snapshot_date, position_id, is_buy, units, unrealized_pnl, close_rate,
                   close_conversion_rate, asset_currency_id, is_partially_altered, total_fees
            FROM broker_account_position_marks
            WHERE environment = %(env)s AND position_id = ANY(%(ids)s)
              AND snapshot_date = ANY(%(dates)s)
            """,
            {"env": environment, "ids": ids, "dates": [snapshot.snapshot_date for snapshot in snapshots]},
        )
        marks_by_date: dict[date, dict[int, Mark]] = defaultdict(dict)
        currency: dict[int, int] = {}
        for row in cur.fetchall():
            pid = int(row["position_id"])
            marks_by_date[row["snapshot_date"]][pid] = Mark(
                position_id=pid,
                is_buy=row["is_buy"],
                units=_dec(row["units"]),
                pnl=_dec(row["unrealized_pnl"]),
                close_rate=_dec(row["close_rate"]),
                conversion_rate=_dec(row["close_conversion_rate"]),
                asset_currency_id=row["asset_currency_id"],
                is_partially_altered=row["is_partially_altered"],
                total_fees=_dec(row["total_fees"]),
            )
            if row["asset_currency_id"] is not None:
                currency[pid] = int(row["asset_currency_id"])

        # ⚠ Unscoped, deliberately: this is the ONE engine pool's event log -- the table has no
        # pool or environment column, and every reader (`strategy_wealth`,
        # `strategy_control_plane`, `ai_trial_start_gate`) reads it whole. A second pool would
        # need the same scoping added to all of them at once.
        cur.execute(
            """
            SELECT strategy_paper_pool_event_id AS event_id, changed_at, capital_limit
            FROM strategy_paper_pool_events
            """
        )
        principal_events = [
            PrincipalEvent(row["changed_at"], int(row["event_id"]), _dec(row["capital_limit"]))
            for row in cur.fetchall()
        ]

    opens: dict[int, list[OpenEvent]] = defaultdict(list)
    closes: dict[int, list[CloseEvent]] = defaultdict(list)
    for row in events:
        pid = int(row["position_id"])
        if row["event_kind"] == "open":
            units = _dec(row["units"])
            if units is None:
                # An open without units cannot define membership; leaving it out names the
                # position `open_event_missing` from its ownership claim.
                continue
            opens[pid].append(OpenEvent(row["executed_at"], units, _dec(row["price"]), _dec(row["investment_usd"])))
        else:
            is_buy = row["is_buy"]
            closes[pid].append(
                CloseEvent(
                    executed_at=row["executed_at"],
                    units=_dec(row["units"]),
                    net_profit=_dec(row["realized_pnl_usd"]),
                    fees_usd=_dec(row["fees_usd"]),
                    open_rate=_dec(row["open_rate"]),
                    close_rate=_dec(row["close_rate"]),
                    is_buy=None if is_buy is None else is_buy == "true",
                )
            )

    positions: list[OwnedPosition] = []
    for pid, (core_linked, claimed_at) in sorted(owned.items()):
        broker = broker_rows.get(pid)
        position_opens = opens.get(pid, [])
        positions.append(
            OwnedPosition(
                position_id=pid,
                core_linked=core_linked,
                claimed_at=claimed_at,
                # Exactly one open row is the only unambiguous lifecycle; zero or several is
                # named `open_event_missing` rather than guessed.
                open=position_opens[0] if len(position_opens) == 1 else None,
                closes=tuple(closes.get(pid, [])),
                leverage=int(broker["leverage"]) if broker and broker["leverage"] is not None else None,
                asset_currency_id=currency.get(pid),
                closed_total_fees=_dec(broker["closed_total_fees"]) if broker else None,
            )
        )

    intervals = bridge_intervals(
        snapshots, marks_by_date, positions, principal_events, trades_without_ownership=trades_without_ownership
    )
    census = BridgeCensus(
        owned_positions=len(positions),
        non_core_owned_positions=sum(1 for position in positions if not position.core_linked),
        trades_without_ownership=trades_without_ownership,
    )
    return intervals, census


__all__ = [
    "BridgeCensus",
    "BridgeInterval",
    "CloseEvent",
    "Mark",
    "OpenEvent",
    "OwnedPosition",
    "PrincipalEvent",
    "Residual",
    "Snapshot",
    "State",
    "bridge_intervals",
    "load_bridge",
    "rate_quantum",
]
