"""Ranking-pot-v1's §7.4 loss check and its ``halted_loss`` writer (#2842 slice 5c-ii-a).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §7.4, "The loss check". The executed book's net
P&L — realised ``netProfit`` of every pot close plus the broker's own ``unrealizedPnL`` of every pot position in a
LIVE account snapshot — at or below ``−LOSS_LIMIT_FRACTION × POT_CAPITAL`` halts new entries (``halted_loss``). The
halt never liquidates and never blocks an exit; it is a brake, not a bound on the pot's loss.

* Fees are charged ADVERSELY, because whether ``netProfit`` / ``unrealizedPnL`` already include them is undocumented
  (``engine_nav_bridge``'s fee memo): − |``fees_usd``| per close event (its sign is undocumented too), and
  − max(``totalFees``, 0) per mark (the portal documents a negative ``totalFees`` as a refund). At 0 — every stored
  value today — both are no-ops.
* A position is MARKED only when its figures reconcile in units: closes plus the mark's units equal to the open
  event's units, or, before the open event is ingested, an unaltered mark with no close events; a position absent
  from the snapshot is fully realised only when its closes equal the open event's units. Anything else — no
  ownership row yet (submitted, uncertain, filled and not yet reconciled), closed at the broker and not yet
  ingested, a missing final close, a stale mark beside an ingested close (its mark then counts only when
  negative) — is charged its trade's STOP BOUND (its submission row's stop
  distance plus the cost cap) on top of its known closes, instead of refusing: an entry submitted this fire has no
  ownership row until the reconciliation sees its fill, so a refusal would serialise a rebalance's entries to one per
  hourly fire. Residual: a gap through the stop, slippage past the ask, or a stop the broker does not hold can exceed
  the bound until the mark lands.
* ``None`` (``pot_loss_check_unavailable``, a deferral) only on a data defect: a NULL or non-finite figure, a
  duplicated or short mark, a position owned by two pot trades, a funded pot trade with no submission row, a
  snapshot whose per-position rows are not the direct positions it counts, an over-closed position, a non-USD or
  unreported account currency.

Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_UP, Decimal
from typing import Any, Final, Literal, Protocol

import psycopg
from psycopg.rows import dict_row

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerDirectPositionInvestment
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.ranking_pot import POT_COST_CAP_PCT

Conn = psycopg.Connection[Any]

_CENT: Final = Decimal("0.01")
#: §7.4: net P&L at or below −20% of ``POT_CAPITAL`` halts entries.
LOSS_LIMIT_FRACTION: Final = Decimal("0.20")
LOSS_LIMIT: Final = "pot_loss_limit"
LOSS_UNAVAILABLE: Final = "pot_loss_check_unavailable"


@dataclass(frozen=True)
class PotClose:
    net_profit: Decimal | None
    fees_usd: Decimal | None
    units: Decimal | None


@dataclass(frozen=True)
class PotPosition:
    position_id: int
    #: Any ``active`` ownership row of the position.
    active: bool
    #: The units of its single ``open`` event; ``None`` when there is none or more than one.
    open_units: Decimal | None
    closes: tuple[PotClose, ...]


@dataclass(frozen=True)
class PotExposure:
    """One pot trade (its ``ranking_pot_exec_submissions`` row) and the positions it owns."""

    strategy_trade_id: int
    trade_status: str
    amount: Decimal
    ask: Decimal
    stop_loss_rate: Decimal
    positions: tuple[PotPosition, ...]


def _finite(value: Decimal | None) -> bool:
    return value is not None and value.is_finite()


def stop_bound(amount: Decimal, ask: Decimal, stop_loss_rate: Decimal) -> Decimal | None:
    """The loss a trade's broker-held stop and the cost cap allow, ``amount × ((ask − SL)/ask + cap%/100)``, rounded
    UP to the cent; ``None`` on an unusable input."""
    if not (_finite(amount) and _finite(ask) and _finite(stop_loss_rate)) or ask <= 0 or amount < 0:
        return None
    bound = amount * ((ask - stop_loss_rate) / ask + POT_COST_CAP_PCT / Decimal(100))
    return max(bound, Decimal(0)).quantize(_CENT, rounding=ROUND_UP)


class PositionMark(Protocol):
    """The fields of a per-position mark this figure reads: a live snapshot row or a stored mark (slice 6c-ii-c-2b)."""

    @property
    def is_buy(self) -> bool | None: ...
    @property
    def units(self) -> Decimal | None: ...
    @property
    def unrealized_pnl(self) -> Decimal | None: ...
    @property
    def total_fees(self) -> Decimal | None: ...
    @property
    def is_partially_altered(self) -> bool | None: ...


def _position_pnl(position: PotPosition, mark: PositionMark | None) -> tuple[Decimal, bool] | None:
    """(known P&L, reconciled) for one position, or ``None`` on a data defect."""
    known = Decimal(0)
    closed_units = Decimal(0)
    for close in position.closes:
        if not (_finite(close.net_profit) and _finite(close.fees_usd) and _finite(close.units)):
            return None
        assert close.net_profit is not None and close.fees_usd is not None and close.units is not None
        known += close.net_profit - abs(close.fees_usd)
        closed_units += close.units
    if mark is not None:
        if mark.is_buy is not True or not (_finite(mark.unrealized_pnl) and _finite(mark.units)):
            return None
        if not _finite(mark.total_fees):
            return None
        pnl, units, fees = mark.unrealized_pnl, mark.units, mark.total_fees
        assert pnl is not None and units is not None and fees is not None
        marked = pnl - max(fees, Decimal(0))
        if position.open_units is not None:
            exact = closed_units + units == position.open_units
        else:
            # No open event ingested yet (a fresh fill): only an unaltered, never-closed position is whole.
            exact = not position.closes and mark.is_partially_altered is False
        if exact:
            return known + marked, True
        # Unreconciled (a stale mark beside an ingested close, or a partial close not yet ingested): the mark's loss
        # counts, its gain never does, and the caller adds the bound.
        return known + min(marked, Decimal(0)), False
    if position.active or not position.closes or position.open_units is None:
        return known, False
    if closed_units > position.open_units:
        return None  # an over-close: the ingest's own partial-close model violation (`trade_events`)
    return known, closed_units == position.open_units


def pot_net_pnl(
    exposures: Sequence[PotExposure], marks: Mapping[int, BrokerDirectPositionInvestment]
) -> Decimal | None:
    """The executed book's net P&L, or ``None`` when a figure it needs is a data defect."""
    seen: set[int] = set()
    total = Decimal(0)
    for exposure in exposures:
        bound = stop_bound(exposure.amount, exposure.ask, exposure.stop_loss_rate)
        if bound is None:
            return None
        if not exposure.positions:
            # A `failed` trade with no position committed nothing; any other status may be filling right now.
            if exposure.trade_status != "failed":
                total -= bound
            continue
        reconciled = True
        for position in exposure.positions:
            if position.position_id in seen:
                return None
            seen.add(position.position_id)
            pnl = _position_pnl(position, marks.get(position.position_id))
            if pnl is None:
                return None
            total += pnl[0]
            reconciled = reconciled and pnl[1]
        if not reconciled:
            total -= bound
    return total


def breached(net: Decimal, pot_capital: Decimal) -> bool:
    return net <= -LOSS_LIMIT_FRACTION * pot_capital


def snapshot_marks(risk: BrokerAccountRiskSnapshot) -> dict[int, BrokerDirectPositionInvestment] | None:
    """The snapshot's per-position rows by broker position id; ``None`` when the account currency is not the
    documented USD, a position id repeats, or the per-position rows are not exactly the direct positions the
    per-instrument rows count (mirrors and pending orders are in ``amount``, never in the counts)."""
    if DOCUMENTED_ACCOUNT_CURRENCIES.get(risk.account_currency_id or 0) != "USD":
        return None
    counted = sum(i.direct_long_positions + i.direct_short_positions for i in risk.instrument_investments)
    if counted != len(risk.direct_positions):
        return None
    marks: dict[int, BrokerDirectPositionInvestment] = {}
    for row in risk.direct_positions:
        if row.position_id in marks:
            return None
        marks[row.position_id] = row
    return marks


_EXPOSURE_SQL: Final = """
    SELECT s.strategy_trade_id, t.status AS trade_status, s.amount, s.ask, s.stop_loss_rate,
           o.broker_position_id, o.status AS ownership_status,
           te.event_id, te.event_kind, te.units, te.realized_pnl_usd, te.fees_usd
      FROM ranking_pot_exec_submissions s
      JOIN ranking_pot_exec_lifecycles l ON l.lifecycle_id = s.lifecycle_id
      JOIN strategy_trades t ON t.strategy_trade_id = s.strategy_trade_id
      LEFT JOIN strategy_position_ownership o ON o.strategy_trade_id = s.strategy_trade_id
      LEFT JOIN trade_events te ON te.position_id = o.broker_position_id AND te.event_kind IN ('open', 'close')
     WHERE l.declaration_id = %(d)s
     ORDER BY s.strategy_trade_id, o.broker_position_id, te.event_id
"""

#: A funded pot trade without its submission row would be invisible to ``_EXPOSURE_SQL`` (``sql/450``'s trigger and
#: the authority make it impossible; this is the check that it stayed so).
_UNSUBMITTED_SQL: Final = """
    SELECT count(*)
      FROM ranking_pot_exec_lifecycles l
      JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id AND fd.verdict = 'allocated'
      JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
     WHERE l.declaration_id = %(d)s
       AND NOT EXISTS (SELECT 1 FROM ranking_pot_exec_submissions s WHERE s.strategy_trade_id = t.strategy_trade_id)
"""


def read_pot_exposures(conn: Conn, declaration_id: int) -> list[PotExposure] | None:
    """Every pot trade of the declaration with its positions; ``None`` when a funded trade has no submission row."""
    unsubmitted = conn.execute(_UNSUBMITTED_SQL, {"d": declaration_id}).fetchone()
    if unsubmitted is None or int(unsubmitted[0]) != 0:
        return None
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(_EXPOSURE_SQL, {"d": declaration_id}).fetchall()
    heads: dict[int, dict[str, Any]] = {}
    active: dict[int, dict[int, bool]] = defaultdict(dict)
    opens: dict[int, dict[int, Decimal | None]] = defaultdict(dict)
    closes: dict[int, dict[int, PotClose]] = defaultdict(dict)
    for r in rows:
        trade = int(r["strategy_trade_id"])
        heads.setdefault(trade, r)
        if r["broker_position_id"] is None:
            continue
        pid = int(r["broker_position_id"])
        active[trade][pid] = active[trade].get(pid, False) or r["ownership_status"] == "active"
        # An ownership row per claim repeats a position's events; the event id de-duplicates them.
        if r["event_kind"] == "open":
            opens[pid][int(r["event_id"])] = r["units"]
        elif r["event_kind"] == "close":
            closes[pid][int(r["event_id"])] = PotClose(r["realized_pnl_usd"], r["fees_usd"], r["units"])
    return [
        PotExposure(
            strategy_trade_id=trade,
            trade_status=str(head["trade_status"]),
            amount=Decimal(head["amount"]),
            ask=Decimal(head["ask"]),
            stop_loss_rate=Decimal(head["stop_loss_rate"]),
            positions=tuple(
                PotPosition(
                    position_id=pid,
                    active=is_active,
                    # Exactly one open event is the only unambiguous lifecycle (the #3540 bridge's rule).
                    open_units=next(iter(opens[pid].values())) if len(opens[pid]) == 1 else None,
                    closes=tuple(closes[pid].values()),
                )
                for pid, is_active in active[trade].items()
            ),
        )
        for trade, head in heads.items()
    ]


@dataclass(frozen=True)
class LossCheck:
    net: Decimal
    breached: bool


def check_loss(
    conn: Conn, declaration_id: int, *, pot_capital: Decimal, risk: BrokerAccountRiskSnapshot
) -> LossCheck | None:
    """§7.4 on the caller's transaction and the given live snapshot; ``None`` = unavailable (a data defect)."""
    if not _finite(pot_capital) or pot_capital <= 0:
        return None
    marks = snapshot_marks(risk)
    if marks is None:
        return None
    exposures = read_pot_exposures(conn, declaration_id)
    if exposures is None:
        return None
    net = pot_net_pnl(exposures, marks)
    if net is None:
        return None
    return LossCheck(net=net, breached=breached(net, pot_capital))


HaltOutcome = Literal["ok", "halted", "unavailable", "not_executing"]


def halt_on_loss(
    conn: Conn, declaration_id: int, *, pot_capital: Decimal, risk: BrokerAccountRiskSnapshot
) -> HaltOutcome:
    """Re-check under the declaration row lock and write ``executing → halted_loss`` (actor ``engine``) on a breach,
    in its own transaction (the caller's connection must be idle). Idempotent: a state no longer ``executing`` writes
    nothing; the check is recomputed under the lock, so a stale breach never halts a resumed declaration."""
    with conn.transaction():
        # The lock the transition trigger takes, taken first: the pot declaration row before anything else (sql/445).
        conn.execute(
            "SELECT 1 FROM ranking_pot_declarations WHERE declaration_id = %s FOR NO KEY UPDATE", (declaration_id,)
        )
        state = conn.execute(
            "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
            (declaration_id,),
        ).fetchone()
        if state is None or state[0] != "executing":
            return "not_executing"
        check = check_loss(conn, declaration_id, pot_capital=pot_capital, risk=risk)
        if check is None:
            return "unavailable"
        if not check.breached:
            return "ok"
        conn.execute(
            "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
            "VALUES (%s, 'executing', 'halted_loss', %s, 'engine')",
            (
                declaration_id,
                f"§7.4: executed net P&L {check.net:.2f} <= -{LOSS_LIMIT_FRACTION * 100:.0f}% of POT_CAPITAL "
                f"{pot_capital} (account snapshot observed {risk.observed_at.isoformat()})",
            ),
        )
        return "halted"


_EXECUTING_SQL: Final = """
    SELECT d.declaration_id, a.pot_capital
      FROM ranking_pot_declarations d
      LEFT JOIN ranking_pot_activations a ON a.declaration_id = d.declaration_id
     WHERE (SELECT e.to_state FROM ranking_pot_state_events e
             WHERE e.declaration_id = d.declaration_id ORDER BY e.event_id DESC LIMIT 1) = 'executing'
     ORDER BY d.declaration_id
"""


def executing_declarations(conn: Conn) -> list[tuple[int, Decimal | None]]:
    """Every ``executing`` declaration and its ``POT_CAPITAL`` (``None`` = no activation row)."""
    return [(int(r[0]), None if r[1] is None else Decimal(r[1])) for r in conn.execute(_EXECUTING_SQL).fetchall()]


__all__ = [
    "LOSS_LIMIT",
    "LOSS_LIMIT_FRACTION",
    "LOSS_UNAVAILABLE",
    "HaltOutcome",
    "LossCheck",
    "PotClose",
    "PotExposure",
    "PotPosition",
    "PositionMark",
    "breached",
    "check_loss",
    "executing_declarations",
    "halt_on_loss",
    "pot_net_pnl",
    "read_pot_exposures",
    "snapshot_marks",
    "stop_bound",
]
