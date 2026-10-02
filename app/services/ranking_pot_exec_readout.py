"""Ranking-pot-v1 readout: executed vs shadow, and the executed book's NAV vs SPY (#2842 slice 6c-ii-c-2b).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §9.4, "Executed vs shadow, and the executed book's
NAV vs SPY". Descriptive only: nothing here enters a decision, a look or a gate. ``executed`` runs inside the
readout's REPEATABLE READ READ ONLY transaction and reads the executed book's append-only rows, its trades and
positions, and the stored daily account snapshots.

- **Per executed rebalance:** the header's stored decision beside the shadow's at the same target session, and per
  executed entry its status, refusal, submission, fill, fill vs ask, fill vs the shadow's entry price construction
  (close_T × (1 + h)) and timing.
- **NAV vs SPY:** at each stored snapshot from activation, 1 + the executed book's net P&L / ``POT_CAPITAL``, using
  §7.4's per-position figure over the facts known at the snapshot instant, STRICT (an unreconciled point is
  ``null``; nothing is charged a stop bound); SPY's stored close at the latest completed session, over the same at
  activation.
- **Outcomes are read now:** statuses, ownership and broker events as they stand in the readout's transaction, so a
  readout at a past E reports the eventual outcomes of the T ≤ E cohort.

Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row

from app.services import ranking_pot_exec as exec_book
from app.services import ranking_pot_loss as loss
from app.services import ranking_pot_sim as sim
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import latest_completed_us_session

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = exec_book._NEW_YORK

FillReason = Literal[
    "no_position", "multiple_positions", "open_event_missing", "multiple_open_events", "fill_price_unusable"
]
NavReason = Literal["unreconciled", "data_defect", "not_usd"]


def _s(x: Decimal | None) -> str | None:
    return None if x is None else str(x)


def _median(values: Sequence[Decimal]) -> Decimal | None:
    # The readout's rule (mean of the two middle values; ``None`` when empty). Not imported: that module imports this.
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


# ---------------------------------------------------------------------------
# Pure: executed vs shadow
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class OpenFact:
    price: Decimal | None
    executed_at: datetime
    units: Decimal | None


@dataclass(frozen=True)
class CloseFact:
    executed_at: datetime
    close: loss.PotClose


@dataclass(frozen=True)
class PositionFacts:
    position_id: int
    opens: tuple[OpenFact, ...]
    closes: tuple[CloseFact, ...]


@dataclass(frozen=True)
class EntryFacts:
    """One executed lifecycle of a rebalance, with its funding, trade, submission and positions."""

    lifecycle_id: int
    instrument_id: int
    slot: int
    #: ``expected_cost.half_spread_fraction`` of its sha-checked ticket.
    half_spread: Fraction
    funding_verdict: str | None
    reason_code: str | None
    trade_status: str | None
    requested_amount: Decimal | None
    amount: Decimal | None
    ask: Decimal | None
    quote_at: datetime | None
    positions: tuple[PositionFacts, ...]


@dataclass(frozen=True)
class Fill:
    price: Decimal
    executed_at: datetime


def fill_of(entry: EntryFacts) -> Fill | FillReason:
    """The single open event of the single position the trade owns, or why there is none."""
    if len(entry.positions) != 1:
        return "no_position" if not entry.positions else "multiple_positions"
    (position,) = entry.positions
    if len(position.opens) != 1:
        return "open_event_missing" if not position.opens else "multiple_open_events"
    (o,) = position.opens
    if o.price is None or not o.price.is_finite() or o.price <= 0:
        return "fill_price_unusable"
    return Fill(o.price, o.executed_at)


def entry_row(entry: EntryFacts, *, target: date, endpoint: date, close_t: Decimal | None) -> dict[str, Any]:
    status = exec_book.classify(
        funding_verdict=entry.funding_verdict,
        trade_status=entry.trade_status,
        target_session=target,
        ny_today=endpoint,
    )
    fill = fill_of(entry)
    vs_ask = vs_ref = seconds = None
    on_target = None
    if isinstance(fill, Fill):
        if entry.ask is None or entry.quote_at is None:
            raise ValueError(f"lifecycle {entry.lifecycle_id}: a fill without its submission row")
        with localcontext(sim.CTX):
            vs_ask = fill.price / entry.ask - 1
            if close_t is not None and close_t.is_finite() and close_t > 0:
                h = Decimal(entry.half_spread.numerator) / Decimal(entry.half_spread.denominator)
                vs_ref = fill.price / (close_t * (1 + h)) - 1
        # Signed, exact: a fill before its sizing quote shows as negative rather than being hidden.
        seconds = Decimal((fill.executed_at - entry.quote_at) // timedelta(microseconds=1)).scaleb(-6)
        on_target = fill.executed_at.astimezone(_NEW_YORK).date() == target
    return {
        "lifecycle_id": entry.lifecycle_id,
        "instrument_id": entry.instrument_id,
        "slot": entry.slot,
        "status": status,
        "refusal": entry.reason_code if entry.funding_verdict == "rejected" else None,
        "trade_status": entry.trade_status,
        "requested_amount": _s(entry.requested_amount),
        "amount": _s(entry.amount),
        "ask": _s(entry.ask),
        "quote_at": None if entry.quote_at is None else entry.quote_at.isoformat(),
        "fill": None if not isinstance(fill, Fill) else str(fill.price),
        "fill_reason": None if isinstance(fill, Fill) else fill,
        "executed_at": None if not isinstance(fill, Fill) else fill.executed_at.isoformat(),
        "fill_vs_ask": _s(vs_ask),
        "fill_vs_reference": _s(vs_ref),
        "seconds_after_quote": _s(seconds),
        "fill_session_is_target": on_target,
    }


@dataclass(frozen=True)
class ExecutedRebalance:
    attempt_id: int
    target: date
    state: str
    entries_allowed: bool
    v1_active: bool
    detail: Mapping[str, Any]
    entries: tuple[EntryFacts, ...]
    #: The shadow's decision in T's applied step row (``entries``, ``slots_unfilled``); ``None`` = no such row yet.
    shadow_decision: Mapping[str, Any] | None
    #: T's closes from the step row's ``inputs.bars``; ``None`` = a bar §9.1 counts as missing (``sim.valid_bar``).
    closes: Mapping[int, Decimal | None]


def executed_vs_shadow(rebalances: Sequence[ExecutedRebalance], endpoint: date) -> dict[str, Any]:
    per = []
    statuses: Counter[str] = Counter()
    refusals: Counter[str] = Counter()
    vs_ask: list[Decimal] = []
    vs_ref: list[Decimal] = []
    # Σ slots_unfilled over the rebalances where both sides exist, so the two sums cover one population.
    unfilled = {"executed": 0, "shadow": 0, "compared": 0, "shadow_missing": 0}
    for rb in sorted(rebalances, key=lambda r: r.target):
        shadow = rb.shadow_decision
        shadow_entries = None if shadow is None else [int(i) for i in shadow["entries"]]
        rows = [
            entry_row(e, target=rb.target, endpoint=endpoint, close_t=rb.closes.get(e.instrument_id))
            for e in sorted(rb.entries, key=lambda e: e.lifecycle_id)
        ]
        for r in rows:
            statuses[r["status"]] += 1
            if r["refusal"] is not None:
                refusals[r["refusal"]] += 1
            if r["fill_vs_ask"] is not None:
                vs_ask.append(Decimal(r["fill_vs_ask"]))
            if r["fill_vs_reference"] is not None:
                vs_ref.append(Decimal(r["fill_vs_reference"]))
        executed_unfilled = int(rb.detail["slots_unfilled"])
        if shadow is None:
            unfilled["shadow_missing"] += 1
        else:
            unfilled["executed"] += executed_unfilled
            unfilled["shadow"] += int(shadow["slots_unfilled"])
            unfilled["compared"] += 1
        per.append(
            {
                "attempt_id": rb.attempt_id,
                "target_session": rb.target.isoformat(),
                "executed": {
                    "state": rb.state,
                    "entries_allowed": rb.entries_allowed,
                    "v1_active": rb.v1_active,
                    "held": int(rb.detail["held"]),
                    "entries": int(rb.detail["entries"]),
                    "exits": int(rb.detail["exits"]),
                    "slots_unfilled": executed_unfilled,
                },
                "shadow": None
                if shadow is None or shadow_entries is None
                else {"entries": len(shadow_entries), "slots_unfilled": int(shadow["slots_unfilled"])},
                # Decided entries, whatever became of them (refused, expired, failed included).
                "decided_entry_overlap": None
                if shadow_entries is None
                else len({e.instrument_id for e in rb.entries} & set(shadow_entries)),
                "entries": rows,
            }
        )
    with localcontext(sim.CTX):
        med_ask, med_ref = _median(vs_ask), _median(vs_ref)
    return {
        "per_rebalance": per,
        "statuses": dict(sorted(statuses.items())),
        "refusals": dict(sorted(refusals.items())),
        "fills": len(vs_ask),
        "fill_vs_ask_median": _s(med_ask),
        "fill_vs_reference_median": _s(med_ref),
        "fill_vs_reference_count": len(vs_ref),
        "slots_unfilled": unfilled,
    }


# ---------------------------------------------------------------------------
# Pure: NAV vs SPY
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PotTrade:
    strategy_trade_id: int
    status: str
    #: Its submission row's ``recorded_at``.
    submitted_at: datetime
    positions: tuple[PositionFacts, ...]


@dataclass(frozen=True)
class StoredMark:
    """One ``broker_account_position_marks`` row: the fields §7.4's per-position figure reads."""

    is_buy: bool | None
    units: Decimal | None
    unrealized_pnl: Decimal | None
    total_fees: Decimal | None
    is_partially_altered: bool | None


def net_at(
    trades: Sequence[PotTrade], marks: Mapping[int, StoredMark], at: datetime
) -> Decimal | Literal["unreconciled", "data_defect"]:
    """The executed book's net P&L at instant ``at`` from the facts known then, strict: any counted position that does
    not reconcile, or a trade submitted by ``at`` whose outcome then is unknown, makes the point ``unreconciled``."""
    with localcontext(sim.CTX):
        return _net_at(trades, marks, at)


def _net_at(
    trades: Sequence[PotTrade], marks: Mapping[int, StoredMark], at: datetime
) -> Decimal | Literal["unreconciled", "data_defect"]:
    seen: set[int] = set()
    total = Decimal(0)
    unreconciled = False
    for trade in sorted(trades, key=lambda t: t.strategy_trade_id):
        for p in sorted(trade.positions, key=lambda p: p.position_id):
            # A position owned by two pot trades of the declaration is a defect at every point (declaration-wide).
            if p.position_id in seen:
                return "data_defect"
            seen.add(p.position_id)
            if any(o.units is None or not o.units.is_finite() or o.units <= 0 for o in p.opens):
                return "data_defect"
            opens = [o for o in p.opens if o.executed_at <= at]
            closes = tuple(c.close for c in p.closes if c.executed_at <= at)
            mark = marks.get(p.position_id)
            if not opens and not closes and mark is None:
                # Not yet filled at ``at`` only when its (later) open event proves it; an owned position with no open
                # event at all is unknown (retrospective: a later event is evidence about ``at``).
                if not p.opens:
                    unreconciled = True
                continue
            if len(opens) > 1:
                unreconciled = True  # two opens for one position: never a reconciled lifecycle (§7.4's rule)
                continue
            position = loss.PotPosition(
                position_id=p.position_id,
                # No ownership state AT ``at`` is stored, so ``_position_pnl`` decides from units alone: an unmarked
                # position reconciles only when its closes by ``at`` equal its open.
                active=False,
                open_units=opens[0].units if opens else None,
                closes=closes,
            )
            pnl = loss._position_pnl(position, mark)
            if pnl is None:
                return "data_defect"
            total += pnl[0]
            unreconciled = unreconciled or not pnl[1]
        # Submitted by ``at`` with no position then: unknown, unless the trade never filled (``failed``, read now) or
        # its position filled after ``at`` (handled above).
        if not trade.positions and trade.submitted_at <= at and trade.status != "failed":
            unreconciled = True
    return "unreconciled" if unreconciled else total


@dataclass(frozen=True)
class SnapshotPoint:
    snapshot_date: date
    observed_at: datetime
    marks: Mapping[int, StoredMark]
    #: The observed ``account_currency_id`` (``sql/341``; ``None`` = assumed, not observed).
    account_currency_id: int | None = 1


def _usable(close: Decimal | None) -> bool:
    return close is not None and close.is_finite() and close > 0


def nav_vs_spy(
    *,
    activated_at: datetime,
    pot_capital: Decimal,
    trades: Sequence[PotTrade],
    points: Sequence[SnapshotPoint],
    spy_closes: Mapping[date, Decimal | None],
    endpoint: date,
) -> dict[str, Any]:
    """``spy_closes`` is the whole masked series (the readout's), so an absent date and an unusable close are both
    ``spy_unavailable``. A point belongs to the window when the session its SPY close is read at is ≤ E."""
    if not pot_capital.is_finite() or pot_capital <= 0:
        raise ValueError(f"POT_CAPITAL {pot_capital} is not positive")
    base_session = latest_completed_us_session(activated_at)
    spy_base = spy_closes.get(base_session)
    rows = []
    last = None
    for point in sorted(points, key=lambda p: p.observed_at):
        if point.observed_at < activated_at:
            continue
        session = latest_completed_us_session(point.observed_at)
        if session > endpoint:
            continue
        # §7.4's rule (``ranking_pot_loss.snapshot_marks``): only an observed, documented USD account is summed.
        usd = DOCUMENTED_ACCOUNT_CURRENCIES.get(point.account_currency_id or 0) == "USD"
        net = net_at(trades, point.marks, point.observed_at) if usd else "not_usd"
        spy_now = spy_closes.get(session)
        nav = spy = diff = None
        nav_reason: NavReason | None = None
        with localcontext(sim.CTX):
            if isinstance(net, Decimal):
                nav = 1 + net / pot_capital
            else:
                nav_reason = net
            if _usable(spy_base) and _usable(spy_now):
                assert spy_base is not None and spy_now is not None
                spy = spy_now / spy_base
            if nav is not None and spy is not None:
                diff = nav - spy
        row = {
            "snapshot_date": point.snapshot_date.isoformat(),
            "observed_at": point.observed_at.isoformat(),
            "spy_session": session.isoformat(),
            "nav": _s(nav),
            "nav_reason": nav_reason,
            "spy": _s(spy),
            "spy_reason": None if spy is not None else "spy_unavailable",
            "difference": _s(diff),
        }
        rows.append(row)
        if diff is not None:
            last = row
    return {
        "activated_at": activated_at.isoformat(),
        "spy_base_session": base_session.isoformat(),
        "pot_capital": str(pot_capital),
        "points": rows,
        "last": last,
    }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
_LIFECYCLES_SQL: Final = """
    SELECT l.lifecycle_id, l.attempt_id, l.instrument_id, l.slot, l.ticket, l.ticket_sha256,
           fd.verdict, fd.reason_code, t.strategy_trade_id, t.status AS trade_status,
           s.requested_amount, s.amount, s.ask, s.quote_at, s.recorded_at AS submitted_at
      FROM ranking_pot_exec_lifecycles l
      LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
      LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
      LEFT JOIN ranking_pot_exec_submissions s ON s.lifecycle_id = l.lifecycle_id
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
     WHERE l.declaration_id = %(d)s AND a.target_session <= %(e)s
     ORDER BY l.lifecycle_id
"""

_POSITIONS_SQL: Final = """
    SELECT DISTINCT o.strategy_trade_id, o.broker_position_id
      FROM strategy_position_ownership o
     WHERE o.strategy_trade_id = ANY(%(t)s)
     ORDER BY o.strategy_trade_id, o.broker_position_id
"""

_EVENTS_SQL: Final = """
    SELECT event_id, position_id, event_kind, executed_at, units, price, realized_pnl_usd, fees_usd
      FROM trade_events
     WHERE position_id = ANY(%(p)s) AND event_kind IN ('open', 'close')
     ORDER BY executed_at, event_id
"""


def _positions(conn: Conn, trade_ids: Sequence[int]) -> dict[int, tuple[PositionFacts, ...]]:
    pairs = conn.execute(_POSITIONS_SQL, {"t": list(trade_ids)}).fetchall()
    pids = sorted({int(p) for _, p in pairs})
    opens: dict[int, list[OpenFact]] = defaultdict(list)
    closes: dict[int, list[CloseFact]] = defaultdict(list)
    with conn.cursor(row_factory=dict_row) as cur:
        for r in cur.execute(_EVENTS_SQL, {"p": pids}).fetchall():
            pid = int(r["position_id"])
            if r["event_kind"] == "open":
                opens[pid].append(OpenFact(r["price"], r["executed_at"], r["units"]))
            else:
                closes[pid].append(
                    CloseFact(r["executed_at"], loss.PotClose(r["realized_pnl_usd"], r["fees_usd"], r["units"]))
                )
    out: dict[int, list[PositionFacts]] = defaultdict(list)
    for trade, pid in pairs:
        out[int(trade)].append(PositionFacts(int(pid), tuple(opens[int(pid)]), tuple(closes[int(pid)])))
    return {t: tuple(ps) for t, ps in out.items()}


def _half_spread(ticket: Mapping[str, Any], lifecycle_id: int) -> Fraction:
    h = Fraction(str(ticket["expected_cost"]["half_spread_fraction"]))
    if h < 0:
        raise ValueError(f"lifecycle {lifecycle_id}: a negative ticket half-spread")
    return h


def _close(bar: Sequence[str] | None) -> Decimal | None:
    parsed = None if bar is None else sim.Bar(*(Decimal(v) for v in bar))
    return parsed.close if parsed is not None and sim.valid_bar(parsed) else None


def _closes(bars: Sequence[Any] | None) -> dict[int, Decimal | None]:
    return {} if bars is None else {int(iid): _close(bar) for iid, bar in bars}


_HEADERS_SQL: Final = """
    SELECT h.attempt_id, a.target_session, h.state, h.entries_allowed, h.v1_active, h.detail,
           s.shadow -> 'decision' AS shadow_decision, s.inputs -> 'bars' AS bars
      FROM ranking_pot_exec_rebalances h
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = h.attempt_id
      LEFT JOIN ranking_pot_steps s ON s.declaration_id = h.declaration_id AND s.applied_attempt_id = h.attempt_id
     WHERE h.declaration_id = %s AND a.target_session <= %s
     ORDER BY a.target_session
"""


def executed(
    conn: Conn, declaration_id: int, endpoint: date, spy_closes: Mapping[date, Decimal | None]
) -> dict[str, Any]:
    """The ``executed`` section on the caller's (the readout's) transaction."""
    act = conn.execute(
        "SELECT a.pot_capital, (SELECT e.at FROM ranking_pot_state_events e "
        "  WHERE e.declaration_id = a.declaration_id AND e.to_state = 'executing' ORDER BY e.event_id LIMIT 1) "
        "FROM ranking_pot_activations a WHERE a.declaration_id = %s",
        (declaration_id,),
    ).fetchone()
    if act is None:
        return {"activated": False}
    pot_capital, activated_at = Decimal(act[0]), act[1]
    if activated_at is None:
        raise ValueError(f"declaration {declaration_id}: an activation row without an executing event")
    if activated_at.astimezone(_NEW_YORK).date() > endpoint:
        return {"activated": False}  # activated after E: as of E it was not

    with conn.cursor(row_factory=dict_row) as cur:
        lifecycles = cur.execute(_LIFECYCLES_SQL, {"d": declaration_id, "e": endpoint}).fetchall()
        headers = cur.execute(_HEADERS_SQL, (declaration_id, endpoint)).fetchall()
    positions = _positions(conn, [int(r["strategy_trade_id"]) for r in lifecycles if r["strategy_trade_id"]])

    by_attempt: dict[int, list[EntryFacts]] = defaultdict(list)
    trades: list[PotTrade] = []
    for r in lifecycles:
        if canonical_sha256(r["ticket"]) != r["ticket_sha256"]:
            raise ValueError(f"lifecycle {r['lifecycle_id']}: the stored ticket does not hash to its sha256")
        trade_id = r["strategy_trade_id"]
        ps = positions.get(int(trade_id), ()) if trade_id is not None else ()
        by_attempt[int(r["attempt_id"])].append(
            EntryFacts(
                lifecycle_id=int(r["lifecycle_id"]),
                instrument_id=int(r["instrument_id"]),
                slot=int(r["slot"]),
                half_spread=_half_spread(r["ticket"], int(r["lifecycle_id"])),
                funding_verdict=r["verdict"],
                reason_code=r["reason_code"],
                trade_status=r["trade_status"],
                requested_amount=r["requested_amount"],
                amount=r["amount"],
                ask=r["ask"],
                quote_at=r["quote_at"],
                positions=ps,
            )
        )
        if trade_id is not None:
            if r["submitted_at"] is None:
                raise ValueError(f"trade {trade_id}: a funded pot trade without its submission row")
            trades.append(PotTrade(int(trade_id), str(r["trade_status"]), r["submitted_at"], ps))

    rebalances = []
    for h in headers:
        rebalances.append(
            ExecutedRebalance(
                attempt_id=int(h["attempt_id"]),
                target=h["target_session"],
                state=h["state"],
                entries_allowed=bool(h["entries_allowed"]),
                v1_active=bool(h["v1_active"]),
                detail=h["detail"],
                entries=tuple(by_attempt.get(int(h["attempt_id"]), ())),
                shadow_decision=h["shadow_decision"],
                closes=_closes(h["bars"]),
            )
        )

    points = []
    # One row per UTC day (sql/324). ``nav_vs_spy`` keeps those whose SPY session is ≤ E: no date bound here, because
    # a holiday run puts a later UTC date on that session.
    snaps = conn.execute(
        "SELECT snapshot_date, observed_at, account_currency_id FROM broker_account_equity_snapshots "
        "WHERE environment = 'demo' AND observed_at >= %s ORDER BY snapshot_date",
        (activated_at,),
    ).fetchall()
    pids = sorted({p.position_id for t in trades for p in t.positions})
    marks: dict[date, dict[int, StoredMark]] = defaultdict(dict)
    with conn.cursor(row_factory=dict_row) as cur:
        for m in cur.execute(
            "SELECT snapshot_date, position_id, is_buy, units, unrealized_pnl, total_fees, is_partially_altered "
            "FROM broker_account_position_marks WHERE environment = 'demo' AND position_id = ANY(%s) "
            "AND snapshot_date = ANY(%s)",
            (pids, [d for d, _, _ in snaps]),
        ).fetchall():
            marks[m["snapshot_date"]][int(m["position_id"])] = StoredMark(
                m["is_buy"], m["units"], m["unrealized_pnl"], m["total_fees"], m["is_partially_altered"]
            )
    for d, observed, currency_id in snaps:
        points.append(SnapshotPoint(d, observed, marks.get(d, {}), currency_id))

    return {
        "activated": True,
        "vs_shadow": executed_vs_shadow(rebalances, endpoint),
        "nav_vs_spy": nav_vs_spy(
            activated_at=activated_at,
            pot_capital=pot_capital,
            trades=trades,
            points=points,
            spy_closes=spy_closes,
            endpoint=endpoint,
        ),
    }


__all__ = [
    "CloseFact",
    "EntryFacts",
    "ExecutedRebalance",
    "Fill",
    "OpenFact",
    "PositionFacts",
    "PotTrade",
    "SnapshotPoint",
    "StoredMark",
    "entry_row",
    "executed",
    "executed_vs_shadow",
    "fill_of",
    "nav_vs_spy",
    "net_at",
]
