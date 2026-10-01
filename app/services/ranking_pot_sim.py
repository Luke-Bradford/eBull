"""Ranking-pot-v1 pure simulator and order-reassignment controls (#2842 slice 3).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §9.1 (books, step function, statistic T) and §9.2
(the control draw, Phipson & Smyth p-values). The shadow and every control are a ``BookState`` advanced one session
at a time by ``step`` from that session's stored bars; slice 6's job appends each ``StepResult`` and the appended
ledger is authoritative.

Arithmetic is ``Decimal`` under the module's fixed context ``CTX`` (34 digits, half-even, traps on), never float, so a
replay on any machine reproduces every figure. Exact rationals were rejected: chained ``units = cash / fill`` over
hundreds of sessions grows their denominators without bound.

Fixed here by construction (each closes a spec Appendix A item; the PR lists them):

- **T (r3-14/15/16).** One record per position per session it is open at any instant of that session's trading:
  the entry session (start = the slot cash invested, end = units × close, so it carries the entry cost alone), every
  later session including a missing-bar one (start = the previous end, end = units × close or the last close), and
  the exit session (end = the exit proceeds, so it carries the exit cost). T = the equal-weight mean of
  ``end / start − 1``. At a look, a still-open position's endpoint record is replaced by its liquidation-charged value
  (``liquidation_charged``), a separate valuation that never touches the ledger (r3-28).
- **Bars (r3-37, r3-48).** Raw stored prices, the same basis as the snapshot (no dividend adjustment: price-only).
  A bar counts only with all four prices finite and > 0, ``high ≥ low`` and open/close inside [low, high].
- **Missing target-session bar (r3-38, r3-39).** An entry whose session has no valid bar is refused in the book
  (``entry_bar_missing``); the slot stays cash until the next rebalance. A stamped exit whose session has no valid
  bar is carried to the first later session with one, or to the 5th consecutive missing session's last-close exit.
- **Splits (r3-49..53).** Each step compares the CURRENT stored close of the session the ledger last marked (the
  caller's ``reference_closes``) with the ledger's own close. A ratio ``k ≠ 1`` rescales prices and levels by ``k``
  and units by ``1/k``, preserving value; the ledger then holds the new close for that session, so it is applied
  once (r3-52). A pending entry's snapshot ATR14 is rescaled by the same rule (r3-53). Residuals, stated: a provider
  that does not rewrite the old close goes undetected (r3-51), and an ordinary correction is absorbed as a
  value-preserving rebase (r3-50).
- **Slots (r3-55, r3-56).** Fills are always > 0, so a slot's cash can never reach ≤ 0 and retirement is
  unreachable; a slot with no positive cash is simply never filled. A refused entry leaves its would-be slot cash;
  the next planned entrant is NOT substituted. Entries take the lowest free slot in entry order.
- **Event order on a session (r3-57).** Split check → protective checks (positions entered before this session) →
  stamped exits at the close → entries at the close. Entries take only slots that were free before the step or
  that a DUE stamped exit released (however it closed); a slot freed by an unplanned protective or missing-bar exit
  stays cash until the next rebalance. The decision is never recomputed.
- **h_exit (r3-44).** A position's exit half-spread is replaced only when ``apply_rebalance`` hands in a valid one
  from that rebalance's snapshot, so a step never reads a later snapshot's quote.
- **Draw (r3-6, r3-7).** Fisher–Yates over S₀ in ascending ``instrument_id``, ``j`` from ``n − 1`` down to 1, swapping
  position ``j`` with a uniform index in ``[0, j]``. Each index is ``int.from_bytes(sha256(seed ‖ k ‖ j ‖ c), "big")``
  with ``k``, ``j`` and the retry counter ``c`` each 4-byte big-endian, accepted when below the largest multiple of
  ``j + 1`` not above 2²⁵⁶ (rejection sampling, so exactly uniform given uniform sha256 output), then taken mod
  ``j + 1``. Recipient ``i``-th smallest id takes the ``i``-th shuffled id as donor.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, InvalidOperation, Overflow, localcontext
from fractions import Fraction
from typing import Final, Literal

from app.services.market_calendar import us_market_status

#: §9.1 — by construction.
MISSING_EXIT_RUN: Final = 5
ATR_STOP_MULTIPLE: Final = 3
TARGET_ATR_MULTIPLE: Final = 6  # 2R at a 3-ATR stop
#: §9.2.
K_CONTROLS: Final = 9_999
_DRAW_SPACE: Final = 1 << 256
CTX: Final = Context(prec=34, rounding=ROUND_HALF_EVEN, traps=[InvalidOperation, DivisionByZero, Overflow])

EntryRefusalReason = Literal[
    "entry_bar_missing", "entry_session_missed", "no_free_slot", "name_held", "protective_levels_invalid"
]


@dataclass(frozen=True)
class Bar:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


def next_session(d: date) -> date:
    """The first NYSE session after ``d``."""
    d += timedelta(days=1)
    while us_market_status(d) == "closed":
        d += timedelta(days=1)
    return d


def valid_bar(bar: Bar | None) -> bool:
    """r3-37: every price finite and > 0, ``high ≥ low``, open and close inside [low, high]. Finiteness is checked
    before any ordering, because ordering a NaN ``Decimal`` raises."""
    if bar is None:
        return False
    prices = (bar.open, bar.high, bar.low, bar.close)
    if not all(isinstance(p, Decimal) and p.is_finite() and p > 0 for p in prices):
        return False
    return bar.low <= bar.high and bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high


def _positive_finite(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


# ---------------------------------------------------------------------------
# Book state
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Position:
    instrument_id: int
    lifecycle: int
    slot: int
    units: Decimal
    entry_session: date
    entry_fill: Decimal
    invested: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    last_close: Decimal
    last_close_session: date
    #: units × the last mark; the start value of the next session's record.
    value: Decimal
    missing_run: int
    h_exit: Decimal
    #: (stamp session, reason) once a rebalance or the wind-down stamps an exit; immutable once set.
    exit_stamp: tuple[date, str] | None = None


@dataclass(frozen=True)
class PendingEntry:
    """An ``enter`` decision for ``session`` (the rebalance's target session), in entry order."""

    instrument_id: int
    session: date
    h: Decimal
    atr14: Decimal
    snapshot_close: Decimal
    snapshot_session: date


@dataclass(frozen=True)
class BookState:
    n: int
    #: Cash per slot; a slot holding a position has 0.
    cash: tuple[Decimal, ...]
    positions: tuple[Position, ...]
    pending: tuple[PendingEntry, ...]
    next_lifecycle: int
    last_session: date | None

    def held(self) -> frozenset[int]:
        return frozenset(p.instrument_id for p in self.positions)


@dataclass(frozen=True)
class PositionSession:
    instrument_id: int
    lifecycle: int
    session: date
    start_value: Decimal
    end_value: Decimal

    def net_return(self) -> Decimal:
        with localcontext(CTX):
            return self.end_value / self.start_value - 1


@dataclass(frozen=True)
class ClosedLifecycle:
    instrument_id: int
    lifecycle: int
    slot: int
    entry_session: date
    exit_session: date
    entry_fill: Decimal
    exit_fill: Decimal
    invested: Decimal
    proceeds: Decimal
    reason: str


@dataclass(frozen=True)
class EntryRefusal:
    instrument_id: int
    session: date
    reason: EntryRefusalReason


@dataclass(frozen=True)
class StepResult:
    state: BookState
    position_sessions: tuple[PositionSession, ...]
    closed: tuple[ClosedLifecycle, ...]
    refusals: tuple[EntryRefusal, ...]
    #: (instrument_id, k) for every basis rescale applied this session.
    rescales: tuple[tuple[int, Decimal], ...]
    nav: Decimal


def new_book(n: int) -> BookState:
    """N slots at 1/N each; NAV 1.0 before the first entry."""
    if n <= 0:
        raise ValueError("n must be positive")
    with localcontext(CTX):
        share = Decimal(1) / n
    return BookState(n=n, cash=(share,) * n, positions=(), pending=(), next_lifecycle=1, last_session=None)


def nav(state: BookState) -> Decimal:
    with localcontext(CTX):
        return sum(state.cash, Decimal(0)) + sum((p.value for p in state.positions), Decimal(0))


def apply_rebalance(
    state: BookState,
    *,
    target_session: date,
    exits: Mapping[int, str],
    entries: Sequence[PendingEntry],
    h_exit: Mapping[int, Decimal],
) -> BookState:
    """Record one rebalance's decision for this book: stamp ``exits`` (id → reason) for ``target_session``, queue
    ``entries`` (entry order), and replace held positions' exit half-spreads with this snapshot's valid ones."""
    held = state.held()
    if not set(exits) <= held:
        raise ValueError("an exit for a name the book does not hold")
    if state.pending:
        raise ValueError("a previous rebalance's entries were never stepped")
    if any(e.session != target_session for e in entries):
        raise ValueError("an entry for another session")
    ids = [e.instrument_id for e in entries]
    if len(set(ids)) != len(ids) or set(ids) & held:
        raise ValueError("entries must be distinct names the book does not hold")
    positions = []
    for p in state.positions:
        if p.instrument_id in exits and p.exit_stamp is None:
            p = replace(p, exit_stamp=(target_session, exits[p.instrument_id]))
        h = h_exit.get(p.instrument_id)
        if h is not None and h.is_finite() and h >= 0:
            p = replace(p, h_exit=h)
        positions.append(p)
    return replace(state, positions=tuple(positions), pending=tuple(entries))


def wind_down(state: BookState, *, session: date) -> BookState:
    """§5.1 rule 3: stamp every unstamped position for ``session`` (reason ``wind_down``) and drop pending entries."""
    positions = tuple(
        p if p.exit_stamp is not None else replace(p, exit_stamp=(session, "wind_down")) for p in state.positions
    )
    return replace(state, positions=positions, pending=())


# ---------------------------------------------------------------------------
# §9.1 step
# ---------------------------------------------------------------------------
def _protective_fill(p: Position, bar: Bar) -> tuple[Decimal, str] | None:
    """§9.1 gap ordering: open ≤ SL → open; open ≥ TP → open; else low ≤ SL → SL (also when high ≥ TP the same
    session); else high ≥ TP → TP. Returns the pre-cost price and the reason."""
    if bar.open <= p.stop_loss:
        return bar.open, "stop_loss_gap"
    if bar.open >= p.take_profit:
        return bar.open, "take_profit_gap"
    if bar.low <= p.stop_loss:
        return p.stop_loss, "stop_loss"
    if bar.high >= p.take_profit:
        return p.take_profit, "take_profit"
    return None


def _rescale(p: Position, k: Decimal) -> Position:
    return replace(
        p,
        units=p.units / k,
        entry_fill=p.entry_fill * k,
        stop_loss=p.stop_loss * k,
        take_profit=p.take_profit * k,
        last_close=p.last_close * k,
    )


def step(
    state: BookState,
    *,
    session: date,
    bars: Mapping[int, Bar | None],
    reference_closes: Mapping[int, Decimal | None],
) -> StepResult:
    """Advance the book through ``session``, which must be the NYSE session after the last stepped one (r3-42): a
    skipped session could never be inserted later, so its records would be silently lost.

    ``bars``: the stored bar for ``session`` per held or entering name (absent or ``None`` = missing).
    ``reference_closes``: per held name, the CURRENT stored close of its ``last_close_session``; per pending entry,
    the current stored close of its ``snapshot_session``. Absent or invalid → no split check for that name.
    """
    if us_market_status(session) == "closed":
        raise ValueError("not an NYSE session")
    if state.last_session is not None and session != next_session(state.last_session):
        raise ValueError("each step must be the NYSE session after the last stepped one")
    with localcontext(CTX):
        return _step(state, session, bars, reference_closes)


def _step(
    state: BookState, session: date, bars: Mapping[int, Bar | None], reference_closes: Mapping[int, Decimal | None]
) -> StepResult:
    rescales: list[tuple[int, Decimal]] = []
    records: list[PositionSession] = []
    closed: list[ClosedLifecycle] = []
    refusals: list[EntryRefusal] = []
    cash = list(state.cash)
    #: Slots a same-session entry may take: free before the step, or released by a DUE stamped exit (the slot the
    #: decision planned on). A slot freed by an unplanned protective or missing-bar exit stays cash (r3-57).
    entry_slots = {i for i in range(state.n) if cash[i] > 0}

    def basis_ratio(iid: int, ledger_close: Decimal) -> Decimal | None:
        ref = reference_closes.get(iid)
        if not _positive_finite(ref) or ref == ledger_close:
            return None
        assert ref is not None
        return ref / ledger_close

    kept: list[Position] = []
    for p in sorted(state.positions, key=lambda q: q.instrument_id):
        k = basis_ratio(p.instrument_id, p.last_close)
        if k is not None:
            p = _rescale(p, k)
            rescales.append((p.instrument_id, k))
        bar = bars.get(p.instrument_id)
        exit_price: Decimal | None = None
        reason = ""
        if valid_bar(bar):
            assert bar is not None
            hit = _protective_fill(p, bar) if p.entry_session < session else None
            if hit is not None:
                exit_price, reason = hit
            elif p.exit_stamp is not None and p.exit_stamp[0] <= session:
                exit_price, reason = bar.close, p.exit_stamp[1]
            else:
                value = p.units * bar.close
                records.append(PositionSession(p.instrument_id, p.lifecycle, session, p.value, value))
                kept.append(replace(p, value=value, last_close=bar.close, last_close_session=session, missing_run=0))
                continue
        else:
            run = p.missing_run + 1
            if run >= MISSING_EXIT_RUN:
                exit_price, reason = p.last_close, "missing_bars"
            else:
                records.append(PositionSession(p.instrument_id, p.lifecycle, session, p.value, p.value))
                kept.append(replace(p, missing_run=run))
                continue
        fill = exit_price * (1 - p.h_exit)
        proceeds = p.units * fill
        records.append(PositionSession(p.instrument_id, p.lifecycle, session, p.value, proceeds))
        closed.append(
            ClosedLifecycle(
                p.instrument_id,
                p.lifecycle,
                p.slot,
                p.entry_session,
                session,
                p.entry_fill,
                fill,
                p.invested,
                proceeds,
                reason,
            )
        )
        cash[p.slot] = proceeds
        if p.exit_stamp is not None and p.exit_stamp[0] <= session:
            entry_slots.add(p.slot)

    occupied = {p.slot for p in kept}
    next_lifecycle = state.next_lifecycle
    for e in state.pending:
        if e.session != session:
            refusals.append(EntryRefusal(e.instrument_id, session, "entry_session_missed"))
            continue
        bar = bars.get(e.instrument_id)
        if not valid_bar(bar):
            refusals.append(EntryRefusal(e.instrument_id, session, "entry_bar_missing"))
            continue
        assert bar is not None
        if any(p.instrument_id == e.instrument_id for p in kept):
            refusals.append(EntryRefusal(e.instrument_id, session, "name_held"))
            continue
        free = sorted(i for i in entry_slots if i not in occupied and cash[i] > 0)
        if not free:
            refusals.append(EntryRefusal(e.instrument_id, session, "no_free_slot"))
            continue
        atr = e.atr14
        k = basis_ratio(e.instrument_id, e.snapshot_close)
        if k is not None:
            atr = atr * k
            rescales.append((e.instrument_id, k))
        fill = bar.close * (1 + e.h)
        stop_loss = fill - ATR_STOP_MULTIPLE * atr
        if not (atr > 0 and stop_loss > 0):
            refusals.append(EntryRefusal(e.instrument_id, session, "protective_levels_invalid"))
            continue
        slot = free[0]
        invested = cash[slot]
        units = invested / fill
        value = units * bar.close
        records.append(PositionSession(e.instrument_id, next_lifecycle, session, invested, value))
        kept.append(
            Position(
                instrument_id=e.instrument_id,
                lifecycle=next_lifecycle,
                slot=slot,
                units=units,
                entry_session=session,
                entry_fill=fill,
                invested=invested,
                stop_loss=stop_loss,
                take_profit=fill + TARGET_ATR_MULTIPLE * atr,
                last_close=bar.close,
                last_close_session=session,
                value=value,
                missing_run=0,
                h_exit=e.h,
            )
        )
        cash[slot] = Decimal(0)
        occupied.add(slot)
        next_lifecycle += 1

    new_state = BookState(
        n=state.n,
        cash=tuple(cash),
        positions=tuple(sorted(kept, key=lambda q: q.instrument_id)),
        pending=(),
        next_lifecycle=next_lifecycle,
        last_session=session,
    )
    return StepResult(new_state, tuple(records), tuple(closed), tuple(refusals), tuple(rescales), nav(new_state))


# ---------------------------------------------------------------------------
# Statistic and look valuation
# ---------------------------------------------------------------------------
def t_statistic(records: Iterable[PositionSession]) -> Decimal | None:
    """§9.1 T: the mean of ``end / start − 1`` over every position-session record; ``None`` when there is none."""
    with localcontext(CTX):
        returns = [r.net_return() for r in records]
        if not returns:
            return None
        return sum(returns, Decimal(0)) / len(returns)


def liquidation_charged(state: BookState, endpoint_records: Sequence[PositionSession]) -> tuple[PositionSession, ...]:
    """The endpoint session's records with every still-open position valued at ``value × (1 − h_exit)`` (r3-16).
    A separate valuation: the state and the stored ledger are unchanged (r3-28)."""
    if state.last_session is None:
        raise ValueError("the book has not been stepped")
    charge = {p.instrument_id: (p.lifecycle, p.h_exit) for p in state.positions}
    out: list[PositionSession] = []
    with localcontext(CTX):
        for r in endpoint_records:
            if r.session != state.last_session:
                raise ValueError("endpoint records must be the last stepped session's")
            hit = charge.get(r.instrument_id)
            if hit is not None and hit[0] == r.lifecycle:
                r = replace(r, end_value=r.end_value * (1 - hit[1]))
            out.append(r)
    return tuple(out)


def charged_nav(state: BookState) -> Decimal:
    """NAV with every open position charged its exit half-spread (§9.3 look valuation)."""
    with localcontext(CTX):
        return sum(state.cash, Decimal(0)) + sum((p.value * (1 - p.h_exit) for p in state.positions), Decimal(0))


# ---------------------------------------------------------------------------
# §9.2 controls
# ---------------------------------------------------------------------------
def control_seed(declaration_sha256: str, first_snapshot_sha256: str) -> bytes:
    """seed = sha256(declaration sha ‖ first rebalance snapshot sha), both as their 32 raw bytes."""
    decl, snap = bytes.fromhex(declaration_sha256), bytes.fromhex(first_snapshot_sha256)
    if len(decl) != 32 or len(snap) != 32:
        raise ValueError("both digests must be sha256 hex")
    return hashlib.sha256(decl + snap).digest()


def _uniform_index(seed: bytes, k: int, j: int) -> int:
    """A uniform integer in [0, j] by rejection sampling over sha256(seed ‖ k ‖ j ‖ c)."""
    bound = j + 1
    limit = _DRAW_SPACE - (_DRAW_SPACE % bound)
    c = 0
    while True:
        digest = hashlib.sha256(seed + k.to_bytes(4, "big") + j.to_bytes(4, "big") + c.to_bytes(4, "big")).digest()
        x = int.from_bytes(digest, "big")
        if x < limit:
            return x % bound
        c += 1


def draw_bijection(s0_ids: Iterable[int], *, seed: bytes, k: int) -> dict[int, int]:
    """Control ``k``'s uniform random bijection of S₀: recipient → donor."""
    if not 1 <= k <= K_CONTROLS:
        raise ValueError("k must be in 1..K")
    recipients = sorted(set(s0_ids))
    donors = list(recipients)
    for j in range(len(donors) - 1, 0, -1):
        i = _uniform_index(seed, k, j)
        donors[i], donors[j] = donors[j], donors[i]
    return dict(zip(recipients, donors, strict=True))


@dataclass(frozen=True)
class PValues:
    p_up: Fraction
    p_down: Fraction


def p_values(t_obs: Decimal | None, t_controls: Sequence[Decimal | None]) -> PValues | Literal["unevaluable"]:
    """Phipson & Smyth (2010): ``(1 + #{T_k ≥ T_obs}) / (K + 1)`` and its mirror. A control with no T or a non-finite
    T counts toward BOTH tails; a missing or non-finite T_obs makes the look unevaluable."""
    if t_obs is None or not t_obs.is_finite():
        return "unevaluable"
    up = down = 0
    for t in t_controls:
        if t is None or not t.is_finite():
            up += 1
            down += 1
            continue
        up += t >= t_obs
        down += t <= t_obs
    total = len(t_controls) + 1
    return PValues(Fraction(1 + up, total), Fraction(1 + down, total))


__all__ = [
    "CTX",
    "K_CONTROLS",
    "MISSING_EXIT_RUN",
    "Bar",
    "BookState",
    "ClosedLifecycle",
    "EntryRefusal",
    "PValues",
    "PendingEntry",
    "Position",
    "PositionSession",
    "StepResult",
    "apply_rebalance",
    "charged_nav",
    "control_seed",
    "draw_bijection",
    "liquidation_charged",
    "nav",
    "new_book",
    "next_session",
    "p_values",
    "step",
    "t_statistic",
    "valid_bar",
    "wind_down",
]
