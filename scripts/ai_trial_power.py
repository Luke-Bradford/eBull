"""#3471 §9 planning table, v6 grid (slice v6-4c, O-v6-5) — simulated forward trial paths.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §9 "v6 grid" (the
authoritative text; every rule below is its, by construction) and §10. A rough planning aid, NOT a
power guarantee, and it never calls the model.

One replicate is one forward trial path on real stored daily bars:

- a start session s₀ and a 50-name pseudo-shortlist drawn from the point-in-time universe (§16.5's
  ``in_universe``) at s₀'s signal session, fixed for the path;
- each signal session, up to 2 arm decisions: a uniform random evaluable name with a detected setup
  and a feasible ``library_plan`` at the cell's horizon (O-v6-5's comparator — no selection skill),
  its setup uniform over the detected ones, its level ids the library plan's;
- the control through the trial's own ``control_pool`` and ``draw_control``;
- fills at the next session's open (standing in for the 15:00 ask/bid): ``no_fill`` →
  ``plan_invalidated`` → ``capacity`` (``TRIAL_MAX_CONCURRENT_PER_LEG``), exits by the executor's
  ``protective_rates``; the walk stops at the stop, the target, the deadline close, the next
  executable open after a missing deadline bar, or §9's censor clock;
- the §9 loss halt per leg on a daily-bar upper bound, the harm stop through
  ``ai_trial_readout.harm_looks`` and the cohort readout through ``ai_trial_readout.primary``.

δ (pp) is added to the arm's per-trade net % for *d* only; dollars and the loss halt never see it.
Outcomes are exclusive per path — ``halted_harm``, ``insufficient``, ``reject``, ``not_reject`` —
with every path in the denominator; ``halted_loss`` and ``halted_loss_below_minimum`` are reported
beside them.

⚠ Caveats (§9 "v6 grid"): a noise model (the arm is a random library-plan pick); the population is
not the ranked shortlist; 2 decisions every session is a throughput ceiling; open-as-ask ignores the
spread and the 15:00 fill; the 0.30% tariff is not the realised cost and no dividends, fees or
financing are modelled; corporate actions enter only as the back-adjusted series carries them; the
loss halt is a daily-bar upper bound; a share is the probability of an outcome under this model, not
of the live trial's verdict.

Usage (background mode, output to a file, never piped)::

    PYTHONPATH=. uv run python -m scripts.ai_trial_power [--replicates 400] [--seed 3471] \
        [--workers 8] [--json out.json] > /tmp/ai_trial_power.log 2>&1
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import os
import statistics
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Final, Literal

import numpy as np
import numpy.typing as npt
import psycopg

from app.services.ai_trial_deadline import exit_deadline_session
from app.services.ai_trial_decision import HORIZON_SESSIONS, ControlPoolExhausted, draw_control, exact
from app.services.ai_trial_executor import TRIAL_MAX_CONCURRENT_PER_LEG
from app.services.ai_trial_guard import control_pool, library_baseline
from app.services.ai_trial_halts import TRIAL_LEG_CAPITAL_USD, TRIAL_LOSS_HALT_PCT
from app.services.ai_trial_intent import TRIAL_TICKET_USD
from app.services.ai_trial_levels import SETUP_TYPES
from app.services.ai_trial_pack import INDICATOR_BARS, SMALL_CAP_N, TOP_N
from app.services.ai_trial_pack_reader import load_setup_library
from app.services.ai_trial_pair_lifecycle import TRIAL_CENSOR_SESSIONS
from app.services.ai_trial_plan import LibraryPlan, derive_plan, library_plan
from app.services.ai_trial_readout import (
    FLOW_WINDOW_SESSIONS,
    HARM_LOOK_EVERY,
    INSUFFICIENT,
    READOUT_WAIT_SESSIONS,
    LegValue,
    PairRecord,
    harm_looks,
    primary,
    readout_seed,
)
from app.services.market_calendar import us_market_status
from app.services.strategy_paper_executor import protective_rates
from scripts.ai_trial_setup_base_rates import (
    COST_ROUND_TRIP,
    MIN_PRIOR_BARS,
    Bar,
    Evaluation,
    evaluate_at,
    in_universe,
)

WINDOW_START: Final = date(2024, 1, 2)
WINDOW_END: Final = date(2026, 6, 30)
#: History load start: 260 + ~130 sessions before ``WINDOW_START``, so every window the sim reads
#: (≤ 261 bars ending at or after s₀'s signal session) lies after it unless a name misses > 130
#: sessions — the only way the cut can differ from a full-history load.
HISTORY_START: Final = date(2022, 6, 1)
SHORTLIST_SIZE: Final = TOP_N + SMALL_CAP_N
DECISIONS_PER_SESSION: Final = 2
ENROLLMENT_SESSIONS: Final = 40
#: Fill sessions s₀ … s₀ + START_ALLOWANCE may open enrollment; a path with no fill by then is ``no_start``.
START_ALLOWANCE: Final = 20
#: s₀ + PATH_SESSIONS ≤ the frontier: the start allowance, 40 enrollment sessions, a 20-session hold,
#: the 10-session censor clock and the 6-session readout wait (96), rounded up.
PATH_SESSIONS: Final = 100
SHIFTS_PCT: Final = (0.0, 1.0, 2.0, 3.0, 5.0)
ALPHA: Final = 0.05
TICKET_USD: Final = float(TRIAL_TICKET_USD["full"])
LOSS_LIMIT_USD: Final = float(TRIAL_LOSS_HALT_PCT / 100 * TRIAL_LEG_CAPITAL_USD)
TARIFF: Final = float(COST_ROUND_TRIP)
TARIFF_PCT: Final = 100 * TARIFF
#: Clusters below the cohort minimum make ``primary`` return ``INSUFFICIENT`` (§9).
LegName = Literal["arm", "control"]
LEGS: Final[tuple[LegName, ...]] = ("arm", "control")
Outcome = Literal["halted_harm", "insufficient", "reject", "not_reject"]
OUTCOMES: Final[tuple[Outcome, ...]] = ("halted_harm", "insufficient", "reject", "not_reject")
ExitLabel = Literal["stop", "target", "deadline", "censored"]


# ---------------------------------------------------------------------------
# Bars and names
# ---------------------------------------------------------------------------
def _positive(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def valid_open(bar: Bar) -> bool:
    return _positive(bar[0])


def valid_bar(bar: Bar) -> bool:
    """Open, high, low and close finite and > 0, low ≤ min(open, close), high ≥ max(open, close)."""
    o, h, lo, c, _ = bar
    if not (_positive(o) and _positive(h) and _positive(lo) and _positive(c)):
        return False
    assert o is not None and h is not None and lo is not None and c is not None
    return lo <= min(o, c) and h >= max(o, c)


@dataclass(frozen=True)
class Name:
    """One instrument's NYSE-cut stored history, compact: float64 OHLCV (NaN = missing) on the
    shared calendar's positions. ``bar`` gives back ``Decimal(repr(float))``, which ``load_names``
    asserts equals the stored ``numeric`` value for every price it reads."""

    instrument_id: int
    #: Calendar positions of the bars, ascending.
    positions: npt.NDArray[np.int64]
    ohlcv: npt.NDArray[np.float64]
    #: Every break date, resolved or not, ascending (the §16.3 submission check).
    breaks: tuple[date, ...] = ()
    #: Per bar: its segment's first index and (exclusive) end, split at unresolved breaks.
    segment_start: npt.NDArray[np.int64] = field(default_factory=lambda: np.zeros(0, np.int64), compare=False)
    segment_end: npt.NDArray[np.int64] = field(default_factory=lambda: np.zeros(0, np.int64), compare=False)

    def index_of(self, position: int) -> int | None:
        i = int(np.searchsorted(self.positions, position))
        return i if i < len(self.positions) and self.positions[i] == position else None

    def bar(self, i: int) -> Bar:
        o, h, lo, c, v = (None if math.isnan(x) else x for x in self.ohlcv[i].tolist())
        return (
            None if o is None else Decimal(repr(o)),
            None if h is None else Decimal(repr(h)),
            None if lo is None else Decimal(repr(lo)),
            None if c is None else Decimal(repr(c)),
            None if v is None else int(v),
        )


def make_name(
    instrument_id: int,
    positions: Sequence[int],
    bars: Sequence[Bar],
    *,
    breaks: Sequence[date] = (),
    unresolved_breaks: Sequence[int] = (),
) -> Name:
    """``unresolved_breaks`` as bar indexes: the first bar at each new scale."""
    starts = np.zeros(len(positions), np.int64)
    ends = np.full(len(positions), len(positions), np.int64)
    cuts = [0, *sorted(set(unresolved_breaks)), len(positions)]
    for a, b in zip(cuts, cuts[1:], strict=False):
        starts[a:b], ends[a:b] = a, b
    ohlcv = np.array([[math.nan if x is None else float(x) for x in bar] for bar in bars], dtype=np.float64).reshape(
        len(bars), 5
    )
    return Name(instrument_id, np.asarray(positions, np.int64), ohlcv, tuple(breaks), starts, ends)


def in_shortlist_universe(name: Name, i: int) -> bool:
    """§16.5's point-in-time universe at bar ``i``, inside its segment, on a valid bar."""
    start = int(name.segment_start[i])
    if not valid_bar(name.bar(i)) or i - start < MIN_PRIOR_BARS:
        return False
    lo = max(start, i - MIN_PRIOR_BARS)
    return in_universe([name.bar(j) for j in range(lo, i + 1)], i - lo)


def evaluate(name: Name, i: int, calendar: Sequence[date]) -> Evaluation | None:
    """The pack's view at bar ``i``: a valid bar and ≥ ``INDICATOR_BARS`` bars in its segment."""
    if not valid_bar(name.bar(i)) or i - int(name.segment_start[i]) + 1 < INDICATOR_BARS:
        return None
    lo = i - INDICATOR_BARS + 1
    dates = [calendar[int(p)] for p in name.positions[lo : i + 1]]
    return evaluate_at(dates, [name.bar(j) for j in range(lo, i + 1)], INDICATOR_BARS - 1)


# ---------------------------------------------------------------------------
# One leg: fill, walk, value
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class LegPlan:
    instrument_id: int
    invalidation_price: Fraction
    target_price: Fraction
    stop_price: Fraction
    stop_pct: Decimal
    target_pct: Decimal


@dataclass(frozen=True)
class Exit:
    price: Decimal
    session: date
    label: ExitLabel
    #: (session, low) of every valid bar read, ascending — the loss marks.
    lows: tuple[tuple[date, Decimal], ...]


def walk(name: Name, fill: int, horizon: int, stop: Decimal, target: Decimal, calendar: Sequence[date]) -> Exit:
    """The §9 v6 walk over the fill bar's segment, from the fill session to the exit."""

    def day(i: int) -> date:
        return calendar[int(name.positions[i])]

    end = int(name.segment_end[fill])
    deadline = exit_deadline_session(day(fill), horizon)
    censor = exit_deadline_session(deadline, TRIAL_CENSOR_SESSIONS)
    lows: list[tuple[date, Decimal]] = []
    i = fill
    while i < end and day(i) <= deadline:
        bar = name.bar(i)
        if valid_bar(bar):
            o, h, lo, c, _ = bar
            assert o is not None and h is not None and lo is not None and c is not None
            lows.append((day(i), lo))
            if i > fill and (o <= stop or o >= target):
                return Exit(o, day(i), "stop" if o <= stop else "target", tuple(lows))
            if lo <= stop:
                return Exit(stop, day(i), "stop", tuple(lows))
            if h >= target:
                return Exit(target, day(i), "target", tuple(lows))
            if day(i) == deadline:
                return Exit(c, deadline, "deadline", tuple(lows))
        i += 1
    # No valid deadline bar: the engine's deadline close executes at the next executable price.
    while i < end and day(i) <= censor:
        bar = name.bar(i)
        if valid_bar(bar):
            assert bar[0] is not None and bar[2] is not None
            lows.append((day(i), bar[2]))
            return Exit(bar[0], day(i), "deadline", tuple(lows))
        i += 1
    # §9 censor: the latest valid close in the segment dated on or before the censor session.
    start = int(name.segment_start[fill])
    j = i - 1
    while j >= start and not valid_bar(name.bar(j)):
        j -= 1
    if j < start:
        raise RuntimeError(f"instrument {name.instrument_id}: no valid close in the fill bar's segment")
    close = name.bar(j)[3]
    assert close is not None
    return Exit(close, censor, "censored", tuple(lows))


@dataclass(frozen=True)
class Trade:
    instrument_id: int
    fill_session: date
    entry: Decimal
    exit: Exit
    #: Unshifted: 100 × (exit − entry) ÷ entry − the tariff.
    net_pct: float

    def low_on(self, session: date) -> Decimal:
        """The session's low; else the latest earlier low since the fill; else the entry."""
        k = bisect.bisect_right(self.exit.lows, (session, Decimal("Infinity"))) - 1
        return self.exit.lows[k][1] if k >= 0 else self.entry


LegRefusal = Literal["no_fill", "plan_invalidated", "capacity"]


def open_leg(
    name: Name,
    signal: date,
    fill: date,
    plan: LegPlan,
    horizon: int,
    *,
    calendar: Sequence[date],
    fill_position: int,
    slots_held: int,
) -> Trade | LegRefusal:
    """Per leg, the first failure named: ``no_fill`` → ``plan_invalidated`` → ``capacity``."""
    i = name.index_of(fill_position)
    if i is None or not valid_open(name.bar(i)):
        return "no_fill"
    entry = name.bar(i)[0]
    assert entry is not None
    o = exact(entry)
    if (
        any(signal < b <= fill for b in name.breaks)
        or o <= plan.stop_price
        or o >= plan.target_price
        or o <= plan.invalidation_price
    ):
        return "plan_invalidated"
    if slots_held >= TRIAL_MAX_CONCURRENT_PER_LEG:
        return "capacity"
    stop, target = protective_rates(entry, plan.stop_pct, plan.target_pct)
    exit_ = walk(name, i, horizon, stop, target, calendar)
    return Trade(name.instrument_id, fill, entry, exit_, float(100 * (exit_.price - entry) / entry) - TARIFF_PCT)


def leg_loss(trades: Sequence[Trade], session: date) -> float:
    """The §9 v6 daily-bar upper bound on one leg's loss at ``session`` (positive = loss)."""
    loss = 0.0
    for trade in trades:
        if trade.exit.session < session:
            loss -= TICKET_USD * trade.net_pct / 100
        elif trade.fill_session <= session:
            low = trade.low_on(session)
            if trade.exit.session == session:
                mark = min(low, trade.exit.price)
                loss += TICKET_USD * (float((trade.entry - mark) / trade.entry) + TARIFF)
            else:
                loss += TICKET_USD * float((trade.entry - low) / trade.entry)
    return loss


# ---------------------------------------------------------------------------
# One path
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Decision:
    pair_seq: int
    signal: date
    arm: LegPlan
    control: LegPlan
    pool_size: int


@dataclass
class SimPair:
    pair_seq: int
    session: date
    trades: dict[LegName, Trade]
    refusals: dict[LegName, LegRefusal]
    pool_size: int
    self_draw: bool


@dataclass
class Diagnostics:
    arm_decisions: int = 0
    control_pool_exhausted: int = 0
    setup_mix: Counter[str] = field(default_factory=Counter)
    pairs: int = 0
    refused_legs: Counter[str] = field(default_factory=Counter)
    broken_pairs: int = 0
    pool_sizes: list[int] = field(default_factory=list)
    self_draws: int = 0
    singleton_self: int = 0


@dataclass(frozen=True)
class PathResult:
    horizon: int
    shift: float
    outcome: Outcome
    units: int
    clusters: int
    no_start: bool
    halted_loss: bool
    pairs_at_loss_halt: int | None
    sum_d: float
    diagnostics: Diagnostics


@dataclass
class PathContext:
    """Everything one replicate shares across its horizons and shifts."""

    names: Mapping[int, Name]
    calendar: Sequence[date]
    position: Mapping[date, int]
    shortlist: tuple[int, ...]
    s0: int
    seed: int
    replicate: int
    evaluate: Callable[[int, date], Evaluation | None]
    plans: dict[tuple[int, date, int], LibraryPlan | None] = field(default_factory=dict)

    def library_plan(self, instrument_id: int, session: date, horizon: int, ev: Evaluation) -> LibraryPlan | None:
        key = (instrument_id, session, horizon)
        if key not in self.plans:
            self.plans[key] = library_plan(ev.levels, ev.atr, horizon_days=horizon)
        return self.plans[key]


def declaration_hex(seed: int, replicate: int, horizon: int) -> str:
    return hashlib.sha256(f"{seed}|{replicate}|{horizon}".encode()).hexdigest()


def _leg_plan(instrument_id: int, ev: Evaluation, invalidation_id: str, target_id: str, horizon: int) -> LegPlan | None:
    invalidation, target = ev.levels.get(invalidation_id), ev.levels.get(target_id)
    verdict = derive_plan(ev.atr, invalidation, target, horizon_days=horizon)
    f = verdict.figures
    if not verdict.feasible or invalidation is None or target is None:
        return None
    assert f.stop_price is not None and f.stop_pct is not None and f.target_pct is not None
    return LegPlan(instrument_id, invalidation.price, target.price, f.stop_price, f.stop_pct, f.target_pct)


def decide(
    ctx: PathContext,
    *,
    horizon: int,
    session: date,
    fill: date,
    holdings: Mapping[LegName, frozenset[int]],
    next_pair_seq: int,
    hex_: str,
    diagnostics: Diagnostics,
) -> list[Decision]:
    """The arm's up-to-2 decisions at ``session``'s close and their controls (§9 v6 "Arm", "Control")."""
    evaluable: list[tuple[int, Evaluation]] = []
    for iid in ctx.shortlist:
        ev = ctx.evaluate(iid, session)
        if ev is not None:
            evaluable.append((iid, ev))
    evaluable_ids = [iid for iid, _ in evaluable]
    ordinal = ctx.position[session] - (ctx.s0 - 1)
    chosen: set[int] = set()
    drawn: set[int] = set()
    decisions: list[Decision] = []
    for position in range(DECISIONS_PER_SESSION):
        rng = np.random.default_rng((ctx.seed, ctx.replicate, horizon, ordinal, position))
        candidates = [
            (iid, ev, plan)
            for iid, ev in evaluable
            if iid not in holdings["arm"]
            and iid not in chosen
            and ev.detected
            and (plan := ctx.library_plan(iid, session, horizon, ev)) is not None
        ]
        if not candidates:
            continue
        iid, ev, plan = candidates[int(rng.integers(len(candidates)))]
        chosen.add(iid)
        detected = [s for s in SETUP_TYPES if s in ev.detected]
        setup = detected[int(rng.integers(len(detected)))]
        diagnostics.arm_decisions += 1
        diagnostics.setup_mix[setup] += 1
        arm = _leg_plan(iid, ev, plan.invalidation_level_id, plan.target_level_id, horizon)
        assert arm is not None  # the library plan is feasible under the same orders 8–12
        controls = {
            cid: control
            for cid, cev in evaluable
            if setup in cev.detected
            and (control := _leg_plan(cid, cev, plan.invalidation_level_id, plan.target_level_id, horizon)) is not None
        }
        pool = control_pool(
            evaluable_ids,
            control_held_instrument_ids=holdings["control"],
            drawn_this_run=frozenset(drawn),
            feasible_instrument_ids=frozenset(controls),
        )
        pair_seq = next_pair_seq + len(decisions)
        try:
            draw = draw_control(declaration_sha256_hex=hex_, session_date=fill, pair_seq=pair_seq, pool=pool)
        except ControlPoolExhausted:
            diagnostics.control_pool_exhausted += 1
            continue
        drawn.add(draw.instrument_id)
        decisions.append(Decision(pair_seq, session, arm, controls[draw.instrument_id], len(pool)))
    return decisions


def pair_records(pairs: Sequence[SimPair], today: date, shift: float) -> list[PairRecord]:
    """The readout's view of the path as of ``today``: only exits known by then are valued."""
    records: list[PairRecord] = []
    for pair in pairs:
        values: dict[LegName, LegValue | None] = {}
        exits: list[date] = []
        live = 0
        for leg in LEGS:
            trade = pair.trades.get(leg)
            if trade is None or trade.exit.session > today:
                values[leg] = None
                live += trade is not None
                continue
            exits.append(trade.exit.session)
            net = trade.net_pct + (shift if leg == "arm" else 0.0)
            values[leg] = LegValue(
                net_pct=net,
                open_amount=TICKET_USD,
                pnl_usd=TICKET_USD * net / 100,
                exit_label=trade.exit.label,
                entry_session=trade.fill_session,
                exit_session=trade.exit.session,
            )
        records.append(
            PairRecord(
                pair_seq=pair.pair_seq,
                session_date=pair.session,
                state="unit" if len(pair.trades) == 2 else "broken",
                broken_reasons=tuple(pair.refusals.values()),
                arm=values["arm"],
                control=values["control"],
                regime_label=None,
                confidence=None,
                resolved_session=max(exits, default=None),
                live_legs=live,
                pool_size=pair.pool_size,
            )
        )
    return records


def _harm_key(records: Sequence[PairRecord], today: date) -> tuple[tuple[int, bool, int, bool], ...]:
    """Everything ``harm_looks`` reads that can change between sessions (a valued *d* is fixed)."""
    return tuple(
        (
            r.pair_seq,
            r.valued,
            r.live_legs,
            r.resolved_session is not None and today > exit_deadline_session(r.resolved_session, FLOW_WINDOW_SESSIONS),
        )
        for r in records
    )


def simulate_path(ctx: PathContext, *, horizon: int, shift: float) -> PathResult:
    """One forward trial path for one cell (§9 v6 "Order of a session")."""
    cal = ctx.calendar
    hex_ = declaration_hex(ctx.seed, ctx.replicate, horizon)
    seed = readout_seed(hex_)
    diagnostics = Diagnostics()
    pairs: list[SimPair] = []
    trades: dict[LegName, list[Trade]] = {"arm": [], "control": []}
    pending: list[Decision] = []
    first_fill: int | None = None
    last_fill_allowed = ctx.s0 + START_ALLOWANCE
    halted: Literal["loss", "harm"] | None = None
    pairs_at_loss_halt: int | None = None
    harm_key: object = None
    p = ctx.s0 - 1
    while True:
        session = cal[p]
        # 1. fills for the decisions taken at the previous close, ascending pair_seq.
        for decision in pending:
            pair = SimPair(decision.pair_seq, session, {}, {}, decision.pool_size, False)
            pair.self_draw = decision.arm.instrument_id == decision.control.instrument_id
            legs: tuple[tuple[LegName, LegPlan], ...] = (("arm", decision.arm), ("control", decision.control))
            for leg, plan in legs:
                held = sum(1 for t in trades[leg] if t.fill_session <= session <= t.exit.session)
                result = open_leg(
                    ctx.names[plan.instrument_id],
                    decision.signal,
                    session,
                    plan,
                    horizon,
                    calendar=cal,
                    fill_position=p,
                    slots_held=held,
                )
                if isinstance(result, Trade):
                    pair.trades[leg] = result
                    trades[leg].append(result)
                else:
                    pair.refusals[leg] = result
                    diagnostics.refused_legs[result] += 1
            pairs.append(pair)
            diagnostics.pairs += 1
            diagnostics.broken_pairs += len(pair.trades) < 2
            diagnostics.pool_sizes.append(decision.pool_size)
            diagnostics.self_draws += pair.self_draw
            diagnostics.singleton_self += decision.pool_size == 1 and pair.self_draw
            if pair.trades and first_fill is None:
                first_fill = p
        pending = []
        enrollment_end = (
            first_fill + ENROLLMENT_SESSIONS - 1
            if first_fill is not None
            else (last_fill_allowed if p >= last_fill_allowed else None)
        )
        # 3. loss, 4. harm — the first halt of either kind is terminal.
        if halted is None and any(leg_loss(trades[leg], session) >= LOSS_LIMIT_USD for leg in LEGS):
            halted = "loss"
            pairs_at_loss_halt = sum(1 for pair in pairs if len(pair.trades) == 2)
        records = pair_records(pairs, session, shift)
        if halted is None and sum(r.valued for r in records) >= HARM_LOOK_EVERY:
            key = _harm_key(records, session)
            if key != harm_key:
                harm_key = key
                if any(look.halts and look.flows_final for look in harm_looks(records, seed=seed, today=session)):
                    halted = "harm"
        # The readout session D, once no further entry can happen.
        if enrollment_end is not None and (p >= enrollment_end or halted is not None):
            anchor = max([cal[enrollment_end], *(t.exit.session for leg in LEGS for t in trades[leg])])
            if session >= exit_deadline_session(anchor, READOUT_WAIT_SESSIONS + 1):
                break
        # 5. decisions at the close, for a fill at the next session inside enrollment.
        fill_p = p + 1
        last_fill = enrollment_end if enrollment_end is not None and first_fill is not None else last_fill_allowed
        if halted is None and fill_p <= last_fill:
            holdings: dict[LegName, frozenset[int]] = {
                leg: frozenset(t.instrument_id for t in trades[leg] if t.fill_session <= session < t.exit.session)
                for leg in LEGS
            }
            pending = decide(
                ctx,
                horizon=horizon,
                session=session,
                fill=cal[fill_p],
                holdings=holdings,
                next_pair_seq=len(pairs),
                hex_=hex_,
                diagnostics=diagnostics,
            )
        p += 1
    units = [r for r in pair_records(pairs, session, shift) if r.state == "unit"]
    reading = primary(units, seed=seed)
    outcome: Outcome
    if halted == "harm":
        outcome = "halted_harm"
    elif reading.verdict == INSUFFICIENT:
        outcome = "insufficient"
    elif reading.p is not None and reading.p <= ALPHA:
        outcome = "reject"
    else:
        outcome = "not_reject"
    return PathResult(
        horizon=horizon,
        shift=shift,
        outcome=outcome,
        units=reading.units,
        clusters=reading.clusters,
        no_start=first_fill is None,
        halted_loss=halted == "loss",
        pairs_at_loss_halt=pairs_at_loss_halt,
        sum_d=sum(u.d for u in units),
        diagnostics=diagnostics,
    )


# ---------------------------------------------------------------------------
# Replicates
# ---------------------------------------------------------------------------
def nyse_sessions(start: date, end: date) -> list[date]:
    return [start + timedelta(days=n) for n in range((end - start).days + 1) if is_session(start + timedelta(days=n))]


def is_session(d: date) -> bool:
    return us_market_status(d) != "closed"


@dataclass(frozen=True)
class Panel:
    names: Mapping[int, Name]
    calendar: tuple[date, ...]
    starts: tuple[int, ...]


def build_panel(names: Mapping[int, Name], calendar: Sequence[date]) -> Panel:
    """``calendar`` = the NYSE sessions from ``HISTORY_START`` to the frontier, which every name's
    ``positions`` index."""
    starts = tuple(
        p
        for p, d in enumerate(calendar)
        if WINDOW_START <= d <= WINDOW_END and p >= 1 and p + PATH_SESSIONS < len(calendar)
    )
    if not starts:
        raise RuntimeError("no start session leaves room for a full path before the frontier")
    return Panel(names, tuple(calendar), starts)


def replicate_context(panel: Panel, *, seed: int, replicate: int) -> PathContext:
    rng = np.random.default_rng((seed, replicate))
    s0 = panel.starts[int(rng.integers(len(panel.starts)))]
    universe = sorted(
        iid
        for iid, name in panel.names.items()
        if (i := name.index_of(s0 - 1)) is not None and in_shortlist_universe(name, i)
    )
    size = min(SHORTLIST_SIZE, len(universe))
    shortlist = tuple(sorted(int(x) for x in rng.choice(np.array(universe, dtype=np.int64), size=size, replace=False)))
    position = {d: p for p, d in enumerate(panel.calendar)}
    cache: dict[tuple[int, date], Evaluation | None] = {}

    def evaluate_cached(instrument_id: int, session: date) -> Evaluation | None:
        key = (instrument_id, session)
        if key not in cache:
            name = panel.names[instrument_id]
            i = name.index_of(position[session])
            cache[key] = None if i is None else evaluate(name, i, panel.calendar)
        return cache[key]

    return PathContext(
        names=panel.names,
        calendar=panel.calendar,
        position=position,
        shortlist=shortlist,
        s0=s0,
        seed=seed,
        replicate=replicate,
        evaluate=evaluate_cached,
    )


def run_replicate(panel: Panel, *, seed: int, replicate: int) -> list[PathResult]:
    ctx = replicate_context(panel, seed=seed, replicate=replicate)
    return [simulate_path(ctx, horizon=h, shift=s) for h in HORIZON_SESSIONS for s in SHIFTS_PCT]


_PANEL: Panel | None = None


def _worker(args: tuple[int, int]) -> list[PathResult]:
    assert _PANEL is not None
    seed, replicate = args
    return run_replicate(_PANEL, seed=seed, replicate=replicate)


def assert_order_6() -> None:
    """Order 6 passes for every (setup, horizon): the frozen library rows meet its minimums."""
    rows = load_setup_library()["rows"]
    missing = [(s, h) for s in SETUP_TYPES for h in HORIZON_SESSIONS if library_baseline(rows, s, h) is None]
    if missing:
        raise RuntimeError(f"order 6 would refuse {missing}; the v6 grid assumes it never does")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def _quantiles(values: Sequence[int]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(values)

    def q(frac: float) -> float:
        return float(ordered[min(len(ordered) - 1, int(frac * len(ordered)))])

    return {"mean": statistics.fmean(ordered), "p10": q(0.1), "p50": q(0.5), "p90": q(0.9)}


def _share(k: int, n: int) -> dict[str, float | int | None]:
    if n == 0:
        return {"share": None, "se": None, "n": 0}
    p = k / n
    return {"share": p, "se": math.sqrt(p * (1 - p) / n), "n": n}


def _ratio(k: int, n: int) -> float | None:
    return None if n == 0 else k / n


def summarise(results: Sequence[PathResult]) -> dict[str, Any]:
    cells: dict[str, Any] = {}
    for h in HORIZON_SESSIONS:
        by_shift: dict[str, Any] = {}
        for s in SHIFTS_PCT:
            rs = [r for r in results if r.horizon == h and r.shift == s]
            n = len(rs)
            outcomes = Counter(r.outcome for r in rs)
            halted = [r for r in rs if r.halted_loss]
            by_shift[str(s)] = {
                "outcomes": {o: _share(outcomes[o], n) for o in OUTCOMES},
                "halted_loss": _share(len(halted), n),
                "halted_loss_below_minimum": _share(sum(r.outcome == "insufficient" for r in halted), n),
                "pairs_at_loss_halt": _quantiles([r.pairs_at_loss_halt or 0 for r in halted]),
                "no_start": _share(sum(r.no_start for r in rs), n),
                "units": _quantiles([r.units for r in rs]),
                "clusters": _quantiles([r.clusters for r in rs]),
            }
        base = [r for r in results if r.horizon == h and r.shift == 0.0]
        diag = [r.diagnostics for r in base]
        arm_decisions = sum(d.arm_decisions for d in diag)
        pairs = sum(d.pairs for d in diag)
        pool_sizes = [x for d in diag for x in d.pool_sizes]
        refused: Counter[str] = Counter()
        mix: Counter[str] = Counter()
        for d in diag:
            refused.update(d.refused_legs)
            mix.update(d.setup_mix)
        units = sum(r.units for r in base)
        cells[str(h)] = {
            "by_shift": by_shift,
            "delta0_mean_d_pct": None if units == 0 else sum(r.sum_d for r in base) / units,
            "delta0_units": units,
            "arm_decisions": arm_decisions,
            "control_pool_exhausted_per_arm_decision": _ratio(
                sum(d.control_pool_exhausted for d in diag), arm_decisions
            ),
            "setup_mix_per_arm_decision": {k: v / arm_decisions for k, v in sorted(mix.items())}
            if arm_decisions
            else None,
            "pairs": pairs,
            "broken_pairs_per_pair": _ratio(sum(d.broken_pairs for d in diag), pairs),
            "refused_legs_per_pair": {k: v / pairs for k, v in sorted(refused.items())} if pairs else None,
            "pool_size": _quantiles(pool_sizes),
            "self_draw_per_pair": _ratio(sum(d.self_draws for d in diag), pairs),
            "singleton_self_per_pair": _ratio(sum(d.singleton_self for d in diag), pairs),
        }
    return cells


def _fmt(value: float | int | None, spec: str = ".3f") -> str:
    return "n/a (0)" if value is None else format(value, spec)


def render(report: Mapping[str, Any]) -> str:
    lines = [
        f"#3471 §9 planning table, v6 grid — start window {report['window'][0]}..{report['window'][1]}, "
        f"frontier {report['frontier']}, {report['instruments']} instruments, {report['replicates']} replicates, "
        f"seed {report['seed']}, one-sided α = {ALPHA}",
        "Shares are over EVERY path (zero-unit and halted paths included), ± Monte-Carlo SE. A share is the",
        "probability of that outcome under this simulation model, not of the live trial's verdict.",
        "halted_loss is a daily-bar UPPER BOUND. At δ = 0 `reject` is the rejection rate under a no-skill arm;",
        "it is the test's size only if E[d] = 0 there (mean(d) printed).",
    ]
    for h, cell in report["cells"].items():
        lines += [
            "",
            f"### horizon {h} — δ=0 mean(d) {_fmt(cell['delta0_mean_d_pct'], '+.3f')} pp over "
            f"{cell['delta0_units']} units",
            f"arm decisions {cell['arm_decisions']}; control_pool_exhausted/decision "
            f"{_fmt(cell['control_pool_exhausted_per_arm_decision'])}; pairs {cell['pairs']}; broken/pair "
            f"{_fmt(cell['broken_pairs_per_pair'])}; refused legs/pair {cell['refused_legs_per_pair']}",
            f"pool size {cell['pool_size']}; self-draw/pair {_fmt(cell['self_draw_per_pair'])}; singleton-self/pair "
            f"{_fmt(cell['singleton_self_per_pair'])}; setup mix {cell['setup_mix_per_arm_decision']}",
            "| δ (pp) | " + " | ".join(OUTCOMES) + " | halted_loss | loss<min | units p50 (p10–p90) | clusters p50 |",
            "|---|" + "---|" * (len(OUTCOMES) + 4),
        ]
        for s, row in cell["by_shift"].items():
            shares = " | ".join(
                f"{_fmt(row['outcomes'][o]['share'])} ±{_fmt(row['outcomes'][o]['se'])}" for o in OUTCOMES
            )
            u, c = row["units"], row["clusters"]
            lines.append(
                f"| {float(s):g} | {shares} | {_fmt(row['halted_loss']['share'])} | "
                f"{_fmt(row['halted_loss_below_minimum']['share'])} | "
                f"{_fmt(u.get('p50'), 'g')} ({_fmt(u.get('p10'), 'g')}–{_fmt(u.get('p90'), 'g')}) | "
                f"{_fmt(c.get('p50'), 'g')} |"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DB load (read-only) and CLI
# ---------------------------------------------------------------------------
_UNIVERSE_SQL = """
    SELECT i.instrument_id
      FROM instruments i
      JOIN exchanges e ON e.exchange_id = i.exchange
     WHERE e.asset_class = 'us_equity'
"""

_ALL_BREAKS_SQL = """
    SELECT instrument_id, break_date
      FROM price_series_break
     WHERE instrument_id = ANY(%(ids)s)
     ORDER BY instrument_id, break_date
"""


def segment_cuts(kept: Sequence[date], unresolved_breaks: Sequence[date]) -> list[int]:
    """Bar indexes where an unresolved break starts a new scale, bisected against the RETAINED
    dates as ``price_segments.series_segment_bounds`` does. A break dated on a weekend or holiday
    (the corpus carries such bars, and the NYSE cut drops them) still cuts at the first retained
    bar after it. A cut at 0 or past the end splits nothing."""
    cuts = {bisect.bisect_left(kept, d) for d in unresolved_breaks}
    return sorted(c for c in cuts if 0 < c < len(kept))


def _price(value: Any) -> Decimal | None:
    """The stored numeric as a float-backed Decimal, asserted exact (``Name`` stores floats)."""
    if value is None:
        return None
    stored = Decimal(value)
    if stored.is_finite() and Decimal(repr(float(stored))) != stored:
        raise RuntimeError(f"price {stored} does not round-trip through float64")
    return stored


def _volume(value: Any) -> int | None:
    if value is None:
        return None
    number = Decimal(value)
    return int(number) if number.is_finite() and number >= 0 else None


def load_names(conn: psycopg.Connection[Any]) -> tuple[dict[int, Name], list[date]]:
    """Every ``us_equity`` name with a bar on or after ``WINDOW_START``, cut to NYSE sessions from
    ``HISTORY_START``; returns the names and the calendar their positions index."""
    from app.services.price_masked_bars import load_bar_spans, load_masked_bars
    from app.services.price_segments import load_unresolved_breaks

    ids = [int(r[0]) for r in conn.execute(_UNIVERSE_SQL).fetchall()]
    spans = load_bar_spans(conn, ids)
    unresolved = load_unresolved_breaks(conn, list(spans))
    every: dict[int, list[date]] = {}
    for iid, day in conn.execute(_ALL_BREAKS_SQL, {"ids": list(spans)}).fetchall():
        every.setdefault(int(iid), []).append(day)
    frontier = max(s.last_bar for s in spans.values())
    calendar = nyse_sessions(HISTORY_START, frontier)
    position = {d: p for p, d in enumerate(calendar)}
    names: dict[int, Name] = {}
    for iid in sorted(i for i, s in spans.items() if s.last_bar >= WINDOW_START):
        series = load_masked_bars(conn, iid).series
        keep = [n for n, d in enumerate(series.dates) if d in position]
        if not keep:
            continue
        kept = [series.dates[n] for n in keep]
        positions = [position[d] for d in kept]
        bars: list[Bar] = []
        for n in keep:
            r = series.rows[n]
            bars.append(
                (
                    _price(r.get("open")),
                    _price(r.get("high")),
                    _price(r.get("low")),
                    _price(r.get("close")),
                    _volume(r.get("volume")),
                )
            )
        cuts = segment_cuts(kept, unresolved.get(iid, ()))
        names[iid] = make_name(
            iid,
            positions,
            bars,
            breaks=every.get(iid, ()),
            unresolved_breaks=cuts,
        )
    return names, calendar


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3471 §9 planning table, v6 grid (never calls the model)")
    parser.add_argument("--replicates", type=int, default=400)
    parser.add_argument("--seed", type=int, default=3471)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    parser.add_argument("--json", type=Path)
    args = parser.parse_args(argv)

    from app.config import settings

    global _PANEL
    started = time.monotonic()
    assert_order_6()
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        names, calendar = load_names(conn)
    _PANEL = build_panel(names, calendar)
    frontier = calendar[-1]
    print(f"loaded {len(names)} names, frontier {frontier}, {time.monotonic() - started:.0f}s", flush=True)
    results: list[PathResult] = []
    jobs = [(args.seed, r) for r in range(args.replicates)]
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("fork")) as pool:
        for done, batch in enumerate(pool.map(_worker, jobs, chunksize=1), start=1):
            results.extend(batch)
            if done % 20 == 0:
                print(f"{done}/{args.replicates} replicates, {time.monotonic() - started:.0f}s", flush=True)
    report: dict[str, Any] = {
        "window": [WINDOW_START.isoformat(), WINDOW_END.isoformat()],
        "frontier": frontier.isoformat(),
        "instruments": len(names),
        "replicates": args.replicates,
        "seed": args.seed,
        "cells": summarise(results),
        "elapsed_s": round(time.monotonic() - started, 1),
    }
    print(render(report))
    if args.json:
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
