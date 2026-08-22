"""S-E's 10-month-SMA overlay on the passive core — the frozen rule, as code.

Contract: ``docs/proposals/ta/2026-08-22-se-ma-overlay-preregistration.md``
(`se-ma-overlay-2026-08-22`). Issue #2837. Part of #2832.

⚠⚠ THE CLAIM IS DRAWDOWN INSURANCE AND NOTHING ELSE. Excess-return significance
is not testable on one index path, is not the claim, and no test of it exists in
this module. The trial is ``falsification_only`` and structurally unpromotable
before it runs (§2 of the contract), so nothing here may be read as an alpha
result however the numbers land.

⚠ PURE. Arrays and bars in, verdicts out, no database and no clock. The impure
half — reading ``spy_chain_v1`` and the distribution yield — is
``scripts/measure_2837_se_overlay.py``. The split is the same one
``market_regime`` / ``market_regime_provider`` already draws, and for the same
reason: the rule is what is frozen, and how its inputs are fetched is not.

⚠ EVERY CONSTANT BELOW IS THE CONTRACT'S, NOT A CHOICE MADE HERE. A different
lookback, offset set, cost band or pass threshold is a NEW declared search
charging the trial register again (#2600 D-0.1) — not a refinement of this one.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

import numpy as np

from app.services.strategy_statistics import max_drawdown_pct
from app.services.tax_ledger import ANNUAL_EXEMPT

#: Contract §4.3. Ten evaluation-date closes, inclusive of the current one.
LOOKBACK: Final[int] = 10

#: Contract §5. A fragility SCREEN over one rule, never three variants to choose
#: between — which is why the register charges ``searches=1`` and why §8 demands
#: all three rather than the best.
OFFSETS: Final[tuple[int, ...]] = (0, 5, 10)

#: Contract §6. 0.322% round trip, charged half on each side.
ROUND_TRIP_COST_PCT: Final[float] = 0.322
SIDE_COST_PCT: Final[float] = ROUND_TRIP_COST_PCT / 2.0

#: Contract §6. Opening account, higher-rate taxpayer, 24% above the annual
#: exempt amount. ⚠ ``ANNUAL_EXEMPT`` is imported rather than retyped so the
#: £3,000 has one definition in the repo; the rate is the contract's declared
#: worst case (``tax_ledger._CGT_RATE_PERIODS`` carries it from 2024-10-30).
OPENING_EQUITY_GBP: Final[float] = 50_000.0
CGT_HIGHER_RATE: Final[float] = 0.24
ANNUAL_EXEMPT_GBP: Final[float] = float(ANNUAL_EXEMPT)

#: Contract §8. Both legs, and the ≥15% episode class §9 reports.
MAX_DRAWDOWN_RATIO_BAR: Final[float] = 2.0 / 3.0
MIN_CAGR_DELTA_PP: Final[float] = -1.5
EPISODE_CLASS_PCT: Final[float] = 15.0

#: Contract §3 — the chain's extent, FROZEN rather than derived. Measured
#: 2026-08-22, dates and row counts only. A corpus refresh that moves any of the
#: three changes the tested span, and a contract that absorbs that silently
#: means something different each time it runs.
CHAIN_FIRST_BAR: Final[date] = date(1993, 1, 29)
CHAIN_LAST_BAR: Final[date] = date(2026, 7, 8)
CHAIN_BARS: Final[int] = 8391

#: Contract §9. The predeclared March-2020 inspection window — fixed before the
#: look so the narration cannot be selected after it.
MARCH_2020_WINDOW: Final[tuple[date, date]] = (date(2019, 12, 1), date(2020, 12, 31))


class OverlayRefused(RuntimeError):
    """The measurement cannot run as declared.

    ⚠ Raised rather than degraded. Every caller of this module is producing a
    number that goes on a preregistered trial; a quietly shortened chain, a
    collapsed offset arm or a non-finite SMA input would all still produce a
    plausible float, and a plausible float is the failure mode that survives
    review.
    """


@dataclass(frozen=True)
class DrawdownEpisode:
    """One peak → trough → recovery excursion. Contract §9.

    ⚠ ``recovery`` is ``None`` for a terminal episode the curve never climbed out
    of, and such an episode is REPORTED, not dropped: dropping it would let a
    measurement that ends in the deepest hole of its life report the second
    deepest.
    """

    peak_date: date
    trough_date: date
    recovery_date: date | None
    depth_pct: float

    @property
    def unrecovered(self) -> bool:
        return self.recovery_date is None


@dataclass(frozen=True)
class TaxCharge:
    """One 31-January payment, and the tax year it settles."""

    tax_year_start: int
    payment_date: date
    net_gain_gbp: float
    taxable_gbp: float
    tax_gbp: float


@dataclass(frozen=True)
class ArmResult:
    """One offset's full readout. Contract §9 — reported pass or fail."""

    offset: int
    first_execution: date
    last_execution: date
    years: float
    evaluation_dates: tuple[date, ...]
    execution_dates: tuple[date, ...]
    positions: tuple[int, ...]
    flips: int
    fraction_in_cash: float
    seam_spanning_windows: int
    reentries_within_30_days: int
    overlay_equity: tuple[float, ...]
    benchmark_equity: tuple[float, ...]
    equity_dates: tuple[date, ...]
    overlay_max_drawdown_pct: float
    benchmark_max_drawdown_pct: float
    overlay_worst_episodes: tuple[DrawdownEpisode, ...]
    benchmark_worst_episodes: tuple[DrawdownEpisode, ...]
    overlay_episodes_over_class: int
    benchmark_episodes_over_class: int
    overlay_cagr_pct: float
    benchmark_cagr_pct: float
    dividend_drag_pp: float
    tax_charges: tuple[TaxCharge, ...]
    symmetric_overlay_terminal_gbp: float
    symmetric_benchmark_terminal_gbp: float

    @property
    def drawdown_ratio(self) -> float | None:
        """Overlay max DD as a fraction of the benchmark's. ``None`` if the
        benchmark never drew down — §8 fails that arm rather than dividing."""
        if self.benchmark_max_drawdown_pct <= 0.0:
            return None
        return self.overlay_max_drawdown_pct / self.benchmark_max_drawdown_pct

    @property
    def net_cagr_delta_pp(self) -> float:
        """§8 leg 2's quantity: the overlay's CAGR after §3.1's dividend drag,
        less the benchmark's."""
        return self.overlay_cagr_pct - self.dividend_drag_pp - self.benchmark_cagr_pct

    @property
    def drawdown_leg_passes(self) -> bool:
        ratio = self.drawdown_ratio
        # ⚠ `None` FAILS. An overlay cannot evidence insurance against a loss
        # that never happened, so a zero-drawdown benchmark is a failed arm and
        # not a division to guard against.
        return ratio is not None and ratio <= MAX_DRAWDOWN_RATIO_BAR

    @property
    def cagr_leg_passes(self) -> bool:
        return self.net_cagr_delta_pp >= MIN_CAGR_DELTA_PP

    @property
    def passes(self) -> bool:
        return self.drawdown_leg_passes and self.cagr_leg_passes


def uk_tax_year_start(day: date) -> int:
    """The starting calendar year of the UK tax year containing ``day``.

    6 April–5 April. ⚠ Deliberately a local four-liner rather than an import of
    ``tax_ledger._compute_tax_year``, which is private and returns a display
    string. ``tests/test_2837_se_ma_overlay.py`` asserts the two agree across a
    date sweep, so there is one RULE with two spellings rather than two rules.
    """
    return day.year - 1 if (day.month, day.day) <= (4, 5) else day.year


def cgt_payment_date(tax_year_start: int) -> date:
    """HMRC's self-assessment payment date: 31 January following the tax year.

    Tax year ``Y/Y+1`` ends ``Y+1``-04-05 and is payable ``Y+2``-01-31.
    """
    return date(tax_year_start + 2, 1, 31)


def month_end_indices(dates: Sequence[date]) -> tuple[int, ...]:
    """Contract §4.1 — the last chain bar of each COMPLETED calendar month.

    ⚠⚠ A month qualifies only when a later bar exists in a SUBSEQUENT calendar
    month. The trailing partial month is excluded: the chain ends mid-July 2026,
    and calling 2026-07-08 July's month-end would invent a decision the calendar
    never offered — and would do it at the one end of the sample where a wrong
    decision has no later bar to correct it.

    ⚠ This is not lookahead. The qualifying evidence is a bar that already
    exists at execution time; §4.5 executes on exactly that bar.
    """
    if not dates:
        return ()
    ends: list[int] = []
    for index in range(len(dates) - 1):
        current, following = dates[index], dates[index + 1]
        if (current.year, current.month) != (following.year, following.month):
            ends.append(index)
    return tuple(ends)


def evaluation_indices(dates: Sequence[date], offset: int) -> tuple[int, ...]:
    """Contract §4.2 — month-ends shifted by ``offset`` CHAIN-BAR POSITIONS.

    ⚠ Positions, not trading days. The chain is the only calendar this
    measurement has and no exchange calendar is frozen, so "+5" means the fifth
    subsequent chain row. Saying "trading days" would be a claim about sessions
    that the data cannot support.

    A shifted index past the end of the chain contributes no evaluation date.
    """
    if offset < 0:
        raise OverlayRefused(f"offset {offset} is negative")
    shifted = tuple(index + offset for index in month_end_indices(dates) if index + offset < len(dates))
    if len(set(shifted)) != len(shifted) or any(b <= a for a, b in zip(shifted, shifted[1:], strict=False)):
        # Structurally impossible on a chain with >=12 bars a month; refused
        # rather than de-duplicated because a collision would silently
        # double-count a close inside the SMA window.
        raise OverlayRefused(f"offset {offset} produced non-increasing or duplicate evaluation indices")
    return shifted


def overlay_positions(evaluation_closes: Sequence[float]) -> tuple[int, ...]:
    """Contract §4.3–4.4 — 1 when above the SMA10, 0 in cash, 1 through warm-up.

    ⚠ STRICTLY GREATER, float64, no rounding and no tolerance. An equality is
    cash. A tolerance would be a second, undeclared parameter.

    ⚠ Warm-up HOLDS rather than sits out. The overlay is insurance on an
    already-invested core: it may remove exposure the core has and may never add
    exposure the core lacks. Sitting out the warm-up would make the arm a
    market-timing bet on its own first ten months.
    """
    positions: list[int] = []
    for index, close in enumerate(evaluation_closes):
        if index + 1 < LOOKBACK:
            positions.append(1)
            continue
        window = evaluation_closes[index + 1 - LOOKBACK : index + 1]
        if not all(math.isfinite(value) and value > 0.0 for value in window):
            raise OverlayRefused(
                f"SMA window ending at evaluation index {index} contains a non-positive or non-finite close"
            )
        positions.append(1 if close > (sum(window) / LOOKBACK) else 0)
    return tuple(positions)


def drawdown_episodes(dates: Sequence[date], equity: Sequence[float]) -> tuple[DrawdownEpisode, ...]:
    """Contract §9 — peak → trough → recovery, deepest first.

    ⚠ NESTED DRAWDOWNS ARE ONE EPISODE. A dip inside an unrecovered decline is
    part of that decline, not a second event; counting it separately would let
    one 2008 report as four and inflate any episode count built on it.

    ⚠ A terminal unrecovered episode is included with its trough to date and
    flagged, for the reason on ``DrawdownEpisode``.
    """
    if len(dates) != len(equity):
        raise OverlayRefused("drawdown inputs disagree in length")
    episodes: list[DrawdownEpisode] = []
    peak = -math.inf
    peak_index = 0
    trough_index: int | None = None
    for index, value in enumerate(equity):
        if value >= peak:
            if trough_index is not None:
                episodes.append(_episode(dates, equity, peak_index, trough_index, index))
                trough_index = None
            peak, peak_index = value, index
        elif trough_index is None or value < equity[trough_index]:
            trough_index = index
    if trough_index is not None:
        episodes.append(_episode(dates, equity, peak_index, trough_index, None))
    return tuple(sorted(episodes, key=lambda episode: episode.depth_pct, reverse=True))


def _episode(
    dates: Sequence[date], equity: Sequence[float], peak_index: int, trough_index: int, recovery_index: int | None
) -> DrawdownEpisode:
    peak_value = equity[peak_index]
    depth = 0.0 if peak_value <= 0.0 else (peak_value - equity[trough_index]) / peak_value * 100.0
    return DrawdownEpisode(
        peak_date=dates[peak_index],
        trough_date=dates[trough_index],
        recovery_date=None if recovery_index is None else dates[recovery_index],
        depth_pct=depth,
    )


def cagr_pct(start_equity: float, end_equity: float, years: float) -> float:
    """Annualised growth over ``years``, in percent.

    ⚠ Refuses a non-positive span or a wiped-out arm rather than returning a
    complex or infinite number that would print as a plausible result.
    """
    if years <= 0.0:
        raise OverlayRefused(f"cannot annualise over {years!r} years")
    if start_equity <= 0.0 or end_equity <= 0.0:
        raise OverlayRefused("cannot annualise a non-positive equity")
    return ((end_equity / start_equity) ** (1.0 / years) - 1.0) * 100.0


class _CgtLedger:
    """Contract §6's CGT, accumulated as the simulation walks forward.

    ⚠⚠ THIS CANNOT BE PRECOMPUTED, and the attempt is the bug worth naming. A
    disposal's gain is `proceeds − pool cost` in POUNDS, the pool cost is the
    equity that was invested at the last entry, and that equity is net of every
    tax payment made before it — so the gains depend on the charges that depend
    on the gains. Walking forward breaks the loop: a tax year can only be
    finalised once the simulation has passed 5 April, and its payment lands the
    following 31 January, which is later still.

    ⚠ LOSSES CARRY FORWARD, THE EXEMPTION DOES NOT. HMRC treats them
    differently and so does this: an unused annual exempt amount is lost at the
    year end, a net loss is available against later gains indefinitely. Carrying
    the exemption would understate the drag; refusing to carry losses would
    overstate it.
    """

    def __init__(self) -> None:
        self._open_year: int | None = None
        self._open_gain = 0.0
        self._carried_loss = 0.0
        self.charges: list[TaxCharge] = []
        self.pending: list[tuple[date, float]] = []

    def realise(self, day: date, gain: float) -> None:
        year = uk_tax_year_start(day)
        if self._open_year is not None and year != self._open_year:
            self._finalise()
        self._open_year = year
        self._open_gain += gain

    def settle_years_before(self, day: date) -> None:
        """Close any tax year that ended before ``day`` so its charge is scheduled."""
        if self._open_year is not None and uk_tax_year_start(day) != self._open_year:
            self._finalise()

    def _tax_for(self, gain: float) -> float:
        """The charge a year holding ``gain`` would settle, given carried losses."""
        return max(max(gain + self._carried_loss, 0.0) - ANNUAL_EXEMPT_GBP, 0.0) * CGT_HIGHER_RATE

    def _finalise(self) -> None:
        assert self._open_year is not None
        net = self._open_gain + self._carried_loss
        tax = self._tax_for(self._open_gain)
        taxable = tax / CGT_HIGHER_RATE
        self._carried_loss = min(net, 0.0)
        payment = cgt_payment_date(self._open_year)
        self.charges.append(
            TaxCharge(
                tax_year_start=self._open_year,
                payment_date=payment,
                net_gain_gbp=net,
                taxable_gbp=taxable,
                tax_gbp=tax,
            )
        )
        if tax > 0.0:
            self.pending.append((payment, tax))
        self._open_year = None
        self._open_gain = 0.0

    def close(self) -> None:
        if self._open_year is not None:
            self._finalise()

    def incremental_tax_on_terminal_gain(self, gain: float) -> float:
        """§7's symmetric variant: one more disposal into the still-open year.

        ⚠⚠ INCREMENTAL, NOT ABSOLUTE, and the difference is a real double-count
        (Codex checkpoint 2, #2837). The open year's own charge is already
        settled by ``close`` and deducted from the primary equity as the terminal
        accrual; returning the full charge here and subtracting it again would
        tax that year twice. What §7 adds is only the extra tax the liquidation
        itself causes — and because the exemption and any carried loss are
        absorbed by the base charge first, that is genuinely not the same as
        taxing the terminal gain on its own.
        """
        return self._tax_for(self._open_gain + gain) - self._tax_for(self._open_gain)


def simulate_arm(
    chain: Sequence[tuple[date, float]],
    *,
    offset: int,
    seam: date,
    dividend_yield_pp: float,
) -> ArmResult:
    """One offset arm, end to end. Contract §4, §6, §8, §9.

    ⚠⚠ THE INTERVAL OWNERSHIP RULE, which is where a timing bug would hide: the
    return from an evaluation close to the following execution close belongs to
    the OLD position; the NEW position owns from that execution close forward.
    Getting this backwards hands the rule the very move it is being tested on.

    ⚠ Equity is marked on EVERY chain bar in the span, not sampled monthly. A
    monthly-sampled drawdown understates the real one, which would flatter §8's
    first leg; and the 31 January tax outflow is a calendar date that is usually
    not an execution date.
    """
    dates = [day for day, _ in chain]
    closes = [close for _, close in chain]
    evaluations = evaluation_indices(dates, offset)
    # A decision with no next chain bar is DISCARDED, not carried: there is no
    # bar to execute it on, and holding it back to the following month would be
    # a different rule.
    executable = [index for index in evaluations if index + 1 < len(dates)]
    if len(executable) < LOOKBACK or len(executable) < 2:
        raise OverlayRefused(f"offset {offset} yields {len(executable)} executable decisions — a degenerate arm")
    executions = [index + 1 for index in executable]
    if set(executions) & set(executable):
        raise OverlayRefused(f"offset {offset} has a bar that is both an evaluation and an execution date")

    positions = overlay_positions([closes[index] for index in executable])
    first_execution, last_execution = executions[0], executions[-1]

    overlay = OPENING_EQUITY_GBP
    benchmark = OPENING_EQUITY_GBP
    equity_dates: list[date] = [dates[first_execution]]
    overlay_curve: list[float] = [overlay]
    benchmark_curve: list[float] = [benchmark]
    execution_at = {index: position for index, position in zip(executions, positions, strict=True)}
    ledger = _CgtLedger()
    charge_cursor = 0
    held = positions[0]
    # §4.8 — inception charges nothing on either arm, so the opening pool cost
    # is the full opening equity.
    pool_cost = OPENING_EQUITY_GBP if held else 0.0
    for bar in range(first_execution + 1, last_execution + 1):
        move = closes[bar] / closes[bar - 1] - 1.0
        # The interval ENDING at this bar is owned by the position held coming
        # into it — so the return is applied before any trade at this bar.
        overlay *= 1.0 + held * move
        benchmark *= 1.0 + move
        ledger.settle_years_before(dates[bar])
        target = execution_at.get(bar)
        if target is not None and target != held:
            overlay *= 1.0 - SIDE_COST_PCT / 100.0
            if target == 0:
                # A complete disposal: the position is all-in or all-out, so
                # every exit empties the §104 pool.
                ledger.realise(dates[bar], overlay - pool_cost)
                pool_cost = 0.0
            else:
                pool_cost = overlay
            held = target
        while charge_cursor < len(ledger.pending) and ledger.pending[charge_cursor][0] <= dates[bar]:
            # §6: an outflow on the calendar date, charged at the first chain bar
            # on or after it when 31 January is not a trading day. Taken from
            # equity directly; the declared omissions are the spread on the
            # implied partial sale and the second CGT event it would create.
            overlay -= ledger.pending[charge_cursor][1]
            if held:
                # The sliver sold to fund the payment leaves the pool with it,
                # so the remaining holding's cost is reduced pro rata rather
                # than left overstating the base of the next disposal.
                pool_cost *= (overlay) / (overlay + ledger.pending[charge_cursor][1])
            charge_cursor += 1
        equity_dates.append(dates[bar])
        overlay_curve.append(overlay)
        benchmark_curve.append(benchmark)

    # §7's INCREMENTAL liquidation tax must be read before ``close`` finalises
    # the open year, but applied after the accrual below — see the method.
    liquidation_tax = ledger.incremental_tax_on_terminal_gain(overlay - pool_cost) if held else 0.0

    # §6 terminal accrual — a liability whose payment date falls after the arm
    # ends must not escape by falling off the end of the sample.
    ledger.close()
    charges = tuple(ledger.charges)
    accrued = sum(amount for _, amount in ledger.pending[charge_cursor:])
    if accrued:
        overlay -= accrued
        overlay_curve[-1] = overlay

    # ⚠ AFTER the accrual, not before (Codex checkpoint 2). Capturing the
    # symmetric figure earlier omitted every tax the primary equity had already
    # been charged, so the sensitivity read better than the thing it is a
    # sensitivity ON — the one direction a declared sensitivity must not fail in.
    symmetric_overlay = overlay - liquidation_tax
    symmetric_benchmark = benchmark - max(benchmark - OPENING_EQUITY_GBP - ANNUAL_EXEMPT_GBP, 0.0) * CGT_HIGHER_RATE

    span_days = (dates[last_execution] - dates[first_execution]).days
    years = span_days / 365.25
    cash_days = sum(
        (dates[executions[index + 1]] - dates[executions[index]]).days
        for index in range(len(executions) - 1)
        if positions[index] == 0
    )
    fraction_in_cash = cash_days / span_days if span_days else 0.0

    overlay_episodes = drawdown_episodes(equity_dates, overlay_curve)
    benchmark_episodes = drawdown_episodes(equity_dates, benchmark_curve)
    return ArmResult(
        offset=offset,
        first_execution=dates[first_execution],
        last_execution=dates[last_execution],
        years=years,
        evaluation_dates=tuple(dates[index] for index in executable),
        execution_dates=tuple(dates[index] for index in executions),
        positions=positions,
        flips=sum(1 for a, b in zip(positions, positions[1:], strict=False) if a != b),
        fraction_in_cash=fraction_in_cash,
        seam_spanning_windows=_seam_spanning_windows([dates[index] for index in executable], seam),
        reentries_within_30_days=_reentries_within_30_days([dates[index] for index in executions], positions),
        overlay_equity=tuple(overlay_curve),
        benchmark_equity=tuple(benchmark_curve),
        equity_dates=tuple(equity_dates),
        overlay_max_drawdown_pct=-max_drawdown_pct(np.asarray(overlay_curve, dtype=np.float64)),
        benchmark_max_drawdown_pct=-max_drawdown_pct(np.asarray(benchmark_curve, dtype=np.float64)),
        overlay_worst_episodes=overlay_episodes[:3],
        benchmark_worst_episodes=benchmark_episodes[:3],
        overlay_episodes_over_class=sum(1 for e in overlay_episodes if e.depth_pct >= EPISODE_CLASS_PCT),
        benchmark_episodes_over_class=sum(1 for e in benchmark_episodes if e.depth_pct >= EPISODE_CLASS_PCT),
        overlay_cagr_pct=cagr_pct(OPENING_EQUITY_GBP, overlay, years),
        benchmark_cagr_pct=cagr_pct(OPENING_EQUITY_GBP, benchmark, years),
        dividend_drag_pp=fraction_in_cash * dividend_yield_pp,
        tax_charges=charges,
        symmetric_overlay_terminal_gbp=symmetric_overlay,
        symmetric_benchmark_terminal_gbp=symmetric_benchmark,
    )


@dataclass(frozen=True)
class RegimeCohort:
    """One regime's slice of the readout. Contract §9."""

    regime: str
    intervals: int
    months_in_cash: int
    overlay_mean_return_pct: float
    benchmark_mean_return_pct: float


def regime_cohorts(result: ArmResult, regimes: Sequence[str | None]) -> tuple[RegimeCohort, ...]:
    """Contract §9 — the readout partitioned by regime, never pooled.

    ⚠ ASSIGNED BY THE EVALUATION DATE THAT DECIDED THE INTERVAL, not by the
    interval's own dates. The question a cohort answers is "what did this rule do
    when it decided under regime X", and the decision is made at the evaluation
    date; keying on the holding period instead would credit a decision to a
    regime that had not happened when it was taken.

    ⚠ The regime is DESCRIPTIVE ONLY. It partitions this readout and enters
    neither the signal, the position, the costs nor the pass bar — so the
    classifier's use of contemporaneous closes cannot leak into a decision.

    ⚠ ``None`` is its own cohort (``warm_up``), never folded into a real one: a
    bar the classifier could not verdict is not evidence about any regime.
    """
    if len(regimes) != len(result.evaluation_dates):
        raise OverlayRefused("regime series and evaluation dates disagree in length")
    overlay_at = dict(zip(result.equity_dates, result.overlay_equity, strict=True))
    benchmark_at = dict(zip(result.equity_dates, result.benchmark_equity, strict=True))
    buckets: dict[str, list[tuple[float, float, int]]] = {}
    for index in range(len(result.execution_dates) - 1):
        start, end = result.execution_dates[index], result.execution_dates[index + 1]
        if start not in overlay_at or end not in overlay_at:
            continue
        label = regimes[index] or "warm_up"
        buckets.setdefault(label, []).append(
            (
                overlay_at[end] / overlay_at[start] - 1.0,
                benchmark_at[end] / benchmark_at[start] - 1.0,
                1 if result.positions[index] == 0 else 0,
            )
        )
    return tuple(
        RegimeCohort(
            regime=label,
            intervals=len(rows),
            months_in_cash=sum(row[2] for row in rows),
            overlay_mean_return_pct=sum(row[0] for row in rows) / len(rows) * 100.0,
            benchmark_mean_return_pct=sum(row[1] for row in rows) / len(rows) * 100.0,
        )
        for label, rows in sorted(buckets.items())
    )


def march_2020_detail(result: ArmResult) -> tuple[tuple[date, date, int, float, float], ...]:
    """Contract §9's predeclared March-2020 inspection.

    ⚠ THE WINDOW AND THE FIELDS ARE FROZEN (``MARCH_2020_WINDOW``), so the
    narration cannot be selected after the look. Every decision in the window is
    returned, not the interesting ones.
    """
    start, end = MARCH_2020_WINDOW
    overlay_at = dict(zip(result.equity_dates, result.overlay_equity, strict=True))
    benchmark_at = dict(zip(result.equity_dates, result.benchmark_equity, strict=True))
    rows: list[tuple[date, date, int, float, float]] = []
    for index, evaluation in enumerate(result.evaluation_dates):
        if not start <= evaluation <= end:
            continue
        execution = result.execution_dates[index]
        if execution not in overlay_at:
            continue
        rows.append((evaluation, execution, result.positions[index], overlay_at[execution], benchmark_at[execution]))
    return tuple(rows)


def _seam_spanning_windows(evaluation_dates: Sequence[date], seam: date) -> int:
    """§9 — how many SMA windows straddle the vendor seam.

    The provider's header argues SPY's lack of split history makes the two
    adjustment bases equivalent, and the 585-date overlap measures the residual
    at ~$1.76 (~0.3%). A window straddling the seam is nonetheless the only
    place a residual level step could enter a SIGNAL, so the count is surfaced
    rather than assumed away.
    """
    return sum(
        1
        for index in range(LOOKBACK - 1, len(evaluation_dates))
        if evaluation_dates[index - LOOKBACK + 1] < seam <= evaluation_dates[index]
    )


def _reentries_within_30_days(execution_dates: Sequence[date], positions: Sequence[int]) -> int:
    """§6 — exits followed by a re-entry inside 30 days.

    The size of the declared 30-day-rule simplification: these are the only
    disposals TCGA 1992 s.106A would have matched differently.
    """
    count = 0
    for index in range(1, len(positions)):
        if positions[index] != 0 or positions[index - 1] != 1:
            continue
        exit_date = execution_dates[index]
        for later in range(index + 1, len(positions)):
            if positions[later] == 1:
                count += (execution_dates[later] - exit_date).days <= 30
                break
    return count


__all__ = [
    "ANNUAL_EXEMPT_GBP",
    "CGT_HIGHER_RATE",
    "CHAIN_BARS",
    "CHAIN_FIRST_BAR",
    "CHAIN_LAST_BAR",
    "EPISODE_CLASS_PCT",
    "LOOKBACK",
    "MARCH_2020_WINDOW",
    "MAX_DRAWDOWN_RATIO_BAR",
    "MIN_CAGR_DELTA_PP",
    "OFFSETS",
    "OPENING_EQUITY_GBP",
    "ROUND_TRIP_COST_PCT",
    "SIDE_COST_PCT",
    "ArmResult",
    "DrawdownEpisode",
    "OverlayRefused",
    "RegimeCohort",
    "TaxCharge",
    "cagr_pct",
    "cgt_payment_date",
    "drawdown_episodes",
    "evaluation_indices",
    "march_2020_detail",
    "month_end_indices",
    "overlay_positions",
    "regime_cohorts",
    "simulate_arm",
    "uk_tax_year_start",
]
