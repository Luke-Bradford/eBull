"""#3609 step 0: cheap baselines, net of our costs.

Spec: ``docs/research/2026-10-04-3609-step0-baselines.md`` (Codex checkpoint 1, then Amendment 1). Every rule here
is the spec's; a comment cites the spec section rather than restating its reason.

- B1, SPY total return; B2a 60/40; B2b Swensen; each a continuous monthly path from the 2009-12 month-end.
- B3, a random 50-name basket re-drawn every December from the seasoned survivorship-free population, 1,000 draws
  per variant (all names; raw close >= $5), under both termination arms and both screen arms.
- Gross, net (``cost_model`` bands) and 2x-band stress on one decision path each.

Not a study: nothing is selected or ranked, so it carries no trial declaration (spec §"Registration").

    PYTHONPATH=. uv run python -m scripts.report_3609_baselines
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import subprocess
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import numpy as np
import psycopg

from app.config import settings
from app.services import cost_model
from app.services.etf_total_return_reader import (
    ETF_TOTAL_RETURN_VERSION,
    EtfTotalReturnPanel,
    etoro_month_ends,
    load_etf_total_return_panel,
)
from app.services.factor_validation import load_reference_series
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.reference_data import REFERENCE_DATASETS
from app.services.series_termination import (
    TERMINATION_RULE_VERSION,
    TerminationClass,
    classify_termination,
    terminal_value_fraction,
)
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import AmbiguityArm
from app.services.total_return_reader import (
    INTRADER_VENDOR,
    LAST_PWB_MONTH,
    LAST_SURVIVORSHIP_FREE_MONTH,
    PWB_CAPTURE_DATE,
    PWB_VENDOR,
    SWITCH_MONTH,
    TOTAL_RETURN_SPLICE_VERSION,
    Month,
    MonthEnd,
    MonthlyTotalReturn,
    add_months,
    load_month_ends,
    load_total_return_panel,
)
from app.services.universe_selection import (
    INTRADER_CAPTURE_DATE,
    UNIVERSE_SELECTION_RULE_VERSION,
    AdmittedSeries,
    load_universe_selection,
)

SPEC_PATH: Final = "docs/research/2026-10-04-3609-step0-baselines.md"
OUT_ROOT: Final = Path("var/research/3609_step0")

#: The decision month of every path's first trade; its first return is 2010-01 (spec §"Timing").
START: Final[Month] = (2009, 12)
FIRST_RETURN_MONTH: Final[Month] = (2010, 1)
W1: Final = ((2010, 1), (2021, 5))
#: Amendment 3: Intrader records no termination before 2014-09, so B3's holding years formed through 2013-12
#: are survivor-conditioned. W1 is split at the first holding year formed after terminations begin (2014-12).
W1A: Final = ((2010, 1), (2014, 12))
W1B: Final = ((2015, 1), (2021, 5))
W2: Final = ((2021, 6), LAST_SURVIVORSHIP_FREE_MONTH)
#: Windows with an FF6 regression (each still needs 60 covered months).
REGRESSION_WINDOWS: Final = ("W1a", "W1b", "W1", "full (mixed coverage)")
WINDOW_NOTES: Final[Mapping[str, str]] = {
    "W1a": "Survivor-only for B3 (Amendment 3): formations 2009-12..2013-12 predate the archive's first terminations.",
    "W1b": "Terminations recorded; completeness unverified against an independent exit list before 2019.",
    "W1": "Supplementary; survivor-only for B3 through 2014 (Amendment 3).",
}

B1_WEIGHTS: Final[Mapping[str, float]] = {"SPY": 1.0}
B2A_WEIGHTS: Final[Mapping[str, float]] = {"SPY": 0.60, "AGG": 0.40}
B2B_WEIGHTS: Final[Mapping[str, float]] = {
    "VTI": 0.30,
    "EFA": 0.15,
    "EEM": 0.05,
    "VNQ": 0.20,
    "IEF": 0.15,
    "TIP": 0.15,
}
FUNDS: Final = tuple(sorted({*B1_WEIGHTS, *B2A_WEIGHTS, *B2B_WEIGHTS}))

B3_NAMES: Final = 50
B3_DRAWS: Final = 1_000
#: History required at formation: the formation month and the 11 before it (spec §B3).
B3_HISTORY_MONTHS: Final = 12
B3_STALE_DAYS: Final = 7
B3_MIN_RAW_CLOSE: Final = Decimal("5")
#: B3-5 needs a raw (Intrader) close, which ends at 2024-08; B3-all re-draws through 2025-12.
B3_LAST_FORMATION: Final[Mapping[str, int]] = {"all": 2025, "5": 2023}
B3_VARIANTS: Final = ("all", "5")
ARMS: Final[tuple[AmbiguityArm, ...]] = ("best_case", "worst_case")

SCREEN_UP: Final = 3.0
SCREEN_DOWN: Final = -0.9
ARBITRATION_TOLERANCE: Final = 0.50
SCREEN_ARMS: Final = ("as_is", "contradicted_replaced")

#: Cost multipliers on every band: the gross comparator, the base case and the stress (spec §Costs).
COSTS: Final[Mapping[str, float]] = {"gross": 0.0, "net": 1.0, "stress_2x": 2.0}

FACTOR_DATASETS: Final = (
    ("french_five_factor_monthly", ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF")),
    ("french_momentum_monthly", ("Mom",)),
)
FACTOR_REGRESSORS: Final = ("Mkt-RF", "SMB", "HML", "RMW", "CMA", "Mom")
FACTOR_UNIT: Final = "decimal_return"
REGRESSION_MIN_MONTHS: Final = 60

COVERAGE_EXIT: Final = "coverage_exit"


def month_range(first: Month, last: Month) -> list[Month]:
    out: list[Month] = []
    m = first
    while m <= last:
        out.append(m)
        m = add_months(m, 1)
    return out


def ym(month: Month) -> str:
    return f"{month[0]}-{month[1]:02d}"


# ---------------------------------------------------------------------------
# Pure rules: the end month, positions, paths
# ---------------------------------------------------------------------------


#: Amendment 1 rule 1: the extension verdict each B1/B2 fund must carry.
REQUIRED_VERDICTS: Final[Mapping[str, str]] = {
    symbol: ("nport_proxy" if symbol == "SPY" else "nport") for symbol in FUNDS
}
#: Amendment 1 rule 4: the first holding month of B3-all's last formation.
EARLIEST_END: Final[Month] = (B3_LAST_FORMATION["all"] + 1, 1)


@dataclass(frozen=True)
class EndMonth:
    month: Month
    #: Each fund's latest returned month (its candidate endpoint).
    fund_ends: Mapping[str, Month]
    limiting: tuple[str, ...]


def common_end_month(verdicts: Mapping[str, str], fund_months: Mapping[str, set[Month]]) -> EndMonth:
    """Amendment 1: the common B1/B2 return endpoint, capped by PWB. Refuses rather than shortening."""
    wrong = {s: verdicts.get(s) for s, want in REQUIRED_VERDICTS.items() if verdicts.get(s) != want}
    if wrong:
        raise RuntimeError(f"B1/B2 funds without their accepted extension verdict {wrong}: coverage refuses")
    empty = [s for s in REQUIRED_VERDICTS if not fund_months.get(s)]
    if empty:
        raise RuntimeError(f"B1/B2 funds with no total-return rows {empty}: coverage refuses")
    fund_ends = {s: max(fund_months[s]) for s in REQUIRED_VERDICTS}
    end = min([LAST_PWB_MONTH, *fund_ends.values()])
    gaps = {s: [ym(m) for m in month_range(FIRST_RETURN_MONTH, end) if m not in fund_months[s]] for s in fund_ends}
    gaps = {s: g for s, g in gaps.items() if g}
    if gaps:
        raise RuntimeError(f"B1/B2 funds missing months inside {ym(FIRST_RETURN_MONTH)}..{ym(end)}: {gaps}")
    if end < EARLIEST_END:
        raise RuntimeError(f"common end month {ym(end)} is before {ym(EARLIEST_END)}: refusing (Amendment 1 rule 4)")
    limiting = tuple(sorted(s for s, m in fund_ends.items() if m == end))
    return EndMonth(month=end, fund_ends=fund_ends, limiting=limiting or ("LAST_PWB_MONTH",))


@dataclass(frozen=True)
class Trajectory:
    """One position over one holding period, per unit invested at the decision close.

    ``growth[t]`` is the position's value (or, once it has left, the cash it left as) at the end of the period's
    month ``t``; cash earns 0% (spec §"Endings inside a holding year").
    """

    growth: np.ndarray
    #: Index of the month the position left the book, or ``None`` when it was held to the period's end.
    exit_index: int | None
    #: A ``TerminationClass`` value, or ``COVERAGE_EXIT``.
    exit_kind: str | None
    #: Per month: the position was held into the month and the month's row is ``dividend_capture_degraded``.
    degraded: np.ndarray
    held_flagged: bool
    held_contradicted: bool


@dataclass(frozen=True)
class Termination:
    termination_class: TerminationClass
    #: The name's last total-return row; the imputed realisation lands the month after it, never earlier.
    last_row_month: Month


def build_trajectory(
    months: Sequence[Month],
    returns: Mapping[Month, float],
    *,
    termination: Termination | None,
    arm: AmbiguityArm,
    degraded: frozenset[Month] = frozenset(),
    flagged: frozenset[Month] = frozenset(),
    contradicted: frozenset[Month] = frozenset(),
) -> Trajectory:
    """Spec §"Endings inside a holding year": the first month with no row ends the position.

    After a terminating name's last row the position realises ``terminal_value_fraction`` of its last value (an
    imputed realisation, no sale, no cost); any other missing row is a coverage exit at the last value. Either
    way the proceeds sit in cash until the next formation.
    """
    value = 1.0
    growth = np.empty(len(months))
    deg = np.zeros(len(months), dtype=bool)
    exit_index: int | None = None
    exit_kind: str | None = None
    held_flagged = held_contradicted = False
    for i, month in enumerate(months):
        if exit_index is None:
            ret = returns.get(month)
            if ret is None:
                exit_index = i
                if termination is not None and month > termination.last_row_month:
                    exit_kind = termination.termination_class.value
                    value *= terminal_value_fraction(termination.termination_class, arm)
                else:
                    exit_kind = COVERAGE_EXIT
            else:
                value *= 1.0 + ret
                deg[i] = month in degraded
                held_flagged |= month in flagged
                held_contradicted |= month in contradicted
        growth[i] = value
    return Trajectory(growth, exit_index, exit_kind, deg, held_flagged, held_contradicted)


@dataclass(frozen=True)
class Period:
    """One decision (trade at ``decision``'s close) and the months held until the next decision or the end."""

    decision: Month
    months: tuple[Month, ...]
    assets: tuple[Hashable, ...]
    weights: tuple[float, ...]
    trajectories: tuple[Trajectory, ...]
    #: The half-spread (fraction) a position pays if it is NEWLY entered here; a held position keeps its own.
    entry_half_spreads: tuple[float, ...]


@dataclass
class PathResult:
    #: Monthly return per month, first return month to the path end, with NO liquidation (Amendment 1). A decision
    #: month's return includes that month's rebalance cost.
    continuing: dict[Month, float]
    #: Per month: the cost of liquidating the month-end book, before any rebalance, as a fraction of that NAV.
    liquidation_by_month: dict[Month, float]
    #: Per decision month after the first: the rebalance cost as a fraction of the pre-trade NAV.
    rebalance_cost: dict[Month, float]
    #: One-way turnover per decision month (initial purchase and liquidation excluded).
    turnover: dict[Month, float]
    #: (kind, NAV share of the position at the start of the month it left) per exit.
    exits: list[tuple[str, float]] = field(default_factory=list)
    #: Per month: the NAV share held into a row flagged ``dividend_capture_degraded``.
    degraded_share: list[float] = field(default_factory=list)
    held_flagged: bool = False
    held_contradicted: bool = False

    @property
    def liquidation(self) -> float:
        return self.liquidation_by_month[max(self.continuing)]

    def returns(self) -> dict[Month, float]:
        """The reported path: the liquidation charged in the last month (which carries no rebalance)."""
        out = dict(self.continuing)
        last = max(out)
        out[last] = (1.0 + out[last]) * (1.0 - self.liquidation) - 1.0
        return out

    def slice_end_return(self, month: Month) -> float:
        """The last-month return of a slice ending at ``month`` (Amendment 1, "Saved paths").

        The slice does not rebalance at its end: the month's rebalance cost is removed and the pre-trade book is
        liquidated instead.
        """
        before_trade = (1.0 + self.continuing[month]) / (1.0 - self.rebalance_cost.get(month, 0.0))
        return before_trade * (1.0 - self.liquidation_by_month[month]) - 1.0


def simulate(periods: Sequence[Period], *, cost_multiplier: float) -> PathResult:
    """One continuous self-financing path (spec §Costs, §"End of the path").

    At each decision: cost = Σ|Δnotional| × half-spread at the pre-cost NAV, then the targets are applied to NAV −
    cost. A position's band is fixed at its entry and never re-keyed. The initial purchase cost is charged in the
    first month's return (its denominator is the pre-cost 1.0).
    """
    held: dict[Hashable, tuple[float, float]] = {}
    cash = 0.0
    nav: dict[Month, float] = {}
    result = PathResult(continuing={}, liquidation_by_month={}, rebalance_cost={}, turnover={})
    for index, period in enumerate(periods):
        pre = 1.0 if index == 0 else sum(v for v, _ in held.values()) + cash
        targets = dict(zip(period.assets, period.weights, strict=True))
        traded = cost = 0.0
        for asset, (value, half) in held.items():
            delta = abs(targets.get(asset, 0.0) * pre - value)
            traded += delta
            cost += delta * half
        bands: list[float] = []
        for asset, weight, entry in zip(period.assets, period.weights, period.entry_half_spreads, strict=True):
            if asset in held:
                bands.append(held[asset][1])
            else:
                traded += weight * pre
                cost += weight * pre * entry
                bands.append(entry)
        post = pre - cost * cost_multiplier
        if index > 0:
            result.turnover[period.decision] = traded / 2.0 / pre
            result.rebalance_cost[period.decision] = cost * cost_multiplier / pre
            nav[period.decision] = post
        units = np.asarray(period.weights) * post
        growth = np.vstack([t.growth for t in period.trajectories])
        values = units[:, None] * growth
        path = values.sum(axis=0)
        start_values = np.hstack([units[:, None], values[:, :-1]])
        before = start_values.sum(axis=0)
        degraded = np.vstack([t.degraded for t in period.trajectories])
        result.degraded_share.extend(((start_values * degraded).sum(axis=0) / before).tolist())
        steps = np.arange(len(period.months))
        still_held = np.vstack(
            [steps < (len(steps) if t.exit_index is None else t.exit_index) for t in period.trajectories]
        )
        liquidation = cost_multiplier * (values * still_held * np.asarray(bands)[:, None]).sum(axis=0) / path
        for i, month in enumerate(period.months):
            nav[month] = float(path[i])
            result.liquidation_by_month[month] = float(liquidation[i])
        held = {}
        cash = 0.0
        for i, (asset, traj, band) in enumerate(zip(period.assets, period.trajectories, bands, strict=True)):
            result.held_flagged |= traj.held_flagged
            result.held_contradicted |= traj.held_contradicted
            end_value = float(values[i, -1])
            if traj.exit_index is None:
                held[asset] = (end_value, band)
            else:
                cash += end_value
                assert traj.exit_kind is not None
                share = float(start_values[i, traj.exit_index] / before[traj.exit_index])
                result.exits.append((traj.exit_kind, share))
    previous = 1.0
    for month in sorted(nav):
        if month < FIRST_RETURN_MONTH:
            continue
        result.continuing[month] = nav[month] / previous - 1.0
        previous = nav[month]
    return result


# ---------------------------------------------------------------------------
# Pure rules: statistics (spec §Outputs)
# ---------------------------------------------------------------------------


def annualised(returns: Sequence[float]) -> float:
    return float(np.prod(1.0 + np.asarray(returns)) ** (12.0 / len(returns)) - 1.0)


def max_drawdown(returns: Sequence[float]) -> float:
    """From monthly observations: the largest fall of the wealth index from its running peak (starting at 1)."""
    wealth = np.concatenate(([1.0], np.cumprod(1.0 + np.asarray(returns))))
    return float((wealth / np.maximum.accumulate(wealth) - 1.0).min())


def newey_west_lag(observations: int) -> int:
    """Newey & West (1994): floor(4 (T/100)^(2/9))."""
    return math.floor(4.0 * (observations / 100.0) ** (2.0 / 9.0))


@dataclass(frozen=True)
class Regression:
    coefficients: np.ndarray
    t_stats: np.ndarray
    observations: int
    lag: int


def ols_newey_west(y: np.ndarray, x: np.ndarray) -> Regression:
    """OLS of ``y`` on a constant and ``x``, with Newey-West HAC standard errors (Bartlett weights)."""
    design = np.column_stack([np.ones(len(y)), x])
    inverse = np.linalg.inv(design.T @ design)
    beta = inverse @ design.T @ y
    scores = design * (y - design @ beta)[:, None]
    lag = newey_west_lag(len(y))
    meat = scores.T @ scores
    for k in range(1, lag + 1):
        gamma = scores[k:].T @ scores[:-k]
        meat += (1.0 - k / (lag + 1.0)) * (gamma + gamma.T)
    covariance = inverse @ meat @ inverse
    return Regression(beta, beta / np.sqrt(np.diag(covariance)), len(y), lag)


STAT_NAMES: Final = (
    "ann_net",
    "ann_vol",
    "max_dd_monthly",
    "active_vs_b1",
    "tracking_error",
    "ir",
    "beta_vs_b1",
    "max_rel_dd_vs_b1",
    "turnover_per_yr",
    "ann_gross",
    "cost_drag",
    "ann_stress_2x",
    "ff6_alpha_ann",
    "ff6_alpha_t",
    *(f"ff6_{name}" for name in FACTOR_REGRESSORS),
)


def window_stats(
    months: Sequence[Month],
    net: Mapping[Month, float],
    gross: Mapping[Month, float],
    stress: Mapping[Month, float],
    benchmark: Mapping[Month, float],
    turnover: Mapping[Month, float],
    factors: Mapping[str, Mapping[Month, float]],
    regression: Sequence[Month] | None,
) -> dict[str, float | None]:
    """Every §Outputs statistic for one path over ``months``; the factor regression only over ``regression``."""
    r = np.array([net[m] for m in months])
    b = np.array([benchmark[m] for m in months])
    active = r - b
    te = float(np.std(active, ddof=1) * math.sqrt(12.0))
    relative = np.cumprod(1.0 + r) / np.cumprod(1.0 + b)
    relative_wealth = np.concatenate(([1.0], relative))
    out: dict[str, float | None] = {
        "ann_net": annualised(list(r)),
        "ann_vol": float(np.std(r, ddof=1) * math.sqrt(12.0)),
        "max_dd_monthly": max_drawdown(list(r)),
        "active_vs_b1": float(12.0 * active.mean()),
        "tracking_error": te,
        "ir": None if te == 0.0 else float(12.0 * active.mean()) / te,
        "beta_vs_b1": float(np.polyfit(b, r, 1)[0]),
        "max_rel_dd_vs_b1": float((relative_wealth / np.maximum.accumulate(relative_wealth) - 1.0).min()),
        "turnover_per_yr": sum(turnover.get(m, 0.0) for m in months) / (len(months) / 12.0),
        "ann_gross": annualised([gross[m] for m in months]),
        "ann_stress_2x": annualised([stress[m] for m in months]),
    }
    out["cost_drag"] = (out["ann_gross"] or 0.0) - (out["ann_net"] or 0.0)
    if regression is not None:
        x = np.array([[factors[name][m] for name in FACTOR_REGRESSORS] for m in regression])
        y = np.array([net[m] - factors["RF"][m] for m in regression])
        fit = ols_newey_west(y, x)
        out["ff6_alpha_ann"] = float(12.0 * fit.coefficients[0])
        out["ff6_alpha_t"] = float(fit.t_stats[0])
        for i, name in enumerate(FACTOR_REGRESSORS, start=1):
            out[f"ff6_{name}"] = float(fit.coefficients[i])
    return out


def nearest_rank(values: Sequence[float], percentile: float) -> float:
    """Nearest-rank percentile on the sorted values (spec §B3 "Reported")."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile / 100.0 * len(ordered)) - 1)]


# ---------------------------------------------------------------------------
# Pure rules: B3 population, draws, screen
# ---------------------------------------------------------------------------


def month_end_day(month: Month) -> date:
    nxt = add_months(month, 1)
    return date(nxt[0], nxt[1], 1) - timedelta(days=1)


@dataclass(frozen=True)
class FormationCensus:
    year: int
    #: Names with any total-return row in the 12 months ending at formation.
    candidates: int
    failed_history: int
    failed_stale: int
    #: B3-5 only: no raw (Intrader) close at the formation month, so the $5 test cannot apply (Amendment 2).
    failed_no_raw_close: int
    failed_low_price: int
    population: int
    unlinked: int
    included_formation_returns: tuple[float, ...]
    excluded_formation_returns: tuple[float, ...]


def formation_population(
    year: int,
    rows: Mapping[int, Mapping[Month, MonthlyTotalReturn]],
    *,
    raw_close: Callable[[int, Month], float | None] | None,
    linked: Callable[[int], bool],
) -> tuple[list[int], FormationCensus]:
    """Spec §B3 "Formation population", reading only rows up to the formation month.

    ``raw_close`` is ``None`` for B3-all; for B3-5 it returns the name's raw (Intrader) formation close.
    """
    formation = (year, 12)
    window = month_range(add_months(formation, -(B3_HISTORY_MONTHS - 1)), formation)
    included: list[int] = []
    inc_ret: list[float] = []
    exc_ret: list[float] = []
    candidates = failed_history = failed_stale = failed_no_raw = failed_low = 0
    for key in sorted(rows):
        months = rows[key]
        if not any(m in months for m in window):
            continue
        candidates += 1
        row = months.get(formation)
        if not all(m in months for m in window) or row is None:
            failed_history += 1
            if row is not None:
                exc_ret.append(row.total_return)
            continue
        if (month_end_day(formation) - row.end_bar).days > B3_STALE_DAYS:
            failed_stale += 1
            exc_ret.append(row.total_return)
            continue
        if raw_close is not None:
            close = raw_close(key, formation)
            if close is None:
                failed_no_raw += 1
                exc_ret.append(row.total_return)
                continue
            if not math.isfinite(close) or close <= 0:
                raise RuntimeError(f"name {key} has raw close {close} at {ym(formation)}: B3-5 refuses")
            if Decimal(repr(close)) < B3_MIN_RAW_CLOSE:
                failed_low += 1
                exc_ret.append(row.total_return)
                continue
        included.append(key)
        inc_ret.append(row.total_return)
    if len(included) < B3_NAMES:
        raise RuntimeError(f"formation {ym(formation)} has {len(included)} eligible names, fewer than {B3_NAMES}")
    census = FormationCensus(
        year=year,
        candidates=candidates,
        failed_history=failed_history,
        failed_stale=failed_stale,
        failed_no_raw_close=failed_no_raw,
        failed_low_price=failed_low,
        population=len(included),
        unlinked=sum(1 for key in included if not linked(key)),
        included_formation_returns=tuple(inc_ret),
        excluded_formation_returns=tuple(exc_ret),
    )
    return included, census


def draw_names(population: Sequence[int], *, variant: str, draw: int, year: int) -> list[int]:
    """Spec §B3 "Draws": 50 names uniformly without replacement from the ``name_key``-sorted population."""
    rng = random.Random(f"3609-step0:{variant}:{draw}:{year}")
    return rng.sample(sorted(population), B3_NAMES)


class ScreenClass:
    CONTRADICTED = "contradicted"
    CORROBORATED = "corroborated"
    UNARBITRATED = "unarbitrated"


def screen_class(row: MonthlyTotalReturn, intrader_return: float | None) -> str | None:
    """Spec §"Monthly-return screen": ``None`` for an unflagged row, else its arbitration class."""
    if SCREEN_DOWN <= row.total_return <= SCREEN_UP:
        return None
    month = (row.month.year, row.month.month)
    if row.vendor != PWB_VENDOR or month > LAST_SURVIVORSHIP_FREE_MONTH or intrader_return is None:
        return ScreenClass.UNARBITRATED
    if abs(row.total_return - intrader_return) > ARBITRATION_TOLERANCE:
        return ScreenClass.CONTRADICTED
    return ScreenClass.CORROBORATED


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_INTRADER_SERIES_SQL = """
SELECT vendor_symbol, series_id FROM research_price_series
WHERE vendor = %(vendor)s AND vendor_symbol = ANY(%(symbols)s::text[])
"""

_INSTRUMENT_SQL = "SELECT symbol, instrument_id FROM instruments WHERE symbol = ANY(%(symbols)s::text[])"

#: Spec §"Factor data" (Amendment 1): per dataset, the latest accepted snapshot under the current parser version.
_FACTOR_SNAPSHOT_SQL = """
SELECT snapshot_id, response_sha256 FROM reference_data_snapshots
WHERE source = %(source)s AND dataset_key = %(dataset_key)s AND parser_version = %(parser_version)s
  AND parse_status = 'accepted' AND (%(snapshot_id)s::bigint IS NULL OR snapshot_id = %(snapshot_id)s::bigint)
ORDER BY fetched_at DESC, snapshot_id DESC LIMIT 1
"""

_CHUNK = 2_000


def month_end_closes(
    conn: psycopg.Connection[Any], series_ids: Sequence[int], wanted: Callable[[int, Month], bool]
) -> dict[tuple[int, Month], MonthEnd]:
    """``load_month_ends`` in chunks, keeping only the (series, month) pairs ``wanted`` asks for."""
    out: dict[tuple[int, Month], MonthEnd] = {}
    ids = sorted(set(series_ids))
    for start in range(0, len(ids), _CHUNK):
        for series_id, months in load_month_ends(conn, ids[start : start + _CHUNK]).items():
            for month, end in months.items():
                if wanted(series_id, month):
                    out[(series_id, month)] = end
    return out


def load_factors(
    conn: psycopg.Connection[Any], pinned: Mapping[str, int]
) -> tuple[dict[str, dict[Month, float]], dict[str, int]]:
    factors: dict[str, dict[Month, float]] = {}
    snapshots: dict[str, int] = {}
    for dataset_key, series_keys in FACTOR_DATASETS:
        spec = REFERENCE_DATASETS[dataset_key]
        row = conn.execute(
            _FACTOR_SNAPSHOT_SQL,
            {
                "source": spec.source,
                "dataset_key": dataset_key,
                "parser_version": spec.parser_version,
                "snapshot_id": pinned.get(dataset_key),
            },
        ).fetchone()
        if row is None:
            raise RuntimeError(f"no accepted {dataset_key} snapshot (pinned {pinned.get(dataset_key)})")
        snapshots[dataset_key] = int(row[0])
        for series_key in series_keys:
            series = load_reference_series(
                conn,
                source=spec.source,
                dataset_key=dataset_key,
                series_key=series_key,
                response_sha256=str(row[1]),
                parser_version=spec.parser_version,
            )
            if series.snapshot_id != int(row[0]):
                raise RuntimeError(f"{dataset_key} resolved snapshot {series.snapshot_id}, expected {row[0]}")
            if series.unit != FACTOR_UNIT:
                raise RuntimeError(f"{dataset_key}/{series_key} unit {series.unit!r}, expected {FACTOR_UNIT!r}")
            factors[series_key] = series.values
    return factors, snapshots


def regression_months(months: Sequence[Month], factors: Mapping[str, Mapping[Month, float]]) -> list[Month] | None:
    """Amendment 1: the window's path months that every factor series covers; ``None`` under 60 months.

    A month missing from a factor series inside the covered span refuses: only the tail may be cut.
    """
    names = (*FACTOR_REGRESSORS, "RF")
    covered = [m for m in months if all(m in factors[n] for n in names)]
    if not covered:
        return None
    span = [m for m in months if m <= covered[-1]]
    if span != covered:
        missing = sorted(set(span) - set(covered))
        raise RuntimeError(f"factor series miss interior months {[ym(m) for m in missing]}: regression refuses")
    return covered if len(covered) >= REGRESSION_MIN_MONTHS else None


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def holding_periods(decisions: Sequence[Month], end: Month) -> list[tuple[Month, tuple[Month, ...]]]:
    """Each decision with the months held after it: to the next decision's close, or to ``end``."""
    out: list[tuple[Month, tuple[Month, ...]]] = []
    for i, decision in enumerate(decisions):
        last = decisions[i + 1] if i + 1 < len(decisions) else end
        out.append((decision, tuple(month_range(add_months(decision, 1), min(last, end)))))
    return out


def windows_for(end: Month) -> dict[str, tuple[Month, Month] | None]:
    """W1a/W1b (Amendment 3), W1, W2, W3 and the full window, clipped to the path end (Amendment 1, B3-5)."""
    return {
        "W1a": W1A,
        "W1b": W1B,
        "W1": W1,
        "W2": (W2[0], min(W2[1], end)),
        "W3": (add_months(LAST_SURVIVORSHIP_FREE_MONTH, 1), end) if end > LAST_SURVIVORSHIP_FREE_MONTH else None,
        "full (mixed coverage)": (FIRST_RETURN_MONTH, end),
    }


@dataclass
class Runs:
    """One baseline's decision path under every cost scenario."""

    by_cost: dict[str, PathResult]

    def returns(self, cost: str) -> dict[Month, float]:
        return self.by_cost[cost].returns()


def run_costs(periods: Sequence[Period]) -> Runs:
    return Runs({name: simulate(periods, cost_multiplier=k) for name, k in COSTS.items()})


def path_stats(
    runs: Runs, end: Month, benchmark: Mapping[Month, float], factors: Mapping[str, Mapping[Month, float]]
) -> dict[str, dict[str, float | None] | None]:
    net, gross, stress = runs.returns("net"), runs.returns("gross"), runs.returns("stress_2x")
    turnover = runs.by_cost["net"].turnover
    out: dict[str, dict[str, float | None] | None] = {}
    for name, window in windows_for(end).items():
        if window is None:
            out[name] = None
            continue
        months = month_range(*window)
        regression = regression_months(months, factors) if name in REGRESSION_WINDOWS else None
        out[name] = window_stats(months, net, gross, stress, benchmark, turnover, factors, regression)
    return out


def etf_periods(
    weights: Mapping[str, float],
    rows: Mapping[str, Mapping[Month, Any]],
    entry_half_spread: Mapping[str, float],
    *,
    rebalance: bool,
    end: Month,
) -> list[Period]:
    """B1/B2: buy at 2009-12; B2 rebalances to target every December close (spec §B2)."""
    decisions = [START] + ([(y, 12) for y in range(START[0] + 1, end[0]) if (y + 1, 1) <= end] if rebalance else [])
    assets = tuple(sorted(weights))
    periods: list[Period] = []
    for decision, months in holding_periods(decisions, end):
        trajectories = tuple(
            build_trajectory(
                months,
                {m: r.total_return for m, r in rows[a].items()},
                termination=None,
                arm="worst_case",
                degraded=frozenset(m for m, r in rows[a].items() if r.dividend_capture_degraded),
            )
            for a in assets
        )
        for asset, traj in zip(assets, trajectories, strict=True):
            if traj.exit_index is not None:
                raise RuntimeError(f"{asset} has no total return in {ym(months[traj.exit_index])}: coverage refuses")
        periods.append(
            Period(
                decision=decision,
                months=months,
                assets=assets,
                weights=tuple(weights[a] for a in assets),
                trajectories=trajectories,
                entry_half_spreads=tuple(entry_half_spread[a] for a in assets),
            )
        )
    return periods


def pct(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value * 100:.{digits}f}%"


def num(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


_PERCENT_STATS: Final = frozenset(
    {
        "ann_net",
        "ann_vol",
        "max_dd_monthly",
        "active_vs_b1",
        "tracking_error",
        "max_rel_dd_vs_b1",
        "turnover_per_yr",
        "ann_gross",
        "cost_drag",
        "ann_stress_2x",
        "ff6_alpha_ann",
    }
)


def fmt(stat: str, value: float | None) -> str:
    return pct(value) if stat in _PERCENT_STATS else num(value)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, payload: Any) -> str:
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return sha256_file(path)


def git_state() -> tuple[str, bool]:
    sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True).stdout
    return sha, bool(dirty.strip())


def parse_pins(values: Sequence[str]) -> dict[str, int]:
    pins: dict[str, int] = {}
    for value in values:
        key, _, snapshot = value.partition("=")
        if key not in dict(FACTOR_DATASETS) or not snapshot.isdigit():
            raise SystemExit(f"--factor-snapshot expects <dataset_key>=<snapshot_id>, got {value!r}")
        pins[key] = int(snapshot)
    return pins


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--draws", type=int, default=B3_DRAWS, help="B3 draws per variant (the spec fixes 1,000)")
    parser.add_argument("--factor-snapshot", action="append", default=[], help="pin <dataset_key>=<snapshot_id>")
    args = parser.parse_args()
    pins = parse_pins(args.factor_snapshot)
    sha, dirty = git_state()
    if args.draws == B3_DRAWS and dirty:
        raise SystemExit("the declared run needs a clean checkout: the manifest's git SHA must identify the code")
    if args.draws != B3_DRAWS:
        print(f"⚠ --draws {args.draws}: NOT the spec's run ({B3_DRAWS} draws); a smoke run only")

    with psycopg.connect(settings.database_url) as conn:
        # One snapshot for every query, so the manifest describes one input state. Set before the first
        # statement and asserted: a claimed isolation level is not a held one (review-prevention-log).
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        row = conn.execute("SHOW transaction_isolation").fetchone()
        if row is None or row[0] != "repeatable read":
            raise RuntimeError(f"expected a repeatable-read snapshot, got {row}")
        report(conn, draws=args.draws, pins=pins, git=f"{sha}{'+dirty' if dirty else ''}")


def saved_path(run: PathResult) -> dict[str, Any]:
    """Amendment 1 "Saved paths": continuing returns, plus what a slice ending in any month must charge."""
    months = sorted(run.continuing)
    return {
        "months": [ym(m) for m in months],
        "continuing": [run.continuing[m] for m in months],
        "liquidation_by_month": [run.liquidation_by_month[m] for m in months],
        "rebalance_cost": [run.rebalance_cost.get(m, 0.0) for m in months],
    }


def report(conn: psycopg.Connection[Any], *, draws: int, pins: Mapping[str, int], git: str) -> None:
    out_dir = OUT_ROOT / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_dir.mkdir(parents=True, exist_ok=False)
    print(f"# #3609 step 0 baselines — {SPEC_PATH}\n")

    # ---- B1/B2 coverage and E ------------------------------------------------
    etf: EtfTotalReturnPanel = load_etf_total_return_panel(conn, FUNDS)
    etf_rows: dict[str, dict[Month, Any]] = defaultdict(dict)
    for row in etf.rows:
        if row.price_return_only:
            raise RuntimeError(f"{row.symbol} {row.month} is price return only: B1/B2 refuse")
        etf_rows[row.symbol][(row.month.year, row.month.month)] = row
    end = common_end_month(
        {s: str(v) for s, v in etf.verdicts.items()},
        {s: {m for m in months if m >= FIRST_RETURN_MONTH} for s, months in etf_rows.items()},
    )
    e = end.month
    print(f"ETF panel {etf.version}; N-PORT {etf.nport_snapshot}")
    print(f"**Common end month E = {ym(e)}**, limited by {', '.join(end.limiting)} (Amendment 1).")
    print("\n## B2 coverage matrix (2010-01..E; source runs per fund)\n")
    print("fund | verdict | identity (paired, median |diff|) | last month | months after E dropped | source runs")
    for symbol in FUNDS:
        runs_text: list[str] = []
        current: tuple[str, str | None] | None = None
        first = previous = FIRST_RETURN_MONTH
        for m in month_range(FIRST_RETURN_MONTH, e):
            row = etf_rows[symbol][m]
            source = (row.source, row.reference_symbol)
            if source != current:
                if current is not None:
                    runs_text.append(
                        f"{current[0]}{'→' + current[1] if current[1] else ''} {ym(first)}..{ym(previous)}"
                    )
                current, first = source, m
            previous = m
        assert current is not None
        runs_text.append(f"{current[0]}{'→' + current[1] if current[1] else ''} {ym(first)}..{ym(previous)}")
        check = etf.identity.get(symbol)
        identity = f"{check.paired_months}, {pct(check.median_abs_diff, 3)}" if check else "—"
        dropped = len(month_range(add_months(e, 1), end.fund_ends[symbol]))
        print(
            f"{symbol} | {etf.verdicts[symbol]} | {identity} | {ym(end.fund_ends[symbol])} | {dropped} | "
            f"{'; '.join(runs_text)}"
        )
    etf_inputs = [
        {
            "symbol": r.symbol,
            "month": ym((r.month.year, r.month.month)),
            "total_return": r.total_return,
            "source": r.source,
            "source_key": r.source_key,
            "reference_symbol": r.reference_symbol,
            "accession_number": r.accession_number,
        }
        for r in sorted(etf.rows, key=lambda r: (r.symbol, r.month))
    ]
    etf_inputs_sha = write_json(
        out_dir / "etf_inputs.json",
        {
            "rows": etf_inputs,
            "verdicts": {s: str(v) for s, v in etf.verdicts.items()},
            "identity": {s: [c.paired_months, c.median_abs_diff, c.passed] for s, c in etf.identity.items()},
        },
    )

    # ---- ETF raw prices and bands --------------------------------------------
    intrader_ids = {
        str(sym): int(sid)
        for sym, sid in conn.execute(_INTRADER_SERIES_SQL, {"vendor": INTRADER_VENDOR, "symbols": list(FUNDS)})
    }
    instruments: dict[str, list[int]] = defaultdict(list)
    for sym, iid in conn.execute(_INSTRUMENT_SQL, {"symbols": list(FUNDS)}):
        instruments[str(sym)].append(int(iid))
    b2_decisions = [START] + [(y, 12) for y in range(START[0] + 1, e[0]) if (y + 1, 1) <= e]
    trade_months = sorted({*b2_decisions, e})
    intrader_closes = month_end_closes(
        conn, list(intrader_ids.values()), lambda _sid, m: m in trade_months and m <= LAST_SURVIVORSHIP_FREE_MONTH
    )
    etoro_closes: dict[str, dict[Month, MonthEnd]] = {}
    for symbol in FUNDS:
        if len(instruments[symbol]) != 1:
            raise RuntimeError(f"{symbol} resolves to eToro instruments {instruments[symbol]}: raw price refuses")
        etoro_closes[symbol] = etoro_month_ends(conn, instruments[symbol][0])
    print("\n## ETF raw prices at each trade (band fixed at the 2009-12 entry; spec §Costs)\n")
    print("fund | trade month | price source | raw close | band at this price | band charged")
    etf_half: dict[str, float] = {}
    for symbol in FUNDS:
        charged: cost_model.PriceBand | None = None
        for month in trade_months:
            if month <= LAST_SURVIVORSHIP_FREE_MONTH:
                end_bar = intrader_closes.get((intrader_ids[symbol], month))
                source = INTRADER_VENDOR
            else:
                end_bar = etoro_closes[symbol].get(month)
                source = "etoro price_daily"
            if end_bar is None or not math.isfinite(end_bar.close) or end_bar.close <= 0:
                raise RuntimeError(f"{symbol} has no usable raw close at {ym(month)} ({source}): costs refuse")
            band = cost_model.cost_band_for(Decimal(repr(end_bar.close)), price_basis="as_traded")
            if month == START:
                etf_half[symbol] = float(band.half_spread)
                charged = band
            assert charged is not None
            print(
                f"{symbol} | {ym(month)} ({end_bar.bar_date}) | {source} | {end_bar.close:.2f} | {band.label} | "
                f"{charged.label}"
            )

    factors, factor_snapshots = load_factors(conn, pins)
    factor_last = min(max(v) for v in factors.values())

    b1 = run_costs(etf_periods(B1_WEIGHTS, etf_rows, etf_half, rebalance=False, end=e))
    b2a = run_costs(etf_periods(B2A_WEIGHTS, etf_rows, etf_half, rebalance=True, end=e))
    b2b = run_costs(etf_periods(B2B_WEIGHTS, etf_rows, etf_half, rebalance=True, end=e))
    benchmark = b1.returns("net")
    single = {"B1 SPY": b1, "B2a 60/40": b2a, "B2b Swensen": b2b}
    single_stats = {label: path_stats(runs, e, benchmark, factors) for label, runs in single.items()}

    # ---- B3 ------------------------------------------------------------------
    validated = load_validated_universe(conn)
    selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset(validated))
    panel = load_total_return_panel(conn, selection)
    admitted: dict[int, AdmittedSeries] = {a.name_key: a for a in selection.admitted}
    rows: dict[int, dict[Month, MonthlyTotalReturn]] = defaultdict(dict)
    for row in panel.rows:
        rows[row.name_key][(row.month.year, row.month.month)] = row
    returns = {key: {m: r.total_return for m, r in months.items()} for key, months in rows.items()}
    degraded = {
        key: frozenset(m for m, r in months.items() if r.dividend_capture_degraded) for key, months in rows.items()
    }

    path_end = {"all": e, "5": LAST_SURVIVORSHIP_FREE_MONTH}
    years = {v: list(range(START[0], B3_LAST_FORMATION[v] + 1)) for v in B3_VARIANTS}
    for v in B3_VARIANTS:
        years[v] = [y for y in years[v] if (y + 1, 1) <= path_end[v]]

    # Raw (Intrader) December closes for every admitted series, and Intrader levels for screen arbitration.
    flagged_pwb = {
        (key, m)
        for key, months in rows.items()
        for m, r in months.items()
        if r.vendor == PWB_VENDOR
        and m <= LAST_SURVIVORSHIP_FREE_MONTH
        and not SCREEN_DOWN <= r.total_return <= SCREEN_UP
    }
    series_of = {key: a.series_id for key, a in admitted.items()}
    arbitration = {(series_of[k], m) for k, m in flagged_pwb} | {
        (series_of[k], add_months(m, -1)) for k, m in flagged_pwb
    }
    closes = month_end_closes(
        conn,
        list(series_of.values()),
        lambda sid, m: (m[1] == 12 and m <= LAST_SURVIVORSHIP_FREE_MONTH) or (sid, m) in arbitration,
    )
    pwb_ids = dict(panel.pwb_series)
    # PWB supplies rows from SWITCH_MONTH, so a PWB December close is the only price a name without a raw one has.
    pwb_closes = month_end_closes(conn, list(pwb_ids.values()), lambda _sid, m: m[1] == 12 and m >= SWITCH_MONTH)

    def raw_close(key: int, month: Month) -> float | None:
        end_bar = closes.get((series_of[key], month))
        return None if end_bar is None else end_bar.close

    def intrader_return(key: int, month: Month) -> float | None:
        now, before = closes.get((series_of[key], month)), closes.get((series_of[key], add_months(month, -1)))
        return None if now is None or before is None else now.adj_close / before.adj_close - 1.0

    entry_cache: dict[tuple[int, int], float] = {}

    def entry_half(key: int, year: int) -> float:
        if (key, year) not in entry_cache:
            month = (year, 12)
            # Amendment 2: a name with no raw close (from 2024-12 every name; in 2022-12 a name whose PWB history
            # predates its Intrader series) is priced split-adjusted, so it pays the widest band.
            price, basis = (raw_close(key, month) if month <= LAST_SURVIVORSHIP_FREE_MONTH else None), "as_traded"
            if price is None:
                pwb = pwb_closes.get((pwb_ids[key], month)) if key in pwb_ids else None
                price, basis = (None if pwb is None else pwb.close), "split_adjusted"
            if price is None or not math.isfinite(price) or price <= 0:
                raise RuntimeError(f"name {key} has no usable price at {ym(month)}: costs refuse")
            band = cost_model.cost_band_for(Decimal(repr(price)), price_basis=basis)  # type: ignore[arg-type]
            entry_cache[(key, year)] = float(band.half_spread)
        return entry_cache[(key, year)]

    terminations = {
        key: Termination(classify_termination(a.termination), max(rows[key]))
        for key, a in admitted.items()
        if a.termination is not None and rows.get(key)
    }

    holdings: dict[str, dict[int, dict[int, list[int]]]] = {}
    censuses: dict[str, list[FormationCensus]] = {}
    screen_census: dict[str, Counter[str]] = {}
    b3_stats: dict[tuple[str, str, str], dict[str, dict[str, list[float]]]] = {}
    b3_extra: dict[tuple[str, str, str], dict[str, Any]] = {}
    saved_paths: dict[str, Any] = {
        "rule": (
            "continuing[m] includes month m's rebalance cost and no liquidation. A slice ending at month m charges, "
            "for its last month, (1 + continuing[m]) / (1 - rebalance_cost[m]) * (1 - liquidation_by_month[m]) - 1."
        ),
        **{label: saved_path(r.by_cost["net"]) for label, r in single.items()},
    }
    for variant in B3_VARIANTS:
        populations: dict[int, list[int]] = {}
        censuses[variant] = []
        for year in years[variant]:
            population, census = formation_population(
                year,
                rows,
                raw_close=raw_close if variant == "5" else None,
                linked=lambda key: admitted[key].instrument_id is not None,
            )
            populations[year] = population
            censuses[variant].append(census)
        periods_of = holding_periods([(y, 12) for y in years[variant]], path_end[variant])
        # Screen over the whole formation population's holding-year rows (spec §"Monthly-return screen").
        classes: dict[tuple[int, Month], str] = {}
        for (decision, months), year in zip(periods_of, years[variant], strict=True):
            for key in populations[year]:
                for m in months:
                    row = rows[key].get(m)
                    if row is None:
                        continue
                    cls = screen_class(row, intrader_return(key, m) if row.vendor == PWB_VENDOR else None)
                    if cls is not None:
                        classes[(key, m)] = cls
        screen_census[variant] = Counter(classes.values())
        flagged_of: dict[int, set[Month]] = defaultdict(set)
        contradicted_of: dict[int, dict[Month, float]] = defaultdict(dict)
        for (key, m), cls in classes.items():
            flagged_of[key].add(m)
            if cls == ScreenClass.CONTRADICTED:
                value = intrader_return(key, m)
                assert value is not None
                contradicted_of[key][m] = value

        draws_of = {
            draw: {
                year: draw_names(populations[year], variant=variant, draw=draw, year=year) for year in years[variant]
            }
            for draw in range(draws)
        }
        holdings[variant] = draws_of
        trajectory_cache: dict[tuple[int, int, str, str], Trajectory] = {}

        def trajectory(key: int, year: int, months: tuple[Month, ...], arm: AmbiguityArm, screen: str) -> Trajectory:
            cache_key = (key, year, arm, screen)
            if cache_key not in trajectory_cache:
                rets = returns[key]
                if screen == "contradicted_replaced" and key in contradicted_of:
                    rets = {**rets, **contradicted_of[key]}
                trajectory_cache[cache_key] = build_trajectory(
                    months,
                    rets,
                    termination=terminations.get(key),
                    arm=arm,
                    degraded=degraded[key],
                    flagged=frozenset(flagged_of.get(key, ())),
                    contradicted=frozenset(contradicted_of.get(key, {})),
                )
            return trajectory_cache[cache_key]

        for arm in ARMS:
            for screen in SCREEN_ARMS:
                label = (variant, arm, screen)
                stats: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
                exits: Counter[str] = Counter()
                exit_share: dict[str, list[float]] = defaultdict(list)
                degraded_means: list[float] = []
                held_flagged = held_contradicted = 0
                for draw in range(draws):
                    periods = [
                        Period(
                            decision=decision,
                            months=months,
                            assets=tuple(draws_of[draw][year]),
                            weights=(1.0 / B3_NAMES,) * B3_NAMES,
                            trajectories=tuple(trajectory(k, year, months, arm, screen) for k in draws_of[draw][year]),
                            entry_half_spreads=tuple(entry_half(k, year) for k in draws_of[draw][year]),
                        )
                        for (decision, months), year in zip(periods_of, years[variant], strict=True)
                    ]
                    runs = run_costs(periods)
                    for window, values in path_stats(runs, path_end[variant], benchmark, factors).items():
                        if values is None:
                            continue
                        for stat, value in values.items():
                            if value is not None:
                                stats[window][stat].append(value)
                    net_run = runs.by_cost["net"]
                    for kind, share in net_run.exits:
                        exits[kind] += 1
                        exit_share[kind].append(share)
                    degraded_means.append(float(np.mean(net_run.degraded_share)))
                    held_flagged += net_run.held_flagged
                    held_contradicted += net_run.held_contradicted
                    if screen == "as_is":
                        saved_paths[f"B3-{variant}:{arm}:{draw}"] = saved_path(net_run)
                b3_stats[label] = stats
                b3_extra[label] = {
                    "exits": exits,
                    "exit_share": exit_share,
                    "degraded": degraded_means,
                    "held_flagged": held_flagged / draws,
                    "held_contradicted": held_contradicted / draws,
                }

    # ---- Print ---------------------------------------------------------------
    print_costs()
    print_census(censuses, screen_census, b3_extra, draws)
    print_results(single_stats, single, b3_stats, e, factors, factor_snapshots, factor_last, draws)

    holdings_sha = write_json(
        out_dir / "holdings.json",
        {
            v: {str(d): {str(y): names for y, names in years_.items()} for d, years_ in by_draw.items()}
            for v, by_draw in holdings.items()
        },
    )
    paths_sha = write_json(out_dir / "paths.json", saved_paths)
    name_keys_sha = hashlib.sha256(",".join(str(k) for k in sorted(admitted)).encode()).hexdigest()
    manifest = {
        "git": git,
        "spec": SPEC_PATH,
        "amendments": ["1: common end month E", "2: formation names with no raw close", "3: W1 split at 2014-12"],
        "draws": draws,
        "declared_run": draws == B3_DRAWS,
        "versions": {
            "TOTAL_RETURN_SPLICE_VERSION": TOTAL_RETURN_SPLICE_VERSION,
            "ETF_TOTAL_RETURN_VERSION": ETF_TOTAL_RETURN_VERSION,
            "UNIVERSE_SELECTION_RULE_VERSION": UNIVERSE_SELECTION_RULE_VERSION,
            "TERMINATION_RULE_VERSION": TERMINATION_RULE_VERSION,
            "COST_MODEL_ID": cost_model.COST_MODEL_ID,
            "QUARANTINE_RULE_SET_VERSION": QUARANTINE_RULE_SET_VERSION,
        },
        "captures": {"pwb": PWB_CAPTURE_DATE.isoformat(), "intrader": INTRADER_CAPTURE_DATE.isoformat()},
        "nport_snapshot": etf.nport_snapshot,
        "factor_snapshots": factor_snapshots,
        "end_month": ym(e),
        "end_limiting": list(end.limiting),
        "fund_last_months": {s: ym(m) for s, m in end.fund_ends.items()},
        "sha256": {"holdings": holdings_sha, "paths": paths_sha, "etf_inputs": etf_inputs_sha},
        "admitted_name_keys_sha256": name_keys_sha,
        "admitted_names": len(admitted),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"\nManifest: {out_dir / 'manifest.json'}")


_UNVERIFIED: Final = {
    TerminationClass.EXCHANGE_FAILURE_A4.value: " — (a)(4) ≈ (b) asserted, unverified per name",
    TerminationClass.Q_SUFFIX_OTC.value: " — asserted, unverified per name",
}


def spread(values: Sequence[float]) -> str:
    if not values:
        return "—"
    return " / ".join(pct(nearest_rank(values, p)) for p in (5, 50, 95))


def print_costs() -> None:
    print("\n## Cost model (spec §Costs)\n")
    print(f"`COST_MODEL_ID` = {cost_model.COST_MODEL_ID}")
    print("band | p75 round trip | half-spread per side | sample size")
    for band in cost_model.BANDS:
        print(f"{band.label} | {band.p75_spread_pct}% | {band.half_spread_pct}% | {band.sample_size}")
    print(
        "No raw price (every B3 formation from 2024-12, and Amendment 2's formation names without an Intrader "
        f"close): {cost_model.UNKNOWN_NOMINAL_PRICE_BAND.label} band"
    )
    print("Scenarios: gross (x0), net (x1), stress (every band x2: a sensitivity, not a bound).")
    print("Calibration limits:")
    for limit in cost_model.CALIBRATION_LIMITS:
        print(f"- {limit}")
    print(
        "- B1/B2 ETFs are charged the STOCK band of the fund's raw price: a provisional assumption, not a "
        "calibrated eToro ETF cost (spec §Costs exception)."
    )
    print(
        "- Charged as zero: slippage beyond the spread, gaps, fees; carry and FX (real settlement assumed); "
        "the $10 minimum ticket. Every figure is pre-withholding."
    )


def print_census(
    censuses: Mapping[str, Sequence[FormationCensus]],
    screen: Mapping[str, Counter[str]],
    extra: Mapping[tuple[str, str, str], Mapping[str, Any]],
    draws: int,
) -> None:
    print("\n## B3 exclusion census (per formation; formation-month return p5 / p50 / p95)\n")
    for variant, rows in censuses.items():
        print(f"\n### B3-{variant}\n")
        print(
            "formation | candidates | fail history | fail stale | no raw close | fail <$5 | population | "
            "no instrument link | included return | excluded return (n)"
        )
        for c in rows:
            print(
                f"{c.year}-12 | {c.candidates} | {c.failed_history} | {c.failed_stale} | {c.failed_no_raw_close} | "
                f"{c.failed_low_price} | {c.population} | {pct(c.unlinked / c.population, 1)} | "
                f"{spread(c.included_formation_returns)} | {spread(c.excluded_formation_returns)} "
                f"({len(c.excluded_formation_returns)})"
            )
    print("\n## Monthly-return screen (> +300% or < −90%, over the formation population's holding-year rows)\n")
    print(
        "variant | contradicted | corroborated | unarbitrated | draws holding a flagged row | "
        "draws holding a contradicted row"
    )
    for variant, counts in screen.items():
        e = extra[(variant, "worst_case", "as_is")]
        print(
            f"B3-{variant} | {counts.get(ScreenClass.CONTRADICTED, 0)} | {counts.get(ScreenClass.CORROBORATED, 0)} | "
            f"{counts.get(ScreenClass.UNARBITRATED, 0)} | {pct(e['held_flagged'], 1)} | "
            f"{pct(e['held_contradicted'], 1)}"
        )
    print("\n## Endings inside a holding year (net, screen as-is; per path means over draws)\n")
    print("variant | arm | kind | events per path | mean NAV share at the event")
    for (variant, arm, screen_arm), e in extra.items():
        if screen_arm != "as_is":
            continue
        for kind in sorted(e["exits"]):
            shares = e["exit_share"][kind]
            label = kind + _UNVERIFIED.get(kind, "")
            imputed = "" if kind == COVERAGE_EXIT else " (imputed realisation, no sale)"
            print(
                f"B3-{variant} | {arm} | {label}{imputed} | {e['exits'][kind] / draws:.2f} | "
                f"{pct(float(np.mean(shares)), 3)}"
            )
    print("\n## NAV share held in `dividend_capture_degraded` rows (path mean; p5 / p50 / p95 over draws)\n")
    for (variant, arm, screen_arm), e in extra.items():
        if screen_arm == "as_is":
            print(f"B3-{variant} {arm}: {spread(e['degraded'])}")


def print_results(
    single_stats: Mapping[str, Mapping[str, Mapping[str, float | None] | None]],
    single: Mapping[str, Runs],
    b3_stats: Mapping[tuple[str, str, str], Mapping[str, Mapping[str, list[float]]]],
    end: Month,
    factors: Mapping[str, Mapping[Month, float]],
    factor_snapshots: Mapping[str, int],
    factor_last: Month,
    draws: int,
) -> None:
    for label, runs in single.items():
        share = float(np.mean(runs.by_cost["net"].degraded_share))
        print(f"{label}: NAV share in `dividend_capture_degraded` rows {pct(share, 3)}")
    print(
        "\n## Results\n\nB1 from 2022-01 is a synthetic history (IVV N-PORT NAV total return). W2 is reused "
        "validation; W3 is survivor-only. B3 cells are p5 / p50 / p95 across "
        f"{draws} draws: basket-selection dispersion on this one history, not confidence intervals; the medians "
        "of different statistics need not come from one portfolio. FF6 = French FF5 + Mom, Newey–West HAC, "
        f"exploratory only (snapshots {dict(factor_snapshots)}; factors through {ym(factor_last)})."
    )
    b3_5_end = LAST_SURVIVORSHIP_FREE_MONTH
    tables: list[tuple[str, tuple[Month, Month], list[str], list[tuple[str, str, str]]]] = []
    for window, span in windows_for(end).items():
        if span is None:
            continue
        b3_5_span = windows_for(b3_5_end)[window]
        labels = [label for label in b3_stats if label[0] == "all" or b3_5_span == span]
        tables.append((window, span, list(single_stats), labels))
    tables.append(
        ("full (mixed coverage)", (FIRST_RETURN_MONTH, b3_5_end), [], [label for label in b3_stats if label[0] == "5"])
    )
    for window, span, single_labels, b3_labels in tables:
        heading = "B3-5 only, its own endpoint" if not single_labels else window
        print(f"\n### {heading}: {ym(span[0])}..{ym(span[1])}\n")
        if window in WINDOW_NOTES:
            print(WINDOW_NOTES[window])
        if window == "W2":
            print("B3-5's path ends 2024-08, so its W2 carries its liquidation; no other W2 does.")
        if window in REGRESSION_WINDOWS:
            regression = regression_months(month_range(*span), factors)
            if regression is None:
                print("FF6 regression: under 60 covered months, not run.")
            else:
                print(f"FF6 regression span {ym(regression[0])}..{ym(regression[-1])}, {len(regression)} months.")
        print("baseline | " + " | ".join(STAT_NAMES))
        for label in single_labels:
            values = single_stats[label][window]
            assert values is not None
            print(label + " | " + " | ".join(fmt(stat, values.get(stat)) for stat in STAT_NAMES))
        for variant, arm, screen_arm in b3_labels:
            stats = b3_stats[(variant, arm, screen_arm)]
            cells = []
            for stat in STAT_NAMES:
                values = stats[window].get(stat, [])
                cells.append(" / ".join(fmt(stat, nearest_rank(values, p)) for p in (5, 50, 95)) if values else "n/a")
            print(f"B3-{variant} {arm} {screen_arm} | " + " | ".join(cells))


if __name__ == "__main__":
    main()
