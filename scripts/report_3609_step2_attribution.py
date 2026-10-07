"""#3609 step 2's turnover and attribution diagnostics (slice 3c-v(b)): printed, never gated.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Diagnostics" (PR #3666): "Turnover above 50% a
month" and "Attribution"; §"The book", "No sector cap" and "The 2× flag". Pure functions over
:class:`~app.services.factor_book_series.SeriesRun`, which has already refused every data, comparator and
non-positive-wealth condition. The verdict's turnover veto is ``report_3609_step2_verdict.turnover_exceedances``;
this module prints the months behind it over the whole path. Windows are ``report_3609_step2_operations``'s.

* **Turnover above 50%.** Formation M's trades are charged in return month M and month M + 1 holds their result
  (``report_3609_step2_verdict``, "Month labels"). So each such formation prints its own order notional and cost,
  and the realised book-minus-control-median return of month M + 1, gross and net. Month M + 1's net return carries
  formation M + 1's cost, as the path labels it; formation M's is the cost column. Labelled realised, from one
  history: no expected-benefit model exists here.
* **Attribution, per window at base cost:** SPY beta (B1 is SPY's total return); the universe effect, the
  equal-weight universe's G minus B1's; selection, the book's G minus the equal-weight universe's; and the full
  FF5 + momentum loadings, G1's regression on the window's own months. G is the verdict's annualised log growth,
  so universe effect plus selection is the book's G minus B1's exactly.
* **FF-12 weights,** the book's against the reconstituted cap-weighted universe's, post-trade at each formation and
  averaged over the formations held into a window's months. They do not depend on the arm. An industry is flagged
  ``2x`` when the book's weight is above twice the reference's, and ``no reference weight`` when the reference holds
  none of it and the book does. A same-universe diagnostic, not the skill's benchmark cap (§"The book").
* **Information, per window:** the lag-1 autocorrelation of the book's monthly active return over B1, and the ratio
  of its mean's Newey–West standard error to its iid one.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

import numpy as np

from app.services.factor_book_path import Decision, Month, PathResult, month_of, next_month
from app.services.factor_book_series import ARMS, SeriesRun
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_baselines import nearest_rank
from scripts.report_3609_step2_operations import Window, stage_windows, window_returns
from scripts.report_3609_step2_signals import mean_errors
from scripts.report_3609_step2_verdict import BASE, GROSS, TURNOVER_VETO, G1Result, g1, log_growth

#: §"The book", "The 2× flag": a book industry weight above this multiple of the reference's is flagged.
OVERWEIGHT: Final = 2.0
FLAG_OVERWEIGHT: Final = "2x"
FLAG_NO_REFERENCE: Final = "no reference weight"
#: §"Diagnostics", Information: the fewest adjacent pairs, and months, either figure is computed on.
MIN_INFORMATION: Final = 24


# --------------------------------------------------------------------------- turnover above 50%


@dataclass(frozen=True)
class HighTurnover:
    """One formation whose base-cost one-way turnover exceeds ``TURNOVER_VETO``."""

    formation: Month
    turnover: float
    notional: float
    cost: float
    #: Month M + 1, which holds the formation's result.
    held: Month
    #: The book's return in ``held`` minus the control draws' nearest-rank median there.
    gross_vs_control: float
    net_vs_control: float


def _median(run: SeriesRun, arm: AmbiguityArm, cost: str, month: Month) -> float:
    return nearest_rank([draw.returns[month] for draw in run.control[(arm, cost)]], 50)


def high_turnover(run: SeriesRun, arm: AmbiguityArm) -> tuple[HighTurnover, ...]:
    """Every formation on the path above ``TURNOVER_VETO`` at base cost, in order."""
    book = run.book[(arm, BASE)]
    gross = run.book[(arm, GROSS)]
    out: list[HighTurnover] = []
    for formation in sorted(m for m, t in book.turnover.items() if t > TURNOVER_VETO):
        trades = [t for t in book.trades if t.month == formation]
        held = next_month(formation)
        out.append(
            HighTurnover(
                formation,
                book.turnover[formation],
                math.fsum(t.notional for t in trades),
                math.fsum(t.cost for t in trades),
                held,
                gross.returns[held] - _median(run, arm, GROSS, held),
                book.returns[held] - _median(run, arm, BASE, held),
            )
        )
    return tuple(out)


@dataclass(frozen=True)
class TurnoverMonths:
    #: Per arm, its formations above the threshold.
    by_arm: Mapping[str, tuple[HighTurnover, ...]]

    @property
    def n(self) -> int:
        """Calendar months above the threshold in either arm."""
        return len({row.formation for rows in self.by_arm.values() for row in rows})


def turnover_months(run: SeriesRun) -> TurnoverMonths:
    return TurnoverMonths({arm: high_turnover(run, arm) for arm in ARMS})


# --------------------------------------------------------------------------- attribution


@dataclass(frozen=True)
class Attribution:
    spy_beta: float | None
    book_g: float
    equal_weight_g: float
    b1_g: float
    regression: G1Result

    @property
    def universe_effect(self) -> float:
        return self.equal_weight_g - self.b1_g

    @property
    def selection(self) -> float:
        return self.book_g - self.equal_weight_g


def _returns(path: PathResult, window: Window) -> dict[Month, float]:
    return window_returns(path.returns, path.nav, path.boundary, window)


def attribution(
    run: SeriesRun, arm: AmbiguityArm, window: Window, factors: Mapping[str, Mapping[Month, float]]
) -> Attribution:
    """The window's attribution at base cost. The beta is the OLS slope on B1 (step 0's ``beta_vs_b1``), ``None``
    when B1 does not vary in the window."""
    months = list(window.months)
    book = _returns(run.book[(arm, BASE)], window)
    equal_weight = _returns(run.equal_weight[(arm, BASE)], window)
    b1 = run.b1[BASE].returns
    x = np.array([b1[m] for m in months])
    y = np.array([book[m] for m in months])
    beta = float(np.polyfit(x, y, 1)[0]) if len(months) > 1 and np.ptp(x) > 0 else None
    return Attribution(
        beta if beta is not None and math.isfinite(beta) else None,
        log_growth(book, months, f"book {arm}"),
        log_growth(equal_weight, months, f"equal-weight universe {arm}"),
        log_growth(b1, months, "b1"),
        g1(book, factors, months),
    )


# --------------------------------------------------------------------------- FF-12 weights


@dataclass(frozen=True)
class IndustryWeight:
    book: float
    reference: float

    @property
    def flag(self) -> str | None:
        if self.book > 0 and self.reference == 0:
            return FLAG_NO_REFERENCE
        if self.book > OVERWEIGHT * self.reference:
            return FLAG_OVERWEIGHT
        return None


def _by_industry(decision: Decision, industry: Mapping[int, str]) -> dict[str, float]:
    missing = sorted(n for n in decision.targets if n not in industry)
    if missing:
        raise ValueError(f"{decision.formation}: {len(missing)} held names have no industry: {missing[:5]}")
    out: dict[str, list[float]] = {}
    for name in decision.targets:
        out.setdefault(industry[name], []).append(decision.share(1.0, name))
    return {k: math.fsum(v) for k, v in out.items()}


def industry_weights(
    book: Sequence[Decision], reference: Sequence[Decision], industries: Sequence[Mapping[int, str]]
) -> dict[Month, dict[str, IndustryWeight]]:
    """Per formation, the post-trade weight in each FF-12 industry (``UNCLASSIFIED`` is its own), for the book and
    the cap-weighted reference, over every industry either holds. A formation holding nothing is all cash."""
    if not len(book) == len(reference) == len(industries):
        raise ValueError(f"{len(book)} book, {len(reference)} reference decisions, {len(industries)} industry maps")
    out: dict[Month, dict[str, IndustryWeight]] = {}
    for mine, theirs, industry in zip(book, reference, industries, strict=True):
        if mine.formation != theirs.formation:
            raise ValueError(f"formations differ: {mine.formation} and {theirs.formation}")
        a, b = _by_industry(mine, industry), _by_industry(theirs, industry)
        out[month_of(mine.formation)] = {k: IndustryWeight(a.get(k, 0.0), b.get(k, 0.0)) for k in sorted(a | b)}
    return out


def average_weights(monthly: Mapping[Month, Mapping[str, IndustryWeight]], window: Window) -> dict[str, IndustryWeight]:
    """The mean weight per industry over the formations held into ``window``'s months (zero where not held)."""
    held = {next_month(formation): weights for formation, weights in monthly.items()}
    rows = [held[m] for m in window.months]
    names = sorted({k for row in rows for k in row})
    zero = IndustryWeight(0.0, 0.0)
    return {
        k: IndustryWeight(
            math.fsum(row.get(k, zero).book for row in rows) / len(rows),
            math.fsum(row.get(k, zero).reference for row in rows) / len(rows),
        )
        for k in names
    }


# --------------------------------------------------------------------------- information


@dataclass(frozen=True)
class Information:
    """§"Diagnostics", "Information, per window", on the book's monthly active return over B1 at base cost."""

    #: Lag-1 Pearson autocorrelation over calendar-adjacent pairs.
    autocorrelation: float | None
    #: The Newey–West standard error of the mean active return over its iid standard error.
    se_ratio: float | None


def _constant(values: np.ndarray) -> bool:
    """Every value equal: tested by equality, never by a float deviation (the #2394 ``ptp`` prevention entry)."""
    return bool(np.ptp(values) == 0.0)


def information(run: SeriesRun, arm: AmbiguityArm, window: Window) -> Information:
    """The autocorrelation is undefined below ``MIN_INFORMATION`` pairs or when the lagged or leading vector is
    constant; the ratio below ``MIN_INFORMATION`` months, at a constant series (zero iid error) or when the
    Newey–West error is not finite and positive; either is undefined where finite but huge returns overflow it. The
    iid error is :func:`mean_errors`'s, so the ratio is
    sqrt(n / n_eff) before the cap. No effective-years figure is printed."""
    book = _returns(run.book[(arm, BASE)], window)
    b1 = run.b1[BASE].returns
    active = np.array([book[m] - b1[m] for m in window.months])
    lagged, leading = active[:-1], active[1:]
    autocorrelation = None
    if len(lagged) >= MIN_INFORMATION and not (_constant(lagged) or _constant(leading)):
        with np.errstate(all="ignore"):  # huge finite returns can overflow the products: undefined, not NaN
            value = float(np.corrcoef(lagged, leading)[0, 1])
        autocorrelation = value if math.isfinite(value) else None
    se_ratio = None
    if len(active) >= MIN_INFORMATION and not _constant(active):
        iid, se = mean_errors(active)
        if math.isfinite(se) and se > 0 and math.isfinite(iid):
            se_ratio = se / math.sqrt(iid)
    return Information(autocorrelation, se_ratio)


# --------------------------------------------------------------------------- assembly


@dataclass(frozen=True)
class Diagnostics:
    turnover: TurnoverMonths
    #: Per arm, then per stage window label.
    attribution: Mapping[str, Mapping[str, Attribution]]
    #: Per formation; and per stage window label, averaged.
    industry_monthly: Mapping[Month, Mapping[str, IndustryWeight]]
    industry_average: Mapping[str, Mapping[str, IndustryWeight]]
    #: Per arm, then per stage window label.
    information: Mapping[str, Mapping[str, Information]]


def diagnostics(
    run: SeriesRun,
    factors: Mapping[str, Mapping[Month, float]],
    book: Sequence[Decision],
    reference: Sequence[Decision],
    industries: Sequence[Mapping[int, str]],
) -> Diagnostics:
    """Every figure in this module. ``book`` is ``book_decisions``'s, ``reference`` the cap-weighted
    ``reference_decisions`` and ``industries`` one FF-12 map per formation."""
    windows = stage_windows(run)
    monthly = industry_weights(book, reference, industries)
    return Diagnostics(
        turnover_months(run),
        {arm: {w.label: attribution(run, arm, w, factors) for w in windows} for arm in ARMS},
        monthly,
        {w.label: average_weights(monthly, w) for w in windows},
        {arm: {w.label: information(run, arm, w) for w in windows} for arm in ARMS},
    )


__all__ = [
    "FLAG_NO_REFERENCE",
    "FLAG_OVERWEIGHT",
    "OVERWEIGHT",
    "Attribution",
    "Diagnostics",
    "HighTurnover",
    "IndustryWeight",
    "Information",
    "MIN_INFORMATION",
    "TurnoverMonths",
    "attribution",
    "average_weights",
    "diagnostics",
    "high_turnover",
    "industry_weights",
    "information",
    "turnover_months",
]
