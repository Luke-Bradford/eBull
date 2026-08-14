"""Phase 5d — criterion 7's metric set, computed on the equity curve.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.4 (three levels,
the return denominator, exposure), §3.4 (what an ambiguous or open position does
to each statistic) and §8 (stage 5d). Parent
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criterion 7 —
*"a metric set that cannot flatter"*. Refs #2240.

⚠⚠ THE ANNUALISATION FACTOR IS MEASURED OFF THE DATE AXIS, NEVER THE 252
CONVENTION.

Every annualised number here (CAGR, volatility, Sharpe, Sortino, turnover)
divides by a periods-per-year, and picking one is exactly the *"am I about to
pick a threshold, ratio or window"* trigger in ``.claude/CLAUDE.md``. No
published rule fixes it — 252 is a convention, not a source rule, and it is
wrong for this panel anyway: the corpus spans 1962-2026 and its per-year trading
date count is not constant. So it is derived, per curve, as
``(len(dates) - 1) / ((last - first) / 365.25 days)``.

⚠ This is the number ``vectorbt`` could not be told. Measured 2026-08-07 on
``vectorbt==1.1.0``: its annualising metrics REFUSE an irregular trading-date
index outright (``ValueError: Index frequency is None``), and the ``freq="1D"``
escape imposes an annualisation of exactly **365.0** — verified by dividing its
Sharpe by the per-period one — against an index carrying ~196 observations per
calendar year, inflating Sharpe by ``sqrt(365/196) = 1.37x``. See
``equity_curve``'s header for the full adoption record.

⚠⚠ TWO METRICS ARE NULLABLE AND NEITHER IS A GAP LEFT OPEN.

- ``effective_sample_size`` is criterion 3's, from a *"block bootstrap over
  calendar blocks with errors clustered by date"*. **Stage 5e-2 computes it**
  (``app/services/block_bootstrap.py``), but only when the caller declares a
  ``bootstrap_seed`` — with no seed it stays ``None`` and the promotion gate
  refuses on it, the same construction ``deflated_sharpe`` still has. ⚠
  Computing a nominal *n* instead would be worse than leaving it null:
  criterion 3 says *"no bare percentage and no nominal n is reported anywhere"*,
  so a filled-in overlap-ignoring count would be the exact number the criterion
  forbids, wearing the name of the one it requires.
- ``profit_factor`` and ``sortino`` are ``None`` only when their DENOMINATOR is
  empty — no losing trade, no losing period. That is a real state, not a missing
  measurement, and ``sql/263`` ties each null to its own count with a CHECK so
  the two cannot be confused.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

import numpy as np
import numpy.typing as npt

from app.services.block_bootstrap import block_bootstrap_expectancy, cluster_by_date
from app.services.equity_curve import EquityCurve

#: Days in a mean Gregorian year, for turning a date span into years.
#: ⚠ 365.25 and not 365: the corpus spans 64 years, where the leap-day drift is
#: 16 days — a sixteenth of a year on a CAGR exponent.
DAYS_PER_YEAR: Final = 365.25

#: The identity of this metric set, hashed nowhere but stamped on the result
#: row's provenance in the same spirit as ``COST_MODEL_ID``: a change to any
#: definition below is a change to what a stored number MEANS, and a reader
#: holding a two-year-old row needs to know which definition produced it.
#: ⚠⚠ BUMPED v1 → v2 by #2623 gap 1, which added the three holding-period fields.
#: A version denotes a RULE SET, not a row population (#2670) — a row carrying
#: metrics `criterion7-v1` never defined cannot truthfully keep that stamp, even
#: though no pre-existing metric changed value.
#:
#: Nothing gates on this VALUE (verified across `app/`, `sql/`, `frontend/src`:
#: written at `strategy_result.py`, read back at `result_ledger.py`, constrained
#: only by `CHECK (metric_set_id <> '')` in `sql/263`). What the bump BUYS is that
#: a null holding period is readable: on a `criterion7-v1` row it is legitimate and
#: permanent, on a `criterion7-v2` row it is a writer defect unless there are no
#: realised trades. `sql/347` enforces exactly that.
METRIC_SET_ID: Final = "criterion7-v2"


@dataclass(frozen=True)
class TradeReturns:
    """The realised trade population, reduced to what criterion 7 reads.

    ⚠ REALISED ONLY, and §3.4 is the reason. An ``ambiguous`` close, an
    ``unresolved`` outcome and a position open at the window end are each
    *"excluded, counted"* from the win rate and from expectancy — but all three
    stay ON the equity curve and IN exposure, because the capital was committed.
    So the trade-level metrics take this object and the path metrics take the
    curve, and the two populations are deliberately different sizes.
    """

    #: Net return per closed trade, in percent. ⚠ NET: ``position_costing``
    #: computes it from adjusted prices, never by subtracting a cost from
    #: ``gross_return_pct``, which ``sql/256`` names GROSS so nothing averages
    #: it as performance.
    net_return_pct: tuple[float, ...]
    #: The entry fill date of each trade above, positionally parallel to it.
    #: ⚠ REQUIRED, not defaulted, and criterion 3 is the reason: it is the key
    #: the block bootstrap clusters on (``block_bootstrap.cluster_by_date``), so
    #: a default would let a caller silently produce a metric set with no
    #: effective sample size and no error.
    entry_fill_date: tuple[date, ...]
    #: The bar each trade CLOSED on, positionally parallel to the two above.
    #: #2623 gap 1 — the "expected turnaround" statistic is derived from this.
    #:
    #: ⚠ NAMED `exit_bar_date`, NOT `exit_fill_date`. The producers hold
    #: `position.close_bar_date` and a permuted exit bar respectively — close
    #: bars, not execution fills. Calling it a fill would quietly equate the two.
    #:
    #: ⚠ REQUIRED, not defaulted, for the same reason `entry_fill_date` is: a
    #: default lets a caller silently produce a metric set whose holding period
    #: is missing, with no error anywhere.
    exit_bar_date: tuple[date, ...]
    #: Positions still open at the window end, and positions whose close
    #: carried no price. Counted, never dropped (§3.2 rule 5).
    open_count: int
    unpriced_count: int

    def __post_init__(self) -> None:
        if len(self.net_return_pct) != len(self.entry_fill_date):
            raise ValueError(
                f"{len(self.net_return_pct)} returns against {len(self.entry_fill_date)} entry dates — the two are "
                "positionally parallel, and a mismatch would cluster returns under the wrong dates"
            )
        if len(self.exit_bar_date) != len(self.entry_fill_date):
            raise ValueError(
                f"{len(self.exit_bar_date)} exit dates against {len(self.entry_fill_date)} entry dates — the two are "
                "positionally parallel, and a mismatch would pair holds across different trades"
            )
        # A same-day close is legal and holds for 0 days; an exit BEFORE its own
        # entry is a producer bug, and it must not reach a statistic that would
        # average it into a plausible-looking median.
        for index, (entry, exit_bar) in enumerate(zip(self.entry_fill_date, self.exit_bar_date, strict=True)):
            if exit_bar < entry:
                raise ValueError(f"trade {index} exits {exit_bar} before it enters {entry}")

    @property
    def hold_days(self) -> tuple[int, ...]:
        """Calendar days held per realised trade. See `compute_metrics`' header."""
        return tuple(
            (exit_bar - entry).days for entry, exit_bar in zip(self.entry_fill_date, self.exit_bar_date, strict=True)
        )


@dataclass(frozen=True)
class StrategyMetrics:
    """Criterion 7's twelve, plus the four counts that make its nulls readable.

    ⚠ FLOATS, NOT ``Decimal``. These are derived statistics off a float64
    equity path; a ``Decimal`` field would advertise an exactness the path does
    not have. ``sql/263`` stores them as ``NUMERIC`` because that is what
    Postgres offers, not because the last digit is meaningful.
    """

    # --- criterion 7's twelve -------------------------------------------
    expectancy_per_trade_pct: float
    profit_factor: float | None
    cagr_pct: float
    annualised_volatility_pct: float
    sharpe: float
    sortino: float | None
    max_drawdown_pct: float
    exposure_time_pct: float
    turnover_annualised: float
    trade_count: int
    effective_sample_size: float | None
    return_vs_buy_and_hold_pct: float

    # --- the supporting record ------------------------------------------
    #: ⚠ Present so ``profit_factor is None`` and ``sortino is None`` are
    #: interpretable rather than ambiguous, and so ``sql/263`` can CHECK each
    #: null against its own denominator.
    losing_trade_count: int
    losing_period_count: int
    open_trade_count: int
    unpriced_trade_count: int
    #: The measured annualisation (see the module header). Stored, because every
    #: annualised number above is a function of it and a reader cannot re-derive
    #: it from the row.
    periods_per_year: float
    total_return_pct: float
    buy_and_hold_return_pct: float

    # --- criterion 3's interval and its provenance ------------------------
    #: The 95% block-bootstrap interval on ``expectancy_per_trade_pct``. ⚠ The
    #: criterion requires BOTH halves — *"report the effective sample size and
    #: confidence interval"* — so these travel with the ESS and never without
    #: it; ``__post_init__`` refuses any partial set.
    expectancy_ci_low_pct: float | None = None
    expectancy_ci_high_pct: float | None = None
    #: ⚠ Declared inputs, stored because criterion 11 makes them part of what a
    #: result MEANS: the same trades under a different block length, seed or
    #: resample count are a different measurement, and a reader holding the row
    #: cannot re-derive any of the three from it.
    bootstrap_block_length: int | None = None
    bootstrap_cluster_count: int | None = None
    bootstrap_resamples: int | None = None
    bootstrap_seed: int | None = None
    bootstrap_design_effect: float | None = None
    bootstrap_model_id: str | None = None

    # --- #2623 gap 1: the "expected turnaround" statistic ------------------
    #: Calendar days held per REALISED trade, at the 25th/50th/75th percentile.
    #:
    #: ⚠⚠ RIGHT-CENSORED, and the direction of the resulting bias is NOT
    #: determinable a priori. `TradeReturns` is realised-only, so a position
    #: still open at the window end contributes nothing — and such a position may
    #: be long-running OR merely recently entered. The censoring is informative;
    #: informative censoring does not fix a sign. Render `open_trade_count` AND
    #: `unpriced_trade_count` beside these, never the median alone.
    #:
    #: ⚠ Null exactly when there are no realised trades. On a `criterion7-v1` row
    #: they are null because the statistic did not exist; `sql/347` is what keeps
    #: those two cases apart. See METRIC_SET_ID.
    median_hold_days: float | None = None
    hold_days_p25: float | None = None
    hold_days_p75: float | None = None
    metric_set_id: str = METRIC_SET_ID

    def __post_init__(self) -> None:
        if self.trade_count < 0 or self.losing_trade_count < 0:
            raise ValueError(f"trade counts must be non-negative: {self.trade_count}, {self.losing_trade_count}")
        if self.losing_trade_count > self.trade_count:
            raise ValueError(
                f"{self.losing_trade_count} losing trades out of {self.trade_count} — a subset cannot exceed its set"
            )
        holds = (self.hold_days_p25, self.median_hold_days, self.hold_days_p75)
        # All-or-nothing: a partial triple means the derivation half-ran, which is
        # a defect that would otherwise render as a plausible single number.
        if any(value is None for value in holds) and any(value is not None for value in holds):
            raise ValueError(f"holding-period percentiles are all-or-nothing, got {holds}")
        if all(value is not None for value in holds):
            p25, median, p75 = (float(value) for value in holds if value is not None)
            if not 0 <= p25 <= median <= p75:
                raise ValueError(f"holding-period percentiles must be ordered and non-negative, got {holds}")
        # ⚠ The same rule `sql/347` enforces, applied HERE so a defect fails at
        # construction with a named field rather than as an integrity error
        # mid-batch. A row stamped with the CURRENT metric set claims to carry
        # the current set's members, so with realised trades the triple is not
        # optional. A legacy `criterion7-v1` object reconstructed from an old
        # stored row keeps its own stamp and is untouched by this.
        if self.metric_set_id == METRIC_SET_ID and self.trade_count > 0 and self.median_hold_days is None:
            raise ValueError(
                f"{self.trade_count} realised trades stamped {self.metric_set_id} with no holding period — "
                "the stamp says this metric set carries one"
            )
        if (self.profit_factor is None) != (self.losing_trade_count == 0):
            raise ValueError(
                f"profit_factor {self.profit_factor!r} against {self.losing_trade_count} losing trades: it is null "
                "exactly when the denominator is empty, and never as a stand-in for 'not computed'"
            )
        if (self.sortino is None) != (self.losing_period_count == 0):
            raise ValueError(
                f"sortino {self.sortino!r} against {self.losing_period_count} losing periods: it is null exactly "
                "when the downside deviation has no observations"
            )
        if self.periods_per_year <= 0.0:
            raise ValueError(f"periods_per_year must be positive, got {self.periods_per_year}")
        if self.max_drawdown_pct > 0.0:
            raise ValueError(
                f"max_drawdown_pct {self.max_drawdown_pct} is positive — a drawdown is a fall from a running peak "
                "and is reported as a non-positive number, so a sign flip cannot read as a good result"
            )
        if not 0.0 <= self.exposure_time_pct <= 100.0:
            raise ValueError(f"exposure_time_pct {self.exposure_time_pct} is outside 0-100")
        if self.effective_sample_size is not None and self.effective_sample_size <= 0.0:
            raise ValueError(f"effective_sample_size must be positive when declared, got {self.effective_sample_size}")
        # ⚠⚠ ALL-OR-NOTHING, and it is criterion 3's own wording that makes it so:
        # the criterion asks for the effective sample size AND the interval, so a
        # row carrying one without the other reports a corrected number whose
        # correction cannot be judged. Enforced here and again by `sql/265` — a
        # partial set is a bug in the caller, not a state the model admits.
        bootstrap_fields = (
            self.effective_sample_size,
            self.expectancy_ci_low_pct,
            self.expectancy_ci_high_pct,
            self.bootstrap_block_length,
            self.bootstrap_cluster_count,
            self.bootstrap_resamples,
            self.bootstrap_seed,
            self.bootstrap_design_effect,
            self.bootstrap_model_id,
        )
        present = sum(field is not None for field in bootstrap_fields)
        if present not in (0, len(bootstrap_fields)):
            raise ValueError(
                f"{present} of {len(bootstrap_fields)} block-bootstrap fields are set: criterion 3 requires the "
                "effective sample size and its interval together, so the set is present or absent as a whole"
            )
        if (
            self.expectancy_ci_low_pct is not None
            and self.expectancy_ci_high_pct is not None
            and self.expectancy_ci_low_pct > self.expectancy_ci_high_pct
        ):
            raise ValueError(
                f"expectancy interval [{self.expectancy_ci_low_pct}, {self.expectancy_ci_high_pct}] is inverted"
            )


def periods_per_year(dates: tuple[date, ...]) -> float:
    """Observations per calendar year, measured off the axis itself.

    ⚠ ``len(dates) - 1`` over the span, not ``len(dates)``: a two-date axis
    spans one interval, and counting endpoints would annualise a 2-day series as
    if it held 2 years of observations. Raises on an axis too short to carry a
    rate — a single date has no span, and inventing one would put a divide-by-
    zero behind a plausible number.
    """
    if len(dates) < 2:
        raise ValueError(f"an axis of {len(dates)} date(s) has no span, so no rate can be measured off it")
    span_days = (dates[-1] - dates[0]).days
    if span_days <= 0:
        raise ValueError(f"axis spans {span_days} days from {dates[0]} to {dates[-1]}")
    return (len(dates) - 1) / (span_days / DAYS_PER_YEAR)


def _daily_returns(equity: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    """Simple period returns off the equity path.

    ⚠ SIMPLE, NOT LOG. Criterion 7's expectancy and profit factor are stated in
    simple returns and the sleeve/portfolio aggregation in §5.4 is a weighted
    sum of simple returns; mixing the two conventions inside one metric set is
    how a Sharpe stops being comparable to the number beside it.
    """
    if len(equity) < 2:
        return np.empty(0, dtype=np.float64)
    previous = equity[:-1]
    with np.errstate(divide="ignore", invalid="ignore"):
        returns = np.where(previous > 0.0, equity[1:] / previous - 1.0, 0.0)
    return np.asarray(returns, dtype=np.float64)


def max_drawdown_pct(equity: npt.NDArray[np.float64]) -> float:
    """The deepest fall from a running peak, in percent, as a NON-POSITIVE number.

    ⚠ PATH-DEPENDENT AND PORTFOLIO-LEVEL, which §5.4 requires and which is the
    reason it is computed from the curve rather than from the trade list: *"a
    per-trade max drawdown does not compose"*. Two trades each down 10% at
    different times do not make a 10% portfolio drawdown, and if they overlap
    they make a worse one.
    """
    if len(equity) == 0:
        return 0.0
    peak = np.maximum.accumulate(equity)
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdown = np.where(peak > 0.0, equity / peak - 1.0, -1.0)
    return float(np.min(drawdown) * 100.0)


def compute_metrics(
    curve: EquityCurve,
    *,
    dates: tuple[date, ...],
    trades: TradeReturns,
    buy_and_hold: EquityCurve | None,
    starting_equity: float = 1.0,
    bootstrap_seed: int | None = None,
) -> StrategyMetrics:
    """Criterion 7's metric set for one curve. Pure; reads no database.

    ⚠⚠ ``buy_and_hold`` IS AN EQUITY CURVE FROM THE SAME MODULE, not a second
    implementation. Criterion 7's twelfth metric is *"return relative to
    buy-and-hold"*, and no published rule says what "buy-and-hold" means on an
    unbalanced panel where instruments list and delist inside the window. Fixed
    by construction: one leg per evaluated instrument, opened at its first
    usable bar in the window and closed at its last, run through
    ``equity_curve`` under the SAME cost model and the SAME fill contract.
    Sharing those is what makes the comparison apples-to-apples — a benchmark
    computed by different machinery would attribute the machinery's difference
    to the strategy.

    ⚠⚠ IT DOES **NOT** SHARE THE SIZING RULE, AND THAT WAS A REAL DEFECT (#2426).
    This docstring used to say "the SAME sizing rule", and the benchmark was
    built by ``build_equity_curve`` accordingly — so it re-imposed equal weight
    on every event date. A rebalanced comparator is not buy-and-hold (Blume &
    Stambaugh, JFE 12, 1983): on the full population it added **23.2 points of
    annual return** to the bar and turned over 137,477,862x the pot. The
    benchmark now comes from ``build_buy_and_hold_curve`` under its own frozen
    ``BENCHMARK_RULE_ID``, which is hashed into the result identity so it cannot
    change again without the version moving.

    ⚠ ``None`` is permitted and means *no benchmark was supplied*, in which case
    the relative return is reported against zero and the absolute one is
    reported beside it. It is NOT a silent 0% benchmark: ``buy_and_hold_return_pct``
    carries the same 0.0 and the two are distinguishable on the row.

    ⚠⚠ ``bootstrap_seed`` IS REQUIRED FOR CRITERION 3 AND DEFAULTS TO OFF.

    ``None`` leaves ``effective_sample_size`` and its interval NULL, and the
    promotion gate refuses on ``effective_sample_size_not_computed`` — the
    fail-closed state phase 5c shipped. It defaults to off rather than to an
    arbitrary seed because the seed is a DECLARED input under criterion 11: a
    default would let two runs of "the same" evaluation differ in a number
    nobody chose, or agree by a coincidence nobody recorded.
    """
    equity = curve.equity
    if len(equity) != len(dates):
        raise ValueError(f"curve has {len(equity)} points against {len(dates)} dates")

    ppy = periods_per_year(dates)
    years = (len(dates) - 1) / ppy

    final_equity = float(equity[-1]) if len(equity) else starting_equity
    total_return = (final_equity / starting_equity - 1.0) * 100.0

    # CAGR. ⚠⚠ THE GUARD IS ON THE NEGATIVE CASE ONLY, and the distinction is
    # not pedantry — it is what a revert probe found. A WIPED-OUT sleeve needs no
    # branch: `0.0 ** x == 0.0` in Python, so the general formula already returns
    # exactly -100%, and a `<= 0` special case is dead code that a test named
    # after it cannot exercise. A NEGATIVE final equity is the state that must
    # not fall through — Python returns a COMPLEX number for a negative base
    # raised to a fractional power ((-0.5) ** 0.0155 is 0.988+0.048j), which
    # would travel into `StrategyMetrics` as a complex and compare against
    # thresholds in ways nothing downstream expects. It is unreachable while
    # `build_equity_curve`'s cash-capped rebalance holds (asserted on the full
    # population as property P3), so this raises rather than returning a number.
    if final_equity < 0.0:
        raise ValueError(
            f"final equity {final_equity} is negative — a negative base under a fractional exponent returns a "
            "COMPLEX number, and the sleeve cannot borrow (equity_curve caps every buy at cash on hand)"
        )
    cagr = ((final_equity / starting_equity) ** (1.0 / years) - 1.0) * 100.0

    returns = _daily_returns(equity)
    if len(returns) > 1:
        volatility = float(np.std(returns, ddof=1))
    else:
        volatility = 0.0
    mean_return = float(np.mean(returns)) if len(returns) else 0.0
    annualised_vol = volatility * math.sqrt(ppy) * 100.0
    # ⚠ RISK-FREE RATE IS ZERO BY §5.4's OWN RULE — "define cash return as zero"
    # — so the excess return IS the return. Stated because a Sharpe with an
    # unstated benchmark rate is not comparable to anything.
    sharpe = (mean_return / volatility * math.sqrt(ppy)) if volatility > 0.0 else 0.0

    downside = returns[returns < 0.0] if len(returns) else np.empty(0, dtype=np.float64)
    losing_periods = int(downside.size)
    if losing_periods:
        # ⚠ The downside deviation divides by ALL periods, not by the losing
        # ones. Dividing by the losing count is a different statistic that
        # rewards a strategy for rarely losing twice over, and the two are
        # routinely confused.
        downside_deviation = float(math.sqrt(float(np.sum(downside**2)) / len(returns)))
        sortino = (mean_return / downside_deviation * math.sqrt(ppy)) if downside_deviation > 0.0 else None
    else:
        sortino = None

    net_returns = trades.net_return_pct
    trade_count = len(net_returns)
    expectancy = (sum(net_returns) / trade_count) if trade_count else 0.0
    gains = sum(value for value in net_returns if value > 0.0)
    losses = -sum(value for value in net_returns if value < 0.0)
    losing_trades = sum(1 for value in net_returns if value < 0.0)
    profit_factor = (gains / losses) if losses > 0.0 else None

    # §5.4: exposure is invested capital-days over ALLOCATED capital-days, and
    # the allocated pot is the denominator whether or not it is at work.
    allocated_capital_days = float(np.sum(curve.equity))
    invested_capital_days = float(np.sum(curve.invested))
    exposure = (invested_capital_days / allocated_capital_days * 100.0) if allocated_capital_days > 0.0 else 0.0
    exposure = min(exposure, 100.0)

    # Turnover, by construction: total notional changing hands, halved into
    # round trips, over the mean pot, per year. ⚠ The halving is what makes
    # "1.0" mean "the pot turned over once", which is the reading every desk
    # uses; the un-halved form doubles it and looks like twice the trading.
    mean_equity = float(np.mean(curve.equity)) if len(curve.equity) else 0.0
    traded = float(np.sum(curve.traded_notional))
    turnover = (traded / 2.0 / mean_equity / years) if mean_equity > 0.0 and years > 0.0 else 0.0

    if buy_and_hold is not None:
        if len(buy_and_hold.equity) != len(equity):
            raise ValueError(
                f"benchmark curve has {len(buy_and_hold.equity)} points against the strategy's {len(equity)} — the "
                "two must run on the same axis or the comparison is between different windows"
            )
        benchmark_return = (float(buy_and_hold.equity[-1]) / starting_equity - 1.0) * 100.0
    else:
        benchmark_return = 0.0

    # Criterion 3. ⚠ The bootstrap runs over the CLUSTER axis, not the trade
    # list — see ``block_bootstrap``'s header for why that is exact rather than
    # an approximation, and why it is what makes a 10^6-trade population
    # tractable at all.
    bootstrap = None
    if bootstrap_seed is not None:
        bootstrap = block_bootstrap_expectancy(
            cluster_by_date(net_returns, trades.entry_fill_date),
            seed=bootstrap_seed,
        )

    holds = _hold_percentiles(trades.hold_days)
    return StrategyMetrics(
        expectancy_per_trade_pct=expectancy,
        profit_factor=profit_factor,
        cagr_pct=cagr,
        annualised_volatility_pct=annualised_vol,
        sharpe=sharpe,
        sortino=sortino,
        max_drawdown_pct=max_drawdown_pct(equity),
        exposure_time_pct=exposure,
        turnover_annualised=turnover,
        trade_count=trade_count,
        effective_sample_size=bootstrap.effective_sample_size if bootstrap else None,
        return_vs_buy_and_hold_pct=total_return - benchmark_return,
        losing_trade_count=losing_trades,
        losing_period_count=losing_periods,
        open_trade_count=trades.open_count,
        unpriced_trade_count=trades.unpriced_count,
        periods_per_year=ppy,
        total_return_pct=total_return,
        buy_and_hold_return_pct=benchmark_return,
        expectancy_ci_low_pct=bootstrap.ci_low_pct if bootstrap else None,
        expectancy_ci_high_pct=bootstrap.ci_high_pct if bootstrap else None,
        bootstrap_block_length=bootstrap.block_length if bootstrap else None,
        bootstrap_cluster_count=bootstrap.cluster_count if bootstrap else None,
        bootstrap_resamples=bootstrap.resamples if bootstrap else None,
        bootstrap_seed=bootstrap.seed if bootstrap else None,
        bootstrap_design_effect=bootstrap.design_effect if bootstrap else None,
        bootstrap_model_id=bootstrap.model_id if bootstrap else None,
        median_hold_days=holds[1],
        hold_days_p25=holds[0],
        hold_days_p75=holds[2],
    )


def _hold_percentiles(hold_days: Sequence[int]) -> tuple[float | None, float | None, float | None]:
    """(p25, median, p75) calendar days held, or three Nones with no realised trades.

    ⚠ ``method="linear"`` is numpy's default and is stated anyway, because the
    point of the choice is that it MATCHES Postgres ``percentile_cont`` — which is
    what the live path already uses for the same quantity in the same unit
    (``strategy_monitoring._ATTRIBUTION_SQL``'s ``median_days_to_outcome``). Two
    adjacent figures on one catalog row disagreeing on identical data because one
    interpolates and the other picks a nearest rank is the failure being avoided.
    ``tests/test_strategy_holding_period_db.py`` pins the two engines together.

    ⚠ CALENDAR days, not bars. ``strategy_outcomes.bars_held`` is the competing
    documented unit and is deliberately not followed: a bar count is not a
    turnaround an operator can plan against, since five bars is a week or a
    fortnight depending on halts and holidays. Same reasoning as the live path.
    """
    if not hold_days:
        return (None, None, None)
    p25, median, p75 = np.percentile(np.asarray(hold_days, dtype=np.float64), [25, 50, 75], method="linear")
    return (float(p25), float(median), float(p75))


__all__ = [
    "DAYS_PER_YEAR",
    "METRIC_SET_ID",
    "StrategyMetrics",
    "TradeReturns",
    "compute_metrics",
    "max_drawdown_pct",
    "periods_per_year",
]
