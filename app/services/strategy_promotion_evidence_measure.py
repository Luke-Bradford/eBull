"""Ledger measurements for #2505's promotion evidence — slices 1 and 4 of #3104.

#2505 shipped the refusal contract and the immutable store; nothing produces a
record. This module produces the part of it that is pure arithmetic over the
realised-trade ledger, and nothing else: no verdicts, no costs, no challengers.

⚠ ITS ONE CALLER IS ``backtest_run._ledger_evidence``, inside
``_measure_namespace``, and it cannot run later than that: ``_measure_namespace``
returns an aggregate and the book is discarded before any row is written, so
"compute it at write time" is not implementable. The result rides
``NamespaceMeasurement`` the same way ``regime_cohorts`` and
``termination_census`` already outlive the book. ⚠ The slice-1 docstring said
"it has no caller yet"; slice 2 gave it one and slice 4 corrected the sentence.

⚠ NOTHING DOWNSTREAM READS IT YET. It reaches no result row and no gate; the
assembler that turns it into a ``PromotionEvidence`` is a later slice.

⚠ NOTHING HERE IS CLAMPED OR DEFAULTED. ``PromotionEvidence`` refuses several of
these values (a positive tail mean, for one); the refusal is the contract's job
and is not this module's to pre-empt. "Missing is not zero" is the contract's own
rule and it applies to measurement too.

Design: ``docs/proposals/ta/2026-09-16-promotion-evidence-producer.md``.
"""

from __future__ import annotations

import hashlib
import math
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final, TypedDict

import numpy as np

from app.services.block_bootstrap import BootstrapResult, block_bootstrap_expectancy, cluster_by_date

#: Frozen because the estimators below are choices, and a stored record must be
#: attributable to the rule that produced it. ⚠ Bump on a RULE change, never on
#: a comment. The version is returned on the measurement rather than written to
#: a column today; carrying it into the stored payload is an open item on the
#: design doc, because ``PromotionEvidence`` has nowhere to put it.
#:
#: ⚠ ``v2`` is slice 4: the recent-year partition changed this module's output
#: shape, and a record produced under v1 summarises a different set of fields.
LEDGER_MEASUREMENT_RULE_VERSION: Final = "promotion-evidence-ledger-measure-2026-09-16-v2"

#: Acerbi & Tasche's alpha for the α-tail average. #2505's field is the 5% one.
EXPECTED_SHORTFALL_ALPHA: Final = 0.05

#: The trim for "expectancy excluding the best 1%". Expressed as a PERCENTILE
#: rather than a count because that is the only in-repo formulation of this
#: statistic (``scripts/verify_2437_short_stops.py:163``).
BEST_TRIM_PERCENTILE: Final = 99.0

#: How many trailing calendar years the recent-year partition covers.
#:
#: ⚠⚠ READ OFF THE EXECUTABLE CONTRACT, not chosen. ``PromotionEvidence``
#: refuses more than five (*"recent year evidence is capped at five aggregate
#: years"*, ``strategy_promotion_evidence.py:289-290``) and ``evidence_refusals``
#: refuses fewer than two. Taking the CAP rather than the floor is also the
#: conservative direction: the gate requires EVERY listed year to have positive
#: after-cost expectancy, so each additional year can only make it harder to
#: pass.
#:
#: ⚠ An earlier draft took "three" from ``RECENT_EVIDENCE_WINDOWS``' three
#: calendar-year entries. That was an invented extraction, and its own
#: corroboration was wrong too — ``rolling-36m`` spans 2021-09-28 to 2024-09-27,
#: which is FOUR calendar years, not three.
RECENT_YEAR_HORIZON: Final = 5


@dataclass(frozen=True)
class RealisedLedger:
    """One namespace's realised legs, plus the open legs still in exposure.

    ⚠⚠ TWO POPULATIONS, DELIBERATELY DIFFERENT SIZES. This is the split
    ``TradeReturns`` already documents (``strategy_statistics.py:84``): the
    trade-level statistics take the realised legs, and anything describing
    EXPOSURE must also see the legs open at the window end. Concurrency is the
    second kind — counting realised legs alone understates it, which is the
    direction that flatters a candidate.

    ``name_key`` is the book's key and NOT always an instrument id: the
    survivorship-free path uses ``-series_id`` for a series admitted without a
    live link (#2721 step 3, ``backtest_run.py:1540``). Concentration only needs
    identity, so that is harmless here — but a later slice joining this key to a
    sector must say what it does with a negative one.
    """

    #: Net return per realised leg, in percent, positionally parallel to the
    #: three tuples below.
    net_return_pct: tuple[float, ...]
    entry_fill_date: tuple[date, ...]
    exit_bar_date: tuple[date, ...]
    name_key: tuple[int, ...]
    #: ``(entry_fill_date, window_end)`` for each leg still open at the end.
    #: An open leg never closes inside the window, so it contributes an opening
    #: event and no closing one.
    open_legs: tuple[tuple[date, date], ...] = ()

    def __post_init__(self) -> None:
        count = len(self.net_return_pct)
        if count == 0:
            # ⚠ REFUSED, NOT ZEROED. Every statistic below is undefined on an
            # empty population, and an invented zero is exactly what the
            # contract's "missing is not zero and does not reduce a score" rule
            # exists to prevent.
            raise ValueError("a realised ledger with no legs carries no measurement")
        for name in ("entry_fill_date", "exit_bar_date", "name_key"):
            if len(getattr(self, name)) != count:
                raise ValueError(
                    f"{name} carries {len(getattr(self, name))} entries against {count} returns — "
                    "the realised columns are positionally parallel"
                )
        if any(not math.isfinite(value) for value in self.net_return_pct):
            raise ValueError("every realised return must be finite")
        for entry, exit_bar in zip(self.entry_fill_date, self.exit_bar_date, strict=True):
            if exit_bar < entry:
                raise ValueError(f"a leg entered {entry} cannot close {exit_bar}")
        for entry, window_end in self.open_legs:
            if window_end < entry:
                raise ValueError(f"an open leg entered {entry} cannot be marked at {window_end}")


@dataclass(frozen=True)
class RecentYearMeasurement:
    """One calendar year of the trailing horizon, measured — NOT a verdict.

    Deliberately not a ``RecentYearEvidence``: that class refuses several states
    this one must be able to report (a year whose bootstrap could not run, an
    interval whose lower bound exceeds its own point estimate, a year before
    2000). Constructing it here would turn a contract refusal into a crashed
    backtest run. The assembler maps this to the contract's class, or to a
    refusal — see the design doc's open items.

    ⚠⚠ A YEAR IS KEPT IFF IT REALISED AT LEAST ONE LEG. The contract's own
    ``observation_count >= 1`` fixes that, and every statistic below is
    undefined without it. Two consequences, both stated rather than discovered:
    a horizon year whose legs are ALL still open at the window end is absent
    (no realised outcome is the honest reading of it), and an absent year is
    not the same fact as a kept year whose bootstrap returned nothing — which
    is why the latter is kept with nulls rather than dropped.
    """

    year: int
    #: Realised legs entered in this year. The contract's "population".
    observation_count: int
    #: Legs entered in this year and STILL OPEN at the window end, so absent
    #: from every statistic beside them. ⚠ Reported because censoring
    #: concentrates in the most recent entry years — exactly the ones this
    #: field exists to describe — and an unreported censoring rate makes the
    #: most-censored year look like the best-measured one.
    open_leg_count: int
    #: Pooled ``sum / count`` over the year's realised legs. ⚠ NOT part of the
    #: bootstrap group below: it is defined whenever the year is kept.
    after_cost_expectancy_pct: Decimal
    expected_shortfall_5_pct: Decimal
    max_date_contribution_pct: Decimal
    max_name_contribution_pct: Decimal
    #: ⚠ ALL PRESENT OR ALL ABSENT, mirroring ``RegimeCohort``'s own rule.
    #: ``block_bootstrap_expectancy`` returns ``None`` in three real states (a
    #: single cluster date, zero trade variance, zero bootstrap variance) and
    #: criterion 3 forbids substituting a nominal figure for a measurement that
    #: could not be made. Measured on the 720 stored regime cohorts — the same
    #: estimator over the same books on a different partition axis — that is
    #: 104 of 720, so it is the common case and not a corner.
    expectancy_ci_low_pct: Decimal | None
    expectancy_ci_high_pct: Decimal | None
    effective_sample_size: float | None
    bootstrap_seed: int | None
    bootstrap_block_length: int | None
    #: Active entry dates in the year. Doubles as the coverage metric: the
    #: contract permits a partial year but sets no minimum span, so two thin
    #: periods either side of New Year satisfy "two years evaluated". Whether to
    #: refuse on that is a gate question and is registered, not decided here.
    bootstrap_cluster_count: int | None
    bootstrap_design_effect: float | None

    def __post_init__(self) -> None:
        if self.observation_count < 1:
            raise ValueError(f"{self.year} is not a kept year: it realised no legs")
        if self.open_leg_count < 0:
            raise ValueError(f"{self.year} cannot carry {self.open_leg_count} open legs")
        for name in (
            "after_cost_expectancy_pct",
            "expected_shortfall_5_pct",
            "max_date_contribution_pct",
            "max_name_contribution_pct",
        ):
            if not getattr(self, name).is_finite():
                raise ValueError(f"{self.year} {name} must be finite")
        for name in ("max_date_contribution_pct", "max_name_contribution_pct"):
            if not Decimal(0) < getattr(self, name) <= Decimal(100):
                raise ValueError(f"{self.year} {name} must lie in (0, 100]")
        bootstrap = (
            self.expectancy_ci_low_pct,
            self.expectancy_ci_high_pct,
            self.effective_sample_size,
            self.bootstrap_seed,
            self.bootstrap_block_length,
            self.bootstrap_cluster_count,
            self.bootstrap_design_effect,
        )
        present = sum(value is not None for value in bootstrap)
        if present not in (0, len(bootstrap)):
            raise ValueError(
                f"{self.year} carries {present} of {len(bootstrap)} bootstrap fields — a partial provenance "
                "cannot be re-run and criterion 3 forbids reporting the interval without it"
            )
        if present == 0:
            return
        assert self.expectancy_ci_low_pct is not None
        assert self.expectancy_ci_high_pct is not None
        assert self.effective_sample_size is not None
        assert self.bootstrap_design_effect is not None
        if not self.expectancy_ci_low_pct.is_finite() or not self.expectancy_ci_high_pct.is_finite():
            raise ValueError(f"{self.year} bootstrap interval bounds must be finite")
        if self.expectancy_ci_low_pct > self.expectancy_ci_high_pct:
            raise ValueError(f"{self.year} bootstrap interval is inverted")
        # ⚠ NOT checked: ``ci_low > after_cost_expectancy_pct``. A percentile
        # interval need not contain its own statistic (Efron & Tibshirani ch.
        # 13) and ``RecentYearEvidence`` is the class that refuses that pair.
        # Duplicating the contract's check here would abort a backtest run over
        # a measurement that is correct. Unobserved on the 720 stored regime
        # cohorts (0 with ``ci_low > expectancy_pct``) — measured, not assumed.
        if self.effective_sample_size <= 0 or self.bootstrap_design_effect <= 0:
            raise ValueError(f"{self.year} effective sample size and design effect must be positive")
        assert self.bootstrap_seed is not None
        assert self.bootstrap_block_length is not None
        assert self.bootstrap_cluster_count is not None
        # ⚠ The seed floor is ZERO, not one. It is the low four bytes of a
        # SHA-256 digest, so 0 is a legal draw; ``RegimeCohort`` uses the same
        # bound for the same reason.
        if self.bootstrap_seed < 0:
            raise ValueError(f"{self.year} bootstrap seed must be non-negative")
        if min(self.bootstrap_block_length, self.bootstrap_cluster_count) < 1:
            raise ValueError(f"{self.year} bootstrap block length and cluster count must be positive")


@dataclass(frozen=True)
class LedgerMeasurements:
    """The #2505 fields that are arithmetic over the ledger, and only those."""

    rule_version: str
    outcome_count: int
    profitable_outcome_count: int
    losing_outcome_count: int
    flat_outcome_count: int
    #: The α-tail MEAN in return units — the negative of Acerbi & Tasche's ES,
    #: because #2505 reports tail losses as non-positive percentages.
    expected_shortfall_5_pct: Decimal
    excluding_best_1_expectancy_pct: Decimal
    #: How many legs the percentile trim actually dropped. ⚠ A threshold trim is
    #: not a fixed-count trim: under ties it removes more or fewer than 1%, so
    #: the number is reported rather than assumed to be ``ceil(0.01 n)``.
    excluded_best_count: int
    max_date_contribution_pct: Decimal
    max_name_contribution_pct: Decimal
    max_concurrency: int
    #: The trailing ``RECENT_YEAR_HORIZON`` calendar years that realised at
    #: least one leg, ascending. ⚠ May be EMPTY (an in-sample namespace whose
    #: legs all predate the horizon) or SHORTER than the horizon; the contract's
    #: floor of two is a gate, not a measurement, and is not enforced here.
    recent_years: tuple[RecentYearMeasurement, ...]

    def __post_init__(self) -> None:
        years = [item.year for item in self.recent_years]
        if years != sorted(set(years)):
            raise ValueError("recent years must be unique and ascending")
        if len(years) > RECENT_YEAR_HORIZON:
            raise ValueError(f"{len(years)} recent years exceed the {RECENT_YEAR_HORIZON}-year horizon")
        # ⚠ The contract bounds the per-year populations by the parent count
        # (``evidence_refusals``). Asserted at the point the two are built
        # together, where a mismatch is a partition bug rather than a gate
        # verdict.
        counted = sum(item.observation_count for item in self.recent_years)
        if counted > self.outcome_count:
            raise ValueError(
                f"the recent years hold {counted} legs against the ledger's {self.outcome_count} — "
                "a year partition cannot exceed the population it partitions"
            )


def _alpha_tail_mean(sorted_ascending: np.ndarray, *, alpha: float) -> float:
    """Acerbi & Tasche (2002) §4's α-tail average, exactly.

    *On the coherence of expected shortfall*, JBF 26(7). With ``k = floor(αn)``
    the estimator is ``(sum of the k smallest + (αn - k) · x_(k+1)) / (αn)`` —
    the boundary observation carries a FRACTIONAL weight.

    ⚠ ``ceil(αn)`` is a different estimator and overstates the tail: at ``n=41``
    it averages three observations, which is 7.32% of the population and not 5%.

    ⚠ For ``αn < 1`` the formula reduces to the single worst observation, which
    is the honest answer at that sample size rather than a degenerate one.
    """
    n = int(sorted_ascending.size)
    alpha_n = alpha * n
    k = int(math.floor(alpha_n))
    head = float(sorted_ascending[:k].sum()) if k else 0.0
    # k < n for every alpha < 1, so the boundary observation always exists.
    boundary = float(sorted_ascending[k])
    return (head + (alpha_n - k) * boundary) / alpha_n


def _year_seed(root_seed: int, year: int) -> int:
    """The repo's existing per-partition seed derivation, reused verbatim.

    ``strategy_regime_evidence._seed`` and ``strategy_cohort_report._seed_for``
    are both ``int.from_bytes(sha256(f"{root_seed}:{label}").digest()[:4],
    "big")``. Using it with the year as the label mints no new rule — and a
    per-year seed is what keeps two years' resample streams from being the same
    draws against different populations.
    """
    return int.from_bytes(hashlib.sha256(f"{root_seed}:{year}".encode()).digest()[:4], "big")


class _BootstrapColumns(TypedDict):
    """The seven all-or-none fields, typed so the spread below is CHECKED.

    ⚠ A ``dict[str, object]`` return forced a blanket ``# type: ignore`` on the
    ``**`` spread, which suppresses type-checking on EVERY keyword it carries —
    including a future field whose type stops matching. Raised as a review
    nitpick on PR #3107 and fixed rather than deferred: the ignore was the cheap
    part, and what it hid was the expensive part.
    """

    expectancy_ci_low_pct: Decimal | None
    expectancy_ci_high_pct: Decimal | None
    effective_sample_size: float | None
    bootstrap_seed: int | None
    bootstrap_block_length: int | None
    bootstrap_cluster_count: int | None
    bootstrap_design_effect: float | None


def _bootstrap_columns(result: BootstrapResult | None) -> _BootstrapColumns:
    if result is None:
        return {
            "expectancy_ci_low_pct": None,
            "expectancy_ci_high_pct": None,
            "effective_sample_size": None,
            "bootstrap_seed": None,
            "bootstrap_block_length": None,
            "bootstrap_cluster_count": None,
            "bootstrap_design_effect": None,
        }
    return {
        "expectancy_ci_low_pct": _decimal(result.ci_low_pct),
        "expectancy_ci_high_pct": _decimal(result.ci_high_pct),
        "effective_sample_size": result.effective_sample_size,
        "bootstrap_seed": result.seed,
        "bootstrap_block_length": result.block_length,
        "bootstrap_cluster_count": result.cluster_count,
        "bootstrap_design_effect": result.design_effect,
    }


def _recent_years(
    ledger: RealisedLedger,
    returns: np.ndarray,
    *,
    root_seed: int,
    anchor_year: int,
) -> tuple[RecentYearMeasurement, ...]:
    """Partition the realised legs by ENTRY year over the trailing horizon.

    ⚠⚠ ENTRY YEAR, fixed by construction and frozen in the rule version. The
    parent row's own ``after_cost_expectancy_ci_low_pct`` is computed from
    ``cluster_by_date(net_returns, trades.entry_fill_date)``
    (``strategy_statistics.py``), and slice 1's date concentration uses the same
    key — a partition on the exit date would put the per-year interval on a
    different axis from the parent figure the contract prints beside it.

    ⚠ The property that buys, stated rather than discovered: an entry-year
    cohort is NOT calendar-year performance. A leg entered in 2024 and closed in
    2026 contributes its whole outcome to 2024, so a year's expectancy can be
    realised by later-year prices and cohorts overlap economically.

    ⚠ ``RegimeCohort`` clusters one step earlier, on ``signal_date``, because a
    regime describes the information the strategy consumed. This field is
    compared against a fill-date statistic, so it uses the fill date.
    """
    horizon = range(anchor_year - RECENT_YEAR_HORIZON + 1, anchor_year + 1)
    # ⚠ ONE pass to extract the years, then numpy masks per year — not a Python
    # bucket loop per year. The realised population reaches 4,228,628 legs.
    entry_years = np.fromiter(
        (when.year for when in ledger.entry_fill_date), dtype=np.int32, count=len(ledger.entry_fill_date)
    )
    open_years = Counter(entry.year for entry, _mark in ledger.open_legs)

    measured: list[RecentYearMeasurement] = []
    for year in horizon:
        positions = np.flatnonzero(entry_years == year)
        if positions.size == 0:
            # Absent, not zeroed: ``RecentYearEvidence`` refuses a year with no
            # realised leg, and every statistic below is undefined on one.
            continue
        year_returns = returns[positions]
        dates = [ledger.entry_fill_date[index] for index in positions]
        bootstrap = block_bootstrap_expectancy(
            cluster_by_date(year_returns.tolist(), dates),
            seed=_year_seed(root_seed, year),
        )
        count = int(positions.size)
        measured.append(
            RecentYearMeasurement(
                year=year,
                observation_count=count,
                open_leg_count=open_years.get(year, 0),
                # ⚠ The bootstrap's own point estimate when it ran — the same
                # ``sum/count`` ratio, taken from the object that computed the
                # interval so the pair cannot disagree by a rounding step.
                after_cost_expectancy_pct=_decimal(
                    bootstrap.point_estimate_pct if bootstrap else float(year_returns.mean())
                ),
                expected_shortfall_5_pct=_decimal(
                    _alpha_tail_mean(np.sort(year_returns), alpha=EXPECTED_SHORTFALL_ALPHA)
                ),
                max_date_contribution_pct=_share_pct(max(Counter(dates).values()), count),
                max_name_contribution_pct=_share_pct(
                    max(Counter(ledger.name_key[index] for index in positions).values()), count
                ),
                **_bootstrap_columns(bootstrap),
            )
        )
    return tuple(measured)


def measure_ledger(ledger: RealisedLedger, *, root_seed: int, anchor_year: int) -> LedgerMeasurements:
    """Measure one realised ledger. Pure; reads no database.

    ``anchor_year`` is the year of the RESULT'S OWN ``window_end`` — the
    ``as_of`` that write-time ``check_promotable`` passes — and NOT the
    namespace metric-axis end. The two differ by years on an in-sample
    namespace, whose axis stops at ``HOLDOUT_BOUNDARY``.

    ``root_seed`` is the run's ``BACKTEST_BOOTSTRAP_SEED``, the same root
    ``build_regime_cohorts`` already takes.
    """
    returns = np.asarray(ledger.net_return_pct, dtype=float)
    count = int(returns.size)
    ordered = np.sort(returns)

    # The percentile trim, and the count it actually removed.
    threshold = float(np.percentile(returns, BEST_TRIM_PERCENTILE))
    kept = returns[returns <= threshold]
    if kept.size == 0:  # pragma: no cover - a percentile is always >= the minimum
        raise RuntimeError("the best-1% trim removed the whole population")

    date_counts = Counter(ledger.entry_fill_date)
    name_counts = Counter(ledger.name_key)

    return LedgerMeasurements(
        rule_version=LEDGER_MEASUREMENT_RULE_VERSION,
        outcome_count=count,
        profitable_outcome_count=int(np.count_nonzero(returns > 0.0)),
        losing_outcome_count=int(np.count_nonzero(returns < 0.0)),
        flat_outcome_count=int(np.count_nonzero(returns == 0.0)),
        expected_shortfall_5_pct=_decimal(_alpha_tail_mean(ordered, alpha=EXPECTED_SHORTFALL_ALPHA)),
        excluding_best_1_expectancy_pct=_decimal(float(kept.mean())),
        excluded_best_count=count - int(kept.size),
        max_date_contribution_pct=_share_pct(max(date_counts.values()), count),
        max_name_contribution_pct=_share_pct(max(name_counts.values()), count),
        max_concurrency=_max_concurrency(ledger),
        recent_years=_recent_years(ledger, returns, root_seed=root_seed, anchor_year=anchor_year),
    )


#: Within one date, the order in which boundary events are applied. Two
#: DIFFERENT rules meet here and neither may swallow the other:
#:
#: ⚠ ``_CLOSE_EARLIER`` before ``_OPEN`` is spec §3.5 rule 4 — *"same-bar
#: ordering is exit before entry"* (``position_builder.py:661``) — so a leg
#: closing on a date and a DIFFERENT leg opening on it are not concurrent.
#:
#: ⚠⚠ ``_CLOSE_SAME_DAY`` comes after ``_OPEN``, because a leg that opens and
#: closes on one bar WAS held. ``bars_held = 0`` is legal — a tp/sl can be
#: touched on the fill bar itself (``position_builder.Position.__post_init__``)
#: — and a single ordering would either erase those legs (reporting 0 for an
#: all-intraday population, which ``PromotionEvidence`` then rejects outright)
#: or make yesterday's exit concurrent with today's entry. Caught at Codex
#: checkpoint 2; the first version had only the first rule.
_CLOSE_EARLIER: Final = 0
_OPEN: Final = 1
_CLOSE_SAME_DAY: Final = 2


def _max_concurrency(ledger: RealisedLedger) -> int:
    """The most positions held at once, over realised AND open legs.

    A sweep over ``2n`` boundary events, because the stored population reaches
    4,228,628 realised legs and a date x position grid is not an option.
    """
    events: list[tuple[date, int, int]] = []
    for entry, exit_bar in zip(ledger.entry_fill_date, ledger.exit_bar_date, strict=True):
        events.append((entry, _OPEN, 1))
        events.append((exit_bar, _CLOSE_SAME_DAY if exit_bar == entry else _CLOSE_EARLIER, -1))
    for entry, _window_end in ledger.open_legs:
        # ⚠ NO CLOSING EVENT. A leg open at the window end never closes inside
        # it, so it is held from its entry to the end of the sweep.
        events.append((entry, _OPEN, 1))
    events.sort()
    held = 0
    peak = 0
    for _event_date, _phase, delta in events:
        held += delta
        peak = max(peak, held)
    return peak


def _share_pct(largest: int, total: int) -> Decimal:
    """One bucket's share of the population, in percent.

    ⚠ COUNT SHARE, NOT PROFIT SHARE. #2505's worked example is *"36.1% of
    accepted 2025 trades enter on one date"*.
    """
    return _decimal(largest / total * 100.0)


def _decimal(value: float) -> Decimal:
    """``repr`` and not ``str(float)`` rounding — the repo's conversion idiom."""
    return Decimal(repr(value))


__all__ = [
    "BEST_TRIM_PERCENTILE",
    "EXPECTED_SHORTFALL_ALPHA",
    "LEDGER_MEASUREMENT_RULE_VERSION",
    "RECENT_YEAR_HORIZON",
    "LedgerMeasurements",
    "RealisedLedger",
    "RecentYearMeasurement",
    "measure_ledger",
]
