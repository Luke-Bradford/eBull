"""Frozen, pure statistical definitions for the sealed #2582 event study.

This module has no database import and cannot open the Schedule 13D outcomes.
It exists separately so clustering, tails, stability and concentration are
reviewed before the trial register permits the price query.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from math import sqrt
from statistics import mean, median, variance
from typing import Final

import numpy as np

BOOTSTRAP_SEED: Final = 2582
BOOTSTRAP_RESAMPLES: Final = 10_000


@dataclass(frozen=True)
class EventOutcome:
    accession_number: str
    issuer_cik: str
    entry_date: date
    exit_date: date
    net_return_pct: float
    maximum_percent_of_class: float | None
    sector: str | None = None


@dataclass(frozen=True)
class ClusteredEstimate:
    mean_pct: float
    lower_95_pct: float
    upper_95_pct: float
    bootstrap_standard_error_pct: float
    effective_sample_size: float
    resamples: int


@dataclass(frozen=True)
class PairedDifference:
    """One treatment-minus-challenger return difference."""

    treatment_accession: str
    treatment_issuer_cik: str
    treatment_entry_date: date
    difference_pct: float


@dataclass(frozen=True)
class DifferenceTest:
    mean_difference_pct: float
    lower_95_pct: float
    upper_95_pct: float
    one_sided_p_value: float
    bootstrap_standard_error_pct: float
    resamples: int


@dataclass(frozen=True)
class StabilityWindow:
    label: str
    start: date
    end: date
    event_count: int
    mean_net_return_pct: float | None


@dataclass(frozen=True)
class OutcomeStatistics:
    event_count: int
    clustered: ClusteredEstimate
    median_net_return_pct: float
    hit_rate_pct: float
    average_winner_pct: float | None
    average_loser_pct: float | None
    profit_factor: float | None
    expected_shortfall_5_pct: float
    worst_net_event_return_pct: float
    maximum_concurrency: int
    maximum_issuer_positive_concentration_pct: float
    maximum_sector_positive_concentration_pct: float
    maximum_entry_session_positive_concentration_pct: float
    excluding_best_1pct_mean_pct: float
    signal_strength_spearman: float | None
    signal_strength_observations: int
    stability: tuple[StabilityWindow, ...]
    break_even_cost_bps: float


def _pigeonhole_estimates(
    values: np.ndarray,
    issuer_values: Sequence[str],
    session_values: Sequence[date],
    *,
    seed: int,
    resamples: int,
) -> np.ndarray:
    if len(values) < 2:
        raise ValueError("clustered inference requires at least two observations")
    if len(issuer_values) != len(values) or len(session_values) != len(values):
        raise ValueError("every observation requires one issuer and one entry session")
    if resamples < 2:
        raise ValueError("resamples must be at least two")
    if not np.all(np.isfinite(values)):
        raise ValueError("observations must all be finite")
    issuers = sorted(set(issuer_values))
    sessions = sorted(set(session_values))
    issuer_index = {value: index for index, value in enumerate(issuers)}
    session_index = {value: index for index, value in enumerate(sessions)}
    event_issuers = np.asarray([issuer_index[value] for value in issuer_values])
    event_sessions = np.asarray([session_index[value] for value in session_values])
    rng = np.random.default_rng(seed)
    estimates = np.empty(resamples, dtype=np.float64)
    valid_draws = 0
    attempts = 0
    while valid_draws < resamples:
        attempts += 1
        if attempts > resamples * 20:
            raise ValueError("pigeonhole bootstrap could not produce the required nonempty intersection draws")
        issuer_counts = rng.multinomial(len(issuers), np.full(len(issuers), 1 / len(issuers)))
        session_counts = rng.multinomial(len(sessions), np.full(len(sessions), 1 / len(sessions)))
        weights = issuer_counts[event_issuers] * session_counts[event_sessions]
        total_weight = int(weights.sum())
        if total_weight == 0:
            continue
        estimates[valid_draws] = float(np.dot(values, weights) / total_weight)
        valid_draws += 1
    return estimates


def two_way_pigeonhole_bootstrap(
    outcomes: Sequence[EventOutcome],
    *,
    seed: int = BOOTSTRAP_SEED,
    resamples: int = BOOTSTRAP_RESAMPLES,
) -> ClusteredEstimate:
    """Bootstrap issuers and entry sessions independently with replacement.

    Each observation receives the product of its issuer and entry-session
    multinomial counts. Percentile bounds use NumPy's linear quantile method.
    Effective sample size is ``sample variance / bootstrap variance of mean``,
    capped at the raw event count; it is zero when clustering supplies no
    estimable precision and never defaults to the row count.
    """

    values = np.asarray([item.net_return_pct for item in outcomes], dtype=np.float64)
    estimates = _pigeonhole_estimates(
        values,
        [item.issuer_cik for item in outcomes],
        [item.entry_date for item in outcomes],
        seed=seed,
        resamples=resamples,
    )
    lower, upper = np.quantile(estimates, (0.025, 0.975), method="linear")
    bootstrap_variance = float(np.var(estimates, ddof=1))
    sample_variance = variance(float(value) for value in values)
    effective_n = 0.0 if bootstrap_variance <= 0 else min(float(len(values)), sample_variance / bootstrap_variance)
    return ClusteredEstimate(
        mean_pct=float(np.mean(values)),
        lower_95_pct=float(lower),
        upper_95_pct=float(upper),
        bootstrap_standard_error_pct=sqrt(bootstrap_variance),
        effective_sample_size=effective_n,
        resamples=resamples,
    )


def paired_clustered_difference_test(
    differences: Sequence[PairedDifference],
    *,
    seed: int = BOOTSTRAP_SEED,
    resamples: int = BOOTSTRAP_RESAMPLES,
) -> DifferenceTest:
    """Test a paired treatment-minus-control mean with treatment clustering.

    The one-sided p-value is computed from the null-centred bootstrap
    distribution with a plus-one finite-resample correction. The confidence
    interval is the preregistered percentile interval of the paired mean.
    """

    values = np.asarray([item.difference_pct for item in differences], dtype=np.float64)
    estimates = _pigeonhole_estimates(
        values,
        [item.treatment_issuer_cik for item in differences],
        [item.treatment_entry_date for item in differences],
        seed=seed,
        resamples=resamples,
    )
    observed = float(np.mean(values))
    lower, upper = np.quantile(estimates, (0.025, 0.975), method="linear")
    centred = estimates - observed
    p_value = (1 + int(np.count_nonzero(centred >= observed))) / (resamples + 1)
    return DifferenceTest(
        mean_difference_pct=observed,
        lower_95_pct=float(lower),
        upper_95_pct=float(upper),
        one_sided_p_value=p_value,
        bootstrap_standard_error_pct=float(np.std(estimates, ddof=1)),
        resamples=resamples,
    )


def holm_adjust(p_values: Sequence[float]) -> tuple[float, ...]:
    """Return Holm step-down adjusted p-values in original input order."""

    if not p_values:
        raise ValueError("at least one p-value is required")
    if any(not np.isfinite(value) or not 0 <= value <= 1 for value in p_values):
        raise ValueError("p-values must be finite and between zero and one")
    ordered = sorted(enumerate(p_values), key=lambda item: (item[1], item[0]))
    adjusted = [0.0] * len(ordered)
    running = 0.0
    count = len(ordered)
    for rank, (original_index, value) in enumerate(ordered):
        running = max(running, min(1.0, (count - rank) * value))
        adjusted[original_index] = running
    return tuple(adjusted)


def _average_rank(values: Sequence[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: (item[1], item[0]))
    ranks = [0.0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        average_rank = (start + 1 + end) / 2
        for position in range(start, end):
            ranks[ordered[position][0]] = average_rank
        start = end
    return ranks


def spearman_strength(outcomes: Sequence[EventOutcome]) -> tuple[float | None, int]:
    pairs = [
        (item.maximum_percent_of_class, item.net_return_pct)
        for item in outcomes
        if item.maximum_percent_of_class is not None
    ]
    if len(pairs) < 3:
        return None, len(pairs)
    left = np.asarray(_average_rank([float(item[0]) for item in pairs]), dtype=np.float64)
    right = np.asarray(_average_rank([item[1] for item in pairs]), dtype=np.float64)
    if np.std(left) == 0 or np.std(right) == 0:
        return None, len(pairs)
    return float(np.corrcoef(left, right)[0, 1]), len(pairs)


def _stability(outcomes: Sequence[EventOutcome]) -> tuple[StabilityWindow, ...]:
    windows = (
        ("six_month_2025_h1", date(2025, 1, 1), date(2025, 6, 30)),
        ("six_month_2025_h2", date(2025, 7, 1), date(2025, 12, 31)),
        ("six_month_2026_h1", date(2026, 1, 1), date(2026, 6, 30)),
        ("latest_twelve_month", date(2025, 7, 1), date(2026, 6, 30)),
    )
    result: list[StabilityWindow] = []
    for label, start, end in windows:
        values = [item.net_return_pct for item in outcomes if start <= item.entry_date <= end]
        result.append(StabilityWindow(label, start, end, len(values), mean(values) if values else None))
    return tuple(result)


def _maximum_concurrency(outcomes: Sequence[EventOutcome]) -> int:
    changes: Counter[date] = Counter()
    for item in outcomes:
        changes[item.entry_date] += 1
        changes[item.exit_date + timedelta(days=1)] -= 1
    current = maximum = 0
    for _, change in sorted(changes.items()):
        current += change
        maximum = max(maximum, current)
    return maximum


def _positive_concentration(outcomes: Sequence[EventOutcome], *, by: str) -> float:
    # Outcomes are net of 50 bps; concentration is preregistered on gross
    # positive return, so add the fixed cost back before clipping at zero.
    positive_total = sum(max(item.net_return_pct + 0.5, 0.0) for item in outcomes)
    if positive_total <= 0:
        return 100.0
    grouped: defaultdict[object, float] = defaultdict(float)
    for item in outcomes:
        if by == "issuer":
            key: object = item.issuer_cik
        elif by == "sector":
            key = item.sector or "unknown"
        else:
            key = item.entry_date
        grouped[key] += max(item.net_return_pct + 0.5, 0.0)
    return max(grouped.values(), default=0.0) / positive_total * 100


def summarise_outcomes(
    outcomes: Sequence[EventOutcome],
    *,
    seed: int = BOOTSTRAP_SEED,
    resamples: int = BOOTSTRAP_RESAMPLES,
) -> OutcomeStatistics:
    if len(outcomes) < 2:
        raise ValueError("summary requires at least two outcomes")
    values = [item.net_return_pct for item in outcomes]
    if any(not np.isfinite(value) for value in values):
        raise ValueError("outcome returns must all be finite")
    winners = [value for value in values if value > 0]
    losers = [value for value in values if value < 0]
    losses = -sum(losers)
    ordered = sorted(values)
    tail_count = max(1, (len(ordered) + 19) // 20)
    remove_count = max(1, (len(ordered) + 99) // 100)
    strength, strength_n = spearman_strength(outcomes)
    return OutcomeStatistics(
        event_count=len(values),
        clustered=two_way_pigeonhole_bootstrap(outcomes, seed=seed, resamples=resamples),
        median_net_return_pct=median(values),
        hit_rate_pct=len(winners) / len(values) * 100,
        average_winner_pct=mean(winners) if winners else None,
        average_loser_pct=mean(losers) if losers else None,
        profit_factor=sum(winners) / losses if losses > 0 else None,
        expected_shortfall_5_pct=mean(ordered[:tail_count]),
        worst_net_event_return_pct=min(values),
        maximum_concurrency=_maximum_concurrency(outcomes),
        maximum_issuer_positive_concentration_pct=_positive_concentration(outcomes, by="issuer"),
        maximum_sector_positive_concentration_pct=_positive_concentration(outcomes, by="sector"),
        maximum_entry_session_positive_concentration_pct=_positive_concentration(outcomes, by="entry"),
        excluding_best_1pct_mean_pct=mean(ordered[:-remove_count]),
        signal_strength_spearman=strength,
        signal_strength_observations=strength_n,
        stability=_stability(outcomes),
        break_even_cost_bps=mean(values) * 100 + 50,
    )
