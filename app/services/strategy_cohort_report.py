"""Shared, outcome-aware cohort verifier for strategy candidates (#2508).

The reporter answers where a frozen candidate worked and failed. It does not
select a winning cell: discovering a cell here creates a new preregistered
candidate for later data. Every interval must be built from identical causal
fills and already-costed returns.
"""

from __future__ import annotations

import hashlib
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final, Literal

import numpy as np

from app.services.block_bootstrap import BOOTSTRAP_MODEL_ID, block_bootstrap_expectancy, cluster_by_date
from app.services.strategy_decision_context import (
    DollarVolumeBand,
    ListingMarket,
    PriceBand,
    SecurityType,
)

# The prospective activation contract in the strategy lifecycle requires 30
# independent trades. Reusing it here avoids inventing a second definition of
# a minimally interpretable cell. Nominal trade count is checked separately;
# neither substitutes for the clustered effective sample size.
MIN_CELL_TRADES: Final = 30
MIN_EFFECTIVE_SAMPLE: Final = 30.0

Dimension = Literal[
    "mechanism",
    "security_type",
    "primary_listing_market",
    "price_band",
    "dollar_volume_band",
]
WindowKind = Literal["walk_forward", "untouched", "prospective"]
CohortVerdict = Literal["economically_positive", "refused"]

# Frozen before outcomes. No arbitrary subset supplied by a caller, because a
# caller-controlled list would turn the reporter into a cell-search engine.
DECLARED_COHORTS: Final[tuple[tuple[Dimension, ...], ...]] = (
    (),
    ("mechanism",),
    ("security_type",),
    ("primary_listing_market",),
    ("price_band",),
    ("dollar_volume_band",),
    ("mechanism", "price_band"),
    ("mechanism", "dollar_volume_band"),
    ("primary_listing_market", "price_band"),
    ("primary_listing_market", "dollar_volume_band"),
)


@dataclass(frozen=True, order=True)
class CohortKey:
    dimensions: tuple[Dimension, ...]
    values: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.dimensions) != len(self.values):
            raise ValueError("cohort dimensions and values must be parallel")
        if self.dimensions not in DECLARED_COHORTS:
            raise ValueError(f"undeclared cohort dimensions {self.dimensions!r}")
        if any(not value for value in self.values):
            raise ValueError("cohort values must be non-empty")

    @property
    def label(self) -> str:
        if not self.dimensions:
            return "all"
        return "|".join(f"{dimension}={value}" for dimension, value in zip(self.dimensions, self.values, strict=True))


UNPOOLED_KEY: Final = CohortKey((), ())


@dataclass(frozen=True)
class CohortObservation:
    instrument_id: int
    sector: str
    entry_date: date
    exit_date: date
    mechanism: str
    security_type: SecurityType
    primary_listing_market: ListingMarket
    price_band: PriceBand
    dollar_volume_band: DollarVolumeBand
    net_return_pct: float
    net_return_double_cost_pct: float
    holding_minutes: float
    turnover_fraction: float
    max_adverse_excursion_pct: float

    def __post_init__(self) -> None:
        if self.instrument_id <= 0:
            raise ValueError("instrument_id must be positive")
        if not self.sector or not self.mechanism:
            raise ValueError("sector and mechanism must be non-empty")
        if self.exit_date < self.entry_date:
            raise ValueError("exit_date cannot precede entry_date")
        for name in (
            "net_return_pct",
            "net_return_double_cost_pct",
            "holding_minutes",
            "turnover_fraction",
            "max_adverse_excursion_pct",
        ):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(f"{name} must be finite")
        if self.net_return_double_cost_pct > self.net_return_pct:
            raise ValueError("doubling a non-negative execution cost cannot improve net return")
        if self.holding_minutes < 0 or self.turnover_fraction < 0:
            raise ValueError("holding_minutes and turnover_fraction must be non-negative")
        if self.max_adverse_excursion_pct > 0:
            raise ValueError("max_adverse_excursion_pct must be non-positive")


CellRefusal = Literal[
    "fewer_than_30_trades",
    "bootstrap_not_computable",
    "effective_sample_below_30",
    "expectancy_ci_not_strictly_positive",
    "double_cost_expectancy_not_positive",
    "path_metrics_not_computed",
]


@dataclass(frozen=True)
class CohortPathMetrics:
    """Portfolio-simulator outputs for one cohort sub-book.

    Drawdown cannot be reconstructed from closed-trade returns: overlapping
    positions and intratrade marks matter. The event-time simulator therefore
    supplies the path metrics explicitly and stamps its model identity.
    """

    max_drawdown_pct: float
    exposure_time_pct: float
    turnover_annualised: float
    path_model_id: str

    def __post_init__(self) -> None:
        if self.max_drawdown_pct > 0:
            raise ValueError("max_drawdown_pct must be non-positive")
        if not 0 <= self.exposure_time_pct <= 100:
            raise ValueError("exposure_time_pct must be inside 0-100")
        if self.turnover_annualised < 0:
            raise ValueError("turnover_annualised must be non-negative")
        if not self.path_model_id:
            raise ValueError("path_model_id must be non-empty")


@dataclass(frozen=True)
class CohortCell:
    key: CohortKey
    trade_count: int
    entry_date_count: int
    effective_sample_size: float | None
    expectancy_pct: float
    expectancy_ci_low_pct: float | None
    expectancy_ci_high_pct: float | None
    hit_rate_pct: float
    profit_factor: float | None
    median_return_pct: float
    double_cost_expectancy_pct: float
    average_holding_minutes: float
    average_turnover_fraction: float
    worst_trade_pct: float
    expected_shortfall_5pct: float
    worst_mae_pct: float
    max_drawdown_pct: float | None
    exposure_time_pct: float | None
    turnover_annualised: float | None
    path_model_id: str | None
    largest_entry_date_share_pct: float
    largest_instrument_share_pct: float
    largest_sector_share_pct: float
    bootstrap_model_id: str | None
    verdict: CohortVerdict
    refusals: tuple[CellRefusal, ...]

    def __post_init__(self) -> None:
        if self.trade_count < 1 or not 1 <= self.entry_date_count <= self.trade_count:
            raise ValueError("cohort counts are inconsistent")
        if not 0 <= self.hit_rate_pct <= 100:
            raise ValueError("hit_rate_pct must be inside 0-100")
        for name in (
            "largest_entry_date_share_pct",
            "largest_instrument_share_pct",
            "largest_sector_share_pct",
        ):
            if not 0 < getattr(self, name) <= 100:
                raise ValueError(f"{name} must be inside (0, 100]")
        bootstrap_values = (
            self.effective_sample_size,
            self.expectancy_ci_low_pct,
            self.expectancy_ci_high_pct,
            self.bootstrap_model_id,
        )
        if sum(value is not None for value in bootstrap_values) not in (0, len(bootstrap_values)):
            raise ValueError("bootstrap fields must be present or absent together")
        path_values = (self.max_drawdown_pct, self.exposure_time_pct, self.turnover_annualised, self.path_model_id)
        if sum(value is not None for value in path_values) not in (0, len(path_values)):
            raise ValueError("path fields must be present or absent together")
        if (self.verdict == "economically_positive") != (not self.refusals):
            raise ValueError("an economically positive cell has no refusals; a refused cell has at least one")


@dataclass(frozen=True)
class CandidateCohortReport:
    strategy_id: str
    strategy_version: str
    context_version: str
    outcome_version: str
    cost_model_id: str
    window_start: date
    window_end: date
    observation_count: int
    cells: tuple[CohortCell, ...]

    def __post_init__(self) -> None:
        for name in ("strategy_id", "strategy_version", "context_version", "outcome_version", "cost_model_id"):
            if not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")
        if self.window_end < self.window_start:
            raise ValueError("report window is inverted")
        if self.observation_count < 1:
            raise ValueError("a cohort report needs at least one observation")
        keys = [cell.key for cell in self.cells]
        if len(keys) != len(set(keys)):
            raise ValueError("cohort report contains duplicate cells")
        if UNPOOLED_KEY not in keys:
            raise ValueError("cohort report must contain the unpooled population")
        unpooled = self.cell(UNPOOLED_KEY)
        if unpooled is None or unpooled.trade_count != self.observation_count:
            raise ValueError("unpooled trade count must equal report observation_count")
        if any(cell.trade_count > self.observation_count for cell in self.cells):
            raise ValueError("a cohort cell cannot exceed its report population")

    def cell(self, key: CohortKey) -> CohortCell | None:
        return next((cell for cell in self.cells if cell.key == key), None)


def _seed_for(root_seed: int, key: CohortKey) -> int:
    payload = f"{root_seed}:{key.label}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:4], "big")


def _share(counter: Counter[object], total: int) -> float:
    return max(counter.values(), default=0) / total * 100.0


def _cell(
    key: CohortKey,
    observations: Sequence[CohortObservation],
    *,
    root_seed: int,
    path: CohortPathMetrics | None,
) -> CohortCell:
    returns = np.asarray([item.net_return_pct for item in observations], dtype=np.float64)
    double_cost = np.asarray([item.net_return_double_cost_pct for item in observations], dtype=np.float64)
    bootstrap = block_bootstrap_expectancy(
        cluster_by_date(returns.tolist(), [item.entry_date for item in observations]),
        seed=_seed_for(root_seed, key),
    )
    winners = returns[returns > 0]
    losers = returns[returns < 0]
    profit_factor = None if not len(losers) else float(winners.sum() / abs(losers.sum()))
    tail_count = max(1, math.ceil(len(returns) * 0.05))
    expected_shortfall = float(np.mean(np.sort(returns)[:tail_count]))

    refusals: list[CellRefusal] = []
    if len(observations) < MIN_CELL_TRADES:
        refusals.append("fewer_than_30_trades")
    if bootstrap is None:
        refusals.append("bootstrap_not_computable")
    else:
        if bootstrap.effective_sample_size < MIN_EFFECTIVE_SAMPLE:
            refusals.append("effective_sample_below_30")
        if bootstrap.ci_low_pct <= 0:
            refusals.append("expectancy_ci_not_strictly_positive")
    if float(np.mean(double_cost)) <= 0:
        refusals.append("double_cost_expectancy_not_positive")
    if path is None:
        refusals.append("path_metrics_not_computed")

    return CohortCell(
        key=key,
        trade_count=len(observations),
        entry_date_count=len({item.entry_date for item in observations}),
        effective_sample_size=None if bootstrap is None else bootstrap.effective_sample_size,
        expectancy_pct=float(np.mean(returns)),
        expectancy_ci_low_pct=None if bootstrap is None else bootstrap.ci_low_pct,
        expectancy_ci_high_pct=None if bootstrap is None else bootstrap.ci_high_pct,
        hit_rate_pct=float(np.mean(returns > 0) * 100.0),
        profit_factor=profit_factor,
        median_return_pct=float(np.median(returns)),
        double_cost_expectancy_pct=float(np.mean(double_cost)),
        average_holding_minutes=float(np.mean([item.holding_minutes for item in observations])),
        average_turnover_fraction=float(np.mean([item.turnover_fraction for item in observations])),
        worst_trade_pct=float(np.min(returns)),
        expected_shortfall_5pct=expected_shortfall,
        worst_mae_pct=min(item.max_adverse_excursion_pct for item in observations),
        max_drawdown_pct=None if path is None else path.max_drawdown_pct,
        exposure_time_pct=None if path is None else path.exposure_time_pct,
        turnover_annualised=None if path is None else path.turnover_annualised,
        path_model_id=None if path is None else path.path_model_id,
        largest_entry_date_share_pct=_share(Counter(item.entry_date for item in observations), len(observations)),
        largest_instrument_share_pct=_share(Counter(item.instrument_id for item in observations), len(observations)),
        largest_sector_share_pct=_share(Counter(item.sector for item in observations), len(observations)),
        bootstrap_model_id=None if bootstrap is None else BOOTSTRAP_MODEL_ID,
        verdict="economically_positive" if not refusals else "refused",
        refusals=tuple(refusals),
    )


def _group_observations(
    observations: Sequence[CohortObservation],
) -> dict[CohortKey, list[CohortObservation]]:
    grouped: dict[CohortKey, list[CohortObservation]] = defaultdict(list)
    for item in observations:
        for dimensions in DECLARED_COHORTS:
            values = tuple(str(getattr(item, dimension)) for dimension in dimensions)
            grouped[CohortKey(dimensions, values)].append(item)
    return grouped


def cohort_keys_for(observations: Sequence[CohortObservation]) -> tuple[CohortKey, ...]:
    """The exact predeclared non-empty cells this population will emit."""
    return tuple(sorted(_group_observations(observations)))


def build_cohort_report(
    observations: Sequence[CohortObservation],
    *,
    strategy_id: str,
    strategy_version: str,
    context_version: str,
    outcome_version: str,
    cost_model_id: str,
    window_start: date,
    window_end: date,
    root_seed: int,
    path_metrics: Mapping[CohortKey, CohortPathMetrics] | None = None,
) -> CandidateCohortReport:
    """Emit every declared non-empty cell, including failures."""
    if not observations:
        raise ValueError("cannot report an empty observation population")
    if any(item.entry_date < window_start or item.exit_date > window_end for item in observations):
        raise ValueError("an observation lies outside the declared report window")

    grouped = _group_observations(observations)
    supplied_paths = {} if path_metrics is None else dict(path_metrics)
    extra_paths = supplied_paths.keys() - grouped.keys()
    if extra_paths:
        raise ValueError(f"path metrics contain undeclared/empty cells: {sorted(extra_paths)!r}")
    cells = tuple(
        _cell(key, grouped[key], root_seed=root_seed, path=supplied_paths.get(key)) for key in sorted(grouped)
    )
    return CandidateCohortReport(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        context_version=context_version,
        outcome_version=outcome_version,
        cost_model_id=cost_model_id,
        window_start=window_start,
        window_end=window_end,
        observation_count=len(observations),
        cells=cells,
    )


StabilityRefusal = Literal[
    "fewer_than_two_walk_forward_folds",
    "terminal_interval_missing",
    "window_order_overlaps",
    "incompatible_report_identity",
    "terminal_interval_not_later",
    "cohort_missing_from_window",
    "cohort_not_economically_positive_in_every_window",
]


@dataclass(frozen=True)
class WindowEvidence:
    kind: WindowKind
    report: CandidateCohortReport


@dataclass(frozen=True)
class CohortStability:
    key: CohortKey
    stable: bool
    window_count: int
    refusals: tuple[StabilityRefusal, ...]


def assess_recent_stability(evidence: Sequence[WindowEvidence], *, key: CohortKey = UNPOOLED_KEY) -> CohortStability:
    """Require two walk-forward folds and a later untouched/prospective interval.

    This is a gate over preregistered cells, never a function that chooses the
    best key. The terminal interval must start after every walk-forward window.
    """
    ordered = sorted(evidence, key=lambda item: item.report.window_start)
    refusals: list[StabilityRefusal] = []
    folds = [item for item in ordered if item.kind == "walk_forward"]
    terminals = [item for item in ordered if item.kind in ("untouched", "prospective")]
    if len(folds) < 2:
        refusals.append("fewer_than_two_walk_forward_folds")
    if not terminals:
        refusals.append("terminal_interval_missing")
    if any(
        later.report.window_start <= earlier.report.window_end
        for earlier, later in zip(ordered, ordered[1:], strict=False)
    ):
        refusals.append("window_order_overlaps")
    identities = {
        (
            item.report.strategy_id,
            item.report.strategy_version,
            item.report.context_version,
            item.report.outcome_version,
            item.report.cost_model_id,
        )
        for item in ordered
    }
    if len(identities) > 1:
        refusals.append("incompatible_report_identity")
    if (
        terminals
        and folds
        and not any(
            terminal.report.window_start > max(fold.report.window_end for fold in folds) for terminal in terminals
        )
    ):
        refusals.append("terminal_interval_not_later")

    cells = [item.report.cell(key) for item in ordered]
    if any(cell is None for cell in cells):
        refusals.append("cohort_missing_from_window")
    elif any(cell.verdict != "economically_positive" for cell in cells if cell is not None):
        refusals.append("cohort_not_economically_positive_in_every_window")

    return CohortStability(key=key, stable=not refusals, window_count=len(ordered), refusals=tuple(refusals))


__all__ = [
    "DECLARED_COHORTS",
    "MIN_CELL_TRADES",
    "MIN_EFFECTIVE_SAMPLE",
    "UNPOOLED_KEY",
    "CandidateCohortReport",
    "CohortCell",
    "CohortKey",
    "CohortObservation",
    "CohortPathMetrics",
    "CohortStability",
    "WindowEvidence",
    "assess_recent_stability",
    "build_cohort_report",
    "cohort_keys_for",
]
