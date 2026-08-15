"""Immutable per-regime trade cohorts attached to a strategy result (#2437).

The regime is classified on the entry SIGNAL date: that is the information the
strategy consumed when it decided to fire.  Classifying on the next-open fill or
the eventual exit would condition a decision on state that did not exist yet.

These cohorts explain a parent result; they are not independently promotable
results.  Portfolio path statistics (especially drawdown) remain on the parent,
because filtering closed trades cannot reconstruct overlapping marked paths.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Final, Literal, get_args

import numpy as np
import psycopg

from app.services.block_bootstrap import (
    BOOTSTRAP_MODEL_ID,
    BootstrapResult,
    block_bootstrap_expectancy,
    cluster_by_date,
)
from app.services.market_regime import Regime

RegimeCohortLabel = Literal["bull_quiet", "bull_volatile", "bear_quiet", "bear_volatile", "unclassified"]
REGIME_COHORT_LABELS: Final[frozenset[str]] = frozenset(get_args(RegimeCohortLabel))


@dataclass(frozen=True)
class RegimeTradeObservation:
    instrument_key: int
    signal_date: date
    net_return_pct: float
    regime: Regime | None

    def __post_init__(self) -> None:
        if self.instrument_key == 0:
            raise ValueError("instrument_key cannot be zero")
        if not math.isfinite(self.net_return_pct):
            raise ValueError("net_return_pct must be finite")

    @property
    def label(self) -> RegimeCohortLabel:
        return "unclassified" if self.regime is None else self.regime.value


@dataclass(frozen=True)
class RegimeCohort:
    regime: RegimeCohortLabel
    trade_count: int
    instrument_count: int
    decision_date_count: int
    losing_trade_count: int
    expectancy_pct: float
    profit_factor: float | None
    worst_trade_pct: float
    effective_sample_size: float | None
    expectancy_ci_low_pct: float | None
    expectancy_ci_high_pct: float | None
    bootstrap_block_length: int | None
    bootstrap_cluster_count: int | None
    bootstrap_resamples: int | None
    bootstrap_seed: int | None
    bootstrap_design_effect: float | None
    bootstrap_model_id: str | None

    def __post_init__(self) -> None:
        if self.regime not in REGIME_COHORT_LABELS:
            raise ValueError(f"unknown regime cohort {self.regime!r}")
        if self.trade_count < 1:
            raise ValueError("a regime cohort needs at least one realised trade")
        if not 1 <= self.instrument_count <= self.trade_count:
            raise ValueError("instrument_count must be inside [1, trade_count]")
        if not 1 <= self.decision_date_count <= self.trade_count:
            raise ValueError("decision_date_count must be inside [1, trade_count]")
        if not 0 <= self.losing_trade_count <= self.trade_count:
            raise ValueError("losing_trade_count must be inside [0, trade_count]")
        if (self.profit_factor is None) != (self.losing_trade_count == 0):
            raise ValueError("profit_factor is absent exactly when the cohort has no losing trade")
        for name in ("expectancy_pct", "worst_trade_pct"):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(f"{name} must be finite")
        if self.profit_factor is not None and (not math.isfinite(self.profit_factor) or self.profit_factor < 0):
            raise ValueError("profit_factor must be finite and non-negative when present")
        if self.worst_trade_pct > self.expectancy_pct:
            raise ValueError("worst_trade_pct cannot exceed the cohort expectancy")
        bootstrap = (
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
        if sum(value is not None for value in bootstrap) not in {0, len(bootstrap)}:
            raise ValueError("bootstrap fields must be all present or all absent")
        if self.effective_sample_size is not None:
            assert self.expectancy_ci_low_pct is not None
            assert self.expectancy_ci_high_pct is not None
            assert self.bootstrap_block_length is not None
            assert self.bootstrap_cluster_count is not None
            assert self.bootstrap_resamples is not None
            assert self.bootstrap_seed is not None
            assert self.bootstrap_design_effect is not None
            assert self.bootstrap_model_id is not None
            numeric = (
                self.effective_sample_size,
                self.expectancy_ci_low_pct,
                self.expectancy_ci_high_pct,
                self.bootstrap_design_effect,
            )
            if any(value is None or not math.isfinite(value) for value in numeric):
                raise ValueError("bootstrap numeric fields must be finite")
            if self.effective_sample_size <= 0 or self.bootstrap_design_effect <= 0:
                raise ValueError("bootstrap effective sample and design effect must be positive")
            if self.expectancy_ci_low_pct > self.expectancy_ci_high_pct:
                raise ValueError("bootstrap expectancy interval is inverted")
            if (
                min(
                    self.bootstrap_block_length,
                    self.bootstrap_cluster_count,
                    self.bootstrap_resamples,
                )
                <= 0
            ):
                raise ValueError("bootstrap counts must be positive")
            if self.bootstrap_seed < 0:
                raise ValueError("bootstrap_seed must be non-negative")
            if not self.bootstrap_model_id.strip():
                raise ValueError("bootstrap_model_id must be non-empty")


def _seed(root_seed: int, label: RegimeCohortLabel) -> int:
    return int.from_bytes(hashlib.sha256(f"{root_seed}:{label}".encode()).digest()[:4], "big")


def _cohort(label: RegimeCohortLabel, rows: Sequence[RegimeTradeObservation], *, root_seed: int) -> RegimeCohort:
    returns = np.asarray([row.net_return_pct for row in rows], dtype=np.float64)
    losing = returns[returns < 0]
    winning = returns[returns > 0]
    bootstrap = block_bootstrap_expectancy(
        cluster_by_date(returns.tolist(), [row.signal_date for row in rows]),
        seed=_seed(root_seed, label),
    )
    return RegimeCohort(
        regime=label,
        trade_count=len(rows),
        instrument_count=len({row.instrument_key for row in rows}),
        decision_date_count=len({row.signal_date for row in rows}),
        losing_trade_count=len(losing),
        expectancy_pct=float(np.mean(returns)),
        profit_factor=None if not len(losing) else float(winning.sum() / abs(losing.sum())),
        worst_trade_pct=float(np.min(returns)),
        **_bootstrap_fields(bootstrap),
    )


def _bootstrap_fields(result: BootstrapResult | None) -> dict[str, Any]:
    if result is None:
        return {
            "effective_sample_size": None,
            "expectancy_ci_low_pct": None,
            "expectancy_ci_high_pct": None,
            "bootstrap_block_length": None,
            "bootstrap_cluster_count": None,
            "bootstrap_resamples": None,
            "bootstrap_seed": None,
            "bootstrap_design_effect": None,
            "bootstrap_model_id": None,
        }
    return {
        "effective_sample_size": result.effective_sample_size,
        "expectancy_ci_low_pct": result.ci_low_pct,
        "expectancy_ci_high_pct": result.ci_high_pct,
        "bootstrap_block_length": result.block_length,
        "bootstrap_cluster_count": result.cluster_count,
        "bootstrap_resamples": result.resamples,
        "bootstrap_seed": result.seed,
        "bootstrap_design_effect": result.design_effect,
        "bootstrap_model_id": BOOTSTRAP_MODEL_ID,
    }


def build_regime_cohorts(observations: Sequence[RegimeTradeObservation], *, root_seed: int) -> tuple[RegimeCohort, ...]:
    grouped: dict[RegimeCohortLabel, list[RegimeTradeObservation]] = defaultdict(list)
    for row in observations:
        grouped[row.label].append(row)
    return tuple(_cohort(label, grouped[label], root_seed=root_seed) for label in sorted(grouped))


def store_result_regime_cohorts(
    conn: psycopg.Connection[Any],
    *,
    result_id: int,
    cohorts: Sequence[RegimeCohort],
    expected_trade_count: int,
) -> None:
    if not cohorts:
        raise ValueError("every result with realised trades must store at least one regime cohort")
    if len({row.regime for row in cohorts}) != len(cohorts):
        raise ValueError("a result cannot store the same regime cohort twice")
    if sum(row.trade_count for row in cohorts) != expected_trade_count:
        raise ValueError("regime cohort trade counts do not reconcile to the parent result")
    parent = conn.execute("SELECT trade_count FROM strategy_results_store WHERE result_id=%s", (result_id,)).fetchone()
    if parent is None:
        raise ValueError(f"parent strategy result {result_id} does not exist")
    if int(parent[0]) != expected_trade_count:
        raise ValueError(
            f"regime cohort expected trade count {expected_trade_count} does not match parent result {int(parent[0])}"
        )
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO strategy_result_regime_cohorts (
                result_id, regime, trade_count, instrument_count, decision_date_count,
                losing_trade_count, expectancy_pct, profit_factor, worst_trade_pct,
                effective_sample_size, expectancy_ci_low_pct, expectancy_ci_high_pct,
                bootstrap_block_length, bootstrap_cluster_count, bootstrap_resamples,
                bootstrap_seed, bootstrap_design_effect, bootstrap_model_id
            ) VALUES (
                %(result_id)s, %(regime)s, %(trade_count)s, %(instrument_count)s, %(decision_date_count)s,
                %(losing_trade_count)s, %(expectancy_pct)s, %(profit_factor)s, %(worst_trade_pct)s,
                %(effective_sample_size)s, %(expectancy_ci_low_pct)s, %(expectancy_ci_high_pct)s,
                %(bootstrap_block_length)s, %(bootstrap_cluster_count)s, %(bootstrap_resamples)s,
                %(bootstrap_seed)s, %(bootstrap_design_effect)s, %(bootstrap_model_id)s
            )
            """,
            ({"result_id": result_id, **row.__dict__} for row in cohorts),
        )
    stored = conn.execute(
        "SELECT count(*), coalesce(sum(trade_count), 0) FROM strategy_result_regime_cohorts WHERE result_id=%s",
        (result_id,),
    ).fetchone()
    if stored is None or int(stored[0]) != len(cohorts) or int(stored[1]) != expected_trade_count:
        raise RuntimeError("stored regime cohorts did not round-trip or reconcile to the parent result")


__all__ = [
    "REGIME_COHORT_LABELS",
    "RegimeCohort",
    "RegimeCohortLabel",
    "RegimeTradeObservation",
    "build_regime_cohorts",
    "store_result_regime_cohorts",
]
