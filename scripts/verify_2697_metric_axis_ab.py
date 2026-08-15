"""In-sample-only full-population A/B for #2697's metric-axis correction.

The treatment and legacy metrics are computed from the same in-memory books
produced by one ``load_corpus`` call. The legacy helper is script-local and
plainly named; production exposes no switch back to position-selected spans.
No hold-out window or stored result is read, and this script writes nothing.

Run the acceptance arm without ``--limit``::

    uv run python -m scripts.verify_2697_metric_axis_ab > /tmp/2697-ab.jsonl

``--limit`` is a wiring smoke only and labels every row accordingly.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import numpy as np
import psycopg

from app.config import settings
from app.services import backtest_run
from app.services.backtest_run import (
    BACKTEST_BOOTSTRAP_SEED,
    BACKTEST_UNIVERSE,
    QUARANTINE_ARM_ORDER,
    NamespaceMeasurement,
    _benchmark_book,
    _Corpus,
    _NamespaceBook,
    _regime_for,
    _shifted,
    evaluate_arm,
    evaluate_level_arms,
    load_corpus,
    runnable_strategies,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import (
    ENTRY_WEIGHT_DRIFT_RULE_ID,
    MONTH_END_REBALANCE_RULE_ID,
    SIZING_RULE_ID,
    build_buy_and_hold_curve,
    build_entry_weight_drift_curve,
    build_equity_curve,
    build_month_end_rebalanced_curve,
)
from app.services.random_entry_cohort import MemberOutcome, SyntheticControl, evaluate_control, member_seed
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import AmbiguityArm, QuarantineArm, ResultNamespace
from app.services.strategy_statistics import DatedEquityCurve, StrategyMetrics, TradeReturns, compute_metrics
from app.services.synthetic_control_run import (
    SYNTHETIC_CONTROL_MAX_WORKERS,
    CohortCollector,
    CohortResult,
    _place_member,
)


@dataclass(frozen=True)
class _LegacyMeasurement:
    dates: tuple[date, ...]
    comparator_population: int
    metrics: StrategyMetrics


@dataclass(frozen=True)
class _LegacyCohortInputs:
    collector: CohortCollector
    axis: tuple[date, ...]
    expected_trade_count: int


_LEGACY_WORKER_INPUTS: _LegacyCohortInputs | None = None
_REPO = Path(__file__).resolve().parents[1]


def _exact_candidate_head() -> str:
    """Bind expensive evidence to one clean, reviewable source tree."""
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=_REPO,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise RuntimeError("the metric-axis A/B requires a clean worktree so its evidence has one exact source head")
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=_REPO,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _legacy_first_index_last_index_measurement(
    book: _NamespaceBook,
    *,
    corpus: _Corpus,
    raw_closes_by_instrument: dict[int, tuple[int, Any]],
    wealth_closes_by_instrument: dict[int, tuple[int, Any]],
    sizing_rule: str,
) -> _LegacyMeasurement | None:
    """The removed position-selected construction, isolated to this A/B."""
    if book.first_index is None or book.last_index is None:
        return None
    lo, hi = book.first_index, book.last_index
    if hi - lo < 1:
        return None
    dates = corpus.axis[lo : hi + 1]
    shifted = _shifted(book.book, lo)
    if sizing_rule == SIZING_RULE_ID:
        curve = build_equity_curve(shifted, date_count=len(dates))
    elif sizing_rule == ENTRY_WEIGHT_DRIFT_RULE_ID:
        curve = build_entry_weight_drift_curve(shifted, date_count=len(dates))
    elif sizing_rule == MONTH_END_REBALANCE_RULE_ID:
        curve = build_month_end_rebalanced_curve(shifted, dates=dates)
    else:  # pragma: no cover - the production caller closes this vocabulary
        raise ValueError(f"unknown sizing rule {sizing_rule!r}")
    legacy_population = frozenset(book.instruments)
    benchmark = build_buy_and_hold_curve(
        _benchmark_book(
            instruments=legacy_population,
            raw_closes_by_instrument=raw_closes_by_instrument,
            wealth_closes_by_instrument=wealth_closes_by_instrument,
            lo=lo,
            hi=hi,
        ),
        date_count=len(dates),
    )
    metrics = compute_metrics(
        DatedEquityCurve(dates=dates, curve=curve),
        trades=TradeReturns(
            net_return_pct=tuple(book.returns),
            entry_fill_date=tuple(book.entry_dates),
            exit_bar_date=tuple(book.exit_dates),
            open_count=book.open_at_end,
            unpriced_count=sum(book.excluded.values()),
        ),
        buy_and_hold=DatedEquityCurve(dates=dates, curve=benchmark),
        bootstrap_seed=BACKTEST_BOOTSTRAP_SEED,
    )
    return _LegacyMeasurement(dates, len(legacy_population), metrics)


def _metric_payload(metrics: StrategyMetrics) -> dict[str, float | None]:
    return {
        "total_return_pct": metrics.total_return_pct,
        "cagr_pct": metrics.cagr_pct,
        "periods_per_year": metrics.periods_per_year,
        "annualised_volatility_pct": metrics.annualised_volatility_pct,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "max_drawdown_pct": metrics.max_drawdown_pct,
        "exposure_time_pct": metrics.exposure_time_pct,
        "turnover_annualised": metrics.turnover_annualised,
        "buy_and_hold_return_pct": metrics.buy_and_hold_return_pct,
        "return_vs_buy_and_hold_pct": metrics.return_vs_buy_and_hold_pct,
    }


def _delta(old: StrategyMetrics, new: StrategyMetrics) -> dict[str, float | None]:
    payload: dict[str, float | None] = {}
    for name, old_value in _metric_payload(old).items():
        new_value = _metric_payload(new)[name]
        payload[name] = None if old_value is None or new_value is None else new_value - old_value
    return payload


def _legacy_cohort_control(
    collector: CohortCollector,
    *,
    axis: tuple[date, ...],
    strategy_metrics: StrategyMetrics,
    cohort_size: int,
    max_workers: int | None = None,
    label: str = "legacy",
) -> SyntheticControl:
    """Reproduce the removed per-member position-span annualisation rule.

    Placement, seed hierarchy, costs and trade population remain identical to
    production. Only each already-placed member's curve axis changes, which is
    the treatment isolated by this A/B.
    """
    if not collector.placements:
        raise ValueError("the legacy cohort cannot be measured from an empty placement space")
    workers = (
        min(SYNTHETIC_CONTROL_MAX_WORKERS, cohort_size)
        if max_workers is None and cohort_size == backtest_run.SPEC_COHORT_SIZE
        else 1
        if max_workers is None
        else max_workers
    )
    if workers < 1:
        raise ValueError(f"max_workers must be positive, got {workers}")
    workers = min(workers, cohort_size)
    inputs = _LegacyCohortInputs(
        collector=collector,
        axis=axis,
        expected_trade_count=collector.matchable_trade_count,
    )
    by_index: dict[int, MemberOutcome] = {}

    def accept(expected_index: int, outcome: MemberOutcome) -> None:
        if outcome.index != expected_index:
            raise RuntimeError(f"legacy cohort task {expected_index} returned member {outcome.index}")
        if outcome.index in by_index:
            raise RuntimeError(f"legacy cohort member {outcome.index} completed more than once")
        by_index[outcome.index] = outcome
        completed = len(by_index)
        if completed == 1 or completed % 10 == 0 or completed == cohort_size:
            print(f"[{label}] legacy synthetic control {completed}/{cohort_size}", file=sys.stderr, flush=True)

    if workers == 1:
        for index in range(cohort_size):
            accept(index, _legacy_member(index, inputs))
    else:
        # Same spawn-only boundary as production. The verifier holds a psycopg
        # connection while measuring but children receive only immutable cohort
        # arrays; no database handle or transaction crosses the process edge.
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=get_context("spawn"),
            initializer=_initialise_legacy_worker,
            initargs=(inputs,),
        ) as pool:
            pending = {pool.submit(_legacy_member_in_worker, index): index for index in range(cohort_size)}
            try:
                for future in as_completed(pending):
                    accept(pending[future], future.result())
            except BaseException:
                for future in pending:
                    future.cancel()
                raise

    expected_indices = set(range(cohort_size))
    if set(by_index) != expected_indices:
        raise RuntimeError(
            f"legacy cohort member set is incomplete: missing={sorted(expected_indices - set(by_index))[:20]}, "
            f"extra={sorted(set(by_index) - expected_indices)[:20]}"
        )
    members = tuple(by_index[index] for index in range(cohort_size))
    return evaluate_control(
        members,
        strategy_sharpe=strategy_metrics.sharpe,
        strategy_return_pct=strategy_metrics.total_return_pct,
    )


def _initialise_legacy_worker(inputs: _LegacyCohortInputs) -> None:
    global _LEGACY_WORKER_INPUTS
    _LEGACY_WORKER_INPUTS = inputs


def _legacy_member_in_worker(index: int) -> MemberOutcome:
    if _LEGACY_WORKER_INPUTS is None:  # pragma: no cover - executor owns initialization
        raise RuntimeError("legacy cohort worker started without member inputs")
    return _legacy_member(index, _LEGACY_WORKER_INPUTS)


def _legacy_member(index: int, inputs: _LegacyCohortInputs) -> MemberOutcome:
    rng = np.random.Generator(np.random.PCG64(member_seed(index)))
    book, returns, entry_dates, exit_dates = _place_member(
        rng,
        inputs.collector.placements,
        axis=inputs.axis,
    )
    if len(book) != inputs.expected_trade_count:
        raise RuntimeError(
            f"legacy cohort member {index} placed {len(book):,} legs against the strategy's "
            f"{inputs.expected_trade_count:,} matchable positions"
        )
    low = min(book.entry_index)
    high = max(book.exit_index)
    dates = inputs.axis[low : high + 1]
    curve = build_equity_curve(book.rebased(low), date_count=len(dates))
    metrics = compute_metrics(
        DatedEquityCurve(dates=dates, curve=curve),
        trades=TradeReturns(
            net_return_pct=tuple(returns),
            entry_fill_date=tuple(entry_dates),
            exit_bar_date=tuple(exit_dates),
            open_count=0,
            unpriced_count=0,
        ),
        buy_and_hold=None,
        bootstrap_seed=None,
    )
    return MemberOutcome(
        index=index,
        sharpe=metrics.sharpe,
        total_return_pct=metrics.total_return_pct,
        exposure_time_pct=metrics.exposure_time_pct,
        turnover_annualised=metrics.turnover_annualised,
        trade_count=metrics.trade_count,
    )


def _control_payload(control: SyntheticControl) -> dict[str, float | bool]:
    return {
        "cohort_sharpe_threshold": control.cohort_sharpe_threshold,
        "cohort_return_threshold_pct": control.cohort_return_threshold_pct,
        "passed": control.passed,
    }


def _control_delta(old: SyntheticControl, new: SyntheticControl) -> dict[str, float]:
    return {
        "cohort_sharpe_threshold": new.cohort_sharpe_threshold - old.cohort_sharpe_threshold,
        "cohort_return_threshold_pct": new.cohort_return_threshold_pct - old.cohort_return_threshold_pct,
    }


def _population_label(*, limit: int | None, strategy: str | None) -> str:
    if limit is None and strategy is None:
        return "full"
    parts = ["smoke"]
    if limit is not None:
        parts.append(f"limit-{limit}")
    if strategy is not None:
        parts.append(f"strategy-{strategy}")
    return "-".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="series limit; makes this a smoke, not acceptance")
    parser.add_argument("--strategy", choices=tuple(sorted(STRATEGY_MANIFEST)), default=None)
    args = parser.parse_args()
    candidate_head = _exact_candidate_head()

    def report_progress(event: backtest_run.BacktestProgressEvent) -> None:
        if event.phase != "synthetic_control" or event.series_seen is None or event.series_total is None:
            return
        if event.series_seen == 1 or event.series_seen % 10 == 0 or event.series_seen == event.series_total:
            print(
                f"[{event.strategy_id}/{event.ambiguity_arm or 'shared'}/{event.quarantine_arm}] "
                f"current synthetic control {event.series_seen}/{event.series_total}",
                file=sys.stderr,
                flush=True,
            )

    with psycopg.connect(settings.database_url) as conn:
        corpus = load_corpus(conn, universe_basis=BACKTEST_UNIVERSE, limit=args.limit, evaluation_window=None)
        runnable, excluded = runnable_strategies()
        if not runnable:
            raise RuntimeError("no runnable strategies exist; an empty A/B is not acceptance evidence")
        if args.strategy is not None:
            if args.strategy not in runnable:
                raise RuntimeError(f"{args.strategy} is not runnable under the current manifest")
            runnable = (args.strategy,)
        elif args.limit is None and excluded:
            raise RuntimeError(
                f"full-population A/B requires every manifest strategy to be runnable; excluded={excluded}"
            )
        population = _population_label(limit=args.limit, strategy=args.strategy)
        expected_rows = sum(
            len(QUARANTINE_ARM_ORDER)
            * (2 if _regime_for(STRATEGY_MANIFEST[item], corpus.axis).level_based or corpus.termination else 1)
            for item in runnable
        )
        emitted_rows = 0

        original_measure = backtest_run._measure_namespace
        original_run_cohort_for = backtest_run._run_cohort_for
        captured: list[_LegacyMeasurement | None] = []
        cohort_pairs: list[tuple[SyntheticControl | None, SyntheticControl | None]] = []

        def capture(namespace: str, book: _NamespaceBook, **kwargs: Any) -> Any:
            assert namespace == "in_sample", "the #2697 A/B must never open hold-out"
            captured.append(
                _legacy_first_index_last_index_measurement(
                    book,
                    corpus=kwargs["corpus"],
                    raw_closes_by_instrument=kwargs["raw_closes_by_instrument"],
                    wealth_closes_by_instrument=kwargs["wealth_closes_by_instrument"],
                    sizing_rule=kwargs.get("sizing_rule", SIZING_RULE_ID),
                )
            )
            return original_measure(namespace, book, **kwargs)

        def capture_cohort(
            collector: CohortCollector | None,
            *,
            measured: Mapping[ResultNamespace, NamespaceMeasurement],
            corpus: _Corpus,
            cohort_size: int | None,
            label: str,
            strategy_id: str,
            quarantine_arm: QuarantineArm,
            ambiguity_arm: AmbiguityArm | None = None,
            progress: backtest_run.ProgressCallback | None = None,
        ) -> CohortResult | None:
            current_result = original_run_cohort_for(
                collector,
                measured=measured,
                corpus=corpus,
                cohort_size=cohort_size,
                label=label,
                progress=progress,
                strategy_id=strategy_id,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=ambiguity_arm,
            )
            legacy = captured[len(cohort_pairs)]
            legacy_control = None
            if current_result is not None and collector is not None and cohort_size is not None and legacy is not None:
                legacy_control = _legacy_cohort_control(
                    collector,
                    axis=corpus.axis,
                    strategy_metrics=legacy.metrics,
                    cohort_size=cohort_size,
                    label=label,
                )
            cohort_pairs.append((legacy_control, None if current_result is None else current_result.control))
            return current_result

        backtest_run._measure_namespace = capture  # type: ignore[assignment]
        backtest_run._run_cohort_for = capture_cohort  # type: ignore[assignment]
        try:
            for strategy_id in runnable:
                entry = STRATEGY_MANIFEST[strategy_id]
                identity = entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
                for quarantine_arm in QUARANTINE_ARM_ORDER:
                    captured.clear()
                    cohort_pairs.clear()
                    if _regime_for(entry, corpus.axis).level_based or corpus.termination:
                        arms = evaluate_level_arms(
                            conn,
                            entry,
                            corpus=corpus,
                            quarantine_arm=quarantine_arm,
                            identity=identity,
                            namespaces=("in_sample",),
                            cohort_size=backtest_run.SPEC_COHORT_SIZE,
                            progress=report_progress,
                        )
                    else:
                        arms = (
                            evaluate_arm(
                                conn,
                                entry,
                                corpus=corpus,
                                quarantine_arm=quarantine_arm,
                                ambiguity_arm=None,
                                identity=identity,
                                namespaces=("in_sample",),
                                cohort_size=backtest_run.SPEC_COHORT_SIZE,
                                progress=report_progress,
                            ),
                        )
                    if len(captured) != len(arms) or len(cohort_pairs) != len(arms):
                        raise RuntimeError(
                            f"captured {len(captured)} legacy rows and {len(cohort_pairs)} cohort pairs "
                            f"for {len(arms)} current arms"
                        )
                    for arm, legacy, controls in zip(arms, captured, cohort_pairs, strict=True):
                        current = arm.namespaces.get("in_sample")
                        legacy_control, current_control = controls
                        row: dict[str, object] = {
                            "record_type": "comparison",
                            "candidate_head": candidate_head,
                            "population": population,
                            "strategy_id": strategy_id,
                            "ambiguity_arm": arm.ambiguity_arm or "shared",
                            "quarantine_arm": quarantine_arm,
                        }
                        if legacy_control is None or current_control is None:
                            row.update(
                                {
                                    "legacy_synthetic_control": legacy_control is not None,
                                    "current_synthetic_control": current_control is not None,
                                    "synthetic_threshold_delta_current_minus_legacy": None,
                                    "synthetic_threshold_note": (
                                        "no control is defined for an all-cash or absent result"
                                    ),
                                }
                            )
                        else:
                            row.update(
                                {
                                    "legacy_synthetic_thresholds": _control_payload(legacy_control),
                                    "current_synthetic_thresholds": _control_payload(current_control),
                                    "synthetic_threshold_delta_current_minus_legacy": _control_delta(
                                        legacy_control, current_control
                                    ),
                                }
                            )
                        if current is None:
                            raise RuntimeError(
                                f"{strategy_id}/{arm.ambiguity_arm or 'shared'}/{quarantine_arm} has no current "
                                "fixed-axis result despite a non-empty frozen opportunity population"
                            )
                        opportunity_count = len(current.universe_record.evaluated_instrument_ids) + len(
                            current.universe_record.evaluated_series_ids
                        )
                        row.update(
                            {
                                "current_axis": [
                                    current.axis_dates[0].isoformat(),
                                    current.axis_dates[-1].isoformat(),
                                ],
                                "current_opportunity_population": opportunity_count,
                                "current_comparator_population": opportunity_count,
                                "current": _metric_payload(current.metrics),
                            }
                        )
                        if legacy is None:
                            row.update(
                                {
                                    "legacy_result": False,
                                    "current_result": True,
                                    "result_note": (
                                        "legacy position-selected span is undefined for an all-cash strategy"
                                    ),
                                }
                            )
                        else:
                            row.update(
                                {
                                    "legacy_axis": [legacy.dates[0].isoformat(), legacy.dates[-1].isoformat()],
                                    "legacy_comparator_population": legacy.comparator_population,
                                    "legacy": _metric_payload(legacy.metrics),
                                    "delta_current_minus_legacy": _delta(legacy.metrics, current.metrics),
                                }
                            )
                        print(json.dumps(row, sort_keys=True))
                        emitted_rows += 1
            if emitted_rows != expected_rows:
                raise RuntimeError(f"A/B emitted {emitted_rows} rows; the declared population requires {expected_rows}")
            print(
                json.dumps(
                    {
                        "record_type": "acceptance_summary",
                        "candidate_head": candidate_head,
                        "population": population,
                        "expected_rows": expected_rows,
                        "observed_rows": emitted_rows,
                        "complete": True,
                    },
                    sort_keys=True,
                )
            )
        finally:
            backtest_run._measure_namespace = original_measure
            backtest_run._run_cohort_for = original_run_cohort_for
            conn.rollback()


if __name__ == "__main__":
    main()
