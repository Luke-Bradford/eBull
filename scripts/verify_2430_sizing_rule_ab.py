"""Read-only full-population A/B for #2430's strategy sizing rule.

The sizing arms re-run the same registered strategy, quarantine, total-return
prices, fills, exits and costs. They differ only after a position event:

* ``equal_weight_concurrent_v1`` restores equal weight across tradeable legs;
* ``entry_weight_drift_v1`` leaves entry-time allocations untouched until exit;
* ``calendar_month_end_equal_weight_v1`` restores equal weight only at month end.

No result or access row is written. A limited run is smoke evidence only.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from typing import Any

import psycopg

from app.config import settings
from app.services.backtest_run import (
    BACKTEST_UNIVERSE,
    ArmMeasurement,
    BacktestProgressEvent,
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
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_recent_evidence import recent_evidence_window

_RULES = (SIZING_RULE_ID, ENTRY_WEIGHT_DRIFT_RULE_ID, MONTH_END_REBALANCE_RULE_ID)
_METRICS = (
    "total_return_pct",
    "max_drawdown_pct",
    "turnover_annualised",
    "sharpe",
    "cagr_pct",
    "exposure_time_pct",
    "buy_and_hold_return_pct",
    "return_vs_buy_and_hold_pct",
)


def _progress(rule: str):
    last: dict[tuple[str | None, str], int] = {}

    def emit(event: BacktestProgressEvent) -> None:
        if event.phase not in {"ranking", "evaluation"} or event.strategy_id is None:
            return
        key = (event.strategy_id, event.phase)
        if event.series_seen not in {1, event.series_total} and event.series_seen - last.get(key, 0) < 500:
            return
        last[key] = event.series_seen
        print(
            f"{rule} {event.strategy_id} {event.phase} {event.series_seen:,}/{event.series_total or 0:,}",
            flush=True,
        )

    return emit


def _run_rule(
    conn: psycopg.Connection[Any],
    *,
    corpus: Any,
    rule: str,
) -> dict[tuple[str, str], ArmMeasurement]:
    runnable, excluded = runnable_strategies()
    if excluded:
        print(f"excluded manifest entries: {[item.strategy_id for item in excluded]}", flush=True)
    measured: dict[tuple[str, str], ArmMeasurement] = {}
    for strategy_id in runnable:
        entry = STRATEGY_MANIFEST[strategy_id]
        identity = entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
        print(f"starting {rule} {strategy_id}", flush=True)
        if entry.exit_regime(entry.decision_calendar(corpus.axis)).level_based:
            arms = evaluate_level_arms(
                conn,
                entry,
                corpus=corpus,
                quarantine_arm="masked",
                identity=identity,
                namespaces=("hold_out",),
                progress=_progress(rule),
                sizing_rule=rule,
            )
        else:
            arms = (
                evaluate_arm(
                    conn,
                    entry,
                    corpus=corpus,
                    quarantine_arm="masked",
                    ambiguity_arm=None,
                    identity=identity,
                    namespaces=("hold_out",),
                    progress=_progress(rule),
                    sizing_rule=rule,
                ),
            )
        for arm in arms:
            measured[(strategy_id, arm.ambiguity_arm or "not_applicable")] = arm
    return measured


def verify(window_id: str, *, limit: int | None, rules: tuple[str, ...] = _RULES) -> dict[str, object]:
    if len(rules) < 2 or rules[0] != SIZING_RULE_ID or len(set(rules)) != len(rules):
        raise ValueError(f"rules must be unique and start with the production baseline {SIZING_RULE_ID!r}")
    if any(rule not in _RULES for rule in rules):
        raise ValueError(f"unknown sizing rule in {rules!r}")
    started = time.monotonic()
    window = recent_evidence_window(window_id).window
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        corpus = load_corpus(conn, limit=limit, evaluation_window=window)
        by_rule = {rule: _run_rule(conn, corpus=corpus, rule=rule) for rule in rules}

    baseline_keys = set(by_rule[SIZING_RULE_ID])
    if any(set(by_rule[rule]) != baseline_keys for rule in rules[1:]):
        raise RuntimeError("sizing arms did not produce the same strategy/ambiguity keys")

    comparisons: list[dict[str, object]] = []
    for alternative_rule in rules[1:]:
        for key in sorted(baseline_keys):
            production_arm = by_rule[SIZING_RULE_ID][key]
            alternative_arm = by_rule[alternative_rule][key]
            production = production_arm.namespaces.get("hold_out")
            alternative = alternative_arm.namespaces.get("hold_out")
            if production is None or alternative is None:
                raise RuntimeError(f"{key} produced no hold-out measurement")
            if production.position_count != alternative.position_count:
                raise RuntimeError(f"{key} changed position count across sizing arms")
            if production.metrics.trade_count != alternative.metrics.trade_count:
                raise RuntimeError(f"{key} changed realised trade count across sizing arms")
            if production.metrics.buy_and_hold_return_pct != alternative.metrics.buy_and_hold_return_pct:
                raise RuntimeError(f"{key} changed its matched benchmark across sizing arms")

            item: dict[str, object] = {
                "strategy_id": key[0],
                "ambiguity_arm": key[1],
                "baseline_rule": SIZING_RULE_ID,
                "alternative_rule": alternative_rule,
                "position_count": production.position_count,
                "trade_count": production.metrics.trade_count,
                "series_evaluated": production_arm.series_evaluated,
                "baseline_elapsed_seconds": production_arm.elapsed_s,
                "alternative_elapsed_seconds": alternative_arm.elapsed_s,
                "path_diagnostics": {
                    SIZING_RULE_ID: {
                        "rebalance_costs": production.rebalance_costs,
                        "short_funded_entries": production.short_funded_entries,
                        "traded_notional_total": production.traded_notional_total,
                    },
                    alternative_rule: {
                        "rebalance_costs": alternative.rebalance_costs,
                        "short_funded_entries": alternative.short_funded_entries,
                        "traded_notional_total": alternative.traded_notional_total,
                    },
                },
                "metrics": {},
            }
            metric_comparisons: dict[str, object] = {}
            production_values = asdict(production.metrics)
            alternative_values = asdict(alternative.metrics)
            for metric in _METRICS:
                baseline_value = production_values[metric]
                alternative_value = alternative_values[metric]
                metric_comparisons[metric] = {
                    SIZING_RULE_ID: baseline_value,
                    alternative_rule: alternative_value,
                    "delta": alternative_value - baseline_value,
                }
            item["metrics"] = metric_comparisons
            comparisons.append(item)

    return {
        "issue": 2430,
        "window_id": window_id,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "limited_smoke": limit is not None,
        "series": len(corpus.pairs),
        "quarantine_arm": "masked",
        "return_basis": "split-dividend-adjusted-wealth-v1",
        "rules": list(rules),
        "elapsed_seconds": time.monotonic() - started,
        "comparisons": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", default="primary-2022-plus")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--rules", nargs="+", choices=_RULES, default=list(_RULES))
    args = parser.parse_args()
    print(json.dumps(verify(args.window, limit=args.limit, rules=tuple(args.rules)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
