"""Full-population, measured A/B for #2429's return-accounting correction.

The raw arm is not reconstructed: it is the immutable result row previously
written by the production engine and labelled by migration 326. The corrected
arm runs the current production writer inside this connection, reads its exact
rows, compares like-for-like identities, then rolls the transaction back.
No evidence row or access record survives this verifier.
"""

from __future__ import annotations

import argparse
import json
import time
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.backtest_run import run_backtest
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_recent_evidence import recent_evidence_window
from app.services.strategy_result import LEGACY_RETURN_BASIS, TOTAL_RETURN_BASIS

_METRICS = (
    "total_return_pct",
    "buy_and_hold_return_pct",
    "return_vs_buy_and_hold_pct",
    "cagr_pct",
    "sharpe",
    "max_drawdown_pct",
    "expectancy_per_trade_pct",
)


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def verify(window_id: str) -> dict[str, object]:
    window = recent_evidence_window(window_id).window
    started = time.monotonic()
    with psycopg.connect(settings.database_url) as conn:
        validated = load_validated_universe(conn)
        with conn.cursor(row_factory=dict_row) as cursor:
            coverage = cursor.execute(
                """
            SELECT count(*) AS bars,
                   count(*) FILTER (WHERE adj_close IS NULL) AS missing_adj_close,
                   count(DISTINCT series_id) AS series
              FROM research_price_daily
             WHERE bar_date BETWEEN %(start)s AND %(end)s
               AND series_id IN (
                   SELECT s.series_id
                     FROM research_price_series s
                    WHERE s.instrument_id = ANY(%(ids)s)
               )
                """,
                {"start": window.start, "end": window.end, "ids": list(validated)},
            ).fetchone()
        if coverage is None:  # pragma: no cover - aggregate always returns one row
            raise RuntimeError("coverage query returned no row")

        report = run_backtest(
            conn,
            holdout_purpose="issue #2429 total-return A/B; transaction rolled back",
            holdout_accessed_by="scripts/verify_2429_total_return.py",
            evaluation_window=window,
        )
        new_versions = [row.result_version for row in report.rows]
        with conn.cursor(row_factory=dict_row) as cursor:
            pairs = cursor.execute(
                """
            SELECT n.strategy_id, n.ambiguity_arm, n.quarantine_arm,
                   n.trade_count, n.unpriced_trade_count,
                   o.result_version AS raw_result_version,
                   n.result_version AS total_result_version,
                   o.total_return_pct AS raw_total_return_pct,
                   n.total_return_pct AS total_total_return_pct,
                   o.buy_and_hold_return_pct AS raw_buy_and_hold_return_pct,
                   n.buy_and_hold_return_pct AS total_buy_and_hold_return_pct,
                   o.return_vs_buy_and_hold_pct AS raw_return_vs_buy_and_hold_pct,
                   n.return_vs_buy_and_hold_pct AS total_return_vs_buy_and_hold_pct,
                   o.cagr_pct AS raw_cagr_pct, n.cagr_pct AS total_cagr_pct,
                   o.sharpe AS raw_sharpe, n.sharpe AS total_sharpe,
                   o.max_drawdown_pct AS raw_max_drawdown_pct,
                   n.max_drawdown_pct AS total_max_drawdown_pct,
                   o.expectancy_per_trade_pct AS raw_expectancy_per_trade_pct,
                   n.expectancy_per_trade_pct AS total_expectancy_per_trade_pct
              FROM strategy_results_store n
              JOIN strategy_results_store o
                ON o.strategy_id = n.strategy_id
               AND o.strategy_version = n.strategy_version
               AND o.result_scope = n.result_scope
               AND o.namespace = n.namespace
               AND o.ambiguity_arm = n.ambiguity_arm
               AND o.quarantine_arm = n.quarantine_arm
               AND o.window_start = n.window_start
               AND o.window_end = n.window_end
               AND o.corpus_version = n.corpus_version
               AND o.cost_model_id = n.cost_model_id
               AND o.sizing_rule = n.sizing_rule
               AND o.benchmark_rule = n.benchmark_rule
               AND o.position_rule_set_version = n.position_rule_set_version
               AND o.outcome_rule_set_version = n.outcome_rule_set_version
               AND o.input_rule_set_version = n.input_rule_set_version
               AND o.return_basis = %(legacy)s
             WHERE n.result_version = ANY(%(versions)s)
               AND n.return_basis = %(total)s
             ORDER BY n.strategy_id, n.ambiguity_arm, n.quarantine_arm
                """,
                {"versions": new_versions, "legacy": LEGACY_RETURN_BASIS, "total": TOTAL_RETURN_BASIS},
            ).fetchall()
        if len(pairs) != report.rows_written:
            raise RuntimeError(
                f"corrected run built {report.rows_written} rows but only {len(pairs)} have one exact stored raw arm"
            )

        comparisons: list[dict[str, Any]] = []
        for row in pairs:
            item: dict[str, Any] = {
                "strategy_id": row["strategy_id"],
                "ambiguity_arm": row["ambiguity_arm"],
                "quarantine_arm": row["quarantine_arm"],
                "trade_count": row["trade_count"],
                "unpriced_trade_count": row["unpriced_trade_count"],
                "raw_result_version": row["raw_result_version"],
                "total_result_version": row["total_result_version"],
            }
            for metric in _METRICS:
                raw = _number(row[f"raw_{metric}"])
                total = _number(row[f"total_{metric}"])
                item[metric] = {
                    "raw": raw,
                    "total": total,
                    "delta": None if raw is None or total is None else total - raw,
                }
            comparisons.append(item)

        conn.rollback()

    return {
        "window_id": window_id,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "series": int(coverage["series"]),
        "bars": int(coverage["bars"]),
        "missing_adj_close": int(coverage["missing_adj_close"]),
        "rows_compared": len(comparisons),
        "rollback_preserved_corrected_rows": True,
        "elapsed_seconds": time.monotonic() - started,
        "comparisons": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", default="primary-2022-plus")
    args = parser.parse_args()
    print(json.dumps(verify(args.window), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
