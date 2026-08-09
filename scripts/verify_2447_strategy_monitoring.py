"""Reproduce #2447's read-only monitoring census and query plans.

Run with::

    PYTHONPATH=. uv run python scripts/verify_2447_strategy_monitoring.py

No writes.  The dev guard prevents an accidental plan run against a remote DB.
"""

from __future__ import annotations

from typing import Any, LiteralString, cast

import psycopg
import psycopg.sql

from app.api.strategies import (
    _FIRED_SIGNALS_SQL,
    _RESULT_COUNTS_SQL,
    _RESULTS_SQL,
    _SCAN_SQL,
    _current_versions,
    get_fired_signals,
    get_strategy_overview,
)
from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_result import CORPUS_VERSION
from scripts._dev_guard import assert_dev_environment

_PLAN_QUERIES = {
    "latest_fired_page": _FIRED_SIGNALS_SQL,
    "cursor_fired_page": _FIRED_SIGNALS_SQL,
    "scan_aggregate": _SCAN_SQL,
    "result_ledger": _RESULTS_SQL,
    "result_counts": _RESULT_COUNTS_SQL,
}


def _plan(conn: psycopg.Connection[Any], query: str, params: dict[str, object]) -> dict[str, object]:
    # Every caller supplies a module-owned SQL constant from ``_PLAN_QUERIES``;
    # no request or operator value is ever promoted into the statement text.
    statement = psycopg.sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}").format(
        psycopg.sql.SQL(cast(LiteralString, query))
    )
    row = conn.execute(statement, params).fetchone()
    if row is None:
        raise RuntimeError("EXPLAIN returned no row")
    payload = row[0][0]
    plan = payload["Plan"]
    return {
        "execution_ms": payload["Execution Time"],
        "planning_ms": payload["Planning Time"],
        "root_node": plan["Node Type"],
        "rows": plan["Actual Rows"],
        "shared_hit_blocks": plan.get("Shared Hit Blocks", 0),
        "shared_read_blocks": plan.get("Shared Read Blocks", 0),
    }


def main() -> None:
    assert_dev_environment()
    versions = list(_current_versions().values())
    with psycopg.connect(settings.database_url) as conn:
        overview = get_strategy_overview(conn)
        signals = get_fired_signals(cursor=None, limit=50, conn=conn)
        params: dict[str, object] = {
            "versions": versions,
            "cursor": None,
            "limit": 50,
            "outcome_version": OUTCOME_RULE_SET_VERSION,
            "input_version": QUARANTINE_RULE_SET_VERSION,
            "corpus_version": CORPUS_VERSION,
            "cost_model_id": COST_MODEL_ID,
            "sizing_rule": SIZING_RULE_ID,
            "benchmark_rule": BENCHMARK_RULE_ID,
            "position_version": POSITION_RULE_SET_VERSION,
        }
        print(
            {
                "strategies": [
                    {
                        "strategy_id": item.strategy_id,
                        "runnable": item.runnable,
                        "legacy_rows": item.legacy_result_count,
                        "recent_complete": item.all_recent_evidence_complete,
                        "fired_entries": item.scan.fired_entries,
                    }
                    for item in overview.strategies
                ],
                "first_page_rows": len(signals.items),
                "next_cursor": signals.next_cursor,
            }
        )
        for name, query in _PLAN_QUERIES.items():
            plan_params = dict(params)
            if name == "cursor_fired_page":
                plan_params["cursor"] = signals.next_cursor or 1
            print(name, _plan(conn, query, plan_params))


if __name__ == "__main__":
    main()
