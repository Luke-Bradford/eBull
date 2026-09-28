"""#3471 slice 2a — DB half of the ``demo_trial`` enumeration test (spec §8).

- ``configure_deployment``: the one narrow branch (a paper deployment with a capital limit and
  no promotion stage) is accepted; live authority is refused; disabling stays allowed.
- The standard paper loader refuses a trial leg's signal before any evidence gate.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, cast

import psycopg
import pytest

from app.services.strategy_control_plane import StrategyControlError, configure_deployment, current_stage
from app.services.strategy_paper_executor import execute_fired_paper_signal

_ARM = "ai-discretionary-v1"
_CONTROL = "ai-discretionary-v1-control"


def _configure(conn: psycopg.Connection[Any], strategy_id: str, *, mode: Any, limit: str, enabled: bool) -> Any:
    return configure_deployment(
        conn,
        strategy_id=strategy_id,
        strategy_version="v1",
        mode=mode,
        capital_limit=Decimal(limit),
        enabled=enabled,
        changed_by="operator",
        reason="#3471 test",
    )


@pytest.mark.parametrize("strategy_id", [_ARM, _CONTROL])
def test_paper_deployment_is_the_one_branch_and_needs_no_stage(
    ebull_test_conn: psycopg.Connection[Any], strategy_id: str
) -> None:
    conn = ebull_test_conn
    assert current_stage(conn, strategy_id, "v1") is None
    deployment = _configure(conn, strategy_id, mode="paper", limit="1000", enabled=True)
    assert deployment.deployment_id > 0
    # Risk-reducing changes stay allowed.
    _configure(conn, strategy_id, mode="paper", limit="0", enabled=False)


@pytest.mark.parametrize(("limit", "enabled"), [("100", False), ("0", True), ("100", True)])
def test_live_authority_is_refused(ebull_test_conn: psycopg.Connection[Any], limit: str, enabled: bool) -> None:
    with pytest.raises(StrategyControlError, match="demo-trial strategies cannot receive live capital"):
        _configure(ebull_test_conn, _ARM, mode="live", limit=limit, enabled=enabled)


def test_standard_paper_loader_refuses_a_trial_signal(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) VALUES (3471,'SYN.T','Trial',TRUE)"
    )
    row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,
            signal_kind,verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
        ) VALUES (%s,'v1',3471,DATE '2026-09-25','entry','fired',
                  DATE '2026-09-28',10,'survivor_only','{"test":"v1"}'::jsonb)
        RETURNING signal_id
        """,
        (_ARM,),
    ).fetchone()
    assert row is not None
    conn.commit()
    # The broker is never reached: the purpose refusal comes first.
    result = execute_fired_paper_signal(conn, broker=cast(Any, object()), signal_id=int(row[0]))
    assert (result.verdict, result.reason_code) == ("rejected", "strategy_not_capital_candidate")
