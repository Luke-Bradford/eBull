"""#2949 — the engine process the harness kills.

Run as ``python -m tests.fixtures.core_restart_child <config.json>``.  It is a
REAL separate process, not a thread or a context manager, because the fault under
test is a process boundary: an in-process double would let Python unwind the
stack, psycopg close the connection cleanly and the interpreter run ``finally``
blocks, none of which a crashed engine gets to do.

It writes its outcome to ``config["result_path"]`` only on the paths that survive.
A missing result file IS the signal that the fault fired.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import psycopg

from tests.fixtures.core_restart import (
    API_CREDENTIAL_ID,
    CLOCK,
    OPERATOR_ID,
    USER_CREDENTIAL_ID,
    FileBackedFakeBroker,
    _kill_self,
    select_core_instrument,
)


def _install_fault(fault: str) -> None:
    """Arm the fault the broker double cannot arm itself.

    ``before_authority_commit`` fires from INSIDE the executor's durable
    transaction (``strategy_core_executor.py:460-588``), after the intent, trade
    and order rows have been written and after they have been linked, and before
    the transaction commits.

    ⚠ It WRAPS ``link_strategy_order`` rather than replacing it, and the
    difference is not cosmetic: replacing it would skip the link itself, so the
    fault would land on a shorter prefix of the transaction than the name claims.
    Wrapping puts the kill as late as this module can reach by name — the only
    statement still outstanding is the reconciliation-state INSERT two lines
    later, which is inside the same transaction and therefore rolls back with it.
    """
    if fault != "before_authority_commit":
        return
    from app.services import strategy_core_executor

    real_link = strategy_core_executor.link_strategy_order

    def _link_then_die(*args: Any, **kwargs: Any) -> None:
        real_link(*args, **kwargs)
        _kill_self()

    strategy_core_executor.link_strategy_order = _link_then_die  # type: ignore[assignment]


def main(argv: list[str]) -> int:
    config: dict[str, Any] = json.loads(Path(argv[1]).read_text())
    select_core_instrument()
    _install_fault(config["fault"])

    from app.services.strategy_core_executor import execute_core_rebalance

    broker = FileBackedFakeBroker(
        config["broker_state_path"],
        fault=config["fault"],
        fill_status=config.get("fill_status", "Filled"),
    )
    with psycopg.connect(config["database_url"]) as conn:
        result = execute_core_rebalance(
            conn,
            broker=broker,  # type: ignore[arg-type]
            operator_id=OPERATOR_ID,
            api_key_credential_id=API_CREDENTIAL_ID,
            user_key_credential_id=USER_CREDENTIAL_ID,
            recorded_by="core-restart-harness",
            clock=lambda: CLOCK,
        )
    Path(config["result_path"]).write_text(
        json.dumps(
            {
                "state": result.state,
                "reason_code": result.reason_code,
                "intent_id": result.intent_id,
                "trade_id": result.trade_id,
                "order_id": result.order_id,
                "amount": str(result.amount),
            }
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    sys.exit(main(sys.argv))
