"""One-shot stdout runner for the sealed #2582 historical falsification."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from app.config import settings
from scripts.evaluate_2582_schedule13d_outcomes import (
    ACKNOWLEDGEMENT,
    require_outcome_gate,
    require_outcome_gate_preconditions,
)
from scripts.schedule13d_artifact import build_artifact
from scripts.schedule13d_orchestrator import evaluate_historical_falsification


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--acknowledgement")
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
    )
    args = parser.parse_args(argv)
    # ⚠ Before the connection, deliberately: a wrong acknowledgement or a moved
    # contract digest is refused without touching the database at all.
    require_outcome_gate_preconditions(acknowledgement=args.acknowledgement, contract_path=args.contract)
    with psycopg.connect(settings.database_url) as conn:
        gate = require_outcome_gate(conn, acknowledgement=args.acknowledgement, contract_path=args.contract)
        # ⚠⚠ #2614 — THE COMMIT IS LOAD-BEARING TWICE OVER, AND ON ONE
        # CONNECTION SO THE COMMITTED ACCESS ROW IS TRIVIALLY VISIBLE TO THE
        # RE-CHECK THAT FOLLOWS.
        #
        # 1. `require_outcome_access` wrote the access row in THIS transaction
        #    and did not commit it. Rolling it back with a failed evaluation
        #    would make the look unlogged — the exact failure #2599 exists to
        #    prevent, in miniature.
        # 2. `evaluate_historical_falsification` opens with
        #    `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`, which
        #    is only valid as the FIRST statement of a transaction. Without this
        #    commit it fails outright.
        conn.commit()
        report = evaluate_historical_falsification(conn, gate)
    sys.stdout.write(build_artifact(gate, report).to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ACKNOWLEDGEMENT", "main"]
