"""One-shot stdout runner for the sealed #2582 historical falsification."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from app.config import settings
from scripts.evaluate_2582_schedule13d_outcomes import ACKNOWLEDGEMENT, require_outcome_gate
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
    gate = require_outcome_gate(acknowledgement=args.acknowledgement, contract_path=args.contract)
    with psycopg.connect(settings.database_url) as conn:
        report = evaluate_historical_falsification(conn, gate)
    sys.stdout.write(build_artifact(gate, report).to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ACKNOWLEDGEMENT", "main"]
