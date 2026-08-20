#!/usr/bin/env python3
"""Run #2772's bounded, outcome-free PostgreSQL read/decode canary.

Discover the current fixed selection without reading bar rows::

    uv run python -m scripts.verify_2772_research_read_canary --plan

Then bind the measured run to that selection::

    uv run python -m scripts.verify_2772_research_read_canary \
      --expected-selection-digest DIGEST --output /tmp/ebull-2772-read-canary.json

The connection and transaction are read-only. The canary always stops after
five series and never evaluates a strategy or emits an outcome.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.research_price_read_canary import (
    ReadDecodeCanaryConfig,
    plan_read_decode_canary,
    run_read_decode_canary,
)


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    raise TypeError(f"cannot render {type(value).__name__}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", action="store_true", help="select and hash strata without reading bar rows")
    parser.add_argument("--expected-selection-digest")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not args.plan and not args.expected_selection_digest:
        parser.error("a measured run requires --expected-selection-digest from a preceding --plan")

    config = ReadDecodeCanaryConfig(expected_selection_digest=args.expected_selection_digest)
    with psycopg.connect(
        settings.database_url,
        options="-c statement_timeout=10000 -c default_transaction_read_only=on",
    ) as conn:
        value = (
            {"mode": "plan", "bar_queries": 0, "plan": asdict(plan_read_decode_canary(conn, config=config))}
            if args.plan
            else {
                "mode": "measured",
                "outcome_fields_emitted": False,
                "report": asdict(run_read_decode_canary(conn, config=config)),
            }
        )
    rendered = json.dumps(value, indent=2, sort_keys=True, default=_json_default)
    if args.output is None:
        print(rendered)
    else:
        args.output.write_text(rendered + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
