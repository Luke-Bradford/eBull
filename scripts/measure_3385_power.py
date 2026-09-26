"""Print a hunt declaration's power statements and check them against the document (#3385).

Spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md``, "Power, before each validation
and holdout look": reads STORED discovery outcomes only (each pinned candidate's canonical
``active_series``), reads no prices, and is a readout, not a search. For the canonical
cell's t > 3 bar only, it prints the minimum detectable annualised active mean at 50% and
80% power, with the statement of what that is NOT. Exits 1 when the recomputed numbers
differ from the document's at 6 significant figures.

    PYTHONPATH=. uv run python -m scripts.measure_3385_power --declaration hunts/hunt-1-validation.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services import hunt_door
from app.services import hunt_harness as hh


def measure(conn: psycopg.Connection[Any], doc: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """The recomputed power per pinned spec, and whether it equals the document's."""
    with hh.hunt_programme_lock(conn):
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        power = hunt_door.pin_power(conn, doc)
        conn.commit()
    return power, power == hh.decode_form(doc["numbers"]["power"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recompute a hunt declaration's power statements.")
    parser.add_argument("--declaration", type=Path, required=True, help="the declaration document (JSON)")
    args = parser.parse_args(argv)
    path = args.declaration if args.declaration.is_absolute() else hunt_door.REPO_ROOT / args.declaration
    doc = json.loads(path.read_bytes())
    with psycopg.connect(settings.database_url) as conn:
        power, matches = measure(conn, doc)
    print(json.dumps(power, indent=2, sort_keys=True))
    print(f"matches the declaration: {'yes' if matches else 'NO'}")
    return 0 if matches else 1


if __name__ == "__main__":
    raise SystemExit(main())
