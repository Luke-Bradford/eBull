"""Count what a ``strategy_version`` rotation actually costs, on the full population.

    PYTHONPATH=. uv run python scripts/census_3031_identity_rotation.py

#3031 adds ``price_quarantine`` to ``strategy_registry.INPUT_RULE_SETS``, which
rotates every strategy identity. The question a reviewer has to answer is what is
LOST by that, and the only honest form of the answer is a count per version-keyed
table of rows sitting on an identity the manifest still produces.

⚠⚠ THE CURRENT VERSIONS ARE RECOMPUTED, NEVER HARD-CODED. A census that filters
stored rows by a version constant imported from the module under test stops
matching the moment that module is edited, and the empty baseline prints 0 —
which is exactly what a healthy result looks like. So the identities come from
``STRATEGY_MANIFEST`` at run time and the script prints them, and the stored rows
are grouped by their own ``strategy_version`` and compared afterwards in Python.

⚠ Read-only. No writes, no broker call, no job dispatch.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, LiteralString

import psycopg

from app.config import settings
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_signal_scan import COST_MODEL_ID, SCAN_UNIVERSE

#: Every table whose rows are addressed by ``(strategy_id, strategy_version)``
#: and whose contents a rotation therefore detaches from the current identity.
#: ⚠ ``strategy_signals`` is counted on ``verdict = 'fired'`` only: a
#: ``not_fired`` row records a decision, not an open economic position, and it is
#: the pending FILLS that a rotation can strand.
_COUNTED: tuple[tuple[str, LiteralString], ...] = (
    (
        "strategy_signals",
        "SELECT strategy_id, strategy_version, count(*) FROM strategy_signals WHERE verdict = 'fired' GROUP BY 1, 2",
    ),
    (
        "strategy_results_store",
        "SELECT strategy_id, strategy_version, count(*) FROM strategy_results_store GROUP BY 1, 2",
    ),
    (
        "strategy_preregistration_declarations",
        "SELECT strategy_id, strategy_version, count(*) FROM strategy_preregistration_declarations GROUP BY 1, 2",
    ),
    (
        "strategy_holdout_accesses",
        "SELECT strategy_id, strategy_version, count(*) FROM strategy_holdout_accesses GROUP BY 1, 2",
    ),
    ("strategy_deployments", "SELECT strategy_id, strategy_version, count(*) FROM strategy_deployments GROUP BY 1, 2"),
    ("strategy_promotions", "SELECT strategy_id, strategy_version, count(*) FROM strategy_promotions GROUP BY 1, 2"),
    (
        "strategy_scan_watermark",
        "SELECT strategy_id, strategy_version, count(*) FROM strategy_scan_watermark GROUP BY 1, 2",
    ),
)

#: Fired signals with no ``strategy_outcomes`` row — the rows
#: ``run_outcome_resolution`` can still reach only while their version is current.
_UNRESOLVED: LiteralString = """
    SELECT s.strategy_id, s.strategy_version, count(*)
      FROM strategy_signals s
      LEFT JOIN strategy_outcomes o ON o.signal_id = s.signal_id
     WHERE s.verdict = 'fired' AND o.signal_id IS NULL
     GROUP BY 1, 2
"""


def _current_versions() -> dict[str, str]:
    """``{strategy_id: version}`` as the manifest produces it in THIS process."""
    return {
        strategy_id: entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id, entry in sorted(STRATEGY_MANIFEST.items())
    }


def _split(rows: list[tuple[str, str, int]], current: dict[str, str]) -> tuple[int, int]:
    """``(on a current identity, total)``."""
    on_current = sum(count for strategy_id, version, count in rows if current.get(strategy_id) == version)
    return on_current, sum(count for *_, count in rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit the census as JSON instead of a table")
    args = parser.parse_args()

    current = _current_versions()
    report: dict[str, Any] = {"current_versions": current, "tables": {}}

    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            for table, sql in _COUNTED:
                cur.execute(sql)
                on_current, total = _split(list(cur.fetchall()), current)
                report["tables"][table] = {"rows": total, "on_current_identity": on_current}
            cur.execute(_UNRESOLVED)
            on_current, total = _split(list(cur.fetchall()), current)
            report["tables"]["strategy_signals (fired, unresolved)"] = {
                "rows": total,
                "on_current_identity": on_current,
            }

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    print("current identities (recomputed, not stored):")
    for strategy_id, version in current.items():
        print(f"  {strategy_id:40} {version}")
    print()
    print(f"{'table':44} {'rows':>10} {'on current identity':>21}")
    for table, counts in report["tables"].items():
        print(f"{table:44} {counts['rows']:>10} {counts['on_current_identity']:>21}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
