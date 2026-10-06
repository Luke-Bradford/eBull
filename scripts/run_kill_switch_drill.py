"""Run one recorded kill-switch drill by hand (#3614 item 4, trigger ``manual``).

Spec: ``docs/specs/ops/2026-10-06-3614-kill-drill-time-to-flat.md``. With the switch
off, the drill activates it inside a transaction it always rolls back and checks that
every broker-entry chokepoint refuses; with the switch on (an operator's toggle), it
observes the committed state. It never commits a kill-switch change and calls no
broker method. Run from ``~/Dev/eBull`` by an attended session; the build stamp on the
event shows which code ran.

Usage: ``uv run python -m scripts.run_kill_switch_drill --actor <name>``

Exit 0 when the entry verdict is ``passed``; 1 otherwise (including when another run
holds the drill lock, which records nothing). A recording failure raises.
"""

from __future__ import annotations

import argparse
import sys

import psycopg

from app.config import Settings
from app.db.build_stamp import install_build_stamp
from app.services.kill_switch_drill import run_kill_switch_drill


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--actor", required=True, help="who ran this drill (recorded on the event)")
    args = p.parse_args(argv)
    if not args.actor.strip():
        p.error("--actor must not be blank")

    install_build_stamp()
    url = Settings().database_url
    run = run_kill_switch_drill(lambda: psycopg.connect(url, connect_timeout=5), trigger="manual", actor=args.actor)
    if run is None:
        print("another kill drill holds the drill lock; nothing recorded")
        return 1
    print(f"event {run.event_id} mode={run.mode} entry_verdict={run.entry_verdict}")
    for c in run.chokepoints:
        print(
            f"  {c.chokepoint}: {c.outcome} code={c.refusal_code} rules={list(c.failed_rules)} error={c.error_detail}"
        )
    if run.run_failure:
        print(f"  run_failure={run.run_failure}: {run.failure_detail}")
    if run.observe_outcome:
        print(f"  observe_outcome={run.observe_outcome}")
    return 0 if run.entry_verdict == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
