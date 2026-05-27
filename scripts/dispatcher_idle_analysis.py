#!/usr/bin/env python3
"""Aggregate Phase 0.5 dispatcher residual-idle JSONL telemetry.

Spec: docs/proposals/etl/phase-0-instrumentation.md §2.9.2.

Reads one ``var/dispatcher_idle/<run_id>.jsonl`` file produced by
``_emit_dispatcher_telemetry`` and emits per-lane aggregates to stdout
as JSON for the operator's decision-rule evaluation (spec §2.9.3).

Per-lane aggregates:

* ``busy_iter`` — iterations where ``in_flight > 0`` for the lane
  (denominator for the ``idle_b_iter > 10% busy_iter`` rule).
* ``idle_a_iter`` — iterations classified ``"A"`` (dependency-natural).
* ``idle_b_iter`` — iterations classified ``"B"`` (actionable bug).
* ``idle_b_max_run_iters`` — longest consecutive run of ``"B"``-classified
  iterations (sustained idle is the bug shape, not isolated blips).
* ``idle_b_stages_seen`` — union of ``pending_ready`` stage names across
  all ``"B"``-classified iterations (which stages were unjustly idle).

CLI:
    uv run python scripts/dispatcher_idle_analysis.py var/dispatcher_idle/17.jsonl
    cat var/dispatcher_idle/17.jsonl | uv run python scripts/dispatcher_idle_analysis.py -

Exit codes:
    0 — parse + emit succeeded.
    2 — bad input (missing file, malformed line).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


class _BadInput(Exception):
    """Bad-input marker; mapped to exit code 2 by ``main``.

    ``SystemExit(str)`` would exit with code 1; this wrapper lets the
    CLI surface the message on stderr and exit with the documented
    code 2 instead. Codex 2 fold.
    """


def _iter_records(source: Iterable[str]) -> Iterator[dict[str, Any]]:
    for lineno, raw in enumerate(source, start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            raise _BadInput(f"line {lineno}: malformed JSONL ({exc})") from exc


def aggregate(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compute per-lane aggregates from a stream of telemetry records.

    Records are consumed in order so consecutive-run detection works.
    Lanes absent from a given record contribute nothing for that
    iteration (their consecutive-B run does NOT reset — the lane was
    not even mentioned, treat as "no observation").
    """
    busy: dict[str, int] = {}
    idle_a: dict[str, int] = {}
    idle_b: dict[str, int] = {}
    cur_b_run: dict[str, int] = {}
    max_b_run: dict[str, int] = {}
    b_stages: dict[str, set[str]] = {}
    total_iterations = 0
    run_id: int | None = None

    for record in records:
        total_iterations += 1
        if run_id is None:
            run_id = record.get("run_id")
        lanes = record.get("lanes") or {}
        for lane, info in lanes.items():
            in_flight = int(info.get("in_flight", 0))
            idle_type = info.get("idle_type")
            if in_flight > 0:
                busy[lane] = busy.get(lane, 0) + 1
                cur_b_run[lane] = 0
            elif idle_type == "A":
                idle_a[lane] = idle_a.get(lane, 0) + 1
                cur_b_run[lane] = 0
            elif idle_type == "B":
                idle_b[lane] = idle_b.get(lane, 0) + 1
                cur_b_run[lane] = cur_b_run.get(lane, 0) + 1
                max_b_run[lane] = max(max_b_run.get(lane, 0), cur_b_run[lane])
                for stage_key in info.get("pending_ready") or []:
                    b_stages.setdefault(lane, set()).add(str(stage_key))
            else:
                # idle_type is None and in_flight==0 → drained / completion
                # iteration. Does not count toward any bucket; consecutive
                # B-run resets so a later isolated B blip is not chained
                # to an earlier one across a drained gap.
                cur_b_run[lane] = 0

    all_lanes = sorted(set(busy) | set(idle_a) | set(idle_b) | set(max_b_run) | set(b_stages))
    return {
        "run_id": run_id,
        "total_iterations": total_iterations,
        "lanes": {
            lane: {
                "busy_iter": busy.get(lane, 0),
                "idle_a_iter": idle_a.get(lane, 0),
                "idle_b_iter": idle_b.get(lane, 0),
                "idle_b_max_run_iters": max_b_run.get(lane, 0),
                "idle_b_stages_seen": sorted(b_stages.get(lane, set())),
            }
            for lane in all_lanes
        },
    }


def _open_source(path: str) -> Iterable[str]:
    if path == "-":
        return sys.stdin
    p = Path(path)
    if not p.is_file():
        raise _BadInput(f"input not a file: {path}")
    return p.read_text().splitlines()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument(
        "input",
        help="Path to <run_id>.jsonl, or '-' to read JSONL from stdin.",
    )
    args = parser.parse_args(argv)
    try:
        source = _open_source(args.input)
        result = aggregate(_iter_records(source))
    except _BadInput as exc:
        print(str(exc), file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
