#!/usr/bin/env python
"""Probe the dev API for a wedge, and capture a thread dump if it is wedged (#3119).

One bounded invocation. No DB. The logic lives in
``app/system/api_wedge_probe.py``, shared with the periodic ``api_wedge_probe``
job; this is the manual CLI. Run it before trusting a dev-verify result: on
2026-09-16 the API served stale code for ~10 hours with every deploy check
looking clean.

Exit status: 0 only when both probes answered AND the served tree is fresh.
Non-zero on a hang, on staleness, and on unknown.

Usage::

    PYTHONPATH=. uv run python scripts/probe_api_wedge.py
    PYTHONPATH=. uv run python scripts/probe_api_wedge.py --base-url http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.system.api_wedge_probe import DEFAULT_BASE_URL, dump_threads, observe  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--no-dump",
        action="store_true",
        help="report only; do not send SIGUSR1 even if a probe hangs",
    )
    args = parser.parse_args()

    obs = observe(args.base_url)
    live, health, sidecar = obs.live, obs.health, obs.sidecar

    for result in (live, health):
        state = f"HTTP {result.status}" if result.answered else f"NO RESPONSE ({result.error})"
        print(f"{result.path:<14} {state}  {result.elapsed_s:.3f}s")

    print(f"{'app_tree':<14} {obs.verdict}  served={obs.served} ondisk={obs.ondisk}")
    if sidecar:
        worker = f"pid={sidecar.get('pid')} started_at={sidecar.get('started_at')} dirty={sidecar.get('dirty')}"
        print(f"{'worker':<14} {worker}")

    if not live.answered and not health.answered:
        print("reading        loop blocked, OR no live worker (the reload parent's backlog still accepts)")
    elif live.answered and not health.answered:
        print("reading        loop alive, sync path not completing")
    elif not live.answered and health.answered:
        print("reading        probes straddled a restart, or an intermittent stall — re-probe")

    if obs.predates_instrumentation:
        print("reading        served build predates #3119 (no /health/live route) — older than this checkout")

    if obs.wedged and not args.no_dump:
        print(f"{'dump':<14} {dump_threads(sidecar)}")

    return obs.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
