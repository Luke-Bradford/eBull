"""First hold-out measurement of ALL TEN strategies at their CURRENT versions.

⚠ WHY THIS EXISTS. Every stored result sat at a SUPERSEDED ``strategy_version`` —
S-5/S-6 moved on the #2780 perf work, S-2/S-10 again on #2797 — so nothing
measured the strategies as they are now. Every claim about whether they work was
inference from stale or in-sample numbers.

⚠⚠ ALL TEN IN ONE INVOCATION, AND THAT IS NOT A CONVENIENCE. §2 puts the Deflated
Sharpe across the whole strategy set, so ``V[SR_n]`` is a function of WHICH trials
were measured. Running nine would deflate them against a narrower variance — a
more confident number bought by losing evidence, which ``run_backtest``'s own
docstring names as the failure to avoid.

⚠ HOLD-OUT ONLY, by construction rather than by choice: ``_resolve_invocation_window``
returns ``("hold_out",)`` whenever an ``evidence_window_id`` is supplied. That also
sidesteps the s1 in-sample identity collision without deleting anything — the
result-ambiguity trigger refuses deletion outright ("records are immutable; create
a new result identity") and it is right to.

⚠ ``synthetic_control=False`` DELIBERATELY. §9's control is 1,000 equity curves PER
ARM on top of the corpus pass; paying that for ten strategies before knowing which
have any edge is the waste. Controls go on whatever clears the metric bar, which is
staging, not rework.
"""

from __future__ import annotations

import sys
import time

import psycopg

from app.config import settings
from app.services.backtest_run import BACKTEST_UNIVERSE, run_backtest

WINDOW = "primary-2022-plus"
PURPOSE = (
    "decisive first hold-out measurement of all ten strategies at current versions; "
    "operator-directed 2026-08-21 to establish whether any candidate has an edge"
)
ACCESSED_BY = "agent, operator-directed"


def main() -> int:
    started = time.monotonic()
    last = [started]

    def progress(event: object) -> None:
        now = time.monotonic()
        if now - last[0] < 60:
            return
        last[0] = now
        print(f"[{now - started:7.0f}s] {event}", flush=True)

    with psycopg.connect(settings.database_url) as conn:
        report = run_backtest(
            conn,
            universe=BACKTEST_UNIVERSE,
            holdout_purpose=PURPOSE,
            holdout_accessed_by=ACCESSED_BY,
            evidence_window_id=WINDOW,
            synthetic_control=False,
            release_read_locks=True,
            progress=progress,
        )
        conn.commit()

    print(f"\nrows written: {report.rows_written}", flush=True)
    for arm in report.arms:
        print(
            f"  {arm.strategy_id:38} {arm.ambiguity_arm or 'shared':10} {arm.quarantine_arm:9} "
            f"series={arm.series_evaluated} {arm.elapsed_s:.1f}s",
            flush=True,
        )
    print(f"\ntotal {time.monotonic() - started:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
