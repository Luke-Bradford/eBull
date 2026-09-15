"""Liveness verdict for an in-flight ``sync_runs`` row (#2274).

Spec: ``docs/proposals/ops/2026-09-15-2274-sync-run-heartbeat.md``.

The sync orchestrator's singleton — ``idx_sync_runs_single_running``, a UNIQUE
partial index on ``((true)) WHERE status = 'running'`` — is a lock expressed as
data: one running row per database, with no owner and no expiry. A stranded row
makes every later ``_start_sync_run`` raise ``SyncAlreadyRunning``, which
``boot_sweep`` logs once and returns from. Until this module there was no
judgement on that row at all: ``GET /sync/status`` reported ``is_running: true``
and a stranded run rendered identically to a healthy one.

⚠⚠ ``live`` means **below the ceiling**, not "a worker is alive". A run whose
worker died 23 hours ago reads ``live``. The only claim this function makes is
that the run has not exceeded its wall-clock ceiling.

⚠⚠ There is deliberately NO heartbeat-staleness verdict, and that is a measured
decision rather than an omission. A ``stalled`` rule needs a threshold that
separates a stranded sync from a legitimately-long one, and today nothing does:

- Only the ``candles`` adapter reaches a loop that calls ``report_progress``
  recurrently. ``refresh_fundamentals`` accepts a ``progress`` callback and does
  not forward it; ``risk_metrics`` / ``price_quarantine`` /
  ``research_price_quarantine`` / ``fair_value_band`` forward it but receive only
  the synthetic install tick.
- So the longest legitimate gap between two run heartbeats is at least the
  longest non-ticking layer, which on the stored corpus is ``fundamentals``.
- The observed stranded runs (``error_category='orchestrator_crash'``) span a
  range that sits entirely inside that. The two populations overlap over their
  whole width, so any threshold fires on real work — which is exactly what
  #2274's own constraint forbids ("a watchdog that fires on legitimately-long
  corpus jobs is worse than none").

The prerequisite for a ``stalled`` rule is tick coverage across more than one
layer, not this module. Re-run the queries in the spec before assuming otherwise;
no derived figure is written into this docstring.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from app.services.processes.stale_detection import RUNTIME_CEILING_S

RunLiveness = Literal["live", "over_ceiling"]

__all__ = ["RUNTIME_CEILING_S", "RunLiveness", "assess_run"]


def assess_run(
    *,
    started_at: datetime,
    last_progress_at: datetime | None,
    now: datetime,
) -> RunLiveness:
    """Classify an in-flight sync run by wall-clock age alone.

    Args:
        started_at: The run's ``sync_runs.started_at``. Timezone-aware.
            ⚠ Defaults to ``now()`` inside the insertion transaction, which
            opens before ``build_execution_plan`` runs — so a long prelude
            publishes a run whose age already includes its own planning.
        last_progress_at: The run heartbeat, or ``None`` before the first layer
            transition. **Accepted and ignored**, by design: rule 5's argument is
            that an age signal a heartbeat can mute is not a backstop. It is in
            the signature so callers pass the whole row and a future
            heartbeat-aware verdict is a change here and nowhere else.
        now: Reference instant, timezone-aware. ⚠ The caller's clock, while
            ``started_at`` is written by Postgres — the two are assumed close.
            Both processes run on one box against one database; a real clock
            split would need a DB-side ``now()`` and is out of scope.

    Returns:
        ``over_ceiling`` when the run is strictly older than
        ``RUNTIME_CEILING_S``, else ``live``. Equality reads ``live``, matching
        ``stale_detection`` rule 5's own boundary.
    """
    if started_at < now - timedelta(seconds=RUNTIME_CEILING_S):
        return "over_ceiling"
    return "live"
