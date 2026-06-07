"""Regression: jobs_liveness_watchdog + jobs_retry_sweeper own single-job lanes (#1526).

Both infra jobs were on the catch-all ``db`` lane, where they lost the
``job_source:db`` advisory-lock race to ``orchestrator_high_frequency_sync``
(every_5min, ``db``) at every shared tick and never ran on schedule — the
#1508 self-healing infra was itself starved (root cause proven via a
``pg_locks`` tick-poll + jobs-log correlation, 2026-06-07). Each now owns a
disjoint single-job lane (``db_liveness`` / ``db_retry``).

Pure registry assertions (no DB) so the invariant gates every push: if a
future edit reverts either ``source`` to ``db`` — or collapses both onto one
shared lane — this fails in the fast tier. The distinct-source -> concurrent-
acquire property itself is already proven by
``tests/test_db_lane_family_split.py``.
"""

from __future__ import annotations

from app.jobs.sources import get_job_name_to_source, source_for
from app.workers.scheduler import JOB_LIVENESS_WATCHDOG, JOB_RETRY_SWEEPER

# (job_name, expected own lane). Job names via the scheduler constants so a
# rename cannot silently pass against a stale literal (review NITPICK PR #1528);
# the lane values are the canonical ``Lane`` literals. Pinned so a future
# re-merge updates in lockstep.
_INFRA_LANES: tuple[tuple[str, str], ...] = (
    (JOB_LIVENESS_WATCHDOG, "db_liveness"),
    (JOB_RETRY_SWEEPER, "db_retry"),
)


def test_each_infra_job_resolves_to_its_own_lane() -> None:
    for job_name, expected in _INFRA_LANES:
        assert source_for(job_name) == expected


def test_infra_lanes_disjoint_from_db_catchall_and_each_other() -> None:
    # The starvation was contention with orchestrator_high_frequency_sync on the
    # catch-all ``db`` lane; the orchestrator stays on ``db`` while the infra
    # jobs leave it, so the cross-thread scheduled fire no longer collides.
    assert source_for("orchestrator_high_frequency_sync") == "db"
    liveness = source_for("jobs_liveness_watchdog")
    retry = source_for("jobs_retry_sweeper")
    assert liveness != "db"
    assert retry != "db"
    # SEPARATE lanes, not one shared ``db_infra`` — a shared lane would
    # re-create the starvation between the 15-min watchdog and the 5-min
    # sweeper at the :00/:15/:30/:45 ticks they co-fire.
    assert liveness != retry


def test_each_infra_lane_owned_by_exactly_one_job() -> None:
    # Catches a future job being assigned db_liveness/db_retry, which would
    # re-introduce the same-lane contention the split removes.
    registry = get_job_name_to_source()
    for job_name, lane in _INFRA_LANES:
        holders = {name for name, src in registry.items() if src == lane}
        assert holders == {job_name}, f"lane {lane!r} owned by {sorted(holders)!r}, expected [{job_name!r}]"
