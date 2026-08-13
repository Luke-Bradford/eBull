"""Regression: monitor_positions + cusip_extid_sweep + ownership_observations_sync
own single-job lanes (#1527 — daily/hourly continuation of #1526).

All three fire on a 5-minute-aligned slot (monitor_positions hourly @ :15,
ownership_observations_sync daily @ :30, cusip_extid_sweep daily @ :50). On the
catch-all ``db`` lane they co-fired ``orchestrator_high_frequency_sync``
(every_5min, ``db``), which holds ``job_source:db`` re-entrantly through its
portfolio/fx ingest, and lost the cross-thread advisory-lock race every
collision — a once-daily job skips a FULL day per collision (root cause proven
for the infra jobs in #1526 via a ``pg_locks`` tick-poll). Each now owns a
disjoint single-job lane; write-target disjointness from the orchestrator's
ingest was verified before extraction (see app/jobs/sources.py::Lane).

Pure registry assertions (no DB) so the invariant gates every push: if a future
edit reverts any ``source`` to ``db`` — or collapses these onto one shared lane
— this fails in the fast tier. The distinct-source -> concurrent-acquire
property itself is already proven by ``tests/test_db_lane_family_split.py``.
"""

from __future__ import annotations

from app.jobs.sources import get_job_name_to_source, source_for
from app.workers.scheduler import (
    JOB_CUSIP_EXTID_SWEEP,
    JOB_MONITOR_POSITIONS,
    JOB_OWNERSHIP_OBSERVATIONS_SYNC,
    JOB_PG_SIZE_SAMPLE,
)

# (job_name, expected own lane). Job names via the scheduler constants so a
# rename cannot silently pass against a stale literal; the lane values are the
# canonical ``Lane`` literals. Pinned so a future re-merge updates in lockstep.
_STEADY_LANES: tuple[tuple[str, str], ...] = (
    (JOB_MONITOR_POSITIONS, "db_positions"),
    (JOB_CUSIP_EXTID_SWEEP, "db_cusip"),
    (JOB_OWNERSHIP_OBSERVATIONS_SYNC, "db_ownership_obs"),
    (JOB_PG_SIZE_SAMPLE, "db_size_sample"),
)


def test_each_steady_job_resolves_to_its_own_lane() -> None:
    for job_name, expected in _STEADY_LANES:
        assert source_for(job_name) == expected


def test_steady_lanes_disjoint_from_db_catchall_and_each_other() -> None:
    # The starvation was contention with orchestrator_high_frequency_sync on the
    # catch-all ``db`` lane; the orchestrator stays on ``db`` while these jobs
    # leave it, so the cross-thread scheduled fire no longer collides.
    assert source_for("orchestrator_high_frequency_sync") == "db"
    lanes = {job_name: source_for(job_name) for job_name, _ in _STEADY_LANES}
    for lane in lanes.values():
        assert lane != "db"
    # Distinct lanes, not one shared ``db_steady`` — a shared lane would
    # re-create the starvation between members when one overruns (#1526 lesson).
    assert len(set(lanes.values())) == len(lanes)


def test_ownership_backfill_stays_on_db_to_avoid_check_migration() -> None:
    # ownership_observations_backfill is an S24 bootstrap stage, so its lane
    # participates in the bootstrap_stages.lane CHECK; it deliberately stays on
    # ``db``. It is the only *remaining* db-lane writer of ownership_*_current
    # the daily sweep overlaps (other writers — live ingesters/bulk paths — run
    # on db_ownership_* / sec_rate, already off db). The one shared mutation,
    # the refresh_*_current DELETE-then-INSERT, is serialised per-instrument by
    # pg_advisory_xact_lock (the lane is not the guard), so the split is safe.
    assert source_for("ownership_observations_backfill") == "db"


def test_each_steady_lane_owned_by_exactly_one_job() -> None:
    # Catches a future job being assigned one of these lanes, which would
    # re-introduce the same-lane contention the split removes.
    registry = get_job_name_to_source()
    for job_name, lane in _STEADY_LANES:
        holders = {name for name, src in registry.items() if src == lane}
        assert holders == {job_name}, f"lane {lane!r} owned by {sorted(holders)!r}, expected [{job_name!r}]"
