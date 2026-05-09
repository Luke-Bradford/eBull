"""Tests for the per-process mid_flight_stuck threshold registry.

Issue #1083 (umbrella #1064) — admin control hub PR8.

The registry is a tiny dict keyed by process_id; the test pinning is
intentional: a typo in an override key would silently leave a
long-running ingester on the 5-min default and operators would see
spurious mid_flight_stuck chips on every snapshot. Cross-checking the
override keys against the live ScheduledJobs registry catches that
class of bug at import-time.
"""

from __future__ import annotations

from app.services.processes.stale_thresholds import (
    DEFAULT_THRESHOLD_S,
    get_threshold,
    overridden_process_ids,
)


def test_default_threshold_is_300_seconds() -> None:
    """Operator-amendment §A3: default is 5 min. The default constant
    is the source of truth — adapters multiply nothing else against
    it.
    """
    assert DEFAULT_THRESHOLD_S == 300


def test_unknown_process_id_falls_back_to_default() -> None:
    assert get_threshold("totally_made_up_job") == DEFAULT_THRESHOLD_S


def test_bootstrap_override_is_30_minutes() -> None:
    """Bootstrap is the slowest-tick mechanism — 17 stages with
    multi-GB SEC archives. A 5-min default would surface
    mid_flight_stuck on every bootstrap.
    """
    assert get_threshold("bootstrap") == 1800


def test_sec_bulk_jobs_overridden() -> None:
    """SEC bulk-download / archive-driven jobs all share the 30-min
    threshold.
    """
    for process_id in (
        "sec_filing_documents_ingest",
        "sec_13f_quarterly_sweep",
        "sec_n_port_ingest",
        "sec_def14a_bootstrap",
        "sec_business_summary_bootstrap",
        "ownership_observations_backfill",
        "sec_insider_transactions_backfill",
    ):
        assert get_threshold(process_id) == 1800, f"override missing for {process_id!r}"


def test_override_keys_resolve_to_real_process_ids() -> None:
    """Codex pre-impl plan-review WARNING: a typo in an override key
    would silently leave a long-running ingester on the 5-min default.
    Cross-check every override key against the live registry — bootstrap
    is special-cased; all others must match a ``ScheduledJob.name``.
    """
    from app.workers.scheduler import SCHEDULED_JOBS

    valid_job_names = {job.name for job in SCHEDULED_JOBS}
    for process_id in overridden_process_ids():
        if process_id == "bootstrap":
            continue
        assert process_id in valid_job_names, f"override key {process_id!r} does not resolve to a real ScheduledJob"
