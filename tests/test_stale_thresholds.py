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
    RUNTIME_CEILING_S,
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
        # The 13F / N-PORT stale-detection surfaces are the ingest-sweep
        # ProcessRows, not the retired/internal job names (#1445).
        "sec_13f_sweep",
        "nport_sweep",
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
    from app.services.processes.ingest_sweep_adapter import sweep_process_ids
    from app.workers.scheduler import SCHEDULED_JOBS

    # An override key must resolve to a real stale-detection surface — a
    # ProcessRow.process_id that ops-monitor actually keys ``get_threshold``
    # by. That surface is scheduled jobs ∪ ingest sweeps ∪ the bootstrap row,
    # NOT the raw _INVOKERS registry (which includes retired/internal job
    # names that surface no ProcessRow — keying an override to one of those
    # silently no-ops, #1445).
    valid_process_ids = {job.name for job in SCHEDULED_JOBS} | set(sweep_process_ids())
    for process_id in overridden_process_ids():
        if process_id == "bootstrap":
            continue
        assert process_id in valid_process_ids, (
            f"override key {process_id!r} does not resolve to a real "
            "stale-detection ProcessRow (scheduled job or ingest sweep)"
        )


def test_full_sync_override_is_the_runtime_ceiling() -> None:
    """#2274 — the full sync's active row comes from ``sync_runs``, which has
    no derivable heartbeat threshold (``run_liveness.assess_run`` carries no
    staleness verdict for exactly that reason). Setting the override TO the
    rule-5 ceiling is how rule 4 declines to invent one.

    Pinned against the constant, never against 86_400: the point is that the
    two are the same number by construction, so a future ceiling change moves
    both together.
    """
    assert get_threshold("orchestrator_full_sync") == RUNTIME_CEILING_S


def test_high_frequency_sync_is_NOT_overridden() -> None:
    """#2274 — the asymmetry is the finding, so it gets its own test.

    Both orchestrator wrappers read the SAME ``sync_runs`` table through the
    same code path, so the obvious move is to exempt them together by run kind.
    That would be wrong: the HF sync's whole-corpus maximum runtime is
    multiples below the 300 s default, so the default costs it nothing AND is
    worth real signal — once the HF row can read ``running``, rule 1
    (``schedule_missed``) is suppressed for it and rule 4 becomes what chips a
    stranded HF singleton. A kind-keyed exemption would have left nothing until
    the 24 h ceiling.
    """
    assert get_threshold("orchestrator_high_frequency_sync") == DEFAULT_THRESHOLD_S
    assert "orchestrator_high_frequency_sync" not in overridden_process_ids()
