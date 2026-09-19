"""Per-process mid_flight_stuck heartbeat thresholds.

Issue #1083 (umbrella #1064) — admin control hub PR8.
Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §A3 line 82 — "per-job override on a per-ingester basis (constant
      in the ingester module sourced from skill notes)".

Default 5 min. A producer that writes one tick per item will heartbeat
inside that window without operator action; an override is only needed
when the natural row-write cadence is slower (SEC bulk archive seeds:
one tick per archive completion, ~1/min, so 30 min keeps the threshold
above the producer's natural emission rate).

The keys are the same ``process_id`` strings adapters surface on
``ProcessRow.process_id``: ``"bootstrap"`` for the bootstrap row,
otherwise the ``ScheduledJob.name`` verbatim. Sweep ``process_id``s
(e.g. ``"sec_form4_sweep"``) are NOT keyed because sweeps have no
own active_run — mid_flight_stuck never fires on them.

A registry mismatch (override key that does not match any real
process_id) is caught by ``tests/test_stale_thresholds.py`` so a typo
cannot silently leave a long-running ingester on the 5-min default.
"""

from __future__ import annotations

from typing import Final

DEFAULT_THRESHOLD_S: Final[int] = 300  # 5 min — operator-amendment §A3.

# #2274 — whole-run wall-clock ceiling for ``stale_detection`` rule 5. No
# published or vendor formulation exists for a background-job runtime ceiling,
# so this is fixed BY CONSTRUCTION and frozen: a ceiling is a safety BACKSTOP,
# not a health alarm, so it sits an order of magnitude above the measured
# population rather than fitted to it.
#
# ⚠ Defined HERE rather than in ``stale_detection`` (where it used to live, and
# from which it is still re-exported for ``run_liveness``) so the
# ``orchestrator_full_sync`` override below can BE this ceiling rather than a
# second hand-picked number. ``stale_detection`` imports this module, so the
# dependency can only run in this direction.
#
# ⚠ Do not read a derived figure out of this comment — run the query. Over
# EVERY status, not successes only (a success-only query cannot establish the
# counterfactual, because the population a ceiling fires on is exactly the runs
# that never succeeded):
#
#   SELECT job_name, status, count(*),
#          round(max(extract(epoch FROM coalesce(finished_at, now()) - started_at)))
#     FROM job_runs
#    WHERE started_at > now() - interval '180 days'
#      AND extract(epoch FROM coalesce(finished_at, now()) - started_at) > 86400
#    GROUP BY 1, 2 ORDER BY 4 DESC;
#
# Measured 2026-09-15 on dev: every row it returns is a `failure` — an
# `orphaned: reaped at boot` run that sat `running` until a restart cleared it,
# up to 4.4 days. No SUCCESSFUL run of any registered job exceeds this ceiling;
# the longest is `sec_filing_documents_ingest` at ~2.7h. So the measured
# false-positive count is zero, which is what #2274's constraint demands ("a
# watchdog that fires on legitimately-long corpus jobs is worse than none").
#
# ⚠ The evidence is right-censored: a reaped row records when a restart
# happened, not when the work would have finished. That understates long runs,
# which argues for the large margin rather than for fitting the number tighter.
RUNTIME_CEILING_S: Final[int] = 86_400  # 24h

# process_id → seconds. Override only when the producer's natural
# row-write cadence is slower than DEFAULT_THRESHOLD_S. Codex review
# warning: keys are grep-validated against the live registry by
# ``tests/test_stale_thresholds.py``.
_OVERRIDES: Final[dict[str, int]] = {
    # Bootstrap drives 17 stages, several of which seed multi-GB SEC
    # archives. The slowest individual stage may emit one
    # `record_processed` per archive completion (~1/min) so 30 min keeps
    # the heartbeat above the producer's natural cadence.
    "bootstrap": 1800,
    # SEC bulk-download / archive-driven scheduled jobs. Each emits one
    # tick per accession or per archive completion; quarterly / monthly
    # ingest jobs are slow-tick by design. 30 min sits well above the
    # observed worst-case inter-tick gap.
    "sec_filing_documents_ingest": 1800,
    # The 13F + N-PORT stale-detection surfaces are the ingest-sweep
    # ProcessRows ``sec_13f_sweep`` / ``nport_sweep`` (ingest_sweep_adapter),
    # NOT the retired/internal job names ``sec_13f_quarterly_sweep`` /
    # ``sec_n_port_ingest`` — those key no real ProcessRow, so the override
    # never applied (#1445: caught while repairing the stale-threshold test).
    "sec_13f_sweep": 1800,
    "nport_sweep": 1800,
    "sec_def14a_bootstrap": 1800,
    "sec_business_summary_bootstrap": 1800,
    "ownership_observations_backfill": 1800,
    "sec_insider_transactions_backfill": 1800,
    # #2274 — the full sync's active row comes from ``sync_runs`` (the
    # orchestrator wrappers write no ``job_runs`` run at all), and it has no
    # derivable heartbeat threshold. Set to the rule-5 ceiling so rule 4
    # DECLINES to add a threshold here rather than inventing one — which is
    # ``run_liveness.assess_run``'s own settled position for a sync run, and
    # the reason that module carries no heartbeat-staleness verdict.
    #
    # ⚠ Do not read a derived figure out of this comment — the queries are in
    # ``docs/proposals/ops/2026-09-19-2274-orchestrator-active-run.md``. The
    # shape they measure: on ``sync_runs`` scope='full', ``last_progress_at``
    # is non-null on a small minority of runs, so for most of them rule 4
    # degenerates into a duration rule and the 300 s default fires on the large
    # majority of runs that COMPLETED successfully.
    #
    # ⚠⚠ Deliberately NOT applied to ``orchestrator_high_frequency_sync``,
    # which reads the same ``sync_runs`` table through the same code path. Its
    # whole-corpus maximum runtime is multiples below the 300 s default, so the
    # default costs nothing there AND is worth real signal: once the HF row can
    # read ``running``, rule 1 (``schedule_missed``) is suppressed for it, and
    # rule 4 becomes the thing that chips a stranded HF singleton. An exemption
    # keyed on the RUN KIND rather than on this job would have taken that away
    # and left nothing until the 24 h ceiling (Codex ckpt-1).
    "orchestrator_full_sync": RUNTIME_CEILING_S,
}


def get_threshold(process_id: str) -> int:
    """Return the mid_flight_stuck threshold (seconds) for ``process_id``.

    Falls back to ``DEFAULT_THRESHOLD_S`` when ``process_id`` is not in
    the override registry.
    """
    return _OVERRIDES.get(process_id, DEFAULT_THRESHOLD_S)


def overridden_process_ids() -> frozenset[str]:
    """Public accessor for the registry test — frozen so callers cannot
    mutate the source of truth.
    """
    return frozenset(_OVERRIDES.keys())


__all__ = [
    "DEFAULT_THRESHOLD_S",
    "RUNTIME_CEILING_S",
    "get_threshold",
    "overridden_process_ids",
]
