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
    "sec_13f_quarterly_sweep": 1800,
    "sec_n_port_ingest": 1800,
    "sec_def14a_bootstrap": 1800,
    "sec_business_summary_bootstrap": 1800,
    "ownership_observations_backfill": 1800,
    "sec_insider_transactions_backfill": 1800,
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


__all__ = ["DEFAULT_THRESHOLD_S", "get_threshold", "overridden_process_ids"]
