"""#2407 — the universe staleness alert must be looser than the refresh cadence.

Two constants in two modules describe one thing, and they contradicted each other
for as long as both existed:

* ``sync_orchestrator.freshness.UNIVERSE_REFRESH_WINDOW`` (7 days, #277) decides
  how old the universe layer is ALLOWED to get. ``nightly_universe_sync`` has no
  cadence of its own; the orchestrator fires it only when that window lapses.
* ``ops_monitor._STALENESS_THRESHOLDS["universe"]`` decides when the operator is
  TOLD the layer is stale.

An alert threshold at or below the refresh window fires on the intended state. It
never showed only because the layer query read ``instruments.last_seen_at``, a
column that barely moves — so the two bugs concealed each other.

⚠ They cannot share a constant: ``sync_orchestrator.adapters`` already imports
``ops_monitor``, so an import back would risk a cycle. A contract test is the
repo's established substitute (``test_job_terminal_status_contract``,
``test_the_deployment_currency_refusal_and_its_constraint_agree``).
"""

from __future__ import annotations

from datetime import timedelta

from app.services.ops_monitor import _LAYER_QUERIES, _STALENESS_THRESHOLDS
from app.services.sync_orchestrator.freshness import UNIVERSE_REFRESH_WINDOW

#: The orchestrator plans once daily (03:00 UTC), so the refresh window can lapse
#: up to a full day before the next planning slot acts on it.
_PLANNING_SLOT = timedelta(days=1)
#: ``last_confirmed_on`` is a DATE, so the layer query resolves it to midnight and
#: reads up to 24h older than the sync that wrote it.
_DATE_GRANULARITY = timedelta(days=1)


def test_the_universe_alert_is_looser_than_the_refresh_window_it_watches() -> None:
    """The whole point: never alert on a layer behaving exactly as declared."""
    assert _STALENESS_THRESHOLDS["universe"] >= UNIVERSE_REFRESH_WINDOW + _PLANNING_SLOT + _DATE_GRANULARITY, (
        f"universe staleness threshold {_STALENESS_THRESHOLDS['universe']} is not loose enough for a "
        f"{UNIVERSE_REFRESH_WINDOW} refresh window plus a {_PLANNING_SLOT} planning slot and "
        f"{_DATE_GRANULARITY} of date granularity — the panel would be red while the cadence is honoured"
    )


def test_the_threshold_is_not_so_loose_it_stops_detecting_a_stopped_sync() -> None:
    """The other direction, which the first assertion alone would let drift.

    A threshold of a month would satisfy the bound above and detect nothing. Two
    consecutive missed refresh windows must still trip it, since that is the
    condition the alert exists for.
    """
    assert _STALENESS_THRESHOLDS["universe"] < 2 * UNIVERSE_REFRESH_WINDOW, (
        f"universe staleness threshold {_STALENESS_THRESHOLDS['universe']} would not trip after two "
        f"consecutive missed {UNIVERSE_REFRESH_WINDOW} windows"
    )


def test_the_universe_layer_query_reads_confirmation_evidence_not_metadata_change() -> None:
    """⚠ Pins the SOURCE, because the threshold above is only correct for it.

    ``instruments.last_seen_at`` means "last time this row's METADATA CHANGED" —
    ``sync_universe``'s upsert bumps it inside a changed-metadata ``WHERE`` that
    suppresses the whole UPDATE when nothing changed. ``last_confirmed_on`` is
    bumped by every code path that OBSERVES PRESENCE, unconditionally
    (``sql/271_instrument_universe_membership.sql:25-50``).

    Also pins that it is NOT read from ``job_runs``: that is the job's own
    self-report, which is the failure class this fix closes.
    """
    query = _LAYER_QUERIES["universe"]
    assert "last_confirmed_on" in query
    assert "instrument_universe_membership" in query
    # Current members only — a closed row's `last_confirmed_on` IS its
    # `effective_to` (sql/271:126), so including them would let a departed
    # instrument's final confirmation stand in for a refresh that never happened.
    assert "effective_to IS NULL" in query
    assert "last_seen_at" not in query
    assert "job_runs" not in query
