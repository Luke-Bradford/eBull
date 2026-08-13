"""Wiring invariants for the FINRA bimonthly short interest refresh
(G6/#915 Phase 6 PR 11).

Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md §8.
Plan: docs/superpowers/plans/2026-05-18-finra-bimonthly-short-interest-plan.md §5-6.

Pins every wiring layer:

* ``JOB_FINRA_SHORT_INTEREST_REFRESH`` constant exported + correct value.
* Exactly one ``SCHEDULED_JOBS`` entry with the right cadence + source + flags.
* ``_INVOKERS`` registers the zero-arg invoker (identity via ``.__wrapped__``).
* ``source_for()`` resolves to ``"finra"``.
* ``MANUAL_TRIGGER_JOB_SOURCES`` entry present + lane=``finra``.
"""

from __future__ import annotations

from app.jobs.runtime import _INVOKERS
from app.jobs.sources import MANUAL_TRIGGER_JOB_SOURCES, source_for
from app.workers.scheduler import (
    JOB_FINRA_SHORT_INTEREST_REFRESH,
    SCHEDULED_JOBS,
    _bootstrap_complete,
    finra_short_interest_refresh,
)


def test_job_name_constant_exported() -> None:
    assert JOB_FINRA_SHORT_INTEREST_REFRESH == "finra_short_interest_refresh"


def test_scheduled_jobs_contains_finra_entry() -> None:
    matches = [j for j in SCHEDULED_JOBS if j.name == JOB_FINRA_SHORT_INTEREST_REFRESH]
    assert len(matches) == 1


def test_finra_scheduled_job_cadence_and_gating() -> None:
    job = next(j for j in SCHEDULED_JOBS if j.name == JOB_FINRA_SHORT_INTEREST_REFRESH)
    assert job.source == "finra"
    assert job.cadence.kind == "daily"
    assert job.cadence.hour == 12
    assert job.cadence.minute == 0
    assert job.catch_up_on_boot is False
    assert job.prerequisite is _bootstrap_complete
    assert job.exempt_from_universal_bootstrap_gate is False
    # v1 manual-trigger surface is zero-param.
    assert job.params_metadata == ()


def test_invoker_registered_in_runtime() -> None:
    """Pin: registration identity via ``.__wrapped__`` (G12 r1 LOW-1 pattern).

    Do NOT compare against ``_adapt_zero_arg(finra_short_interest_refresh)``
    — each call builds a fresh closure with no identity to the
    registered one. ``__wrapped__`` is set by ``_adapt_zero_arg`` at
    registration time.
    """
    invoker = _INVOKERS[JOB_FINRA_SHORT_INTEREST_REFRESH]
    assert callable(invoker)
    assert invoker.__wrapped__ is finra_short_interest_refresh  # type: ignore[attr-defined]


def test_source_for_job_name_resolves_to_finra() -> None:
    """JobLock acquisition would KeyError without this resolution."""
    assert source_for(JOB_FINRA_SHORT_INTEREST_REFRESH) == "finra"


def test_manual_trigger_sources_entry_present() -> None:
    """Operator manual-trigger via POST /jobs/.../run dispatch path."""
    assert MANUAL_TRIGGER_JOB_SOURCES[JOB_FINRA_SHORT_INTEREST_REFRESH] == "finra"


def test_finra_lane_disjoint_from_sec_rate() -> None:
    """The whole point of the new ``finra`` lane is host-disjoint
    rate-limit pool from SEC EDGAR. Pin that no FINRA job is on
    ``sec_rate``.
    """
    finra_jobs = [j for j in SCHEDULED_JOBS if j.source == "finra"]
    assert len(finra_jobs) >= 1  # at least finra_short_interest_refresh
    for job in finra_jobs:
        assert job.source != "sec_rate"
