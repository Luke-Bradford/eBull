"""Wiring invariants for the G12 master.idx quarterly sweep.

Spec: docs/superpowers/specs/2026-05-17-g12-master-idx-quarterly-walker.md §6.3.
Plan: docs/superpowers/plans/2026-05-17-g12-master-idx-quarterly-walker-plan.md §T8.

Pins every wiring layer:
- JOB_ constant exported + correct value.
- Exactly one SCHEDULED_JOBS entry with the right cadence + source + flags.
- _INVOKERS registers the zero-arg invoker (identity via .__wrapped__).
- source_for() resolves to "sec_rate".
"""

from __future__ import annotations

from app.jobs.runtime import _INVOKERS
from app.jobs.sources import source_for
from app.workers.scheduler import (
    JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP,
    SCHEDULED_JOBS,
    _bootstrap_complete,
    sec_master_idx_quarterly_sweep,
)


def test_job_name_constant_exported() -> None:
    assert JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP == "sec_master_idx_quarterly_sweep"


def test_scheduled_jobs_contains_master_idx_entry() -> None:
    matches = [j for j in SCHEDULED_JOBS if j.name == JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP]
    assert len(matches) == 1


def test_master_idx_scheduled_job_cadence_and_gating() -> None:
    job = next(j for j in SCHEDULED_JOBS if j.name == JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP)
    assert job.source == "sec_rate"
    assert job.cadence.kind == "weekly"
    assert job.cadence.weekday == 6  # Sunday
    assert job.cadence.hour == 5
    assert job.cadence.minute == 15
    assert job.catch_up_on_boot is False
    assert job.prerequisite is _bootstrap_complete
    assert job.exempt_from_universal_bootstrap_gate is False
    # No operator-tunable params for this job.
    assert job.params_metadata == ()


def test_invoker_registered_in_runtime() -> None:
    """Pin: registration identity via ``.__wrapped__`` (Codex 1b r1 LOW-1).

    Do NOT compare against ``_adapt_zero_arg(sec_master_idx_quarterly_sweep)``
    — each call to ``_adapt_zero_arg`` builds a fresh closure with no
    identity to the registered one. The ``__wrapped__`` attribute is
    set by ``_adapt_zero_arg`` at registration time.
    """
    invoker = _INVOKERS[JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP]
    assert callable(invoker)
    assert invoker.__wrapped__ is sec_master_idx_quarterly_sweep  # type: ignore[attr-defined]


def test_source_for_job_name_resolves_to_sec_rate() -> None:
    """JobLock acquisition would KeyError without this resolution."""
    assert source_for(JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP) == "sec_rate"
