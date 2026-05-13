"""Job source registry — Lane type + JOB_NAME_TO_SOURCE lookup.

PR1a of #1064 admin-control-hub follow-up sequence.
Plan: docs/internal/plans/pr1-job-registry-refactor.md (uncommitted).
Audit: docs/wiki/job-registry-audit.md.

## Why a dedicated module

Three things needed to coexist without circular imports:

1. The ``Lane`` Literal type (used by ``ScheduledJob.source``,
   ``StageSpec.lane``, and the ``JOB_NAME_TO_SOURCE`` lookup).
2. The ``JobInvoker`` callable alias (used by ``_INVOKERS`` in
   ``app/jobs/runtime.py`` after PR1b widens the contract).
3. The ``JOB_NAME_TO_SOURCE`` registry built from BOTH
   ``SCHEDULED_JOBS`` AND ``_BOOTSTRAP_STAGE_SPECS`` (used by
   ``JobLock`` to resolve a job_name to its source-keyed lock).

If ``Lane`` lived in ``app/workers/scheduler.py``, the bootstrap
orchestrator would import scheduler at module-load — currently
scheduler imports nothing from bootstrap_orchestrator, but the reverse
direction is heavy. Hoisting to a leaf module avoids the cycle.

## JOB_NAME_TO_SOURCE construction

The lookup MUST cover every job_name that ``JobLock`` may receive:

* Every entry in ``SCHEDULED_JOBS`` (~27 entries).
* Every entry in ``_BOOTSTRAP_STAGE_SPECS`` whose ``job_name`` is NOT
  also in ``SCHEDULED_JOBS`` (~10 bootstrap-only entries today —
  ``nightly_universe_sync``, ``daily_candle_refresh``,
  ``daily_cik_refresh``, ``sec_bulk_download``, the four
  ``sec_*_ingest_from_dataset`` entries, ``sec_submissions_files_walk``,
  and the three bespoke wrapper job names that PR1c will collapse into
  the SCHEDULED_JOBS set).

Conflict detection: if a job_name appears in both registries with
different effective sources, raise at module-load. Silent fallback
violates the locked source-lock decision.

Codex round-1 BLOCKING addressed: no per-name fallback in production.
``JobLock`` raises ``KeyError`` on unknown job_name (test fixtures
must register or use the explicit test-only escape hatch).

## Why ``Mapping`` and not ``dict`` for ``JobInvoker`` param

The invoker contract is read-only consumption of the params dict.
``Mapping`` makes the contract explicit and prevents accidental
mutation that would diverge ``params_snapshot`` from what the invoker
actually consumed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Type aliases — used across scheduler, bootstrap_orchestrator, locks, runtime.
# ---------------------------------------------------------------------------

Lane = Literal[
    "init",
    "etoro",
    "sec_rate",
    "sec_bulk_download",
    "db",
    "db_filings",
    "db_fundamentals_raw",
    "db_ownership_inst",
    "db_ownership_insider",
    "db_ownership_funds",
]
"""Source-level concurrency bucket. Operator-locked decision (#1064): same-source
jobs serialise under one ``JobLock``; cross-source jobs run in parallel.

* ``init`` — universe-sync only. Pre-everything fence; one job total.
* ``etoro`` — eToro REST budget. ``execute_approved_orders`` +
  ``daily_candle_refresh`` + ``etoro_lookups_refresh`` +
  ``exchanges_metadata_refresh`` serialise.
* ``sec_rate`` — SEC 10 req/s shared per-IP bucket. Every per-CIK +
  per-accession SEC fetch competes here.
* ``sec_bulk_download`` — fixed-URL SEC archive downloads. Disjoint
  from ``sec_rate`` — large fixed downloads, no per-issuer iteration.
* ``db`` — DB-bound stages NOT owned by a finer family lane — Phase E
  derivations (``fundamentals_sync``, ``ownership_observations_backfill``)
  + scheduler catch-all (``orchestrator_full_sync``,
  ``orchestrator_high_frequency_sync``, ``retry_deferred``,
  ``monitor_positions``, ``ownership_observations_sync``).

The next five are bootstrap Phase C bulk-ingest family lanes
(#1141 / Task E of audit #1136). Each owns a disjoint write
target so the five Phase C stages can dispatch cross-source in
parallel during first-install bootstrap, recovering the
~4-hour wall-clock saving the May 8 design called out before
PR1c #1064 collapsed everything onto a single ``db`` source.

* ``db_filings`` — ``sec_submissions_ingest``; writes
  ``filing_events`` + ``instrument_sec_profile``.
* ``db_fundamentals_raw`` — ``sec_companyfacts_ingest``; writes
  ``company_facts`` via ``upsert_facts_for_instrument``.
* ``db_ownership_inst`` — ``sec_13f_ingest_from_dataset``; writes
  ``ownership_institutions_observations``.
* ``db_ownership_insider`` — ``sec_insider_ingest_from_dataset``;
  writes ``insider_transactions`` + ``form3_holdings_initial``.
* ``db_ownership_funds`` — ``sec_nport_ingest_from_dataset``;
  writes ``n_port_*`` + ``sec_fund_series``.
"""


JobInvoker = Callable[[Mapping[str, Any]], None]
"""Invoker callable shape. PR1a keeps the ``_INVOKERS`` dict zero-arg
shape unchanged; PR1b widens to this contract so bodies can read
operator-supplied params via the queue-consumer dispatch path. The
``Mapping`` contract is read-only — invokers must not mutate the
``params`` dict (mutation would diverge ``job_runs.params_snapshot``
from what the invoker consumed)."""


# ---------------------------------------------------------------------------
# JOB_NAME_TO_SOURCE — the canonical source-lookup registry.
# ---------------------------------------------------------------------------
#
# Construction is deferred to ``_build_job_name_to_source()`` (called
# from a single module-load site at the bottom of this module) so the
# imports of ``SCHEDULED_JOBS`` + ``_BOOTSTRAP_STAGE_SPECS`` happen
# AFTER both modules have populated their registries. Any conflict
# (same job_name appearing in both with different effective sources)
# raises ``RuntimeError`` at import time — fail-fast prevents the
# silent source-lock semantic drift Codex round-1 BLOCKING flagged.


class JobSourceRegistryError(RuntimeError):
    """Raised at module-load when JOB_NAME_TO_SOURCE construction fails.

    Two failure modes:

    * Conflict: the same job_name appears in both registries with
      different effective sources.
    * Coverage gap: a bootstrap stage references a job_name not in
      either registry (only triggerable if the bootstrap stage table
      is hand-edited inconsistently).
    """


def _build_job_name_to_source() -> dict[str, Lane]:
    """Build the canonical job_name -> source lookup.

    Imports happen inside the function to defer the dependency on
    ``app/workers/scheduler.py`` and ``app/services/bootstrap_orchestrator.py``
    until both have populated their respective registries.
    """
    # Local imports to avoid module-load cycles.
    from app.services.bootstrap_orchestrator import (
        _BOOTSTRAP_STAGE_SPECS,
        _effective_lane,
    )
    from app.workers.scheduler import SCHEDULED_JOBS

    registry: dict[str, Lane] = {}

    # Pass 1: scheduled jobs.
    for job in SCHEDULED_JOBS:
        registry[job.name] = job.source

    # Pass 2: bootstrap stages. ``_effective_lane`` consults the
    # ``_STAGE_LANE_OVERRIDES`` map then falls back to the StageSpec.lane;
    # the resulting Lane is the source for that job_name when invoked
    # from bootstrap.
    conflicts: list[str] = []
    for stage in _BOOTSTRAP_STAGE_SPECS:
        bootstrap_source: Lane = _effective_lane(stage.stage_key, stage.lane)  # type: ignore[assignment]
        existing = registry.get(stage.job_name)
        if existing is None:
            registry[stage.job_name] = bootstrap_source
        elif existing != bootstrap_source:
            conflicts.append(
                f"job_name={stage.job_name!r}: scheduled.source={existing!r} vs bootstrap.lane={bootstrap_source!r}"
            )

    if conflicts:
        raise JobSourceRegistryError(
            "Source/lane conflict between SCHEDULED_JOBS and _BOOTSTRAP_STAGE_SPECS:\n  - "
            + "\n  - ".join(conflicts)
            + "\nFix the offending entries so a job_name resolves to the same source from both paths."
        )

    return registry


_REGISTRY_CACHE: dict[str, Lane] | None = None


def get_job_name_to_source() -> dict[str, Lane]:
    """Return the canonical job_name -> source lookup, building on first call.

    Lazy construction breaks the import cycle: ``app/workers/scheduler.py``
    imports the ``Lane`` type from this module at module-load time, so
    eagerly building the registry here would re-enter scheduler.py mid-load.
    First call materialises + caches; subsequent calls return the cached dict.

    Any source/lane conflict raises ``JobSourceRegistryError`` at the first
    call site — typically the FastAPI lifespan or the first ``JobLock``
    acquisition, both of which are smoke-tested.
    """
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is None:
        _REGISTRY_CACHE = _build_job_name_to_source()
    return _REGISTRY_CACHE


def reset_job_name_to_source_cache() -> None:
    """Test-only reset of the lazy cache. Production code never calls this."""
    global _REGISTRY_CACHE
    _REGISTRY_CACHE = None


def source_for(job_name: str) -> Lane:
    """Return the source-lock bucket for ``job_name``.

    Raises ``KeyError`` for unknown job_name. Production callers MUST
    have the job in ``SCHEDULED_JOBS`` or ``_BOOTSTRAP_STAGE_SPECS``.
    Test fixtures should register their job in the appropriate registry
    (or use ``JobLock.test_only_per_name`` once that escape hatch lands
    in PR1a).
    """
    registry = get_job_name_to_source()
    try:
        return registry[job_name]
    except KeyError as exc:
        raise KeyError(
            f"unknown job_name {job_name!r}: not found in SCHEDULED_JOBS or "
            f"_BOOTSTRAP_STAGE_SPECS. Either register it in the appropriate "
            f"registry (production) or use JobLock.test_only_per_name (tests)."
        ) from exc


__all__ = [
    "JobInvoker",
    "JobSourceRegistryError",
    "Lane",
    "get_job_name_to_source",
    "reset_job_name_to_source_cache",
    "source_for",
]
