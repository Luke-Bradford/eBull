"""First-install bootstrap orchestrator.

Runs the 24-stage end-to-end first-install backfill described in
``docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md``
(supersedes the original 17-stage shape in
``docs/superpowers/specs/2026-05-07-first-install-bootstrap.md``).

Phases (S-numbers match the runbook stage list):

1. **Phase A — init** (sequential, ``init`` lane): S1 ``universe_sync``.
   Every later stage depends on a populated ``instruments`` table.
2. **Phase B — lanes** (parallel by source lock): the eToro lane
   (S2 ``candle_refresh``) runs alongside the SEC reference lane
   (S3..S6: filer directories, CIK refresh).
3. **Phase A3 — bulk archive download** (``sec_bulk_download`` lane):
   S7 ``sec_bulk_download`` ships fixed-URL SEC archives in one
   request, disjoint from the per-IP ``sec_rate`` budget.
4. **Phase C — DB-bound bulk ingest** (``db`` lane): S8..S12 ingest
   the bulk archives into ``filing_events`` / ``ownership_*`` /
   ``company_facts``.
5. **Phase C' — secondary-pages walker** (``sec_rate``): S13
   ``sec_submissions_files_walk`` covers deep-history submission
   pages the bulk archive truncates.
6. **Legacy / fallback chain** (``sec_rate``): S14..S22 ingest the
   per-filing path. Idempotent no-ops when Phase C populated rows;
   primary write path on the slow-connection bypass (see #1041).
7. **Phase E — final derivations** (``db`` lane): S23
   ``ownership_observations_backfill`` + S24 ``fundamentals_sync``.
8. **Finalize**: inspects per-stage outcomes and transitions
   ``bootstrap_state`` to ``complete`` or ``partial_error``.

Per-stage execution contract (every stage):

1. Pre-check stage status; skip if ``success``.
2. Mark stage ``running``.
3. Acquire ``JobLock(database_url, job_name)`` — same primitive
   that scheduled + manual paths use; resolves to a per-source
   advisory lock (PR1a #1064).
4. Invoke ``_INVOKERS[job_name](params)`` (PR1b-2 #1064 widened the
   contract from zero-arg to ``(Mapping) -> None``).
5. Catch exceptions; record ``error`` with truncated message.
6. On success record ``success`` + ``rows_processed``.

Bootstrap dispatches stage jobs by direct invocation, bypassing the
scheduler's prerequisite gate (intentional: bootstrap is the operator
forcing first-install work). The advisory lock is acquired so a
parallel manual / scheduled trigger cannot run twice simultaneously.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Final, Literal

import psycopg

from app.config import settings
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.services.bootstrap_preconditions import BootstrapPhaseSkipped
from app.services.bootstrap_state import (
    StageSpec,
    finalize_run,
    mark_run_cancelled,
    mark_stage_blocked,
    mark_stage_error,
    mark_stage_running,
    mark_stage_skipped,
    mark_stage_success,
    read_latest_run_with_stages,
)
from app.services.process_stop import is_stop_requested
from app.services.process_stop import mark_completed as mark_stop_completed
from app.services.process_stop import mark_observed as mark_stop_observed
from app.services.processes.param_metadata import (
    ParamValidationError,
    validate_job_params,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage catalogue — single source of truth for which stages run.
# ---------------------------------------------------------------------------

# Job names registered in app/jobs/runtime.py:_INVOKERS that PR2 adds:
JOB_BOOTSTRAP_ORCHESTRATOR = "bootstrap_orchestrator"

# PR1c #1064 — three bespoke wrappers (``bootstrap_filings_history_seed``,
# ``sec_first_install_drain_job``, ``bootstrap_sec_13f_recent_sweep_job``)
# were lifted into params-aware ``JobInvoker`` bodies in
# ``app/workers/scheduler.py``. The hardcoded values that lived inside
# the wrapper bodies (``days_back=730``, ``min_period_of_report=today-380d``,
# ``source_label="sec_edgar_13f_directory_bootstrap"``, etc.) now live
# in ``StageSpec.params`` for stages 14, 15, 21 below.
#
# Constants imported from the scheduler so the bootstrap-stage entries
# below carry the canonical names and a future rename is single-site.
from app.workers.scheduler import (  # noqa: E402  (after dataclass to avoid cycle)
    JOB_FILINGS_HISTORY_SEED,
    JOB_SEC_13F_QUARTERLY_SWEEP,
    JOB_SEC_FIRST_INSTALL_DRAIN,
)

# These already exist as scheduled jobs but were not registered in
# _INVOKERS until PR2; we re-use the existing job-name constants so
# operator records / job_runs trail stays consistent.
JOB_DAILY_CIK_REFRESH = "daily_cik_refresh"
JOB_DAILY_FINANCIAL_FACTS = "daily_financial_facts"


# PR1c #1064 — bootstrap-bounded 13F sweep recency cut-off. Used to
# live as a constant inside the deleted ``bootstrap_sec_13f_recent_sweep_job``
# wrapper. 4 quarters (~380 days) = current + 3 prior periods, matches
# the rolling ownership-card window. Older 13Fs add no value to
# current-quarter ranking and pre-2013 ones don't have machine-readable
# holdings (#1008).
_BOOTSTRAP_13F_QUARTERS_BACK = 4
_BOOTSTRAP_13F_RECENCY_DAYS = _BOOTSTRAP_13F_QUARTERS_BACK * 95

# PR1c #1064 — filings_history_seed bootstrap default form-type
# allow-list. Imported once at module load so the StageSpec.params
# dict is plain data; the underlying constant lives in the canonical
# owner (``app.services.filings``) so the three-tier allow-list stays
# single-sourced. Tuple (not list) for hashable, frozen-stage-spec
# compat.
from app.services.filings import SEC_INGEST_KEEP_FORMS  # noqa: E402

_FILINGS_HISTORY_KEEP_FORMS_TUPLE: tuple[str, ...] = tuple(sorted(SEC_INGEST_KEEP_FORMS))


# Sentinel for params values that depend on dispatch-time state
# (e.g. ``date.today()``). Module-load evaluation would freeze the
# value into ``_BOOTSTRAP_STAGE_SPECS`` for the lifetime of the jobs
# process; a long-lived process would dispatch stage 21 with a stale
# cutoff. ``_resolve_dynamic_params`` materialises the absolute value
# at dispatch time. The sentinel string is namespaced so it never
# collides with a legitimate operator value.
_PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF = "<dynamic:bootstrap_13f_cutoff>"


def _resolve_dynamic_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """Materialise dispatch-time dynamic values in a stage params dict.

    Today the only dynamic value is the bootstrap-13F recency cutoff;
    the helper is structured for forward extensibility (additional
    sentinels can be added without touching call sites).

    The dispatcher calls this immediately before invoking the
    underlying ``JobInvoker`` so the absolute value is what flows
    into ``job_runs.params_snapshot`` and the invoker body.
    """
    resolved: dict[str, Any] = dict(params)
    if resolved.get("min_period_of_report") == _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF:
        resolved["min_period_of_report"] = date.today() - timedelta(days=_BOOTSTRAP_13F_RECENCY_DAYS)
    return resolved


def _spec(
    stage_key: str,
    stage_order: int,
    lane: str,
    job_name: str,
    *,
    params: Mapping[str, Any] | None = None,
) -> StageSpec:
    return StageSpec(
        stage_key=stage_key,
        stage_order=stage_order,
        lane=lane,  # type: ignore[arg-type]
        job_name=job_name,
        params=params if params is not None else {},
    )


# ---------------------------------------------------------------------------
# Lane concurrency model (#1020)
# ---------------------------------------------------------------------------
#
# Each lane has a max-concurrency cap. Stages in the same lane share
# a budget (rate-bound: SEC clock; or DB-bound: psycopg conn pool).
# Stages in different lanes run in parallel.

_LANE_MAX_CONCURRENCY: Final[dict[str, int]] = {
    "init": 1,
    "etoro": 1,
    "sec": 1,  # legacy catch-all; preserved for migration compat
    "sec_rate": 1,  # SEC per-IP rate clock
    "sec_bulk_download": 1,
    # PR1c #1064: every lane serialises now that JobLock is source-keyed
    # (one ``JobLock(source)`` covers all jobs in the same lane). The
    # operator-locked source-lock decision is unambiguous: same-source =
    # serialised. Setting ``db`` to 1 retires the parallel-DB-stage claim
    # from #1020 — a misleading dispatcher shape (5 db stages submitted,
    # 4 immediately blocked on the source lock). The map structure is
    # kept (rather than deleted) for one cycle so the
    # ``_phase_batched_dispatch`` shape stays stable; a follow-up PR
    # removes the map entirely.
    #
    # Tech-debt: first-install bootstrap wall-clock regresses from "5 db
    # stages parallel" → "1 db stage at a time". Measure on dev and file
    # follow-up if operator-visible — tracked in PR description.
    "db": 1,
}


# ---------------------------------------------------------------------------
# Capability layer (#1138 / Task A of #1136 audit)
# ---------------------------------------------------------------------------
#
# Stages declare capabilities they ``provide`` on success. Downstream
# stages declare a ``CapRequirement`` (all_of / any_of) over those
# capabilities rather than over concrete stage-key predecessors.
# This decouples the dependency graph from the bulk-vs-legacy
# topology so a partial bulk failure can be recovered via the legacy
# chain when both paths feed the same capability.
#
# Spec: docs/superpowers/specs/2026-05-13-bootstrap-capability-layer.md.

Capability = Literal[
    "universe_seeded",
    "cik_mapping_ready",
    "cusip_mapping_ready",
    "bulk_archives_ready",
    "filing_events_seeded",
    "submissions_secondary_pages_walked",
    "insider_inputs_seeded",
    "form3_inputs_seeded",
    "institutional_inputs_seeded",
    "nport_inputs_seeded",
    "fundamentals_raw_seeded",
]


@dataclass(frozen=True)
class CapRequirement:
    """All-of / any-of (DNF) dependency requirement over capabilities.

    Satisfied iff every cap in ``all_of`` is present in the satisfied
    set AND (``any_of`` is empty OR at least one inner tuple is
    fully ⊆ satisfied set).
    """

    all_of: tuple[Capability, ...] = ()
    any_of: tuple[tuple[Capability, ...], ...] = ()


# Each stage's capabilities provided on ``success``. Stages absent
# from this map provide nothing (e.g. ``candle_refresh``, filer
# directory syncs, typed parsers, final derivations).
_STAGE_PROVIDES: Final[dict[str, tuple[Capability, ...]]] = {
    "universe_sync": ("universe_seeded",),
    "cusip_universe_backfill": ("cusip_mapping_ready",),
    "cik_refresh": ("cik_mapping_ready",),
    # S7 bulk download provides bulk_archives_ready ONLY on real bulk
    # mode. The fallback path in sec_bulk_download_job() raises
    # BootstrapPhaseSkipped (#1138 §4.3) so the stage transitions to
    # `skipped` and this provider entry never fires.
    "sec_bulk_download": ("bulk_archives_ready",),
    "sec_submissions_ingest": ("filing_events_seeded",),
    "sec_companyfacts_ingest": ("fundamentals_raw_seeded",),
    # Bulk ownership ingester covers both insider transactions + Form 3.
    "sec_insider_ingest_from_dataset": ("insider_inputs_seeded", "form3_inputs_seeded"),
    "sec_13f_ingest_from_dataset": ("institutional_inputs_seeded",),
    "sec_nport_ingest_from_dataset": ("nport_inputs_seeded",),
    "sec_submissions_files_walk": ("submissions_secondary_pages_walked",),
    "filings_history_seed": ("filing_events_seeded",),
    # sec_first_install_drain runs with follow_pagination=True
    # (app/workers/scheduler.py) so it walks the same
    # filings.files[] surface as the dedicated walker (S13). Providing
    # both caps lets typed parsers run on the legacy / slow-connection
    # path where S13 is cascade-skipped or self-skipped.
    "sec_first_install_drain": ("filing_events_seeded", "submissions_secondary_pages_walked"),
    "sec_insider_transactions_backfill": ("insider_inputs_seeded",),
    "sec_form3_ingest": ("form3_inputs_seeded",),
    "sec_13f_recent_sweep": ("institutional_inputs_seeded",),
    "sec_n_port_ingest": ("nport_inputs_seeded",),
}


# Per-stage caps provided on ``skipped`` status. Intentionally empty
# by default — skipped stages do NOT provide capabilities. The
# slow-connection fallback (#1041) relies on the *legacy* chain
# providing the same caps, not on a skipped bulk stage masquerading
# as a provider. Add an entry here only when a skip is semantically
# equivalent to success.
_STAGE_PROVIDES_ON_SKIP: Final[dict[str, tuple[Capability, ...]]] = {}


# #1140 / Task C of #1136 audit — strict-gate row-count floors.
#
# For caps in this map: a provider stage's ``success`` status alone
# does NOT advertise the cap; the provider's ``rows_processed`` must
# also be ``>= min_rows``. Caps absent from this map fall back to
# status-only gating (Task A behaviour preserved).
#
# Default ``min_rows = 1`` for the cap-providing stages whose
# downstream consumers (``fundamentals_sync``,
# ``ownership_observations_backfill``) MUST observe non-zero ingest
# to be useful. The audit (#1136 §2 acceptance) requires this for
# fundamentals + every ownership family.
#
# Threshold ``1`` is the cheapest non-trivial floor: any positive
# write counts. Higher floors (e.g. universe-coverage ratios) are
# out of scope for v1 — the structural hook stays the per-cap int
# knob, no hardcoded percentages.
#
_CAPABILITY_MIN_ROWS: Final[dict[Capability, int]] = {
    "fundamentals_raw_seeded": 1,
    "insider_inputs_seeded": 1,
    "form3_inputs_seeded": 1,
    "institutional_inputs_seeded": 1,
    "nport_inputs_seeded": 1,
}


# #1140 / Task C of #1136 audit — strict-gate provider exclusions.
#
# For a strict-gate cap, providers listed here CANNOT contribute to
# satisfying the floor via their aggregate ``rows_processed``. Used
# when a multi-cap provider's aggregate row count can't be split per
# advertised cap. The provider's ``success`` still keeps the cap
# alive (i.e. doesn't kill it) but doesn't satisfy the strict floor
# either — the cap must be carried by other (single-cap) providers.
#
# Today the only entry is the bulk insider ingester
# (``sec_insider_ingest_from_dataset``): it advertises both
# ``insider_inputs_seeded`` and ``form3_inputs_seeded`` from a single
# aggregate ``rows_processed`` (bulk maps rows to form3 vs form4
# internally but records the sum). A bulk wash that landed 10 Form 4
# + 0 Form 3 rows would otherwise falsely advertise
# ``form3_inputs_seeded`` under the strict rule. Excluding the bulk
# provider for ``form3_inputs_seeded`` forces the legacy
# ``sec_form3_ingest`` (single-cap, scoped to form3) to validate the
# form3 path — Codex pre-push round 2 BLOCKING.
#
# When per-family bulk row counts land (follow-up ticket) this map
# entry can be dropped and the bulk provider can satisfy form3
# directly.
_STRICT_CAP_PROVIDER_EXCLUSIONS: Final[dict[Capability, frozenset[str]]] = {
    "form3_inputs_seeded": frozenset({"sec_insider_ingest_from_dataset"}),
}


# Stage-key → CapRequirement. Replaces the old AND-only
# ``_STAGE_REQUIRES`` (#1138 Task A). Every entry in
# ``_BOOTSTRAP_STAGE_SPECS`` must appear here (enforced by the
# catalogue-invariant test).
_STAGE_REQUIRES_CAPS: Final[dict[str, CapRequirement]] = {
    # Phase A
    "universe_sync": CapRequirement(),
    "candle_refresh": CapRequirement(all_of=("universe_seeded",)),
    "cusip_universe_backfill": CapRequirement(all_of=("universe_seeded",)),
    "sec_13f_filer_directory_sync": CapRequirement(all_of=("universe_seeded",)),
    "sec_nport_filer_directory_sync": CapRequirement(all_of=("universe_seeded",)),
    "cik_refresh": CapRequirement(all_of=("universe_seeded",)),
    "sec_bulk_download": CapRequirement(all_of=("universe_seeded",)),
    # Phase C — DB-bound bulk ingesters
    "sec_submissions_ingest": CapRequirement(all_of=("bulk_archives_ready", "cik_mapping_ready")),
    "sec_companyfacts_ingest": CapRequirement(all_of=("bulk_archives_ready", "cik_mapping_ready")),
    "sec_13f_ingest_from_dataset": CapRequirement(all_of=("bulk_archives_ready", "cusip_mapping_ready")),
    "sec_insider_ingest_from_dataset": CapRequirement(all_of=("bulk_archives_ready", "cik_mapping_ready")),
    "sec_nport_ingest_from_dataset": CapRequirement(all_of=("bulk_archives_ready", "cusip_mapping_ready")),
    # Phase C' — walker
    "sec_submissions_files_walk": CapRequirement(all_of=("filing_events_seeded",)),
    # Legacy chain
    "filings_history_seed": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_first_install_drain": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_def14a_bootstrap": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_business_summary_bootstrap": CapRequirement(
        all_of=("filing_events_seeded", "submissions_secondary_pages_walked")
    ),
    "sec_insider_transactions_backfill": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_form3_ingest": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_8k_events_ingest": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_13f_recent_sweep": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_n_port_ingest": CapRequirement(all_of=("cik_mapping_ready",)),
    # Phase E — final derivations. Per-family caps in §4 of the spec
    # encode the bulk-OR-legacy alternative at the *provider* side
    # (each cap has both a bulk and a legacy producer), so the
    # consumer can simply require all four families with `all_of`.
    "ownership_observations_backfill": CapRequirement(
        all_of=(
            "cik_mapping_ready",
            "insider_inputs_seeded",
            "form3_inputs_seeded",
            "institutional_inputs_seeded",
            "nport_inputs_seeded",
        ),
    ),
    "fundamentals_sync": CapRequirement(all_of=("fundamentals_raw_seeded",)),
}


def _build_capability_providers(
    provides: Mapping[str, tuple[Capability, ...]],
    provides_on_skip: Mapping[str, tuple[Capability, ...]],
) -> dict[Capability, tuple[str, ...]]:
    """Inverse map ``Capability → tuple of provider stage_keys``."""
    out: dict[Capability, list[str]] = {}
    for stage_key, caps in provides.items():
        for cap in caps:
            out.setdefault(cap, []).append(stage_key)
    for stage_key, caps in provides_on_skip.items():
        for cap in caps:
            existing = out.setdefault(cap, [])
            if stage_key not in existing:
                existing.append(stage_key)
    return {cap: tuple(keys) for cap, keys in out.items()}


_CAPABILITY_PROVIDERS: Final[dict[Capability, tuple[str, ...]]] = _build_capability_providers(
    _STAGE_PROVIDES,
    _STAGE_PROVIDES_ON_SKIP,
)


# ---------------------------------------------------------------------------
# Capability-evaluation helpers (#1138 §6)
# ---------------------------------------------------------------------------


def _provider_meets_floor(
    cap: Capability,
    provider_key: str,
    rows_processed: int | None,
    min_rows: Mapping[Capability, int],
    *,
    exclusions: Mapping[Capability, frozenset[str]] = _STRICT_CAP_PROVIDER_EXCLUSIONS,
) -> bool:
    """Return True iff a ``success`` provider's row count satisfies the
    cap's strict-gate floor (if any).

    Non-strict caps (absent from ``min_rows``) are always satisfied by
    ``success`` — preserves Task A behaviour. Strict-gate caps require
    ``rows_processed`` to be non-None AND ``>= floor``.

    A provider in ``exclusions[cap]`` (e.g. a multi-cap bulk ingester
    whose aggregate ``rows_processed`` can't be split per cap) cannot
    satisfy a strict-gate floor — even with non-zero rows. The
    provider is treated as if its row count was always below the
    floor; the cap must be carried by another (single-cap) provider.
    See ``_STRICT_CAP_PROVIDER_EXCLUSIONS`` for the rationale.

    #1140 Task C.
    """
    floor = min_rows.get(cap)
    if floor is None:
        return True
    if provider_key in exclusions.get(cap, frozenset()):
        return False
    if rows_processed is None:
        return False
    return rows_processed >= floor


def _satisfied_capabilities(
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None] | None = None,
    *,
    provides: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES,
    provides_on_skip: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES_ON_SKIP,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
    exclusions: Mapping[Capability, frozenset[str]] = _STRICT_CAP_PROVIDER_EXCLUSIONS,
) -> set[Capability]:
    """Cap set derived from current stage statuses + per-stage rows.

    For a cap ``C`` with ``min_rows[C] = N``: a provider ``P`` satisfies
    ``C`` iff ``statuses[P] == 'success'`` AND ``rows_processed[P]``
    is not None AND ``>= N``. ``skipped`` providers still satisfy via
    ``provides_on_skip`` (the skip path is never row-counted).

    For a cap ``C`` NOT in ``min_rows``: a provider ``P`` satisfies
    ``C`` iff ``statuses[P] == 'success'`` (legacy Task A behaviour
    preserved).

    Production callers can omit ``rows_processed`` (defaults to an
    empty mapping; strict-gate caps then fall to "below floor" since
    every lookup returns None). Tests can pass overrides for
    ``provides`` / ``provides_on_skip`` / ``min_rows`` to register
    synthetic caps for fixture stage_keys.
    """
    rows = rows_processed or {}
    caps: set[Capability] = set()
    for stage_key, status in statuses.items():
        if status == "success":
            for cap in provides.get(stage_key, ()):
                if _provider_meets_floor(cap, stage_key, rows.get(stage_key), min_rows, exclusions=exclusions):
                    caps.add(cap)
        elif status == "skipped":
            caps.update(provides_on_skip.get(stage_key, ()))
    return caps


def _capability_is_dead(
    cap: Capability,
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None] | None = None,
    *,
    providers_map: Mapping[Capability, tuple[str, ...]] = _CAPABILITY_PROVIDERS,
    provides_on_skip: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES_ON_SKIP,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
    exclusions: Mapping[Capability, frozenset[str]] = _STRICT_CAP_PROVIDER_EXCLUSIONS,
) -> bool:
    """A cap is dead iff every registered provider is in a state where
    it cannot now (or in the future) provide the cap.

    Cannot-provide states:
    * ``error`` / ``blocked`` / ``cancelled`` — terminal failure.
    * ``skipped`` without an explicit ``provides_on_skip`` entry.
    * For strict-gate caps (``cap in min_rows``): ``success`` with
      ``rows_processed`` either ``None`` or below ``min_rows[cap]``.
      The provider already terminalised so no future write will
      change its row count — the cap can never be satisfied via
      this provider (#1140 Task C).

    Can-still-provide states: ``pending`` / ``running`` (provider
    hasn't decided yet); ``success`` meeting the floor (or no floor);
    ``skipped`` with the cap in ``provides_on_skip``.
    """
    providers = providers_map.get(cap, ())
    if not providers:
        # Cap with no provider — dead by construction. The catalogue
        # invariant test should have caught this at test time; runtime
        # check is defence-in-depth.
        return True
    rows = rows_processed or {}
    for provider_key in providers:
        status = statuses.get(provider_key)
        if status is None:
            continue
        if status in ("pending", "running"):
            return False
        if status == "success":
            if _provider_meets_floor(cap, provider_key, rows.get(provider_key), min_rows, exclusions=exclusions):
                return False
            # Below floor (or excluded multi-cap provider) — this
            # provider cannot satisfy a strict cap. Keep checking the
            # others (a parallel provider may still be alive).
            continue
        if status == "skipped":
            on_skip = provides_on_skip.get(provider_key, ())
            if cap in on_skip:
                return False
    return True


def _classify_dead_cap(
    cap: Capability,
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None] | None = None,
    *,
    providers_map: Mapping[Capability, tuple[str, ...]] = _CAPABILITY_PROVIDERS,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
    exclusions: Mapping[Capability, frozenset[str]] = _STRICT_CAP_PROVIDER_EXCLUSIONS,
) -> Literal["skip_only", "error"]:
    """Return the failure mode that killed a (confirmed-dead) cap.

    Precondition: ``_capability_is_dead(cap, statuses, rows_processed)``
    is True.

    Returns ``"error"`` (block downstream) when ANY provider is in
    ``error`` / ``blocked`` / ``cancelled`` OR (for strict-gate caps)
    in ``success`` with row count below the floor — a provider that
    ran and produced too few rows is a failure mode, not a deliberate
    bypass, so the operator should see ``blocked`` not ``skipped``.

    Returns ``"skip_only"`` only when every dead provider is
    ``skipped`` without an explicit ``provides_on_skip`` entry.

    Defensive default: a cap with zero registered providers, or a
    cap with NO ``skipped`` provider (only unknown/pending), is
    classified ``"error"`` so the dispatcher hard-blocks rather than
    silently cascading skip on a malformed catalogue.
    """
    providers = providers_map.get(cap, ())
    if not providers:
        return "error"
    rows = rows_processed or {}
    saw_skipped = False
    for provider_key in providers:
        status = statuses.get(provider_key)
        if status is None:
            continue
        if status in ("error", "blocked", "cancelled"):
            return "error"
        if status == "success" and not _provider_meets_floor(
            cap, provider_key, rows.get(provider_key), min_rows, exclusions=exclusions
        ):
            # #1140 Task C — provider succeeded but its row count
            # didn't meet the strict floor. Excluded multi-cap providers
            # (e.g. bulk insider for form3) are NEUTRAL — they cannot
            # satisfy the floor but they shouldn't drive the
            # classification either; the cap death is whatever the
            # OTHER providers tell us. Skip them.
            if provider_key in exclusions.get(cap, frozenset()):
                continue
            # Non-excluded provider under floor → classify as error so
            # the consumer transitions to ``blocked`` with a structured
            # reason naming the under-floor provider.
            return "error"
        if status == "skipped":
            saw_skipped = True
    return "skip_only" if saw_skipped else "error"


def _requirement_satisfied(req: CapRequirement, caps: set[Capability]) -> bool:
    if not all(c in caps for c in req.all_of):
        return False
    if not req.any_of:
        return True
    return any(all(c in caps for c in group) for group in req.any_of)


def _classify_requirement_unsatisfiable(
    req: CapRequirement,
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None] | None = None,
    *,
    providers_map: Mapping[Capability, tuple[str, ...]] = _CAPABILITY_PROVIDERS,
    provides_on_skip: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES_ON_SKIP,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
    exclusions: Mapping[Capability, frozenset[str]] = _STRICT_CAP_PROVIDER_EXCLUSIONS,
) -> tuple[Literal["skip_only", "error"], list[Capability]] | None:
    """If ``req`` is unsatisfiable now, return ``(classification, dead_caps)``.

    Returns ``None`` when the requirement is still potentially
    satisfiable (some provider remains pending/running).

    Classification rules:
    * If any cap in ``all_of`` is dead → unsatisfiable.
    * If ``any_of`` is non-empty AND every alternative group contains
      at least one dead cap → unsatisfiable.
    * Else → still satisfiable; return None.

    Per #1138 §6.3: when unsatisfiable, classify ``"error"`` if any
    contributing dead cap is error-classified; otherwise
    ``"skip_only"``. #1140 Task C extends "error-classified" to include
    strict-gate caps whose only surviving provider hit ``success`` but
    under the row floor.
    """
    is_dead = lambda c: _capability_is_dead(  # noqa: E731
        c,
        statuses,
        rows_processed,
        providers_map=providers_map,
        provides_on_skip=provides_on_skip,
        min_rows=min_rows,
        exclusions=exclusions,
    )
    dead_in_all: list[Capability] = [c for c in req.all_of if is_dead(c)]

    dead_in_any_groups: list[list[Capability]] = []
    any_group_live = not req.any_of  # vacuously true when any_of is empty
    if req.any_of:
        for group in req.any_of:
            dead_in_group: list[Capability] = [c for c in group if is_dead(c)]
            if not dead_in_group:
                any_group_live = True
            else:
                dead_in_any_groups.append(dead_in_group)

    if not dead_in_all and any_group_live:
        return None

    all_dead_caps: list[Capability] = list(dead_in_all)
    if not any_group_live:
        for group_deads in dead_in_any_groups:
            for cap in group_deads:
                if cap not in all_dead_caps:
                    all_dead_caps.append(cap)

    for cap in all_dead_caps:
        if (
            _classify_dead_cap(
                cap,
                statuses,
                rows_processed,
                providers_map=providers_map,
                min_rows=min_rows,
                exclusions=exclusions,
            )
            == "error"
        ):
            return ("error", all_dead_caps)
    return ("skip_only", all_dead_caps)


def _format_block_reason(
    dead_caps: list[Capability],
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None] | None = None,
    *,
    providers_map: Mapping[Capability, tuple[str, ...]] = _CAPABILITY_PROVIDERS,
    min_rows: Mapping[Capability, int] = _CAPABILITY_MIN_ROWS,
    exclusions: Mapping[Capability, frozenset[str]] = _STRICT_CAP_PROVIDER_EXCLUSIONS,
) -> str:
    """Build the structured ``last_error`` string for a blocked stage.

    For strict-gate caps the per-provider annotation includes
    ``rows_processed=N`` or ``rows_processed=NULL`` so the operator
    timeline reads exactly which provider fell short of the floor
    (#1140 Task C).
    """
    rows = rows_processed or {}
    parts: list[str] = []
    for cap in dead_caps:
        providers = providers_map.get(cap, ())
        excluded_for_cap = exclusions.get(cap, frozenset())
        annotated: list[str] = []
        for p in providers:
            status = statuses.get(p, "?")
            if status == "success" and cap in min_rows:
                value = rows.get(p)
                rows_str = "NULL" if value is None else str(value)
                marker = " [excluded]" if p in excluded_for_cap else ""
                annotated.append(f"{p}=success [rows_processed={rows_str}]{marker}")
            else:
                annotated.append(f"{p}={status}")
        provider_states = ", ".join(annotated) or "(no providers)"
        floor = min_rows.get(cap)
        if floor is not None:
            parts.append(
                f"missing capability {cap}; no surviving provider met rows floor {floor} (providers: {provider_states})"
            )
        else:
            parts.append(f"missing capability {cap}; no surviving provider (providers: {provider_states})")
    return "; ".join(parts)


def _format_cascade_skip_reason(dead_caps: list[Capability]) -> str:
    cap_str = ", ".join(dead_caps)
    return f"cascaded skip: required capability {cap_str} provided only by skipped upstream(s)"


# Lane override map — stage_key → lane name, used by the
# concurrency dispatcher. Stages NOT in this map default to their
# ``StageSpec.lane`` field; the override map lets the dispatcher
# refine the lane (e.g. retire ``etoro`` for SEC-lane stages) without
# rewriting every spec.
_STAGE_LANE_OVERRIDES: Final[dict[str, str]] = {
    "cusip_universe_backfill": "sec_rate",
    "sec_13f_filer_directory_sync": "sec_rate",
    "sec_nport_filer_directory_sync": "sec_rate",
    "cik_refresh": "sec_rate",
    "sec_bulk_download": "sec_bulk_download",
    "sec_submissions_ingest": "db",
    "sec_companyfacts_ingest": "db",
    "sec_13f_ingest_from_dataset": "db",
    "sec_insider_ingest_from_dataset": "db",
    "sec_nport_ingest_from_dataset": "db",
    "sec_submissions_files_walk": "sec_rate",
    "sec_def14a_bootstrap": "sec_rate",
    "sec_business_summary_bootstrap": "sec_rate",
    "sec_8k_events_ingest": "sec_rate",
    "sec_13f_recent_sweep": "sec_rate",
}


def _effective_lane(stage_key: str, default_lane: str) -> str:
    return _STAGE_LANE_OVERRIDES.get(stage_key, default_lane)


# Bulk-archive job names for the #1020 first-install bulk-datasets-first
# pipeline. Re-exported from the canonical owners so duplicate-constant
# drift is impossible (Codex review WARNING for PR #1035).
from app.services.sec_bulk_download import JOB_SEC_BULK_DOWNLOAD  # noqa: E402
from app.services.sec_bulk_orchestrator_jobs import (  # noqa: E402
    JOB_SEC_13F_INGEST_FROM_DATASET,
    JOB_SEC_COMPANYFACTS_INGEST,
    JOB_SEC_INSIDER_INGEST_FROM_DATASET,
    JOB_SEC_NPORT_INGEST_FROM_DATASET,
    JOB_SEC_SUBMISSIONS_INGEST,
)
from app.services.sec_submissions_files_walk import (  # noqa: E402
    JOB_SEC_SUBMISSIONS_FILES_WALK,
)

_BOOTSTRAP_STAGE_SPECS: tuple[StageSpec, ...] = (
    # Phase A (init, sequential)
    _spec("universe_sync", 1, "init", "nightly_universe_sync"),
    # eToro lane (separate rate budget; runs concurrent with SEC).
    _spec("candle_refresh", 2, "etoro", "daily_candle_refresh"),
    # SEC reference lane — share per-IP rate clock.
    _spec("cusip_universe_backfill", 3, "sec_rate", "cusip_universe_backfill"),
    _spec("sec_13f_filer_directory_sync", 4, "sec_rate", "sec_13f_filer_directory_sync"),
    _spec("sec_nport_filer_directory_sync", 5, "sec_rate", "sec_nport_filer_directory_sync"),
    _spec("cik_refresh", 6, "sec_rate", JOB_DAILY_CIK_REFRESH),
    # Phase A3 — bulk archive download (#1020). Ships the heavy data
    # in <10 min on a fast connection; the C-stages below ingest
    # locally with no rate-budget cost.
    _spec("sec_bulk_download", 7, "sec_bulk_download", JOB_SEC_BULK_DOWNLOAD),
    # Phase C — DB-bound bulk ingesters (#1020). Parallel within db
    # lane (max_concurrency=5).
    _spec("sec_submissions_ingest", 8, "db", JOB_SEC_SUBMISSIONS_INGEST),
    _spec("sec_companyfacts_ingest", 9, "db", JOB_SEC_COMPANYFACTS_INGEST),
    _spec("sec_13f_ingest_from_dataset", 10, "db", JOB_SEC_13F_INGEST_FROM_DATASET),
    _spec("sec_insider_ingest_from_dataset", 11, "db", JOB_SEC_INSIDER_INGEST_FROM_DATASET),
    _spec("sec_nport_ingest_from_dataset", 12, "db", JOB_SEC_NPORT_INGEST_FROM_DATASET),
    # Phase C' — per-CIK secondary-pages walk for deep-history parity.
    _spec("sec_submissions_files_walk", 13, "sec_rate", JOB_SEC_SUBMISSIONS_FILES_WALK),
    # Legacy per-filing stages — kept as a fallback path. After the
    # bulk pass these are largely idempotent DB no-ops on populated
    # observation tables; on the slow-connection bypass path they are
    # the primary write path.
    # PR1c #1064 — three bespoke wrappers in this module collapsed into
    # the SCHEDULED_JOBS-side ``filings_history_seed`` /
    # ``sec_first_install_drain`` / ``sec_13f_quarterly_sweep`` bodies.
    # Bootstrap-only knobs ride here as ``StageSpec.params``; the bodies
    # consume the same dict shape that the manual API publishes.
    _spec(
        "filings_history_seed",
        14,
        "sec_rate",
        JOB_FILINGS_HISTORY_SEED,
        params={
            "days_back": 730,
            "filing_types": tuple(_FILINGS_HISTORY_KEEP_FORMS_TUPLE),
        },
    ),
    _spec(
        "sec_first_install_drain",
        15,
        "sec_rate",
        JOB_SEC_FIRST_INSTALL_DRAIN,
        params={"max_subjects": None},
    ),
    _spec("sec_def14a_bootstrap", 16, "sec_rate", "sec_def14a_bootstrap"),
    _spec("sec_business_summary_bootstrap", 17, "sec_rate", "sec_business_summary_bootstrap"),
    _spec("sec_insider_transactions_backfill", 18, "sec_rate", "sec_insider_transactions_backfill"),
    _spec("sec_form3_ingest", 19, "sec_rate", "sec_form3_ingest"),
    _spec("sec_8k_events_ingest", 20, "sec_rate", "sec_8k_events_ingest"),
    # #1008 — first-install bootstrap uses a recency-bounded sweep
    # (last 4 quarters, ~12 months) instead of the full historical
    # sweep. Walking decades of pre-2013 filings yields zero rows
    # (no machine-readable primary_doc/infotable) and turns the
    # bootstrap into an 11+ hour wait. Standalone weekly cron keeps
    # the full historical sweep — same job, no min_period_of_report
    # bound. On the bulk path (#1020) C3 has already populated
    # ownership_institutions_observations; this stage tops up.
    #
    # PR1c #1064: bootstrap-only ``source_label`` overrides the default
    # so audit history distinguishes this bounded sweep from the
    # standalone weekly run. The validator allows ``source_label`` here
    # via ``JOB_INTERNAL_KEYS`` (PR1a) — manual API rejects it.
    _spec(
        "sec_13f_recent_sweep",
        21,
        "sec_rate",
        JOB_SEC_13F_QUARTERLY_SWEEP,
        # ``min_period_of_report`` resolves to ``today() - 380d`` at
        # dispatch time (see ``_resolve_dynamic_params``). Hardcoding
        # ``date.today()`` here would freeze the cutoff at module-load,
        # so a long-lived jobs process would dispatch stage 21 with a
        # stale floor. The sentinel keeps the StageSpec data-only.
        params={
            "min_period_of_report": _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF,
            "source_label": "sec_edgar_13f_directory_bootstrap",
        },
    ),
    _spec("sec_n_port_ingest", 22, "sec_rate", "sec_n_port_ingest"),
    _spec("ownership_observations_backfill", 23, "db", "ownership_observations_backfill"),
    _spec("fundamentals_sync", 24, "db", "fundamentals_sync"),
)


def get_bootstrap_stage_specs() -> tuple[StageSpec, ...]:
    """Public read-only accessor for the stage catalogue.

    The API endpoint that creates a new run imports this to seed
    ``bootstrap_stages`` rows. Lives in code (not the DB) because the
    catalogue is a deployable contract — adding / reordering stages
    is a code change with tests, not a runtime config change.
    """
    return _BOOTSTRAP_STAGE_SPECS


# ---------------------------------------------------------------------------
# Per-stage runner
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StageOutcome:
    stage_key: str
    success: bool
    error: str | None
    skipped: bool = False
    # PR3d #1064 — operator-cancel observed mid-stage. The dispatcher
    # maps this onto stage status='cancelled' (not 'error') so the
    # Timeline tones gray instead of red. The next dispatcher
    # iteration's run-level cancel checkpoint then sweeps remaining
    # stages and terminalises the run.
    cancelled: bool = False
    # #1140 Task C — resolved rows_processed for the stage's
    # invocation. ``None`` when no side-channel has data (the cap-eval
    # layer treats ``None`` as "below floor" for strict-gate caps,
    # status-only for non-strict caps).
    rows_processed: int | None = None


def _snapshot_job_runs_max_id(
    conn: psycopg.Connection[Any],
    *,
    job_name: str,
) -> int:
    """Return ``COALESCE(MAX(run_id), 0) FROM job_runs WHERE job_name = ...``.

    Used by ``_run_one_stage`` to bracket the ``job_runs`` window
    (see ``_resolve_stage_rows``). #1140 / Task C of #1136 audit.

    ``job_runs.run_id`` (BIGSERIAL PRIMARY KEY per sql/014) is the
    canonical id column — there is no ``id`` alias.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(run_id), 0) FROM job_runs WHERE job_name = %s",
            (job_name,),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


def _resolve_stage_rows(
    conn: psycopg.Connection[Any],
    *,
    bootstrap_run_id: int,
    stage_key: str,
    job_name: str,
    job_runs_id_before: int,
    job_runs_id_after: int,
) -> int | None:
    """Resolve ``rows_processed`` for a freshly-succeeded stage.

    Returns ``None`` when no side-channel has data. Resolution order:

    1. Per-archive ``bootstrap_archive_results`` (non-``__job__``). If
       ``COUNT > 0`` → return ``SUM(rows_written)``, preserving 0.
       Phase C ingester shape (e.g. ``sec_companyfacts_ingest``).
    2. ``__job__`` row with operator-set ``rows_written > 0``. Service-
       invoker shape (e.g. ``sec_submissions_files_walk`` overloads
       the provenance row with ``filings_upserted``). The default
       orchestrator-written ``__job__`` row carries ``rows_written=0``
       (with the new ``record_archive_result_if_absent`` it only fires
       when the service invoker didn't already write); a ``0`` here
       falls through to source 3.
    3. ``job_runs.row_count`` for the latest matching run in the
       ``id > job_runs_id_before AND id <= job_runs_id_after`` window.
       The double bound pins to rows created while the dispatcher held
       ``JobLock`` for this stage; the upper bound rejects same-
       ``job_name`` scheduled fires that landed after lock release.

    #1140 / Task C of #1136 audit (spec at
    docs/superpowers/specs/2026-05-13-precondition-final-data-gates.md).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(rows_written), 0)
              FROM bootstrap_archive_results
             WHERE bootstrap_run_id = %s
               AND stage_key = %s
               AND archive_name <> '__job__'
            """,
            (bootstrap_run_id, stage_key),
        )
        row = cur.fetchone()
        archive_count = int(row[0]) if row else 0
        archive_sum = int(row[1]) if row and row[1] is not None else 0
        if archive_count > 0:
            return archive_sum

        cur.execute(
            """
            SELECT rows_written
              FROM bootstrap_archive_results
             WHERE bootstrap_run_id = %s
               AND stage_key = %s
               AND archive_name = '__job__'
            """,
            (bootstrap_run_id, stage_key),
        )
        row = cur.fetchone()
        if row is not None and row[0] is not None and int(row[0]) > 0:
            return int(row[0])

        cur.execute(
            """
            SELECT row_count
              FROM job_runs
             WHERE job_name = %s
               AND run_id > %s
               AND run_id <= %s
               AND status  = 'success'
             ORDER BY run_id DESC
             LIMIT 1
            """,
            (job_name, job_runs_id_before, job_runs_id_after),
        )
        row = cur.fetchone()
        if row is not None and row[0] is not None:
            return int(row[0])
    return None


def _run_one_stage(
    *,
    run_id: int,
    stage_key: str,
    job_name: str,
    invoker: Callable[[Mapping[str, Any]], None],
    database_url: str,
    params: Mapping[str, Any] | None = None,
) -> _StageOutcome:
    """Execute one stage end-to-end with `JobLock` + bookkeeping.

    Exceptions inside the invoker are caught and recorded as
    ``error`` so the lane can proceed to the next stage. The only
    exceptions that escape this function are programmer errors
    (e.g. the bookkeeping query fails) — those propagate so the
    orchestrator surfaces them, but the lane runner catches a broad
    ``Exception`` to keep going.

    PR1c #1064: ``params`` carries the dispatcher-supplied stage
    params dict (from ``StageSpec.params`` after dynamic-value
    resolution + validation). The invoker consumes them through the
    widened ``JobInvoker`` contract; bootstrap-only knobs like
    ``min_period_of_report`` and ``source_label`` flow through here
    instead of living inside bespoke wrapper bodies. Default
    ``None`` = empty dict for backwards compat with any direct
    test caller that hasn't migrated.
    """
    effective_params: Mapping[str, Any] = params if params is not None else {}
    with psycopg.connect(database_url) as conn:
        mark_stage_running(conn, run_id=run_id, stage_key=stage_key)
        conn.commit()

    # #1140 Task C — snapshot the job_runs MAX(id) BEFORE acquiring
    # ``JobLock`` so the row resolver can pin its fallback window to
    # rows created during this stage's run. ``job_runs_id_after`` is
    # captured INSIDE the lock (after the invoker returns, before lock
    # release) so a same-``job_name`` scheduled fire that lands after
    # release cannot pollute the pick.
    with psycopg.connect(database_url) as conn:
        job_runs_id_before = _snapshot_job_runs_max_id(conn, job_name=job_name)
    job_runs_id_after = job_runs_id_before  # set below; default if invoker raises

    # PR1c #1064 (Codex pre-push WARNING): the promoted scheduler-side
    # invokers call ``_tracked_job`` which reads ``_params_snapshot_var``
    # via ``consume_params_snapshot()`` to populate
    # ``job_runs.params_snapshot``. Bootstrap dispatch bypasses
    # ``run_with_prelude``'s contextvar set, so we plumb the snapshot
    # here. Without this, stage 21's audit row would persist ``{}``
    # even though the body executed with a real ``min_period_of_report``
    # cutoff + ``source_label`` override.
    from app.jobs.runtime import _params_snapshot_var

    # PR3d #1064 — expose the cancel signal to long-running invokers.
    # Stages with multi-minute loops poll
    # ``bootstrap_cancel_requested()`` and raise
    # ``BootstrapStageCancelled`` to bail out cooperatively. The
    # context manager scopes the contextvar so scheduled / manual
    # triggers of the same job (outside bootstrap) see no signal.
    from app.services.bootstrap_state import BootstrapStageCancelled, mark_stage_cancelled
    from app.services.processes.bootstrap_cancel_signal import active_bootstrap_run

    try:
        with JobLock(database_url, job_name), active_bootstrap_run(run_id, stage_key):
            snap_token = _params_snapshot_var.set(effective_params)
            try:
                invoker(effective_params)
            finally:
                _params_snapshot_var.reset(snap_token)
            # #1140 Task C — capture the upper bound while still
            # holding ``JobLock``. Any job_runs row created here must
            # belong to our invocation; same-source serialisation
            # prevents a parallel same-job_name fire from sneaking in.
            #
            # Wrapped in try/except so a snapshot failure (transient DB
            # blip, pool exhausted) does NOT mark a successful invoker
            # as error (Codex pre-push round 2 WARNING). The resolver
            # falls back to ``job_runs_id_before`` window (which is the
            # same value as ``job_runs_id_after`` initialised above) →
            # an empty window → ``rows_processed = None``. The stage
            # still records ``success``; cap-eval handles the None per
            # the strict-gate rule.
            try:
                with psycopg.connect(database_url) as snap_conn:
                    job_runs_id_after = _snapshot_job_runs_max_id(snap_conn, job_name=job_name)
            except Exception as snap_exc:  # noqa: BLE001 — snapshot is best-effort
                logger.warning(
                    "bootstrap stage %s: failed to capture job_runs_id_after: %s",
                    stage_key,
                    snap_exc,
                )
    except JobAlreadyRunning:
        message = (
            f"another instance of {job_name!r} holds the advisory lock; "
            "retry from the bootstrap panel after the other run completes"
        )
        with psycopg.connect(database_url) as conn:
            mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message)
    except BootstrapStageCancelled as exc:
        # PR3d #1064 — operator clicked Cancel mid-stage; the invoker
        # observed the signal at one of its checkpoints and bailed
        # out. Mark the stage ``cancelled`` (not ``error``) so the
        # Timeline tones gray. The next dispatcher iteration's
        # run-level cancel checkpoint terminalises remaining stages.
        message = str(exc) or "stage cancelled by operator"
        logger.info(
            "bootstrap stage %s observed cancel signal; marking cancelled (%s)",
            stage_key,
            message,
        )
        with psycopg.connect(database_url) as conn:
            mark_stage_cancelled(conn, run_id=run_id, stage_key=stage_key, reason=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message, cancelled=True)
    except BootstrapPhaseSkipped as exc:
        # Operator-policy skip: A3 wrote a fallback manifest because
        # bandwidth was below threshold, and the legacy chain handles
        # ingest. Mark the stage `skipped` so finalize_run does NOT
        # count it as a failure (#1041).
        message = f"skipped: {exc}"
        logger.info("bootstrap stage %s skipped: %s", stage_key, exc)
        with psycopg.connect(database_url) as conn:
            mark_stage_skipped(conn, run_id=run_id, stage_key=stage_key, reason=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=True, error=None, skipped=True)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        logger.exception("bootstrap stage %s raised; lane continues", stage_key)
        with psycopg.connect(database_url) as conn:
            mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=message)
            conn.commit()
        return _StageOutcome(stage_key=stage_key, success=False, error=message)

    # Auto-record the __job__ row in bootstrap_archive_results so
    # downstream stages can verify provenance via the precondition
    # checker. C-stages write their own per-archive rows; this catches
    # the B-stages and any other invoker that doesn't self-record.
    #
    # #1140 Task C — uses ``record_archive_result_if_absent`` (ON
    # CONFLICT DO NOTHING) so a service invoker that already wrote
    # ``__job__`` with a real ``rows_written`` count (e.g.
    # ``sec_submissions_files_walk`` overloads the provenance row
    # with ``filings_upserted``) is preserved. The pre-1140 helper
    # was last-write-wins which clobbered the invoker's value back
    # to 0.
    from app.services.bootstrap_preconditions import record_archive_result_if_absent

    with psycopg.connect(database_url) as conn:
        try:
            record_archive_result_if_absent(
                conn,
                bootstrap_run_id=run_id,
                stage_key=stage_key,
                archive_name="__job__",
                rows_written=0,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — auditing must not fail the stage
            logger.warning(
                "bootstrap stage %s: failed to record __job__ result: %s",
                stage_key,
                exc,
            )

    # #1140 Task C — resolve rows_processed from the side-channels and
    # commit it onto the stage row so the cap-eval layer + the
    # operator panel aggregate read real numbers instead of NULL.
    resolved_rows: int | None = None
    try:
        with psycopg.connect(database_url) as conn:
            resolved_rows = _resolve_stage_rows(
                conn,
                bootstrap_run_id=run_id,
                stage_key=stage_key,
                job_name=job_name,
                job_runs_id_before=job_runs_id_before,
                job_runs_id_after=job_runs_id_after,
            )
    except Exception as exc:  # noqa: BLE001 — auditing must not fail the stage
        logger.warning(
            "bootstrap stage %s: failed to resolve rows_processed: %s",
            stage_key,
            exc,
        )

    with psycopg.connect(database_url) as conn:
        mark_stage_success(
            conn,
            run_id=run_id,
            stage_key=stage_key,
            rows_processed=resolved_rows,
        )
        conn.commit()
    return _StageOutcome(
        stage_key=stage_key,
        success=True,
        error=None,
        rows_processed=resolved_rows,
    )


def _should_run(stage_status: str) -> bool:
    """Pre-check from the stage execution contract.

    On a fresh run every stage starts ``pending`` and runs. On a
    retry-failed pass, stages already in ``success`` are skipped so
    we touch only the affected stages.
    """
    return stage_status != "success"


# ---------------------------------------------------------------------------
# Lane runners
# ---------------------------------------------------------------------------


def _run_lane(
    *,
    run_id: int,
    lane_specs: Sequence[tuple[str, str, str, str, Callable[[Mapping[str, Any]], None]]],
    database_url: str,
    log_label: str,
) -> None:
    """Run a sequence of stages serially within a single lane.

    ``lane_specs`` is a sequence of
    ``(stage_key, job_name, lane, current_status, invoker)`` tuples.
    """
    logger.info("bootstrap %s lane: starting (%d stages)", log_label, len(lane_specs))
    for stage_key, job_name, _lane, status, invoker in lane_specs:
        if not _should_run(status):
            logger.info("bootstrap %s lane: skipping %s (already %s)", log_label, stage_key, status)
            continue
        outcome = _run_one_stage(
            run_id=run_id,
            stage_key=stage_key,
            job_name=job_name,
            invoker=invoker,
            database_url=database_url,
        )
        if outcome.success:
            logger.info("bootstrap %s lane: %s OK", log_label, stage_key)
        else:
            logger.warning("bootstrap %s lane: %s ERROR (%s)", log_label, stage_key, outcome.error)
    logger.info("bootstrap %s lane: done", log_label)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


@dataclass
class _RunnableStage:
    stage_key: str
    job_name: str
    lane: str
    invoker: Callable[[Mapping[str, Any]], None]
    # #1138 Task A — requires is now a CapRequirement (DNF over named
    # capabilities) instead of a tuple of predecessor stage keys.
    requires: CapRequirement = field(default_factory=CapRequirement)
    # PR1c #1064: per-stage params dict from ``StageSpec.params``.
    # Default empty so existing test fixtures that build
    # ``_RunnableStage`` directly without params keep working.
    params: Mapping[str, Any] = field(default_factory=dict)


def _phase_batched_dispatch(
    *,
    run_id: int,
    runnable: list[_RunnableStage],
    database_url: str,
    preexisting_statuses: dict[str, str] | None = None,
    preexisting_rows_processed: dict[str, int | None] | None = None,
    provides_map: Mapping[str, tuple[Capability, ...]] | None = None,
    provides_on_skip_map: Mapping[str, tuple[Capability, ...]] | None = None,
    min_rows_map: Mapping[Capability, int] | None = None,
    exclusions_map: Mapping[Capability, frozenset[str]] | None = None,
) -> tuple[dict[str, str], bool]:
    """Dispatch ``runnable`` stages in phase-batched fashion with lane concurrency.

    Returns a tuple ``(statuses, cancelled)``:

    * ``statuses`` — ``{stage_key: terminal_status}`` (success / error /
      blocked / skipped) for every input stage.
    * ``cancelled`` — True if the dispatcher exited early due to an
      observed cooperative-cancel signal at a checkpoint. The caller
      uses this to skip ``finalize_run`` (the run is already in the
      terminal ``cancelled`` state).

    Algorithm:

      1. Build per-stage status map (initially ``pending``).
      2. **Cancel checkpoint** — at the top of each iteration check
         ``is_stop_requested`` against ``(target_run_kind='bootstrap_run',
         target_run_id=run_id)``. On observed cancel: mark stop
         request observed, call ``mark_run_cancelled`` (terminalises
         run + state + sweeps remaining stages), mark stop request
         completed, and return early. This is the operator-cancel
         observation point per spec §Cancel semantics — cooperative.
      3. While any stage is pending: compute the satisfied-capability
         set from current stage statuses (via ``_STAGE_PROVIDES`` +
         ``_STAGE_PROVIDES_ON_SKIP``; #1138 Task A). For each pending
         stage:
         * If its ``CapRequirement`` is satisfied → "ready batch".
         * If unsatisfiable AND any contributing cap is error-dead
           (some provider in ``error``/``blocked``/``cancelled``) →
           propagate to ``blocked`` with structured "missing capability"
           reason.
         * If unsatisfiable AND every contributing dead cap is
           skip-only (providers only ``skipped`` without explicit
           ``provides_on_skip``) → cascade to ``skipped`` (no
           invocation). This is the slow-connection-fallback path.
         * Else → leave as ``pending`` and wait for upstream
           ``pending``/``running`` providers to terminalise.
      4. Group the ready batch by lane. For each lane, run up to
         ``_LANE_MAX_CONCURRENCY[lane]`` stages concurrently via a
         per-lane ``ThreadPoolExecutor``.
      5. Join all lane workers; refresh status from the DB; loop.
      6. Stop when no stage is pending.

    Stages with no `requires` start in the first batch. The dispatcher
    is fully data-driven by ``_STAGE_REQUIRES_CAPS`` (capability-based
    DNF dependency graph; #1138 Task A) + ``_STAGE_LANE_OVERRIDES``.

    Cancel observation latency: at most the duration of the longest
    in-flight batch (a 13F sweep is ~30 min; CIK refresh ~30s).
    Mid-stage work runs to completion — the watermark advances on
    commit and the next Iterate resumes from there.
    """
    from concurrent.futures import ThreadPoolExecutor, wait

    # #1138 Task A — cap-evaluation map overrides. Production callers
    # omit these (the module-level _STAGE_PROVIDES / _STAGE_PROVIDES_ON_SKIP
    # are used). Test fixtures can pass overrides to register synthetic
    # caps for fixture stage_keys. The inverse map is rebuilt from the
    # union of base + override.
    effective_provides: Mapping[str, tuple[Capability, ...]] = (
        {**_STAGE_PROVIDES, **provides_map} if provides_map else _STAGE_PROVIDES
    )
    effective_provides_on_skip: Mapping[str, tuple[Capability, ...]] = (
        {**_STAGE_PROVIDES_ON_SKIP, **provides_on_skip_map} if provides_on_skip_map else _STAGE_PROVIDES_ON_SKIP
    )
    effective_min_rows: Mapping[Capability, int] = (
        {**_CAPABILITY_MIN_ROWS, **min_rows_map} if min_rows_map else _CAPABILITY_MIN_ROWS
    )
    effective_exclusions: Mapping[Capability, frozenset[str]] = (
        {**_STRICT_CAP_PROVIDER_EXCLUSIONS, **exclusions_map} if exclusions_map else _STRICT_CAP_PROVIDER_EXCLUSIONS
    )
    if provides_map or provides_on_skip_map:
        effective_providers_inverse: Mapping[Capability, tuple[str, ...]] = _build_capability_providers(
            effective_provides, effective_provides_on_skip
        )
    else:
        effective_providers_inverse = _CAPABILITY_PROVIDERS

    by_key = {r.stage_key: r for r in runnable}
    statuses: dict[str, str] = {r.stage_key: "pending" for r in runnable}
    # #1140 Task C — parallel dict tracking rows_processed for each
    # stage. Seeded from preexisting terminal stages (so a retry-pass
    # respects what a prior pass wrote) and updated from each
    # _StageOutcome as stages complete. The cap-eval helpers consult
    # this to decide whether strict-gate caps are satisfied.
    rows_processed: dict[str, int | None] = {r.stage_key: None for r in runnable}
    # Merge in upstream stages already in a terminal state so the
    # dependency check sees them.
    if preexisting_statuses:
        for key, status in preexisting_statuses.items():
            if key not in statuses:
                statuses[key] = status
    if preexisting_rows_processed:
        for key, value in preexisting_rows_processed.items():
            rows_processed[key] = value

    while True:
        # Cancel checkpoint — covers (W1) "before submitting Phase A's
        # first batch", "between any two ready batches", "before
        # kicking off Phase B lanes", and "between stages within a
        # lane" (the loop body re-enters here after every wait()).
        #
        # Single-tx atomicity (Codex pre-push round 1 WARNING W4):
        # observation, cancellation, and stop-completion all commit
        # together. A worker crash between two of three separate
        # commits would otherwise leave the stop row observed-but-
        # unfinished with the run still ``running``. Boot-recovery
        # would still clean up after the next jobs restart, but
        # collapsing into one tx makes the happy path clean.
        with psycopg.connect(database_url) as cancel_conn:
            stop = is_stop_requested(
                cancel_conn,
                target_run_kind="bootstrap_run",
                target_run_id=run_id,
            )
            if stop is not None:
                logger.info(
                    "bootstrap dispatcher: cancel observed at checkpoint (run_id=%d, stop_id=%d, mode=%s)",
                    run_id,
                    stop.id,
                    stop.mode,
                )
                with cancel_conn.transaction():
                    mark_stop_observed(cancel_conn, stop.id)
                    mark_run_cancelled(
                        cancel_conn,
                        run_id=run_id,
                        notes_line="cancelled by operator at dispatcher checkpoint",
                    )
                    mark_stop_completed(cancel_conn, stop.id)
                return statuses, True

        pending_keys = [k for k, s in statuses.items() if s == "pending"]
        if not pending_keys:
            break

        # #1138 Task A — capability-based runnability + cascade-skip /
        # error-block classification. Compute the satisfied cap set
        # once per dispatcher iteration, then decide per pending
        # stage whether to (a) dispatch, (b) block with structured
        # reason, (c) cascade-skip with structured reason, or (d)
        # wait for upstream pending/running providers.
        caps = _satisfied_capabilities(
            statuses,
            rows_processed,
            provides=effective_provides,
            provides_on_skip=effective_provides_on_skip,
            min_rows=effective_min_rows,
            exclusions=effective_exclusions,
        )
        ready: list[_RunnableStage] = []
        cascade_transitioned = False
        for key in pending_keys:
            stage = by_key[key]
            req = stage.requires
            if _requirement_satisfied(req, caps):
                ready.append(stage)
                continue
            classification = _classify_requirement_unsatisfiable(
                req,
                statuses,
                rows_processed,
                providers_map=effective_providers_inverse,
                provides_on_skip=effective_provides_on_skip,
                min_rows=effective_min_rows,
                exclusions=effective_exclusions,
            )
            if classification is None:
                continue  # still potentially satisfiable; wait
            kind, dead_caps = classification
            if kind == "error":
                reason = _format_block_reason(
                    dead_caps,
                    statuses,
                    rows_processed,
                    providers_map=effective_providers_inverse,
                    min_rows=effective_min_rows,
                    exclusions=effective_exclusions,
                )
                with psycopg.connect(database_url) as conn:
                    mark_stage_blocked(
                        conn,
                        run_id=run_id,
                        stage_key=key,
                        reason=reason,
                    )
                    conn.commit()
                statuses[key] = "blocked"
                cascade_transitioned = True
                logger.warning("bootstrap dispatcher: %s BLOCKED (%s)", key, reason)
            else:  # skip_only
                reason = _format_cascade_skip_reason(dead_caps)
                with psycopg.connect(database_url) as conn:
                    mark_stage_skipped(
                        conn,
                        run_id=run_id,
                        stage_key=key,
                        reason=reason,
                    )
                    conn.commit()
                statuses[key] = "skipped"
                cascade_transitioned = True
                logger.info("bootstrap dispatcher: %s SKIPPED (cascade: %s)", key, reason)

        # Codex pre-push WARNING: if a stage was cascade-blocked or
        # cascade-skipped within this inner loop, an EARLIER pending
        # key may have been evaluated against a still-pending upstream
        # and left as pending. Restart the outer loop so caps are
        # recomputed and the now-terminalised upstream propagates to
        # those downstream pendings instead of dropping them into the
        # "deadlock" branch below.
        if cascade_transitioned and not ready:
            continue

        if not ready:
            # No stage advanced this iteration. Any stage still in
            # `pending` means its requirements are stuck (e.g. all
            # in unmet_reqs) — the dispatcher cannot make progress.
            # Mark them blocked so finalize_run sees a terminal state
            # and the operator panel doesn't show "pending forever".
            # Codex review BLOCKING (PR #1039).
            stuck_keys = [k for k, s in statuses.items() if s == "pending"]
            for key in stuck_keys:
                with psycopg.connect(database_url) as conn:
                    mark_stage_blocked(
                        conn,
                        run_id=run_id,
                        stage_key=key,
                        reason="dispatcher could not resolve dependencies; stage abandoned",
                    )
                    conn.commit()
                statuses[key] = "blocked"
                logger.warning(
                    "bootstrap dispatcher: %s ABANDONED (deadlock in dependency graph)",
                    key,
                )
            break

        # Group ready by lane. Per-lane, dispatch only up to
        # ``max_concurrency`` stages in this iteration — the rest stay
        # pending and roll into the next iteration. This prevents a
        # long-running stage in one lane (e.g. sec_first_install_drain
        # in sec_rate) from blocking blocked-status propagation in
        # other lanes (e.g. db lane's C-stages waiting on a failed
        # sec_bulk_download). Without this cap, ``wait()`` blocks on
        # the entire heterogeneous batch, leaving the operator panel
        # showing C-stages as ``pending`` long after their upstream
        # has failed.
        by_lane_batch: dict[str, list[_RunnableStage]] = {}
        for stage in ready:
            by_lane_batch.setdefault(stage.lane, []).append(stage)

        # Cap each lane to its max_concurrency. Stages over the cap
        # stay in `pending` and re-enter the next outer iteration.
        for lane, stages in list(by_lane_batch.items()):
            cap = _LANE_MAX_CONCURRENCY.get(lane, 1)
            by_lane_batch[lane] = stages[:cap]

        logger.info(
            "bootstrap dispatcher: ready batch — %s",
            {lane: [s.stage_key for s in stages] for lane, stages in by_lane_batch.items()},
        )

        # One ThreadPoolExecutor per lane, sized to lane's concurrency.
        # Lanes run concurrently with each other; within a lane,
        # the cap above ensures we submit no more than max_concurrency.
        lane_executors: list[ThreadPoolExecutor] = []
        all_futures = []
        try:
            for lane, stages in by_lane_batch.items():
                max_concurrency = _LANE_MAX_CONCURRENCY.get(lane, 1)
                ex = ThreadPoolExecutor(
                    max_workers=max_concurrency,
                    thread_name_prefix=f"bootstrap-{lane}",
                )
                lane_executors.append(ex)
                for stage in stages:
                    # PR1c #1064: resolve dispatch-time dynamic values
                    # (e.g. _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF →
                    # ``today() - 380d``) and validate via the canonical
                    # registry validator with allow_internal_keys=True
                    # so audit-only keys (``source_label``) pass through.
                    # Validation failure is a programmer error in the
                    # stage spec — fail-fast, dispatch surfaces it as a
                    # stage error.
                    resolved = _resolve_dynamic_params(stage.params)
                    try:
                        validated_params = validate_job_params(
                            stage.job_name,
                            resolved,
                            allow_internal_keys=True,
                        )
                    except ParamValidationError as exc:
                        logger.error(
                            "bootstrap dispatcher: stage %s params invalid: %s",
                            stage.stage_key,
                            exc,
                        )
                        with psycopg.connect(database_url) as conn:
                            mark_stage_error(
                                conn,
                                run_id=run_id,
                                stage_key=stage.stage_key,
                                error_message=f"stage params invalid: {exc}",
                            )
                            conn.commit()
                        statuses[stage.stage_key] = "error"
                        continue

                    fut = ex.submit(
                        _run_one_stage,
                        run_id=run_id,
                        stage_key=stage.stage_key,
                        job_name=stage.job_name,
                        invoker=stage.invoker,
                        database_url=database_url,
                        params=validated_params,
                    )
                    all_futures.append((stage.stage_key, fut))
            wait([f for _, f in all_futures])
        finally:
            for ex in lane_executors:
                ex.shutdown(wait=True)

        for stage_key, fut in all_futures:
            outcome = fut.result()
            # #1140 Task C — refresh the per-stage row count from the
            # outcome so the next dispatcher iteration's cap-eval
            # reads it.
            rows_processed[stage_key] = outcome.rows_processed
            if outcome.skipped:
                statuses[stage_key] = "skipped"
                logger.info("bootstrap dispatcher: %s SKIPPED", stage_key)
            elif outcome.cancelled:
                # PR3d #1064 — stage observed operator cancel
                # mid-loop and exited cooperatively. Status maps to
                # 'cancelled' so the Timeline tones gray; the
                # outer cancel checkpoint at the next loop iteration
                # picks up the run-level cancel signal and sweeps
                # remaining stages.
                statuses[stage_key] = "cancelled"
                logger.info("bootstrap dispatcher: %s CANCELLED (%s)", stage_key, outcome.error)
            elif outcome.success:
                statuses[stage_key] = "success"
                logger.info(
                    "bootstrap dispatcher: %s OK (rows_processed=%s)",
                    stage_key,
                    outcome.rows_processed,
                )
            else:
                statuses[stage_key] = "error"
                logger.warning("bootstrap dispatcher: %s ERROR (%s)", stage_key, outcome.error)

    return statuses, False


def run_bootstrap_orchestrator() -> None:
    """``_INVOKERS['bootstrap_orchestrator']`` — drive a queued run
    via lane-aware phase-batched dispatch (#1020).

    Replaces the prior "init thread + 2 lane threads" model with a
    data-driven dependency-graph dispatcher: stages declare
    ``requires`` via ``_STAGE_REQUIRES_CAPS`` (a CapRequirement DNF
    over named capabilities; #1138 Task A); dispatcher fans out ready
    batches respecting per-lane ``max_concurrency``.
    """
    # Lazy import: app.jobs.runtime imports app.services.bootstrap_orchestrator
    # via the orchestrator job invoker registration, and importing back the
    # other way at module load would be a circular import.
    from app.jobs.runtime import _INVOKERS

    database_url = settings.database_url

    with psycopg.connect(database_url) as conn:
        snapshot = read_latest_run_with_stages(conn)
    if snapshot is None:
        logger.error("bootstrap_orchestrator: no bootstrap_runs row found; nothing to do")
        return
    run_id = snapshot.run_id
    if snapshot.run_status != "running":
        logger.info(
            "bootstrap_orchestrator: latest run %d is %r; nothing to do",
            run_id,
            snapshot.run_status,
        )
        return

    # PR1c #1064 — build a stage_key → params lookup from the static
    # ``_BOOTSTRAP_STAGE_SPECS`` so the per-stage params dict can be
    # plumbed into ``_RunnableStage`` below. ``bootstrap_stages`` in
    # DB doesn't store params (immutable across runs; lives in code),
    # so the dispatch path consults the spec table at run time.
    stage_params_by_key: dict[str, Mapping[str, Any]] = {spec.stage_key: spec.params for spec in _BOOTSTRAP_STAGE_SPECS}

    # Pre-populate statuses with stages already in a terminal state
    # so the dependency graph sees them when a downstream pending
    # stage's `requires` references them. Without this, a retry pass
    # could treat an upstream `error`/`blocked` row as satisfied
    # because that upstream was filtered out of `runnable`. Codex
    # review BLOCKING for #1020 PR2.
    preexisting_statuses: dict[str, str] = {}
    # #1140 Task C — seed rows_processed for preexisting terminal
    # stages so the cap-eval layer can read them on retry passes.
    # ``StageRow.rows_processed`` is already projected by
    # ``read_latest_run_with_stages``.
    preexisting_rows_processed: dict[str, int | None] = {}
    runnable: list[_RunnableStage] = []
    for stage in sorted(snapshot.stages, key=lambda s: s.stage_order):
        # Skip stages already in a terminal state (re-runs); record
        # their status so dispatch dependency checks see them.
        if stage.status in ("success", "error", "blocked", "skipped", "cancelled"):
            preexisting_statuses[stage.stage_key] = stage.status
            preexisting_rows_processed[stage.stage_key] = stage.rows_processed
            logger.info("bootstrap dispatcher: skipping %s (already %s)", stage.stage_key, stage.status)
            continue
        invoker = _INVOKERS.get(stage.job_name)
        if invoker is None:
            logger.error(
                "bootstrap dispatcher: stage %s has unknown job_name %r; marking error",
                stage.stage_key,
                stage.job_name,
            )
            with psycopg.connect(database_url) as conn:
                mark_stage_running(conn, run_id=run_id, stage_key=stage.stage_key)
                mark_stage_error(
                    conn,
                    run_id=run_id,
                    stage_key=stage.stage_key,
                    error_message=f"unknown job_name {stage.job_name!r}",
                )
                conn.commit()
            continue
        runnable.append(
            _RunnableStage(
                stage_key=stage.stage_key,
                job_name=stage.job_name,
                lane=_effective_lane(stage.stage_key, stage.lane),
                invoker=invoker,
                requires=_STAGE_REQUIRES_CAPS.get(stage.stage_key, CapRequirement()),
                params=stage_params_by_key.get(stage.stage_key, {}),
            )
        )

    logger.info(
        "bootstrap dispatcher: run_id=%d runnable=%d (lane breakdown: %s)",
        run_id,
        len(runnable),
        {lane: sum(1 for r in runnable if r.lane == lane) for lane in _LANE_MAX_CONCURRENCY},
    )

    _statuses, cancelled = _phase_batched_dispatch(
        run_id=run_id,
        runnable=runnable,
        database_url=database_url,
        preexisting_statuses=preexisting_statuses,
        preexisting_rows_processed=preexisting_rows_processed,
    )

    if cancelled:
        # The cancel checkpoint already terminalised the run via
        # mark_run_cancelled; finalize_run would no-op against the
        # status='running' guard, but skipping it is clearer.
        logger.info("bootstrap dispatcher: run_id=%d cancelled by operator", run_id)
        return

    with psycopg.connect(database_url) as conn:
        terminal = finalize_run(conn, run_id=run_id)
    logger.info("bootstrap dispatcher: run_id=%d finalised as %s", run_id, terminal)


# PR1c #1064 — three bespoke wrappers
# (``bootstrap_filings_history_seed``, ``sec_first_install_drain_job``,
# ``bootstrap_sec_13f_recent_sweep_job``) deleted. Their bodies were
# lifted into params-aware ``JobInvoker`` bodies in
# ``app/workers/scheduler.py`` (``filings_history_seed``,
# ``sec_first_install_drain``, extended ``sec_13f_quarterly_sweep``).
# Bootstrap stages 14, 15, 21 dispatch the promoted bodies via
# ``StageSpec.params``; the deleted JOB_* constants are gone too,
# so any straggling reference fails fast on import.


__all__ = [
    "JOB_BOOTSTRAP_ORCHESTRATOR",
    "JOB_DAILY_CIK_REFRESH",
    "JOB_DAILY_FINANCIAL_FACTS",
    "get_bootstrap_stage_specs",
    "run_bootstrap_orchestrator",
]


# Stage count assertion — pin so a future refactor that adds /
# removes a spec deliberately surfaces in code review and doesn't
# silently break the tests + frontend + runbook that hardcode the
# current 24-stage shape.
assert len(_BOOTSTRAP_STAGE_SPECS) == 24, (
    f"_BOOTSTRAP_STAGE_SPECS expected 24 stages, got {len(_BOOTSTRAP_STAGE_SPECS)}; "
    "update the spec, frontend, runbook, and stage_count tests in lockstep. "
    "#1027 added 7 bulk-archive stages (sec_bulk_download + C1.a/C2/C3/C4/C5 ingesters + C1.b walker)."
)
