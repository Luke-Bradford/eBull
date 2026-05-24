"""First-install bootstrap orchestrator.

Runs the 26-stage end-to-end first-install backfill described in
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
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
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
# #1174 — dedicated MF directory refresh + N-CSR fund-scoped bootstrap drain.
JOB_MF_DIRECTORY_SYNC = "mf_directory_sync"
JOB_SEC_N_CSR_BOOTSTRAP_DRAIN = "sec_n_csr_bootstrap_drain"
# #1233 PR-1b — OpenFIGI CUSIP resolver post-bulk sweep (Phase D, S13).
# Owns the ``openfigi`` Lane (cap=1). Invoker registered in
# ``app/jobs/runtime.py`` alongside the other bootstrap-only jobs.
JOB_CUSIP_RESOLVER_POST_BULK_SWEEP = "cusip_resolver_post_bulk_sweep"
# PR1c #1064 — bootstrap-bounded 13F sweep recency cut-off. Used to
# live as a constant inside the deleted ``bootstrap_sec_13f_recent_sweep_job``
# wrapper. 4 quarters (~380 days) = current + 3 prior periods, matches
# the rolling ownership-card window. Older 13Fs add no value to
# current-quarter ranking and pre-2013 ones don't have machine-readable
# holdings (#1008).
_BOOTSTRAP_13F_QUARTERS_BACK = 4
_BOOTSTRAP_13F_RECENCY_DAYS = _BOOTSTRAP_13F_QUARTERS_BACK * 95

# PR7 #1233 §4.6 — N-PORT cohort recency window. Aliases to the 13F
# 380d window today (#1010 precedent) but lives in its own constant
# so a future 13F-only tuning of ``_BOOTSTRAP_13F_RECENCY_DAYS``
# doesn't silently drift the N-PORT cohort cutoff. Bot review on PR
# #1243 WARNING — keep the two namespaced even when their numerical
# value is identical.
_BOOTSTRAP_NPORT_RECENCY_DAYS = _BOOTSTRAP_13F_RECENCY_DAYS

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
# #1010 — sibling sentinel for the HR-recency cohort cutoff on stage
# 21. Filters ``institutional_filers.last_13f_hr_at >= cutoff`` so the
# sweep iterates only currently-active HR filers.
_PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF = "<dynamic:bootstrap_13f_hr_cutoff>"
# #1233 PR7 (mirror of #1010 for N-PORT) — recency cohort cutoff for
# stage 22 sec_n_port_ingest. Filters
# ``sec_nport_filer_directory.last_seen_filed_at >= cutoff`` so the
# bootstrap sweep iterates only currently-active fund-trust filers.
# Daily / Admin / manual paths dispatch with empty params → full
# cohort (#1010 precedent — safety-net for re-emerging filers).
_PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF = "<dynamic:bootstrap_nport_cutoff>"


def _resolve_dynamic_params(params: Mapping[str, Any]) -> dict[str, Any]:
    """Materialise dispatch-time dynamic values in a stage params dict.

    Today the dynamic values are the bootstrap-13F recency cutoffs
    (one for ``min_period_of_report``, one for ``min_last_13f_hr_at``).
    The helper is structured for forward extensibility (additional
    sentinels can be added without touching call sites).

    The dispatcher calls this immediately before invoking the
    underlying ``JobInvoker`` so the absolute value is what flows
    into ``job_runs.params_snapshot`` and the invoker body.
    """
    resolved: dict[str, Any] = dict(params)
    if resolved.get("min_period_of_report") == _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF:
        resolved["min_period_of_report"] = date.today() - timedelta(days=_BOOTSTRAP_13F_RECENCY_DAYS)
    if resolved.get("min_last_13f_hr_at") == _PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF:
        # UTC start-of-day so the boundary is inclusive against
        # ``form.idx`` dates which are stored at midnight UTC. Two
        # subtle pitfalls avoided here:
        #   (1) using the full timestamp (no ``.date()`` truncation)
        #       would drift the cutoff during the day and exclude
        #       filings whose form.idx date is exactly ``today() -
        #       380d`` (Codex 1a B-3); (2) ``date.today()`` returns
        #       *local* time — on a non-UTC dev host around the
        #       local/UTC date boundary it would shift the cutoff by
        #       ±1 day and silently exclude an exact-boundary UTC
        #       filer (Codex 2 MEDIUM). Take the UTC date explicitly.
        cutoff_date = datetime.now(tz=UTC).date() - timedelta(days=_BOOTSTRAP_13F_RECENCY_DAYS)
        resolved["min_last_13f_hr_at"] = datetime.combine(cutoff_date, time(0, 0), tzinfo=UTC)
    if resolved.get("min_last_seen_filed_at") == _PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF:
        # PR7 #1233 §4.6 — mirror of the 13F HR-cutoff resolution.
        # UTC start-of-day so the boundary is inclusive against
        # ``sec_nport_filer_directory.last_seen_filed_at`` (stored at
        # midnight UTC by ``sec_nport_filer_directory_sync``). The
        # 380d window aliases the 13F figure today but lives behind
        # the dedicated ``_BOOTSTRAP_NPORT_RECENCY_DAYS`` constant so
        # a future 13F-only tuning doesn't silently drift the N-PORT
        # cohort cutoff (PR #1243 bot review WARNING).
        nport_cutoff_date = datetime.now(tz=UTC).date() - timedelta(days=_BOOTSTRAP_NPORT_RECENCY_DAYS)
        resolved["min_last_seen_filed_at"] = datetime.combine(nport_cutoff_date, time(0, 0), tzinfo=UTC)
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
    # Each lane serialises within itself; cross-lane is parallel.
    # PR1c #1064 collapsed Phase C onto a single ``db`` source +
    # ``db=1``, which retired #1020's parallel-DB-stage claim and
    # added ~4 h to first-install wall-clock (measured on
    # ``bootstrap_run_id=3``: 5 db-lane stages serial summed to 283
    # min vs 110 min if cross-source-parallel).
    #
    # #1141 / Task E of audit #1136 restores Phase C parallelism by
    # splitting the ``db`` source by table family — see
    # ``docs/superpowers/specs/2026-05-13-db-lane-family-split.md``.
    # The five family lanes below each carry exactly one stage; the
    # parallelism win is cross-lane, not intra-lane. ``db`` stays
    # the catch-all for Phase E derivations + scheduler ``db``-source
    # jobs (no change to their serialisation).
    "db": 1,
    "db_filings": 1,
    "db_fundamentals_raw": 1,
    "db_ownership_inst": 1,
    "db_ownership_insider": 1,
    "db_ownership_funds": 1,
    # #1233 PR-1b — OpenFIGI CUSIP resolver post-bulk sweep stage S13.
    # Single-stage lane (the only consumer is ``cusip_resolver_post_bulk_sweep``),
    # cap=1 so the per-instance ``OpenFigiResolver`` rate limiter
    # (``app/services/openfigi_resolver.py::_RateLimiter``) is the
    # canonical budget gate. Disjoint from every SEC lane by host —
    # see ``app/jobs/sources.py::Lane`` docstring for SD-1 cross-ref.
    "openfigi": 1,
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
    # #1174 — classId → instrument_id mapping + fund-trust directory.
    # Provided by S25 ``mf_directory_sync``; required by S26
    # ``sec_n_csr_bootstrap_drain``.
    "class_id_mapping_ready",
    # PR-2 lock-contention fix: S8 (sec_submissions_ingest) and S15
    # (filings_history_seed) both write to ``filing_events`` for the
    # same ``(instrument_id, …)`` keys. PR-2's cross-lane parallelism
    # let them run concurrently → row-level lock contention left S8
    # stuck on a wait-graph for 17+ min during bootstrap run #5.
    # Adding a hard ordering: S15 requires ``submissions_processed``
    # which only S8 provides (on success OR skip). Effect: S15 runs
    # AFTER S8 terminalises, eliminating concurrent writes.
    # Provided ON SKIP so the slow-connection fallback (#1041 — S7
    # skipped → S8 cascade-skipped) still flows into S15 as the
    # legacy chain owner of ``filing_events_seeded``.
    "submissions_processed",
    # #1233 — extension of the PR-1292 cap-ordering pattern to the
    # other bulk/legacy pairs flagged by the 2026-05-23 lock-contention
    # audit. Same shape:
    #
    #   * ``insider_dataset_processed`` — S11 sec_insider_ingest_from_dataset
    #     and S19 sec_insider_transactions_backfill + S20 sec_form3_ingest
    #     all write ``ownership_insiders_observations``. Without an
    #     ordering cap, PR-2 ``as_completed`` parallelism would let the
    #     bulk path and the legacy backfills run concurrently against
    #     overlapping (instrument_id, holder, observed_at) rows —
    #     identical row-lock storm shape as the S8↔S15 case PR-1292
    #     fixed.
    #
    #   * ``institutional_dataset_processed`` — S10 sec_13f_ingest_from_dataset
    #     and S22 sec_13f_recent_sweep both write
    #     ``ownership_institutions_observations``. Same pattern;
    #     largest write fanout of any audited pair (institutional 13F
    #     rows during a full bootstrap can total tens of millions).
    #
    # Both caps are PROVIDED on SUCCESS by their respective bulk
    # ingester and ON SKIP for cascade-skip parity. Required by the
    # legacy stages downstream so they serialise after the bulk
    # ingester terminalises.
    "insider_dataset_processed",
    "institutional_dataset_processed",
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
    "sec_submissions_ingest": ("filing_events_seeded", "submissions_processed"),
    "sec_companyfacts_ingest": ("fundamentals_raw_seeded",),
    # Bulk ownership ingester covers both insider transactions + Form 3.
    # #1233 lock-contention cap-gates: bulk ingesters advertise an
    # ordering cap on top of their content caps. Required by S19/S20
    # (insider) and S22 (institutional) so the legacy backfills wait
    # for the bulk path to terminalise. See ``insider_dataset_processed``
    # / ``institutional_dataset_processed`` docstrings.
    "sec_insider_ingest_from_dataset": (
        "insider_inputs_seeded",
        "form3_inputs_seeded",
        "insider_dataset_processed",
    ),
    "sec_13f_ingest_from_dataset": (
        "institutional_inputs_seeded",
        "institutional_dataset_processed",
    ),
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
    # #1174 — dedicated MF directory refresh advertises class_id_mapping_ready.
    # S26 ``sec_n_csr_bootstrap_drain`` is terminal (no provides entry).
    "mf_directory_sync": ("class_id_mapping_ready",),
}


# Per-stage caps provided on ``skipped`` status. Intentionally empty
# by default — skipped stages do NOT provide capabilities. The
# slow-connection fallback (#1041) relies on the *legacy* chain
# providing the same caps, not on a skipped bulk stage masquerading
# as a provider. Add an entry here only when a skip is semantically
# equivalent to success.
_STAGE_PROVIDES_ON_SKIP: Final[dict[str, tuple[Capability, ...]]] = {
    # PR-2 lock-contention fix: S8 cascade-skipped on slow-connection
    # (S7 skipped → bulk_archives_ready unprovided → S8 cascade-skipped)
    # still satisfies the ``submissions_processed`` ordering cap so
    # S15 (filings_history_seed) — the legacy chain owner of
    # filing_events seeding on the slow path — proceeds. S8 SUCCESS
    # also provides this cap (see ``_STAGE_PROVIDES`` above); the
    # SKIP entry covers the cascade-skip case without "masquerading
    # as success" — the cap is purely an ordering constraint on S15,
    # not a content-validity signal.
    "sec_submissions_ingest": ("submissions_processed",),
    # #1233 lock-contention cap-gates: same cascade-skip parity as
    # ``sec_submissions_ingest``. If S7 sec_bulk_download is skipped
    # on slow-connection fallback (#1041), S10/S11 cascade-skip too —
    # but the ordering cap still flows so the legacy ingesters
    # proceed without waiting on a bulk run that will never happen.
    # The cap is purely an ordering constraint; the cap-on-skip does
    # NOT masquerade as "content was ingested", same as the existing
    # ``submissions_processed`` cascade-skip entry.
    "sec_insider_ingest_from_dataset": ("insider_dataset_processed",),
    "sec_13f_ingest_from_dataset": ("institutional_dataset_processed",),
}


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
    # #1174 — refresh_mf_directory returns directory_rows=0 on an empty
    # or malformed mf.json without raising (fail-soft for the daily
    # cron path). Strict-gate floor of 1 ensures S25 success advertises
    # ``class_id_mapping_ready`` only when cik_refresh_mf_directory was
    # actually populated — Codex 2 BLOCKING.
    "class_id_mapping_ready": 1,
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


# #1233 lock-contention cap-gates — "ordering-only" caps that
# advertise "the upstream stage has terminalised, no concurrent
# writer remains" rather than "the upstream produced usable content".
# These caps are SATISFIED on ANY terminal status of their provider
# stage (success / skipped / blocked / error / cancelled), not just
# success or skip.
#
# Rationale: the row-lock storm shape PR-1292 fixed exists ONLY while
# the bulk stage is actively writing. Once the bulk stage has
# terminalised — for any reason, including a cascade-block from an
# earlier failure — the legacy downstream can write safely. Without
# this concession the cap-gate would FALSELY block the legacy
# recovery path during a partial-bulk-failure run
# (``test_partial_bulk_failure_legacy_recovers``).
#
# Content caps (``filing_events_seeded``, ``insider_inputs_seeded``,
# etc.) deliberately stay non-ordering: they must observe actual
# content writes to be satisfied. Ordering caps only need the
# upstream stage to be done.
_ORDERING_ONLY_CAPS: Final[frozenset[Capability]] = frozenset(
    {
        "submissions_processed",
        "insider_dataset_processed",
        "institutional_dataset_processed",
    }
)


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
    # Phase D — #1233 PR-1b. OpenFIGI sweep over the bulk-source rows
    # written by S10 + S12. Requires both bulk ingesters to have
    # advertised their caps so we know the unresolved_13f_cusips
    # bulk partition is in a settled state (no in-flight ingest
    # mutating it while the sweep iterates). Does NOT require
    # ``cusip_mapping_ready`` because the sweep's ENTIRE PURPOSE is
    # to extend the CUSIP mapping; a missing cusip_mapping cap means
    # S3 cusip_universe_backfill never ran, which is a different
    # failure mode — the sweep can still run safely.
    "cusip_resolver_post_bulk_sweep": CapRequirement(
        all_of=("institutional_inputs_seeded", "nport_inputs_seeded"),
    ),
    # Phase C' — walker
    "sec_submissions_files_walk": CapRequirement(all_of=("filing_events_seeded",)),
    # Legacy chain
    # ``submissions_processed`` cap pinned here serialises S15 after
    # S8 terminalises (success / skip). See cap docstring on
    # ``submissions_processed`` for the lock-contention rationale.
    "filings_history_seed": CapRequirement(all_of=("cik_mapping_ready", "submissions_processed")),
    "sec_first_install_drain": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_def14a_bootstrap": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_business_summary_bootstrap": CapRequirement(
        all_of=("filing_events_seeded", "submissions_secondary_pages_walked")
    ),
    # #1233 lock-contention cap-gates — same shape as PR-1292 for
    # S15↔S8. The legacy backfills (S19/S20) and the legacy 13F sweep
    # (S22) all write the same observation table their bulk
    # counterparts (S11 / S10) write. Without the ordering cap, PR-2
    # cross-lane parallelism lets them run concurrently and produces
    # the row-lock storm shape that killed bootstrap run #5. Caps are
    # ``*_dataset_processed`` (provided by S11 / S10 on success or
    # skip), so the legacy stages run AFTER the bulk path terminalises.
    "sec_insider_transactions_backfill": CapRequirement(all_of=("cik_mapping_ready", "insider_dataset_processed")),
    "sec_form3_ingest": CapRequirement(all_of=("cik_mapping_ready", "insider_dataset_processed")),
    "sec_8k_events_ingest": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_13f_recent_sweep": CapRequirement(all_of=("cik_mapping_ready", "institutional_dataset_processed")),
    "sec_n_port_ingest": CapRequirement(all_of=("cik_mapping_ready",)),
    # #1174 — dedicated MF directory refresh + N-CSR drain (S25 + S26).
    "mf_directory_sync": CapRequirement(all_of=("universe_seeded",)),
    "sec_n_csr_bootstrap_drain": CapRequirement(all_of=("class_id_mapping_ready",)),
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
    # Stream A PR-C1 T1.2 (#1233): strengthened from 1-cap to 4-cap
    # requirement. Pre-PR-C1, S25 only waited for ``fundamentals_raw_seeded``
    # (S9 companyfacts ingest). The audit-during-bootstrap defence in
    # T1.2's bootstrap entrypoint (lands in PR-C2) needs to know that
    # bulk archives + CIK mapping + S8 terminalisation are all done
    # too — otherwise ``audit_all_instruments`` would misclassify mid-
    # bootstrap and re-introduce HTTP backfill.
    #
    # Terminal-status safety: ``submissions_processed`` is in
    # ``_ORDERING_ONLY_CAPS`` (the frozenset defined just above), so
    # ``_satisfied_capabilities`` adds it on ``blocked|error|cancelled``
    # terminals + ``_capability_is_dead`` treats those terminal providers
    # as still satisfying ordering caps — this cap addition does NOT
    # create a stuck-S25 failure mode when S8 errors. Symbol references
    # over line refs (line numbers drift; Codex 2 LOW pre-merge review).
    #
    # Spec: docs/proposals/etl/stream-a-run-8-fixes.md v2.3 §13.
    "fundamentals_sync": CapRequirement(
        all_of=(
            "bulk_archives_ready",
            "cik_mapping_ready",
            "submissions_processed",
            "fundamentals_raw_seeded",
        ),
    ),
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
        elif status in ("blocked", "error", "cancelled"):
            # #1233 — ordering-only caps advertise "upstream is done"
            # regardless of how it ended. Content caps stay unsatisfied
            # on terminal failure (no usable rows landed). Without this
            # branch a cascade-blocked bulk ingester would falsely gate
            # its legacy counterpart from recovering — see
            # ``_ORDERING_ONLY_CAPS`` docstring.
            for cap in provides.get(stage_key, ()):
                if cap in _ORDERING_ONLY_CAPS:
                    caps.add(cap)
    return caps


def _capability_is_dead(
    cap: Capability,
    statuses: Mapping[str, str],
    rows_processed: Mapping[str, int | None] | None = None,
    *,
    providers_map: Mapping[Capability, tuple[str, ...]] = _CAPABILITY_PROVIDERS,
    provides_on_skip: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES_ON_SKIP,
    provides: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES,
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
        # #1233 ordering-only caps: a cascade-blocked / errored /
        # cancelled provider STILL satisfies the cap because the
        # cap's only semantic is "this stage is no longer writing".
        # Content caps stay dead on terminal failure (handled by the
        # ``return True`` fallthrough below).
        if status in ("blocked", "error", "cancelled") and cap in _ORDERING_ONLY_CAPS:
            on_provides = provides.get(provider_key, ())
            if cap in on_provides:
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
    provides: Mapping[str, tuple[Capability, ...]] = _STAGE_PROVIDES,
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
        provides=provides,
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
    # #1141 — Phase C bulk ingesters split off ``db`` into per-family
    # source lanes so disjoint table-family writes run cross-source-
    # parallel under separate ``JobLock``s. Each new lane is registered
    # in ``Lane`` (``app/jobs/sources.py``) + ``_LANE_MAX_CONCURRENCY``
    # above + the ``bootstrap_stages.lane`` CHECK constraint
    # (sql/147_bootstrap_stages_lane_family_split.sql). See
    # ``docs/superpowers/specs/2026-05-13-db-lane-family-split.md``.
    "sec_submissions_ingest": "db_filings",
    "sec_companyfacts_ingest": "db_fundamentals_raw",
    "sec_13f_ingest_from_dataset": "db_ownership_inst",
    "sec_insider_ingest_from_dataset": "db_ownership_insider",
    "sec_nport_ingest_from_dataset": "db_ownership_funds",
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
    # Phase D — #1233 PR-1b. OpenFIGI CUSIP resolver sweep. Runs AFTER
    # the bulk-13F + bulk-NPORT ingesters drop unresolved CUSIPs into
    # ``unresolved_13f_cusips`` (source IN ('bulk_13f_dataset',
    # 'bulk_nport_dataset')) and BEFORE every downstream stage that
    # joins on the resolved CUSIP map. The sweep promotes each
    # OpenFIGI-resolved CUSIP into ``external_identifiers`` with
    # provider='openfigi', then leaves the unresolved row in place
    # (a subsequent ``_load_cusip_map`` read picks the new mapping up
    # via the WHERE provider IN ('sec', 'openfigi') filter).
    #
    # Lane=``openfigi`` so it is disjoint from every SEC budget — the
    # OpenFIGI rate limiter (per-instance, in
    # ``app/services/openfigi_resolver.py``) is the sole budget gate.
    #
    # No new capability provided — the sweep writes external_identifiers
    # rows that already satisfy ``cusip_mapping_ready`` (advertised by
    # S3 ``cusip_universe_backfill``). The post-sweep coverage check
    # writes ``bootstrap_runs.coverage_floor_met`` informationally.
    _spec(
        "cusip_resolver_post_bulk_sweep",
        13,
        "openfigi",
        JOB_CUSIP_RESOLVER_POST_BULK_SWEEP,
    ),
    # Phase C' — per-CIK secondary-pages walk for deep-history parity.
    _spec("sec_submissions_files_walk", 14, "sec_rate", JOB_SEC_SUBMISSIONS_FILES_WALK),
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
        15,
        "sec_rate",
        JOB_FILINGS_HISTORY_SEED,
        params={
            "days_back": 730,
            "filing_types": tuple(_FILINGS_HISTORY_KEEP_FORMS_TUPLE),
        },
    ),
    _spec(
        "sec_first_install_drain",
        16,
        "sec_rate",
        JOB_SEC_FIRST_INSTALL_DRAIN,
        params={"max_subjects": None},
    ),
    _spec("sec_def14a_bootstrap", 17, "sec_rate", "sec_def14a_bootstrap"),
    _spec("sec_business_summary_bootstrap", 18, "sec_rate", "sec_business_summary_bootstrap"),
    _spec("sec_insider_transactions_backfill", 19, "sec_rate", "sec_insider_transactions_backfill"),
    _spec("sec_form3_ingest", 20, "sec_rate", "sec_form3_ingest"),
    _spec("sec_8k_events_ingest", 21, "sec_rate", "sec_8k_events_ingest"),
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
        22,
        "sec_rate",
        JOB_SEC_13F_QUARTERLY_SWEEP,
        # ``min_period_of_report`` resolves to ``today() - 380d`` at
        # dispatch time (see ``_resolve_dynamic_params``). Hardcoding
        # ``date.today()`` here would freeze the cutoff at module-load,
        # so a long-lived jobs process would dispatch stage 21 with a
        # stale floor. The sentinel keeps the StageSpec data-only.
        # ``min_last_13f_hr_at`` (#1010) bounds the cohort to filers
        # whose most recent 13F-HR / HR/A is within the same 380-day
        # window — collapses 11,205 → ≈ 3-5k active filers and drops
        # bootstrap stage 21 wall-clock from ~8h to ≤3h.
        params={
            "min_period_of_report": _PARAM_DYNAMIC_BOOTSTRAP_13F_CUTOFF,
            "min_last_13f_hr_at": _PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF,
            "source_label": "sec_edgar_13f_directory_bootstrap",
        },
    ),
    _spec(
        "sec_n_port_ingest",
        23,
        "sec_rate",
        "sec_n_port_ingest",
        # PR7 #1233 §4.6 — ``min_last_seen_filed_at`` (mirror of #1010
        # ``min_last_13f_hr_at`` for stage 21) bounds the cohort to
        # trust CIKs whose most recent NPORT-P / NPORT-P/A filed_at is
        # within the 380-day window. Collapses ~5k registered trusts
        # to ~3-4k actively-filing trusts and drops bootstrap stage 22
        # wall-clock proportionally. Daily / Admin "Run now" paths
        # dispatch ``sec_n_port_ingest`` with empty params → full
        # cohort (safety-net for previously-inactive trusts re-
        # emerging).
        params={
            "min_last_seen_filed_at": _PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF,
        },
    ),
    _spec("ownership_observations_backfill", 24, "db", "ownership_observations_backfill"),
    # Stream A PR-C2 T1.2 (#1233): S25 dispatches the bootstrap-only
    # ``fundamentals_sync_bootstrap`` invoker (NOT the steady-state
    # ``fundamentals_sync`` job) on the dedicated ``db_fundamentals_raw``
    # lane. The job_name divergence (stage_key=fundamentals_sync vs
    # job_name=fundamentals_sync_bootstrap) is what lets PR-C2's lane
    # reassignment coexist with the steady-state ScheduledJob's
    # ``source="db"`` registration — see ``app/jobs/sources.py:
    # _build_job_name_to_source`` Pass 1/2 separation. The cap
    # requirement at ``_STAGE_REQUIRES_CAPS["fundamentals_sync"]``
    # (4-cap tuple from PR-C1) still keys by stage_key and applies.
    _spec("fundamentals_sync", 25, "db_fundamentals_raw", "fundamentals_sync_bootstrap"),
    # #1174 — dedicated MF directory refresh + N-CSR fund-scoped bootstrap
    # drain (T8 deferred from #1171). S25 (post #1233 PR-1b: S26) advertises
    # class_id_mapping_ready; S26 (post #1233 PR-1b: S27, terminal)
    # drains N-CSR + N-CSRS accessions per trust for the #1171
    # fund-metadata parser to consume.
    _spec("mf_directory_sync", 26, "sec_rate", JOB_MF_DIRECTORY_SYNC),
    # #1233 §4.12 / PR8 — terminal stage. Dispatches with no params.
    # The 730d retention window is hard-pinned at
    # ``app/services/manifest_parsers/sec_n_csr.py::N_CSR_RETENTION_DAYS``
    # and the previous ``horizon_days`` param was removed (single
    # source of truth for every N-CSR writer chokepoint).
    _spec(
        "sec_n_csr_bootstrap_drain",
        27,
        "sec_rate",
        JOB_SEC_N_CSR_BOOTSTRAP_DRAIN,
    ),
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

    Algorithm (PR-2 #1233 — ``as_completed`` poll loop):

      1. Build per-stage status map (initially ``pending``).
      2. **Cancel checkpoint** — at the top of each poll iteration
         (after every completion, not every batch) check
         ``is_stop_requested`` against ``(target_run_kind='bootstrap_run',
         target_run_id=run_id)``. On observed cancel: mark stop
         request observed, call ``mark_run_cancelled`` (terminalises
         run + state + sweeps remaining stages), mark stop request
         completed, and return early. This is the operator-cancel
         observation point per spec §Cancel semantics — cooperative.
      3. While any stage is pending OR any future is in flight:
         compute the satisfied-capability set from current stage
         statuses (via ``_STAGE_PROVIDES`` + ``_STAGE_PROVIDES_ON_SKIP``;
         #1138 Task A). For each pending stage:
         * If its ``CapRequirement`` is satisfied → submit to its
           lane executor (subject to in-flight cap).
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
      4. Per-lane concurrency cap is enforced via
         ``lane_in_flight_count`` decremented on completion; once at
         cap, the stage stays ``pending`` and is reconsidered on the
         next poll iteration.
      5. ``wait(in_flight, return_when=FIRST_COMPLETED, timeout=1.0s)``
         picks up the first completed future. Statuses + rows_processed
         update IMMEDIATELY (not at end of batch); caps recompute on
         the next iteration so a freshly-completed cap-provider
         unblocks its consumers on the very next pass.
      6. Stop when no stage is pending AND no future is in flight.
      7. Deadlock detection: if no future is in flight AND no
         ready/cascade transition happened on the iteration, the
         dispatcher cannot make progress — flip remaining pending
         stages to ``blocked`` with the canonical "abandoned" reason.

    One persistent ``ThreadPoolExecutor`` per lane lives for the
    duration of this function (try/finally ``shutdown(wait=True)``).
    Stages with no ``requires`` start in the first iteration. The
    dispatcher is fully data-driven by ``_STAGE_REQUIRES_CAPS``
    (capability-based DNF dependency graph; #1138 Task A) +
    ``_STAGE_LANE_OVERRIDES``.

    Cancel observation latency: at most one completion interval +
    the cancel-poll timeout (~1.0s when nothing is completing).
    Mid-stage work runs to completion — the watermark advances on
    commit and the next Iterate resumes from there. Pre-PR-2 the
    latency was the duration of the longest in-flight BATCH (5+ min
    on 13F sweeps); the per-completion checkpoint reduces this to
    the longest single stage.
    """
    from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

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

    # PR-2 #1233 — persistent per-lane executors. One executor per
    # lane is created on first submission and lives for the duration
    # of the dispatcher. The poll loop submits + tracks per-lane
    # in-flight counts so the structural cap is preserved AND
    # cross-lane parallelism is no longer gated on the slowest sibling.
    lane_executors: dict[str, ThreadPoolExecutor] = {}
    # Map future → (stage_key, lane) so completion handling has both.
    in_flight: dict[Future[_StageOutcome], tuple[str, str]] = {}
    lane_in_flight_count: dict[str, int] = {}
    # Cancel-exit fast path: when set, the ``finally`` clause skips
    # ``shutdown(wait=True)`` and uses ``wait=False`` so dispatcher
    # exit doesn't block on still-running stages. Pre-PR-2 had the
    # same issue (also ``shutdown(wait=True)``); PR-2 narrows it to
    # the cancel return path so an operator-cancel returns from the
    # dispatcher promptly. Already-running invokers continue to
    # completion in their worker threads — they observe the
    # ``bootstrap_cancel_requested()`` contextvar via
    # ``_run_one_stage`` and bail cooperatively. Codex 2 BLOCKING.
    cancel_exit = False

    def _ensure_executor(lane: str) -> ThreadPoolExecutor:
        if lane not in lane_executors:
            max_workers = _LANE_MAX_CONCURRENCY.get(lane, 1)
            lane_executors[lane] = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=f"bootstrap-{lane}",
            )
        return lane_executors[lane]

    # Cancel-poll cadence (sec). On every poll iteration we also
    # checkpoint via DB; when futures are in flight but none has
    # completed yet, the wait() timeout bounds cancel latency.
    _CANCEL_POLL_INTERVAL = 1.0

    def _check_cancel() -> bool:
        """Return True iff an operator-cancel has been observed.
        Side-effect: terminalises the run + state under a single tx
        per the W4 atomicity rule from the pre-PR-2 implementation.
        """
        with psycopg.connect(database_url) as cancel_conn:
            stop = is_stop_requested(
                cancel_conn,
                target_run_kind="bootstrap_run",
                target_run_id=run_id,
            )
            if stop is None:
                return False
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
            return True

    def _apply_outcome(stage_key: str, outcome: _StageOutcome) -> None:
        """Map a completed stage outcome onto the statuses +
        rows_processed maps. Identical semantics to the pre-PR-2 batch
        post-process — just runs once per completion now."""
        rows_processed[stage_key] = outcome.rows_processed
        if outcome.skipped:
            statuses[stage_key] = "skipped"
            logger.info("bootstrap dispatcher: %s SKIPPED", stage_key)
        elif outcome.cancelled:
            # PR3d #1064 — stage observed operator cancel mid-loop and
            # exited cooperatively. Status maps to 'cancelled' so the
            # Timeline tones gray; the outer cancel checkpoint picks
            # up the run-level cancel signal and sweeps the rest.
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

    try:
        while True:
            # Cancel checkpoint — fires before EVERY submission round
            # and is re-checked after every completion. Pre-PR-2 only
            # checked between batches; PR-2 reduces observation
            # latency to ~ longest single stage + _CANCEL_POLL_INTERVAL.
            if _check_cancel():
                # Stop accepting new work. Set the cancel-exit flag so
                # the ``finally`` clause uses ``shutdown(wait=False)``
                # — dispatcher exit must not block on the slowest
                # in-flight stage. In-flight workers continue to
                # completion; they observe the
                # ``bootstrap_cancel_requested()`` contextvar via
                # ``_run_one_stage`` and bail cooperatively.
                # ``mark_run_cancelled`` has already swept the
                # remaining bootstrap_stages rows into ``cancelled``.
                cancel_exit = True
                return statuses, True

            # #1138 Task A — capability-based runnability + cascade
            # classification. Recompute caps EVERY iteration (per
            # completion under the new poll loop), so a freshly
            # completed cap-provider unblocks its consumers on the
            # very next pass — not at the end of the heterogeneous
            # ready batch.
            caps = _satisfied_capabilities(
                statuses,
                rows_processed,
                provides=effective_provides,
                provides_on_skip=effective_provides_on_skip,
                min_rows=effective_min_rows,
                exclusions=effective_exclusions,
            )

            # ``in_flight_keys`` excludes already-submitted stages
            # from re-evaluation. A submitted stage keeps
            # ``statuses[key] == "pending"`` until its worker completes
            # and _apply_outcome flips it; without this filter, the
            # next poll iteration with a lane cap >1 (or a wait timeout
            # firing before any completion) could re-add the same
            # stage to ``ready`` and resubmit it. With production cap=1
            # the lane gate masks this, but it would silently break
            # the moment any lane cap was widened. Codex 2 HIGH.
            in_flight_keys = {sk for sk, _ in in_flight.values()}
            pending_keys = [k for k, s in statuses.items() if s == "pending" and k not in in_flight_keys]
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
                    provides=effective_provides,
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

            # Submission gate — respect per-lane in-flight cap. Stages
            # that don't fit stay in ``pending`` and will be
            # reconsidered on the next iteration after a sibling
            # completes. With production cap=1 across all lanes, the
            # gate also prevents JobLock collisions between same-lane
            # siblings dispatched in the same outer iteration.
            submitted_this_iteration: list[str] = []
            for stage in ready:
                lane = stage.lane
                cap = _LANE_MAX_CONCURRENCY.get(lane, 1)
                if lane_in_flight_count.get(lane, 0) >= cap:
                    continue  # at-cap; leave pending for next iteration

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

                executor = _ensure_executor(lane)
                fut = executor.submit(
                    _run_one_stage,
                    run_id=run_id,
                    stage_key=stage.stage_key,
                    job_name=stage.job_name,
                    invoker=stage.invoker,
                    database_url=database_url,
                    params=validated_params,
                )
                in_flight[fut] = (stage.stage_key, lane)
                lane_in_flight_count[lane] = lane_in_flight_count.get(lane, 0) + 1
                submitted_this_iteration.append(stage.stage_key)

            if submitted_this_iteration:
                logger.info(
                    "bootstrap dispatcher: submitted — %s (in-flight=%d)",
                    submitted_this_iteration,
                    len(in_flight),
                )

            # Termination + deadlock detection.
            if not in_flight:
                if not pending_keys:
                    # Everything terminalised.
                    break
                if cascade_transitioned:
                    # Cascade flipped at least one stage this
                    # iteration but didn't free any ready stage. Loop
                    # to recompute caps — a now-terminalised upstream
                    # may unblock another pending downstream.
                    continue
                # Nothing in flight, nothing ready, no cascade
                # transition: the dependency graph cannot resolve.
                # Mark remaining pending as blocked with the
                # canonical "abandoned" reason (matches pre-PR-2 line
                # 1622 of the legacy dispatcher; Codex review BLOCKING
                # for PR #1039).
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

            # Wait for first completion (or timeout to re-check
            # cancel). ``return_when=FIRST_COMPLETED`` means: pick up
            # ONE completion and immediately re-evaluate caps, cascade
            # classification, and the cancel checkpoint. Cross-lane
            # parallelism: an idle lane gets its next stage submitted
            # the moment the fast-lane sibling finishes — no waiting
            # for the slow-lane batch-mate.
            done, _pending_futs = wait(
                set(in_flight.keys()),
                return_when=FIRST_COMPLETED,
                timeout=_CANCEL_POLL_INTERVAL,
            )
            if not done:
                # Timeout fired — fall through to the cancel
                # checkpoint at the top of the next iteration.
                continue

            # Process EVERY future that's already done. ``wait`` with
            # FIRST_COMPLETED may return more than one if they raced
            # to completion; consume them all in one pass so caps are
            # only recomputed once before we loop.
            for fut in done:
                stage_key, lane = in_flight.pop(fut)
                lane_in_flight_count[lane] = lane_in_flight_count.get(lane, 0) - 1
                outcome = fut.result()
                _apply_outcome(stage_key, outcome)
    finally:
        # Shutdown all lane executors. ``cancel_futures=True`` cancels
        # any QUEUED futures (none expected — submission is gated on
        # in-flight cap); running futures still complete naturally on
        # their worker threads.
        #
        # ``wait`` is True on the happy path so a test fixture's
        # monkeypatched _INVOKERS map can't get unbound while a worker
        # is still calling it. On cancel exit, ``wait=False`` keeps
        # dispatcher return latency bounded by ~ the cancel checkpoint
        # interval (1.0s) — operator-cancel must not block on the
        # slowest in-flight stage. Cooperative cancel inside
        # ``_run_one_stage`` brings the workers home shortly after.
        wait_on_shutdown = not cancel_exit
        for executor in lane_executors.values():
            executor.shutdown(wait=wait_on_shutdown, cancel_futures=True)

    return statuses, False


# ---------------------------------------------------------------------------
# Manifest reset prelude (#1233 PR-5a / spec §9)
# ---------------------------------------------------------------------------
#
# Background: run #4 inherited 1.18M ``sec_filing_manifest`` rows from
# the cancelled run #3, including ``ingest_status='failed'`` rows whose
# ``next_retry_at`` watermarks lived in the future. Those rows refused
# to drain in the new run even when (a) the parser_version had bumped
# since the prior attempt, or (b) the failure was transient. Operator-
# visible symptom: bootstrap completes with stale failure state.
#
# The prelude flips ``failed`` rows back to ``pending`` at run start
# subject to two gates:
#
# 1. Source whitelist — only the subset of ``ManifestSource`` values
#    the orchestrator's stage catalogue actually drives. FINRA sources
#    have their own non-bootstrap drivers; their failure state must not
#    be papered over here. The set is derived statically from
#    ``_MANIFEST_SOURCES_BY_STAGE``.
#
# 2. Time filter — only rows whose ``last_attempted_at`` is strictly
#    before the current run's start watermark (``triggered_at``). A
#    concurrent live cron writer landing a fresh ``failed`` row mid-
#    reset survives because its ``last_attempted_at >= reset_started_at``.
#
# The reset is idempotent: re-invoking against the same run is a no-op
# once the predicate's tail-end is empty.

# Per-stage manifest source sets. Pinned here so adding a new stage
# that writes manifest rows for an existing source family becomes a
# single-line edit, not a sweep. Stages NOT in this mapping (init /
# eToro / cusip / cik refresh / bulk download / Phase C bulk
# ingesters / openfigi sweep / mf_directory_sync / observations
# backfill / fundamentals_sync) do not write to ``sec_filing_manifest``
# — their data lands in other tables.
#
# The aggregate union (``_BOOTSTRAP_MANIFEST_SOURCES``) is what
# ``reset_manifest_for_run`` flips. Per-stage breakdown is documented
# here so a future stage-trim audit can answer "which manifest
# sources lose coverage if I drop stage X?" without re-reading every
# invoker body.
_MANIFEST_SOURCES_BY_STAGE: Final[dict[str, frozenset[str]]] = {
    # filings_history_seed walks the per-issuer filings history; the
    # form-type allow-list (``SEC_INGEST_KEEP_FORMS``) covers every
    # issuer-scoped SEC manifest source.
    "filings_history_seed": frozenset(
        {
            "sec_form3",
            "sec_form4",
            "sec_form5",
            "sec_13d",
            "sec_13g",
            "sec_def14a",
            "sec_10k",
            "sec_10q",
            "sec_8k",
            "sec_xbrl_facts",
        }
    ),
    # sec_first_install_drain is the manifest worker itself; it drains
    # every pending manifest row regardless of source.
    "sec_first_install_drain": frozenset(
        {
            "sec_form3",
            "sec_form4",
            "sec_form5",
            "sec_13d",
            "sec_13g",
            "sec_13f_hr",
            "sec_def14a",
            "sec_n_port",
            "sec_n_csr",
            "sec_10k",
            "sec_10q",
            "sec_8k",
            "sec_xbrl_facts",
        }
    ),
    "sec_def14a_bootstrap": frozenset({"sec_def14a"}),
    "sec_business_summary_bootstrap": frozenset({"sec_10k"}),
    "sec_insider_transactions_backfill": frozenset({"sec_form4"}),
    "sec_form3_ingest": frozenset({"sec_form3"}),
    "sec_8k_events_ingest": frozenset({"sec_8k"}),
    "sec_13f_recent_sweep": frozenset({"sec_13f_hr"}),
    "sec_n_port_ingest": frozenset({"sec_n_port"}),
    "sec_n_csr_bootstrap_drain": frozenset({"sec_n_csr"}),
}


def _bootstrap_manifest_sources() -> frozenset[str]:
    """Union of every manifest source a bootstrap stage drives.

    Computed once at module import (idempotent — pure function over
    the static map). Returned as a frozenset so callers can pass it
    straight to the SQL ``ANY()`` binding without aliasing concerns.
    """
    out: set[str] = set()
    for sources in _MANIFEST_SOURCES_BY_STAGE.values():
        out.update(sources)
    return frozenset(out)


_BOOTSTRAP_MANIFEST_SOURCES: Final[frozenset[str]] = _bootstrap_manifest_sources()


def reset_manifest_for_run(
    conn: psycopg.Connection[Any],
    *,
    sources: Iterable[str],
    reset_started_at: datetime,
) -> int:
    """Flip stale ``failed`` manifest rows back to ``pending`` at run start.

    Reasons a row's failure state may be stale at run start:

    * Parser version bumped since the prior attempt — the new parser
      may extract a previously-failed body successfully.
    * Backoff watermark (``next_retry_at``) is in the future but the
      operator deliberately triggered a fresh bootstrap; the watermark
      semantic targets routine retry pacing, not full-run resets.
    * The prior failure was transient (network blip mid-fetch); the
      run-start reset gives every accession a fresh attempt.

    Two filters keep the reset narrow:

    * ``source = ANY(sources)`` — only sources the orchestrator drives.
      Sources owned by non-bootstrap drivers (FINRA short-interest /
      RegSHO) are untouched.
    * ``last_attempted_at < reset_started_at`` — defends against a
      concurrent live cron worker landing a fresh ``failed`` row
      mid-reset. Such a row has ``last_attempted_at >= NOW() >=
      reset_started_at`` and would survive the filter even if the
      orchestrator's ``UPDATE`` was already in flight when the worker
      committed.

    Rows in any other ``ingest_status`` (pending / fetched / parsed /
    tombstoned) are left untouched: a stuck-``pending`` row is the
    worker's problem; a ``parsed`` row stays parsed; a ``tombstoned``
    row needs an explicit operator action (``POST /jobs/sec_rebuild``).

    Returns the count of rows flipped for telemetry. Idempotent — a
    second invocation finds zero matching rows.

    The caller must commit the connection's transaction; the helper
    does not start its own ``with conn.transaction():`` because the
    orchestrator's prelude lives outside any open transaction (each
    ``with psycopg.connect()`` block opens its own).
    """
    source_list = list(sources)
    if not source_list:
        # Empty source set means no stages in the catalogue write to
        # ``sec_filing_manifest`` — defense-in-depth no-op. ``ANY('{}'
        # )`` would match nothing anyway, but the early return saves a
        # round trip and keeps the log line accurate.
        logger.info("reset_manifest_for_run: empty source set; nothing to reset")
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sec_filing_manifest
               SET ingest_status     = 'pending',
                   next_retry_at     = NULL,
                   error             = NULL,
                   last_attempted_at = NULL
             WHERE source = ANY(%(sources)s::text[])
               AND ingest_status = 'failed'
               AND last_attempted_at IS NOT NULL
               AND last_attempted_at < %(reset_started_at)s
            """,
            {"sources": source_list, "reset_started_at": reset_started_at},
        )
        count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    logger.info(
        "reset_manifest_for_run: %d failed rows flipped to pending (reset_started_at=%s, sources=%d)",
        count,
        reset_started_at.isoformat(),
        len(source_list),
    )
    return count


# ---------------------------------------------------------------------------
# Orphan-stage reaper (#1233 PR-6)
# ---------------------------------------------------------------------------
#
# Symptom this fixes: if the jobs process crashes mid-stage,
# ``bootstrap_stages.status`` stays ``'running'`` forever. On jobs-process
# restart, the dispatcher's ``_should_run`` accepts ``'running'`` as runnable
# BUT the per-stage dispatcher path then calls
# ``mark_stage_running(... AND status='pending')`` which silently no-ops
# against the stale running row, and the run sits stuck. Operator has to
# manually clear via Re-run failed.
#
# Solution: just before the dispatcher loop, sweep rows whose worker is
# provably dead (advisory lock NOT held in any session) and whose
# ``started_at`` is older than ``_REAPER_GRACE_SECONDS`` (5 min — longer
# than the slowest known stage start-up). Stale rows transition back to
# ``'pending'`` so the dispatcher can pick them up cleanly.
#
# Liveness probe: read-only ``SELECT FROM pg_locks`` — NEVER acquires.
# Lock key derivation MUST match ``JobLock`` exactly: ``hashtext('job_source:'
# || <source>)::int`` (NOT ``hashtextextended`` — the locks key space is
# int4 by construction; see ``app/jobs/locks.py:224``).
#
# Caveats (documented residual risk):
# * Cannot detect a hung-but-alive worker that holds the lock without
#   making progress. The grace window catches the obvious case (worker
#   crashed before its first commit); a deeper deadlock requires an
#   operator Re-run failed.
# * Cannot detect re-entrancy (#1184) edge where the outer-thread holds
#   the lock but the stage-thread itself crashed — accepted residual risk
#   because the outer holder will release on its own crash, and the next
#   reaper pass after grace will reset.
# * The reset path is guarded by ``AND status='running'`` so a stage that
#   transitioned to ``'success'``/``'error'`` between the SELECT and the
#   UPDATE is left alone (Codex pre-push W3 pattern).

_REAPER_GRACE_SECONDS: Final[int] = 300
"""Minimum age before a ``running`` stage with no held lock is reset.

Longer than the slowest known stage's start-up window (Phase B SEC
filer-directory fetches take up to ~3 min on cold caches). A reset that
fires before the worker has reached its first transaction is the
worst-case false positive — the dispatcher would then race the worker
on the same stage row. 5 minutes is a safe over-estimate of "the worker
would have committed SOMETHING by now if it were alive."
"""


def _hashtext_int(conn: psycopg.Connection[Any], text: str) -> int:
    """Return Postgres ``hashtext(text)::int`` — JobLock's lock-key shape.

    MUST match ``app/jobs/locks.py:224``:
        SELECT pg_try_advisory_lock(hashtext(%s)::int)

    ``hashtext`` (NOT ``hashtextextended``) returns int4; the cast to
    ``::int`` is a no-op in current PG but kept explicit so the call
    site here mirrors the JobLock SQL byte-for-byte. Computing this
    Python-side via a re-implementation of PG's hashtext is hostile to
    review — a single PG version drift between Python clone and PG
    server would silently miscompute the key. Round-tripping through
    the active connection is honest and cheap (one query per stage).

    Returned int is the signed int4 value (can be negative). Empirical:
    ``hashtext('job_source:finra') = -685386401``. Callers probing
    ``pg_locks`` must split this signed-int-widened-to-bigint into
    ``(classid, objid)`` halves — see the probe SQL in
    ``reap_orphaned_running_stages``.
    """
    row = conn.execute("SELECT hashtext(%(text)s)::int", {"text": text}).fetchone()
    if row is None:
        # Defensive: hashtext is a deterministic builtin; an empty row
        # means the connection or DB itself is in an unexpected state.
        raise RuntimeError(f"hashtext({text!r}) returned no row")
    return int(row[0])


def reap_orphaned_running_stages(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
) -> int:
    """Reset ``running`` stages whose worker is provably dead.

    Criteria for reset:

    1. ``bootstrap_stages.status = 'running'``.
    2. ``started_at < NOW() - INTERVAL '5 minutes'`` (grace window).
    3. The corresponding ``JobLock`` advisory lock is NOT held in any
       Postgres session — probed read-only via ``pg_locks``.

    Lock key = ``hashtext('job_source:' || source)::int`` where
    ``source = app.jobs.sources.source_for(job_name)``. Bytes-for-bytes
    match with ``JobLock``'s acquisition SQL (``app/jobs/locks.py:224``).

    On reset the stage transitions ``running`` → ``pending``:
      * ``started_at`` / ``completed_at`` cleared.
      * ``last_error`` is APPENDED (not replaced) with a reaper marker
        so forensic context for the next crash is preserved.

    Returns the count of stages reset. Idempotent — repeat calls with
    no orphans return 0.

    Defensive fallbacks:
      * ``source_for(job_name)`` raising ``KeyError`` (registry gap)
        logs a warning and leaves the row alone. The reaper must never
        reset a stage whose lock semantics it cannot prove.
    """
    # Lazy import: app.jobs.sources -> app.services.bootstrap_orchestrator
    # at module load via the JOB_NAME_TO_SOURCE construction. Importing
    # back here at top-of-module would close the cycle.
    from app.jobs.sources import source_for

    candidate_rows = conn.execute(
        """
        SELECT stage_key, job_name
          FROM bootstrap_stages
         WHERE bootstrap_run_id = %(run_id)s
           AND status = 'running'
           AND started_at IS NOT NULL
           AND started_at < NOW() - make_interval(secs => %(grace)s)
        """,
        {"run_id": run_id, "grace": _REAPER_GRACE_SECONDS},
    ).fetchall()

    count = 0
    for stage_key, job_name in candidate_rows:
        try:
            source = source_for(job_name)
        except KeyError:
            logger.warning(
                "reap_orphaned_running_stages: stage %r job_name %r has no source mapping; "
                "leaving stale running row alone (defensive — cannot prove lock-not-held without "
                "the source key)",
                stage_key,
                job_name,
            )
            continue

        lock_key = _hashtext_int(conn, f"job_source:{source}")
        # ``pg_locks`` splits the bigint advisory-lock key into two
        # uint32 halves: ``classid`` (high 32 bits) and ``objid`` (low
        # 32 bits). For a *positive* int4 key like 1_447_707_902 the
        # widening to int8 leaves the high half all-zero so
        # ``classid=0``; for a *negative* int4 key like -685_386_401
        # the sign-extension fills the high half with 1s so
        # ``classid=4_294_967_295`` (= ``0xFFFFFFFF``). Probing only
        # ``classid=0`` would miss every negative-hashtext key (e.g.
        # ``job_source:finra``) and silently fail to detect the held
        # lock — the reaper would then reset a stage whose worker IS
        # alive. The arithmetic split is done PG-side via ``::bigint``
        # so the byte shape is identical to the kernel split.
        # ``objsubid=1`` is session-scope (PG sets ``=2`` for
        # transaction-scoped advisory locks; the JobLock path uses
        # the session form).
        #
        # ``database = (current_database OID)`` scopes the probe to
        # locks held in THIS database only (Codex 2 medium). The same
        # advisory key in a sibling DB on the same Postgres cluster
        # (e.g. dev + ebull_test running on localhost:5432 with their
        # own bootstrap processes) would otherwise spuriously satisfy
        # the probe and suppress a legitimate reset in this DB.
        # ``pg_locks.database`` is OID; ``current_database()`` returns
        # name → join via ``pg_database`` rather than rely on a
        # hardcoded OID literal.
        held_row = conn.execute(
            """
            SELECT 1
              FROM pg_locks
             WHERE locktype = 'advisory'
               AND objsubid = 1
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND classid  = ((%(lock_key)s::bigint >> 32) & 4294967295)::oid
               AND objid    = (%(lock_key)s::bigint & 4294967295)::oid
            """,
            {"lock_key": lock_key},
        ).fetchone()
        if held_row is not None:
            # Worker (or some other session) still holds the lock; the
            # stage may actually be running. Skip reset; operator can
            # force-cancel via the existing Re-run failed UX if the
            # worker is hung-but-alive.
            continue

        result = conn.execute(
            """
            UPDATE bootstrap_stages
               SET status       = 'pending',
                   started_at   = NULL,
                   completed_at = NULL,
                   last_error   = COALESCE(last_error, '')
                                  || CASE WHEN COALESCE(last_error, '') = '' THEN '' ELSE E'\n' END
                                  || 'reaper: reset from orphaned running ('
                                  || NOW()::text || ')'
             WHERE bootstrap_run_id = %(run_id)s
               AND stage_key        = %(stage_key)s
               AND status           = 'running'
            """,
            {"run_id": run_id, "stage_key": stage_key},
        )
        if result.rowcount > 0:
            count += 1

    if count > 0:
        logger.info(
            "reap_orphaned_running_stages: run_id=%d reset %d orphan stage(s) back to pending",
            run_id,
            count,
        )
    else:
        logger.debug(
            "reap_orphaned_running_stages: run_id=%d no orphans (grace=%ds)",
            run_id,
            _REAPER_GRACE_SECONDS,
        )
    return count


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

    # #1233 PR-5a — manifest reset prelude.
    #
    # Runs AFTER the snapshot validation (so we know this run is
    # actually ``running`` and not a stale ``complete`` row the
    # orchestrator was re-invoked against) and BEFORE every dispatcher
    # bookkeeping step (so a flipped row has the maximum time to drain
    # in the same run). The opt-out key
    # ``params['reset_failed_manifest']`` defaults to TRUE; an operator
    # who wants to preserve stale failure state on a re-run can flip
    # it FALSE at API-call time.
    #
    # The reset opens its own short-lived ``psycopg.connect()`` block
    # rather than reusing the dispatcher's per-stage connections —
    # the prelude must commit before the first stage runs so a stage
    # that itself dispatches a manifest-touching invoker sees the
    # ``pending`` state. Bundling the reset with a long-lived dispatch
    # transaction would queue every reset row update behind every
    # stage's row-level locks.
    #
    # Opt-out type discipline (Codex pre-push LOW): the JSONB CHECK
    # only pins object SHAPE — a future internal writer that persists
    # ``{"reset_failed_manifest": "false"}`` (string) or ``0`` (number)
    # would pass shape validation, and naive truthiness ``.get()``
    # would mis-classify the string ``"false"`` as truthy. Test for
    # exact ``is False``: only the JSON boolean ``false`` (which
    # psycopg decodes to Python ``False``) flips the prelude off.
    # Every other persisted value (missing key, ``None``, ``"false"``,
    # ``0``, ``1``, ``"true"``) preserves the default reset-on
    # semantic — fail-closed against silent opt-out via type drift.
    if snapshot.params.get("reset_failed_manifest", True) is not False:
        with psycopg.connect(database_url) as conn:
            reset_count = reset_manifest_for_run(
                conn,
                sources=_BOOTSTRAP_MANIFEST_SOURCES,
                reset_started_at=snapshot.triggered_at,
            )
            conn.commit()
        logger.info(
            "bootstrap_orchestrator: run_id=%d manifest reset flipped %d rows",
            run_id,
            reset_count,
        )
    else:
        logger.info(
            "bootstrap_orchestrator: run_id=%d manifest reset skipped (params.reset_failed_manifest=False)",
            run_id,
        )

    # #1233 PR-6 — sweep stages stuck in ``running`` from a previous
    # jobs-process crash. Reset is guarded by ``pg_locks`` evidence that
    # the worker's advisory lock is no longer held + a 5-min grace
    # window. Runs AFTER the manifest reset prelude and BEFORE the
    # dispatcher loop so the freshly reset rows enter dispatch as
    # ``pending``. Idempotent — zero orphans is the steady state.
    with psycopg.connect(database_url) as conn:
        reap_orphaned_running_stages(conn, run_id=run_id)
        conn.commit()

    # PR1c #1064 — build a stage_key → params lookup from the static
    # ``_BOOTSTRAP_STAGE_SPECS`` so the per-stage params dict can be
    # plumbed into ``_RunnableStage`` below. ``bootstrap_stages`` in
    # DB doesn't store params (immutable across runs; lives in code),
    # so the dispatch path consults the spec table at run time.
    stage_params_by_key: dict[str, Mapping[str, Any]] = {spec.stage_key: spec.params for spec in _BOOTSTRAP_STAGE_SPECS}
    # #1136 Phase A.3 dispatch hardening — index the catalogue by
    # ``stage_key`` so the dispatcher resolves ``job_name`` from the
    # spec rather than the DB row's persisted ``job_name``. Removes
    # the stale-name failure mode observed in run_id=3 stage 21
    # (where the DB row carried ``bootstrap_sec_13f_recent_sweep``
    # after PR1c #1064 renamed the canonical to
    # ``JOB_SEC_13F_QUARTERLY_SWEEP``). The DB column stays as the
    # audit snapshot of "what the run was created to dispatch" — the
    # runtime decision is logged below for forensic replay.
    spec_by_stage_key: dict[str, StageSpec] = {spec.stage_key: spec for spec in _BOOTSTRAP_STAGE_SPECS}

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
        # #1136 Phase A.3 — resolve job_name from the catalogue by
        # stage_key. Fail closed if the stage_key has been trimmed
        # from the catalogue (silently dispatching the DB row's
        # stored job_name would lose canonical params /
        # CapRequirement / lane semantics — worst-of-both-worlds).
        canonical_spec = spec_by_stage_key.get(stage.stage_key)
        if canonical_spec is None:
            logger.error(
                "bootstrap dispatcher: stage_key %r not in current catalogue; "
                "stored_job_name=%r is stale, refusing to dispatch",
                stage.stage_key,
                stage.job_name,
            )
            with psycopg.connect(database_url) as conn:
                # pending → running → error: ``mark_stage_error`` has
                # ``AND status = 'running'`` so a direct call against
                # the pending row would silently no-op and let the
                # stage survive finalize_run still pending (Codex 1b
                # finding §1).
                mark_stage_running(conn, run_id=run_id, stage_key=stage.stage_key)
                mark_stage_error(
                    conn,
                    run_id=run_id,
                    stage_key=stage.stage_key,
                    error_message=(
                        f"stage_key {stage.stage_key!r} not in current bootstrap catalogue; "
                        f"row job_name={stage.job_name!r} is stale and dispatch is refused"
                    ),
                )
                conn.commit()
            continue
        effective_job_name = canonical_spec.job_name
        if effective_job_name != stage.job_name:
            # Forensic trail — DB row stays as the audit snapshot;
            # the log line records the runtime decision (Codex 1a §4).
            logger.info(
                "bootstrap dispatcher: stage %s remapped stored_job_name=%r -> "
                "effective_job_name=%r (catalogue rename)",
                stage.stage_key,
                stage.job_name,
                effective_job_name,
            )
        invoker = _INVOKERS.get(effective_job_name)
        if invoker is None:
            # Catalogue carries the stage_key but its job_name is not
            # registered — points at a registry / SCHEDULED_JOBS gap,
            # NOT a stale-DB-row case. The catalogue-invariant test
            # (`tests/test_bootstrap_orchestrator_source_registry.py`)
            # is meant to prevent this at push time; the runtime guard
            # is defense-in-depth.
            logger.error(
                "bootstrap dispatcher: stage %s effective_job_name %r is unregistered in _INVOKERS; marking error",
                stage.stage_key,
                effective_job_name,
            )
            with psycopg.connect(database_url) as conn:
                mark_stage_running(conn, run_id=run_id, stage_key=stage.stage_key)
                mark_stage_error(
                    conn,
                    run_id=run_id,
                    stage_key=stage.stage_key,
                    error_message=f"unknown job_name {effective_job_name!r}",
                )
                conn.commit()
            continue
        runnable.append(
            _RunnableStage(
                stage_key=stage.stage_key,
                # effective_job_name flows into _RunnableStage so all
                # downstream consumers — validate_job_params,
                # _run_one_stage, _snapshot_job_runs_max_id — see the
                # canonical name (Codex 1a §3).
                job_name=effective_job_name,
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
    "JOB_MF_DIRECTORY_SYNC",
    "JOB_SEC_N_CSR_BOOTSTRAP_DRAIN",
    "get_bootstrap_stage_specs",
    "reap_orphaned_running_stages",
    "reset_manifest_for_run",
    "run_bootstrap_orchestrator",
]


# Stage count assertion — pin so a future refactor that adds /
# removes a spec deliberately surfaces in code review and doesn't
# silently break the tests + frontend + runbook that hardcode the
# current 27-stage shape.
assert len(_BOOTSTRAP_STAGE_SPECS) == 27, (
    f"_BOOTSTRAP_STAGE_SPECS expected 27 stages, got {len(_BOOTSTRAP_STAGE_SPECS)}; "
    "update the spec, frontend, runbook, and stage_count tests in lockstep. "
    "#1027 added 7 bulk-archive stages (sec_bulk_download + C1.a/C2/C3/C4/C5 ingesters + C1.b walker); "
    "#1174 added 2 fund-stages (S25 mf_directory_sync + S26 sec_n_csr_bootstrap_drain); "
    "#1233 PR-1b inserted S13 cusip_resolver_post_bulk_sweep (renumbering S13-S26 to S14-S27)."
)
