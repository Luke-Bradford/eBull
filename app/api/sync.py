"""Sync orchestrator HTTP endpoints.

Phase 1: POST /sync returns 503 while ORCHESTRATOR_ENABLED=false.
GET endpoints work for inspection regardless of the flag.

Auth: same dependency as /jobs — require_session_or_service_token.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.services.fundamentals_observability import (
    get_cik_timing_summary,
    get_seed_progress,
)
from app.services.layer_enabled import SAFETY_CRITICAL_LAYERS, set_layer_enabled
from app.services.sync_orchestrator import (
    ExecutionPlan,
    SyncAlreadyRunning,
    SyncScope,
    submit_sync,
)
from app.services.sync_orchestrator.cascade import collapse_cascades
from app.services.sync_orchestrator.layer_failure_history import (
    all_layer_error_excerpts,
    all_layer_histories,
)
from app.services.sync_orchestrator.layer_state import compute_layer_states_from_db
from app.services.sync_orchestrator.layer_types import (
    REMEDIES,
    FailureCategory,
    LayerState,
)
from app.services.sync_orchestrator.registry import JOB_TO_LAYERS, LAYERS

# Registry invariant: every layer is emitted by at most one legacy job.
# Built once at import time; a duplicate emit fails loudly at startup
# rather than silently 500-ing per request under client traffic.
#
# Uses explicit `if/raise` rather than `assert` — `python -O` or
# PYTHONOPTIMIZE=1 strips asserts, and this is a security-relevant
# registry invariant, not an internal sanity check.
_LAYER_TO_JOB: dict[str, str] = {}
for _job, _emits in JOB_TO_LAYERS.items():
    for _emit in _emits:
        if _emit in _LAYER_TO_JOB:
            raise RuntimeError(
                f"layer {_emit!r} emitted by both {_LAYER_TO_JOB[_emit]!r} "
                f"and {_job!r}; JOB_TO_LAYERS must have disjoint emits"
            )
        _LAYER_TO_JOB[_emit] = _job

router = APIRouter(
    prefix="/sync",
    tags=["sync"],
    dependencies=[Depends(require_session_or_service_token)],
)


class SyncRequest(BaseModel):
    # Default `behind` — resync only layers the state machine says are
    # DEGRADED / ACTION_NEEDED plus their non-HEALTHY upstreams. Full
    # sync is available via explicit `scope: "full"` for rare operator
    # overrides; the Admin UI no longer fires full by default.
    scope: Literal["full", "layer", "high_frequency", "job", "behind"] = "behind"
    layer: str | None = None
    job: str | None = None


class ActionNeededItem(BaseModel):
    root_layer: str
    display_name: str
    category: Literal[
        "auth_expired",
        "rate_limited",
        "source_down",
        "schema_drift",
        "db_constraint",
        "data_gap",
        "upstream_waiting",
        "master_key_missing",
        "internal_error",
    ]
    operator_message: str
    operator_fix: str | None
    self_heal: bool
    consecutive_failures: int
    affected_downstream: list[str]
    # First line of the latest captured exception (sync_layer_progress
    # .error_message). None when the layer has never recorded a
    # forensic message — older rows pre-#645 will be NULL until the
    # next failure is recorded. The Admin banner shows this alongside
    # the category so "Unclassified error" is no longer opaque.
    error_excerpt: str | None = None


class SecretMissingItem(BaseModel):
    layer: str
    display_name: str
    missing_secret: str
    operator_fix: str


class LayerSummary(BaseModel):
    layer: str
    display_name: str
    last_updated: datetime | None


class CascadeGroupModel(BaseModel):
    root: str
    affected: list[str]


class LayerEntry(BaseModel):
    """Per-layer UI row. Canonical source for LayerHealthList in chunk 1.

    `state` is typed against the `LayerState` enum directly so future
    additions (e.g. new states) propagate to this contract at type-check
    time — no silent drift between the enum's source of truth and a
    hand-copied Literal.
    """

    layer: str
    display_name: str
    state: LayerState
    last_updated: datetime | None
    plain_language_sla: str


class SyncLayersV2Response(BaseModel):
    generated_at: datetime
    system_state: Literal["ok", "catching_up", "needs_attention"]
    system_summary: str
    action_needed: list[ActionNeededItem]
    degraded: list[LayerSummary]
    secret_missing: list[SecretMissingItem]
    healthy: list[LayerSummary]
    disabled: list[LayerSummary]
    cascade_groups: list[CascadeGroupModel]
    layers: list[LayerEntry]


class LayerEnabledRequest(BaseModel):
    enabled: bool
    # #346: safety-critical layers (``fx_rates`` / ``portfolio_sync``)
    # require a non-empty ``reason`` when ``enabled=False``; the
    # endpoint enforces this. Other layers may include reason +
    # changed_by for audit completeness without strict enforcement.
    reason: str | None = None
    changed_by: str | None = None


class LayerEnabledResponse(BaseModel):
    layer: str
    display_name: str
    is_enabled: bool
    warning: str | None = None


def _safety_warning(layer_name: str, enabled: bool) -> str | None:
    if enabled:
        return None
    if layer_name == "fx_rates":
        return "FX rates disabled — portfolio valuations and P&L will drift. Re-enable before resuming live operation."
    if layer_name == "portfolio_sync":
        return "Portfolio sync disabled — broker positions will not refresh. Re-enable before resuming live operation."
    return None


def _scope_from(body: SyncRequest) -> SyncScope:
    if body.scope == "full":
        return SyncScope.full()
    if body.scope == "high_frequency":
        return SyncScope.high_frequency()
    if body.scope == "behind":
        return SyncScope.behind()
    if body.scope == "layer":
        if not body.layer:
            raise HTTPException(status_code=422, detail="layer required when scope='layer'")
        return SyncScope.layer(body.layer)
    if body.scope == "job":
        if not body.job:
            raise HTTPException(status_code=422, detail="job required when scope='job'")
        return SyncScope.job(body.job, force=True)
    raise HTTPException(status_code=422, detail=f"unknown scope {body.scope!r}")


def _plan_to_json(plan: ExecutionPlan) -> dict[str, Any]:
    return {
        "layers_to_refresh": [
            {
                "name": lp.name,
                "emits": list(lp.emits),
                "reason": lp.reason,
                "dependencies": list(lp.dependencies),
                "is_blocking": lp.is_blocking,
                "estimated_items": lp.estimated_items,
            }
            for lp in plan.layers_to_refresh
        ],
        "layers_skipped": [{"name": s.name, "reason": s.reason} for s in plan.layers_skipped],
    }


@router.post("", status_code=status.HTTP_202_ACCEPTED)
def post_sync(body: SyncRequest) -> Any:
    if not settings.orchestrator_enabled:
        raise HTTPException(status_code=503, detail="sync orchestrator disabled (Phase 1)")
    try:
        sync_run_id, plan = submit_sync(_scope_from(body), trigger="manual")
    except SyncAlreadyRunning as exc:
        # Use JSONResponse to get a top-level body per spec §4.4 instead
        # of FastAPI's HTTPException "detail" wrapper.
        return JSONResponse(
            status_code=409,
            content={
                "error": "sync_already_running",
                "sync_run_id": exc.active_sync_run_id,
            },
        )
    return {"sync_run_id": sync_run_id, "plan": _plan_to_json(plan)}


@router.get("/status")
def get_sync_status(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict[str, Any]:
    """Current running sync (if any) + active layer row."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT sync_run_id, scope, trigger, started_at,
                   layers_planned, layers_done, layers_failed, layers_skipped
            FROM sync_runs
            WHERE status = 'running'
            ORDER BY started_at DESC
            LIMIT 1
            """,
        )
        row = cur.fetchone()
    if row is None:
        return {"is_running": False, "current_run": None, "active_layer": None}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT layer_name, started_at, items_total, items_done
            FROM sync_layer_progress
            WHERE sync_run_id = %s AND status = 'running'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (row["sync_run_id"],),
        )
        active = cur.fetchone()
    return {
        "is_running": True,
        "current_run": {
            "sync_run_id": row["sync_run_id"],
            "scope": row["scope"],
            "trigger": row["trigger"],
            "started_at": row["started_at"].isoformat(),
            "layers_planned": row["layers_planned"],
            "layers_done": row["layers_done"],
            "layers_failed": row["layers_failed"],
            "layers_skipped": row["layers_skipped"],
        },
        "active_layer": None
        if active is None
        else {
            "name": active["layer_name"],
            "started_at": active["started_at"].isoformat() if active["started_at"] else None,
            "items_total": active["items_total"],
            "items_done": active["items_done"],
        },
    }


@router.get("/layers")
def get_sync_layers(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict[str, Any]:
    """All 15 layers with freshness + last successful run."""
    # One pair of queries instead of two-per-layer in the loop below.
    # Was O(N) round-trips; now O(1). The layer name filter keeps both
    # queries on an index seek regardless of how large the history
    # table grows.
    failure_streaks, persisted_errors = all_layer_histories(conn, list(LAYERS.keys()))

    # _LAYER_TO_JOB is built and asserted-disjoint at module import.
    out: list[dict[str, Any]] = []
    for name, layer in LAYERS.items():
        # Per-layer isolation: one broken predicate should not 500 the
        # whole endpoint. Operators need the dashboard most when things
        # are red; masking a partial failure would hide which layer broke.
        #
        # Exception type names are NEVER exposed raw — they leak Python
        # internals and violate spec §3.4 sanitized-category contract.
        # Route through classify_exception so the response only carries a
        # stable, documented category.
        try:
            fresh, detail = layer.is_fresh(conn)
            predicate_error: str | None = None
        except Exception as exc:
            from app.services.sync_orchestrator.exception_classifier import classify_exception

            predicate_error = classify_exception(exc).value
            fresh = False
            detail = f"freshness predicate error ({predicate_error})"
        job_name = _LAYER_TO_JOB[name]
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT started_at, finished_at
                FROM job_runs
                WHERE job_name = %s AND status = 'success'
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (job_name,),
            )
            last = cur.fetchone()
        last_success_at = last["finished_at"] if last else None
        last_start = last["started_at"] if last else None
        # Failure history comes from sync_layer_progress, not from
        # job_runs. The in-request predicate_error above only fires
        # when freshness evaluation itself raised; it does not cover
        # "the layer has failed three runs in a row". Both callers
        # are triage signals, so we prefer the in-request error when
        # present and fall back to the most-recent-persisted category
        # otherwise. Both values come from the two batched queries
        # above — no per-layer round-trips.
        consec = failure_streaks.get(name, 0)
        persisted_error = persisted_errors.get(name)
        out.append(
            {
                "name": name,
                "display_name": layer.display_name,
                "tier": layer.tier,
                "is_fresh": fresh,
                "freshness_detail": detail,
                "last_success_at": last_success_at.isoformat() if last_success_at else None,
                "last_duration_seconds": (
                    int((last_success_at - last_start).total_seconds()) if last_success_at and last_start else None
                ),
                "last_error_category": predicate_error or persisted_error,
                "consecutive_failures": consec,
                "dependencies": list(layer.dependencies),
                "is_blocking": layer.is_blocking,
            }
        )
    return {"layers": out}


@router.get("/layers/v2", response_model=SyncLayersV2Response)
def get_sync_layers_v2(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SyncLayersV2Response:
    """v2: structured triage payload (spec §8).

    Returns one bucket per LayerState, plus cascade groups + a one-line
    system_summary. Designed as the sole feed for the new Admin UI
    (sub-project C). v1 /sync/layers is unchanged.
    """
    states = compute_layer_states_from_db(conn)
    names = list(states.keys())
    streaks, categories = all_layer_histories(conn, names)
    error_excerpts = all_layer_error_excerpts(conn, names)
    last_updates = _layer_last_updated_map(conn, names)

    if any(s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING} for s in states.values()):
        system_state = "needs_attention"
    elif any(
        s in {LayerState.DEGRADED, LayerState.RUNNING, LayerState.RETRYING, LayerState.CASCADE_WAITING}
        for s in states.values()
    ):
        system_state = "catching_up"
    else:
        system_state = "ok"

    action_needed: list[ActionNeededItem] = []
    secret_missing: list[SecretMissingItem] = []
    degraded: list[LayerSummary] = []
    healthy: list[LayerSummary] = []
    disabled: list[LayerSummary] = []

    # Build once per request using the extracted pure function.
    deps_map: dict[str, tuple[str, ...]] = {n: lay.dependencies for n, lay in LAYERS.items()}
    groups = collapse_cascades(deps_map, states)
    groups_by_root: dict[str, list[str]] = {g.root: list(g.affected) for g in groups}

    category_values = {c.value for c in FailureCategory}

    for name, state in states.items():
        layer = LAYERS[name]
        summary = LayerSummary(
            layer=name,
            display_name=layer.display_name,
            last_updated=last_updates.get(name),
        )
        if state is LayerState.ACTION_NEEDED:
            raw_cat = categories.get(name) or "internal_error"
            category = FailureCategory(raw_cat) if raw_cat in category_values else FailureCategory.INTERNAL_ERROR
            remedy = REMEDIES[category]
            action_needed.append(
                ActionNeededItem(
                    root_layer=name,
                    display_name=layer.display_name,
                    category=category.value,
                    operator_message=remedy.message,
                    operator_fix=remedy.operator_fix,
                    self_heal=remedy.self_heal,
                    consecutive_failures=streaks.get(name, 0),
                    affected_downstream=groups_by_root.get(name, []),
                    error_excerpt=error_excerpts.get(name),
                )
            )
        elif state is LayerState.SECRET_MISSING:
            # Use the first env var that is currently missing as the
            # displayed one. Fall back to the first declared secret_ref
            # if none look missing (a race with env-set between state
            # computation and this loop) — the layer still shows up in
            # the response rather than silently disappearing.
            missing = next(
                (ref for ref in layer.secret_refs if not os.environ.get(ref.env_var)),
                layer.secret_refs[0] if layer.secret_refs else None,
            )
            if missing is None:
                # A layer without any secret_refs should never reach
                # SECRET_MISSING (state machine wouldn't emit it), but
                # if it does, keep it visible via a generic row.
                secret_missing.append(
                    SecretMissingItem(
                        layer=name,
                        display_name=layer.display_name,
                        missing_secret="(unknown)",
                        operator_fix="Check layer secret configuration",
                    )
                )
            else:
                secret_missing.append(
                    SecretMissingItem(
                        layer=name,
                        display_name=layer.display_name,
                        missing_secret=missing.env_var,
                        operator_fix=f"Set {missing.env_var} in Settings → Providers",
                    )
                )
        elif state is LayerState.DEGRADED:
            degraded.append(summary)
        elif state is LayerState.HEALTHY:
            healthy.append(summary)
        elif state is LayerState.DISABLED:
            disabled.append(summary)
        # RUNNING / RETRYING / CASCADE_WAITING feed into system_state
        # counts + cascade_groups; no top-level bucket.

    cascade_groups = [CascadeGroupModel(root=g.root, affected=list(g.affected)) for g in groups]

    running_count = sum(1 for s in states.values() if s is LayerState.RUNNING)
    retrying_count = sum(1 for s in states.values() if s is LayerState.RETRYING)
    cascade_waiting_count = sum(1 for s in states.values() if s is LayerState.CASCADE_WAITING)

    layers_entries = [
        LayerEntry(
            layer=name,
            display_name=LAYERS[name].display_name,
            state=states[name],
            last_updated=last_updates.get(name),
            plain_language_sla=LAYERS[name].plain_language_sla,
        )
        for name in sorted(states.keys())
    ]

    return SyncLayersV2Response(
        generated_at=datetime.now(UTC),
        system_state=system_state,
        system_summary=_system_summary(
            action_needed=action_needed,
            secret_missing=secret_missing,
            degraded=degraded,
            running_count=running_count,
            retrying_count=retrying_count,
            cascade_waiting_count=cascade_waiting_count,
        ),
        action_needed=action_needed,
        degraded=degraded,
        secret_missing=secret_missing,
        healthy=healthy,
        disabled=disabled,
        cascade_groups=cascade_groups,
        layers=layers_entries,
    )


@router.post("/layers/{layer_name}/enabled", response_model=LayerEnabledResponse)
def post_layer_enabled(
    layer_name: str,
    body: LayerEnabledRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> LayerEnabledResponse:
    """Toggle a layer's operator-enabled flag.

    Any layer can be toggled. Safety-critical layers (fx_rates,
    portfolio_sync) surface a warning the UI shows as a toast; actual
    BUY/ADD blocking lives in the execution_guard
    safety_layers_enabled rule (chunk 2 task 2.2).
    """
    if layer_name not in LAYERS:
        raise HTTPException(status_code=404, detail=f"unknown layer: {layer_name}")
    # #346: safety-critical disables MUST carry a reason. Direct API
    # / service-token callers (no UI confirm dialog in the loop) would
    # otherwise be able to flip ``fx_rates`` or ``portfolio_sync`` off
    # without operator attribution, which is inconsistent with the
    # kill-switch / live-trading toggles.
    # Trim once so both the safety gate and the audit row see the same
    # value — passing the unstripped form to set_layer_enabled would
    # store leading/trailing whitespace despite the gate having
    # rejected it.
    reason_trimmed = body.reason.strip() if body.reason is not None else None
    if not body.enabled and layer_name in SAFETY_CRITICAL_LAYERS:
        if not reason_trimmed:
            raise HTTPException(
                status_code=400,
                detail=(f"layer '{layer_name}' is safety-critical; supply a non-empty 'reason' to disable it."),
            )
    set_layer_enabled(
        conn,
        layer_name,
        enabled=body.enabled,
        reason=reason_trimmed,
        changed_by=body.changed_by,
    )
    conn.commit()
    return LayerEnabledResponse(
        layer=layer_name,
        display_name=LAYERS[layer_name].display_name,
        is_enabled=body.enabled,
        warning=_safety_warning(layer_name, body.enabled),
    )


# ---------------------------------------------------------------------------
# Non-orchestrator ingest toggles (#414)
# ---------------------------------------------------------------------------

# Ingest keys the operator can pause/resume at runtime without a restart.
# Distinct from ``LAYERS`` because these gate scheduled *jobs*, not
# orchestrator layers — ``fundamentals_sync`` has no layer emit and is not
# in ``JOB_TO_LAYERS``. Stored in the same ``layer_enabled`` table for
# operational convenience; absent row counts as enabled.
INGEST_TOGGLES: dict[str, str] = {
    "fundamentals_ingest": "Fundamentals ingest (fundamentals_sync)",
}


class IngestToggleResponse(BaseModel):
    key: str
    display_name: str
    is_enabled: bool


@router.post("/ingest/{key}/enabled", response_model=IngestToggleResponse)
def post_ingest_enabled(
    key: str,
    body: LayerEnabledRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> IngestToggleResponse:
    """Pause or resume a scheduled ingest job without restarting the
    server (#414 design goal F).

    Flip ``fundamentals_ingest`` to False to skip the daily
    ``fundamentals_sync`` run (used during demos, or when SEC rate-
    limits us). The job still runs at its cron time, logs the skip,
    and records a zero-row ``job_runs`` entry — so the admin UI
    shows the operator-initiated pause explicitly rather than silent
    staleness.
    """
    if key not in INGEST_TOGGLES:
        raise HTTPException(
            status_code=404,
            detail=f"unknown ingest key: {key}; allowed: {sorted(INGEST_TOGGLES)}",
        )
    set_layer_enabled(
        conn,
        key,
        enabled=body.enabled,
        reason=body.reason,
        changed_by=body.changed_by,
    )
    conn.commit()
    return IngestToggleResponse(
        key=key,
        display_name=INGEST_TOGGLES[key],
        is_enabled=body.enabled,
    )


# ---------------------------------------------------------------------------
# SEC ingest observability (#414 design goal G, #418)
# ---------------------------------------------------------------------------


class CikTimingModeModel(BaseModel):
    mode: Literal["seed", "refresh"]
    count: int
    p50_seconds: float | None
    p95_seconds: float | None
    max_seconds: float | None
    facts_upserted_total: int


class SlowCikModel(BaseModel):
    cik: str
    mode: Literal["seed", "refresh"]
    seconds: float
    facts_upserted: int
    outcome: str
    finished_at: datetime


class CikTimingSummaryResponse(BaseModel):
    ingestion_run_id: int | None
    run_source: str | None
    run_started_at: datetime | None
    run_finished_at: datetime | None
    run_status: str | None
    modes: list[CikTimingModeModel]
    slowest: list[SlowCikModel]


@router.get("/ingest/cik_timing/latest", response_model=CikTimingSummaryResponse)
def get_cik_timing_latest(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CikTimingSummaryResponse:
    """Return p50/p95 per-CIK timing for the most recent SEC XBRL
    ingest run (#418 acceptance: validate ADR 0004 Shape-B bench
    ratios against production without tailing logs).

    If no timing rows exist yet (pre-#418 deploy or empty table), every
    field except an empty-list ``modes``/``slowest`` is null — the UI
    renders "no data yet" rather than a 500.
    """
    summary = get_cik_timing_summary(conn)
    return CikTimingSummaryResponse(
        ingestion_run_id=summary.ingestion_run_id,
        run_source=summary.run_source,
        run_started_at=summary.run_started_at,
        run_finished_at=summary.run_finished_at,
        run_status=summary.run_status,
        modes=[
            CikTimingModeModel(
                mode=m.mode,
                count=m.count,
                p50_seconds=m.p50_seconds,
                p95_seconds=m.p95_seconds,
                max_seconds=m.max_seconds,
                facts_upserted_total=m.facts_upserted_total,
            )
            for m in summary.modes
        ],
        slowest=[
            SlowCikModel(
                cik=s.cik,
                mode=s.mode,
                seconds=s.seconds,
                facts_upserted=s.facts_upserted,
                outcome=s.outcome,
                finished_at=s.finished_at,
            )
            for s in summary.slowest
        ],
    )


class SeedSourceModel(BaseModel):
    source: str
    key_description: str
    seeded: int
    total: int


class LatestIngestionRunModel(BaseModel):
    ingestion_run_id: int
    source: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    rows_upserted: int
    rows_skipped: int


class SeedProgressResponse(BaseModel):
    sources: list[SeedSourceModel]
    latest_run: LatestIngestionRunModel | None
    ingest_paused: bool


@router.get("/ingest/seed_progress", response_model=SeedProgressResponse)
def get_seed_progress_endpoint(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> SeedProgressResponse:
    """Return seed progress ratios + latest-run state + pause flag for
    the admin UI (#414 design goal G).

    ``sources[].total`` is the count of CIK-mapped tradable
    instruments; ``seeded`` is the number of that set with a committed
    watermark. Ratio is the operator's "N of 5,134" seed-progress
    number.
    """
    summary = get_seed_progress(conn)
    return SeedProgressResponse(
        sources=[
            SeedSourceModel(
                source=s.source,
                key_description=s.key_description,
                seeded=s.seeded,
                total=s.total,
            )
            for s in summary.sources
        ],
        latest_run=(
            LatestIngestionRunModel(
                ingestion_run_id=summary.latest_run.ingestion_run_id,
                source=summary.latest_run.source,
                started_at=summary.latest_run.started_at,
                finished_at=summary.latest_run.finished_at,
                status=summary.latest_run.status,
                rows_upserted=summary.latest_run.rows_upserted,
                rows_skipped=summary.latest_run.rows_skipped,
            )
            if summary.latest_run is not None
            else None
        ),
        ingest_paused=summary.ingest_paused,
    )


def _system_summary(
    *,
    action_needed: list[ActionNeededItem],
    secret_missing: list[SecretMissingItem],
    degraded: list[LayerSummary],
    running_count: int,
    retrying_count: int,
    cascade_waiting_count: int,
) -> str:
    """One-line human summary of system state (spec §8).

    Order is deliberate: ACTION_NEEDED > SECRET_MISSING > DEGRADED >
    RUNNING/RETRYING > healthy. RUNNING/RETRYING count is only
    surfaced when nothing ahead of them in priority matches, so the
    summary never contradicts the bucketed response (e.g. a red
    action_needed banner will never be drowned by "2 layers running").
    """
    if action_needed:
        first = action_needed[0].display_name
        count = len(action_needed)
        if count == 1:
            return f"{first} needs attention"
        return f"{count} layer(s) need attention ({first})"
    if secret_missing:
        return f"{len(secret_missing)} layer(s) missing credentials"
    if degraded:
        return f"{len(degraded)} layer(s) catching up"
    # RUNNING / RETRYING / CASCADE_WAITING share the catching_up
    # system_state — surface them here so the summary stays consistent
    # with system_state for a fire-in-flight or self-heal round.
    transient = running_count + retrying_count + cascade_waiting_count
    if transient:
        return f"{transient} layer(s) catching up"
    return "All layers healthy"


def _layer_last_updated_map(conn: psycopg.Connection[Any], names: list[str]) -> dict[str, datetime]:
    """Most recent complete/partial finished_at per layer.

    Orders by `finished_at DESC` — this map selects the *latest
    update time* the UI shows, not the latest start. A long-running
    layer whose started_at precedes finished_at on a quicker sibling
    run must still surface the most recent completion.
    """
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                layer_name, finished_at,
                ROW_NUMBER() OVER (
                    PARTITION BY layer_name
                    ORDER BY finished_at DESC, sync_run_id DESC
                ) AS rn
            FROM sync_layer_progress
            WHERE layer_name = ANY(%s)
              AND status IN ('complete', 'partial')
              AND finished_at IS NOT NULL
        )
        SELECT layer_name, finished_at FROM ranked WHERE rn = 1
        """,
        (names,),
    ).fetchall()
    return {str(r[0]): r[1] for r in rows if r[1] is not None}


@router.get("/runs")
def get_sync_runs(
    conn: psycopg.Connection[object] = Depends(get_conn),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Recent sync runs, newest first."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT sync_run_id, scope, scope_detail, trigger, started_at,
                   finished_at, status, layers_planned, layers_done,
                   layers_failed, layers_skipped
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return {
        "runs": [
            {
                "sync_run_id": r["sync_run_id"],
                "scope": r["scope"],
                "scope_detail": r["scope_detail"],
                "trigger": r["trigger"],
                "started_at": r["started_at"].isoformat(),
                "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
                "status": r["status"],
                "layers_planned": r["layers_planned"],
                "layers_done": r["layers_done"],
                "layers_failed": r["layers_failed"],
                "layers_skipped": r["layers_skipped"],
            }
            for r in rows
        ]
    }
