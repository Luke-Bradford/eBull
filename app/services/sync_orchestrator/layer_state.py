"""Compute LayerState for every registered layer (spec §3.2).

`compute_layer_state(ctx) -> LayerState` is pure. Input is a
LayerContext, output is a LayerState. The DB-facing builder
`compute_layer_states_from_db(conn)` lives in the same module and
applies fixed-point cascade propagation.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.services.sync_orchestrator.layer_types import (
    REMEDIES,
    FailureCategory,
    LayerState,
)
from app.services.sync_orchestrator.types import PREREQ_SKIP_MARKER

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LayerContext:
    is_enabled: bool
    is_running: bool
    # sync_layer_progress.status vocabulary: 'pending' | 'running' |
    # 'complete' | 'failed' | 'skipped' | 'partial'. A caller reading
    # from job_runs (success/failure/skipped) must translate before
    # building a context.
    latest_status: str
    latest_category: str | None
    attempts: int
    upstream_states: dict[str, LayerState]
    secret_present: bool
    content_ok: bool
    age_seconds: float
    cadence_seconds: float
    grace_multiplier: float
    max_attempts: int


def compute_layer_state(ctx: LayerContext) -> LayerState:
    # Rule 1: operator toggle wins.
    if not ctx.is_enabled:
        return LayerState.DISABLED
    # Rule 2: run in flight.
    if ctx.is_running:
        return LayerState.RUNNING
    # Rule 3: missing secrets beat stale failure rows.
    if not ctx.secret_present:
        return LayerState.SECRET_MISSING
    # Rules 4-6: local-failure branches.
    if ctx.latest_status == "failed":
        category_values = {c.value for c in FailureCategory}
        category = (
            FailureCategory(ctx.latest_category)
            if ctx.latest_category in category_values
            else FailureCategory.INTERNAL_ERROR
        )
        remedy = REMEDIES[category]
        if not remedy.self_heal:
            return LayerState.ACTION_NEEDED
        if ctx.attempts >= ctx.max_attempts:
            return LayerState.ACTION_NEEDED
        return LayerState.RETRYING
    # Rule 7: cascade. Terminal upstream states (ACTION_NEEDED,
    # SECRET_MISSING) originate a cascade; CASCADE_WAITING upstream
    # propagates it transitively so every layer downstream of a root
    # failure is visible to the collapse_cascades grouper (spec §6).
    # DEGRADED, RUNNING, RETRYING are self-healing and do NOT cascade
    # (spec §3.3).
    if any(
        s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING, LayerState.CASCADE_WAITING}
        for s in ctx.upstream_states.values()
    ):
        return LayerState.CASCADE_WAITING
    # Rule 8: content predicate.
    if not ctx.content_ok:
        return LayerState.DEGRADED
    # Rule 9: age vs grace window.
    if ctx.age_seconds > ctx.cadence_seconds * ctx.grace_multiplier:
        return LayerState.DEGRADED
    return LayerState.HEALTHY


# ---------------------------------------------------------------------------
# DB-facing builder
# ---------------------------------------------------------------------------

# Max cascade-propagation iterations. DAG depth is ≤ 10 in practice
# (test pins this). The loop exits early once states stabilise.
MAX_STATE_ITERATIONS = 16


def compute_layer_states_from_db(
    conn: psycopg.Connection[Any],
    *,
    suppress_auth_expired_before: datetime | None = None,
) -> dict[str, LayerState]:
    """Build a LayerState for every registered layer by reading
    sync_layer_progress + content predicates + layer_enabled + secrets.

    Fixed-point iteration propagates cascade state across DAG depth.
    Converges in at most MAX_STATE_ITERATIONS rounds; safety-capped so
    a future cycle in the registry cannot hang the planning query.

    AUTH_EXPIRED suppression (#977 / #974/C):
      ``suppress_auth_expired_before`` is plumbed through to
      ``all_layer_histories`` so an old pre-recovery auth_expired
      failure cannot push a layer into ACTION_NEEDED after the
      operator has fixed their keys (Codex pre-push r3.2). Caller
      resolves the timestamp from
      ``credential_health.get_last_recovered_at``; passing None means
      no suppression.
    """
    # Deferred imports to avoid circular imports at module load time.
    # layer_types is at the bottom of the import graph; registry imports
    # layer_types; layer_enabled imports nothing from sync_orchestrator.
    from app.services.layer_enabled import read_all_enabled
    from app.services.sync_orchestrator.layer_failure_history import all_layer_histories
    from app.services.sync_orchestrator.registry import LAYERS

    names = list(LAYERS.keys())
    enabled = read_all_enabled(conn, names)
    streaks, categories = all_layer_histories(
        conn,
        names,
        suppress_auth_expired_before=suppress_auth_expired_before,
    )
    running_set = _running_layers(conn, names)
    latest_status = _latest_status_map(conn, names)
    latest_ages = _latest_age_seconds_map(conn, names)
    content_results = _content_ok_map(conn)
    # Snapshot a single ``now`` per state-machine evaluation so all
    # calendar-month boundary computations share the same reference
    # instant. Drift between layers within a single evaluation would
    # be incoherent (different layers seeing different "this month").
    now = datetime.now(UTC)

    def build(name: str, upstream: dict[str, LayerState]) -> LayerContext:
        layer = LAYERS[name]
        status = latest_status.get(name, "__never_run__")
        if status == "__never_run__":
            age_seconds: float = float("inf")
            status = "complete"
        else:
            age_seconds = latest_ages.get(name, float("inf"))
        # Belt-and-braces: any future caller (logging, ratio math)
        # that touches ``cadence_seconds`` should never see zero.
        # ``Cadence.cadence_seconds_for_state_machine`` already
        # floors to 1 second; this assert turns a regression into a
        # loud failure during the planning sweep.
        cadence_seconds = layer.cadence.cadence_seconds_for_state_machine(now, layer.grace_multiplier)
        assert cadence_seconds > 0, f"cadence_seconds must be positive (got {cadence_seconds} for {name})"
        return LayerContext(
            is_enabled=enabled.get(name, True),
            is_running=name in running_set,
            latest_status=status,
            latest_category=categories.get(name),
            attempts=streaks.get(name, 0),
            upstream_states=upstream,
            secret_present=all(bool(os.environ.get(ref.env_var)) for ref in layer.secret_refs),
            content_ok=content_results.get(name, True),
            age_seconds=age_seconds,
            # ``cadence_seconds_for_state_machine`` is calendar-aware
            # (#335): for ``calendar_months`` cadences it returns the
            # boundary distance such that rule 9
            # (``age_seconds > cadence_seconds * grace_multiplier``)
            # fires at the calendar tick, not after a 31-day rolling
            # window.
            cadence_seconds=cadence_seconds,
            grace_multiplier=layer.grace_multiplier,
            max_attempts=layer.retry_policy.max_attempts,
        )

    # Round 0: compute every layer without upstream info.
    current: dict[str, LayerState] = {name: compute_layer_state(build(name, {})) for name in names}

    # Fixed-point: iterate until stable or cap reached.
    for _ in range(MAX_STATE_ITERATIONS):
        next_states = {
            name: compute_layer_state(build(name, {dep: current[dep] for dep in LAYERS[name].dependencies}))
            for name in names
        }
        if next_states == current:
            return next_states
        current = next_states
    return current


def _running_layers(conn: psycopg.Connection[Any], names: list[str]) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT layer_name
        FROM sync_layer_progress
        WHERE status = 'running' AND layer_name = ANY(%s)
        """,
        (names,),
    ).fetchall()
    return {str(r[0]) for r in rows}


def _latest_status_map(conn: psycopg.Connection[Any], names: list[str]) -> dict[str, str]:
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                layer_name, status,
                ROW_NUMBER() OVER (
                    PARTITION BY layer_name
                    ORDER BY COALESCE(started_at, finished_at) DESC NULLS LAST, sync_run_id DESC
                ) AS rn
            FROM sync_layer_progress
            WHERE layer_name = ANY(%s)
        )
        SELECT layer_name, status FROM ranked WHERE rn = 1
        """,
        (names,),
    ).fetchall()
    out = {str(r[0]): str(r[1]) for r in rows}
    # Never-run layer: sentinel that the context-builder translates to
    # age=inf → DEGRADED. Do NOT default to 'complete' — that would
    # mark a layer HEALTHY despite having no runs on record.
    for name in names:
        out.setdefault(name, "__never_run__")
    return out


def _latest_age_seconds_map(conn: psycopg.Connection[Any], names: list[str]) -> dict[str, float]:
    # Counting rows anchor freshness age: complete/partial runs always
    # count; skipped rows count ONLY when they were PREREQ_SKIP (layer
    # intentionally did nothing because a prereq was missing). A
    # DEP_SKIPPED row marks a layer that was prevented from running by
    # an upstream failure — it is not evidence of freshness and must
    # not make a downstream look HEALTHY after the upstream clears.
    prereq_pattern = f"{PREREQ_SKIP_MARKER}%"
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT
                layer_name,
                COALESCE(finished_at, started_at) AS anchor,
                ROW_NUMBER() OVER (
                    PARTITION BY layer_name
                    ORDER BY COALESCE(started_at, finished_at) DESC NULLS LAST, sync_run_id DESC
                ) AS rn
            FROM sync_layer_progress
            WHERE layer_name = ANY(%s)
              AND (
                status IN ('complete', 'partial')
                OR (status = 'skipped' AND skip_reason LIKE %s)
              )
        )
        SELECT layer_name, EXTRACT(EPOCH FROM (now() - anchor)) AS age
        FROM ranked WHERE rn = 1 AND anchor IS NOT NULL
        """,
        (names, prereq_pattern),
    ).fetchall()
    return {str(r[0]): float(r[1]) for r in rows}


def _content_ok_map(conn: psycopg.Connection[Any]) -> dict[str, bool]:
    """Invoke each layer's `content_predicate` if declared. Layers
    without a predicate default to True (content checks are opt-in
    per spec §4)."""
    from app.services.sync_orchestrator.registry import LAYERS

    out: dict[str, bool] = {}
    for name, layer in LAYERS.items():
        if layer.content_predicate is None:
            out[name] = True
            continue
        try:
            ok, _detail = layer.content_predicate(conn)
        except Exception:
            # Broken content predicate is not a freshness signal —
            # treat as content-ok to avoid masking real failures, but
            # surface the exception so operators can see when a
            # predicate is wedged (bad SQL, missing table, etc.).
            logger.warning(
                "content_predicate for %s raised; treating content_ok=True",
                name,
                exc_info=True,
            )
            ok = True
        out[name] = ok
    return out
