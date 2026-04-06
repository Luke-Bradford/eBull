"""System config and kill switch API endpoints (issue #56).

Endpoints:
  - GET   /config              — current runtime flags + kill switch + env
  - PATCH /config              — partial update of runtime flags
  - POST  /config/kill-switch  — toggle the DB-backed kill switch

All endpoints require operator auth.  GET is auth-protected because the
runtime flags reveal whether live trading is enabled — sensitive on its own.

Source of truth:
  - enable_auto_trading / enable_live_trading live in `runtime_config`
    (DB-backed singleton).  They are NOT read from app.config.settings.
  - kill switch lives in the existing `kill_switch` singleton.
  - app_env / etoro_env are deployment config and are read from settings.

Fail-closed posture:
  - Missing `runtime_config` row -> 503 (configuration corrupt).  Never
    auto-recreated and never substituted with default values.
  - Missing `kill_switch` row -> the underlying service surfaces is_active=True
    (already implemented in ops_monitor.get_kill_switch_status); we surface
    that string in the response without claiming the system is healthy.

PATCH semantics:
  - Partial: any flag left unset stays as-is.
  - Both `updated_by` and `reason` are required for every mutation.
  - Enabling enable_live_trading=True additionally requires
    `confirm_live_enable=true` — live trading is the highest-stakes flag.
    Disabling live trading requires no confirmation (you can always stop).
  - At least one flag must be provided; an empty patch is a 422.
"""

from __future__ import annotations

import logging
from datetime import datetime

import psycopg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator

from app.api.auth import require_auth
from app.config import settings
from app.db import get_conn
from app.services.ops_monitor import (
    activate_kill_switch,
    deactivate_kill_switch,
    get_kill_switch_status,
)
from app.services.runtime_config import (
    RuntimeConfigCorrupt,
    RuntimeConfigNoOp,
    get_runtime_config,
    update_runtime_config,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/config",
    tags=["config"],
    dependencies=[Depends(require_auth)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class RuntimeFlagsResponse(BaseModel):
    enable_auto_trading: bool
    enable_live_trading: bool
    updated_at: datetime
    updated_by: str
    reason: str


class KillSwitchResponse(BaseModel):
    # active=True is also used as the fail-closed value when the singleton row
    # is missing — see ops_monitor.get_kill_switch_status.
    active: bool
    activated_at: datetime | None
    activated_by: str | None
    reason: str | None


class ConfigResponse(BaseModel):
    app_env: str
    etoro_env: str
    runtime: RuntimeFlagsResponse
    kill_switch: KillSwitchResponse


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class ConfigPatchRequest(BaseModel):
    """Partial update of runtime config flags.

    `updated_by` and `reason` are mandatory on every mutation so the audit
    trail always carries attribution.

    `confirm_live_enable` is only consulted when the patch sets
    `enable_live_trading=True`.  It is ignored otherwise — disabling live
    trading must always be possible without ceremony.
    """

    updated_by: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    enable_auto_trading: bool | None = None
    enable_live_trading: bool | None = None
    confirm_live_enable: bool = False

    @model_validator(mode="after")
    def _validate_patch(self) -> ConfigPatchRequest:
        if self.enable_auto_trading is None and self.enable_live_trading is None:
            raise ValueError("at least one of enable_auto_trading / enable_live_trading must be provided")
        if self.enable_live_trading is True and not self.confirm_live_enable:
            raise ValueError("enable_live_trading=true requires confirm_live_enable=true")
        return self


class KillSwitchRequest(BaseModel):
    active: bool
    reason: str = ""
    activated_by: str = ""

    @model_validator(mode="after")
    def _reason_required_when_activating(self) -> KillSwitchRequest:
        if self.active and not self.reason.strip():
            raise ValueError("reason is required when activating the kill switch")
        return self


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _build_kill_switch_response(conn: psycopg.Connection[object]) -> KillSwitchResponse:
    ks = get_kill_switch_status(conn)
    return KillSwitchResponse(
        active=bool(ks["is_active"]),
        activated_at=ks.get("activated_at"),
        activated_by=ks.get("activated_by"),
        reason=ks.get("reason"),
    )


@router.get("", response_model=ConfigResponse)
def get_config(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ConfigResponse:
    """Return effective runtime config + kill switch + env."""
    try:
        runtime = get_runtime_config(conn)
    except RuntimeConfigCorrupt as exc:
        # Fail closed: never substitute defaults.
        # Prevention-log: "Health endpoint returns HTTP 200 on infra failure".
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return ConfigResponse(
        app_env=settings.app_env,
        etoro_env=settings.etoro_env,
        runtime=RuntimeFlagsResponse(
            enable_auto_trading=runtime.enable_auto_trading,
            enable_live_trading=runtime.enable_live_trading,
            updated_at=runtime.updated_at,
            updated_by=runtime.updated_by,
            reason=runtime.reason,
        ),
        kill_switch=_build_kill_switch_response(conn),
    )


@router.patch("", response_model=RuntimeFlagsResponse)
def patch_config(
    body: ConfigPatchRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> RuntimeFlagsResponse:
    """Partial update of runtime flags.

    Returns the post-update runtime config (not the full /config response;
    the caller can re-GET if they need kill switch state too).
    """
    try:
        updated = update_runtime_config(
            conn,
            updated_by=body.updated_by,
            reason=body.reason,
            enable_auto_trading=body.enable_auto_trading,
            enable_live_trading=body.enable_live_trading,
        )
    except RuntimeConfigCorrupt as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except RuntimeConfigNoOp as exc:
        # No-op patch — reject so audit table never diverges from singleton.
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        # update_runtime_config raises ValueError only on empty-patch which
        # the pydantic validator already blocks.  Defensive 400 in case the
        # validator is bypassed.
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return RuntimeFlagsResponse(
        enable_auto_trading=updated.enable_auto_trading,
        enable_live_trading=updated.enable_live_trading,
        updated_at=updated.updated_at,
        updated_by=updated.updated_by,
        reason=updated.reason,
    )


@router.post("/kill-switch", response_model=KillSwitchResponse)
def post_kill_switch(
    body: KillSwitchRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> KillSwitchResponse:
    """Activate or deactivate the system-wide kill switch.

    The underlying service writes a runtime_config_audit row in the same
    transaction as the kill_switch UPDATE so the audit trail and the live
    state cannot drift.
    """
    try:
        if body.active:
            activate_kill_switch(
                conn,
                reason=body.reason,
                activated_by=body.activated_by,
            )
        else:
            deactivate_kill_switch(
                conn,
                deactivated_by=body.activated_by,
                reason=body.reason,
            )
    except RuntimeError as exc:
        # Singleton row missing -> configuration corrupt.  503 not 500: this
        # is an environmental fault, not a programmer error.
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    # Read post-write state so the response reflects the authoritative DB row
    # (activated_at, activated_by, reason) rather than echoing the request body.
    return _build_kill_switch_response(conn)
