from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from app.api._debug_ws import router as debug_ws_router
from app.api.alerts import router as alerts_router
from app.api.attribution import router as attribution_router
from app.api.audit import router as audit_router
from app.api.auth import require_session_or_service_token
from app.api.auth_bootstrap import router as auth_bootstrap_router
from app.api.auth_session import router as auth_session_router
from app.api.auth_setup import router as auth_setup_router
from app.api.broker_credentials import router as broker_credentials_router
from app.api.budget import router as budget_router
from app.api.business_summary_admin import router as business_summary_admin_router
from app.api.capability_overrides_admin import router as capability_overrides_admin_router
from app.api.config import KillSwitchRequest, KillSwitchResponse, post_kill_switch
from app.api.config import router as config_router
from app.api.copy_trading import router as copy_trading_router
from app.api.coverage import router as coverage_router
from app.api.filings import router as filings_router
from app.api.instruments import router as instruments_router
from app.api.jobs import router as jobs_router
from app.api.news import router as news_router
from app.api.operator_ingest import router as operator_ingest_router
from app.api.operators import router as operators_router
from app.api.orders import router as orders_router
from app.api.portfolio import router as portfolio_router
from app.api.recommendations import router as recommendations_router
from app.api.reports import router as reports_router
from app.api.scores import router as scores_router
from app.api.sse_quotes import router as sse_quotes_router
from app.api.sync import router as sync_router
from app.api.system import router as system_router
from app.api.theses import instrument_thesis_router
from app.api.theses import router as theses_router
from app.api.watchlist import router as watchlist_router
from app.config import settings
from app.db import get_conn
from app.db.migrations import migration_status, run_migrations
from app.db.pool import open_pool
from app.jobs.credential_health_listener import listener_loop as credential_health_listener_loop
from app.security import master_key
from app.security.secrets_crypto import set_active_key as set_broker_encryption_key
from app.services.broker_credentials import (
    CredentialNotFound,
    load_credential_for_provider_use,
)
from app.services.coverage import override_tier
from app.services.credential_health_cache import CredentialHealthCache
from app.services.etoro_websocket import EtoroWebSocketSubscriber
from app.services.operator_setup import ensure_startup_token, operators_empty
from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)
from app.services.quote_stream import QuoteBus
from app.services.sync_orchestrator.layer_state import compute_layer_states_from_db
from app.services.sync_orchestrator.layer_types import LayerState

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

# Third-party loggers default to INFO, which is far too noisy for our
# workload. The SEC ingester fan-out emits 500-1000 httpx requests
# per hourly tick; without this hoist, every fetch logs an INFO line
# to stdout. Under sustained load the resulting print-throughput
# starves the asyncio event loop on stdout I/O — unrelated request
# handlers go unresponsive while the SEC backfill is mid-run.
# WARNING preserves error visibility (4xx/5xx, retries) without the
# per-request flood.
logging.getLogger("httpx").setLevel(logging.WARNING)
# httpcore is the connection-level layer underneath httpx; its
# DEBUG/INFO is even noisier (one line per socket event).
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Running pending migrations...")
    applied = await asyncio.to_thread(run_migrations)
    if applied:
        logger.info("Applied %d migration(s): %s", len(applied), applied)
    else:
        logger.info("No pending migrations.")

    # Open the connection pool after migrations so the schema is up to date.
    pool = open_pool("db_pool", min_size=1, max_size=10)
    logger.info("Connection pool opened (min=1, max=10).")
    app.state.db_pool = pool

    # #111: dedicated small pool for durable credential-access audit
    # writes. Using the request pool risks losing audit rows under
    # saturation (the request handler holds one slot for the read,
    # acquires another for the audit, and a saturated request pool
    # would drop the audit). Sized at min=1, max=2 — audit writes
    # are short-lived single-row INSERTs that don't need
    # parallelism. ADR 0001 requires audit-on-every-decryption to
    # be durable independent of caller outcome.
    audit_pool = open_pool("audit_pool", min_size=1, max_size=2)
    logger.info("Audit pool opened (min=1, max=2).")
    app.state.audit_pool = audit_pool

    # First-run bootstrap token (issue #106 / ADR 0002).
    with pool.connection() as conn:
        ensure_startup_token(operators_empty=operators_empty(conn))

        # Master-key bootstrap (#114 / ADR-0003). Must run AFTER the
        # pool is open (we need the connection to verify ciphertext)
        # and BEFORE yield (so the boot state is fixed before the
        # first request lands). Never raises on a missing file -- a
        # missing file with existing credentials puts the app in
        # recovery_required mode and the frontend routes to /recover.
        # Does raise on EBULL_SECRETS_KEY mismatch with existing
        # ciphertext (fail-loud, ADR-0003 §6).
        boot = master_key.bootstrap(conn)
    app.state.boot_state = boot.state
    app.state.needs_setup = boot.needs_setup
    app.state.recovery_required = boot.recovery_required
    app.state.broker_key_loaded = boot.broker_encryption_key is not None
    if boot.broker_encryption_key is not None:
        set_broker_encryption_key(boot.broker_encryption_key)
    logger.info(
        "Master-key bootstrap: state=%s needs_setup=%s recovery_required=%s",
        boot.state,
        boot.needs_setup,
        boot.recovery_required,
    )

    # Reaper relocates to the jobs entrypoint in #719. The API
    # process must not start the orchestrator and must not reap on
    # behalf of a process it no longer owns.

    # FX bootstrap (#502). Some non-SSE handlers (portfolio, copy-
    # trading, budget/execution) read ``live_fx_rates`` synchronously
    # during request handling and degrade or fail-closed if the
    # table is empty. After PR C cut the cron from hourly to daily,
    # a fresh DB or wiped table would have no rates available until
    # the next 17:00 CET tick. Fire one inline Frankfurter fetch
    # here so the operator's first request post-boot has rates ready.
    #
    # #719 race note: the jobs process also runs a boot-time freshness
    # sweep that may call ``fx_rates_refresh`` if the layer is past its
    # freshness target. The API's ``_bootstrap_fx_rates`` and the
    # orchestrator both check the table is empty / stale before
    # fetching, and the Frankfurter UPSERT keys on (asof, base, quote)
    # so a concurrent double-fetch is at worst one wasted HTTP call —
    # not a correctness bug. The API-side path is preserved because
    # API request-handlers cannot wait on the jobs process to boot.
    try:
        _bootstrap_fx_rates(pool)
    except Exception:
        logger.exception("fx bootstrap failed — continuing startup, daily cron will retry")

    # JobRuntime, sync orchestrator executor registration, reaper, and
    # boot freshness sweep all moved to the out-of-process jobs runtime
    # in #719 (`python -m app.jobs`). The API process serves HTTP only.
    # Smoke tests assert ``app.state.job_runtime`` is not set.

    # In-process quote-tick fan-out bus (#274 Slice 3). Created here
    # so it lives for the full app lifetime; the WS subscriber
    # publishes to it, the SSE endpoint reads from it. Always
    # constructed even if the WS subscriber fails to start — the
    # bus is harmless when nothing publishes, and the SSE endpoint
    # simply emits heartbeats until ticks flow.
    quote_bus = QuoteBus()
    app.state.quote_bus = quote_bus

    # Credential health cache + listener (#976 / #974/B). The cache
    # is process-local and starts in pre-initialized state — every
    # read returns MISSING until the listener completes its first
    # full-table scan. Consumers (admin UI, future WS subscriber
    # reload after #978) treat MISSING as "fail-safe; do not
    # connect / run".
    credential_health_cache = CredentialHealthCache()
    app.state.credential_health_cache = credential_health_cache
    credential_health_stop = threading.Event()
    app.state.credential_health_stop = credential_health_stop
    credential_health_thread = threading.Thread(
        target=credential_health_listener_loop,
        kwargs={
            "cache": credential_health_cache,
            "pool": pool,
            "stop_event": credential_health_stop,
        },
        name="api-credential-health-listener",
        daemon=True,
    )
    credential_health_thread.start()
    app.state.credential_health_thread = credential_health_thread

    # eToro WebSocket live-price subscriber (#274 Slice 1+2+3). Starts
    # only when broker credentials are loadable — otherwise the
    # operator hasn't completed setup yet and there's nothing to
    # subscribe to. WS failures must NOT block the rest of the app.
    ws_subscriber = await _maybe_start_etoro_ws(pool, audit_pool, quote_bus, credential_health_cache)
    app.state.etoro_ws = ws_subscriber

    yield

    if ws_subscriber is not None:
        try:
            # Bound the WS stop. ``EtoroWebSocketSubscriber.stop()``
            # already has internal 30s caps on its reconcile-thread
            # wait, but a stuck WS task cancel (rare but observed
            # during watcher-driven reload races) would otherwise
            # block the lifespan teardown indefinitely. 35s gives the
            # internal 30s wait headroom + a 5s outer envelope.
            await asyncio.wait_for(ws_subscriber.stop(), timeout=35.0)
        except TimeoutError:
            logger.warning("EtoroWebSocketSubscriber.stop exceeded 35s — proceeding with teardown")
        except Exception:
            logger.exception("EtoroWebSocketSubscriber.stop failed")

    # Stop the credential-health listener before closing the pool so
    # the inner LISTEN loop's connection close doesn't race a pool
    # shutdown. Daemon thread auto-joins on process exit, but we wait
    # briefly for clean teardown logs.
    try:
        credential_health_stop.set()
        credential_health_thread.join(timeout=5.0)
    except Exception:
        logger.exception("credential-health listener stop raised")

    audit_pool.close()
    logger.info("Audit pool closed.")
    pool.close()
    logger.info("Connection pool closed.")


def _bootstrap_fx_rates(pool: ConnectionPool[Any]) -> None:
    """Populate ``live_fx_rates`` synchronously when the table is empty.

    Per the visibility-driven live-prices spec
    (docs/superpowers/specs/2026-04-25-visibility-driven-live-prices-spec.md
    PR C), the daily Frankfurter cron does not guarantee rates are
    in the table at boot — a fresh DB, a wiped table, or a process
    restart between ECB publishes would all leave readers seeing
    no rows. Non-SSE handlers (``/portfolio``, ``/portfolio/copy-trading``,
    ``budget``) read ``live_fx_rates`` synchronously and either
    degrade or fail-closed if it is empty.

    Strategy: count the table; if non-empty, no-op. If empty, call
    ``fx_rates_refresh`` directly. Calling the actual job (rather
    than duplicating its body) means the FX watermark + ``job_runs``
    audit row advance the same way they would on a normal cron tick
    — so APScheduler's boot catch-up sees the job as fresh and
    does not fire a second back-to-back Frankfurter hit (Codex
    round 2 finding 1 on PR for #502).

    Runs BEFORE ``start_runtime()`` so the scheduler boot path
    cannot race this call on the same row (spec v3 ordering pin).
    """
    from app.workers.scheduler import fx_rates_refresh

    with pool.connection() as conn:
        row = conn.execute("SELECT COUNT(*) FROM live_fx_rates").fetchone()
        existing = int(row[0]) if row and row[0] is not None else 0
        if existing > 0:
            logger.info("fx bootstrap: %d existing rows, skipping inline fetch", existing)
            return

    logger.info("fx bootstrap: live_fx_rates is empty, running fx_rates_refresh inline")
    try:
        fx_rates_refresh()
    except Exception:
        logger.warning(
            "fx bootstrap: fx_rates_refresh raised; daily cron will retry",
            exc_info=True,
        )


async def _maybe_start_etoro_ws(
    pool: ConnectionPool[Any],
    audit_pool: ConnectionPool[Any],
    bus: QuoteBus,
    credential_cache: CredentialHealthCache,
) -> EtoroWebSocketSubscriber | None:
    """Boot the WS subscriber when credentials are available.

    Pulled out of ``lifespan`` so the credential-load + subscriber
    start runs under a single broad try/except — a missing or
    invalid credential pair must not crash startup. Returns ``None``
    when credentials aren't loadable yet (pre-setup flow).
    """
    try:
        with pool.connection() as conn:
            op_id = sole_operator_id(conn)
            # #111: pass the dedicated audit pool so the audit row
            # is written on a side connection from a separate pool
            # — durable independent of this conn's transaction state
            # AND independent of request-pool saturation.
            api_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="api_key",
                environment=settings.etoro_env,
                caller="etoro_ws_subscriber",
                audit_pool=audit_pool,
            )
            user_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="user_key",
                environment=settings.etoro_env,
                caller="etoro_ws_subscriber",
                audit_pool=audit_pool,
            )
    except (NoOperatorError, AmbiguousOperatorError, CredentialNotFound) as exc:
        logger.info(
            "EtoroWebSocketSubscriber not started (%s): %s",
            type(exc).__name__,
            exc,
        )
        return None
    except Exception:
        logger.exception("EtoroWebSocketSubscriber credential load failed")
        return None

    subscriber = EtoroWebSocketSubscriber(
        api_key=api_key,
        user_key=user_key,
        env=settings.etoro_env,
        pool=pool,
        bus=bus,
        # Credential-aware mode (#978 / #974/D): pre-flight on cache,
        # exponential backoff on auth failure, write-through on every
        # auth outcome. Falls back to legacy fixed-5s reconnect when
        # any of these is None.
        operator_id=op_id,
        credential_cache=credential_cache,
        audit_pool=audit_pool,
    )
    try:
        await subscriber.start()
    except Exception:
        logger.exception("EtoroWebSocketSubscriber.start failed")
        return None
    return subscriber


app = FastAPI(title="eBull", version="0.1.0", lifespan=lifespan)
app.include_router(alerts_router)
app.include_router(attribution_router)
app.include_router(auth_setup_router)
app.include_router(auth_bootstrap_router)
app.include_router(auth_session_router)
app.include_router(operators_router)
app.include_router(audit_router)
app.include_router(budget_router)
app.include_router(broker_credentials_router)
app.include_router(config_router)
app.include_router(copy_trading_router)
app.include_router(coverage_router)
app.include_router(business_summary_admin_router)
# Debug router is dev/test-only — exposes operator-credentialled
# pass-throughs to eToro (`/_debug/etoro-candles-probe`,
# `/_debug/etoro-instrument-raw`) plus internal subscriber state
# (`/_debug/etoro-ws`). Allowlist (NOT denylist on `prod`) so future
# environments like `staging`/`qa`/`uat` are denied by default and
# never silently expose operator credentials. Add new envs here
# explicitly when they need diagnostic access. PR #610 review.
if settings.app_env in {"dev", "test", "local"}:
    app.include_router(debug_ws_router)
app.include_router(capability_overrides_admin_router)
app.include_router(filings_router)
app.include_router(instruments_router)
app.include_router(jobs_router)
app.include_router(operator_ingest_router)
app.include_router(news_router)
app.include_router(orders_router)
app.include_router(portfolio_router)
app.include_router(recommendations_router)
app.include_router(reports_router)
app.include_router(scores_router)
app.include_router(sse_quotes_router)
app.include_router(sync_router)
app.include_router(system_router)
app.include_router(theses_router)
app.include_router(instrument_thesis_router)
app.include_router(watchlist_router)


@app.get("/health")
def health(request: Request) -> JSONResponse:
    """Liveness + layer-state rollup.

    200 when every layer is HEALTHY / DEGRADED / RUNNING / RETRYING /
    CASCADE_WAITING / DISABLED (self-healing or operator-gated states).
    503 when ANY layer is ACTION_NEEDED or SECRET_MISSING — external
    monitoring should alert.
    Falls through to 503 with system_state="error" when the state
    machine itself cannot be evaluated (DB pool exhausted, DB down,
    etc.). Acquires the connection inline rather than via
    `Depends(get_conn)` so pool checkout failures map to the same
    503 JSON shape instead of FastAPI's default 500 HTML.

    Trading-mode flags intentionally NOT returned here.  They live in
    runtime_config (DB-backed) and are exposed via /config — surfacing the
    env-backed values would be misleading and stale (issue #56).
    """
    base: dict[str, object] = {
        "env": settings.app_env,
        "etoro_env": settings.etoro_env,
    }
    try:
        pool = getattr(request.app.state, "db_pool", None)
        if pool is None:
            # No pool on app.state — either the lifespan hasn't run
            # (test harness) or the app booted into a degraded mode.
            # Treat as "cannot evaluate" and fall through to the
            # 503 error branch so external monitoring sees it.
            raise RuntimeError("db_pool not initialised on app.state")
        with pool.connection() as conn:
            states = compute_layer_states_from_db(conn)
    except Exception:
        logger.exception("/health: compute_layer_states_from_db failed")
        return JSONResponse(
            {"status": "error", "system_state": "error", **base},
            status_code=503,
        )
    needs_attention = any(s in {LayerState.ACTION_NEEDED, LayerState.SECRET_MISSING} for s in states.values())
    return JSONResponse(
        {
            "status": "ok",
            "system_state": "needs_attention" if needs_attention else "ok",
            **base,
        },
        status_code=503 if needs_attention else 200,
    )


@app.get("/health/db")
def health_db(conn: psycopg.Connection[object] = Depends(get_conn)) -> dict:
    """Public liveness probe for the DB layer.

    Returns ONLY ``{"db_reachable": bool}`` to unauthenticated callers
    (#240). Earlier revisions echoed the full ``public`` schema's table
    list, the migration history, and raw ``str(exc)`` error text — all
    useful fingerprints for an attacker once the app is reachable
    off-loopback. Schema state and migration history have moved
    behind operator auth in the existing observability endpoints
    (``/sync/layers``, ``/sync/layers/v2``); the public probe is now a
    binary up/down signal only.

    Any exception during the probe is logged at warning level
    (framework + ``logger.warning`` so the operator can still
    investigate) but is NEVER echoed in the response body.
    """
    try:
        # ``migration_status`` itself runs a SELECT against
        # schema_migrations — sufficient to verify the pool is up
        # and the bootstrap table exists, which is all a liveness
        # probe needs to assert.
        migration_status(conn)
        return {"db_reachable": True}
    except Exception:
        logger.warning("/health/db: probe failed", exc_info=True)
        return {"db_reachable": False}


# ---------------------------------------------------------------------------
# Coverage tier override
# ---------------------------------------------------------------------------


class TierOverrideRequest(BaseModel):
    instrument_id: int
    new_tier: int = Field(ge=1, le=3)
    rationale: str = Field(min_length=1)


@app.post("/coverage/override", dependencies=[Depends(require_session_or_service_token)])
def coverage_override(
    body: TierOverrideRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> dict:
    """Manually override an instrument's coverage tier."""
    try:
        change = override_tier(
            conn=conn,
            instrument_id=body.instrument_id,
            new_tier=body.new_tier,
            rationale=body.rationale,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "instrument_id": change.instrument_id,
        "old_tier": change.old_tier,
        "new_tier": change.new_tier,
        "change_type": change.change_type,
        "rationale": change.rationale,
    }


# ---------------------------------------------------------------------------
# Kill switch — deprecated alias
# ---------------------------------------------------------------------------
#
# The canonical route is POST /config/kill-switch (issue #56).  This alias is
# kept temporarily so any existing operator scripts do not break in this
# release.  It delegates to the same handler, so behaviour and audit writes
# are identical.  Remove once the settings UI (#65) lands.


@app.post(
    "/kill-switch",
    dependencies=[Depends(require_session_or_service_token)],
    deprecated=True,
    tags=["config"],
)
def set_kill_switch_deprecated(
    body: KillSwitchRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> KillSwitchResponse:
    """Deprecated alias for POST /config/kill-switch."""
    return post_kill_switch(body, conn)
