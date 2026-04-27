"""TEMPORARY debug endpoint for eToro WS subscriber state (#602 live ticks).

Surfaces the bits we need to diagnose "SSE connected but no ticks
arriving":
  * Is the WS connection currently open?
  * Which instrument topics has add_instruments() registered refs for?
  * Is the subscriber task running, or has it died?

Remove this file once the live-tick path is confirmed healthy on
this environment.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/_debug", tags=["debug"])


class EtoroWsStatus(BaseModel):
    subscriber_present: bool
    ws_connected: bool
    topic_refs: dict[int, int]
    task_done: bool | None
    last_quote_max: str | None


@router.get("/etoro-candles-probe")
def etoro_candles_probe(
    request: Request,
    instrument_id: int = 1699,
    count: int = 90,
    interval: str = "OneMinute",
) -> dict:
    """Probe eToro intraday candles directly. Returns last N bars at
    the requested interval + timestamps so we can see actual session
    coverage and gap structure.
    """
    import uuid

    import httpx

    from app.config import settings
    from app.services.broker_credentials import load_credential_for_provider_use
    from app.services.operators import sole_operator_id

    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        return {"error": "no pool"}
    with pool.connection() as conn:
        op_id = sole_operator_id(conn)
        api = load_credential_for_provider_use(
            conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment=settings.etoro_env,
            caller="diag",
        )
        conn.commit()
        user = load_credential_for_provider_use(
            conn,
            operator_id=op_id,
            provider="etoro",
            label="user_key",
            environment=settings.etoro_env,
            caller="diag",
        )
        conn.commit()

    url = (
        f"{settings.etoro_base_url}/api/v1/market-data/instruments/"
        f"{instrument_id}/history/candles/asc/{interval}/{count}"
    )
    headers = {"x-api-key": api, "x-user-key": user, "x-request-id": str(uuid.uuid4())}
    with httpx.Client(timeout=15.0) as c:
        r = c.get(url, headers=headers)
        if r.status_code != 200:
            return {"status": r.status_code, "body": r.text[:500]}
        body = r.json()
        outer = body.get("candles", [])
        if not outer:
            return {"empty": True}
        inner = outer[0].get("candles", [])
        # Compact timestamp + close-only ladder so the response is
        # scannable for gap analysis without 30+ KB of JSON.
        ladder = [{"t": b.get("fromDate"), "c": b.get("close")} for b in inner]
        return {
            "count": len(inner),
            "first": inner[0] if inner else None,
            "last": inner[-1] if inner else None,
            "ladder": ladder,
        }


@router.get("/etoro-instrument-raw")
def etoro_instrument_raw(request: Request, instrument_id: int = 1699) -> dict:
    """Probe eToro instruments-list raw response — return the full raw
    dict for one instrument so we can see every field (including
    trading-hours / extended-hours classification fields the
    `_normalise_instrument` mapper drops)."""
    import uuid

    import httpx

    from app.config import settings
    from app.services.broker_credentials import load_credential_for_provider_use
    from app.services.operators import sole_operator_id

    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        return {"error": "no pool"}
    with pool.connection() as conn:
        op_id = sole_operator_id(conn)
        api = load_credential_for_provider_use(
            conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment=settings.etoro_env,
            caller="diag",
        )
        conn.commit()
        user = load_credential_for_provider_use(
            conn,
            operator_id=op_id,
            provider="etoro",
            label="user_key",
            environment=settings.etoro_env,
            caller="diag",
        )
        conn.commit()

    headers = {"x-api-key": api, "x-user-key": user, "x-request-id": str(uuid.uuid4())}
    with httpx.Client(timeout=20.0) as c:
        r = c.get(
            f"{settings.etoro_base_url}/api/v1/market-data/instruments",
            headers=headers,
        )
        if r.status_code != 200:
            return {"status": r.status_code, "body": r.text[:500]}
        body = r.json()
        items = body if isinstance(body, list) else body.get("instruments", [])
        for item in items:
            if isinstance(item, dict) and item.get("instrumentID") == instrument_id:
                return {"found": True, "raw": item, "all_keys": sorted(item.keys())}
        return {"found": False, "sample_keys": sorted(items[0].keys()) if items else []}


@router.get("/etoro-ws", response_model=EtoroWsStatus)
def etoro_ws_status(request: Request) -> EtoroWsStatus:
    sub = getattr(request.app.state, "etoro_ws", None)
    if sub is None:
        return EtoroWsStatus(
            subscriber_present=False,
            ws_connected=False,
            topic_refs={},
            task_done=None,
            last_quote_max=None,
        )
    ws = getattr(sub, "_ws", None)
    refs: dict[int, int] = dict(getattr(sub, "_topic_refs", {}))
    task = getattr(sub, "_task", None)
    task_done = task.done() if task is not None else None

    last_quote_max: str | None = None
    pool = getattr(request.app.state, "db_pool", None)
    if pool is not None:
        with pool.connection() as conn:
            row = conn.execute("SELECT MAX(quoted_at) FROM quotes").fetchone()
            if row is not None and row[0] is not None:
                last_quote_max = row[0].isoformat()

    return EtoroWsStatus(
        subscriber_present=True,
        ws_connected=ws is not None,
        topic_refs=refs,
        task_done=task_done,
        last_quote_max=last_quote_max,
    )
