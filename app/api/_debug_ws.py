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
