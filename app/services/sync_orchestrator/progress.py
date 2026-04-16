"""Progress reporting for long-running layer refreshes (spec §2.7).

Legacy job functions (``daily_candle_refresh``, ``daily_financial_facts``,
``daily_thesis_refresh``) have the signature ``() -> None`` and are also
invoked directly by APScheduler. To avoid forking their signatures or
plumbing an extra argument through multiple service-layer calls, the
orchestrator stashes the active ``ProgressCallback`` in a ``ContextVar``
before calling ``legacy_fn()`` and exposes ``report_progress`` as a
no-op-when-unset helper the loops call at item granularity.

Throttling (spec: every N items **or** every 10 seconds, whichever
comes first) is implemented here so loop bodies stay clean. When no
progress callback is active — e.g. APScheduler calling the legacy
function directly, or unit tests of the service-layer function —
``report_progress`` returns without doing any work.

The ContextVar is set/cleared by the orchestrator adapter; services
layer only imports ``report_progress``.
"""

from __future__ import annotations

import time
from contextvars import ContextVar, Token
from dataclasses import dataclass

from app.services.sync_orchestrator.types import ProgressCallback

_DEFAULT_TICK_EVERY_ITEMS = 5
_DEFAULT_TICK_EVERY_SECONDS = 10.0


@dataclass
class _ProgressState:
    callback: ProgressCallback
    last_tick_items: int
    last_tick_time: float


_active: ContextVar[_ProgressState | None] = ContextVar("sync_orchestrator_progress", default=None)


def set_active_progress(callback: ProgressCallback) -> Token[_ProgressState | None]:
    """Install ``callback`` as the active progress reporter for the
    current context. Returns a token the caller passes to
    ``clear_active_progress`` to restore the previous state.

    Fires an immediate ``(0, None)`` tick so the UI moves from 'pending'
    to 'running' on the first poll after the layer begins — otherwise
    the first visible tick waits for the loop to reach the throttle
    threshold, which may be tens of seconds on slow providers.
    """
    state = _ProgressState(
        callback=callback,
        last_tick_items=0,
        last_tick_time=time.monotonic(),
    )
    token = _active.set(state)
    try:
        callback(0, None)
    except Exception:
        pass
    return token


def clear_active_progress(token: Token[_ProgressState | None]) -> None:
    """Restore the previous progress state. Always called from the
    adapter's ``finally`` to guarantee the context is clean for the
    next layer in the same sync run."""
    _active.reset(token)


def report_progress(
    items_done: int,
    items_total: int | None,
    *,
    tick_every_items: int = _DEFAULT_TICK_EVERY_ITEMS,
    tick_every_seconds: float = _DEFAULT_TICK_EVERY_SECONDS,
    force: bool = False,
) -> None:
    """Tick the active progress callback.

    No-op when no callback is installed (the common case outside an
    orchestrator-driven sync). Throttled so fast loops do not hammer
    the database: emits only when ``tick_every_items`` items have
    elapsed since the last tick OR ``tick_every_seconds`` seconds
    have elapsed, whichever comes first. The final-state tick (loop
    finished) should pass ``force=True`` so the last ``items_done``
    is always persisted even if the delta is below threshold.

    Callback exceptions are caught and ignored — progress reporting
    must never abort the underlying work.
    """
    state = _active.get()
    if state is None:
        return

    now = time.monotonic()
    items_since = items_done - state.last_tick_items
    seconds_since = now - state.last_tick_time

    if not force and items_since < tick_every_items and seconds_since < tick_every_seconds:
        return

    try:
        state.callback(items_done, items_total)
    except Exception:
        return

    state.last_tick_items = items_done
    state.last_tick_time = now
