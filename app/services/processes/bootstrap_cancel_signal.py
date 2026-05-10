"""Bootstrap cancel-signal plumbing for long-running stage invokers.

Issue #1064 PR3d (`#1093` schema is the prerequisite). Operator clicks
Cancel on the bootstrap row → ``cancel_run`` writes
``bootstrap_runs.cancel_requested_at`` and the
``process_stop_requests`` row. The orchestrator's main dispatcher
loop checks ``is_stop_requested`` between stage batches but the
checkpoint cadence is "between stages, not within". A 20-minute SEC
drain or 30-minute 13F sweep observes the cancel only at completion.

This module exposes the signal to long-running stages so they can
poll periodically and bail out cooperatively. The pattern:

    from app.services.processes.bootstrap_cancel_signal import (
        active_bootstrap_stage_key,
        bootstrap_cancel_requested,
    )
    from app.services.bootstrap_state import BootstrapStageCancelled

    for n, item in enumerate(big_iterable):
        if n % 50 == 0 and bootstrap_cancel_requested():
            raise BootstrapStageCancelled(
                stage_key=active_bootstrap_stage_key() or "",
            )
        ...

The bootstrap orchestrator's ``_run_one_stage`` sets the run id and
the dispatching stage_key on this module's ContextVar around the
invoker call; outside of a bootstrap dispatch the helpers return
``False`` / ``None`` so scheduled / manual triggers of the same job
are unaffected.

Issue #1114: the contextvar carries both ``run_id`` AND ``stage_key``
so adopters never hardcode the stage_key. If a future bootstrap
stage invokes a helper that previously hardcoded its stage_key, the
exception's ``stage_key`` attribute would otherwise misattribute the
cancel to the wrong stage in the audit log.

Polling cost: one SQL probe per call. Stage invokers should batch the
polling (e.g. every 50 iterations) rather than calling on every loop
body — the cancel-observation latency target is "seconds, not 20+
minutes", not "instant".
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import psycopg

from app.config import settings
from app.services.process_stop import is_stop_requested

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _BootstrapContext:
    """Identifies the in-flight bootstrap run + dispatching stage.

    Set by the orchestrator's ``_run_one_stage`` boundary so any
    long-running invoker called from that stage can read both the
    run_id (to probe for a cancel signal) and the stage_key (to
    label the ``BootstrapStageCancelled`` exception with the
    dispatching stage's name).
    """

    run_id: int
    stage_key: str


# When set, identifies the in-flight bootstrap run + dispatching
# stage_key. Outside of bootstrap dispatch the value is ``None`` and
# ``bootstrap_cancel_requested`` short-circuits to False.
_active_bootstrap_context: contextvars.ContextVar[_BootstrapContext | None] = contextvars.ContextVar(
    "_active_bootstrap_context", default=None
)


@contextlib.contextmanager
def active_bootstrap_run(run_id: int, stage_key: str) -> Iterator[None]:
    """Context manager that exposes ``(run_id, stage_key)`` to stage invokers.

    The bootstrap orchestrator's ``_run_one_stage`` wraps the invoker
    call in ``with active_bootstrap_run(run_id, stage_key): invoker(...)``
    so any long-running loop inside the invoker can poll
    ``bootstrap_cancel_requested()`` to check whether the operator has
    cancelled the run AND read ``active_bootstrap_stage_key()`` to
    label the cancel exception with the dispatching stage.

    Issue #1114: stage_key is now required (was implicit in caller
    hardcoding). The orchestrator always knows its own stage_key at
    the dispatch boundary; helpers should never hardcode it because
    the same helper may be invoked from multiple stages in future.
    """
    token = _active_bootstrap_context.set(_BootstrapContext(run_id=run_id, stage_key=stage_key))
    try:
        yield
    finally:
        _active_bootstrap_context.reset(token)


def active_bootstrap_stage_key() -> str | None:
    """Return the stage_key of the in-flight bootstrap stage, or None.

    Issue #1114. Long-running helpers that raise
    ``BootstrapStageCancelled`` read this to label the exception
    with the dispatching stage's name instead of hardcoding their
    own. Outside ``active_bootstrap_run`` the contextvar is unset
    and this returns ``None``; callers should treat ``None`` as
    "stage_key unknown" and pass an empty string to the exception
    so audit log readers see a clear sentinel rather than a wrong
    value.
    """
    ctx = _active_bootstrap_context.get()
    if ctx is None:
        return None
    return ctx.stage_key


def bootstrap_cancel_requested(
    *,
    conn: psycopg.Connection[Any] | None = None,
) -> bool:
    """Return True iff the active bootstrap run has been cancelled.

    Reads the ``_active_bootstrap_context`` contextvar; if unset (the
    invoker is being called from a scheduled trigger, manual API
    POST, or test fixture rather than the bootstrap dispatcher),
    returns False without touching the DB. When set, opens a
    short-lived autocommit connection (or uses ``conn`` when
    supplied) and probes ``process_stop_requests`` for an unobserved
    stop row targeting this run.

    Polling cost: one SQL probe. Callers should batch (every 50
    iterations of a CIK-loop, every 100 archive entries, etc.) so the
    DB doesn't bear the cost of a probe per row.

    Errors are logged and treated as "not cancelled" — a transient
    DB hiccup must not stall a stage's loop. Worst case: cancel
    observation falls back to the orchestrator's between-stage
    checkpoint at the next stage boundary.
    """
    ctx = _active_bootstrap_context.get()
    if ctx is None:
        return False
    run_id = ctx.run_id

    try:
        if conn is not None:
            stop = is_stop_requested(
                conn,
                target_run_kind="bootstrap_run",
                target_run_id=run_id,
            )
        else:
            with psycopg.connect(settings.database_url, autocommit=True) as probe_conn:
                stop = is_stop_requested(
                    probe_conn,
                    target_run_kind="bootstrap_run",
                    target_run_id=run_id,
                )
    except Exception as exc:
        # Defensive: fall back to "not cancelled" on transient DB
        # error so the stage doesn't stall on a probe failure. The
        # orchestrator's between-stage checkpoint still observes the
        # cancel at the next boundary.
        logger.warning(
            "bootstrap_cancel_requested: probe failed for run_id=%d (%s); treating as not cancelled",
            run_id,
            exc,
        )
        return False

    return stop is not None


__all__ = [
    "active_bootstrap_run",
    "active_bootstrap_stage_key",
    "bootstrap_cancel_requested",
]
