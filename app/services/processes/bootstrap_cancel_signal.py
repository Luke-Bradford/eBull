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
        bootstrap_cancel_requested,
    )
    from app.services.bootstrap_state import BootstrapStageCancelled

    for n, item in enumerate(big_iterable):
        if n % 50 == 0 and bootstrap_cancel_requested():
            raise BootstrapStageCancelled(stage_key="sec_first_install_drain")
        ...

The bootstrap orchestrator's ``_run_one_stage`` sets the run id on
this module's ContextVar around the invoker call; outside of a
bootstrap dispatch the helper returns False (the contextvar is unset)
so scheduled / manual triggers of the same job are unaffected.

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
from typing import Any

import psycopg

from app.config import settings
from app.services.process_stop import is_stop_requested

logger = logging.getLogger(__name__)


# When set, identifies the in-flight bootstrap run that the calling
# stage invoker is part of. Outside of bootstrap dispatch the value is
# ``None`` and ``bootstrap_cancel_requested`` short-circuits to False.
_active_bootstrap_run_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_active_bootstrap_run_id", default=None
)


@contextlib.contextmanager
def active_bootstrap_run(run_id: int) -> Iterator[None]:
    """Context manager that exposes ``run_id`` to stage invokers.

    The bootstrap orchestrator's ``_run_one_stage`` wraps the invoker
    call in ``with active_bootstrap_run(run_id): invoker(...)`` so any
    long-running loop inside the invoker can poll
    ``bootstrap_cancel_requested()`` to check whether the operator
    has cancelled the run.
    """
    token = _active_bootstrap_run_id.set(run_id)
    try:
        yield
    finally:
        _active_bootstrap_run_id.reset(token)


def bootstrap_cancel_requested(
    *,
    conn: psycopg.Connection[Any] | None = None,
) -> bool:
    """Return True iff the active bootstrap run has been cancelled.

    Reads the ``_active_bootstrap_run_id`` contextvar; if unset (the
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
    run_id = _active_bootstrap_run_id.get()
    if run_id is None:
        return False

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
    "bootstrap_cancel_requested",
]
