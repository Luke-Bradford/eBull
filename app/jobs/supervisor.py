"""Listener / heartbeat / scheduler subsystem supervision (#719).

Watches ``ListenerState.last_progress_at`` for a stalled listener
thread. If the watchdog fires, it signals the listener to stop and
restarts it on a fresh thread + fresh psycopg.Connection. The
supervisor itself runs on the entrypoint's main thread and feeds
the ``main`` heartbeat row so its own liveness is observable too.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from app.jobs.listener import ListenerState

logger = logging.getLogger(__name__)


# Stall detection threshold. The listener loops at ~NOTIFY_BLOCK_TIMEOUT_S
# (1s) cadence; 60s without progress means the LISTEN socket is dead or
# the worker thread is wedged. Restart resolves both.
LISTENER_STALL_THRESHOLD_S: float = 60.0


def supervise(
    *,
    listener_state: ListenerState,
    listener_stop: threading.Event,
    listener_thread_factory: Callable[[], threading.Thread],
    main_stop: threading.Event,
    on_main_tick: Callable[[], None] | None = None,
    tick_seconds: float = 5.0,
) -> threading.Thread | None:
    """Watch the listener for stalls until ``main_stop`` is set.

    Returns the active listener thread when ``main_stop`` fires so the
    entrypoint can join it on shutdown. ``on_main_tick`` is called
    every supervision pass — the entrypoint uses it to emit the
    ``main`` heartbeat row alongside the per-subsystem rows the loops
    themselves emit.

    The listener thread runs its own loop until either
    ``listener_stop`` is set (clean restart) or ``main_stop`` is set
    (process shutdown). The supervisor flips ``listener_stop`` on
    restart, joins the old thread, then starts a new one from
    ``listener_thread_factory``.
    """
    active_thread = listener_thread_factory()
    active_thread.start()

    while not main_stop.is_set():
        if on_main_tick is not None:
            try:
                on_main_tick()
            except Exception:
                logger.warning("supervisor: on_main_tick raised", exc_info=True)

        idle = time.monotonic() - listener_state.last_progress_at
        if idle > LISTENER_STALL_THRESHOLD_S:
            listener_state.restart_count += 1
            logger.warning(
                "listener stalled for %.1fs (>%.1fs threshold); restarting (count=%d)",
                idle,
                LISTENER_STALL_THRESHOLD_S,
                listener_state.restart_count,
            )
            listener_stop.set()
            active_thread.join(timeout=10.0)
            if active_thread.is_alive():
                logger.error("listener thread did not stop within 10s — abandoning daemon and continuing")
            listener_stop.clear()
            listener_state.last_progress_at = time.monotonic()
            active_thread = listener_thread_factory()
            active_thread.start()

        if main_stop.wait(timeout=tick_seconds):
            break

    listener_stop.set()
    active_thread.join(timeout=10.0)
    return active_thread if active_thread.is_alive() else None
