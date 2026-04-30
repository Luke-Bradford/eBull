"""Listener supervision (#719).

The supervisor watches ``ListenerState.last_progress_at`` and restarts
the listener thread when the idle threshold is exceeded. The full
supervisor loop relies on real threads + clock; we exercise it under
a tight tick + fast-stall configuration so the assertion runs in
under a second of wall-clock.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

from app.jobs import supervisor as supervisor_mod
from app.jobs.listener import ListenerState
from app.jobs.supervisor import supervise


def test_supervise_restarts_stalled_listener() -> None:
    """A listener whose ``last_progress_at`` is older than the stall
    threshold must be replaced with a fresh thread.
    """
    state = ListenerState()
    listener_stop = threading.Event()
    main_stop = threading.Event()

    started_threads: list[threading.Thread] = []

    def _factory() -> threading.Thread:
        def _quiet_thread() -> None:
            # Listener thread blocks on listener_stop. Supervisor
            # signals listener_stop when it decides to restart, so
            # this thread exits cleanly each time.
            listener_stop.wait(timeout=5.0)

        t = threading.Thread(target=_quiet_thread, name="test-listener", daemon=True)
        started_threads.append(t)
        return t

    # Force the threshold below the test's stall window so the
    # supervisor restarts on the first tick.
    with patch.object(supervisor_mod, "LISTENER_STALL_THRESHOLD_S", 0.1):
        # Pre-stall: rewind last_progress_at so the very first
        # supervision pass sees it as stale.
        state.last_progress_at = time.monotonic() - 1.0

        # Run the supervisor in its own thread so we can stop it
        # from the test body.
        sup_thread = threading.Thread(
            target=supervise,
            kwargs={
                "listener_state": state,
                "listener_stop": listener_stop,
                "listener_thread_factory": _factory,
                "main_stop": main_stop,
                "tick_seconds": 0.05,
            },
            name="test-supervisor",
            daemon=True,
        )
        sup_thread.start()
        # Give the supervisor enough wall-clock to detect the stall
        # and restart the listener at least once.
        time.sleep(0.5)
        main_stop.set()
        sup_thread.join(timeout=2.0)

    # At least two listener threads should have been created — one
    # initial, one post-restart.
    assert len(started_threads) >= 2
    assert state.restart_count >= 1
