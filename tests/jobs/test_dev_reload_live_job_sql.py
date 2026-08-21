"""#2274 — `dev_reload.live_job`'s statement, run against a real `job_runs`.

Split out of `test_dev_reload.py` on purpose. `live_job` swallows every
exception by design (it must fail OPEN, so a DB blip reloads as before
rather than freezing the daemon on stale code). The cost of that contract
is that a typo in its SQL degrades the probe to a permanent `None`: the
deferral silently stops working and every pure-logic test still passes.
Executing the real statement is the only thing that catches it.

It lives in its own module because `tests/conftest.py::pytest_collection_modifyitems`
applies the `db` marker per MODULE — keeping this here leaves the pure
supervisor tests on the fast `-m "not db"` pre-push gate.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.jobs import dev_reload
from app.services.processes.stale_thresholds import (
    DEFAULT_THRESHOLD_S,
    get_threshold,
    overridden_process_ids,
)
from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test Postgres not reachable",
)

_FIXTURE_JOB = "dev_reload_probe_fixture"

# Real registry entries, so the per-job branch is exercised with the
# names the supervisor will actually see. `sec_insider_transactions_backfill`
# is a 1800s slow-tick producer; `strategy_backtest_run` takes the 300s
# default and is the job #2274 was opened over.
_SLOW_TICK_JOB = "sec_insider_transactions_backfill"
_DEFAULT_TICK_JOB = "strategy_backtest_run"

_CLEANUP = "DELETE FROM job_runs WHERE job_name = ANY(%s)"
_ALL_FIXTURE_JOBS = [_FIXTURE_JOB, _SLOW_TICK_JOB, _DEFAULT_TICK_JOB]

_SEED = """
    INSERT INTO job_runs (job_name, status, started_at, last_progress_at, processed_count, target_count)
    VALUES (%s, 'running', now(), now() - make_interval(secs => %s), 7, 9)
"""


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """A test-DB connection, with `settings.database_url` redirected.

    `live_job` opens its OWN connection from `settings.database_url`, so
    without the redirect it would probe the operator's dev DB — and the
    seeded row would have to be committed there to be visible.
    """
    from app.config import settings

    monkeypatch.setattr(settings, "database_url", _test_database_url())
    c: psycopg.Connection[Any] = psycopg.connect(_test_database_url(), autocommit=True)
    c.execute(_CLEANUP, (_ALL_FIXTURE_JOBS,))
    try:
        yield c
    finally:
        try:
            c.execute(_CLEANUP, (_ALL_FIXTURE_JOBS,))
        finally:
            c.close()


def test_a_fresh_heartbeat_blocks_a_reload(conn: Any) -> None:
    conn.execute(_SEED, (_FIXTURE_JOB, 0))
    described = dev_reload.live_job()
    assert described is not None, "a running job heartbeating now must block a reload"
    assert _FIXTURE_JOB in described
    assert "7/9 done" in described


def test_a_stale_heartbeat_stops_blocking_on_its_own(conn: Any) -> None:
    """Why this needs no watchdog: a wedged job ages out of protection.

    Deferral is unbounded in wall-clock but bounded by liveness, so a job
    that dies without a terminal status cannot hold the daemon on stale
    code forever.

    Aged past the WIDEST threshold in the registry, so this stays true
    whatever `stale_thresholds.py` says about this job name.
    """
    widest = max([DEFAULT_THRESHOLD_S, *(get_threshold(p) for p in overridden_process_ids())])
    conn.execute(_SEED, (_FIXTURE_JOB, widest + 60))
    assert dev_reload.live_job() is None


def test_a_slow_tick_job_is_live_past_the_default_threshold(conn: Any) -> None:
    """Codex checkpoint 2, round 2. Eight producers in `stale_thresholds.py`
    are given 1800s because their natural inter-tick gap exceeds five minutes.
    On the 300s default they would be 'healthy' to the admin console and
    'stale' to the supervisor at the same instant — still SIGKILL-able, with
    nothing reporting them at risk."""
    assert get_threshold(_SLOW_TICK_JOB) == 1800, "registry changed; revisit this test"
    conn.execute(_SEED, (_SLOW_TICK_JOB, 900))  # quiet 15 min, live under its 1800s cut
    described = dev_reload.live_job()
    assert described is not None
    assert _SLOW_TICK_JOB in described


def test_a_default_threshold_job_goes_stale_at_the_default(conn: Any) -> None:
    """The other half: honouring the overrides must not silently promote every
    job to the widest one, or a wedged run blocks reloads for half an hour."""
    assert get_threshold(_DEFAULT_TICK_JOB) == 300, "registry changed; revisit this test"
    conn.execute(_SEED, (_DEFAULT_TICK_JOB, 900))  # same 15 min, stale at its 300s cut
    assert dev_reload.live_job() is None


def test_a_stale_fast_tick_row_cannot_mask_a_live_slow_tick_one(conn: Any) -> None:
    """Codex checkpoint 2, round 3. The staleness cut must be applied BEFORE
    any row limit. Filtering in Python after `LIMIT` let a fresher-but-stale
    default-threshold row hide an older-but-live override row, returning
    'nothing to defer' and preempting the run this change exists to protect.

    The stale row is seeded with the FRESHER heartbeat, so a query that ordered
    and limited before applying the per-job cut would return it and miss the
    live one.
    """
    conn.execute(_SEED, (_DEFAULT_TICK_JOB, 400))  # fresher, but past its 300s cut
    conn.execute(_SEED, (_SLOW_TICK_JOB, 1200))  # older, but inside its 1800s cut
    described = dev_reload.live_job()
    assert described is not None, "a live slow-tick job was masked by a stale fast-tick one"
    assert _SLOW_TICK_JOB in described


def test_a_running_row_that_never_heartbeats_does_not_block(conn: Any) -> None:
    """8 of 111,409 `job_runs` rows carry `last_progress_at` at all, so the
    NULL case is the overwhelmingly common one and must behave as pre-#2274."""
    conn.execute(
        """
        INSERT INTO job_runs (job_name, status, started_at, last_progress_at)
        VALUES (%s, 'running', now(), NULL)
        """,
        (_FIXTURE_JOB,),
    )
    assert dev_reload.live_job() is None
