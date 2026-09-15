"""#2274 — the sync-run heartbeat against a real ``sync_runs``, with a real pool.

⚠⚠ THE REGISTERED POOL IS THE TEST, NOT SCAFFOLDING. ``background_write_connection``'s
pooled branch rolls back any non-IDLE transaction before returning the connection,
while its raw fallback commits on clean exit — which is exactly how
``checkpoint_progress`` shipped broken (fixed in ``3f3c3517``). A raw-connection
test passes against that bug.

Separate module from the pure-logic tests because ``tests/conftest.py`` applies
the ``db`` marker per MODULE.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

import psycopg
import pytest

from app.db.background_write import set_background_pool
from app.services.sync_orchestrator import executor
from app.services.sync_orchestrator.types import LayerOutcome, RefreshResult
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.skipif(not test_db_available(), reason="ebull_test Postgres not reachable")

# ``sync_runs.scope`` carries a CHECK constraint, so the fixture cannot mint its
# own sentinel value. ``behind`` is the honest choice: it is the scope of the
# boot sweep, which is 148 of the 157 stranded runs this heartbeat is for.
# ⚠ Cleanup is therefore keyed on the rows this module INSERTED, never on the
# scope — a scope-wide DELETE would eat a sibling test's fixture rows.
_FIXTURE_SCOPE = "behind"
_FIXTURE_TRIGGER = "boot_sweep"
_FIXTURE_LAYER = "candles"


@contextmanager
def _registered_pool(url: str) -> Iterator[None]:
    """A minimal real pool so the seam takes its POOLED branch."""

    class _Pool:
        def __init__(self) -> None:
            self._conn = psycopg.connect(url)

        @contextmanager
        def connection(self) -> Iterator[psycopg.Connection[Any]]:
            yield self._conn

        def close(self) -> None:
            self._conn.close()

    pool = _Pool()
    # cast: the seam only ever calls ``.connection()``.
    set_background_pool(cast(Any, pool))
    try:
        yield
    finally:
        set_background_pool(None)
        pool.close()


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[psycopg.Connection[Any]]:
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())
    conn: psycopg.Connection[Any] = psycopg.connect(test_database_url(), autocommit=True)
    set_background_pool(None)
    try:
        yield conn
    finally:
        set_background_pool(None)
        # The in-process throttle is keyed on sync_run_id and each test mints a
        # fresh id, but clear it anyway so a re-used id across a session cannot
        # silently suppress a write a test is asserting on.
        executor._run_heartbeat_last.clear()
        try:
            if _inserted:
                conn.execute("DELETE FROM sync_layer_progress WHERE sync_run_id = ANY(%s)", (_inserted,))
                conn.execute("DELETE FROM sync_runs WHERE sync_run_id = ANY(%s)", (_inserted,))
                _inserted.clear()
        finally:
            conn.close()


# Run ids this module inserted, so teardown deletes exactly those.
_inserted: list[int] = []


def _insert_run(conn: psycopg.Connection[Any], *, status: str = "running") -> int:
    """Insert a fixture run.

    ⚠ ``status='running'`` collides with ``idx_sync_runs_single_running`` — the
    UNIQUE partial index on ``((true)) WHERE status='running'`` that is the whole
    subject of this ticket. Each test therefore inserts at most one running row
    and teardown removes it; a test that needs two must terminalise the first.
    """
    row = conn.execute(
        """
        INSERT INTO sync_runs (scope, trigger, status, layers_planned)
        VALUES (%(scope)s, %(trigger)s, %(status)s, 1)
        RETURNING sync_run_id
        """,
        {"scope": _FIXTURE_SCOPE, "trigger": _FIXTURE_TRIGGER, "status": status},
    ).fetchone()
    assert row is not None
    run_id = int(row[0])
    _inserted.append(run_id)
    conn.execute(
        """
        INSERT INTO sync_layer_progress (sync_run_id, layer_name, status)
        VALUES (%s, %s, 'pending')
        """,
        (run_id, _FIXTURE_LAYER),
    )
    return run_id


def _success_result() -> RefreshResult:
    return RefreshResult(
        outcome=LayerOutcome.SUCCESS,
        row_count=1,
        items_processed=1,
        items_total=1,
        detail="fixture",
    )


def _heartbeat(conn: psycopg.Connection[Any], run_id: int) -> datetime | None:
    row = conn.execute(
        "SELECT last_progress_at FROM sync_runs WHERE sync_run_id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None
    return row[0]


def test_heartbeat_is_null_before_any_hook(reader: psycopg.Connection[Any]) -> None:
    """The state the whole ticket is about: a fresh run is opaque.

    This is the assertion that would have failed the day ``last_progress_at``
    was added with no producer.
    """
    run_id = _insert_run(reader)
    assert _heartbeat(reader, run_id) is None


@pytest.mark.parametrize(
    "hook",
    [
        pytest.param(lambda run_id: executor._record_layer_started(run_id, _FIXTURE_LAYER), id="layer_started"),
        # ⚠ The SUCCESS path, and it was missing from the first draft of this
        # list (Codex checkpoint 3). It is the hook that fires on every ordinary
        # layer, so omitting it left the widest path unpinned while the file
        # claimed per-hook coverage.
        pytest.param(
            lambda run_id: executor._record_layer_result(run_id, _FIXTURE_LAYER, _success_result()),
            id="layer_result",
        ),
        pytest.param(
            lambda run_id: executor._record_layer_skipped(run_id, _FIXTURE_LAYER, "prereq missing"),
            id="layer_skipped",
        ),
        pytest.param(
            lambda run_id: executor._record_layer_failed(run_id, _FIXTURE_LAYER, RuntimeError("boom")),
            id="layer_failed",
        ),
        pytest.param(lambda run_id: executor._fail_unfinished_layers(run_id), id="fail_unfinished"),
    ],
)
def test_each_layer_hook_advances_the_run_heartbeat(
    reader: psycopg.Connection[Any],
    hook: Any,
) -> None:
    """Per-hook, not "some hook".

    A single "the timestamp is non-null after a sync" assertion passes when only
    one of the five hooks is wired, and the two most likely to be missed are the
    one that only fires on a crash and — as this file itself demonstrated — the
    ordinary success path.
    """
    run_id = _insert_run(reader)
    with _registered_pool(test_database_url()):
        hook(run_id)
    assert _heartbeat(reader, run_id) is not None


def test_progress_callback_advances_the_run_heartbeat(reader: psycopg.Connection[Any]) -> None:
    run_id = _insert_run(reader)
    callback = executor._make_progress_callback(run_id, (_FIXTURE_LAYER,))
    with _registered_pool(test_database_url()):
        callback(7, 100)
    assert _heartbeat(reader, run_id) is not None
    items = reader.execute(
        "SELECT items_done, items_total FROM sync_layer_progress WHERE sync_run_id = %s",
        (run_id,),
    ).fetchone()
    assert items == (7, 100)


def test_terminal_run_heartbeat_is_not_resurrected(reader: psycopg.Connection[Any]) -> None:
    """A late callback must not make a finished run look in-flight.

    The executor's finalize and a straggling adapter thread genuinely race — the
    guard is ``AND status = 'running'``, and without it a completed run would
    carry a heartbeat newer than its own ``finished_at``.
    """
    run_id = _insert_run(reader, status="complete")
    with _registered_pool(test_database_url()):
        executor._touch_run_heartbeat(run_id)
    assert _heartbeat(reader, run_id) is None


def test_heartbeat_never_moves_backwards(reader: psycopg.Connection[Any]) -> None:
    """Monotonic guard.

    ``now()`` is transaction-START time, so two overlapping heartbeat
    transactions can carry different instants and the later writer can hold the
    earlier stamp. A heartbeat that goes backwards would read as a stall.
    """
    run_id = _insert_run(reader)
    future = reader.execute(
        """
        UPDATE sync_runs SET last_progress_at = now() + interval '1 hour'
         WHERE sync_run_id = %s RETURNING last_progress_at
        """,
        (run_id,),
    ).fetchone()
    assert future is not None
    with _registered_pool(test_database_url()):
        executor._touch_run_heartbeat(run_id)
    assert _heartbeat(reader, run_id) == future[0]


def test_heartbeat_failure_does_not_change_the_layer_result(
    reader: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The isolation property, asserted rather than argued.

    If the heartbeat shared ``_record_layer_result``'s transaction, a heartbeat
    failure would roll the layer row back and committed work would be recorded
    as failed. Exercised on ``_record_layer_result`` specifically — that is the
    writer carrying the authoritative outcome, so it is the one where a rollback
    costs real information (Codex checkpoint 3).
    """
    run_id = _insert_run(reader)

    def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("heartbeat writer is broken")

    monkeypatch.setattr(executor, "_touch_run_heartbeat", _boom)
    with _registered_pool(test_database_url()):
        with pytest.raises(RuntimeError):
            executor._record_layer_result(run_id, _FIXTURE_LAYER, _success_result())

    status = reader.execute(
        "SELECT status, row_count FROM sync_layer_progress WHERE sync_run_id = %s",
        (run_id,),
    ).fetchone()
    assert status == ("complete", 1)


def test_touch_never_raises_on_a_broken_connection(
    reader: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The never-raises contract, at the seam a caller cannot control.

    ``_record_layer_result`` calls the helper AFTER its own commit, so a raising
    helper would turn a successful layer into an exception the executor's crash
    handler catches — the layer would be recorded, then the run failed.
    """
    run_id = _insert_run(reader)

    @contextmanager
    def _broken() -> Iterator[Any]:
        raise psycopg.OperationalError("pool is gone")
        yield  # pragma: no cover - unreachable, keeps the generator shape

    monkeypatch.setattr(executor, "background_write_connection", _broken)
    executor._touch_run_heartbeat(run_id)  # must not raise
    assert _heartbeat(reader, run_id) is None


def test_throttle_suppresses_a_second_unforced_write(reader: psycopg.Connection[Any]) -> None:
    """``force=False`` is the tick path, and a tick fires every 5 ITEMS.

    A fast loop would otherwise open one transaction per item. The layer-boundary
    callers leave ``force=True`` because there are at most a handful per run and
    each is a real lifecycle event.
    """
    run_id = _insert_run(reader)
    with _registered_pool(test_database_url()):
        executor._touch_run_heartbeat(run_id, force=False)
        first = _heartbeat(reader, run_id)
        assert first is not None
        executor._touch_run_heartbeat(run_id, force=False)
    assert _heartbeat(reader, run_id) == first


def test_forced_write_bypasses_the_throttle(reader: psycopg.Connection[Any]) -> None:
    run_id = _insert_run(reader)
    with _registered_pool(test_database_url()):
        executor._touch_run_heartbeat(run_id, force=False)
        reader.execute(
            "UPDATE sync_runs SET last_progress_at = now() - interval '1 minute' WHERE sync_run_id = %s",
            (run_id,),
        )
        before = _heartbeat(reader, run_id)
        executor._touch_run_heartbeat(run_id, force=True)
    after = _heartbeat(reader, run_id)
    assert before is not None and after is not None and after > before


def test_liveness_reads_from_started_at_when_the_heartbeat_is_absent(
    reader: psycopg.Connection[Any],
) -> None:
    """The intended degradation for the intervals no hook covers.

    Between ``_insert_sync_run`` committing and the first layer starting — the
    prelude, credential and cascade checks — there is no heartbeat, and an empty
    plan never reaches a hook at all. The verdict falls back to run age, which is
    the honest reading.
    """
    from app.services.sync_orchestrator.run_liveness import RUNTIME_CEILING_S, assess_run

    run_id = _insert_run(reader)
    reader.execute(
        "UPDATE sync_runs SET started_at = now() - make_interval(secs => %s) WHERE sync_run_id = %s",
        (RUNTIME_CEILING_S + 60, run_id),
    )
    row = reader.execute(
        "SELECT started_at, last_progress_at FROM sync_runs WHERE sync_run_id = %s",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert row[1] is None
    assert assess_run(started_at=row[0], last_progress_at=row[1], now=datetime.now(UTC)) == "over_ceiling"
