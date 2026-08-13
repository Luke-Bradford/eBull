"""Tests for the bootstrap orphan-stage reaper (#1233 PR-6).

The reaper resets ``bootstrap_stages`` rows that have been stuck in
``status='running'`` since a previous jobs-process crash so the
dispatcher can pick them up cleanly on restart. Reset criteria:

1. ``status = 'running'``.
2. ``started_at`` older than ``_REAPER_GRACE_SECONDS`` (5 min).
3. The corresponding JobLock advisory lock is NOT held in any session.

All three predicates must hold; failing any one leaves the row alone.

Test DB: each test starts from a TRUNCATEd ``ebull_test_conn`` so
synthetic rows don't bleed between cases. Rows that simulate "started
N minutes ago" use ``NOW() - INTERVAL '...'`` so the grace-window
comparison is exercised against the real wall clock.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.bootstrap_orchestrator import (
    _REAPER_GRACE_SECONDS,
    _hashtext_int,
    reap_orphaned_running_stages,
)
from app.services.bootstrap_state import StageSpec, start_run


def _reset_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status            = 'pending',
               last_run_id       = NULL,
               last_completed_at = NULL
         WHERE id = 1
        """
    )
    conn.commit()


def _register_synthetic_jobs(monkeypatch: pytest.MonkeyPatch, mapping: dict[str, str]) -> None:
    """Add ``job_name -> Lane`` entries to the source-lock registry.

    Same helper shape as ``tests/test_bootstrap_orchestrator.py``.
    """
    from app.jobs.sources import get_job_name_to_source

    registry = get_job_name_to_source()
    for name, lane in mapping.items():
        monkeypatch.setitem(registry, name, lane)  # type: ignore[arg-type]


def _seed_stage(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    status: str,
    started_minutes_ago: int | None,
    last_error: str | None = None,
) -> None:
    """Force a bootstrap_stages row to the given (status, started_at) shape.

    Bypasses the ``mark_stage_*`` helpers so we can set ``started_at``
    to a past time without sleeping.
    """
    conn.execute(
        """
        UPDATE bootstrap_stages
           SET status     = %(status)s,
               started_at = CASE
                              WHEN %(minutes)s::int IS NULL THEN NULL
                              ELSE NOW() - make_interval(mins => %(minutes)s::int)
                            END,
               last_error = %(last_error)s
         WHERE bootstrap_run_id = %(run_id)s
           AND stage_key        = %(stage_key)s
        """,
        {
            "run_id": run_id,
            "stage_key": stage_key,
            "status": status,
            "minutes": started_minutes_ago,
            "last_error": last_error,
        },
    )
    conn.commit()


def _status_of(conn: psycopg.Connection[tuple], *, run_id: int, stage_key: str) -> str:
    row = conn.execute(
        "SELECT status FROM bootstrap_stages WHERE bootstrap_run_id = %s AND stage_key = %s",
        (run_id, stage_key),
    ).fetchone()
    assert row is not None
    return str(row[0])


def _last_error_of(conn: psycopg.Connection[tuple], *, run_id: int, stage_key: str) -> str | None:
    row = conn.execute(
        "SELECT last_error FROM bootstrap_stages WHERE bootstrap_run_id = %s AND stage_key = %s",
        (run_id, stage_key),
    ).fetchone()
    assert row is not None
    return None if row[0] is None else str(row[0])


def _make_test_run(
    conn: psycopg.Connection[tuple],
    *,
    stages: list[tuple[str, str]],  # (stage_key, job_name) pairs; all on lane "init"
) -> int:
    """Materialise a minimal bootstrap run + stage rows for the reaper."""
    specs = tuple(
        StageSpec(
            stage_key=stage_key,
            stage_order=idx + 1,
            lane="init",
            job_name=job_name,
        )
        for idx, (stage_key, job_name) in enumerate(stages)
    )
    run_id = start_run(conn, operator_id=None, stage_specs=specs)
    conn.commit()
    return run_id


# ---------------------------------------------------------------------------
# _hashtext_int round-trip
# ---------------------------------------------------------------------------


def test_hashtext_int_matches_joblock_acquisition_key(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``_hashtext_int`` MUST byte-for-byte match the JobLock acquisition.

    JobLock acquires via ``pg_try_advisory_lock(hashtext(%s)::int)``;
    the reaper probes ``pg_locks`` using the same key. If the two
    derivations diverge the reaper's lock-held check returns false
    positives and resets stages whose workers are still alive.
    """
    for source in ("init", "etoro", "sec_rate", "db", "finra", "openfigi"):
        lock_text = f"job_source:{source}"
        python_key = _hashtext_int(ebull_test_conn, lock_text)

        row = ebull_test_conn.execute("SELECT hashtext(%s)::int", (lock_text,)).fetchone()
        assert row is not None
        assert python_key == int(row[0])


# ---------------------------------------------------------------------------
# Reaper happy-path cases
# ---------------------------------------------------------------------------


def test_reaper_resets_stuck_stage_when_lock_not_held(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stage running 10 min with no held lock should be reset to pending.

    No JobLock has been acquired in any session, so the ``pg_locks``
    probe returns no row → reaper proceeds.
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"stuck_job": "init"})

    run_id = _make_test_run(ebull_test_conn, stages=[("stuck_stage", "stuck_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="stuck_stage",
        status="running",
        started_minutes_ago=10,
    )

    reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert reset == 1
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="stuck_stage") == "pending"


def test_reaper_skips_stuck_stage_when_lock_held(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If a sibling session holds the lock, the stage might still be live.

    Acquire ``pg_try_advisory_lock`` on the same key the reaper would
    probe (``hashtext('job_source:init')::int``) from an autocommit
    session, then call the reaper. The pg_locks row exists in that
    other session → reaper leaves the stuck stage alone.
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"locked_job": "init"})

    run_id = _make_test_run(ebull_test_conn, stages=[("locked_stage", "locked_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="locked_stage",
        status="running",
        started_minutes_ago=10,
    )

    # Hold the lock from a SECOND connection so pg_locks shows it held
    # globally (not just on `ebull_test_conn`). Same DB URL as the
    # ebull_test fixture so we share the cluster's lock-table view.
    # ``test_database_url()`` returns the worker-private DB URL with
    # auth credentials intact; ``ebull_test_conn.info.dsn`` strips the
    # password and would fail with ``fe_sendauth: no password supplied``.
    from tests.fixtures.ebull_test_db import test_database_url

    holder_url = test_database_url()
    with psycopg.connect(holder_url, autocommit=True) as holder:
        held = holder.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
            ("job_source:init",),
        ).fetchone()
        assert held is not None and held[0] is True

        try:
            reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
            ebull_test_conn.commit()
        finally:
            holder.execute(
                "SELECT pg_advisory_unlock(hashtext(%s)::int)",
                ("job_source:init",),
            )

    assert reset == 0
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="locked_stage") == "running"


def test_reaper_leaves_recent_running_stage_alone(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stage started 2 min ago is INSIDE the 5-min grace window.

    The worker is most likely still alive in the worker startup
    window; the grace floor guards against premature reset.
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"recent_job": "init"})

    run_id = _make_test_run(ebull_test_conn, stages=[("recent_stage", "recent_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="recent_stage",
        status="running",
        started_minutes_ago=2,
    )

    # Even with NO lock held, the grace window means reaper must wait.
    reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert reset == 0
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="recent_stage") == "running"


def test_reaper_resets_multiple_orphans_mixed_lock_state(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three stuck stages: 2 free + 1 lock-held → reaper resets 2.

    Each stage maps to its own source so the per-source lock-held
    decision is independent. Holding ``job_source:db`` should ONLY
    suppress reset of the db-lane stage, not the init-lane or
    etoro-lane stages.
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(
        monkeypatch,
        {
            "alpha_job": "init",
            "bravo_job": "etoro",
            "charlie_job": "db",
        },
    )

    run_id = _make_test_run(
        ebull_test_conn,
        stages=[
            ("alpha_stage", "alpha_job"),
            ("bravo_stage", "bravo_job"),
            ("charlie_stage", "charlie_job"),
        ],
    )
    for stage_key in ("alpha_stage", "bravo_stage", "charlie_stage"):
        _seed_stage(
            ebull_test_conn,
            run_id=run_id,
            stage_key=stage_key,
            status="running",
            started_minutes_ago=10,
        )

    from tests.fixtures.ebull_test_db import test_database_url

    holder_url = test_database_url()
    with psycopg.connect(holder_url, autocommit=True) as holder:
        held = holder.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
            ("job_source:db",),
        ).fetchone()
        assert held is not None and held[0] is True

        try:
            reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
            ebull_test_conn.commit()
        finally:
            holder.execute(
                "SELECT pg_advisory_unlock(hashtext(%s)::int)",
                ("job_source:db",),
            )

    assert reset == 2
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="alpha_stage") == "pending"
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="bravo_stage") == "pending"
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="charlie_stage") == "running"


def test_reaper_skips_stage_with_unknown_source_mapping(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A stage whose ``job_name`` is not in the source registry is left alone.

    Defensive: the reaper MUST NOT reset a stage whose lock-key shape
    it cannot derive, because doing so would silently reset a stage
    whose worker is alive (the registry gap is the operator's mistake
    — not a crash signal).

    Synthetic job name ``ghost_job`` is NEVER registered; ``source_for``
    raises ``KeyError`` → reaper logs a warning and skips.
    """
    _reset_state(ebull_test_conn)

    run_id = _make_test_run(ebull_test_conn, stages=[("ghost_stage", "ghost_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="ghost_stage",
        status="running",
        started_minutes_ago=10,
    )

    reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert reset == 0
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="ghost_stage") == "running"


def test_reaper_appends_last_error_does_not_replace(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Existing last_error context is preserved + appended.

    Forensic value: if the operator wants to investigate WHY the stage
    crashed, the previous attempt's last_error must survive the
    reaper's reset.
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"forensic_job": "init"})

    run_id = _make_test_run(ebull_test_conn, stages=[("forensic_stage", "forensic_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="forensic_stage",
        status="running",
        started_minutes_ago=10,
        last_error="previous attempt: ConnectionResetError mid-COPY",
    )

    reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert reset == 1
    last_error = _last_error_of(ebull_test_conn, run_id=run_id, stage_key="forensic_stage")
    assert last_error is not None
    assert "previous attempt: ConnectionResetError mid-COPY" in last_error
    assert "reaper: reset from orphaned running" in last_error


# ---------------------------------------------------------------------------
# Idempotence
# ---------------------------------------------------------------------------


def test_reaper_is_idempotent_when_no_orphans(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeat calls on a clean run return 0; never throws.

    Steady state of every dispatcher prelude — the reaper MUST be safe
    to call on every restart even when nothing is wrong.
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"clean_job": "init"})

    run_id = _make_test_run(ebull_test_conn, stages=[("clean_stage", "clean_job")])
    # Stage stays at 'pending' default — no started_at.

    for _ in range(3):
        reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
        ebull_test_conn.commit()
        assert reset == 0

    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="clean_stage") == "pending"


def test_reaper_grace_window_is_five_minutes() -> None:
    """Pin the grace constant so a future surgery surfaces in review."""
    assert _REAPER_GRACE_SECONDS == 300


def test_reaper_detects_held_lock_for_negative_hashtext_source(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: negative hashtext keys must split correctly in pg_locks.

    Empirical (2026-05-23): ``hashtext('job_source:finra') = -685_386_401``.
    When widened to bigint for ``pg_try_advisory_lock(bigint)``, the
    sign extension fills the high 32 bits with 1s, so ``pg_locks`` ends
    up with ``classid=4_294_967_295`` (NOT 0) and ``objid=3_609_580_895``.
    A naive ``classid=0 AND objid=<hashtext>`` probe would miss the
    held lock entirely and the reaper would reset a stage whose worker
    is alive.

    This test pins the signed-split SQL end-to-end: acquire from a
    sibling session, run the reaper against a stuck ``finra`` stage,
    assert the reaper leaves it alone (== the probe found the held
    lock).
    """
    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"finra_neg_job": "finra"})

    # Sanity: confirm 'finra' source key is negative; if a future
    # source-naming change makes the hashtext positive this regression
    # has lost its teeth and we should pick a different signature.
    h = _hashtext_int(ebull_test_conn, "job_source:finra")
    assert h < 0, (
        f"job_source:finra hashtext is now {h}; pick a different negative-hashtext "
        f"source for this regression test or revisit the probe shape."
    )

    run_id = _make_test_run(ebull_test_conn, stages=[("finra_stuck_stage", "finra_neg_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="finra_stuck_stage",
        status="running",
        started_minutes_ago=10,
    )

    from tests.fixtures.ebull_test_db import test_database_url

    holder_url = test_database_url()
    with psycopg.connect(holder_url, autocommit=True) as holder:
        held = holder.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
            ("job_source:finra",),
        ).fetchone()
        assert held is not None and held[0] is True

        try:
            reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
            ebull_test_conn.commit()
        finally:
            holder.execute(
                "SELECT pg_advisory_unlock(hashtext(%s)::int)",
                ("job_source:finra",),
            )

    assert reset == 0, "reaper should detect the negative-hashtext lock as held"
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="finra_stuck_stage") == "running"


# ---------------------------------------------------------------------------
# sec_rate in-process gate path (#1542)
# ---------------------------------------------------------------------------


def test_reaper_resets_sec_rate_stage_not_in_gate(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sec_rate stage absent from the in-process gate IS reset.

    After a jobs-process crash, a new process starts with an empty
    SecLaneGate. A ``running`` bootstrap stage that was being driven by
    the crashed process has no gate entry -> worker provably dead ->
    reaper must reset it.
    """
    from app.jobs.sec_lane_gate import reset_for_tests

    reset_for_tests()  # ensure gate is clean for this test

    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"sec_rate_orphan_job": "sec_rate"})

    run_id = _make_test_run(ebull_test_conn, stages=[("sec_rate_orphan_stage", "sec_rate_orphan_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_rate_orphan_stage",
        status="running",
        started_minutes_ago=10,
    )

    reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
    ebull_test_conn.commit()

    assert reset == 1
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="sec_rate_orphan_stage") == "pending"


def test_reaper_skips_sec_rate_stage_held_in_gate(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sec_rate stage present in the in-process gate is NOT reset.

    The gate entry means the job is actively running in THIS process;
    the reaper must leave it alone.

    Access the singleton via the MODULE attribute (``sec_lane_gate.SEC_LANE_GATE``)
    after ``reset_for_tests()``, NOT via a name imported before the reset — the
    import would capture the old object and ``try_acquire`` / ``is_held`` would
    operate on different instances.
    """
    from app.jobs import sec_lane_gate as slg

    slg.reset_for_tests()
    # Access via module attribute so try_acquire, is_held (in the reaper), and
    # release all operate on the same post-reset instance.
    assert slg.SEC_LANE_GATE.try_acquire("sec_rate_live_job") is True

    _reset_state(ebull_test_conn)
    _register_synthetic_jobs(monkeypatch, {"sec_rate_live_job": "sec_rate"})

    run_id = _make_test_run(ebull_test_conn, stages=[("sec_rate_live_stage", "sec_rate_live_job")])
    _seed_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_rate_live_stage",
        status="running",
        started_minutes_ago=10,
    )

    try:
        reset = reap_orphaned_running_stages(ebull_test_conn, run_id=run_id)
        ebull_test_conn.commit()
    finally:
        slg.SEC_LANE_GATE.release("sec_rate_live_job")

    assert reset == 0
    assert _status_of(ebull_test_conn, run_id=run_id, stage_key="sec_rate_live_stage") == "running"
