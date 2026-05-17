"""#1184 — JobLock per-source re-entrancy regression gate.

Spec: ``docs/superpowers/specs/2026-05-17-orchestrator-inner-lock-removal.md``.

Six tests covering the new same-source re-entrancy contract added to
``app/jobs/locks.py::JobLock`` plus the ``test_only_per_name`` escape
hatch invariant.

Tests that only exercise advisory locks use ``settings.database_url``
directly — advisory locks are cluster-wide, not database-scoped, and no
rows are written. Tests that go through ``_run_with_lock`` /
``_latest_job_outcome`` (which write/read ``job_runs`` rows) monkeypatch
``settings.database_url`` to ``test_database_url()`` and use
``ebull_test_conn`` for per-test cleanup so the dev DB is never touched.

The same ``xdist_group`` marker as ``tests/test_joblock_per_source.py``
serialises this module onto a single xdist worker — Postgres advisory
locks are cluster-wide and parallel workers competing for the same
``hashtext('job_source:db')`` would otherwise see cross-test contention.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from unittest.mock import patch

import psycopg
import pytest

from app.config import settings
from app.jobs.locks import _HELD_SOURCES, JobAlreadyRunning, JobLock
from app.services.sync_orchestrator import adapters
from app.services.sync_orchestrator.types import LayerOutcome
from tests.fixtures.ebull_test_db import test_database_url

pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


# ---------------------------------------------------------------------------
# §6.6.2(a) — Same-source re-entrant acquire bypasses Postgres
# ---------------------------------------------------------------------------


def test_same_source_reentrant_bypasses_pg_lock() -> None:
    """Outer ``orchestrator_full_sync`` (source=db) + inner
    ``fx_rates_refresh`` (source=db, via MANUAL_TRIGGER_JOB_SOURCES)
    must NOT cause a second ``psycopg.connect`` call inside
    ``JobLock.__enter__`` — the re-entrancy bypass short-circuits before
    the connection is opened.

    Pre-#1184 the second acquire would have opened a fresh psycopg
    session, called ``pg_try_advisory_lock``, received FALSE (the outer
    holds the source-key on its own session), and raised
    ``JobAlreadyRunning``. Post-fix, the contextvar check short-circuits
    BEFORE Postgres is touched.
    """
    real_connect = psycopg.connect

    with JobLock(settings.database_url, "orchestrator_full_sync"):
        # Count connect calls made FROM app.jobs.locks during the inner
        # acquire. Other modules may legitimately open connections;
        # patching the symbol on app.jobs.locks scopes the count.
        connect_calls: list[tuple[object, ...]] = []

        def counting_connect(*args: object, **kwargs: object) -> object:
            connect_calls.append(args)
            return real_connect(*args, **kwargs)  # type: ignore[arg-type]

        with patch("app.jobs.locks.psycopg.connect", side_effect=counting_connect):
            with JobLock(settings.database_url, "fx_rates_refresh") as inner:
                assert inner._reentrant is True, "inner JobLock should have bypassed via re-entrancy"

        assert connect_calls == [], (
            f"re-entrant JobLock acquire must NOT open a psycopg connection; got {len(connect_calls)} calls"
        )


# ---------------------------------------------------------------------------
# §6.6.2(b) — Cross-source acquire still takes a real PG lock
# ---------------------------------------------------------------------------


def test_different_source_still_acquires_real_pg_lock() -> None:
    """Outer ``orchestrator_full_sync`` (source=db) + inner
    ``daily_portfolio_sync`` (source=etoro) — inner DOES go to
    Postgres because the source is not in ``_HELD_SOURCES``. A second
    raw psycopg connection mimicking a different process must see
    ``pg_try_advisory_lock`` return FALSE on the etoro key while the
    inner is held.

    Regression gate for Codex 1a v1 BLOCKING 1: ensures non-db adapter
    targets still serialise across processes under the new outer-db
    re-entrancy.
    """
    with JobLock(settings.database_url, "orchestrator_full_sync"):
        with JobLock(settings.database_url, "daily_portfolio_sync") as inner:
            assert inner._reentrant is False
            assert inner._conn is not None
            # Second connection mimics another process trying the same source.
            with psycopg.connect(settings.database_url, autocommit=True) as side:
                row = side.execute(
                    "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
                    ("job_source:etoro",),
                ).fetchone()
            assert row is not None
            acquired_on_side = bool(row[0])
            # Release the side lock if Postgres somehow gave it to us
            # (would be a real serialisation regression).
            if acquired_on_side:
                with psycopg.connect(settings.database_url, autocommit=True) as side:
                    side.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s)::int)",
                        ("job_source:etoro",),
                    )
            assert not acquired_on_side, (
                "cross-process pg_try_advisory_lock on the etoro key MUST return FALSE while the inner JobLock holds it"
            )


# ---------------------------------------------------------------------------
# §6.6.2(c) — Scheduled-cron orchestrator + inner db adapter completes
# ---------------------------------------------------------------------------


def test_orchestrator_outer_holds_db_inner_db_adapter_runs(
    monkeypatch: pytest.MonkeyPatch,
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The #1184 symptom regression gate.

    Acquire the outer scheduled-cron JobLock; call
    ``adapters._run_with_lock("fx_rates_refresh", legacy_fn=...)`` where
    ``legacy_fn`` writes a ``job_runs`` row with ``status='success'``.
    Assert the returned ``LayerOutcome.SUCCESS`` — pre-fix this returned
    the PREREQ_SKIP string because the inner JobLock self-skipped.
    """
    test_url = test_database_url()
    # ``settings`` is a singleton imported by both modules under test;
    # mutating the attribute on the instance is visible everywhere.
    monkeypatch.setattr(settings, "database_url", test_url)

    called = {"count": 0}

    def fake_fn() -> None:
        called["count"] += 1
        # Write a job_runs success row that _latest_job_outcome can read.
        with psycopg.connect(test_url, autocommit=True) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count)
                    VALUES (%s, %s, %s, 'success', 1)
                    """,
                    ("fx_rates_refresh", datetime.now(UTC), datetime.now(UTC)),
                )

    with JobLock(test_url, "orchestrator_full_sync"):
        result = adapters._run_with_lock(job_name="fx_rates_refresh", legacy_fn=fake_fn)

    assert called["count"] == 1, "legacy_fn must have been invoked"
    assert not isinstance(result, str), f"adapter returned PREREQ_SKIP string instead of an outcome tuple: {result!r}"
    outcome, row_count, _err = result
    assert outcome is LayerOutcome.SUCCESS
    assert row_count == 1


# ---------------------------------------------------------------------------
# §6.6.2(d) — /sync HTTP path inner lock serialises against manual
# ---------------------------------------------------------------------------


def test_sync_http_path_inner_lock_serialises_against_manual(
    monkeypatch: pytest.MonkeyPatch,
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The /sync HTTP path does NOT hold an outer JobLock. The inner
    adapter JobLock must still serialise against a concurrent same-
    source manual trigger held on a separate connection.

    Simulates the manual hold by acquiring the real
    ``pg_try_advisory_lock`` for ``job_source:db`` on a side connection,
    then calling ``adapters._run_with_lock("fx_rates_refresh", ...)``
    with no outer in this context. The adapter must return the
    PREREQ_SKIP string and ``legacy_fn`` must NEVER be called.

    Regression gate for Codex 1a v2 finding (c).
    """
    test_url = test_database_url()
    # ``settings`` is a singleton imported by both modules under test;
    # mutating the attribute on the instance is visible everywhere.
    monkeypatch.setattr(settings, "database_url", test_url)

    # Drain _HELD_SOURCES — should already be empty in this test
    # context, but defend against pollution from prior tests in the
    # same pytest worker.
    assert _HELD_SOURCES.get() == frozenset(), "test must start with empty _HELD_SOURCES; prior test leaked state"

    called = {"count": 0}

    def fake_fn() -> None:
        called["count"] += 1

    side = psycopg.connect(test_url, autocommit=True)
    try:
        row = side.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
            ("job_source:db",),
        ).fetchone()
        assert row is not None and bool(row[0]), "side lock must acquire on empty db source"

        result = adapters._run_with_lock(job_name="fx_rates_refresh", legacy_fn=fake_fn)

        assert isinstance(result, str), (
            f"adapter must return PREREQ_SKIP string when inner JobLock busy; got {result!r}"
        )
        assert "JobLock busy" in result or "cron holder active" in result
        assert called["count"] == 0, "legacy_fn must NOT be called when inner JobLock was busy"
    finally:
        side.execute(
            "SELECT pg_advisory_unlock(hashtext(%s)::int)",
            ("job_source:db",),
        )
        side.close()


# ---------------------------------------------------------------------------
# §6.6.2(e) — Reset restores prior held set on exception
# ---------------------------------------------------------------------------


def test_reset_restores_prior_held_set_on_exception() -> None:
    """If an inner JobLock acquire raises during ``__enter__``, the
    outer JobLock's ``__exit__`` must still restore ``_HELD_SOURCES``
    to its prior value via the saved token.

    Pin the LIFO contract Codex 1a v2 minor flagged.
    """
    real_connect = psycopg.connect

    raise_next = {"flag": False}

    def maybe_raising_connect(*args: object, **kwargs: object) -> object:
        if raise_next["flag"]:
            raise_next["flag"] = False
            raise RuntimeError("simulated connect failure during inner __enter__")
        return real_connect(*args, **kwargs)  # type: ignore[arg-type]

    assert _HELD_SOURCES.get() == frozenset()

    with JobLock(settings.database_url, "orchestrator_full_sync"):
        assert _HELD_SOURCES.get() == frozenset({"db"})
        # Patch only DURING the inner acquire attempt so the outer
        # __exit__'s release path still uses the real connect.
        with patch("app.jobs.locks.psycopg.connect", side_effect=maybe_raising_connect):
            raise_next["flag"] = True
            with pytest.raises(RuntimeError, match="simulated connect failure"):
                with JobLock(settings.database_url, "daily_portfolio_sync"):
                    pytest.fail("inner __enter__ should have raised before entering body")
        # Inner acquire raised before mutating _HELD_SOURCES, so the
        # held set should still be {"db"} (outer only).
        assert _HELD_SOURCES.get() == frozenset({"db"})

    # Outer __exit__ restores to prior (empty) frozenset.
    assert _HELD_SOURCES.get() == frozenset()


# ---------------------------------------------------------------------------
# §6.6.2(f) — test_only_per_name escape hatch is NEVER re-entrant
# ---------------------------------------------------------------------------


def test_test_only_per_name_acquires_never_treated_as_reentrant() -> None:
    """``test_only_per_name`` keys on raw ``job_name``, not on a
    ``source``. Two sibling acquires on the same job_name open two
    psycopg sessions and collide at the real Postgres advisory lock
    — even if a production ``JobLock`` outer is held in the same
    context with a source that happens to match the test job_name's
    semantics.

    Pins the escape-hatch opt-out from #1184 re-entrancy.
    """
    raw_key = "fake_test_job_for_reentrancy_pin"

    with JobLock(settings.database_url, "orchestrator_full_sync"):
        # _HELD_SOURCES contains 'db' here. test_only_per_name acquires
        # MUST ignore that — _source is None for the escape hatch.
        assert _HELD_SOURCES.get() == frozenset({"db"})

        with JobLock.test_only_per_name(settings.database_url, raw_key) as first:
            assert first._reentrant is False
            assert first._conn is not None, "test_only_per_name must always open a real connection"
            # Sibling acquire of the SAME raw key from a different
            # JobLock instance must collide at the Postgres layer.
            with pytest.raises(JobAlreadyRunning):
                with JobLock.test_only_per_name(settings.database_url, raw_key):
                    pytest.fail("second test_only_per_name acquire on same raw key must raise")


# ---------------------------------------------------------------------------
# Cross-thread serialisation invariant (sanity check for Python
# threading.Thread NOT propagating ContextVar) — referenced from
# tests/test_joblock_per_source.py rewrites.
# ---------------------------------------------------------------------------


def test_threads_do_not_inherit_held_sources() -> None:
    """ContextVar is not auto-propagated across ``threading.Thread``.
    A new thread starts with the default empty frozenset even when the
    spawning thread holds an outer JobLock.

    This is a load-bearing assumption for the rewritten same-source
    serialisation tests in ``tests/test_joblock_per_source.py``: those
    tests rely on the inner thread NOT inheriting the outer thread's
    ``_HELD_SOURCES`` so the inner acquire goes to Postgres and
    collides as expected.
    """
    observed: list[frozenset[str]] = []

    def worker() -> None:
        observed.append(_HELD_SOURCES.get())

    with JobLock(settings.database_url, "orchestrator_full_sync"):
        assert _HELD_SOURCES.get() == frozenset({"db"})
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        t.join(timeout=5.0)
        assert not t.is_alive(), "worker thread hung"

    assert observed == [frozenset()], (
        f"new thread inherited _HELD_SOURCES = {observed!r}; cross-thread serialisation tests would silently bypass."
    )
