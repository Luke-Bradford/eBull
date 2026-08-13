"""#2628 — the evaluation phase must hold no lock on the strategy result tables.

``strategy_backtest_run`` runs for hours on a ``psycopg`` connection with
``autocommit=False``, so §10's pre-flight ``SELECT`` on ``strategy_results_store``
used to keep an ``AccessShareLock`` on it until the very end. A *pending*
``AccessExclusiveLock`` queues ahead of new readers, so a migration that is merely
waiting stops every subsequent read of that relation from starting — which is how
a waiting ``ALTER TABLE`` stalled the dev stack and made
``tests/smoke/test_app_boots.py`` fail for environmental reasons.

⚠ This file uses the REAL pre-flight query against a real connection. A stub
connection would pass whatever we told it to and prove nothing about locks: the
whole claim is a property of a Postgres transaction, not of our call sequence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from app.services import backtest_run
from app.services.backtest_run import _Corpus, run_backtest
from app.services.strategy_manifest import STRATEGY_MANIFEST
from tests.fixtures.ebull_test_db import test_database_url

#: The relations a migration on the strategy result surface takes ACCESS
#: EXCLUSIVE on, and therefore the ones the long phases must not be holding.
_STRATEGY_RESULT_RELATIONS = ("strategy_results_store", "strategy_results", "strategy_result_folds")

_LOCKED_RELATIONS = """
    SELECT c.relname
    FROM pg_locks l
    JOIN pg_class c ON c.oid = l.relation
    WHERE l.pid = %(pid)s AND l.locktype = 'relation'
"""


class _StopAfterEvaluation(Exception):
    """Sentinel — the phases after evaluation are not what this file measures."""


@dataclass
class _Observation:
    transaction_status: int
    locked: frozenset[str]


def _tiny_corpus() -> _Corpus:
    axis = (date(2022, 1, 3), date(2022, 1, 4))
    return _Corpus(
        universe=(1,),
        axis=axis,
        axis_pos={when: index for index, when in enumerate(axis)},
        pairs=(),
        evaluation_start=axis[0],
        evaluation_end=axis[-1],
        in_sample_axis=(),
        in_sample_bar_counts=(),
    )


def _observe(conn: psycopg.Connection[Any], probe: psycopg.Connection[Any]) -> _Observation:
    rows = probe.execute(_LOCKED_RELATIONS, {"pid": conn.info.backend_pid}).fetchall()
    return _Observation(
        transaction_status=int(conn.info.transaction_status),
        locked=frozenset(str(row[0]) for row in rows),
    )


def _run_until_evaluation(
    conn: psycopg.Connection[Any],
    probe: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    release_read_locks: bool,
) -> _Observation:
    """Drive the real pre-flight, then observe the connection at evaluation."""
    seen: list[_Observation] = []
    monkeypatch.setattr(backtest_run, "load_corpus", lambda *_args, **_kwargs: _tiny_corpus())

    def _evaluate(inner_conn: psycopg.Connection[Any], *_args: object, **_kwargs: object) -> object:
        seen.append(_observe(inner_conn, probe))
        raise _StopAfterEvaluation

    monkeypatch.setattr(backtest_run, "evaluate_arm", _evaluate)
    monkeypatch.setattr(backtest_run, "evaluate_level_arms", _evaluate)

    strategy_id = next(iter(sorted(STRATEGY_MANIFEST)))
    with pytest.raises(_StopAfterEvaluation):
        run_backtest(
            conn,
            strategy_id=strategy_id,
            manifest={strategy_id: STRATEGY_MANIFEST[strategy_id]},
            release_read_locks=release_read_locks,
        )
    assert len(seen) == 1
    return seen[0]


@pytest.fixture
def probe_conn(ebull_test_conn: psycopg.Connection[Any]) -> Any:
    """A second session, so the locks are read from outside the transaction.

    ⚠ Built from ``test_database_url()`` and not ``ebull_test_conn.info.dsn`` —
    ``libpq`` strips the password out of the reported DSN, so the round trip
    fails with ``no password supplied``.
    """
    assert not ebull_test_conn.closed
    conn = psycopg.connect(test_database_url(), autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def test_the_preflight_read_takes_a_lock_the_evaluation_phase_must_not_keep(
    ebull_test_conn: psycopg.Connection[Any],
    probe_conn: psycopg.Connection[Any],
) -> None:
    """The control: without a release, a bare read holds the lock indefinitely.

    This pins the MECHANISM the fix rests on. If Postgres or ``psycopg`` ever
    stopped holding relation locks to the end of the transaction, the release
    below would be pointless and this test — not a production incident — is what
    should say so.
    """
    ebull_test_conn.execute("SELECT count(*) FROM strategy_results_store").fetchone()
    held = _observe(ebull_test_conn, probe_conn)
    assert "strategy_results_store" in held.locked
    assert held.transaction_status != int(psycopg.pq.TransactionStatus.IDLE)

    ebull_test_conn.rollback()
    after = _observe(ebull_test_conn, probe_conn)
    assert after.locked == frozenset()
    assert after.transaction_status == int(psycopg.pq.TransactionStatus.IDLE)


def test_evaluation_holds_no_strategy_result_lock_when_the_caller_releases(
    ebull_test_conn: psycopg.Connection[Any],
    probe_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = _run_until_evaluation(ebull_test_conn, probe_conn, monkeypatch, release_read_locks=True)
    assert observed.transaction_status == int(psycopg.pq.TransactionStatus.IDLE)
    assert observed.locked.intersection(_STRATEGY_RESULT_RELATIONS) == frozenset()


def test_without_the_opt_in_the_preflight_lock_is_still_held_at_evaluation(
    ebull_test_conn: psycopg.Connection[Any],
    probe_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default is unchanged, and deliberately so.

    ``scripts/verify_2429_total_return.py`` and
    ``scripts/benchmark_2488_evidence_refresh.py`` run the whole thing inside
    their own transaction and roll it back so the measurement never charges the
    trial register. Releasing under them would commit their rows.
    """
    observed = _run_until_evaluation(ebull_test_conn, probe_conn, monkeypatch, release_read_locks=False)
    assert "strategy_results_store" in observed.locked


def test_the_write_phase_runs_inside_one_transaction_even_after_a_release(
    ebull_test_conn: psycopg.Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invocation atomicity survives the release, and that is load-bearing.

    Once the read locks are dropped the connection is idle, so each pair
    writer's ``conn.transaction()`` would become TOP-LEVEL and commit on its own.
    A run that died part-way would then leave a partially written pinned window
    behind, which ``_recent_evidence_completion`` refuses as needing operator
    repair — turning a resumable failure into one that needs a human.
    """
    seen: list[int] = []
    monkeypatch.setattr(backtest_run, "load_corpus", lambda *_args, **_kwargs: _tiny_corpus())
    # A measurement that carries no namespace: the deflation pass then has
    # nothing to group and the run reaches the write phase with an empty book,
    # which is all this test needs it to do.
    measurement = SimpleNamespace(
        strategy_id="stub",
        ambiguity_arm=None,
        quarantine_arm="masked",
        series_evaluated=0,
        elapsed_s=0.0,
        namespaces={},
        holdout_positions_discarded=0,
    )
    monkeypatch.setattr(backtest_run, "_assert_ambiguity_contract", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backtest_run, "evaluate_arm", lambda *_args, **_kwargs: measurement)
    monkeypatch.setattr(backtest_run, "evaluate_level_arms", lambda *_args, **_kwargs: (measurement,))

    def _write(inner_conn: psycopg.Connection[Any], **_kwargs: object) -> object:
        seen.append(int(inner_conn.info.transaction_status))
        raise _StopAfterEvaluation

    monkeypatch.setattr(backtest_run, "_write_rows", _write)

    strategy_id = next(iter(sorted(STRATEGY_MANIFEST)))
    with pytest.raises(_StopAfterEvaluation):
        run_backtest(
            ebull_test_conn,
            strategy_id=strategy_id,
            manifest={strategy_id: STRATEGY_MANIFEST[strategy_id]},
            release_read_locks=True,
        )
    assert seen == [int(psycopg.pq.TransactionStatus.INTRANS)]


def test_release_is_refused_when_the_caller_already_holds_a_transaction(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The opt-in is checked, not trusted — otherwise it discards caller work."""
    ebull_test_conn.execute("SELECT 1").fetchone()
    with pytest.raises(RuntimeError, match="already in a transaction"):
        run_backtest(ebull_test_conn, release_read_locks=True)


def test_a_caller_supplied_transaction_is_untouched_without_the_opt_in(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The guard fires on the flag, not on the connection state.

    Asserted through the NEXT check in the function: a mismatched trial-register
    assertion raises ``ValueError``, which is only reachable once the entry guard
    has let the call through.
    """
    ebull_test_conn.execute("SELECT 1").fetchone()
    with pytest.raises(ValueError, match="trial register"):
        run_backtest(ebull_test_conn, trial_register_version="not-the-live-register")
