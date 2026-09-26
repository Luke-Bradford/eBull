"""#3385 obligation 131 — the retry record and abandonment, against a real database.

``evaluate`` records every infrastructure error after the registration commits
(``sql/429``, append-only); ``abandon_trial`` writes ``abandoned`` only on a recurring one,
through the audited door for validation.
"""

from __future__ import annotations

from typing import Any, LiteralString

import psycopg
import pytest

from app.services import hunt_door, hunt_inference
from app.services import hunt_harness as hh
from app.services.hunt_harness import HuntOutcome, TrialSpec
from tests.test_hunt_door_db import (  # noqa: F401 - fixtures are used by name
    _count,
    _freeze,
    _register,
    bound,
    discovered,
)
from tests.test_hunt_harness_db import _ok, _run, _spec


class _Flaky(RuntimeError):
    pass


def _fail(conn: psycopg.Connection[Any], spec: TrialSpec, times: int, error: type[Exception] = _Flaky) -> None:
    def crash(_conn: psycopg.Connection[Any], _spec: TrialSpec, **_kw: Any) -> Any:
        raise error("price read timed out")

    for _ in range(times):
        with pytest.raises(error):
            _run(conn, spec, crash)


def test_a_failure_after_registration_is_recorded_and_append_only(
    ebull_test_conn: psycopg.Connection[Any],
    bound: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
) -> None:
    spec = _spec(bound)
    _fail(ebull_test_conn, spec, 2)
    rows = ebull_test_conn.execute("SELECT hunt_trial_id, error_class, error_text FROM hunt_trial_retries").fetchall()
    ebull_test_conn.commit()
    assert [(row[1], row[2]) for row in rows] == [(hh.error_class_of(_Flaky()), "price read timed out")] * 2
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        ebull_test_conn.execute("DELETE FROM hunt_trial_retries")
    ebull_test_conn.rollback()
    # Once the trial has an outcome there is nothing left to retry.
    assert isinstance(_run(ebull_test_conn, spec, _ok), HuntOutcome)
    with pytest.raises(psycopg.errors.RaiseException, match="already has an outcome"):
        ebull_test_conn.execute(
            "INSERT INTO hunt_trial_retries (hunt_trial_id, error_class, error_text, recorded_by) "
            "VALUES (%s, 'x', 'y', 'test')",
            (rows[0][0],),
        )
    ebull_test_conn.rollback()


def test_abandonment_needs_a_recurring_error_then_completes_the_trial(
    ebull_test_conn: psycopg.Connection[Any],
    bound: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
) -> None:
    spec = _spec(bound)
    _fail(ebull_test_conn, spec, 3)
    trial = _count(ebull_test_conn, "SELECT max(hunt_trial_id) FROM hunt_trials")
    with pytest.raises(hh.AbandonmentRefused, match="3 recorded failures"):
        hh.abandon_trial(ebull_test_conn, trial, reason="flaky", abandoned_by="test")
    _fail(ebull_test_conn, spec, 1, error=ValueError)
    with pytest.raises(hh.AbandonmentRefused, match="not one recurring error"):
        hh.abandon_trial(ebull_test_conn, trial, reason="flaky", abandoned_by="test")
    _fail(ebull_test_conn, spec, 4)
    hh.abandon_trial(ebull_test_conn, trial, reason="series 1 unreadable (#test)", abandoned_by="test")
    cached = _run(ebull_test_conn, spec, _ok)
    assert isinstance(cached, HuntOutcome) and cached.cached and cached.status == "abandoned"
    assert cached.statistics["abandoned"]["reason"] == "series 1 unreadable (#test)"
    assert len(cached.statistics["abandoned"]["retry_ids"]) == 8
    with pytest.raises(hh.AbandonmentRefused, match="without an outcome"):
        hh.abandon_trial(ebull_test_conn, trial, reason="again", abandoned_by="test")


def test_a_validation_abandonment_passes_the_door_and_reads_not_pass_refused(
    ebull_test_conn: psycopg.Connection[Any],
    discovered: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    _fail(ebull_test_conn, discovered["pinned"], hh.ABANDON_MIN_FAILURES)
    trial = _count(ebull_test_conn, "SELECT hunt_trial_id FROM hunt_trials WHERE split = 'validation'")
    reads: LiteralString = "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = 'hunt-1-validation'"
    before = _count(ebull_test_conn, reads)
    hh.abandon_trial(ebull_test_conn, trial, reason="unreadable (#test)", abandoned_by="test")
    assert _count(ebull_test_conn, reads) == before + 1
    readout = hunt_door.validation_readout(ebull_test_conn, "hunt-1")
    (candidate,) = readout.candidates
    assert readout.complete and candidate.verdict is hunt_inference.Verdict.NOT_PASS_REFUSED
    assert candidate.reasons == ("abandoned",)
    assert readout.closure is hunt_inference.HuntClosure.NO_DEMONSTRATED_EDGE
