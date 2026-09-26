"""#3385 slice 2a — the hunt trial log against a real database.

One test per new SQL mechanism (append-only triggers, both partial UNIQUEs, the outcome
binding trigger, the column/spec CHECK), plus ``evaluate``'s ordering: the search is
committed before ``compute`` runs, a crash leaves it registered and retryable even at a
full budget, and every refusal before registration writes nothing.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import psycopg
import pytest

from app.services import hunt_harness as hh
from app.services.hunt_harness import ComputedOutcome, HuntOutcome, HuntRefused, TrialSpec
from tests.test_hunt_harness import trial_spec, universe_identity

_SIGNAL_SOURCE = b"def gap_score(t, view, constants):\n    return {}\n"


@pytest.fixture
def bound(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> dict[str, Any]:
    """Make a spec's identities the running ones, without a research archive."""
    (tmp_path / "overnight_intraday.py").write_bytes(_SIGNAL_SOURCE)
    monkeypatch.setattr(hh, "SIGNAL_PACKAGE_DIR", tmp_path)
    tariff = hh.HuntTariff(
        url="https://www.etoro.com/trading/fees/",
        fetched_on=date(2026, 9, 26),
        text_sha256="5" * 64,
        residence_country="United Kingdom",
        account_currency="USD",
        proportional_commission_per_side=0.0,
    )
    monkeypatch.setattr(hh, "HUNT_TARIFF", tariff)
    monkeypatch.setattr(hh, "HUNT_BUDGETS", {"hunt-1": 3, "hunt-2": 3})
    monkeypatch.setattr(hh, "read_universe_identity", lambda _conn, _universe: universe_identity())
    return {
        "signal_code_sha256": hh.signal_code_sha256("overnight_intraday:gap_score"),
        "cost_model_id": tariff.cost_model_id(),
        "calendar_identity": hh.calendar_identity(),
    }


def _spec(bound: dict[str, Any], **overrides: Any) -> TrialSpec:
    return trial_spec(**{**bound, **overrides})


def _ok(_conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
    return ComputedOutcome("computed", {"t": 1.25, "mean": 0.001, "cells": {"zero_recovery": "ok"}}, (0.001, -0.002))


def _rows(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT hunt_id, split, purpose, candidate_sha256 FROM hunt_trials ORDER BY hunt_trial_id"
    ).fetchall()
    conn.commit()
    return rows


def _outcomes(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute("SELECT count(*) FROM hunt_trial_outcomes").fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


class _PriceReadRaised(RuntimeError):
    pass


# --- evaluate ordering ----------------------------------------------------------------------


def test_the_search_is_registered_before_compute_and_survives_a_crash(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    spec = _spec(bound)
    seen: list[list[tuple[Any, ...]]] = []

    def crash(conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        # Read from ANOTHER session: only a committed registration is visible there.
        with psycopg.connect(conn.info.dsn, password=conn.info.password) as other:
            seen.append(other.execute("SELECT hunt_id, purpose FROM hunt_trials").fetchall())
        raise _PriceReadRaised

    with pytest.raises(_PriceReadRaised):
        hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=crash)
    assert seen == [[("hunt-1", "evaluate")]]
    assert _rows(ebull_test_conn) == [("hunt-1", "discovery", "evaluate", spec.candidate_sha256)]
    assert _outcomes(ebull_test_conn) == 0


def test_a_retry_reuses_the_registration_even_at_a_full_budget(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(bound)
    monkeypatch.setattr(hh, "HUNT_BUDGETS", {"hunt-1": 1})

    def crash(_conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        raise _PriceReadRaised

    with pytest.raises(_PriceReadRaised):
        hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=crash)
    result = hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=_ok)
    assert isinstance(result, HuntOutcome) and not result.cached
    assert len(_rows(ebull_test_conn)) == 1
    # The budget is now full for any NEW candidate.
    other = hh.evaluate(ebull_test_conn, _spec(bound, lag=2), registered_by="test", compute=_ok)
    assert isinstance(other, HuntRefused) and other.reason == "budget_exhausted"
    assert len(_rows(ebull_test_conn)) == 1


def test_a_stored_outcome_is_returned_not_recomputed(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    spec = _spec(bound)
    first = hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=_ok)

    def never(_conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        raise AssertionError("a cached discovery outcome must not recompute")

    relabelled = _spec(bound, family="renamed_family", mechanism="new story")
    second = hh.evaluate(ebull_test_conn, relabelled, registered_by="test", compute=never)
    assert isinstance(first, HuntOutcome) and isinstance(second, HuntOutcome)
    assert second.cached and second.hunt_trial_id == first.hunt_trial_id
    assert second.statistics == first.statistics == {"t": 1.25, "mean": 0.001, "cells": {"zero_recovery": "ok"}}
    assert second.active_series == (0.001, -0.002)
    assert second.outcome_sha256 == first.outcome_sha256
    assert len(_rows(ebull_test_conn)) == 1


def test_a_relabelled_retry_binds_its_outcome_to_the_registered_spec(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    spec = _spec(bound)

    def crash(_conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        raise _PriceReadRaised

    with pytest.raises(_PriceReadRaised):
        hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=crash)
    relabelled = _spec(bound, family="renamed_family")
    assert relabelled.spec_sha256 != spec.spec_sha256
    first = hh.evaluate(ebull_test_conn, relabelled, registered_by="test", compute=_ok)
    cached = hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=_ok)
    assert isinstance(first, HuntOutcome) and isinstance(cached, HuntOutcome)
    assert cached.cached and cached.outcome_sha256 == first.outcome_sha256


def test_a_statistical_refusal_is_an_outcome(ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]) -> None:
    def refuse(_conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        return ComputedOutcome("refused", {"reasons": ["short_sample"]}, None)

    result = hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=refuse)
    assert isinstance(result, HuntOutcome) and result.status == "refused" and result.active_series is None
    assert _outcomes(ebull_test_conn) == 1


def test_a_candidate_is_owned_by_the_hunt_that_registered_it(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=_ok)
    result = hh.evaluate(ebull_test_conn, _spec(bound, hunt_id="hunt-2"), registered_by="test", compute=_ok)
    assert isinstance(result, HuntRefused) and result.reason == "candidate_owned_by_other_hunt"
    assert len(_rows(ebull_test_conn)) == 1


def test_a_universe_change_during_compute_is_an_infrastructure_error(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def moved(conn: psycopg.Connection[Any], spec: TrialSpec) -> ComputedOutcome:
        monkeypatch.setattr(hh, "read_universe_identity", lambda _c, _u: universe_identity(archive_sha256="9" * 64))
        return _ok(conn, spec)

    with pytest.raises(hh.HuntHarnessError, match="universe identity changed"):
        hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=moved)
    assert len(_rows(ebull_test_conn)) == 1
    assert _outcomes(ebull_test_conn) == 0


# --- refusals before registration -----------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "patch", "reason"),
    [
        ({"lane": "stock_cfd_long_x1"}, None, "unpriced_lane"),
        ({"h": 1, "entry_point": "close"}, None, "refused_timeline"),
        ({}, ("HUNT_CLOSED", {"hunt-1": "no demonstrated edge"}), "hunt_closed"),
        ({"split": "validation"}, None, "door_unavailable"),
        ({"split": "holdout"}, None, "door_unavailable"),
        ({}, ("HUNT_BUDGETS", {}), "no_budget"),
        ({"signal_code_sha256": "b" * 64}, None, "identity_mismatch"),
        ({"calendar_identity": "stale"}, None, "identity_mismatch"),
        ({"harness_model_id": "hunt-harness-v0"}, None, "identity_mismatch"),
        ({"universe_identity": universe_identity(archive_sha256="8" * 64)}, None, "identity_mismatch"),
    ],
)
def test_a_refusal_before_registration_writes_nothing(
    ebull_test_conn: psycopg.Connection[Any],
    bound: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
    patch: tuple[str, Any] | None,
    reason: str,
) -> None:
    if patch is not None:
        monkeypatch.setattr(hh, patch[0], patch[1])
    result = hh.evaluate(ebull_test_conn, _spec(bound, **overrides), registered_by="test", compute=_ok)
    assert isinstance(result, HuntRefused) and result.reason == reason, result
    assert _rows(ebull_test_conn) == []


def test_a_frozen_validation_declaration_closes_discovery(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hh, "_SELECT_VALIDATION_FROZEN", "SELECT 1 WHERE %(strategy_id)s = 'hunt-1-validation'")
    result = hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=_ok)
    assert isinstance(result, HuntRefused) and result.reason == "discovery_closed"
    assert _rows(ebull_test_conn) == []


def test_the_discovery_closed_query_reads_the_declarations_table(ebull_test_conn: psycopg.Connection[Any]) -> None:
    assert (
        ebull_test_conn.execute(hh._SELECT_VALIDATION_FROZEN, {"strategy_id": "hunt-1-validation"}).fetchone() is None
    )


# --- recorded looks -------------------------------------------------------------------------


def test_a_recorded_look_burns_its_candidate_and_is_idempotent(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    spec = _spec(bound)
    first = hh.record_outside_look(ebull_test_conn, note="notebook 2026-09-26 panel 1", registered_by="test", spec=spec)
    again = hh.record_outside_look(ebull_test_conn, note="notebook 2026-09-26 panel 1", registered_by="test", spec=spec)
    assert first == again
    result = hh.evaluate(ebull_test_conn, spec, registered_by="test", compute=_ok)
    assert isinstance(result, HuntRefused) and result.reason == "split_burned"
    # Another candidate in the same split is untouched.
    assert isinstance(hh.evaluate(ebull_test_conn, _spec(bound, lag=2), registered_by="test", compute=_ok), HuntOutcome)


def test_an_unreconstructed_look_burns_the_whole_hunt_split(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    hh.record_outside_look(
        ebull_test_conn,
        note="shell session, variants unknown",
        registered_by="test",
        hunt_id="hunt-1",
        family="overnight_intraday",
        split="discovery",
        floor=True,
    )
    result = hh.evaluate(ebull_test_conn, _spec(bound, lag=3), registered_by="test", compute=_ok)
    assert isinstance(result, HuntRefused) and result.reason == "split_burned"
    other_hunt = hh.evaluate(ebull_test_conn, _spec(bound, hunt_id="hunt-2"), registered_by="test", compute=_ok)
    assert isinstance(other_hunt, HuntOutcome)


def test_recorded_looks_count_in_the_budget(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hh, "HUNT_BUDGETS", {"hunt-1": 1})
    hh.record_outside_look(ebull_test_conn, note="chart A variant 1", registered_by="test", spec=_spec(bound, lag=4))
    result = hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=_ok)
    assert isinstance(result, HuntRefused) and result.reason == "budget_exhausted"


def test_a_note_cannot_record_two_different_looks(
    ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]
) -> None:
    hh.record_outside_look(ebull_test_conn, note="chart B", registered_by="test", spec=_spec(bound))
    with pytest.raises(hh.HuntHarnessError, match="different look"):
        hh.record_outside_look(ebull_test_conn, note="chart B", registered_by="test", spec=_spec(bound, lag=5))


def test_a_note_cannot_change_its_floor_flag(ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]) -> None:
    hh.record_outside_look(ebull_test_conn, note="chart C", registered_by="test", spec=_spec(bound))
    with pytest.raises(hh.HuntHarnessError, match="different look"):
        hh.record_outside_look(ebull_test_conn, note="chart C", registered_by="test", spec=_spec(bound), floor=True)


# --- the lock -------------------------------------------------------------------------------


def test_the_programme_lock_is_not_reentrant_and_refuses_an_open_transaction(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    with hh.hunt_programme_lock(ebull_test_conn):
        with pytest.raises(hh.HuntHarnessError, match="not re-entrant"):
            with hh.hunt_programme_lock(ebull_test_conn):
                pass
    ebull_test_conn.execute("SELECT 1")
    with pytest.raises(hh.HuntHarnessError, match="open transaction"):
        with hh.hunt_programme_lock(ebull_test_conn):
            pass
    ebull_test_conn.rollback()


def test_the_programme_lock_is_released_after_a_crash(ebull_test_conn: psycopg.Connection[Any]) -> None:
    with pytest.raises(_PriceReadRaised):
        with hh.hunt_programme_lock(ebull_test_conn):
            raise _PriceReadRaised
    with psycopg.connect(ebull_test_conn.info.dsn, password=ebull_test_conn.info.password) as other:
        row = other.execute("SELECT pg_try_advisory_lock(%s, %s)", hh.HUNT_PROGRAMME_LOCK).fetchone()
        assert row == (True,)
        other.execute("SELECT pg_advisory_unlock(%s, %s)", hh.HUNT_PROGRAMME_LOCK)


# --- SQL mechanisms -------------------------------------------------------------------------


def test_both_tables_are_append_only(ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]) -> None:
    hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=_ok)
    for statement in (
        "UPDATE hunt_trials SET note = 'x'",
        "DELETE FROM hunt_trials",
        "UPDATE hunt_trial_outcomes SET status = 'abandoned'",
        "DELETE FROM hunt_trial_outcomes",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            ebull_test_conn.execute(statement)
        ebull_test_conn.rollback()


def _insert(conn: psycopg.Connection[Any], trial: TrialSpec, **overrides: Any) -> None:
    params: dict[str, Any] = {
        "hunt_id": trial.hunt_id,
        "family": trial.family,
        "split": trial.split,
        "candidate": trial.candidate_sha256,
        "spec_sha256": trial.spec_sha256,
        "spec": hh.dumps_form(trial.form()),
        "harness_model_id": trial.harness_model_id,
        "registered_by": "test",
        "purpose": "evaluate",
        "floor": False,
        "note": None,
    }
    params.update(overrides)
    conn.execute(hh._INSERT_TRIAL.replace("ON CONFLICT DO NOTHING", ""), params)


def test_one_evaluate_row_per_candidate_and_split(ebull_test_conn: psycopg.Connection[Any]) -> None:
    spec = trial_spec()
    _insert(ebull_test_conn, spec)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert(ebull_test_conn, spec)
    ebull_test_conn.rollback()
    # A recorded look of the same candidate is a separate row (it burns the split instead).
    _insert(ebull_test_conn, spec)
    _insert(ebull_test_conn, spec, purpose="recorded_after", note="look 1")
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert(ebull_test_conn, spec, purpose="recorded_after", note="look 1")
    ebull_test_conn.rollback()


@pytest.mark.parametrize(
    "overrides",
    [
        {"family": "other_family"},
        {"split": "validation"},
        {"hunt_id": "hunt-9"},
        {"harness_model_id": "other"},
        {"spec": None},
        {"floor": True},
        {"purpose": "recorded_after"},
        {"candidate": "nothex"},
    ],
)
def test_the_row_checks_refuse_a_disagreeing_or_incomplete_row(
    ebull_test_conn: psycopg.Connection[Any], overrides: dict[str, Any]
) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        _insert(ebull_test_conn, trial_spec(), **overrides)
    ebull_test_conn.rollback()


def test_a_stored_spec_rehashes_to_its_columns(ebull_test_conn: psycopg.Connection[Any], bound: dict[str, Any]) -> None:
    hh.evaluate(ebull_test_conn, _spec(bound), registered_by="test", compute=_ok)
    row = ebull_test_conn.execute(
        "SELECT hunt_id, family, split, candidate_sha256, spec_sha256, spec, harness_model_id, note FROM hunt_trials"
    ).fetchone()
    ebull_test_conn.commit()
    assert row is not None
    names = ("hunt_id", "family", "split", "candidate_sha256", "spec_sha256", "spec", "harness_model_id", "note")
    assert hh.verify_trial_row(**dict(zip(names, row, strict=True))) == []


def _outcome_params(trial_id: int, **overrides: Any) -> dict[str, Any]:
    params: dict[str, Any] = {
        "trial": trial_id,
        "status": "computed",
        "statistics": json.dumps({}),
        "active_series": None,
        "access_id": None,
        "declaration_sha256": None,
        "outcome": "0" * 64,
    }
    params.update(overrides)
    return params


_RAW_OUTCOME = """
    INSERT INTO hunt_trial_outcomes (hunt_trial_id, status, statistics, active_series, access_id,
                                     declaration_sha256, outcome_sha256)
    VALUES (%(trial)s, %(status)s, %(statistics)s::jsonb, %(active_series)s::jsonb, %(access_id)s,
            %(declaration_sha256)s, %(outcome)s)
"""


def _trial_id(conn: psycopg.Connection[Any], purpose: str) -> int:
    row = conn.execute("SELECT hunt_trial_id FROM hunt_trials WHERE purpose = %s", (purpose,)).fetchone()
    assert row is not None
    return int(row[0])


def _access(conn: psycopg.Connection[Any], strategy_id: str) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_holdout_accesses (strategy_id, strategy_version, access_kind, accessed_by, purpose)
        VALUES (%s, 'v1', 'read', 'test', 'test') RETURNING access_id
        """,
        (strategy_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_the_outcome_trigger_binds_trial_purpose_split_and_access(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    discovery = trial_spec()
    _insert(conn, discovery)
    _insert(conn, trial_spec(lag=2), purpose="recorded_after", note="look")
    validation = trial_spec(split="validation")
    _insert(conn, validation)
    conn.commit()
    evaluate_id = _trial_id(conn, "evaluate")
    recorded_id = _trial_id(conn, "recorded_after")
    row = conn.execute("SELECT hunt_trial_id FROM hunt_trials WHERE split = 'validation'").fetchone()
    assert row is not None
    validation_id = int(row[0])
    wrong_access = _access(conn, "hunt-2-validation")
    right_access = _access(conn, "hunt-1-validation")
    conn.commit()

    refused = [
        (_outcome_params(recorded_id), "not an evaluate registration"),
        (_outcome_params(evaluate_id, access_id=right_access, declaration_sha256="1" * 64), "carries a door access"),
        (_outcome_params(validation_id), "has no door access"),
        (
            _outcome_params(validation_id, access_id=wrong_access, declaration_sha256="1" * 64),
            "is for hunt-2-validation",
        ),
    ]
    for params, message in refused:
        with pytest.raises(psycopg.errors.RaiseException, match=message):
            conn.execute(_RAW_OUTCOME, params)
        conn.rollback()
    conn.execute(_RAW_OUTCOME, _outcome_params(validation_id, access_id=right_access, declaration_sha256="1" * 64))
    conn.execute(_RAW_OUTCOME, _outcome_params(evaluate_id))
    conn.commit()
