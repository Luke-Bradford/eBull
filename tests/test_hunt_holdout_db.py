"""#3385 slice 2b-iii — the holdout declaration, the release barrier and the holdout readout.

Seeded through the real path: discovery and validation outcomes go through
``hunt_harness.evaluate`` (compute stubbed) and the validation declaration is frozen by
``freeze_validation_declaration``, so the holdout freeze reads validation verdicts the
way production will. Obligations 132-133 (holdout M/V, the no-PASS path) and 149 (a
cached holdout releases nothing).
"""

from __future__ import annotations

from datetime import date
from typing import Any, LiteralString

import numpy as np
import psycopg
import pytest

from app.services import hunt_door, hunt_inference, result_ledger
from app.services import hunt_harness as hh
from app.services.hunt_compute import cell_key
from app.services.hunt_harness import ComputedOutcome, HoldoutRecorded, HuntOutcome, HuntRefused, TrialSpec
from app.services.r6_exclusion_trial import PROGRAMME_POLICIES
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister, declaration_backed_evidence
from tests.test_hunt_door_db import (  # noqa: F401 - fixtures are used by name
    INHERITED,
    _count,
    _evaluate,
    _freeze,
    _register,
    bound,
    discovered,
    discovered_bound,
)
from tests.test_hunt_harness_db import _spec

HOLDOUT_PATH = "hunts/hunt-1-holdout.json"
END = date(2024, 9, 27)
HOLDOUT_READS: LiteralString = "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = 'hunt-1-holdout'"


def _statistics(split: hh.Split, spec: TrialSpec, *, mean: float, n: int) -> tuple[dict[str, Any], tuple[float, ...]]:
    """Every base and stress cell from one series of mean ``mean``; a large mean PASSes."""
    series = tuple(float(v) + mean for v in np.random.default_rng(7).normal(0.0, 0.01, n))
    cell = hunt_inference.cell_statistics(series, h=1, entered_formations=range(n))
    assert isinstance(cell, hunt_inference.CellStatistics)
    cells = {
        cell_key(policy.label, dividends, cost): cell.form()
        for policy in PROGRAMME_POLICIES
        for dividends in (True, False)
        for cost in ("base", "stress")
    }
    end = END if split == "holdout" else None
    grid = hh.split_grid(split, lag=spec.lag, h=spec.h, end=end)
    assert not isinstance(grid, hunt_inference.StatRefused)
    first_session = hh.split_sessions(split, end=end)[grid.first].isoformat()
    return {"cells": cells, "grid": {"first_session": first_session}}, series


def _validate(conn: psycopg.Connection[Any], seeded: dict[str, Any], *, mean: float) -> None:
    statistics, series = _statistics("validation", seeded["pinned"], mean=mean, n=1500)
    result = _evaluate(conn, seeded["pinned"], ComputedOutcome("computed", statistics, series))
    assert isinstance(result, HuntOutcome)


@pytest.fixture
def validated(
    ebull_test_conn: psycopg.Connection[Any],
    discovered: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """hunt-1's validation frozen and its one pin evaluated to a PASS."""
    _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    _validate(ebull_test_conn, discovered, mean=0.03)
    readout = hunt_door.validation_readout(ebull_test_conn, "hunt-1")
    assert [c.verdict for c in readout.candidates] == [hunt_inference.Verdict.PASS]
    return discovered


def _holdout_register(conn: psycopg.Connection[Any], validated: dict[str, Any], sha: str) -> TrialRegister:
    base = _register(conn, validated["sha"])
    entry = DeclaredTrial(
        "hunt-1-holdout",
        "hunt-1 holdout",
        declaration_backed_evidence(declaration_path=HOLDOUT_PATH, declaration_sha256=sha, pinned_specs=1),
        TrialExactness.EXACT,
        1,
        declared_for=("hunt-1-holdout", "v1"),
    )
    return TrialRegister(version="test-r3", trials=(*base.trials, entry))


def _freeze_holdout(conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> Any:
    # Only M_inh is read from the register when building; the freeze re-reads the full one.
    base = TrialRegister(version="test-r2", trials=(INHERITED,))
    doc, codes = hunt_door.build_holdout_declaration(conn, hunt_id="hunt-1", end_session=END, register=base)
    assert codes == []
    sha = hunt_door.write_declaration(validated["tmp"] / HOLDOUT_PATH, doc)
    register = _holdout_register(conn, validated, sha)
    monkeypatch.setattr(result_ledger, "TRIAL_REGISTER", register)
    hunt_door.freeze_holdout_declaration(conn, doc_path=HOLDOUT_PATH, declared_by="test", register=register)
    return doc


# --- the declaration ------------------------------------------------------------------------


def test_the_holdout_pins_the_validation_pass_with_validation_v_and_a_fresh_m(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any]
) -> None:
    register = TrialRegister(version="test-r2", trials=(INHERITED,))
    doc, codes = hunt_door.build_holdout_declaration(
        ebull_test_conn, hunt_id="hunt-1", end_session=END, register=register
    )
    assert codes == []
    (pin,) = doc["pins"]
    holdout = TrialSpec.from_form(pin["spec"])
    assert (holdout.split, holdout.candidate_sha256) == ("holdout", validated["pinned"].candidate_sha256)
    assert pin["validation_spec_sha256"] == validated["pinned"].spec_sha256
    numbers = doc["numbers"]
    # M = M_inh (5) + 12 discovery rows + 1 validation row + the holdout pin, reserved.
    assert (numbers["m"], numbers["reserved_pins"]) == (19, 1)
    assert numbers["v_sr"] == validated["doc"]["numbers"]["v_sr"]
    assert "blocked" not in numbers["power"][holdout.spec_sha256]
    assert doc["end_session"] == END.isoformat()


def test_the_holdout_refuses_before_validation_completes_and_without_a_pass(
    ebull_test_conn: psycopg.Connection[Any],
    discovered: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def codes() -> list[str]:
        base = TrialRegister(version="test-r2", trials=())
        return hunt_door.build_holdout_declaration(ebull_test_conn, hunt_id="hunt-1", end_session=END, register=base)[1]

    assert codes() == ["validation_not_frozen"]
    _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    assert codes() == ["validation_incomplete"]
    # Obligation 133: a completed batch with no PASS closes the hunt; there is no holdout.
    _validate(ebull_test_conn, discovered, mean=0.01)
    assert codes() == ["no_validation_pass"]


def test_the_end_session_is_a_holdout_session_within_the_archive(validated: dict[str, Any]) -> None:
    holdout = [_spec(discovered_bound(validated), lag=validated["pinned"].lag, split="holdout")]
    assert hunt_door._end_session_codes(END, holdout) == []
    assert hunt_door._end_session_codes(date(2024, 9, 30), holdout) == [
        "end_session_after_archive_last_complete_session:2024-09-27"
    ]
    assert hunt_door._end_session_codes(date(2024, 9, 21), holdout) == ["end_session_not_a_holdout_session"]


# --- evaluate: the release barrier ----------------------------------------------------------


def test_holdout_evaluate_writes_through_the_door_and_returns_no_number(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    doc = _freeze_holdout(ebull_test_conn, validated, monkeypatch)
    pinned = TrialSpec.from_form(doc["pins"][0]["spec"])
    unpinned = _spec(discovered_bound(validated), lag=5, split="holdout")
    refused = _evaluate(ebull_test_conn, unpinned, ComputedOutcome("computed", {"cells": {}}, (0.1,)))
    assert isinstance(refused, HuntRefused) and refused.reason == "spec_not_in_declaration"

    ends: list[date | None] = []

    def compute(_conn: psycopg.Connection[Any], _spec: TrialSpec, *, end: date | None = None) -> ComputedOutcome:
        ends.append(end)
        statistics, series = _statistics("holdout", pinned, mean=0.03, n=600)
        return ComputedOutcome("computed", statistics, series)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(hh, "compute_trial", compute)
        fresh = hh.evaluate(ebull_test_conn, pinned, registered_by="test")
        # Obligation 149: a cached holdout releases nothing and opens no access.
        cached = hh.evaluate(ebull_test_conn, pinned, registered_by="test")
    assert ends == [END]
    assert isinstance(fresh, HoldoutRecorded) and not fresh.cached
    assert isinstance(cached, HoldoutRecorded) and cached.cached and cached.hunt_trial_id == fresh.hunt_trial_id
    assert _count(ebull_test_conn, HOLDOUT_READS) == 1
    stored = ebull_test_conn.execute(
        "SELECT status, access_id FROM hunt_trial_outcomes o JOIN hunt_trials t USING (hunt_trial_id) "
        "WHERE t.split = 'holdout'"
    ).fetchall()
    ebull_test_conn.commit()
    assert [(row[0], row[1] is not None) for row in stored] == [("computed", True)]


# --- the readout ----------------------------------------------------------------------------


def test_the_holdout_readout_releases_verdicts_only_when_every_pin_has_one(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    doc = _freeze_holdout(ebull_test_conn, validated, monkeypatch)
    pinned = TrialSpec.from_form(doc["pins"][0]["spec"])
    pending = hunt_door.holdout_readout(ebull_test_conn, "hunt-1")
    assert not pending.complete and pending.pending == (pinned.spec_sha256,)
    assert pending.closure is None and [c.verdict for c in pending.candidates] == [None]
    assert pending.labels == (hunt_door.CORROBORATION_PARTLY_SEEN,)

    statistics, series = _statistics("holdout", pinned, mean=0.03, n=600)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(hh, "compute_trial", lambda _conn, _spec, **_kw: ComputedOutcome("computed", statistics, series))
        assert isinstance(hh.evaluate(ebull_test_conn, pinned, registered_by="test"), HoldoutRecorded)
    before = _count(ebull_test_conn, HOLDOUT_READS)
    readout = hunt_door.holdout_readout(ebull_test_conn, "hunt-1")
    (candidate,) = readout.candidates
    assert readout.complete and readout.pending == ()
    assert candidate.verdict is hunt_inference.Verdict.PASS
    assert readout.closure is hunt_inference.HuntClosure.HOLDOUT_REPORTED
    assert readout.m == doc["numbers"]["m"] and readout.end_session == END
    assert candidate.survivor_subspans["on_or_after"]["sessions"] == len(series)
    # Stored provenance is verified; no fresh access is opened.
    assert _count(ebull_test_conn, HOLDOUT_READS) == before


def test_a_pin_refused_before_registration_reads_not_pass_refused(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    doc = _freeze_holdout(ebull_test_conn, validated, monkeypatch)
    pinned = TrialSpec.from_form(doc["pins"][0]["spec"])
    hh.record_outside_look(ebull_test_conn, note="holdout chart look #1", registered_by="test", spec=pinned)
    refused = _evaluate(ebull_test_conn, pinned, ComputedOutcome("computed", {"cells": {}}, (0.1,)))
    assert isinstance(refused, HuntRefused) and refused.reason == "split_burned"
    readout = hunt_door.holdout_readout(ebull_test_conn, "hunt-1")
    (candidate,) = readout.candidates
    assert readout.complete and candidate.hunt_trial_id is None
    assert candidate.verdict is hunt_inference.Verdict.NOT_PASS_REFUSED
    assert candidate.reasons[0].startswith("refused before registration: split_burned")


def test_the_holdout_freeze_refuses_a_second_holdout(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _freeze_holdout(ebull_test_conn, validated, monkeypatch)
    base = TrialRegister(version="test-r3", trials=())
    _doc, codes = hunt_door.build_holdout_declaration(ebull_test_conn, hunt_id="hunt-1", end_session=END, register=base)
    assert "holdout_already_frozen" in codes
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_declarations WHERE split = 'holdout'") == 1


def test_a_relabelled_spec_cannot_ride_a_cached_holdout(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex ckpt-2: same candidate, other ``spec_sha256`` is unpinned, cached or not."""
    doc = _freeze_holdout(ebull_test_conn, validated, monkeypatch)
    pinned = TrialSpec.from_form(doc["pins"][0]["spec"])
    statistics, series = _statistics("holdout", pinned, mean=0.03, n=600)
    assert isinstance(
        _evaluate(ebull_test_conn, pinned, ComputedOutcome("computed", statistics, series)), HoldoutRecorded
    )
    relabelled = TrialSpec.from_form({**pinned.form(), "family": "relabelled"})
    assert relabelled.candidate_sha256 == pinned.candidate_sha256
    refused = _evaluate(ebull_test_conn, relabelled, ComputedOutcome("computed", statistics, series))
    assert isinstance(refused, HuntRefused) and refused.reason == "spec_not_in_declaration"
    assert _count(ebull_test_conn, HOLDOUT_READS) == 1


def test_a_registered_pin_whose_retry_is_refused_does_not_wedge_the_readout(
    ebull_test_conn: psycopg.Connection[Any], validated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex ckpt-2: a crash after registration, then a burn, would leave the pin pending forever."""
    doc = _freeze_holdout(ebull_test_conn, validated, monkeypatch)
    pinned = TrialSpec.from_form(doc["pins"][0]["spec"])

    def crash(_conn: psycopg.Connection[Any], _spec: TrialSpec, **_kw: Any) -> ComputedOutcome:
        raise RuntimeError("price read failed")

    with pytest.MonkeyPatch.context() as patch, pytest.raises(RuntimeError):
        patch.setattr(hh, "compute_trial", crash)
        hh.evaluate(ebull_test_conn, pinned, registered_by="test")
    assert hunt_door.holdout_readout(ebull_test_conn, "hunt-1").pending == (pinned.spec_sha256,)
    hh.record_outside_look(ebull_test_conn, note="holdout chart look #2", registered_by="test", spec=pinned)
    readout = hunt_door.holdout_readout(ebull_test_conn, "hunt-1")
    (candidate,) = readout.candidates
    assert readout.complete and candidate.hunt_trial_id is not None
    assert candidate.verdict is hunt_inference.Verdict.NOT_PASS_REFUSED
    assert candidate.reasons[0].startswith("registered, its retry refused: split_burned")
