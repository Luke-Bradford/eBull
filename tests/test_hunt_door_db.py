"""#3385 slice 2b-i — the validation door against a real database.

The discovery log is seeded through ``hunt_harness.evaluate`` itself (compute stubbed), so
every row the freeze reads went through the real registration path. One test per new
mechanism: ``sql/428``'s triggers; the freeze (tripwire, completeness, recomputed numbers,
the #2599 row and the document in one transaction); the membership refusal audited
through ``sql/340`` with no registration; and the access row committed before compute.
"""

from __future__ import annotations

import dataclasses
import math
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
import pytest

from app.services import hunt_door, result_ledger
from app.services import hunt_harness as hh
from app.services.hunt_compute import CANONICAL_CELL
from app.services.hunt_harness import ComputedOutcome, HuntOutcome, HuntRefused, TrialSpec
from app.services.trial_register import (
    DeclaredTrial,
    TrialExactness,
    TrialRegister,
    declaration_backed_evidence,
)
from tests.test_hunt_harness_db import _spec, bound  # noqa: F401 - the fixture is used by name

DOC_PATH = "hunts/hunt-1-validation.json"
DISCOVERY_LAGS = tuple(range(1, 13))
#: The one candidate the stub gives a decisive p; BY flags it and nothing else.
WINNER_LAG = 3
INHERITED = DeclaredTrial(
    trial_id="legacy-session",
    description="an inherited search family",
    evidence="test fixture",
    exactness=TrialExactness.EXACT,
    searches=5,
)


def _discovery_outcome(lag: int) -> ComputedOutcome:
    p = 0.0 if lag == WINNER_LAG else 0.5
    series = tuple(0.001 * math.sin(0.3 * lag * i + lag) + 0.0001 * lag for i in range(60))
    cells = {CANONICAL_CELL: {"p": p, "t_stat": 4.0 if p == 0.0 else 0.1}}
    return ComputedOutcome("computed", {"cells": cells}, series)


def _evaluate(conn: psycopg.Connection[Any], spec: TrialSpec, outcome: ComputedOutcome) -> Any:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(hh, "compute_trial", lambda _conn, _spec: outcome)
        return hh.evaluate(conn, spec, registered_by="test")


@pytest.fixture
def discovered(
    ebull_test_conn: psycopg.Connection[Any],
    bound: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> dict[str, Any]:
    """hunt-1's discovery, complete, and its document written under a tmp repo root."""
    monkeypatch.setattr(hh, "HUNT_BUDGETS", {"hunt-1": 50, "hunt-2": 50})
    monkeypatch.setattr(hunt_door, "REPO_ROOT", tmp_path)
    for lag in DISCOVERY_LAGS:
        result = _evaluate(ebull_test_conn, _spec(bound, lag=lag), _discovery_outcome(lag))
        assert isinstance(result, HuntOutcome), result
    pinned = _spec(bound, lag=WINNER_LAG, split="validation")
    base = TrialRegister(version="test-r1", trials=(INHERITED,))
    doc, codes = hunt_door.build_validation_declaration(ebull_test_conn, hunt_id="hunt-1", pins=[pinned], register=base)
    assert codes == []
    (tmp_path / "hunts").mkdir()
    sha = hunt_door.write_declaration(tmp_path / DOC_PATH, doc)
    return {"pinned": pinned, "doc": doc, "sha": sha, "tmp": tmp_path}


def _register(conn: psycopg.Connection[Any], sha: str, *, pins: int = 1) -> TrialRegister:
    evidence, exactness, searches = hunt_door.discovery_register_evidence(conn, "hunt-1")
    return TrialRegister(
        version="test-r2",
        trials=(
            INHERITED,
            DeclaredTrial("hunt-1-discovery", "hunt-1 discovery", evidence, exactness, searches),
            DeclaredTrial(
                "hunt-1-validation",
                "hunt-1 validation batch",
                declaration_backed_evidence(declaration_path=DOC_PATH, declaration_sha256=sha, pinned_specs=pins),
                TrialExactness.EXACT,
                pins,
                declared_for=("hunt-1-validation", "v1"),
            ),
        ),
    )


def _freeze(conn: psycopg.Connection[Any], register: TrialRegister, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(result_ledger, "TRIAL_REGISTER", register)
    return hunt_door.freeze_validation_declaration(conn, doc_path=DOC_PATH, declared_by="test", register=register)


def _count(conn: psycopg.Connection[Any], sql: LiteralString) -> int:
    row = conn.execute(sql).fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


# --- the document's numbers -----------------------------------------------------------------


def test_the_document_pins_m_v_by_and_power(discovered: dict[str, Any]) -> None:
    numbers = discovered["doc"]["numbers"]
    # M = M_inh (5) + 12 discovery rows + the 1 pinned spec not yet evaluated.
    assert (numbers["m"], numbers["reserved_pins"], numbers["m_is_floor"]) == (18, 1, False)
    assert numbers["by"]["m"] == 5 + len(DISCOVERY_LAGS)
    (pin,) = discovered["doc"]["pins"]
    assert numbers["by"]["flagged"] == [pin["discovery_hunt_trial_id"]]
    assert len(numbers["v_sr"]["trial_ids"]) == len(DISCOVERY_LAGS)
    power = numbers["power"][discovered["pinned"].spec_sha256]
    assert "blocked" not in power and power["clears_short_sample"] is True


# --- the freeze -----------------------------------------------------------------------------


def test_the_freeze_writes_the_declaration_and_its_document_together(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    declaration_id = _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    stored = hh.load_hunt_declaration(ebull_test_conn, declaration_id)
    frozen = result_ledger.load_preregistration(ebull_test_conn, "hunt-1-validation", "v1")
    ebull_test_conn.commit()
    assert stored is not None and frozen is not None
    assert stored.doc_sha256 == discovered["sha"]
    assert frozen.declaration.contract_version == hh.declaration_contract_version(discovered["sha"])
    # A #2634 successor carries no document of its own: the chain resolves to the root's.
    successor = dataclasses.replace(frozen, declaration_id=10**9, chain_declaration_ids=(declaration_id, 10**9))
    chained = hh.load_chain_hunt_declaration(ebull_test_conn, successor)
    ebull_test_conn.commit()
    assert chained is not None and chained.declaration_id == declaration_id
    # Freezing closes the hunt's discovery.
    closed = _evaluate(ebull_test_conn, _spec(discovered_bound(discovered), lag=20), _discovery_outcome(20))
    assert isinstance(closed, HuntRefused) and closed.reason == "discovery_closed"
    # sql/428: append-only, and a document cannot ride on another trial's declaration.
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        ebull_test_conn.execute("UPDATE hunt_declarations SET doc_path = 'x'")
    ebull_test_conn.rollback()
    foreign = {**discovered["doc"], "hunt_id": "hunt-2"}
    with pytest.raises(psycopg.errors.RaiseException, match="not hunt-2-validation/v1"):
        ebull_test_conn.execute(
            "INSERT INTO hunt_declarations (declaration_id, hunt_id, split, doc_path, doc, doc_sha256, "
            "register_version) VALUES (%s, 'hunt-2', 'validation', 'p', %s::jsonb, %s, 'r')",
            (declaration_id, hh.dumps_form(foreign), hh.sha256_form(foreign)),
        )
    ebull_test_conn.rollback()


def discovered_bound(discovered: dict[str, Any]) -> dict[str, Any]:
    spec: TrialSpec = discovered["pinned"]
    return {
        "signal_code_sha256": spec.signal_code_sha256,
        "cost_model_id": spec.cost_model_id,
        "calendar_identity": spec.calendar_identity,
    }


def test_a_stale_document_refuses_and_writes_nothing(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    doc = dict(discovered["doc"])
    doc["numbers"] = {**doc["numbers"], "m": doc["numbers"]["m"] + 1}
    sha = hunt_door.write_declaration(discovered["tmp"] / DOC_PATH, doc)
    with pytest.raises(hunt_door.HuntDeclarationRefused) as refused:
        _freeze(ebull_test_conn, _register(ebull_test_conn, sha), monkeypatch)
    assert refused.value.codes == ("declaration_numbers_stale",)
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_declarations") == 0
    assert _count(ebull_test_conn, "SELECT count(*) FROM strategy_preregistration_declarations") == 0


def test_the_tripwire_refuses_a_register_that_disagrees_with_the_log(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    register = _register(ebull_test_conn, discovered["sha"], pins=2)
    # A late look after the register closed discovery: the log now has a newer row.
    hh.record_outside_look(
        ebull_test_conn,
        note="late chart look #1",
        registered_by="test",
        hunt_id="hunt-1",
        family="x",
        split="discovery",
    )
    with pytest.raises(hunt_door.HuntDeclarationRefused) as refused:
        _freeze(ebull_test_conn, register, monkeypatch)
    codes = refused.value.codes
    assert "register_disagrees_with_log:hunt-1-discovery_rows_newer_than_close" in codes
    assert "register_disagrees_with_log:hunt-1-validation_pins" in codes
    # The late look also moved M, so the document's numbers are stale too.
    assert "declaration_numbers_stale" in codes
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_declarations") == 0


def test_the_freeze_refuses_while_a_discovery_registration_lacks_an_outcome(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def crash(_conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        raise RuntimeError("price read failed")

    with pytest.MonkeyPatch.context() as patch, pytest.raises(RuntimeError):
        patch.setattr(hh, "compute_trial", crash)
        hh.evaluate(ebull_test_conn, _spec(discovered_bound(discovered), lag=30), registered_by="test")
    with pytest.raises(hunt_door.HuntDeclarationRefused) as refused:
        _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    assert any(code.startswith("discovery_registration_without_outcome:") for code in refused.value.codes)


# --- evaluate through the door --------------------------------------------------------------


def test_a_pinned_spec_passes_the_door_and_an_unpinned_one_registers_nothing(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    seen: list[tuple[int, int]] = []

    def compute(conn: psycopg.Connection[Any], _spec: TrialSpec) -> ComputedOutcome:
        # Another session sees only committed rows: the registration AND the access row.
        with psycopg.connect(conn.info.dsn, password=conn.info.password) as other:
            trials = other.execute("SELECT count(*) FROM hunt_trials WHERE split = 'validation'").fetchone()
            reads = other.execute(
                "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = 'hunt-1-validation'"
            ).fetchone()
            assert trials is not None and reads is not None
            seen.append((int(trials[0]), int(reads[0])))
        return ComputedOutcome("computed", {"cells": {}}, (0.001, 0.002))

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(hh, "compute_trial", compute)
        outcome = hh.evaluate(ebull_test_conn, discovered["pinned"], registered_by="test")
    assert isinstance(outcome, HuntOutcome) and not outcome.cached
    assert seen == [(1, 1)]
    stored = ebull_test_conn.execute(
        "SELECT access_id, declaration_sha256 FROM hunt_trial_outcomes WHERE hunt_trial_id = %s",
        (outcome.hunt_trial_id,),
    ).fetchone()
    ebull_test_conn.commit()
    assert stored is not None and stored[0] is not None and stored[1] is not None

    # A cached validation outcome is returned only through a fresh audited access.
    again = _evaluate(ebull_test_conn, discovered["pinned"], _discovery_outcome(1))
    assert isinstance(again, HuntOutcome) and again.cached
    reads: LiteralString = "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = 'hunt-1-validation'"
    assert _count(ebull_test_conn, reads) == 2

    unpinned = _spec(discovered_bound(discovered), lag=5, split="validation")
    refused = _evaluate(ebull_test_conn, unpinned, _discovery_outcome(5))
    assert isinstance(refused, HuntRefused) and refused.reason == "spec_not_in_declaration"
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_trials WHERE split = 'validation'") == 1
    assert _count(ebull_test_conn, reads) == 2
    audited = ebull_test_conn.execute(
        "SELECT refusals FROM strategy_holdout_access_refusals WHERE strategy_id = 'hunt-1-validation'"
    ).fetchall()
    ebull_test_conn.commit()
    assert [row[0] for row in audited] == [["spec_not_in_declaration"]]


def test_a_burned_validation_candidate_cannot_be_pinned(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any]
) -> None:
    """Codex ckpt-2: a frozen pin that ``evaluate`` refuses ``split_burned`` would wedge the batch."""
    hh.record_outside_look(
        ebull_test_conn, note="validation chart look #1", registered_by="test", spec=discovered["pinned"]
    )
    base = TrialRegister(version="test-r1", trials=(INHERITED,))
    _doc, codes = hunt_door.build_validation_declaration(
        ebull_test_conn, hunt_id="hunt-1", pins=[discovered["pinned"]], register=base
    )
    assert f"pin_{discovered['pinned'].spec_sha256[:12]}_split_burned" in codes


def test_the_validation_readout_waits_for_every_pin_then_gives_verdicts(
    ebull_test_conn: psycopg.Connection[Any], discovered: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import numpy as np

    from app.services import hunt_inference
    from app.services.hunt_compute import cell_key
    from app.services.r6_exclusion_trial import PROGRAMME_POLICIES

    _freeze(ebull_test_conn, _register(ebull_test_conn, discovered["sha"]), monkeypatch)
    pending = hunt_door.validation_readout(ebull_test_conn, "hunt-1")
    assert not pending.complete and pending.closure is None and pending.candidates[0].verdict is None
    assert pending.labels == (hunt_door.PREVIOUSLY_EXAMINED,)

    series = tuple(float(v) + 0.01 for v in np.random.default_rng(7).normal(0.0, 0.01, 1500))
    cell = hunt_inference.cell_statistics(series, h=1, entered_formations=range(1500))
    assert isinstance(cell, hunt_inference.CellStatistics)
    cells = {
        cell_key(policy.label, dividends, cost): cell.form()
        for policy in PROGRAMME_POLICIES
        for dividends in (True, False)
        for cost in ("base", "stress")
    }
    grid = hh.split_grid("validation", lag=discovered["pinned"].lag, h=discovered["pinned"].h)
    assert not isinstance(grid, hunt_inference.StatRefused)
    first_session = hh.split_sessions("validation")[grid.first].isoformat()
    statistics = {"cells": cells, "grid": {"first_session": first_session}}
    result = _evaluate(ebull_test_conn, discovered["pinned"], ComputedOutcome("computed", statistics, series))
    # A fresh outcome reports the provenance it was stored with (Codex ckpt-2).
    assert isinstance(result, HuntOutcome) and result.access_id is not None
    assert result.spec_sha256 == discovered["pinned"].spec_sha256 and result.declaration_sha256 is not None
    # A late look in ANOTHER hunt still qualifies hunt-1's readout: the frozen M counts every
    # hunt's rows, so any look after the freeze makes it an under-count (decision 114).
    hh.record_outside_look(
        ebull_test_conn,
        note="late look after freeze",
        registered_by="test",
        hunt_id="hunt-2",
        family="x",
        split="discovery",
    )
    reads: LiteralString = "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = 'hunt-1-validation'"
    before = _count(ebull_test_conn, reads)
    readout = hunt_door.validation_readout(ebull_test_conn, "hunt-1")
    (candidate,) = readout.candidates
    # t clears the bar, but the stub discovery Sharpes are widely spread, so V[SR] is large
    # and the DSR (deflated against the FROZEN M and V) does not: underpowered, not "no edge".
    assert readout.complete and candidate.verdict is hunt_inference.Verdict.UNDETERMINED
    assert all(isinstance(value, float) and value < hunt_inference.DSR_BAR for value in candidate.dsr.values())
    assert readout.closure is hunt_inference.HuntClosure.NO_DEMONSTRATED_EDGE
    assert readout.labels == (hunt_door.PREVIOUSLY_EXAMINED, hunt_door.QUALIFIED_BY_LATE_LOOK)
    counts = [candidate.survivor_subspans[side]["sessions"] for side in ("before", "on_or_after")]
    assert all(isinstance(count, int) and count > 0 for count in counts)
    assert sum(cast(list[int], counts)) == len(series)
    # The readout verifies stored provenance and opens no fresh access.
    assert _count(ebull_test_conn, reads) == before
