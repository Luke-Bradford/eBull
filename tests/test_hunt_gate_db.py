"""#3454 slice B — a gated hunt's terminal refusal and the lineage closure, against a real database.

One test per new mechanism: ``sql/431``'s append-only terminal row written by a substantive
FREEZE refusal; the same row written by a stale-pin LOOK and READOUT refusal, after which
``evaluate`` refuses ``hunt_terminal``; and ``lineage_closed`` keyed on a validation-split row.
The fixture ids reuse ``hunt-1`` (gated here by monkeypatch), as the door's DB tests do.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import psycopg
import pytest

from app.services import hunt_door, hunt_gate
from app.services import hunt_harness as hh
from app.services.hunt_harness import ComputedOutcome, HuntOutcome, HuntRefused, TrialSpec
from app.services.trial_register import TrialRegister
from tests.test_hunt_door_db import DOC_PATH, INHERITED, _count, _evaluate, _freeze, _register
from tests.test_hunt_gate import gate_statistics
from tests.test_hunt_harness_db import _spec, bound  # noqa: F401 - the fixture is used by name

SERIES = tuple(0.001 * math.sin(0.9 * i) + 0.0003 for i in range(60))


@pytest.fixture
def gated(
    ebull_test_conn: psycopg.Connection[Any],
    bound: dict[str, Any],  # noqa: F811 - the imported fixture, requested by name
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> dict[str, Any]:
    monkeypatch.setattr(hh, "HUNT_BUDGETS", {"hunt-1": 1})
    monkeypatch.setattr(hunt_door, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(hunt_gate, "GATED_HUNTS", frozenset({"hunt-1"}))
    (tmp_path / "hunts").mkdir()
    return {"bound": bound, "tmp": tmp_path}


def _discover_and_write(
    conn: psycopg.Connection[Any], gated: dict[str, Any], **statistics: Any
) -> tuple[TrialSpec, str, list[str]]:
    result = _evaluate(
        conn, _spec(gated["bound"], lag=3), ComputedOutcome("computed", gate_statistics(**statistics), SERIES)
    )
    assert isinstance(result, HuntOutcome), result
    pinned = _spec(gated["bound"], lag=3, split="validation")
    base = TrialRegister(version="test-r1", trials=(INHERITED,))
    doc, codes = hunt_door.build_validation_declaration(conn, hunt_id="hunt-1", pins=[pinned], register=base)
    return pinned, hunt_door.write_declaration(gated["tmp"] / DOC_PATH, doc), codes


def _terminal(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    rows = conn.execute("SELECT hunt_id, lineage, reason FROM hunt_terminal ORDER BY hunt_terminal_id").fetchall()
    conn.commit()
    return rows


def test_a_substantive_freeze_refusal_is_terminal(
    ebull_test_conn: psycopg.Connection[Any], gated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _pinned, sha, codes = _discover_and_write(ebull_test_conn, gated, excess=0.05)
    assert codes == ["hunt_gate_condition_not_met:excess_at_bar"]
    with pytest.raises(hunt_door.HuntDeclarationRefused) as refused:
        _freeze(ebull_test_conn, _register(ebull_test_conn, sha), monkeypatch)
    assert "hunt_gate_condition_not_met:excess_at_bar" in refused.value.codes
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_declarations") == 0
    ((hunt_id, lineage, reason),) = _terminal(ebull_test_conn)
    assert (hunt_id, lineage) == ("hunt-1", None) and "excess_at_bar" in reason
    # A retry cannot re-open it: the freeze and every evaluate of the hunt now refuse.
    with pytest.raises(hunt_door.HuntDeclarationRefused) as again:
        _freeze(ebull_test_conn, _register(ebull_test_conn, sha), monkeypatch)
    assert "hunt_terminal" in again.value.codes and len(_terminal(ebull_test_conn)) == 1
    later = _evaluate(ebull_test_conn, _spec(gated["bound"], lag=4), ComputedOutcome("computed", {}, SERIES))
    assert isinstance(later, HuntRefused) and later.reason == "hunt_terminal"
    # sql/431: append-only.
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        ebull_test_conn.execute("DELETE FROM hunt_terminal")
    ebull_test_conn.rollback()


def _frozen_then_moved(
    conn: psycopg.Connection[Any], gated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> tuple[TrialSpec, dict[str, str]]:
    pinned, sha, codes = _discover_and_write(conn, gated)
    assert codes == []
    _freeze(conn, _register(conn, sha), monkeypatch)
    return pinned, {**hunt_gate.decision_pins(), "app/services/hunt_door.py": "0" * 64}


def test_a_stale_decision_pin_after_the_freeze_refuses_the_look_terminally(
    ebull_test_conn: psycopg.Connection[Any], gated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned, moved = _frozen_then_moved(ebull_test_conn, gated, monkeypatch)
    with monkeypatch.context() as patch:
        patch.setattr(hunt_gate, "decision_pins", lambda _root=None: moved)
        look = _evaluate(ebull_test_conn, pinned, ComputedOutcome("computed", gate_statistics(), SERIES))
    assert isinstance(look, HuntRefused) and look.reason == "door_refused"
    assert "hunt_gate_pin_stale:app/services/hunt_door.py" in look.detail
    # The refused look registered nothing; the terminal row is its only trace.
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_trials WHERE split = 'validation'") == 0
    ((_hunt, _lineage, reason),) = _terminal(ebull_test_conn)
    assert reason.startswith("validation look refused: hunt_gate_pin_stale")
    # With the code restored, the hunt stays closed: no look, and no readout (Codex ckpt-2).
    retry = _evaluate(ebull_test_conn, pinned, ComputedOutcome("computed", gate_statistics(), SERIES))
    assert isinstance(retry, HuntRefused) and retry.reason == "hunt_terminal"
    with pytest.raises(hunt_door.HuntDeclarationRefused) as readout:
        hunt_door.validation_readout(ebull_test_conn, "hunt-1")
    assert readout.value.codes == ("hunt_terminal",)
    assert len(_terminal(ebull_test_conn)) == 1


def test_a_stale_decision_pin_refuses_the_readout_terminally(
    ebull_test_conn: psycopg.Connection[Any], gated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _pinned, moved = _frozen_then_moved(ebull_test_conn, gated, monkeypatch)
    with monkeypatch.context() as patch:
        patch.setattr(hunt_gate, "decision_pins", lambda _root=None: moved)
        with pytest.raises(hunt_door.HuntDeclarationRefused) as refused:
            hunt_door.validation_readout(ebull_test_conn, "hunt-1")
    assert refused.value.codes == ("hunt_gate_pin_stale:app/services/hunt_door.py",)
    ((_hunt, _lineage, reason),) = _terminal(ebull_test_conn)
    assert reason.startswith("validation readout refused: hunt_gate_pin_stale")


def test_a_validation_row_closes_its_lineage_for_every_other_spec(
    ebull_test_conn: psycopg.Connection[Any], gated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned: TrialSpec = _spec(gated["bound"], lag=3, split="validation")
    monkeypatch.setattr(hh, "LAST_LOOK_LINEAGES", {pinned.family: frozenset({pinned.signal_code_sha256})})
    monkeypatch.setattr(hh, "HUNT_BUDGETS", {"hunt-1": 1, "hunt-2": 1})
    # Any validation-split row counts, a look recorded outside the harness included.
    hh.record_outside_look(ebull_test_conn, note="validation look", registered_by="test", spec=pinned)
    other = _evaluate(ebull_test_conn, _spec(gated["bound"], lag=4), ComputedOutcome("computed", {}, SERIES))
    assert isinstance(other, HuntRefused) and other.reason == "lineage_closed"
    renamed = _spec(gated["bound"], lag=4, hunt_id="hunt-2", family="renamed_family")
    by_hash = _evaluate(ebull_test_conn, renamed, ComputedOutcome("computed", {}, SERIES))
    assert isinstance(by_hash, HuntRefused) and by_hash.reason == "lineage_closed"
    # The row's own candidate is not a new look: it falls through to the burn check.
    own = _evaluate(ebull_test_conn, pinned, ComputedOutcome("computed", {}, SERIES))
    assert isinstance(own, HuntRefused) and own.reason == "split_burned"


def test_a_closed_lineage_refuses_without_any_validation_row(
    ebull_test_conn: psycopg.Connection[Any], gated: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(gated["bound"], lag=4)
    monkeypatch.setattr(hh, "LAST_LOOK_LINEAGES", {spec.family: frozenset({spec.signal_code_sha256})})
    monkeypatch.setattr(hh, "CLOSED_LINEAGES", {spec.family: "closed on its record"})
    refused = _evaluate(ebull_test_conn, spec, ComputedOutcome("computed", {}, SERIES))
    assert isinstance(refused, HuntRefused) and refused.reason == "lineage_closed"
    assert _count(ebull_test_conn, "SELECT count(*) FROM hunt_trials") == 0
