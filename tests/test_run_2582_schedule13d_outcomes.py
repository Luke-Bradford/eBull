from __future__ import annotations

from pathlib import Path

import pytest

from scripts.evaluate_2582_schedule13d_outcomes import ACKNOWLEDGEMENT, OutcomeGateRefusal
from scripts.run_2582_schedule13d_outcomes import main

_CONTRACT = Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json")


def test_runner_refuses_before_database_connection_on_a_wrong_acknowledgement() -> None:
    """⚠ BEFORE the connection, which is why the preconditions are split out.

    This test used to assert ``absent from trial-register`` — the refusal #2614
    deliberately removes by charging C-4's arms to the register. The property
    worth keeping from it is the one it was actually demonstrating: a refusal
    that does not cost a database connection. Folding the declaration check into
    ``require_outcome_gate`` gave that function a ``conn``, so the cheap checks
    moved to ``require_outcome_gate_preconditions`` and the runner calls them
    first.

    ⚠ If this ever starts failing with a connection error rather than
    ``OutcomeGateRefusal``, the ordering in ``main`` has regressed — the whole
    point is that no ``psycopg.connect`` runs before this raises.
    """

    with pytest.raises(OutcomeGateRefusal, match="remain closed"):
        main(["--acknowledgement", "not-the-acknowledgement", "--contract", str(_CONTRACT)])


def test_runner_refuses_before_database_connection_on_a_moved_contract(tmp_path: Path) -> None:
    moved = tmp_path / "contract.json"
    moved.write_text(_CONTRACT.read_text() + "\n")
    with pytest.raises(AssertionError, match="frozen contract digest moved"):
        main(["--acknowledgement", ACKNOWLEDGEMENT, "--contract", str(moved)])
