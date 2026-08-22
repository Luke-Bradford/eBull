"""#2616 rule 4 against a real relation: a register entry charges ONE committed look.

⚠ NOT MOCKABLE, same argument as ``tests/test_c4_declaration_gate_db.py``: the
spent-marker is a row in ``strategy_holdout_accesses``, and the property under
test is that the row the first gate call writes is what refuses the second.
Codex checkpoint 2 caught the first draft without this rule — a newly declared
re-run entry stayed reusable forever, recreating the uncharged second look the
gate exists to stop.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.result_ledger import freeze_preregistration
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister
from scripts.freeze_2616_precutoff_declarations import build_pead_declaration
from scripts.sealed_rerun_gate import RerunGateRefusal, require_outcome_gate
from scripts.verify_2476_pead_outcomes import SEALED_TRIAL

# #2829 — freezes synthetic or pre-mapped identities while testing a different
# gate; see `assume_trial_registered` in tests/conftest.py.
pytestmark = pytest.mark.usefixtures("assume_trial_registered")

_RERUN_ID = "pead-historical-sue-net-income-v1-rerun-db-test"

#: Synthetic on purpose: the real register holds no re-run entry yet, and that
#: absence is the gate's rule 3 working. Rule 4 needs an entry that passes 1-3.
_REGISTER = TrialRegister(
    version="trial-register-2616-db-test",
    trials=(
        DeclaredTrial(
            trial_id=_RERUN_ID,
            description="synthetic re-run entry for rule 4's single-use property",
            evidence="this test",
            exactness=TrialExactness.EXACT,
        ),
    ),
)


def test_a_rerun_register_entry_charges_exactly_one_committed_look(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with ebull_test_conn.transaction():
        freeze_preregistration(ebull_test_conn, build_pead_declaration())
        first = require_outcome_gate(ebull_test_conn, SEALED_TRIAL, trial_id=_RERUN_ID, register=_REGISTER)
        assert first.rerun_trial_id == _RERUN_ID
        # ⚠ The refusal must come from the ACCESS ROW the first call wrote, in
        # the same transaction — not from any in-process state, which a second
        # invocation of the script would not share.
        with pytest.raises(RerunGateRefusal, match="single-use"):
            require_outcome_gate(ebull_test_conn, SEALED_TRIAL, trial_id=_RERUN_ID, register=_REGISTER)
