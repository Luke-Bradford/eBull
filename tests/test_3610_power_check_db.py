"""#3610 — the freeze stores the power check on the declaration row (``sql/469``)."""

from __future__ import annotations

import psycopg
import pytest

from app.services.result_ledger import freeze_preregistration
from app.services.trial_register import DeclaredTrial, EvidenceTrack, TrialDesign, TrialExactness, TrialRegister
from tests.test_3610_power_check import _declaration

_PAIR = ("power-db-test-trial", "v1")


def test_the_power_check_is_stored_on_the_frozen_row(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    design = TrialDesign(
        track=EvidenceTrack.ADOPTION,
        effect_ir=0.5,
        effect_basis="test prior",
        effective_years=60.0,
        dependence="test: annual non-overlapping",
    )
    trial = DeclaredTrial(
        trial_id="power-db-test-trial",
        description="d",
        evidence="e",
        exactness=TrialExactness.EXACT,
        declared_for=_PAIR,
        design=design,
    )
    register = TrialRegister(version="t-db", trials=(trial,))
    monkeypatch.setattr("app.services.result_ledger.TRIAL_REGISTER", register)

    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, _declaration(*_PAIR))

    row = ebull_test_conn.execute(
        "SELECT power_check FROM strategy_preregistration_declarations WHERE declaration_id = %s",
        (declaration_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == register.freeze_power_record(trial)
    assert row[0]["track"] == "adoption"
