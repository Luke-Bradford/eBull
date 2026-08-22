"""#2720 — freeze-time validation of declared cost stamps, as pure logic.

A frozen declaration is a PREDICTION of the stamps its run will produce. For a
MANIFEST strategy the run is ``backtest_run``, which stamps the ``cost_model``
module constants — so a mismatching declaration must be refused AT FREEZE, the
only time refusing is cheap (``sql/333`` makes the row immutable). A bespoke
contract trial (the #2582 schedule-13D catalyst charges its own flat 50 bps)
OWNS its stamps and is exempt.

Deliberately NOT in ``PreregDeclaration.__post_init__``: that class is also the
read-back of stored rows, and rows frozen under an earlier cost model
legitimately declare stamps today's constants do not produce.

⚠ DB-free by design: the validation fires before ``freeze_preregistration``
touches its connection, which is what these tests lean on.
"""

from __future__ import annotations

from typing import Any, cast
from unittest import mock

import pytest

from app.services.cost_model import CARRY_UNMODELLED, FX_UNMODELLED
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import freeze_preregistration
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION

# #2829 — freezes synthetic or pre-mapped identities while testing a different
# gate; see `assume_trial_registered` in tests/conftest.py.
pytestmark = pytest.mark.usefixtures("assume_trial_registered")


#: A real manifest id — the check is scoped to the manifest, so the fixture
#: must sit inside it for the refusal arm to be reachable.
_MANIFEST_ID = "s1-time-series-momentum"


def _declaration(**overrides: object) -> PreregDeclaration:
    base: dict[str, object] = {
        "strategy_id": _MANIFEST_ID,
        "strategy_version": "strategy-registry-v1+abc123",
        "contract_version": "test-contract-v1",
        "prereg_purpose": "falsification_only",
        "structural_refusal_policy_version": STRUCTURAL_REFUSAL_POLICY_VERSION,
        "declared_universe_basis": "survivor_only",
        "declared_carry_unmodelled": CARRY_UNMODELLED,
        "declared_fx_unmodelled": FX_UNMODELLED,
        "expected_structural_refusals": ("universe_basis_not_survivorship_free",),
        "forward_shadow": ForwardShadowFloor(
            min_independent_decision_dates=40,
            min_calendar_weeks=12,
            derivation="planning power calculation, candidate contract §statistics",
        ),
        "declared_by": "tests/test_2720_freeze_stamp_validation.py",
    }
    base.update(overrides)
    return PreregDeclaration(**base)  # type: ignore[arg-type]


_STALE = {
    "declared_carry_unmodelled": True,
    "declared_fx_unmodelled": True,
    "expected_structural_refusals": (
        "universe_basis_not_survivorship_free",
        "carry_unmodelled",
        "fx_unmodelled",
    ),
}


def test_the_premises_hold() -> None:
    """Stated so a failure names #2720 (or a manifest rename) rather than a
    mysterious coherence mismatch in the fixtures below."""
    assert (CARRY_UNMODELLED, FX_UNMODELLED) == (False, False)
    assert _MANIFEST_ID in STRATEGY_MANIFEST


def test_a_manifest_declaration_of_stale_stamps_is_refused_before_any_write() -> None:
    """Declaring ``True`` under a model that stamps ``False`` would burn an
    immutable trial (sql/333 bars UPDATE and DELETE). ⚠ ``conn=None`` is the
    proof the refusal happens before the connection is touched."""
    with pytest.raises(ValueError, match="stamps its run cannot produce"):
        freeze_preregistration(cast(Any, None), _declaration(**_STALE))


@pytest.mark.parametrize("field", ["declared_carry_unmodelled", "declared_fx_unmodelled"])
def test_one_stale_stamp_is_enough(field: str) -> None:
    """Either half alone mis-predicts the run; the pair is checked whole.

    ⚠ ``expected_structural_refusals`` is kept CONSISTENT with the mutated
    stamp, so ``declaration_refusals`` passes and the stamp check is the ONLY
    thing refusing — otherwise this would re-test coherence, not #2720."""
    refusal = "carry_unmodelled" if field == "declared_carry_unmodelled" else "fx_unmodelled"
    stale = _declaration(
        **{field: True},
        expected_structural_refusals=("universe_basis_not_survivorship_free", refusal),
    )
    with pytest.raises(ValueError, match="stamps its run cannot produce"):
        freeze_preregistration(cast(Any, None), stale)


def test_a_current_stamp_declaration_reaches_the_lock() -> None:
    """The pass side: matching stamps proceed past the check to the trial lock
    (the first connection touch), where a sentinel proves the order."""
    sentinel = RuntimeError("reached-the-lock")
    conn = mock.MagicMock()
    conn.execute.side_effect = sentinel
    with pytest.raises(RuntimeError, match="reached-the-lock"):
        freeze_preregistration(cast(Any, conn), _declaration())


def test_a_bespoke_trial_keeps_owning_its_stamps() -> None:
    """⚠ THE SCOPE, not a loophole: a non-manifest contract trial (#2582's
    schedule-13D catalyst charges its own flat 50 bps and models no carry)
    honestly declares ``True`` — refusing it would force a false "modelled"
    claim onto a run that charges no carry. It must reach the lock."""
    sentinel = RuntimeError("reached-the-lock")
    conn = mock.MagicMock()
    conn.execute.side_effect = sentinel
    bespoke = _declaration(strategy_id="c4-bespoke-contract-trial", **_STALE)
    assert bespoke.strategy_id not in STRATEGY_MANIFEST
    with pytest.raises(RuntimeError, match="reached-the-lock"):
        freeze_preregistration(cast(Any, conn), bespoke)
