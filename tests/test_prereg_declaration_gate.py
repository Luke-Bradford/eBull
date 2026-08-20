"""#2599 — the preregistration declaration gate, as pure logic.

Spec: ``docs/proposals/ta/2026-08-12-preregistration-declaration-gate.md``.
Rules: ``app/services/prereg_contract.py``. Floor enforcement:
``app/services/strategy_live_gate.live_gate_refusals``.

⚠ NO DATABASE. Everything asserted here is a decision, and a decision is a pure
function of its inputs — the repo's standing preference (*"extract the decision
into a pure function and table-test it"*). The DB-tier half of this ticket
(freeze, immutability, the chokepoint refusal) lives in
``tests/test_strategy_holdout_namespace.py``, because those ARE properties of a
relation and a mocked cursor cannot stand in for one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.prereg_contract import (
    ForwardShadowFloor,
    PreregDeclaration,
    declaration_refusals,
    is_coherent,
)
from app.services.strategy_live_gate import LiveGateFacts, LiveGatePolicy, live_gate_refusals
from app.services.strategy_result import (
    STRUCTURAL_REFUSAL_POLICY_VERSION,
    PromotionCandidate,
    check_promotable,
    structural_promotion_refusals,
)
from tests.test_result_ledger import build_result

_INELIGIBLE = ("universe_basis_not_survivorship_free", "carry_unmodelled", "fx_unmodelled")


def _declaration(**overrides: object) -> PreregDeclaration:
    """A coherent falsification declaration over the PRE-#2720 stamps.

    ⚠ Still a valid fixture: ``declaration_refusals`` judges internal coherence
    and stored rows of this shape must keep loading. A NEW freeze of this shape
    for a MANIFEST strategy is refused by ``freeze_preregistration``'s stamp
    check — ``tests/test_2720_freeze_stamp_validation.py`` owns that side
    ("S-1" here is not a manifest id, so these fixtures stay freezable)."""
    base: dict[str, object] = {
        "strategy_id": "S-1",
        "strategy_version": "strategy-registry-v1+abc123",
        "contract_version": "test-contract-v1",
        "prereg_purpose": "falsification_only",
        "structural_refusal_policy_version": STRUCTURAL_REFUSAL_POLICY_VERSION,
        "declared_universe_basis": "survivor_only",
        "declared_carry_unmodelled": True,
        "declared_fx_unmodelled": True,
        "expected_structural_refusals": _INELIGIBLE,
        "forward_shadow": ForwardShadowFloor(
            min_independent_decision_dates=40,
            min_calendar_weeks=12,
            derivation="planning power calculation, candidate contract §statistics",
        ),
        "declared_by": "tests/test_prereg_declaration_gate.py",
    }
    base.update(overrides)
    return PreregDeclaration(**base)  # type: ignore[arg-type]


def _eligible(**overrides: object) -> PreregDeclaration:
    """The declaration a promotable candidate would freeze.

    ⚠ Overrides applied LAST, so a caller can flip `prereg_purpose` here — the
    eligible+falsification case needs exactly that, and passing it through
    `**overrides` alongside a hardcoded keyword is a `TypeError`.
    """
    eligible: dict[str, object] = {
        "prereg_purpose": "capital_candidate",
        "declared_universe_basis": "survivorship_free",
        "declared_carry_unmodelled": False,
        "declared_fx_unmodelled": False,
        "expected_structural_refusals": (),
    }
    eligible.update(overrides)
    return _declaration(**eligible)


# ---------------------------------------------------------------------------
# The four acceptance cases the ticket enumerates
# ---------------------------------------------------------------------------


def test_eligible_capital_candidate_is_coherent() -> None:
    assert declaration_refusals(_eligible()) == ()
    assert is_coherent(_eligible())


def test_ineligible_capital_candidate_is_refused() -> None:
    """The whole ticket in one assertion.

    Survivor-only stamps plus a `capital_candidate` claim is a trial that cannot
    promote pretending it can — the state five sealed trials were in between
    2026-08-10 and 2026-08-12.
    """
    refusals = declaration_refusals(_declaration(prereg_purpose="capital_candidate"))
    assert "ineligible_trial_not_declared_falsification" in refusals


def test_ineligible_falsification_only_is_allowed() -> None:
    """A declared falsification stays legitimate.

    ⚠ It still charges the trial register — any look must. What #2599 forbids is
    the trial that never said so.
    """
    assert declaration_refusals(_declaration()) == ()


def test_eligible_falsification_only_is_allowed() -> None:
    """Voluntarily restricting yourself is strictly tighter than the rule."""
    assert declaration_refusals(_eligible(prereg_purpose="falsification_only")) == ()


# ---------------------------------------------------------------------------
# The coherence rules
# ---------------------------------------------------------------------------


def test_superseded_policy_version_is_refused() -> None:
    """A frozen expectation means what it meant under ITS OWN policy.

    Same shape as `trial_register_superseded`: refused, never re-interpreted.
    """
    refusals = declaration_refusals(
        _declaration(structural_refusal_policy_version="structural-refusal-policy-1999-01-01-v0")
    )
    assert refusals == ("structural_refusal_policy_superseded",)


def test_declared_list_disagreeing_with_recomputed_is_refused() -> None:
    refusals = declaration_refusals(_declaration(expected_structural_refusals=("carry_unmodelled",)))
    assert "expected_structural_refusals_mismatch" in refusals


def test_falsified_empty_list_yields_both_refusals_not_just_the_mismatch() -> None:
    """⚠ THE REGRESSION THIS FILE EXISTS FOR.

    The purpose check reads the RECOMPUTED list. Had it read the declared one, a
    writer that lied by declaring `[]` over survivor-only stamps would produce a
    bare `expected_structural_refusals_mismatch` and the substantive refusal
    would vanish in exactly the case it exists for.
    """
    refusals = declaration_refusals(_declaration(prereg_purpose="capital_candidate", expected_structural_refusals=()))
    assert set(refusals) == {
        "expected_structural_refusals_mismatch",
        "ineligible_trial_not_declared_falsification",
    }


def test_refusal_order_within_the_list_does_not_matter() -> None:
    """Two spellings of the same expectation are the same expectation."""
    assert declaration_refusals(_declaration(expected_structural_refusals=tuple(reversed(_INELIGIBLE)))) == ()


def test_reordered_declaration_hashes_the_same() -> None:
    assert _declaration().sha256 == _declaration(expected_structural_refusals=tuple(reversed(_INELIGIBLE))).sha256


def test_changing_a_declared_field_changes_the_digest() -> None:
    assert _declaration().sha256 != _declaration(contract_version="test-contract-v2").sha256


@pytest.mark.parametrize("dates,weeks", [(0, 12), (40, 0), (-1, -1)])
def test_non_positive_floor_is_refused(dates: int, weeks: int) -> None:
    refusals = declaration_refusals(
        _declaration(
            forward_shadow=ForwardShadowFloor(
                min_independent_decision_dates=dates, min_calendar_weeks=weeks, derivation="x"
            )
        )
    )
    assert "forward_shadow_floor_not_positive" in refusals


def test_floor_without_a_derivation_is_refused() -> None:
    """A floor that names no power calculation is the #2600 padded-floor defect."""
    refusals = declaration_refusals(
        _declaration(
            forward_shadow=ForwardShadowFloor(
                min_independent_decision_dates=40, min_calendar_weeks=12, derivation="   "
            )
        )
    )
    assert "forward_shadow_derivation_missing" in refusals


@pytest.mark.parametrize(
    "overrides,fragment",
    [
        ({"prereg_purpose": "exploratory"}, "unknown prereg_purpose"),
        ({"strategy_id": "  "}, "must be non-empty"),
        ({"declared_by": ""}, "must be non-empty"),
        ({"expected_structural_refusals": ("not_a_real_code",)}, "outside the promotion vocabulary"),
        ({"expected_structural_refusals": ("carry_unmodelled", "carry_unmodelled")}, "duplicate code"),
    ],
)
def test_malformed_declaration_raises_at_construction(overrides: dict[str, object], fragment: str) -> None:
    """⚠ RAISES rather than returning a refusal, deliberately.

    A malformed declaration is a WRITER bug — the convention
    ``StrategyResult.__post_init__`` already sets — and freezing is where it
    should fail, loudly, naming the field.
    """
    with pytest.raises(ValueError, match=fragment):
        _declaration(**overrides)


# ---------------------------------------------------------------------------
# The extraction did not move the promotion gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "basis,carry,fx,expected",
    [
        # ⚠ ALL FOUR (carry, fx) STATES, not just the two single-clear ones
        # (#2363). The whole point of the split is that each closes on its own
        # evidence, so a test that only ever moves them together would pass
        # against the coupled flag it replaced.
        ("survivorship_free", False, False, ()),
        ("survivorship_free", True, True, ("carry_unmodelled", "fx_unmodelled")),
        ("survivorship_free", True, False, ("carry_unmodelled",)),
        ("survivorship_free", False, True, ("fx_unmodelled",)),
        ("survivor_only", True, True, _INELIGIBLE),
        ("", True, True, ("universe_basis_absent", "carry_unmodelled", "fx_unmodelled")),
        (None, False, False, ("universe_basis_absent",)),
        ("a_label_nobody_anticipated", False, False, ("universe_basis_not_survivorship_free",)),
    ],
)
def test_structural_refusals_are_an_allowlist(
    basis: str | None, carry: bool, fx: bool, expected: tuple[str, ...]
) -> None:
    assert structural_promotion_refusals(universe_basis=basis, carry_unmodelled=carry, fx_unmodelled=fx) == expected


def test_check_promotable_still_emits_the_same_stamp_refusals() -> None:
    """The extraction is a refactor. If it were not, this fails.

    ⚠ Asserts the codes are still THERE and still adjacent — `check_promotable`
    returns refusals in the spec's own order so a missing block is visible as a
    gap, and `extend` must not have reordered them.
    """
    candidate = PromotionCandidate(
        result=build_result(universe_basis="survivor_only", carry_unmodelled=True, fx_unmodelled=True),
        validated_universe_ids=frozenset({1}),
        evaluated_instrument_ids=frozenset({1}),
    )
    refusals = check_promotable(candidate)
    assert "universe_basis_not_survivorship_free" in refusals
    assert "carry_unmodelled" in refusals
    assert "fx_unmodelled" in refusals
    first = refusals.index("universe_basis_not_survivorship_free")
    assert refusals[first : first + len(_INELIGIBLE)] == _INELIGIBLE


# ---------------------------------------------------------------------------
# The forward-shadow floor at the live gate
# ---------------------------------------------------------------------------


def _facts(**overrides: object) -> LiveGateFacts:
    base: dict[str, object] = {
        "stage": "paper_enabled",
        "forward_resolved_signals": 500,
        "forward_decision_dates": 40,
        "forward_days": 84,
        "paper_closed_trades": 100,
        "paper_days": 100,
        "forward_observation_entries": 1,
        "paper_enabled_entries": 1,
        "funded_shadow_average_return_pct": Decimal("1"),
        "unfunded_shadow_average_return_pct": Decimal("0"),
        "shadow_alpha_pct": Decimal("1"),
        "average_slippage_pct": Decimal("0"),
        "cost_drift_pct": Decimal("0"),
        "max_observed_drawdown_pct": Decimal("1"),
        "reconciliation_order_count": 1,
        "reconciliation_breach_count": 0,
        "scan_age_seconds": Decimal("1"),
        "active_owned_instrument_count": 0,
        "oldest_owned_quote_age_seconds": None,
        "halt_feed_age_seconds": Decimal("1"),
        "broker_health_age_seconds": Decimal("1"),
        "broker_health_active_block": False,
        "paper_pnl_complete": True,
        "completed_kill_drills": ("quote_lag", "scan_lag", "broker_outage", "reconciliation_backlog", "drawdown"),
        "auto_trading_enabled": True,
        "live_trading_enabled": True,
        "global_kill_active": False,
        "active_execution_block_count": 0,
    }
    base.update(overrides)
    return LiveGateFacts(**base)  # type: ignore[arg-type]


def _policy() -> LiveGatePolicy:
    from datetime import UTC, datetime

    return LiveGatePolicy(
        live_gate_policy_id=1,
        strategy_id="S-1",
        strategy_version="strategy-registry-v1+abc123",
        min_forward_resolved_signals=1,
        min_forward_days=1,
        min_paper_closed_trades=1,
        min_paper_days=1,
        max_reconciliation_age_seconds=60,
        min_shadow_alpha_pct=Decimal("0"),
        max_cost_drift_pct=Decimal("100"),
        max_average_slippage_pct=Decimal("100"),
        max_drawdown_pct=Decimal("99"),
        max_scan_age_seconds=600,
        max_quote_age_seconds=600,
        max_broker_health_age_seconds=600,
        max_live_capital=Decimal("1000"),
        currency="USD",
        leverage=1,
        registered_at=datetime(2026, 8, 12, tzinfo=UTC),
        declaration_id=1,
    )


def _gate(**kwargs: object) -> tuple[str, ...]:
    params: dict[str, object] = {
        "purpose": "capital_candidate",
        "policy": _policy(),
        "declaration": _eligible(),
        "facts": _facts(),
        "requested_capital": Decimal("100"),
    }
    params.update(kwargs)
    return live_gate_refusals(**params)  # type: ignore[arg-type]


def test_missing_declaration_refuses_the_live_gate() -> None:
    """The acceptance case the nullable column exists to produce.

    A policy registered before #2599 carries no floor, and "unset" is read as
    refused rather than unbounded.
    """
    assert "forward_shadow_floor_missing" in _gate(declaration=None)


def test_floor_present_and_met_adds_no_forward_shadow_refusal() -> None:
    codes = _gate()
    assert "forward_shadow_floor_missing" not in codes
    assert "forward_decision_dates_insufficient" not in codes
    assert "forward_calendar_weeks_insufficient" not in codes


def test_too_few_decision_dates_is_refused() -> None:
    assert "forward_decision_dates_insufficient" in _gate(facts=_facts(forward_decision_dates=39))


def test_same_day_fan_out_cannot_clear_the_dates_floor() -> None:
    """⚠ THE REASON THIS FLOOR COUNTS DATES AND NOT SIGNALS.

    Ten thousand signals fired on three days is three decisions' worth of
    evidence. `min_forward_resolved_signals` cannot tell that apart; this can.
    """
    codes = _gate(facts=_facts(forward_resolved_signals=10_000, forward_decision_dates=3))
    assert "forward_decision_dates_insufficient" in codes
    assert "forward_sample_insufficient" not in codes


@pytest.mark.parametrize("days,refused", [(83, True), (84, False)])
def test_calendar_weeks_floor_needs_fully_elapsed_days(days: int, refused: bool) -> None:
    """12 weeks means 84 fully elapsed days.

    ⚠ `forward_days` comes from `timedelta.days`, which TRUNCATES, so a run
    83.9 days old reads as 83 and is refused. Truncation makes the bound
    stricter — the fail-closed direction.
    """
    codes = _gate(facts=_facts(forward_days=days))
    assert ("forward_calendar_weeks_insufficient" in codes) is refused


def test_the_frozen_floor_binds_independently_of_the_operator_policy() -> None:
    """⚠ DUAL ENFORCEMENT, NOT REPLACEMENT.

    The operator-registered `min_forward_days` is satisfied here (1 day). The
    contract-frozen floor is not. A policy that could out-vote the frozen floor
    would make the declaration decorative.
    """
    codes = _gate(facts=_facts(forward_days=7, forward_decision_dates=40))
    assert "forward_duration_insufficient" not in codes
    assert "forward_calendar_weeks_insufficient" in codes


def test_the_broker_contract_refusal_still_terminates_every_assessment() -> None:
    """No arrangement of #2599's inputs can make the gate pass.

    Guards against this ticket accidentally becoming a path to live capital.
    """
    assert "live_strategy_broker_contract_not_validated" in _gate()


def test_a_rewritten_declaration_is_refused_by_the_live_gate() -> None:
    """⚠ A DISTINCT CODE FROM "no floor was frozen".

    Codex checkpoint 2 found the live-gate path checking coherence but not the
    digest, while the outcome-access path checked both. A declaration edited
    around the immutability trigger can stay perfectly coherent and carry a
    different floor, so coherence alone is not the frozen-contract guarantee.
    """
    codes = _gate(declaration_digest_intact=False)
    assert "declaration_digest_mismatch" in codes
    assert "forward_shadow_floor_missing" not in codes


def test_an_intact_digest_leaves_the_floor_usable() -> None:
    assert "declaration_digest_mismatch" not in _gate()


def test_a_declaration_that_became_incoherent_stops_authorising_the_floor() -> None:
    """⚠ THE TWO ENFORCEMENT POINTS MUST NOT DIVERGE.

    The live-gate policy is immutable; the structural-refusal POLICY VERSION is
    not. `record_holdout_access` refuses a superseded declaration on every look,
    and the live gate re-checks the same thing on every assessment — checking
    only the digest here left the research side refusing while the capital side
    kept honouring the frozen floor.
    """
    codes = _gate(declaration_coherent=False)
    assert "declaration_no_longer_coherent" in codes
    assert "forward_shadow_floor_missing" not in codes
    assert "declaration_digest_mismatch" not in codes


def test_a_rewritten_declaration_reports_the_digest_not_the_coherence() -> None:
    """Three states, three codes — collapsing them would hide two."""
    codes = _gate(declaration_digest_intact=False, declaration_coherent=False)
    assert "declaration_digest_mismatch" in codes
    assert "declaration_no_longer_coherent" not in codes
