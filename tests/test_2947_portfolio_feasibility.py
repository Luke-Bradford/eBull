"""Prospective portfolio feasibility screen (#2947).

Pure: no DB, no clock, no broker. Every fixture sets ``now`` EXPLICITLY -- the
measured eligibility proofs are dated 2026-08-22, so reusing their shape against a
real clock would assert staleness rather than feasibility.

The measured shapes come from ``strategy_core_eligibility_proofs`` on the dev DB,
2026-09-13: ``SPY`` is ``not_underlying`` with zero qualifying arms, ``SPY.RTH`` is
``underlying`` with one, and every row quotes a 10 USD ``min_position_exposure``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from app.services.portfolio_feasibility import (
    DEFAULT_WEIGHT_TOLERANCE,
    AccountScope,
    FeasibilityLeg,
    LegEligibility,
    PortfolioFeasibilityError,
    screen_portfolio_feasibility,
)

NOW = datetime(2026, 8, 22, 23, 0, tzinfo=UTC)
OBSERVED = datetime(2026, 8, 22, 21, 15, tzinfo=UTC)
#: The real clock at the time the census was taken -- 22 days after the newest proof.
LATER = datetime(2026, 9, 13, 5, 0, tzinfo=UTC)

SCOPE = AccountScope(
    provider="etoro",
    environment="demo",
    operator_id=UUID("00000000-0000-0000-0000-0000000000a1"),
    api_key_credential_id=UUID("00000000-0000-0000-0000-0000000000b1"),
    user_key_credential_id=UUID("00000000-0000-0000-0000-0000000000c1"),
)


def _proof(
    instrument_id: int,
    *,
    verdict: str = "underlying",
    reason_code: str | None = None,
    arms: int = 1,
    allow_open: bool | None = True,
    exposure: Decimal | None = Decimal("10.000000"),
    amount: Decimal | None = Decimal("10.000000"),
    currency: str = "usd",
    observed_at: datetime = OBSERVED,
    scope: AccountScope = SCOPE,
) -> LegEligibility:
    """The measured proof shape. Defaults are the recorded ``SPY.RTH`` row."""
    return LegEligibility(
        instrument_id=instrument_id,
        scope=scope,
        observed_at=observed_at,
        verdict=verdict,  # type: ignore[arg-type]
        reason_code=reason_code,
        response_currency=currency,
        qualifying_arm_count=arms,
        allow_open_position=allow_open,
        min_position_exposure=exposure,
        min_position_amount=amount,
    )


def _not_underlying(instrument_id: int, **kwargs: object) -> LegEligibility:
    """The recorded ``SPY`` shape: the broker ANSWERED, and the answer is no."""
    return _proof(
        instrument_id,
        verdict="not_underlying",
        reason_code="no_underlying_arm",
        arms=0,
        amount=None,
        **kwargs,  # type: ignore[arg-type]
    )


def _screen(
    legs: list[FeasibilityLeg],
    eligibility: dict[int, LegEligibility],
    *,
    capital: Decimal = Decimal(50_000),
    currency: str = "USD",
    rate: Decimal | None = None,
    reserve: Decimal = Decimal(0),
    now: datetime = NOW,
    **kwargs: object,
):
    return screen_portfolio_feasibility(
        assigned_capital=capital,
        capital_currency=currency,
        usd_per_capital_unit=rate,
        cash_reserve_fraction=reserve,
        scope=SCOPE,
        legs=legs,
        eligibility=eligibility,
        now=now,
        **kwargs,  # type: ignore[arg-type]
    )


def _equal_legs(count: int) -> list[FeasibilityLeg]:
    """``count`` equal-weight legs whose weights sum to exactly 1.

    The last weight absorbs the rounding residual: ``1/4034`` summed 4,034 times is
    not 1 in ``Decimal``, and a fixture that tripped the tolerance would be testing
    its own arithmetic rather than the screen's.
    """
    share = (Decimal(1) / count).quantize(Decimal("1e-12"))
    weights = [share] * (count - 1)
    weights.append(Decimal(1) - sum(weights, Decimal(0)))
    return [FeasibilityLeg(instrument_id=1000 + i, symbol=f"SYM{i}", weight=w) for i, w in enumerate(weights)]


# --------------------------------------------------------------------------- #
# The #2947 fixture set, one case per named requirement.
# --------------------------------------------------------------------------- #


def test_a_known_feasible_case_passes_on_the_measured_spy_rth_shape() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)})
    assert report.verdict == "feasible"
    assert report.portfolio_code is None
    assert report.legs[0].code == "feasible"
    assert report.legs[0].open_minimum_usd == Decimal("10.000000")
    assert report.legs[0].allocated_notional_usd == Decimal(50_000)
    # `feasible` still reports the pot the leg needed: 10 / (1 * 1 * 1).
    assert report.minimum_viable_capital == Decimal(10)
    # A passing verdict is "not refused on the dimensions checked", not "deployable".
    assert report.close_side_floor == "unknown"
    assert report.costs_and_rounding_excluded is True
    assert report.max_units_per_order_checked is False


def test_a_cfd_only_listing_refuses_on_the_measured_spy_shape() -> None:
    legs = [FeasibilityLeg(instrument_id=8, symbol="SPY", weight=Decimal(1))]
    report = _screen(legs, {8: _not_underlying(8)})
    assert report.verdict == "refused"
    assert report.portfolio_code == "leg_refused"
    assert report.legs[0].code == "not_underlying_product"
    # No pot size fixes a CFD-only listing.
    assert report.legs[0].minimum_viable_capital is None
    assert report.minimum_viable_capital is None
    assert report.legs_with_unknown_minimum == 1


def test_inadequate_capital_refuses_and_reports_the_pot_it_would_take() -> None:
    # Two legs, 50/50, 10 USD floor each: 15 USD investable puts each leg at 7.50.
    legs = [
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("0.5")),
        FeasibilityLeg(instrument_id=3434, symbol="CSPX.L", weight=Decimal("0.5")),
    ]
    report = _screen(legs, {3417: _proof(3417), 3434: _proof(3434)}, capital=Decimal(15))
    assert report.verdict == "refused"
    assert [leg.code for leg in report.legs] == ["below_open_minimum", "below_open_minimum"]
    assert report.legs[0].allocated_notional_usd == Decimal("7.5")
    # 10 / (0.5 * 1 * 1) == 20.
    assert report.minimum_viable_capital == Decimal(20)


def test_an_unmapped_leg_is_indeterminate_and_is_not_dropped() -> None:
    legs = [
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("0.5")),
        FeasibilityLeg(instrument_id=9999, symbol="NOPROOF", weight=Decimal("0.5")),
    ]
    report = _screen(legs, {3417: _proof(3417)})
    assert report.verdict == "indeterminate"
    assert report.portfolio_code == "leg_indeterminate"
    assert report.legs[1].code == "eligibility_unrecorded"
    # #2947 step 4: screening must never shrink the candidate list.
    assert len(report.legs) == 2
    assert [leg.leg.symbol for leg in report.legs] == ["SPY.RTH", "NOPROOF"]


def test_an_unrecorded_minimum_is_indeterminate_not_unlimited() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, exposure=None, amount=None)})
    assert report.legs[0].code == "open_minimum_unrecorded"
    assert report.legs[0].disposition == "indeterminate"


def test_a_non_usd_pot_with_no_rate_refuses_rather_than_assuming_parity() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, currency="GBP", rate=None)
    assert report.portfolio_code == "capital_currency_unconvertible"
    assert report.verdict == "refused"


# --------------------------------------------------------------------------- #
# FX and reserve arithmetic -- the correction Codex ckpt-1 forced.
# --------------------------------------------------------------------------- #


def test_a_non_usd_pot_converts_before_comparing_with_the_usd_floor() -> None:
    # £8 at 1.25 USD/£ is 10 USD, which is exactly the floor.
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, capital=Decimal(8), currency="GBP", rate=Decimal("1.25"))
    assert report.verdict == "feasible"
    assert report.investable_usd == Decimal("10.00")
    # 10 / (1 * 1 * 1.25) == 8 in POT currency, not 10.
    assert report.minimum_viable_capital == Decimal(8)


def test_the_reserve_raises_the_required_pot_by_one_over_one_minus_r() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, capital=Decimal(12), reserve=Decimal("0.2"))
    assert report.investable_usd == Decimal("9.6")
    assert report.legs[0].code == "below_open_minimum"
    # 10 / (1 * 0.8 * 1) == 12.5, i.e. above the 12 supplied.
    assert report.minimum_viable_capital == Decimal("12.5")


def test_a_usd_pot_may_not_be_handed_a_rate_other_than_one() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    with pytest.raises(PortfolioFeasibilityError, match="exactly 1"):
        _screen(legs, {3417: _proof(3417)}, currency="USD", rate=Decimal("1.25"))


def test_unequal_minima_take_the_binding_leg() -> None:
    legs = [
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("0.5")),
        FeasibilityLeg(instrument_id=3434, symbol="CSPX.L", weight=Decimal("0.5")),
    ]
    eligibility = {
        3417: _proof(3417),
        3434: _proof(3434, exposure=Decimal(200), amount=Decimal(200)),
    }
    report = _screen(legs, eligibility, capital=Decimal(1_000))
    assert report.verdict == "feasible"
    # max(10/0.5, 200/0.5) == 400, not 20.
    assert report.minimum_viable_capital == Decimal(400)


# --------------------------------------------------------------------------- #
# Threshold boundaries.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("capital", "expected"),
    [
        (Decimal("10.000000"), "feasible"),
        (Decimal("9.999999"), "below_open_minimum"),
    ],
)
def test_a_notional_exactly_on_the_floor_clears_it(capital: Decimal, expected: str) -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, capital=capital)
    assert report.legs[0].code == expected


# --------------------------------------------------------------------------- #
# Precedence -- overlapping conditions must have ONE defined answer.
# --------------------------------------------------------------------------- #


def test_stale_outranks_zero_arms_because_a_fresh_proof_is_unknown() -> None:
    legs = [FeasibilityLeg(instrument_id=8, symbol="SPY", weight=Decimal(1))]
    report = _screen(legs, {8: _not_underlying(8)}, now=LATER)
    assert report.legs[0].code == "eligibility_proof_stale"
    assert report.legs[0].disposition == "indeterminate"
    assert report.legs[0].proof_age == LATER - OBSERVED


def test_duplicate_outranks_an_invalid_weight_on_the_same_leg() -> None:
    legs = [
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1)),
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(-1)),
    ]
    report = _screen(legs, {3417: _proof(3417)})
    assert report.legs[1].code == "duplicate_leg"


def test_an_account_mismatch_outranks_every_capability_answer() -> None:
    other = AccountScope(
        provider="etoro",
        environment="demo",
        operator_id=SCOPE.operator_id,
        api_key_credential_id=UUID("00000000-0000-0000-0000-0000000000ff"),
        user_key_credential_id=SCOPE.user_key_credential_id,
    )
    legs = [FeasibilityLeg(instrument_id=8, symbol="SPY", weight=Decimal(1))]
    report = _screen(legs, {8: _not_underlying(8, scope=other)})
    assert report.legs[0].code == "eligibility_account_mismatch"


def test_provider_and_environment_casing_is_not_an_account_difference() -> None:
    shouty = AccountScope(
        provider="ETORO",
        environment="DEMO",
        operator_id=SCOPE.operator_id,
        api_key_credential_id=SCOPE.api_key_credential_id,
        user_key_credential_id=SCOPE.user_key_credential_id,
    )
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, scope=shouty)})
    assert report.legs[0].code == "feasible"


def test_a_non_usd_eligibility_response_refuses_before_the_helper_raises() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, currency="gbp")})
    assert report.legs[0].code == "eligibility_currency_unsupported"


@pytest.mark.parametrize(
    "reason",
    [
        "instrument_not_resolved",
        "eligibility_row_ambiguous",
        "eligibility_currency_mismatch",
        "eligibility_arm_ambiguous",
    ],
)
def test_an_unresolved_proof_is_indeterminate_even_though_its_arm_count_is_zero(
    reason: str,
) -> None:
    """Codex ckpt-2 regression.

    ``evaluate_core_eligibility`` leaves ``qualifying_arm_count`` at its default ``0``
    for an ``unresolved`` proof because it never evaluated an arm. Reading that count
    as a measurement turns "the response did not answer" into a definitive ``refused``,
    which would reject a candidate portfolio on an incomplete broker response.
    """
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    proof = _proof(3417, verdict="unresolved", reason_code=reason, arms=0, amount=None)
    report = _screen(legs, {3417: proof})
    assert report.legs[0].code == "eligibility_unresolved"
    assert report.legs[0].disposition == "indeterminate"
    assert report.verdict == "indeterminate"


def test_an_unresolved_reason_outranks_a_pass_verdict_on_the_same_row() -> None:
    """A row claiming the pass verdict while carrying an unresolved reason is not
    evidence of anything; the reason wins, because it names a response that did not
    answer the question."""
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    proof = _proof(3417, verdict="not_underlying", reason_code="eligibility_row_ambiguous")
    assert _screen(legs, {3417: proof}).legs[0].code == "eligibility_unresolved"


def test_a_pass_verdict_with_zero_arms_is_an_inconsistent_projection_not_a_refusal() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, arms=0)})
    assert report.legs[0].code == "eligibility_projection_inconsistent"
    assert report.legs[0].disposition == "indeterminate"


def test_many_qualifying_arms_is_ambiguous_not_feasible() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, arms=2)})
    assert report.legs[0].code == "arm_selection_ambiguous"


def test_a_null_open_permission_is_unrecorded_not_false() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, allow_open=None)})
    assert report.legs[0].code == "open_permission_unrecorded"
    assert report.legs[0].disposition == "indeterminate"


def test_an_explicitly_prohibited_open_is_refused_not_indeterminate() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417, allow_open=False)})
    assert report.legs[0].code == "open_not_permitted"
    assert report.legs[0].disposition == "refused"


def test_refused_outranks_indeterminate_at_the_portfolio_level() -> None:
    legs = [
        FeasibilityLeg(instrument_id=8, symbol="SPY", weight=Decimal("0.5")),
        FeasibilityLeg(instrument_id=9999, symbol="NOPROOF", weight=Decimal("0.5")),
    ]
    report = _screen(legs, {8: _not_underlying(8)})
    assert report.verdict == "refused"
    assert report.portfolio_code == "leg_refused"


# --------------------------------------------------------------------------- #
# Domain guards -- every one must REFUSE, never raise.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("capital", [Decimal("NaN"), Decimal("Infinity"), Decimal(0), Decimal(-1)])
def test_an_unusable_capital_refuses_rather_than_raising(capital: Decimal) -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, capital=capital)
    assert report.portfolio_code == "capital_not_positive"


@pytest.mark.parametrize("reserve", [Decimal("NaN"), Decimal(-1), Decimal(1), Decimal("1.5"), Decimal("Infinity")])
def test_a_reserve_outside_zero_to_one_refuses_and_never_divides_by_zero(
    reserve: Decimal,
) -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, reserve=reserve)
    assert report.portfolio_code == "reserve_out_of_range"


@pytest.mark.parametrize("rate", [Decimal("NaN"), Decimal(0), Decimal(-1), Decimal("Infinity")])
def test_an_unusable_rate_refuses(rate: Decimal) -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    report = _screen(legs, {3417: _proof(3417)}, currency="GBP", rate=rate)
    assert report.portfolio_code == "capital_currency_unconvertible"


def test_a_nan_weight_refuses_at_both_levels() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("NaN"))]
    report = _screen(legs, {3417: _proof(3417)})
    assert report.portfolio_code == "weights_do_not_sum"
    # The leg is still named, so the reader learns WHICH weight is wrong.
    assert report.legs[0].code == "weight_not_positive"


@pytest.mark.parametrize(
    ("weights", "expected"),
    [
        ([Decimal("0.5"), Decimal("0.4")], "weights_do_not_sum"),
        ([Decimal("0.6"), Decimal("0.5")], "weights_do_not_sum"),
    ],
)
def test_weights_that_do_not_conserve_capital_refuse(weights: list[Decimal], expected: str) -> None:
    legs = [FeasibilityLeg(instrument_id=3417 + i, symbol=f"S{i}", weight=w) for i, w in enumerate(weights)]
    eligibility = {leg.instrument_id: _proof(leg.instrument_id) for leg in legs}
    assert _screen(legs, eligibility).portfolio_code == expected


def test_a_weight_above_one_is_refused_by_name() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("1.5"))]
    report = _screen(legs, {3417: _proof(3417)})
    # Portfolio conservation fires too; the leg carries the specific reason.
    assert report.legs[0].code == "weight_above_unity"


def test_an_empty_portfolio_is_a_caller_defect_not_a_feasible_one() -> None:
    with pytest.raises(PortfolioFeasibilityError, match="no legs"):
        _screen([], {})


def test_a_naive_now_is_a_caller_defect() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    with pytest.raises(PortfolioFeasibilityError, match="timezone-aware"):
        _screen(legs, {3417: _proof(3417)}, now=datetime(2026, 8, 22, 23, 0))  # noqa: DTZ001


def test_a_future_stamped_proof_beyond_the_skew_is_stale() -> None:
    legs = [FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal(1))]
    ahead = _proof(3417, observed_at=NOW + timedelta(hours=1))
    assert _screen(legs, {3417: ahead}).legs[0].code == "eligibility_proof_stale"


# --------------------------------------------------------------------------- #
# The R6 negative control, and the #2833 sleeve.
# --------------------------------------------------------------------------- #


def test_the_4034_leg_control_reports_its_threshold_and_does_not_refuse_a_50k_usd_pot() -> None:
    """#2947's negative-control example, with the arithmetic corrected.

    An earlier draft of the spec claimed this portfolio "must refuse". It does not:
    4,034 equal legs at a 10 USD floor need 40,340 USD, which a 50,000 USD pot
    clears. The R6 portfolio's failure is its RETURN, not its affordability, and the
    screen must not be sold as having caught it.
    """
    legs = _equal_legs(4_034)
    eligibility = {leg.instrument_id: _proof(leg.instrument_id) for leg in legs}
    report = _screen(legs, eligibility, capital=Decimal(50_000))
    assert report.verdict == "feasible"
    assert report.minimum_viable_capital is not None
    assert report.minimum_viable_capital.quantize(Decimal("1")) == Decimal(40_340)
    assert abs(report.weights_sum - 1) <= DEFAULT_WEIGHT_TOLERANCE


def test_the_4034_leg_control_refuses_once_the_pot_falls_under_that_threshold() -> None:
    legs = _equal_legs(4_034)
    eligibility = {leg.instrument_id: _proof(leg.instrument_id) for leg in legs}
    report = _screen(legs, eligibility, capital=Decimal(40_000))
    assert report.verdict == "refused"
    assert all(leg.code == "below_open_minimum" for leg in report.legs)


def test_the_2833_sleeve_is_feasible_on_its_declared_candidates() -> None:
    """``candidate_ids`` [3417, 3434, 3075] from #2833's checked-in declaration.

    All three are recorded ``underlying``, one arm, open permitted, 10 USD floor --
    i.e. #2833 did NOT pick the CFD-only plain US listings.
    """
    legs = [
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("0.4")),
        FeasibilityLeg(instrument_id=3434, symbol="CSPX.L", weight=Decimal("0.4")),
        FeasibilityLeg(instrument_id=3075, symbol="IUSA.L", weight=Decimal("0.2")),
    ]
    eligibility = {leg.instrument_id: _proof(leg.instrument_id) for leg in legs}
    assert _screen(legs, eligibility).verdict == "feasible"


def test_the_2833_sleeve_is_indeterminate_today_because_its_evidence_expired() -> None:
    """The operationally useful output: a re-census precedes any allocation."""
    legs = [
        FeasibilityLeg(instrument_id=3417, symbol="SPY.RTH", weight=Decimal("0.4")),
        FeasibilityLeg(instrument_id=3434, symbol="CSPX.L", weight=Decimal("0.4")),
        FeasibilityLeg(instrument_id=3075, symbol="IUSA.L", weight=Decimal("0.2")),
    ]
    eligibility = {leg.instrument_id: _proof(leg.instrument_id) for leg in legs}
    report = _screen(legs, eligibility, now=LATER)
    assert report.verdict == "indeterminate"
    assert all(leg.code == "eligibility_proof_stale" for leg in report.legs)
    assert report.minimum_viable_capital is None
