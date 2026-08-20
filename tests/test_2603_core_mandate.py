"""#2603 item 1 — the core/cash mandate validator, as pure policy.

No DB: the rules are arithmetic over operator input, and `sql/336`'s CHECKs are
their backstop rather than their definition. The migration's own effects are in
``test_2603_core_mandate_db``.
"""

from decimal import Decimal

import pytest

from app.services.strategy_core_mandate import (
    CORE_MANDATE_BASE_CURRENCY,
    CORE_MANDATE_POLICY_VERSION,
    CoreMandate,
    CoreMandateError,
    validate_core_mandate,
)

# A mandate that satisfies every rule: 60/40 core/cash, 5pp band, 20% reserve.
# Worst-case cash is 100 - (60 + 5) = 35, which clears the 20 reserve.
_VALID = {
    "enabled": True,
    "base_currency": "USD",
    "core_instrument_id": 1,
    "core_target_pct": Decimal("60"),
    "liquidity_reserve_pct": Decimal("20"),
    "rebalance_band_pct": Decimal("5"),
    "min_rebalance_amount": Decimal("25"),
}


def test_the_reference_mandate_validates() -> None:
    values = validate_core_mandate(**_VALID)
    assert values.core_target_pct == Decimal("60")
    assert values.base_currency == CORE_MANDATE_BASE_CURRENCY


def test_cash_is_the_complement_and_never_a_second_stored_value() -> None:
    """The weights-sum rule is structural, so there is no state to disagree with.

    Asserted on the dataclass because that is the only place cash exists.
    """
    mandate = CoreMandate(
        event_id=1,
        revision=1,
        enabled=True,
        base_currency="USD",
        core_instrument_id=1,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("20"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("25"),
        policy_version=CORE_MANDATE_POLICY_VERSION,
    )
    assert mandate.cash_target_pct == Decimal("40")
    assert mandate.core_target_pct + mandate.cash_target_pct == Decimal("100")


def test_the_policy_version_records_which_arithmetic_wrote_the_row() -> None:
    """#2670 bumped this, and the bump is the point rather than an incident.

    A version denotes a RULE SET, not a row population, so 0 stored rows did not
    excuse leaving it: ``CoreMandate`` is publicly constructible, so a v1 mandate
    can exist without ever having been stored and changes validity across the
    tightening. Pinned so a later invariant change cannot land without one.
    """
    assert CORE_MANDATE_POLICY_VERSION == "core-mandate-v2"


def test_a_band_that_drifts_through_the_reserve_is_refused() -> None:
    """The invariant this table exists to carry.

    core 60 + band 25 = 85, leaving 15 cash against a 20 reserve. Storing it
    would make the reserve a number the mandate states and the band contradicts.
    """
    with pytest.raises(CoreMandateError, match="drifting through liquidity_reserve_pct"):
        validate_core_mandate(**{**_VALID, "rebalance_band_pct": Decimal("25")})


def test_the_reserve_boundary_is_inclusive() -> None:
    """Worst-case cash exactly equal to the reserve is permitted.

    Pinned because the CHECK is `>=`: whether the band then *triggers* at
    equality is execution semantics and belongs to item 3, but the mandate is
    storable and that must not drift silently.
    """
    values = validate_core_mandate(**{**_VALID, "rebalance_band_pct": Decimal("20")})
    assert values.rebalance_band_pct == Decimal("20")


def test_a_band_wider_than_the_target_is_refused() -> None:
    """Otherwise a declared two-sided band is silently one-sided.

    core 4 with a 5pp band puts the lower trigger below zero, unreachable short
    of the core going to nothing. Reserve is 0 here so only the range rule fires.
    """
    with pytest.raises(CoreMandateError, match="lower trigger is unreachable"):
        validate_core_mandate(
            **{
                **_VALID,
                "core_target_pct": Decimal("4"),
                "rebalance_band_pct": Decimal("5"),
                "liquidity_reserve_pct": Decimal("0"),
            }
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        # band == target -> lower == 0, and `core_pct < 0` is unreachable for a
        # non-negative sleeve, INCLUDING by the core going to zero: 0 is not < 0.
        (
            {
                "core_target_pct": Decimal("20"),
                "rebalance_band_pct": Decimal("20"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "lower trigger is unreachable",
        ),
        # target + band == 100 at reserve 0 -> upper == 100, and `core_pct > 100`
        # is unreachable for a sleeve whose cash cannot go negative.
        (
            {
                "core_target_pct": Decimal("60"),
                "rebalance_band_pct": Decimal("40"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "upper trigger is unreachable",
        ),
        # The granularity boundary on the upper side: NUMERIC(8,4) means
        # 99.9999 + 0.0001 lands exactly on the dead point. Storable under
        # sql/336, refused by sql/344.
        (
            {
                "core_target_pct": Decimal("99.9999"),
                "rebalance_band_pct": Decimal("0.0001"),
                "liquidity_reserve_pct": Decimal("0"),
            },
            "upper trigger is unreachable",
        ),
    ],
)
def test_a_band_whose_trigger_can_never_fire_is_refused(overrides: dict[str, Decimal], message: str) -> None:
    """#2670. Equality is the DEAD POINT on each side, not a boundary case.

    ``core_pct = 100 * core_mv / (core_mv + cash)`` is bounded to [0,100] for a
    non-negative sleeve and both allocator triggers are strict, so ``lower == 0``
    and ``upper == 100`` are comparators that cannot become true. A mandate
    holding one is weaker than the two-sided band it declares, and nothing
    downstream can tell it apart from a genuinely one-sided intent.
    """
    with pytest.raises(CoreMandateError, match=message):
        validate_core_mandate(**{**_VALID, **overrides})


@pytest.mark.parametrize(
    "overrides",
    [
        # One quantum inside each dead point, which is what makes the tests above
        # about REACHABILITY rather than about narrow bands.
        {
            "core_target_pct": Decimal("20"),
            "rebalance_band_pct": Decimal("19.9999"),
            "liquidity_reserve_pct": Decimal("0"),
        },
        {
            "core_target_pct": Decimal("60"),
            "rebalance_band_pct": Decimal("39.9999"),
            "liquidity_reserve_pct": Decimal("0"),
        },
        # The smallest mandate NUMERIC(8,4) can express at all: both bounds need
        # 0.0001 of clearance, so this is target 0.0002 / band 0.0001.
        {
            "core_target_pct": Decimal("0.0002"),
            "rebalance_band_pct": Decimal("0.0001"),
            "liquidity_reserve_pct": Decimal("0"),
        },
    ],
)
def test_the_narrowest_mandates_with_both_triggers_live_are_accepted(overrides: dict[str, Decimal]) -> None:
    """#2670 tightened a bound; it must not have moved it further than one quantum."""
    values = validate_core_mandate(**{**_VALID, **overrides})
    assert values.core_target_pct - values.rebalance_band_pct > 0
    assert values.core_target_pct + values.rebalance_band_pct < Decimal("100")


def test_a_zero_band_is_refused() -> None:
    """A zero band authorises a rebalance on any drift; turnover is the
    first-order cost filter, so a mandate that cannot state a band is not
    storable."""
    with pytest.raises(CoreMandateError, match="rebalance_band_pct must be above 0"):
        validate_core_mandate(**{**_VALID, "rebalance_band_pct": Decimal("0")})


@pytest.mark.parametrize("currency", ["GBP", "EUR", "gbp"])
def test_non_usd_is_refused_whatever_the_case(currency: str) -> None:
    """#2603 item 4: the deferral is total. Six sites lift together or none do."""
    with pytest.raises(CoreMandateError, match="must be USD"):
        validate_core_mandate(**{**_VALID, "base_currency": currency})


def test_lowercase_usd_is_normalised_rather_than_refused() -> None:
    """`usd` is the same mandate, so it must not reach the CHECK as a raw
    constraint violation."""
    assert validate_core_mandate(**{**_VALID, "base_currency": "usd"}).base_currency == "USD"


def test_an_enabled_mandate_without_an_instrument_is_refused() -> None:
    with pytest.raises(CoreMandateError, match="requires a core instrument"):
        validate_core_mandate(**{**_VALID, "core_instrument_id": None})


def test_a_disabled_mandate_may_omit_the_instrument() -> None:
    """Disabling must not force the operator to discard the rest of the mandate."""
    values = validate_core_mandate(**{**_VALID, "enabled": False, "core_instrument_id": None})
    assert values.enabled is False
    assert values.core_instrument_id is None


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("core_target_pct", Decimal("101"), "core_target_pct must be between 0 and 100"),
        ("core_target_pct", Decimal("-1"), "core_target_pct must be between 0 and 100"),
        ("liquidity_reserve_pct", Decimal("100"), "liquidity_reserve_pct must be at least 0"),
        ("liquidity_reserve_pct", Decimal("-1"), "liquidity_reserve_pct must be at least 0"),
        ("min_rebalance_amount", Decimal("0"), "min_rebalance_amount must be above 0"),
        ("min_rebalance_amount", Decimal("-5"), "min_rebalance_amount must be above 0"),
    ],
)
def test_out_of_range_values_are_refused(field: str, value: Decimal, message: str) -> None:
    with pytest.raises(CoreMandateError, match=message):
        validate_core_mandate(**{**_VALID, field: value})


def test_precision_finer_than_the_column_is_refused_not_rounded() -> None:
    """Postgres would round 8 decimal places into NUMERIC(8,4) silently.

    A stored mandate differing from the requested one is the failure; refusing is
    the only way the two stay the same number.
    """
    with pytest.raises(CoreMandateError, match="more precision than 4 decimal places"):
        validate_core_mandate(**{**_VALID, "core_target_pct": Decimal("60.000012")})


def test_trailing_zeros_are_not_mistaken_for_precision() -> None:
    """`60.00000` is exact at four places. Testing the exponent instead of
    representability would reject it."""
    values = validate_core_mandate(**{**_VALID, "core_target_pct": Decimal("60.00000")})
    assert values.core_target_pct == Decimal("60")


@pytest.mark.parametrize(
    "field,message",
    [
        ("core_target_pct", "core_target_pct must be between 0 and 100"),
        ("liquidity_reserve_pct", "liquidity_reserve_pct must be at least 0 and below 100"),
        ("rebalance_band_pct", "rebalance_band_pct must be above 0 and at most 100"),
    ],
)
def test_an_out_of_range_percentage_reports_its_range_not_its_digit_count(field: str, message: str) -> None:
    """Check ORDER, which is the reason the range checks precede storability.

    `10000` violates both the [0,100] range and NUMERIC(8,4)'s four integer
    digits. The range is the apter message, so it must win. Review round 1 read
    the precision bound as dead code for percentages; it is reachable, and this
    ordering is what makes it unreachable *through this function* — deliberately,
    since the bound stays as a generic backstop in the shared helper.

    Parametrised over all three percentage fields per review round 2: the
    docstring's "unreachable through this function" claim covers the whole
    percentage surface, so a guard on one field would leave two unpinned.
    """
    with pytest.raises(CoreMandateError, match=message):
        validate_core_mandate(**{**_VALID, field: Decimal("10000")})


def test_a_non_finite_percentage_is_caught_before_any_comparison() -> None:
    """`Decimal("NaN") < 0` raises InvalidOperation rather than returning False,
    so finiteness must precede every range check or a NaN escapes as a raw
    arithmetic error instead of a named one."""
    with pytest.raises(CoreMandateError, match="must be a finite decimal"):
        validate_core_mandate(**{**_VALID, "core_target_pct": Decimal("NaN")})


def test_an_amount_too_large_for_the_column_is_a_named_error() -> None:
    """NUMERIC(18,6) holds 12 integer digits.

    Without the precision half of the check, 10**12 passes validation and then
    fails in Postgres with NumericValueOutOfRange — a raw driver error where the
    validator promised a named one. Found by Codex at checkpoint 2.
    """
    with pytest.raises(CoreMandateError, match="exceeds 12 integer digits"):
        validate_core_mandate(**{**_VALID, "min_rebalance_amount": Decimal(10) ** 12})


def test_the_largest_storable_amount_is_accepted() -> None:
    """The boundary itself, so the bound cannot drift a digit either way."""
    largest = Decimal(10) ** 12 - Decimal("0.000001")
    assert validate_core_mandate(**{**_VALID, "min_rebalance_amount": largest}).min_rebalance_amount == largest


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity")])
def test_non_finite_values_are_refused(value: Decimal) -> None:
    with pytest.raises(CoreMandateError, match="must be a finite decimal"):
        validate_core_mandate(**{**_VALID, "core_target_pct": value})
