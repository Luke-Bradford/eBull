"""#2603 item 2 — the arm predicate and the eligibility verdict, as pure logic.

No database and no broker: everything here is a function of one parsed response.
The storage backstop and the mandate gate live in
``test_2603_core_eligibility_db``.
"""

from decimal import Decimal

import pytest

from app.providers.broker import (
    BrokerEligibilityResponse,
    BrokerInstrumentEligibility,
    BrokerLeverageConfig,
)
from app.services.broker_settlement_arms import (
    is_underlying_long_arm,
    offers_unleveraged,
    select_underlying_long_arms,
)
from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_REASONS,
    UNRESOLVED_REASONS,
    evaluate_core_eligibility,
    response_digest,
)

INSTRUMENT = 3417


def arm(
    settlement_type: str = "real",
    direction: str = "long",
    leverage_values: tuple[object, ...] = (1,),
    min_position_amount: Decimal | None = Decimal("10"),
) -> BrokerLeverageConfig:
    return BrokerLeverageConfig(
        settlement_type=settlement_type,
        direction=direction,
        leverage_values=leverage_values,  # type: ignore[arg-type]
        min_position_amount=min_position_amount,
        allow_edit_stop_loss=True,
        allow_edit_take_profit=True,
        allow_stop_loss_take_profit=True,
        raw_payload={},
    )


def row(
    *arms: BrokerLeverageConfig,
    instrument_id: int = INSTRUMENT,
    allow_open_position: bool = True,
    min_position_exposure: Decimal | None = Decimal("10"),
    max_units_per_order: Decimal | None = Decimal("134"),
) -> BrokerInstrumentEligibility:
    return BrokerInstrumentEligibility(
        instrument_id=instrument_id,
        symbol="SPY.RTH",
        min_position_exposure=min_position_exposure,
        max_units_per_order=max_units_per_order,
        allow_open_position=allow_open_position,
        allow_close_position=True,
        allow_partial_close_position=True,
        allow_trailing_stop_loss=True,
        leverage_configs=tuple(arms),
        raw_payload={},
    )


def response(
    *rows: BrokerInstrumentEligibility,
    currency: str = "usd",
    not_found: tuple[int, ...] = (),
    raw: dict[str, object] | None = None,
) -> BrokerEligibilityResponse:
    return BrokerEligibilityResponse(
        currency=currency,
        eligibilities=tuple(rows),
        not_found_instrument_ids=not_found,
        not_found_symbols=(),
        raw_payload=raw if raw is not None else {"currency": currency},
    )


# --------------------------------------------------------------------------
# The shared arm predicate
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "settlement_type",
    ["cfd", "realFutures", "marginTrade", "something_the_portal_has_not_documented"],
)
def test_only_real_is_the_underlying_product(settlement_type: str) -> None:
    """Every other documented value is a derivative or is leveraged.

    ``marginTrade`` IS the real instrument, so it is here for a reason the
    vocabulary alone does not give: it is held on margin, which the standing
    no-leverage posture bars. The undocumented value pins fail-closed.
    """
    assert not is_underlying_long_arm(arm(settlement_type=settlement_type))


def test_the_vocabulary_is_read_case_insensitively() -> None:
    """The live demo response answers `usd` where the request sent `USD`."""
    assert is_underlying_long_arm(arm(settlement_type=" REAL ", direction="LONG"))


def test_a_short_arm_is_not_the_core_holding() -> None:
    assert not is_underlying_long_arm(arm(direction="short"))


def test_a_leveraged_only_arm_is_not_unleveraged() -> None:
    assert not is_underlying_long_arm(arm(leverage_values=(2, 5, 10, 20)))


def test_a_boolean_true_does_not_prove_x1_eligibility() -> None:
    """⚠ ``bool`` subclasses ``int``, so ``1 in (True,)`` is True.

    The provider parser accepts ``leverageValues: [true]`` because
    ``isinstance(True, int)`` passes its integer check, so without this guard
    malformed broker data proves unleveraged eligibility. Revert-probe: drop the
    ``not isinstance(value, bool)`` conjunct and this goes green the wrong way.
    """
    assert not offers_unleveraged((True,))
    assert not is_underlying_long_arm(arm(leverage_values=(True,)))
    assert offers_unleveraged((1,))


def test_zero_and_many_qualifying_arms_are_distinguishable() -> None:
    """The helper returns ALL matches so callers can tell them apart."""
    assert select_underlying_long_arms(row(arm(settlement_type="cfd"))) == ()
    assert len(select_underlying_long_arms(row(arm(), arm()))) == 2


# --------------------------------------------------------------------------
# The verdict
# --------------------------------------------------------------------------


def test_the_spy_rth_shape_proves_the_underlying() -> None:
    """The measured SPY.RTH arm set: one real/long/x1 beside two CFD arms."""
    assessment = evaluate_core_eligibility(
        response(
            row(
                arm(),
                arm(settlement_type="cfd", direction="short", leverage_values=(1, 2, 5, 10, 20)),
                arm(settlement_type="cfd", leverage_values=(2, 5, 10, 20)),
            )
        ),
        instrument_id=INSTRUMENT,
    )
    assert assessment.verdict == "underlying"
    assert assessment.reason_code is None
    assert assessment.settlement_type == "real"
    assert assessment.direction == "long"
    assert assessment.qualifying_arm_count == 1
    assert assessment.leverage_values == (1,)


def test_the_plain_spy_shape_is_not_underlying_rather_than_ambiguous() -> None:
    """SPY's x1 long arm is a CFD, so the answer is a fact about the instrument.

    ``eligibility_arm_ambiguous`` would say the response could not be read. It
    was read perfectly: this fund is not offered as the underlying product.
    """
    assessment = evaluate_core_eligibility(
        response(
            row(
                arm(settlement_type="cfd"),
                arm(settlement_type="cfd", direction="short", leverage_values=(1, 2, 5, 10, 20)),
                arm(settlement_type="cfd", leverage_values=(2, 5, 10, 20)),
            )
        ),
        instrument_id=INSTRUMENT,
    )
    assert assessment.verdict == "not_underlying"
    assert assessment.reason_code == "no_underlying_arm"
    assert assessment.qualifying_arm_count == 0
    assert assessment.settlement_type is None


def test_a_failing_verdict_carries_no_arm_projection() -> None:
    """sql/346 refuses pass-shaped evidence on a failing row; so does the evaluator."""
    assessment = evaluate_core_eligibility(response(row(arm(), arm())), instrument_id=INSTRUMENT)
    assert assessment.verdict == "unresolved"
    assert assessment.reason_code == "eligibility_arm_ambiguous"
    assert (assessment.settlement_type, assessment.direction, assessment.leverage_values) == (
        None,
        None,
        None,
    )
    # But the count survives, which is what makes "exactly one arm" checkable later.
    assert assessment.qualifying_arm_count == 2


def test_a_not_found_instrument_is_unresolved_not_a_finding() -> None:
    assessment = evaluate_core_eligibility(response(not_found=(INSTRUMENT,)), instrument_id=INSTRUMENT)
    assert (assessment.verdict, assessment.reason_code) == (
        "unresolved",
        "instrument_not_resolved",
    )


def test_not_found_wins_over_a_row_that_also_came_back() -> None:
    """A response asserting both is not readable; report the more specific failure."""
    assessment = evaluate_core_eligibility(response(row(arm()), not_found=(INSTRUMENT,)), instrument_id=INSTRUMENT)
    assert assessment.reason_code == "instrument_not_resolved"


def test_duplicate_rows_for_one_id_are_unresolved() -> None:
    assessment = evaluate_core_eligibility(response(row(arm()), row(arm())), instrument_id=INSTRUMENT)
    assert assessment.reason_code == "eligibility_row_ambiguous"


def test_a_currency_the_request_did_not_ask_for_is_unresolved() -> None:
    """The minimums are denominated in it, so a mismatch makes them unreadable."""
    assessment = evaluate_core_eligibility(response(row(arm()), currency="gbp"), instrument_id=INSTRUMENT)
    assert assessment.reason_code == "eligibility_currency_mismatch"


def test_a_closed_instrument_is_not_underlying() -> None:
    assessment = evaluate_core_eligibility(response(row(arm(), allow_open_position=False)), instrument_id=INSTRUMENT)
    assert (assessment.verdict, assessment.reason_code) == ("not_underlying", "instrument_not_open")


def test_a_missing_minimum_does_not_block_the_product_proof() -> None:
    """Product identity is not order sizing.

    A genuine real/long/x1 arm IS the underlying product whether or not the
    response quoted a floor. Requiring one would conflate what the product is
    with whether an order can be sized, which is item 3's question.
    """
    assessment = evaluate_core_eligibility(
        response(row(arm(min_position_amount=None), min_position_exposure=None)),
        instrument_id=INSTRUMENT,
    )
    assert assessment.verdict == "underlying"
    assert assessment.min_position_amount is None
    assert assessment.min_position_exposure is None


@pytest.mark.parametrize("bogus", [Decimal("0"), Decimal("-1"), Decimal("NaN")])
def test_a_non_positive_or_non_finite_bound_is_recorded_as_not_quoted(bogus: Decimal) -> None:
    """The provider may OMIT a minimum; it may not assert a nonsense one.

    Dropping it to NULL keeps the observation storable (sql/346 refuses ``<= 0``)
    and records what the value actually tells us.
    """
    assessment = evaluate_core_eligibility(
        response(row(arm(min_position_amount=bogus), min_position_exposure=bogus)),
        instrument_id=INSTRUMENT,
    )
    assert assessment.verdict == "underlying"
    assert assessment.min_position_amount is None
    assert assessment.min_position_exposure is None


def test_every_reason_the_evaluator_can_emit_is_in_the_closed_vocabulary() -> None:
    """The frozenset mirrors sql/346's CHECK; a new code must land in both."""
    emitted = {
        evaluate_core_eligibility(payload, instrument_id=INSTRUMENT).reason_code
        for payload in (
            response(not_found=(INSTRUMENT,)),
            response(row(arm()), row(arm())),
            response(row(arm()), currency="gbp"),
            response(row(arm(), arm())),
            response(row(arm(), allow_open_position=False)),
            response(row(arm(settlement_type="cfd"))),
        )
    }
    assert emitted == CORE_ELIGIBILITY_REASONS
    assert UNRESOLVED_REASONS < CORE_ELIGIBILITY_REASONS


# --------------------------------------------------------------------------
# The digest
# --------------------------------------------------------------------------


def test_the_digest_ignores_key_order_but_not_a_new_field() -> None:
    """Deliberate asymmetry, stated in sql/346.

    Reordering is not drift; an ADDED field is, and this provider has shipped
    exactly that before (documented ``amount`` → undocumented ``value``).
    """
    a = response_digest({"currency": "usd", "eligibilities": []})
    b = response_digest({"eligibilities": [], "currency": "usd"})
    c = response_digest({"currency": "usd", "eligibilities": [], "newField": 1})
    assert a == b
    assert a != c
    assert len(a) == 64


def test_a_non_finite_number_is_an_error_not_a_digest() -> None:
    """``allow_nan=False``: NaN would otherwise serialise to invalid JSON."""
    with pytest.raises(ValueError):
        response_digest({"currency": "usd", "value": float("nan")})
