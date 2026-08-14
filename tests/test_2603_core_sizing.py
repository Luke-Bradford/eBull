"""#2603 step 3b-2 item 1: re-solving a core rebalance size against the quoted cost.

Pure-logic tier — no DB, no clock, no broker.  Spec:
``docs/proposals/ta/2026-08-14-core-rebalance-cost-sizing.md``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from app.providers.broker import BrokerCostComponent, BrokerWhatIfCostResponse
from app.services.strategy_core_allocator import CoreSleeveState, evaluate_core_rebalance
from app.services.strategy_core_mandate import PERCENT_BASIS
from app.services.strategy_core_sizing import (
    CoreSizingContractError,
    QuotedTradeCost,
    decode_quoted_trade_cost,
    resolve_core_trade_size,
)
from tests.test_2603_core_allocator import AS_OF, CORE_ID, mandate, sleeve


def cost(
    *,
    bound: str = "1",
    ticket: str = "100",
    instrument_id: int = CORE_ID,
    currency: str = "USD",
    last_updated: datetime = AS_OF,
) -> QuotedTradeCost:
    return QuotedTradeCost(
        instrument_id=instrument_id,
        ticket_amount=Decimal(ticket),
        cost_upper_bound=Decimal(bound),
        bound_source="quoted",
        currency=currency,
        last_updated=last_updated,
    )


def response(
    rows: list[tuple[str, str | None, str | None, str]],
    *,
    instrument_id: int = CORE_ID,
    last_updated: datetime = AS_OF,
) -> BrokerWhatIfCostResponse:
    return BrokerWhatIfCostResponse(
        instrument_id=instrument_id,
        symbol="IVV",
        costs=tuple(
            BrokerCostComponent(
                cost_type=cost_type,
                amount=None if amount is None else Decimal(amount),
                value=None if value is None else Decimal(value),
                currency=currency,
                raw_payload={},
            )
            for cost_type, amount, value, currency in rows
        ),
        last_updated=last_updated,
        raw_payload={},
    )


def decode(rows: list[tuple[str, str | None, str | None, str]], **kwargs: object) -> object:
    return decode_quoted_trade_cost(
        response(rows, **{k: v for k, v in kwargs.items() if k in {"instrument_id", "last_updated"}}),  # type: ignore[arg-type]
        instrument_id=CORE_ID,
        ticket_amount=Decimal("1000"),
        base_currency="USD",
        valuation_as_of=AS_OF,
    )


# --------------------------------------------------------------------------
# The arithmetic
# --------------------------------------------------------------------------


def test_a_buy_is_sized_LARGER_than_the_pre_cost_amount() -> None:
    """A price-embedded cost shrinks the numerator AND the denominator, so the
    pre-cost ticket undershoots the near edge.  Q3's cash-fee correction makes a
    buy SMALLER — the opposite direction, which is the whole point of the re-derive.
    """
    # 50/50 on a 60±5 mandate: core 50% is below the 55 lower edge.
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    assert decision.action == "buy_core"

    sized = resolve_core_trade_size(m, s, decision, cost(bound="1", ticket="50"))
    assert sized.sized
    assert sized.amount > decision.amount


def test_a_sell_is_sized_LARGER_too() -> None:
    # 80/20 on a 60±5 mandate: core 80% is above the 65 upper edge.
    m, s = mandate(), sleeve("800", "200")
    decision = evaluate_core_rebalance(m, s)
    assert decision.action == "sell_core"

    sized = resolve_core_trade_size(m, s, decision, cost(bound="1", ticket="150"))
    assert sized.sized
    assert sized.amount > decision.amount


def core_pct_at(state: CoreSleeveState, amount: Decimal, gamma: Decimal, *, buying: bool) -> Decimal:
    """Post-trade core weight for a ticket of ``amount`` at cost rate ``gamma``.

    Written out here rather than imported so the test measures the spec's arithmetic
    against the module's, not the module against itself.
    """
    core = state.core_market_value
    sleeve_value = core + state.cash_balance
    if buying:
        return PERCENT_BASIS * (core + amount * (1 - gamma)) / (sleeve_value - gamma * amount)
    return PERCENT_BASIS * (core - amount) / (sleeve_value - gamma * amount)


def test_the_near_edge_is_RESTORED_at_the_cost_bound() -> None:
    """The property the whole slice exists for: at mu = gamma the post-trade weight
    is back inside the band, not merely closer to it — AND the pre-cost amount would
    not have been.  The second half is the control: without it, "restored" is
    unfalsifiable, because a size already inside the band restores it trivially.
    """
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    assert decision.lower_pct is not None
    gamma = Decimal("1") / Decimal("50")  # the fixture's bound / ticket

    # Control arm: the allocator's own pre-cost size lands SHORT of the near edge.
    assert core_pct_at(s, decision.amount, gamma, buying=True) < decision.lower_pct

    sized = resolve_core_trade_size(m, s, decision, cost(bound="1", ticket="50"))
    assert sized.resulting_core_pct_at_bound is not None
    # Rounding is ROUND_UP, so the near edge is reached or passed — never left short.
    assert sized.resulting_core_pct_at_bound >= decision.lower_pct
    # And independently recomputed, not just read back off the result.
    assert core_pct_at(s, sized.amount, gamma, buying=True) >= decision.lower_pct


def test_a_zero_cost_bound_reproduces_the_pre_cost_amount() -> None:
    """gamma = 0 must collapse to the allocator's own answer, or the correction is
    not a correction — it is a different formula that happens to be near.
    """
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    sized = resolve_core_trade_size(m, s, decision, cost(bound="0", ticket="50"))
    assert sized.sized
    assert sized.amount == decision.amount


def test_the_reserve_holds_without_being_checked_separately() -> None:
    """Post-trade cash fraction is 1 - core fraction, and the band bounds the core
    fraction by ``upper <= 100 - reserve`` (sql/336:50).  Asserted on the returned
    numbers so the proof is not only prose.
    """
    m, s = mandate(target="60", reserve="35", band="5"), sleeve("800", "200")
    decision = evaluate_core_rebalance(m, s)
    assert decision.action == "sell_core"
    sized = resolve_core_trade_size(m, s, decision, cost(bound="1", ticket="150"))
    assert sized.sized
    assert sized.resulting_core_pct_at_bound is not None
    assert sized.resulting_core_pct_at_zero_cost is not None
    # This mandate sits at the CHECK's boundary: 100 - (60 + 5) == 35 == reserve.
    for weight in (sized.resulting_core_pct_at_bound, sized.resulting_core_pct_at_zero_cost):
        assert PERCENT_BASIS - weight >= m.liquidity_reserve_pct


# --------------------------------------------------------------------------
# Refusals
# --------------------------------------------------------------------------


def test_a_cost_rate_of_one_or_more_refuses() -> None:
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    sized = resolve_core_trade_size(m, s, decision, cost(bound="50", ticket="50"))
    assert not sized.sized
    assert sized.refusal_code == "cost_rate_implausible"
    assert sized.amount == 0


def test_a_solve_far_from_its_own_quoted_ticket_refuses() -> None:
    """The linearity that makes the closed form exact was measured over one decade.
    A solve that lands 3x from the ticket it was quoted for is extrapolating.
    """
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    sized = resolve_core_trade_size(m, s, decision, cost(bound="0.01", ticket="10"))
    assert not sized.sized
    assert sized.refusal_code == "cost_quote_ticket_mismatch"


def test_the_far_edge_refusal_IS_REACHABLE() -> None:
    """A narrow band and a large cost rate: the zero-cost end of the bracket lands
    past ``upper``, so no size restores the band across the whole interval.
    """
    m, s = mandate(target="10", band="1", reserve="0"), sleeve("0", "1000")
    decision = evaluate_core_rebalance(m, s)
    assert decision.action == "buy_core"
    sized = resolve_core_trade_size(
        m, s, decision, cost(bound=str(Decimal("0.2") * decision.amount), ticket=str(decision.amount))
    )
    assert not sized.sized
    assert sized.refusal_code == "cost_breaches_far_edge"


def test_no_schema_valid_input_can_make_a_buy_unfundable() -> None:
    """The reachability sweep, and the reason there is no ``cost_exceeds_available_cash``.

    ``A > C`` reduces to ``gamma > V / C``, and ``C <= V`` puts that at 1 or above, so
    ``cost_rate_implausible`` always refuses first.  Asserted over every
    ``(target, band, reserve) x sleeve x gamma`` the schema admits rather than left as
    algebra — a refusal that cannot fire is #2437's R4 shape, and the sweep is what
    would catch a future change that makes one reachable (or this one unreachable).
    """
    from itertools import product

    from app.services.strategy_core_mandate import CoreMandateError

    fired: set[str] = set()
    overspends = 0
    for target, band, reserve in product(["10", "30", "50", "60", "80", "95"], ["1", "5", "10", "20"], ["0", "1", "5"]):
        try:
            m = mandate(target=target, band=band, reserve=reserve)
        except CoreMandateError:
            continue
        if evaluate_core_rebalance(m, sleeve("500", "500")).reason_code == "core_mandate_invalid":
            continue
        for core, cash in [("0", "1000"), ("1", "999"), ("500", "500"), ("999", "1"), ("990", "10")]:
            s = sleeve(core, cash)
            decision = evaluate_core_rebalance(m, s)
            if decision.action not in ("buy_core", "sell_core"):
                continue
            for gamma in ["0.001", "0.01", "0.1", "0.2", "0.4", "0.9", "0.99"]:
                ticket = decision.amount
                result = resolve_core_trade_size(
                    m, s, decision, cost(bound=str(Decimal(gamma) * ticket), ticket=str(ticket))
                )
                fired.add(result.refusal_code or "sized")
                if result.sized and decision.action == "buy_core" and result.amount > s.cash_balance:
                    overspends += 1

    assert overspends == 0
    # The sweep must actually exercise both arms, or "never overspends" is vacuous.
    assert "sized" in fired
    assert "cost_breaches_far_edge" in fired


def test_a_wrong_instrument_quote_refuses() -> None:
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    sized = resolve_core_trade_size(m, s, decision, cost(instrument_id=CORE_ID + 1, ticket="50"))
    assert not sized.sized
    assert sized.refusal_code == "cost_quote_unusable"


def test_a_wrong_currency_quote_refuses() -> None:
    m, s = mandate(), sleeve("500", "500")
    decision = evaluate_core_rebalance(m, s)
    sized = resolve_core_trade_size(m, s, decision, cost(currency="GBP", ticket="50"))
    assert not sized.sized
    assert sized.refusal_code == "cost_quote_unusable"


def test_a_hold_cannot_be_sized() -> None:
    m, s = mandate(), sleeve("600", "400")
    decision = evaluate_core_rebalance(m, s)
    assert decision.action == "hold"
    with pytest.raises(CoreSizingContractError):
        resolve_core_trade_size(m, s, decision, cost())


def test_a_decision_from_a_DIFFERENT_sleeve_raises() -> None:
    """The #2704 shape: every field is well-formed and the answer is for the wrong
    sleeve.  A docstring obligation would not catch it.
    """
    m = mandate()
    decision = evaluate_core_rebalance(m, sleeve("500", "500"))
    with pytest.raises(CoreSizingContractError):
        resolve_core_trade_size(m, sleeve("520", "480"), decision, cost(ticket="50"))


# --------------------------------------------------------------------------
# Decoding one what-if response
# --------------------------------------------------------------------------


def test_every_component_is_summed_not_just_marketSpread() -> None:
    """The vocabulary is provider-owned.  A component we do not recognise must widen
    the bound, never be dropped.
    """
    decoded = decode(
        [
            ("marketSpread", None, "0.90", "USD"),
            ("someFeeWeHaveNeverSeen", None, "0.50", "USD"),
        ]
    )
    assert isinstance(decoded, QuotedTradeCost)
    # 0.90 + 0.50 + six rounding quanta (see the per-component slack test).
    assert decoded.cost_upper_bound == Decimal("1.46")


def test_an_absent_marketSpread_row_is_bounded_not_zeroed() -> None:
    """The tightest names — which is what a core sleeve holds — are exactly the ones
    whose row is omitted.  Zeroing prices them as free; refusing refuses the core
    rebalance permanently while looking transient (the 3b-1 shape).
    """
    decoded = decode([("markup", None, "0", "USD")])
    assert isinstance(decoded, QuotedTradeCost)
    assert decoded.cost_upper_bound > Decimal("0")
    assert decoded.bound_source == "rounding_quantum"


def test_rounding_slack_is_PER_COMPONENT_not_per_response() -> None:
    """Rounding is applied per row, so N rows carry N quanta of understatement
    independently.  Three rows reported as zero can each stand for a real cost just
    under 0.01, so a single quantum would bound the trio at 0.01 against a realised
    0.03 — an UNDER-statement of gamma, the one direction ``mu <= gamma`` cannot
    survive.  The count is the documented vocabulary (6), because an OMITTED component
    is exactly the one that rounded away and contributes no row to count.
    """
    three_zero_rows = decode(
        [
            ("markup", None, "0", "USD"),
            ("marketSpread", None, "0", "USD"),
            ("overnightFee", None, "0", "USD"),
        ]
    )
    assert isinstance(three_zero_rows, QuotedTradeCost)
    # The realised total can approach 0.03 on these three alone, and three more
    # components can be omitted and rounded away unseen.
    assert three_zero_rows.cost_upper_bound >= Decimal("0.03")
    assert three_zero_rows.cost_upper_bound == Decimal("0.06")


def test_a_huge_but_finite_component_refuses_instead_of_raising() -> None:
    """``is_finite()`` is TRUE for ``Decimal("9e999999")``, so such a value reaches the
    addition and raises ``decimal.Overflow`` — which is NOT an ``InvalidOperation``.
    The promised refusal must hold for every arithmetic failure, not the guessed one.
    """
    enormous = "9" + "9" * 0 + "e999999"
    assert decode([("markup", None, enormous, "USD"), ("marketSpread", None, enormous, "USD")]) == (
        "cost_quote_unusable"
    )


def test_a_present_but_null_value_refuses() -> None:
    """Malformed, not omitted — and unobserved, so it is refused rather than bounded
    by analogy with the omitted case.
    """
    assert decode([("marketSpread", None, None, "USD")]) == "cost_quote_unusable"


def test_amount_and_value_disagreeing_refuses() -> None:
    """#2598's rule is to preserve both and fail, never to silently prefer one."""
    assert decode([("marketSpread", "0.90", "0.95", "USD")]) == "cost_quote_unusable"


def test_amount_wins_when_it_agrees() -> None:
    decoded = decode([("marketSpread", "0.90", "0.90", "USD")])
    assert isinstance(decoded, QuotedTradeCost)
    assert decoded.cost_upper_bound == Decimal("0.96")


def test_a_per_row_currency_mismatch_refuses() -> None:
    """Per row, not once for the response: a single foreign row poisons the sum."""
    assert decode([("marketSpread", None, "0.90", "USD"), ("markup", None, "0.10", "GBP")]) == "cost_quote_unusable"


def test_a_negative_component_refuses() -> None:
    assert decode([("marketSpread", None, "-0.90", "USD")]) == "cost_quote_unusable"


def test_no_rows_at_all_refuses() -> None:
    """Not "no cost" — a response we cannot read.  Every decodable observation in the
    #2598 census returned three components.
    """
    assert decode([]) == "cost_quote_unusable"


def test_a_quote_older_than_the_valuation_refuses() -> None:
    """`lastUpdated` staleness is per-instrument and has been observed at 26 days."""
    stale = decode(
        [("marketSpread", None, "0.90", "USD")],
        last_updated=AS_OF - timedelta(minutes=5),
    )
    assert stale == "cost_quote_stale"


def test_a_quote_within_the_clock_skew_is_accepted() -> None:
    """Two clocks — the broker stamps its cost figure, we stamp our receipt of the
    position payload — so the rule needs a tolerance or it refuses on drift alone.
    """
    fresh = decode(
        [("marketSpread", None, "0.90", "USD")],
        last_updated=AS_OF - timedelta(seconds=10),
    )
    assert isinstance(fresh, QuotedTradeCost)


def test_a_response_for_another_instrument_refuses() -> None:
    assert decode([("marketSpread", None, "0.90", "USD")], instrument_id=CORE_ID + 1) == ("cost_quote_unusable")
