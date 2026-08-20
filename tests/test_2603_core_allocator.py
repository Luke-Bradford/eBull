"""#2603 item 3: the core/cash rebalance decision.

Pure-logic tier — no DB, no clock, no broker.  Spec:
``docs/proposals/ta/2026-08-13-core-cash-allocator.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.services.strategy_core_allocator import (
    CoreRebalanceDecision,
    CoreSleeveState,
    evaluate_core_rebalance,
)
from app.services.strategy_core_mandate import CORE_MANDATE_POLICY_VERSION, CoreMandate

AS_OF = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
CORE_ID = 4242


def mandate(
    *,
    enabled: bool = True,
    core_instrument_id: int | None = CORE_ID,
    target: str = "60",
    reserve: str = "5",
    band: str = "5",
    # Low enough that the default sleeve's trades clear it: a fixture floor that
    # silently suppresses is a test that passes for the wrong reason.
    floor: str = "1",
    base_currency: str = "USD",
    policy_version: str = CORE_MANDATE_POLICY_VERSION,
) -> CoreMandate:
    return CoreMandate(
        event_id=1,
        revision=1,
        enabled=enabled,
        base_currency=base_currency,
        core_instrument_id=core_instrument_id,
        core_target_pct=Decimal(target),
        liquidity_reserve_pct=Decimal(reserve),
        rebalance_band_pct=Decimal(band),
        min_rebalance_amount=Decimal(floor),
        policy_version=policy_version,
    )


def sleeve(core: str, cash: str, *, instrument_id: int = CORE_ID, currency: str = "USD") -> CoreSleeveState:
    return CoreSleeveState(
        core_instrument_id=instrument_id,
        core_market_value=Decimal(core),
        cash_balance=Decimal(cash),
        currency=currency,
        as_of=AS_OF,
    )


def test_in_band_holds_without_a_reason_code() -> None:
    # 60/40 on a 60±5 mandate: dead on target.
    decision = evaluate_core_rebalance(mandate(), sleeve("600", "400"))
    assert decision.action == "hold"
    assert decision.reason_code is None
    assert decision.amount == 0
    assert decision.core_pct == Decimal("60")
    # Cash 40% against a 5% reserve.
    assert decision.reserve_margin_pct == Decimal("35")
    assert decision.reserve_breached is False


@pytest.mark.parametrize(
    ("core", "cash", "action", "amount"),
    [
        # 70% core on 60±5 -> sell to the 65 edge: 0.65*1000 - 700 = -50.
        ("700", "300", "sell_core", "50"),
        # 50% core on 60±5 -> buy to the 55 edge: 0.55*1000 - 500 = 50.
        ("500", "500", "buy_core", "50"),
    ],
)
def test_outside_the_band_trades_to_the_near_edge_not_the_target(
    core: str, cash: str, action: str, amount: str
) -> None:
    """Leland (2000): trade to the region's boundary, not to the target.

    Trading to target would move 100 in both rows; the edge moves 50.
    """
    decision = evaluate_core_rebalance(mandate(), sleeve(core, cash))
    assert (decision.action, decision.amount) == (action, Decimal(amount))
    assert decision.reason_code is None


@pytest.mark.parametrize("core_pct", ["65", "55"])
def test_the_band_edge_itself_is_inside_the_band(core_pct: str) -> None:
    """Strictly outside, by construction: an allowance consumed exactly is still
    within the allowance."""
    core = Decimal(core_pct) * 10
    decision = evaluate_core_rebalance(mandate(), sleeve(str(core), str(1000 - core)))
    assert decision.action == "hold"
    assert decision.reason_code is None


def test_a_trade_lands_on_the_edge_it_was_sized_to() -> None:
    decision = evaluate_core_rebalance(mandate(), sleeve("700", "300"))
    assert decision.upper_pct == Decimal("65")
    # Post-trade cash is 35%, against a 5% reserve.
    assert decision.reserve_margin_pct == Decimal("30")


# --- Q1: a reserve breach strictly implies an upper-band breach ----------------


def test_reserve_breach_always_implies_a_band_breach() -> None:
    """Q1's proof, exercised across the schema-valid mandate space.

    For any mandate satisfying sql/336's CHECKs, `cash_pct < reserve` cannot occur
    without `core_pct > upper`.  If it could, the allocator would need a second
    trigger it does not have.
    """
    checked = 0
    for target in range(0, 101, 5):
        for band in range(1, 101, 3):
            for reserve in range(0, 100, 7):
                # The schema's three band CHECKs, as of sql/344.  Kept in step
                # with the migration deliberately: filtering by the allocator's
                # own refusal instead would let the enumerated space silently
                # follow the code under test, and a tightening would then narrow
                # coverage rather than fail.
                if target - band <= 0:
                    continue
                if target + band >= 100:
                    continue
                if 100 - (target + band) < reserve:
                    continue
                spec = mandate(target=str(target), band=str(band), reserve=str(reserve), floor="0.000001")
                for core_pct in range(0, 101):
                    state = sleeve(str(core_pct * 10), str((100 - core_pct) * 10))
                    decision = evaluate_core_rebalance(spec, state)
                    assert decision.action != "refused", (
                        f"a schema-valid mandate was refused ({decision.reason_code}): "
                        f"target={target} band={band} reserve={reserve} core_pct={core_pct}"
                    )
                    checked += 1
                    assert decision.reserve_breached is not None
                    if decision.reserve_breached:
                        assert decision.action == "sell_core", (
                            f"reserve breach not caught by the band: target={target} band={band} "
                            f"reserve={reserve} core_pct={core_pct}"
                        )
    assert checked > 10_000, f"coverage collapsed to {checked} states"


def test_a_directly_constructed_invalid_mandate_is_refused_not_computed() -> None:
    """The Q1 proof assumes a schema-valid mandate, and `CoreMandate` is a public
    frozen dataclass anyone can construct."""
    # Band drives the worst-case cash straight through the reserve: 100-(90+9)=1 < 20.
    rogue = mandate(target="90", band="9", reserve="20")
    decision = evaluate_core_rebalance(rogue, sleeve("950", "50"))
    assert decision.action == "refused"
    assert decision.reason_code == "core_mandate_invalid"
    assert decision.reserve_breached is None


# --- Q4: the floor wins, and the breach it leaves is bounded -------------------


def test_floor_suppresses_the_trade_and_reports_the_breach_it_leaves() -> None:
    # 96% core on a 60±5 mandate with a 4% reserve: cash 4% < 4%? no -- use 5%.
    spec = mandate(target="60", band="5", reserve="5", floor="1000000")
    decision = evaluate_core_rebalance(spec, sleeve("960", "40"))
    assert decision.action == "hold"
    assert decision.reason_code == "below_min_rebalance_amount"
    assert decision.amount == 0
    assert decision.reserve_breached is True
    # Reported against the CURRENT state, because no trade happens: 4% - 5%.
    assert decision.reserve_margin_pct == Decimal("-1")
    assert decision.effective_floor == Decimal("1000000")
    assert decision.floor_source == "mandate"


def test_the_suppressed_breach_never_exceeds_the_effective_floor() -> None:
    """Q4's bound: sell-to-edge >= the currency shortfall, so a suppressed sell
    leaves a shortfall below the floor.

    ⚠ Proved on the raw pre-cost amount; the implementation compares the ROUNDED
    amount, so the bound carries one quantum of slack.  Asserted with that slack.
    """
    quantum = Decimal("0.000001")
    for reserve in ("1", "5", "12.5"):
        for floor in ("10", "250.5", "9999"):
            spec = mandate(target="60", band="5", reserve=reserve, floor=floor)
            for core in range(880, 1000):
                state = sleeve(str(core), str(1000 - core))
                decision = evaluate_core_rebalance(spec, state)
                if decision.reason_code != "below_min_rebalance_amount":
                    continue
                if not decision.reserve_breached:
                    continue
                shortfall = (Decimal(reserve) - (Decimal(1000 - core) / 10)) / 100 * 1000
                assert shortfall <= Decimal(floor) + quantum, (
                    f"shortfall {shortfall} exceeded floor {floor} at core={core}"
                )


def test_a_broker_minimum_raises_the_floor_and_says_so() -> None:
    spec = mandate(floor="10")
    state = sleeve("700", "300")  # a 50 sell
    assert evaluate_core_rebalance(spec, state, broker_minimum=Decimal("20")).action == "sell_core"
    suppressed = evaluate_core_rebalance(spec, state, broker_minimum=Decimal("75"))
    assert suppressed.action == "hold"
    assert suppressed.reason_code == "below_min_rebalance_amount"
    assert (suppressed.effective_floor, suppressed.floor_source) == (Decimal("75"), "broker")


def test_a_broker_minimum_below_the_mandate_floor_does_not_lower_it() -> None:
    decision = evaluate_core_rebalance(mandate(floor="100"), sleeve("700", "300"), broker_minimum=Decimal("1"))
    assert (decision.effective_floor, decision.floor_source) == (Decimal("100"), "mandate")


def test_a_tie_reports_the_mandate_as_the_floor_source() -> None:
    """The applied value is identical either way; the mandate is the floor that
    always exists."""
    decision = evaluate_core_rebalance(mandate(floor="100"), sleeve("700", "300"), broker_minimum=Decimal("100"))
    assert (decision.effective_floor, decision.floor_source) == (Decimal("100"), "mandate")


def test_equality_with_the_floor_acts() -> None:
    decision = evaluate_core_rebalance(mandate(floor="50"), sleeve("700", "300"))
    assert (decision.action, decision.amount) == ("sell_core", Decimal("50"))


# --- rounding ------------------------------------------------------------------


def test_rounding_down_never_overshoots_and_cannot_re_trigger() -> None:
    """A residual below one quantum is below any storable floor, so the next pass
    suppresses it rather than trading again."""
    # Sleeve of 3 forces a repeating division: 0.65*3 - 2.1 = -0.15 exactly, so
    # pick one that does not divide cleanly.
    spec = mandate(target="60", band="5", floor="0.000001")
    state = sleeve("7", "3")  # 70% of 10 -> sell 0.5 exactly
    first = evaluate_core_rebalance(spec, state)
    assert first.action == "sell_core"
    # Apply it and re-evaluate: the post-trade state is inside the band.
    after = sleeve(str(Decimal("7") - first.amount), str(Decimal("3") + first.amount))
    second = evaluate_core_rebalance(spec, after)
    assert second.action == "hold"
    assert second.reason_code is None


def test_a_repeating_division_rounds_down_and_settles_next_pass() -> None:
    spec = mandate(target="33.3333", band="1", floor="0.000001")
    state = sleeve("700", "300")
    first = evaluate_core_rebalance(spec, state)
    assert first.action == "sell_core"
    assert first.amount == first.amount.quantize(Decimal("0.000001"))
    after = sleeve(str(Decimal("700") - first.amount), str(Decimal("300") + first.amount))
    second = evaluate_core_rebalance(spec, after)
    assert second.action == "hold", second
    assert second.reason_code in (None, "below_min_rebalance_amount")


def test_no_zero_amount_trade_is_ever_emitted() -> None:
    spec = mandate(target="60", band="5", floor="0.000001")
    for core in range(0, 1001):
        decision = evaluate_core_rebalance(spec, sleeve(str(core), str(1000 - core)))
        if decision.action in ("buy_core", "sell_core"):
            assert decision.amount > 0


# --- refusals, in precedence order --------------------------------------------


def test_absent_mandate_is_a_state_not_a_default() -> None:
    decision = evaluate_core_rebalance(None, sleeve("600", "400"))
    assert decision.reason_code == "core_mandate_absent"
    assert decision.core_pct is None


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        # v3 is a LATER policy; v1 is the one #2670 superseded. Both unsupported,
        # for opposite reasons, and the refusal must not distinguish them.
        (mandate(policy_version="core-mandate-v3"), "core_mandate_policy_unsupported"),
        (mandate(policy_version="core-mandate-v1"), "core_mandate_policy_unsupported"),
        (mandate(enabled=False, core_instrument_id=None), "core_mandate_disabled"),
        (mandate(target="90", band="9", reserve="20"), "core_mandate_invalid"),
    ],
)
def test_mandate_refusals(spec: CoreMandate, expected: str) -> None:
    decision = evaluate_core_rebalance(spec, sleeve("600", "400"))
    assert decision.action == "refused"
    assert decision.reason_code == expected


def test_policy_version_is_checked_before_validity() -> None:
    """A row written under a later policy must not be blamed for our staleness."""
    stale = mandate(policy_version="core-mandate-v3", target="90", band="9", reserve="20")
    assert evaluate_core_rebalance(stale, sleeve("600", "400")).reason_code == "core_mandate_policy_unsupported"


def test_enabled_mandate_without_an_instrument_is_refused() -> None:
    """Unreachable through the CHECK and the validator, reachable through the
    dataclass — which is exactly why the guard is here."""
    rogue = CoreMandate(
        event_id=1,
        revision=1,
        enabled=True,
        base_currency="USD",
        core_instrument_id=None,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("5"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("100"),
        policy_version=CORE_MANDATE_POLICY_VERSION,
    )
    # `validate_core_mandate` catches this one first, and either code is a refusal;
    # what must not happen is a computed verdict.
    decision = evaluate_core_rebalance(rogue, sleeve("600", "400"))
    assert decision.action == "refused"
    assert decision.reason_code in ("core_mandate_invalid", "core_instrument_unset")


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (sleeve("600", "400", currency="GBP"), "sleeve_currency_mismatch"),
        (sleeve("600", "400", instrument_id=CORE_ID + 1), "sleeve_instrument_mismatch"),
        (sleeve("-600", "400"), "sleeve_valuation_invalid"),
        (sleeve("600", "-400"), "sleeve_valuation_invalid"),
        (sleeve("NaN", "400"), "sleeve_valuation_invalid"),
        (sleeve("Infinity", "400"), "sleeve_valuation_invalid"),
        # 12 integer digits is the NUMERIC(18,6) ceiling.
        (sleeve("999999999999", "1"), "sleeve_valuation_invalid"),
        # Two components that each clear the bound but sum past it.
        (sleeve("600000000000", "600000000000"), "sleeve_valuation_invalid"),
        # Finite, and near Decimal's Emax: the SUM raises decimal.Overflow, so the
        # per-component check must run first or the refusal never happens.
        (sleeve("9e999999", "9e999999"), "sleeve_valuation_invalid"),
        (sleeve("0", "0"), "core_sleeve_empty"),
    ],
)
def test_state_refusals(state: CoreSleeveState, expected: str) -> None:
    decision = evaluate_core_rebalance(mandate(), state)
    assert decision.action == "refused"
    assert decision.reason_code == expected


def test_a_sleeve_just_under_the_bound_is_accepted() -> None:
    decision = evaluate_core_rebalance(mandate(), sleeve("999999999998", "1"))
    assert decision.action != "refused"


@pytest.mark.parametrize("bad", ["0", "-1", "NaN", "Infinity"])
def test_an_invalid_broker_minimum_is_refused(bad: str) -> None:
    decision = evaluate_core_rebalance(mandate(), sleeve("700", "300"), broker_minimum=Decimal(bad))
    assert decision.reason_code == "broker_minimum_invalid"


def test_currency_is_compared_case_and_whitespace_insensitively() -> None:
    decision = evaluate_core_rebalance(mandate(), sleeve("600", "400", currency=" usd "))
    assert decision.action == "hold"


def test_a_refusal_nulls_the_arithmetic_it_could_not_compute() -> None:
    decision: CoreRebalanceDecision = evaluate_core_rebalance(mandate(), sleeve("0", "0"))
    assert (decision.core_pct, decision.upper_pct, decision.lower_pct) == (None, None, None)
    assert (decision.reserve_breached, decision.reserve_margin_pct) == (None, None)
    assert (decision.effective_floor, decision.floor_source) == (None, None)


# --- the degenerate mandates, now refused at the source (#2670) ----------------
#
# These two shapes were storable under sql/336 and reached the allocator, which
# computed correctly on them while one trigger could never fire.  sql/344 makes
# both unstorable and `validate_core_mandate` rejects them, so the allocator now
# meets them only as directly-constructed objects — and refuses.  The tests are
# kept (rather than deleted with the defect) because the dataclass is public: the
# refusal is the only thing standing between a degenerate mandate and a verdict
# computed on a band that is silently one-sided.


@pytest.mark.parametrize(
    ("spec", "dead_side"),
    [
        # band == target -> lower == 0, and `core_pct < 0` is unreachable.
        (mandate(target="20", band="20", reserve="0", floor="0.000001"), "lower"),
        # target + band == 100 at reserve 0 -> upper == 100, `core_pct > 100`
        # unreachable.  Storable under sql/336; rejected by sql/344.
        (mandate(target="60", band="40", reserve="0", floor="0.000001"), "upper"),
    ],
)
def test_a_mandate_with_a_dead_trigger_is_refused_not_computed(spec: CoreMandate, dead_side: str) -> None:
    """#2670. The extreme state on the dead side is the probe: an empty core is
    the most extreme underweight there is, and a fully-invested one the most
    extreme overweight, so if the trigger were merely *narrow* rather than dead
    this would fire."""
    extreme = sleeve("0", "1000") if dead_side == "lower" else sleeve("1000", "0")
    decision = evaluate_core_rebalance(spec, extreme)
    assert decision.action == "refused"
    assert decision.reason_code == "core_mandate_invalid"


def test_the_dead_trigger_refusal_is_what_changed_and_not_the_arithmetic() -> None:
    """Guards the claim in sql/344's header that the allocator was never wrong.

    The same band width one quantum inside the dead point is storable, reaches
    the arithmetic, and fires — so the refusal above is about reachability and
    not about narrow bands.
    """
    spec = mandate(target="20", band="19.9999", reserve="0", floor="0.000001")
    decision = evaluate_core_rebalance(spec, sleeve("0", "1000"))
    assert decision.lower_pct == Decimal("0.0001")
    assert decision.action == "buy_core"
