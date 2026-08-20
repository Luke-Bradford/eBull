"""The core sleeve observer (#2704) -- pure, no DB, no broker.

The behaviour under test is mostly REFUSAL, because the module's job is to turn a
snapshot that cannot describe the sleeve into a loud failure rather than a coherent
number answering a different question.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerInstrumentInvestment
from app.services.account_equity_evidence import DOCUMENTED_ACCOUNT_CURRENCIES
from app.services.strategy_core_allocator import CoreSleeveState, evaluate_core_rebalance
from app.services.strategy_core_mandate import CORE_MANDATE_POLICY_VERSION, CoreMandate
from app.services.strategy_core_sleeve import CoreSleeveObservationError, observe_core_sleeve

CORE_ID = 2449001
_NOW = datetime(2026, 8, 14, 13, 7, tzinfo=UTC)


def snapshot(
    *,
    investments: tuple[BrokerInstrumentInvestment, ...] = (),
    cash: str = "600",
    currency_id: int | None = 1,
    observed_at: datetime = _NOW,
) -> BrokerAccountRiskSnapshot:
    return BrokerAccountRiskSnapshot(
        available_cash=Decimal(cash),
        total_invested=Decimal("400"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("1000"),
        instrument_investments=investments,
        observed_at=observed_at,
        raw_payload={},
        account_currency_id=currency_id,
    )


def row(
    *,
    instrument_id: int = CORE_ID,
    amount: str = "250",
    market_value: str = "250",
    longs: int = 1,
    shorts: int = 0,
) -> BrokerInstrumentInvestment:
    return BrokerInstrumentInvestment(instrument_id, Decimal(amount), Decimal(market_value), longs, shorts)


def test_the_sleeve_reads_market_value_and_not_committed_capital() -> None:
    """The defect #2704 exists for: both fields are Decimals and only one is right."""
    state = observe_core_sleeve(
        snapshot(investments=(row(amount="9999.85", market_value="12665.70"),)),
        core_instrument_id=CORE_ID,
    )

    assert state.core_market_value == Decimal("12665.70")
    assert state.cash_balance == Decimal("600")
    assert state.currency == "USD"
    assert state.core_instrument_id == CORE_ID


def test_both_components_carry_the_snapshots_own_instant() -> None:
    """The one-snapshot warranty holds by construction, not by caller discipline."""
    state = observe_core_sleeve(snapshot(investments=(row(),)), core_instrument_id=CORE_ID)

    assert state.as_of is _NOW


def test_an_unheld_instrument_is_an_empty_sleeve_not_a_refusal() -> None:
    state = observe_core_sleeve(snapshot(investments=(row(instrument_id=1002),)), core_instrument_id=CORE_ID)

    assert state.core_market_value == Decimal("0")


def test_a_mirror_only_row_values_the_sleeve_at_zero() -> None:
    """33 of 38 live rows were exactly this: committed capital, no direct holding."""
    state = observe_core_sleeve(
        snapshot(investments=(row(amount="1969.71", market_value="0", longs=0),)),
        core_instrument_id=CORE_ID,
    )

    assert state.core_market_value == Decimal("0")


def test_a_direct_short_refuses_rather_than_valuing_the_sleeve() -> None:
    """Folding it in misstates the sleeve; dropping it misstates it the other way."""
    with pytest.raises(CoreSleeveObservationError, match="short"):
        observe_core_sleeve(
            snapshot(investments=(row(shorts=1),)),
            core_instrument_id=CORE_ID,
        )


def test_a_short_is_refused_even_when_the_long_leg_looks_ordinary() -> None:
    """The count is what carries it -- no money field could (offsetting lots)."""
    with pytest.raises(CoreSleeveObservationError, match="short"):
        observe_core_sleeve(
            snapshot(investments=(row(market_value="12665.70", longs=2, shorts=1),)),
            core_instrument_id=CORE_ID,
        )


def test_an_unreported_account_currency_refuses_rather_than_assuming_usd() -> None:
    """#2602 item 2. Every money field would still be internally consistent."""
    with pytest.raises(CoreSleeveObservationError, match="not reported"):
        observe_core_sleeve(snapshot(investments=(row(),), currency_id=None), core_instrument_id=CORE_ID)


def test_an_undocumented_currency_id_refuses_rather_than_inferring_a_code() -> None:
    unknown = max(DOCUMENTED_ACCOUNT_CURRENCIES) + 1
    with pytest.raises(CoreSleeveObservationError, match="undocumented"):
        observe_core_sleeve(snapshot(investments=(row(),), currency_id=unknown), core_instrument_id=CORE_ID)


def test_the_currency_label_comes_from_the_single_source() -> None:
    """`_state_refusal` compares it to the mandate as a LABEL, so it cannot be a literal."""
    for currency_id, code in DOCUMENTED_ACCOUNT_CURRENCIES.items():
        state = observe_core_sleeve(snapshot(investments=(row(),), currency_id=currency_id), core_instrument_id=CORE_ID)
        assert state.currency == code


def test_a_naive_observed_at_refuses() -> None:
    with pytest.raises(CoreSleeveObservationError, match="timezone-aware"):
        observe_core_sleeve(
            snapshot(investments=(row(),), observed_at=datetime(2026, 8, 14, 13, 7)),
            core_instrument_id=CORE_ID,
        )


def test_duplicate_rows_refuse_rather_than_picking_one() -> None:
    """The parser cannot emit two; the public dataclass can, and both readings lie."""
    with pytest.raises(CoreSleeveObservationError, match="ambiguous"):
        observe_core_sleeve(
            snapshot(investments=(row(market_value="100"), row(market_value="900"))),
            core_instrument_id=CORE_ID,
        )


def test_currency_is_refused_before_a_short_is() -> None:
    """The declared precedence: a snapshot with several defects fails the same way."""
    with pytest.raises(CoreSleeveObservationError, match="not reported"):
        observe_core_sleeve(
            snapshot(investments=(row(shorts=1),), currency_id=None),
            core_instrument_id=CORE_ID,
        )


def test_a_negative_market_value_is_passed_through_for_the_allocator_to_refuse() -> None:
    """The refusal belongs at `_state_refusal`, which labels it sleeve_valuation_invalid."""
    state = observe_core_sleeve(snapshot(investments=(row(market_value="-50"),)), core_instrument_id=CORE_ID)

    assert state.core_market_value == Decimal("-50")
    assert evaluate_core_rebalance(_mandate(), state).reason_code == "sleeve_valuation_invalid"


def _mandate() -> CoreMandate:
    return CoreMandate(
        event_id=1,
        revision=1,
        enabled=True,
        base_currency="USD",
        core_instrument_id=CORE_ID,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("5"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("1"),
        policy_version=CORE_MANDATE_POLICY_VERSION,
    )


def test_the_observed_state_reaches_a_verdict_and_the_wrong_field_inverts_it() -> None:
    """The end-to-end point of #2704, on the live 3006 figures.

    Committed capital 9,999.85 against 12,665.70 of market value and 6,000 cash: the
    sleeve is 67.86% of 18,665.70, above the 65% band, so the right verdict is
    SELL.  Fed `amount` instead, the same arithmetic sees 62.50% -- inside the band
    -- and HOLDS.  Nothing is malformed in either case, which is exactly why no
    refusal can catch it.
    """
    investments = (row(amount="9999.85", market_value="12665.70"),)
    state = observe_core_sleeve(snapshot(investments=investments, cash="6000"), core_instrument_id=CORE_ID)

    verdict = evaluate_core_rebalance(_mandate(), state)
    assert verdict.action == "sell_core"
    assert verdict.reason_code is None

    committed = CoreSleeveState(
        core_instrument_id=CORE_ID,
        core_market_value=investments[0].amount,
        cash_balance=Decimal("6000"),
        currency="USD",
        as_of=_NOW,
    )
    assert evaluate_core_rebalance(_mandate(), committed).action == "hold"
