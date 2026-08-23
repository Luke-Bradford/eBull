"""#2603 step 3b-2 — the broker's OPEN-position minimum, from the portal's own fields.

Source rule (live portal 2026-08-23,
``api-reference/trading--demo/check-instrument-trading-eligibility``):

* ``minPositionExposure``, on the eligibility ROW — *"Minimum exposure value required to
  open a position on this instrument. The exposure is always calculated in USD as the
  number of units times the rate times the conversion rate to USD."*
* ``minPositionAmount``, on a ``leverageConfigs`` ARM — *"Minimum margin required to open
  a position under this leverage configuration."*

Two constraints on one act, not two spellings of one number — so the combinator is
``max`` and the precedence it replaces (``arm.min_position_amount or
row.min_position_exposure``) was fail-OPEN by the gap between them.

Spec: ``docs/proposals/ta/2026-08-23-broker-open-minimum.md``.

⚠ Pure-logic tests. No DB, no broker.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.broker_settlement_arms import effective_open_minimum

_USD = "USD"


class TestCombination:
    """``max``, because both figures gate the same act."""

    def test_the_larger_of_the_two_binds(self) -> None:
        """The regression the ``or`` precedence allowed.

        An arm quoting a SMALLER margin than the row's exposure is the fail-open case:
        ``or`` returned 10 and admitted a ticket the instrument-level rule refuses.
        """
        assert effective_open_minimum(
            response_currency=_USD,
            min_position_exposure=Decimal("50"),
            min_position_amount=Decimal("10"),
        ) == Decimal("50")

    def test_the_larger_binds_in_the_other_direction_too(self) -> None:
        assert effective_open_minimum(
            response_currency=_USD,
            min_position_exposure=Decimal("10"),
            min_position_amount=Decimal("50"),
        ) == Decimal("50")

    def test_equal_figures_are_that_figure(self) -> None:
        """The shape every stored proof carries today: 12 of 12 agree at 10.00."""
        assert effective_open_minimum(
            response_currency=_USD,
            min_position_exposure=Decimal("10"),
            min_position_amount=Decimal("10"),
        ) == Decimal("10")

    @pytest.mark.parametrize(
        ("exposure", "amount", "expected"),
        [
            (Decimal("10"), None, Decimal("10")),
            (None, Decimal("10"), Decimal("10")),
        ],
    )
    def test_one_quoted_figure_is_the_floor(
        self, exposure: Decimal | None, amount: Decimal | None, expected: Decimal
    ) -> None:
        """The 11 ``not_underlying`` proofs carry an exposure and no arm amount."""
        assert (
            effective_open_minimum(response_currency=_USD, min_position_exposure=exposure, min_position_amount=amount)
            == expected
        )

    def test_neither_quoted_is_None_and_callers_fail_closed_on_it(self) -> None:
        """``None`` means "the broker quoted no usable threshold", NOT "any size".

        Both call sites keep ``minimum is None or amount < minimum`` → refuse, so this
        value can only ever refuse.
        """
        assert (
            effective_open_minimum(response_currency=_USD, min_position_exposure=None, min_position_amount=None) is None
        )


class TestSanitisation:
    """Applied HERE so the stored-proof path and the executor's live path agree.

    ``strategy_core_eligibility._positive_or_none`` drops these before storing and
    ``sql/346`` refuses to store them; the executor's live response was never sanitised at
    all, so without this the two callers reach different verdicts on identical broker
    data.
    """

    @pytest.mark.parametrize("bad", [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")])
    def test_an_unusable_figure_reads_as_not_quoted(self, bad: Decimal) -> None:
        assert effective_open_minimum(
            response_currency=_USD, min_position_exposure=Decimal("10"), min_position_amount=bad
        ) == Decimal("10")

    @pytest.mark.parametrize("bad", [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")])
    def test_an_unusable_figure_does_not_win_the_max(self, bad: Decimal) -> None:
        """⚠ ``Decimal("NaN")`` compares FALSE against everything, and ``Infinity`` would
        win a naive ``max`` outright and refuse every order.  Dropping first is what
        keeps both out of the comparison."""
        assert effective_open_minimum(
            response_currency=_USD, min_position_exposure=bad, min_position_amount=Decimal("10")
        ) == Decimal("10")

    def test_two_unusable_figures_read_as_not_quoted(self) -> None:
        assert (
            effective_open_minimum(
                response_currency=_USD, min_position_exposure=Decimal("0"), min_position_amount=Decimal("-5")
            )
            is None
        )


class TestCurrencyComparability:
    """``minPositionAmount`` documents NO currency, so the two combine only in USD."""

    @pytest.mark.parametrize("currency", ["USD", "usd", " Usd "])
    def test_a_usd_response_combines_however_it_is_cased(self, currency: str) -> None:
        """⚠ Measured, not hypothetical: ``response_currency`` is lower-case ``usd`` on
        all 23 stored proofs against a ``requested_currency`` of ``USD``."""
        assert effective_open_minimum(
            response_currency=currency,
            min_position_exposure=Decimal("10"),
            min_position_amount=Decimal("10"),
        ) == Decimal("10")

    @pytest.mark.parametrize("currency", ["GBP", "EUR", ""])
    def test_a_non_usd_response_raises_rather_than_comparing_two_denominations(self, currency: str) -> None:
        """A caller bug, not a state of the world.

        Both callers refuse a currency mismatch BEFORE reaching here
        (``_eligibility_reason`` → ``eligibility_unresolved``,
        ``evaluate_core_eligibility`` → ``eligibility_currency_mismatch``), so arriving
        with anything else means a caller skipped its own check.  #2603 scope item 4 is
        the change that must revisit it.
        """
        with pytest.raises(ValueError, match="scope item 4"):
            effective_open_minimum(
                response_currency=currency,
                min_position_exposure=Decimal("10"),
                min_position_amount=Decimal("10"),
            )

    def test_the_currency_is_checked_even_when_nothing_is_quoted(self) -> None:
        """Otherwise the guard is skippable by sending two nulls."""
        with pytest.raises(ValueError):
            effective_open_minimum(response_currency="GBP", min_position_exposure=None, min_position_amount=None)
