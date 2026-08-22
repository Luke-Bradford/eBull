"""The capital sandbox bound — the engine's allocation boundary (#2844).

Pure-logic throughout. The bound is arithmetic over four numbers, so a DB-tier test
would exercise Postgres rather than the rule; the one integration test that matters
(the executor actually returning ``sandbox_exceeded``) lives beside the executor.

The invariant sweep at the bottom is hand-rolled rather than property-based:
``hypothesis`` is not a dependency here and "do not add libraries casually" outranks
the convenience. A deterministic grid over the cases that can differ is enough for a
rule with no branching beyond the mode.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.strategy_capital_sandbox import (
    OPERATOR_WORD_FOR_CAPITAL_MODE,
    CapitalMode,
    effective_realised_delta,
    headroom_from_bound,
    sandbox_bound,
    sandbox_headroom,
)


class TestEffectiveRealisedDelta:
    """The asymmetry IS the capped pot; without it the two modes are one mode."""

    def test_a_capped_pot_takes_realised_losses(self) -> None:
        assert effective_realised_delta(Decimal("-250"), "fixed") == Decimal("-250")

    def test_a_capped_pot_refuses_realised_profits(self) -> None:
        # Profits skim to unmanaged cash. Letting them raise the ceiling would make
        # `capped` mean "capped until it wins", which is not a cap.
        assert effective_realised_delta(Decimal("250"), "fixed") == Decimal("0")

    @pytest.mark.parametrize("realised", [Decimal("-250"), Decimal("0"), Decimal("250")])
    def test_an_expanding_pot_takes_both_directions(self, realised: Decimal) -> None:
        assert effective_realised_delta(realised, "compound") == realised


class TestSandboxBound:
    def test_a_capped_pot_never_exceeds_its_assignment(self) -> None:
        assert sandbox_bound(
            capital_limit=Decimal("1000"), capital_mode="fixed", realised_delta=Decimal("400")
        ) == Decimal("1000")

    def test_an_expanding_pot_grows_by_realised_profit(self) -> None:
        assert sandbox_bound(
            capital_limit=Decimal("1000"), capital_mode="compound", realised_delta=Decimal("400")
        ) == Decimal("1400")

    @pytest.mark.parametrize("mode", ["fixed", "compound"])
    def test_realised_losses_shrink_the_bound_under_both_modes(self, mode: CapitalMode) -> None:
        # A loss is not a mode question: the money is gone either way.
        assert sandbox_bound(
            capital_limit=Decimal("1000"), capital_mode=mode, realised_delta=Decimal("-250")
        ) == Decimal("750")

    @pytest.mark.parametrize("mode", ["fixed", "compound"])
    def test_a_pot_that_lost_more_than_its_assignment_is_exhausted_not_negative(self, mode: CapitalMode) -> None:
        """⚠ Zero, not -200.

        A negative bound makes every `committed <= bound` comparison false in a way
        that reads as arithmetic rather than as the exhaustion it is, and it would
        report a breach on a pot holding nothing.
        """
        bound = sandbox_bound(capital_limit=Decimal("1000"), capital_mode=mode, realised_delta=Decimal("-1200"))
        assert bound == Decimal("0")
        assert headroom_from_bound(bound=bound, committed=Decimal("0")).within_bound


class TestHeadroom:
    def test_a_pot_spent_to_exactly_its_bound_is_full_not_breached(self) -> None:
        """⚠ The distinction the whole refusal turns on.

        `within_bound` is `committed <= bound`, not `remaining > 0`. Collapsing them
        would report a boundary breach on the ordinary fully-invested state.
        """
        headroom = sandbox_headroom(
            capital_limit=Decimal("1000"),
            capital_mode="fixed",
            realised_delta=Decimal("0"),
            committed=Decimal("1000"),
        )
        assert headroom.remaining == Decimal("0")
        assert headroom.within_bound

    def test_a_pot_spent_past_its_bound_has_no_backwards_headroom(self) -> None:
        headroom = sandbox_headroom(
            capital_limit=Decimal("1000"),
            capital_mode="fixed",
            realised_delta=Decimal("0"),
            committed=Decimal("1400"),
        )
        assert headroom.remaining == Decimal("0")
        assert not headroom.within_bound

    def test_the_direct_constructor_agrees_with_the_computed_one(self) -> None:
        # `strategy_paper_executor` resolves the bound in an earlier pass and uses
        # `headroom_from_bound`; if the two disagreed, the control and the panel
        # would be back to two arithmetics under one name.
        computed = sandbox_headroom(
            capital_limit=Decimal("1000"),
            capital_mode="compound",
            realised_delta=Decimal("120"),
            committed=Decimal("300"),
        )
        direct = headroom_from_bound(bound=Decimal("1120"), committed=Decimal("300"))
        assert computed == direct


class TestVocabulary:
    """The operator's words and the stored values must stay traceable to each other."""

    def test_every_stored_mode_has_an_operator_word(self) -> None:
        assert OPERATOR_WORD_FOR_CAPITAL_MODE == {"fixed": "capped", "compound": "expanding"}

    def test_nothing_branches_on_the_operator_word(self) -> None:
        # The map is presentation. If a second spelling ever becomes branchable it is
        # a second mode waiting to disagree with the first, so the arithmetic is
        # asserted to be blind to it.
        for stored in OPERATOR_WORD_FOR_CAPITAL_MODE:
            assert sandbox_bound(
                capital_limit=Decimal("100"),
                capital_mode=stored,
                realised_delta=Decimal("-10"),
            ) == Decimal("90")


class TestTheInvariant:
    """#2844's acceptance: no sequence of allocations can breach the bound."""

    LIMITS = [Decimal("0.001"), Decimal("1"), Decimal("1000"), Decimal("104060.06")]
    REALISED = [Decimal("-2000"), Decimal("-1"), Decimal("0"), Decimal("1"), Decimal("2000")]
    TICKETS = [Decimal("0.01"), Decimal("7.77"), Decimal("500"), Decimal("100000")]

    @pytest.mark.parametrize("mode", ["fixed", "compound"])
    def test_repeated_allocation_capped_by_remaining_never_breaches(self, mode: CapitalMode) -> None:
        """Allocate greedily, forever, and the bound still holds.

        This is the shape the executor uses: every entry is sized by `min(..., remaining,
        ...)`, so the sweep asks whether that cap is sufficient on its own. It is the
        one property worth stating, because every other capacity term can be relaxed
        or reconfigured while this one is the operator's declared safety net.
        """
        for limit in self.LIMITS:
            for realised in self.REALISED:
                bound = sandbox_bound(capital_limit=limit, capital_mode=mode, realised_delta=realised)
                committed = Decimal("0")
                for ticket in self.TICKETS * 3:
                    headroom = headroom_from_bound(bound=bound, committed=committed)
                    committed += min(ticket, headroom.remaining)
                    assert committed <= bound, (
                        f"breached: limit={limit} mode={mode} realised={realised} committed={committed} bound={bound}"
                    )
                    assert headroom_from_bound(bound=bound, committed=committed).within_bound

    @pytest.mark.parametrize("mode", ["fixed", "compound"])
    def test_a_realised_loss_arriving_mid_life_can_strand_exposure_above_the_bound(self, mode: CapitalMode) -> None:
        """⚠ The one case the invariant does NOT prevent, asserted so it stays known.

        Capital already committed cannot be un-committed by a later loss, so a pot can
        legitimately sit ABOVE its bound. The rule that must hold is the ENTRY rule —
        no new commitment — and the refusal is what enforces it. Forcing a liquidation
        instead is explicitly not the engine's call (#2844: lowering the assignment
        "blocks new entries only, never forces liquidation").
        """
        bound_before = sandbox_bound(capital_limit=Decimal("1000"), capital_mode=mode, realised_delta=Decimal("0"))
        committed = bound_before  # fully invested, legitimately
        bound_after = sandbox_bound(capital_limit=Decimal("1000"), capital_mode=mode, realised_delta=Decimal("-400"))
        stranded = headroom_from_bound(bound=bound_after, committed=committed)
        assert not stranded.within_bound
        # ...and the only consequence is that nothing further may be committed.
        assert stranded.remaining == Decimal("0")
