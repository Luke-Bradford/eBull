"""The core sleeve's stop/target derivation (#3284) — pure logic, no DB, no broker.

The arithmetic is three lines; what needs testing is the four decisions AROUND it,
each of which is silent when wrong: the rounding DIRECTION, the tolerance that keeps a
protected position from reporting a gap forever, the refusals on a bad anchor, and the
fact that the constants themselves are guarded.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.providers.broker import BrokerPosition
from app.services.core_exit_levels import (
    CORE_EXIT_MAX_QUOTE_AGE_SECONDS,
    CORE_EXIT_RATE_TOLERANCE,
    CORE_STOP_LOSS_PCT,
    CORE_TAKE_PROFIT_PCT,
    core_exit_level_satisfied,
    core_exit_levels,
)
from app.services.strategy_core_preflight import CORE_MAX_QUOTE_AGE_SECONDS
from app.services.strategy_position_manager import _edit_landed, _OwnedPosition


def test_the_live_position_reproduces_the_levels_the_operator_set_by_hand() -> None:
    """The one anchored case, and the reason it is first.

    Position ``3601264304`` was opened at 759.86 on 2026-09-18 and hand-protected on
    2026-09-21 at **SL 379.93 / TP 2279.58** — read back from ``broker_positions`` on
    dev, not copied from prose.  The derivation must land on those numbers, because the
    hand-set pair is the only independent statement of intent that exists; every other
    expectation in this file is computed by the same rule it is testing.

    ⚠⚠ **-50% / +200%, and an earlier version of this test asserted -25% / +100%.**  The
    ticket BODY still describes the superseded levels; they were corrected the same day
    once the operator asked whether they were realistic, because a -25% stop fires on
    35.7% of entries and is therefore a bear-market exit rather than a spike guard.
    Reading the body and not the correction is exactly how the wrong constants shipped.

    ⚠ Both products are exact at 2dp (759.86 * 0.50 and * 3.00), so the derivation
    matches the broker byte for byte and no tolerance is needed to reconcile them.
    """
    levels = core_exit_levels(Decimal("759.86"))
    assert levels.stop_loss_rate == Decimal("379.93")
    assert levels.take_profit_rate == Decimal("2279.58")
    assert core_exit_level_satisfied(observed=Decimal("379.93"), desired=levels.stop_loss_rate)
    assert core_exit_level_satisfied(observed=Decimal("2279.58"), desired=levels.take_profit_rate)


@pytest.mark.parametrize(
    ("entry", "stop", "take"),
    [
        (Decimal("100"), Decimal("50.00"), Decimal("300.00")),
        (Decimal("759.86"), Decimal("379.93"), Decimal("2279.58")),
        # The stop needs rounding, the target does not: 0.33 * 0.5 = 0.165 truncates to
        # 0.16, while 0.33 * 3 = 0.99 is already exact.
        (Decimal("0.33"), Decimal("0.16"), Decimal("0.99")),
        # A third-of-a-cent entry forces the stop DOWN and the target UP at once:
        # 1.005 * 0.5 = 0.5025 -> 0.50, and 1.005 * 3 = 3.015 -> 3.02.
        (Decimal("1.005"), Decimal("0.50"), Decimal("3.02")),
    ],
)
def test_levels_quantize_away_from_the_entry_price(entry: Decimal, stop: Decimal, take: Decimal) -> None:
    levels = core_exit_levels(entry)
    assert (levels.stop_loss_rate, levels.take_profit_rate) == (stop, take)


def test_neither_level_can_fire_before_the_declared_move() -> None:
    """The invariant the rounding direction exists to protect, stated as a property.

    Quantizing toward the entry would make the REALISED policy tighter than the
    DECLARED -25% / +100%, which is the failure a reader would never suspect of a
    rounding step: the sleeve would stop out slightly early and take profit slightly
    early, and both would look like correct behaviour.

    Swept across entries whose products land on, just above and just below a cent
    boundary — a single example passes under either rounding mode.
    """
    hundred = Decimal("100")
    for cents in range(1, 400):
        entry = Decimal(cents) / Decimal("3")  # recurring, so most products need rounding
        levels = core_exit_levels(entry)
        assert levels.stop_loss_rate <= entry * (hundred - CORE_STOP_LOSS_PCT) / hundred
        assert levels.take_profit_rate >= entry * (hundred + CORE_TAKE_PROFIT_PCT) / hundred


@pytest.mark.parametrize("bad", [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")])
def test_a_non_positive_or_non_finite_anchor_is_refused(bad: Decimal) -> None:
    """An unusable anchor must raise, never silently produce a level.

    The anchor is ``BrokerPosition.open_price``.  A zero or absent fill reaching this
    function would otherwise derive a stop of 0 — a body eToro may well accept, and
    which would leave the position reading as protected while having no floor at all.
    """
    with pytest.raises(ValueError):
        core_exit_levels(bad)


def test_the_tolerance_is_one_cent_and_is_inclusive_at_the_boundary() -> None:
    desired = Decimal("379.93")
    assert core_exit_level_satisfied(observed=desired, desired=desired)
    assert core_exit_level_satisfied(observed=desired + CORE_EXIT_RATE_TOLERANCE, desired=desired)
    assert core_exit_level_satisfied(observed=desired - CORE_EXIT_RATE_TOLERANCE, desired=desired)
    assert not core_exit_level_satisfied(observed=desired + Decimal("0.02"), desired=desired)
    assert not core_exit_level_satisfied(observed=desired - Decimal("0.02"), desired=desired)


@pytest.mark.parametrize("absent", [None, Decimal("NaN")])
def test_an_absent_or_unreadable_rate_is_never_satisfied(absent: Decimal | None) -> None:
    """``None`` is the naked position — the entire subject of #3284 — so it must be a gap.

    ``NaN`` is here because it compares False to everything, so an ``abs(...) <= tol``
    written without the finiteness guard would return False and read as "satisfied
    is False" by luck rather than by rule.  The guard makes it deliberate.
    """
    assert not core_exit_level_satisfied(observed=absent, desired=Decimal("379.93"))


def _owned(*, is_core: bool) -> _OwnedPosition:
    """The two fields ``_edit_landed`` reads, with the rest at their absent values."""
    return _OwnedPosition(
        ownership_id=1,
        strategy_trade_id=2,
        broker_position_id=3601264304,
        instrument_id=3417,
        is_core=is_core,
        deployment_id=None if is_core else 7,
        entry_stop=None if is_core else Decimal("379.93"),
        entry_take_profit=None if is_core else Decimal("2279.58"),
        max_position_age_seconds=None,
        max_quote_age_seconds=None if is_core else 750,
        ratchet_variant_id=None,
        break_atr_multiple=None,
        chandelier_atr_multiple=None,
        structure_atr_multiple=None,
        quote_bid=Decimal("773.81"),
        quoted_at=None,
    )


def _broker_position(*, stop: Decimal | None, take: Decimal | None) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=3417,
        units=Decimal("1"),
        open_price=Decimal("759.86"),
        current_price=Decimal("773.81"),
        raw_payload={},
        position_id=3601264304,
        stop_loss_rate=stop,
        take_profit_rate=take,
        is_no_stop_loss=stop is None,
        is_no_take_profit=take is None,
    )


def test_a_core_edit_that_landed_one_cent_off_is_recognised_as_applied() -> None:
    """The consistency bug Codex checkpoint 2 found, and the worse half of it.

    The core arm DECIDES with a one-cent tolerance, so the resume path must JUDGE with
    the same rule.  With exact equality there instead, an edit that landed at 379.94
    against an intent of 379.93 resolves as not-landed — and the two consequences are
    both bad in the quiet direction: an ``intent_persisted`` operation is recorded
    ``reconcile_required`` despite having applied, and a ``submitted`` one stays
    ``broker_edit_pending`` forever.  ``_resume_operation`` runs before close handling,
    so that second state would make the position **unclosable** — a stop mechanism
    whose failure mode is trapping the holding it protects.
    """
    landed = _edit_landed(
        owned=_owned(is_core=True),
        position=_broker_position(stop=Decimal("379.94"), take=Decimal("2279.58")),
        desired_stop=Decimal("379.93"),
        desired_take=Decimal("2279.58"),
    )
    assert landed is True


def test_the_signal_arm_keeps_exact_equality() -> None:
    """Deliberately NOT widened: the alpha arm has no tolerance to be consistent with.

    Its rates come from a stored preflight rather than a live derivation, so a one-cent
    difference there is an unexplained divergence and must stay `reconcile_required`.
    """
    landed = _edit_landed(
        owned=_owned(is_core=False),
        position=_broker_position(stop=Decimal("379.94"), take=Decimal("2279.58")),
        desired_stop=Decimal("379.93"),
        desired_take=Decimal("2279.58"),
    )
    assert landed is False


@pytest.mark.parametrize("is_core", [True, False])
def test_a_cleared_exit_never_reads_as_landed_on_either_arm(is_core: bool) -> None:
    """An absent stop is the naked position — it cannot be a landed edit on any arm."""
    assert not _edit_landed(
        owned=_owned(is_core=is_core),
        position=_broker_position(stop=None, take=None),
        desired_stop=Decimal("379.93"),
        desired_take=Decimal("2279.58"),
    )
    # And a missing position is not evidence of anything.
    assert not _edit_landed(
        owned=_owned(is_core=is_core),
        position=None,
        desired_stop=Decimal("379.93"),
        desired_take=Decimal("2279.58"),
    )


def test_the_quote_freshness_bound_is_the_sleeve_s_existing_measured_one() -> None:
    """Not a duplicate constant — the core sleeve's own bound, re-exported.

    A first draft defined 300 s here "by construction, one paper cycle".  Two things
    were wrong with it and both are silent: 300 s is BELOW one producer period, which
    ``_freshness_bound`` documents as a recurring false refusal (immediately before each
    refresh the newest row is one full period old), so the repair would have refused
    ``fixed_exit_quote_unsafe`` in the tail of every cycle; and #3157 measured that the
    quote producer loses fires, which the tolerated-miss term in the real bound accounts
    for and an invented constant cannot.

    This test exists so the two can never drift apart again.
    """
    assert CORE_EXIT_MAX_QUOTE_AGE_SECONDS == CORE_MAX_QUOTE_AGE_SECONDS
    # And the property that made 300 wrong, asserted directly rather than as a literal:
    # the bound must be at least one full producer period.
    assert CORE_EXIT_MAX_QUOTE_AGE_SECONDS >= 300


def test_the_declared_percentages_are_the_ones_the_operator_chose() -> None:
    """A change to either constant is an operator decision (#3284), not tuning.

    This test is a tripwire, not a calculation: it fails loudly if a later session
    'improves' the levels, and points at the ticket that fixed them.

    ⚠ The measured basis is deliberately NOT restated here — it lives in
    ``core_exit_levels``'s module docstring, beside the query that reproduces it.  An
    earlier version of this docstring copied the figures, and one of the copies
    ("worst 10-day -24.95%") was already stale by the time it was written: the real
    figure is -26.77%, and -24.95% is the worst NINE-day move.  A hand-copied statistic
    goes stale silently and in the place a reader trusts most, which is why the repo
    rule is to compute it or cite where it is computed — never to duplicate it.
    """
    assert (CORE_STOP_LOSS_PCT, CORE_TAKE_PROFIT_PCT) == (Decimal("50"), Decimal("200"))
