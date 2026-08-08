"""Phase 5e-5b — the random-entry cohort's placement, its pricing bridge and §9's thresholds.

Pure tier: no database. Everything here is a property of a construction that is
declared rather than sourced, so the tests are what pin it.

⚠ THE ``SPEC_*`` LITERALS BELOW ARE RESTATED, NOT IMPORTED, and that is the
#2240 S-3 lesson: *"a reference that IMPORTS the constant it validates is a
tautology"*. They are transcribed from
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §5 ("Acceptance
for the harness itself") and ``docs/proposals/ta/2026-08-07-bounded-backtester.md``
§9. ``TestSpecConstants`` is the single bridge asserting the module agrees.
"""

from __future__ import annotations

import math
from collections import Counter
from decimal import Decimal

import numpy as np
import pytest

from app.services.cost_model import BANDS, buy_price, half_spread_for, sell_price
from app.services.random_entry_cohort import (
    COHORT_BOOTSTRAP_RESAMPLES,
    COHORT_MODEL_ID,
    COHORT_ROOT_SEED,
    SPEC_CI_PERCENT,
    SPEC_COHORT_SIZE,
    SPEC_SHARPE_PERCENTILE,
    MemberOutcome,
    SyntheticControl,
    cohort_threshold,
    evaluate_control,
    match_residual,
    member_seed,
    net_entry_prices,
    net_exit_prices,
    percentile_bootstrap_mean,
    place_entries,
    slack,
)

# --- transcribed from the spec, never imported -----------------------------

#: Parent §5: "run 1,000 random-entry strategies matched to each real strategy's
#: exposure and turnover".
SPEC_COHORT_MEMBERS = 1000
#: Parent §5: "each real strategy's Sharpe must exceed the 95th percentile of
#: the random cohort's".
SPEC_PERCENTILE = 95.0
#: Parent §5: "must lie within its own 95% bootstrap CI of zero".
SPEC_INTERVAL = 95.0


def _rng(seed: int = 1) -> np.random.Generator:
    return np.random.Generator(np.random.PCG64(np.random.SeedSequence(seed)))


def _member(index: int, **overrides: object) -> MemberOutcome:
    base: dict[str, object] = {
        "index": index,
        "sharpe": 0.0,
        "total_return_pct": 0.0,
        "exposure_time_pct": 20.0,
        "turnover_annualised": 3.0,
        "trade_count": 100,
    }
    base.update(overrides)
    return MemberOutcome(**base)  # type: ignore[arg-type]


def _control(**overrides: object) -> SyntheticControl:
    base: dict[str, object] = {
        "model_id": COHORT_MODEL_ID,
        "cohort_size": 1000,
        "root_seed": 20260808,
        "mean_return_pct": 0.0,
        "mean_return_ci_low_pct": -1.0,
        "mean_return_ci_high_pct": 1.0,
        "sharpe_percentile": 95.0,
        "cohort_sharpe_threshold": 0.10,
        "strategy_sharpe": 0.50,
        "cohort_return_threshold_pct": 4.0,
        "strategy_return_pct": 9.0,
    }
    base.update(overrides)
    return SyntheticControl(**base)  # type: ignore[arg-type]


class TestSpecConstants:
    """The one bridge between the transcribed spec literals and the module."""

    def test_the_cohort_is_the_spec_cohort(self) -> None:
        assert SPEC_COHORT_SIZE == SPEC_COHORT_MEMBERS

    def test_the_sharpe_threshold_is_the_spec_percentile(self) -> None:
        assert SPEC_SHARPE_PERCENTILE == SPEC_PERCENTILE

    def test_the_interval_is_the_spec_interval(self) -> None:
        assert SPEC_CI_PERCENT == SPEC_INTERVAL

    def test_the_construction_is_named_and_the_seed_is_declared(self) -> None:
        """⚠ §9 requires the seed RECORDED. A seed drawn from the clock, or a
        model id nobody wrote down, makes the run unreproducible while every
        number it prints still looks fine."""
        assert COHORT_MODEL_ID
        assert isinstance(COHORT_ROOT_SEED, int)

    def test_the_resample_count_is_at_or_above_the_published_floor(self) -> None:
        """Efron & Tibshirani (1993) ch. 13 put the INTERVAL floor at 1,000."""
        assert COHORT_BOOTSTRAP_RESAMPLES >= 1000


class TestMemberSeeds:
    def test_a_members_stream_does_not_depend_on_the_shard_it_ran_in(self) -> None:
        """⚠⚠ THE WHOLE REASON ``spawn_key`` IS USED. ``SeedSequence.spawn()`` is
        stateful on the parent, so member 700 drawn as the first of a shard and
        as the 700th of a single run would get DIFFERENT entries — and the
        cohort would silently depend on how the work was divided."""
        direct = _draw(700)
        after_others = None
        for index in (0, 1, 2, 699, 700):
            drawn = _draw(index)
            if index == 700:
                after_others = drawn
        assert after_others is not None
        assert np.array_equal(direct, after_others)

    def test_two_members_draw_different_placements(self) -> None:
        assert not np.array_equal(_draw(0), _draw(1))

    def test_a_negative_index_is_refused(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            member_seed(-1)


def _draw(index: int) -> np.ndarray:
    rng = np.random.Generator(np.random.PCG64(member_seed(index)))
    entries, _holds = place_entries(rng, eligible=500, holds=np.asarray([3, 5, 2, 9], dtype=np.int64))
    return entries


class TestPlacement:
    """The construction §9 leaves open, and this module declares."""

    def test_the_trade_count_is_preserved_exactly(self) -> None:
        holds = np.asarray([1, 4, 2, 7, 3], dtype=np.int64)
        entries, permuted = place_entries(_rng(), eligible=200, holds=holds)
        assert entries.size == holds.size
        assert permuted.size == holds.size

    def test_the_holding_period_multiset_is_preserved(self) -> None:
        """⚠ The multiset, not the ORDER. A long hold following a short one is a
        fact about the signal, and preserving the sequence would carry a piece of
        the strategy into the null it is measured against."""
        holds = np.asarray([1, 1, 4, 9, 2], dtype=np.int64)
        _entries, permuted = place_entries(_rng(3), eligible=300, holds=holds)
        assert Counter(permuted.tolist()) == Counter(holds.tolist())

    def test_the_holds_do_not_keep_the_strategys_own_order(self) -> None:
        """⚠⚠ THE MULTISET TEST ABOVE PASSES FOR THE IDENTITY PERMUTATION, so it
        cannot see a construction that shuffles nothing. Keeping the order pairs
        each random entry with the hold the strategy chose in that SLOT, which
        carries a piece of the signal into the null — the sequence "short hold,
        then long hold" is a fact about what the strategy saw."""
        holds = np.asarray([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.int64)
        orders = {tuple(place_entries(_rng(seed), eligible=400, holds=holds)[1].tolist()) for seed in range(40)}
        assert len(orders) > 1
        assert tuple(holds.tolist()) not in orders or len(orders) > 5

    def test_positions_never_overlap(self) -> None:
        """§3.1's pyramiding rule, on every seed in a sweep."""
        holds = np.asarray([2, 5, 1, 8, 3, 3], dtype=np.int64)
        for seed in range(200):
            entries, permuted = place_entries(_rng(seed), eligible=60, holds=holds)
            order = np.argsort(entries, kind="stable")
            starts, spans = entries[order], permuted[order]
            assert np.all(starts[1:] >= starts[:-1] + spans[:-1])

    def test_touching_is_permitted_and_is_reachable(self) -> None:
        """⚠⚠ §3.2 rule 4 — *"a closed position whose close date equals a later
        entry's fill bar does NOT suppress it"*. Forbidding it here would make
        the cohort's placement space strictly smaller than the real strategy's,
        so a construction that could never produce a touch would be wrong in a
        direction no overlap test can see. Exercised on the tightest fit there
        is: zero slack, where every position must touch its neighbour."""
        holds = np.asarray([2, 3, 4], dtype=np.int64)
        entries, permuted = place_entries(_rng(11), eligible=10, holds=holds)
        order = np.argsort(entries, kind="stable")
        starts, spans = entries[order], permuted[order]
        assert np.array_equal(starts[1:], starts[:-1] + spans[:-1])

    def test_every_leg_lands_inside_the_eligible_space(self) -> None:
        holds = np.asarray([1, 6, 2], dtype=np.int64)
        for seed in range(200):
            entries, permuted = place_entries(_rng(seed), eligible=25, holds=holds)
            assert int(entries.min()) >= 0
            assert int((entries + permuted).max()) <= 24

    def test_an_empty_series_places_nothing(self) -> None:
        entries, permuted = place_entries(_rng(), eligible=0, holds=np.empty(0, dtype=np.int64))
        assert entries.size == 0
        assert permuted.size == 0

    def test_a_series_that_cannot_carry_its_holds_is_refused(self) -> None:
        """⚠ REFUSED, not trimmed. The real positions are non-overlapping in this
        same ordinal space, so their holds fitting is a THEOREM — a violation is
        a bug upstream, and shortening a hold to make room would change the very
        thing §9 asks to be matched."""
        with pytest.raises(ValueError, match="cannot carry its own realised trade population"):
            place_entries(_rng(), eligible=5, holds=np.asarray([3, 3], dtype=np.int64))

    def test_slack_reports_the_shortfall_rather_than_raising(self) -> None:
        assert slack(eligible=10, holds=np.asarray([2, 3], dtype=np.int64)) == 4
        assert slack(eligible=5, holds=np.asarray([3, 3], dtype=np.int64)) < 0
        assert slack(eligible=0, holds=np.empty(0, dtype=np.int64)) < 0

    def test_the_placement_is_reproducible_from_the_recorded_seed(self) -> None:
        holds = np.asarray([2, 4, 1], dtype=np.int64)
        first = place_entries(_rng(42), eligible=90, holds=holds)
        second = place_entries(_rng(42), eligible=90, holds=holds)
        assert np.array_equal(first[0], second[0])
        assert np.array_equal(first[1], second[1])

    def test_the_placement_actually_moves_across_the_eligible_space(self) -> None:
        """⚠ A generator that always returned the same entries would satisfy
        every structural test above while producing a null distribution of one
        point. Asserted on the SPREAD, which is the property the null needs."""
        holds = np.asarray([1, 1], dtype=np.int64)
        firsts = {int(place_entries(_rng(seed), eligible=400, holds=holds)[0].min()) for seed in range(60)}
        assert len(firsts) > 20


class TestCostBridge:
    """⚠⚠ THE VECTORISED PRICING IS A SECOND IMPLEMENTATION, AND THIS IS WHAT
    STOPS IT DRIFTING. The cohort prices ~10^9 legs and ``cost_model`` takes
    ``Decimal``; the float form must agree with it everywhere, including at
    every band boundary, where a mis-keyed band is a silent re-pricing of the
    whole null."""

    def test_the_float_form_agrees_with_the_decimal_form_across_every_band(self) -> None:
        # ⚠ BOTH SIDES OF EVERY BOUNDARY. `PriceBand.contains` is `>= lower` and
        # `< upper`, so the band a price lands in flips exactly at `upper` — and
        # a float form that keyed the band itself (this one does not; it takes
        # the half-spread as an argument) would diverge there and nowhere else.
        probes: list[Decimal] = [Decimal("0.01")]
        for band in BANDS:
            if band.lower is not None:
                probes.extend((band.lower - Decimal("0.01"), band.lower, band.lower + Decimal("0.01")))
            if band.upper is not None:
                probes.extend((band.upper - Decimal("0.01"), band.upper, band.upper + Decimal("0.01")))
        for price in probes:
            half = half_spread_for(price)
            fast_entry = net_entry_prices(
                np.asarray([float(price)], dtype=np.float64),
                np.asarray([float(half)], dtype=np.float64),
            )
            fast_exit = net_exit_prices(
                np.asarray([float(price)], dtype=np.float64),
                np.asarray([float(half)], dtype=np.float64),
            )
            assert math.isclose(float(fast_entry[0]), float(buy_price(price, half_spread=half)), rel_tol=1e-12)
            assert math.isclose(float(fast_exit[0]), float(sell_price(price, half_spread=half)), rel_tol=1e-12)

    def test_the_entry_side_is_charged_up_and_the_exit_side_down(self) -> None:
        """⚠ §5.1's direction. A sign flip would make costs a subsidy, and every
        structural test above passes either way."""
        opens = np.asarray([100.0], dtype=np.float64)
        half = np.asarray([0.0015], dtype=np.float64)
        assert float(net_entry_prices(opens, half)[0]) > 100.0
        assert float(net_exit_prices(opens, half)[0]) < 100.0


class TestCohortInterval:
    def test_the_interval_brackets_the_mean(self) -> None:
        values = np.asarray([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float64)
        mean, low, high = percentile_bootstrap_mean(values, seed=1)
        assert math.isclose(mean, 3.0)
        assert low <= mean <= high

    def test_a_constant_cohort_has_a_degenerate_interval(self) -> None:
        """⚠ Not a defensive branch: it is the shape that tells a reader the
        interval measures the RANDOMISATION and nothing else. Every member
        identical means the randomisation moved nothing."""
        values = np.full(50, 7.0, dtype=np.float64)
        mean, low, high = percentile_bootstrap_mean(values, seed=2)
        assert (mean, low, high) == (7.0, 7.0, 7.0)

    def test_the_interval_is_reproducible_from_its_seed(self) -> None:
        values = np.linspace(-3.0, 9.0, 41)
        assert percentile_bootstrap_mean(values, seed=5) == percentile_bootstrap_mean(values, seed=5)

    def test_a_different_seed_moves_the_interval(self) -> None:
        values = np.linspace(-3.0, 9.0, 41)
        assert percentile_bootstrap_mean(values, seed=5) != percentile_bootstrap_mean(values, seed=6)

    def test_an_empty_cohort_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no cohort members"):
            percentile_bootstrap_mean(np.empty(0, dtype=np.float64), seed=1)

    def test_a_zero_resample_count_is_refused(self) -> None:
        with pytest.raises(ValueError, match="resamples must be positive"):
            percentile_bootstrap_mean(np.asarray([1.0, 2.0]), seed=1, resamples=0)


class TestThresholds:
    """§9's two, read literally, plus the one that is reported and does not gate."""

    def test_both_thresholds_holding_passes(self) -> None:
        assert _control().passed is True

    def test_an_interval_excluding_zero_fails_the_first_threshold(self) -> None:
        control = _control(mean_return_ci_low_pct=0.5, mean_return_ci_high_pct=2.0)
        assert control.mean_return_ci_contains_zero is False
        assert control.passed is False

    def test_an_interval_excluding_zero_from_below_fails_too(self) -> None:
        """⚠ The threshold is two-sided. A cohort losing money reliably is as
        much a statement that the null is not centred at zero as one making it,
        and a one-sided reading would pass the cost-drag case silently."""
        control = _control(mean_return_ci_low_pct=-9.0, mean_return_ci_high_pct=-1.0)
        assert control.mean_return_ci_contains_zero is False

    def test_an_interval_touching_zero_at_either_end_contains_it(self) -> None:
        assert _control(mean_return_ci_low_pct=0.0, mean_return_ci_high_pct=2.0).mean_return_ci_contains_zero
        assert _control(mean_return_ci_low_pct=-2.0, mean_return_ci_high_pct=0.0).mean_return_ci_contains_zero

    def test_a_sharpe_equal_to_the_threshold_does_not_exceed_it(self) -> None:
        """§9 says "must EXCEED"."""
        control = _control(cohort_sharpe_threshold=0.5, strategy_sharpe=0.5)
        assert control.sharpe_exceeds_cohort is False
        assert control.passed is False

    def test_the_return_percentile_is_reported_and_does_not_enter_the_verdict(self) -> None:
        control = _control(cohort_return_threshold_pct=1e9)
        assert control.return_exceeds_cohort is False
        assert control.passed is True

    def test_an_inverted_interval_is_refused(self) -> None:
        with pytest.raises(ValueError, match="inverted"):
            _control(mean_return_ci_low_pct=2.0, mean_return_ci_high_pct=1.0)

    def test_a_percentile_outside_the_open_interval_is_refused(self) -> None:
        with pytest.raises(ValueError, match="sharpe_percentile"):
            _control(sharpe_percentile=100.0)

    def test_a_control_with_no_declared_construction_is_refused(self) -> None:
        with pytest.raises(ValueError, match="model_id is required"):
            _control(model_id="")


class TestEvaluateControl:
    def test_the_percentile_is_taken_on_the_cohort_and_the_size_is_recorded(self) -> None:
        members = tuple(_member(index, sharpe=float(index) / 100.0) for index in range(101))
        control = evaluate_control(members, strategy_sharpe=2.0, strategy_return_pct=5.0)
        assert control.cohort_size == 101
        assert math.isclose(control.cohort_sharpe_threshold, 0.95, abs_tol=1e-9)
        assert control.sharpe_exceeds_cohort is True

    def test_the_threshold_is_an_order_statistic_and_not_an_interpolation(self) -> None:
        """⚠⚠ NumPy's DEFAULT would fail this. Linear interpolation on the
        ``(n-1)`` grid puts the 95th percentile of a 1,000-member cohort at
        950.05 — between the 950th and 951st members, a value no member
        achieved — and §9 asks a strategy to exceed the cohort's percentile,
        not a number sitting in the gap between two of its draws.

        Caught at Codex checkpoint 2: this module's header declared the 950th
        order statistic and the code interpolated."""
        values = np.arange(1.0, 1001.0)
        assert cohort_threshold(values, percentile=95.0) == 950.0
        assert float(np.percentile(values, 95.0)) != 950.0

    def test_the_threshold_is_always_a_value_some_member_produced(self) -> None:
        """The property that makes the choice above checkable without restating
        the estimator: a null distribution's cut is one of its own draws."""
        rng = np.random.default_rng(9)
        for size in (7, 50, 101, 1000):
            values = rng.normal(size=size)
            assert cohort_threshold(values, percentile=95.0) in set(values.tolist())

    def test_an_empty_cohort_has_no_threshold(self) -> None:
        with pytest.raises(ValueError, match="empty null distribution"):
            cohort_threshold(np.empty(0, dtype=np.float64), percentile=95.0)

    def test_a_duplicated_member_is_refused(self) -> None:
        """⚠ One draw counted twice NARROWS the null distribution it is supposed
        to widen — and a shard re-run that forgot to delete its old output is
        exactly how it happens."""
        members = (_member(0), _member(0))
        with pytest.raises(ValueError, match="distinct indices"):
            evaluate_control(members, strategy_sharpe=1.0, strategy_return_pct=1.0)

    def test_an_empty_cohort_is_refused(self) -> None:
        with pytest.raises(ValueError, match="empty null distribution"):
            evaluate_control((), strategy_sharpe=1.0, strategy_return_pct=1.0)

    def test_the_strategy_side_travels_through_untouched(self) -> None:
        control = evaluate_control((_member(0),), strategy_sharpe=-0.25, strategy_return_pct=-11.0)
        assert control.strategy_sharpe == -0.25
        assert control.strategy_return_pct == -11.0


class TestMatchResidual:
    def test_an_exact_trade_count_match_is_reported_as_exact(self) -> None:
        members = tuple(_member(index, trade_count=814) for index in range(5))
        residual = match_residual(
            members,
            strategy_trade_count=814,
            strategy_exposure_time_pct=25.0,
            strategy_turnover_annualised=9.0,
        )
        assert residual.trade_count_matches is True

    def test_one_member_short_by_a_single_trade_breaks_the_match(self) -> None:
        """⚠ EQUALITY, and this is why. A tolerance would absorb exactly the
        failure the permutation can have — a series whose holds were dropped —
        and the cohort would still look matched."""
        members = (_member(0, trade_count=814), _member(1, trade_count=813))
        residual = match_residual(
            members,
            strategy_trade_count=814,
            strategy_exposure_time_pct=25.0,
            strategy_turnover_annualised=9.0,
        )
        assert residual.trade_count_matches is False

    def test_exposure_and_turnover_are_reported_as_signed_deltas(self) -> None:
        """⚠ REPORTED, not gated. Equal weight makes a position's capital-days
        depend on how many siblings are open beside it, so the drift is real and
        no source rule fixes how much of it is acceptable."""
        members = (_member(0, exposure_time_pct=22.0, turnover_annualised=7.5),)
        residual = match_residual(
            members,
            strategy_trade_count=100,
            strategy_exposure_time_pct=25.0,
            strategy_turnover_annualised=9.0,
        )
        assert math.isclose(residual.exposure_delta_pct_points, -3.0)
        assert math.isclose(residual.turnover_delta, -1.5)

    def test_an_empty_cohort_is_refused(self) -> None:
        with pytest.raises(ValueError, match="nothing to compare"):
            match_residual((), strategy_trade_count=1, strategy_exposure_time_pct=1.0, strategy_turnover_annualised=1.0)
