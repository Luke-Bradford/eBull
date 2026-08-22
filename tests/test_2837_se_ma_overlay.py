"""#2837 — S-E's 10-month-SMA overlay, against its frozen contract.

Contract: ``docs/proposals/ta/2026-08-22-se-ma-overlay-preregistration.md``.

⚠ PURE. No database, no fixtures, no clock — the module under test is pure and
these tests are the reason it was written that way. The DB half is exercised by
``scripts/measure_2837_se_overlay.py`` against the real corpus.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.se_ma_overlay import (
    ANNUAL_EXEMPT_GBP,
    CGT_HIGHER_RATE,
    LOOKBACK,
    MARCH_2020_WINDOW,
    MAX_DRAWDOWN_RATIO_BAR,
    OFFSETS,
    OPENING_EQUITY_GBP,
    ROUND_TRIP_COST_PCT,
    SIDE_COST_PCT,
    ArmResult,
    DrawdownEpisode,
    OverlayRefused,
    cgt_payment_date,
    drawdown_episodes,
    evaluation_indices,
    march_2020_detail,
    month_end_indices,
    overlay_positions,
    regime_cohorts,
    simulate_arm,
    uk_tax_year_start,
)
from app.services.tax_ledger import _compute_tax_year

BARS_PER_MONTH = 21
SEAM = date(2022, 5, 10)


def _chain(monthly: list[float], *, start_year: int = 2000, tail_bars: int = 0) -> list[tuple[date, float]]:
    """A synthetic chain: ``BARS_PER_MONTH`` bars a month, flat within the month.

    Flat within the month on purpose — the rule only ever reads month-end
    closes, so intra-month shape is noise for every test except the ones that
    deliberately set a specific bar.
    """
    bars: list[tuple[date, float]] = []
    for index, close in enumerate(monthly):
        year = start_year + index // 12
        month = index % 12 + 1
        for day in range(BARS_PER_MONTH):
            bars.append((date(year, month, 1) + timedelta(days=day), close))
    for day in range(tail_bars):
        last_date, last_close = bars[-1]
        bars.append((last_date + timedelta(days=day + 1), last_close))
    return bars


class TestTheMonthEndClock:
    def test_the_trailing_partial_month_contributes_no_month_end(self) -> None:
        """⚠⚠ THE CONTRACT'S §4.1, and the one place a wrong decision has no
        later bar to correct it.

        The real chain ends 2026-07-08. Calling that July's month-end would
        invent a decision the calendar never offered.
        """
        chain = _chain([100.0] * 3)
        # Three whole months of bars: only the first two months are completed by
        # a later bar in a subsequent month.
        assert len(month_end_indices([day for day, _ in chain])) == 2

    def test_a_month_end_is_the_last_bar_before_the_month_changes(self) -> None:
        chain = _chain([100.0] * 4)
        dates = [day for day, _ in chain]
        for index in month_end_indices(dates):
            assert (dates[index].year, dates[index].month) != (dates[index + 1].year, dates[index + 1].month)

    def test_an_empty_chain_yields_no_month_ends(self) -> None:
        assert month_end_indices([]) == ()


class TestTheOffsets:
    @pytest.mark.parametrize("offset", OFFSETS)
    def test_an_offset_shifts_by_chain_positions_not_calendar_days(self, offset: int) -> None:
        """⚠ Positions, not trading days. No exchange calendar is frozen, so
        "+5" can only mean the fifth subsequent chain ROW."""
        chain = _chain([100.0] * 6)
        dates = [day for day, _ in chain]
        ends = month_end_indices(dates)
        assert evaluation_indices(dates, offset) == tuple(i + offset for i in ends if i + offset < len(dates))

    def test_a_shifted_index_past_the_chain_end_is_dropped(self) -> None:
        chain = _chain([100.0] * 3)
        dates = [day for day, _ in chain]
        # The last completed month-end sits BARS_PER_MONTH-1 bars from the end,
        # so a large offset walks off the chain rather than wrapping.
        assert len(evaluation_indices(dates, 40)) < len(month_end_indices(dates))

    def test_a_negative_offset_is_refused(self) -> None:
        with pytest.raises(OverlayRefused, match="negative"):
            evaluation_indices([day for day, _ in _chain([100.0] * 3)], -1)


class TestTheSignal:
    def test_warm_up_holds_rather_than_sitting_out(self) -> None:
        """⚠ The overlay is insurance on an ALREADY-INVESTED core. Sitting out
        the warm-up would make the arm a market-timing bet on its first ten
        months, which is a different rule."""
        closes = [100.0 - index for index in range(LOOKBACK + 2)]
        assert overlay_positions(closes)[: LOOKBACK - 1] == (1,) * (LOOKBACK - 1)

    def test_a_close_below_its_sma_goes_to_cash(self) -> None:
        closes = [100.0] * (LOOKBACK - 1) + [50.0]
        assert overlay_positions(closes)[-1] == 0

    def test_an_exact_equality_is_cash_not_held(self) -> None:
        """⚠ Strictly greater, no tolerance. A tolerance would be a second,
        undeclared parameter — and equality is exactly reachable on a flat
        series, so this is not a hypothetical boundary."""
        assert overlay_positions([100.0] * LOOKBACK)[-1] == 0

    def test_a_non_finite_close_inside_the_window_refuses(self) -> None:
        closes = [100.0] * (LOOKBACK - 1) + [float("nan"), 100.0]
        with pytest.raises(OverlayRefused, match="non-positive or non-finite"):
            overlay_positions(closes)


class TestIntervalOwnership:
    """⚠⚠ WHERE A TIMING BUG WOULD HIDE, so it gets its own class.

    The return from an evaluation close to the following EXECUTION close belongs
    to the OLD position. Getting it backwards hands the rule the exact move it
    is being tested on — the overlay would dodge a crash it decided to dodge
    only after seeing it.
    """

    def _falling_chain(self) -> list[tuple[date, float]]:
        # Ten flat months arm the SMA; month 11 closes below it, so the decision
        # at month 11's end is CASH, executing on month 12's first bar.
        monthly = [100.0] * LOOKBACK + [50.0] + [50.0] * 6
        return _chain(monthly)

    def test_the_drop_into_the_execution_bar_is_taken_by_the_old_position(self) -> None:
        chain = self._falling_chain()
        # Force a further halving on the execution bar itself.
        execution_index = (
            next(index for index, (_, close) in enumerate(chain) if close == 50.0) + BARS_PER_MONTH
        )  # first bar of the month AFTER the signal month
        chain[execution_index] = (chain[execution_index][0], 25.0)
        result = simulate_arm(chain, offset=0, seam=SEAM, dividend_yield_pp=0.0)
        # Both arms must show the fall: the overlay was still invested into it.
        overlay_at = dict(zip(result.equity_dates, result.overlay_equity, strict=True))
        benchmark_at = dict(zip(result.equity_dates, result.benchmark_equity, strict=True))
        execution_date = chain[execution_index][0]
        assert overlay_at[execution_date] < OPENING_EQUITY_GBP
        assert benchmark_at[execution_date] < OPENING_EQUITY_GBP

    def test_once_in_cash_the_overlay_stops_tracking_the_index(self) -> None:
        monthly = [100.0] * LOOKBACK + [50.0, 10.0, 10.0, 10.0]
        result = simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)
        assert 0 in result.positions
        # The benchmark rides the whole collapse; the overlay must not.
        assert result.benchmark_max_drawdown_pct > result.overlay_max_drawdown_pct


class TestTheUkTaxRules:
    @pytest.mark.parametrize("day_offset", range(0, 800, 7))
    def test_the_tax_year_rule_agrees_with_the_ledger(self, day_offset: int) -> None:
        """⚠ ONE RULE, TWO SPELLINGS. ``tax_ledger._compute_tax_year`` is private
        and returns a display string, so this module keeps a local integer form —
        which is only safe while the two provably agree."""
        day = date(2024, 1, 1) + timedelta(days=day_offset)
        assert _compute_tax_year(day).split("/")[0] == str(uk_tax_year_start(day))

    def test_the_boundary_days_land_in_the_right_year(self) -> None:
        assert uk_tax_year_start(date(2025, 4, 5)) == 2024
        assert uk_tax_year_start(date(2025, 4, 6)) == 2025

    def test_the_payment_date_is_the_following_31_january(self) -> None:
        """Tax year 2020/21 ends 2021-04-05 and is payable 2022-01-31."""
        assert cgt_payment_date(2020) == date(2022, 1, 31)

    def test_losses_carry_forward_but_the_exemption_does_not(self) -> None:
        """⚠ HMRC treats them differently and so must this. Carrying the
        exemption would understate the drag; refusing to carry losses would
        overstate it."""
        # A loss year then a gain year: the loss must reduce the later charge,
        # and the unused exemption from the loss year must not.
        monthly = [100.0] * LOOKBACK + [50.0, 200.0, 200.0] + [200.0] * 30
        result = simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)
        assert result.tax_charges, "a flipping arm with a realised gain must produce a charge"
        for charge in result.tax_charges:
            assert charge.taxable_gbp == pytest.approx(max(max(charge.net_gain_gbp, 0.0) - ANNUAL_EXEMPT_GBP, 0.0))
            assert charge.tax_gbp == pytest.approx(charge.taxable_gbp * CGT_HIGHER_RATE)


class TestDrawdownEpisodes:
    def test_a_dip_inside_an_unrecovered_decline_is_one_episode(self) -> None:
        """⚠ Otherwise one 2008 reports as four, and every episode COUNT built
        on it — including §9's ≥15% class — inflates."""
        dates = [date(2020, 1, 1) + timedelta(days=index) for index in range(6)]
        episodes = drawdown_episodes(dates, [100.0, 80.0, 90.0, 60.0, 95.0, 105.0])
        assert len(episodes) == 1
        assert episodes[0].depth_pct == pytest.approx(40.0)

    def test_a_terminal_unrecovered_episode_is_reported_and_flagged(self) -> None:
        """⚠ Dropping it would let a curve that ends in the deepest hole of its
        life report the second deepest."""
        dates = [date(2020, 1, 1) + timedelta(days=index) for index in range(4)]
        episodes = drawdown_episodes(dates, [100.0, 120.0, 90.0, 70.0])
        assert episodes[0].unrecovered
        assert episodes[0].recovery_date is None
        assert episodes[0].depth_pct == pytest.approx(100 * (120.0 - 70.0) / 120.0)

    def test_episodes_come_back_deepest_first(self) -> None:
        dates = [date(2020, 1, 1) + timedelta(days=index) for index in range(7)]
        episodes = drawdown_episodes(dates, [100.0, 90.0, 100.0, 100.0, 50.0, 100.0, 100.0])
        assert [round(episode.depth_pct) for episode in episodes] == [50, 10]

    def test_a_monotonically_rising_curve_has_no_episodes(self) -> None:
        dates = [date(2020, 1, 1) + timedelta(days=index) for index in range(4)]
        assert drawdown_episodes(dates, [1.0, 2.0, 3.0, 4.0]) == ()

    def test_mismatched_lengths_are_refused(self) -> None:
        with pytest.raises(OverlayRefused, match="disagree in length"):
            drawdown_episodes([date(2020, 1, 1)], [1.0, 2.0])


class TestThePassBar:
    def _armed(self, monthly: list[float]) -> ArmResult:
        return simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)

    def test_drawdowns_are_reported_as_non_negative_magnitudes(self) -> None:
        """⚠⚠ THE SIGN CONVENTION IS THE PASS BAR'S SAFETY. ``max_drawdown_pct``
        returns a NON-POSITIVE number; if a magnitude were compared as a signed
        value, ``overlay <= 2/3 × benchmark`` would invert and a WORSE overlay
        would pass."""
        result = self._armed([100.0] * LOOKBACK + [50.0, 10.0, 10.0, 10.0])
        assert result.overlay_max_drawdown_pct >= 0.0
        assert result.benchmark_max_drawdown_pct >= 0.0

    def test_a_benchmark_that_never_drew_down_fails_the_insurance_leg(self) -> None:
        """⚠ ``None`` FAILS. An overlay cannot evidence insurance against a loss
        that never happened — and a guarded division that returned "pass" would
        be the worst possible reading of a monotone benchmark."""
        result = self._armed([100.0 + index for index in range(LOOKBACK + 8)])
        assert result.benchmark_max_drawdown_pct == pytest.approx(0.0)
        assert result.drawdown_ratio is None
        assert not result.drawdown_leg_passes
        assert not result.passes

    def test_both_legs_are_required(self) -> None:
        result = self._armed([100.0] * LOOKBACK + [50.0, 10.0, 10.0, 10.0])
        assert result.passes == (result.drawdown_leg_passes and result.cagr_leg_passes)

    def test_the_dividend_drag_only_ever_lowers_the_delta(self) -> None:
        """§3.1 — the drag is charged, so a non-zero yield can only make the
        CAGR leg harder, never easier."""
        monthly = [100.0] * LOOKBACK + [50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        without = simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)
        with_yield = simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=2.0)
        assert with_yield.fraction_in_cash > 0.0
        assert with_yield.net_cagr_delta_pp < without.net_cagr_delta_pp

    def test_a_degenerate_arm_is_refused_rather_than_reported(self) -> None:
        with pytest.raises(OverlayRefused, match="degenerate arm"):
            simulate_arm(_chain([100.0] * 3), offset=0, seam=SEAM, dividend_yield_pp=0.0)


class TestTheDeclaredConstants:
    def test_the_cost_band_splits_the_round_trip_in_half(self) -> None:
        assert SIDE_COST_PCT * 2 == pytest.approx(ROUND_TRIP_COST_PCT)

    def test_the_contract_constants_are_the_ones_the_ticket_declared(self) -> None:
        """⚠ Pinned so a later edit to any of them fails here and has to be
        argued as a NEW declared search rather than slipped in as a tweak."""
        assert (LOOKBACK, OFFSETS, ROUND_TRIP_COST_PCT) == (10, (0, 5, 10), 0.322)
        assert (OPENING_EQUITY_GBP, CGT_HIGHER_RATE, ANNUAL_EXEMPT_GBP) == (50_000.0, 0.24, 3000.0)
        assert MAX_DRAWDOWN_RATIO_BAR == pytest.approx(2 / 3)


class TestTheSeamReport:
    def test_windows_straddling_the_seam_are_counted(self) -> None:
        """§9 — the only place a residual vendor level step could enter a
        SIGNAL, so it is surfaced rather than assumed away."""
        monthly = [100.0] * 30
        chain = _chain(monthly, start_year=2021)
        result = simulate_arm(chain, offset=0, seam=date(2022, 5, 10), dividend_yield_pp=0.0)
        # Ten-bar windows around a seam that falls mid-series: at most LOOKBACK-1
        # windows can straddle it, and at least one must.
        assert 1 <= result.seam_spanning_windows <= LOOKBACK - 1

    def test_a_seam_outside_the_evaluated_span_straddles_nothing(self) -> None:
        result = simulate_arm(_chain([100.0] * 30), offset=0, seam=date(1980, 1, 1), dividend_yield_pp=0.0)
        assert result.seam_spanning_windows == 0


class TestTheSymmetricSensitivity:
    """§7 — the variant that liquidates both arms and taxes the result.

    ⚠⚠ CODEX CHECKPOINT 2 FOUND THIS ONE. The first draft captured the symmetric
    figure BEFORE the terminal accrual, so it omitted taxes the primary equity
    had already been charged and the sensitivity read better than the thing it is
    a sensitivity on — the one direction a declared sensitivity must not fail in.
    """

    def _armed(self) -> ArmResult:
        monthly = [100.0] * LOOKBACK + [50.0, 200.0, 210.0, 220.0] + [230.0] * 20
        return simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)

    def test_liquidating_never_leaves_an_arm_better_off_than_not(self) -> None:
        result = self._armed()
        assert result.symmetric_overlay_terminal_gbp <= result.overlay_equity[-1] + 1e-9
        assert result.symmetric_benchmark_terminal_gbp <= result.benchmark_equity[-1] + 1e-9

    def test_the_liquidation_tax_is_incremental_not_a_second_charge_on_the_year(self) -> None:
        """⚠ The open year's own charge is settled by the terminal accrual. If
        §7 subtracted the FULL charge on the terminal gain the year would be
        taxed twice, and the gap below would exceed 24% of the terminal gain."""
        result = self._armed()
        charged = result.overlay_equity[-1] - result.symmetric_overlay_terminal_gbp
        assert charged >= 0.0
        assert charged <= CGT_HIGHER_RATE * result.overlay_equity[-1]

    def test_a_benchmark_gain_inside_the_exemption_is_untaxed(self) -> None:
        monthly = [100.0] * LOOKBACK + [100.01] * 6
        result = simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)
        assert result.benchmark_equity[-1] - OPENING_EQUITY_GBP < ANNUAL_EXEMPT_GBP
        assert result.symmetric_benchmark_terminal_gbp == pytest.approx(result.benchmark_equity[-1])


class TestTheRegimeCohorts:
    def _armed(self) -> ArmResult:
        monthly = [100.0] * LOOKBACK + [50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0]
        return simulate_arm(_chain(monthly), offset=0, seam=SEAM, dividend_yield_pp=0.0)

    def test_an_unverdicted_bar_is_its_own_cohort_never_folded_in(self) -> None:
        """⚠ A bar the classifier could not verdict is not evidence about any
        regime. Folding warm-up into a real cohort would attribute returns to a
        market state nobody observed."""
        result = self._armed()
        cohorts = regime_cohorts(result, [None] * len(result.evaluation_dates))
        assert [cohort.regime for cohort in cohorts] == ["warm_up"]

    def test_the_cohort_keys_on_the_deciding_evaluation_date(self) -> None:
        """⚠ Not on the holding period. The question is what the rule did when it
        DECIDED under a regime, and the decision predates the interval."""
        result = self._armed()
        regimes: list[str | None] = ["calm"] * len(result.evaluation_dates)
        regimes[0] = "stormy"
        cohorts = {cohort.regime: cohort for cohort in regime_cohorts(result, regimes)}
        assert cohorts["stormy"].intervals == 1
        assert cohorts["calm"].intervals == len(result.execution_dates) - 2

    def test_the_cohorts_partition_the_intervals_exactly(self) -> None:
        result = self._armed()
        regimes: list[str | None] = ["a" if index % 2 else "b" for index in range(len(result.evaluation_dates))]
        cohorts = regime_cohorts(result, regimes)
        assert sum(cohort.intervals for cohort in cohorts) == len(result.execution_dates) - 1

    def test_a_regime_series_of_the_wrong_length_is_refused(self) -> None:
        result = self._armed()
        with pytest.raises(OverlayRefused, match="disagree in length"):
            regime_cohorts(result, ["calm"])


class TestTheMarchTwentyTwentyWindow:
    def test_every_decision_in_the_frozen_window_is_returned(self) -> None:
        """⚠ The window and the fields are predeclared, so the narration cannot
        be selected after the look — every decision, not the interesting ones."""
        chain = _chain([100.0] * LOOKBACK + [90.0] * 14, start_year=2019)
        result = simulate_arm(chain, offset=0, seam=SEAM, dividend_yield_pp=0.0)
        start, end = MARCH_2020_WINDOW
        expected = [day for day in result.evaluation_dates if start <= day <= end]
        assert [row[0] for row in march_2020_detail(result)] == expected


class TestDrawdownEpisodeShape:
    def test_the_episode_record_carries_its_own_recovery_verdict(self) -> None:
        recovered = DrawdownEpisode(date(2020, 1, 1), date(2020, 2, 1), date(2020, 3, 1), 12.0)
        assert not recovered.unrecovered
