"""#3046 — the five composing clauses over one window.

Pure-logic, no DB. ⚠ Most of these cases CANNOT be reached from the live corpus:
clause 1 fires on 0 windows, no series break is resolved, no instrument lacks a
coverage row, and no deferred transition sits inside a day-change window. A table
test is the only thing that exercises them, which is why they are here rather than
left to the full-population A/B.
"""

from __future__ import annotations

from datetime import date

import pytest

from app.services.price_window_verdict import (
    REASON_BAR_RETURN_UNUSABLE,
    REASON_COVERAGE_AFTER_LAST_BAR,
    REASON_COVERAGE_BEFORE_FIRST_BAR,
    REASON_COVERAGE_MISSING,
    REASON_HORIZON_STRETCHED,
    REASON_NON_SESSION_BAR,
    REASON_QUARANTINED_TRANSITION,
    REASON_UNRESOLVED_BREAK,
    REASON_VERDICT_DEFERRED,
    VERDICT_OK,
    VERDICT_QUARANTINED,
    VERDICT_UNVERIFIED,
    WEEKEND_HABIT_MIN_BARS,
    WindowInputs,
    assess_window,
)

FRI = date(2026, 9, 11)
MON = date(2026, 9, 14)
TUE = date(2026, 9, 15)


def _inputs(
    *,
    coverage: tuple[date, date] | None = (date(2020, 1, 1), date(2026, 12, 31)),
    transitions: tuple[date, ...] = (),
    deferred: tuple[date, ...] = (),
    breaks: tuple[date, ...] = (),
    bars: tuple[date, ...] = (),
    weekend_bars: frozenset[date] = frozenset(),
    trades_weekends: bool = False,
    asset_class: str | None = "us_equity",
    habit_bar_count: int = 250,
) -> WindowInputs:
    return WindowInputs(
        coverage=coverage,
        quarantined_transitions=transitions,
        deferred_transitions=deferred,
        unresolved_breaks=breaks,
        return_unusable_bars=bars,
        weekend_bar_dates=weekend_bars,
        asset_class=asset_class,
        habit_bar_count=habit_bar_count,
        trades_weekends=trades_weekends,
    )


class TestTheOrdinaryCase:
    def test_a_friday_to_monday_pair_inside_coverage_is_ok(self) -> None:
        got = assess_window(_inputs(), window_start=FRI, window_end=MON, bar_count=2)
        assert (got.verdict, got.reasons) == (VERDICT_OK, ())

    def test_a_consecutive_weekday_pair_is_ok(self) -> None:
        got = assess_window(_inputs(), window_start=MON, window_end=TUE, bar_count=2)
        assert got.verdict == VERDICT_OK


class TestTheFourClauses:
    def test_clause_1_a_condemned_bar_anywhere_in_the_window_quarantines(self) -> None:
        got = assess_window(_inputs(bars=(MON,)), window_start=FRI, window_end=MON, bar_count=2)
        assert got.verdict == VERDICT_QUARANTINED
        assert REASON_BAR_RETURN_UNUSABLE in got.reasons

    def test_clause_1_is_inclusive_of_the_EARLIER_endpoint(self) -> None:
        """A condemned operand damages the ratio from either side.

        ⚠ This is the one clause that is NOT ``(start, end]``: clauses 2-4 ask about
        something that HAPPENED inside the window, clause 1 asks about the operands.
        """
        got = assess_window(_inputs(bars=(FRI,)), window_start=FRI, window_end=MON, bar_count=2)
        assert REASON_BAR_RETURN_UNUSABLE in got.reasons

    def test_clause_2_an_unresolved_break_quarantines(self) -> None:
        got = assess_window(_inputs(breaks=(MON,)), window_start=FRI, window_end=MON, bar_count=2)
        assert got.verdict == VERDICT_QUARANTINED
        assert REASON_UNRESOLVED_BREAK in got.reasons

    def test_clause_3_a_quarantined_transition_quarantines(self) -> None:
        got = assess_window(_inputs(transitions=(MON,)), window_start=FRI, window_end=MON, bar_count=2)
        assert got.verdict == VERDICT_QUARANTINED
        assert REASON_QUARANTINED_TRANSITION in got.reasons

    def test_a_transition_INTO_the_first_bar_does_not_contaminate(self) -> None:
        """``rule_w1``'s ``(start, end]`` convention, asserted at the boundary."""
        got = assess_window(_inputs(transitions=(FRI,)), window_start=FRI, window_end=MON, bar_count=2)
        assert got.verdict == VERDICT_OK

    def test_clause_4_a_stretched_horizon_quarantines(self) -> None:
        got = assess_window(_inputs(), window_start=date(2026, 6, 12), window_end=date(2026, 6, 24), bar_count=2)
        assert got.verdict == VERDICT_QUARANTINED
        assert REASON_HORIZON_STRETCHED in got.reasons


class TestTheWeekendQualifier:
    """⚠⚠ The regression that Codex checkpoint 1 caught on the spec.

    The obvious gate is the asset class's ``calendar_days_per_bar``. The rule set
    declares ``fx``/``commodity``/``index`` SEVEN-day for hole tolerance, so that
    gate denies them the weekend allowance and fires W2 on an ordinary
    Friday-to-Monday pair — 28 of 63 FX day-changes on the live corpus. The
    instrument's own weekend-session HABIT answers it instead, and no asset class
    reaches this function at all.
    """

    def test_an_ordinary_weekend_gap_is_not_a_stretched_horizon(self) -> None:
        got = assess_window(_inputs(), window_start=FRI, window_end=MON, bar_count=2)
        assert REASON_HORIZON_STRETCHED not in got.reasons

    def test_a_weekend_TRADING_instrument_gets_no_exemption(self) -> None:
        """A seven-day venue's Saturday is a session, so a MISSING one is a hole.

        ⚠⚠ This is Codex checkpoint 2's finding, and the inputs are the finding.
        The earlier version passed the weekend days as STORED BARS while also passing
        ``bar_count=2`` — a state the loader cannot produce, since two weekend bars
        inside ``[FRI, MON]`` make four stored bars. It therefore asserted the rule
        against a fixture that does not exist, and the real case — a 24/7 instrument
        whose weekend bars are ABSENT — returned ``ok``.

        Here the instrument trades weekends and only the two endpoints are stored:
        a genuine two-session hole, which must fire W2.
        """
        got = assess_window(
            _inputs(trades_weekends=True),
            window_start=FRI,
            window_end=MON,
            bar_count=2,
        )
        assert REASON_HORIZON_STRETCHED in got.reasons

    def test_a_five_day_venues_PRINTED_weekend_day_is_not_deducted(self) -> None:
        """The other half of the composition, and 6 live instruments turn on it.

        Wednesday to Sunday with only the two endpoints stored is a gappy window. The
        Sunday carries a bar, so it is not deducted and the span stays 3 trading days
        against 1 interval — stretched. Deducting it (habit-only) shortens the span to
        2 and talks the window out of firing.
        """
        wed, sun = date(2026, 9, 9), date(2026, 9, 13)
        got = assess_window(
            _inputs(weekend_bars=frozenset({sun})),
            window_start=wed,
            window_end=sun,
            bar_count=2,
        )
        assert REASON_HORIZON_STRETCHED in got.reasons

    def test_a_five_day_venues_ABSENT_weekend_day_IS_deducted(self) -> None:
        """The same window with no Sunday print: both weekend days are non-sessions,
        so the span is 2 trading days and the window is ordinary."""
        wed, sun = date(2026, 9, 9), date(2026, 9, 13)
        got = assess_window(_inputs(), window_start=wed, window_end=sun, bar_count=2)
        assert REASON_HORIZON_STRETCHED not in got.reasons

    def test_a_weekend_TRADING_instrument_with_its_sessions_PRESENT_is_ok(self) -> None:
        """The other side of the same rule, so the fix is not just "always fire".

        Friday to Monday with Saturday and Sunday both stored is four bars over three
        days — a complete seven-day series, and nothing is stretched about it.
        """
        got = assess_window(
            _inputs(trades_weekends=True),
            window_start=FRI,
            window_end=MON,
            bar_count=4,
        )
        assert REASON_HORIZON_STRETCHED not in got.reasons

    def test_a_long_span_is_not_excused_by_its_weekends(self) -> None:
        """Codex checkpoint 2's counterexample on the original helper.

        20 bars over 2026-01-05..2026-03-06 is 60 calendar days and 44 weekdays for
        19 intervals — stretched by any reading. The first version returned
        "explained" because it stripped the weekend from the span while leaving the
        nominal at ``1.4``, which already carries the weekend allowance.
        """
        got = assess_window(_inputs(), window_start=date(2026, 1, 5), window_end=date(2026, 3, 6), bar_count=20)
        assert REASON_HORIZON_STRETCHED in got.reasons

    def test_a_single_missing_session_is_INSIDE_the_rules_own_band(self) -> None:
        """Disclosed, not fixed: ``rule_w2``'s gate is ``> 2x`` nominal.

        Friday to Tuesday with Monday absent is 2 session-days for 1 interval, which
        is exactly 2x and therefore admitted. Changing that means changing
        ``rule_w2``, which sits in ``INPUT_RULE_SETS``.
        """
        got = assess_window(_inputs(), window_start=FRI, window_end=TUE, bar_count=2)
        assert REASON_HORIZON_STRETCHED not in got.reasons


class TestCoverageIsAnIntervalNotAnExistenceTest:
    def test_no_coverage_row_reads_unverified_not_clean(self) -> None:
        got = assess_window(_inputs(coverage=None), window_start=FRI, window_end=MON, bar_count=2)
        assert (got.verdict, got.reasons) == (VERDICT_UNVERIFIED, (REASON_COVERAGE_MISSING,))

    def test_a_missing_key_reads_unverified(self) -> None:
        got = assess_window(None, window_start=FRI, window_end=MON, bar_count=2)
        assert (got.verdict, got.reasons) == (VERDICT_UNVERIFIED, (REASON_COVERAGE_MISSING,))

    def test_a_window_past_the_evaluated_interval_reads_unverified(self) -> None:
        got = assess_window(
            _inputs(coverage=(date(2020, 1, 1), FRI)),
            window_start=FRI,
            window_end=MON,
            bar_count=2,
        )
        assert (got.verdict, got.reasons) == (VERDICT_UNVERIFIED, (REASON_COVERAGE_AFTER_LAST_BAR,))

    def test_a_window_before_the_evaluated_interval_reads_unverified(self) -> None:
        got = assess_window(
            _inputs(coverage=(MON, date(2026, 12, 31))),
            window_start=FRI,
            window_end=MON,
            bar_count=2,
        )
        assert (got.verdict, got.reasons) == (VERDICT_UNVERIFIED, (REASON_COVERAGE_BEFORE_FIRST_BAR,))

    def test_a_deferred_verdict_is_unverified_not_clean(self) -> None:
        """⚠ A provisional transition carries EMPTY ``rules`` — it is deferred, not
        admitted (``sql/247:60-68``). A ``cardinality(rules) > 0`` predicate alone
        reads it as clean, which is the hole this closes."""
        got = assess_window(_inputs(deferred=(MON,)), window_start=FRI, window_end=MON, bar_count=2)
        assert (got.verdict, got.reasons) == (VERDICT_UNVERIFIED, (REASON_VERDICT_DEFERRED,))


class TestPrecedence:
    def test_a_fired_clause_outranks_missing_coverage_and_keeps_both_reasons(self) -> None:
        """Positive evidence beats absence — and an instrument with no coverage row
        can still carry verdict rows, so the evidence must not be discarded in order
        to report UNKNOWN."""
        got = assess_window(
            _inputs(coverage=None, transitions=(MON,)),
            window_start=FRI,
            window_end=MON,
            bar_count=2,
        )
        assert got.verdict == VERDICT_QUARANTINED
        assert got.reasons == (REASON_COVERAGE_MISSING, REASON_QUARANTINED_TRANSITION)

    def test_reasons_are_sorted_and_deduplicated(self) -> None:
        got = assess_window(
            _inputs(coverage=(MON, FRI), transitions=(MON,), breaks=(MON,), bars=(MON,)),
            window_start=FRI,
            window_end=MON,
            bar_count=2,
        )
        assert list(got.reasons) == sorted(set(got.reasons))


class TestBoundaries:
    def test_a_reversed_window_raises(self) -> None:
        with pytest.raises(ValueError, match="precedes"):
            assess_window(_inputs(), window_start=MON, window_end=FRI, bar_count=2)

    def test_a_non_positive_bar_count_raises(self) -> None:
        with pytest.raises(ValueError, match="bar_count"):
            assess_window(_inputs(), window_start=FRI, window_end=MON, bar_count=0)

    def test_a_single_bar_window_never_stretches(self) -> None:
        """``rule_w2`` returns False below 2 bars; asserted so the guard above is not
        mistaken for the rule's own floor."""
        got = assess_window(_inputs(), window_start=FRI, window_end=FRI, bar_count=1)
        assert got.verdict == VERDICT_OK

    def test_a_sentinel_bar_between_the_operands_raises_the_bar_count(self) -> None:
        """⚠ The two most recent POSITIVE closes need not be adjacent stored bars.
        A third stored bar makes the same span two intervals, which is why
        ``bar_count`` is read from ``price_daily`` and never assumed to be 2."""
        start, end = date(2026, 6, 12), date(2026, 6, 24)
        two = assess_window(_inputs(), window_start=start, window_end=end, bar_count=2)
        ten = assess_window(_inputs(), window_start=start, window_end=end, bar_count=10)
        assert REASON_HORIZON_STRETCHED in two.reasons
        assert REASON_HORIZON_STRETCHED not in ten.reasons


class TestClause5NonSessionBar:
    """#3046 build item 2 — an endpoint bar dated on a day the venue held no session.

    ⚠ The operand is ``endpoint_bar_dates``, never the window BOUNDS. A bound is a
    calendar instant the caller chose; a stored bar is an observation the vendor sent.
    A caller that supplies nothing gets no clause-5 verdict rather than a guess.
    """

    THU = date(2026, 4, 2)  # ordinary session
    GOOD_FRIDAY = date(2026, 4, 3)  # NYSE full closure, 534 stored us_equity bars
    THANKS_FRI = date(2026, 11, 27)  # 13:00 ET EARLY CLOSE — a real session
    MOURNING = date(2025, 1, 9)  # extraordinary closure (President Carter)
    SATURDAY = date(2026, 9, 12)

    def _assess(self, inputs: WindowInputs | None, *, ends: tuple[date, ...]) -> tuple[str, tuple[str, ...]]:
        got = assess_window(
            inputs,
            window_start=self.THU,
            window_end=self.GOOD_FRIDAY,
            bar_count=2,
            endpoint_bar_dates=ends,
        )
        return got.verdict, got.reasons

    def test_a_us_holiday_endpoint_quarantines(self) -> None:
        verdict, reasons = self._assess(_inputs(), ends=(self.THU, self.GOOD_FRIDAY))
        assert verdict == VERDICT_QUARANTINED
        assert REASON_NON_SESSION_BAR in reasons

    def test_it_fires_on_the_EARLIER_endpoint_too(self) -> None:
        # Both positions matter: the prior close is as much an operand as the latest.
        verdict, reasons = self._assess(_inputs(), ends=(self.GOOD_FRIDAY, self.THU))
        assert verdict == VERDICT_QUARANTINED
        assert REASON_NON_SESSION_BAR in reasons

    def test_a_weekend_endpoint_quarantines(self) -> None:
        _, reasons = self._assess(_inputs(), ends=(self.SATURDAY,))
        assert REASON_NON_SESSION_BAR in reasons

    def test_an_extraordinary_closure_quarantines(self) -> None:
        _, reasons = self._assess(_inputs(), ends=(self.MOURNING,))
        assert REASON_NON_SESSION_BAR in reasons

    def test_a_HALF_DAY_is_a_real_session_and_does_not_fire(self) -> None:
        # A 13:00 ET early close still trades. Full closures only.
        _, reasons = self._assess(_inputs(), ends=(self.THANKS_FRI,))
        assert REASON_NON_SESSION_BAR not in reasons

    def test_identical_endpoints_on_a_closure_fire_once(self) -> None:
        _, reasons = self._assess(_inputs(), ends=(self.GOOD_FRIDAY, self.GOOD_FRIDAY))
        assert reasons.count(REASON_NON_SESSION_BAR) == 1

    def test_no_endpoints_supplied_means_no_clause_5_verdict(self) -> None:
        got = assess_window(_inputs(), window_start=self.THU, window_end=self.GOOD_FRIDAY, bar_count=2)
        assert REASON_NON_SESSION_BAR not in got.reasons

    # -- the four ways it must DECLINE, none of which is "the venue was open" -------

    def test_a_non_us_class_never_fires(self) -> None:
        # No published calendar exists for it. Silence, not an assumption: the Saudi
        # Exchange trades Sunday-Thursday inside the same five-day ``mena_equity``.
        _, reasons = self._assess(_inputs(asset_class="mena_equity"), ends=(self.SATURDAY,))
        assert REASON_NON_SESSION_BAR not in reasons

    def test_an_absent_asset_class_never_fires(self) -> None:
        _, reasons = self._assess(_inputs(asset_class=None), ends=(self.GOOD_FRIDAY,))
        assert REASON_NON_SESSION_BAR not in reasons

    def test_absent_inputs_never_fire(self) -> None:
        verdict, reasons = self._assess(None, ends=(self.GOOD_FRIDAY,))
        assert REASON_NON_SESSION_BAR not in reasons
        assert verdict == VERDICT_UNVERIFIED  # coverage_missing, not a quarantine

    def test_a_measured_weekend_TRADING_instrument_is_exempt(self) -> None:
        # eToro's ``.24-7`` synthetics are typed ``us_equity`` and really do trade
        # Saturdays and US holidays. This is constraint 4 of #3046 residual 2.
        _, reasons = self._assess(
            _inputs(trades_weekends=True, habit_bar_count=152), ends=(self.SATURDAY, self.GOOD_FRIDAY)
        )
        assert REASON_NON_SESSION_BAR not in reasons

    def test_a_habit_below_the_BAR_FLOOR_is_unknown_and_grants_no_exemption_and_no_verdict(self) -> None:
        # ⚠ Measured: ``PSTX.CVR`` scores a weekend ratio of 1.0000 off a SINGLE bar.
        # Below ``WEEKEND_HABIT_MIN_BARS`` the habit is UNKNOWN, so clause 5 declines
        # in BOTH directions — it neither exempts on it nor condemns on its absence.
        _, exempt_side = self._assess(_inputs(trades_weekends=True, habit_bar_count=1), ends=(self.GOOD_FRIDAY,))
        assert REASON_NON_SESSION_BAR not in exempt_side
        _, condemn_side = self._assess(_inputs(trades_weekends=False, habit_bar_count=1), ends=(self.GOOD_FRIDAY,))
        assert REASON_NON_SESSION_BAR not in condemn_side

    def test_a_below_floor_habit_does_not_exempt_CLAUSE_4_either(self) -> None:
        """⚠ The floor is applied in ``assess_window``, not only in the loader's SQL.

        A hand-built ``WindowInputs`` can carry ``trades_weekends=True`` off one bar —
        the loader's rows never can. Before this gate clause 5 declined on it while
        clause 4 honoured it, which is a silent divergence from the loader's semantics
        (review bot NITPICK on PR #3079). A below-floor habit must read as UNKNOWN to
        every clause, which for clause 4 means the weekend days are deducted.
        """
        start, end = date(2026, 9, 9), date(2026, 9, 21)
        measured = assess_window(
            _inputs(trades_weekends=True, habit_bar_count=250), window_start=start, window_end=end, bar_count=2
        )
        asserted = assess_window(
            _inputs(trades_weekends=True, habit_bar_count=1), window_start=start, window_end=end, bar_count=2
        )
        unknown = assess_window(
            _inputs(trades_weekends=False, habit_bar_count=1), window_start=start, window_end=end, bar_count=2
        )
        # A MEASURED seven-day habit keeps the full span, so the gap is visible to W2.
        assert REASON_HORIZON_STRETCHED in measured.reasons
        # An ASSERTED one must behave exactly like the unknown case, not like the measured one.
        assert asserted.reasons == unknown.reasons

    def test_the_floor_boundary_is_inclusive(self) -> None:
        _, at_floor = self._assess(
            _inputs(trades_weekends=False, habit_bar_count=WEEKEND_HABIT_MIN_BARS), ends=(self.GOOD_FRIDAY,)
        )
        assert REASON_NON_SESSION_BAR in at_floor
        _, below = self._assess(
            _inputs(trades_weekends=False, habit_bar_count=WEEKEND_HABIT_MIN_BARS - 1),
            ends=(self.GOOD_FRIDAY,),
        )
        assert REASON_NON_SESSION_BAR not in below

    # -- composition: clause 5 ADDS, it never displaces ----------------------------

    def test_it_composes_with_every_other_clause_without_removing_one(self) -> None:
        got = assess_window(
            _inputs(bars=(self.GOOD_FRIDAY,), transitions=(self.GOOD_FRIDAY,), breaks=(self.GOOD_FRIDAY,)),
            window_start=self.THU,
            window_end=self.GOOD_FRIDAY,
            bar_count=2,
            endpoint_bar_dates=(self.THU, self.GOOD_FRIDAY),
        )
        assert got.verdict == VERDICT_QUARANTINED
        assert set(got.reasons) >= {
            REASON_BAR_RETURN_UNUSABLE,
            REASON_QUARANTINED_TRANSITION,
            REASON_UNRESOLVED_BREAK,
            REASON_NON_SESSION_BAR,
        }

    def test_it_outranks_a_coverage_reason_and_keeps_it(self) -> None:
        got = assess_window(
            _inputs(coverage=(date(2020, 1, 1), date(2026, 4, 1))),
            window_start=self.THU,
            window_end=self.GOOD_FRIDAY,
            bar_count=2,
            endpoint_bar_dates=(self.GOOD_FRIDAY,),
        )
        assert got.verdict == VERDICT_QUARANTINED
        assert REASON_COVERAGE_AFTER_LAST_BAR in got.reasons
