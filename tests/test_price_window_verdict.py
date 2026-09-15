"""#3046 — the four composing clauses over one window.

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
    REASON_QUARANTINED_TRANSITION,
    REASON_UNRESOLVED_BREAK,
    REASON_VERDICT_DEFERRED,
    VERDICT_OK,
    VERDICT_QUARANTINED,
    VERDICT_UNVERIFIED,
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
) -> WindowInputs:
    return WindowInputs(
        coverage=coverage,
        quarantined_transitions=transitions,
        deferred_transitions=deferred,
        unresolved_breaks=breaks,
        return_unusable_bars=bars,
        weekend_bar_dates=weekend_bars,
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
        span = {"window_start": date(2026, 6, 12), "window_end": date(2026, 6, 24)}
        assert REASON_HORIZON_STRETCHED in assess_window(_inputs(), **span, bar_count=2).reasons
        assert REASON_HORIZON_STRETCHED not in assess_window(_inputs(), **span, bar_count=10).reasons
