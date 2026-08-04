"""Per-rule table tests for the impossible-bar quarantine (#2261).

ONE TEST CLASS PER RULE, deliberately. Both bugs Codex caught in the S7 spike
(#2247) were "the SQL is not the written rule" — a raw ``high/low`` test where
the prose said *wick*, and range-only rules feeding the *return* quarantine. A
rejection census is plausible at any magnitude, so neither produced a symptom.
Per-rule tests with a named counter-example each are what makes a drift fail
loudly instead of silently re-scoring the corpus.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.price_quarantine import (
    PROVISIONAL_WINDOW_DAYS,
    RULE_SET_ID,
    RULE_SET_VERSION,
    Bar,
    evaluate_series,
    params_for,
    rule_b1,
    rule_b2,
    rule_b3,
    rule_b4,
    rule_w1,
    rule_w2,
)

D = Decimal
AS_OF = date(2026, 8, 4)
LONG_AGO = date(2024, 6, 3)  # far outside the provisional window


def bar(day: date, o: str | None, h: str | None, low: str | None, c: str | None, v: str | None = None) -> Bar:
    return Bar(
        price_date=day,
        open=None if o is None else D(o),
        high=None if h is None else D(h),
        low=None if low is None else D(low),
        close=None if c is None else D(c),
        volume=None if v is None else D(v),
    )


def flat(day: date, price: str, volume: str | None = None) -> Bar:
    return bar(day, price, price, price, price, volume)


class TestB1NonPositiveOrNull:
    @pytest.mark.parametrize(
        "values",
        [
            (None, "10", "9", "9.5"),
            ("9.5", None, "9", "9.5"),
            ("9.5", "10", None, "9.5"),
            ("9.5", "10", "9", None),
            ("0", "10", "9", "9.5"),
            ("9.5", "10", "-1", "9.5"),
            ("9.5", "10", "9", "0"),
        ],
    )
    def test_fires_on_any_null_or_non_positive_field(self, values: tuple[str | None, ...]) -> None:
        assert rule_b1(bar(LONG_AGO, *values)) is True

    def test_clean_bar_passes(self) -> None:
        assert rule_b1(bar(LONG_AGO, "9.5", "10", "9", "9.8")) is False

    def test_sentinel_close_is_positive_and_passes_b1(self) -> None:
        # The 2025-12-24 sentinel closes are 0.01 / 0.0001 — strictly POSITIVE.
        # B1 does not catch them; B4 does, as reverting spikes. A B1 that
        # "obviously" catches sentinels is a misreading of the rule.
        assert rule_b1(flat(LONG_AGO, "0.0001")) is False


class TestB2Containment:
    @pytest.mark.parametrize(
        "values",
        [
            ("9.5", "9", "10", "9.5"),  # high < low
            ("9.5", "10", "9", "12"),  # close above high
            ("9.5", "10", "9", "8"),  # close below low
            ("12", "10", "9", "9.5"),  # open above high
            ("8", "10", "9", "9.5"),  # open below low
        ],
    )
    def test_fires(self, values: tuple[str, ...]) -> None:
        assert rule_b2(bar(LONG_AGO, *values)) is True

    def test_clean_bar_passes(self) -> None:
        assert rule_b2(bar(LONG_AGO, "9.5", "10", "9", "9.8")) is False

    def test_b1_bar_is_b1s_business(self) -> None:
        # A both-false bar must not also be counted as a range-only rejection;
        # double-counting inflates the census the operator reads.
        assert rule_b2(bar(LONG_AGO, "9.5", "10", None, "9.5")) is False


class TestB3PhantomWick:
    def test_fires_on_phantom_low(self) -> None:
        # XPER 2024-06-03: perfect close, bar claims a one-cent trade.
        assert rule_b3(bar(date(2024, 6, 3), "8.497", "8.737", "0.010", "8.298")) is True

    def test_fires_on_phantom_high(self) -> None:
        assert rule_b3(bar(LONG_AGO, "8.4", "40.0", "8.2", "8.3")) is True

    def test_real_move_with_no_wick_is_kept(self) -> None:
        # CNTM 2025-09-17 ran 0.16 -> 4.96, a real +3,000% day. high/low = 31,
        # so a RAW high/low test rejects it; the written rule is a WICK test and
        # keeps it. This is the exact divergence Codex caught in the spike.
        cntm = bar(date(2025, 9, 17), "0.16", "4.96", "0.16", "4.96")
        assert cntm.high is not None and cntm.low is not None
        assert cntm.high / cntm.low > Decimal(30)
        assert rule_b3(cntm) is False


class TestB4RevertingSpike:
    params = params_for("us_equity")

    def test_fires_on_sentinel_that_reverts(self) -> None:
        prev = flat(date(2025, 12, 23), "296.45")
        spike = flat(date(2025, 12, 24), "0.0001")
        nxt = flat(date(2025, 12, 26), "296.45")
        assert rule_b4(prev, spike, nxt, self.params) is True

    def test_does_not_fire_on_a_persistent_level_break(self) -> None:
        # KLAC 2022-07-06 x0.099 does NOT come back. That is a transition-level
        # concern (T3), not a bar defect — the bars either side are valid prices
        # in their own unit regime and must be kept.
        prev = flat(date(2022, 7, 5), "100")
        broke = flat(date(2022, 7, 6), "9.9")
        nxt = flat(date(2022, 7, 7), "10.0")
        assert rule_b4(prev, broke, nxt, self.params) is False

    def test_does_not_fire_across_a_calendar_hole(self) -> None:
        prev = flat(date(2022, 1, 3), "100")
        spike = flat(date(2022, 3, 1), "1")
        nxt = flat(date(2022, 3, 2), "100")
        assert rule_b4(prev, spike, nxt, self.params) is False

    def test_does_not_fire_below_threshold(self) -> None:
        prev = flat(date(2025, 1, 6), "100")
        dip = flat(date(2025, 1, 7), "60")
        nxt = flat(date(2025, 1, 8), "100")
        assert rule_b4(prev, dip, nxt, self.params) is False


class TestTwoVerdictsPerBar:
    """B1/B4 -> both false. B2/B3 -> range only.

    Folding B2/B3 into the return quarantine over-rejected by 587 windows in
    the spike's first draft, and a return-only rule set hands phantom fills to
    the outcome resolver. One verdict class, one column.
    """

    def test_phantom_wick_keeps_returns(self) -> None:
        bars = [
            flat(date(2024, 6, 2), "8.4"),
            bar(date(2024, 6, 3), "8.497", "8.737", "0.010", "8.298"),
            flat(date(2024, 6, 4), "8.3"),
        ]
        verdicts = evaluate_series(bars, "us_equity", as_of=AS_OF).bars
        assert verdicts[1].rules == ("B3",)
        assert verdicts[1].return_usable is True
        assert verdicts[1].range_usable is False

    def test_containment_breach_keeps_returns(self) -> None:
        bars = [flat(date(2024, 6, 2), "8.4"), bar(date(2024, 6, 3), "8.4", "8.7", "8.2", "9.9")]
        verdicts = evaluate_series(bars, "us_equity", as_of=AS_OF).bars
        assert verdicts[1].rules == ("B2",)
        assert (verdicts[1].return_usable, verdicts[1].range_usable) == (True, False)

    def test_b1_breaks_both(self) -> None:
        verdicts = evaluate_series([bar(LONG_AGO, "1", "1", "1", "0")], "us_equity", as_of=AS_OF).bars
        assert (verdicts[0].return_usable, verdicts[0].range_usable) == (False, False)

    def test_clean_bar_is_not_notable_and_is_not_stored(self) -> None:
        verdicts = evaluate_series([bar(LONG_AGO, "9.5", "10", "9", "9.8")], "us_equity", as_of=AS_OF).bars
        assert verdicts[0].notable is False


class TestAssetClassParameters:
    def test_us_equity_and_crypto_get_the_lax_threshold(self) -> None:
        assert params_for("us_equity").magnitude_threshold == Decimal(5)
        assert params_for("crypto").magnitude_threshold == Decimal(5)

    @pytest.mark.parametrize("asset_class", ["fx", "index", "commodity"])
    def test_measured_seven_and_exchange_classes_get_the_strict_threshold(self, asset_class: str) -> None:
        assert params_for(asset_class).magnitude_threshold == Decimal(2)

    @pytest.mark.parametrize("asset_class", [None, "unknown", "something_new"])
    def test_unknown_metadata_gets_the_STRICT_gate(self, asset_class: str | None) -> None:
        # Unknown metadata means unknown normal move size. The safe error is
        # over-containment, which the census shows, not admission, which nothing
        # shows.
        assert params_for(asset_class).magnitude_threshold == Decimal(2)

    @pytest.mark.parametrize("asset_class", ["eu_equity", "uk_equity", "asia_equity", "mena_equity"])
    def test_non_us_equity_gets_equity_parameters_not_the_strict_default(self, asset_class: str) -> None:
        # Documented deviation from S7 §5, which listed only the five classes
        # that had bars to measure. These are known equities, not unknown
        # metadata, and they carry ~0 bars until #2262's seeding lands.
        assert params_for(asset_class) == params_for("us_equity")

    def test_seven_day_markets_get_a_tighter_hole_threshold(self) -> None:
        assert params_for("crypto").hole_days == 4
        assert params_for("us_equity").hole_days == 10


class TestT1EndpointUnusable:
    def test_transition_across_an_unusable_close_is_quarantined(self) -> None:
        bars = [flat(date(2025, 1, 6), "100"), bar(date(2025, 1, 7), "1", "1", "1", "0")]
        transitions = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions
        assert transitions[0].rules == ("T1",)

    def test_phantom_wick_does_not_quarantine_the_transition(self) -> None:
        bars = [
            flat(date(2024, 6, 2), "8.4"),
            bar(date(2024, 6, 3), "8.497", "8.737", "0.010", "8.298"),
        ]
        transitions = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions
        assert transitions[0].quarantined is False


class TestT2SeriesHole:
    def test_weekend_is_a_hole_on_crypto_but_not_on_equities(self) -> None:
        bars = [flat(date(2025, 1, 3), "100"), flat(date(2025, 1, 10), "101")]
        assert evaluate_series(bars, "crypto", as_of=AS_OF).transitions[0].rules == ("T2",)
        assert evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0].rules == ()

    def test_wide_gap_is_a_hole_everywhere(self) -> None:
        bars = [flat(date(2025, 1, 3), "100"), flat(date(2025, 3, 3), "101")]
        assert "T2" in evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0].rules


class TestT3LevelBreak:
    def test_uncorroborated_break_is_quarantined(self) -> None:
        bars = [flat(date(2025, 1, 6), "100"), flat(date(2025, 1, 7), "9.9")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.rules == ("T3",)
        assert transition.corroboration == "unclassifiable"

    def test_turnover_spike_admits_the_break_back(self) -> None:
        # Arm B: turnover (close x volume) is split-invariant, so a spike says
        # the level change was a real move on real interest.
        bars = [flat(date(2025, 1, 6), "1", "1000"), flat(date(2025, 1, 7), "10", "10000")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.corroboration == "spike"
        assert transition.quarantined is False

    def test_flat_turnover_stays_quarantined(self) -> None:
        bars = [flat(date(2025, 1, 6), "1", "10000"), flat(date(2025, 1, 7), "10", "1000")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.corroboration == "flat"
        assert transition.rules == ("T3",)

    def test_t1_explained_transition_does_not_also_fire_t3(self) -> None:
        bars = [flat(date(2025, 1, 6), "100"), bar(date(2025, 1, 7), "1", "1", "1", "0")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.rules == ("T1",)
        assert transition.corroboration == "not_applicable"

    def test_t2_explained_transition_does_not_also_fire_t3(self) -> None:
        # A ratio spanning a series HOLE is not a same-scale comparison, so
        # "is this a level break?" is not a meaningful question about it — and a
        # price_series_break minted from a gap would strand history behind a
        # break that never happened. Including T2 overlaps adds 32 spurious
        # triggers on the full corpus (S7's 148 reproduce exactly without them).
        bars = [flat(date(2025, 1, 3), "100"), flat(date(2025, 3, 3), "9.9")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.rules == ("T2",)
        assert transition.corroboration == "not_applicable"

    def test_admitted_transition_is_still_stored_as_census_evidence(self) -> None:
        # A narrowing gate is measured against what it SAW. Storing only the
        # rejected side makes the denominator unmeasurable — and this row is the
        # audit trail for the one signal that can overturn a quarantine.
        bars = [flat(date(2025, 1, 6), "1", "1000"), flat(date(2025, 1, 7), "10", "10000")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.quarantined is False
        assert transition.notable is True

    def test_below_threshold_move_is_not_a_trigger(self) -> None:
        bars = [flat(date(2025, 1, 6), "100"), flat(date(2025, 1, 7), "60")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.rules == ()
        assert transition.corroboration == "not_applicable"


class TestProvisionalTrailingWindow:
    """Taxonomy class 8 — today's bar is a PARTIAL.

    AAPL 2026-08-04 carried volume 87,572 against 53,121,635 the prior day. T3's
    corroboration reads volume, so a genuine move today reads as turnover ~0.002
    and would be quarantined as split-like. Provisional bars are never
    verdict-bearing.
    """

    def test_recent_bars_are_marked_provisional(self) -> None:
        bars = [flat(AS_OF - timedelta(days=30), "100"), flat(AS_OF, "101")]
        verdicts = evaluate_series(bars, "us_equity", as_of=AS_OF).bars
        assert verdicts[0].provisional is False
        assert verdicts[1].provisional is True

    def test_boundary_bar_is_provisional(self) -> None:
        boundary = AS_OF - timedelta(days=PROVISIONAL_WINDOW_DAYS)
        verdicts = evaluate_series([flat(boundary, "100")], "us_equity", as_of=AS_OF).bars
        assert verdicts[0].provisional is True

    def test_t3_is_deferred_not_decided_on_a_provisional_transition(self) -> None:
        bars = [flat(AS_OF - timedelta(days=1), "1", "53121635"), flat(AS_OF, "10", "87572")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.provisional is True
        assert transition.quarantined is False
        assert transition.turnover_ratio is None
        assert transition.notable is True  # visible as deferred, not silently dropped

    def test_ordinary_provisional_transition_is_not_notable(self) -> None:
        # Provisional ALONE is not a reason to store a row. Every instrument has
        # a few transitions inside the correction window on any run (16,907
        # corpus-wide); one that never approached the magnitude threshold has
        # nothing deferred about it. Storing them made
        # `transitions_provisional_deferred` count all of them while the API
        # described the figure as T3-deferred — an operator-visible number that
        # did not match its own stated rule.
        bars = [flat(AS_OF - timedelta(days=1), "100", "1000"), flat(AS_OF, "101", "1100")]
        transition = evaluate_series(bars, "us_equity", as_of=AS_OF).transitions[0]
        assert transition.provisional is True
        assert transition.corroboration == "not_applicable"
        assert transition.notable is False


class TestWindowRules:
    def test_w1_fires_on_a_quarantined_transition_inside_the_window(self) -> None:
        assert rule_w1(date(2025, 1, 1), date(2025, 2, 1), [date(2025, 1, 15)]) is True

    def test_w1_ignores_the_transition_into_the_first_bar(self) -> None:
        # That transition happened before the window opened.
        assert rule_w1(date(2025, 1, 1), date(2025, 2, 1), [date(2025, 1, 1)]) is False

    def test_w1_ignores_transitions_outside_the_window(self) -> None:
        assert rule_w1(date(2025, 1, 1), date(2025, 2, 1), [date(2025, 3, 1)]) is False

    def test_w2_fires_when_20_bars_span_a_year(self) -> None:
        # SP.24-7 has a single 20-bar window spanning 2025-04-09 -> 2026-04-18.
        assert rule_w2(date(2025, 4, 9), date(2026, 4, 18), 20, params_for("crypto")) is True

    def test_w2_passes_on_a_normal_equity_window(self) -> None:
        assert rule_w2(date(2025, 1, 2), date(2025, 1, 31), 20, params_for("us_equity")) is False


class TestRuleSetVersion:
    def test_version_carries_the_id_and_a_code_hash(self) -> None:
        # "rule-set id + code hash, not an int" (S7 §7) — an int cannot tell you
        # whether two stored rows came from the same code.
        assert RULE_SET_VERSION.startswith(f"{RULE_SET_ID}+")
        assert len(RULE_SET_VERSION.split("+", 1)[1]) == 12
