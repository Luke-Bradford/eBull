"""S-10 relative-strength leader — module rules + the refinement contract (#2437).

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §S-10; plan +
Codex ckpt-1 resolutions ``docs/proposals/ta/2026-08-14-s10-implementation-plan.md``.
Modules under test: ``app/services/strategies/s10_relative_strength_leader.py``
and the ``admissible_indices`` / ``mandatory_indices`` refinements added to
``app/services/strategy_registry.py`` for it.

The resolution rule under test, everywhere a staged bar survives every refusal:

    fired iff mandatory OR (selected AND admissible)

with precedence pinned: ``no_fill_bar`` → unevaluable input → non-decision
``not_fired`` → ``thin_cross_section`` → the rule. Mandatory beats NEITHER a
refusal NOR a thin panel.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries, IndicatorSeries
from app.services.market_regime import Regime, RegimeSeries
from app.services.strategies.s10_relative_strength_leader import (
    LOOKBACK_BARS,
    SMA_BARS,
    return_series,
    s10_entry_member,
    s10_entry_select,
    s10_exit_member,
    s10_exit_select,
    s10_rebalance_dates,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_registry import (
    CrossSectionalMember,
    StrategyInput,
    evaluate_cross_sectional,
    stage_cross_sectional_member,
)
from app.services.strategy_segmented_evaluation import segmented_member
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"
S10 = "s10-relative-strength-leader"

#: 100 consecutive calendar days from 2024-01-01. The first rebalance with the
#: 63-interval score warm the whole panel is 2024-04-01 (index 91, a Monday).
START = date(2024, 1, 1)
N_BARS = 100
DATES = tuple(START + timedelta(days=i) for i in range(N_BARS))
DECISION = date(2024, 4, 1)
DECISION_INDEX = DATES.index(DECISION)


def _bars(closes: list[float] | list[float | None]) -> BarSeries:
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(round(c, 6))),
            "high": None if c is None else Decimal(str(round(c + 1, 6))),
            "low": None if c is None else Decimal(str(round(c - 1, 6))),
            "close": None if c is None else Decimal(str(round(c, 6))),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    return BarSeries(dates=DATES[: len(closes)], rows=tuple(rows))


def _regime(value: Regime | None = Regime.BULL_QUIET, *, holes: tuple[int, ...] = ()) -> RegimeSeries:
    values: list[Regime | None] = [value] * N_BARS
    for index in holes:
        values[index] = None
    return RegimeSeries(values=tuple(values), not_evaluable_indices=holes)


def _rising(final: float) -> list[float]:
    """Linear 100 → 100*(1+final); close > trailing mean everywhere after warm-up."""
    return [100.0 * (1.0 + final * i / (N_BARS - 1)) for i in range(N_BARS)]


def _rising_then_falling(peak: float) -> list[float]:
    """Strong rise to bar 79 then a fall — 63-bar return still high at the
    decision bar, close < 50-SMA there. The selected-but-inadmissible shape."""
    up = [100.0 * (1.0 + peak * i / 79) for i in range(80)]
    top = up[-1]
    down = [top * (1.0 - 0.012 * (i + 1)) for i in range(N_BARS - 80)]
    return up + down


def _plain_sma(closes: list[float], index: int) -> float:
    """Independent check of the fixture's close-vs-SMA relation — NOT the
    module's own sma_series, which would make the assertion circular."""
    return sum(closes[index - SMA_BARS + 1 : index + 1]) / SMA_BARS


class TestRebalanceCalendar:
    def test_weekends_never_take_a_month(self) -> None:
        """2024-06-01 is a Saturday; an unfiltered first-bar rule would hand
        June's one rebalance to an 11-instrument junk date (measured on
        #2437). The first WEEKDAY takes it instead."""
        calendar = [date(2024, 5, 1) + timedelta(days=i) for i in range(45)]
        picked = s10_rebalance_dates(calendar)
        assert date(2024, 6, 3) in picked
        assert date(2024, 6, 1) not in picked and date(2024, 6, 2) not in picked

    def test_the_first_date_is_not_a_rebalance(self) -> None:
        calendar = [date(2024, 5, 1) + timedelta(days=i) for i in range(10)]
        assert date(2024, 5, 1) not in s10_rebalance_dates(calendar)

    def test_the_manifest_calendar_is_this_rule(self) -> None:
        calendar = [date(2024, 5, 1) + timedelta(days=i) for i in range(45)]
        assert STRATEGY_MANIFEST[S10].decision_calendar(calendar) == s10_rebalance_dates(calendar)


class TestReturnSeries:
    def test_warmup_then_the_63_interval_ratio(self) -> None:
        closes = _rising(0.5)
        series = return_series(_bars(closes), universe=UNIVERSE)
        assert series.values[LOOKBACK_BARS - 1] is None
        got = series.values[LOOKBACK_BARS]
        assert got is not None
        assert got == pytest.approx(closes[LOOKBACK_BARS] / closes[0] - 1.0)

    def test_a_masked_endpoint_refuses_rather_than_interpolates(self) -> None:
        closes: list[float | None] = list(_rising(0.5))
        closes[DECISION_INDEX - LOOKBACK_BARS] = None
        series = return_series(_bars(closes), universe=UNIVERSE)
        assert series.values[DECISION_INDEX] is None
        assert DECISION_INDEX in series.not_evaluable_indices


class TestSelects:
    def test_entry_is_the_floor_decile_with_the_frozen_tie_break(self) -> None:
        scores = {key: 1.0 for key in range(1, 21)}  # 20 members, all tied
        winners = s10_entry_select(DECISION, scores)
        assert winners == frozenset({1, 2})  # 20 // 10 = 2, ids ascending on the tie

    def test_exit_is_the_complement_of_the_band(self) -> None:
        scores = {key: float(100 - key) for key in range(1, 21)}  # 1 is best
        leavers = s10_exit_select(DECISION, scores)
        assert leavers == frozenset(range(7, 21))  # band = 3*20//10 = 6 → 1..6 stay

    def test_a_sub_decile_panel_selects_nobody(self) -> None:
        assert s10_entry_select(DECISION, {1: 1.0, 2: 2.0}) == frozenset()


class TestEntryMember:
    def test_sub_dollar_bars_are_not_decision_bars(self) -> None:
        closes = [0.5 * (1.0 + 0.3 * i / (N_BARS - 1)) for i in range(N_BARS)]
        member = s10_entry_member(
            _bars(closes),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
            regime=_regime(),
        )
        assert member.decision_indices == frozenset()

    def test_below_sma_or_wrong_regime_is_inadmissible(self) -> None:
        falling = _rising_then_falling(0.8)
        assert falling[DECISION_INDEX] < _plain_sma(falling, DECISION_INDEX)
        member = s10_entry_member(
            _bars(falling),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
            regime=_regime(),
        )
        assert member.decision_indices == frozenset({DECISION_INDEX})
        assert member.admissible_indices == frozenset()

        rising = _rising(0.5)
        bear = s10_entry_member(
            _bars(rising),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
            regime=_regime(Regime.BEAR_QUIET),
        )
        assert bear.admissible_indices == frozenset()

    def test_a_benchmark_hole_refuses_as_missing_market_context(self) -> None:
        member = s10_entry_member(
            _bars(_rising(0.5)),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
            regime=_regime(holes=(DECISION_INDEX,)),
        )
        staged = stage_cross_sectional_member(member)
        verdict = staged.verdicts[DECISION_INDEX]
        assert verdict is not None
        assert (verdict.verdict, verdict.reason) == ("not_evaluable", "missing_market_context")

    def test_equality_with_the_sma_admits_nothing(self) -> None:
        flat = [100.0] * N_BARS  # close == 50-SMA exactly, and 63-bar return 0
        member = s10_entry_member(
            _bars(flat),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
            regime=_regime(),
        )
        assert member.decision_indices == frozenset({DECISION_INDEX})
        assert member.admissible_indices == frozenset()


class TestExitMember:
    def test_below_sma_is_mandatory_and_a_benchmark_hole_does_not_refuse(self) -> None:
        """The exit leg declares NO regime — a missing benchmark session must
        never refuse the exit verdict for an open position (S-7's rule)."""
        falling = _rising_then_falling(0.8)
        member = s10_exit_member(
            _bars(falling),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
        )
        assert member.mandatory_indices == frozenset({DECISION_INDEX})
        staged = stage_cross_sectional_member(member, kind="exit")
        assert staged.verdicts[DECISION_INDEX] is None  # participates; not refused

    def test_equality_with_the_sma_is_not_an_exit(self) -> None:
        flat = [100.0] * N_BARS
        member = s10_exit_member(
            _bars(flat),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
        )
        assert member.mandatory_indices == frozenset()

    def test_sub_dollar_names_still_rank_on_the_exit_panel(self) -> None:
        closes = [0.5] * N_BARS
        member = s10_exit_member(
            _bars(closes),
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            close_reason=REASON,
        )
        assert member.decision_indices == frozenset({DECISION_INDEX})


def _panel(regime_value: Regime = Regime.BULL_QUIET) -> dict[int, BarSeries]:
    """20 members: 1 = best rising, 2 = second-best but below its SMA at the
    decision bar, 3.. descending returns, 20 = worst."""
    finals = {key: 0.6 - 0.03 * (key - 1) for key in range(1, 21)}
    panel = {key: _bars(_rising(finals[key])) for key in range(1, 21) if key != 2}
    panel[2] = _bars(_rising_then_falling(1.2))
    return panel


class TestPanelResolution:
    """The rule matrix, end to end through ``evaluate_cross_sectional`` with
    the S-10 members and selects. ``min_participants`` is 10 here rather than
    the shipped 1000 — the floor is a refusal threshold, not part of the rule,
    and the shipped value is exercised by the full-population census."""

    def _run(self, regime_value: Regime = Regime.BULL_QUIET) -> tuple[dict, dict]:
        panel = _panel()
        regime = _regime(regime_value)
        entries = evaluate_cross_sectional(
            members={
                key: s10_entry_member(
                    series,
                    panel_decision_dates=frozenset({DECISION}),
                    universe=UNIVERSE,
                    close_reason=REASON,
                    regime=regime,
                )
                for key, series in panel.items()
            },
            select=s10_entry_select,
            min_participants=10,
            kind="entry",
        )
        exits = evaluate_cross_sectional(
            members={
                key: s10_exit_member(
                    series,
                    panel_decision_dates=frozenset({DECISION}),
                    universe=UNIVERSE,
                    close_reason=REASON,
                )
                for key, series in panel.items()
            },
            select=s10_exit_select,
            min_participants=10,
            kind="exit",
        )
        return entries, exits

    def test_the_matrix_at_the_decision_bar(self) -> None:
        member2_closes = _rising_then_falling(1.2)
        member3_closes = _rising(0.6 - 0.03 * 2)
        assert member2_closes[DECISION_INDEX] < _plain_sma(member2_closes, DECISION_INDEX)
        # The fixture's load-bearing ordering: member 2's 63-bar return beats
        # member 3's, so 2 is genuinely SELECTED and only the SMA blocks it.
        member2_return = member2_closes[DECISION_INDEX] / member2_closes[DECISION_INDEX - LOOKBACK_BARS] - 1.0
        member3_return = member3_closes[DECISION_INDEX] / member3_closes[DECISION_INDEX - LOOKBACK_BARS] - 1.0
        assert member2_return > member3_return

        entries, exits = self._run()
        entry_at = {key: signals[DECISION_INDEX] for key, signals in entries.items()}
        exit_at = {key: signals[DECISION_INDEX] for key, signals in exits.items()}

        # k = 20 // 10 = 2: members 2 (highest 63-bar return) and 1 are selected.
        # 1 fires; 2 is selected-but-below-SMA — not_fired WITHOUT backfilling 3.
        assert entry_at[1].verdict == "fired"
        assert entry_at[2].verdict == "not_fired"
        assert entry_at[3].verdict == "not_fired"

        # Exit band = 3 * 20 // 10 = 6. Member 2 is INSIDE the band and below
        # its SMA → mandatory exit. Member 3 is inside and above → holds.
        # Member 20 is outside the band → exits by selection.
        assert exit_at[2].verdict == "fired"
        assert exit_at[3].verdict == "not_fired"
        assert exit_at[20].verdict == "fired"

    def test_a_bear_regime_blocks_every_entry_and_no_exit(self) -> None:
        entries, exits = self._run(Regime.BEAR_QUIET)
        entry_verdicts = {signals[DECISION_INDEX].verdict for signals in entries.values()}
        assert entry_verdicts == {"not_fired"}
        assert exits[20][DECISION_INDEX].verdict == "fired"

    def test_thin_panel_refuses_both_legs_mandatory_included(self) -> None:
        """Mandatory does NOT beat ``thin_cross_section`` — the refusal comes
        first, so a sliver panel cannot fire the below-SMA exit either."""
        panel = {2: _panel()[2]}
        exits = evaluate_cross_sectional(
            members={
                2: s10_exit_member(
                    panel[2],
                    panel_decision_dates=frozenset({DECISION}),
                    universe=UNIVERSE,
                    close_reason=REASON,
                )
            },
            select=s10_exit_select,
            min_participants=10,
            kind="exit",
        )
        verdict = exits[2][DECISION_INDEX]
        assert (verdict.verdict, verdict.reason) == ("not_evaluable", "thin_cross_section")


class TestRefinementContract:
    """The registry-level refinements, on synthetic members — including the
    S-2 regression: both ``None`` is exactly the old behaviour."""

    @staticmethod
    def _member(
        *,
        admissible: frozenset[int] | None,
        mandatory: frozenset[int] | None,
    ) -> CrossSectionalMember:
        dates = (date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3))
        score = IndicatorSeries(values=(1.0, 2.0, 3.0), universe=UNIVERSE)
        return CrossSectionalMember(
            dates=dates,
            inputs=(StrategyInput(series=score, reason=REASON),),
            score=score,
            decision_indices=frozenset({1}),
            admissible_indices=admissible,
            mandatory_indices=mandatory,
        )

    def test_none_refinements_are_the_old_rule(self) -> None:
        member = self._member(admissible=None, mandatory=None)
        selected = evaluate_cross_sectional(
            members={7: member}, select=lambda when, scores: frozenset(scores), min_participants=1
        )
        assert selected[7][1].verdict == "fired"
        unselected = evaluate_cross_sectional(
            members={7: member}, select=lambda when, scores: frozenset(), min_participants=1
        )
        assert unselected[7][1].verdict == "not_fired"

    def test_selected_but_inadmissible_is_not_fired(self) -> None:
        member = self._member(admissible=frozenset(), mandatory=None)
        out = evaluate_cross_sectional(
            members={7: member}, select=lambda when, scores: frozenset(scores), min_participants=1
        )
        assert out[7][1].verdict == "not_fired"

    def test_mandatory_fires_unselected(self) -> None:
        member = self._member(admissible=None, mandatory=frozenset({1}))
        out = evaluate_cross_sectional(members={7: member}, select=lambda when, scores: frozenset(), min_participants=1)
        assert out[7][1].verdict == "fired"

    def test_mandatory_wins_over_inadmissible(self) -> None:
        """fired iff mandatory OR (selected AND admissible) — the OR is real."""
        member = self._member(admissible=frozenset(), mandatory=frozenset({1}))
        out = evaluate_cross_sectional(members={7: member}, select=lambda when, scores: frozenset(), min_participants=1)
        assert out[7][1].verdict == "fired"

    def test_a_refinement_outside_decision_indices_raises(self) -> None:
        with pytest.raises(ValueError, match="outside decision_indices"):
            self._member(admissible=frozenset({0}), mandatory=None)
        with pytest.raises(ValueError, match="outside decision_indices"):
            self._member(admissible=None, mandatory=frozenset({2}))

    def test_an_inadmissible_score_still_ranks_the_others(self) -> None:
        """The panel denominator is unchanged — that is the whole point."""
        member = self._member(admissible=frozenset(), mandatory=None)
        staged = stage_cross_sectional_member(member)
        assert date(2024, 1, 2) in staged.scores


class TestManifestAdaptersMatchTheDirectCalls:
    """The staging the scan and backtest share (``segmented_member``) must be
    the module's own members — verdicts, scores AND refinements. The
    resolution itself cannot drift: all three resolvers call
    ``resolve_participating_bar``, which is the single source of the rule."""

    @pytest.mark.parametrize("leg", ["entry", "exit"])
    def test_segmented_staging_equals_direct_staging(self, leg: str) -> None:
        entry = STRATEGY_MANIFEST[S10]
        series = _panel()[2]
        regime = _regime()
        via_runner = segmented_member(
            entry,
            series,
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            masked_reason=REASON,
            unresolved_breaks=(),
            regime=regime,
            leg=leg,  # type: ignore[arg-type]
        )
        if leg == "entry":
            direct = s10_entry_member(
                series,
                panel_decision_dates=frozenset({DECISION}),
                universe=UNIVERSE,
                close_reason=REASON,
                regime=regime,
            )
        else:
            direct = s10_exit_member(
                series,
                panel_decision_dates=frozenset({DECISION}),
                universe=UNIVERSE,
                close_reason=REASON,
            )
        expected = stage_cross_sectional_member(direct, kind=leg)  # type: ignore[arg-type]
        assert via_runner.verdicts == expected.verdicts
        assert via_runner.scores == expected.scores
        assert via_runner.admissible_dates == expected.admissible_dates
        assert via_runner.mandatory_dates == expected.mandatory_dates

    def test_the_exit_leg_carries_exit_kind_throughout(self) -> None:
        entry = STRATEGY_MANIFEST[S10]
        staged = segmented_member(
            entry,
            _panel()[2],
            panel_decision_dates=frozenset({DECISION}),
            universe=UNIVERSE,
            masked_reason=REASON,
            unresolved_breaks=(),
            regime=_regime(),
            leg="exit",
        )
        kinds = {verdict.kind for verdict in staged.verdicts if verdict is not None}
        assert kinds == {"exit"}
