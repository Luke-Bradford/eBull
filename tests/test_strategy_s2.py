"""S-2 cross-sectional momentum — the catalogue's ranked strategy (#2240).

Spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §4
(S-2), §3.5, §4.0, §9 Q3, §5 criteria 4/8/9/11. Contract:
``strategy_registry.evaluate_cross_sectional``. Design:
``docs/proposals/ta/2026-08-06-cross-sectional-contract-and-s2.md``.

⚠ THE EXPECTED SCORES ARE DERIVED FROM A NAIVE REFERENCE, NOT HAND-WRITTEN.
``_reference_momentum`` indexes the close list directly at ``i-21`` and
``i-252``; the module walks the same series building a tuple. A shared
off-by-one would have to occur in both to pass.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.signal_ledger import resolve_fills
from app.services.strategies.s2_cross_sectional_momentum import (
    DECILE,
    ELIGIBILITY_BARS,
    LOOKBACK_BARS,
    MIN_CLOSE,
    MIN_CROSS_SECTION,
    S2_PARAMS,
    S2_STRATEGY_ID,
    SKIP_BARS,
    momentum_series,
    rebalance_dates,
    s2_identity,
    s2_member,
    s2_select,
    s2_signals,
)
from app.services.strategy_registry import stage_cross_sectional_member
from app.services.technical_analysis import OHLCVRow

UNIVERSE = "survivor_only"
REASON = "quarantined_bar"
COST_MODEL = COST_MODEL_ID

#: Enough bars to clear the 273-bar eligibility and leave a live tail.
BARS = ELIGIBILITY_BARS + 40


def _bars(closes: Sequence[float | None], *, start: date = date(2020, 1, 1)) -> BarSeries:
    """One bar per close, one calendar day apart. ``None`` is a MASKED close, as
    ``load_masked_series`` produces — the field is present and empty."""
    rows: list[OHLCVRow] = [
        {
            "open": None if c is None else Decimal(str(c)),
            "high": None if c is None else Decimal(str(c + 1)),
            "low": None if c is None else Decimal(str(max(c - 1, 0.01))),
            "close": None if c is None else Decimal(str(c)),
            "volume": 1_000,
        }  # type: ignore[typeddict-item]
        for c in closes
    ]
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _reference_momentum(closes: Sequence[float | None], index: int) -> float | None:
    """Naive 12-1 return at ``index``, or None when it cannot be computed."""
    if index + 1 < ELIGIBILITY_BARS:
        return None
    past = closes[index - LOOKBACK_BARS]
    recent = closes[index - SKIP_BARS]
    if past is None or recent is None or past <= 0 or recent <= 0:
        return None
    return recent / past - 1.0


def _ramp(slope: float, *, n: int = BARS, base: float = 100.0) -> list[float | None]:
    """A monotone close path whose 12-1 return is a function of ``slope``."""
    return [base + slope * i for i in range(n)]


class TestSpecConstants:
    """⚠ The literals are written out HERE and nowhere else in this file.

    A reference that imports the constant it validates is a tautology (#2240
    S-4's review lesson). These four numbers come from parent §4 and §9 Q3; this
    class is the ONE bridge between the spec's text and the module's constants,
    and every other test reads the module.
    """

    SPEC_LOOKBACK_BARS = 252
    SPEC_SKIP_BARS = 21
    SPEC_DECILE = 10
    SPEC_ELIGIBILITY_BARS = 273
    SPEC_MIN_CLOSE = 1.0

    def test_module_constants_match_the_spec(self) -> None:
        assert LOOKBACK_BARS == self.SPEC_LOOKBACK_BARS
        assert SKIP_BARS == self.SPEC_SKIP_BARS
        assert DECILE == self.SPEC_DECILE
        assert ELIGIBILITY_BARS == self.SPEC_ELIGIBILITY_BARS
        assert MIN_CLOSE == self.SPEC_MIN_CLOSE

    def test_params_carry_every_constant_into_the_identity(self) -> None:
        assert S2_PARAMS == {
            "lookback_bars": self.SPEC_LOOKBACK_BARS,
            "skip_bars": self.SPEC_SKIP_BARS,
            "decile": self.SPEC_DECILE,
            "eligibility_bars": self.SPEC_ELIGIBILITY_BARS,
            "min_close": self.SPEC_MIN_CLOSE,
            "min_cross_section": self.SPEC_DECILE,
        }

    def test_no_max_hold_is_declared(self) -> None:
        """S-2's hold is "until the next rebalance", a calendar fact. A bar count
        would be an invented parameter — see the module docstring."""
        assert "max_hold_bars" not in S2_PARAMS


class TestMomentumSeries:
    def test_matches_the_naive_reference_at_every_bar(self) -> None:
        closes = _ramp(0.5)
        series = momentum_series(_bars(closes), universe=UNIVERSE)
        assert len(series) == len(closes)
        for index in range(len(closes)):
            expected = _reference_momentum(closes, index)
            actual = series.values[index]
            if expected is None:
                assert actual is None, index
            else:
                assert actual == pytest.approx(expected), index

    def test_the_eligibility_boundary_is_exact(self) -> None:
        """⚠ The parent's window needs 253 bars and its stated eligibility is
        273. Both ship, so the first scored index is 272 and 271 is warm-up —
        a 20-bar narrowing this pins rather than infers."""
        series = momentum_series(_bars(_ramp(0.5)), universe=UNIVERSE)
        assert series.values[ELIGIBILITY_BARS - 2] is None
        assert series.values[ELIGIBILITY_BARS - 1] is not None
        assert ELIGIBILITY_BARS - 2 not in series.not_evaluable_indices

    def test_warmup_is_not_a_data_gap(self) -> None:
        series = momentum_series(_bars(_ramp(0.5)), universe=UNIVERSE)
        assert not [i for i in series.not_evaluable_indices if i < ELIGIBILITY_BARS - 1]

    @pytest.mark.parametrize("offset", [SKIP_BARS, LOOKBACK_BARS])
    def test_a_masked_window_close_is_a_data_gap(self, offset: int) -> None:
        index = ELIGIBILITY_BARS + 5
        closes = _ramp(0.5)
        closes[index - offset] = None
        series = momentum_series(_bars(closes), universe=UNIVERSE)
        assert series.values[index] is None
        assert index in series.not_evaluable_indices

    @pytest.mark.parametrize("bad", [0.0, -3.0])
    @pytest.mark.parametrize("offset", [SKIP_BARS, LOOKBACK_BARS])
    def test_a_non_positive_window_close_is_refused_not_divided_by(self, offset: int, bad: float) -> None:
        """A zero denominator raises; a negative one returns a sign-flipped
        number that ranks like a winner, which is the worse of the two."""
        index = ELIGIBILITY_BARS + 5
        closes = _ramp(0.5)
        closes[index - offset] = bad
        series = momentum_series(_bars(closes), universe=UNIVERSE)
        assert series.values[index] is None
        assert index in series.not_evaluable_indices

    def test_the_decision_bars_own_close_does_not_enter_the_score(self) -> None:
        """S-2 reads t-21 and t-252, never t. The close at t is refused by being
        a DECLARED INPUT (see the member tests), not by the score."""
        index = ELIGIBILITY_BARS + 5
        closes = _ramp(0.5)
        closes[index] = None
        series = momentum_series(_bars(closes), universe=UNIVERSE)
        assert series.values[index] is not None
        assert index not in series.not_evaluable_indices


class TestRebalanceDates:
    def test_only_the_first_bar_of_each_month(self) -> None:
        calendar = [date(2020, 1, 30), date(2020, 1, 31), date(2020, 2, 3), date(2020, 2, 4), date(2020, 3, 2)]
        assert rebalance_dates(calendar) == {date(2020, 2, 3), date(2020, 3, 2)}

    def test_the_first_date_is_never_a_rebalance(self) -> None:
        assert rebalance_dates([date(2020, 1, 30)]) == frozenset()

    def test_a_year_boundary_is_a_month_change(self) -> None:
        assert rebalance_dates([date(2019, 12, 31), date(2020, 1, 2)]) == {date(2020, 1, 2)}

    def test_the_same_month_a_year_apart_is_a_change(self) -> None:
        """A gap year must not read as "same month, no rebalance"."""
        assert rebalance_dates([date(2019, 3, 1), date(2020, 3, 2)]) == {date(2020, 3, 2)}

    def test_unordered_input_gives_the_same_answer(self) -> None:
        calendar = [date(2020, 3, 2), date(2020, 1, 31), date(2020, 2, 3)]
        assert rebalance_dates(calendar) == rebalance_dates(sorted(calendar))


def _member(closes: Sequence[float | None], dates: frozenset[date] | None = None):
    series = _bars(closes)
    rebals = dates if dates is not None else rebalance_dates(series.dates)
    return series, s2_member(series, panel_rebalance_dates=rebals, universe=UNIVERSE, close_reason=REASON)


class TestMemberEligibility:
    def test_decision_bars_are_rebalance_dates_only(self) -> None:
        series, member = _member(_ramp(0.5))
        rebals = rebalance_dates(series.dates)
        for index in member.decision_indices:
            assert series.dates[index] in rebals
        # Every eligible rebalance date is a decision bar on a clean ramp.
        eligible = {
            index for index, when in enumerate(series.dates) if when in rebals and index >= ELIGIBILITY_BARS - 1
        }
        assert eligible <= member.decision_indices

    def test_a_sub_dollar_close_is_not_a_decision_bar(self) -> None:
        """§9 Q3's floor is an ELIGIBILITY rule: the data is present and the rule
        excludes it, so the verdict is not_fired, never not_evaluable."""
        closes = _ramp(0.0, base=0.5)
        series, member = _member(closes)
        assert member.decision_indices == frozenset()
        staged = stage_cross_sectional_member(member)
        live = [v for v in staged.verdicts[ELIGIBILITY_BARS : len(closes) - 1] if v is not None]
        assert live and all(v.verdict == "not_fired" for v in live)

    def test_the_floor_is_inclusive_at_one_dollar(self) -> None:
        _, member = _member(_ramp(0.0, base=MIN_CLOSE))
        assert member.decision_indices != frozenset()

    def test_a_masked_close_at_the_decision_bar_refuses_the_bar(self) -> None:
        """⚠ The close at t is DECLARED, so a quarantined decision bar fails
        closed instead of being ranked on a score that happens to be computable."""
        closes = _ramp(0.5)
        series = _bars(closes)
        rebals = sorted(d for d in rebalance_dates(series.dates))
        target = series.dates.index(rebals[-1])
        assert target < len(closes) - 1
        closes[target] = None
        _, member = _member(closes)
        assert target not in member.decision_indices
        staged = stage_cross_sectional_member(member)
        verdict = staged.verdicts[target]
        assert verdict is not None
        assert (verdict.verdict, verdict.reason) == ("not_evaluable", REASON)

    def test_an_unknown_reason_code_is_rejected(self) -> None:
        series = _bars(_ramp(0.5))
        with pytest.raises(ValueError, match="unknown reason code"):
            s2_member(
                series,
                panel_rebalance_dates=rebalance_dates(series.dates),
                universe=UNIVERSE,
                close_reason="made_up",  # type: ignore[arg-type]
            )


class TestSelection:
    @staticmethod
    def _scores(n: int) -> dict[int, float]:
        return {key: float(key) for key in range(1, n + 1)}

    @pytest.mark.parametrize(("n", "expected"), [(10, 1), (19, 1), (20, 2), (37, 3), (100, 10)])
    def test_the_cut_is_a_floor(self, n: int, expected: int) -> None:
        assert len(s2_select(date(2020, 1, 2), self._scores(n))) == expected

    def test_below_the_cut_nothing_is_selected(self) -> None:
        """The runner refuses a thin panel before reaching here; this is the
        backstop for a direct caller."""
        assert s2_select(date(2020, 1, 2), self._scores(DECILE - 1)) == frozenset()

    def test_the_highest_scores_win(self) -> None:
        assert s2_select(date(2020, 1, 2), self._scores(20)) == {19, 20}

    def test_ties_break_on_the_lower_instrument_id(self) -> None:
        """⚠ Built HIGHEST id first, deliberately, and the revert probe is why.

        Python's ``sorted`` is stable, so a dict built in ascending key order
        selects {1, 2} even with no tie-break in the code at all — the probe that
        deletes ``item[0]`` from the sort key reported NOT CAUGHT against that
        fixture. Same shape as S-1's flat-fixture lesson: a fixture that agrees
        with the defect pins nothing.
        """
        scores = {key: 0.5 for key in range(20, 0, -1)}
        assert s2_select(date(2020, 1, 2), scores) == {1, 2}

    def test_insertion_order_does_not_change_the_answer(self) -> None:
        forward = self._scores(20)
        backward = dict(reversed(list(forward.items())))
        assert s2_select(date(2020, 1, 2), forward) == s2_select(date(2020, 1, 2), backward)


def _panel(count: int, *, n: int = BARS) -> dict[int, BarSeries]:
    """``count`` members whose 12-1 return rises with the key."""
    return {key: _bars(_ramp(0.1 * key, n=n)) for key in range(1, count + 1)}


class TestPanel:
    def test_one_leg_only(self) -> None:
        signals = s2_signals(_panel(12), universe=UNIVERSE, close_reason=REASON)
        assert {s.kind for member in signals.values() for s in member} == {"entry"}

    def test_one_verdict_per_bar_per_member(self) -> None:
        panel = _panel(12)
        signals = s2_signals(panel, universe=UNIVERSE, close_reason=REASON)
        assert signals.keys() == panel.keys()
        for key, series in panel.items():
            assert [s.signal_index for s in signals[key]] == list(range(len(series)))

    def test_the_top_decile_fires_and_nobody_else_does(self) -> None:
        panel = _panel(20)
        signals = s2_signals(panel, universe=UNIVERSE, close_reason=REASON)
        fired = {key: [s.signal_index for s in member if s.verdict == "fired"] for key, member in signals.items()}
        winners = {key for key, indices in fired.items() if indices}
        assert winners == {19, 20}
        assert fired[20] and fired[19] == fired[20]

    def test_a_thin_cross_section_is_refused_not_reported_as_not_fired(self) -> None:
        signals = s2_signals(_panel(MIN_CROSS_SECTION - 1), universe=UNIVERSE, close_reason=REASON)
        reasons = {s.reason for member in signals.values() for s in member if s.verdict == "not_evaluable"}
        assert "thin_cross_section" in reasons
        assert not [s for member in signals.values() for s in member if s.verdict == "fired"]

    def test_the_last_bar_of_every_member_has_no_fill(self) -> None:
        panel = _panel(12)
        signals = s2_signals(panel, universe=UNIVERSE, close_reason=REASON)
        for key, series in panel.items():
            last = signals[key][-1]
            assert (last.verdict, last.reason) == ("not_evaluable", "no_fill_bar")
            assert last.signal_index == len(series) - 1

    def test_members_are_ranked_against_the_same_DATE_not_the_same_index(self) -> None:
        """⚠ The one bug this design exists to prevent. Member B starts a month
        after A, so index i is a different date on each; grouping by index would
        rank A's 2020 bar against B's 2021 one."""
        early = _bars(_ramp(0.1), start=date(2020, 1, 1))
        late = _bars(_ramp(0.9), start=date(2020, 2, 1))
        panel = {1: early, **{key: _bars(_ramp(0.1 * key)) for key in range(2, 12)}, 12: late}
        signals = s2_signals(panel, universe=UNIVERSE, close_reason=REASON)
        fired_dates = {
            key: {panel[key].dates[s.signal_index] for s in member if s.verdict == "fired"}
            for key, member in signals.items()
        }
        # Every fired bar sits on a date that is a rebalance for the whole panel.
        calendar = rebalance_dates({when for series in panel.values() for when in series.dates})
        for dates in fired_dates.values():
            assert dates <= calendar

    def test_fills_resolve_to_the_next_bar_open(self) -> None:
        panel = _panel(20)
        signals = s2_signals(panel, universe=UNIVERSE, close_reason=REASON)
        identity = s2_identity(universe=UNIVERSE, cost_model_id=COST_MODEL)
        rows = resolve_fills(signals[20], series=panel[20], identity=identity, instrument_id=20)
        fired = [row for row in rows if row.verdict == "fired"]
        assert fired
        for row in fired:
            index = panel[20].dates.index(row.signal_bar_date)
            assert row.fill_bar_date == panel[20].dates[index + 1]
            assert row.fill_price == panel[20].rows[index + 1]["open"]


class TestIdentity:
    def test_the_source_hash_is_this_strategys_own_source(self) -> None:
        """Criterion 11 requires identity to cover CODE. A constant hash lets an
        edited rule inherit the prior track record."""
        import hashlib
        from pathlib import Path

        import app.services.strategies.s2_cross_sectional_momentum as module

        expected = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()[:12]
        assert s2_identity(universe=UNIVERSE, cost_model_id=COST_MODEL).source_hash == expected

    def test_a_blank_cost_model_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="cost_model_id"):
            s2_identity(universe=UNIVERSE, cost_model_id="  ")

    def test_the_universe_is_inside_the_version(self) -> None:
        one = s2_identity(universe="survivor_only", cost_model_id=COST_MODEL)
        other = s2_identity(universe="survivorship_free", cost_model_id=COST_MODEL)
        assert one.version != other.version
        assert one.strategy_id == other.strategy_id == S2_STRATEGY_ID

    def test_the_cost_model_is_inside_the_version(self) -> None:
        one = s2_identity(universe=UNIVERSE, cost_model_id=COST_MODEL)
        other = s2_identity(universe=UNIVERSE, cost_model_id="measured-half-spread-v1")
        assert one.version != other.version
