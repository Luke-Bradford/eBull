"""Phase 5e-5a — criterion 9's census and delta, pure tier.

No database. The arithmetic and the refusals are pure functions over what a run
measured, which is the whole reason ``quarantine_sensitivity`` holds no SQL: the
full-population sweep (``scripts/verify_2240_quarantine_sensitivity.py``) is
where the corpus is read, and it is not table-testable.

The loader's two arms ARE exercised here, on hand-built rows rather than a
connection — ``_apply_arm`` is the masking rule and takes a row sequence, so the
one thing that could silently make the sensitivity arm a copy of the shipped one
is checkable without Postgres.
"""

from __future__ import annotations

from dataclasses import fields
from datetime import date
from decimal import Decimal

import pytest

from app.services.price_structure import StructureBar
from app.services.quarantine_sensitivity import (
    SPEC_CRITERION7_METRICS,
    ArmCensus,
    MetricDelta,
    QuarantineCensus,
    compare_metrics,
)
from app.services.research_price_structure_store import (
    QUARANTINE_RULE_SET_VERSION,
    MaskedSeries,
    _apply_arm,
    load_arms,
)
from app.services.strategy_statistics import StrategyMetrics

#: ⚠ TRANSCRIBED from parent criterion 7's own sentence, not imported from the
#: module under test — the #2240 S-3 lesson that a reference importing the
#: constant it validates is a tautology. This list and
#: ``SPEC_CRITERION7_METRICS`` are two independent copies of the criterion, and
#: the bridge test below is what ties them to ``StrategyMetrics``.
SPEC_METRIC_NAMES = {
    "expectancy_per_trade_pct",
    "profit_factor",
    "cagr_pct",
    "annualised_volatility_pct",
    "sharpe",
    "sortino",
    "max_drawdown_pct",
    "exposure_time_pct",
    "turnover_annualised",
    "trade_count",
    "effective_sample_size",
    "return_vs_buy_and_hold_pct",
}


def _row(
    day: int,
    *,
    range_usable: bool = True,
    return_usable: bool = True,
    close: Decimal | None = Decimal("10"),
    adj_close: Decimal | None = Decimal("10"),
    open_: Decimal | None = Decimal("9"),
) -> tuple[object, ...]:
    """One ``_LOAD_SQL`` row, in the order the loader unpacks it."""
    return (
        date(2024, 1, day),
        open_,
        Decimal("11"),
        Decimal("8"),
        close,
        adj_close,
        1000,
        range_usable,
        return_usable,
    )


def _one_bar() -> tuple[StructureBar, ...]:
    """One bar, so a flagged-bar count of 1 is expressible at all."""
    return (
        StructureBar(
            bar_date=date(2024, 1, 2),
            open=Decimal("9"),
            high=Decimal("11"),
            low=Decimal("8"),
            close=Decimal("10"),
            volume=1000,
        ),
    )


def test_both_quarantine_arms_apply_the_outcome_boundary_in_the_database_read() -> None:
    boundary = date(2021, 6, 28)

    class _Rows:
        def fetchall(self) -> list[object]:
            return []

    class _Connection:
        params: object = None
        query = ""

        def execute(self, query: str, params: object) -> _Rows:
            self.query = query
            self.params = params
            return _Rows()

    conn = _Connection()
    arms = load_arms(conn, 17, through_date=boundary)  # type: ignore[arg-type]

    assert set(arms) == {"masked", "admitted"}
    assert "d.bar_date <= %(through_date)s::date" in conn.query
    assert conn.params == {
        "series_id": 17,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
        "through_date": boundary,
    }


def _metrics(**overrides: object) -> StrategyMetrics:
    base: dict[str, object] = {
        "expectancy_per_trade_pct": 0.5,
        "profit_factor": 1.2,
        "cagr_pct": 4.0,
        "annualised_volatility_pct": 12.0,
        "sharpe": 0.33,
        "sortino": 0.41,
        "max_drawdown_pct": -18.0,
        "exposure_time_pct": 55.0,
        "turnover_annualised": 3.5,
        "trade_count": 100,
        # ⚠ None, and the whole criterion-3 block with it: ``StrategyMetrics``
        # refuses a PARTIAL bootstrap set, so the honest fixture is all-absent.
        "effective_sample_size": None,
        "return_vs_buy_and_hold_pct": -2.0,
        "losing_trade_count": 40,
        "losing_period_count": 300,
        "open_trade_count": 3,
        "unpriced_trade_count": 1,
        "periods_per_year": 196.0,
        "total_return_pct": 40.0,
        "buy_and_hold_return_pct": 42.0,
        # #2623 gap 1. ⚠ NOT one of criterion 7's twelve, so `compare_metrics`
        # deliberately does not compare it — present here only because
        # `StrategyMetrics` requires it alongside a non-zero trade_count.
        "hold_days_p25": 3.0,
        "median_hold_days": 8.0,
        "hold_days_p75": 21.0,
    }
    base.update(overrides)
    return StrategyMetrics(**base)  # type: ignore[arg-type]


def _census(arm: str, **overrides: object) -> ArmCensus:
    base: dict[str, object] = {
        "arm": arm,
        "series_evaluated": 5266,
        "series_fail_closed": 0,
        "bars": 23_339_583,
        "bars_flagged": 643,
        "range_flagged": 643,
        "return_flagged": 65,
        "not_evaluable": {"quarantined_bar": 12, "insufficient_warmup": 900},
        "trades": 1000,
    }
    base.update(overrides)
    return ArmCensus(**base)  # type: ignore[arg-type]


class TestTheTwoArms:
    """The masking rule itself — the one thing that makes the arm an arm."""

    def test_the_masked_arm_drops_the_flagged_fields(self) -> None:
        loaded = _apply_arm(1, [_row(2, range_usable=False, return_usable=False)], arm="masked")
        bar = loaded.bars[0]
        assert (bar.high, bar.low, bar.close) == (None, None, None)
        # ⚠ A POSITIVE open survives both verdicts — no axis covers it, and #2354
        # masks it on its VALUE, not on the bar being flagged. A both-false bar
        # is exactly where the two rules are easiest to conflate.
        assert bar.open == Decimal("9")

    def test_the_admitted_arm_keeps_them_at_their_stored_values(self) -> None:
        """C9's own definition: *"admitted at their stored values"*."""
        loaded = _apply_arm(1, [_row(2, range_usable=False, return_usable=False)], arm="admitted")
        bar = loaded.bars[0]
        assert (bar.high, bar.low, bar.close) == (Decimal("11"), Decimal("8"), Decimal("10"))

    def test_the_wealth_close_follows_the_return_mask_without_replacing_raw_ohlc(self) -> None:
        masked = _apply_arm(
            1,
            [_row(2, close=Decimal("10"), adj_close=Decimal("12"), return_usable=False)],
            arm="masked",
        )
        admitted = _apply_arm(
            1,
            [_row(2, close=Decimal("10"), adj_close=Decimal("12"), return_usable=False)],
            arm="admitted",
        )
        assert masked.bars[0].close is None
        assert masked.wealth_closes == (None,)
        assert admitted.bars[0].close == Decimal("10")
        assert admitted.wealth_closes == (Decimal("12"),)


class TestTheOpenIsMaskedOnItsValue:
    """#2354. The open is the one OHLC field the two axes do not cover, and the
    loader carried it through untouched — so a stored ``open = 0`` reached
    ``signal_ledger.resolve_fills`` and became ``fill_price = 0`` on a fired
    row. The rule applied is ``price_quarantine.rule_b1``'s own clause, *"any of
    open/high/low/close NULL or <= 0"*.
    """

    def test_a_zero_open_is_masked_even_though_no_verdict_names_it(self) -> None:
        loaded = _apply_arm(1, [_row(2, open_=Decimal("0"))], arm="masked")
        assert loaded.bars[0].open is None

    def test_a_null_open_stays_none_and_is_not_compared_against_zero(self) -> None:
        """⚠ The None half of ``open_ is not None and open_ > 0``. Both columns
        are nullable, and dropping the None guard does not fail a comparison
        quietly — it raises ``TypeError`` deep inside a corpus sweep. Neither
        corpus stores a NULL open today, which is exactly why the guard needs a
        test rather than a measurement."""
        loaded = _apply_arm(1, [_row(2, open_=None)], arm="masked")
        assert loaded.bars[0].open is None

    def test_a_negative_open_is_masked(self) -> None:
        """⚠ No negative open is stored in either corpus today. That is a fact
        about an ingest run, not a property of the column, so the bound is
        ``<= 0`` and this pins the half the corpus cannot currently exercise."""
        loaded = _apply_arm(1, [_row(2, open_=Decimal("-1"))], arm="masked")
        assert loaded.bars[0].open is None

    def test_the_admitted_arm_still_admits_the_stored_open(self) -> None:
        """⚠ The arm is C9's *"stored values rather than masked"* and an
        exception for the open would make it a different arm from the one the
        criterion names. Safe because ``resolve_fills`` refuses a non-positive
        open independently, so the admitted arm reports rather than crashes."""
        loaded = _apply_arm(1, [_row(2, open_=Decimal("0"))], arm="admitted")
        assert loaded.bars[0].open == Decimal("0")

    def test_masking_the_open_does_not_touch_the_criterion_9_counts(self) -> None:
        """⚠ Every non-positive open in both corpora is `B1`, i.e. already
        flagged on BOTH axes (measured, full population). The census counts bars
        the QUARANTINE flagged; inventing a third axis here would double-count
        the same bars in a figure criterion 9 reads as a share."""
        loaded = _apply_arm(1, [_row(2, open_=Decimal("0"), range_usable=False, return_usable=False)], arm="masked")
        assert (loaded.range_flagged, loaded.return_flagged, loaded.bars_flagged) == (1, 1, 1)

    def test_the_arms_agree_on_what_was_flagged_and_differ_on_what_was_masked(self) -> None:
        """⚠⚠ The distinction the census depends on. If ``*_flagged`` moved with
        the arm, the sensitivity arm would report its own exclusion as empty —
        an arm measuring the cost of masking, printing "nothing was masked"."""
        rows = [_row(1), _row(2, range_usable=False), _row(3, return_usable=False)]
        masked = _apply_arm(1, rows, arm="masked")
        admitted = _apply_arm(1, rows, arm="admitted")
        assert (masked.range_flagged, masked.return_flagged) == (1, 1)
        assert (admitted.range_flagged, admitted.return_flagged) == (1, 1)
        assert (masked.range_masked, masked.return_masked) == (1, 1)
        assert (admitted.range_masked, admitted.return_masked) == (0, 0)

    def test_flagged_bars_are_counted_once_not_summed_across_verdicts(self) -> None:
        """⚠ The two verdicts OVERLAP on this corpus — every return-flagged bar
        is also range-flagged. ``range_flagged + return_flagged`` would
        double-count, and criterion 9 asks for a SHARE of bars."""
        loaded = _apply_arm(1, [_row(1, range_usable=False, return_usable=False), _row(2)], arm="masked")
        assert (loaded.range_flagged, loaded.return_flagged) == (1, 1)
        assert loaded.bars_flagged == 1

    def test_an_unknown_arm_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown quarantine arm"):
            _apply_arm(1, [_row(1)], arm="conservative")  # type: ignore[arg-type]

    def test_a_series_carrying_more_masked_than_flagged_is_refused(self) -> None:
        """The invariant a future wiring slip would break, asserted on the model
        rather than trusted to the loader that currently satisfies it."""
        with pytest.raises(ValueError, match="exceeds flagged"):
            MaskedSeries(
                series_id=1,
                bars=_one_bar(),
                wealth_closes=(Decimal("10"),),
                range_masked=3,
                return_masked=0,
                range_flagged=1,
                return_flagged=0,
                bars_flagged=1,
                arm="masked",
            )

    def test_wealth_closes_must_align_with_raw_bars(self) -> None:
        with pytest.raises(ValueError, match="one-for-one"):
            MaskedSeries(
                series_id=1,
                bars=_one_bar(),
                wealth_closes=(),
                range_masked=0,
                return_masked=0,
                range_flagged=0,
                return_flagged=0,
                bars_flagged=0,
                arm="masked",
            )

    def test_the_admitted_arm_may_not_claim_to_have_masked_anything(self) -> None:
        with pytest.raises(ValueError, match="the admitted arm masks none"):
            MaskedSeries(
                series_id=1,
                bars=_one_bar(),
                wealth_closes=(Decimal("10"),),
                range_masked=1,
                return_masked=0,
                range_flagged=1,
                return_flagged=0,
                bars_flagged=1,
                arm="admitted",
            )


class TestTheMetricDelta:
    def test_every_criterion_7_metric_is_compared(self) -> None:
        deltas = compare_metrics(_metrics(), _metrics())
        assert len(deltas) == 12
        assert {delta.metric for delta in deltas} == SPEC_METRIC_NAMES

    def test_the_transcribed_list_and_the_metric_set_agree(self) -> None:
        """⚠⚠ THE BRIDGE. Two independent transcriptions of criterion 7 and the
        dataclass they describe. A metric renamed on ``StrategyMetrics`` without
        the criterion being revisited fails HERE, where the module's own
        reflection would simply agree with itself."""
        assert set(SPEC_CRITERION7_METRICS) == SPEC_METRIC_NAMES
        field_names = {f.name for f in fields(StrategyMetrics)}
        assert SPEC_METRIC_NAMES <= field_names

    def test_a_delta_is_the_admitted_value_minus_the_masked_one(self) -> None:
        deltas = {d.metric: d for d in compare_metrics(_metrics(sharpe=0.30), _metrics(sharpe=0.45))}
        sharpe = deltas["sharpe"]
        assert sharpe.state == "measured"
        assert sharpe.delta == pytest.approx(0.15)
        assert sharpe.relative_pct == pytest.approx(50.0)

    def test_a_metric_null_in_one_arm_has_no_delta_and_is_not_zero(self) -> None:
        """⚠⚠ The interesting case, and the one a naive subtraction destroys: the
        admitted arm gained a losing trade, so ``profit_factor`` became
        computable. A ``0.0`` here would read as "unchanged"."""
        masked = _metrics(profit_factor=None, losing_trade_count=0)
        admitted = _metrics(profit_factor=1.4, losing_trade_count=7)
        delta = {d.metric: d for d in compare_metrics(masked, admitted)}["profit_factor"]
        assert delta.state == "masked_null"
        assert delta.delta is None
        assert delta.relative_pct is None

    def test_both_arms_null_is_its_own_state(self) -> None:
        masked = _metrics(sortino=None, losing_period_count=0)
        admitted = _metrics(sortino=None, losing_period_count=0)
        delta = {d.metric: d for d in compare_metrics(masked, admitted)}["sortino"]
        assert delta.state == "both_null"
        assert delta.delta is None

    def test_a_relative_change_on_a_zero_base_is_none_not_an_infinity(self) -> None:
        deltas = {d.metric: d for d in compare_metrics(_metrics(sharpe=0.0), _metrics(sharpe=0.2))}
        assert deltas["sharpe"].delta == pytest.approx(0.2)
        assert deltas["sharpe"].relative_pct is None

    def test_a_delta_that_disagrees_with_its_state_is_refused(self) -> None:
        with pytest.raises(ValueError, match="is present exactly when both arms are"):
            MetricDelta(metric="sharpe", masked=None, admitted=0.2, delta=0.2, state="masked_null")

    def test_a_metric_outside_criterion_7_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not one of criterion 7's twelve"):
            MetricDelta(metric="periods_per_year", masked=1.0, admitted=1.0, delta=0.0, state="measured")


class TestTheCensus:
    def test_the_share_is_computed_not_transcribed(self) -> None:
        census = _census("masked", bars=1000, bars_flagged=25, range_flagged=25, return_flagged=4)
        assert census.flagged_bar_share_pct == pytest.approx(2.5)

    def test_an_empty_read_has_no_share_rather_than_a_zero_one(self) -> None:
        census = _census("masked", bars=0, bars_flagged=0, range_flagged=0, return_flagged=0)
        assert census.flagged_bar_share_pct is None

    def test_arms_that_read_different_populations_are_refused(self) -> None:
        """⚠⚠ THE CONTROLLED-EXPERIMENT CHECK. Both arms come off one fetch, so a
        disagreement means the delta compares populations rather than handling —
        and every metric difference below it would be attributed to the wrong
        cause."""
        with pytest.raises(ValueError, match="the arms disagree on bars"):
            QuarantineCensus(
                strategy="S-1",
                masked=_census("masked", bars=1000),
                admitted=_census("admitted", bars=999),
            )

    def test_mislabelled_arms_are_refused(self) -> None:
        with pytest.raises(ValueError, match="mislabelled"):
            QuarantineCensus(strategy="S-1", masked=_census("admitted"), admitted=_census("admitted"))

    def test_the_trade_delta_is_signed(self) -> None:
        """⚠ Admitting a close can LOSE trades — an earlier exit frees the
        instrument for another entry, a later one blocks one. Nothing asserts a
        direction, so the model must be able to carry a negative one."""
        pair = QuarantineCensus(
            strategy="S-1",
            masked=_census("masked", trades=1000),
            admitted=_census("admitted", trades=940),
        )
        assert pair.trade_delta == -60
        assert pair.trade_delta_share_pct == pytest.approx(-6.0)

    def test_a_zero_trade_arm_has_no_delta_share(self) -> None:
        pair = QuarantineCensus(
            strategy="S-1",
            masked=_census("masked", trades=0),
            admitted=_census("admitted", trades=4),
        )
        assert pair.trade_delta == 4
        assert pair.trade_delta_share_pct is None

    def test_an_unknown_reason_code_is_refused(self) -> None:
        """Criterion 8's vocabulary is closed, and a census that admitted a
        free-text reason could not be counted against it."""
        with pytest.raises(ValueError, match="unknown not_evaluable reason codes"):
            _census("masked", not_evaluable={"bad_data": 1})

    def test_a_field_count_above_the_flagged_bar_count_is_refused(self) -> None:
        with pytest.raises(ValueError, match="exceeds the flagged bar count"):
            _census("masked", bars_flagged=1, range_flagged=2, return_flagged=0)
