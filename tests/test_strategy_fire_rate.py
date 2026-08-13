"""#2623 gap 2 — the entry fire rate derived from the durable scan census.

Spec: ``docs/proposals/ta/2026-08-14-strategy-fire-rate.md``.

The derivation is pure, so every row of the spec's §3 state table is a unit case.

⚠ The loader's DB test lives in ``test_strategy_fire_rate_db.py`` and not here on
purpose: ``tests/conftest.py::_module_source_touches_db`` marks a whole MODULE
``db`` when its source touches psycopg, so one DB test in this file would drop
every case below off the fast push gate.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.strategy_monitoring import StrategyFireRate, derive_fire_rate


def _derive(**overrides: object) -> StrategyFireRate:
    kwargs: dict[str, object] = {
        "scanned_days": 5,
        "fired_days": 3,
        "fired_entry_signals": 100,
        "evaluable_entry_decisions": 1_000,
        "not_evaluable_entry_decisions": 0,
        "first_scanned_bar": date(2026, 7, 28),
        "last_scanned_bar": date(2026, 8, 4),
    }
    kwargs.update(overrides)
    return derive_fire_rate(**kwargs)  # type: ignore[arg-type]


class TestUnavailableReason:
    """§3's table: `None` would otherwise mean three different things."""

    def test_never_scanned_reports_the_reason_not_a_zero_rate(self) -> None:
        rate = _derive(scanned_days=0, fired_days=0, fired_entry_signals=0, evaluable_entry_decisions=0)
        assert rate.rate_unavailable_reason == "never_scanned"
        assert rate.fired_share_of_evaluable is None
        assert rate.entries_per_calendar_week is None

    def test_default_state_is_never_scanned(self) -> None:
        # The API substitutes this for a key absent from the census, so the
        # default must be the honest "no evidence" state, not an implied zero.
        assert StrategyFireRate().rate_unavailable_reason == "never_scanned"
        assert StrategyFireRate().fired_share_of_evaluable is None

    def test_single_scan_day_gives_a_share_but_refuses_a_weekly_rate(self) -> None:
        # This is every current strategy version's state as at 2026-08-14.
        rate = _derive(
            scanned_days=1,
            fired_days=1,
            fired_entry_signals=1_740,
            evaluable_entry_decisions=3_340,
            first_scanned_bar=date(2026, 8, 10),
            last_scanned_bar=date(2026, 8, 10),
        )
        assert rate.rate_unavailable_reason == "single_scan_day"
        assert rate.entries_per_calendar_week is None
        assert rate.fired_share_of_evaluable == Decimal("0.5210")

    def test_a_measurable_axis_carries_no_reason(self) -> None:
        assert _derive().rate_unavailable_reason is None


class TestScannedButNeverFired:
    """The distinction the whole ticket turns on. s2 is this row of the table."""

    def test_zero_fires_over_a_real_axis_is_a_measurement_not_an_absence(self) -> None:
        rate = _derive(fired_days=0, fired_entry_signals=0, evaluable_entry_decisions=3_273)
        assert rate.fired_share_of_evaluable == Decimal("0.0000")
        assert rate.entries_per_calendar_week == Decimal("0.00")
        assert rate.rate_unavailable_reason is None

    def test_no_evaluable_decisions_leaves_the_share_null(self) -> None:
        # A day on which every bar was `not_evaluable`: the strategy was never
        # offered a decision, so its propensity is unknown rather than zero.
        rate = _derive(
            fired_days=0,
            fired_entry_signals=0,
            evaluable_entry_decisions=0,
            not_evaluable_entry_decisions=2_438,
        )
        assert rate.fired_share_of_evaluable is None
        assert rate.not_evaluable_entry_decisions == 2_438


class TestWeeklyRateIsMeasuredOffTheAxis:
    """Not `fires per scanned day x 5` — see `strategy_statistics`' settled rule."""

    def test_rate_divides_by_the_calendar_span_not_the_scanned_day_count(self) -> None:
        # 100 fires over 5 scanned days spanning 7 calendar days is 100/week.
        # A `fires per scanned day x 5` construction would say 100/5*5 = 100 too,
        # so the discriminating case is a span that is NOT a round week.
        assert _derive().entries_per_calendar_week == Decimal("100.00")

    def test_a_sparse_axis_is_not_treated_as_contiguous(self) -> None:
        # 5 scanned days over a 14-day span: the scan missed days, and the
        # throughput the operator actually saw is halved accordingly.
        rate = _derive(first_scanned_bar=date(2026, 7, 21), last_scanned_bar=date(2026, 8, 4))
        assert rate.entries_per_calendar_week == Decimal("50.00")

    def test_two_day_axis_is_the_shortest_that_carries_a_rate(self) -> None:
        rate = _derive(
            scanned_days=2,
            fired_entry_signals=2,
            first_scanned_bar=date(2026, 8, 3),
            last_scanned_bar=date(2026, 8, 4),
        )
        assert rate.rate_unavailable_reason is None
        assert rate.entries_per_calendar_week == Decimal("14.00")

    def test_repeated_scans_of_one_date_still_have_no_span(self) -> None:
        # `scanned_days` counts DISTINCT bar dates, so this shape cannot arise
        # from the query — but a zero span must refuse regardless of the count,
        # or the guard depends on two facts agreeing rather than on the axis.
        rate = _derive(
            scanned_days=4,
            first_scanned_bar=date(2026, 8, 4),
            last_scanned_bar=date(2026, 8, 4),
        )
        assert rate.rate_unavailable_reason == "single_scan_day"
        assert rate.entries_per_calendar_week is None


class TestReportedCounts:
    def test_raw_counts_pass_through_for_the_catalog_to_show_provenance(self) -> None:
        rate = _derive(not_evaluable_entry_decisions=2_450)
        assert (rate.scanned_days, rate.fired_days) == (5, 3)
        assert rate.fired_entry_signals == 100
        assert rate.evaluable_entry_decisions == 1_000
        assert rate.not_evaluable_entry_decisions == 2_450
        assert rate.first_scanned_bar == date(2026, 7, 28)
        assert rate.last_scanned_bar == date(2026, 8, 4)

    def test_universe_is_labelled_per_2288(self) -> None:
        assert _derive().universe == "survivor_only"
