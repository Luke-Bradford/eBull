"""#3104 slice 9 — the per-leg session-gap observation.

Pure-logic only: the module reads no database, and the acceptance list in
``docs/proposals/ta/2026-09-16-promotion-evidence-exit-gap.md`` is table tests
over the arithmetic plus the accounting invariants.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.strategy_exit_gap import (
    EXCLUSION_PRECEDENCE,
    EXIT_GAP_RULE_VERSION,
    HOLE_DAYS,
    ExitGapMeasurement,
    GapObservation,
    boundary_gaps,
    summarise,
    worse_of,
)

#: Far enough past every fixture date that nothing is provisional unless a test
#: makes it so.
NEVER_PROVISIONAL = date(2100, 1, 1)
START = date(2020, 1, 1)


def _dates(count: int) -> list[date]:
    return [START + timedelta(days=offset) for offset in range(count)]


def _gaps(
    *,
    opens: list[Decimal | float | None],
    raw: list[float],
    wealth: list[float],
    dates: list[date] | None = None,
    offsets: list[int | None] | None = None,
    provisional_from: date = NEVER_PROVISIONAL,
    unresolved_breaks: tuple[date, ...] = (),
) -> tuple[tuple[float, ...], tuple[str | None, ...]]:
    when = dates if dates is not None else _dates(len(opens))
    return boundary_gaps(
        dates=when,
        opens=opens,
        offsets=offsets if offsets is not None else list(range(len(opens))),
        raw_closes=raw,
        wealth_closes=wealth,
        provisional_from=provisional_from,
        unresolved_breaks=unresolved_breaks,
    )


# ---------------------------------------------------------------------------
# Arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("bar_open", "expected"),
    [
        (Decimal("100"), 0.0),
        (Decimal("90"), -10.0),
        (Decimal("110"), 10.0),
    ],
)
def test_flat_carry_is_open_over_prior_close(bar_open: Decimal, expected: float) -> None:
    values, reasons = _gaps(opens=[Decimal("100"), bar_open], raw=[100.0, 100.0], wealth=[100.0, 100.0])
    assert reasons == ("no_session_boundary", None)
    assert values[1] == pytest.approx(expected)


def test_index_zero_ends_no_boundary() -> None:
    _, reasons = _gaps(opens=[Decimal("100")], raw=[100.0], wealth=[100.0])
    assert reasons == ("no_session_boundary",)


def test_a_changing_carry_factor_cancels() -> None:
    """A 2-for-1 split between the bars: raw halves, the wealth series does not.

    ⚠ The decisive case, and a factor-of-ONE fixture cannot reach it. The stored
    open is on the NEW scale while the prior total-return close is on the old
    one; the carry is what makes the boundary read flat rather than -50%.
    """
    values, reasons = _gaps(
        opens=[Decimal("100"), Decimal("50")],
        raw=[100.0, 50.0],
        wealth=[100.0, 100.0],
    )
    assert reasons[1] is None
    assert values[1] == pytest.approx(0.0)


def test_a_dividend_drop_is_not_an_adverse_gap() -> None:
    """Price opens 2% lower, the total-return close carries the distribution."""
    values, _ = _gaps(opens=[Decimal("100"), Decimal("98")], raw=[100.0, 98.0], wealth=[100.0, 100.0])
    assert values[1] == pytest.approx(0.0)


@pytest.mark.parametrize("level", [1e-200, 1e200])
def test_extreme_price_levels_do_not_manufacture_a_gap(level: float) -> None:
    """Regression: the product form returns -100% at 1e-200 and inf at 1e200.

    ``(open * wealth / raw) / prior_wealth`` under/overflows on a FLAT boundary.
    ``(open / raw) * (wealth / prior_wealth)`` keeps both ratios near one.
    """
    values, reasons = _gaps(
        opens=[Decimal(repr(level)), Decimal(repr(level))], raw=[level, level], wealth=[level, level]
    )
    assert reasons[1] is None
    assert values[1] == pytest.approx(0.0)


def test_offsets_are_mapped_independently_of_the_series_index() -> None:
    """A sparse panel: the series' three bars sit at dense offsets 0, 2 and 5.

    Stepping one index in both domains would read the NaN filler between them.
    """
    raw = [100.0, float("nan"), 100.0, float("nan"), float("nan"), 100.0]
    wealth = list(raw)
    values, reasons = _gaps(
        opens=[Decimal("100"), Decimal("95"), Decimal("90")],
        raw=raw,
        wealth=wealth,
        offsets=[0, 2, 5],
    )
    assert reasons[1] is None and reasons[2] is None
    assert values[1] == pytest.approx(-5.0)
    assert values[2] == pytest.approx(-10.0)


# ---------------------------------------------------------------------------
# Exclusions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("offsets", [[0, None], [0, 9], [None, 1]])
def test_an_unmappable_endpoint_is_off_axis(offsets: list[int | None]) -> None:
    _, reasons = _gaps(
        opens=[Decimal("100"), Decimal("90")], raw=[100.0, 100.0], wealth=[100.0, 100.0], offsets=offsets
    )
    assert reasons[1] == "off_axis"


def test_a_long_calendar_hole_is_excluded() -> None:
    when = [START, START + timedelta(days=HOLE_DAYS + 1)]
    _, reasons = _gaps(opens=[Decimal("100"), Decimal("50")], raw=[100.0, 100.0], wealth=[100.0, 100.0], dates=when)
    assert reasons[1] == "session_hole_spanned"


def test_a_hole_exactly_at_the_threshold_is_measured() -> None:
    when = [START, START + timedelta(days=HOLE_DAYS)]
    _, reasons = _gaps(opens=[Decimal("100"), Decimal("50")], raw=[100.0, 100.0], wealth=[100.0, 100.0], dates=when)
    assert reasons[1] is None


def test_a_boundary_spanning_an_unresolved_break_is_excluded() -> None:
    """The case Codex checkpoint 1 reproduced: a signal-pair leg CAN cross one."""
    when = _dates(2)
    _, reasons = _gaps(
        opens=[Decimal("100"), Decimal("50")],
        raw=[100.0, 100.0],
        wealth=[100.0, 100.0],
        dates=when,
        unresolved_breaks=(when[1],),
    )
    assert reasons[1] == "scale_break_spanned"


def test_a_break_on_the_prior_bar_does_not_exclude_the_next_boundary() -> None:
    """``break_date`` is the FIRST date at the new scale, so the boundary AFTER
    it lies wholly inside the new segment."""
    when = _dates(3)
    _, reasons = _gaps(
        opens=[Decimal("100"), Decimal("100"), Decimal("100")],
        raw=[100.0] * 3,
        wealth=[100.0] * 3,
        dates=when,
        unresolved_breaks=(when[1],),
    )
    assert reasons[1] == "scale_break_spanned"
    assert reasons[2] is None


@pytest.mark.parametrize("provisional_index", [0, 1])
def test_a_provisional_bar_on_either_end_withholds(provisional_index: int) -> None:
    when = _dates(2)
    _, reasons = _gaps(
        opens=[Decimal("100"), Decimal("90")],
        raw=[100.0, 100.0],
        wealth=[100.0, 100.0],
        dates=when,
        provisional_from=when[provisional_index],
    )
    assert reasons[1] == "provisional_bar"


@pytest.mark.parametrize("bad", [None, Decimal("0"), Decimal("-1"), Decimal("NaN"), float("inf")])
def test_an_unusable_open_is_excluded(bad: Decimal | float | None) -> None:
    _, reasons = _gaps(opens=[Decimal("100"), bad], raw=[100.0, 100.0], wealth=[100.0, 100.0])
    assert reasons[1] == "open_unusable"


@pytest.mark.parametrize(
    ("raw", "wealth"),
    [
        ([100.0, 100.0], [float("nan"), 100.0]),
        ([100.0, 100.0], [100.0, 0.0]),
        ([100.0, -1.0], [100.0, 100.0]),
    ],
)
def test_an_unusable_close_is_excluded(raw: list[float], wealth: list[float]) -> None:
    _, reasons = _gaps(opens=[Decimal("100"), Decimal("90")], raw=raw, wealth=wealth)
    assert reasons[1] == "close_unusable"


def test_precedence_resolves_a_collision_to_the_earlier_rule() -> None:
    """A long hole AND an unusable open on the same boundary reports the hole."""
    when = [START, START + timedelta(days=HOLE_DAYS + 1)]
    _, reasons = _gaps(opens=[Decimal("100"), None], raw=[100.0, 100.0], wealth=[100.0, 100.0], dates=when)
    assert reasons[1] == "session_hole_spanned"


def test_worse_of_ranks_rather_than_ordering_chronologically() -> None:
    assert worse_of("open_unusable", "off_axis") == "off_axis"
    assert worse_of("off_axis", "open_unusable") == "off_axis"
    assert worse_of(None, "non_finite") == "non_finite"


def test_misaligned_inputs_raise_rather_than_excluding() -> None:
    """Structural corruption is a plumbing bug, not an ordinary exclusion."""
    with pytest.raises(ValueError, match="positionally parallel"):
        boundary_gaps(
            dates=_dates(2),
            opens=[Decimal("100")],
            offsets=[0, 1],
            raw_closes=[100.0, 100.0],
            wealth_closes=[100.0, 100.0],
            provisional_from=NEVER_PROVISIONAL,
        )
    with pytest.raises(ValueError, match="same length"):
        boundary_gaps(
            dates=_dates(2),
            opens=[Decimal("100"), Decimal("100")],
            offsets=[0, 1],
            raw_closes=[100.0, 100.0],
            wealth_closes=[100.0],
            provisional_from=NEVER_PROVISIONAL,
        )


# ---------------------------------------------------------------------------
# Binding observation
# ---------------------------------------------------------------------------


def _observation(value: float, *, name_key: int = 1, bar: date = START) -> GapObservation:
    return GapObservation(
        value=value,
        name_key=name_key,
        fill_date=START,
        exit_date=START + timedelta(days=5),
        prior_bar_date=bar - timedelta(days=1),
        bar_date=bar,
        boundary_calendar_days=1,
        close_source="level",
    )


def test_a_tie_breaks_on_the_declared_key_in_both_directions() -> None:
    low = _observation(-5.0, name_key=2)
    tied = _observation(-5.0, name_key=1)
    assert tied.is_lower_than(low)
    assert not low.is_lower_than(tied)
    assert tied.is_higher_than(low)
    assert not low.is_higher_than(tied)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def _summary(**overrides: object) -> ExitGapMeasurement:
    kwargs: dict[str, object] = {
        "min_per_leg": [-5.0, -1.0],
        "max_per_leg": [2.0, 3.0],
        "realised_leg_count": 2,
        "excluded": {},
        "measured_boundary_count": 6,
        "partial_coverage_leg_count": 0,
        "unmeasurable_boundary_count": 0,
        "unlinked_series_leg_count": 0,
        "open_leg_count": 0,
        "binding_min": _observation(-5.0),
        "binding_max": _observation(3.0),
        "adjustment_basis": "unadjusted",
        "return_basis": "total_return",
    }
    kwargs.update(overrides)
    return summarise(**kwargs)  # type: ignore[arg-type]


def test_the_summary_carries_its_own_identity() -> None:
    measurement = _summary()
    assert measurement.rule_version == EXIT_GAP_RULE_VERSION
    assert measurement.hole_days == HOLE_DAYS
    assert measurement.return_basis == "total_return"
    assert measurement.min_gap_pct == Decimal("-5.0000")
    assert measurement.max_gap_pct == Decimal("3.0000")


def test_an_empty_population_carries_no_summary_and_no_invented_zero() -> None:
    measurement = _summary(
        min_per_leg=[],
        max_per_leg=[],
        realised_leg_count=1,
        excluded={"no_session_boundary": 1},
        measured_boundary_count=0,
        binding_min=None,
        binding_max=None,
    )
    assert measurement.measured_leg_count == 0
    assert measurement.min_gap_pct is None
    assert measurement.max_gap_pct is None
    assert measurement.binding_min is None
    assert measurement.excluded == {"no_session_boundary": 1}


def test_an_all_adverse_population_keeps_a_negative_maximum() -> None:
    """⚠ Named for the ARITHMETIC: "best" would be wrong here."""
    measurement = _summary(
        min_per_leg=[-5.0, -3.0],
        max_per_leg=[-4.0, -1.0],
        binding_max=_observation(-1.0),
    )
    assert measurement.max_gap_pct == Decimal("-1.0000")
    assert measurement.min_gap_pct == Decimal("-5.0000")


def test_rounding_is_outward_from_the_interval() -> None:
    measurement = _summary(
        min_per_leg=[-5.000051],
        max_per_leg=[3.000051],
        realised_leg_count=1,
        measured_boundary_count=1,
        binding_min=_observation(-5.000051),
        binding_max=_observation(3.000051),
    )
    assert measurement.min_gap_pct == Decimal("-5.0001")
    assert measurement.max_gap_pct == Decimal("3.0001")


def test_a_broken_accounting_equality_raises() -> None:
    with pytest.raises(ValueError, match="every leg carries exactly one verdict"):
        _summary(realised_leg_count=5)


def test_a_coverage_count_larger_than_the_measured_population_raises() -> None:
    with pytest.raises(ValueError, match="counts a SUBSET"):
        _summary(unlinked_series_leg_count=3)


def test_more_partial_legs_than_unmeasurable_boundaries_raises() -> None:
    with pytest.raises(ValueError, match="at least one"):
        _summary(partial_coverage_leg_count=2, unmeasurable_boundary_count=1)


def test_fewer_boundaries_than_measured_legs_raises() -> None:
    with pytest.raises(ValueError, match="a measured leg has at least one"):
        _summary(measured_boundary_count=1)


def test_an_unknown_exclusion_reason_raises() -> None:
    with pytest.raises(ValueError, match="unknown exclusion reasons"):
        _summary(realised_leg_count=3, excluded={"invented": 1})


def test_not_instrumented_is_part_of_the_vocabulary() -> None:
    """⚠ Slice 7a's zero-record escape returns ``None`` and takes the counts with
    it; here the state is named and the equality survives."""
    assert EXCLUSION_PRECEDENCE[0] == "not_instrumented"
    measurement = _summary(
        min_per_leg=[],
        max_per_leg=[],
        realised_leg_count=4,
        excluded={"not_instrumented": 4},
        measured_boundary_count=0,
        binding_min=None,
        binding_max=None,
    )
    assert measurement.excluded == {"not_instrumented": 4}
    assert measurement.realised_leg_count == 4
