"""#3841 pure rules: anchors, the window factor, selection and steps, the ported checks, Gates 1 and 2.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` § Acceptance "Tests". Each test states its
expected outcome in its name.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services import market_calendar
from app.services.factor_panel import Check
from app.services.pit_market_cap import (
    Anchor,
    AnchorStatus,
    Concept,
    FloatFact,
    GateOutcome,
    Refusal,
    ShareFact,
    Trade,
    apply_gate1,
    apply_gate2,
    build_steps,
    classify_anchor,
    eligible,
    gate1,
    gate2,
    last_session_on_or_before,
    quarter_ends,
    rule_version,
    select,
    window_factor,
)

AS_OF = date(2026, 10, 16)
Q = quarter_ends(date(2024, 6, 30), AS_OF)  # 2024-06-30 .. 2026-09-30


def sessions(first: date, last: date) -> tuple[date, ...]:
    days = (first + timedelta(days=i) for i in range((last - first).days + 1))
    return tuple(d for d in days if market_calendar.us_market_status(d) != "closed")


ALL_SESSIONS = sessions(date(2024, 6, 1), AS_OF)


def anchor(q: date, f_hat: float = 1.0, status: AnchorStatus = AnchorStatus.OK, provisional: bool = False) -> Anchor:
    session = last_session_on_or_before(ALL_SESSIONS, q)
    assert session is not None
    return Anchor(q, session, status, 25, 1.0, f_hat, max(f_hat, 1 / f_hat), provisional)


def series(*phis: float | AnchorStatus) -> tuple[Anchor, ...]:
    """One anchor per quarter in Q, from the start: a float is an ok F-hat, a status a failed quarter."""
    assert len(phis) == len(Q)
    return tuple(
        anchor(q, 1.0, p) if isinstance(p, AnchorStatus) else anchor(q, p) for q, p in zip(Q, phis, strict=True)
    )


FLAT = series(*([1.0] * len(Q)))


def at(day: date, hour: int = 21) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def cover(context: date, accepted: datetime, value: int, accession: str = "a1", form: str = "10-Q") -> ShareFact:
    return ShareFact(Concept.COVER, accession, form, context, accepted, Decimal(value))


def sheet(context: date, accepted: datetime, value: int, accession: str = "a1", form: str = "10-Q") -> ShareFact:
    return ShareFact(Concept.BALANCE_SHEET, accession, form, context, accepted, Decimal(value))


# --------------------------------------------------------------------------- anchors


def test_rule_version_is_a_stable_sha256() -> None:
    assert rule_version() == rule_version()
    assert len(rule_version()) == 64


@pytest.mark.parametrize(
    ("median", "close", "status", "phi"),
    [
        (110.0, 100.0, AnchorStatus.OK, 1.0),  # m = 1.10 exactly: snapped
        (100.0, 110.0, AnchorStatus.OK, 1.0),  # the reciprocal edge snaps too
        (111.0, 100.0, AnchorStatus.AMBIGUOUS, None),
        (124.0, 100.0, AnchorStatus.AMBIGUOUS, None),
        (125.0, 100.0, AnchorStatus.OK, 1.25),  # m = 1.25 exactly: split-sized
        (100.0, 125.0, AnchorStatus.OK, 0.8),  # reverse direction
    ],
)
def test_band_boundaries_and_reciprocals(median: float, close: float, status: AnchorStatus, phi: float | None) -> None:
    result = classify_anchor(Q[0], Q[0], [median] * 20, close, as_of=AS_OF)
    assert result.status is status
    assert result.phi == (None if phi is None else pytest.approx(phi))


def test_thin_below_twenty_filers() -> None:
    assert classify_anchor(Q[0], Q[0], [100.0] * 19, 100.0, as_of=AS_OF).status is AnchorStatus.THIN


def test_no_close_wins_over_thin() -> None:
    for close in (None, 0.0, -1.0, float("nan"), float("inf")):
        assert classify_anchor(Q[0], Q[0], [100.0] * 3, close, as_of=AS_OF).status is AnchorStatus.NO_CLOSE


def test_coherent_mixed_population_is_incoherent() -> None:
    # 11 filers at 100 and 9 at 200: the median is 100 and only 55% lie within 2% of it.
    result = classify_anchor(Q[0], Q[0], [100.0] * 11 + [200.0] * 9, 100.0, as_of=AS_OF)
    assert result.status is AnchorStatus.INCOHERENT
    assert result.coherence == pytest.approx(0.55)


def test_coherence_threshold_is_inclusive_at_75_percent() -> None:
    result = classify_anchor(Q[0], Q[0], [100.0] * 15 + [200.0] * 5, 100.0, as_of=AS_OF)
    assert result.status is AnchorStatus.OK


def test_provisional_until_the_13f_due_date_passes() -> None:
    q = date(2026, 9, 30)
    assert classify_anchor(q, q, [1.0] * 20, 1.0, as_of=date(2026, 11, 14)).provisional
    assert not classify_anchor(q, q, [1.0] * 20, 1.0, as_of=date(2026, 11, 15)).provisional


def test_non_finite_unit_value_raises() -> None:
    with pytest.raises(ValueError):
        classify_anchor(Q[0], Q[0], [100.0] * 19 + [float("nan")], 100.0, as_of=AS_OF)


# --------------------------------------------------------------------------- the window factor


def test_bracketed_split_window_publishes_the_measured_factor() -> None:
    # NFLX-like: F-hat 10 until a split, ~1 after. A count dated and filed inside the 10x era carries F = 10.
    anchors = series(*([10.0] * 6 + [1.0] * (len(Q) - 6)))
    basis = window_factor(date(2024, 10, 20), date(2024, 10, 25), anchors)
    assert basis.refusal is None and basis.bracketed
    assert basis.factor == 10.0
    assert [a.quarter for a in basis.window] == [date(2024, 9, 30), date(2024, 12, 31)]


def test_reverse_split_factor() -> None:
    anchors = series(*([1 / 3] * 7 + [1.0] * (len(Q) - 7)))
    basis = window_factor(date(2024, 10, 20), date(2024, 10, 25), anchors)
    assert basis.refusal is None
    assert basis.factor == pytest.approx(1 / 3)


def test_no_left_anchor_before_the_first_quarter() -> None:
    basis = window_factor(date(2024, 5, 1), date(2024, 5, 10), FLAT)
    assert basis.refusal is Refusal.NO_LEFT_ANCHOR


def test_left_edge_not_ok_is_no_left_anchor() -> None:
    anchors = series(AnchorStatus.THIN, *([1.0] * (len(Q) - 1)))
    assert window_factor(date(2024, 7, 10), date(2024, 8, 5), anchors).refusal is Refusal.NO_LEFT_ANCHOR


@pytest.mark.parametrize("status", [AnchorStatus.THIN, AnchorStatus.INCOHERENT, AnchorStatus.AMBIGUOUS])
def test_a_failed_quarter_inside_the_window_refuses_never_bridged(status: AnchorStatus) -> None:
    # Count dated 2024-08 and filed 2025-02: W = 2024-06-30 .. 2025-03-31; the 2024-12-31 quarter fails.
    phis: list[float | AnchorStatus] = [1.0] * len(Q)
    phis[2] = status
    basis = window_factor(date(2024, 8, 1), date(2025, 2, 10), series(*phis))
    assert basis.refusal is Refusal.BASIS_UNRESOLVED


def test_interior_anchor_that_disagrees_refuses() -> None:
    phis: list[float | AnchorStatus] = [1.0] * len(Q)
    phis[2] = 2.0  # a split-sized interior quarter between q_a and the right edge
    assert window_factor(date(2024, 8, 1), date(2025, 2, 10), series(*phis)).refusal is Refusal.BASIS_UNRESOLVED


def test_agreement_is_max_over_min() -> None:
    within = series(*([10.0, 10.5] + [10.0] * (len(Q) - 2)))  # 1.05 exactly: agrees
    assert window_factor(date(2024, 7, 1), date(2024, 10, 1), within).factor == 10.0
    beyond = series(*([10.0, 10.6] + [10.0] * (len(Q) - 2)))
    assert window_factor(date(2024, 7, 1), date(2024, 10, 1), beyond).refusal is Refusal.BASIS_UNRESOLVED


def test_trailing_window_with_phi_one_publishes() -> None:
    basis = window_factor(date(2026, 9, 30), date(2026, 10, 15), FLAT)
    assert basis.refusal is None and not basis.bracketed and basis.factor == 1.0


def test_trailing_window_with_phi_not_one_refuses() -> None:
    anchors = series(*([1.0] * (len(Q) - 1) + [2.0]))
    assert window_factor(date(2026, 9, 30), date(2026, 10, 15), anchors).refusal is Refusal.BASIS_UNRESOLVED


def test_trailing_window_whose_latest_quarter_is_stale_refuses() -> None:
    # Observation stopped two quarters ago (the last two quarters are provisional and thin): W ends too early.
    phis: list[float | AnchorStatus] = [1.0] * len(Q)
    anchors = list(series(*phis))
    anchors[-1] = anchor(Q[-1], 1.0, AnchorStatus.THIN, provisional=True)
    anchors[-2] = anchor(Q[-2], 1.0, AnchorStatus.THIN, provisional=True)
    assert window_factor(date(2026, 4, 1), date(2026, 10, 15), anchors).refusal is Refusal.BASIS_UNRESOLVED


def test_provisional_thin_latest_quarter_is_not_yet_observed() -> None:
    # 13F filings for 2026-09-30 are still arriving: the previous quarter still bounds a current count.
    anchors = list(FLAT)
    anchors[-1] = anchor(Q[-1], 1.0, AnchorStatus.THIN, provisional=True)
    basis = window_factor(date(2026, 7, 25), date(2026, 8, 5), anchors)
    assert basis.refusal is None and not basis.bracketed
    assert basis.window[-1].quarter == date(2026, 6, 30)


@pytest.mark.parametrize("status", [AnchorStatus.NO_CLOSE, AnchorStatus.INCOHERENT, AnchorStatus.AMBIGUOUS])
def test_provisional_observed_failure_is_not_skipped(status: AnchorStatus) -> None:
    # Only a provisional THIN quarter is "not yet observed"; any other failure is a discrepancy and refuses.
    anchors = list(FLAT)
    anchors[-1] = anchor(Q[-1], 1.0, status, provisional=True)
    assert window_factor(date(2026, 7, 25), date(2026, 8, 5), anchors).refusal is Refusal.BASIS_UNRESOLVED


def test_settled_thin_latest_quarter_refuses() -> None:
    anchors = list(FLAT)
    anchors[-1] = anchor(Q[-1], 1.0, AnchorStatus.THIN, provisional=False)
    assert window_factor(date(2026, 7, 25), date(2026, 10, 5), anchors).refusal is Refusal.BASIS_UNRESOLVED


def test_offsetting_split_pair_publishes_the_wrong_factor_stated_limit() -> None:
    # A 2-for-1 then a 1-for-2 between two anchors nets to 1: the anchors cannot see it, so F = 1 publishes.
    basis = window_factor(date(2024, 7, 10), date(2024, 8, 5), FLAT)
    assert basis.refusal is None and basis.factor == 1.0


# --------------------------------------------------------------------------- selection


def test_latest_context_then_latest_acceptance() -> None:
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1"),
        cover(date(2025, 7, 20), at(date(2025, 8, 1)), 110, "a2"),
    ]
    picked = select(facts, date(2025, 8, 4))
    assert picked is not None and picked.shares == 110


def test_later_amendment_does_not_reach_back() -> None:
    original = cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1")
    amendment = cover(date(2025, 4, 20), at(date(2025, 6, 2)), 105, "a2", form="10-Q/A")
    before = select([original, amendment], date(2025, 6, 2))
    after = select([original, amendment], date(2025, 6, 3))
    assert before is not None and before.shares == 100
    assert after is not None and after.shares == 105 and after.accession == "a2"


def test_distinct_values_at_the_chosen_key_are_ambiguous_with_no_fallback() -> None:
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1"),
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 120, "a1"),
        cover(date(2025, 1, 20), at(date(2025, 2, 1)), 90, "a0"),
    ]
    picked = select(facts, date(2025, 5, 5))
    assert picked is not None and picked.shares is None
    assert picked.values == (Decimal(100), Decimal(120))


def test_cover_expiry_falls_to_the_balance_sheet_count() -> None:
    facts = [
        cover(date(2024, 4, 20), at(date(2024, 5, 1)), 100, "a1"),
        sheet(date(2024, 6, 30), at(date(2024, 8, 1)), 101, "a2"),
    ]
    # add_months(2025-07-21, -15) = 2024-04-21 > 2024-04-20: the cover count has expired.
    assert (picked := select(facts, date(2025, 7, 18))) is not None and picked.concept is Concept.COVER
    assert (picked := select(facts, date(2025, 7, 21))) is not None and picked.concept is Concept.BALANCE_SHEET


def test_out_of_scope_form_is_not_admitted() -> None:
    assert select([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, form="20-F")], date(2025, 6, 2)) is None


@pytest.mark.parametrize(
    ("session", "context", "admitted"),
    [
        # Month-end clamp in a leap year: 2025-05-29/30/31 - 15 months all clamp to 2024-02-29.
        (date(2025, 5, 29), date(2024, 2, 29), True),
        (date(2025, 5, 31), date(2024, 2, 29), True),
        (date(2025, 6, 2), date(2024, 2, 29), False),  # 2024-03-02 > 2024-02-29
        (date(2025, 4, 30), date(2024, 1, 30), True),  # 2025-04-30 - 15 = 2024-01-30
        (date(2025, 4, 30), date(2024, 1, 29), False),
    ],
)
def test_age_predicate_at_month_ends_and_in_a_leap_year(session: date, context: date, admitted: bool) -> None:
    fact = cover(context, at(date(2024, 3, 5)), 100)
    assert eligible(fact, session) is admitted


def test_dst_and_weekend_acceptance_use_the_new_york_date() -> None:
    # 2026-03-08 01:30Z is Saturday 2026-03-07 20:30 EST (DST starts later that night): public from Monday.
    weekend = cover(date(2026, 3, 1), datetime(2026, 3, 8, 1, 30, tzinfo=UTC), 100)
    assert weekend.accepted == date(2026, 3, 7)
    assert not eligible(weekend, date(2026, 3, 7))
    assert eligible(weekend, date(2026, 3, 9))
    # 2026-11-02 03:30Z is Sunday 2026-11-01 22:30 EST after DST ends: still Sunday in New York.
    assert cover(date(2026, 10, 1), datetime(2026, 11, 2, 3, 30, tzinfo=UTC), 100).accepted == date(2026, 11, 1)
    # 2026-07-01 03:30Z is 2026-06-30 23:30 EDT.
    assert cover(date(2026, 6, 1), datetime(2026, 7, 1, 3, 30, tzinfo=UTC), 100).accepted == date(2026, 6, 30)


# --------------------------------------------------------------------------- steps and the ported checks

RANGE = sessions(date(2025, 5, 1), date(2025, 9, 30))
CLOSES = dict.fromkeys(RANGE, 10.0)


def _steps(facts: list[ShareFact], anchors: tuple[Anchor, ...] = FLAT, **overrides: object):  # type: ignore[no-untyped-def]
    kwargs: dict[str, object] = {"closes": CLOSES, "dollar_volume": {}, "multiclass": False, **overrides}
    return build_steps(facts, anchors, RANGE, **kwargs)  # type: ignore[arg-type]


def test_steps_are_half_open_and_never_overlap() -> None:
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1"),
        sheet(date(2025, 3, 31), at(date(2025, 5, 1)), 100, "a1"),
        cover(date(2025, 7, 20), at(date(2025, 8, 1)), 102, "a2"),
        sheet(date(2025, 6, 30), at(date(2025, 8, 1)), 102, "a2"),
    ]
    steps = _steps(facts)
    assert [s.valid_from for s in steps] == [date(2025, 5, 2), date(2025, 8, 4)]
    assert steps[0].valid_to == steps[1].valid_from
    assert steps[-1].valid_to == RANGE[-1] + timedelta(days=1)
    assert all(a.valid_to <= b.valid_from for a, b in zip(steps, steps[1:], strict=False))
    assert all(s.refusal is None and s.split_factor == 1.0 for s in steps)
    assert steps[0].evidence["checks"]["scale"]["outcome"] == "pass"
    assert steps[0].evidence["checks"]["discontinuity"]["outcome"] == "untested"
    assert steps[1].evidence["checks"]["discontinuity"]["outcome"] == "pass"
    assert steps[0].market_cap(10.0) == pytest.approx(1000.0)


def test_scale_conflict_against_the_same_accessions_sheet() -> None:
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100_000, "a1"),
        sheet(date(2025, 3, 31), at(date(2025, 5, 1)), 100, "a1"),  # x1000 apart
    ]
    (step,) = _steps(facts)
    assert step.refusal is Refusal.SHARES_SCALE_CONFLICT and step.split_factor is None


def test_scale_untested_without_a_comparator() -> None:
    (step,) = _steps([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1")])
    assert step.refusal is None
    assert step.evidence["checks"]["scale"]["outcome"] == str(Check.UNTESTED)


def test_turnover_is_evaluated_per_session_and_splits_the_step() -> None:
    facts = [cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1")]
    spike = date(2025, 6, 2)
    volume = {d: (20_000.0 if d == spike else 500.0) for d in RANGE}  # cap = 100 x 10 = 1,000
    steps = _steps(facts, dollar_volume=volume)
    assert [(s.valid_from, s.refusal) for s in steps] == [
        (date(2025, 5, 2), None),
        (spike, Refusal.SHARES_TURNOVER_IMPLAUSIBLE),
        (date(2025, 6, 3), None),
    ]


def test_zero_dollar_volume_is_tested_and_passes() -> None:
    (step,) = _steps(
        [cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1")], dollar_volume=dict.fromkeys(RANGE, 0.0)
    )
    assert step.refusal is None
    assert step.evidence["checks"]["turnover"]["outcome"] == str(Check.PASS)


def test_cap_product_that_overflows_is_invalid_input() -> None:
    facts = [cover(date(2025, 4, 20), at(date(2025, 5, 1)), 10**308, "a1")]
    (step,) = _steps(facts, closes=dict.fromkeys(RANGE, 10.0))
    assert step.refusal is Refusal.INVALID_INPUT and step.split_factor is None


def test_count_beyond_float_range_refuses_rather_than_raising() -> None:
    # float(Decimal) converts through the decimal string, so a count above sys.float_info.max becomes inf (no
    # OverflowError, unlike float(int)); the non-finite product then refuses.
    assert float(Decimal(10**400)) == float("inf")
    (step,) = _steps([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 10**400, "a1")])
    assert step.refusal is Refusal.INVALID_INPUT and step.market_cap(10.0) is None


def test_extreme_exponent_count_with_a_sheet_comparator_refuses_rather_than_raising() -> None:
    facts = [
        ShareFact(Concept.COVER, "a1", "10-Q", date(2025, 4, 20), at(date(2025, 5, 1)), Decimal("1E+999999")),
        sheet(date(2025, 3, 31), at(date(2025, 5, 1)), 1, "a1"),
    ]
    # The finite-positive guard (math.isfinite reads the Decimal as float inf) stops the scale ratio being computed.
    (step,) = _steps(facts)
    assert step.refusal is Refusal.INVALID_INPUT
    assert step.evidence["checks"]["scale"]["outcome"] == str(Check.UNTESTED)


def test_discontinuity_against_the_previous_published_count() -> None:
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1"),
        cover(date(2025, 7, 20), at(date(2025, 8, 1)), 100 * 1000, "a2"),
    ]
    first, second = _steps(facts)
    assert first.refusal is None
    assert second.refusal is Refusal.SHARES_DISCONTINUITY
    assert second.evidence["checks"]["discontinuity"]["reference"]["accession"] == "a1"


def test_discontinuity_compares_on_a_common_basis() -> None:
    # A 10-for-1 split between two counts: F = 10 on the old count, 1 on the new; shares x F agree.
    anchors = series(*([10.0] * 5 + [1.0] * (len(Q) - 5)))  # split between 2025-06-30's session and 2025-09-30's
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1"),
        cover(date(2025, 10, 20), at(date(2025, 10, 28)), 1000, "a2"),
    ]
    steps = build_steps(
        facts,
        anchors,
        sessions(date(2025, 5, 1), date(2025, 11, 28)),
        closes={},
        dollar_volume={},
        multiclass=False,
    )
    assert [(s.split_factor, s.refusal) for s in steps] == [(10.0, None), (1.0, None)]
    assert steps[1].evidence["checks"]["discontinuity"]["outcome"] == "pass"


def test_ambiguous_count_and_multiclass_refuse_with_the_selection_kept() -> None:
    facts = [
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100, "a1"),
        cover(date(2025, 4, 20), at(date(2025, 5, 1)), 200, "a1"),
    ]
    (step,) = _steps(facts)
    assert step.refusal is Refusal.SHARES_AMBIGUOUS and step.selection.accession == "a1"
    (step,) = _steps(facts[:1], multiclass=True)
    assert step.refusal is Refusal.MULTICLASS_UNAVAILABLE


def test_non_positive_count_is_invalid_input() -> None:
    (step,) = _steps([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 0, "a1")])
    assert step.refusal is Refusal.INVALID_INPUT


def test_no_eligible_count_has_no_step() -> None:
    assert _steps([cover(date(2023, 1, 20), at(date(2023, 2, 1)), 100)]) == ()


# --------------------------------------------------------------------------- gate 1

SPLIT10 = series(*([10.0] * 5 + [1.0] * (len(Q) - 5)))  # q 2025-06-30 reads 10, 2025-09-30 reads 1


def test_gate1_post_split_trade_on_an_old_count_uses_the_trade_date_factor() -> None:
    # Trades after 2025-09-30's session sit between two Phi = 1 quarters: r = price / close.
    days = [date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8)]
    closes = dict.fromkeys(days, 50.0)
    quarters, untested = gate1([Trade(d, 50.5) for d in days], closes, SPLIT10)
    assert untested == 0
    assert quarters[date(2025, 9, 30)].outcome is GateOutcome.PASS
    # Before the split (between two Phi = 10 quarters) a raw trade at 500 on a stored close of 50 also passes.
    pre = [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8)]
    quarters, _ = gate1([Trade(d, 500.0) for d in pre], dict.fromkeys(pre, 50.0), SPLIT10)
    assert quarters[date(2024, 12, 31)].outcome is GateOutcome.PASS


def test_gate1_trade_between_disagreeing_quarters_is_untested() -> None:
    day = date(2025, 8, 5)  # between 2025-06-30 (10) and 2025-09-30 (1)
    quarters, untested = gate1([Trade(day, 50.0)], {day: 50.0}, SPLIT10)
    assert quarters == {} and untested == 1


def test_gate1_one_bad_trade_among_three_moves_the_median() -> None:
    days = [date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8)]
    closes = dict.fromkeys(days, 50.0)
    good, _ = gate1([Trade(days[0], 50.0), Trade(days[1], 50.0), Trade(days[2], 500.0)], closes, SPLIT10)
    assert good[date(2025, 9, 30)].outcome is GateOutcome.PASS
    bad, _ = gate1([Trade(days[0], 50.0), Trade(days[1], 500.0), Trade(days[2], 500.0)], closes, SPLIT10)
    assert bad[date(2025, 9, 30)].outcome is GateOutcome.FAIL


def test_gate1_needs_three_trades_on_two_dates() -> None:
    day = date(2025, 10, 6)
    quarters, _ = gate1([Trade(day, 50.0)] * 3, {day: 50.0}, SPLIT10)
    assert quarters[date(2025, 9, 30)].outcome is GateOutcome.UNTESTED


def test_gate1_failure_refuses_every_step_whose_window_holds_the_quarter() -> None:
    (step,) = _steps([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100)])
    assert date(2025, 3, 31) in step.window
    days = [date(2025, 4, 7), date(2025, 4, 8), date(2025, 4, 9)]
    quarters, _ = gate1([Trade(d, 100.0) for d in days], dict.fromkeys(days, 10.0), FLAT)
    (refused,) = apply_gate1([step], quarters)
    assert refused.refusal is Refusal.ANCHOR_CONTRADICTED and refused.split_factor is None


# --------------------------------------------------------------------------- gate 2


def test_gate2_fails_below_the_float_and_refuses_the_covering_step() -> None:
    (step,) = _steps([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100)])  # cap 1,000 at close 10
    results = gate2([FloatFact(date(2025, 6, 30), 2000.0)], [step], CLOSES, RANGE)
    assert results[0].outcome is GateOutcome.FAIL
    (refused,) = apply_gate2([step], results)
    assert refused.refusal is Refusal.FLOAT_BOUND
    passing = gate2([FloatFact(date(2025, 6, 30), 1100.0)], [step], CLOSES, RANGE)
    assert passing[0].outcome is GateOutcome.PASS  # 1,000 >= 0.9 x 1,100


def test_gate2_context_with_no_step_is_uncomparable() -> None:
    (step,) = _steps([cover(date(2025, 4, 20), at(date(2025, 5, 1)), 100)])
    results = gate2([FloatFact(date(2025, 4, 30), 2000.0)], [step], CLOSES, RANGE)
    assert results[0].outcome is GateOutcome.UNCOMPARABLE
    assert apply_gate2([step], results)[0].refusal is None
