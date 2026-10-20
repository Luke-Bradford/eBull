"""#3925 (spec Amendment 3): the scale check's EPS-identity comparator when the accession has no balance-sheet count.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` § "Amendment 3", "Tests". Each test states its
expected outcome in its name. Pure: no database.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services import market_calendar
from app.services.factor_panel import Check
from app.services.pit_market_cap import (
    EPS_BASIC,
    INCOME_AVAILABLE,
    NET_INCOME,
    WEIGHTED_AVERAGE,
    Anchor,
    AnchorStatus,
    Concept,
    IdentityFact,
    PeriodStatus,
    Refusal,
    ShareFact,
    build_steps,
    last_session_on_or_before,
    period_status,
    quarter_ends,
    weighted_average_scale,
)
from scripts import measure_3925_premise as oracle

AS_OF = date(2026, 10, 16)
Q = quarter_ends(date(2024, 6, 30), AS_OF)
SESSIONS = tuple(
    d
    for d in (date(2024, 6, 1) + timedelta(days=i) for i in range(900))
    if market_calendar.us_market_status(d) != "closed"
)
FLAT = tuple(Anchor(q, last_session_on_or_before(SESSIONS, q) or q, AnchorStatus.OK, 25, 1.0, 1.0, 1.0) for q in Q)
RANGE = tuple(d for d in SESSIONS if date(2025, 5, 1) <= d <= date(2025, 9, 30))
ACCEPTED = date(2025, 5, 1)
YTD = (date(2025, 1, 1), date(2025, 3, 31))  # a Q1 period: 90 days, no DQC minimum


def at(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 21, tzinfo=UTC)


def cover(value: int | Decimal, accession: str = "a1") -> ShareFact:
    return ShareFact(Concept.COVER, accession, "10-Q", date(2025, 4, 20), at(ACCEPTED), Decimal(value))


def sheet(value: int | Decimal, accession: str = "a1") -> ShareFact:
    return ShareFact(Concept.BALANCE_SHEET, accession, "10-Q", date(2025, 3, 31), at(ACCEPTED), Decimal(value))


def period(
    wa: str | None,
    eps: str | None,
    ni: str | None,
    *,
    span: tuple[date, date] = YTD,
    focus: str | None = "Q1",
    available: str | None = None,
) -> list[IdentityFact]:
    start, end = span
    rows = [(WEIGHTED_AVERAGE, wa), (EPS_BASIC, eps), (NET_INCOME, ni), (INCOME_AVAILABLE, available)]
    return [IdentityFact(c, start, end, Decimal(v), focus) for c, v in rows if v is not None]


def _steps(facts: list[ShareFact], identity: dict[str, list[IdentityFact]]):  # type: ignore[no-untyped-def]
    return build_steps(
        facts, FLAT, RANGE, closes=dict.fromkeys(RANGE, 10.0), dollar_volume={}, multiclass=False, identity=identity
    )


def _operands(rows: list[IdentityFact]) -> dict[str, set[Decimal]]:
    out: dict[str, set[Decimal]] = {}
    for r in rows:
        out.setdefault(r.concept, set()).add(r.value)
    return out


# --------------------------------------------------------------------------- rule 1: eligibility


def test_no_sheet_uses_the_weighted_average_and_fails_a_x1000_cover() -> None:
    # CPK 0001628280-25-039014: 23,544,479,000 on the cover; 23,133,000 weighted, $74.8M / $3.23.
    (step,) = _steps(
        [cover(23_544_479_000)],
        {"a1": period("23133000", "3.23", "74800000")},
    )
    scale = step.evidence["checks"]["scale"]
    assert step.refusal is Refusal.SHARES_SCALE_CONFLICT
    assert scale["comparator"] == "weighted_average_basic" and scale["sheet_unavailable"] == "none"
    assert scale["periods"][0]["cover_vs"] == {"weighted_average": "fail", "implied": "fail"}


def test_no_sheet_and_a_correct_cover_passes() -> None:
    (step,) = _steps(
        [cover(24_106_455)],
        {"a1": period("23997000", "3.53", "84700000")},
    )
    assert step.refusal is None
    assert step.evidence["checks"]["scale"]["outcome"] == str(Check.PASS)


def test_ambiguous_sheet_falls_back() -> None:
    facts = [cover(100_000), sheet(100), ShareFact(Concept.BALANCE_SHEET, "a1", "10-Q", date(2025, 3, 31),
                                                  at(ACCEPTED), Decimal(200))]  # fmt: skip
    (step,) = _steps(facts, {"a1": period("100", "1.00", "100")})
    scale = step.evidence["checks"]["scale"]
    assert scale["sheet_unavailable"] == "ambiguous" and step.refusal is Refusal.SHARES_SCALE_CONFLICT


def test_zero_sheet_falls_back() -> None:
    (step,) = _steps([cover(100), sheet(0)], {"a1": period("100", "1.00", "100")})
    scale = step.evidence["checks"]["scale"]
    assert scale["sheet_unavailable"] == "invalid" and scale["outcome"] == str(Check.PASS)


def test_valid_sheet_keeps_the_balance_sheet_check_and_ignores_the_weighted_average() -> None:
    (step,) = _steps([cover(100), sheet(100)], {"a1": period("1", "100.00", "100")})  # weighted would fail
    scale = step.evidence["checks"]["scale"]
    assert scale["comparator"] == "balance_sheet" and scale["outcome"] == str(Check.PASS) and "periods" not in scale


def test_zero_cover_is_untested_with_no_fallback() -> None:
    (step,) = _steps([cover(0)], {"a1": period("100", "1.00", "100")})
    scale = step.evidence["checks"]["scale"]
    assert step.refusal is Refusal.INVALID_INPUT and scale["comparator"] is None and "periods" not in scale


# --------------------------------------------------------------------------- rules 3 and 4: periods


def test_latest_end_is_used_and_an_earlier_end_ignored() -> None:
    rows = period("100", "1.00", "100") + period("1", "100.00", "100", span=(date(2024, 1, 1), date(2024, 3, 31)))
    outcome, evidence = weighted_average_scale(Decimal(100), ACCEPTED, rows)
    assert outcome is Check.PASS and evidence["latest_end"] == "2025-03-31" and len(evidence["periods"]) == 1


def test_latest_end_after_acceptance_has_no_period() -> None:
    rows = period("100", "1.00", "100", span=(date(2025, 4, 1), date(2025, 6, 30)))
    outcome, evidence = weighted_average_scale(Decimal(100_000), ACCEPTED, rows)
    assert outcome is Check.UNTESTED and evidence["no_period"] == "end_after_acceptance"


def test_no_weighted_average_has_no_period() -> None:
    outcome, evidence = weighted_average_scale(Decimal(100), ACCEPTED, period(None, "1.00", "100"))
    assert outcome is Check.UNTESTED and evidence["no_period"] == "no_weighted_average"


def test_q2_quarter_is_short_and_the_year_to_date_is_consistent() -> None:
    end = date(2025, 6, 30)
    rows = period("100", "1.00", "100", span=(date(2025, 4, 1), end), focus="Q2")
    rows += period("100", "2.00", "200", span=(date(2025, 1, 1), end), focus="Q2")
    _, evidence = weighted_average_scale(Decimal(100), date(2025, 8, 1), rows)
    assert [p["status"] for p in evidence["periods"]] == ["consistent", "dqc_short_period"]


def test_q1_single_period_is_used() -> None:
    _, evidence = weighted_average_scale(Decimal(100), ACCEPTED, period("100", "1.00", "100", focus="Q1"))
    assert evidence["periods"][0]["status"] == "consistent"


@pytest.mark.parametrize(("focus", "minimum"), [("Q2", 120), ("Q3", 200), ("FY", 310)])
def test_dqc_minimum_days_boundaries(focus: str, minimum: int) -> None:
    operands = _operands(period("100", "1.00", "100"))
    assert period_status(minimum - 1, focus, operands)[0] is PeriodStatus.DQC_SHORT_PERIOD
    assert period_status(minimum, focus, operands)[0] is PeriodStatus.CONSISTENT


def test_unknown_and_conflicting_focus_are_unused() -> None:
    assert period_status(90, None, _operands(period("100", "1.00", "100")))[0] is PeriodStatus.FOCUS_UNKNOWN
    rows = period("100", "1.00", "100", focus="Q1") + [
        IdentityFact(EPS_BASIC, date(2024, 1, 1), date(2024, 3, 31), Decimal("0.5"), "Q2")
    ]
    _, evidence = weighted_average_scale(Decimal(100), ACCEPTED, rows)
    assert evidence["focus"] is None and evidence["periods"][0]["status"] == "focus_unknown"


def test_equal_values_with_different_fiscal_period_are_one_value() -> None:
    rows = period("100", "1.00", "100", focus="Q1") + period("100", None, None, focus="")
    outcome, evidence = weighted_average_scale(Decimal(100), ACCEPTED, rows)
    assert outcome is Check.PASS and evidence["periods"][0]["values"][WEIGHTED_AVERAGE] == ["100"]


def test_two_distinct_values_are_operand_count() -> None:
    operands = _operands(period("100", "1.00", "100") + period("101", None, None))
    assert period_status(90, "Q1", operands)[0] is PeriodStatus.OPERAND_COUNT


def test_preferred_numerator_present_absent_and_ambiguous() -> None:
    # Present: income available to common (90) decides, not net income (100).
    assert period_status(90, "Q1", _operands(period("100", "0.90", "100", available="90")))[0] is (
        PeriodStatus.CONSISTENT
    )
    assert period_status(90, "Q1", _operands(period("100", "1.00", "100", available="90")))[0] is (
        PeriodStatus.IDENTITY_DISAGREES
    )
    # Absent: net income.
    assert period_status(90, "Q1", _operands(period("100", "1.00", "100")))[0] is PeriodStatus.CONSISTENT
    # Two values of the preferred concept: unusable, no fallback to net income.
    rows = period("100", "1.00", "100", available="90") + period(None, None, None, available="95")
    assert period_status(90, "Q1", _operands(rows))[0] is PeriodStatus.OPERAND_COUNT


def test_loss_period_is_consistent_and_a_sign_mismatch_invalid() -> None:
    assert period_status(90, "Q1", _operands(period("100", "-1.00", "-100")))[0] is PeriodStatus.CONSISTENT
    assert period_status(90, "Q1", _operands(period("100", "1.00", "-100")))[0] is PeriodStatus.OPERAND_INVALID


def test_zero_eps_and_non_finite_operand_are_invalid() -> None:
    assert period_status(90, "Q1", _operands(period("100", "0", "100")))[0] is PeriodStatus.OPERAND_INVALID
    assert period_status(90, "Q1", _operands(period("100", "1E+400", "100")))[0] is PeriodStatus.OPERAND_INVALID


@pytest.mark.parametrize("sign", ["", "-"])
def test_penny_boundary_is_exact_decimal(sign: str) -> None:
    # |101 - 1 x 100| = 1 = 0.01 x 100: exactly one cent per share passes (a float 101/100 - 1 would not).
    assert period_status(90, "Q1", _operands(period("100", f"{sign}1.00", f"{sign}101")))[0] is (
        PeriodStatus.CONSISTENT
    )
    assert period_status(90, "Q1", _operands(period("100", f"{sign}1.00", f"{sign}101.000001")))[0] is (
        PeriodStatus.IDENTITY_DISAGREES
    )


def test_hundredfold_bound_on_the_implied_count_at_eps_one_cent() -> None:
    # EPS 0.01 and net income 1M imply 100M shares; a weighted average 1,000x larger still meets the penny test
    # (1M / 100B = 0.00001), so the 100x bound is what refuses it.
    assert period_status(90, "Q1", _operands(period("100000000000", "0.01", "1000000")))[0] is (
        PeriodStatus.IDENTITY_DISAGREES
    )
    assert period_status(90, "Q1", _operands(period("100000000", "0.01", "1000000")))[0] is PeriodStatus.CONSISTENT


# --------------------------------------------------------------------------- rule 5: outcome


def test_cover_at_exactly_100x_passes_and_just_over_fails() -> None:
    rows = period("100", "1.00", "100")
    assert weighted_average_scale(Decimal(10_000), ACCEPTED, rows)[0] is Check.PASS
    assert weighted_average_scale(Decimal(10_001), ACCEPTED, rows)[0] is Check.FAIL


def test_one_consistent_period_agreeing_and_one_not_fails() -> None:
    end = date(2025, 3, 31)
    rows = period("100", "1.00", "100", span=(date(2025, 1, 1), end))
    rows += period("1000000", "1.00", "1000000", span=(date(2024, 10, 1), end))
    assert weighted_average_scale(Decimal(100), ACCEPTED, rows)[0] is Check.FAIL


def test_no_consistent_period_is_untested_and_does_not_refuse() -> None:
    (step,) = _steps([cover(100_000)], {"a1": period("100", "5.00", "100")})  # identity disagrees
    scale = step.evidence["checks"]["scale"]
    assert step.refusal is None and scale["outcome"] == str(Check.UNTESTED) and scale["comparator"] is None
    assert scale["periods"][0]["status"] == "identity_disagrees" and scale["sheet_unavailable"] == "none"


def test_substituted_numerator_can_change_the_verdict_stated_limit() -> None:
    # Net income stands in for income available to common. Both pairs pass the screen, yet the substituted
    # implied count (1) fails a cover of 150 that the documented numerator's (100) would pass.
    assert weighted_average_scale(Decimal(150), ACCEPTED, period("100", "0.0001", "0.01"))[0] is Check.PASS
    assert weighted_average_scale(Decimal(150), ACCEPTED, period("100", "0.0001", "0.0001"))[0] is Check.FAIL


def test_lost_reference_lets_a_later_wrong_count_publish_stated_limit() -> None:
    # A correct count refused by a mis-scaled consistent period is no discontinuity reference, so a later x1000
    # count with no consistent period publishes.
    later = ShareFact(Concept.COVER, "a2", "10-Q", date(2025, 7, 20), at(date(2025, 8, 1)), Decimal(100_000))
    facts = [cover(100, "a1"), later]
    first, second = _steps(facts, {"a1": period("1000000", "1.00", "1000000")})
    assert first.refusal is Refusal.SHARES_SCALE_CONFLICT
    assert second.refusal is None


# --------------------------------------------------------------------------- the oracle agrees


@pytest.mark.parametrize(
    "rows",
    [
        period("100", "1.00", "100"),
        period("100", "1.00", "101"),
        period("100", "-1.00", "-101.000001"),
        period("100", "1.00", "100", available="90"),
        period("100000000000", "0.01", "1000000"),
        period("100", "1.00", "100", focus=None),
        period("23133000", "3.23", "74800000", span=(date(2025, 1, 1), date(2025, 3, 31)), focus="Q2"),
    ],
)
@pytest.mark.parametrize("cover_value", [Decimal(100), Decimal(23_544_479_000), Decimal(10_001)])
def test_production_matches_the_premise_scripts_independent_evaluator(
    rows: list[IdentityFact], cover_value: Decimal
) -> None:
    outcome, evidence = weighted_average_scale(cover_value, ACCEPTED, rows)
    facts = [oracle.Fact(r.concept, r.start, r.end, r.value, r.fiscal_period) for r in rows]
    expected = oracle.evaluate(cover_value, ACCEPTED, None, facts)
    assert str(outcome) == expected.outcome
    assert [p["status"] for p in evidence["periods"]] == [status for _, _, status, _, _ in expected.periods]
