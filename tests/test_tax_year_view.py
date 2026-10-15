"""#3616 slice 3d-4a part A: the tax-year computation, flags, classes and reserve arithmetic (spec Tests 1–10).

Every expected figure is computed by hand in the test's own comment.
"""

from __future__ import annotations

import decimal
from datetime import date
from decimal import Decimal

import pytest

from app.services.tax_assumptions import ALWAYS_STATED, ASSUMPTIONS
from app.services.tax_year_view import (
    EvidenceSubject,
    FundDisposal,
    FundSecurity,
    Member,
    RunRows,
    ShareDisposal,
    WithheldSecurity,
    YearUnsupported,
    _exact,
    compute_reading,
    reserve_figures,
    run_flags,
    supported_tax_years,
    to_usd_ceiling,
    year_view,
)

D = Decimal
CURRENT = "2026/27"
DAY = date(2026, 6, 1)  # 2026/27, 18% / 24%


def _share(gain: str | None, *, uk_date: date = DAY, sid: int = 1, reasons: tuple[str, ...] = ("r",)) -> ShareDisposal:
    from app.services.tax_ledger import _compute_tax_year

    return ShareDisposal(
        tax_security_id=sid,
        uk_date=uk_date,
        tax_year=_compute_tax_year(uk_date),
        status="computed" if gain is not None else "amount_missing",
        gain_gbp=D(gain) if gain is not None else None,
        missing_lot_keys=() if gain is not None else ("lot:x",),
        reasons=reasons,
    )


def _fund(
    character: str | None,
    computed: str | None,
    *,
    uk_date: date = DAY,
    sid: int = 9,
    seq: int = 1,
    reasons: tuple[str, ...] = ("f",),
) -> FundDisposal:
    from app.services.tax_ledger import _compute_tax_year

    c = D(computed) if computed is not None else None
    basic = max(c, D(0)) if c is not None else None
    return FundDisposal(
        tax_security_id=sid,
        seq=seq,
        lot_key=f"lot:{sid}:{seq}",
        uk_date=uk_date,
        tax_year=_compute_tax_year(uk_date),
        status="computed" if c is not None else "amount_missing",
        gain_character=character,  # type: ignore[arg-type]
        basic_gain_gbp=basic,
        offshore_income_gain_gbp=basic
        if character == "offshore_income_gain"
        else (D(0) if character in ("capital_loss", "nil") else None),
        chargeable_gain_gbp=c
        if character in ("capital_loss", "nil")
        else (D(0) if character == "offshore_income_gain" else None),
        missing_lot_keys=() if c is not None else ("lot:y",),
        reasons=reasons,
    )


def _rows(**kw: object) -> RunRows:
    base: dict[str, object] = {
        "share_disposals": (),
        "fund_disposals": (),
        "withheld": (),
        "subjects": (),
        "non_reporting_funds": (),
    }
    base.update(kw)
    return RunRows(**base)  # type: ignore[arg-type]


def _m(amount: str, uk_date: date = DAY) -> Member:
    return Member(D(amount), uk_date)


# --- Design 2 arithmetic (Tests 1-5) -----------------------------------------------------------------------------


def test_1_gains_less_losses_then_aea() -> None:
    # 5,000 - 1,000 = 4,000; AEA 3,000; taxable 1,000; 18% = 180, 24% = 240.
    r = compute_reading([_m("5000"), _m("-1000")])
    assert (r.gains, r.losses, r.net_current) == (D(5000), D(1000), D(4000))
    assert (r.aea_used, r.aea_remaining, r.taxable) == (D(3000), D(0), D(1000))
    assert (r.tax_basic, r.tax_higher) == (D(180), D(240))


def test_2_losses_exceed_gains_are_carried() -> None:
    # 1,000 - 1,500: net 0, 500 carried, AEA untouched.
    r = compute_reading([_m("1000"), _m("-1500")])
    assert (r.net_current, r.current_losses_carried_forward, r.losses_carried_forward) == (D(0), D(500), D(500))
    assert (r.aea_used, r.aea_remaining, r.taxable, r.tax_basic) == (D(0), D(3000), D(0), D(0))


def test_3_current_loss_deducted_even_below_the_aea() -> None:
    # CG21500: 3,500 - 1,000 = 2,500 < 3,000; AEA used 2,500, 500 unused, nothing carried.
    r = compute_reading([_m("3500"), _m("-1000")])
    assert (r.net_current, r.aea_used, r.aea_remaining, r.taxable) == (D(2500), D(2500), D(500), D(0))
    assert r.losses_carried_forward == D(0)


def test_4_brought_forward_only_down_to_the_aea() -> None:
    # 4,000 net; AEA 3,000; 1,000 of the 2,000 brought forward used; 1,000 remains.
    r = compute_reading([_m("4000")], brought_forward_opening=D(2000))
    assert (r.aea_used, r.brought_forward_used, r.brought_forward_remaining) == (D(3000), D(1000), D(1000))
    assert (r.losses_carried_forward, r.taxable) == (D(1000), D(0))
    # 2,500 - 500 = 2,000 < AEA: none of the 500 brought forward is used.
    r = compute_reading([_m("2500"), _m("-500")], brought_forward_opening=D(500))
    assert (r.net_current, r.brought_forward_used, r.brought_forward_remaining) == (D(2000), D(0), D(500))


def test_5_2024_25_deducts_from_the_higher_rate_period_first() -> None:
    early, late = date(2024, 6, 1), date(2025, 1, 10)  # 10%/20%, then 18%/24%
    # AEA 3,000 off the later 4,000 -> later 1,000 taxed: 4,000*10% + 1,000*18% = 580; 4,000*20% + 1,000*24% = 1,040.
    r = compute_reading([_m("4000", early), _m("4000", late)])
    assert (r.taxable, r.tax_basic, r.tax_higher) == (D(5000), D(580), D(1040))
    # Loss 2,000 and 2,000 of the AEA off the later gains, the other 1,000 off the earlier:
    # 3,000*10% = 300; 3,000*20% = 600.
    r = compute_reading([_m("4000", early), _m("4000", late), _m("-2000", late)])
    assert (r.taxable, r.tax_basic, r.tax_higher) == (D(3000), D(300), D(600))


def test_negative_brought_forward_is_refused() -> None:
    with pytest.raises(ValueError):
        compute_reading([], brought_forward_opening=D(-1))


def test_disposal_outside_the_rate_table_is_unsupported() -> None:
    with pytest.raises(YearUnsupported) as exc:
        compute_reading([_m("10", date(2023, 5, 1))])
    assert exc.value.years == ("2023/24",)


# --- Readings (Test 6) -------------------------------------------------------------------------------------------


def test_6_two_readings_bound_the_cgt_figure() -> None:
    rows = _rows(
        share_disposals=(_share("10000"),),
        fund_disposals=(
            _fund("oig_or_capital_unresolved", "2000", seq=1),
            _fund("oig_or_capital_unresolved", "1500", seq=2, uk_date=date(2026, 9, 1)),
            _fund("offshore_income_gain", "700", seq=3, sid=10),
            _fund("capital_loss", "-400", seq=4),
            _fund("nil", "0", seq=5),
        ),
    )
    v = year_view(rows, CURRENT, CURRENT)
    # Income reading: 10,000 - 400 = 9,600; taxable 6,600; 18% 1,188; 24% 1,584.
    assert (v.income_reading.net_current, v.income_reading.taxable) == (D(9600), D(6600))
    assert (v.income_reading.tax_basic, v.income_reading.tax_higher) == (D(1188), D(1584))
    # Capital reading adds the 3,500 unresolved: taxable 10,100; 18% 1,818; 24% 2,424.
    assert v.capital_reading is not None
    assert (v.capital_reading.taxable, v.capital_reading.tax_basic, v.capital_reading.tax_higher) == (
        D(10100),
        D(1818),
        D(2424),
    )
    assert v.unresolved_gains == D(3500) and v.offshore_income_gains == D(700)
    diff = v.capital_reading.tax_higher - v.income_reading.tax_higher
    assert D(0) <= diff <= v.unresolved_gains * D("0.24")


def test_6_a_year_without_unresolved_gains_has_one_reading() -> None:
    v = year_view(_rows(share_disposals=(_share("5"),)), CURRENT, CURRENT)
    assert v.capital_reading is None


def test_dev_outcome_run_8() -> None:
    # The spec's dev outcome: share loss -0.00753863550700, SPY unresolved 1.10443720572584.
    rows = _rows(
        share_disposals=(_share("-0.00753863550700"),),
        fund_disposals=(_fund("oig_or_capital_unresolved", "1.10443720572584"),),
        withheld=(WithheldSecurity(3, ("AAA",), ("unresolved_subject",)),),
        non_reporting_funds=(FundSecurity(9, ("SPY",)),),
    )
    v = year_view(rows, CURRENT, CURRENT)
    assert v.capital_reading is not None
    assert (v.capital_reading.net_current, v.capital_reading.taxable) == (D("1.09689857021884"), D(0))
    assert (v.income_reading.net_current, v.income_reading.current_losses_carried_forward) == (
        D(0),
        D("0.00753863550700"),
    )
    res = reserve_figures(rows, "higher", CURRENT)
    assert str(res.total_gbp) == "1.10443720572584"
    assert res.status == "incomplete"


# --- Flags (Test 7) ----------------------------------------------------------------------------------------------


def _codes(flags: object) -> set[str]:
    return {f.code for f in flags}  # type: ignore[attr-defined]


def test_7_amount_missing_and_withheld() -> None:
    rows = _rows(
        share_disposals=(_share(None),),
        withheld=(WithheldSecurity(4, ("BBB",), ("unresolved_subject",)),),
    )
    v = year_view(rows, CURRENT, CURRENT)
    assert {"amounts_missing", "securities_withheld", "dividends_not_computed", "account_only"} <= _codes(v.flags)
    assert v.complete is False
    # Withheld is raised on every year; a missing amount only on its own.
    other = year_view(rows, "2025/26", CURRENT)
    assert "securities_withheld" in _codes(other.flags)
    assert "amounts_missing" not in _codes(other.flags)


@pytest.mark.parametrize(
    ("first", "last", "inside"),
    [
        (date(2026, 5, 1), date(2026, 5, 2), {"2026/27"}),  # inside one year
        (date(2025, 3, 1), date(2026, 5, 2), {"2024/25", "2025/26", "2026/27"}),  # across boundaries
        (date(2024, 5, 1), date(2024, 6, 1), {"2024/25"}),  # outside the later years
        (date(2025, 5, 1), None, {"2025/26", "2026/27"}),  # first only: its year and later
        (None, date(2025, 5, 1), {"2024/25", "2025/26"}),  # last only: up to its year
        (None, None, {"2024/25", "2025/26", "2026/27"}),  # neither: every year
    ],
)
def test_7_subject_year_scope(first: date | None, last: date | None, inside: set[str]) -> None:
    subject = EvidenceSubject("order:1", "excluded_non_share_product", "cfd", (), first, last)
    rows = _rows(subjects=(subject,))
    hit = {y for y in supported_tax_years(CURRENT) if "products_excluded" in _codes(year_view(rows, y, CURRENT).flags)}
    assert hit == inside


def test_7_fund_income_and_income_tax_flags() -> None:
    rows = _rows(
        fund_disposals=(_fund("offshore_income_gain", "5"),),
        non_reporting_funds=(FundSecurity(9, ("SPY",)),),
    )
    codes = _codes(year_view(rows, CURRENT, CURRENT).flags)
    assert {"fund_income_not_computed", "income_tax_not_computed"} <= codes
    assert "income_tax_not_computed" not in _codes(year_view(rows, "2025/26", CURRENT).flags)


def test_7_all_withheld_and_empty_runs_are_incomplete_with_zero_members() -> None:
    rows = _rows(withheld=(WithheldSecurity(4, ("BBB",), ("r",)),))
    v = year_view(rows, CURRENT, CURRENT)
    assert v.complete is False and v.income_reading.gains == D(0)
    empty = year_view(_rows(), CURRENT, CURRENT)
    assert empty.complete is False  # dividends_not_computed is always raised
    assert [f.code for f in run_flags(_rows(), CURRENT)] == ["dividends_not_computed", "account_only"]


# --- Class (Test 8) ----------------------------------------------------------------------------------------------


def test_8_every_figure_is_provisional() -> None:
    assert year_view(_rows(), CURRENT, CURRENT).figure_class == "provisional"
    reason_free = year_view(_rows(share_disposals=(_share("5", reasons=()),)), CURRENT, CURRENT)
    assert reason_free.figure_class == "provisional" and reason_free.reasons == ()
    assert reason_free.assumption_ids == ALWAYS_STATED


def test_assumptions_are_defined_and_a8_follows_its_reason() -> None:
    assert set(ALWAYS_STATED) | {"A8"} == set(ASSUMPTIONS)
    v = year_view(_rows(share_disposals=(_share("5", reasons=("distribution_character_assumed",)),)), CURRENT, CURRENT)
    assert "A8" in v.assumption_ids


# --- Arithmetic (Test 9) -----------------------------------------------------------------------------------------


def test_9_rounding_raises() -> None:
    with _exact(), pytest.raises(decimal.Inexact):
        _ = D(1) / D(3)


# --- Reserve (Test 10) -------------------------------------------------------------------------------------------


def test_10_reserve_keeps_every_year_and_reserves_income_whole() -> None:
    rows = _rows(
        share_disposals=(_share("4000", uk_date=date(2025, 7, 1)), _share("3500")),
        fund_disposals=(
            _fund("offshore_income_gain", "100", seq=1),
            _fund("oig_or_capital_unresolved", "50", seq=2),
        ),
    )
    res = reserve_figures(rows, "higher", CURRENT)
    by_year = {y.tax_year: y for y in res.years}
    assert set(by_year) == {"2025/26", "2026/27"}  # no year released on a deadline
    # 2025/26: 4,000 - 3,000 = 1,000 * 24% = 240.
    assert (by_year["2025/26"].cgt_gbp, by_year["2025/26"].income_buffer_gbp) == (D(240), D(0))
    # 2026/27: 3,500 - 3,000 = 500 * 24% = 120 under the income reading; buffer 100 + 50 whole.
    assert (by_year["2026/27"].cgt_gbp, by_year["2026/27"].income_buffer_gbp) == (D(120), D(150))
    assert res.total_gbp == D(510)
    basic = reserve_figures(rows, "basic", CURRENT)
    assert basic.total_gbp == D(180) + D(90) + D(150)


def test_10_reserve_of_a_run_with_no_disposal() -> None:
    res = reserve_figures(_rows(), "higher", CURRENT)
    assert (res.years, res.total_gbp, res.status) == ((), D(0), "incomplete")
    assert "dividends_not_computed" in _codes(res.flags)


def test_10_reserve_names_unsupported_years() -> None:
    rows = _rows(share_disposals=(_share("5", uk_date=date(2023, 5, 1)), _share("5", uk_date=date(2022, 5, 1), sid=2)))
    with pytest.raises(YearUnsupported) as exc:
        reserve_figures(rows, "higher", CURRENT)
    assert exc.value.years == ("2022/23", "2023/24")


def test_10_usd_conversion_rounds_up() -> None:
    assert to_usd_ceiling(D("1.10443720572584"), D("1.3")) == D("1.44")  # 1.435768... -> 1.44
    assert to_usd_ceiling(D("1.00"), D("1.25")) == D("1.25")


def test_supported_years_run_from_the_rate_table() -> None:
    assert supported_tax_years(CURRENT) == ("2026/27", "2025/26", "2024/25")
