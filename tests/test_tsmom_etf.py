"""#3620 slice 1 — the pure rules of ``app/services/tsmom_etf.py``, on fixtures only.

Spec: ``docs/research/2026-10-06-3620-cross-asset-tsmom.md``. No database: nothing here reads a real return.
"""

from __future__ import annotations

import math
import statistics
from datetime import date

import pytest

from app.services.etf_total_return_reader import EtfMonthlyReturn, EtfVerdict
from app.services.total_return_reader import Month, add_months
from app.services.tsmom_etf import (
    CLASSES,
    COMPARATORS,
    COVERAGE_CAP,
    FUNDS,
    FundCoverage,
    TsmomRefusal,
    baseline_fills,
    basket_weights,
    build_census,
    c1x_weights,
    c2_offsets,
    c3_baskets,
    c4_signal,
    check_weights,
    circular_shift,
    evaluated_formations,
    fill_month,
    form,
    fund_coverage,
    month_range,
    simulate,
    slots,
    solve_cost,
    stamp_audit,
    tsmom_signal,
    tsmom_weights,
    validate_through,
    volatility,
)


def _row(
    symbol: str, month: Month, value: float, *, end_bar: date | None = None, start_bar: date | None = None
) -> EtfMonthlyReturn:
    return EtfMonthlyReturn(
        symbol=symbol,
        month=date(month[0], month[1], 1),
        total_return=value,
        source="intrader",
        source_key="1",
        reference_symbol=None,
        start_bar=start_bar,
        end_bar=end_bar,
        accession_number=None,
        price_return_only=False,
        dividend_capture_degraded=False,
    )


def _coverage(symbol: str, first: Month, last: Month, value: float = 0.01) -> FundCoverage:
    return FundCoverage(symbol, EtfVerdict.NPORT, first, last, {m: value for m in month_range(first, last)})


# --- universe ----------------------------------------------------------------------------------------------------


def test_the_partition_is_six_disjoint_classes_of_thirty_funds_without_dbc() -> None:
    members = [s for funds in CLASSES.values() for s in funds]
    assert len(CLASSES) == 6
    assert len(members) == len(set(members)) == 30
    assert "DBC" not in members
    assert members.count("XLRE") == 1 and "XLRE" in CLASSES["real_estate"]
    assert set(COMPARATORS) == {"SPY", "AGG", "VTI", "EFA", "EEM", "VNQ", "IEF", "TIP"}


# --- validity and coverage ---------------------------------------------------------------------------------------


def test_a_pool_must_carry_the_intrader_only_verdict_and_a_fund_its_nport_verdict() -> None:
    rows = [_row("GLD", (2010, 1), 0.01)]
    assert fund_coverage("GLD", EtfVerdict.PRICE_RETURN_EXCLUDED, rows).first_month == (2010, 1)
    with pytest.raises(TsmomRefusal, match="verdict"):
        fund_coverage("GLD", EtfVerdict.PRICE_RETURN_ONLY, rows)
    with pytest.raises(TsmomRefusal, match="verdict"):
        fund_coverage("TLT", EtfVerdict.UNRESOLVED_REFERENCE, [_row("TLT", (2010, 1), 0.01)])
    assert (
        fund_coverage("SPY", EtfVerdict.NPORT_PROXY, [_row("SPY", (2010, 1), 0.01)]).verdict is EtfVerdict.NPORT_PROXY
    )


def test_a_month_end_bar_more_than_seven_days_before_the_month_end_refuses() -> None:
    ok = fund_coverage("TLT", EtfVerdict.NPORT, [_row("TLT", (2010, 1), 0.01, end_bar=date(2010, 1, 24))])
    validate_through(ok, (2010, 1))
    stale = fund_coverage("TLT", EtfVerdict.NPORT, [_row("TLT", (2010, 1), 0.01, end_bar=date(2010, 1, 23))])
    with pytest.raises(TsmomRefusal, match="stale_month_end"):
        validate_through(stale, (2010, 1))


def test_a_stale_opening_anchor_on_the_first_row_refuses() -> None:
    fresh = [_row("TLT", (2010, 1), 0.01, start_bar=date(2009, 12, 31), end_bar=date(2010, 1, 29))]
    validate_through(fund_coverage("TLT", EtfVerdict.NPORT, fresh), (2010, 1))
    stale = [_row("TLT", (2010, 1), 0.01, start_bar=date(2009, 12, 15), end_bar=date(2010, 1, 29))]
    with pytest.raises(TsmomRefusal, match="opening bar"):
        validate_through(fund_coverage("TLT", EtfVerdict.NPORT, stale), (2010, 1))


@pytest.mark.parametrize("value", [-1.0, -1.5, math.nan, math.inf])
def test_an_invalid_wealth_return_refuses_through_e_only(value: float) -> None:
    coverage = fund_coverage("TLT", EtfVerdict.NPORT, [_row("TLT", (2010, 1), 0.0), _row("TLT", (2010, 2), value)])
    validate_through(coverage, (2010, 1))  # the bad row is after E: untouched
    with pytest.raises(TsmomRefusal, match="invalid_value"):
        validate_through(coverage, (2010, 2))


def test_no_rows_refuse_and_a_duplicate_month_refuses_through_e_only() -> None:
    with pytest.raises(TsmomRefusal, match="no_panel_rows"):
        fund_coverage("TLT", EtfVerdict.NPORT, [])
    rows = [_row("TLT", (2010, 1), 0.0), _row("TLT", (2010, 2), 0.0), _row("TLT", (2010, 2), 0.0)]
    coverage = fund_coverage("TLT", EtfVerdict.NPORT, rows)
    validate_through(coverage, (2010, 1))
    with pytest.raises(TsmomRefusal, match="duplicate_month"):
        validate_through(coverage, (2010, 2))


def test_the_census_ignores_an_invalid_row_after_e() -> None:
    funds, comparators = _universe()
    bad = dict(funds["TLT"].returns)
    bad[(2025, 6)] = math.nan
    funds["TLT"] = FundCoverage("TLT", EtfVerdict.NPORT, (2000, 1), (2026, 1), bad)
    assert build_census(funds, comparators).end == COVERAGE_CAP
    bad[(2024, 6)] = math.nan
    with pytest.raises(TsmomRefusal, match="invalid_value"):
        build_census(funds, comparators)


def _universe(
    first: Month = (2000, 1), last: Month = (2026, 1)
) -> tuple[dict[str, FundCoverage], dict[str, FundCoverage]]:
    funds = {s: _coverage(s, first, last) for s in FUNDS}
    comparators = {s: _coverage(s, first, last) for s in COMPARATORS}
    return funds, comparators


def test_start_is_the_first_month_every_class_has_an_eligible_fund() -> None:
    funds, comparators = _universe()
    funds["GLD"] = _coverage("GLD", (2004, 12), (2026, 1))
    for symbol in ("IAU", "SLV", "USO"):
        funds[symbol] = _coverage(symbol, (2006, 5), (2026, 1))
    census = build_census(funds, comparators)
    assert census.start == (2005, 11)
    assert census.start_constituents["commodities_metals"] == ("GLD",)


def test_e_is_the_cap_unless_a_fund_or_comparator_ends_earlier() -> None:
    funds, comparators = _universe()
    assert build_census(funds, comparators).end == COVERAGE_CAP
    comparators["VTI"] = _coverage("VTI", (2000, 1), (2023, 6))
    census = build_census(funds, comparators)
    assert census.end == (2023, 6) and census.limiting == ("VTI",)
    funds, comparators = _universe()
    funds["SPY"] = _coverage("SPY", (2000, 1), (2023, 6))  # SPY is also a comparator; the fund entry still counts
    assert build_census(funds, comparators).end == (2023, 6)
    funds, comparators = _universe(last=COVERAGE_CAP)
    assert build_census(funds, comparators).limiting[0] == "cap" and "TLT" in build_census(funds, comparators).limiting


def test_a_missing_month_after_the_first_panel_month_refuses_but_pre_history_does_not() -> None:
    funds, comparators = _universe()
    funds["XLC"] = _coverage("XLC", (2018, 7), (2026, 1))
    build_census(funds, comparators)
    gap = dict(funds["TLT"].returns)
    del gap[(2010, 3)]
    funds["TLT"] = FundCoverage("TLT", EtfVerdict.NPORT, (2000, 1), (2026, 1), gap)
    with pytest.raises(TsmomRefusal, match="missing_month"):
        build_census(funds, comparators)


def test_e_before_s_plus_two_and_a_never_eligible_fund_refuse() -> None:
    funds, comparators = _universe(first=(2023, 1), last=(2026, 1))
    funds["SPY"] = _coverage("SPY", (2023, 1), (2024, 1))
    with pytest.raises(TsmomRefusal, match="too_short"):
        build_census(funds, comparators)
    funds, comparators = _universe()
    funds["XLC"] = _coverage("XLC", (2023, 10), (2026, 1))
    with pytest.raises(TsmomRefusal, match="never_eligible"):
        build_census(funds, comparators)


def test_a_comparator_starting_after_the_first_reported_month_refuses() -> None:
    funds, comparators = _universe()
    comparators["VTI"] = _coverage("VTI", (2010, 1), (2026, 1))
    with pytest.raises(TsmomRefusal, match="comparator_late"):
        build_census(funds, comparators)


# --- signals, volatility, slots ----------------------------------------------------------------------------------


def test_the_tsmom_signal_compares_compounded_wealth_and_is_strict() -> None:
    rf = [0.0] * 12
    assert tsmom_signal([0.01] + [0.0] * 11, rf)
    assert not tsmom_signal([0.0] * 12, rf)  # equal wealth is cash
    assert not tsmom_signal([0.5, -0.4] + [0.0] * 10, rf)  # 1.5 × 0.6 = 0.9 < 1
    assert not tsmom_signal([0.01] * 12, [0.01] * 12)


def test_c4_uses_the_arithmetic_sum_and_holds_at_exactly_zero() -> None:
    assert c4_signal([0.5, -0.4] + [0.0] * 10, [0.0] * 12)  # Σ = +0.1, where TSMOM is cash
    assert c4_signal([0.02, -0.02], [0.0, 0.0])  # Σ = 0 → hold (Huang et al. Table 9: non-negative)
    assert not c4_signal([0.01, -0.02], [0.0, 0.0])


def test_volatility_is_the_sample_standard_deviation_times_root_twelve() -> None:
    returns = [0.01, -0.02, 0.03, 0.0, 0.015, -0.01, 0.02, 0.005, -0.03, 0.01, 0.0, 0.025]
    assert volatility(returns) == pytest.approx(statistics.stdev(returns) * math.sqrt(12))
    assert volatility(returns) != pytest.approx(statistics.pstdev(returns) * math.sqrt(12))


def test_slots_are_normalised_inverse_volatilities_and_refuse_a_zero_sigma() -> None:
    assert slots({"A": 0.10, "B": 0.20}) == pytest.approx({"A": 2 / 3, "B": 1 / 3})
    with pytest.raises(TsmomRefusal, match="invalid_volatility"):
        slots({"A": 0.10, "B": 0.0})
    with pytest.raises(TsmomRefusal, match="invalid_volatility"):
        slots({"A": math.nan})


def _wiggle(first: Month, last: Month, drift: float) -> dict[Month, float]:
    return {m: drift + (0.01 if i % 2 else -0.01) for i, m in enumerate(month_range(first, last))}


def test_a_fund_is_eligible_from_its_twelfth_month_and_c4_reads_its_whole_prefix() -> None:
    coverage = {
        "A": FundCoverage("A", EtfVerdict.NPORT, (2010, 1), (2012, 12), _wiggle((2010, 1), (2012, 12), 0.005)),
        "B": FundCoverage("B", EtfVerdict.NPORT, (2010, 6), (2012, 12), _wiggle((2010, 6), (2012, 12), -0.005)),
    }
    rf = {m: 0.0 for m in month_range((2009, 1), (2012, 12))}
    early = form((2010, 12), ["A", "B"], coverage, rf)
    assert early.eligible == ("A",) and early.slot == {"A": 1.0}
    later = form((2011, 5), ["A", "B"], coverage, rf)
    assert later.eligible == ("A", "B")
    assert later.tsmom == {"A": True, "B": False}
    assert sum(later.slot.values()) == pytest.approx(1.0)
    assert tsmom_weights(later) == {"A": later.slot["A"]}


def test_a_missing_rf_month_refuses_the_formation() -> None:
    coverage = {"A": FundCoverage("A", EtfVerdict.NPORT, (2010, 1), (2011, 12), _wiggle((2010, 1), (2011, 12), 0.0))}
    rf = {m: 0.0 for m in month_range((2010, 1), (2011, 12)) if m != (2010, 6)}
    with pytest.raises(TsmomRefusal, match="missing_month"):
        form((2010, 12), ["A"], coverage, rf)


# --- controls and samplers ---------------------------------------------------------------------------------------


def test_c1x_scales_every_slot_and_c3_keeps_full_book_slots() -> None:
    coverage = {
        s: FundCoverage(s, EtfVerdict.NPORT, (2010, 1), (2011, 12), _wiggle((2010, 1), (2011, 12), 0.0)) for s in "AB"
    }
    formation = form((2010, 12), ["A", "B"], coverage, {m: 0.0 for m in month_range((2010, 1), (2011, 12))})
    assert c1x_weights(formation, 0.4) == pytest.approx({s: w * 0.4 for s, w in formation.slot.items()})
    assert basket_weights(formation, ["B"]) == {"B": formation.slot["B"]}


def test_check_weights_refuses_negative_or_overfull_books() -> None:
    check_weights({"A": 0.5, "B": 0.5})
    with pytest.raises(TsmomRefusal):
        check_weights({"A": -0.1})
    with pytest.raises(TsmomRefusal):
        check_weights({"A": 0.7, "B": 0.4})


def test_c2_offsets_are_deterministic_and_a_degenerate_fund_draws_nothing() -> None:
    lengths = {"A": 10, "C": 20}
    first = c2_offsets("lagged", "all", 7, lengths)
    assert first == c2_offsets("lagged", "all", 7, lengths)
    assert all(1 <= first[s] < lengths[s] for s in lengths)
    with_degenerate = c2_offsets("lagged", "all", 7, {**lengths, "B": 1})
    assert with_degenerate["B"] == 0
    assert {s: with_degenerate[s] for s in lengths} == first  # B made no RNG call, so C's draw is unchanged


def test_circular_shift_keeps_the_on_count_and_shifts_right() -> None:
    sequence = [True, True, False, False, False]
    shifted = circular_shift(sequence, 2)
    assert shifted == [False, False, True, True, False]
    assert sum(shifted) == sum(sequence)


def test_c3_draws_k_funds_per_formation_deterministically() -> None:
    formations = [((2010, 2), ["C", "A", "B"], 2), ((2010, 1), ["A", "B"], 1)]
    baskets = c3_baskets("lagged", "all", 3, formations)
    assert baskets == c3_baskets("lagged", "all", 3, list(reversed(formations)))
    assert len(baskets[(2010, 2)]) == 2 and len(baskets[(2010, 1)]) == 1


def test_timing_and_evaluated_formations() -> None:
    assert fill_month((2010, 1), "lagged") == (2010, 2)
    assert fill_month((2010, 1), "same_close") == (2010, 1)
    assert evaluated_formations((2010, 1), (2010, 6), "lagged") == month_range((2010, 1), (2010, 4))
    assert evaluated_formations((2010, 1), (2010, 6), "same_close") == month_range((2010, 1), (2010, 5))


def test_baselines_buy_at_the_first_fill_and_rebalance_each_december_before_e() -> None:
    fills = baseline_fills({"SPY": 1.0}, (2005, 11), (2008, 3), "lagged")
    assert sorted(fills) == [(2005, 12), (2006, 12), (2007, 12)]
    assert sorted(baseline_fills({"SPY": 1.0}, (2005, 11), (2006, 12), "same_close")) == [(2005, 11), (2005, 12)]


# --- cost solve and path -----------------------------------------------------------------------------------------


def test_the_cost_solve_matches_the_closed_form_for_a_purchase_from_cash() -> None:
    h, w = 0.01, 0.6
    assert solve_cost({}, 1.0, {"A": w}, {"A": h}) == pytest.approx(h * w / (1 + h * w), abs=1e-15)


def test_the_cost_solve_charges_a_full_sale_of_a_dropped_position() -> None:
    assert solve_cost({"A": 0.5}, 0.5, {}, {"A": 0.02}) == pytest.approx(0.01, abs=1e-15)


def _flat(symbols: str, first: Month, last: Month, value: float) -> dict[str, dict[Month, float]]:
    return {s: {m: value for m in month_range(first, last)} for s in symbols}


def test_same_close_carries_the_opening_cost_into_the_first_reported_month() -> None:
    h = 0.005
    result = simulate(
        start=(2010, 1),
        end=(2010, 3),
        fills={(2010, 1): {"A": 1.0}},
        returns=_flat("A", (2010, 1), (2010, 3), 0.02),
        cash_return=None,
        entry_half_spread=lambda s, m: h,
        cost_multiplier=1.0,
    )
    after_buy = 1.0 / (1.0 + h)
    assert sorted(result.returns) == [(2010, 2), (2010, 3)]
    assert result.returns[(2010, 2)] == pytest.approx(after_buy * 1.02 - 1.0)
    final = after_buy * 1.02 * 1.02 * (1.0 - h)
    assert result.final_nav == pytest.approx(final)
    assert math.prod(1 + r for r in result.returns.values()) == pytest.approx(final)


def test_the_lagged_arm_holds_cash_until_its_first_fill_and_cash_can_earn_rf() -> None:
    kwargs = dict(
        start=(2010, 1),
        end=(2010, 4),
        fills={(2010, 2): {"A": 1.0}},
        returns=_flat("A", (2010, 1), (2010, 4), 0.02),
        entry_half_spread=lambda s, m: 0.0,
        cost_multiplier=1.0,
    )
    zero = simulate(cash_return=None, **kwargs)  # type: ignore[arg-type]
    assert zero.returns[(2010, 2)] == 0.0 and zero.returns[(2010, 3)] == pytest.approx(0.02)
    rf = simulate(cash_return={m: 0.001 for m in month_range((2010, 1), (2010, 4))}, **kwargs)  # type: ignore[arg-type]
    assert rf.returns[(2010, 2)] == pytest.approx(0.001)


def test_turnover_excludes_the_first_fill_and_the_liquidation_but_counts_later_trades() -> None:
    result = simulate(
        start=(2010, 1),
        end=(2010, 4),
        fills={(2010, 1): {"A": 1.0}, (2010, 2): {"A": 0.5}, (2010, 3): {"A": 0.5}},
        returns=_flat("A", (2010, 1), (2010, 4), 0.0),
        cash_return=None,
        entry_half_spread=lambda s, m: 0.0,
        cost_multiplier=1.0,
    )
    assert result.turnover == {2010: pytest.approx(0.25)}


def test_an_exit_clears_the_band_and_a_re_entry_takes_a_fresh_one() -> None:
    bands = {(2010, 1): 0.01, (2010, 3): 0.03}
    result = simulate(
        start=(2010, 1),
        end=(2010, 4),
        fills={(2010, 1): {"A": 1.0}, (2010, 2): {}, (2010, 3): {"A": 1.0}},
        returns=_flat("A", (2010, 1), (2010, 4), 0.0),
        cash_return=None,
        entry_half_spread=lambda s, m: bands.get(m, 0.02),
        cost_multiplier=1.0,
    )
    by_month = {(t.month, t.notional > 0): t.half_spread for t in result.trades}
    assert by_month[((2010, 2), False)] == 0.01  # the exit pays the entry band
    assert by_month[((2010, 3), True)] == 0.03  # the re-entry takes March's band
    assert by_month[((2010, 4), False)] == 0.03  # the liquidation pays the re-entry band


def test_the_gross_arm_charges_nothing_and_the_stress_arm_doubles_the_spread() -> None:
    kwargs = dict(
        start=(2010, 1),
        end=(2010, 2),
        fills={(2010, 1): {"A": 1.0}},
        returns=_flat("A", (2010, 1), (2010, 2), 0.0),
        cash_return=None,
        entry_half_spread=lambda s, m: 0.01,
    )
    assert simulate(cost_multiplier=0.0, **kwargs).final_nav == 1.0  # type: ignore[arg-type]
    stress = simulate(cost_multiplier=2.0, **kwargs)  # type: ignore[arg-type]
    assert stress.final_nav == pytest.approx((1 / 1.02) * 0.98)


def test_an_empty_fill_is_all_cash_and_fills_outside_the_path_refuse() -> None:
    result = simulate(
        start=(2010, 1),
        end=(2010, 3),
        fills={(2010, 1): {}},
        returns={},
        cash_return=None,
        entry_half_spread=lambda s, m: 0.01,
        cost_multiplier=1.0,
    )
    assert result.returns == {(2010, 2): 0.0, (2010, 3): 0.0}
    with pytest.raises(TsmomRefusal, match="fill_month"):
        simulate(
            start=(2010, 1),
            end=(2010, 3),
            fills={(2010, 3): {}},
            returns={},
            cash_return=None,
            entry_half_spread=lambda s, m: 0.0,
            cost_multiplier=1.0,
        )


def test_a_held_month_with_no_return_refuses() -> None:
    returns = _flat("A", (2010, 1), (2010, 3), 0.0)
    del returns["A"][(2010, 3)]
    with pytest.raises(TsmomRefusal, match="missing_month"):
        simulate(
            start=(2010, 1),
            end=(2010, 3),
            fills={(2010, 1): {"A": 1.0}},
            returns=returns,
            cash_return=None,
            entry_half_spread=lambda s, m: 0.0,
            cost_multiplier=1.0,
        )


# --- stamp screen ------------------------------------------------------------------------------------------------


def test_the_stamp_screen_flags_full_years_off_the_mode_and_never_partial_years() -> None:
    events = [date(y, m, 1) for y in range(2003, 2022) for m in range(1, 13) if not (y == 2019 and m > 9)]
    events = [d for d in events if d.year != 2002] + [date(2002, 11, 1)]
    full = {y: True for y in range(2002, 2022)} | {2002: False}
    audit = stamp_audit("SHY", events, full, (2021, 12))
    assert audit.status == "ok" and audit.mode == 12 and audit.flagged == (2019,)
    assert next(y for y in audit.years if y.year == 2002).full is False


def test_the_stamp_screen_counts_only_months_the_panel_uses_and_reports_thin_or_tied_history() -> None:
    full = {y: True for y in range(2018, 2025)}
    events = [date(y, 6, 1) for y in range(2018, 2025)]
    audit = stamp_audit("DBC", events, full, (2024, 8))
    assert next(y for y in audit.years if y.year == 2024).full is False  # last used month is not December
    assert stamp_audit("X", [], {2020: True, 2021: True}, (2021, 12)).status == "insufficient history"
    tied = [date(2018, 1, 1), date(2019, 1, 1), date(2019, 2, 1)]
    assert stamp_audit("X", tied, {2018: True, 2019: True, 2020: True, 2021: True}, (2021, 12)).status == "ok"
    tied_mode = [
        date(2018, 1, 1),
        date(2019, 1, 1),
        date(2020, 1, 1),
        date(2020, 2, 1),
        date(2021, 1, 1),
        date(2021, 2, 1),
    ]
    assert (
        stamp_audit("X", tied_mode, {2018: True, 2019: True, 2020: True, 2021: True}, (2021, 12)).status == "multimodal"
    )


def test_add_months_round_trip_used_by_the_eligibility_rule() -> None:
    assert add_months((2004, 12), 11) == (2005, 11)
