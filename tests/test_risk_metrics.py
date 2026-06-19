"""Pure-math tests for app.services.risk_metrics (risk_v1).

NO DB. Every test encodes a math contract from the #591 quant review.
Numbered references in comments map to the required-test-cases list.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services import risk_metrics as rm

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _closes(values, start=date(2024, 1, 1)):
    """Build a list of (date, close) with consecutive calendar days."""
    return [(start + timedelta(days=i), v) for i, v in enumerate(values)]


def _D(x) -> Decimal:
    return Decimal(str(x))


# ===========================================================================
# Group 1 — simple returns + chain breaking
# ===========================================================================


def test_clean_returns_exact_decimal():  # case 1
    closes = _closes([100, 110, 121])
    rets = rm.simple_returns(closes)
    assert [r for _, r in rets] == [Decimal("0.10"), Decimal("0.10")]
    # keyed to the LATER close's date
    assert [d for d, _ in rets] == [date(2024, 1, 2), date(2024, 1, 3)]


@pytest.mark.parametrize("bad", [float("nan"), 0, -5])
def test_mid_series_invalid_breaks_chain(bad):  # case 2
    # close[2] invalid -> no return into it AND no return out of it (gap span)
    closes = _closes([100, 110, bad, 121, 133.1])
    rets = rm.simple_returns(closes)
    # surviving consecutive pairs: (d1->d2)=0.10, then (d4->d5)=0.10
    # NO synthetic 110->121 across the invalid row.
    dates = [d for d, _ in rets]
    vals = [r for _, r in rets]
    assert dates == [date(2024, 1, 2), date(2024, 1, 5)]
    assert vals == [Decimal("0.10"), Decimal("0.10")]


def test_fewer_than_two_valid_returns_empty():  # case 3
    assert rm.simple_returns(_closes([100])) == []
    assert rm.simple_returns(_closes([float("nan"), 100])) == []
    assert rm.simple_returns([]) == []


# ===========================================================================
# Group 2 — sample std + annualized vol
# ===========================================================================


def test_sample_std_n_minus_1():
    # returns [0.1, 0.1] -> std = 0
    rets = [Decimal("0.1"), Decimal("0.1")]
    assert rm._sample_std(rets) == Decimal("0")


def test_sample_std_none_below_2():
    assert rm._sample_std([Decimal("0.1")]) is None
    assert rm._sample_std([]) is None


def test_vol_equals_hand_std_times_sqrt252():  # case 4
    rets = [Decimal("0.01"), Decimal("-0.01"), Decimal("0.02"), Decimal("-0.02")]
    # hand sample std (n-1):
    fl = [0.01, -0.01, 0.02, -0.02]
    mean = sum(fl) / len(fl)
    var = sum((x - mean) ** 2 for x in fl) / (len(fl) - 1)
    expected = Decimal(str(math.sqrt(var))) * Decimal(252).sqrt()
    got = rm.annualized_vol(rets)
    assert got is not None
    assert abs(got - expected) < Decimal("1e-12")


def test_vol_one_return_none():  # case 5
    assert rm.annualized_vol([Decimal("0.01")]) is None


def test_vol_std_matches_distribution_std():  # case 6
    rets = [Decimal("0.01"), Decimal("-0.02"), Decimal("0.03"), Decimal("-0.01"), Decimal("0.02")]
    std_helper = rm._sample_std(rets)
    vol = rm.annualized_vol(rets)
    assert vol is not None and std_helper is not None
    # vol = std * sqrt(252); divide back out and compare
    assert abs(vol / Decimal(252).sqrt() - std_helper) < Decimal("1e-15")


# ===========================================================================
# Group 3 — drawdown
# ===========================================================================


def test_drawdown_monotonic_up():  # case 7
    res = rm.drawdown(_closes([100, 110, 120, 130]))
    assert res.max_drawdown == Decimal("0")
    assert res.current_drawdown == Decimal("0")


def test_drawdown_v_shape():  # case 8
    closes = _closes([100, 120, 60, 90])
    res = rm.drawdown(closes)
    assert res.max_drawdown == Decimal("-0.5")  # 60/120 - 1
    assert res.current_drawdown == Decimal("-0.25")  # 90/120 - 1
    assert res.peak_date == date(2024, 1, 2)  # the 120
    assert res.trough_date == date(2024, 1, 3)  # the 60


def test_drawdown_open_current_equals_max():  # case 9
    closes = _closes([100, 120, 60])
    res = rm.drawdown(closes)
    assert res.current_drawdown == res.max_drawdown == Decimal("-0.5")


# ===========================================================================
# Group 4 — OLS beta (date-intersection)
# ===========================================================================


def test_beta_exact_2x():  # case 10
    # bench returns vary; inst = 2 * bench, same dates
    bench = _closes([100, 110, 121, 108.9])  # rets: .1, .1, -.1
    inst = _closes([100, 120, 144, 115.2])  # rets: .2, .2, -.2
    res = rm.ols_beta(rm.simple_returns(inst), rm.simple_returns(bench))
    assert res.beta is not None and abs(res.beta - Decimal("2")) < Decimal("1e-9")
    assert res.r2 is not None and abs(res.r2 - Decimal("1")) < Decimal("1e-9")
    assert res.n_obs == 3


def test_beta_half_with_hand_value():  # case 11
    # inst = 0.5*bench + noise; pick noise so it's hand-checkable
    # bench rets: m = [0.10, -0.10, 0.20, -0.20]
    # inst rets : i = [0.06, -0.04, 0.11, -0.09]  (0.5*m + [0.01,0.01,0.01,0.01])
    m = [0.10, -0.10, 0.20, -0.20]
    i = [0.5 * x + 0.01 for x in m]
    mb = sum(m) / len(m)
    ib = sum(i) / len(i)
    cov = sum((a - ib) * (b - mb) for a, b in zip(i, m, strict=True)) / (len(m) - 1)
    varm = sum((b - mb) ** 2 for b in m) / (len(m) - 1)
    expected_beta = cov / varm  # = 0.5 exactly
    dates = [date(2024, 2, 1) + timedelta(days=k) for k in range(4)]
    inst_rets = list(zip(dates, [_D(x) for x in i], strict=True))
    bench_rets = list(zip(dates, [_D(x) for x in m], strict=True))
    res = rm.ols_beta(inst_rets, bench_rets)
    assert res.beta is not None
    assert abs(res.beta - _D(expected_beta)) < Decimal("1e-9")
    assert abs(res.beta - Decimal("0.5")) < Decimal("1e-9")


def test_beta_flat_bench_none():  # case 12
    dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    bench_rets = list(zip(dates, [Decimal("0"), Decimal("0"), Decimal("0")], strict=True))
    inst_rets = list(zip(dates, [Decimal("0.1"), Decimal("-0.1"), Decimal("0.2")], strict=True))
    res = rm.ols_beta(inst_rets, bench_rets)
    assert res.beta is None
    assert res.r2 is None


def test_beta_date_misalignment_join_vs_zip():  # case 13
    # inst is missing the mid day that bench has.
    # bench rets keyed: d2:0.1, d3:-0.1, d4:0.2
    bd = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    bench_rets = list(zip(bd, [Decimal("0.1"), Decimal("-0.1"), Decimal("0.2")], strict=True))
    # inst has rets only on d2 and d4 (missing d3): inst = 2*bench on shared days
    inst_rets = [(date(2024, 1, 2), Decimal("0.2")), (date(2024, 1, 4), Decimal("0.4"))]
    res = rm.ols_beta(inst_rets, bench_rets)
    # date-join pairs: (0.2,0.1) and (0.4,0.2) -> perfect beta 2.0
    assert res.beta is not None and abs(res.beta - Decimal("2")) < Decimal("1e-9")
    assert res.n_obs == 2
    # positional-zip would have paired inst[1]=0.4 with bench[1]=-0.1 -> different.
    zip_pairs_m = [0.1, -0.1]
    zip_pairs_i = [0.2, 0.4]
    mb = sum(zip_pairs_m) / 2
    ib = sum(zip_pairs_i) / 2
    cov = sum((a - ib) * (b - mb) for a, b in zip(zip_pairs_i, zip_pairs_m, strict=True))
    varm = sum((b - mb) ** 2 for b in zip_pairs_m)
    zip_beta = cov / varm
    assert abs(_D(zip_beta) - Decimal("2")) > Decimal("0.1")  # proves they differ


def test_beta_fewer_than_two_pairs():  # case 14
    inst_rets = [(date(2024, 1, 2), Decimal("0.1"))]
    bench_rets = [(date(2024, 1, 2), Decimal("0.05"))]
    res = rm.ols_beta(inst_rets, bench_rets)
    assert res.beta is None
    assert res.r2 is None
    assert res.n_obs == 1


def test_beta_aligned_start_excludes_inst_only_history():  # case 15
    # inst has extra early returns bench lacks; aligned window = intersection.
    inst_rets = [
        (date(2024, 1, 2), Decimal("99")),  # inst-only, must be excluded
        (date(2024, 1, 3), Decimal("0.2")),
        (date(2024, 1, 4), Decimal("0.4")),
    ]
    bench_rets = [
        (date(2024, 1, 3), Decimal("0.1")),
        (date(2024, 1, 4), Decimal("0.2")),
    ]
    res = rm.ols_beta(inst_rets, bench_rets)
    assert res.n_obs == 2  # only the 2 shared dates
    assert res.beta is not None and abs(res.beta - Decimal("2")) < Decimal("1e-9")


# ===========================================================================
# Group 5 — distribution (float island)
# ===========================================================================


def test_var5_exact_type7_on_fixed_array():  # case 16
    # 20-element array, hand-compute type-7 5th percentile.
    arr = [
        -0.10,
        -0.08,
        -0.06,
        -0.05,
        -0.04,
        -0.03,
        -0.02,
        -0.01,
        0.00,
        0.01,
        0.02,
        0.03,
        0.04,
        0.05,
        0.06,
        0.07,
        0.08,
        0.09,
        0.10,
        0.12,
    ]
    rets = [_D(x) for x in arr]
    res = rm.distribution(rets)
    # type-7: h = 0.05*(n-1) = 0.05*19 = 0.95; sorted asc.
    s = sorted(arr)
    h = 0.05 * (len(s) - 1)
    lo = math.floor(h)
    expected = s[lo] + (h - lo) * (s[lo + 1] - s[lo])
    assert res.var_5 is not None
    assert abs(res.var_5 - Decimal(str(round(expected, 8)))) < Decimal("1e-8")
    # SIGNED: left-tail loss must be negative
    assert res.var_5 < Decimal("0")


def test_skew_symmetric_near_zero_and_right_skew_positive():  # case 17
    sym = [_D(x) for x in [-0.02, -0.01, 0.0, 0.01, 0.02]]
    res_sym = rm.distribution(sym)
    assert res_sym.skew is not None and abs(res_sym.skew) < Decimal("1e-6")
    right = [_D(x) for x in [-0.01, -0.01, -0.01, -0.01, 0.10]]
    res_right = rm.distribution(right)
    assert res_right.skew is not None and res_right.skew > Decimal("0")


def test_kurtosis_heavy_normal_and_constant():  # case 18
    heavy = [_D(x) for x in ([0.0] * 8 + [-0.5, 0.5])]
    res_heavy = rm.distribution(heavy)
    assert res_heavy.excess_kurtosis is not None and res_heavy.excess_kurtosis > Decimal("0")
    constant = [_D("0.01")] * 10
    res_const = rm.distribution(constant)
    assert res_const.skew is None
    assert res_const.excess_kurtosis is None


def test_low_sample_flag_boundary():  # case 19
    n_low = [_D("0.0") if k % 2 else _D("0.01") for k in range(249)]
    assert rm.distribution(n_low).low_sample is True
    n_ok = [_D("0.0") if k % 2 else _D("0.01") for k in range(250)]
    assert rm.distribution(n_ok).low_sample is False


def test_worst_best_day():  # case 20
    rets = [_D("0.03"), _D("-0.05"), _D("0.01"), _D("0.07"), _D("-0.02")]
    res = rm.distribution(rets)
    assert res.worst_day == Decimal("-0.05")
    assert res.best_day == Decimal("0.07")
    assert res.n_obs == 5


# ===========================================================================
# Group 6 — cagr (calendar-time)
# ===========================================================================


def test_cagr_double_over_365_days():  # case 21
    closes = [(date(2024, 1, 1), 100.0), (date(2024, 12, 31), 200.0)]
    # calendar_days = 365 -> (200/100)^(365/365) - 1 = 1.0
    got = rm.cagr(closes)
    assert got is not None and abs(got - Decimal("1")) < Decimal("1e-9")


def test_cagr_same_total_return_diff_gap_counts_same():  # case 22 anti-regression
    # both: 100 -> 150 over the SAME calendar span, different #observations.
    span_days = 200
    sparse = [(date(2024, 1, 1), 100.0), (date(2024, 1, 1) + timedelta(days=span_days), 150.0)]
    dense = [
        (date(2024, 1, 1), 100.0),
        (date(2024, 1, 1) + timedelta(days=50), 120.0),
        (date(2024, 1, 1) + timedelta(days=120), 110.0),
        (date(2024, 1, 1) + timedelta(days=span_days), 150.0),
    ]
    a = rm.cagr(sparse)
    b = rm.cagr(dense)
    assert a is not None and b is not None
    assert abs(a - b) < Decimal("1e-9")  # proves calendar-time, not 252/n_returns


def test_cagr_calendar_days_zero_none():  # case 26
    closes = [(date(2024, 1, 1), 100.0), (date(2024, 1, 1), 200.0)]
    assert rm.cagr(closes) is None


# ===========================================================================
# Group 7 — calmar
# ===========================================================================


def test_calmar_known_fixture():  # case 23
    got = rm.calmar(Decimal("0.30"), Decimal("-0.15"))
    assert got is not None and got == Decimal("2")


def test_calmar_tiny_dd_none():  # case 24
    assert rm.calmar(Decimal("0.30"), Decimal("-1e-12")) is None
    assert rm.calmar(Decimal("0.30"), Decimal("0")) is None


# ===========================================================================
# Group 8 — trailing return + excess
# ===========================================================================


def test_trailing_return_basic():
    base = date(2024, 6, 1)
    closes = [
        (base - timedelta(days=40), 100.0),
        (base - timedelta(days=35), 105.0),
        (base, 130.0),
    ]
    # lookback 30d: as_of - 30 = base-30; nearest valid <= that is base-35 (105)
    got = rm.trailing_return(closes, base, 30)
    assert got is not None and abs(got - (Decimal("130") / Decimal("105") - 1)) < Decimal("1e-12")


def test_trailing_return_none_when_no_history():
    base = date(2024, 6, 1)
    closes = [(base - timedelta(days=5), 100.0), (base, 110.0)]
    assert rm.trailing_return(closes, base, 365) is None


def test_excess_trailing_benchmark_missing():
    base = date(2024, 6, 1)
    closes = [(base - timedelta(days=40), 100.0), (base, 130.0)]
    val, status = rm.excess_trailing_return(closes, [], base, 30)
    assert val is None
    assert status == "benchmark_missing"


# ===========================================================================
# Group 9 — excess cagr
# ===========================================================================


def test_excess_cagr_first_class():
    inst = [(date(2024, 1, 1), 100.0), (date(2024, 12, 31), 200.0)]  # cagr 1.0
    spy = [(date(2024, 1, 1), 100.0), (date(2024, 12, 31), 150.0)]  # cagr 0.5
    val, status = rm.excess_cagr(inst, spy, "1y")
    assert status == "ok"
    assert val is not None and abs(val - Decimal("0.5")) < Decimal("1e-9")


def test_excess_cagr_benchmark_missing():
    inst = [(date(2024, 1, 1), 100.0), (date(2024, 12, 31), 200.0)]
    val, status = rm.excess_cagr(inst, [], "1y")
    assert val is None
    assert status == "benchmark_missing"


# ===========================================================================
# Group 10 — boundaries / status
# ===========================================================================


def test_partial_window_cagr_boundary_251_vs_252():  # case 25
    # 252 returns => not partial; 251 => partial.
    closes_252 = _closes([100.0 + k for k in range(253)])  # 252 returns
    closes_251 = _closes([100.0 + k for k in range(252)])  # 251 returns
    assert rm.annualized_status(252) == "ok"
    assert rm.annualized_status(251) == "partial_window"
    # plumbed through compute
    wm252 = rm.compute_instrument_risk(closes_252, [], "full", closes_252[-1][0])
    wm251 = rm.compute_instrument_risk(closes_251, [], "full", closes_251[-1][0])
    assert wm252.cagr_status == "ok"
    assert wm251.cagr_status == "partial_window"


def test_vol_beta_boundary_60_vs_59_and_midgap():  # case 27
    # exactly 60 returns => pass; 59 => fail.
    closes_61 = _closes([100.0 + k for k in range(61)])  # 60 returns
    assert rm.vol_beta_status(60) == "ok"
    assert rm.vol_beta_status(59) == "insufficient_history"
    # 61 calendar days with one missing-date row => 60 rows => 59 returns => fails.
    # (A missing-date gap drops exactly one return; an invalid-close gap breaks
    #  the chain and drops two — that case is covered by
    #  test_mid_series_invalid_breaks_chain.)
    full = _closes([100.0 + k for k in range(61)])  # 61 dated rows, 60 returns
    closes_gap = full[:30] + full[31:]  # drop the row at index 30
    rets_gap = rm.simple_returns(closes_gap)
    assert len(rets_gap) == 59
    assert rm.vol_beta_status(len(rets_gap)) == "insufficient_history"
    # sanity: clean 60-return chain
    assert len(rm.simple_returns(closes_61)) == 60
    # and an invalid-close mid-gap breaks the chain => 58 returns (also fails)
    vals = [100.0 + k for k in range(61)]
    vals[30] = float("nan")
    rets_broken = rm.simple_returns(_closes(vals))
    assert len(rets_broken) == 58
    assert rm.vol_beta_status(len(rets_broken)) == "insufficient_history"


# ===========================================================================
# Group 13 — invalid_price_chain status (invalids caused the shortfall)
# ===========================================================================


def test_count_invalid_closes():
    # 5 rows, two invalid (nan and 0); rest valid.
    closes = _closes([100.0, float("nan"), 110.0, 0, 121.0])
    assert rm._count_invalid_closes(closes) == 2
    # None entries also count as invalid
    assert rm._count_invalid_closes([(date(2024, 1, 1), None), (date(2024, 1, 2), 100.0)]) == 1
    # all-clean => 0
    assert rm._count_invalid_closes(_closes([100.0, 101.0, 102.0])) == 0


def test_status_helpers_invalid_price_chain_trigger():
    # below min-obs AND invalids dropped => invalid_price_chain
    assert rm.vol_beta_status(59, invalids_dropped=3) == "invalid_price_chain"
    assert rm.annualized_status(251, invalids_dropped=5) == "invalid_price_chain"
    # below min-obs but NO invalids => genuine short history
    assert rm.vol_beta_status(59, invalids_dropped=0) == "insufficient_history"
    assert rm.annualized_status(251, invalids_dropped=0) == "partial_window"
    # meets min-obs even with invalids dropped earlier => ok (single break is fine)
    assert rm.vol_beta_status(60, invalids_dropped=10) == "ok"
    assert rm.annualized_status(252, invalids_dropped=10) == "ok"
    # default invalids_dropped=0 preserves the old single-arg call shape
    assert rm.vol_beta_status(60) == "ok"
    assert rm.annualized_status(252) == "ok"


def test_compute_invalid_price_chain_drops_vol_below_threshold():  # case (a)
    # 70 dated rows; sprinkle enough invalids that valid returns < 60.
    # Each invalid close breaks the chain and costs 2 returns (in + out).
    vals = [100.0 + k for k in range(70)]
    # 6 invalids at spaced positions => removes ~12 returns from the clean 69.
    for idx in (10, 20, 30, 40, 50, 60):
        vals[idx] = float("nan")
    closes = _closes(vals)
    rets = rm.simple_returns(closes)
    assert len(rets) < rm.MIN_RETURNS_VOL_BETA  # invalids drove it under threshold
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.vol_status == "invalid_price_chain"


def test_compute_single_invalid_with_enough_remaining_is_ok():  # case (b)
    # 70 dated rows, one mid-chain invalid. Even with the 2-return cost,
    # >= 60 valid returns remain => vol_status stays ok despite an invalid present.
    vals = [100.0 + k for k in range(70)]
    vals[35] = float("nan")
    closes = _closes(vals)
    rets = rm.simple_returns(closes)
    assert len(rets) >= rm.MIN_RETURNS_VOL_BETA
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.vol_status == "ok"


def test_compute_short_clean_series_is_insufficient_history():  # case (c)
    # genuinely-short clean series (< 60 returns, no invalids) => insufficient_history.
    closes = _closes([100.0 + k for k in range(40)])  # 39 returns, all valid
    assert rm._count_invalid_closes(closes) == 0
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.vol_status == "insufficient_history"


# ===========================================================================
# Group 14 — drawdown_status / distribution_status / trailing_status
# (every persisted status column has a compute-layer source)
# ===========================================================================


def test_drawdown_status_ok_two_valid_closes():
    # ≥ 2 valid closes => a drawdown is computable => ok.
    assert rm.drawdown_status(2) == "ok"
    closes = _closes([100.0, 90.0])
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.drawdown_status == "ok"


def test_drawdown_status_insufficient_below_two_closes():
    # < 2 valid closes, no invalids => insufficient_history.
    assert rm.drawdown_status(1) == "insufficient_history"
    closes = _closes([100.0])  # one clean close, no invalids
    assert rm._count_invalid_closes(closes) == 0
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.drawdown_status == "insufficient_history"


def test_drawdown_status_invalid_price_chain():
    # invalids dropped the usable chain below 2 => invalid_price_chain.
    assert rm.drawdown_status(1, invalids_dropped=2) == "invalid_price_chain"
    # one valid close + two invalids => valid chain = 1 (< 2), invalids present.
    closes = _closes([100.0, float("nan"), 0])
    assert rm._count_invalid_closes(closes) == 2
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.drawdown_status == "invalid_price_chain"


def test_distribution_status_ok_at_min_obs():
    # >= MIN_OBS_MOMENTS (250) returns => ok.
    assert rm.distribution_status(rm.MIN_OBS_MOMENTS) == "ok"
    closes = _closes([100.0 + k for k in range(rm.MIN_OBS_MOMENTS + 1)])  # 250 returns
    rets = rm.simple_returns(closes)
    assert len(rets) == rm.MIN_OBS_MOMENTS
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.distribution_status == "ok"
    assert wm.distribution is not None and wm.distribution.low_sample is False


def test_distribution_status_partial_window_low_sample():
    # 2 <= n < 250 returns => partial_window (mirrors low_sample flag).
    assert rm.distribution_status(2) == "partial_window"
    assert rm.distribution_status(rm.MIN_OBS_MOMENTS - 1) == "partial_window"
    closes = _closes([100.0 + k for k in range(50)])  # 49 returns
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.distribution_status == "partial_window"
    assert wm.distribution is not None and wm.distribution.low_sample is True


def test_distribution_status_insufficient_below_two_returns():
    # < 2 returns, no invalids => insufficient_history.
    assert rm.distribution_status(1) == "insufficient_history"
    closes = _closes([100.0, 110.0])  # exactly 1 return
    assert rm._count_invalid_closes(closes) == 0
    wm = rm.compute_instrument_risk(closes, [], "full", closes[-1][0])
    assert wm.distribution_status == "insufficient_history"
    # invalid-driven path: < 2 returns AND invalids => invalid_price_chain.
    assert rm.distribution_status(1, invalids_dropped=3) == "invalid_price_chain"


def test_trailing_status_ok_when_shortest_window_computable():
    base = date(2024, 6, 1)
    closes = [
        (base - timedelta(days=40), 100.0),
        (base, 130.0),
    ]
    # a close ≥ 30 calendar days back exists => 1m trailing computable => ok.
    assert rm.trailing_status(closes, base) == "ok"
    wm = rm.compute_instrument_risk(closes, [], "full", base)
    assert wm.trailing_status == "ok"


def test_trailing_status_insufficient_when_series_shorter_than_30d():
    base = date(2024, 6, 1)
    closes = [
        (base - timedelta(days=5), 100.0),
        (base, 110.0),
    ]
    # no close ≥ 30 days back => not even 1m computable => insufficient_history.
    assert rm.trailing_return(closes, base, 30) is None
    assert rm.trailing_status(closes, base) == "insufficient_history"
    wm = rm.compute_instrument_risk(closes, [], "full", base)
    assert wm.trailing_status == "insufficient_history"


# ===========================================================================
# Group 11 — float island doesn't leak
# ===========================================================================


def test_persisted_distribution_scalars_are_decimal():  # case 28
    arr = [
        -0.10,
        -0.08,
        -0.06,
        -0.05,
        -0.04,
        -0.03,
        -0.02,
        -0.01,
        0.00,
        0.01,
        0.02,
        0.03,
        0.04,
        0.05,
        0.06,
        0.07,
        0.08,
        0.09,
        0.10,
        0.12,
    ]
    res = rm.distribution([_D(x) for x in arr])
    for v in (res.skew, res.excess_kurtosis, res.var_5):
        assert isinstance(v, Decimal)
        # equals Decimal(str(round(float, 8))) form — 8dp quantization
        assert v == Decimal(str(round(float(v), 8)))


# ===========================================================================
# Group 12 — orchestration smoke
# ===========================================================================


def test_compute_instrument_risk_full_window():
    closes = _closes([100.0 + k for k in range(300)])
    spy = _closes([100.0 + 0.5 * k for k in range(300)])
    wm = rm.compute_instrument_risk(closes, spy, "full", closes[-1][0])
    assert wm.window_key == "full"
    assert wm.annualized_vol is not None
    assert wm.cagr is not None
    assert wm.max_drawdown is not None
    assert wm.beta is not None
    assert wm.distribution is not None


# ===========================================================================
# Group 15 — BUG A: window_key actually slices the series
# ===========================================================================


def _three_year_series():
    """~3y daily series whose LAST 1y has a DIFFERENT mean/vol than the whole.

    First ~2 years: tiny low-vol uptrend. Last ~1 year: large high-vol swings.
    The 1y sub-window therefore has materially different vol/cagr than 3y/full.
    """
    import random

    rng = random.Random(1234)
    start = date(2021, 1, 1)
    closes: list = []
    price = 100.0
    # 730 days of low-vol drift
    for i in range(730):
        price *= 1.0 + rng.uniform(-0.001, 0.0015)
        closes.append((start + timedelta(days=i), price))
    # 365 days of high-vol, sharply different regime
    for i in range(730, 1095):
        price *= 1.0 + rng.uniform(-0.05, 0.06)
        closes.append((start + timedelta(days=i), price))
    return closes


def test_slice_window_keeps_only_lookback_span():
    closes = _three_year_series()
    as_of = closes[-1][0]
    full = rm._slice_window(closes, as_of, "full")
    one_y = rm._slice_window(closes, as_of, "1y")
    three_y = rm._slice_window(closes, as_of, "3y")
    # full keeps everything
    assert len(full) == len(closes)
    # 1y is a strict subset and shorter than 3y/full
    assert len(one_y) < len(three_y) <= len(full)
    # every 1y row is within 365 calendar days of as_of
    cutoff = as_of - timedelta(days=365)
    assert all(d >= cutoff for d, _ in one_y)
    assert all(d <= as_of for d, _ in one_y)
    # the earliest full row predates the 1y cutoff (proves slicing dropped rows)
    assert full[0][0] < cutoff


def test_window_key_changes_vol_and_cagr():  # BUG A core
    closes = _three_year_series()
    spy = _closes([400.0 + 0.2 * k for k in range(len(closes))], start=closes[0][0])
    as_of = closes[-1][0]
    wm_1y = rm.compute_instrument_risk(closes, spy, "1y", as_of)
    wm_3y = rm.compute_instrument_risk(closes, spy, "3y", as_of)
    wm_full = rm.compute_instrument_risk(closes, spy, "full", as_of)

    # vol of the high-vol last year must differ from the 3y/full blend.
    assert wm_1y.annualized_vol is not None
    assert wm_3y.annualized_vol is not None
    assert wm_full.annualized_vol is not None
    assert wm_1y.annualized_vol != wm_3y.annualized_vol
    assert wm_1y.annualized_vol != wm_full.annualized_vol
    # cagr likewise differs across windows.
    assert wm_1y.cagr is not None and wm_3y.cagr is not None and wm_full.cagr is not None
    assert wm_1y.cagr != wm_3y.cagr
    assert wm_1y.cagr != wm_full.cagr
    # n_returns reflects the sliced window (1y has far fewer returns than full).
    assert wm_1y.n_returns < wm_full.n_returns


def test_full_window_uses_all_closes():
    closes = _three_year_series()
    as_of = closes[-1][0]
    wm_full = rm.compute_instrument_risk(closes, [], "full", as_of)
    # full == compute on the whole series directly (no lower bound).
    all_rets = rm.simple_returns(closes)
    assert wm_full.n_returns == len(all_rets)
    assert wm_full.annualized_vol == rm.annualized_vol([r for _, r in all_rets])
    assert wm_full.cagr == rm.cagr(closes)


def test_window_slice_below_min_returns_flags():
    # A 1y window holding < MIN_RETURNS_VOL_BETA (60) clean returns flags
    # insufficient_history on vol even when the full series is long.
    start = date(2021, 1, 1)
    closes: list = []
    # 800 daily closes for ~2y of dense history (full window is long)...
    for i in range(800):
        closes.append((start + timedelta(days=i), 100.0 + 0.1 * i))
    # ...then sparse monthly closes for the last year so the 1y slice is thin.
    last = closes[-1][0]
    sparse: list = []
    for k in range(1, 12):
        sparse.append((last + timedelta(days=30 * k), 200.0 + k))
    closes = closes + sparse
    as_of = closes[-1][0]
    wm_1y = rm.compute_instrument_risk(closes, [], "1y", as_of)
    wm_full = rm.compute_instrument_risk(closes, [], "full", as_of)
    assert wm_1y.n_returns < rm.MIN_RETURNS_VOL_BETA
    assert wm_1y.vol_status == "insufficient_history"
    # full window has plenty of returns → ok.
    assert wm_full.n_returns >= rm.MIN_RETURNS_VOL_BETA
    assert wm_full.vol_status == "ok"


def test_excess_cagr_and_beta_reflect_window():
    closes = _three_year_series()
    # SPY tracks inst loosely; use a distinct shape so beta differs by window.
    spy_vals = [400.0 * (1.0 + 0.0005 * k) for k in range(len(closes))]
    spy = [(d, v) for (d, _), v in zip(closes, spy_vals, strict=True)]
    as_of = closes[-1][0]
    wm_1y = rm.compute_instrument_risk(closes, spy, "1y", as_of)
    wm_full = rm.compute_instrument_risk(closes, spy, "full", as_of)
    # excess_cagr is window-relative: the high-vol last year vs the blended full.
    assert wm_1y.excess_cagr is not None and wm_full.excess_cagr is not None
    assert wm_1y.excess_cagr != wm_full.excess_cagr
    # beta over the volatile 1y differs from the full-history beta.
    assert wm_1y.beta is not None and wm_full.beta is not None
    assert wm_1y.beta != wm_full.beta


# ===========================================================================
# Group 16 — BUG B: trailing-return scalars are emitted on WindowMetrics
# ===========================================================================


def test_trailing_scalars_populated_and_match_helper():
    base = date(2024, 6, 1)
    # dense enough that all four lookbacks resolve.
    closes = [(base - timedelta(days=400 - k), 100.0 + 0.2 * k) for k in range(401)]
    spy = [(base - timedelta(days=400 - k), 400.0 + 0.1 * k) for k in range(401)]
    wm = rm.compute_instrument_risk(closes, spy, "1y", base)
    for key, lookback in rm.TRAILING_LOOKBACK_DAYS.items():
        expected = rm.trailing_return(closes, base, lookback)
        got = getattr(wm, f"trailing_{key}")
        assert got is not None
        assert got == expected
        # excess trailing == inst trailing minus spy trailing.
        xs_expected, _ = rm.excess_trailing_return(closes, spy, base, lookback)
        xs_got = getattr(wm, f"excess_trailing_{key}")
        assert xs_got == xs_expected


def test_excess_trailing_null_when_no_spy():
    base = date(2024, 6, 1)
    closes = [(base - timedelta(days=400 - k), 100.0 + 0.2 * k) for k in range(401)]
    wm = rm.compute_instrument_risk(closes, [], "full", base)
    # inst trailing still populated...
    assert wm.trailing_1m is not None
    # ...but excess trailing is null with no benchmark.
    assert wm.excess_trailing_1m is None
    assert wm.excess_trailing_3m is None
    assert wm.excess_trailing_6m is None
    assert wm.excess_trailing_1y is None


def test_trailing_scalars_window_independent():
    # Trailing returns are calendar-lookback from as_of → identical across
    # 1y / 3y / full window rows (intentional redundancy).
    closes = _three_year_series()
    as_of = closes[-1][0]
    wm_1y = rm.compute_instrument_risk(closes, [], "1y", as_of)
    wm_3y = rm.compute_instrument_risk(closes, [], "3y", as_of)
    wm_full = rm.compute_instrument_risk(closes, [], "full", as_of)
    for key in rm.TRAILING_LOOKBACK_DAYS:
        v1 = getattr(wm_1y, f"trailing_{key}")
        v3 = getattr(wm_3y, f"trailing_{key}")
        vf = getattr(wm_full, f"trailing_{key}")
        assert v1 == v3 == vf


# ===========================================================================
# Group 18 — sector-relative beta/excess (#1674)
# A SECOND OLS + excess-CAGR vs the instrument's sector SPDR, threaded through
# compute_instrument_risk's `sector_closes` arg. Statuses reuse beta_status
# (>=60 aligned) + excess_cagr's own status — NOT re-derived.
# ===========================================================================


def test_sector_beta_ok_and_independent_of_spy():
    # 70 closes (69 returns >= 60). sector = 2x the inst series => identical
    # returns => sector_beta == 1, r2 == 1, status ok. SPY is ABSENT ([]) — the
    # sector block must still compute (proves it does not depend on SPY).
    inst = _closes([100.0 + k for k in range(70)])
    sector = _closes([(100.0 + k) * 2 for k in range(70)])
    wm = rm.compute_instrument_risk(inst, [], "full", inst[-1][0], sector)
    # SPY absent -> SPY beta benchmark_missing; sector still ok and computed.
    assert wm.beta is None
    assert wm.beta_status == "benchmark_missing"
    assert wm.sector_beta_status == "ok"
    assert wm.sector_beta is not None and abs(wm.sector_beta - Decimal("1")) < Decimal("1e-9")
    assert wm.sector_r2 is not None and abs(wm.sector_r2 - Decimal("1")) < Decimal("1e-9")
    assert wm.sector_beta_n_obs == 69
    assert wm.sector_excess_cagr_status == "ok"


def test_sector_beta_matches_direct_ols_and_differs_from_spy():
    # Distinct inst / spy / sector series: the sector beta must use the SECTOR
    # series (== a direct ols_beta on it), and differ from the SPY beta.
    inst = _closes([100.0 + k for k in range(70)])
    spy = _closes([100.0 + 2 * k for k in range(70)])
    sector = _closes([100.0 * (1.01**k) for k in range(70)])
    wm = rm.compute_instrument_risk(inst, spy, "full", inst[-1][0], sector)
    direct = rm.ols_beta(rm.simple_returns(inst), rm.simple_returns(sector))
    assert wm.sector_beta == direct.beta  # exact: same inputs, same fn
    assert wm.beta is not None and wm.sector_beta is not None
    assert wm.beta != wm.sector_beta  # different benchmark series
    assert wm.beta_status == "ok" and wm.sector_beta_status == "ok"


def test_sector_unresolved_is_benchmark_missing():
    # No sector series (instrument has no resolvable sector) => sector fields NULL
    # + benchmark_missing for BOTH the beta and the excess-CAGR status.
    inst = _closes([100.0 + k for k in range(70)])
    wm = rm.compute_instrument_risk(inst, [], "full", inst[-1][0], ())
    assert wm.sector_beta is None
    assert wm.sector_r2 is None
    assert wm.sector_beta_status == "benchmark_missing"
    assert wm.sector_excess_cagr is None
    assert wm.sector_excess_cagr_status == "benchmark_missing"


def test_sector_beta_insufficient_history_below_60():
    # Sector series present but the aligned window has < MIN_RETURNS_VOL_BETA (60)
    # pairs => benchmark_insufficient_history (the real threshold, NOT 2).
    inst = _closes([100.0 + k for k in range(40)])  # 39 returns
    sector = _closes([(100.0 + k) * 2 for k in range(40)])
    wm = rm.compute_instrument_risk(inst, [], "full", inst[-1][0], sector)
    assert wm.sector_beta_n_obs < rm.MIN_RETURNS_VOL_BETA
    assert wm.sector_beta_status == "benchmark_insufficient_history"
