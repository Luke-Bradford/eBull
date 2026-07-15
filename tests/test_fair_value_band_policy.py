import datetime as _d
from typing import Any

import pytest

from app.services.fair_value_band import (
    _SCREEN_TIERS,
    MIN_OWN_POINTS,
    CompanionVars,
    OwnPct,
    PeerPct,
    QualityInputs,
    TargetInputs,
    _shape_fair_value_band,
    band_quality_status,
    cap_envelope,
    combine_across,
    companion_vars,
    compute_band,
    compute_divergence,
    currency_coherent,
    filter_dual_class,
    own_range,
    percentiles,
    screen_passes,
    select_multiples,
    synth_multiple,
    target_screenable,
    to_per_share,
)


def _t(**kw: Any) -> TargetInputs:
    base: dict[str, Any] = dict(
        eps_diluted_ttm=None,
        revenue_ttm=None,
        shareholders_equity=None,
        net_income_ttm=None,
        shares_outstanding=1_000.0,
        sic="3571",
        reported_currency="USD",
        instrument_currency="USD",
        target_basis="not_multiclass",
    )
    base.update(kw)
    return TargetInputs(**base)


def test_financial_selects_pb_and_pe():
    # SIC 6021 (national commercial bank) -> financial gate first.
    t = _t(sic="6021", eps_diluted_ttm=2.0, shareholders_equity=5_000.0, revenue_ttm=9_000.0)
    assert select_multiples(t) == ["pb", "pe"]


def test_profitable_nonfinancial_selects_pe_and_ps():
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=2.0, revenue_ttm=9_000.0)
    assert select_multiples(t) == ["pe", "ps"]


def test_revenue_only_selects_ps():
    t = _t(net_income_ttm=-10.0, revenue_ttm=9_000.0, eps_diluted_ttm=-1.0)
    assert select_multiples(t) == ["ps"]


def test_none_computable_empty():
    t = _t(net_income_ttm=None, revenue_ttm=0.0, eps_diluted_ttm=0.0, shareholders_equity=0.0)
    assert select_multiples(t) == []


def test_dual_class_target_intersects_to_pe_only():
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=2.0, revenue_ttm=9_000.0, target_basis="total_company")
    assert select_multiples(t) == ["pe"]


def test_dual_class_financial_keeps_pe_drops_pb():
    t = _t(sic="6021", eps_diluted_ttm=2.0, shareholders_equity=5_000.0, target_basis="total_company")
    assert select_multiples(t) == ["pe"]


def test_eligibility_gate_drops_multiple_with_nonpositive_denominator():
    # profitable but eps not positive -> pe dropped, ps kept
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=0.0, revenue_ttm=9_000.0)
    assert select_multiples(t) == ["ps"]


def test_percentiles_match_postgres_continuous():
    # percentile_cont semantics: linear interpolation between closest ranks.
    vals = [10.0, 20.0, 30.0, 40.0]
    assert percentiles(vals, (0.25, 0.5, 0.75)) == [17.5, 25.0, 32.5]


def test_percentiles_single_value():
    assert percentiles([42.0], (0.2, 0.5, 0.8)) == [42.0, 42.0, 42.0]


def test_percentiles_zero_variance():
    assert percentiles([5.0, 5.0, 5.0], (0.25, 0.5, 0.75)) == [5.0, 5.0, 5.0]


def test_currency_coherent():
    assert currency_coherent("USD", "USD") is True
    assert currency_coherent("EUR", "USD") is False
    assert currency_coherent(None, "USD") is False


def test_synth_blend_and_envelope_both_present():
    peer = PeerPct(p25=10.0, p50=20.0, p75=30.0)
    own = OwnPct(p25=12.0, p50=24.0, p75=28.0)
    result = synth_multiple(peer, own)
    assert result is not None
    low, base, high = result
    assert base == 22.0  # mean(20, 24)
    assert low == 10.0  # min(peer_p25=10, own_p25=12)
    assert high == 30.0  # max(peer_p75=30, own_p75=28)


def test_synth_degrades_to_peer_only():
    peer = PeerPct(p25=10.0, p50=20.0, p75=30.0)
    own = OwnPct(p25=None, p50=None, p75=None)
    assert synth_multiple(peer, own) == (10.0, 20.0, 30.0)


def test_synth_degrades_to_own_only():
    peer = PeerPct(p25=None, p50=None, p75=None)
    own = OwnPct(p25=12.0, p50=24.0, p75=28.0)
    assert synth_multiple(peer, own) == (12.0, 24.0, 28.0)


def test_synth_none_when_neither():
    assert synth_multiple(PeerPct(None, None, None), OwnPct(None, None, None)) is None


def test_to_per_share_pe():
    assert to_per_share("pe", 30.0, 34.0, 37.0, eps=8.0, revenue=None, shareholders_equity=None, shares=None) == (
        240.0,
        272.0,
        296.0,
    )


def test_to_per_share_ps():
    # revenue 9000 / shares 1000 = 9 rev/share
    assert to_per_share("ps", 1.0, 2.0, 3.0, eps=None, revenue=9000.0, shareholders_equity=None, shares=1000.0) == (
        9.0,
        18.0,
        27.0,
    )


def test_to_per_share_pb():
    # equity 5000 / shares 1000 = 5 book/share
    assert to_per_share("pb", 1.0, 2.0, 3.0, eps=None, revenue=None, shareholders_equity=5000.0, shares=1000.0) == (
        5.0,
        10.0,
        15.0,
    )


def test_combine_across_median_and_envelope():
    # two multiples' per-share triples
    triples = [(240.0, 272.0, 296.0), (250.0, 260.0, 300.0)]
    bear, base, bull = combine_across(triples)
    assert base == 266.0  # median([272, 260]) = mean = 266
    assert bear == 240.0  # min lows
    assert bull == 300.0  # max highs


def test_own_range_below_min_points_absent():
    assert own_range([10.0] * (MIN_OWN_POINTS - 1)) == OwnPct(None, None, None)


def test_own_range_drops_nonpositive():
    # 6 positive, 3 non-positive -> uses the 6 positive
    vals = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, -1.0, 0.0, -5.0]
    r = own_range(vals)
    assert r.p50 == 35.0


def test_own_range_interior_quantiles_v2():
    # v2 (#2022): own_range emits p25/p50/p75 (interior), not p20/p80.
    r = own_range([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0])
    assert r.p25 == 27.5  # continuous p25 of 8 points
    assert r.p50 == 45.0
    assert r.p75 == 62.5


# --- v2 (#2022) envelope-ratio cap (cap_envelope) ---


def test_cap_envelope_clamps_high():
    # ps R_UP=3.1: high 40 > base 10 * 3.1 = 31 -> clamped; low 5 within -> untouched.
    low, high, capped_low, capped_high = cap_envelope("ps", 5.0, 10.0, 40.0)
    assert (low, high) == (5.0, 31.0)
    assert capped_high is True and capped_low is False


def test_cap_envelope_clamps_low():
    # ps R_DN=4.3: low 1 < base 10 / 4.3 = 2.326 -> clamped; high 15 within -> untouched.
    low, high, capped_low, capped_high = cap_envelope("ps", 1.0, 10.0, 15.0)
    assert round(low, 3) == 2.326 and high == 15.0
    assert capped_low is True and capped_high is False


def test_cap_envelope_noop_within_bounds():
    # pe R_UP=2.6 / R_DN=2.7: 8 and 20 both inside [10/2.7, 10*2.6] -> no-op, flags false.
    assert cap_envelope("pe", 8.0, 10.0, 20.0) == (8.0, 20.0, False, False)


def test_cap_envelope_preserves_order():
    # Extreme peer-only wings still yield low <= base <= high after the clamp.
    low, high, _, _ = cap_envelope("ps", 0.1, 10.0, 1000.0)
    assert low <= 10.0 <= high


def test_cap_envelope_unknown_multiple_is_noop():
    # A multiple not in the cap table (e.g. a future p_fcf) passes through
    # unclamped. ev_ebitda WAS the example here until #2021 gave it a real cap
    # entry (see test_cap_envelope_ev_ebitda_clamps).
    assert cap_envelope("p_fcf", 1.0, 10.0, 1000.0) == (1.0, 1000.0, False, False)


def test_cap_envelope_ev_ebitda_clamps():
    # #2021: ev_ebitda now has cap entries (R_UP=2.8, R_DN=1.9) — no longer the
    # unknown-multiple no-op. high 1000 -> base 10*2.8 = 28; low 1 -> 10/1.9.
    low, high, capped_low, capped_high = cap_envelope("ev_ebitda", 1.0, 10.0, 1000.0)
    assert high == 28.0 and round(low, 4) == round(10.0 / 1.9, 4)
    assert capped_low is True and capped_high is True


def test_cap_envelope_nonpositive_base_is_noop():
    assert cap_envelope("ps", 1.0, 0.0, 100.0) == (1.0, 100.0, False, False)


def test_compute_band_cap_bounds_peer_only_tail_base_neutral():
    # Peer-only P/S leg with a fat peer.p75 (the widest-tail case). The cap bounds the
    # BULL to base*_R_UP["ps"] while leaving the audited BASE untouched (base-neutral).
    res = compute_band(
        TargetInputs(
            eps_diluted_ttm=None,
            revenue_ttm=1000.0,
            shareholders_equity=None,
            net_income_ttm=None,
            shares_outstanding=100.0,
            sic="3571",
            reported_currency="USD",
            instrument_currency="USD",
            target_basis="not_multiclass",
        ),
        peer_by_multiple={"ps": PeerPct(p25=5.0, p50=10.0, p75=100.0)},
        own_by_multiple={"ps": OwnPct(None, None, None)},
        own_points_by_multiple={"ps": 0},
        cohort_meta={"ps": {"cohort_n": 40, "excluded_stale_n": 0}},
        sic_level=3,
    )
    assert res.reason == "ok"
    per_share = 1000.0 / 100.0  # revenue / shares
    assert res.base == 10.0 * per_share  # 100.0 — UNCHANGED by the cap
    assert res.bull is not None and round(res.bull, 6) == 310.0  # high 100 clamped to base*3.1=31
    assert res.bear == 5.0 * per_share  # 50.0 — within bounds, uncapped
    assert res.basis["multiples"]["ps"]["capped_high"] is True
    assert res.basis["multiples"]["ps"]["capped_low"] is False
    assert res.basis["multiples"]["ps"]["precap_high_mult"] == 100.0  # pre-cap audit


def test_compute_band_cap_two_legs_base_unchanged_when_nonbase_leg_capped():
    # Codex ckpt-1 #3: two selected legs (pe + ps); the ps leg's HIGH is capped, pe
    # is not. The cross-multiple base = median([pe_base, ps_base]) must be UNCHANGED
    # by the cap — the challenged non-base-determining-leg case.
    res = compute_band(
        TargetInputs(
            eps_diluted_ttm=10.0,
            revenue_ttm=1000.0,
            shareholders_equity=None,
            net_income_ttm=500.0,
            shares_outstanding=100.0,
            sic="3571",
            reported_currency="USD",
            instrument_currency="USD",
            target_basis="not_multiclass",
        ),
        peer_by_multiple={
            "pe": PeerPct(p25=6.0, p50=8.0, p75=12.0),  # base_mult 8 -> base 80, ratio 1.5 uncapped
            "ps": PeerPct(p25=5.0, p50=10.0, p75=100.0),  # base_mult 10 -> base 100, high 100 capped to 31
        },
        own_by_multiple={"pe": OwnPct(None, None, None), "ps": OwnPct(None, None, None)},
        own_points_by_multiple={"pe": 0, "ps": 0},
        cohort_meta={
            "pe": {"cohort_n": 40, "excluded_stale_n": 0},
            "ps": {"cohort_n": 40, "excluded_stale_n": 0},
        },
        sic_level=3,
    )
    assert res.reason == "ok"
    # pe per_share=eps=10 -> base 80; ps per_share=rev/shares=10 -> base 100.
    assert res.base == 90.0  # median([80, 100]) — UNCHANGED though ps.high was capped
    assert res.bull is not None and round(res.bull, 6) == 310.0  # ps 100 -> 31 -> 310
    assert res.bear == 50.0  # min(pe low 60, ps low 50)
    assert res.basis["multiples"]["ps"]["capped_high"] is True
    assert res.basis["multiples"]["pe"]["capped_high"] is False


def test_cap_constants_cover_all_multiples_and_are_valid():
    # PR #2033 review NITPICK: lock the cap constants against a silent bad edit /
    # doc drift. Guards the cap invariant — every synthesizable multiple
    # {pe,ps,pb,ev_ebitda} has an R_UP AND R_DN, and each R >= 1 (R < 1 makes
    # cap_hi < base / cap_lo > base, inverting low <= base <= high and tripping
    # combine_across's fail-closed order check). A new multiple added without a
    # cap entry fails here, forcing a conscious calibration + spec update.
    # ev_ebitda joined in #2021 (fvb_v3) — calibration in spec 2026-07-15 §3.5.
    from app.services.fair_value_band import _R_DN, _R_UP

    multiples = {"pe", "ps", "pb", "ev_ebitda"}
    assert set(_R_UP) == multiples and set(_R_DN) == multiples
    assert all(v >= 1.0 for v in _R_UP.values()), _R_UP
    assert all(v >= 1.0 for v in _R_DN.values()), _R_DN


def test_percentiles_deterministic_regression():
    # Interpolation-convention lock (Hyndman & Fan 1996: 9 divergent definitions).
    # Fixed input -> fixed output; guards the wing/own-history stability contract.
    assert percentiles([1.0, 2.0, 3.0, 4.0, 5.0], (0.25, 0.5, 0.75)) == [2.0, 3.0, 4.0]
    assert percentiles([10.0, 20.0, 30.0, 40.0], (0.25, 0.5, 0.75)) == [17.5, 25.0, 32.5]


def test_filter_dual_class_anti_join():
    rows = [(1, 10.0), (2, 20.0), (3, 30.0)]
    assert filter_dual_class(rows, {2}) == [10.0, 30.0]


def test_filter_dual_class_all_dropped_empty():
    assert filter_dual_class([(1, 10.0), (2, 20.0)], {1, 2}) == []


def test_quality_high():
    q = QualityInputs(
        n_selected=2,
        n_comparator_sides=2,
        own_points=12,
        cohort_n=20,
        excluded_stale_n=0,
        sic_level=4,
        cross_multiple_spread=0.05,
    )
    assert band_quality_status(q) == "high"


def test_quality_low_thin_and_stale():
    q = QualityInputs(
        n_selected=1,
        n_comparator_sides=1,
        own_points=0,
        cohort_n=8,
        excluded_stale_n=6,
        sic_level=2,
        cross_multiple_spread=0.0,
    )
    assert band_quality_status(q) == "low"


def _aapl() -> TargetInputs:
    return TargetInputs(
        eps_diluted_ttm=8.26,
        revenue_ttm=None,
        shareholders_equity=None,
        net_income_ttm=100_000.0,
        shares_outstanding=15_000.0,
        sic="3571",
        reported_currency="USD",
        instrument_currency="USD",
        target_basis="not_multiclass",
    )


def test_golden_aapl_pe_band():
    # §3 worked fixture: own trailing P/E p25/p50/p75 = 31.2/34.5/36.9, peer absent.
    # Band = 31.2*8.26 / 34.5*8.26 / 36.9*8.26 ~= 257.7 / 285.0 / 304.8
    # (cap no-op: high 36.9 < base 34.5 * _R_UP["pe"] 2.6 = 89.7).
    res = compute_band(
        _aapl(),
        peer_by_multiple={"pe": PeerPct(None, None, None)},
        own_by_multiple={"pe": OwnPct(p25=31.2, p50=34.5, p75=36.9)},
        own_points_by_multiple={"pe": 7},
        cohort_meta={"pe": {"cohort_n": 0, "excluded_stale_n": 0}},
        sic_level=4,
    )
    assert res.reason == "ok"
    assert res.target_basis == "not_multiclass"
    assert res.base is not None and res.bear is not None and res.bull is not None
    assert round(res.base, 1) == 285.0
    assert round(res.bear, 1) == 257.7
    assert round(res.bull, 1) == 304.8


def test_compute_band_no_multiple_statused():
    t = TargetInputs(
        eps_diluted_ttm=0.0,
        revenue_ttm=0.0,
        shareholders_equity=0.0,
        net_income_ttm=None,
        shares_outstanding=1000.0,
        sic="3571",
        reported_currency="USD",
        instrument_currency="USD",
        target_basis="not_multiclass",
    )
    res = compute_band(
        t,
        peer_by_multiple={},
        own_by_multiple={},
        own_points_by_multiple={},
        cohort_meta={},
        sic_level=4,
    )
    assert res.reason == "no_multiple"
    assert res.base is None
    assert res.target_basis == "not_multiclass"


def test_compute_band_thin_cohort_when_all_comparators_absent():
    t = _aapl()
    res = compute_band(
        t,
        peer_by_multiple={"pe": PeerPct(None, None, None)},
        own_by_multiple={"pe": OwnPct(None, None, None)},
        own_points_by_multiple={"pe": 0},
        cohort_meta={"pe": {"cohort_n": 3, "excluded_stale_n": 0}},
        sic_level=4,
    )
    assert res.reason == "thin_cohort"
    assert res.base is None


def test_divergence_normal():
    pct, flag = compute_divergence(120.0, 100.0, 0.30)
    assert pct == pytest.approx(0.20)
    assert flag is False


def test_divergence_flagged():
    pct, flag = compute_divergence(150.0, 100.0, 0.30)
    assert flag is True


def test_divergence_band_base_none_is_null_not_zero():
    assert compute_divergence(120.0, None, 0.30) == (None, None)


def test_divergence_band_base_zero_is_null():
    assert compute_divergence(120.0, 0.0, 0.30) == (None, None)


def test_divergence_llm_nan_is_null():
    assert compute_divergence(float("nan"), 100.0, 0.30) == (None, None)


def test_divergence_band_base_nan_is_null():
    # nan <= 0 is False — a NaN band_base must NOT slip past to (nan, False).
    assert compute_divergence(120.0, float("nan"), 0.30) == (None, None)


def test_divergence_llm_inf_is_null():
    assert compute_divergence(float("inf"), 100.0, 0.30) == (None, None)


def test_shape_absent_row():
    out = _shape_fair_value_band(None)
    assert out == {"available": False, "reason": "no_band"}


def test_shape_partial_triple_fails_closed():
    # base present but bull NULL (storage CHECK permits it) -> absent, no crash.
    row = (100.0, 110.0, None, "medium", "ok", _d.date(2026, 7, 13), _d.date(2026, 6, 30), _d.date(2026, 7, 11), {})
    out = _shape_fair_value_band(row)
    assert out["available"] is False


def test_shape_present_row_carries_price_as_of():
    row = (
        100.0,
        110.0,
        130.0,
        "high",
        "ok",
        _d.date(2026, 7, 13),
        _d.date(2026, 6, 30),
        _d.date(2026, 7, 11),
        {"selected": ["pe"]},
    )
    out = _shape_fair_value_band(row)
    assert out["available"] is True
    assert out["price_as_of"] == "2026-07-11"
    assert out["base"] == 110.0


# --- #2021 EV/EBITDA (fvb_v3) ---
# Spec: docs/proposals/valuation/2026-07-15-fair-value-band-ev-ebitda.md


def _ev_kwargs(**kw: Any) -> dict[str, Any]:
    """Fully ev-computable profitable non-financial; override per case."""
    base: dict[str, Any] = dict(
        net_income_ttm=500.0,
        eps_diluted_ttm=2.0,
        revenue_ttm=9_000.0,
        operating_income_ttm=800.0,
        depreciation_amort_ttm=200.0,
        long_term_debt=1_000.0,
        short_term_debt=100.0,
        cash=600.0,
        interest_expense_ttm=50.0,
    )
    base.update(kw)
    return base


def test_ebitda_ttm_strict_none_propagation():
    from app.services.fair_value_band import ebitda_ttm

    assert ebitda_ttm(800.0, 200.0) == 1_000.0
    assert ebitda_ttm(None, 200.0) is None  # strict: no COALESCE(op, 0)
    assert ebitda_ttm(800.0, None) is None  # strict: no COALESCE(d&a, 0)


def test_net_debt_coalesce_and_cash_gate():
    from app.services.fair_value_band import net_debt

    assert net_debt(1_000.0, 100.0, 600.0) == 500.0
    assert net_debt(None, None, 600.0) == -600.0  # debt-null -> 0 (net cash)
    assert net_debt(1_000.0, None, 600.0) == 400.0
    assert net_debt(1_000.0, 100.0, None) is None  # cash gate: None iff cash None


def test_profitable_with_ebitda_selects_three():
    t = _t(**_ev_kwargs())
    assert select_multiples(t) == ["pe", "ps", "ev_ebitda"]


def test_profitable_without_da_keeps_pe_ps():
    # Strict D&A: the dominant real-world absence (671 names full-pop) — the
    # name keeps its pe/ps legs, ev never assigned.
    t = _t(**_ev_kwargs(depreciation_amort_ttm=None))
    assert select_multiples(t) == ["pe", "ps"]


def test_ev_gate_nonpositive_ebitda():
    t = _t(**_ev_kwargs(operating_income_ttm=-300.0, depreciation_amort_ttm=100.0))
    assert select_multiples(t) == ["pe", "ps"]


def test_ev_gate_cash_null():
    t = _t(**_ev_kwargs(cash=None))
    assert select_multiples(t) == ["pe", "ps"]


def test_ev_gate_debt_null_with_interest_incoherent():
    # Debt-both-NULL + positive interest = unrecorded debt (13/103 full-pop).
    t = _t(**_ev_kwargs(long_term_debt=None, short_term_debt=None, interest_expense_ttm=50.0))
    assert select_multiples(t) == ["pe", "ps"]


def test_ev_gate_debt_null_without_interest_ok():
    # Debt-both-NULL + no positive interest = consistent with zero debt (90/103).
    t = _t(**_ev_kwargs(long_term_debt=None, short_term_debt=None, interest_expense_ttm=None))
    assert select_multiples(t) == ["pe", "ps", "ev_ebitda"]
    t0 = _t(**_ev_kwargs(long_term_debt=None, short_term_debt=None, interest_expense_ttm=0.0))
    assert select_multiples(t0) == ["pe", "ps", "ev_ebitda"]


def test_dual_class_target_drops_ev():
    # EV is cap-based; the multiclass intersect-{pe} gate covers it (sql/201:254).
    t = _t(**_ev_kwargs(target_basis="total_company"))
    assert select_multiples(t) == ["pe"]


def test_financial_never_gets_ev():
    t = _t(**_ev_kwargs(sic="6021", shareholders_equity=5_000.0))
    assert select_multiples(t) == ["pb", "pe"]


def test_to_per_share_ev_affine():
    # implied = (mult * EBITDA - net_debt) / shares
    triple = to_per_share(
        "ev_ebitda",
        5.0,
        10.0,
        15.0,
        eps=None,
        revenue=None,
        shareholders_equity=None,
        shares=10.0,
        ebitda=100.0,
        net_debt_value=200.0,
    )
    assert triple == (30.0, 80.0, 130.0)  # (5*100-200)/10 etc.
    assert triple[0] <= triple[1] <= triple[2]  # affine preserves order


def test_to_per_share_ev_net_cash_raises_value():
    with_cash = to_per_share(
        "ev_ebitda",
        5.0,
        10.0,
        15.0,
        eps=None,
        revenue=None,
        shareholders_equity=None,
        shares=10.0,
        ebitda=100.0,
        net_debt_value=-200.0,
    )
    assert with_cash == (70.0, 120.0, 170.0)  # net cash adds equity value


def test_to_per_share_ev_missing_inputs_raises():
    with pytest.raises(ValueError):
        to_per_share(
            "ev_ebitda",
            5.0,
            10.0,
            15.0,
            eps=None,
            revenue=None,
            shareholders_equity=None,
            shares=10.0,
            ebitda=None,
            net_debt_value=200.0,
        )


def _ev_target(**kw: Any) -> TargetInputs:
    return _t(**_ev_kwargs(**kw))


def test_compute_band_ev_leg_dropped_nonpositive_sibling_survives():
    # ev converts <= 0 (mult*EBITDA < net_debt) -> leg dropped fail-closed with
    # peer stats retained + flag; the pe leg still produces the band.
    t = _ev_target(revenue_ttm=None, long_term_debt=20_000.0, cash=100.0)
    assert select_multiples(t) == ["pe", "ev_ebitda"]
    res = compute_band(
        t,
        peer_by_multiple={
            "pe": PeerPct(p25=6.0, p50=8.0, p75=12.0),
            "ev_ebitda": PeerPct(p25=5.0, p50=10.0, p75=15.0),  # 15*1000 < 20000 net debt
        },
        own_by_multiple={"pe": OwnPct(None, None, None), "ev_ebitda": OwnPct(None, None, None)},
        own_points_by_multiple={"pe": 0, "ev_ebitda": 0},
        cohort_meta={
            "pe": {"cohort_n": 40, "excluded_stale_n": 0},
            "ev_ebitda": {"cohort_n": 40, "excluded_stale_n": 0},
        },
        sic_level=3,
    )
    assert res.reason == "ok"
    assert res.base == 16.0  # pe alone: 8 * eps 2.0
    ev_entry = res.basis["multiples"]["ev_ebitda"]
    assert ev_entry["dropped_nonpositive"] is True
    assert "base_value" not in ev_entry  # never contributed
    assert ev_entry["peer"]["p50"] == 10.0  # stats retained for audit
    assert ev_entry["net_debt"] == 20_000.0  # 20000 ltd + 100 std - 100 cash
    assert res.n_selected == 2  # profile-selected, synth-None precedent


def test_compute_band_ev_only_leg_dropped_is_thin_cohort():
    t = _ev_target(eps_diluted_ttm=None, revenue_ttm=None, long_term_debt=20_000.0, cash=100.0)
    assert select_multiples(t) == ["ev_ebitda"]
    res = compute_band(
        t,
        peer_by_multiple={"ev_ebitda": PeerPct(p25=5.0, p50=10.0, p75=15.0)},
        own_by_multiple={"ev_ebitda": OwnPct(None, None, None)},
        own_points_by_multiple={"ev_ebitda": 0},
        cohort_meta={"ev_ebitda": {"cohort_n": 40, "excluded_stale_n": 0}},
        sic_level=3,
    )
    assert res.reason == "thin_cohort"
    assert res.base is None
    assert res.basis["multiples"]["ev_ebitda"]["dropped_nonpositive"] is True


def test_compute_band_ev_peer_only_cap_binds():
    # Peer-only ev leg with a fat p75: bull clamped to base*_R_UP["ev_ebitda"]=2.8;
    # base untouched (base-neutral cap, the peer-only tail case the cap exists for).
    t = _ev_target(
        eps_diluted_ttm=None,
        revenue_ttm=None,
        long_term_debt=None,
        short_term_debt=None,
        interest_expense_ttm=None,
        cash=0.0,
    )
    assert select_multiples(t) == ["ev_ebitda"]
    res = compute_band(
        t,
        peer_by_multiple={"ev_ebitda": PeerPct(p25=5.0, p50=10.0, p75=100.0)},
        own_by_multiple={"ev_ebitda": OwnPct(None, None, None)},
        own_points_by_multiple={"ev_ebitda": 0},
        cohort_meta={"ev_ebitda": {"cohort_n": 40, "excluded_stale_n": 0}},
        sic_level=3,
    )
    assert res.reason == "ok"
    # ebitda 1000, net_debt 0, shares 1000: base = 10*1000/1000 = 10
    assert res.base == 10.0
    assert res.bull is not None and round(res.bull, 6) == 28.0  # 100 -> capped 28
    assert res.basis["multiples"]["ev_ebitda"]["capped_high"] is True
    assert res.basis["multiples"]["ev_ebitda"]["precap_high_mult"] == 100.0


def test_golden_hd_ev_leg():
    # §2 worked fixture (HD-shaped): EBITDA 24.307B, net debt 1.902B, shares 997M.
    # Frozen drift guard for the affine conversion at real-world magnitudes.
    t = _ev_target(
        eps_diluted_ttm=None,
        revenue_ttm=None,
        operating_income_ttm=20_738e6,
        depreciation_amort_ttm=3_569e6,
        long_term_debt=1_902e6,
        short_term_debt=0.0,
        cash=0.0,
        shares_outstanding=997e6,
    )
    res = compute_band(
        t,
        peer_by_multiple={"ev_ebitda": PeerPct(p25=10.0, p50=13.0, p75=16.0)},
        own_by_multiple={"ev_ebitda": OwnPct(None, None, None)},
        own_points_by_multiple={"ev_ebitda": 0},
        cohort_meta={"ev_ebitda": {"cohort_n": 20, "excluded_stale_n": 0}},
        sic_level=4,
    )
    assert res.reason == "ok"
    assert res.bear is not None and res.base is not None and res.bull is not None
    assert round(res.bear, 1) == 241.9  # (10*24.307e9 - 1.902e9) / 997e6
    assert round(res.base, 1) == 315.0  # (13*24.307e9 - 1.902e9) / 997e6
    assert round(res.bull, 1) == 388.2  # (16*24.307e9 - 1.902e9) / 997e6
    assert res.basis["multiples"]["ev_ebitda"]["ebitda_ttm"] == 24_307e6
    assert res.basis["multiples"]["ev_ebitda"]["net_debt"] == 1_902e6


# --- #2032 (fvb_v4) companion-variable peer screen — pure policy ---


def test_companion_vars_derivations():
    cv = companion_vars(
        revenue_ttm=10_000.0,
        net_income_ttm=1_000.0,
        shareholders_equity=50_000.0,
        rev_prior_ttm=8_000.0,
        ttm_start=_d.date(2024, 3, 31),
        prior_end=_d.date(2023, 12, 31),  # 91d — the normal adjacent-quarter gap
    )
    assert cv.net_margin == pytest.approx(0.10)
    assert cv.rev_growth_yoy == pytest.approx(0.25)
    assert cv.roe == pytest.approx(0.02)


def test_companion_vars_growth_adjacency_boundaries():
    # Spec §4.5: ttm_start - prior_end in [1, 120] days. 0d (overlap) and 121d
    # (skipped quarter) fail; the 1d and 120d boundaries pass.
    def growth(days: int) -> float | None:
        return companion_vars(
            revenue_ttm=10.0,
            net_income_ttm=None,
            shareholders_equity=None,
            rev_prior_ttm=8.0,
            ttm_start=_d.date(2024, 1, 1) + _d.timedelta(days=days),
            prior_end=_d.date(2024, 1, 1),
        ).rev_growth_yoy

    assert growth(0) is None
    assert growth(1) is not None
    assert growth(120) is not None
    assert growth(121) is None


def test_companion_vars_growth_gates():
    # rev_prior == 0 -> None (no denominator); negative prior uses abs() so a
    # contra-revenue prior still yields a signed, finite growth.
    base: dict[str, Any] = dict(
        revenue_ttm=10.0,
        net_income_ttm=None,
        shareholders_equity=None,
        ttm_start=_d.date(2024, 3, 31),
        prior_end=_d.date(2023, 12, 31),
    )
    assert companion_vars(rev_prior_ttm=0.0, **base).rev_growth_yoy is None
    assert companion_vars(rev_prior_ttm=None, **base).rev_growth_yoy is None
    assert companion_vars(rev_prior_ttm=-5.0, **base).rev_growth_yoy == pytest.approx(3.0)


def test_companion_vars_margin_and_roe_gates():
    # margin requires revenue > 0 AND net income present; roe requires equity > 0.
    cv = companion_vars(
        revenue_ttm=0.0,
        net_income_ttm=1.0,
        shareholders_equity=-5.0,
        rev_prior_ttm=None,
        ttm_start=None,
        prior_end=None,
    )
    assert cv.net_margin is None and cv.roe is None
    cv = companion_vars(
        revenue_ttm=10.0,
        net_income_ttm=None,
        shareholders_equity=5.0,
        rev_prior_ttm=None,
        ttm_start=None,
        prior_end=None,
    )
    assert cv.net_margin is None and cv.roe is None  # ni absent gates BOTH


def test_screen_tiers_frozen_shape():
    # pe EXCLUDED (wrong companion — spec §4.2); ps/ev share the margin+growth
    # tiers; pb is ROE-only; every schedule widens monotonically.
    assert set(_SCREEN_TIERS) == {"ps", "pb", "ev_ebitda"}
    assert _SCREEN_TIERS["ps"] == _SCREEN_TIERS["ev_ebitda"]
    for tiers in _SCREEN_TIERS.values():
        for field in ("net_margin", "rev_growth_yoy", "roe"):
            widths = [getattr(t, field) for t in tiers]
            assert all(w is None for w in widths) or all(w is not None for w in widths)
            present = [w for w in widths if w is not None]
            assert present == sorted(present)  # monotone widening
    assert _SCREEN_TIERS["pb"][0].roe == 0.05 and _SCREEN_TIERS["pb"][0].net_margin is None


def test_screen_passes_boundary_and_nulls():
    tier = _SCREEN_TIERS["ps"][0]  # margin +/-0.05, growth +/-0.10
    tgt = CompanionVars(net_margin=0.10, rev_growth_yoy=0.20, roe=None)
    # Boundary |delta| == width passes (sim used <=).
    assert screen_passes(tier, tgt, CompanionVars(0.15, 0.30, None)) is True
    assert screen_passes(tier, tgt, CompanionVars(0.151, 0.30, None)) is False
    assert screen_passes(tier, tgt, CompanionVars(0.15, 0.301, None)) is False
    # NULL companion on the peer side fails (never imputed).
    assert screen_passes(tier, tgt, CompanionVars(None, 0.20, None)) is False
    assert screen_passes(tier, tgt, CompanionVars(0.10, None, None)) is False
    # roe irrelevant for ps (width None) — a peer without roe still passes.
    pb_tier = _SCREEN_TIERS["pb"][2]  # roe +/-0.20
    tgt_fin = CompanionVars(net_margin=None, rev_growth_yoy=None, roe=0.10)
    assert screen_passes(pb_tier, tgt_fin, CompanionVars(None, None, 0.30)) is True
    assert screen_passes(pb_tier, tgt_fin, CompanionVars(None, None, 0.31)) is False


def test_target_screenable():
    both = CompanionVars(net_margin=0.1, rev_growth_yoy=0.2, roe=0.05)
    no_growth = CompanionVars(net_margin=0.1, rev_growth_yoy=None, roe=0.05)
    assert target_screenable("ps", both) is True
    assert target_screenable("ps", no_growth) is False  # ps needs margin AND growth
    assert target_screenable("pb", no_growth) is True  # pb needs roe only
    assert target_screenable("pb", CompanionVars(0.1, 0.2, None)) is False
    assert target_screenable("pe", both) is False  # pe excluded
    assert target_screenable("ps", None) is False


def test_quality_screen_fallback_knock():
    # Boundary case: score 7 (sides 2 + own 12 + stale 0 + sic3 1) = high;
    # the v4 screen-fallback knock drops it to 6 = medium.
    kw: dict[str, Any] = dict(
        n_selected=1,
        n_comparator_sides=2,
        own_points=12,
        cohort_n=20,
        excluded_stale_n=0,
        sic_level=3,
        cross_multiple_spread=0.0,
    )
    assert band_quality_status(QualityInputs(**kw)) == "high"
    assert band_quality_status(QualityInputs(**kw, screen_fallback=True)) == "medium"


def _screen_meta(*, screened: bool, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    meta: dict[str, Any] = {"cohort_n": 40, "excluded_stale_n": 0, "sic_level": 4}
    meta["cohort_screened"] = screened
    meta["screen"] = extra if extra is not None else {"reason": "no_screened_cohort"}
    return meta


def test_compute_band_screen_provenance_and_knock():
    # A contributing peer-backed ps leg that fell back unscreened: provenance
    # lands in the basis entry AND the quality knock fires.
    t = _t(revenue_ttm=1000.0, net_income_ttm=100.0, eps_diluted_ttm=10.0)
    res = compute_band(
        t,
        peer_by_multiple={
            "pe": PeerPct(p25=6.0, p50=8.0, p75=12.0),
            "ps": PeerPct(p25=5.0, p50=10.0, p75=20.0),
        },
        own_by_multiple={"pe": OwnPct(None, None, None), "ps": OwnPct(None, None, None)},
        own_points_by_multiple={"pe": 0, "ps": 0},
        cohort_meta={
            "pe": {"cohort_n": 40, "excluded_stale_n": 0, "sic_level": 4},
            "ps": _screen_meta(screened=False),
        },
        sic_level=4,
    )
    assert res.reason == "ok"
    ps_entry = res.basis["multiples"]["ps"]
    assert ps_entry["cohort_screened"] is False
    assert ps_entry["screen"] == {"reason": "no_screened_cohort"}
    assert ps_entry["sic_level"] == 4  # per-leg cohort level (v4 §4.4)
    pe_entry = res.basis["multiples"]["pe"]
    assert "cohort_screened" not in pe_entry and "screen" not in pe_entry  # pe: N/A
    # Knock: identical shape with the ps leg SCREENED must score one tier higher
    # or equal — assert via the explicit flag path instead of tier arithmetic.
    res_screened = compute_band(
        t,
        peer_by_multiple={
            "pe": PeerPct(p25=6.0, p50=8.0, p75=12.0),
            "ps": PeerPct(p25=5.0, p50=10.0, p75=20.0),
        },
        own_by_multiple={"pe": OwnPct(None, None, None), "ps": OwnPct(None, None, None)},
        own_points_by_multiple={"pe": 0, "ps": 0},
        cohort_meta={
            "pe": {"cohort_n": 40, "excluded_stale_n": 0, "sic_level": 4},
            "ps": _screen_meta(screened=True, extra={"sic_level": 4, "width_tier": 1, "survivors_n": 11}),
        },
        sic_level=4,
    )
    held_entry = res_screened.basis["multiples"]["ps"]
    assert held_entry["cohort_screened"] is True
    assert held_entry["screen"] == {"sic_level": 4, "width_tier": 1, "survivors_n": 11}


def test_compute_band_own_only_screenable_leg_does_not_knock():
    # ps leg with NO peer side (own-only) marked cohort_screened=False: the knock
    # must NOT fire — there is no peer cohort to screen (spec §4.3). Quality here
    # equals the identical no-screen-keys case.
    def run(meta_ps: dict[str, Any]) -> str | None:
        res = compute_band(
            _t(revenue_ttm=1000.0),
            peer_by_multiple={"ps": PeerPct(None, None, None)},
            own_by_multiple={"ps": OwnPct(p25=4.0, p50=5.0, p75=6.0)},
            own_points_by_multiple={"ps": 12},
            cohort_meta={"ps": meta_ps},
            sic_level=0,
        )
        assert res.reason == "ok"
        return res.quality_status

    knocked = run(_screen_meta(screened=False))
    plain = run({"cohort_n": 40, "excluded_stale_n": 0, "sic_level": 4})  # same meta, no screen keys
    assert knocked == plain


def test_compute_band_synth_none_leg_keeps_screen_provenance():
    # v4 §4.4 (Codex ckpt-1 MED-3): a selected screenable leg with BOTH
    # comparator sides absent still records its basis entry (screen audit trail
    # survives non-contribution); the band statuses thin_cohort.
    res = compute_band(
        _t(revenue_ttm=1000.0),
        peer_by_multiple={"ps": PeerPct(None, None, None)},
        own_by_multiple={"ps": OwnPct(None, None, None)},
        own_points_by_multiple={"ps": 0},
        cohort_meta={"ps": _screen_meta(screened=False)},
        sic_level=0,
    )
    assert res.reason == "thin_cohort"
    entry = res.basis["multiples"]["ps"]
    assert entry["cohort_screened"] is False
    assert entry["screen"] == {"reason": "no_screened_cohort"}
    assert "base_value" not in entry and "capped_high" not in entry
