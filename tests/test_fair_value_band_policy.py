import datetime as _d
from typing import Any

import pytest

from app.services.fair_value_band import (
    MIN_OWN_POINTS,
    OwnPct,
    PeerPct,
    QualityInputs,
    TargetInputs,
    _shape_fair_value_band,
    band_quality_status,
    cap_envelope,
    combine_across,
    compute_band,
    compute_divergence,
    currency_coherent,
    filter_dual_class,
    own_range,
    percentiles,
    select_multiples,
    synth_multiple,
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
    # A multiple not in the cap table (e.g. future ev_ebitda) passes through unclamped.
    assert cap_envelope("ev_ebitda", 1.0, 10.0, 1000.0) == (1.0, 1000.0, False, False)


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
    # doc drift. Guards the cap invariant — every synthesizable multiple {pe,ps,pb}
    # has an R_UP AND R_DN, and each R >= 1 (R < 1 makes cap_hi < base / cap_lo > base,
    # inverting low <= base <= high and tripping combine_across's fail-closed order
    # check). A new multiple added without a cap entry (e.g. Phase-2 ev_ebitda) fails
    # here, forcing a conscious calibration + spec §6.2 update.
    from app.services.fair_value_band import _R_DN, _R_UP

    multiples = {"pe", "ps", "pb"}
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
