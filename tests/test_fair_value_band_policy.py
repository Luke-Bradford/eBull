from typing import Any

from app.services.fair_value_band import (
    MIN_OWN_POINTS,
    OwnPct,
    PeerPct,
    QualityInputs,
    TargetInputs,
    band_quality_status,
    combine_across,
    compute_band,
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
    own = OwnPct(p20=12.0, p50=24.0, p80=28.0)
    result = synth_multiple(peer, own)
    assert result is not None
    low, base, high = result
    assert base == 22.0  # mean(20, 24)
    assert low == 10.0  # min(peer_p25=10, own_p20=12)
    assert high == 30.0  # max(peer_p75=30, own_p80=28)


def test_synth_degrades_to_peer_only():
    peer = PeerPct(p25=10.0, p50=20.0, p75=30.0)
    own = OwnPct(p20=None, p50=None, p80=None)
    assert synth_multiple(peer, own) == (10.0, 20.0, 30.0)


def test_synth_degrades_to_own_only():
    peer = PeerPct(p25=None, p50=None, p75=None)
    own = OwnPct(p20=12.0, p50=24.0, p80=28.0)
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
    # §3 worked fixture: own trailing P/E p20/p50/p80 = 31.2/34.5/36.9, peer absent.
    # Band = 31.2*8.26 / 34.5*8.26 / 36.9*8.26 ~= 257.7 / 285.0 / 304.8.
    res = compute_band(
        _aapl(),
        peer_by_multiple={"pe": PeerPct(None, None, None)},
        own_by_multiple={"pe": OwnPct(p20=31.2, p50=34.5, p80=36.9)},
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
