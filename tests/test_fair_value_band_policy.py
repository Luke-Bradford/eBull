from typing import Any

from app.services.fair_value_band import (
    OwnPct,
    PeerPct,
    TargetInputs,
    combine_across,
    currency_coherent,
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
    t = _t(net_income_ttm=500.0, eps_diluted_ttm=2.0, revenue_ttm=9_000.0, target_basis="dual_class_combined")
    assert select_multiples(t) == ["pe"]


def test_dual_class_financial_keeps_pe_drops_pb():
    t = _t(sic="6021", eps_diluted_ttm=2.0, shareholders_equity=5_000.0, target_basis="dual_class_combined")
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
