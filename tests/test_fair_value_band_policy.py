from typing import Any

from app.services.fair_value_band import TargetInputs, currency_coherent, percentiles, select_multiples


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
