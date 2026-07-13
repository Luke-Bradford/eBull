"""Pure-logic tests for peer ranking + factor mapping (#1751). No DB."""

from __future__ import annotations

from app.services.peer_comparison import (
    FACTOR_KEYS,
    MIN_COHORT,
    THIN_COVERAGE_RATIO,
    _rank_peers,
    _row_factors,
    is_factor_thin,
    resolve_sic_level,
)


def _row(iid: int, ta: float | None, **factors: float | None) -> dict[str, object]:
    base: dict[str, object] = {
        "instrument_id": iid,
        "symbol": f"S{iid}",
        "company_name": f"C{iid}",
        "total_assets": ta,
    }
    for k in FACTOR_KEYS:
        base.setdefault(k, None)
    base.update(factors)
    return base


def test_rank_peers_excludes_self_and_orders_by_proximity() -> None:
    rows = [_row(1, 100.0), _row(2, 110.0), _row(3, 1000.0), _row(4, 95.0)]
    peers = _rank_peers(rows, self_id=1, self_total_assets=100.0, limit=8)
    # self excluded; nearest log-size first: 95 (0.051), 110 (0.095), 1000 (2.30)
    assert [p.instrument_id for p in peers] == [4, 2, 3]


def test_rank_peers_drops_nonpositive_total_assets() -> None:
    rows = [_row(1, 100.0), _row(2, None), _row(3, 0.0), _row(4, 120.0)]
    peers = _rank_peers(rows, self_id=1, self_total_assets=100.0, limit=8)
    assert [p.instrument_id for p in peers] == [4]


def test_rank_peers_caps_at_limit() -> None:
    rows = [_row(i, 100.0 + i) for i in range(1, 20)]
    peers = _rank_peers(rows, self_id=1, self_total_assets=100.0, limit=8)
    assert len(peers) == 8


def test_row_factors_maps_all_keys() -> None:
    f = _row_factors(_row(1, 100.0, roe=0.1, pe_ratio=15.0))
    assert set(f) == set(FACTOR_KEYS)
    assert f["roe"] == 0.1
    assert f["pe_ratio"] == 15.0
    assert f["net_margin"] is None


def test_is_factor_thin_price_gated_always_thin() -> None:
    # pe_ratio is structurally dev-limited regardless of coverage.
    assert is_factor_thin("pe_ratio", cohort_n=1000, cohort_member_count=1000) is True


def test_is_factor_thin_low_coverage_flagged() -> None:
    # revenue_growth_yoy at ~5% coverage (dev DB: 3-12%) → thin.
    assert is_factor_thin("revenue_growth_yoy", cohort_n=40, cohort_member_count=951) is True


def test_is_factor_thin_healthy_coverage_not_flagged() -> None:
    # operating_margin floors at 24.6% on the dev DB — above the 20% cut.
    assert is_factor_thin("operating_margin", cohort_n=152, cohort_member_count=617) is False


def test_is_factor_thin_threshold_boundary() -> None:
    # Exactly at the threshold is NOT thin (strict <); just below is thin.
    n = int(THIN_COVERAGE_RATIO * 100)
    assert is_factor_thin("roe", cohort_n=n, cohort_member_count=100) is False
    assert is_factor_thin("roe", cohort_n=n - 1, cohort_member_count=100) is True


def test_is_factor_thin_empty_base_is_thin() -> None:
    # No complete-TTM members → no signal → thin (avoids div-by-zero).
    assert is_factor_thin("roe", 0, 0) is True


def test_is_factor_thin_fallback_cohort_always_thin() -> None:
    # cohort_sic_level==0 fallback: even a fully-covered factor is thin, because
    # the ABSOLUTE base is below MIN_COHORT (coverage ratio would say otherwise).
    assert is_factor_thin("roe", 100, 100, cohort_is_fallback=True) is True
    # Non-fallback with the same full coverage is NOT thin.
    assert is_factor_thin("roe", 100, 100, cohort_is_fallback=False) is False


# --- SIC cohort walk (#2023) — resolve_sic_level ----------------------------


def test_resolve_sic_level_finest_wins() -> None:
    # SIC-4 clears MIN_COHORT → level 4, marker 4 (narrowest granularity).
    assert resolve_sic_level(n4=8, n3=50, n2=200, min_cohort=MIN_COHORT) == (4, 4)


def test_resolve_sic_level_walks_to_sic3() -> None:
    # SIC-4 short, SIC-3 clears → column level 3, marker 3.
    assert resolve_sic_level(n4=7, n3=8, n2=200, min_cohort=MIN_COHORT) == (3, 3)


def test_resolve_sic_level_walks_to_sic2() -> None:
    # Only SIC-2 clears → column level 2, marker 2.
    assert resolve_sic_level(n4=2, n3=5, n2=8, min_cohort=MIN_COHORT) == (2, 2)


def test_resolve_sic_level_self_count_boundary() -> None:
    # Counts are PEER counts (self already excluded upstream), so n4=8 = 8 real
    # peers and clears. Regression guard against re-introducing self in the count
    # (which would need 9 to clear). MIN_COHORT-1 does NOT clear at level 4.
    assert resolve_sic_level(n4=MIN_COHORT, n3=0, n2=0, min_cohort=MIN_COHORT)[0] == 4
    assert resolve_sic_level(n4=MIN_COHORT - 1, n3=MIN_COHORT - 1, n2=MIN_COHORT - 1, min_cohort=MIN_COHORT) == (2, 0)


def test_resolve_sic_level_no_level_clears_falls_back_thin() -> None:
    # No level reaches MIN_COHORT → widen to SIC-2 (column 2) but mark 0 (thin
    # fallback), NOT a raise/None. peer_comparison must still render.
    assert resolve_sic_level(n4=3, n3=3, n2=3, min_cohort=MIN_COHORT) == (2, 0)
    assert resolve_sic_level(n4=0, n3=0, n2=0, min_cohort=MIN_COHORT) == (2, 0)
