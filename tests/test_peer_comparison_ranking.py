"""Pure-logic tests for peer ranking + factor mapping (#1751). No DB."""

from __future__ import annotations

from app.services.peer_comparison import FACTOR_KEYS, _rank_peers, _row_factors


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
