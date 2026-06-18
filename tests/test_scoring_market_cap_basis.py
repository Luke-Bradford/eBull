"""Pure-logic tests for the #1664 ranking-view dual-class market-cap overlay.

``_apply_market_cap_basis`` overlays the #1662 total-company cap onto the
``instrument_valuation`` view row that scoring reads — restoring the correct
``market_cap_live`` + ``fcf_yield`` for a curated dual-class issuer (which the
view NULLs because it cannot build the total in SQL). No DB.
"""

from __future__ import annotations

import pathlib
import re
from datetime import date
from decimal import Decimal

from app.services.scoring import _apply_market_cap_basis
from app.services.xbrl_derived_stats import MarketCapResolution, TotalCompanyMarketCap

# The eight shares-distorted columns (each carries a price × combined-shares term)
# the view must NULL for a curated dual-class issuer; and the clean ones it must
# keep (per-share / ratio — no shares×price term).
_SUPPRESSED = (
    "market_cap_live",
    "enterprise_value",
    "pb_ratio",
    "price_sales",
    "p_fcf_ratio",
    "fcf_yield",
    "ev_revenue",
    "ev_ebitda",
)
_KEPT = ("pe_ratio", "debt_equity_ratio")


def test_sql_201_wraps_every_distorted_column_and_no_clean_one() -> None:
    """SQL tripwire (#1664): sql/201's final SELECT must wrap each shares-distorted
    column in the dual-class suppression CASE and leave the clean columns bare.

    The legacy-CTE DB test only exercises 4 of the 8 suppressed columns (the
    new_pipeline-only ones are NULL in legacy and need a 4-quarter financial_periods
    fixture to drive). This locks all 8 at the source so a column silently losing its
    CASE wrapper (or a clean column gaining one) fails here. Mirrors the #1648
    docstring-stripped SQL tripwire pattern."""
    sql = pathlib.Path("sql/201_instrument_valuation_dual_class_suppress.sql").read_text()
    # Strip line comments so commented examples can't satisfy the assertions.
    body = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    for col in _SUPPRESSED:
        pat = rf"CASE WHEN dc\.instrument_id IS NULL THEN v\.{col} END\s+AS {col}"
        assert re.search(pat, body), f"{col} not wrapped in dual-class suppression CASE"
    for col in _KEPT:
        assert re.search(rf"\bv\.{col},", body), f"{col} should be emitted unwrapped"
        assert not re.search(rf"CASE WHEN dc\.instrument_id IS NULL THEN v\.{col} END", body), (
            f"{col} is clean (no shares×price term) and must NOT be suppressed"
        )


def _total(value: str) -> TotalCompanyMarketCap:
    return TotalCompanyMarketCap(
        value=Decimal(value),
        period_end=date(2024, 12, 31),
        combined_shares=Decimal("12211000000"),
        sum_mapped_shares=Decimal("11350000000"),
        residual_shares=Decimal("861000000"),
        imputed_residual=True,
        leg_count=2,
    )


def _row(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "market_cap_live": None,  # view already NULLed for dual-class
        "fcf_yield": None,
        "fcf_ttm": Decimal("88944000000"),
        "pe_ratio": Decimal("25.0"),
    }
    base.update(over)
    return base


def test_total_company_overlays_market_cap_and_fcf_yield() -> None:
    res = MarketCapResolution(basis="total_company", total=_total("4447600000000"))
    out = _apply_market_cap_basis(_row(), res)
    assert out is not None
    assert out["market_cap_live"] == Decimal("4447600000000")
    # fcf_yield = company TTM FCF / total-company cap (float).
    assert out["fcf_yield"] == 88944000000.0 / 4447600000000.0


def test_total_company_fcf_yield_none_when_fcf_ttm_missing() -> None:
    res = MarketCapResolution(basis="total_company", total=_total("4447600000000"))
    out = _apply_market_cap_basis(_row(fcf_ttm=None), res)
    assert out is not None
    assert out["market_cap_live"] == Decimal("4447600000000")
    assert out["fcf_yield"] is None


def test_total_company_fcf_yield_none_when_cap_zero() -> None:
    # value=0 is not produced by the builder (Σ of positive products) but the
    # division must still be guarded.
    res = MarketCapResolution(basis="total_company", total=_total("0"))
    out = _apply_market_cap_basis(_row(), res)
    assert out is not None
    assert out["fcf_yield"] is None


def test_multiclass_unavailable_leaves_view_nulls() -> None:
    # Curated dual-class, no clean total → the view's NULLs stand (honest degrade).
    res = MarketCapResolution(basis="multiclass_unavailable")
    out = _apply_market_cap_basis(_row(), res)
    assert out is not None
    assert out["market_cap_live"] is None
    assert out["fcf_yield"] is None
    assert out["pe_ratio"] == Decimal("25.0")  # clean column untouched


def test_not_multiclass_leaves_legacy_product_untouched() -> None:
    # Single-class: the view's combined×price product is exact — pass through.
    row = _row(market_cap_live=Decimal("3000000000000"), fcf_yield=Decimal("0.03"))
    res = MarketCapResolution(basis="not_multiclass")
    out = _apply_market_cap_basis(row, res)
    assert out is not None
    assert out["market_cap_live"] == Decimal("3000000000000")
    assert out["fcf_yield"] == Decimal("0.03")


def test_none_row_passes_through() -> None:
    res = MarketCapResolution(basis="total_company", total=_total("4447600000000"))
    assert _apply_market_cap_basis(None, res) is None
