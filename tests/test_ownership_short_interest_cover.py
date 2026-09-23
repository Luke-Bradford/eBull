"""#2226 — attributing a pie overage to 13F short-lending double counts.

Form 13F reports long positions only and a lender keeps reporting loaned shares
(SEC 13F FAQ Q41/Q42), so institutional totals can exceed shares outstanding by up
to the open short interest. ``attribute_overage_to_short_interest`` attaches that
attribution only when the overage fits inside the figure. Pure — no DB.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.ownership_rollup import (
    Holder,
    OwnershipSlice,
    SliceCategory,
    _build_slice,
    _compute_residual,
    attribute_overage_to_short_interest,
)

_OUT = Decimal("1000")
_SETTLE = date(2026, 6, 30)


def _slice(category: SliceCategory, shares: str, *, basis: str = "pie_wedge") -> OwnershipSlice:
    holder = Holder(
        filer_cik="0000000001",
        filer_name="Holder",
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source="13f",
        winning_accession="acc-1",
        winning_edgar_url=None,
        as_of_date=_SETTLE,
        filer_type=None,
        dropped_sources=(),
    )
    return _build_slice(category, [holder], _OUT, denominator_basis=basis)  # type: ignore[arg-type]


def _attribute(slices: list[OwnershipSlice], si: tuple[Decimal, date] | None):
    residual = _compute_residual(_OUT, slices)
    return attribute_overage_to_short_interest(residual, outstanding=_OUT, slices=slices, short_interest=si)


@pytest.mark.parametrize(
    ("pie", "si", "expect_overage"),
    [
        # overage 100 inside SI 150 -> attributed
        (["1100"], Decimal("150"), Decimal("100")),
        # boundary: overage exactly equals SI -> attributed (<=)
        (["1100"], Decimal("100"), Decimal("100")),
        # overage summed across wedges: 900 + 250 = 1150, overage 150 <= 200
        (["900", "250"], Decimal("200"), Decimal("150")),
        # overage 100 beyond SI 99 -> not attributed
        (["1100"], Decimal("99"), None),
        # no short interest on file -> not attributed (absence is not evidence)
        (["1100"], None, None),
        # not oversubscribed -> never attributed, even with SI present
        (["900"], Decimal("500"), None),
    ],
)
def test_attribution_table(pie: list[str], si: Decimal | None, expect_overage: Decimal | None) -> None:
    categories: list[SliceCategory] = ["institutions", "insiders"]
    slices = [_slice(categories[i], s) for i, s in enumerate(pie)]
    out = _attribute(slices, (si, _SETTLE) if si is not None else None)
    if expect_overage is None:
        assert out.short_interest_cover is None
    else:
        assert out.short_interest_cover is not None
        assert out.short_interest_cover.overage_shares == expect_overage
        assert out.short_interest_cover.short_interest_shares == si
        assert out.short_interest_cover.settlement_date == _SETTLE


def test_memo_overlay_is_not_part_of_the_overage() -> None:
    # Funds are fund-level detail inside the 13F aggregate; counting them would
    # inflate the overage past SI and wrongly withhold the attribution.
    slices = [_slice("institutions", "1050"), _slice("funds", "500", basis="institution_subset")]
    out = _attribute(slices, (Decimal("60"), _SETTLE))
    assert out.short_interest_cover is not None
    assert out.short_interest_cover.overage_shares == Decimal("50")


def test_attribution_leaves_residual_figures_untouched() -> None:
    slices = [_slice("institutions", "1100")]
    before = _compute_residual(_OUT, slices)
    after = _attribute(slices, (Decimal("150"), _SETTLE))
    assert (after.shares, after.pct_outstanding, after.oversubscribed) == (
        before.shares,
        before.pct_outstanding,
        before.oversubscribed,
    )
