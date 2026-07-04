"""#1916 Finding A — zero-share holders must not render as pie/memo rows.

A holder whose reconciled current holding is 0 shares is not a holder of the
issuer; rendering it produces a duplicate-looking row (e.g. AAPL's Katherine
Adams: a live ``direct`` lot plus a stale 0-share ``indirect`` lot from a
2-year-old accession showed the name twice). Dropping zero-share holders is
figure-neutral — they contribute 0 to every slice total, pct, and the residual
(full-population check on the dev DB found zero strictly-negative share rows in
any ``ownership_*_current`` table), and it corrects ``filer_count``.

Pure-logic — no DB (see test-tiering guidance).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    SourceTag,
    _bucket_into_slices,
    _build_slice,
)


def _h(cik: str | None, name: str, source: SourceTag, shares: str) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession="0000000000-00-000000",
        winning_edgar_url=None,
        as_of_date=date(2026, 1, 1),
        filer_type=None,
        dropped_sources=(),
    )


OUT = Decimal("1000000")


def test_build_slice_drops_zero_share_holder() -> None:
    s = _build_slice(
        "insiders",
        [_h("0000000001", "Live Person", "form4", "175408"), _h("0000000001", "Live Person", "form4", "0")],
        OUT,
    )
    assert s.filer_count == 1
    assert [h.filer_name for h in s.holders] == ["Live Person"]
    assert s.total_shares == Decimal("175408")


def test_bucket_skips_all_zero_pie_slice() -> None:
    slices = _bucket_into_slices(
        {"insiders": [_h("1", "Zeroed Out", "form4", "0")]},
        [],
        OUT,
    )
    assert [s.category for s in slices] == []  # all-zero category produces no slice


def test_bucket_drops_zero_share_memo_fund_holder() -> None:
    slices = _bucket_into_slices(
        {},
        [],
        OUT,
        funds_holders=[_h("S000000001", "Real Fund", "nport", "500"), _h("S000000002", "Empty Fund", "nport", "0")],
    )
    funds = [s for s in slices if s.category == "funds"]
    assert len(funds) == 1
    assert funds[0].filer_count == 1
    assert [h.filer_name for h in funds[0].holders] == ["Real Fund"]


def test_zero_share_drop_is_figure_neutral_for_nonzero_holders() -> None:
    holders = [_h("1", "A", "form4", "300"), _h("2", "B", "form4", "700")]
    s_clean = _build_slice("insiders", list(holders), OUT)
    s_with_zero = _build_slice("insiders", [*holders, _h("3", "Ghost", "form4", "0")], OUT)
    assert s_clean.total_shares == s_with_zero.total_shares == Decimal("1000")
    assert s_clean.pct_outstanding == s_with_zero.pct_outstanding
    assert s_clean.filer_count == s_with_zero.filer_count == 2
