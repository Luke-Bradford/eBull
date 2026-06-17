"""Pure-logic tests for the ownership machine-trust envelope (#1647 PR-A).

Covers the three additive-contract helpers — all pure, no DB:
  * ``_calendar_quarter`` — (year, 1..4) bucketing.
  * ``_slice_coherence`` — per-slice as-of span incl. collapsed-family members.
  * ``_compute_sanity`` — raw plausibility facts over pie-wedge slices.
  * ``is_estimate`` derivation via ``_per_category_state`` / ``_compute_coverage``.

See docs/specs/etl/2026-06-16-ownership-machine-trust-envelope.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    FamilyMember,
    Holder,
    OwnershipSlice,
    SliceCategory,
    SourceTag,
    _build_slice,
    _calendar_quarter,
    _compute_concentration,
    _compute_coverage,
    _compute_residual,
    _compute_sanity,
    _slice_coherence,
)


def _h(
    shares: str,
    *,
    as_of: date | None,
    source: SourceTag = "13f",
    family: tuple[FamilyMember, ...] = (),
    name: str = "Holder",
) -> Holder:
    return Holder(
        filer_cik="0000000001",
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession="acc-1",
        winning_edgar_url=None,
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=(),
        family_members=family,
    )


def _fm(shares: str, *, as_of: date | None) -> FamilyMember:
    return FamilyMember(
        filer_cik="0000000002",
        filer_name="Sub",
        shares=Decimal(shares),
        source="13f",
        accession_number="acc-2",
        edgar_url=None,
        as_of_date=as_of,
    )


def _slice(
    category: SliceCategory,
    holders: list[Holder],
    *,
    outstanding: str = "1000",
    basis: str = "pie_wedge",
) -> OwnershipSlice:
    return _build_slice(category, holders, Decimal(outstanding), denominator_basis=basis)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _calendar_quarter
# ---------------------------------------------------------------------------


def test_calendar_quarter_boundaries() -> None:
    assert _calendar_quarter(date(2025, 1, 1)) == (2025, 1)
    assert _calendar_quarter(date(2025, 3, 31)) == (2025, 1)
    assert _calendar_quarter(date(2025, 4, 1)) == (2025, 2)
    assert _calendar_quarter(date(2025, 6, 30)) == (2025, 2)
    assert _calendar_quarter(date(2025, 9, 30)) == (2025, 3)
    assert _calendar_quarter(date(2025, 10, 1)) == (2025, 4)
    assert _calendar_quarter(date(2025, 12, 31)) == (2025, 4)


def test_calendar_quarter_year_distinguished() -> None:
    # Same quarter index, different year → distinct buckets.
    assert _calendar_quarter(date(2024, 3, 31)) != _calendar_quarter(date(2025, 3, 31))


# ---------------------------------------------------------------------------
# _slice_coherence
# ---------------------------------------------------------------------------


def test_coherence_empty() -> None:
    assert _slice_coherence([]) == (None, None, 0, False)


def test_coherence_all_null_as_of() -> None:
    holders = [_h("100", as_of=None), _h("50", as_of=None)]
    assert _slice_coherence(holders) == (None, None, 0, False)


def test_coherence_single_quarter() -> None:
    holders = [_h("100", as_of=date(2025, 3, 31)), _h("50", as_of=date(2025, 1, 15))]
    lo, hi, distinct, mixed = _slice_coherence(holders)
    assert lo == date(2025, 1, 15)
    assert hi == date(2025, 3, 31)
    assert distinct == 1
    assert mixed is False


def test_coherence_multi_quarter() -> None:
    holders = [
        _h("100", as_of=date(2025, 3, 31)),  # Q1
        _h("50", as_of=date(2025, 6, 30)),  # Q2
        _h("25", as_of=date(2024, 12, 31)),  # 2024 Q4
    ]
    lo, hi, distinct, mixed = _slice_coherence(holders)
    assert lo == date(2024, 12, 31)
    assert hi == date(2025, 6, 30)
    assert distinct == 3
    assert mixed is True


def test_coherence_unsorted_dates() -> None:
    holders = [_h("1", as_of=date(2025, 6, 30)), _h("2", as_of=date(2025, 1, 1))]
    lo, hi, _distinct, _mixed = _slice_coherence(holders)
    assert lo == date(2025, 1, 1)
    assert hi == date(2025, 6, 30)


def test_coherence_looks_through_family_members() -> None:
    # Collapsed family: holder.as_of = min(members) = Q2, but a member is in Q3.
    # Looking only at the holder would falsely read single-quarter.
    family = (_fm("60", as_of=date(2025, 4, 1)), _fm("40", as_of=date(2025, 9, 30)))
    holders = [_h("100", as_of=date(2025, 4, 1), family=family)]
    lo, hi, distinct, mixed = _slice_coherence(holders)
    assert lo == date(2025, 4, 1)
    assert hi == date(2025, 9, 30)
    assert distinct == 2
    assert mixed is True


def test_coherence_family_member_null_ignored() -> None:
    family = (_fm("60", as_of=date(2025, 4, 1)), _fm("40", as_of=None))
    holders = [_h("100", as_of=date(2025, 4, 1), family=family)]
    lo, hi, distinct, mixed = _slice_coherence(holders)
    assert lo == hi == date(2025, 4, 1)
    assert distinct == 1
    assert mixed is False


def test_build_slice_populates_coherence_fields() -> None:
    slc = _slice("institutions", [_h("100", as_of=date(2025, 3, 31)), _h("50", as_of=date(2025, 6, 30))])
    assert slc.as_of_min == date(2025, 3, 31)
    assert slc.as_of_max == date(2025, 6, 30)
    assert slc.distinct_quarters == 2
    assert slc.mixed_period is True


# ---------------------------------------------------------------------------
# _compute_sanity
# ---------------------------------------------------------------------------


def test_sanity_empty_slices() -> None:
    s = _compute_sanity([], Decimal("1000"))
    assert s.max_distinct_quarters == 0
    assert s.institutions_pct == Decimal(0)
    assert s.institutions_over_100pct is False
    assert s.largest_single_holder_pct == Decimal(0)
    assert s.any_pie_slice_over_100pct is False


def test_sanity_zero_outstanding_zeroes_pct_but_keeps_quarters() -> None:
    slc = _slice(
        "institutions", [_h("100", as_of=date(2025, 3, 31)), _h("50", as_of=date(2025, 6, 30))], outstanding="1"
    )
    # Re-measure against a 0 denominator: quarters survive, pct collapse to 0.
    s = _compute_sanity([slc], Decimal(0))
    assert s.max_distinct_quarters == 2
    assert s.institutions_pct == Decimal(0)
    assert s.institutions_over_100pct is False
    assert s.largest_single_holder_pct == Decimal(0)
    assert s.any_pie_slice_over_100pct is False


def test_sanity_institutions_and_etfs_summed() -> None:
    inst = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))], outstanding="1000")
    etf = _slice("etfs", [_h("300", as_of=date(2025, 3, 31))], outstanding="1000")
    s = _compute_sanity([inst, etf], Decimal("1000"))
    assert s.institutions_pct == Decimal("0.7")
    assert s.institutions_over_100pct is False


def test_sanity_institutions_over_100pct() -> None:
    inst = _slice("institutions", [_h("700", as_of=date(2025, 3, 31))], outstanding="1000")
    etf = _slice("etfs", [_h("400", as_of=date(2025, 3, 31))], outstanding="1000")
    s = _compute_sanity([inst, etf], Decimal("1000"))
    assert s.institutions_pct == Decimal("1.1")
    assert s.institutions_over_100pct is True


def test_sanity_memo_overlay_excluded() -> None:
    # A funds memo-overlay slice tagged institution_subset must NOT leak into
    # institutions_pct, the largest-holder pick, or max_distinct_quarters.
    inst = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))], outstanding="1000")
    funds = _slice(
        "funds",
        [_h("9999", as_of=date(2024, 1, 1)), _h("1", as_of=date(2025, 12, 31))],
        outstanding="1000",
        basis="institution_subset",
    )
    s = _compute_sanity([inst, funds], Decimal("1000"))
    assert s.institutions_pct == Decimal("0.4")  # funds excluded
    assert s.largest_single_holder_pct == Decimal("0.4")  # 400/1000, not 9999
    assert s.max_distinct_quarters == 1  # funds' 2-quarter spread excluded
    assert s.any_pie_slice_over_100pct is False


def test_sanity_largest_single_holder_across_slices() -> None:
    inst = _slice(
        "institutions", [_h("200", as_of=date(2025, 3, 31)), _h("100", as_of=date(2025, 3, 31))], outstanding="1000"
    )
    block = _slice("blockholders", [_h("350", as_of=date(2025, 3, 31), source="13d")], outstanding="1000")
    s = _compute_sanity([inst, block], Decimal("1000"))
    assert s.largest_single_holder_pct == Decimal("0.35")  # the 350 blockholder


def test_sanity_any_slice_over_100pct() -> None:
    block = _slice("blockholders", [_h("1200", as_of=date(2025, 3, 31), source="13d")], outstanding="1000")
    s = _compute_sanity([block], Decimal("1000"))
    assert s.any_pie_slice_over_100pct is True


# ---------------------------------------------------------------------------
# DEF 14A proxy_disclosure overlay is non-additive (#1659)
# ---------------------------------------------------------------------------


def _proxy(*holders: Holder) -> OwnershipSlice:
    # ``_slice``'s ``basis`` kwarg maps to ``_build_slice(denominator_basis=...)``;
    # assert it actually took so these exclusion tests can never silently go
    # vacuous if the helper signature drifts (review PREVENTION).
    s = _slice("def14a_unmatched", list(holders), outstanding="1000", basis="proxy_disclosure")
    assert s.denominator_basis == "proxy_disclosure"
    return s


def test_proxy_disclosure_excluded_from_sanity() -> None:
    inst = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))], outstanding="1000")
    # A huge proxy "All officers as a group" deemed block must NOT become the
    # largest single holder or inflate any sanity fact.
    proxy = _proxy(_h("900", as_of=date(2025, 3, 31), source="def14a", name="All officers"))
    s = _compute_sanity([inst, proxy], Decimal("1000"))
    assert s.largest_single_holder_pct == Decimal("0.4")  # the 400 inst, not the 900 proxy
    assert s.institutions_pct == Decimal("0.4")
    assert s.any_pie_slice_over_100pct is False  # the 900 proxy slice is not a pie slice


def test_proxy_disclosure_excluded_from_concentration() -> None:
    inst = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))], outstanding="1000")
    proxy = _proxy(_h("300", as_of=date(2025, 3, 31), source="def14a", name="Sponsor group"))
    c = _compute_concentration(Decimal("1000"), [inst, proxy])
    # Known (additive) filers = 400/1000 only; the proxy deemed block is excluded.
    assert c.pct_outstanding_known == Decimal("0.4")


def test_proxy_disclosure_excluded_from_residual() -> None:
    inst = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))], outstanding="1000")
    proxy = _proxy(_h("300", as_of=date(2025, 3, 31), source="def14a", name="Sponsor group"))
    r = _compute_residual(Decimal("1000"), [inst, proxy], None)
    # Residual = 1000 - 400 (only the additive institutions); proxy not subtracted.
    assert r.shares == Decimal("600")
    assert r.oversubscribed is False


# ---------------------------------------------------------------------------
# is_estimate derivation
# ---------------------------------------------------------------------------


def test_is_estimate_true_when_no_universe_estimate() -> None:
    # Tier-0 default: every category NULL estimate → is_estimate True.
    slc = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))])
    coverage = _compute_coverage([slc], {"insiders": None, "blockholders": None, "institutions": None, "etfs": None})
    assert all(c.is_estimate for c in coverage.categories.values())
    assert coverage.categories["institutions"].state == "unknown_universe"


def test_is_estimate_false_when_estimate_seeded() -> None:
    slc = _slice("institutions", [_h("400", as_of=date(2025, 3, 31))])
    coverage = _compute_coverage([slc], {"insiders": None, "blockholders": None, "institutions": 10, "etfs": None})
    assert coverage.categories["institutions"].is_estimate is False
    assert coverage.categories["insiders"].is_estimate is True


def test_is_estimate_false_for_real_zero_estimate() -> None:
    # estimate==0 is a real seeded value ("universe is empty here"), vacuously
    # green — NOT an estimate.
    slc = _slice("institutions", [_h("0", as_of=date(2025, 3, 31))])
    coverage = _compute_coverage([slc], {"insiders": 0, "blockholders": None, "institutions": None, "etfs": None})
    assert coverage.categories["insiders"].is_estimate is False
    assert coverage.categories["insiders"].state == "green"
