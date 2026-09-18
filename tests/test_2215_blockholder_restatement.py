"""#2215 — the folded 13D/G channel becomes a legible memo overlay.

Pure tests, no DB. ``_blockholder_restatement_holders`` is a function of the
already-reconciled pie slices, so the whole decision is table-testable and the
DB-backed rollup tests stay untouched.

What each test pins is the rule it names in its own docstring; every one was
revert-probed against the implementation (see the PR).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    DroppedSource,
    Holder,
    OwnershipSlice,
    SliceCategory,
    SourceTag,
    _blockholder_restatement_holders,
    _build_slice,
    count_additive_institutional_holders,
)

OUTSTANDING = Decimal(1000)


def _dropped(source: SourceTag, shares: str, *, accession: str, as_of: date | None = None) -> DroppedSource:
    return DroppedSource(
        source=source,
        shares=Decimal(shares),
        accession_number=accession,
        edgar_url=f"https://example.invalid/{accession}",
        as_of_date=as_of,
    )


def _holder(
    name: str,
    shares: str,
    *,
    source: SourceTag = "13f",
    accession: str = "survivor-accession",
    as_of: date | None = None,
    cik: str | None = "0000001",
    dropped: tuple[DroppedSource, ...] = (),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=f"https://example.invalid/{accession}",
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=dropped,
    )


def _slice(category: SliceCategory, holders: list[Holder]) -> OwnershipSlice:
    return _build_slice(category, holders, OUTSTANDING)


def test_a_13g_folded_into_institutions_is_restated_from_the_DROPPED_entry() -> None:
    """The overlay row's provenance is the losing filing, never the survivor's.

    Stamping the survivor's source/accession would label a 13G figure with a 13F
    accession, and the survivor's ``as_of_date`` would corrupt the slice as-of
    coherence envelope (#1647 part 1).
    """
    survivor = _holder(
        "Vanguard",
        "500",
        source="13f",
        accession="13f-acc",
        as_of=date(2026, 6, 30),
        dropped=(_dropped("13g", "300", accession="13g-acc", as_of=date(2026, 2, 14)),),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(300)
    assert restated.winning_source == "13g"
    assert restated.winning_accession == "13g-acc"
    assert restated.winning_edgar_url == "https://example.invalid/13g-acc"
    assert restated.as_of_date == date(2026, 2, 14)
    assert restated.filer_name == "Vanguard"
    assert restated.filer_cik == "0000001"
    assert restated.dropped_sources == ()


def test_the_blockholders_slice_itself_is_excluded() -> None:
    """A dropped 13G behind the same owner's surviving 13D is a WITHIN-channel
    supersession, already represented by the blockholder row. Re-listing it
    would show one stake twice in one panel."""
    survivor = _holder(
        "Activist LP",
        "400",
        source="13d",
        accession="13d-acc",
        dropped=(_dropped("13g", "380", accession="13g-acc"),),
    )

    assert _blockholder_restatement_holders([_slice("blockholders", [survivor])]) == []


def test_an_owner_with_both_a_dropped_13d_and_13g_is_restated_at_the_MAX() -> None:
    """A 13D and a 13G from one filer are overlapping restatements of the same
    block, not additive holdings (#1640/#1889 — MAX overlapping, SUM additive).
    Summing would double one invisible position."""
    survivor = _holder(
        "Dual Filer",
        "900",
        dropped=(
            _dropped("13d", "250", accession="13d-acc"),
            _dropped("13g", "310", accession="13g-acc"),
        ),
    )

    [restated] = _blockholder_restatement_holders([_slice("institutions", [survivor])])

    assert restated.shares == Decimal(310)
    assert restated.winning_accession == "13g-acc"


def test_non_blockholder_dropped_channels_are_ignored() -> None:
    """Only 13D/G restatements belong in this overlay — a dropped DEF 14A or
    Form 3 line is a different channel with its own surface."""
    survivor = _holder(
        "Officer",
        "120",
        source="form4",
        dropped=(
            _dropped("def14a", "119", accession="proxy-acc"),
            _dropped("form3", "100", accession="f3-acc"),
        ),
    )

    assert _blockholder_restatement_holders([_slice("insiders", [survivor])]) == []


def test_a_holder_with_no_dropped_sources_contributes_nothing() -> None:
    assert _blockholder_restatement_holders([_slice("institutions", [_holder("Plain", "700")])]) == []


def test_the_overlay_never_enters_the_additive_institutional_count() -> None:
    """The basis, not the category list, is what keeps the overlay out of the
    additive paths — ``count_additive_institutional_holders`` filters
    ``denominator_basis == "pie_wedge"``, so a restated 13F-adjacent owner
    cannot be counted a second time (#2232 arm 3's pigeonhole)."""
    institutions = _slice(
        "institutions",
        [_holder("Vanguard", "500", dropped=(_dropped("13g", "300", accession="13g-acc"),))],
    )
    overlay = _build_slice(
        "blockholders_restated",
        _blockholder_restatement_holders([institutions]),
        OUTSTANDING,
        denominator_basis="cross_channel_restatement",
    )

    assert overlay.denominator_basis == "cross_channel_restatement"
    assert overlay.filer_count == 1
    assert count_additive_institutional_holders([institutions]) == 1
    assert count_additive_institutional_holders([institutions, overlay]) == 1


def test_every_pie_category_except_blockholders_is_scanned() -> None:
    """``_reconcile_owner_once`` can route a 13D/G filer into any of the other
    three pie categories, so scanning must not be institutions-only."""
    slices = [
        _slice(
            "insiders",
            [_holder("Insider", "100", cik="A", source="form4", dropped=(_dropped("13d", "90", accession="a"),))],
        ),
        _slice(
            "institutions",
            [_holder("Manager", "200", cik="B", dropped=(_dropped("13g", "150", accession="b"),))],
        ),
        _slice(
            "etfs",
            [_holder("ETF Sponsor", "300", cik="C", dropped=(_dropped("13g", "250", accession="c"),))],
        ),
    ]

    assert sorted(h.filer_name for h in _blockholder_restatement_holders(slices)) == [
        "ETF Sponsor",
        "Insider",
        "Manager",
    ]
