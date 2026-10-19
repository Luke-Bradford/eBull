"""Pure-logic tests for the co-filer group pass (#3227, #3895 and the widened links).

Co-filers of a joint accession are distinct CIKs, so owner-once keeps each; ``_reconcile_cofiler_groups``
counts a clique of them once, at its largest member. No DB — hand-built ``Holder`` objects and a
co-filer map. See docs/specs/ownership/2026-10-19-3227-cofiler-group-identity.md and
docs/specs/ownership/2026-10-19-3227-cofiler-links-widened.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.ownership_rollup import (
    Holder,
    SliceCategory,
    SourceTag,
    _canonical_cik,
    _cofiler_graph,
    _reconcile_cofiler_groups,
)

_P = date(2026, 6, 12)


def _h(cik: str | None, name: str, shares: int, *, source: SourceTag = "form4", accession: str = "") -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession or f"acc-{cik}-{source}",
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _cats(
    insiders: list[Holder] = (),  # type: ignore[assignment]
    blockholders: list[Holder] = (),  # type: ignore[assignment]
    institutions: list[Holder] = (),  # type: ignore[assignment]
) -> dict[SliceCategory, list[Holder]]:
    return {
        "insiders": list(insiders),
        "blockholders": list(blockholders),
        "institutions": list(institutions),
        "etfs": [],
    }


def _pie(by_category: dict[SliceCategory, list[Holder]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _tmus() -> dict[SliceCategory, list[Holder]]:
    """TMUS on dev, 2026-10-19: the subsidiary's Form 4 and the parent's 13D, one block."""
    return _cats(
        insiders=[_h("0001597213", "Deutsche Telekom Holding B.V.", 582_065_875, accession="F4")],
        blockholders=[_h("0000946770", "Deutsche Telekom AG", 592_065_875, source="13d", accession="D13")],
    )


_TMUS_COFILERS = {"F4": frozenset({"1597213", "946770"}), "D13": frozenset({"1597213", "946770"})}


def test_cross_channel_clique_counts_once_at_the_largest_member() -> None:
    out, corrections = _reconcile_cofiler_groups(_tmus(), _TMUS_COFILERS)
    assert out["insiders"] == []
    assert [(h.filer_name, h.shares) for h in out["blockholders"]] == [("Deutsche Telekom AG", Decimal(592_065_875))]
    (keeper,) = out["blockholders"]
    assert [(d.filer_cik, d.source, d.accession_number, d.shares) for d in keeper.dropped_sources] == [
        ("0001597213", "form4", "F4", Decimal(582_065_875))
    ]
    (c,) = corrections
    assert c.kind == "cofiler_group_collapse"
    assert c.shares_removed == Decimal(582_065_875)
    assert "figures sum to 1174131750" in c.detail and "Overlap, dates and class scope unverified" in c.detail
    assert "13d-5" not in c.detail


def test_origin_must_itself_be_on_the_accession() -> None:
    # The Form 4 row's accession names the 13D filer but not the Form 4 filer itself.
    cats = _tmus()
    out, corrections = _reconcile_cofiler_groups(cats, {"F4": frozenset({"946770", "99"})})
    assert corrections == [] and _pie(out) == _pie(cats)


def test_padded_and_unpadded_ciks_match_and_blank_ciks_never_link() -> None:
    assert _canonical_cik(" 0000946770 ") == "946770"
    assert _canonical_cik("0000000000") is None and _canonical_cik("") is None and _canonical_cik(None) is None
    cats = _cats(
        insiders=[_h("", "No Cik Person", 500, accession="J")],
        blockholders=[_h("0000000007", "Fund", 400, source="13d", accession="J")],
    )
    out, corrections = _reconcile_cofiler_groups(cats, {"J": frozenset({"7", "8"})})
    assert corrections == [] and _pie(out) == Decimal(900)


@pytest.mark.parametrize(
    "excluded",
    [
        _h("0000000002", "Zero", 0, accession="J"),
        _h("0000000002", "Proxy", 900, source="def14a", accession="J"),
    ],
    ids=["zero-total", "def14a-figure"],
)
def test_excluded_identities_neither_link_nor_bridge(excluded: Holder) -> None:
    # A and C each co-file only with the excluded identity B.
    cats = _cats(
        insiders=[_h("0000000001", "A", 100, accession="X"), excluded, _h("0000000003", "C", 300, accession="Y")]
    )
    cofilers = {"X": frozenset({"1", "2"}), "Y": frozenset({"2", "3"}), "J": frozenset({"1", "2", "3"})}
    out, corrections = _reconcile_cofiler_groups(cats, cofilers)
    assert corrections == [] and _pie(out) == _pie(cats)


def test_a_chain_stays_split() -> None:
    # A co-files with B, B with C; A and C never co-file (spec, Change 2).
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 100, accession="X"),
            _h("0000000002", "B", 10, accession="Y"),
            _h("0000000003", "C", 200, accession="Z"),
        ]
    )
    cofilers = {"X": frozenset({"1", "2"}), "Y": frozenset({"2", "3"})}
    out, corrections = _reconcile_cofiler_groups(cats, cofilers)
    assert corrections == [] and _pie(out) == Decimal(310)


def test_tie_goes_to_the_insiders_member_then_the_lowest_cik() -> None:
    cats = _cats(
        insiders=[_h("0000000009", "AI Alpine", 646_500_000, accession="F4")],
        blockholders=[_h("0000000005", "Advent", 646_500_000, source="13g", accession="G")],
    )
    out, _ = _reconcile_cofiler_groups(cats, {"F4": frozenset({"9", "5"})})
    assert [h.filer_name for h in out["insiders"]] == ["AI Alpine"] and out["blockholders"] == []
    cats = _cats(insiders=[_h("0000000009", "Nine", 50, accession="J"), _h("0000000005", "Five", 50, accession="J")])
    out, _ = _reconcile_cofiler_groups(cats, {"J": frozenset({"9", "5"})})
    assert [h.filer_name for h in out["insiders"]] == ["Five"]


def test_no_identity_changes_category() -> None:
    # A small insider and a large blockholder: the keeper stays a blockholder, nothing moves into insiders.
    cats = _cats(
        insiders=[_h("0000000001", "Insider", 5, accession="J")],
        blockholders=[_h("0000000002", "Block", 300, source="13d", accession="J")],
    )
    out, _ = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2"})})
    assert out["insiders"] == [] and [h.filer_name for h in out["blockholders"]] == ["Block"]


def test_institutions_are_untouched() -> None:
    cats = _cats(
        insiders=[_h("0000000001", "Insider", 100, accession="J")],
        institutions=[_h("0000000002", "Manager", 300, source="13f", accession="J")],
    )
    out, corrections = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2"})})
    assert corrections == [] and out == cats


def test_one_row_already_folded_upstream_is_a_single_member() -> None:
    # #1764 left one representative for the accession; the other co-filer has no row here.
    cats = _cats(insiders=[_h("0000000001", "Rep", 100, accession="J")])
    out, corrections = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2"})})
    assert corrections == [] and out == cats


def test_mixed_shared_block_and_separate_holding_is_the_stated_residual() -> None:
    # A and B report one 100 block; C holds 200 apart. #1764 already left A for the block; the union
    # is 300, the pass counts max(100, 200) = 200 (spec, accepted residual 1).
    cats = _cats(insiders=[_h("0000000001", "A", 100, accession="J"), _h("0000000003", "C", 200, accession="J")])
    out, _ = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2", "3"})})
    assert _pie(out) == Decimal(200)


def test_an_owner_total_spans_its_lots_and_every_removed_row_is_dropped() -> None:
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 60, accession="J"),
            _h("0000000001", "A", 50, source="form3", accession="K"),
            _h("0000000002", "B", 100, accession="J"),
        ]
    )
    out, (c,) = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2"})})
    # A's total 110 beats B's 100: both of A's rows stay; B's row is the dropped one.
    assert sorted(h.shares for h in out["insiders"]) == [Decimal(50), Decimal(60)]
    assert c.shares_removed == Decimal(100)
    keeper = max(out["insiders"], key=lambda h: h.shares)
    assert [d.filer_name for d in keeper.dropped_sources] == ["B"]


def test_empty_cofiler_map_changes_nothing() -> None:
    cats = _tmus()
    out, corrections = _reconcile_cofiler_groups(cats, {})
    assert out is cats and corrections == []


def test_one_identity_in_two_categories_raises() -> None:
    cats = _cats(
        insiders=[_h("0000000001", "A", 100)],
        blockholders=[_h("0000000001", "A", 100, source="13d")],
    )
    with pytest.raises(ValueError, match="two categories"):
        _reconcile_cofiler_groups(cats, {})


def test_cik_tie_break_is_numeric() -> None:
    cats = _cats(insiders=[_h("0000000010", "Ten", 50, accession="J"), _h("0000000002", "Two", 50, accession="J")])
    out, _ = _reconcile_cofiler_groups(cats, {"J": frozenset({"10", "2"})})
    assert [h.filer_name for h in out["insiders"]] == ["Two"]


def test_a_name_keyed_identity_never_links() -> None:
    # No CIK at all: the identity is ``NAME:…``, so it is not a node even if its name's filer is on the accession.
    cats = _cats(
        insiders=[_h(None, "Person Without Cik", 500, accession="J")],
        blockholders=[_h("0000000007", "Fund", 400, source="13d", accession="J")],
    )
    out, corrections = _reconcile_cofiler_groups(cats, {"J": frozenset({"7", "8"})})
    assert corrections == [] and _pie(out) == Decimal(900)


# --- Widened links (docs/specs/ownership/2026-10-19-3227-cofiler-links-widened.md) ---


def test_owners_a_third_members_filing_names_link_without_a_row_of_their_own_there() -> None:
    # A's Form 4 names A, B and C; B and C each report on their own accessions. #3895 linked A-B and
    # A-C only (a star, so a chain); the filing names all three, so they are one clique.
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 82, accession="J"),
            _h("0000000002", "B", 73, accession="K"),
            _h("0000000003", "C", 68, accession="L"),
        ]
    )
    out, (c,) = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2", "3"})})
    assert [h.filer_name for h in out["insiders"]] == ["A"]
    assert c.shares_removed == Decimal(141)


def test_an_accession_no_nodes_own_row_sits_on_gives_no_edge() -> None:
    cats = _cats(insiders=[_h("0000000002", "B", 73, accession="K"), _h("0000000003", "C", 68, accession="L")])
    out, corrections = _reconcile_cofiler_groups(cats, {"M": frozenset({"2", "3"})})
    assert corrections == [] and _pie(out) == Decimal(141)


def test_an_institutions_identity_with_a_13dg_figure_links_and_keeps_its_category() -> None:
    cats = _cats(
        insiders=[_h("0000000001", "Fund", 75, accession="F4")],
        institutions=[_h("0000000005", "Sponsor", 113, source="13d", accession="D13")],
    )
    out, (c,) = _reconcile_cofiler_groups(cats, {"D13": frozenset({"1", "5"})})
    # An institution-only origin: the only joint accession is the institutions row's own.
    assert out["insiders"] == [] and [h.filer_name for h in out["institutions"]] == ["Sponsor"]
    assert c.shares_removed == Decimal(75)


@pytest.mark.parametrize(
    "rows",
    [
        [_h("0000000005", "Manager", 129, source="13f", accession="D13")],
        [
            _h("0000000005", "Manager", 100, source="13g", accession="D13"),
            _h("0000000005", "Manager", 29, source="13f", accession="Q"),
        ],
    ],
    ids=["13f-only", "13f-and-13g"],
)
def test_an_identity_with_a_13f_row_never_links(rows: list[Holder]) -> None:
    # Spec, proposition 3: a 13F row is a discretion count, not comparable with a co-filer's figure.
    cats = _cats(insiders=[_h("0000000001", "Fund", 129, accession="F4")], institutions=rows)
    out, corrections = _reconcile_cofiler_groups(cats, {"D13": frozenset({"1", "5"}), "F4": frozenset({"1", "5"})})
    assert corrections == [] and _pie(out) == _pie(cats)


def test_guard_a_3895_clique_grown_into_a_non_clique_still_collapses() -> None:
    # #3895: A and B co-file J. The widened node I (13D figure) co-files K with A only, so {A, B, I}
    # is not a clique. Without the guard it would stay split and the pie would rise to 100 + 90 + 50.
    cats = _cats(
        insiders=[_h("0000000001", "A", 100, accession="J"), _h("0000000002", "B", 90, accession="J")],
        institutions=[_h("0000000009", "I", 50, source="13d", accession="K")],
    )
    cofilers = {"J": frozenset({"1", "2"}), "K": frozenset({"1", "9"})}
    out, (c,) = _reconcile_cofiler_groups(cats, cofilers)
    assert c.shares_removed == Decimal(90)
    assert _pie(out) == Decimal(150)
    base, _ = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2"})})
    assert _pie(out) <= _pie(base)


def test_two_3895_cliques_merged_into_one_clique_collapse_once() -> None:
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 100, accession="J"),
            _h("0000000001", "A", 1, source="form3", accession="L"),
            _h("0000000002", "B", 90, accession="J"),
            _h("0000000003", "C", 80, accession="K"),
            _h("0000000004", "D", 70, accession="K"),
        ]
    )
    cofilers = {"J": frozenset({"1", "2"}), "K": frozenset({"3", "4"}), "L": frozenset({"1", "2", "3", "4"})}
    out, corrections = _reconcile_cofiler_groups(cats, cofilers)
    assert len(corrections) == 1 and corrections[0].shares_removed == Decimal(240)
    assert _pie(out) == Decimal(101)


def test_a_widened_non_clique_component_gets_no_collapse_of_its_own() -> None:
    # A's filing names A and B; C's filing names B and C; no #3895 clique and no widened clique.
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 100, accession="X"),
            _h("0000000002", "B", 10, accession="Q"),
            _h("0000000003", "C", 200, accession="Z"),
        ]
    )
    out, corrections = _reconcile_cofiler_groups(cats, {"X": frozenset({"1", "2"}), "Z": frozenset({"2", "3"})})
    assert corrections == [] and _pie(out) == Decimal(310)


def test_guard_invariant_breach_falls_back_to_the_3895_result() -> None:
    # An insiders identity carrying a 13F row is a #3895 node but not a widened one. The pass then
    # returns #3895's cliques for the rollup, with B's link resolved against #3895's own nodes.
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 100, accession="J"),
            _h("0000000002", "B", 60, accession="K"),
            _h("0000000002", "B", 30, source="13f", accession="Q"),
        ]
    )
    out, (c,) = _reconcile_cofiler_groups(cats, {"J": frozenset({"1", "2"})})
    assert c.shares_removed == Decimal(90)
    assert [h.filer_name for h in out["insiders"]] == ["A"]


def test_every_3895_clique_lies_inside_one_widened_component() -> None:
    # The guard's premise: #3895's edges are a subset of the widened ones, so each #3895 clique is
    # connected, and collapsed whole, inside one widened component. Here {A, B} is #3895's clique and
    # the widened graph adds I-B and C-I links, so {A, B, I, C} is one non-clique component.
    cats = _cats(
        insiders=[
            _h("0000000001", "A", 100, accession="J"),
            _h("0000000002", "B", 90, accession="J"),
            _h("0000000003", "C", 20, accession="L"),
        ],
        institutions=[_h("0000000009", "I", 50, source="13d", accession="K")],
    )
    graph = _cofiler_graph(cats, {"J": frozenset({"1", "2"}), "K": frozenset({"2", "9"}), "L": frozenset({"3", "9"})})
    component = ["CIK:0000000001", "CIK:0000000002", "CIK:0000000003", "CIK:0000000009"]
    assert [sorted(m) for m in graph.chains] == [component]
    assert graph.cliques == [["CIK:0000000001", "CIK:0000000002"]]
