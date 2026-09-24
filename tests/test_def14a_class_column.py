"""#2351 slices 3 / 3b — pure tests for the dual-class class-column and V-shape row rules
and their locator."""

from __future__ import annotations

from decimal import Decimal

from app.providers.implementations.sec_def14a import ShareLocation, share_locations
from app.services.def14a_recipients import (
    REASON_CLASS_COLUMN,
    REASON_CLASS_ROW,
    Cover,
    Sibling,
    decide_class_column,
    decide_class_rows,
    designators,
    keyed_designators,
    location_label,
    row_class_key,
    row_class_label,
    usable_common_classes,
)

GOOGL, GOOG = Sibling(1, "GOOGL"), Sibling(2, "GOOG")
ALPHABET = Cover(
    accession="c-1",
    pairs=frozenset(
        {("Class A Common Stock, $0.001 par value", "GOOGL"), ("Class C Capital Stock, $0.001 par value", "GOOG")}
    ),
)


def at(*captions: str, interior: tuple[str, ...] = (), row: tuple[str, ...] = ()) -> ShareLocation:
    return ShareLocation(captions=captions, interior=interior, row_texts=row)


A, B, C = at("Class A Common Stock", "Shares"), at("Class B Common Stock", "Shares"), at("Class C Capital Stock")
UNLABELLED = at("Amount and Nature of Beneficial Ownership")


def decide(siblings: list[Sibling], cover: Cover, rows: dict[int, list[list[ShareLocation]]]) -> set[int]:
    out = decide_class_column(
        accession_number="p-1", issuer_cik="0001", siblings=siblings, cover=cover, row_locations=rows
    )
    assert all(s.reason == REASON_CLASS_COLUMN for s in out)
    return {s.instrument_id for s in out}


def test_designators() -> None:
    assert designators("Class A Common Stock (1)") == {"A"}
    assert designators("Series C Liberty Formula One Common Stock") == {"C"}
    assert designators("Class A and Class B Stock") == {"A", "B"}
    assert designators("Class A and B Common Stock") == {"A", "B"}
    assert designators("Classes A and B Common Stock") == {"A", "B"}
    assert designators("Class A, B or C") == {"A", "B", "C"}
    assert designators("Class A-1 Common Stock") == frozenset()
    assert designators("Class II Units") == frozenset()
    assert designators("Percentage of Shares of Class Outstanding") == frozenset()
    assert designators("Class A and Nature of Beneficial Ownership") == {"A"}
    assert designators("Common stock") == frozenset()


def test_location_label() -> None:
    assert location_label(A) == "A"
    assert location_label(at("Common Stock Beneficially Owned (a)", "Class B")) == "B"
    assert location_label(at("Class A and Class B Stock", "Shares")) is None
    assert location_label(UNLABELLED) is None
    assert location_label(at("Shares of Voting Common Stock")) is None  # no letter
    assert location_label(at()) is None
    assert location_label(at("Series A Preferred Stock")) is None  # non-common caption
    assert location_label(at("Class A Common Stock", interior=("Class B holders",))) is None  # re-header
    assert location_label(at("Class A Common Stock", row=("Jane", "Class B", "50.0%"))) is None  # V-shape cell


def test_usable_common_classes() -> None:
    usable = usable_common_classes([GOOGL, GOOG], ALPHABET)
    assert {letter: [c.sibling for c in cs] for letter, cs in usable.items()} == {"A": [GOOGL], "C": [GOOG]}
    # Undesignated common titles never qualify.
    mkc = Cover("c", frozenset({("Common Stock Non-Voting", "MKC"), ("Common Stock", "MKC.V")}))
    assert usable_common_classes([Sibling(1, "MKC"), Sibling(2, "MKC.V")], mkc) == {}
    # A letter carried by a second cover title of any kind is not unique.
    agm = Cover("c", frozenset({("Class A Common Stock", "AGM.A"), ("Series A Preferred Stock", "AGM.PRA")}))
    assert usable_common_classes([Sibling(1, "AGM.A")], agm) == {}


def test_columns_of_other_classes_suppress_the_class_c_sibling() -> None:
    # GOOG (Class C) shows figures read from the Class A column and, for some rows, Class B
    # (unregistered, not on the cover): every row is provably not Class C.
    rows = {1: [[A], [A], [B]], 2: [[A], [A], [B]]}
    assert decide([GOOGL, GOOG], ALPHABET, rows) == {2}


def test_a_row_under_the_siblings_own_letter_keeps_it() -> None:
    assert decide([GOOGL, GOOG], ALPHABET, {2: [[A], [C]]}) == set()


def test_unlocated_unlabelled_or_conflicting_row_keeps_it() -> None:
    assert decide([GOOGL, GOOG], ALPHABET, {2: [[A], []]}) == set()  # no location / NULL shares
    assert decide([GOOGL, GOOG], ALPHABET, {2: [[A], [UNLABELLED]]}) == set()
    assert decide([GOOGL, GOOG], ALPHABET, {2: [[A], [A, B]]}) == set()  # two tables disagree
    assert decide([GOOGL, GOOG], ALPHABET, {2: [[at("Class A and Class B Stock")]]}) == set()


def test_no_rows_or_no_witness_keeps_it() -> None:
    assert decide([GOOGL, GOOG], ALPHABET, {}) == set()
    # Only an unlisted letter: nothing names another sibling on the cover.
    assert decide([GOOGL, GOOG], ALPHABET, {2: [[B], [B]]}) == set()


def test_undesignated_siblings_are_never_suppressed_nor_witnesses() -> None:
    uhal = Cover(
        "c", frozenset({("Common stock, $0.25 par value", "UHAL"), ("Series N Non-Voting Common Stock", "UHAL.B")})
    )
    sibs = [Sibling(1, "UHAL"), Sibling(2, "UHAL.B")]
    assert decide(sibs, uhal, {1: [[at("Series N")]], 2: [[at("Voting Common Stock")]]}) == set()


def test_us_duplicates_are_decided_together_and_never_witness_each_other() -> None:
    fox = Cover("c", frozenset({("Class A Common Stock", "FOXA"), ("Class B Common Stock", "FOX")}))
    sibs = [Sibling(1, "FOXA"), Sibling(2, "FOX"), Sibling(3, "FOX.US")]
    assert decide(sibs, fox, {2: [[A]], 3: [[A]]}) == {2, 3}
    assert decide(sibs, fox, {2: [[B]], 3: [[B]]}) == set()


H_SHAPE = """<table>
<tr><td></td><td colspan="2">Class A Common Stock</td><td colspan="2">Class B Common Stock</td></tr>
<tr><td>Name</td><td>Shares</td><td>Percent</td><td>Shares</td><td>Percent</td></tr>
<tr><td>Jane Founder</td><td>1,000</td><td>1.0%</td><td>500,000</td><td>50.0%</td></tr>
<tr><td>BlackRock, Inc.</td><td>9,000</td><td>9.0%</td><td>&#8212;</td><td>&#8212;</td></tr>
<tr><td colspan="5">Class B holders (section label, below the header block)</td></tr>
<tr><td>Late Holder</td><td>7</td><td>*</td><td>&#8212;</td><td>&#8212;</td></tr>
</table>"""

V_SHAPE = """<table>
<tr><td>Name</td><td>Title of Class</td><td>Amount and Nature of Beneficial Ownership</td><td>Percent of Class</td></tr>
<tr><td>Jane Founder</td><td>Class A</td><td>1,000</td><td>1.0%</td></tr>
<tr><td></td><td>Class B</td><td>500,000</td><td>50.0%</td></tr>
</table>"""


TITLED = """<table>
<tr><td colspan="3">Beneficial ownership of Class A stock</td></tr>
<tr><td>Name</td><td>Shares</td><td>Percent</td></tr>
<tr><td>Jane Founder</td><td>500,000</td><td>50.0%</td></tr>
</table>"""


def labels(stacks: list[list[ShareLocation]]) -> list[list[str | None]]:
    return [[location_label(loc) for loc in per] for per in stacks]


def test_locator_reads_the_group_caption_above_the_located_column() -> None:
    holders = [("Jane Founder", Decimal(500000)), ("BlackRock, Inc.", Decimal(9000)), ("Late Holder", Decimal(7))]
    # Late Holder sits below a mid-table 'Class B holders' label: unlabelled, not A.
    assert labels(share_locations([H_SHAPE], holders)) == [["B"], ["A"], [None]]


def test_locator_on_a_v_shape_table_is_unlabelled() -> None:
    assert labels(share_locations([V_SHAPE], [("Jane Founder", Decimal(1000))])) == [[None]]


def test_locator_ignores_a_spanning_table_title() -> None:
    assert labels(share_locations([TITLED], [("Jane Founder", Decimal(500000))])) == [[None]]


def test_locator_never_matches_a_percent_cell() -> None:
    assert share_locations([H_SHAPE], [("Jane Founder", Decimal(50))]) == [[]]


def test_locator_unlocated_holder_and_empty_name() -> None:
    assert share_locations([H_SHAPE], [("Nobody", Decimal(1000)), ("", Decimal(1000))]) == [[], []]


def test_locator_one_location_per_table() -> None:
    assert len(share_locations([H_SHAPE, H_SHAPE], [("BlackRock, Inc.", Decimal(9000))])[0]) == 2


# ---------------------------------------------------------------------------
# Slice 3b — V-shape: the row's Item 403 *Title of class* cell
# ---------------------------------------------------------------------------

LEN_A, LEN_B = Sibling(1, "LEN"), Sibling(2, "LEN.B")
LENNAR = Cover("c-2", frozenset({("Class A Common Stock", "LEN"), ("Class B Common Stock", "LEN.B")}))


def v(
    cell: str, *, captions: tuple[str, ...] = ("Amount",), class_captions: tuple[str, ...] = ("Title of Class",)
) -> ShareLocation:
    return ShareLocation(captions, (), (), class_captions, (cell,))


def withheld(
    rows: dict[int, list[tuple[str, list[ShareLocation]]]],
    *,
    whole: set[int] | None = None,
    cover: Cover = LENNAR,
    siblings: list[Sibling] | None = None,
) -> set[tuple[int, str]]:
    out = decide_class_rows(
        accession_number="p-1",
        issuer_cik="0001",
        siblings=siblings or [LEN_A, LEN_B],
        cover=cover,
        row_locations=rows,
        whole=whole or set(),
    )
    assert all(r.reason == REASON_CLASS_ROW for r in out)
    return {(r.instrument_id, r.holder_name) for r in out}


def test_keyed_designators_separate_class_from_series() -> None:
    assert keyed_designators("Class B Common Stock") == {("class", "B")}
    assert keyed_designators("10% Series B") == {("series", "B")}
    assert designators("Series B") == {"B"}


def test_row_class_label() -> None:
    assert row_class_label(v("Class B Common Stock")) == ("class", "B", "Class B Common Stock")
    assert row_class_label(v("Liberty Global Class C")) == ("class", "C", "Liberty Global Class C")
    # The share column is itself class-captioned (H-shape) — slice 3's, not this rule's.
    assert row_class_label(v("Class B", captions=("Class A Common Stock",))) is None
    # A preferred series or a dividend rate in the cell or the class caption.
    assert row_class_label(v("10% Series B")) is None
    assert row_class_label(v("Series B Preferred Stock")) is None
    assert row_class_label(v("Class B", class_captions=("Title of Class", "6% Preferred Stock"))) is None
    # Zero or two letters; zero or two class cells; no class column at all.
    assert row_class_label(v("Class A Class B")) is None
    assert row_class_label(v("Common Stock")) is None
    assert row_class_label(ShareLocation(("Amount",), (), (), ("Title of Class",), ("Class A", "Class B"))) is None
    assert row_class_label(ShareLocation(("Amount",), (), ("Class B",))) is None
    # A mid-table non-common section label in the share column (Codex ckpt-2).
    assert (
        row_class_label(ShareLocation(("Amount",), ("Preferred Stockholders",), (), ("Title of Class",), ("Class B",)))
        is None
    )
    # A mid-table re-header in the share column.
    assert (
        row_class_label(ShareLocation(("Amount",), ("Class B holders",), (), ("Title of Class",), ("Class B",))) is None
    )


def test_row_class_key_needs_every_location_to_agree() -> None:
    assert row_class_key([v("Class B"), v("Class B")]) == ("class", "B", "Class B")
    assert row_class_key([v("Class A"), v("Class B")]) is None
    assert row_class_key([v("Class B"), v("Common")]) is None


def test_each_sibling_loses_the_other_classes_lines() -> None:
    rows = {
        1: [("Stuart Miller", [v("Class B Common Stock")]), ("BlackRock", [v("Class A Common Stock")])],
        2: [("Stuart Miller", [v("Class B Common Stock")]), ("BlackRock", [v("Class A Common Stock")])],
    }
    assert withheld(rows) == {(1, "Stuart Miller"), (2, "BlackRock")}


def test_unbound_rows_and_whole_accession_suppressions_keep() -> None:
    rows = {1: [("Nameless", []), ("Two", [v("Class A"), v("Class B")]), ("Plain", [v("Common Stock")])]}
    assert withheld(rows) == set()
    assert withheld({1: [("Stuart Miller", [v("Class B")])]}, whole={1}) == set()


def test_a_letter_without_a_witness_or_of_another_keyword_keeps() -> None:
    # Class C is on no cover; Series B is not the cover's Class B.
    assert withheld({1: [("X", [v("Class C")]), ("Y", [v("Series B")])]}) == set()


def test_undesignated_sibling_is_never_withheld() -> None:
    cover = Cover("c", frozenset({("Common Stock", "UHAL"), ("Class B Common Stock", "UHAL.B")}))
    sibs = [Sibling(1, "UHAL"), Sibling(2, "UHAL.B")]
    assert withheld({1: [("X", [v("Class B")])]}, cover=cover, siblings=sibs) == set()


def test_locator_reads_the_title_of_class_cell() -> None:
    holders = [("Jane Founder", Decimal(1000))]
    [[loc]] = share_locations([V_SHAPE], holders)
    assert loc.class_captions == ("Title of Class",)
    assert loc.class_cells == ("Class A",)
    assert row_class_label(loc) == ("class", "A", "Class A")
    # No Title-of-class column: no class cells.
    [[h]] = share_locations([H_SHAPE], [("BlackRock, Inc.", Decimal(9000))])
    assert h.class_cells == ()


def test_locator_excludes_a_name_cell_under_the_class_caption() -> None:
    table = """<table>
<tr><td>Title of Class</td><td>Name</td><td>Amount</td></tr>
<tr><td>Class B Common Stock</td><td></td><td>5</td></tr>
</table>"""
    [[loc]] = share_locations([table], [("Class B Common Stock", Decimal(5))])
    assert loc.class_cells == ()
