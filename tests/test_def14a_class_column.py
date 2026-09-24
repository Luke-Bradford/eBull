"""#2351 slice 3 — pure tests for the dual-class class-column rule and its locator."""

from __future__ import annotations

from decimal import Decimal

from app.providers.implementations.sec_def14a import ShareLocation, share_locations
from app.services.def14a_recipients import (
    REASON_CLASS_COLUMN,
    Cover,
    Sibling,
    decide_class_column,
    designators,
    location_label,
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
