"""#2351 M1 — Item 403 lines at (holder, title of class) grain. Pure."""

from __future__ import annotations

from decimal import Decimal

from app.services.def14a_item403_lines import (
    Item403Line,
    _full_key,
    _table_lines,
    class_evidence,
    item403_lines,
    keyed_designators,
)


def _keys(*names: str) -> dict[str, list[tuple[str, str | None]]]:
    out: dict[str, list[tuple[str, str | None]]] = {}
    for name in names:
        out.setdefault(_full_key(name), []).append((name, None))
    return out


def _lines(table: str, *names: str) -> list[Item403Line]:
    return _table_lines(0, table, _keys(*names))


def _view(lines: list[Item403Line]) -> list[tuple[str, str, list[str], list[str]]]:
    return [
        (ln.holder_name, ln.class_state, sorted(ln.group_evidence), [c.raw_text for c in ln.amount_cells])
        for ln in lines
    ]


META_SHAPE = """<table>
<tr><td>Name of Beneficial Owner</td><td colspan="4">Shares Beneficially Owned</td>
<td>% of Total Voting Power</td></tr>
<tr><td>Name of Beneficial Owner</td><td colspan="2">Class A</td><td colspan="2">Class B</td>
<td>% of Total Voting Power</td></tr>
<tr><td>Name of Beneficial Owner</td><td>Shares</td><td>%</td><td>Shares</td><td>%</td>
<td>% of Total Voting Power</td></tr>
<tr><td>Mark Founder (2)</td><td>141,000</td><td>*</td><td>342,606,985</td><td>99.8</td><td>61.0</td></tr>
<tr><td>Susan Officer (3)</td><td>125,163</td><td>*</td><td>&#8212;</td><td>&#8212;</td><td>*</td></tr>
</table>"""


def test_h_shape_yields_one_line_per_class_group_plus_the_voting_column() -> None:
    assert _view(_lines(META_SHAPE, "Mark Founder", "Susan Officer")) == [
        ("Mark Founder", "labelled", ["class:A"], ["141,000", "*"]),
        ("Mark Founder", "labelled", ["class:B"], ["342,606,985", "99.8"]),
        ("Mark Founder", "unlabelled", [], ["61.0"]),
        ("Susan Officer", "labelled", ["class:A"], ["125,163", "*"]),
        ("Susan Officer", "labelled", ["class:B"], ["—", "—"]),
        ("Susan Officer", "unlabelled", [], ["*"]),
    ]


def test_a_star_is_kept_raw_and_never_converted() -> None:
    star = _lines(META_SHAPE, "Mark Founder")[0].amount_cells[1]
    assert (star.raw_text, star.shares, star.percent) == ("*", None, None)


V_SHAPE = """<table>
<tr><td>Name</td><td>Title of Class</td><td>Amount and Nature of Beneficial Ownership</td><td>Percent of Class</td></tr>
<tr><td>Jane Founder</td><td>Class A Common Stock</td><td>1,000</td><td>1.0%</td></tr>
<tr><td></td><td>Class B Common Stock</td><td>500,000</td><td>50.0%</td></tr>
<tr><td>Unaccepted Person</td><td>Class B Common Stock</td><td>9</td><td>*</td></tr>
<tr><td></td><td>Class A Common Stock</td><td>8</td><td>*</td></tr>
</table>"""


def test_v_shape_continuation_belongs_to_the_holder_above_and_carries_its_class() -> None:
    lines = _lines(V_SHAPE, "Jane Founder")
    assert [(ln.continuation, ln.row_class_cells, ln.class_state) for ln in lines] == [
        (False, ("Class A Common Stock",), "labelled"),
        (True, ("Class B Common Stock",), "labelled"),
    ]
    assert [c.shares for c in lines[1].amount_cells] == [Decimal(500000), None]


def test_a_row_naming_an_unaccepted_owner_ends_the_continuation() -> None:
    assert all(ln.holder_name == "Jane Founder" for ln in _lines(V_SHAPE, "Jane Founder"))
    assert len(_lines(V_SHAPE, "Jane Founder")) == 2


S_SHAPE = """<table>
<tr><td>Name</td><td>Shares</td><td>Percent</td></tr>
<tr><td colspan="3">Class B Common Stock</td></tr>
<tr><td>Jane Founder</td><td>500,000</td><td>50.0%</td></tr>
<tr><td colspan="3">Common Stock</td></tr>
<tr><td>BlackRock, Inc.</td><td>9,000</td><td>9.0%</td></tr>
<tr><td colspan="3">Class A and Class B holders combined</td></tr>
<tr><td>Late Holder</td><td>7</td><td>*</td></tr>
</table>"""


def test_section_labels_apply_below_reset_on_common_and_ignore_two_class_titles() -> None:
    lines = _lines(S_SHAPE, "Jane Founder", "BlackRock, Inc.", "Late Holder")
    assert [(ln.holder_name, ln.section_label) for ln in lines] == [
        ("Jane Founder", "Class B Common Stock"),
        ("BlackRock, Inc.", "Common Stock"),
        ("Late Holder", "Common Stock"),
    ]


def test_holders_sharing_a_long_common_prefix_are_told_apart() -> None:
    table = """<table>
<tr><td>Name</td><td>Shares</td><td>Percent</td></tr>
<tr><td>Entities affiliated with BlackRock, Inc. (5)</td><td>9,000</td><td>9.0%</td></tr>
<tr><td>Entities affiliated with Vanguard (6)</td><td>8,000</td><td>8.0%</td></tr>
</table>"""
    lines = _lines(table, "Entities affiliated with BlackRock, Inc.", "Entities affiliated with Vanguard")
    assert [(ln.holder_name, ln.amount_cells[0].raw_text) for ln in lines] == [
        ("Entities affiliated with BlackRock, Inc.", "9,000"),
        ("Entities affiliated with Vanguard", "8,000"),
    ]


def test_one_person_emitted_under_two_spellings_goes_to_the_first() -> None:
    table = """<table>
<tr><td>Name</td><td>Shares</td></tr>
<tr><td>John Smith ........</td><td>9,000</td></tr>
</table>"""
    keys = _keys("John Smith ....", "John Smith ........")
    assert [ln.holder_name for ln in _table_lines(0, table, keys)] == ["John Smith ...."]


def test_an_expanded_colspan_amount_is_one_cell() -> None:
    table = """<table>
<tr><td>Name</td><td colspan="2">Shares</td><td>Percent</td></tr>
<tr><td>Jane Founder</td><td colspan="2">1,000</td><td>1.0%</td></tr>
</table>"""
    assert [c.raw_text for c in _lines(table, "Jane Founder")[0].amount_cells] == ["1,000", "1.0%"]


def test_a_row_class_cell_under_another_class_caption_is_a_conflict() -> None:
    table = """<table>
<tr><td>Name</td><td>Title of Class</td><td>Class B Shares</td></tr>
<tr><td>Jane Founder</td><td>Class A Common Stock</td><td>1,000</td></tr>
</table>"""
    assert _lines(table, "Jane Founder")[0].class_state == "conflict"


def test_class_a_preferred_is_not_class_a_common() -> None:
    assert class_evidence(["Class A Preferred Stock"]) != class_evidence(["Class A Common Stock"])


def test_keyed_designators_switch_keyword_inside_a_list() -> None:
    assert keyed_designators("Class A and Series B") == {("class", "A"), ("series", "B")}
    assert keyed_designators("Classes A and B") == {("class", "A"), ("class", "B")}
    assert keyed_designators("Class A-1 and Class II") == frozenset()


def test_single_class_table_yields_one_unlabelled_line_per_holder() -> None:
    table = """<table>
<tr><td>Name</td><td>Shares Beneficially Owned</td><td>Percent of Class</td></tr>
<tr><td>Jane Founder</td><td>1,000</td><td>1.0%</td></tr>
</table>"""
    assert _view(_lines(table, "Jane Founder")) == [("Jane Founder", "unlabelled", [], ["1,000", "1.0%"])]


def test_empty_document_has_no_lines() -> None:
    assert item403_lines("").lines == []


def test_units_and_rights_in_an_amount_caption_are_nature_not_class() -> None:
    table = """<table>
<tr><td>Name</td><td>Common Stock Beneficially Owned</td><td>Restricted Stock Units</td><td>Percent</td></tr>
<tr><td>Jane Founder</td><td>1,000</td><td>200</td><td>1.0%</td></tr>
</table>"""
    lines = _lines(table, "Jane Founder")
    assert len(lines) == 1
    assert lines[0].class_state == "unlabelled"


def test_a_zero_width_leading_cell_is_not_the_owner_cell() -> None:
    table = """<table>
<tr><td>​</td><td>Name</td><td>Shares</td><td>Percent</td></tr>
<tr><td>​</td><td>Jane Founder (1)</td><td>1,000</td><td>1.0 %</td></tr>
</table>"""
    assert [c.raw_text for c in _lines(table, "Jane Founder")[0].amount_cells] == ["1,000", "1.0 %"]


def test_a_title_of_class_column_left_of_the_name_is_not_the_owner_cell() -> None:
    table = """<table>
<tr><td>Title of Class</td><td>Name and Address of Beneficial Owner</td><td>Amount</td><td>Percent of Class</td></tr>
<tr><td>Common Stock</td><td>Melissa Barra</td><td>131,665</td><td>*</td></tr>
<tr><td>Class B</td><td></td><td>7</td><td>*</td></tr>
</table>"""
    lines = _lines(table, "Melissa Barra")
    assert [(ln.continuation, ln.row_class_cells, ln.amount_cells[0].raw_text) for ln in lines] == [
        (False, ("Common Stock",), "131,665"),
        (True, ("Class B",), "7"),
    ]


def test_figures_on_the_row_below_the_name_and_address_belong_to_that_holder() -> None:
    table = """<table>
<tr><td>Name and Address</td><td>Shares</td><td>Percent</td></tr>
<tr><td>FMR LLC</td><td></td><td></td></tr>
<tr><td>245 Summer Street</td><td>2,372,387</td><td>7.1%</td></tr>
<tr><td>Jane Founder</td><td></td><td></td></tr>
<tr><td colspan="3">Class B Common Stock</td></tr>
<tr><td></td><td>9</td><td>*</td></tr>
</table>"""
    lines = _lines(table, "FMR LLC", "Jane Founder")
    assert [(ln.holder_name, ln.continuation, ln.amount_cells[0].raw_text) for ln in lines] == [
        ("FMR LLC", True, "2,372,387"),
    ]


def test_a_zero_width_space_inside_an_amount_is_ignored() -> None:
    table = """<table>
<tr><td>Name</td><td>Shares</td></tr>
<tr><td>Glenn Reed</td><td>​17,779</td></tr>
</table>"""
    assert _lines(table, "Glenn Reed")[0].amount_cells[0].shares == Decimal(17779)


def test_a_replaced_section_label_stops_counting_as_evidence() -> None:
    table = """<table>
<tr><td>Name</td><td>Shares</td><td>Percent</td></tr>
<tr><td colspan="3">Class B Common Stock</td></tr>
<tr><td>Jane</td><td>2</td><td>2%</td></tr>
<tr><td colspan="3">Common Stock</td></tr>
<tr><td>Joe</td><td>3</td><td>3%</td></tr>
</table>"""
    joe = _lines(table, "Jane", "Joe")[1]
    assert (joe.section_label, joe.group_evidence) == ("Common Stock", frozenset())


def test_a_percent_only_table_still_yields_lines() -> None:
    table = """<table>
<tr><td>Name</td><td>Shares</td><td>Percent of Class</td></tr>
<tr><td>Jane Founder</td><td>&#8212;</td><td>5.0%</td></tr>
</table>"""
    # The dash sits in a column that never holds a figure, so it is not provably an amount.
    assert [c.raw_text for c in _lines(table, "Jane Founder")[0].amount_cells] == ["5.0%"]
