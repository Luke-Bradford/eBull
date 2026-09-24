"""#2351 M2 — pure binding rules (spec: docs/proposals/ownership/2026-09-24-2351-m2-binding.md)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.def14a_binding import Identity, common_identity, decide_row, figure, relation
from app.services.def14a_item403_lines import AmountCell, Item403Line
from app.services.def14a_recipients import Cover, Sibling

CLASS_A = Identity(("class", "A"), "Class A Common Stock", "c1")
PLAIN = Identity(None, "Common Stock", "c1")


def _cell(shares: str | None = None, percent: str | None = None, col: int = 1, raw: str = "") -> AmountCell:
    return AmountCell(
        col,
        (),
        (),
        raw or shares or percent or "",
        None if shares is None else Decimal(shares),
        None if percent is None else Decimal(percent),
    )


def _line(
    *cells: AmountCell,
    group: frozenset[str] = frozenset(),
    class_cells: tuple[str, ...] = (),
    label: str | None = None,
    table: int = 0,
    row: int = 5,
    state: str = "labelled",
) -> Item403Line:
    return Item403Line("Jane Doe", None, table, row, False, class_cells, label, group, cells, state)


A = frozenset({"class:A"})
B = frozenset({"class:B"})


# --- identity ---------------------------------------------------------------------------


def _cover(*pairs: tuple[str, str]) -> Cover:
    return Cover("c1", frozenset(pairs))


def test_identity_lettered() -> None:
    goog = [Sibling(1, "GOOGL"), Sibling(2, "GOOG")]
    cover = _cover(("Class A Common Stock", "GOOGL"), ("Class C Capital Stock", "GOOG"))
    assert common_identity(goog[0], goog, cover) == Identity(("class", "A"), "Class A Common Stock", "c1")


def test_identity_plain_sole_common() -> None:
    me = Sibling(1, "XYZ")
    assert common_identity(me, [me], _cover(("Common Stock", "XYZ"))) == Identity(None, "Common Stock", "c1")


def test_identity_plain_beside_another_common_is_unresolved() -> None:
    sibs = [Sibling(1, "XYZ"), Sibling(2, "XYZB")]
    cover = _cover(("Common Stock", "XYZ"), ("Class B Common Stock", "XYZB"))
    assert common_identity(sibs[0], sibs, cover) == "plain_not_sole_common"


@pytest.mark.parametrize(
    ("title", "reason"),
    [
        ("Class A-1 Common Stock", "unsupported_title"),
        ("Classes II and III Common Stock", "unsupported_title"),
        ("American Depositary Shares, each representing one Class A ordinary share", "unsupported_title"),
        ("Class A Common Stock Linked Notes", "unsupported_title"),
        ("Warrants to purchase Class A Common Stock", "not_common"),
    ],
)
def test_identity_unresolved(title: str, reason: str) -> None:
    me = Sibling(1, "XYZ")
    assert common_identity(me, [me], _cover((title, "XYZ"))) == reason


def test_identity_not_on_cover_and_several_titles() -> None:
    me = Sibling(1, "XYZ")
    assert common_identity(me, [me], _cover(("Common Stock", "ABC"))) == "not_on_cover"
    two = _cover(("Common Stock", "XYZ"), ("Class A Common Stock", "XYZ"))
    assert common_identity(me, [me], two) == "several_titles"


# --- relation ---------------------------------------------------------------------------


def test_relation_lettered() -> None:
    assert relation(_line(group=A), CLASS_A) == "match"
    assert relation(_line(group=B), CLASS_A) == "mismatch"
    assert relation(_line(group=frozenset({"series:A"})), CLASS_A) == "neutral"
    assert relation(_line(state="unlabelled"), CLASS_A) == "neutral"
    assert relation(_line(group=frozenset({"class:A", "class:B"}), state="conflict"), CLASS_A) == "neutral"
    assert relation(_line(group=frozenset({"preferred"})), CLASS_A) == "mismatch"


def test_relation_forced_neutral() -> None:
    assert relation(_line(class_cells=("7% Series B",)), CLASS_A) == "neutral"
    assert relation(_line(class_cells=("Class A-1 Common",)), CLASS_A) == "neutral"
    assert relation(_line(class_cells=("Class B Notes",)), CLASS_A) == "neutral"


def test_relation_plain() -> None:
    assert relation(_line(label="Common Stock"), PLAIN) == "match"
    assert relation(_line(group=B), PLAIN) == "neutral"
    assert relation(_line(group=frozenset({"preferred"})), PLAIN) == "mismatch"


# --- figure -----------------------------------------------------------------------------


def test_figure_needs_exactly_one_positive_share_cell() -> None:
    assert figure(_line(_cell("100"), _cell(percent="5"))) == Decimal("100")
    assert figure(_line(_cell("100"), _cell("20", col=2))) is None
    assert figure(_line(_cell("0"))) is None
    assert figure(_line(_cell(raw="—"))) is None
    assert figure(_line(_cell("48.176"))) is None  # "48,176" typed with a decimal point
    assert figure(_line(_cell("100.0000"))) == Decimal("100.0000")


# --- decide_row -------------------------------------------------------------------------


def test_confirmed_when_legacy_sits_in_a_match_line() -> None:
    lines = [_line(_cell("141000"), group=A), _line(_cell("342606985"), group=B)]
    assert decide_row(legacy_shares=Decimal("141000"), identity=CLASS_A, lines=lines).outcome == "confirmed"


def test_rebind_when_legacy_is_another_class_and_one_match_line_has_a_figure() -> None:
    lines = [_line(_cell("141000"), group=A), _line(_cell("342606985"), group=B)]
    d = decide_row(legacy_shares=Decimal("342606985"), identity=CLASS_A, lines=lines)
    assert (d.outcome, d.shares) == ("rebind", Decimal("141000"))


def test_withhold_when_own_class_cell_is_a_dash() -> None:
    lines = [_line(_cell(raw="—"), group=A), _line(_cell("389051160"), group=B)]
    d = decide_row(legacy_shares=Decimal("389051160"), identity=CLASS_A, lines=lines)
    assert d.outcome == "withhold"


def test_no_rebind_across_tables() -> None:
    lines = [_line(_cell("5"), group=A, table=1), _line(_cell("9"), group=B, table=0)]
    assert decide_row(legacy_shares=Decimal("9"), identity=CLASS_A, lines=lines).outcome == "withhold"


def test_plain_identity_never_rebinds() -> None:
    lines = [_line(_cell("5"), label="Common Stock"), _line(_cell("9"), group=frozenset({"preferred"}))]
    assert decide_row(legacy_shares=Decimal("9"), identity=PLAIN, lines=lines).outcome == "withhold"


def test_any_neutral_source_keeps() -> None:
    lines = [_line(_cell("9"), group=B), _line(_cell("9"), state="unlabelled", row=9)]
    d = decide_row(legacy_shares=Decimal("9"), identity=CLASS_A, lines=lines)
    assert (d.outcome, d.reason) == ("keep", "abstained:ambiguous")


@pytest.mark.parametrize(
    ("shares", "lines", "reason"),
    [
        (None, [_line(_cell("9"), group=B)], "abstained:no_figure"),
        (Decimal("9"), [], "abstained:no_line"),
        (Decimal("9"), [_line(_cell("8"), group=B)], "abstained:no_source"),
    ],
)
def test_abstentions(shares: Decimal | None, lines: list[Item403Line], reason: str) -> None:
    assert decide_row(legacy_shares=shares, identity=CLASS_A, lines=lines).reason == reason
