"""#3227 — the Form 3 XML writers sum a (filer, D/I) group's Table I lines only when they are
provably distinct holdings of one class; otherwise the last line by ``row_num`` stands."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.services.insider_form3_ingest import form3_observation_shares
from app.services.ownership_observations_sync import _StoredForm3Line


@dataclass(frozen=True)
class _Line:
    row_num: int | None
    shares: Decimal | None
    direct_indirect: str | None = "I"
    nature_of_ownership: str | None = None
    security_title: str | None = "Ordinary shares"
    filer_cik: str | None = "0001"
    is_derivative: bool = False


def _lines(*specs: tuple[int, str | None, str | None, str | None]) -> list[_Line]:
    """(shares, D/I, nature, title) per line, row_num in order."""
    return [
        _Line(row_num=i, shares=Decimal(sh), direct_indirect=di, nature_of_ownership=nat, security_title=title)
        for i, (sh, di, nat, title) in enumerate(specs)
    ]


def test_distinct_vehicles_of_one_class_sum() -> None:
    # MNSO, Ye Guofu: five I lines of one class, one per holding vehicle.
    lines = _lines(
        (314_290_482, "I", "by Mini Investment Limited", "Ordinary shares"),
        (14_000_000, "I", "by Mini Investments SP1 Limited", "Ordinary shares"),
        (8_936_000, "I", "by YGF MC LIMITED", "Ordinary shares"),
        (194_465_382, "I", "by YGF MN LIMITED", "Ordinary shares"),
        (257_849_197, "I", "by YYY MC LIMITED", "Ordinary shares"),
    )
    assert form3_observation_shares(lines, owner_count=1) == {("0001", "I"): Decimal(789_541_061)}


def test_d_and_i_groups_stay_separate_observations() -> None:
    lines = _lines((100, "D", None, "Common"), (20, "I", "By Trust A", "Common"), (30, "I", "By Trust B", "Common"))
    assert form3_observation_shares(lines, owner_count=1) == {("0001", "D"): Decimal(100), ("0001", "I"): Decimal(50)}


def test_two_classes_keep_the_last_line() -> None:
    # CNH: common and special voting shares are different classes and never sum.
    lines = _lines((10, "I", "By A", "Common Shares"), (20, "I", "By B", "Special Voting Shares"))
    assert form3_observation_shares(lines, owner_count=1) == {("0001", "I"): Decimal(20)}


def test_joint_filing_keeps_the_last_line() -> None:
    lines = _lines((10, "I", "By A", "Common"), (20, "I", "By B", "Common"))
    assert form3_observation_shares(lines, owner_count=2) == {("0001", "I"): Decimal(20)}


def test_equal_amount_across_d_and_i_refuses() -> None:
    # YPF: one block restated D and I (Instr. 5(b)(iv)); a second I line must not pull it in.
    lines = _lines((58_603_606, "D", None, "ADS"), (58_603_606, "I", "By P", "ADS"), (5, "I", "By Q", "ADS"))
    assert form3_observation_shares(lines, owner_count=1) == {
        ("0001", "D"): Decimal(58_603_606),
        ("0001", "I"): Decimal(5),
    }


def test_equal_amounts_of_another_class_do_not_block() -> None:
    lines = _lines((7, "D", None, "Preferred"), (7, "I", "By A", "Common"), (3, "I", "By B", "Common"))
    assert form3_observation_shares(lines, owner_count=1)[("0001", "I")] == Decimal(10)


def test_repeated_nature_sums() -> None:
    # Two unequal D lines of one class are two direct holdings (ASTE 0001140361-18-001899).
    lines = _lines((10, "D", None, "Common"), (20, "D", None, "Common"))
    assert form3_observation_shares(lines, owner_count=1) == {("0001", "D"): Decimal(30)}


def test_blank_title_or_negative_shares_refuse() -> None:
    blank = _lines((10, "I", "By A", " "), (20, "I", "By B", " "))
    assert form3_observation_shares(blank, owner_count=1) == {("0001", "I"): Decimal(20)}
    negative = _lines((-10, "I", "By A", "Common"), (20, "I", "By B", "Common"))
    assert form3_observation_shares(negative, owner_count=1) == {("0001", "I"): Decimal(20)}


def test_last_line_is_by_row_num_not_input_order() -> None:
    lines = [
        _Line(row_num=1, shares=Decimal(20), security_title="A", nature_of_ownership="x"),
        _Line(row_num=0, shares=Decimal(10), security_title="B", nature_of_ownership="y"),
    ]
    assert form3_observation_shares(lines, owner_count=1) == {("0001", "I"): Decimal(20)}


def test_derivative_and_shareless_lines_are_ignored() -> None:
    lines = [
        _Line(row_num=0, shares=Decimal(10), nature_of_ownership="By A"),
        _Line(row_num=1, shares=None, nature_of_ownership="By B"),
        _Line(row_num=2, shares=Decimal(99), nature_of_ownership="By C", is_derivative=True),
        _Line(row_num=3, shares=Decimal(5), nature_of_ownership="By D"),
    ]
    assert form3_observation_shares(lines, owner_count=1) == {("0001", "I"): Decimal(15)}


def test_stored_line_strips_the_cik_like_the_old_writer() -> None:
    row = {
        "row_num": 0,
        "is_derivative": False,
        "filer_cik": " 0001 ",
        "security_title": "Common",
        "shares": "12.5",
        "direct_indirect": "I",
    }
    line = _StoredForm3Line.from_row(row)
    assert (line.filer_cik, line.shares) == ("0001", Decimal("12.5"))
    assert _StoredForm3Line.from_row({**row, "filer_cik": "  "}).filer_cik is None


def test_unknown_direct_indirect_refuses() -> None:
    # Codex ckpt-2: the caller labels a None D/I ``direct``; never sum lines of unknown form.
    lines = _lines((10, None, "By A", "Common"), (20, None, "By B", "Common"))
    assert form3_observation_shares(lines, owner_count=1) == {("0001", None): Decimal(20)}
