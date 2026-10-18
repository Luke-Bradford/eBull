"""#3227 — a Form 4's indirect observation sums its vehicles' balances only where the filing's
arithmetic does not contradict per-form column 5 (spec
``docs/specs/ownership/2026-10-17-3227-form4-vehicle-balance.md``); otherwise the latest line
stands."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from app.services.insider_transactions import FORM4_VEHICLE_SUMS, form4_observation_shares

_CIK = "0001"


@dataclass(frozen=True)
class _Line:
    txn_row_num: int
    shares: Decimal | None
    acquired_disposed_code: str | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    security_title: str | None = "Common Stock"
    txn_date: date | None = date(2026, 6, 1)
    txn_date_invalid: bool = False
    is_derivative: bool = False


def _lines(*specs: tuple[str | None, int | None, int | None, str | None, str | None]) -> list[_Line]:
    """(A/D code, shares, post, D/I, nature) per line, ``txn_row_num`` in order."""
    return [
        _Line(
            txn_row_num=i,
            acquired_disposed_code=ad,
            shares=Decimal(sh) if sh is not None else None,
            post_transaction_shares=Decimal(post) if post is not None else None,
            direct_indirect=di,
            nature_of_ownership=nat,
        )
        for i, (ad, sh, post, di, nat) in enumerate(specs)
    ]


def _verdict(
    lines: list[_Line],
    *,
    document_type: str = "4",
    owners: tuple[str, ...] = (_CIK,),
    history: Sequence[tuple[str | None, str | None, Decimal]] | None = None,
) -> tuple[str, Decimal | None]:
    return form4_observation_shares(
        lines,
        document_type=document_type,
        filer_cik=_CIK,
        owner_ciks=owners,
        earlier_balances=lambda _title: [] if history is None else history,
    )


# MNSO 0001104659-26-069242 (Ye Guofu): Mini chains within its own form; YGF MN opens at its
# Form 3 balance. The writer kept Mini's 315,740,482 alone.
_MNSO = _lines(
    ("A", 1_300_000, 315_590_482, "I", "by Mini Investment Limited"),
    ("A", 150_000, 315_740_482, "I", "by Mini Investment Limited"),
    ("A", 650_000, 195_115_382, "I", "by YGF MN LIMITED"),
)

# WOOF 0000950170-25-048788: one line per vehicle; the writer kept "By son" 7,874.
_WOOF = _lines(
    ("D", 100_000, 853_304, "I", "By 2020 Trust"),
    ("A", 100_000, 724_910, "I", "By 2025 Trust"),
    ("A", 7_874, 7_874, "I", "By son"),
)


def test_mnso_vehicles_sum() -> None:
    assert _verdict(_MNSO) == (FORM4_VEHICLE_SUMS, Decimal(510_855_864))


def test_woof_vehicles_sum() -> None:
    assert _verdict(_WOOF) == (FORM4_VEHICLE_SUMS, Decimal(1_586_088))


def test_a_4a_sums() -> None:
    assert _verdict(_WOOF, document_type="4/A") == (FORM4_VEHICLE_SUMS, Decimal(1_586_088))


def test_form5_is_out_of_scope() -> None:
    assert _verdict(_WOOF, document_type="5") == ("out_of_scope", None)


def test_null_cik_is_out_of_scope() -> None:
    verdict = form4_observation_shares(
        _WOOF, document_type="4", filer_cik=None, owner_ciks=[None], earlier_balances=lambda _t: []
    )
    assert verdict == ("out_of_scope", None)


def test_one_indirect_form_is_not_a_candidate() -> None:
    lines = _lines(("A", 10, 110, "D", None), ("A", 5, 105, "I", "By Trust"), ("A", 5, 110, "I", "by trust"))
    assert _verdict(lines) == ("1 one_form", None)


def test_abdiel_running_total_in_the_filing_refuses() -> None:
    # 0000950170-23-052960: "Abdiel Capital LP" opens at the Master Fund's 13,586,608.
    lines = _lines(
        ("D", 1_000, 13_586_608, "I", "Abdiel Qualified Master Fund"),
        ("D", 305, 13_586_303, "I", "Abdiel Capital LP"),
    )
    assert _verdict(lines) == ("5b running_total", None)


def test_reverse_order_running_total_refuses() -> None:
    # The first row opens at the SECOND row's (another form's) balance: row order is not proven
    # chronological, so either direction refuses.
    lines = _lines(("D", 305, 13_586_303, "I", "Fund B"), ("D", 1_000, 13_586_608, "I", "Fund A"))
    assert _verdict(lines) == ("5b running_total", None)


def test_zero_balance_opening_does_not_refuse() -> None:
    # A new vehicle opening at 0 matches another form's 0 balance, which adds nothing.
    lines = _lines(("D", 50, 0, "I", "By Trust A"), ("A", 70, 70, "I", "By Trust B"))
    assert _verdict(lines) == (FORM4_VEHICLE_SUMS, Decimal(70))


def test_running_total_across_filings_refuses() -> None:
    # Form B opens at 900, which the filer last reported as Form A's balance, never as Form B's.
    lines = _lines(("A", 100, 500, "I", "By A"), ("A", 100, 1_000, "I", "By B"))
    history = [("I", "By A", Decimal(900)), ("I", "By A", Decimal(400))]
    assert _verdict(lines, history=history) == ("5c running_total_across_filings", None)


def test_a_same_form_match_clears_an_other_form_match() -> None:
    lines = _lines(("A", 100, 500, "I", "By A"), ("A", 100, 1_000, "I", "By B"))
    history = [("I", "By A", Decimal(900)), ("I", "by b", Decimal(900)), ("I", "By A", Decimal(400))]
    assert _verdict(lines, history=history) == (FORM4_VEHICLE_SUMS, Decimal(1_500))


def test_a_direct_balance_counts_as_another_form() -> None:
    lines = _lines(("A", 100, 500, "I", "By A"), ("A", 100, 1_000, "I", "By B"))
    assert _verdict(lines, history=[("D", None, Decimal(900))]) == ("5c running_total_across_filings", None)


def test_no_filing_time_refuses() -> None:
    lines = _lines(("A", 100, 500, "I", "By A"), ("A", 100, 1_000, "I", "By B"))
    verdict = form4_observation_shares(
        lines, document_type="4", filer_cik=_CIK, owner_ciks=[_CIK], earlier_balances=lambda _t: None
    )
    assert verdict == ("5c running_total_across_filings", None)


def test_two_trust_sequences_that_break_the_chain_refuse() -> None:
    # Two vehicles both normalised to "bytrust", interleaved: within the form, the second line
    # does not open at the first line's balance.
    lines = _lines(
        ("A", 10, 110, "I", "By Trust"),
        ("A", 10, 210, "I", "By Trust"),
        ("A", 5, 55, "I", "By LLC"),
    )
    assert _verdict(lines) == ("5a form_breaks_chain", None)


def test_block_balances_refuse() -> None:
    # Lines repeating one total for the form, rather than chaining.
    lines = _lines(
        ("A", 10, 1_000, "I", "By Fund A"),
        ("A", 20, 1_000, "I", "By Fund A"),
        ("A", 5, 55, "I", "By Fund B"),
    )
    assert _verdict(lines) == ("5a form_breaks_chain", None)


def test_equal_finals_across_d_and_i_refuse() -> None:
    lines = _lines(("A", 10, 500, "D", None), ("A", 20, 500, "I", "By LLC"), ("A", 5, 55, "I", "By Trust"))
    assert _verdict(lines) == ("6 equal_finals", None)


def test_equal_sibling_grats_refuse() -> None:
    # 0000950142-24-002955: equal gifts to sibling GRATs keep a single line.
    lines = _lines(("A", 50_000, 50_000, "I", "By GRAT 1"), ("A", 50_000, 50_000, "I", "By GRAT 2"))
    assert _verdict(lines) == ("6 equal_finals", None)


def test_null_acquired_disposed_code_refuses() -> None:
    lines = _lines(("A", 10, 110, "I", "By A"), (None, 5, 55, "I", "By B"))
    assert _verdict(lines) == ("2 incomplete", None)


def test_whitespace_only_title_refuses() -> None:
    lines = [*_lines(("A", 10, 110, "I", "By A")), _Line(1, Decimal(5), "A", Decimal(55), "I", "By B", "  ")]
    assert _verdict(lines) == ("2 incomplete", None)


def test_a_d_line_with_no_balance_refuses() -> None:
    # Completeness is over every line of the unit, including the ones the writers skip.
    lines = _lines(("A", 10, None, "D", None), ("A", 10, 110, "I", "By A"), ("A", 5, 55, "I", "By B"))
    assert _verdict(lines) == ("2 incomplete", None)


def test_a_blank_nature_refuses() -> None:
    lines = _lines(("A", 10, 110, "I", "By A"), ("A", 5, 55, "I", "  "))
    assert _verdict(lines) == ("2 incomplete", None)


def test_negative_opening_refuses() -> None:
    lines = _lines(("A", 200, 110, "I", "By A"), ("A", 5, 55, "I", "By B"))
    assert _verdict(lines) == ("2 incomplete", None)


def test_two_titles_refuse() -> None:
    lines = [*_lines(("A", 10, 110, "I", "By A")), _Line(1, Decimal(5), "A", Decimal(55), "I", "By B", "Class B")]
    assert _verdict(lines) == ("3 multi_title", None)


def test_joint_filing_refuses() -> None:
    assert _verdict(_WOOF, owners=(_CIK, "0002")) == ("4 joint_or_owner_mismatch", None)


def test_owner_set_mismatch_refuses() -> None:
    assert _verdict(_WOOF, owners=("0002",)) == ("4 joint_or_owner_mismatch", None)


def test_derivative_lines_are_ignored() -> None:
    deriv = _Line(9, None, None, None, "I", "", is_derivative=True)
    assert _verdict([*_WOOF, deriv]) == (FORM4_VEHICLE_SUMS, Decimal(1_586_088))


def test_latest_balance_per_form_is_by_date_then_row() -> None:
    # Form A's row 1 is dated before row 0: the chain is by row, the form's final by date.
    lines = [
        _Line(0, Decimal(10), "A", Decimal(110), "I", "By A", txn_date=date(2026, 6, 2)),
        _Line(1, Decimal(5), "A", Decimal(115), "I", "By A", txn_date=date(2026, 6, 1)),
        _Line(2, Decimal(5), "A", Decimal(55), "I", "By B"),
    ]
    assert _verdict(lines) == (FORM4_VEHICLE_SUMS, Decimal(165))
