"""Unit tests for the DEF 14A Item 402(c) Summary Compensation Table parser
(#1945 — ``parse_summary_compensation_table``).

Fixture HTML is hand-built to reproduce the exact structural traps observed
on real full-population proxies (AAPL / HD / JPM / MSFT and a 400-body dev-DB
scan) without pulling production payloads into the repo. Each scenario pins a
distinct hazard the reg-fixed-order parser must survive:

  * name-cell ``rowspan`` → continuation-year rows are index-shifted (AAPL)
  * year folded into the name column, ``—`` for a null bonus (HD)
  * lone ``$`` spacer cells + a bare footnote-superscript cell mid-row (JPM)
  * wide empty layout-spacer columns + a legitimate ``0`` bonus (MSFT)
  * zero-width-space (``​``) spacer cells from iXBRL rendering
  * an interior null rendered BLANK (not ``—``) → the ends stay anchored
  * SRC scaled table (§ 229.402(n)) — fewer columns resolve, no special-case
  * Pay-versus-Performance table rejected (Total but no Salary; negative CAP)
  * a proxy with no SCT → empty rows, no guess
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.providers.implementations.sec_def14a import (
    _parse_dollar,
    _resolve_sct_fields,
    _split_name_position,
    parse_summary_compensation_table,
)


def _sct_doc(table_rows_html: str, *, heading: str = "Summary Compensation Table") -> str:
    """Wrap SCT rows in a minimal proxy doc with the section heading."""
    return f"<html><body><h2>{heading}</h2>{table_rows_html}</body></html>"


def _row(*cells: str) -> str:
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


# ---------------------------------------------------------------------------
# Real-layout regression scenarios
# ---------------------------------------------------------------------------


def test_aapl_rowspan_continuation_shift() -> None:
    """Name in a rowspan cell → continuation-year rows have one fewer cell
    and every column shifts. Reg-order zip + name carry-forward must keep
    each year's Total correct. AAPL layout: no Bonus/Option columns."""
    header = _row(
        "Name and Principal Position",
        "",
        "Year",
        "",
        "Salary ($)",
        "",
        "Stock Awards ($)",
        "",
        "Non-Equity Incentive Plan Compensation ($)",
        "",
        "All Other Compensation ($)",
        "",
        "Total ($)",
    )
    # First row carries the name; continuation rows omit it (rowspan collapse).
    r2025 = _row(
        "Tim Cook\nChief Executive Officer",
        "",
        "2025",
        "",
        "3,000,000",
        "",
        "57,535,293",
        "",
        "12,000,000",
        "",
        "1,759,518",
        "",
        "74,294,811",
    )
    r2024 = _row("", "2024", "", "3,000,000", "", "58,088,946", "", "12,000,000", "", "1,520,856", "", "74,609,802")
    table = f"<table>{header}{r2025}{r2024}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert len(result.rows) == 2
    cook_2025 = result.rows[0]
    assert cook_2025.executive_name == "Tim Cook"
    assert cook_2025.principal_position == "Chief Executive Officer"
    assert cook_2025.fiscal_year == 2025
    assert cook_2025.salary == Decimal("3000000")
    assert cook_2025.stock_awards == Decimal("57535293")
    assert cook_2025.non_equity_incentive == Decimal("12000000")
    assert cook_2025.other_comp == Decimal("1759518")
    assert cook_2025.total_comp == Decimal("74294811")
    assert cook_2025.bonus is None  # column absent
    assert cook_2025.option_awards is None
    # Continuation row carries the name forward and stays aligned.
    cook_2024 = result.rows[1]
    assert cook_2024.executive_name == "Tim Cook"
    assert cook_2024.fiscal_year == 2024
    assert cook_2024.total_comp == Decimal("74609802")
    assert cook_2024.salary == Decimal("3000000")


def test_hd_folded_year_and_dash_null() -> None:
    """Separate name-only row; year folded into the value stream; ``—`` bonus
    is an explicit null (kept as a slot, not dropped). Full 8-column table."""
    header = _row(
        "Name, Principal Position and Year",
        "Salary ($)",
        "Bonus ($)",
        "Stock Awards ($)",
        "Option Awards ($)",
        "Non-Equity Incentive Plan Compensation ($)",
        "Change in Pension Value and Nonqualified Deferred Compensation Earnings ($)",
        "All Other Compensation ($)",
        "Total ($)",
    )
    name_row = _row("Edward P. Decker Chair, President and Chief Executive Officer")
    y2025 = _row("2025", "1,400,000", "—", "9,612,251", "2,369,917", "2,657,631", "—", "151,328", "16,191,127")
    table = f"<table>{header}{name_row}{y2025}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.executive_name == "Edward P. Decker"
    assert row.principal_position == "Chair, President and Chief Executive Officer"
    assert row.fiscal_year == 2025
    assert row.salary == Decimal("1400000")
    assert row.bonus is None  # explicit em-dash null
    assert row.stock_awards == Decimal("9612251")
    assert row.option_awards == Decimal("2369917")
    assert row.pension_nqdc is None  # explicit em-dash null
    assert row.other_comp == Decimal("151328")
    assert row.total_comp == Decimal("16191127")


def test_jpm_dollar_spacers_and_footnote_cell() -> None:
    """Inline lone ``$`` spacer cells + a bare footnote-superscript cell
    (``6``) must both be dropped without shifting the value mapping. JPM
    layout: no Option/Non-Equity columns."""
    header = _row(
        "Name and principal position",
        "Year",
        "Salary ($) 1",
        "Bonus ($) 2",
        "Stock awards ($) 3",
        "Change in pension value and non-qualified deferred compensation earnings ($) 4",
        "All other compensation ($) 5",
        "Total ($)",
    )
    r = _row(
        "James Dimon Chairman and CEO",
        "2025",
        "$",
        "1,500,000",
        "$",
        "5,000,000",
        "$",
        "32,500,000",
        "$",
        "44,872",
        "$",
        "1,587,852",
        "6",
        "$",
        "40,632,724",
    )
    table = f"<table>{header}{r}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.executive_name == "James Dimon"
    assert row.principal_position == "Chairman and CEO"
    assert row.fiscal_year == 2025
    assert row.salary == Decimal("1500000")
    assert row.bonus == Decimal("5000000")
    assert row.stock_awards == Decimal("32500000")
    assert row.pension_nqdc == Decimal("44872")
    assert row.other_comp == Decimal("1587852")
    assert row.total_comp == Decimal("40632724")  # stray '6' footnote dropped
    assert row.option_awards is None
    assert row.non_equity_incentive is None


def test_msft_wide_spacers_and_zero_bonus() -> None:
    """Many empty layout-spacer columns; a legitimate ``0`` bonus must be
    KEPT (not mistaken for a footnote/spacer)."""
    header = _row(
        "Named Executive and Principal Position",
        "",
        "Year",
        "",
        "",
        "Salary ($)",
        "",
        "",
        "Bonus ($)",
        "",
        "",
        "Stock Awards ($)",
        "",
        "",
        "Non-equity Incentive Plan Compensation ($)",
        "",
        "",
        "All Other Compensation ($)",
        "",
        "",
        "Total ($)",
    )
    r = _row(
        "Satya Nadella Chairman and Chief Executive Officer",
        "",
        "",
        "2025",
        "",
        "",
        "",
        "2,500,000",
        "",
        "",
        "",
        "0",
        "",
        "",
        "",
        "84,245,496",
        "",
        "",
        "",
        "9,555,000",
        "",
        "",
        "",
        "196,294",
        "",
        "",
        "",
        "96,496,790",
    )
    table = f"<table>{header}{r}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.executive_name == "Satya Nadella"
    assert row.salary == Decimal("2500000")
    assert row.bonus == Decimal("0")  # legit zero kept
    assert row.stock_awards == Decimal("84245496")
    assert row.total_comp == Decimal("96496790")


def test_zero_width_space_spacers() -> None:
    """iXBRL proxies use ``​`` (zero-width space) cells as spacers.
    Python str.strip() does NOT treat these as whitespace, so they must be
    scrubbed or they hide the name/value cells."""
    zw = "​"
    header = _row(
        zw,
        "Name and Principal Position",
        zw,
        "Year",
        zw,
        "Salary ($)",
        zw,
        "Stock Awards ($)",
        zw,
        "All Other Compensation ($)",
        zw,
        "Total ($)",
    )
    r = _row(
        zw,
        "Jane Roe\nChief Executive Officer",
        zw,
        "2025",
        zw,
        "$1,000,000",
        zw,
        "$5,000,000",
        zw,
        "$250,000",
        zw,
        "$6,250,000",
    )
    table = f"<table>{header}{r}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.executive_name == "Jane Roe"
    assert row.fiscal_year == 2025
    assert row.salary == Decimal("1000000")
    assert row.total_comp == Decimal("6250000")


def test_interior_blank_null_keeps_total_correct() -> None:
    """When a filer renders an interior null column as BLANK (not ``—``) so
    it is dropped as a spacer, the value count is short by one. The parser
    must NOT left-shift the Total into another column: it anchors Total to the
    reg's last column and Salary to the first, leaving the ambiguous middle
    NULL rather than emitting a wrong figure."""
    header = _row(
        "Name and Principal Position",
        "Year",
        "Salary ($)",
        "Bonus ($)",
        "Stock Awards ($)",
        "Option Awards ($)",
        "Non-Equity Incentive Plan Compensation ($)",
        "Change in Pension Value ($)",
        "All Other Compensation ($)",
        "Total ($)",
    )
    # Pension column is BLANK here (dropped as spacer) → 7 values for 8 fields.
    r = _row(
        "John Doe Chief Executive Officer",
        "2024",
        "598,026",
        "—",
        "1,037,401",
        "165,030",
        "723,400",
        "",
        "105,009",
        "2,628,866",
    )
    table = f"<table>{header}{r}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert len(result.rows) == 1
    row = result.rows[0]
    # Anchored ends are always correct:
    assert row.total_comp == Decimal("2628866")
    assert row.salary == Decimal("598026")
    # Ambiguous middle is left NULL rather than mis-mapped:
    assert row.other_comp is None
    assert row.pension_nqdc is None


def test_src_scaled_table_fewer_columns() -> None:
    """§ 229.402(n) scaled SCT: two years, no Option/Non-Equity/Pension
    columns. Strict header-text resolution simply yields a smaller field set —
    no special-casing, absent columns resolve NULL."""
    header = _row(
        "Name and Principal Position",
        "Year",
        "Salary ($)",
        "Bonus ($)",
        "Stock Awards ($)",
        "All Other Compensation ($)",
        "Total ($)",
    )
    r2024 = _row("Mary Small Chief Executive Officer", "2024", "400,000", "50,000", "600,000", "10,000", "1,060,000")
    r2023 = _row("", "2023", "380,000", "40,000", "500,000", "9,000", "929,000")
    table = f"<table>{header}{r2024}{r2023}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    fields = _resolve_sct_fields(
        (
            "Name and Principal Position",
            "Year",
            "Salary ($)",
            "Bonus ($)",
            "Stock Awards ($)",
            "All Other Compensation ($)",
            "Total ($)",
        )
    )
    assert fields == ("salary", "bonus", "stock_awards", "other_comp", "total_comp")
    assert len(result.rows) == 2
    assert result.rows[0].total_comp == Decimal("1060000")
    assert result.rows[0].option_awards is None
    assert result.rows[0].pension_nqdc is None
    assert result.rows[1].fiscal_year == 2023
    assert result.rows[1].total_comp == Decimal("929000")


def test_pay_versus_performance_table_rejected() -> None:
    """The Pay-versus-Performance table (Item 402(v)) carries a "Summary
    Compensation Table Total" header (so it scores) and negative
    "Compensation Actually Paid" values, but NO Salary column. Requiring both
    Salary and Total rejects it — no negative totals leak in."""
    header = _row(
        "Year",
        "Summary Compensation Table Total for PEO ($)",
        "Compensation Actually Paid to PEO ($)",
        "Average SCT Total for Non-PEO NEOs ($)",
        "Average CAP to Non-PEO NEOs ($)",
        "Total Shareholder Return ($)",
    )
    r = _row("2022", "10,000,000", "-1,803,086", "3,000,000", "-500,000", "95")
    table = f"<table>{header}{r}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))
    assert result.rows == ()


def test_no_sct_returns_empty() -> None:
    """A proxy with no SCT (notice-only / soliciting material) returns empty
    rows — the parser logs and does not guess."""
    body = "<html><body><h2>Notice of Annual Meeting</h2><p>Please vote.</p></body></html>"
    result = parse_summary_compensation_table(body)
    assert result.rows == ()


def test_empty_input() -> None:
    assert parse_summary_compensation_table("").rows == ()


# ---------------------------------------------------------------------------
# Pure-helper unit tests
# ---------------------------------------------------------------------------


def test_parse_dollar_variants() -> None:
    assert _parse_dollar("$1,234,567") == Decimal("1234567")
    assert _parse_dollar("450,000(3)") == Decimal("450000")  # trailing footnote
    assert _parse_dollar("1,234,567 ") == Decimal("1234567")  # nbsp
    assert _parse_dollar("(1,234)") == Decimal("-1234")  # parenthesised negative
    assert _parse_dollar("0") == Decimal("0")
    assert _parse_dollar("—") is None
    assert _parse_dollar("N/A") is None
    assert _parse_dollar("") is None
    assert _parse_dollar("   ") is None


def test_split_name_position_newline() -> None:
    name, pos = _split_name_position("Tim Cook \nChief Executive Officer")
    assert name == "Tim Cook"
    assert pos == "Chief Executive Officer"


def test_split_name_position_role_keyword() -> None:
    name, pos = _split_name_position("James Dimon Chairman and CEO")
    assert name == "James Dimon"
    assert pos == "Chairman and CEO"


def test_split_name_position_no_title() -> None:
    name, pos = _split_name_position("Jane Doe")
    assert name == "Jane Doe"
    assert pos is None


@pytest.mark.parametrize(
    ("cell", "expected_name", "expected_pos"),
    [
        # Leading title modifiers must go to the position, not the name (#1967).
        ("Ann-Marie Campbell Senior Executive Vice President", "Ann-Marie Campbell", "Senior Executive Vice President"),
        (
            "Fahim Siddiqui Former Executive Vice President and CIO",
            "Fahim Siddiqui",
            "Former Executive Vice President and CIO",
        ),
        ("Hector A. Padilla Former Executive Vice President", "Hector A. Padilla", "Former Executive Vice President"),
        # "Vice Chair" must split at Vice, not Chair.
        ("Bradford L. Smith Vice Chair and President", "Bradford L. Smith", "Vice Chair and President"),
        # Inline footnote reference digit between name and title is stripped.
        (
            "Daniel Pinto 11 Vice Chair; Former President and COO",
            "Daniel Pinto",
            "Vice Chair; Former President and COO",
        ),
        ("Douglas Petno 9 Co-CEO, CIB", "Douglas Petno", "Co-CEO, CIB"),
        ("Troy Rohrbaugh 7 Co-CEO, CIB", "Troy Rohrbaugh", "Co-CEO, CIB"),
        # Footnote digit on the newline path is stripped too.
        ("James Dimon 5 \nChairman and CEO", "James Dimon", "Chairman and CEO"),
        # Two stacked leading modifiers (within the {0,3} bound) still split.
        ("Jane Roe Former Senior Vice President", "Jane Roe", "Former Senior Vice President"),
    ],
)
def test_split_name_position_modifier_and_footnote_bleed(cell: str, expected_name: str, expected_pos: str) -> None:
    name, pos = _split_name_position(cell)
    assert name == expected_name
    assert pos == expected_pos


def test_split_name_position_modifier_run_is_bounded() -> None:
    # ReDoS guard (#1967 review): the modifier prefix is {0,3}, so a long
    # adversarial run of modifier tokens stays linear, not quadratic. This
    # completes near-instantly; a regression to `*` would hang for seconds.
    pathological = "Senior " * 5000 + "X"
    name, pos = _split_name_position(pathological)  # must return, not hang
    assert isinstance(name, str)
    assert pos is None or isinstance(pos, str)


def test_split_name_position_preserves_clean_names() -> None:
    # Regression guard: modifier/footnote stripping must not corrupt clean cells.
    assert _split_name_position("Tim Cook \nChief Executive Officer") == ("Tim Cook", "Chief Executive Officer")
    assert _split_name_position("James Dimon Chairman and CEO") == ("James Dimon", "Chairman and CEO")
    assert _split_name_position("Satya Nadella Chairman and Chief Executive Officer") == (
        "Satya Nadella",
        "Chairman and Chief Executive Officer",
    )
    assert _split_name_position("Jane Doe") == ("Jane Doe", None)
