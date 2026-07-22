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
    _position_only_cell,
    _resolve_sct_fields,
    _split_name_position,
    parse_pvp_neo_names,
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


def test_gme_stacked_name_position_rows() -> None:
    """Stacked name/position layout (#2088 — GME): the title renders on its
    OWN physical row (the second year-row per NEO) and the third year-row's
    name cell is empty. The title row must not clobber the carried name, the
    title attaches to the carried NEO, and the already-emitted name row gets
    the position backfilled. A following NEO resets the carry."""
    header = _row(
        "Name and Principal Position",
        "Year",
        "Salary ($)",
        "Bonus ($)",
        "Stock Awards ($)",
        "Total ($)",
    )
    r1_2025 = _row("Ryan Cohen", "2025", "—", "—", "1,760,467", "1,760,467")
    r1_2024 = _row("Chief Executive Officer", "2024", "—", "—", "268,553", "268,553")
    r1_2023 = _row("", "2023", "—", "—", "100", "100")
    r2_2025 = _row("Dan Moore", "2025", "200,000", "—", "2,166,562", "2,366,562")
    r2_2024 = _row("Principal Financial and Accounting Officer", "2024", "192,615", "—", "636,600", "829,215")
    table = f"<table>{header}{r1_2025}{r1_2024}{r1_2023}{r2_2025}{r2_2024}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Ryan Cohen", "Chief Executive Officer", 2025),
        ("Ryan Cohen", "Chief Executive Officer", 2024),
        ("Ryan Cohen", "Chief Executive Officer", 2023),
        ("Dan Moore", "Principal Financial and Accounting Officer", 2025),
        ("Dan Moore", "Principal Financial and Accounting Officer", 2024),
    ]
    assert result.rows[0].total_comp == Decimal("1760467")
    assert result.rows[0].salary is None  # Cohen draws no salary — explicit dash
    assert result.rows[3].salary == Decimal("200000")


def test_prdo_wrapped_title_fragments() -> None:
    """Wrapped first-column layout (#2094 — PRDO): the logical name+title cell
    wraps across the NEO block's three physical year rows. Row-2 fragments may
    match the role lexicon at offset 0, but row-3 tails start with arbitrary
    words ('Officer', 'Technical University') no lexicon can enumerate. The
    fiscal-year descent (2024 → 2023 → 2022 inside a block; a new NEO restarts
    at a newer year) must keep the fragments attached to the carried NEO and
    concatenate the full title, backfilled onto every emitted row."""
    header = _row(
        "Name and Principal Position",
        "Year",
        "Salary ($)",
        "Stock Awards ($)",
        "Total ($)",
    )
    nelson_2024 = _row("Todd S. Nelson", "2024", "1,057,491", "3,323,796", "6,725,760")
    nelson_2023 = _row("President and Chief Executive", "2023", "1,036,756", "2,138,972", "4,504,386")
    nelson_2022 = _row("Officer", "2022", "1,011,469", "2,148,140", "4,512,179")
    baskel_2024 = _row("Elise L. Baskel", "2024", "494,846", "515,919", "1,495,528")
    baskel_2023 = _row("Senior Vice President – Colorado", "2023", "480,433", "434,439", "1,534,471")
    baskel_2022 = _row("Technical University", "2022", "465,000", "381,145", "1,229,324")
    table = f"<table>{header}{nelson_2024}{nelson_2023}{nelson_2022}{baskel_2024}{baskel_2023}{baskel_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Todd S. Nelson", "President and Chief Executive Officer", 2024),
        ("Todd S. Nelson", "President and Chief Executive Officer", 2023),
        ("Todd S. Nelson", "President and Chief Executive Officer", 2022),
        ("Elise L. Baskel", "Senior Vice President – Colorado Technical University", 2024),
        ("Elise L. Baskel", "Senior Vice President – Colorado Technical University", 2023),
        ("Elise L. Baskel", "Senior Vice President – Colorado Technical University", 2022),
    ]
    assert result.rows[2].total_comp == Decimal("4512179")
    assert result.rows[5].total_comp == Decimal("1229324")


def test_hbnc_non_lexicon_second_row_fragment() -> None:
    """Wrapped-title variant (#2094 — HBNC): even the SECOND row's fragment
    starts with a non-lexicon word ('EVP,'), so no position has been captured
    yet when it arrives. Year descent must still classify it as a title
    continuation, and a later lexicon-matching tail appends to it."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    secor_2024 = _row("Mark E. Secor", "2024", "430,000", "672,269")
    secor_2023 = _row("EVP,", "2023", "415,000", "630,000")
    secor_2022 = _row("Chief Financial Officer", "2022", "400,000", "610,000")
    table = f"<table>{header}{secor_2024}{secor_2023}{secor_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Mark E. Secor", "EVP, Chief Financial Officer", 2024),
        ("Mark E. Secor", "EVP, Chief Financial Officer", 2023),
        ("Mark E. Secor", "EVP, Chief Financial Officer", 2022),
    ]


def test_intracell_name_wrap_before_title() -> None:
    """#2097 — the whole name+title rides ONE first cell but the NEO's NAME
    itself wraps (render break) before the title onset (Alphabet template):
    'Sundar<br>Pichai<br>Chief Executive Officer, …'. A newline-first split
    truncated the name to its first token ('Sundar'); the role-boundary split
    keeps the full name. The wrap delimiter is incidental (``<br>`` collapses to
    a space here, a block element yields ``\\n`` on other filers) — both flatten
    to the same role-keyword split."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    pichai = _row("Sundar<br>Pichai<br>Chief Executive Officer, Alphabet", "2024", "2,000,000", "10,725,043")
    porat = _row("Ruth M.<br>Porat<br>President and Chief Investment Officer", "2024", "1,000,000", "30,166,427")
    table = f"<table>{header}{pichai}{porat}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Sundar Pichai", "Chief Executive Officer, Alphabet", 2024),
        ("Ruth M. Porat", "President and Chief Investment Officer", 2024),
    ]


def test_wrapped_title_fragment_with_embedded_newline() -> None:
    """#2094 + #2097 — a year-descending wrapped-TITLE fragment that itself
    carries an embedded render wrap ('President and<br>Chief Executive') must
    still attach to the carried NEO, not escape as a new one. Flattening the
    newline (role-boundary split) does not change the #2094 continuation
    classification (Codex ckpt-1 regression guard)."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    nelson_2024 = _row("Todd S. Nelson", "2024", "1,057,491", "6,725,760")
    nelson_2023 = _row("President and<br>Chief Executive", "2023", "1,036,756", "4,504,386")
    nelson_2022 = _row("Officer", "2022", "1,011,469", "4,512,179")
    table = f"<table>{header}{nelson_2024}{nelson_2023}{nelson_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Todd S. Nelson", "President and Chief Executive Officer", 2024),
        ("Todd S. Nelson", "President and Chief Executive Officer", 2023),
        ("Todd S. Nelson", "President and Chief Executive Officer", 2022),
    ]


def test_former_exec_block_starts_below_table_max_year() -> None:
    """A departed NEO's block may start BELOW the table's newest year (no
    FY2024 row). The block boundary is a year INCREASE relative to the
    previous physical row — not equality with the table max — so the new
    name must open a fresh block, not be absorbed as a title fragment."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    ceo_2024 = _row("Ann Incumbent\nChief Executive Officer", "2024", "900,000", "3,000,000")
    ceo_2023 = _row("", "2023", "850,000", "2,800,000")
    ceo_2022 = _row("", "2022", "800,000", "2,600,000")
    former_2023 = _row("Jane Q. Departed\nFormer Chief Financial Officer", "2023", "600,000", "1,900,000")
    former_2022 = _row("", "2022", "580,000", "1,700,000")
    table = f"<table>{header}{ceo_2024}{ceo_2023}{ceo_2022}{former_2023}{former_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.fiscal_year) for r in result.rows] == [
        ("Ann Incumbent", 2024),
        ("Ann Incumbent", 2023),
        ("Ann Incumbent", 2022),
        ("Jane Q. Departed", 2023),
        ("Jane Q. Departed", 2022),
    ]
    assert result.rows[3].principal_position == "Former Chief Financial Officer"


def test_new_neo_opens_below_previous_rows_year() -> None:
    """Codex ckpt-2 (#2094): a departed NEO's block can start BELOW the
    previous physical row's year (current-year-only NEO first). A cell that
    splits into a plausible person name + title must open a new block even
    on a year-descending row; a title fragment ('Financial Officer &
    Treasurer') must not."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    alice_2024 = _row("Alice A. Alpha\nChief Executive Officer", "2024", "500,000", "2,000,000")
    bob_2023 = _row("Bob B. Beta\nFormer Chief Financial Officer", "2023", "400,000", "1,500,000")
    bob_2022 = _row("", "2022", "380,000", "1,400,000")
    table = f"<table>{header}{alice_2024}{bob_2023}{bob_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Alice A. Alpha", "Chief Executive Officer", 2024),
        ("Bob B. Beta", "Former Chief Financial Officer", 2023),
        ("Bob B. Beta", "Former Chief Financial Officer", 2022),
    ]


def test_fragment_with_interior_role_keyword_still_continues() -> None:
    """A wrapped-title tail containing a role keyword at offset > 0
    ('Financial Officer & Treasurer' — PRDO/Ghia) splits into a title-vocab
    prefix, NOT a person name, so it must append to the carried NEO rather
    than open a bogus block."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    ghia_2024 = _row("Ashish R. Ghia", "2024", "530,000", "3,405,287")
    ghia_2023 = _row("Senior Vice President, Chief", "2023", "515,000", "2,290,706")
    ghia_2022 = _row("Financial Officer & Treasurer", "2022", "500,000", "1,842,560")
    table = f"<table>{header}{ghia_2024}{ghia_2023}{ghia_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Ashish R. Ghia", "Senior Vice President, Chief Financial Officer & Treasurer", 2024),
        ("Ashish R. Ghia", "Senior Vice President, Chief Financial Officer & Treasurer", 2023),
        ("Ashish R. Ghia", "Senior Vice President, Chief Financial Officer & Treasurer", 2022),
    ]


def test_single_year_table_new_neo_on_equal_year() -> None:
    """One row per NEO, all the same fiscal year: equal years are NOT a
    descent, so every name-like cell opens a new block."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    r1 = _row("Alice A. Alpha\nChief Executive Officer", "2024", "500,000", "2,000,000")
    r2 = _row("Bob B. Beta\nChief Financial Officer", "2024", "400,000", "1,500,000")
    table = f"<table>{header}{r1}{r2}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.fiscal_year) for r in result.rows] == [
        ("Alice A. Alpha", 2024),
        ("Bob B. Beta", 2024),
    ]


def test_name_repeated_on_each_year_row() -> None:
    """Some filers repeat the NEO's name (or full name+title) on every year
    row instead of using rowspan. A repeated first cell inside a descending
    block must neither clobber the position nor be appended to it."""
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    r_2024 = _row("Carol C. Gamma\nChief Executive Officer", "2024", "700,000", "2,500,000")
    r_2023 = _row("Carol C. Gamma", "2023", "650,000", "2,200,000")
    r_2022 = _row("Carol C. Gamma\nChief Executive Officer", "2022", "600,000", "2,000,000")
    table = f"<table>{header}{r_2024}{r_2023}{r_2022}</table>"
    result = parse_summary_compensation_table(_sct_doc(table))

    assert [(r.executive_name, r.principal_position, r.fiscal_year) for r in result.rows] == [
        ("Carol C. Gamma", "Chief Executive Officer", 2024),
        ("Carol C. Gamma", "Chief Executive Officer", 2023),
        ("Carol C. Gamma", "Chief Executive Officer", 2022),
    ]


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


def test_position_only_cell_detection() -> None:
    """Position-only = first role keyword at offset 0 (#2088). A cell leading
    with a person's name is NOT position-only, even with a title after."""
    assert _position_only_cell("Chief Executive Officer") == "Chief Executive Officer"
    assert _position_only_cell("Principal Financial and Accounting Officer") == (
        "Principal Financial and Accounting Officer"
    )
    assert _position_only_cell("General Counsel and Secretary") == "General Counsel and Secretary"
    assert _position_only_cell("James Dimon Chairman and CEO") is None
    assert _position_only_cell("Tim Cook\nChief Executive Officer") is None
    assert _position_only_cell("") is None
    # #2097 — "executive" is a leading title modifier, so a bare "Executive
    # Chairman" title row is now recognised as position-only (previously it
    # matched only at "Chairman", minting a bogus "Executive" NEO).
    assert _position_only_cell("Executive Chairman") == "Executive Chairman"
    assert _position_only_cell("Executive Vice- Chairman") == "Executive Vice- Chairman"


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


def test_split_name_position_intracell_newline_2097() -> None:
    """#2097 — a newline inside the cell is a render wrap, not a name/title
    delimiter. It falls mid-name or mid-title; the role-boundary split recovers
    the correct name in both faces, and a title-free wrapped name stays whole."""
    # Bare-first-name wrap (Alphabet): '\n' falls mid-name.
    assert _split_name_position("Sundar\n Pichai \n Chief Executive Officer, Alphabet") == (
        "Sundar Pichai",
        "Chief Executive Officer, Alphabet",
    )
    # Mid-title wrap: '\n' falls inside the title — name must not absorb it.
    assert _split_name_position("T. Wilson Eglin Chief Executive Officer\nand President") == (
        "T. Wilson Eglin",
        "Chief Executive Officer and President",
    )
    # Wrapped NAME with no title in the cell (Cato) — whole cell is the name.
    assert _split_name_position("John\n P. D. Cato") == ("John P. D. Cato", None)
    # Compound "Executive [Vice-] Chairman" title splits at "Executive" (#2097
    # exec modifier + hyphen-tolerant vice-chair), not one word late.
    assert _split_name_position("Raymond\nR. Quirk Executive Vice- Chairman") == (
        "Raymond R. Quirk",
        "Executive Vice- Chairman",
    )
    assert _split_name_position("Morgan E. O'Brien \n Executive Chairman") == (
        "Morgan E. O'Brien",
        "Executive Chairman",
    )


def test_split_name_position_surname_is_title_vocab_word() -> None:
    """Codex ckpt-1 — the fix never mutates the name, so a real surname that
    happens to be a title-vocabulary word ('Bank') survives intact. (No
    trailing-title trim was adopted; the role-boundary split alone handles the
    compound-title leaks it was meant to catch.)"""
    assert _split_name_position("Robert A. Bank") == ("Robert A. Bank", None)
    assert _split_name_position("Mary Global") == ("Mary Global", None)


# ---------------------------------------------------------------------------
# def14a-v6 (#2100 + #2099) — group/managing modifiers + same-document
# truncated-name repair (sibling / camel-verbatim / PvP iXBRL oracle)
# ---------------------------------------------------------------------------


def _pvp_context(cid: str, start: str, end: str, member: str | None = None) -> str:
    dim = f'<xbrldi:explicitMember dimension="ecd:IndividualAxis">{member}</xbrldi:explicitMember>' if member else ""
    return (
        f'<xbrli:context id="{cid}"><xbrli:entity>{dim}</xbrli:entity>'
        f"<xbrli:period><xbrli:startDate>{start}</xbrli:startDate>"
        f"<xbrli:endDate>{end}</xbrli:endDate></xbrli:period></xbrli:context>"
    )


def _pvp_doc(body_html: str, *, contexts: str = "", facts: str = "", ecd_prefix: str = "ecd") -> str:
    """Minimal iXBRL proxy shell: xmlns declarations + hidden contexts/facts."""
    return (
        f'<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" '
        f'xmlns:xbrli="http://www.xbrl.org/2003/instance" '
        f'xmlns:{ecd_prefix}="http://xbrl.sec.gov/ecd/2025">'
        f"<body><div style='display:none'><ix:header><ix:resources>{contexts}"
        f"</ix:resources></ix:header>{facts}</div>{body_html}</body></html>"
    )


def _simple_sct(*name_year_total: tuple[str, str, str]) -> str:
    header = _row("Name and Principal Position", "Year", "Salary ($)", "Total ($)")
    rows = "".join(_row(n, y, "100,000", t) for n, y, t in name_year_total)
    return f"<h2>Summary Compensation Table</h2><table>{header}{rows}</table>"


def test_group_modifier_splits_title() -> None:
    """#2100 Class 3 — 'Group President…' / 'Senior Managing Director' are
    titles; the split lands before the modifier."""
    assert _split_name_position("David E. Govrin Group President, Americas") == (
        "David E. Govrin",
        "Group President, Americas",
    )
    assert _split_name_position("Susan D. Nickey Senior Managing Director") == (
        "Susan D. Nickey",
        "Senior Managing Director",
    )
    # A bare stacked title row classifies position-only (no bogus NEO).
    assert _position_only_cell("Managing Director, Finance") == "Managing Director, Finance"
    assert _position_only_cell("Group President") == "Group President"


def test_new_modifiers_fuzz_stays_bounded() -> None:
    """L2121 — the {0,3} modifier bound holds for the new group/managing
    alternatives (no quadratic blowup on adversarial runs)."""
    import time

    for run in ("Group " * 5000, "Managing " * 5000):
        start = time.monotonic()
        _split_name_position(run + "X")
        assert time.monotonic() - start < 0.5


def test_pvp_neo_names_extraction() -> None:
    """PeoName facts grouped per person; entity-decode; whitespace-flattened
    fact values; covered end-years unioned across per-FY contexts."""
    contexts = (
        _pvp_context("c1", "2024-01-01", "2024-12-31", "aapl:CookMember")
        + _pvp_context("c2", "2025-01-01", "2025-12-31", "aapl:CookMember")
        + _pvp_context("c3", "2025-01-01", "2025-12-31", "aapl:OBrienMember")
    )
    facts = (
        '<ix:nonNumeric contextRef="c1" name="ecd:PeoName">Mr. Cook</ix:nonNumeric>'
        '<ix:nonNumeric contextRef="c2" name="ecd:PeoName">Mr. Cook</ix:nonNumeric>'
        '<ix:nonNumeric contextRef="c3" name="ecd:PeoName">Deirdre O&#8217;Brien</ix:nonNumeric>'
    )
    result = parse_pvp_neo_names(_pvp_doc("", contexts=contexts, facts=facts))
    by_member = {p.individual_member: p for p in result}
    assert by_member["aapl:CookMember"].name_text == "Mr. Cook"
    assert by_member["aapl:CookMember"].covered_end_years == frozenset({2024, 2025})
    assert by_member["aapl:OBrienMember"].name_text == "Deirdre O’Brien"


def test_pvp_neo_names_uri_resolved_prefix() -> None:
    """ECD matching is namespace-URI-resolved — a non-'ecd' declared prefix
    still extracts; and no facts → ()."""
    contexts = _pvp_context("c1", "2025-01-01", "2025-12-31")
    facts = '<ix:nonNumeric contextRef="c1" name="pvp:PeoName">Jane Roe</ix:nonNumeric>'
    doc = _pvp_doc("", contexts=contexts, facts=facts, ecd_prefix="pvp")
    assert [p.name_text for p in parse_pvp_neo_names(doc)] == ["Jane Roe"]
    assert parse_pvp_neo_names("<html><body>no ixbrl here</body></html>") == ()


def test_surname_only_repaired_from_pvp_oracle() -> None:
    """CXW shape — surname-only SCT rows + a PvP PeoName covering every row
    FY → repaired to the oracle's full form."""
    sct = _simple_sct(("Hininger", "2025", "7,203,173"), ("", "2024", "7,471,923"))
    contexts = "".join(
        _pvp_context(f"c{i}", f"{y}-01-01", f"{y}-12-31") for i, y in enumerate((2021, 2022, 2023, 2024, 2025))
    )
    facts = "".join(
        f'<ix:nonNumeric contextRef="c{i}" name="ecd:PeoName">Damon T. Hininger</ix:nonNumeric>' for i in range(5)
    )
    result = parse_summary_compensation_table(_pvp_doc(sct, contexts=contexts, facts=facts))
    assert sorted({r.executive_name for r in result.rows}) == ["Damon T. Hininger"]
    assert {r.fiscal_year for r in result.rows} == {2024, 2025}


def test_oracle_fy_gate_blocks_partial_coverage() -> None:
    """Per-name atomic FY gate — an oracle fact covering only FY2025 must not
    rename rows spanning 2023-25 (no partial renames)."""
    sct = _simple_sct(("Charles", "2025", "1,000,000"), ("", "2024", "900,000"), ("", "2023", "800,000"))
    contexts = _pvp_context("c1", "2025-01-01", "2025-12-31")
    facts = '<ix:nonNumeric contextRef="c1" name="ecd:PeoName">Dirkson Charles</ix:nonNumeric>'
    result = parse_summary_compensation_table(_pvp_doc(sct, contexts=contexts, facts=facts))
    assert sorted({r.executive_name for r in result.rows}) == ["Charles"]


def test_oracle_honorific_never_shortens() -> None:
    """'Mr. Cook' (honorific-only oracle form) can never repair 'Cook' — the
    replacement must be strictly more token-complete after honorific strip."""
    sct = _simple_sct(("Cook", "2025", "74,294,811"))
    contexts = _pvp_context("c1", "2025-01-01", "2025-12-31")
    facts = '<ix:nonNumeric contextRef="c1" name="ecd:PeoName">Mr. Cook</ix:nonNumeric>'
    result = parse_summary_compensation_table(_pvp_doc(sct, contexts=contexts, facts=facts))
    assert sorted({r.executive_name for r in result.rows}) == ["Cook"]


def test_sibling_superset_repairs_wrapped_name() -> None:
    """A single-token name with EXACTLY ONE intra-SCT token-superset sibling
    on non-overlapping FYs adopts the sibling's spelling. (The single-token
    block opens FIRST at a lower year so the full-name block is a genuine
    new-NEO open, not a #2094 carry.)"""
    sct = _simple_sct(
        ("Pferdehirt", "2022", "14,774,294"),
        ("Douglas J. Pferdehirt\nChief Executive Officer", "2023", "17,062,495"),
    )
    result = parse_summary_compensation_table(f"<html><body>{sct}</body></html>")
    assert {r.executive_name for r in result.rows} == {"Douglas J. Pferdehirt"}
    assert {r.fiscal_year for r in result.rows} == {2022, 2023}


def test_sibling_collision_blocks_repair() -> None:
    """FTI shape — the suspicious row's FY collides with the sibling's own
    row for the same FY (conflicting totals) → NO repair, both stay visible."""
    sct = _simple_sct(
        ("Pferdehirt", "2023", "393,737"),
        ("Douglas J. Pferdehirt\nChief Executive Officer", "2023", "17,062,495"),
    )
    result = parse_summary_compensation_table(f"<html><body>{sct}</body></html>")
    names = sorted({r.executive_name for r in result.rows})
    assert names == ["Douglas J. Pferdehirt", "Pferdehirt"]


def test_camel_glued_split_validated_by_document() -> None:
    """CJK glued romanisation splits ONLY when the spaced form occurs verbatim
    in the same document; Mc-style surnames and unvalidated camels survive."""
    sct = _simple_sct(("HechunWei", "2024", "7,025"))
    result = parse_summary_compensation_table(f"<html><body>{sct}<p>Hechun Wei is our CEO.</p></body></html>")
    assert sorted({r.executive_name for r in result.rows}) == ["Hechun Wei"]
    # No spaced form in document → unchanged.
    sct2 = _simple_sct(("LushaNiu", "2024", "5,000"))
    result2 = parse_summary_compensation_table(f"<html><body>{sct2}</body></html>")
    assert sorted({r.executive_name for r in result2.rows}) == ["LushaNiu"]
    # 2-char first capital run (real camel surname) is never split.
    sct3 = _simple_sct(("McDonald", "2024", "5,000"))
    result3 = parse_summary_compensation_table(f"<html><body>{sct3}<p>Mc Donald</p></body></html>")
    assert sorted({r.executive_name for r in result3.rows}) == ["McDonald"]


def test_cross_source_disagreement_blocks_repair() -> None:
    """Conflicting initials across sources ('Douglas J.' sibling vs
    'Douglas P.' oracle) are a disagreement → no repair even without an FY
    collision."""
    sct = _simple_sct(
        ("Pferdehirt", "2022", "14,774,294"),
        ("Douglas J. Pferdehirt\nChief Executive Officer", "2023", "17,062,495"),
    )
    contexts = _pvp_context("c1", "2022-01-01", "2022-12-31")
    facts = '<ix:nonNumeric contextRef="c1" name="ecd:PeoName">Douglas P. Pferdehirt</ix:nonNumeric>'
    result = parse_summary_compensation_table(_pvp_doc(sct, contexts=contexts, facts=facts))
    assert sorted({r.executive_name for r in result.rows}) == ["Douglas J. Pferdehirt", "Pferdehirt"]


def test_fragment_rows_never_repaired() -> None:
    """Bogus fragment names ('Executive') with no unanimous evidence stay
    untouched — repairing them would be wrong, deleting them is forbidden."""
    sct = _simple_sct(("Executive", "2024", "1,000"), ("Jane Doe\nChief Executive Officer", "2024", "2,000"))
    result = parse_summary_compensation_table(f"<html><body>{sct}</body></html>")
    assert "Executive" in {r.executive_name for r in result.rows}


def test_candidates_agree_order_and_initials() -> None:
    """Direct unit pins on the agreement predicate (fresh-agent review):
    permutation of the same token set = two different people = disagreement;
    conflicting initials = disagreement; subset (incl. one-side initials,
    honorifics) = agreement."""
    from app.providers.implementations.sec_def14a import _candidates_agree

    assert _candidates_agree("Cook", "Tim Cook")
    assert _candidates_agree("Damon Hininger", "Damon T. Hininger")
    assert _candidates_agree("Mr. Cook", "Tim Cook")
    assert not _candidates_agree("Hechun Wei", "Wei Hechun")
    assert not _candidates_agree("Douglas J. Pferdehirt", "Douglas P. Pferdehirt")
    assert _candidates_agree("Hechun Wei", "Hechun Wei")


def test_permuted_sibling_names_block_repair() -> None:
    """Two real people whose names are token-permutations ('Hechun Wei' /
    'Wei Hechun') must never let a shared-token single-token row repair onto
    either — the set-equal-order-differs pair disagrees."""
    sct = _simple_sct(
        ("Wei", "2022", "1,000"),
        ("Hechun Wei\nChief Executive Officer", "2023", "2,000"),
        ("Wei Hechun\nChief Financial Officer", "2023", "3,000"),
    )
    result = parse_summary_compensation_table(f"<html><body>{sct}</body></html>")
    assert "Wei" in {r.executive_name for r in result.rows}


def test_camel_split_needs_word_boundary() -> None:
    """The camel-verbatim check is word-bounded — 'Jon Smithson' in prose must
    not validate a 'JonSmith' split."""
    sct = _simple_sct(("JonSmith", "2024", "1,000"))
    doc = f"<html><body>{sct}<p>Our counsel Jon Smithson advised.</p></body></html>"
    result = parse_summary_compensation_table(doc)
    assert sorted({r.executive_name for r in result.rows}) == ["JonSmith"]
