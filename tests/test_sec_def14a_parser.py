"""Unit tests for the SEC DEF 14A beneficial-ownership parser (#769 PR 1).

Fixture HTML is hand-built to mirror the shape of real DEF 14A
proxy statements without pulling production payloads into the repo.
Each scenario pins a single behaviour:

  * Section locator — finds the heading even with extra inline
    markup; falls back to whole-document scan when absent.
  * Table scoring — picks the beneficial-ownership table over a
    competing options-grants / compensation table on the same page.
  * Footnote stripping — ``(1)``, ``(*)``, asterisks, brackets all
    drop from holder names + numeric cells.
  * Less-than-1% convention — bare ``*`` in the percent column maps
    to ``Decimal('0.5')`` per industry convention.
  * Role inference — section subheadings flip the role tag for
    subsequent rows; inline labels fire as fallback.
  * Numeric tolerance — commas, em-dash, N/A all parse safely.
  * No-match safety — a proxy without a recognisable
    beneficial-ownership table returns empty rows + score floor.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.providers.implementations.sec_def14a import (
    _SCHEDULE_13D_COVER_LABEL_RE,
    Def14ABeneficialHolder,
    Def14ABeneficialOwnershipTable,
    _clean_beneficial_holder_name,
    _clean_holder_name,
    _contains_specific_name,
    _detect_role_heading,
    _expand_row_spans,
    _extract_table_holders,
    _has_item403_value_rows,
    _header_caption_set,
    _is_address_fragment,
    _is_beneficial_owner_identity,
    _is_name_then_address,
    _is_owner_identity,
    _is_percent_caption,
    _item403_value_signature,
    _layout_name_key,
    _layout_percent_by_row,
    _layout_rows,
    _looks_like_label_row,
    _looks_like_subheader,
    _parse_percent,
    _parse_share_count,
    _parse_table_html,
    _RawTable,
    _resolve_columns,
    _score_table_headers,
    _shares_cell_percent_signature,
    _split_stacked_holder_row,
    _strip_inline_html,
    _subsection_sibling_tables,
    extract_plan_name_and_trustee,
    is_esop_plan,
    parse_beneficial_ownership_table,
)

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _proxy_html(*, body: str, heading: str = "Security Ownership of Certain Beneficial Owners and Management") -> str:
    """Wrap an HTML fragment in a minimal proxy-statement skeleton."""
    return f"""<!DOCTYPE html>
<html><head><title>Proxy Statement</title></head>
<body>
<h1>Notice of Annual Meeting</h1>
<p>Some preamble prose.</p>

<h2>{heading}</h2>
<p>The following table sets forth the beneficial ownership as of March 1, 2026.</p>
{body}
<p>Footnotes:</p>
<ol><li>Includes options exercisable within 60 days.</li></ol>
</body></html>"""


_STANDARD_TABLE = """
<table>
  <tr>
    <th>Name and Address of Beneficial Owner</th>
    <th>Number of Shares Beneficially Owned</th>
    <th>Percent of Class</th>
  </tr>
  <tr><td>John Doe, CEO</td><td>1,500,000</td><td>5.5%</td></tr>
  <tr><td>Jane Smith, Director</td><td>250,000(1)</td><td>*</td></tr>
  <tr><td>Vanguard Group, Inc.</td><td>3,000,000</td><td>11.0%</td></tr>
  <tr>
    <td>All directors and executive officers as a group (5 persons)</td>
    <td>2,100,000</td>
    <td>7.7%</td>
  </tr>
</table>
"""


# ---------------------------------------------------------------------------
# Happy-path parsing
# ---------------------------------------------------------------------------


def test_standard_table_parses_holder_count_and_percent() -> None:
    parsed = parse_beneficial_ownership_table(_proxy_html(body=_STANDARD_TABLE))

    assert isinstance(parsed, Def14ABeneficialOwnershipTable)
    assert len(parsed.rows) == 4
    assert parsed.rows[0].holder_name == "John Doe, CEO"
    assert parsed.rows[0].shares == Decimal("1500000")
    assert parsed.rows[0].percent_of_class == Decimal("5.5")
    assert parsed.rows[0].holder_role == "officer"  # via inline "CEO"

    # Footnote (1) stripped from shares + name; ``*`` percent maps to
    # the less-than-one-percent convention (0.5).
    assert parsed.rows[1].holder_name == "Jane Smith, Director"
    assert parsed.rows[1].shares == Decimal("250000")
    assert parsed.rows[1].percent_of_class == Decimal("0.5")
    assert parsed.rows[1].holder_role == "director"

    assert parsed.rows[2].holder_name == "Vanguard Group, Inc."
    assert parsed.rows[2].shares == Decimal("3000000")
    assert parsed.rows[2].percent_of_class == Decimal("11.0")

    assert parsed.rows[3].holder_role == "group"  # synthesis row
    assert parsed.rows[3].shares == Decimal("2100000")


def test_as_of_date_extracted_from_section_intro() -> None:
    parsed = parse_beneficial_ownership_table(_proxy_html(body=_STANDARD_TABLE))
    assert parsed.as_of_date == date(2026, 3, 1)


def test_as_of_date_iso_format_supported() -> None:
    body = _STANDARD_TABLE
    html = _proxy_html(body=body).replace("March 1, 2026", "2026-03-01")
    parsed = parse_beneficial_ownership_table(html)
    assert parsed.as_of_date == date(2026, 3, 1)


def test_as_of_date_slash_format_supported() -> None:
    html = _proxy_html(body=_STANDARD_TABLE).replace("March 1, 2026", "3/1/2026")
    parsed = parse_beneficial_ownership_table(html)
    assert parsed.as_of_date == date(2026, 3, 1)


def test_as_of_date_absent_returns_none() -> None:
    body = _STANDARD_TABLE
    html = _proxy_html(body=body).replace("as of March 1, 2026", "shown below")
    parsed = parse_beneficial_ownership_table(html)
    assert parsed.as_of_date is None


# ---------------------------------------------------------------------------
# Section locator + table scoring
# ---------------------------------------------------------------------------


def test_options_grants_table_is_not_picked_over_ownership_table() -> None:
    """A competing grants table ahead of the ownership section
    must NOT be picked — the section locator scopes the scan to
    the post-heading window."""
    competing = """
    <h2>Stock Option Grants in Last Fiscal Year</h2>
    <table>
      <tr><th>Name</th><th>Options Granted</th><th>Exercise Price</th></tr>
      <tr><td>John Doe, CEO</td><td>50,000</td><td>$120.00</td></tr>
    </table>
    """
    html = competing + _proxy_html(body=_STANDARD_TABLE)
    parsed = parse_beneficial_ownership_table(html)
    assert len(parsed.rows) == 4
    # Make sure none of the parsed shares were 50,000 (the grants
    # value) — that would mean the parser picked the wrong table.
    assert all(r.shares != Decimal("50000") for r in parsed.rows)


def test_section_heading_variants_all_resolve() -> None:
    for heading in (
        "Security Ownership of Certain Beneficial Owners and Management",
        "Beneficial Ownership of Common Stock",
        "Principal Stockholders",
        "5% Holders",
        "5 % or more Beneficial Owners",
    ):
        parsed = parse_beneficial_ownership_table(_proxy_html(body=_STANDARD_TABLE, heading=heading))
        assert len(parsed.rows) >= 1, f"heading variant did not resolve: {heading!r}"


def test_no_section_heading_falls_back_to_whole_document() -> None:
    """Small DEF 14As sometimes inline the table without a
    dedicated heading. Whole-document scan still picks it up."""
    html = f"<html><body><p>Annual meeting notice.</p>{_STANDARD_TABLE}</body></html>"
    parsed = parse_beneficial_ownership_table(html)
    assert len(parsed.rows) == 4


def test_no_recognisable_table_returns_empty_rows() -> None:
    """A proxy without an ownership table (notice-only filing,
    options-only filing) returns zero rows and a low score so the
    ingester can tombstone."""
    html = _proxy_html(
        body="<table><tr><th>Auditor</th><th>Term</th></tr><tr><td>Acme LLP</td><td>1 year</td></tr></table>"
    )
    parsed = parse_beneficial_ownership_table(html)
    assert parsed.rows == []
    assert parsed.raw_table_score < 3


# ---------------------------------------------------------------------------
# Role inference
# ---------------------------------------------------------------------------


def test_role_section_heading_flips_role_for_subsequent_rows() -> None:
    """A single-cell heading row inside the table (some issuers split
    officers from 5%-holders this way) flips the role tag."""
    body = """
    <table>
      <tr><th>Name</th><th>Shares</th><th>Percent</th></tr>
      <tr><td>Officers and Directors</td><td></td><td></td></tr>
      <tr><td>John Doe</td><td>1,500,000</td><td>5.5%</td></tr>
      <tr><td>Jane Smith</td><td>800,000</td><td>3.0%</td></tr>
      <tr><td>5% Holders</td><td></td><td></td></tr>
      <tr><td>Vanguard Group</td><td>3,000,000</td><td>11.0%</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 3
    assert parsed.rows[0].holder_role == "officer"
    assert parsed.rows[1].holder_role == "officer"
    assert parsed.rows[2].holder_role == "principal"


def test_inline_role_label_fires_when_no_section_heading() -> None:
    """Without a section subheading, the parser detects the role
    from inline text in the holder cell."""
    body = """
    <table>
      <tr><th>Beneficial Owner</th><th>Shares Owned</th><th>Percent</th></tr>
      <tr><td>John Doe</td><td>1,500,000</td><td>5.5%</td></tr>
      <tr><td>Jane Smith - Director</td><td>800,000</td><td>3.0%</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert parsed.rows[0].holder_role is None  # no inline label
    assert parsed.rows[1].holder_role == "director"


# ---------------------------------------------------------------------------
# Numeric tolerance + footnote stripping
# ---------------------------------------------------------------------------


def test_dash_and_na_share_counts_resolve_to_none() -> None:
    body = """
    <table>
      <tr><th>Name</th><th>Shares Beneficially Owned</th><th>Percent of Class</th></tr>
      <tr><td>Holder A</td><td>—</td><td>—</td></tr>
      <tr><td>Holder B</td><td>N/A</td><td>—</td></tr>
      <tr><td>Holder C</td><td>0</td><td>0%</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    # Holder A + B drop because both shares AND percent unparseable.
    # Holder C survives because shares=0 + percent=0 are valid.
    assert len(parsed.rows) == 1
    assert parsed.rows[0].holder_name == "Holder C"
    assert parsed.rows[0].shares == Decimal("0")
    assert parsed.rows[0].percent_of_class == Decimal("0")


def test_bracketed_footnote_markers_stripped() -> None:
    body = """
    <table>
      <tr><th>Name</th><th>Number of Shares</th><th>Percent of Class</th></tr>
      <tr><td>Bracketed Holder [1]</td><td>1,000,000 [2]</td><td>3.5%[3]</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 1
    assert parsed.rows[0].holder_name == "Bracketed Holder"
    assert parsed.rows[0].shares == Decimal("1000000")
    assert parsed.rows[0].percent_of_class == Decimal("3.5")


def test_sup_footnote_markers_stripped() -> None:
    body = """
    <table>
      <tr><th>Name</th><th>Number of Shares</th><th>Percent of Class</th></tr>
      <tr><td>Sup Holder<sup>(1)</sup></td><td>500,000<sup>(2)</sup></td><td>2.0%</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 1
    assert parsed.rows[0].holder_name == "Sup Holder"
    assert parsed.rows[0].shares == Decimal("500000")


# ---------------------------------------------------------------------------
# Sanity guards
# ---------------------------------------------------------------------------


def test_empty_html_returns_empty_result_safely() -> None:
    parsed = parse_beneficial_ownership_table("")
    assert parsed.rows == []
    assert parsed.raw_table_score == 0
    assert parsed.as_of_date is None


def test_garbage_html_does_not_raise() -> None:
    """The parser never raises — best-effort extraction. The
    ingester is responsible for tombstoning malformed accessions."""
    parsed = parse_beneficial_ownership_table("<not really html<<<>>>")
    assert parsed.rows == []


# ---------------------------------------------------------------------------
# Codex pre-push fixes — TOC trap, multi-column block, alpha footnotes
# ---------------------------------------------------------------------------


def test_toc_entry_does_not_anchor_section_window() -> None:
    """Real DEF 14As open with a Table of Contents listing every
    section heading. The section locator must pick the LAST match,
    not the first — otherwise the TOC entry's window would miss the
    real section (especially on large filings where >500KB of prose
    sits between TOC and section). Codex pre-push review caught
    this."""
    toc = """
    <h1>Table of Contents</h1>
    <ul>
      <li>Election of Directors</li>
      <li>Security Ownership of Certain Beneficial Owners and Management</li>
      <li>Auditor Ratification</li>
    </ul>
    """
    # Pad with prose to simulate distance between TOC and section.
    padding = "<p>Some governance prose.</p>" * 50
    real_section = _proxy_html(body=_STANDARD_TABLE)
    html = toc + padding + real_section
    parsed = parse_beneficial_ownership_table(html)
    assert len(parsed.rows) == 4, "TOC entry stole the window from the real section"


def test_sole_shared_total_layout_picks_total_column() -> None:
    """SEC-prescribed Sole/Shared/Total/Percent layout — the parser
    must pick the ``Total`` column for shares, not ``Sole`` (the
    first column matching ``"shares"``-ish). Codex pre-push review
    caught the prior version reading ``Sole`` as shares and
    ``Shared`` as percent."""
    body = """
    <table>
      <tr>
        <th>Name and Address of Beneficial Owner</th>
        <th>Sole Voting Power</th>
        <th>Shared Voting Power</th>
        <th>Total Shares Beneficially Owned</th>
        <th>Percent of Class</th>
      </tr>
      <tr>
        <td>Activist Holder LLC</td>
        <td>100</td><td>50</td><td>1,500,000</td><td>5.5%</td>
      </tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 1
    assert parsed.rows[0].shares == Decimal("1500000"), f"expected Total column (1.5M), got {parsed.rows[0].shares}"
    assert parsed.rows[0].percent_of_class == Decimal("5.5")


def test_in_table_subheading_does_not_anchor_section_window() -> None:
    """A table cell whose text reads ``"5% Holders"`` (mid-table
    subheading splitting officers from principals) matches the
    section-heading regex but is not a real heading. The locator
    must skip it so the real ``<h2>`` heading anchors the window —
    not the in-cell text. Codex pre-push review caught this when
    fixing the TOC trap."""
    body_with_subheading_row = """
    <table>
      <tr>
        <th>Name and Address of Beneficial Owner</th>
        <th>Number of Shares Beneficially Owned</th>
        <th>Percent of Class</th>
      </tr>
      <tr><td>John Doe</td><td>1,000,000</td><td>3.5%</td></tr>
      <tr><td>5% Holders</td><td></td><td></td></tr>
      <tr><td>Vanguard Group</td><td>3,000,000</td><td>11.0%</td></tr>
    </table>
    """
    # Add a competing post-table compensation table so the wrong
    # window would land somewhere with rows.
    competing_after = """
    <h3>Executive Compensation Summary</h3>
    <table>
      <tr><th>Name</th><th>Salary</th><th>Bonus</th></tr>
      <tr><td>Option Grants Bucket</td><td>50,000</td><td>25</td></tr>
    </table>
    """
    html = _proxy_html(body=body_with_subheading_row) + competing_after
    parsed = parse_beneficial_ownership_table(html)
    # Real ownership table parsed — 2 holder rows + 1 mid-table
    # subheading row that flips role tag (no data emitted from it).
    assert len(parsed.rows) == 2
    assert parsed.rows[0].holder_name == "John Doe"
    assert parsed.rows[1].holder_name == "Vanguard Group"
    # Compensation row must NOT have leaked through.
    assert all(r.shares != Decimal("50000") for r in parsed.rows)


def test_holder_named_common_fund_is_not_treated_as_subheader() -> None:
    """A holder cell containing the word ``"common"`` (e.g. a fund
    named ``"Common Fund LLC"``) must not be silently promoted to
    column-headers and dropped from the data set. Codex / bot
    review caught this on PR review — ``common`` was originally in
    the sub-header keyword list as a share-class indicator but
    collided with legitimate holder names."""
    body = """
    <table>
      <tr>
        <th>Name and Address of Beneficial Owner</th>
        <th>Number of Shares</th>
        <th>Percent of Class</th>
      </tr>
      <tr><td>Common Fund LLC</td><td>3,000,000</td><td>11.0%</td></tr>
      <tr><td>Other Holder</td><td>1,000,000</td><td>3.5%</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 2
    names = [r.holder_name for r in parsed.rows]
    assert "Common Fund LLC" in names


def test_two_row_header_with_sole_shared_total_promotes_subheader() -> None:
    """Some DEF 14As use a merged top header
    (``Name | Amount and Nature | Percent``) with a sub-row
    (``Sole | Shared | Total``) underneath. The parser must promote
    the sub-row to canonical headers so the column resolver finds
    ``Total``. Codex pre-push review caught this."""
    body = """
    <table>
      <tr>
        <th>Name and Address of Beneficial Owner</th>
        <th>Amount and Nature of Beneficial Ownership</th>
        <th>Percent of Class</th>
      </tr>
      <tr>
        <th></th><th>Sole</th><th>Shared</th><th>Total</th><th></th>
      </tr>
      <tr>
        <td>Activist Holder LLC</td>
        <td>100</td><td>50</td><td>1,500,000</td><td>5.5%</td>
      </tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 1
    # Total column wins, not Sole.
    assert parsed.rows[0].shares == Decimal("1500000")
    assert parsed.rows[0].percent_of_class == Decimal("5.5")


def test_alphabetic_footnote_markers_stripped_from_holder_and_numeric_cells() -> None:
    """``[a]`` / ``(b)`` / ``[c]`` footnote markers (used by some
    issuers instead of numeric ``(1)`` / ``[1]``) must strip
    cleanly so the share-count parser sees a clean number. Codex
    pre-push review caught the prior regex matching only digits and
    asterisks."""
    body = """
    <table>
      <tr>
        <th>Name and Address of Beneficial Owner</th>
        <th>Number of Shares</th>
        <th>Percent of Class</th>
      </tr>
      <tr><td>Holder With Letter Footnote [a]</td><td>1,000,000 [b]</td><td>3.5% [c]</td></tr>
      <tr><td>Holder With Paren Letter (d)</td><td>500,000(e)</td><td>1.5%(f)</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 2
    assert parsed.rows[0].holder_name == "Holder With Letter Footnote"
    assert parsed.rows[0].shares == Decimal("1000000")
    assert parsed.rows[0].percent_of_class == Decimal("3.5")
    assert parsed.rows[1].holder_name == "Holder With Paren Letter"
    assert parsed.rows[1].shares == Decimal("500000")


# ---------------------------------------------------------------------------
# ESOP / employee-benefit-plan detection (#843)
# ---------------------------------------------------------------------------


class TestIsEsopPlan:
    """Each of the 9 conservative regex patterns must match a
    representative real holder_name string. Generic Trust / Trustee
    must NOT match (false-positive guard against Vanguard Fiduciary
    Trust / BlackRock Institutional Trust 5%-holder rows)."""

    def test_esop_acronym_matches(self) -> None:
        assert is_esop_plan("ABC Inc. ESOP")
        assert is_esop_plan("Acme ESOP Trust")

    def test_full_employee_stock_ownership_plan_matches(self) -> None:
        assert is_esop_plan("Apple Employee Stock Ownership Plan")

    def test_401k_with_and_without_parens_matches(self) -> None:
        assert is_esop_plan("Apple Inc. 401(k) Plan")
        assert is_esop_plan("Apple Inc. 401k Plan")
        assert is_esop_plan("Microsoft Corporation 401 (k) Plan")

    def test_401_plan_without_k_matches_for_cleaned_legacy_names(self) -> None:
        """``_clean_holder_name`` strips ``(k)`` as a footnote
        marker so legacy stored holder_names read ``401 Plan``
        without the ``k``. The pattern accepts the cleaned form via
        an optional ``k``-suffix. Codex pre-push review #843
        round 5 caught this gap."""
        assert is_esop_plan("Apple Inc. 401 Plan")

    def test_bare_401_does_not_match(self) -> None:
        """Numeric ``401`` without a ``Plan`` suffix MUST NOT match —
        catches false positives on share counts / index references."""
        assert not is_esop_plan("Acme 401st Quarter Filing")
        assert not is_esop_plan("Holder 401")

    def test_employee_savings_plan_matches(self) -> None:
        assert is_esop_plan("Acme Employee Savings Plan")

    def test_retirement_savings_plan_matches(self) -> None:
        assert is_esop_plan("Acme Retirement Savings Plan")

    def test_profit_sharing_plan_matches_with_hyphen_or_space(self) -> None:
        assert is_esop_plan("Acme Profit-Sharing Plan")
        assert is_esop_plan("Acme Profit Sharing Plan")

    def test_employee_benefit_plan_matches(self) -> None:
        assert is_esop_plan("Acme Employee Benefit Plan")

    def test_company_stock_fund_matches(self) -> None:
        assert is_esop_plan("Apple Company Stock Fund")

    def test_savings_plan_trust_matches(self) -> None:
        assert is_esop_plan("Acme Savings Plan Trust")
        assert is_esop_plan("Acme Retirement Plan Trust")
        assert is_esop_plan("Acme Profit-Sharing Plan Trust")

    def test_generic_trust_does_not_match(self) -> None:
        """Critical false-positive guard: bare Trust / Trustee /
        Trustee for must NOT tag as ESOP. Vanguard Fiduciary Trust
        Company appears in every Vanguard 5%-holder row + would
        over-tag every institutional position as ESOP."""
        assert not is_esop_plan("Vanguard Fiduciary Trust Company")
        assert not is_esop_plan("BlackRock Institutional Trust Company")
        assert not is_esop_plan("State Street Bank and Trust Company")
        assert not is_esop_plan("Trustee for the Cohen Family Trust")
        assert not is_esop_plan("The Vanguard Group, Inc.")
        assert not is_esop_plan("BlackRock, Inc.")

    def test_empty_or_none_returns_false(self) -> None:
        assert not is_esop_plan("")


class TestExtractPlanNameAndTrustee:
    """The trustee suffix split must produce a canonical plan_name
    that matches across years even as the trustee changes (issuers
    re-bid plan administration periodically)."""

    def test_c_o_trustee_suffix_split(self) -> None:
        plan, trustee = extract_plan_name_and_trustee("Apple Inc. 401(k) Plan, c/o Vanguard Fiduciary Trust as Trustee")
        assert plan == "Apple Inc. 401(k) Plan"
        assert trustee == "Vanguard Fiduciary Trust"

    def test_comma_trustee_suffix_split(self) -> None:
        plan, trustee = extract_plan_name_and_trustee(
            "Acme Profit-Sharing Plan, Fidelity Management Trust Company, Trustee"
        )
        assert plan == "Acme Profit-Sharing Plan"
        assert trustee == "Fidelity Management Trust Company"

    def test_paren_trustee_suffix_split(self) -> None:
        plan, trustee = extract_plan_name_and_trustee(
            "Microsoft Savings Plus Plan (State Street Bank and Trust, Trustee)"
        )
        assert plan == "Microsoft Savings Plus Plan"
        assert trustee == "State Street Bank and Trust"

    def test_by_trustee_suffix_split(self) -> None:
        plan, trustee = extract_plan_name_and_trustee("ABC ESOP by Computershare Trust as trustee")
        assert plan == "ABC ESOP"
        assert trustee == "Computershare Trust"

    def test_no_trustee_suffix_returns_holder_name_unchanged(self) -> None:
        plan, trustee = extract_plan_name_and_trustee("Apple Inc. 401(k) Plan")
        assert plan == "Apple Inc. 401(k) Plan"
        assert trustee is None

    def test_empty_holder_name_returns_empty(self) -> None:
        plan, trustee = extract_plan_name_and_trustee("")
        assert plan == ""
        assert trustee is None


def test_parser_overrides_role_to_esop_for_matching_holder_name() -> None:
    """Critical integration: when an ESOP plan crosses the 5%
    disclosure threshold, the existing parser tags it as 'principal'
    via section context. The #843 ESOP override must flip that to
    'esop' so the row lands in the dedicated ownership_esop_*
    slice, NOT the blockholders slice."""
    body = """
    <table>
      <tr>
        <th>Name and Address of Beneficial Owner</th>
        <th>Number of Shares</th>
        <th>Percent of Class</th>
      </tr>
      <tr><td>5% Beneficial Stockholders</td></tr>
      <tr><td>Vanguard Group, Inc.</td><td>3,000,000</td><td>9.5%</td></tr>
      <tr><td>Apple Inc. 401(k) Plan, c/o Vanguard Fiduciary Trust as Trustee</td><td>2,000,000</td><td>6.5%</td></tr>
      <tr><td>BlackRock, Inc.</td><td>1,800,000</td><td>5.7%</td></tr>
    </table>
    """
    parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
    assert len(parsed.rows) == 3
    by_name = {r.holder_name: r for r in parsed.rows}
    # Vanguard + BlackRock land as 'principal' (5%-holder section
    # heading inside the table flips current_role).
    assert by_name["Vanguard Group, Inc."].holder_role == "principal"
    assert by_name["BlackRock, Inc."].holder_role == "principal"
    # Apple plan flips to 'esop' via the name-pattern override even
    # though section context tagged it 'principal'. The holder_name
    # stored is the cleaned form — ``_clean_holder_name`` strips
    # ``(k)`` as a single-alpha-in-parens footnote marker, so the
    # canonical name is "Apple Inc. 401 Plan" (without the (k)).
    # The ESOP detection runs on the RAW holder_name pre-clean so
    # the override still fires.
    plan_holder = by_name["Apple Inc. 401 Plan, c/o Vanguard Fiduciary Trust as Trustee"]
    assert plan_holder.holder_role == "esop"
    assert plan_holder.shares == Decimal("2000000")
    assert plan_holder.percent_of_class == Decimal("6.5")


# ---------------------------------------------------------------------------
# #1228 — _parse_percent out-of-range clamp
# ---------------------------------------------------------------------------


class TestParsePercentRangeClamp:
    """Regression for #1228 — DEF14A NUMERIC(8,4) overflow when a
    column-resolver misfire routes a 7-digit shares value into the
    percent cell. Schema rejects values > 9999.9999 and previously
    aborted the entire batch in ``ingest_def14a``."""

    def test_valid_low_percent_passes(self) -> None:
        assert _parse_percent("0.5") == Decimal("0.5")

    def test_valid_mid_percent_passes(self) -> None:
        assert _parse_percent("12.34") == Decimal("12.34")

    def test_valid_max_boundary_100_passes(self) -> None:
        assert _parse_percent("100") == Decimal("100")

    def test_valid_zero_passes(self) -> None:
        assert _parse_percent("0") == Decimal("0")

    def test_misrouted_shares_count_rejected(self) -> None:
        # 9,000,000 — typical 7-digit shares count misrouted into
        # percent slot via positional fallback in ``_resolve_columns``.
        assert _parse_percent("9000000") is None

    def test_just_above_100_rejected(self) -> None:
        # Real percent ownership cannot exceed 100. A 100.01 value is
        # almost certainly a parsing artefact, not a real holding.
        assert _parse_percent("100.01") is None

    def test_just_below_zero_rejected(self) -> None:
        assert _parse_percent("-0.01") is None

    def test_comma_thousand_separator_shares_rejected(self) -> None:
        # "9,000,000" — same as above with the SEC-canonical comma
        # separator. ``_parse_percent`` strips commas + parses;
        # the clamp catches the resulting 9000000 Decimal.
        assert _parse_percent("9,000,000") is None

    def test_asterisk_less_than_one_still_passes(self) -> None:
        # Industry-convention asterisk maps to 0.5 — must not regress
        # under the new clamp.
        from app.providers.implementations.sec_def14a import _LESS_THAN_ONE_PERCENT_VALUE

        assert _parse_percent("*") == _LESS_THAN_ONE_PERCENT_VALUE

    def test_dash_returns_none(self) -> None:
        assert _parse_percent("—") is None

    def test_empty_returns_none(self) -> None:
        assert _parse_percent("") is None

    def test_invalid_text_returns_none(self) -> None:
        assert _parse_percent("N/A") is None

    def test_nan_returns_none(self) -> None:
        # ``Decimal("NaN")`` is a valid Decimal but comparing it
        # against a finite Decimal raises ``InvalidOperation``. Guard
        # explicitly so the clamp doesn't blow up on a malformed cell.
        assert _parse_percent("NaN") is None

    def test_infinity_returns_none(self) -> None:
        assert _parse_percent("Infinity") is None
        assert _parse_percent("-Infinity") is None


# ---------------------------------------------------------------------------
# #2140 — column resolution + role classification
#
# Each fixture below reproduces the header/row SHAPE of a real filing that the
# parser got wrong, traced from the stored ``def14a_body`` payload named in the
# test. Source rule: Reg S-K Item 403 (via Schedule 14A Item 6(d)) prescribes
# BOTH "Name and address of beneficial owner" / "Name of beneficial owner" AND
# "Amount and nature of beneficial ownership" — so the token ``beneficial``
# appears on both sides of the table and cannot discriminate a name column.
# ---------------------------------------------------------------------------


class TestResolveColumnsNeverCollide:
    """``_resolve_columns`` must return three DISTINCT roles for the real
    header shapes that previously collapsed onto one index."""

    def test_blank_name_caption_with_total_beneficial_ownership(self) -> None:
        # LOGI 0001032975-26-000037. "Total Beneficial Ownership" is the SHARES
        # column; matching bare "beneficial" made it the name column too.
        headers = (
            "",
            "Number of Shares Owned (1)",
            "Shares that May be Acquired Within 60 Days (2)",
            "Total Beneficial Ownership",
            "Total as a Percentage of Shares Outstanding (3)",
        )
        name_idx, shares_idx, percent_idx = _resolve_columns(headers)
        assert name_idx == 0
        assert shares_idx == 3
        assert percent_idx == 4

    def test_blank_name_caption_with_shares_beneficially_owned(self) -> None:
        # MKTX 0001193125-26-191601.
        headers = ("", "", "Number of Shares Beneficially Owned", "", "Percentage of Stock Owned")
        name_idx, shares_idx, percent_idx = _resolve_columns(headers)
        assert name_idx == 0
        assert shares_idx == 2
        assert percent_idx == 4

    def test_item_403_prescribed_captions_still_resolve(self) -> None:
        # HLF 0001213900-26-029131 — the shape that already worked; pinned so
        # the exclusion-ordered rewrite cannot regress it.
        headers = (
            "Name of beneficial owner",
            "",
            "Amount and nature of beneficial ownership",
            "",
            "Percentage ownership (1)",
        )
        assert _resolve_columns(headers) == (0, 2, 4)

    def test_name_never_aliases_shares_for_any_real_header_shape(self) -> None:
        # The invariant the 3,209-row defect violated: whenever there is more
        # than one column, the name and shares roles are distinct.
        for headers in (
            ("", "Shares Beneficially Owned"),
            ("", "", "Number of Shares Beneficially Owned", "", "Percentage of Stock Owned"),
            ("Beneficial Owner", "Amount and Nature of Beneficial Ownership", "Percent"),
            ("Title of Class", "Name and Address of Beneficial Owner", "Amount and Nature", "Percent of Class"),
            ("", "Amount and Nature of Beneficial Ownership", "Percent of Class"),
        ):
            name_idx, shares_idx, percent_idx = _resolve_columns(headers)
            assert name_idx != shares_idx, headers
            assert percent_idx in (-1, name_idx) or percent_idx != shares_idx, headers

    def test_total_as_a_percentage_header_does_not_steal_the_shares_column(self) -> None:
        # Codex pre-push review: the shares tiering's generic ``total`` keyword
        # matches "Total as a Percentage of Shares Outstanding", so resolving
        # shares BEFORE percent let the percent column win shares_idx. The row
        # parser then read "5.0%" as a share count and dropped every row.
        # Percent is resolved first and excluded from the shares tiering.
        headers = ("Name", "Shares Beneficially Owned", "Total as a Percentage of Shares Outstanding")
        assert _resolve_columns(headers) == (0, 1, 2)

    def test_total_as_a_percentage_table_still_yields_rows(self) -> None:
        body = """
        <table>
          <tr><th>Name</th><th>Shares Beneficially Owned</th>
              <th>Total as a Percentage of Shares Outstanding</th></tr>
          <tr><td>Jane Smith</td><td>250,000</td><td>5.0%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "Jane Smith"
        assert parsed.rows[0].shares == Decimal("250000")
        assert parsed.rows[0].percent_of_class == Decimal("5.0")

    def test_percent_absent_is_reported_as_minus_one_not_an_alias(self) -> None:
        # A table with no distinguishable percent column must say so — callers
        # treat a negative index as "absent" and must never index end-relative.
        _, shares_idx, percent_idx = _resolve_columns(("Shares Beneficially Owned",))
        assert percent_idx == -1
        assert shares_idx == 0


class TestSpanningHeaderPromotion:
    """#2140 D2 — a spanning row 0 over the real label row."""

    def test_name_shares_percent_label_row_is_promoted(self) -> None:
        # UBER 0001308179-26-000125 / CYH 0001193125-26-140269 shape: row 0
        # spans, row 1 carries the real labels but none of the legacy
        # Sole/Shared/Total keywords.
        body = """
        <table>
          <tr><th></th><th></th><th>Shares Beneficially Owned</th></tr>
          <tr><td>Name of Beneficial Owner</td><td></td><td>Shares</td><td></td><td>% of Shares Outstanding</td></tr>
          <tr><td>Dara Khosrowshahi (1)</td><td></td><td>2,380,203</td><td></td><td>1.2%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Dara Khosrowshahi"]
        assert parsed.rows[0].shares == Decimal("2380203")
        assert parsed.rows[0].percent_of_class == Decimal("1.2")

    def test_section_heading_row_is_not_promoted_to_header(self) -> None:
        # Guard for the substring trap: "Named Executive Officers and
        # Directors" contains "name", carries no digits, and sits exactly
        # where a promoted row would. It must stay a section heading.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares</th><th>Percent</th></tr>
          <tr><td>Named Executive Officers and Directors</td><td></td><td></td></tr>
          <tr><td>Dara Khosrowshahi</td><td>2,380,203</td><td>1.2%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Dara Khosrowshahi"]
        assert parsed.rows[0].holder_role == "officer"  # from the heading row

    def test_performance_award_table_is_not_promoted(self) -> None:
        # CYH regression guard: a PSU vesting row matches percent+amount but
        # carries no NAME label. Promoting it inflated that table's score
        # enough to beat the real ownership table.
        assert not _looks_like_subheader(
            ("% of Target Achieved", "% of Granted Shares Earned", "", "Percentile Rank", "% of Granted Shares Earned")
        )

    def test_legacy_sole_shared_total_subheader_still_promotes(self) -> None:
        assert _looks_like_subheader(("", "Sole", "Shared", "Total", ""))


class TestFullPopulationRegressions:
    """Shapes surfaced only by the full-population A/B (#2140) — each one lost
    EVERY row of a real filing until fixed. Sampled from the 159 accessions the
    first branch scan showed losing rows."""

    def test_shares_column_holding_a_percent_still_yields_a_percent(self) -> None:
        # 0001308179-25-000615: row 0 is full width and IS kept as the header
        # ('Name and Address of Beneficial Owner', '', '', '', 'Number of
        # Shares Beneficially Owned'), so shares_idx lands on a cell holding
        # '10.2%'. It parses as no share count, and the percent recovery
        # previously SKIPPED the shares column, so the row lost both values and
        # was dropped -- 20 real holders (Vanguard/FMR/BlackRock) -> 0.
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th><th></th><th></th><th></th>
              <th>Number of Shares Beneficially Owned</th></tr>
          <tr><td>The Vanguard Group (1)</td><td></td><td>7,196,087</td><td></td><td>10.2%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "The Vanguard Group"
        assert parsed.rows[0].percent_of_class == Decimal("10.2")

    def test_equal_width_parent_header_still_promotes_a_label_row(self) -> None:
        # The `<` width test only promoted a NARROWER parent, but an issuer can
        # render a full-width row 0 that is still not the label row.
        body = """
        <table>
          <tr><th>Amount and Nature of Beneficial Ownership</th><th></th><th></th><th></th>
              <th>Shares Beneficially Owned</th></tr>
          <tr><td>Name of Beneficial Owner</td><td></td><td>Number</td><td></td><td>Percentage</td></tr>
          <tr><td>The Vanguard Group (1)</td><td></td><td>7,196,087</td><td></td><td>10.2%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["The Vanguard Group"]
        assert parsed.rows[0].shares == Decimal("7196087")
        assert parsed.rows[0].percent_of_class == Decimal("10.2")

    def test_single_cell_section_heading_is_not_a_label_row(self) -> None:
        # 0001628280-25-020660: '5% Stockholders' matches percent (via '%') and
        # name (via 'stockholders') at once. Promoting it destroyed the real
        # header, so an Item 402(f) equity-awards table won selection instead.
        assert not _looks_like_label_row(("5% Stockholders",))
        assert not _looks_like_label_row(("5% Stockholders", "", "", ""))

    def test_merged_amount_and_percent_caption_is_the_amount_column(self) -> None:
        # 0001140361-25-008248: issuers merge Item 403's two captions into one
        # column. Percent-first must not claim it, or shares never resolves.
        headers = (
            "Name of Beneficial Owner (1)",
            "",
            "",
            "Amount and Nature of Beneficial Ownership and Percent of Class (2)",
        )
        name_idx, shares_idx, percent_idx = _resolve_columns(headers)
        assert (name_idx, shares_idx) == (0, 3)
        assert percent_idx != 3

    def test_non_breaking_spaces_do_not_break_the_item_403_caption(self) -> None:
        # 0001466593-25-000049: a literal U+00A0 in the markup meant
        # "amount and nature" never matched, so a "Common Shares of <Issuer>"
        # title column won the shares tiering and shares_idx landed on the NAME
        # column. 17 rows -> 0.
        body = (
            "<table>"
            "<tr><th>Common Shares of Otter Tail Corporation</th>"
            "<th>Amount\xa0and\xa0Nature\xa0of\xa0Beneficial\xa0Ownership 1</th>"
            "<th>Percent of Class 1</th></tr>"
            "<tr><td>John S. Abbott 3</td><td>55,494</td><td>*</td></tr>"
            "</table>"
        ).replace("\\xa0", "\xa0")
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "John S. Abbott"
        assert parsed.rows[0].shares == Decimal("55494")


class TestItem403SiblingTables:
    """Item 403 has TWO subsections — 403(a) >5% owners and 403(b) management
    plus the group aggregate — and issuers render them as separate tables."""

    def test_both_item_403_tables_are_collected(self) -> None:
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>BlackRock, Inc.</td><td>52,606,862</td><td>9.0%</td></tr>
        </table>
        <table>
          <tr><th>Name</th><th>Total Common Shares Beneficially Owned</th><th>Percent of Class</th></tr>
          <tr><td>Karen B. Bailo</td><td>32,345</td><td>*</td></tr>
          <tr><td>All directors and executive officers as a group (5 persons)</td><td>90,000</td><td>1.1%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        names = [r.holder_name for r in parsed.rows]
        assert "BlackRock, Inc." in names
        assert "Karen B. Bailo" in names
        assert parsed.rows[-1].holder_role == "group"

    def test_a_breakdown_table_does_not_duplicate_its_holders(self) -> None:
        # 0000080661-25-000018 renders the SAME 16 people twice: a totals table
        # and, below it, the split into restricted/equivalent/other. Keying the
        # dedup on (name, shares, percent) kept both and put everyone in twice,
        # reintroducing the duplicate-holder defect this ticket removes.
        body = """
        <table>
          <tr><th>Name</th><th>Total Common Shares Beneficially Owned</th><th>Percent of Class</th></tr>
          <tr><td>Karen B. Bailo</td><td>32,345</td><td>*</td></tr>
          <tr><td>Philip Bleser</td><td>21,325</td><td>*</td></tr>
        </table>
        <table>
          <tr><th>Name</th><th>Common Shares Subject to Restricted Stock</th>
              <th>Other Common Shares Beneficially Owned</th></tr>
          <tr><td>Karen B. Bailo</td><td>—</td><td>32,345</td></tr>
          <tr><td>Philip Bleser</td><td>974</td><td>20,351</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        names = [r.holder_name for r in parsed.rows]
        assert names.count("Karen B. Bailo") == 1
        assert names.count("Philip Bleser") == 1
        # The strongest Item 403 caption wins the figures.
        assert parsed.rows[0].shares == Decimal("32345")

    def test_item_402_award_table_is_disqualified(self) -> None:
        # Item 402(d)/(f) captions share "number of shares" with Item 403, so an
        # award table could out-score a real ownership table on keyword weight.
        assert _score_table_headers(("Name", "Grant Date", "Number of Shares of Stock or Units")) == 0
        assert _score_table_headers(("Name", "Option Exercise Price ($)", "Number of Shares")) == 0
        # A real Item 403 header is unaffected.
        assert _score_table_headers(("Name of Beneficial Owner", "Number of Shares", "Percent of Class")) > 0


class TestSharesRecoveryDoesNotEatPercents:
    def test_bare_percent_is_not_stored_as_a_share_count(self) -> None:
        # Codex pre-push: the shares recovery scanned every non-name cell, and
        # _parse_share_count("8.4") succeeds — so a row reporting
        # ``Shares = -`` with ``Percent = 8.4`` stored shares=8.4. Leaving
        # shares NULL is the safe fallback.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>Acme Fund LP</td><td>&#8212;</td><td>8.4</td></tr>
          <tr><td>Jane Smith</td><td>250,000</td><td>1.2%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        by_name = {r.holder_name: r for r in parsed.rows}
        assert by_name["Acme Fund LP"].shares is None
        assert by_name["Acme Fund LP"].percent_of_class == Decimal("8.4")
        assert by_name["Jane Smith"].shares == Decimal("250000")


class TestShareCountFootnote:
    def test_unbracketed_trailing_footnote_digit_does_not_inflate_shares(self) -> None:
        # '52,606,862 1' -> removing the space first produced 526,068,621:
        # a 10x overstatement of BlackRock's holding (0000080661-25-000018).
        assert _parse_share_count("52,606,862 1") == Decimal("52606862")
        assert _parse_share_count("52,606,862 (1)") == Decimal("52606862")
        assert _parse_share_count("1,650,489") == Decimal("1650489")


class TestAddressFragmentsAreNotHolders:
    """Item 403's column is "Name AND ADDRESS of beneficial owner". When an
    issuer splits the address across sibling <tr> rows, the continuation lines
    land in the name column of their own row and parse as holders carrying real
    share numbers (0001193125-25-095068)."""

    def test_address_only_cells_are_rejected(self) -> None:
        for text in (
            "c/o Dolan Family Office",
            "P.O. Box 420",
            "620 Eighth Avenue New York, NY 10018",
            "100 Vanguard Blvd, Malvern, PA 19355",
            "50 Hudson Yards, New York, NY 10001",
            "2000 Avenue of the Stars, Suite 1110",
            "462 S. 4 th Street, Suite 2000",
            "6300 Bee Cave Road, Austin, TX 78746",
            "Attn: Legal Department",
        ):
            assert _is_address_fragment(text), text

    def test_entities_whose_name_starts_with_digits_are_kept(self) -> None:
        # A leading street NUMBER alone is not an address — requiring a
        # street-type token nearby is what stops the rule eating real holders.
        # The full-population distinct-holder check caught all three of these.
        for text in (
            "325 Capital LLC",
            "2025 Acquisition Corp",
            "2025 Irrevocable Two-Year Grantor Retained Annuity Trust",
            # Leads with the holder, address trails — still a holder.
            "325 Capital LLC 200 Park Avenue, 17th Floor",
        ):
            assert not _is_address_fragment(text), text

    def test_a_name_leading_its_address_is_kept(self) -> None:
        for text in (
            "BlackRock, Inc. 55 East 52nd Street New York, NY 10055",
            "The Vanguard Group",
            "3M Company",
            "1st Source Corp",
            "All directors and executive officers as a group (5 persons)",
        ):
            assert not _is_address_fragment(text), text

    def test_address_continuation_row_is_dropped(self) -> None:
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>Dolan Family Group</td><td>1,074,594</td><td>3.19%</td></tr>
          <tr><td>c/o Dolan Family Office</td><td>11,484,408</td><td>100%</td></tr>
          <tr><td>P.O. Box 420</td><td>2,010,611</td><td>17.51%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Dolan Family Group"]


class TestHolderNameStructuralGuard:
    """#2140 — a holder name must carry name evidence; a share count or a
    percent marker can never be persisted as one."""

    def test_numeric_cell_falls_back_to_the_named_cell_in_the_row(self) -> None:
        body = """
        <table>
          <tr><th></th><th>Total Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>BlackRock, Inc.</td><td>9,777,832</td><td>6.8%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "BlackRock, Inc."
        assert parsed.rows[0].shares == Decimal("9777832")

    def test_row_with_no_name_evidence_anywhere_is_dropped(self) -> None:
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares</th><th>Percent</th></tr>
          <tr><td>1,234,567</td><td>1,234,567</td><td>5.0%</td></tr>
          <tr><td>Jane Smith</td><td>250,000</td><td>1.0%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Jane Smith"]


class TestGroupAggregateOverridesSectionContext:
    """#2140 D4 — Item 403(b)'s "all directors and officers as a group" row is
    NON-ADDITIVE with its constituents, so it must stay distinguishable even
    though it sits inside the management block that sets ``current_role``."""

    def test_group_row_under_management_heading_is_tagged_group(self) -> None:
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares</th><th>Percent</th></tr>
          <tr><td>Directors and Executive Officers</td><td></td><td></td></tr>
          <tr><td>John Doe</td><td>1,000</td><td>*</td></tr>
          <tr><td>All directors and executive officers as a group (17 persons)</td><td>5,297,686</td><td>5.12%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_role == "officer"
        assert parsed.rows[-1].holder_role == "group"
        assert parsed.rows[-1].shares == Decimal("5297686")

    def test_group_heading_does_not_become_a_sticky_section_role(self) -> None:
        # A group pattern in _ROLE_HEADING_PATTERNS would set current_role for
        # every SUBSEQUENT row. Rows after a group-shaped heading must keep the
        # management role, not inherit 'group'.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares</th><th>Percent</th></tr>
          <tr><td>All directors and executive officers as a group</td><td></td><td></td></tr>
          <tr><td>John Doe</td><td>1,000</td><td>*</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["John Doe"]
        assert parsed.rows[0].holder_role != "group"


class TestHolderNameNewlineNormalisation:
    """#2140 D5 — ``holder_name_key`` is ``lower(trim(holder_name))``; ``trim``
    does not touch INTERIOR whitespace, so a render wrap split one person into
    two identities across ``ownership_def14a_current``."""

    def test_interior_line_break_is_flattened(self) -> None:
        body = """
        <table>
          <tr><th>Name of beneficial owner</th><th>Amount and nature of beneficial ownership</th>
              <th>Percentage</th></tr>
          <tr><td>Michael<br/> O. Johnson</td><td>1,650,489</td><td>1.60%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "Michael O. Johnson"

    def test_unbracketed_trailing_footnote_digit_is_stripped(self) -> None:
        # MKTX: the superscript carried no parentheses, so it survived
        # _FOOTNOTE_RE as a bare trailing number once the wrap was flattened.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares</th><th>Percent</th></tr>
          <tr><td>BlackRock, Inc.<br/> 1</td><td>4,034,537</td><td>11.5%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "BlackRock, Inc."

    def test_sct_name_title_split_is_unaffected(self) -> None:
        # Blast-radius pin: the newline/footnote cleaning lives in
        # _clean_beneficial_holder_name, NOT the shared _clean_holder_name,
        # because _normalize_first_cell feeds SCT title fragments through it.
        assert _clean_holder_name("EVP,") == "EVP,"
        assert _clean_beneficial_holder_name("EVP,") == "EVP"


class TestRaggedRowPercentRecovery:
    """#2140 D3 — issuers interleave footnote-only cells, so a data row can be
    WIDER than its header row and the positional percent cell lands wrong."""

    def test_percent_recovered_from_a_shifted_cell(self) -> None:
        # CYH 0001193125-26-140269 shape, reproduced in full: spanning row 0,
        # 5-wide label row 1, and 6-wide data rows carrying an interleaved
        # footnote-only cell. Values verified against edgartools' independent
        # extraction of the same filing.
        body = """
        <table>
          <tr><th></th><th></th><th>Shares Beneficially Owned (1)</th></tr>
          <tr><td>Name</td><td></td><td>Number</td><td></td><td>Percent</td></tr>
          <tr><td>Apollo Management Holdings GP, LLC</td><td></td><td>11,838,609</td>
              <td>(2)</td><td></td><td>8.4%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "Apollo Management Holdings GP, LLC"
        assert parsed.rows[0].shares == Decimal("11838609")
        assert parsed.rows[0].percent_of_class == Decimal("8.4")

    def test_bare_number_is_not_accepted_as_a_percent(self) -> None:
        # The fallback must only accept unambiguous percents ('%' or '*') —
        # a bare small number in another column would otherwise be read as a
        # percentage, which is the exact misfire class this ticket removes.
        body = """
        <table>
          <tr><th></th><th></th><th>Shares Beneficially Owned (1)</th></tr>
          <tr><td>Name</td><td></td><td>Number</td><td></td><td>Percent</td></tr>
          <tr><td>Jane Smith</td><td></td><td>250,000</td><td>(2)</td><td></td><td>42</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].shares == Decimal("250000")
        assert parsed.rows[0].percent_of_class is None


class TestPromotedLabelRowStillScoresTheTable:
    """#2158 D14 — the label arm promoted row 1 to ``column_headers`` but left
    ``score_headers`` on the spanning row 0, so Item 403's prescribed captions
    were never scored and the real table fell below the floor (or lost the
    sibling gate to a prose/ToC table). 181 accessions returned zero rows."""

    def test_share_class_spanning_row_does_not_hide_item_403_captions(self) -> None:
        # 0000908311-26-000065: row 0 is the share-CLASS row Item 403(a)'s "any
        # class" disclosure produces; row 1 carries the prescribed captions.
        # Scored on row 0 alone the table reaches 0 — below the floor of 3.
        headers = ("Common Stock", "Series A Preferred Stock")
        assert _score_table_headers(headers) == 0
        body = """
        <table>
          <tr><th>Common Stock</th><th>Series A Preferred Stock</th></tr>
          <tr><td>Name of Beneficial Owner</td><td>No. of Shares</td><td>Percent of Class</td></tr>
          <tr><td>Richard Ressler</td><td>91,986</td><td>3.3%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Richard Ressler"]
        assert parsed.rows[0].shares == Decimal("91986")
        assert parsed.rows[0].percent_of_class == Decimal("3.3")
        # The fold is what lifts it over the floor, so the diagnostic score
        # must reflect the captions the issuer actually used.
        assert parsed.raw_table_score >= 3

    def test_prose_footnote_table_no_longer_outranks_the_real_table(self) -> None:
        # 0000898432-21-000355: a layout <table> holding the ownership
        # FOOTNOTE scored 8 on incidental keyword hits, became the window
        # best, and the sibling gate (>=6 or ==best) dropped the real table.
        body = """
        <table>
          <tr><th></th><th>(1)</th>
              <th>Percentage of ownership is based on 12,312,184 shares of our common stock
                  issued and outstanding. Beneficial ownership is determined in accordance
                  with the rules of the SEC and generally includes voting power.</th></tr>
          <tr><td></td><td></td><td>See above.</td></tr>
        </table>
        <table>
          <tr><th></th><th></th><th>Beneficial Ownership</th><th></th></tr>
          <tr><td>Name and Address of Beneficial Owner</td><td></td><td>Number of Shares</td>
              <td></td><td>Percentage (1)</td></tr>
          <tr><td>Renaissance Technologies, LLC (2)</td><td></td><td>738,157</td>
              <td></td><td>6.0</td><td>%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert "Renaissance Technologies, LLC" in [r.holder_name for r in parsed.rows]

    def test_legacy_sole_shared_total_scoring_is_unchanged(self) -> None:
        # The legacy arm already folded both rows; this ticket must not move it.
        body = """
        <table>
          <tr><th>Name</th><th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td></td><td>Sole</td><td>Shared</td><td>Total</td><td></td></tr>
          <tr><td>John Doe</td><td>1,000</td><td>500</td><td>1,500</td><td>2.0%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows[0].holder_name == "John Doe"
        assert parsed.rows[0].shares == Decimal("1500")


class TestItem402gOptionExercisesAndStockVested:
    """#2158 — folding the promoted row made the Item 402(g) "Option Exercises
    and Stock Vested" table visible to the scorer for the first time (0 -> 5,
    over the floor of 3). Reg S-K Item 402(g)(2) prescribes its captions; Item
    403 reports a HOLDING, never an exercise/vesting EVENT, so they never
    collide. Without the disqualifier five guard-blocked accessions "recovered"
    vesting rows as beneficial owners."""

    def test_vesting_captions_disqualify_the_table(self) -> None:
        assert (
            _score_table_headers(
                (
                    "Stock Awards",
                    "Name",
                    "Number of Shares Acquired on Vesting (#)",
                    "Value Realized on Vesting ($)",
                )
            )
            == 0
        )

    def test_exercise_captions_disqualify_the_table(self) -> None:
        assert (
            _score_table_headers(
                (
                    "Option Awards",
                    "Name",
                    "Number of Shares Acquired on Exercise (#)",
                    "Value Realized on Exercise ($)",
                )
            )
            == 0
        )

    def test_exercisable_options_column_on_a_real_item_403_table_survives(self) -> None:
        # Guard for the narrowness claim: "acquired on exercise" is safe where
        # a bare "exercise" would not be — Hershey's real ownership table has
        # an "Exercisable Stock Options" column.
        assert (
            _score_table_headers(
                (
                    "Name of Beneficial Owner",
                    "Exercisable Stock Options",
                    "Total Beneficial Ownership",
                    "Percent of Class",
                )
            )
            > 0
        )

    def test_vesting_table_does_not_win_over_the_real_ownership_table(self) -> None:
        # 0001193125-26-103020 shape: the 402(g) table sits under a spanning
        # 'STOCK AWARDS' row, exactly like the Item 403 two-row layout.
        body = """
        <table>
          <tr><th></th><th></th><th>STOCK AWARDS (1)</th></tr>
          <tr><td>NAME</td><td></td><td>NUMBER OF SHARES ACQUIRED ON VESTING (#)</td>
              <td></td><td>VALUE REALIZED ON VESTING ($)</td></tr>
          <tr><td>Ajay Kataria</td><td></td><td>12,345</td><td></td><td>456,789</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows == []


class TestRule13d3DaysCaptionIsALabelNotData:
    """#2158 — `_looks_like_label_row` rejected any cell with a 2+ digit run.
    Rule 13d-3(d)(1)(i) deems a person the beneficial owner of securities
    acquirable "within sixty days", so Item 403 tables caption a column
    "Options Exercisable within 60 days of April 1, 2025" — the one numeric
    literal these captions legitimately carry."""

    def test_within_60_days_caption_row_is_promoted(self) -> None:
        # 0001437749-25-011586: the caption row scores 15 once promoted. Left
        # unpromoted the table scored 4 on its spanning title alone, and the
        # sibling gate then dropped the whole Item 403(b) subsection.
        row = (
            "Name",
            "",
            "Company Stock Beneficially Owned Excluding Options",
            "",
            "Company Stock Options Exercisable within 60 days of April 1, 2025",
            "",
            "Percent of Class (1)",
        )
        assert _looks_like_label_row(row)

    def test_a_share_count_row_is_still_data(self) -> None:
        assert not _looks_like_label_row(("Elliot Noss", "", "633,945", "", "3.5%"))

    def test_footnoted_share_count_is_still_data(self) -> None:
        # Without stripping footnote markers first, '1,000,000 [2]' reads as
        # prose and a data row whose holder is literally named "… Holder"
        # promotes over its own table.
        assert not _looks_like_label_row(("Bracketed Holder [1]", "1,000,000 [2]", "3.5%[3]"))

    def test_promoted_caption_row_lifts_the_table_over_the_sibling_gate(self) -> None:
        body = """
        <table>
          <tr><th></th><th></th><th>BENEFICIAL OWNERSHIP OF COMPANY STOCK</th><th></th></tr>
          <tr><td>Name</td><td></td><td>Company Stock Beneficially Owned</td><td></td>
              <td>Company Stock Options Exercisable within 60 days of April 1, 2025</td>
              <td></td><td>Percent of Class</td></tr>
          <tr><td>Allen Karp</td><td></td><td>633,945</td><td></td><td>1,000</td>
              <td></td><td>5.7%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Allen Karp"]
        assert parsed.raw_table_score >= 6


class TestEmptyTableCannotWinSelection:
    """#2158 — `_score_table_headers` reads headers only, so a layout <table>
    holding one prose paragraph can out-score the real Item 403 table and take
    the window while having no holders to report at all."""

    def test_prose_block_with_no_data_rows_does_not_displace_the_real_table(self) -> None:
        # 0000936468-25-000015 shape: a voting-methods prose block promoted its
        # own sentences as a "label row", scored 6, and left 0 data rows —
        # beating the real 5%-holder table at 5 and taking it to zero.
        body = """
        <table>
          <tr><th>Voting Methods</th><th>Registered Stockholder</th><th>Beneficial Owner</th></tr>
          <tr><td>Your shares are registered directly in your name with our transfer agent</td>
              <td>Your shares are allocated to a Company savings plan account</td>
              <td>Your shares are held in a stock brokerage account by a bank or broker</td></tr>
        </table>
        <table>
          <tr><th>Name and Address</th><th>Amount of Common Stock</th><th>Percent of Outstanding Shares</th></tr>
          <tr><td>The Vanguard Group</td><td>49,224,906</td><td>9.22%</td></tr>
          <tr><td>BlackRock, Inc.</td><td>44,982,057</td><td>8.43%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["The Vanguard Group", "BlackRock, Inc."]


class TestZeroWidthSpacersInValueCells:
    """#2164 — `_sct_norm` has scrubbed U+200B/U+200C/U+200D/U+2060/U+FEFF on
    the Item 402(c) path since #1945, but the Item 403 path reaches cell text
    through `_strip_inline_html`, which folded unicode SPACES and never learned
    zero-width. `str.strip()` does not treat these as whitespace, so a
    '​ 17,464 (2)' cell failed both `_parse_share_count` and
    `_parse_percent` and the row died on the 'neither parsed' guard."""

    def test_zero_width_prefixed_values_still_parse(self) -> None:
        # 0001140361-26-008786 shape: score 15, 18 raw rows, 0 holders on main.
        body = """
        <table>
          <tr><th></th><th>Name of Beneficial Owner</th><th></th>
              <th>Shares Beneficially Owned</th><th>Percentage of Total Voting Power (1)</th></tr>
          <tr><td></td><td>Robert Antoine</td><td></td>
              <td>​ 17,464 (2)</td><td>​ *</td></tr>
          <tr><td></td><td>William G. Smith, Jr.</td><td></td>
              <td>​ 2,970,720 (4)</td><td>​ 17.3%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("Robert Antoine", Decimal("17464"), Decimal("0.5")),
            ("William G. Smith, Jr.", Decimal("2970720"), Decimal("17.3")),
        ]

    def test_zero_width_is_stripped_at_the_value_parsers_not_upstream(self) -> None:
        # Stripping must NOT happen in _strip_inline_html: that feeds header
        # text into scoring / label-row promotion / window selection, and
        # emptying an all-U+200B header row on 0001558370-25-003243 promoted a
        # caption row, let an EARLIER window clear the floor, and lost
        # Vanguard / BlackRock / Dimensional / Harris / Victory. 35 genuine
        # holders across ~12 accessions, found by the full-population A/B.
        assert _strip_inline_html("<td>1​234,567</td>") == "1​234,567"
        # …and it MUST happen where a cell's meaning is read.
        assert _parse_share_count("​ 17,464 (2)") == Decimal("17464")
        assert _parse_percent("​ *") == Decimal("0.5")
        assert _parse_percent("﻿ 12.76%") == Decimal("12.76")
        # Holder identity too: holder_name_key is lower(trim(...)) and trim()
        # does not remove U+200B, so a zero-width suffix made one person two
        # identities — the #2140 D5 class. 554 holders re-keyed full-population.
        assert _clean_beneficial_holder_name("Abraham Ceesay​") == "Abraham Ceesay"


class TestStackedNameAddressRows:
    """#2164 — 17 CFR 229.403(a) prescribes ONE column, "Name and address of
    beneficial owner". Issuers routinely render it as TWO STACKED ROWS: the
    name on row N, the address plus the share count and percent on row N+1.
    Neither row survives alone, so every holder of such a table was lost."""

    def test_name_row_above_an_address_row_yields_one_holder(self) -> None:
        # 0000950170-25-048978 shape: 4 raw rows, 0 holders on main.
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>The Vanguard Group, Inc. (9)</td><td></td><td></td></tr>
          <tr><td>100 Vanguard Blvd. Malvern, PA 19355</td><td>94,052,723</td><td>12.76%</td></tr>
          <tr><td>BlackRock, Inc. (10)</td><td></td><td></td></tr>
          <tr><td>50 Hudson Yards New York, NY 10001</td><td>64,137,817</td><td>8.70%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("The Vanguard Group, Inc.", Decimal("94052723"), Decimal("12.76")),
            ("BlackRock, Inc.", Decimal("64137817"), Decimal("8.70")),
        ]

    def test_single_cell_name_row_carrying_a_title_is_not_a_section_heading(self) -> None:
        # 0000799233-25-000020 shape: the name row is colspan-collapsed to one
        # cell and its inline title matches the director/officer role patterns,
        # so `_detect_role_heading` ate it. Diverting it must NOT set
        # current_role — otherwise the 5% holders below inherit 'director'.
        body = """
        <table>
          <tr><th>Title of Class</th><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>Mr. Michael J. Gerdin, Chief Executive Officer, Chairman, President and Director</td></tr>
          <tr><td>Common Stock</td><td>901 Heartland Way, North Liberty, Iowa 52317</td>
              <td>31,805,618 (1)</td><td>40.5%</td></tr>
          <tr><td>Ms. Angela K. Janssen</td></tr>
          <tr><td>Common Stock</td><td>901 Heartland Way, North Liberty, Iowa 52317</td>
              <td>21,662,653 (3)</td><td>27.6%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.holder_role) for r in parsed.rows] == [
            (
                "Mr. Michael J. Gerdin, Chief Executive Officer, Chairman, President and Director",
                Decimal("31805618"),
                "director",
            ),
            # No sticky role leaked from the holder above — she is not an officer.
            ("Ms. Angela K. Janssen", Decimal("21662653"), None),
        ]

    def test_a_section_heading_is_never_promoted_to_a_holder_name(self) -> None:
        # The name half and a genuine section heading share the same single-cell
        # row shape. The positive owner-identity test is what separates them.
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>Directors and Executive Officers:</td></tr>
          <tr><td>c/o Acme Corporate Secretary</td><td>1,234,567</td><td>4.1%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == []

    def test_carry_requires_the_next_row_to_be_an_address_row(self) -> None:
        # A value-less name row followed by an ordinary data row must stay
        # dropped — the carry is gated on the stacked shape, not on "any row
        # with no values", so the behaviour change is bounded.
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>Some Annotation Row</td><td></td><td></td></tr>
          <tr><td>The Vanguard Group</td><td>49,224,906</td><td>9.22%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["The Vanguard Group"]

    def test_address_row_after_a_holder_that_had_values_is_still_dropped(self) -> None:
        # #2140's Dolan case (0001193125-25-095068) must not regress: the
        # preceding row emitted, so there is no pending name to attach and the
        # address continuation rows carrying real numbers stay dropped.
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>Dolan Family Group</td><td>1,074,594</td><td>3.19%</td></tr>
          <tr><td>c/o Dolan Family Office</td><td>11,484,408</td><td>100%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Dolan Family Group"]


class TestOwnerIdentitySeparatesNamesFromHeadings:
    """#2164 — the positive test that decides whether a value-less single-cell
    row is a holder NAME or a section heading."""

    def test_owner_identities_are_recognised(self) -> None:
        for text in (
            "Mr. Michael J. Gerdin, Chief Executive Officer, Chairman, President and Director",
            "Ms. Angela K. Janssen",
            "Ann S. Gerdin Revocable Trust",
            "The Vanguard Group, Inc.",
            "BlackRock, Inc.",
            "2009 Gerdin Heartland Trust, UTA July 15, 2009",
            "All directors and executive officers as a group (14 persons)",
        ):
            assert _is_owner_identity(text), text

    def test_section_headings_are_rejected(self) -> None:
        for text in (
            "Directors and Executive Officers:",
            "Other Shareowners that Beneficially Own More than 5%:",
            "5% Stockholders",
            "",
            "   ",
        ):
            assert not _is_owner_identity(text), text


class TestFivePercentHeadingIsOrderInsensitive:
    """#2164 — the old `5\\s*%.*holders?` required the noun AFTER the threshold
    and did not know 'shareowners', so 'Other Shareowners that Beneficially Own
    More than 5%' matched nothing, `current_role` stayed on the management
    block above, and BlackRock at 9.96% was tagged 'director'."""

    def test_compound_holder_nouns_still_match(self) -> None:
        # A leading \b cannot match inside 'Equityholders'. Anchoring it dropped
        # the 'principal' role from 40 holders (BlackRock / The Vanguard Group /
        # State Street on 0001193125-26-170704 and siblings) — found by the
        # full-population role audit, A/B arm 3.
        for heading in (
            "5% Equityholders",
            "5% Unitholders",
            "5% Noteholders",
            "5% Stockholders",
            "Other Shareowners that Beneficially Own More than 5%:",
        ):
            assert _detect_role_heading((heading, "")) == "principal", heading

    def test_ownership_is_not_a_holder_noun(self) -> None:
        # The TRAILING \b keeps 'ownership' out.
        assert _detect_role_heading(("Percentage Ownership of more than 5%", "")) != "principal"

    def test_noun_before_threshold_sets_the_principal_role(self) -> None:
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares Beneficially Owned</th>
              <th>Percentage of Total Voting Power</th></tr>
          <tr><td>Directors and Executive Officers:</td></tr>
          <tr><td>Ashbel C. Williams</td><td>6,242</td><td>*</td></tr>
          <tr><td>Other Shareowners that Beneficially Own More than 5%:</td></tr>
          <tr><td>BlackRock, Inc. (5)</td><td>1,707,759</td><td>9.96%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.holder_role) for r in parsed.rows] == [
            ("Ashbel C. Williams", "officer"),
            ("BlackRock, Inc.", "principal"),
        ]


class TestPercentStoredAsShareCount:
    """#2163 — 17 CFR 229.403 column 3 ("Amount and nature of beneficial
    ownership") is a COUNT of shares; column 4 ("Percent of class") is a
    percentage. A header row carrying empty SPACER cells its data rows do not
    carry is WIDER than the data, so ``shares_idx`` lands on the percent column.
    ``_parse_share_count('17.4')`` succeeds, so the ragged-row recovery never
    fires and the real count one cell to the left is discarded."""

    def test_spacer_widened_header_does_not_store_the_percent_as_shares(self) -> None:
        # 0001308179-24-000672 shape: headers ('Name and address of beneficial
        # owner', '', 'Number of shares', '', 'Percent of class*') — 5 cells
        # over 4-cell data rows — resolved (name=0, shares=2, percent=4), so
        # BlackRock stored shares=17.4 / percent=NULL and 6,236,345 was lost.
        body = """
        <table>
          <tr><th>Name and address of beneficial owner</th><th></th><th>Number of shares</th>
              <th></th><th>Percent of class*</th></tr>
          <tr><td>BlackRock, Inc.</td><td>6,236,345</td><td>17.4</td><td>%</td></tr>
          <tr><td>Vanguard Group, Inc.</td><td>3,761,632</td><td>10.5</td><td>%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("BlackRock, Inc.", Decimal("6236345"), Decimal("17.4")),
            ("Vanguard Group, Inc.", Decimal("3761632"), Decimal("10.5")),
        ]

    def test_a_whole_percent_is_caught_too(self) -> None:
        # The SQL proxy for this class (`shares <> trunc(shares)`) is a FLOOR: a
        # percent of exactly 5 is stored as 5 shares and is invisible to it. The
        # lone '%' sibling cell is what makes it detectable.
        body = """
        <table>
          <tr><th>Name and address of beneficial owner</th><th></th><th>Number of shares</th>
              <th></th><th>Percent of class</th></tr>
          <tr><td>State Street Corporation</td><td>1,904,790</td><td>5</td><td>%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("State Street Corporation", Decimal("1904790"), Decimal("5"))
        ]

    def test_a_share_count_above_the_percent_ceiling_is_never_held_back(self) -> None:
        # A percent of class is bounded by definition; _parse_percent has
        # clamped to [0, 100] since #1228. Without that ceiling a row rendering
        # the sign cell BEFORE the percent value would read the '%' sibling as
        # decisive and drop a genuine 1.2M-share holding.
        cells = ["Ninepoint Partners LP", "1,234,567", "%", "5.6"]
        headers = ("Name of Beneficial Owner", "Shares", "", "Percent")
        assert _shares_cell_percent_signature(cells, 1, headers) is None

    def test_a_fractional_value_with_nothing_better_in_the_row_is_kept(self) -> None:
        # 'fractional' alone is suggestive, not decisive — a fractional holding
        # is unusual but not impossible. With no whole candidate anywhere in the
        # row the original reading is restored rather than the row losing its
        # only number.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Amount and Nature of Beneficial Ownership</th></tr>
          <tr><td>Jane Q. Holder</td><td>1,234.5</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares) for r in parsed.rows] == [("Jane Q. Holder", Decimal("1234.5"))]


class TestEmbeddedSchedule13DCoverPage:
    """#2163 — 17 CFR 240.13d-101 / -102 prescribe a NUMBERED cover page for
    Schedules 13D / 13G. Proxies embed those cover pages as exhibits, and the
    numbered layout parses as a table whose holder names are the item labels and
    whose share counts are the ROW NUMBERS. 229.403 column 2 is a beneficial
    owner (a person or entity, per Rule 13d-3); an item label is not."""

    def test_cover_page_power_rows_are_not_holders(self) -> None:
        # 0001104659-17-023458 stored 'SHARED VOTING POWER -0' with shares=8,
        # 'SOLE DISPOSITIVE POWER 32,005,260 shares…' with shares=9, etc.
        body = """
        <table>
          <tr><th>NUMBER OF SHARES BENEFICIALLY OWNED BY EACH REPORTING PERSON WITH</th><th></th>
              <th>7.</th><th></th><th>SOLE VOTING POWER 32,005,260 shares of Common Stock</th></tr>
          <tr><td></td><td></td><td>8.</td><td></td><td>SHARED VOTING POWER -0-</td></tr>
          <tr><td></td><td></td><td>9.</td><td></td><td>SOLE DISPOSITIVE POWER 32,005,260 shares</td></tr>
          <tr><td></td><td></td><td>10.</td><td></td><td>SHARED DISPOSITIVE POWER -0-</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert parsed.rows == []

    def test_the_label_test_is_anchored_and_spares_real_holders(self) -> None:
        assert _SCHEDULE_13D_COVER_LABEL_RE.match("SHARED VOTING POWER -0")
        # Rule 13d-3 says voting OR INVESTMENT power; 0001308179-25-000114
        # renders a TRANSPOSED table whose rows are the four power types.
        assert _SCHEDULE_13D_COVER_LABEL_RE.match("Sole investment power")
        assert _SCHEDULE_13D_COVER_LABEL_RE.match("Shared investment power")
        assert _SCHEDULE_13D_COVER_LABEL_RE.match("Aggregate Amount Beneficially Owned by Each Reporting Person")
        assert _SCHEDULE_13D_COVER_LABEL_RE.match("TYPE OF REPORTING PERSON")
        assert not _SCHEDULE_13D_COVER_LABEL_RE.match("BlackRock, Inc.")
        assert not _SCHEDULE_13D_COVER_LABEL_RE.match("Power Corporation of Canada")
        assert not _SCHEDULE_13D_COVER_LABEL_RE.match("Sole Proprietor Holdings LLC")


class TestTwoPercentColumnsOrdering:
    """#2163 A/B round 3. Multi-class Item 403 tables carry SEVERAL percent
    columns, and `_resolve_columns`' `total` shares-tier happily matches
    'Percent of Total Voting Rights'. The held-back cell is therefore only a
    LAST-resort percent source: it must never pre-empt the ragged-row scan,
    which accepts only cells carrying a '%' or the '*' marker."""

    def test_an_unambiguous_percent_cell_beats_the_held_back_cell(self) -> None:
        # 0000950170-24-100030 (Richardson Electronics) shape. On main this
        # stored shares=98.1 / percent=14.8; pre-empting the scan stored
        # percent=98.1 and lost the real 14.8.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares of Common Stock</th>
              <th>Percent of Common Stock Class</th><th>Percent of Total Voting Rights</th></tr>
          <tr><td>Edward J. Richardson</td><td>2,129,271</td><td>14.8%</td><td>98.1</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("Edward J. Richardson", Decimal("2129271"), Decimal("14.8"))
        ]

    def test_the_recovery_scan_will_not_take_another_percent_as_the_count(self) -> None:
        # Regression B: when the primary cell was held back as a percent, the
        # recovery must not simply grab the next percent along
        # (0001177394-25-000016 held back 30.0 then recovered 100.0).
        cells = ["Dennis Polk", "30.0", "100.0", "%"]
        headers = ("Name", "Target as a Percentage of Base Salary", "Maximum", "")
        assert _shares_cell_percent_signature(cells, 2, headers) is not None

    def test_a_small_holding_before_a_lone_percent_sign_is_not_dropped(self) -> None:
        # Codex ckpt-2. When the issuer renders the '%' sign in its OWN column,
        # a genuine small holding sits immediately to its left and trips the
        # sibling test. That evidence is POSITIONAL, so it is "weak": with no
        # whole-number alternative in the row the original reading is restored
        # rather than the row losing its share count.
        body = """
        <table>
          <tr><th>Name of Beneficial Owner</th><th>Shares Beneficially Owned</th>
              <th></th><th>Percent of Class</th></tr>
          <tr><td>Jane Q. Director</td><td>50</td><td>%</td><td>0.1</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares) for r in parsed.rows] == [("Jane Q. Director", Decimal("50"))]

    def test_a_percent_CAPTION_is_still_decisive(self) -> None:
        # The complement: caption evidence is SEMANTIC, so it is never restored.
        cells = ["Simon Leung", "29.6"]
        headers = ("Name", "Minimum Payment (if Threshold is Met) as Percentage of Base Salary")
        assert _shares_cell_percent_signature(cells, 1, headers) == "decisive"


class TestItem403ValueSignature:
    """#2160 D4 — the Item 403 VALUE-column gate.

    The first cut of this gate emptied 14 of the regression fixtures below and
    3.6% of the corpus. Each group here pins one of the four defects that
    measuring the NARROWING direction exposed.
    """

    def test_bare_percent_caption_is_admitted(self) -> None:
        """229.403 column 4 is 'Percent of class', but issuers caption it bare
        and leave the class implied by the neighbouring amount column. The
        first cut required a class noun AFTER the percent token and rejected
        every one of these."""
        for headers in (
            ("Name", "", "Number", "", "Percent"),  # CYH 0001193125-26-140269
            ("Name of Beneficial Owner", "Shares Beneficially Owned", "Percent"),
            ("Name of Beneficial Owner", "Shares Beneficially Owned", "%"),
            ("Name", "Shares", "Percent"),
            ("Beneficial Owner", "Shares Owned", "Percent"),
            ("Name of Beneficial Owner", "", "Number", "", "Percentage"),
        ):
            assert _item403_value_signature(headers), headers

    def test_prescribed_wording_outranks_the_compensation_veto(self) -> None:
        """Rule 13d-3(d)(1)(i) DEEMS a person the beneficial owner of shares
        acquirable within 60 days, so a genuine Item 403 table legitimately
        carries 'vesting' and 'performance' columns. A blanket comp veto over
        the whole header deleted 18-, 22- and 10-holder Vanguard / BlackRock /
        First Eagle tables."""
        for headers in (
            (
                "Name and Address",
                "Number of Outstanding Shares Beneficially Owned",
                "Number of Shares Underlying RSUs/MSUs vesting within 60 days",
            ),
            (
                "Number of Shares Beneficially Owned (1)",
                "Percentage of Outstanding Shares",
                "Number of Performance Shares Granted",
            ),
            (
                "NAME OF BENEFICIAL OWNER",
                "COMMON STOCK",
                "OPTIONS EXERCISABLE OR VESTING WITHIN 60 DAYS",
                "AMOUNT OWNED",
            ),
        ):
            assert _item403_value_signature(headers), headers

    def test_value_columns_saying_owned_need_no_percent_column(self) -> None:
        """Dual-class and direct/indirect tables omit column 4 outright. The
        word 'Beneficial' sits in the NAME column, a '|' away, so the strong
        arm cannot reach it and the amount+percent pair rejects them."""
        for headers in (
            (
                "Name of Beneficial Owner",
                "Class A Common Stock Owned",
                "Class B Common Stock Owned",
                "Total Voting Power",
            ),
            (
                "Name of Beneficial Owner",
                "Directly Owned (a)",
                "Indirectly Owned",
                "Options to Acquire Stock (b)",
            ),
        ):
            assert _item403_value_signature(headers), headers

    def test_item_402_tables_are_still_rejected(self) -> None:
        """The gate must not be loosened into a no-op. 'Named Executive
        Officer' is Item 402(a)(3)'s own term of art and vetoes the weak pair;
        'Beneficial Owner | Number of RSUs' is why the column-3 arm keys on
        own(ed|ership) and not on 'owner'."""
        for headers in (
            ("Named Executive Officer", "Shares at Target", "Final PSU Payout %"),
            ("Named Executive Officer", "2022 Fiscal Year PSU Shares Granted (#)", "Final Achievement %"),
            ("Name", "Threshold (Percentage of Base Salary)", "Target (Percentage of Base Salary)"),
            ("Name of Individual or Identity of Group and Position", "Shares Underlying Options"),
            ("Beneficial Owner", "Number of RSUs"),
            ("Position", "Minimum Dollar Value", "Minimum Number of Shares"),
            ("", "Authorized for issuance", "Issued and outstanding"),
            ("50 th Percentile", "25 th Percentile"),
        ):
            assert not _item403_value_signature(headers), headers


class TestBeneficialOwnerIdentityIgnoresLeaderDots:
    """#2160 D1 — presentation debris is stripped BEFORE the length cap.

    Issuers pad the name column with HTML leader dots to rule across to the
    figures. Testing the raw cell blew the 120-char cap, rejected the holder,
    took the table under ``_ROW_IDENTITY_FLOOR`` and dropped a genuine
    'Amount and Nature of Beneficial Ownership | Percent of Class' table
    (0000074303-25-000056).
    """

    def test_a_name_padded_with_leader_dots_is_still_an_identity(self) -> None:
        padded = "Hotchkis & Wiley Capital Management, LLC " + "." * 100
        assert len(padded) > 120
        assert _is_beneficial_owner_identity(padded)
        assert _is_beneficial_owner_identity("BlackRock, Inc. " + "." * 90)

    def test_the_length_cap_still_rejects_a_footnote_paragraph(self) -> None:
        """The cap exists to keep Schedule 13G footnote PARAGRAPHS out of the
        holder set; stripping debris must not defeat it."""
        assert not _is_beneficial_owner_identity(
            "Based solely on an amendment to a Schedule 13G filed by BlackRock, Inc. "
            "with the SEC on January 29, 2025, reporting sole voting power over "
            "12,312,184 shares of our common stock."
        )


class TestGateArmsCannotOverreach:
    """#2160 — Codex ckpt-1 findings. Each strong arm admits OUTRIGHT, ahead of
    the Item 402 vetoes, so each one's precision is load-bearing."""

    def test_the_60_day_window_needs_an_acquisition_verb(self) -> None:
        """Rule 13d-3(d)(1)(i) is about securities a person has the right to
        ACQUIRE within sixty days. The bare phrase also appears in
        change-in-control and termination tables, and admitting those ahead of
        the comp veto would emit severance data as beneficial ownership."""
        assert _item403_value_signature(("Name", "Options Exercisable or Vesting Within 60 Days", "Common Stock"))
        assert _item403_value_signature(("Name", "Subject to Rights to Acquire Within 60 Days"))
        assert not _item403_value_signature(
            ("Named Executive Officer", "Payments upon Termination within 60 Days", "Cash Severance ($)")
        )

    def test_the_name_and_address_arm_needs_address_evidence(self) -> None:
        """Item 403(a) prescribes name AND address in one column. A proper-noun
        run followed by any digit is not that -- it also matches metric rows."""
        for text in (
            "MUFG 4-5, Marunouchi 1-chome Chiyoda-ku, Tokyo 100-8330, Japan",
            "Vanguard 100 Vanguard Boulevard Malvern, PA 19355",
            "BlackRock 50 Hudson Yards New York, NY 10001",
        ):
            assert _is_name_then_address(text), text
        for text in ("Adjusted EBITDA 2024", "Net Sales 2025", "Retail Adjusted EBITDA 2024"):
            assert not _is_name_then_address(text), text


class TestItem403DataRowValueFallback:
    """#2160 — D4's data-row fallback, borrowed from edgartools'
    ``_build_column_map`` (skill G17).

    A header-only gate empties genuine Item 403 tables whose captions have
    degraded to blank cells. 17 CFR 229.403 prescribes column 3 (amount) AND
    column 4 (percent of class); when the captions are gone, both columns
    PARSING for a majority of rows is the remaining evidence that both exist.
    """

    @staticmethod
    def _holder(name: str, shares: str | None, percent: str | None) -> Def14ABeneficialHolder:
        return Def14ABeneficialHolder(
            holder_name=name,
            shares=Decimal(shares) if shares is not None else None,
            percent_of_class=Decimal(percent) if percent is not None else None,
            holder_role=None,
        )

    def test_both_value_columns_parsing_is_evidence(self) -> None:
        holders = [
            self._holder("BlackRock, Inc.", "1000", "5.1"),
            self._holder("First Light Asset Management, LLC", "900", "4.2"),
            self._holder("Soleus Capital Master Fund, L.P.", "800", "3.9"),
        ]
        assert _has_item403_value_rows(holders)

    def test_an_amount_column_alone_is_NOT_evidence(self) -> None:
        """An Item 402 award table has an amount column and no percent. Requiring
        BOTH is what keeps this from becoming a bypass."""
        holders = [
            self._holder("Kevin R.M. Smith", "1000", None),
            self._holder("Jennifer F. Scanlon", "900", None),
            self._holder("Dr. Hou", "800", None),
        ]
        assert not _has_item403_value_rows(holders)

    def test_empty_holder_set_is_not_evidence(self) -> None:
        assert not _has_item403_value_rows([])

    def test_the_fallback_ranks_BELOW_the_item_402_vetoes(self) -> None:
        """Weak evidence must not bypass the veto — a comp table with a payout
        percent column also satisfies the data-row test."""
        comp = ("Named Executive Officer", "Shares at Target", "Final PSU Payout %")
        assert not _item403_value_signature(comp, data_row_evidence=True)
        salary = ("Name", "Percentage of Annual Total Direct Compensation")
        assert not _item403_value_signature(salary, data_row_evidence=True)

    def test_the_fallback_admits_a_caption_less_table(self) -> None:
        bare = ("", "", "", "Number of Shares", "", "", "", "", "", "Number of Shares", "", "")
        assert not _item403_value_signature(bare)
        assert _item403_value_signature(bare, data_row_evidence=True)


class TestStrongArmsCannotAdmitItem402Outcomes:
    """#2160 Codex ckpt-1 HIGH — the precedence design is only sound if every
    STRONG arm is near-perfect, because a strong arm admits AHEAD of the Item 402
    veto. ``_ITEM403_CLASS_PCT_RE`` was not.

    229.403 column 4 is "Percent of CLASS": the denominator is a class of
    securities, so the class-noun run ENDS the denominator phrase. A trailing
    participle makes it a percent of an OUTCOME, which is Item 402.
    """

    def test_a_percent_of_an_outcome_is_not_a_percent_of_class(self) -> None:
        for headers in (
            ("Name", "Percentage of Shares Earned"),
            ("Name", "Percentage of Shares Vested"),
            ("Name", "Percentage of Common Stock Earned"),
            ("Name", "Percentage of Total Stock Earned"),
            ("Name", "Percentage of Stock Options Vesting"),
            ("Name", "Percentage of total stock incentive awards (%)"),
        ):
            assert not _item403_value_signature(headers), headers

    def test_the_class_noun_run_does_not_backtrack(self) -> None:
        """'Percentage of Common Stock Earned' matched by consuming only
        'Common' and seeing 'Stock' in the allowed-follow set. The run is
        possessive so that path is closed."""
        assert not _item403_value_signature(("Name", "Percentage of Common Stock Earned"))
        assert _item403_value_signature(("Name", "Percentage of Common Stock"))

    def test_genuine_class_denominators_still_admit(self) -> None:
        for headers in (
            ("Name of Beneficial Owner", "Shares", "Percent of Class (1)"),
            ("Name", "Shares (1)", "Percent of Outstanding Shares of Common Stock"),
            ("Shareholder", "Number of Voting Rights (#)", "Percentage of Voting Rights (%)"),
            ("Name", "Number of Shares", "Percent of Total Voting Power"),
            ("Name", "Shares (1)", "% of all shares of Class A common stock"),
            ("Name", "Number of Common Shares", "Approximate Percentage of Outstanding Common"),
        ):
            assert _item403_value_signature(headers), headers

    def test_the_data_row_fallback_cannot_admit_an_award_outcome_table(self) -> None:
        """Codex ckpt-1 HIGH-2: 'Shares at Target | Final Achievement %' parses
        both a share count and a percent for every NEO row, so the data-row
        evidence is TRUE. The Item 402 outcome vocabulary is what rejects it."""
        headers = ("Name", "Shares at Target", "Final Achievement %")
        assert not _item403_value_signature(headers, data_row_evidence=True)


class TestHeadingTestDoesNotRejectNamedHolders:
    """#2160 arm-1/arm-2 round 2 — ``_HOLDER_CLASS_PLURAL_RE`` is a SECTION-HEADING
    test (#2164) and it outranks the entity arm on purpose, so that 'Directors and
    Executive Officers of the Company' cannot be rescued by its trailing 'Company'.

    But Schedule 13D/G joint-filer names legitimately contain those same class
    nouns. Hyatt 0001104659-26-038759 lost an 11-holder sibling table this way,
    taking the Pritzker family trusts, CIBC Caribbean, Massachusetts Financial
    Services and Baron Capital with it.

    A heading names a class abstractly; a holder carries a SPECIFIC proper name.
    """

    def test_joint_filer_holder_names_survive(self) -> None:
        for text in (
            "CIBC Caribbean and Other Reporting Persons",
            "Trustees of the Thomas J. Pritzker Family Trusts and Other Reporting Persons",
            "Trustees of the Karen L. Pritzker Family Trusts",
        ):
            assert _is_beneficial_owner_identity(text), text

    def test_section_headings_are_still_rejected(self) -> None:
        for text in (
            "Directors and Executive Officers:",
            "Directors and Executive Officers of the Company",
            "Other Shareowners that Beneficially Own More than 5%:",
            "5% Stockholders",
            "Named Executive Officers",
        ):
            assert not _is_beneficial_owner_identity(text), text

    def test_the_rescue_needs_hard_proper_noun_evidence(self) -> None:
        """A qualifying name RUN alone is not enough — an initial, an all-caps
        entity token, or a corporate designator must also be present."""
        assert not _contains_specific_name("Directors and Executive Officers of the Company")
        assert _contains_specific_name("Trustees of the Thomas J. Pritzker Family Trusts")


class TestAmpersandFirmNames:
    """#2160 Codex ckpt-2 P2 — partnership-style firm names joined by '&'.

    They carry no corporate suffix, so the entity arm misses them, and the
    start-anchored person arm stops dead at the '&'. Under the new selection
    gate that made a small 5%-holder table ineligible outright. Common in Item
    403(a): the corpus holds Cohen & Steers, Cooke & Bieler, Cede & Co and
    Bill & Melinda Gates Foundation Trust.
    """

    def test_ampersand_firms_are_owner_identities(self) -> None:
        for text in (
            "Dodge & Cox 555 California Street San Francisco, CA 94104",
            "Brown & Brown 220 South Ridgewood Avenue Daytona Beach, FL 32114",
            "Cohen & Steers",
            "Cede & Co",
            "Ruane, Cunniff & Goldfarb 9 West 57th Street New York, NY 10019",
            "Bill & Melinda Gates Foundation Trust",
        ):
            assert _is_beneficial_owner_identity(text), text

    def test_ampersand_does_not_rescue_a_heading(self) -> None:
        for text in ("Directors and Executive Officers:", "5% Stockholders"):
            assert not _is_beneficial_owner_identity(text), text


class TestRowSpanExpansion:
    """#2175 — the HTML table model, which this parser did not implement.

    A ``rowspan=N`` cell occupies the same COLUMN SLOT in the next N-1 rows
    (HTML Living Standard §4.9.12, downward-growing cells), so those rows carry
    fewer ``<td>``s and everything in them sits further LEFT than its markup
    position. Reading markup position as column index mis-columns every
    continuation row — which is why an Item 403 multi-series table stored the
    'Title of Series' ticker as the beneficial owner.
    """

    def test_a_leading_span_is_carried_down_into_the_following_rows(self) -> None:
        rows: list[tuple[tuple[str, int, int], ...]] = [
            (("John C. Malone", 3, 1), ("LLYVA", 1, 1), ("251", 1, 1)),
            (("LLYVB", 1, 1), ("18", 1, 1)),
            (("LLYVK", 1, 1), ("5", 1, 1)),
        ]
        assert [row.cells for row in _expand_row_spans(rows)] == [
            ("John C. Malone", "LLYVA", "251"),
            ("John C. Malone", "LLYVB", "18"),
            ("John C. Malone", "LLYVK", "5"),
        ]

    def test_a_span_beyond_a_short_rows_own_cells_still_occupies_its_slot(self) -> None:
        """The trailing-remainder branch: the spanning cell's column index is
        past the end of the next row's own cells, so the per-cell drain never
        reaches it and only the after-the-loop pass places it."""
        rows: list[tuple[tuple[str, int, int], ...]] = [
            (("Common Stock", 1, 1), ("Vanguard", 1, 1), ("footnote (1)", 2, 1)),
            (("Common Stock", 1, 1), ("BlackRock", 1, 1)),
        ]
        assert [row.cells for row in _expand_row_spans(rows)] == [
            ("Common Stock", "Vanguard", "footnote (1)"),
            ("Common Stock", "BlackRock", "footnote (1)"),
        ]

    def test_rowspan_zero_is_treated_as_one(self) -> None:
        """HTML reads ``rowspan=0`` as 'to the end of the row group'. EDGAR
        proxies do not use it, and the pandas reference treats it as 1 — so a
        malformed attribute must not invent a span over the whole table."""
        rows: list[tuple[tuple[str, int, int], ...]] = [
            (("Name", 0, 1), ("100", 1, 1)),
            (("Other", 1, 1), ("200", 1, 1)),
        ]
        assert [row.cells for row in _expand_row_spans(rows)] == [("Name", "100"), ("Other", "200")]

    def test_a_carried_cell_is_placed_by_LAYOUT_column_not_markup_index(self) -> None:
        """0001628280-26-025998: ``colspan=3 | colspan=6 | rowspan=4 colspan=3``.
        The spanning caption's markup index is 2 and its layout column is 9. A
        markup-index carry inserted it third in the label row, which wrecked the
        promoted header and took a 16-holder Item 403 table to ZERO rows."""
        rows: list[tuple[tuple[str, int, int], ...]] = [
            (("Class A Common Stock", 1, 3), ("Class B Common Stock", 1, 6), ("% of Total Voting Power", 2, 3)),
            (("Name of Beneficial Owner", 1, 3), ("Shares", 1, 3), ("%", 1, 3)),
        ]
        assert [row.cells for row in _expand_row_spans(rows)] == [
            ("Class A Common Stock", "Class B Common Stock", "% of Total Voting Power"),
            # Appended LAST — layout column 9 is right of every own cell.
            ("Name of Beneficial Owner", "Shares", "%", "% of Total Voting Power"),
        ]

    def test_a_BLANK_cell_spacer_row_is_a_spacer_too(self) -> None:
        """Codex ckpt-2 P2. EDGAR renders spacers both ways — no cells at all, and
        a row of blank cells (`<tr><td>&nbsp;</td></tr>`, which
        ``_strip_inline_html`` returns as ``''``). Testing only `not cells`
        materialised the second into a phantom row carrying the spanning caption,
        which is the shape that reaches the header promotion.

        ⚠ A U+200B cell is NOT blank (`'\\u200b'` is a non-empty, non-whitespace
        string) and is deliberately not covered — ``main`` kept those rows too, so
        treating them as spacers would be a change this A/B has not measured."""
        rows: list[tuple[tuple[str, int, int], ...]] = [
            (("Name", 3, 1), ("Shares", 1, 1)),
            (("", 1, 1), ("", 1, 1)),
            (("100", 1, 1),),
        ]
        assert [row.cells for row in _expand_row_spans(rows)] == [("Name", "Shares"), (), ("Name", "100")]

    def test_a_cell_less_spacer_row_emits_nothing_but_still_consumes_the_span(self) -> None:
        """EDGAR's generated markup puts cell-less ``<tr>``s around header rows.
        They are rows per the table model, so a span must decay across them —
        but everything they could show repeats the row above, and emitting one
        inserted a phantom header row that the promotion then adopted."""
        rows: list[tuple[tuple[str, int, int], ...]] = [
            (("Name", 3, 1), ("Shares", 1, 1)),
            (),
            (("100", 1, 1),),
            (("200", 1, 1),),
        ]
        assert [row.cells for row in _expand_row_spans(rows)] == [
            ("Name", "Shares"),
            (),
            ("Name", "100"),
            # The span was 3 rows and the spacer consumed the second, so it has
            # expired by here — a spacer that emitted would have shifted this.
            ("200",),
        ]

    def test_a_second_percent_caption_does_not_win_the_shares_column(self) -> None:
        """17 CFR 229.403 column 3 is a COUNT, column 4 a PERCENT. The percent
        pass takes only the FIRST match, so on a multi-class table the second
        percent caption stayed eligible for the shares tiering — and 'total' is
        the top tier, so '% of Total Voting Power' outranked the real 'Shares'
        column and filed a Class B count against a Class A percent."""
        headers = ("Name of Beneficial Owner", "Shares", "%", "Shares", "%", "% of Total Voting Power †")
        assert _resolve_columns(headers) == (0, 1, 2)

    def test_a_stacked_address_row_does_not_inherit_the_holders_figures(self) -> None:
        """0000107140-24-000176: the VALUE cells carry ``rowspan=2`` over a
        stacked name/address pair. Before the expansion the address row parsed
        no values and died on the 'neither shares nor percent' guard; after it,
        the row inherited BlackRock's own 6,782,743 / 14.97% and stored a
        verbatim duplicate under the name 'New York, NY 10055'."""
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>BlackRock, Inc.</td><td rowspan="2">6,782,743</td><td rowspan="2">14.97%</td></tr>
          <tr><td>New York, NY 10055</td></tr>
          <tr><td>The Vanguard Group, Inc.</td><td rowspan="2">5,466,211</td><td rowspan="2">12.07%</td></tr>
          <tr><td>Malverne, PA 19355</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("BlackRock, Inc.", Decimal("6782743"), Decimal("14.97")),
            ("The Vanguard Group, Inc.", Decimal("5466211"), Decimal("12.07")),
        ]

    def test_an_inherited_value_row_is_dropped_even_when_its_cell_reads_as_a_name(self) -> None:
        """The same accession's other case, and the one no NAME test can reach:
        E.P. Hamilton's address continuation is the law firm 'Baker Botts
        L.L.P. 2001 Ross Avenue, Suite 900 Dallas, TX 75201' — a corporate name
        with a street address, indistinguishable in isolation from a genuine
        nominee holder. Only the markup separates them: every figure in the row
        is inherited. Deliberately NOT an address-only cell, so this test fails
        if the continuation rule is removed even though the address rule stays."""
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>
          <tr><td>E.P. Hamilton Trusts, LLC (2)(7)</td>
              <td rowspan="2">462,338</td><td rowspan="2">1.02%</td></tr>
          <tr><td>Baker Botts L.L.P. 2001 Ross Avenue, Suite 900 Dallas, TX 75201</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("E.P. Hamilton Trusts, LLC", Decimal("462338"), Decimal("1.02")),
        ]

    def test_a_two_row_header_inheriting_a_spanning_caption_is_not_a_continuation(self) -> None:
        """The rule above must test that the INHERITED cells are values. A
        two-row header's second row also inherits (the spanning caption) and
        also has no own value — dropping it left the table with no label row and
        took 0001628280-26-025998's 16 holders to zero a second time."""
        body = """
        <table>
          <tr><td colspan="3">Class A Common Stock</td><td colspan="3">Class B Common Stock</td>
              <td colspan="3" rowspan="2">% of Total Voting Power</td></tr>
          <tr><td colspan="3">Name of Beneficial Owner</td><td colspan="3">Shares</td></tr>
          <tr><td>The Vanguard Group (1)</td><td>13,114,167</td><td>7.90%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["The Vanguard Group"]

    def test_multi_series_table_names_the_holder_not_the_series_ticker(self) -> None:
        """0001104659-25-029081 (Liberty Media) shape. On main this stored
        'LLYVB' / 'LLYVK' as beneficial owners with share counts read from
        whichever cell the recovery scan reached."""
        body = """
        <table>
          <tr><th>Name</th><th>Title of Series</th>
              <th>Amount and Nature of Beneficial Ownership</th><th>Percent of Series</th></tr>
          <tr><td rowspan="3">Chase Carey, Director</td><td>LLYVA</td><td>1,200</td><td>1.1%</td></tr>
          <tr><td>LLYVB</td><td>—</td><td>—</td></tr>
          <tr><td>LLYVK</td><td>1,425</td><td>2.2%</td></tr>
          <tr><td rowspan="2">Berkshire Hathaway, Inc.</td><td>LLYVA</td><td>4,986,588</td><td>8.4%</td></tr>
          <tr><td>LLYVK</td><td>2,000,000</td><td>3.1%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        names = [r.holder_name for r in parsed.rows]
        assert names == ["Chase Carey, Director", "Berkshire Hathaway, Inc."]
        # No series ticker survives as a holder identity.
        assert not [n for n in names if n in ("LLYVA", "LLYVB", "LLYVK")]
        # The kept row is the FIRST series the issuer lists. Collapsing a
        # holder's N series rows to one is the dedup in ``_extract_holder_rows``
        # (identity-keyed, Item 403 counts a beneficial owner once) and there is
        # no class column to key on — tracked separately, see the class
        # docstring reference in the module.
        assert [(r.shares, r.percent_of_class) for r in parsed.rows] == [
            (Decimal("1200"), Decimal("1.1")),
            (Decimal("4986588"), Decimal("8.4")),
        ]


class TestStackedCellHolders:
    """#2169 — one ``<tr>`` holding N beneficial owners, ``<br>``-stacked.

    17 CFR 229.403(a)/(b) prescribe a table with one entry per beneficial owner
    and ONE amount per entry (column 3), so a value cell holding two whole share
    counts on separate lines is two owners rendered inside one ``<tr>``, not one
    owner with two figures. ``0000351998-18-000006`` scores 12 on a textbook
    Item 403 header and stored ZERO holders: neither value parser accepts the
    stacked cell, so the row died on the "neither shares nor percent" guard.

    Every gate has its own test, because the risk here is the OPPOSITE
    direction — #2140 D5 flattens interior line breaks precisely because a
    render wrap otherwise splits one person across two holder identities
    (704 rows / 117 instruments full-pop). The trigger is the VALUE side.
    """

    def test_one_tr_holding_two_holders_yields_two_rows(self) -> None:
        """The ticket's accession, cell-for-cell: two amounts, two percents,
        two footnote markers, and a name cell whose two blocks are separated by
        a blank line and internally wrapped mid-address."""
        body = """
        <table>
          <tr><th>Name and Address</th><th>Amount and Nature of Beneficial Ownership</th>
              <th></th><th>Percent of Shares Outstanding</th></tr>
          <tr><td>Penbrook Management, LLC
AnKap Partners, L.P.
880 Third Avenue, 16 th
Floor
New York, NY 10022

Renaissance Technologies LLC
800
Third Avenue
New
York, NY 10022</td><td>486,340



658,400</td><td>(1)



(2)</td><td>5.86%



7.94%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.shares, r.percent_of_class) for r in parsed.rows] == [
            (Decimal("486340"), Decimal("5.86")),
            (Decimal("658400"), Decimal("7.94")),
        ]
        assert parsed.rows[0].holder_name.startswith("Penbrook Management, LLC AnKap Partners, L.P.")
        assert parsed.rows[1].holder_name.startswith("Renaissance Technologies LLC")

    def test_a_wrapped_name_over_a_single_amount_is_not_split(self) -> None:
        """#2140 D5's flatten must survive intact. A render wrap puts interior
        line breaks in the name cell — including a blank one — but the value
        side does not stack, so the row is one holder and the name flattens."""
        body = """
        <table>
          <tr><th>Name and Address of Beneficial Owner</th>
              <th>Number of Shares</th><th>Percent of Class</th></tr>
          <tr><td>Michael
 O. Johnson

 55 East 52nd Street</td><td>1,500,000</td><td>5.5%</td></tr>
          <tr><td>The Vanguard Group</td><td>3,000,000</td><td>11.0%</td></tr>
        </table>
        """
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares) for r in parsed.rows] == [
            ("Michael O. Johnson 55 East 52nd Street", Decimal("1500000")),
            ("The Vanguard Group", Decimal("3000000")),
        ]

    def test_a_partially_numeric_value_stack_is_not_split(self) -> None:
        """Gate 1. A stacked value cell whose segments do not ALL parse as
        whole share counts is not evidence of k owners — the count would then
        rest on whichever lines happened to be numeric, and the name blocks
        would be aligned against it."""
        assert (
            _split_stacked_holder_row(
                ("Alpha Capital LLC\n\nBeta Partners LP", "486,340\nsee note\n658,400", "5.86%\n\n7.94%"),
                name_idx=0,
                shares_idx=1,
                percent_idx=2,
            )
            is None
        )

    def test_value_columns_that_disagree_on_the_count_are_not_split(self) -> None:
        """Gate 2. Two amounts against three percents cannot both be right, and
        picking either would align the name blocks against a count the other
        column contradicts."""
        assert (
            _split_stacked_holder_row(
                ("Alpha Capital LLC\n\nBeta Partners LP", "486,340\n\n658,400", "5.86%\n7.10%\n7.94%"),
                name_idx=0,
                shares_idx=1,
                percent_idx=2,
            )
            is None
        )

    def test_a_name_cell_with_no_blank_line_separator_is_not_split(self) -> None:
        """Gate 3. Without a blank line the name cell yields ONE block, so
        there is no construction that assigns the second amount an owner — the
        row is left exactly as it is today rather than guessed at."""
        assert (
            _split_stacked_holder_row(
                ("Alpha Capital LLC\nBeta Partners LP", "486,340\n\n658,400", "5.86%\n\n7.94%"),
                name_idx=0,
                shares_idx=1,
                percent_idx=2,
            )
            is None
        )

    def test_a_stacked_row_distributes_every_aligned_cell_by_ordinal(self) -> None:
        """The split is positional across the row: a non-value column carrying
        exactly k non-empty lines (the footnote column of the cited accession)
        is distributed, and one that does not align is blanked rather than left
        holding a two-value string the recovery scans would then read."""
        assert _split_stacked_holder_row(
            (
                "Alpha Capital LLC\n\nBeta Partners LP",
                "486,340\n\n658,400",
                "(1)\n\n(2)",
                "unaligned",
                "5.86%\n\n7.94%",
            ),
            name_idx=0,
            shares_idx=1,
            percent_idx=4,
        ) == [
            ("Alpha Capital LLC", "486,340", "(1)", "", "5.86%"),
            ("Beta Partners LP", "658,400", "(2)", "", "7.94%"),
        ]

    def test_a_percent_only_stack_splits_when_the_amounts_are_absent(self) -> None:
        """229.403 column 4 is as prescribed as column 3, and issuers omit the
        count column outright. Two percents over two name blocks is the same
        evidence."""
        assert _split_stacked_holder_row(
            ("Alpha Capital LLC\n\nBeta Partners LP", "—\n\n—", "5.86%\n\n7.94%"),
            name_idx=0,
            shares_idx=1,
            percent_idx=2,
        ) == [
            ("Alpha Capital LLC", "—", "5.86%"),
            ("Beta Partners LP", "—", "7.94%"),
        ]

    def test_an_ordinary_single_holder_row_is_never_split(self) -> None:
        """The whole corpus is this shape; the expansion must be inert on it."""
        assert (
            _split_stacked_holder_row(
                ("The Vanguard Group, Inc.", "3,000,000", "11.0%"), name_idx=0, shares_idx=1, percent_idx=2
            )
            is None
        )

    def test_a_single_percent_beside_a_stacked_amount_column_is_not_copied_to_both(self) -> None:
        """Review WARNING on PR #2359. When the amounts stack to k=2 and the
        percent cell holds ONE value, that value is blanked rather than
        duplicated across the split rows.

        17 CFR 229.403 column 4 is per-owner: one percent beside two amounts
        belongs to at most ONE of them, and the markup does not say which.
        Copying it would fabricate a figure for the other — and
        `ownership_def14a_*` reads `percent_of_class` as the holder's own. NULL
        is the honest value; the amounts, which the markup DOES align, survive.

        Measured before it was chosen: the full-population A/B over 42,705
        payloads loses zero distinct holders, and no accession in the corpus
        carries this shape today — so this test pins a decision, not a
        behaviour anyone has observed.
        """
        assert _split_stacked_holder_row(
            ("Alpha Capital LLC\n\nBeta Partners LP", "486,340\n\n658,400", "5.86%"),
            name_idx=0,
            shares_idx=1,
            percent_idx=2,
        ) == [
            ("Alpha Capital LLC", "486,340", ""),
            ("Beta Partners LP", "658,400", ""),
        ]


class TestBlockLevelLineStructure:
    """#2358 — a ``<br>`` with no source newline glues two Item 403 amounts.

    ``_strip_inline_html`` replaces every tag with a SPACE, so a cell's line
    structure came from whichever source newlines the filer agent happened to
    emit rather than from the markup. ``'486,340<br>658,400'`` arrived as
    ``'486,340 658,400'`` and ``_parse_share_count`` — which strips spaces AND
    commas — returned 486,340,658,400.

    The line-structured rendering is carried on a PARALLEL grid
    (``_RawTable.line_rows``) rather than replacing ``rows``, because header
    scoring and column resolution substring-match SEC-prescribed multi-word
    captions on ``" ".join(headers).lower()`` and would break on an interior
    newline. That pin is asserted below, not assumed.
    """

    def test_br_stacked_amounts_do_not_glue_into_one_number(self) -> None:
        """The defect, cell-for-cell: no source newline anywhere in the row."""
        body = (
            "<table>"
            "<tr><th>Name and Address</th>"
            "<th>Amount and Nature of Beneficial Ownership</th>"
            "<th>Percent of Shares Outstanding</th></tr>"
            # The separator is a BREAK-ONLY paragraph, not ``&nbsp;`` — the
            # shape Codex checkpoint 2 found this branch merging.
            "<tr><td><p>Penbrook Management, LLC</p><p><br/></p>"
            "<p>Renaissance Technologies LLC</p></td>"
            "<td>486,340<br/>658,400</td>"
            "<td>5.86%<br/>7.94%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares, r.percent_of_class) for r in parsed.rows] == [
            ("Penbrook Management, LLC", Decimal("486340"), Decimal("5.86")),
            ("Renaissance Technologies LLC", Decimal("658400"), Decimal("7.94")),
        ]

    def test_one_holder_across_two_classes_reads_as_the_first_class(self) -> None:
        """0000043920-25-000004 (Greif). One NAME against ``Class A<br/>Class B``
        and ``118,028<br/>165,426`` is two 229.403 entries for one holder, one
        per class — not one entry of 118,028,165,426.

        Reads as the FIRST class, which is what this parser already does for the
        ``rowspan`` rendering of the identical shape (Liberty Media, see
        ``test_multi_series_table_names_the_holder_not_the_series_ticker``).
        """
        body = (
            "<table>"
            "<tr><th>Name of Beneficial Owner</th><th>Title of Class</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Lawrence A. Hilsheimer</td><td>Class A<br/>Class B</td>"
            "<td>118,028<br/>165,426</td><td>*<br/>*</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares) for r in parsed.rows] == [
            ("Lawrence A. Hilsheimer", Decimal("118028")),
        ]

    def test_a_multi_line_header_caption_still_matches_its_prescribed_phrase(self) -> None:
        """The pin. 229.403 column 3's caption is routinely rendered across
        ``<br/>``; scoring it as ``'Amount\\nand Nature of\\nBeneficial
        Ownership'`` would stop ``'amount and nature'`` matching and move which
        table wins corpus-wide — the #2164 incident exactly."""
        table = _parse_table_html(
            "<table>"
            "<tr><th>Name of<br/>Beneficial Owner</th>"
            "<th>Amount<br/>and Nature of<br/>Beneficial Ownership</th>"
            "<th>Percent<br/>of Class</th></tr>"
            "<tr><td>The Vanguard Group</td><td>13,114,167</td><td>7.9%</td></tr>"
            "</table>"
        )
        assert table is not None
        assert "\n" not in " ".join(table.score_headers)
        assert "amount and nature" in " ".join(table.score_headers).lower()
        assert _resolve_columns(table.column_headers) == (0, 1, 2)

    def test_the_flat_grid_the_sct_path_reads_carries_no_tag_derived_newline(self) -> None:
        """Item 402(c) splits a NEO's name from their title on ``\\n``
        (``_split_name_position``), so injecting one per ``<br/>`` would re-cut
        every SCT name cell. ``rows`` is what that path reads and it must stay
        byte-identical; ``line_rows`` is the new grid."""
        table = _parse_table_html(
            "<table>"
            "<tr><th>Name and Principal Position</th><th>Salary</th></tr>"
            "<tr><td>Jane Roe<br/>Chief Executive Officer</td><td>1,000,000</td></tr>"
            "</table>"
        )
        assert table is not None
        assert table.rows == (("Jane Roe Chief Executive Officer", "1,000,000"),)
        assert table.line_rows == (("Jane Roe\nChief Executive Officer", "1,000,000"),)

    def test_an_empty_block_is_a_blank_line_and_adjacent_breaks_are_one(self) -> None:
        """Both halves are load-bearing for #2169's holder split, which
        separates stacked owners on a BLANK line.

        ``<p>&nbsp;</p>`` is a real blank line and must survive.
        ``</p>`` immediately followed by ``<p>`` — and a trailing ``<br/>``
        before a close tag (0001193125-25-061365, Coca-Cola Consolidated) —
        render as ONE break and must not fabricate one.
        """
        assert _strip_inline_html("<p>A</p><p>&nbsp;</p><p>B</p>", block_breaks=True) == "A\n \nB"
        # Codex checkpoint 2 on this branch: a block whose only content is a
        # ``<br>`` is empty too, and so is a run of consecutive breaks. Over the
        # first 4,000 accessions of the corpus the first shape appears in 293
        # Item 403 candidate cells across 59 accessions, the second in 444 / 124
        # — without these two the stacked owners merge into one identity.
        assert _strip_inline_html("<p>A</p><p><br/></p><p>B</p>", block_breaks=True) == "A\n \nB"
        assert _strip_inline_html("A<br/><br/>B", block_breaks=True) == "A\n \nB"
        assert _strip_inline_html("<p>A</p>\n  <p>B</p>", block_breaks=True) == "A\nB"
        assert _strip_inline_html("<p>A<br/></p> <p>B</p>", block_breaks=True) == "A\nB"
        # A run of literal SOURCE newlines carries no tag break and is left as
        # the flat rendering has it — 0000351998-18-000006 separates its two
        # holders that way and #2169 reads it.
        assert _strip_inline_html("A\n\n\nB", block_breaks=True) == "A\n\n\nB"

    def test_a_row_that_never_parsed_is_not_resurrected_by_the_collapse(self) -> None:
        """The collapse is CORRECTIVE only. This row's values stack on SOURCE
        newlines, so the flat cell already fails to parse and ``main`` drops the
        row — collapsing it would ADD a holder, and the full-population A/B
        found the one real instance (0001999371-25-003796) is two 229.403
        Instruction 5 group captions sharing a cell with no blank line between
        them, which ``_stacked_name_blocks`` cannot separate. Storing the first
        amount under the two captions glued together adds a mangled identity to
        a table keyed on ``lower(trim(holder_name))``.

        ⚠ The name cell carries a ``<br/>`` so the two grids DIFFER, as they do
        on the cited accession. Without it the row short-circuits on
        ``line_row == flat_row`` and this test pins nothing — which is exactly
        what the revert probe reported when that short-circuit was added.
        """
        body = (
            "<table>"
            "<tr><th>Name of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>All Non-Employee Directors<br/>All Executive Officers as a Group (18 Persons)</td>"
            "<td>471,042 \n 1,273,440</td><td>2.26% \n 6.12%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == []

    def test_a_footnote_line_above_the_amount_is_not_read_as_the_amount(self) -> None:
        """The stack gate, on a shape the corrective precondition does not
        already cover. ``'(3)<br/>1,234'`` parses FLAT — ``_FOOTNOTE_RE`` drops
        the marker — but its lines are not all values, so it is not a stack and
        must not collapse to its first line, which parses to nothing."""
        body = (
            "<table>"
            "<tr><th>Name of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>The Vanguard Group</td><td>(3)<br/>1,234</td><td>7.9%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [(r.holder_name, r.shares) for r in parsed.rows] == [("The Vanguard Group", Decimal("1234"))]

    def test_a_wrapped_name_is_not_re_cut_by_the_value_collapse(self) -> None:
        """#2140 D5: a render wrap inside the name cell split ONE person across
        two holder identities on 704 rows / 117 instruments. The value columns
        stack here, so the row DOES reach the collapse — what protects the name
        is the stack gate, which requires every line of a cell to parse as a
        value. There is no ``name_idx`` exemption and deliberately so; this is
        the test that would fail if the gate were relaxed to "any multi-line
        cell"."""
        body = (
            "<table>"
            "<tr><th>Name of Beneficial Owner</th><th>Title of Class</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Napoleon B. Rutledge,<br/>Jr.</td><td>Class A<br/>Class B</td>"
            "<td>118,028<br/>165,426</td><td>*<br/>*</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Napoleon B. Rutledge, Jr."]


class TestItem403RowIsABeneficialOwner:
    """#2176 class 4 — a row whose NAME is not a person or entity at all.

    17 CFR 229.403 column 2 is "Name and address of beneficial owner"; Rule
    13d-3 makes that a holder of voting or investment power. A row named
    ``Total`` or ``Series A Common Shares`` is neither, and both were stored as
    beneficial owners because ``_is_beneficial_owner_identity`` has exactly one
    call site — inside ``_owner_identity_fraction``, which gates the TABLE and
    never the row. ``_ROW_IDENTITY_FLOOR`` is 0.5, so an admitted table could
    write up to 49% non-owner rows.

    The guard is the NEGATIVE test (``_is_instrument_not_owner``), never the
    positive one. #2176 §2 measured the positive predicate per row against all
    110,748 stored rows and it rejects genuine holders — bare ``BlackRock``,
    ``Margareth Øvrum``, and the Instruction 5 group row — far faster than it
    removes junk.
    """

    def test_a_presentation_total_row_is_not_stored_as_a_holder(self) -> None:
        """The ticket's own shape (``0001308179-25-000114``): a ``Total`` row
        carrying a share count, sitting below the real holders. It reaches the
        person arm as a single capitalised token and was stored."""
        body = (
            "<table>"
            "<tr><th>Name and Address of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>H. Lawrence Culp, Jr.</td><td>1,210,834</td><td>0.5%</td></tr>"
            "<tr><td>The Vanguard Group, Inc.</td><td>3,000,000</td><td>11.0%</td></tr>"
            "<tr><td>Total</td><td>17,967</td><td>0.5%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["H. Lawrence Culp, Jr.", "The Vanguard Group, Inc."]

    def test_a_title_of_class_value_in_the_name_column_is_not_a_holder(self) -> None:
        """229.403 column 1 is ``Title of class``. When it leaks into the name
        column its values ('Series A Common Shares') are securities, not
        owners — #2176 class 2's residue after #2175 fixed the alignment.

        ⚠ The table carries enough genuine holders to clear
        ``_ROW_IDENTITY_FLOOR`` on its UNPRUNED rows, because that is the shape
        the corpus actually has: on `0001051512-25-000021` the class rows are a
        small minority of an otherwise-genuine 18-holder table. A table that is
        mostly class labels is not rescued by this guard and must not be — see
        ``test_the_row_guard_is_not_a_table_selection_change``."""
        body = (
            "<table>"
            "<tr><th>Shareholder's Name and Address</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Telephone and Data Systems, Inc.</td><td>7,570,000</td><td>96.4%</td></tr>"
            "<tr><td>BlackRock, Inc.</td><td>2,100,000</td><td>4.1%</td></tr>"
            "<tr><td>The Vanguard Group, Inc.</td><td>1,900,000</td><td>3.7%</td></tr>"
            "<tr><td>Series A Common Shares</td><td>6,446,264</td><td>*</td></tr>"
            "<tr><td>Common Shares</td><td>1,123,736</td><td>*</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == [
            "Telephone and Data Systems, Inc.",
            "BlackRock, Inc.",
            "The Vanguard Group, Inc.",
        ]

    def test_the_instruction_5_group_row_survives(self) -> None:
        """⚠ The test that bounds the guard. 229.403(b) Instruction 5 requires
        the directors-and-officers-as-a-group aggregate — "in computing the
        aggregate number of shares owned by directors and officers of the
        registrant as a group, the same shares shall not be counted more than
        once". Issuers label it with the word ``Total``.

        The widening this rules out is "the name CONTAINS the aggregate noun",
        which is what probe B injects. It deletes the mandated row, and the size
        of that is measured rather than asserted — ``census_2176_def14a_
        aggregate_rows.py`` arm 5 recomputes it against a control parse, so the
        figure cannot go stale here the way a hand-copied one would. Hence the
        test is "every word is instrument vocabulary" and never "contains an
        aggregate noun"."""
        body = (
            "<table>"
            "<tr><th>Name and Address of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Jane Smith, Director</td><td>250,000</td><td>*</td></tr>"
            "<tr><td>Total shares owned by executive officers and directors (13 persons)</td>"
            "<td>4,120,000</td><td>7.1%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == [
            "Jane Smith, Director",
            "Total shares owned by executive officers and directors (13 persons)",
        ]

    def test_the_row_guard_is_not_a_table_selection_change(self) -> None:
        """⚠ The invariant the full-population A/B forced, and the reason
        ``_is_item403_eligible`` extracts with ``drop_non_owner_rows=False``.

        The guard is a STORAGE filter. If it also feeds
        ``_owner_identity_fraction`` it becomes a SELECTION change: pruning
        non-owner rows raises the fraction for every table, so tables that
        correctly failed ``_ROW_IDENTITY_FLOOR`` start clearing it.

        Here one genuine holder sits under two class labels — 1/3 = 0.33,
        below the floor. Eligibility must still see 1/3 and reject, exactly as
        ``origin/main`` does. An earlier revision of this branch let the prune
        lift this table to 1/1 and admit it; the A/B showed that lifted ZERO
        genuine holders corpus-wide and admitted 10 Item 402 plan rows."""
        body = (
            "<table>"
            "<tr><th>Shareholder's Name and Address</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Series A Common Shares</td><td>6,446,264</td><td>*</td></tr>"
            "<tr><td>Common Shares</td><td>1,123,736</td><td>*</td></tr>"
            "<tr><td>Telephone and Data Systems, Inc.</td><td>7,570,000</td><td>96.4%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == []

    def test_the_designator_strip_does_not_de_admit_an_instruction_5_table(self) -> None:
        """⚠ The second selection change, and the A/B is what found it.

        ``_is_beneficial_owner_identity`` SHORT-CIRCUITS on
        ``_is_instrument_not_owner``, so turning the class-designator strip on
        unconditionally does not just widen the storage guard — it narrows
        OWNER IDENTITY, lowers ``_owner_identity_fraction`` and de-admits whole
        tables. Hence ``strip_class_designator`` is a keyword the guard opts
        into and this predicate's other caller does not.

        Shape and numbers from `0000062234-25-000015` (Marcus Corporation),
        measured against both trees: `main` scores this table 2/3 = 0.667 and
        admits it; with the strip unconditional it scores 1/3 = 0.333 and the
        table goes, taking its 229.403(b) Instruction 5 group row with it.
        `Class B Shares` is the pivot — three capitalised tokens, so it passes
        the person arm unless the strip reaches it first."""
        body = (
            "<table>"
            "<tr><th>Name and Address of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Common Shares</td><td>67,006</td><td>*</td></tr>"
            "<tr><td>All directors and executive officers as a group (13 persons)</td>"
            "<td>2,569,425</td><td>10.4%</td></tr>"
            "<tr><td>Class B Shares</td><td>2,444,278</td><td>35.0%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["All directors and executive officers as a group (13 persons)"]

    def test_an_item_402_plan_table_is_not_admitted_by_pruning(self) -> None:
        """The regression the A/B caught, pinned on its own shape.

        `0000950170-25-008792` and `0000950170-25-058781` newly emitted four
        equity-compensation-plan rows each once the prune fed eligibility.
        These are Item 402 plan disclosures, not 229.403 beneficial ownership.

        The row mix is taken from those accessions and reproduces the arithmetic
        that broke, rather than merely looking like a plan table. Filings render
        these in title case, and the two predicates were measured on the real
        strings:

        - ``Total Shares Outstanding`` / ``Restricted Stock Units Outstanding``
          — every word is instrument vocabulary, so the guard prunes them;
        - ``Weighted Average Exercise Price`` — three capitalised tokens, so it
          PASSES the person arm of ``_is_beneficial_owner_identity``.

        Unpruned that is 1 owner-identity row of 4 = 0.25, correctly under
        ``_ROW_IDENTITY_FLOOR``. Pruned it becomes 1 of 2 = 0.5 and clears it.
        The floor must see 0.25."""
        body = (
            "<table>"
            "<tr><th>Plan Category</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Total Shares Outstanding</td><td>4,200,000</td><td>*</td></tr>"
            "<tr><td>Restricted Stock Units Outstanding</td><td>900,000</td><td>*</td></tr>"
            "<tr><td>Weighted Average Exercise Price</td><td>27</td><td>*</td></tr>"
            "<tr><td>Number of shares remaining available for grant</td><td>1,800,000</td><td>*</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == []

    def test_a_class_designator_letter_does_not_rescue_a_title_of_class_row(self) -> None:
        """Codex checkpoint 2. `a` is in ``_INSTRUMENT_VOCAB`` as a connective
        ARTICLE, so 'Class A Common Stock' tested as an instrument while
        'Class B Common Stock' did not — the latter reached
        ``_is_beneficial_owner_identity`` and passed.

        That asymmetry is load-bearing once the per-row guard exists: pruning
        the Class A row RAISES ``_owner_identity_fraction``, so a table of
        nothing but class labels could clear ``_ROW_IDENTITY_FLOOR`` on the
        strength of its Class B row alone and store it as a holder. On the base
        branch the same table scored 1/3 and was rejected outright.

        17 CFR 229.403 column 1 is 'Title of class'; the designator letter is
        not a word, so it is dropped before the vocabulary test.

        ⚠ Exercised on an ELIGIBLE table (3 genuine holders of 4 rows), because
        eligibility now scores unpruned rows — on an all-class table the row
        guard never runs and this would pass whether or not the designator is
        stripped, which is a test that proves nothing."""
        body = (
            "<table>"
            "<tr><th>Name and Address of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Telephone and Data Systems, Inc.</td><td>7,570,000</td><td>96.4%</td></tr>"
            "<tr><td>BlackRock, Inc.</td><td>2,100,000</td><td>4.1%</td></tr>"
            "<tr><td>The Vanguard Group, Inc.</td><td>1,900,000</td><td>3.7%</td></tr>"
            "<tr><td>Class B Common Stock</td><td>2,000,000</td><td>4.1%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == [
            "Telephone and Data Systems, Inc.",
            "BlackRock, Inc.",
            "The Vanguard Group, Inc.",
        ]

    def test_a_real_entity_whose_name_contains_series_is_not_a_title_of_class(self) -> None:
        """The bound on the designator rule. Dropping short tokens is scoped to
        names that actually name a class, and even there the remaining words
        must ALL be instrument vocabulary — 'capital' and 'partners' are not in
        it, so a fund named for a series survives."""
        body = (
            "<table>"
            "<tr><th>Name and Address of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>Series B Capital Partners LP</td><td>4,000,000</td><td>8.2%</td></tr>"
            "<tr><td>Class A Common Stock</td><td>1,000,000</td><td>*</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["Series B Capital Partners LP"]

    def test_a_bare_entity_name_without_a_corporate_designator_survives(self) -> None:
        """The negative test must not drift into the positive one. ``BlackRock``
        carries no LLC/Inc/Trust token and does not reach the two-capitalised-
        token person pattern, so ``_is_beneficial_owner_identity`` returns False
        for it — 28 rows in the #2176 §2 census. It is a real 13G filer and must
        be stored."""
        body = (
            "<table>"
            "<tr><th>Name and Address of Beneficial Owner</th>"
            "<th>Amount and Nature of Beneficial Ownership</th><th>Percent of Class</th></tr>"
            "<tr><td>BlackRock</td><td>64,137,817</td><td>8.7%</td></tr>"
            "<tr><td>Vanguard Group, Inc.</td><td>94,052,723</td><td>12.76%</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=body))
        assert [r.holder_name for r in parsed.rows] == ["BlackRock", "Vanguard Group, Inc."]


# ---------------------------------------------------------------------------
# #2176 class 1 — 229.403's OTHER subsection, vetoed by one component column
# ---------------------------------------------------------------------------


_403A_TABLE = """
<table>
  <tr><th></th><th>Name and address (1)</th><th>Shares</th><th>% (2)</th></tr>
  <tr><td></td><td>Orogen Ventures II LLC</td><td>7,300,000</td><td>9.0%</td></tr>
  <tr><td></td><td>BlackRock, Inc.</td><td>5,100,000</td><td>6.3%</td></tr>
</table>
"""

# The SAME captions plus a Rule 13d-3(d)(1)(i) component column. ``Vested``
# is Item 402 vocabulary, so ``_item403_value_signature`` vetoes the whole
# table on the joined header.
_403B_TABLE = """
<table>
  <tr><th></th><th>Name and address (1)</th><th>Shares</th><th>% (2)</th>
      <th>Vested but unsettled RSUs (3)</th><th>Total</th></tr>
  <tr><td></td><td>Rohit Kapoor</td><td>1,100,000</td><td>1.3%</td><td>40,000</td><td>1,140,000</td></tr>
  <tr><td></td><td>Maurizio Nicolelli</td><td>90,000</td><td>*</td><td>12,000</td><td>102,000</td></tr>
  <tr><td></td><td>All directors and executive officers as a group (13 persons)</td>
      <td>2,400,000</td><td>2.9%</td><td>180,000</td><td>2,580,000</td></tr>
</table>
"""


class TestItem403SubsectionSiblings:
    """17 CFR 229.403 is ONE Item with TWO subsections, and issuers render
    403(b) as 403(a)'s header plus the component columns Rule 13d-3(d)(1)(i)
    makes part of beneficial ownership. Measured on ExlService
    0001193125-25-103261 / 0001193125-26-181891: 3 holders extracted, 13 lost."""

    def test_403b_with_a_component_column_joins_its_403a_sibling(self) -> None:
        parsed = parse_beneficial_ownership_table(_proxy_html(body=_403A_TABLE + _403B_TABLE))
        names = [r.holder_name for r in parsed.rows]
        assert "Orogen Ventures II LLC" in names
        assert "Rohit Kapoor" in names
        assert "All directors and executive officers as a group (13 persons)" in names

    def test_the_403b_table_alone_is_still_vetoed(self) -> None:
        """The sibling rule is not a licence — without an eligible anchor in the
        window the same table stays rejected, so the widening cannot escape the
        window it was measured in."""
        parsed = parse_beneficial_ownership_table(_proxy_html(body=_403B_TABLE))
        assert parsed.rows == []

    def test_a_comp_table_in_the_same_window_is_not_vouched_for(self) -> None:
        """Superset of the anchor's CAPTIONS, not merely presence in the window.

        ⚠ The comp table has to CLEAR ``_WINDOW_SCORE_FLOOR`` or the assertion
        passes without the superset test ever running — the first draft scored 2
        and the revert-probe reported NOT CAUGHT. ``Number of Shares`` normalises
        to a different caption from the anchor's ``Shares``, so the header is a
        near-miss rather than a superset."""
        comp = (
            "<table>"
            "<tr><th></th><th>Name</th><th>Number of Shares</th><th>Percentage of Base Salary</th></tr>"
            "<tr><td></td><td>Rohit Kapoor</td><td>1,200,000</td><td>140%</td></tr>"
            "<tr><td></td><td>Maurizio Nicolelli</td><td>600,000</td><td>135%</td></tr>"
            "</table>"
        )
        assert _score_table_headers(("", "Name", "Number of Shares", "Percentage of Base Salary")) >= 3
        parsed = parse_beneficial_ownership_table(_proxy_html(body=_403A_TABLE + comp))
        assert [r.holder_name for r in parsed.rows] == ["Orogen Ventures II LLC", "BlackRock, Inc."]

    def test_a_vouched_sibling_still_has_to_clear_the_row_identity_floor(self) -> None:
        """The captions are the anchor's plus one, but no row NAMES an owner —
        the widening admits a table the value-signature gate rejected, so D2 is
        the only limb left standing over it.

        ⚠ Class-label rows do NOT test this. #2373's per-row guard deletes them
        at extraction whatever the floor does, so the probe reported NOT CAUGHT.
        Bare single-token entity names are the shape that fails
        ``_is_beneficial_owner_identity`` and survives ``_is_instrument_not_owner``."""
        classes = (
            "<table>"
            "<tr><th></th><th>Name and address (1)</th><th>Shares</th><th>% (2)</th><th>Vesting Date</th></tr>"
            "<tr><td></td><td>Fidelity</td><td>1,000</td><td>1.0%</td><td>2027-01-01</td></tr>"
            "<tr><td></td><td>Wellington</td><td>2,000</td><td>2.0%</td><td>2027-01-01</td></tr>"
            "</table>"
        )
        parsed = parse_beneficial_ownership_table(_proxy_html(body=_403A_TABLE + classes))
        assert [r.holder_name for r in parsed.rows] == ["Orogen Ventures II LLC", "BlackRock, Inc."]

    def test_a_two_caption_anchor_cannot_vouch_for_anything(self) -> None:
        """``_SUBSECTION_CAPTION_FLOOR``. A one- or two-caption anchor is a
        subset of most headers in a proxy, so it would vouch vacuously."""
        anchor = _parse_table_html(
            "<table><tr><th>Shares Beneficially Owned</th><th>Percent</th></tr>"
            "<tr><td>Vanguard Group, Inc.</td><td>11.0%</td></tr></table>"
        )
        sibling = _parse_table_html(
            "<table><tr><th>Shares Beneficially Owned</th><th>Percent</th><th>Vesting Date</th></tr>"
            "<tr><td>Rohit Kapoor</td><td>1.3%</td><td>2027-01-01</td></tr></table>"
        )
        assert anchor is not None and sibling is not None
        assert len(_header_caption_set(anchor)) == 2
        assert _subsection_sibling_tables([anchor], [sibling]) == []

    def test_caption_matching_ignores_footnote_markers_and_case(self) -> None:
        """The two subsections footnote their columns independently, so the
        captions compare equal only after the marker is stripped."""
        table = _parse_table_html(
            "<table><tr><th>Name and Address(3)</th><th>SHARES</th><th>% (2)</th></tr>"
            "<tr><td>Vanguard Group, Inc.</td><td>1</td><td>1%</td></tr></table>"
        )
        assert table is not None
        assert _header_caption_set(table) == frozenset({"name and address", "shares", "%"})


# ---------------------------------------------------------------------------
# #2376 — layout-attested percent recovery
# ---------------------------------------------------------------------------
#
# Fixture shape is taken from 0001193125-25-103261 (ExlService) — the accession
# the ticket cites — reduced to the two features that produce the defect:
# self-closing `<td/>` layout spacers, and `colspan` on the caption cells. The
# real table's header carries `Shares` and `% (2)` over four layout columns
# each, and the data rows put their values under the LAST of those columns.


_SPACER_OFFSET_TABLE = """
<table><tr>
<td>&#160;</td>
<td/>
<td>&#8195;&#8202;Name and address<sup>(1)</sup></td>
<td/>
<td colspan="4">Shares</td>
<td/>
<td colspan="4">%<sup>(2)</sup></td>
<td/>
<td colspan="4"> &#160;<p>&#160;</p></td>
<td/>
<td colspan="4"> &#160;<p>&#160;</p></td></tr><tr>
<td/>
<td/>
<td> <p>Blackrock Inc.<sup>(11)</sup></p></td>
<td/>
<td/>
<td>&#160;</td>
<td>23,308,871</td>
<td/>
<td/>
<td/>
<td>&#160;</td>
<td>14.33</td>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/></tr><tr>
<td/>
<td/>
<td> <p>The Vanguard Group, Inc.<sup>(12)</sup></p></td>
<td/>
<td/>
<td>&#160;</td>
<td>17,015,630</td>
<td/>
<td/>
<td/>
<td>&#160;</td>
<td>10.46</td>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/>
<td/></tr></table>
"""


class TestLayoutAttestedPercent:
    """#2376 — a percent lost because the header and the data rows are not in a
    common column space, and no rescue may take a BARE number without knowing
    which caption covers it."""

    def test_self_closing_spacers_and_colspan_offset_the_percent(self) -> None:
        """The defect, pinned at the grid rather than at the symptom.

        ``_CELL_RE`` reads ``<td style="x"/>`` as an OPENING tag (``[^>]*``
        accepts the trailing ``/``), so the header emits 6 cells and the data
        rows 5, and the ``colspan="4"`` is read off the wrong tag. The resolved
        percent index therefore lands on a spacer."""
        table = _parse_table_html(_SPACER_OFFSET_TABLE)
        assert table is not None
        name_idx, shares_idx, percent_idx = _resolve_columns(table.column_headers)
        assert table.column_headers == ("", "Name and address (1)", "Shares", "% (2)", "", "")
        assert (name_idx, shares_idx, percent_idx) == (1, 2, 3)
        # Six header cells against five data cells, so the resolved percent
        # index lands on the issuer's blank spacer and the real percent sits one
        # further right. `_pad_row` only pads SHORT rows; it has nothing to say
        # about a row whose cells are offset from the header's.
        assert table.rows[0] == ("Blackrock Inc. (11)", "", "23,308,871", "", "14.33")
        assert table.rows[0][percent_idx] == ""

    def test_layout_grid_matches_the_html_table_model(self) -> None:
        """Captions occupy every column they span; values sit under them."""
        rows = _layout_rows(_SPACER_OFFSET_TABLE)
        header = rows[0]
        assert header[2] == "Name and address (1)"
        assert [header[c] for c in (4, 5, 6, 7)] == ["Shares"] * 4
        assert [header[c] for c in (9, 10, 11, 12)] == ["% (2)"] * 4
        # BlackRock's count is under the Shares span, its percent under the %
        # span — the same placement `pandas.read_html` 3.0.2 returns for this
        # table, which is what makes the bare 14.33 unambiguous.
        assert rows[1][6] == "23,308,871"
        assert rows[1][11] == "14.33"

    def test_bare_percent_is_recovered_end_to_end(self) -> None:
        """The ticket's reported symptom: 14.33 / 10.46 stored as NULL."""
        parsed = parse_beneficial_ownership_table(_proxy_html(body=_SPACER_OFFSET_TABLE))
        by_name = {r.holder_name: r for r in parsed.rows}
        assert by_name["Blackrock Inc."].shares == Decimal("23308871")
        assert by_name["Blackrock Inc."].percent_of_class == Decimal("14.33")
        assert by_name["The Vanguard Group, Inc."].percent_of_class == Decimal("10.46")

    def test_a_share_count_under_a_shares_caption_is_never_attested(self) -> None:
        """The one failure this must not have. ``_parse_percent`` clamps to
        [0,100], so a SMALL share count would parse happily as a percent — only
        the caption above the column keeps it out."""
        assert _layout_percent_by_row(_SPACER_OFFSET_TABLE) == {
            _layout_name_key("Blackrock Inc. (11)"): Decimal("14.33"),
            _layout_name_key("The Vanguard Group, Inc. (12)"): Decimal("10.46"),
        }

    def test_merged_amount_and_percent_caption_is_not_a_percent_column(self) -> None:
        """Issuers merge 229.403 columns 3 and 4 into one caption, and that
        single column holds the SHARE COUNT — ``_resolve_columns`` already
        records this. Attesting it as a percent would publish a share count as a
        percent."""
        assert not _is_percent_caption("Amount and Nature of Beneficial Ownership and Percent of Class")
        assert not _is_percent_caption("Shares Beneficially Owned (%)")
        assert _is_percent_caption("Percent of Class")
        assert _is_percent_caption("% (2)")

    def test_a_threshold_phrase_is_not_a_column_caption(self) -> None:
        """``5% Beneficial owners`` is Item 403(a)'s section LABEL — the sign
        binds to the number on its left. Reading it as a caption made
        ExlService's own table look like it carried two different percent
        captions, and the ambiguity guard then refused the rows this ticket
        exists to recover."""
        assert not _is_percent_caption("5% Beneficial owners")
        assert not _is_percent_caption("Owners of more than 5%")
        assert _is_percent_caption("% of Total Voting Power (1)")

    def test_a_threshold_phrase_spelled_as_a_word_is_not_a_caption_either(self) -> None:
        """Same label, sign written out. The first cut tested the CHARACTER, so a
        ``"percent" in lowered`` early return admitted ``5 Percent Beneficial
        Owners`` — and a table carrying that section label beside a real
        ``Percent of Class`` header then reads as two distinct captions and
        fails closed, losing the recovery it was meant to have. Codex caught it
        at checkpoint 2.

        The two number words are the reg's own: 229.403(a) is the 5% threshold,
        Section 16 the 10% one."""
        assert not _is_percent_caption("5 Percent Beneficial Owners")
        assert not _is_percent_caption("Five Percent Holders")
        assert not _is_percent_caption("Ten Percent Owners")
        assert not _is_percent_caption("Beneficial owners of more than five percent")
        # Still captions — no quantity binds to the sign.
        assert _is_percent_caption("Percent of Class")
        assert _is_percent_caption("Approximate Percent of Class")
        assert _is_percent_caption("Percent of Class (5)")

    def test_two_distinct_percent_captions_attest_nothing(self) -> None:
        """Domo (0001505952-25-000062) renders
        ``Shares | % | Shares | % | % of Total Voting Power``, and its FIRST
        percent-captioned header row carries only the voting-power caption — one
        clean contiguous run, so a contiguity test alone passes it and stores
        voting power as percent of class. Josh James' row reads
        ``3,263,659 100 1,022,375 2.8 78.5``: 78.5% of the vote, 2.8% of Class A.

        This is why the scan unions across EVERY caption row rather than
        stopping at the first one that matches."""
        two_captions = (
            "<table>"
            '<tr><td>Name</td><td colspan="2">Shares</td><td colspan="2">Percent of Class</td>'
            '<td colspan="2">% of Total Voting Power</td></tr>'
            "<tr><td>Joshua G. James</td><td/><td>1,022,375</td><td/><td>2.8</td>"
            "<td/><td>78.5</td></tr></table>"
        )
        assert _layout_percent_by_row(two_captions) == {}

    def test_an_ambiguous_name_prefix_is_dropped_rather_than_guessed(self) -> None:
        """The join is a 16-character prefix, so two holders can collide. When
        the colliding rows disagree the entry is dropped — a null percent is
        recoverable, a wrong one is not."""
        collide = (
            '<table><tr><td>Name</td><td colspan="2">Percent of Class</td></tr>'
            "<tr><td>Wellington Management Group A</td><td/><td>3.1</td></tr>"
            "<tr><td>Wellington Management Group B</td><td/><td>4.2</td></tr>"
            "</table>"
        )
        assert _layout_percent_by_row(collide) == {}

    def test_an_existing_percent_is_never_overwritten(self) -> None:
        """The rescue runs last and only when ``percent is None`` — and that
        guard is load-bearing, not defensive.

        A caption's ``colspan`` can OVER-cover: here ``Percent of Class`` spans
        two layout columns and the issuer puts a bare footnote marker in the
        second, so the layout attests ``3`` where the flat grid reads the
        correct ``7.7%``. Confining the rescue to rows that have no percent at
        all is what bounds it to NULL -> value."""
        over_covering = (
            "<table>"
            "<tr><td/><td>Name of Beneficial Owner</td>"
            '<td colspan="2">Percent of Class</td></tr>'
            "<tr><td>Acme Holdings LLC</td><td>7.7%</td><td>3</td></tr>"
            "</table>"
        )
        assert _layout_percent_by_row(over_covering) == {_layout_name_key("Acme Holdings LLC"): Decimal("3")}
        parsed = parse_beneficial_ownership_table(_proxy_html(body=over_covering))
        assert [r.percent_of_class for r in parsed.rows] == [Decimal("7.7")]

    def test_a_table_with_no_percent_caption_recovers_nothing(self) -> None:
        """Rule 13d-3 sole/shared-power tables carry no percent column at all.
        Fabricating one from whichever number sits rightmost is the failure the
        caption requirement exists to prevent."""
        no_percent = (
            "<table><tr><td>Name of Beneficial Owner</td><td>Sole Voting Power</td>"
            "<td>Shared Voting Power</td></tr>"
            "<tr><td>Wellington Management</td><td>12,000</td><td>3.5</td></tr></table>"
        )
        assert _layout_percent_by_row(no_percent) == {}

    def test_the_rescue_cannot_change_which_tables_are_selected(self) -> None:
        """A recovered percent must not decide ELIGIBILITY.

        ``_is_item403_eligible`` judges a table by extracting it, so with the
        rescue live during that probe a table can qualify on a percent the flat
        grid never had. The full-population A/B measured the consequence on
        0001140361-25-012231: a junk ``Brian H. Hertzman`` row at 446,200 shares
        / 89.2% was admitted beside the genuine ``Brian S. Hertzman`` at 0.5%.

        The probe is the ``rows is None`` caller, so this pins that the two
        callers disagree — eligibility sees no attested percent, extraction
        does."""
        table = _parse_table_html(_SPACER_OFFSET_TABLE)
        assert table is not None

        as_probe = _extract_table_holders(table)
        assert [h.percent_of_class for h in as_probe] == [None, None]

        collected: list[Def14ABeneficialHolder] = []
        _extract_table_holders(table, rows=collected, seen=set())
        assert [h.percent_of_class for h in collected] == [Decimal("14.33"), Decimal("10.46")]

    def test_a_leading_title_of_class_column_does_not_shadow_the_holder(self) -> None:
        """17 CFR 229.403(a) prescribes column 1 ``Title of class`` AHEAD of
        column 2 ``Name and address of beneficial owner``.

        Keying the row on its FIRST text cell therefore files the percent under
        ``commonstock`` on every table that renders column 1, and the lookup —
        which is by ``_layout_name_key(holder_name)`` — never finds it. The
        recovery is silently disabled for the reg's own table shape. Caught by
        Codex at checkpoint 2, on this exact markup."""
        titled = (
            "<table>"
            "<tr><td>Title of Class</td><td>Name of Beneficial Owner</td><td/>"
            '<td colspan="2">Shares</td><td/><td colspan="2">Percent of Class</td></tr>'
            "<tr><td>Common Stock</td><td>Acme Capital LLC</td><td/><td/><td>1,000</td>"
            "<td/><td/><td>7.7</td></tr></table>"
        )
        attested = _layout_percent_by_row(titled)
        assert attested[_layout_name_key("Acme Capital LLC")] == Decimal("7.7")
        # The class label is registered too, and that is harmless by
        # construction: nothing ever looks a holder up under it.
        assert attested[_layout_name_key("Common Stock")] == Decimal("7.7")
        parsed = parse_beneficial_ownership_table(_proxy_html(body=titled))
        assert [(r.holder_name, r.percent_of_class) for r in parsed.rows] == [("Acme Capital LLC", Decimal("7.7"))]

    def test_a_repeated_class_label_collides_with_itself_and_drops_out(self) -> None:
        """The second key is safe because the ambiguity guard already covers it.

        A multi-row table repeats ``Common Stock`` against different percents,
        so the label is dropped exactly as a colliding holder name would be —
        one mechanism, not a new one. The holders keep their own values."""
        repeated = (
            "<table>"
            "<tr><td>Title of Class</td><td>Name of Beneficial Owner</td>"
            '<td colspan="2">Percent of Class</td></tr>'
            "<tr><td>Common Stock</td><td>Acme Capital LLC</td><td/><td>7.7</td></tr>"
            "<tr><td>Common Stock</td><td>Beta Partners LP</td><td/><td>5.1</td></tr>"
            "</table>"
        )
        assert _layout_percent_by_row(repeated) == {
            _layout_name_key("Acme Capital LLC"): Decimal("7.7"),
            _layout_name_key("Beta Partners LP"): Decimal("5.1"),
        }

    def test_a_digit_beside_an_asterisk_states_a_threshold_not_a_holding(self) -> None:
        """``1*`` against a ``* Less than 1.0%`` legend is the ISSUER writing
        "under one percent" — the digit is the threshold.

        ``_parse_percent`` strips a trailing footnote marker, so ``1*`` reads as
        a flat ``1``: a figure the filing does not state, and one the parser's
        own convention for that meaning writes as ``0.5``. Found by the
        gain-side arm on 0001437749-25-025111, where seven holders would have
        stored it.

        A BARE ``*`` is unambiguous and stays accepted — that is the settled
        convention, and declining it would drop percents the parser gets right
        today (Campbell Soup 0001308179-25-000618 recovers thirteen)."""
        threshold = (
            '<table><tr><td>Name of Beneficial Owner</td><td colspan="2">Percentage of Shares</td></tr>'
            "<tr><td>David Wheadon, M.D.</td><td/><td>1*</td></tr>"
            "<tr><td>Andrei Floroiu</td><td/><td>1.4</td></tr>"
            "<tr><td>Fabiola R. Arredondo</td><td/><td>*</td></tr></table>"
        )
        assert _layout_percent_by_row(threshold) == {
            _layout_name_key("Andrei Floroiu"): Decimal("1.4"),
            _layout_name_key("Fabiola R. Arredondo"): Decimal("0.5"),
        }

    def test_a_dual_class_table_attests_nothing(self) -> None:
        """229.403 column 4 appears TWICE on a dual-class table, once per class,
        and this helper cannot tell which class the extractor's share count came
        from. Measured consequence of pooling them (0001308179-25-000518,
        Regeneron): a flat 28 attested to fifteen directors holding between
        4,472 and 268,499 shares."""
        dual_class = (
            "<table><tr><td>Name and Address of Beneficial Owner</td>"
            '<td colspan="2">Number</td><td colspan="2">Percent of Class</td>'
            '<td colspan="2">Number</td><td colspan="2">Percent of Class</td></tr>'
            "<tr><td>Bonnie L. Bassler</td><td/><td>—</td><td/><td>28</td>"
            "<td/><td>18,058</td><td/><td/></tr></table>"
        )
        percent_columns = sorted(
            column for column, text in _layout_rows(dual_class)[0].items() if _is_percent_caption(text)
        )
        # Two runs, 3-4 and 7-8 — not contiguous, so the binding is ambiguous.
        assert percent_columns == [3, 4, 7, 8]
        assert _layout_percent_by_row(dual_class) == {}

    def test_a_percent_equal_to_the_share_count_is_the_same_cell_twice(self) -> None:
        """0001375365-25-000009's group-total row parses ``shares`` as 33.6 —
        a pre-existing share-column defect. Attesting 33.6 as its percent would
        manufacture agreement out of one number rather than add a second fact."""
        html = (
            "<table><tr><td>Name</td><td>Percent of Class</td></tr>"
            "<tr><td>Acme Holdings LLC</td><td>33.6</td></tr></table>"
        )
        # The layout does attest it — the refusal is at the point of USE, where
        # the row's own share count is known.
        assert _layout_percent_by_row(html) == {_layout_name_key("Acme Holdings LLC"): Decimal("33.6")}
        table = _RawTable(
            score_headers=("Name", "Shares"),
            column_headers=("Name", "Shares"),
            rows=(("Acme Holdings LLC", "33.6"),),
            line_rows=(("Acme Holdings LLC", "33.6"),),
            table_html=html,
        )
        holders = _extract_table_holders(table, rows=[], seen=set())
        assert [(h.shares, h.percent_of_class) for h in holders] == [(Decimal("33.6"), None)]
