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
    Def14ABeneficialOwnershipTable,
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
