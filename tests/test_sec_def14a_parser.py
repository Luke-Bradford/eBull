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
    Def14ABeneficialOwnershipTable,
    _clean_beneficial_holder_name,
    _clean_holder_name,
    _detect_role_heading,
    _is_address_fragment,
    _is_beneficial_owner_identity,
    _is_name_then_address,
    _is_owner_identity,
    _item403_value_signature,
    _looks_like_label_row,
    _looks_like_subheader,
    _parse_percent,
    _parse_share_count,
    _resolve_columns,
    _score_table_headers,
    _shares_cell_percent_signature,
    _strip_inline_html,
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
