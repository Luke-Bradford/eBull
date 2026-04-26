"""Parser must preserve <table> blocks as structured payloads (#559)."""

from __future__ import annotations

from app.services.business_summary import (
    ParsedTable,
    extract_business_sections,
)


def test_table_block_extracted_as_parsed_table() -> None:
    raw = """
    <html><body>
    <p>Item 1. Business</p>
    <p>As of January 31, 2026 we operated 2,206 stores:</p>
    <table>
      <tr><th>Segment</th><th>Stores</th></tr>
      <tr><td>United States</td><td>1,598</td></tr>
      <tr><td>Europe</td><td>308</td></tr>
      <tr><td>Australia</td><td>300</td></tr>
    </table>
    <p>Our stores operate primarily under GameStop brands.</p>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections, "expected at least one section"
    s0 = sections[0]
    assert len(s0.tables) == 1
    table = s0.tables[0]
    assert isinstance(table, ParsedTable)
    assert table.headers == ("Segment", "Stores")
    assert table.rows == (
        ("United States", "1,598"),
        ("Europe", "308"),
        ("Australia", "300"),
    )
    assert "TABLE_0" in s0.body or "␞TABLE_0␞" in s0.body, (
        "body should retain a sentinel marking the table's insertion point"
    )


def test_section_with_no_tables_has_empty_tuple() -> None:
    raw = """
    <html><body>
    <p>Item 1. Business</p>
    <p>We sell video games.</p>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections
    assert sections[0].tables == ()
