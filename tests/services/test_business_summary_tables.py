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
    assert "␞TABLE_0␞" in s0.body, "body should retain a sentinel marking the table's insertion point"


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


def test_two_sections_each_with_one_table_get_local_indices():
    """Two sections each containing a distinct <table> should both
    end up with ``tables[0]`` (per-section local indexing)."""
    raw = """
    <html><body>
    <p>Item 1. Business</p>
    <p><b>Segments</b></p>
    <p>Segment data:</p>
    <table>
      <tr><th>Segment</th><th>Stores</th></tr>
      <tr><td>US</td><td>1,598</td></tr>
    </table>
    <p><b>Human Capital</b></p>
    <p>Headcount data:</p>
    <table>
      <tr><th>Region</th><th>Headcount</th></tr>
      <tr><td>US</td><td>5,000</td></tr>
    </table>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    sections_with_tables = [s for s in sections if s.tables]
    assert len(sections_with_tables) >= 2, (
        f"expected at least two sections with tables; got {[(s.section_label, len(s.tables)) for s in sections]}"
    )
    for s in sections_with_tables:
        assert s.tables[0].order == 0, (
            f"section {s.section_label!r}: table 0 should have local order 0, got {s.tables[0].order}"
        )
        # Body must reference the LOCAL index, not the global one.
        assert "␞TABLE_0␞" in s.body, f"section {s.section_label!r}: body should reference TABLE_0 locally"
