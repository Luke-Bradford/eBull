"""Parser must preserve <table> blocks as structured payloads (#559)."""

from __future__ import annotations

import psycopg
import pytest

from app.services.business_summary import (
    ParsedBusinessSection,
    ParsedTable,
    extract_business_sections,
    get_business_sections,
    upsert_business_sections,
)


def _seed_instrument(conn: psycopg.Connection[tuple], symbol: str = "GSE", iid: int = 99) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Test Co Tables"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


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


@pytest.mark.integration
def test_upsert_persists_tables_json(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """tables_json round-trips: write via upsert_business_sections, read
    back via get_business_sections, and the ParsedTable is intact."""
    instrument_id = _seed_instrument(ebull_test_conn)
    accession = "0000950170-26-999999"

    section = ParsedBusinessSection(
        section_order=0,
        section_key="general",
        section_label="General",
        body="We operate ␞TABLE_0␞ stores globally.",
        cross_references=(),
        tables=(
            ParsedTable(
                order=0,
                headers=("Segment", "Stores"),
                rows=(
                    ("United States", "1,598"),
                    ("Europe", "308"),
                ),
            ),
        ),
    )

    count = upsert_business_sections(
        ebull_test_conn,
        instrument_id=instrument_id,
        source_accession=accession,
        sections=(section,),
    )
    assert count == 1

    rows = get_business_sections(ebull_test_conn, instrument_id=instrument_id)
    assert len(rows) == 1
    result = rows[0]
    assert len(result.tables) == 1
    tbl = result.tables[0]
    assert tbl.headers == ("Segment", "Stores")
    assert tbl.rows == (
        ("United States", "1,598"),
        ("Europe", "308"),
    )
    assert tbl.order == 0


def test_nested_tables_outer_only_extracted_no_prose_leak():
    """Outer <table> wrapping inner <table> should yield ONE ParsedTable
    matching the outer cells, with no inner-table text bleeding into
    the prose body."""
    raw = """
    <html><body>
    <p>Item 1. Business</p>
    <p>Pre-prose.</p>
    <table>
      <tr><th>Outer A</th><th>Outer B</th></tr>
      <tr>
        <td>Outer Left</td>
        <td>
          <table>
            <tr><th>InnerH</th></tr>
            <tr><td>InnerCell</td></tr>
          </table>
        </td>
      </tr>
      <tr><td>Outer C</td><td>Outer D</td></tr>
    </table>
    <p>Post-prose.</p>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections, "expected at least one section"
    s0 = sections[0]
    # Exactly one table — the outer.
    assert len(s0.tables) == 1, f"expected 1 outer table, got {len(s0.tables)}: {[t.headers for t in s0.tables]}"
    t = s0.tables[0]
    assert t.headers == ("Outer A", "Outer B"), f"headers should be the outer table's, got {t.headers}"
    # Inner-table cells must NOT leak into prose body.
    assert "InnerCell" not in s0.body, f"inner table cell leaked into prose: {s0.body!r}"
    assert "InnerH" not in s0.body, f"inner table heading leaked into prose: {s0.body!r}"
    # Outer-table cell text also must not appear in prose.
    assert "Outer A" not in s0.body
    assert "Outer Left" not in s0.body


def test_table_with_excess_rows_is_truncated():
    """A table with > _MAX_TABLE_ROWS rows must truncate to the cap."""
    rows_html = "".join(f"<tr><td>R{i}A</td><td>R{i}B</td></tr>" for i in range(300))
    raw = f"""
    <html><body>
    <p>Item 1. Business</p>
    <table>
      <tr><th>A</th><th>B</th></tr>
      {rows_html}
    </table>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections
    t = sections[0].tables[0]
    assert len(t.rows) == 200, f"expected 200 rows after cap, got {len(t.rows)}"


def test_cell_content_capped():
    """Cell content longer than _MAX_CELL_LEN must truncate."""
    long_cell = "X" * 500
    raw = f"""
    <html><body>
    <p>Item 1. Business</p>
    <table>
      <tr><th>A</th></tr>
      <tr><td>{long_cell}</td></tr>
    </table>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections
    cell = sections[0].tables[0].rows[0][0]
    assert len(cell) == 200, f"expected 200-char cap, got {len(cell)}"
