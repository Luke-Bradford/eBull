"""GET /instruments/{symbol}/business_sections must return tables (#559)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.instruments import router as instruments_router
from app.db import get_conn
from app.services.business_summary import (
    BusinessSectionRow,
    ParsedCrossReference,
    ParsedTable,
)


def _build_app(conn: MagicMock) -> FastAPI:
    app = FastAPI()
    app.include_router(instruments_router)

    def _yield_conn():  # type: ignore[return]
        yield conn

    app.dependency_overrides[get_conn] = _yield_conn
    return app


def _cursor_returning_instrument() -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = {"instrument_id": 1, "symbol": "GME"}
    return cur


_FAKE_SECTIONS: tuple[BusinessSectionRow, ...] = (
    BusinessSectionRow(
        section_order=0,
        section_key="general",
        section_label="General",
        body="Body with ␞TABLE_0␞ marker.",
        cross_references=(),
        source_accession="0001-test",
        tables=(
            ParsedTable(
                order=0,
                headers=("Segment", "Stores"),
                rows=(("US", "1598"), ("EU", "308")),
            ),
        ),
    ),
    BusinessSectionRow(
        section_order=1,
        section_key="strategy",
        section_label="Strategy",
        body="Strategy prose with no tables.",
        cross_references=(
            ParsedCrossReference(
                reference_type="item",
                target="Item 1A",
                context="See Item 1A for risk factors.",
            ),
        ),
        source_accession="0001-test",
        tables=(),
    ),
)


def test_business_sections_response_includes_tables_field() -> None:
    """``tables`` key is present on every section in the response."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_instrument()
    app = _build_app(conn)

    with patch(
        "app.services.business_summary.get_business_sections",
        return_value=_FAKE_SECTIONS,
    ):
        client = TestClient(app)
        r = client.get("/instruments/GME/business_sections")

    assert r.status_code == 200
    data = r.json()
    assert "sections" in data
    for section in data["sections"]:
        assert "tables" in section, f"section {section['section_key']!r} missing 'tables'"


def test_business_sections_table_shape() -> None:
    """Tables with data have correct ``{order, headers, rows}`` shapes."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_instrument()
    app = _build_app(conn)

    with patch(
        "app.services.business_summary.get_business_sections",
        return_value=_FAKE_SECTIONS,
    ):
        client = TestClient(app)
        r = client.get("/instruments/GME/business_sections")

    assert r.status_code == 200
    sections = r.json()["sections"]

    # Section 0 has one table with the expected shape.
    section_with_tables = sections[0]
    assert len(section_with_tables["tables"]) == 1
    tbl = section_with_tables["tables"][0]
    assert tbl["order"] == 0
    assert tbl["headers"] == ["Segment", "Stores"]
    assert tbl["rows"] == [["US", "1598"], ["EU", "308"]]

    # Section 1 has no tables.
    section_without_tables = sections[1]
    assert section_without_tables["tables"] == []


def test_business_sections_mixed_coverage() -> None:
    """At least one section has tables and at least one section has none."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_instrument()
    app = _build_app(conn)

    with patch(
        "app.services.business_summary.get_business_sections",
        return_value=_FAKE_SECTIONS,
    ):
        client = TestClient(app)
        r = client.get("/instruments/GME/business_sections")

    assert r.status_code == 200
    sections = r.json()["sections"]

    sections_with_tables = [s for s in sections if s["tables"]]
    sections_without_tables = [s for s in sections if not s["tables"]]

    assert sections_with_tables, "expected at least one section with tables"
    assert sections_without_tables, "expected at least one section without tables"


_FAKE_SECTIONS_OLD: tuple[BusinessSectionRow, ...] = (
    BusinessSectionRow(
        section_order=0,
        section_key="general",
        section_label="General",
        body="Old filing body.",
        cross_references=(),
        source_accession="acc-old",
        tables=(),
    ),
)


def test_business_sections_with_accession_returns_that_filing() -> None:
    """?accession= param is forwarded to get_business_sections and the
    response carries that specific accession."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_instrument()
    app = _build_app(conn)

    with patch(
        "app.services.business_summary.get_business_sections",
        return_value=_FAKE_SECTIONS_OLD,
    ) as mock_get:
        client = TestClient(app)
        r = client.get("/instruments/GME/business_sections?accession=acc-old")

    assert r.status_code == 200
    # Verify the accession param was forwarded to the service call.
    mock_get.assert_called_once_with(conn, instrument_id=1, accession="acc-old")
    data = r.json()
    assert data["source_accession"] == "acc-old"


def test_business_sections_unknown_accession_404() -> None:
    """?accession= for an unknown filing returns HTTP 404."""
    conn = MagicMock()
    conn.cursor.return_value = _cursor_returning_instrument()
    app = _build_app(conn)

    with patch(
        "app.services.business_summary.get_business_sections",
        return_value=(),
    ):
        client = TestClient(app)
        r = client.get("/instruments/GME/business_sections?accession=does-not-exist")

    assert r.status_code == 404
