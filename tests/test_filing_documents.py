"""Unit tests for ``app.services.filing_documents.parse_filing_index`` (#452)."""

from __future__ import annotations

from app.services.filing_documents import (
    ParsedFilingDocument,
    parse_filing_index,
)

_INDEX = {
    "cik": "320193",
    "form": "10-K",
    "primaryDocument": "aapl-20240930.htm",
    "filingDate": "2024-11-01",
    "items": [
        {
            "name": "aapl-20240930.htm",
            "type": "10-K",
            "description": "10-K",
            "size": 1258402,
        },
        {
            "name": "ex-21.htm",
            "type": "EX-21",
            "description": "Subsidiaries of the Registrant",
            "size": 1892,
        },
        {
            "name": "ex-99-1.htm",
            "type": "EX-99.1",
            "description": "Press Release dated Nov 1, 2024",
            "size": 10234,
        },
        {
            "name": "Financial_Report.xlsx",
            "type": "EXCEL",
            "description": None,
            "size": 5120,
        },
    ],
}


class TestParseFilingIndex:
    def test_all_documents_surface(self) -> None:
        docs = parse_filing_index(_INDEX, accession_number="0000320193-24-000001")
        assert len(docs) == 4
        names = {d.document_name for d in docs}
        assert {"aapl-20240930.htm", "ex-21.htm", "ex-99-1.htm", "Financial_Report.xlsx"} == names

    def test_primary_flag_set_only_on_primary(self) -> None:
        docs = parse_filing_index(_INDEX, accession_number="0000320193-24-000001")
        primaries = [d for d in docs if d.is_primary]
        assert len(primaries) == 1
        assert primaries[0].document_name == "aapl-20240930.htm"

    def test_url_reconstruction(self) -> None:
        docs = parse_filing_index(_INDEX, accession_number="0000320193-24-000001")
        ex21 = next(d for d in docs if d.document_name == "ex-21.htm")
        assert ex21.document_url == ("https://www.sec.gov/Archives/edgar/data/320193/000032019324000001/ex-21.htm")

    def test_null_description_preserved(self) -> None:
        docs = parse_filing_index(_INDEX, accession_number="0000320193-24-000001")
        xlsx = next(d for d in docs if d.document_name == "Financial_Report.xlsx")
        assert xlsx.description is None

    def test_missing_size_parses_to_none(self) -> None:
        index = {
            "cik": "320193",
            "primaryDocument": "x.htm",
            "items": [{"name": "x.htm", "type": "10-K", "description": "X", "size": None}],
        }
        docs = parse_filing_index(index, accession_number="0000320193-24-000001")
        assert docs[0].size_bytes is None

    def test_duplicate_names_dedup(self) -> None:
        """Two entries with the same ``name`` only yield one row."""
        index = {
            "cik": "320193",
            "primaryDocument": "x.htm",
            "items": [
                {"name": "x.htm", "type": "10-K", "description": "X", "size": 1000},
                {"name": "x.htm", "type": "10-K", "description": "X dup", "size": 1000},
            ],
        }
        docs = parse_filing_index(index, accession_number="0000320193-24-000001")
        assert len(docs) == 1

    def test_missing_items_returns_empty(self) -> None:
        assert parse_filing_index({"cik": "320193"}, accession_number="X") == ()

    def test_shape(self) -> None:
        docs = parse_filing_index(_INDEX, accession_number="0000320193-24-000001")
        assert isinstance(docs[0], ParsedFilingDocument)
