"""Unit tests for ``app.services.filing_documents.parse_filing_index`` (#452 / #723)."""

from __future__ import annotations

from app.services.filing_documents import (
    ParsedFilingDocument,
    parse_filing_index,
)

# Real shape captured from a live SEC index.json (AAPL 8-K filed
# 2026-04-30, accession 0000320193-26-000011, fetched against the
# live SEC archive 2026-04-30 22:50 UTC). The shape is documented
# at #723 — top-level ``directory.item`` array, ``type`` is a
# content-type icon name (``text.gif``, ``compressed.gif``) NOT the
# SEC document-type label, ``size`` is a string (sometimes empty
# for index/header entries that have no file size).
_INDEX_AAPL_8K = {
    "directory": {
        "name": "/Archives/edgar/data/320193/000032019326000011",
        "parent-dir": "/Archives/edgar/data/320193/",
        "item": [
            {
                "last-modified": "2026-04-30 16:30:41",
                "name": "0000320193-26-000011-index-headers.html",
                "type": "text.gif",
                "size": "",
            },
            {
                "last-modified": "2026-04-30 16:30:41",
                "name": "0000320193-26-000011.txt",
                "type": "text.gif",
                "size": "",
            },
            {
                "last-modified": "2026-04-30 16:30:41",
                "name": "0000320193-26-000011-xbrl.zip",
                "type": "compressed.gif",
                "size": "24456",
            },
            {
                "last-modified": "2026-04-30 16:30:41",
                "name": "a8-kex991q2202603282026.htm",
                "type": "text.gif",
                "size": "168815",
            },
            {
                "last-modified": "2026-04-30 16:30:41",
                "name": "aapl-20260430.htm",
                "type": "text.gif",
                "size": "37639",
            },
            {
                "last-modified": "2026-04-30 16:30:41",
                "name": "aapl-20260430.xsd",
                "type": "text.gif",
                "size": "3650",
            },
        ],
    }
}


class TestParseFilingIndex:
    def test_all_documents_surface(self) -> None:
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name="aapl-20260430.htm",
        )
        assert len(docs) == 6
        names = {d.document_name for d in docs}
        assert "aapl-20260430.htm" in names
        assert "a8-kex991q2202603282026.htm" in names
        assert "aapl-20260430.xsd" in names

    def test_primary_flag_set_only_on_supplied_primary_name(self) -> None:
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name="aapl-20260430.htm",
        )
        primaries = [d for d in docs if d.is_primary]
        assert len(primaries) == 1
        assert primaries[0].document_name == "aapl-20260430.htm"

    def test_no_primary_flagged_when_caller_passes_none(self) -> None:
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name=None,
        )
        assert all(not d.is_primary for d in docs)

    def test_url_reconstruction_uses_int_cik(self) -> None:
        """SEC archive paths drop CIK leading zeroes; passing a
        zero-padded ``cik`` string still produces the canonical URL."""
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="0000320193",
            primary_document_name="aapl-20260430.htm",
        )
        primary = next(d for d in docs if d.is_primary)
        assert primary.document_url == (
            "https://www.sec.gov/Archives/edgar/data/320193/000032019326000011/aapl-20260430.htm"
        )

    def test_empty_string_size_parses_to_none(self) -> None:
        """SEC emits ``"size": ""`` for index/header entries that
        have no file size. Coerce to None, not 0."""
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name="aapl-20260430.htm",
        )
        headers = next(d for d in docs if d.document_name.endswith("-index-headers.html"))
        assert headers.size_bytes is None

    def test_numeric_string_size_parses_to_int(self) -> None:
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name="aapl-20260430.htm",
        )
        primary = next(d for d in docs if d.is_primary)
        assert primary.size_bytes == 37639

    def test_document_type_and_description_are_null(self) -> None:
        """SEC's ``index.json`` ``type`` field is a content-type icon
        name, not a document-type label. Parser intentionally leaves
        ``document_type`` and ``description`` NULL — rich types
        require HTML-page parsing (see module docstring)."""
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name="aapl-20260430.htm",
        )
        for d in docs:
            assert d.document_type is None
            assert d.description is None

    def test_duplicate_names_dedup(self) -> None:
        """Two entries with the same ``name`` only yield one row."""
        index = {
            "directory": {
                "item": [
                    {"name": "x.htm", "type": "text.gif", "size": "1000"},
                    {"name": "x.htm", "type": "text.gif", "size": "1000"},
                ],
            }
        }
        docs = parse_filing_index(
            index,
            accession_number="0000320193-24-000001",
            cik="320193",
            primary_document_name=None,
        )
        assert len(docs) == 1

    def test_missing_directory_returns_empty(self) -> None:
        assert (
            parse_filing_index(
                {"foo": "bar"},
                accession_number="X",
                cik="320193",
                primary_document_name=None,
            )
            == ()
        )

    def test_missing_item_array_returns_empty(self) -> None:
        assert (
            parse_filing_index(
                {"directory": {"name": "x"}},
                accession_number="X",
                cik="320193",
                primary_document_name=None,
            )
            == ()
        )

    def test_legacy_pre723_shape_returns_empty(self) -> None:
        """The pre-#723 hypothetical shape (top-level ``items``,
        ``cik``, ``primaryDocument``) is not what SEC actually
        returns. If a stale fixture or mock provider feeds us that
        shape, parse cleanly to empty rather than guessing."""
        legacy = {
            "cik": "320193",
            "form": "10-K",
            "primaryDocument": "x.htm",
            "items": [{"name": "x.htm", "size": 100}],
        }
        assert (
            parse_filing_index(
                legacy,
                accession_number="0000320193-24-000001",
                cik="320193",
                primary_document_name="x.htm",
            )
            == ()
        )

    def test_shape(self) -> None:
        docs = parse_filing_index(
            _INDEX_AAPL_8K,
            accession_number="0000320193-26-000011",
            cik="320193",
            primary_document_name="aapl-20260430.htm",
        )
        assert isinstance(docs[0], ParsedFilingDocument)
