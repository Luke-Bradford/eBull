"""#3590 — a filing-INDEX page is never a document: Atom discovery maps it to the complete submission, and a
stored body fetched from one is never reused.

Pure (no DB): ``read_raw`` is stubbed, so this module stays in the fast tier.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from app.providers.implementations.sec_getcurrent import parse_getcurrent_atom
from app.services import raw_filings
from app.services.raw_filings import RawFilingDocument
from app.services.sec_manifest import complete_submission_url, is_filing_index_url

# Real shape, as stored on dev for a tombstoned Form 4 (0001104659-26-112062).
_INDEX = "https://www.sec.gov/Archives/edgar/data/1813814/000110465926112062/0001104659-26-112062-index.htm"
_TXT = "https://www.sec.gov/Archives/edgar/data/1813814/0001104659-26-112062.txt"


@pytest.mark.parametrize(
    ("url", "is_index", "submission"),
    [
        (_INDEX, True, _TXT),
        (_INDEX + "l", True, _TXT),  # ``-index.html``
        (_TXT, False, None),
        ("https://www.sec.gov/Archives/edgar/data/1813814/000110465926112062/form4.xml", False, None),
        # ``-index-headers.html`` is a different page and keeps its URL.
        (_INDEX.replace("-index.htm", "-index-headers.html"), False, None),
        # The flat shape (no accession folder).
        (
            "https://www.sec.gov/Archives/edgar/data/320193/0000320193-26-000042-index.htm",
            True,
            "https://www.sec.gov/Archives/edgar/data/320193/0000320193-26-000042.txt",
        ),
        # A one-digit CIK folder.
        (
            "https://www.sec.gov/Archives/edgar/data/7/000000000726000001/0000000007-26-000001-index.htm",
            True,
            "https://www.sec.gov/Archives/edgar/data/7/0000000007-26-000001.txt",
        ),
    ],
)
def test_filing_index_url_mapping(url: str, is_index: bool, submission: str | None) -> None:
    assert is_filing_index_url(url) is is_index
    assert complete_submission_url(url) == submission


def test_is_filing_index_url_none() -> None:
    assert is_filing_index_url(None) is False


def _atom(href: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>4 - Some Insider (0001813814) (Reporting)</title>
    <updated>2026-09-29T17:14:03-04:00</updated>
    <link rel="alternate" type="text/html" href="{href}"/>
    <category scheme="https://www.sec.gov/" label="form type" term="4"/>
    <id>urn:tag:sec.gov,2008:accession-number=0001104659-26-112062</id>
  </entry>
</feed>
""".encode()


def test_atom_index_link_becomes_complete_submission() -> None:
    (row,) = parse_getcurrent_atom(_atom(_INDEX))
    assert row.primary_document_url == _TXT


def test_atom_non_index_link_is_kept() -> None:
    doc = "https://www.sec.gov/Archives/edgar/data/1813814/000110465926112062/form4.xml"
    (row,) = parse_getcurrent_atom(_atom(doc))
    assert row.primary_document_url == doc


def _stub_read_raw(monkeypatch: pytest.MonkeyPatch, source_url: str | None, payload: str | None) -> None:
    def _read(conn: Any, *, accession_number: str, document_kind: str) -> RawFilingDocument:
        return RawFilingDocument(
            accession_number=accession_number,
            document_kind="form4_xml",
            payload=payload,
            byte_count=None if payload is None else len(payload),
            parser_version="form4-v1",
            fetched_at=datetime(2026, 9, 30, tzinfo=UTC),
            source_url=source_url,
        )

    monkeypatch.setattr(raw_filings, "read_raw", _read)


def test_stored_body_refuses_an_index_page_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_read_raw(monkeypatch, _INDEX, "<!DOCTYPE HTML PUBLIC ...>")
    assert raw_filings.stored_body(None, accession_number="0001104659-26-112062", document_kind="form4_xml") is None  # type: ignore[arg-type]


@pytest.mark.parametrize("source_url", [_TXT, None])
def test_stored_body_reuses_a_document_body(monkeypatch: pytest.MonkeyPatch, source_url: str | None) -> None:
    _stub_read_raw(monkeypatch, source_url, "<SEC-DOCUMENT>...")
    assert (
        raw_filings.stored_body(None, accession_number="0001104659-26-112062", document_kind="form4_xml")  # type: ignore[arg-type]
        == "<SEC-DOCUMENT>..."
    )
