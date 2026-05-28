"""Unit tests for the shared ``submissions.zip`` accelerator (#1340).

No DB / no HTTP — exercises the URL matcher, the zip-entry reader, and the
``ZipBackedArchiveFetcher`` routing contract (every non-hit delegates to the
wrapped fetcher; only a clean hit short-circuits).
"""

from __future__ import annotations

import io
import zipfile
from unittest.mock import MagicMock

import pytest

from app.services.sec_submissions_zip import (
    PRIMARY_SUBMISSIONS_URL_RE,
    ZipBackedArchiveFetcher,
    match_primary_submissions_cik,
    read_zip_entry,
)

_PRIMARY = "https://data.sec.gov/submissions/CIK0000320193.json"
_SECONDARY = "https://data.sec.gov/submissions/CIK0000320193-submissions-001.json"
_DOC = "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/primary_doc.xml"


def _zip_with(entries: dict[str, bytes]) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    buf.seek(0)
    return zipfile.ZipFile(buf)


class TestMatchPrimaryCik:
    def test_primary_url_matches(self) -> None:
        assert match_primary_submissions_cik(_PRIMARY) == "0000320193"

    def test_secondary_url_no_match(self) -> None:
        assert match_primary_submissions_cik(_SECONDARY) is None

    def test_doc_url_no_match(self) -> None:
        assert match_primary_submissions_cik(_DOC) is None

    def test_regex_anchored(self) -> None:
        # Trailing junk must not match (anchored $).
        assert PRIMARY_SUBMISSIONS_URL_RE.match(_PRIMARY + "?x=1") is None


class TestReadZipEntry:
    def test_hit_returns_bytes(self) -> None:
        zf = _zip_with({"CIK0000320193.json": b'{"cik":"320193"}'})
        assert read_zip_entry(zf, "CIK0000320193.json") == b'{"cik":"320193"}'

    def test_miss_returns_none(self) -> None:
        zf = _zip_with({"CIK0000320193.json": b"{}"})
        assert read_zip_entry(zf, "CIK9999999999.json") is None

    def test_read_error_raises(self) -> None:
        zf = MagicMock(spec=zipfile.ZipFile)
        zf.open.side_effect = zipfile.BadZipFile("crc mismatch")
        with pytest.raises(zipfile.BadZipFile):
            read_zip_entry(zf, "CIK0000320193.json")


class TestZipBackedArchiveFetcher:
    def test_primary_hit_decodes_and_skips_fallback(self) -> None:
        zf = _zip_with({"CIK0000320193.json": b'{"cik":"320193"}'})
        fallback = MagicMock()
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_PRIMARY) == '{"cik":"320193"}'
        fallback.fetch_document_text.assert_not_called()

    def test_primary_miss_delegates(self) -> None:
        zf = _zip_with({"CIK0000000001.json": b"{}"})
        fallback = MagicMock()
        fallback.fetch_document_text.return_value = '{"from":"http"}'
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_PRIMARY) == '{"from":"http"}'
        fallback.fetch_document_text.assert_called_once_with(_PRIMARY)

    def test_primary_miss_propagates_fallback_none(self) -> None:
        # A genuine 404 at the HTTP layer (fallback returns None) is returned
        # verbatim — caller's "no work" path still reachable when truly absent.
        zf = _zip_with({"CIK0000000001.json": b"{}"})
        fallback = MagicMock()
        fallback.fetch_document_text.return_value = None
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_PRIMARY) is None

    def test_primary_bad_zip_delegates(self) -> None:
        zf = MagicMock(spec=zipfile.ZipFile)
        zf.open.side_effect = zipfile.BadZipFile("truncated")
        fallback = MagicMock()
        fallback.fetch_document_text.return_value = "http-body"
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_PRIMARY) == "http-body"
        fallback.fetch_document_text.assert_called_once_with(_PRIMARY)

    def test_primary_bad_utf8_delegates(self) -> None:
        zf = _zip_with({"CIK0000320193.json": b"\xff\xfe\x00bad"})
        fallback = MagicMock()
        fallback.fetch_document_text.return_value = "http-body"
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_PRIMARY) == "http-body"
        fallback.fetch_document_text.assert_called_once_with(_PRIMARY)

    def test_secondary_url_delegates(self) -> None:
        zf = _zip_with({"CIK0000320193.json": b"{}"})
        fallback = MagicMock()
        fallback.fetch_document_text.return_value = "secondary-body"
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_SECONDARY) == "secondary-body"
        fallback.fetch_document_text.assert_called_once_with(_SECONDARY)

    def test_doc_url_delegates(self) -> None:
        zf = _zip_with({"CIK0000320193.json": b"{}"})
        fallback = MagicMock()
        fallback.fetch_document_text.return_value = "<xml/>"
        fetcher = ZipBackedArchiveFetcher(zf, fallback=fallback)
        assert fetcher.fetch_document_text(_DOC) == "<xml/>"
        fallback.fetch_document_text.assert_called_once_with(_DOC)
