"""#3947 Amendment 2 B2: the archive acquisition, its fallbacks and limits, and the SGML header records (B1.3).

Stub provider and in-memory zips: no network, no DB. The provider's own bytes method runs over an
``httpx.MockTransport``.
"""

from __future__ import annotations

import io
import warnings
import zipfile
from datetime import date

import httpx
import pytest

import app.services.dps_inline_scale_scan as scan
from app.providers.implementations.sec_edgar import ResponseTooLarge, SecFilingsProvider
from app.providers.resilient_client import ResilientClient
from app.services.dps_inline_scale_scan import (
    Target,
    acquire_document_source,
    member_types,
    parse_header_records,
)
from app.services.inline_xbrl_dps import DocumentSetError

_ACCESSION = "0001628280-26-055148"
_BASE = "https://www.sec.gov/Archives/edgar/data/1411342/000162828026055148/"
_ZIP = f"{_ACCESSION}-xbrl.zip"
_HEADERS = f"{_ACCESSION}-index-headers.html"
_SUMMARY = "<FilingSummary><MyReports><Report><HtmlFileName>R1.htm</HtmlFileName></Report></MyReports></FilingSummary>"


def _record(doc_type: str, sequence: int, filename: str) -> str:
    return (
        f"&lt;DOCUMENT&gt;\n&lt;TYPE&gt;{doc_type}\n&lt;SEQUENCE&gt;{sequence}\n&lt;FILENAME&gt;{filename}\n"
        f"&lt;DESCRIPTION&gt;{doc_type}\n&lt;TEXT&gt;\n"
        f'<a href="{filename}">Document {sequence} - file: {filename}</a><br>\n&lt;/DOCUMENT&gt;\n'
    )


# The shape of EDGAR's page (``0001628280-26-055148``, fetched 2026-10-23): the submission header unescaped inside an
# HTML comment, carrying the submission's own <TYPE> outside any <DOCUMENT>; the SGML escaped inside a <PRE>.
_PAGE = (
    f"<HTML><HEAD><TITLE>SEC EDGAR Submission {_ACCESSION}</TITLE>\n<!--\n<SEC-HEADER>{_ACCESSION}.hdr.sgml\n"
    "<TYPE>10-Q\n</SEC-HEADER>\n-->\n</HEAD><BODY>\n<PRE>&lt;SEC-HEADER&gt;\nCONFORMED SUBMISSION TYPE:\t10-Q\n"
    "&lt;/SEC-HEADER&gt;\n"
    + _record("10-Q", 1, "efc-20260630.htm")
    + _record("EX-31.1", 2, "efcex3112026q2.htm")
    + _record("EX-101.SCH", 3, "efc-20260630.xsd")
    + "</PRE></BODY></HTML>\n"
)


def _zip(members: list[tuple[str, bytes]], *, compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # zipfile warns on a duplicate name; the fixture wants one
        with zipfile.ZipFile(buffer, "w", compression) as archive:
            for name, body in members:
                archive.writestr(name, body)
    return buffer.getvalue()


_MEMBERS = [
    ("efc-20260630.htm", b"<html>primary</html>"),
    ("efc-20260630.xsd", b"<schema/>" * 50),
    ("efcex3112026q2.htm", b"<html>ex31</html>"),
]


def _bad_crc() -> bytes:
    body = _zip([("a.htm", b"<html>abcdef</html>")], compression=zipfile.ZIP_STORED)
    return body.replace(b"abcdef", b"abcdeX", 1)


class _Provider:
    """``archive`` is the zip's bytes, ``None`` (404/410) or an exception to raise; ``max_bytes`` is honoured as
    the real method honours it."""

    def __init__(
        self,
        archive: bytes | Exception | None,
        *,
        listing: list[str] | None = None,
        files: dict[str, bytes] | None = None,
        header: str | Exception | None = _PAGE,
    ) -> None:
        self.archive = archive
        self.listing = listing if listing is not None else ["FilingSummary.xml", "R1.htm", "a.htm", "b.xhtml"]
        self.files = files if files is not None else {"a.htm": b"A", "b.xhtml": b"B"}
        self.header = header
        self.calls: list[str] = []

    def fetch_filing_index(self, provider_filing_id: str, *, issuer_cik: str | None = None) -> dict[str, object] | None:
        self.calls.append("index.json")
        return {"directory": {"item": [{"name": n, "size": "1"} for n in self.listing]}}

    def fetch_document_text(self, absolute_url: str) -> str | None:
        name = absolute_url.removeprefix(_BASE)
        self.calls.append(name)
        if name == _HEADERS:
            if isinstance(self.header, Exception):
                raise self.header
            return self.header
        return _SUMMARY if name == "FilingSummary.xml" else None

    def fetch_document_bytes(self, absolute_url: str, *, max_bytes: int) -> bytes | None:
        name = absolute_url.removeprefix(_BASE)
        self.calls.append(name)
        body = self.archive if name == _ZIP else self.files.get(name)
        if isinstance(body, Exception):
            raise body
        if body is not None and len(body) > max_bytes:
            raise ResponseTooLarge(name)
        return body


def _target(cik: str | None = "0001411342") -> Target:
    return Target(_ACCESSION, date(2026, 8, 10), cik, (7,), None, None)


def test_the_archive_path_reads_only_html_members_then_the_header() -> None:
    provider = _Provider(_zip(_MEMBERS))
    source = acquire_document_source(provider, _target())
    assert (source.path, source.fallback, source.requests) == ("archive", None, 2)
    assert source.candidates == (_MEMBERS[0], _MEMBERS[2])
    assert provider.calls == [_ZIP, _HEADERS]
    assert source.header["efc-20260630.htm"] == (("10-Q",),)


# A zip's bytes carry its timestamp, so each case is named by its reason (xdist workers must collect equal ids).
@pytest.mark.parametrize(
    ("archive", "reason"),
    [
        pytest.param(None, "archive_not_found", id="archive_not_found-0"),  # the provider's 404 or 410
        pytest.param(ResponseTooLarge(_ZIP), "archive_too_large", id="archive_too_large-1"),
        pytest.param(b"not a zip", "archive_not_zip", id="archive_not_zip-2"),
        pytest.param(_bad_crc(), "archive_bad_crc", id="archive_bad_crc-3"),
        pytest.param(_zip([]), "archive_empty", id="archive_empty-4"),
        pytest.param(_zip([("a.htm", b"1"), ("a.htm", b"2")]), "archive_duplicate_name", id="archive_duplicate_name-5"),
        pytest.param(_zip([("sub/a.htm", b"1")]), "archive_directory_member", id="archive_directory_member-6"),
        pytest.param(_zip([("sub\\a.htm", b"1")]), "archive_directory_member", id="archive_directory_member-7"),
    ],
)
def test_each_b2_4_reason_takes_the_listing_path(archive: bytes | Exception | None, reason: str) -> None:
    provider = _Provider(archive)
    source = acquire_document_source(provider, _target())
    assert (source.path, source.fallback) == ("listing", reason)
    # The rendered page FilingSummary.xml names is not fetched; every other listed HTML file is, as bytes.
    assert source.candidates == (("a.htm", b"A"), ("b.xhtml", b"B"))
    assert provider.calls == [_ZIP, "index.json", "FilingSummary.xml", "a.htm", "b.xhtml", _HEADERS]
    assert source.requests == 6


@pytest.mark.parametrize("compression", [zipfile.ZIP_DEFLATED, zipfile.ZIP_BZIP2, zipfile.ZIP_LZMA])
def test_a_corrupt_member_of_any_compression_method_takes_the_listing_path(compression: int) -> None:
    body = b"<html>" + b"inline xbrl " * 400 + b"</html>"
    archive = bytearray(_zip([("a.htm", body)], compression=compression))
    start = archive.index(b"a.htm") + len("a.htm")  # the local header's name; the compressed data follows it
    archive[start + 8 : start + 40] = bytes(32)
    assert acquire_document_source(_Provider(bytes(archive)), _target()).fallback in (
        "archive_not_zip",
        "archive_bad_crc",
    )


def test_the_response_limit_reads_at_the_limit_and_falls_back_one_byte_over(monkeypatch: pytest.MonkeyPatch) -> None:
    archive = _zip(_MEMBERS)
    monkeypatch.setattr(scan, "RESPONSE_MAX_BYTES", len(archive))
    assert acquire_document_source(_Provider(archive), _target()).path == "archive"
    monkeypatch.setattr(scan, "RESPONSE_MAX_BYTES", len(archive) - 1)
    assert acquire_document_source(_Provider(archive), _target()).fallback == "archive_too_large"


def test_the_html_total_counts_html_members_only(monkeypatch: pytest.MonkeyPatch) -> None:
    html_total = len(_MEMBERS[0][1]) + len(_MEMBERS[2][1])  # the .xsd member is larger than both and is not read
    monkeypatch.setattr(scan, "ARCHIVE_HTML_MAX_BYTES", html_total)
    assert acquire_document_source(_Provider(_zip(_MEMBERS)), _target()).path == "archive"
    monkeypatch.setattr(scan, "ARCHIVE_HTML_MAX_BYTES", html_total - 1)
    source = acquire_document_source(_Provider(_zip(_MEMBERS)), _target())
    assert (source.path, source.fallback) == ("listing", "archive_html_too_large")


def test_any_other_archive_error_fails_the_attempt() -> None:
    request = httpx.Request("GET", _BASE + _ZIP)
    error = httpx.HTTPStatusError("500", request=request, response=httpx.Response(500, request=request))
    provider = _Provider(error)
    with pytest.raises(httpx.HTTPStatusError):
        acquire_document_source(provider, _target())
    assert provider.calls == [_ZIP]


@pytest.mark.parametrize(
    ("provider", "target", "reason"),
    [
        (_Provider(_zip(_MEMBERS)), _target(None), "no_issuer_cik"),
        (_Provider(_zip(_MEMBERS)), _target("CIK1411342"), "issuer_cik_not_numeric"),
        (_Provider(_zip(_MEMBERS), header=None), _target(), "header_not_found"),
        (_Provider(None, files={"a.htm": b"A"}), _target(), "document_not_found:b.xhtml"),
    ],
)
def test_a_missing_cik_header_or_listed_file_fails_the_attempt(
    provider: _Provider, target: Target, reason: str
) -> None:
    with pytest.raises(DocumentSetError, match=reason):
        acquire_document_source(provider, target)


def test_a_failed_header_request_fails_the_attempt() -> None:
    with pytest.raises(httpx.ConnectError):
        acquire_document_source(_Provider(_zip(_MEMBERS), header=httpx.ConnectError("down")), _target())


def test_header_records_are_per_document_and_skip_the_submission_type() -> None:
    assert parse_header_records(_PAGE) == {
        "efc-20260630.htm": (("10-Q",),),
        "efcex3112026q2.htm": (("EX-31.1",),),
        "efc-20260630.xsd": (("EX-101.SCH",),),
    }
    page = _record("EX-99.1", 2, "ex99.htm") + _record("EX-99.2", 3, "ex99.htm")
    page += "&lt;DOCUMENT&gt;\n&lt;TYPE&gt;8-K\n&lt;TYPE&gt;EX-99\n&lt;FILENAME&gt;two.htm\n&lt;/DOCUMENT&gt;\n"
    page += "&lt;DOCUMENT&gt;\n&lt;TYPE&gt;\n&lt;FILENAME&gt;empty.htm\n&lt;/DOCUMENT&gt;\n"
    assert parse_header_records(page) == {
        "ex99.htm": (("EX-99.1",), ("EX-99.2",)),
        "two.htm": (("8-K", "EX-99"),),
        "empty.htm": (("",),),
    }


def test_member_types_needs_exactly_one_non_empty_type_per_inline_document() -> None:
    page = _PAGE + _record("EX-99.1", 4, "ex99.htm") + _record("EX-99.2", 5, "ex99.htm")
    page += "&lt;DOCUMENT&gt;\n&lt;TYPE&gt;8-K\n&lt;TYPE&gt;EX-99\n&lt;FILENAME&gt;two.htm\n&lt;/DOCUMENT&gt;\n"
    page += "&lt;DOCUMENT&gt;\n&lt;TYPE&gt;\n&lt;FILENAME&gt;empty.htm\n&lt;/DOCUMENT&gt;\n"
    header = parse_header_records(page)
    assert member_types(header, ["efc-20260630.htm", "efcex3112026q2.htm"]) == {
        "efc-20260630.htm": "10-Q",
        "efcex3112026q2.htm": "EX-31.1",
    }
    # Records of other files are not checked: ex99.htm is repeated, and only a member named ex99.htm fails on it.
    for member, reason in [
        ("absent.htm", "header_missing:absent.htm"),
        ("ex99.htm", "header_repeated:ex99.htm"),
        ("two.htm", "header_type:two.htm"),
        ("empty.htm", "header_type:empty.htm"),
    ]:
        with pytest.raises(DocumentSetError, match=reason):
            member_types(header, ["efc-20260630.htm", member])


def _provider(handler: httpx.MockTransport) -> SecFilingsProvider:
    provider = SecFilingsProvider(user_agent="test test@example.com")
    provider._tickers_client = httpx.Client(headers={"User-Agent": "test test@example.com"}, transport=handler)  # noqa: SLF001
    provider._http_tickers = ResilientClient(provider._tickers_client, min_request_interval_s=0.0, max_retries=0)  # noqa: SLF001
    return provider


_SERVED = b'<html><body>x<script type="text/javascript" src="/a/b"></script></body></html>'


def test_fetch_document_bytes_returns_the_served_bytes_up_to_the_limit() -> None:
    provider = _provider(httpx.MockTransport(lambda r: httpx.Response(200, content=_SERVED)))
    # As served: the front end's script is not stripped from bytes.
    assert provider.fetch_document_bytes(_BASE + "a.htm", max_bytes=len(_SERVED)) == _SERVED
    with pytest.raises(ResponseTooLarge):
        provider.fetch_document_bytes(_BASE + "a.htm", max_bytes=len(_SERVED) - 1)


@pytest.mark.parametrize("status", [404, 410])
def test_fetch_document_bytes_is_none_when_the_file_is_gone(status: int) -> None:
    provider = _provider(httpx.MockTransport(lambda r: httpx.Response(status)))
    assert provider.fetch_document_bytes(_BASE + _ZIP, max_bytes=10) is None


def test_fetch_document_bytes_raises_any_other_error() -> None:
    provider = _provider(httpx.MockTransport(lambda r: httpx.Response(500)))
    with pytest.raises(httpx.HTTPStatusError):
        provider.fetch_document_bytes(_BASE + _ZIP, max_bytes=10)
