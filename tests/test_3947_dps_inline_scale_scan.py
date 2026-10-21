"""#3947 scan fetch path (spec §Document set), against a stub provider: no network, no DB."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.dps_inline_scale_scan import Target, fetch_document_set
from app.services.inline_xbrl_dps import DocumentSetError

_BASE = "https://www.sec.gov/Archives/edgar/data/1411342/000162828026033463/"
_SUMMARY = "<FilingSummary><MyReports><Report><HtmlFileName>R1.htm</HtmlFileName></Report></MyReports></FilingSummary>"


class _Provider:
    def __init__(self, names: list[str] | None, bodies: dict[str, str]) -> None:
        self.names = names
        self.bodies = bodies
        self.fetched: list[str] = []
        self.index_cik: str | None = None

    def fetch_filing_index(self, provider_filing_id: str, *, issuer_cik: str | None = None) -> dict[str, object] | None:
        self.index_cik = issuer_cik
        if self.names is None:
            return None
        return {"directory": {"item": [{"name": n, "size": "1"} for n in self.names]}}

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.fetched.append(absolute_url.removeprefix(_BASE))
        return self.bodies.get(absolute_url.removeprefix(_BASE))


def _target(cik: str | None = "0001411342") -> Target:
    return Target("0001628280-26-033463", date(2026, 5, 1), cik, (7,), None, None)


def test_reads_every_html_file_except_rendered_pages_in_listing_order() -> None:
    names = [
        "0001628280-26-033463-index.html",
        "FilingSummary.xml",
        "R1.htm",
        "efc.htm",
        "ex31.HTM",
        "efc.xsd",
        "x.xhtml",
    ]
    provider = _Provider(names, {**{n: n for n in names}, "FilingSummary.xml": _SUMMARY})
    documents = fetch_document_set(provider, _target())
    assert [n for n, _ in documents] == ["0001628280-26-033463-index.html", "efc.htm", "ex31.HTM", "x.xhtml"]
    assert provider.fetched == ["FilingSummary.xml", *[n for n, _ in documents]]
    assert provider.index_cik == "0001411342"


def test_summary_is_only_an_exclusion_list() -> None:
    provider = _Provider(["FilingSummary.xml", "R1.htm"], {"FilingSummary.xml": _SUMMARY, "R1.htm": "r"})
    assert fetch_document_set(provider, _target()) == []
    # No summary in the listing: nothing excluded, at the cost of fetching R1.htm.
    provider = _Provider(["R1.htm"], {"R1.htm": "r"})
    assert fetch_document_set(provider, _target()) == [("R1.htm", "r")]


@pytest.mark.parametrize(
    ("provider", "target", "reason"),
    [
        (_Provider(["a.htm"], {"a.htm": ""}), _target(None), "no_issuer_cik"),
        (_Provider(None, {}), _target(), "index_not_found"),
        (_Provider([], {}), _target(), "index_unparseable"),
        (_Provider(["a.htm"], {}), _target(), "document_not_found:a.htm"),
    ],
)
def test_a_missing_listing_or_document_fails_the_attempt(provider: _Provider, target: Target, reason: str) -> None:
    with pytest.raises(DocumentSetError, match=reason):
        fetch_document_set(provider, target)
