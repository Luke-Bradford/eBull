"""#3947 scan listing path (spec §Document set, as Amendment 2 B2.4 keeps it for a fallback), against a stub provider
whose archive is gone: no network, no DB."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.dps_inline_scale_scan import Target, acquire_document_source
from app.services.inline_xbrl_dps import DocumentSetError

_BASE = "https://www.sec.gov/Archives/edgar/data/1411342/000162828026033463/"
_SUMMARY = "<FilingSummary><MyReports><Report><HtmlFileName>R1.htm</HtmlFileName></Report></MyReports></FilingSummary>"
_HEADERS = "0001628280-26-033463-index-headers.html"


class _Provider:
    """The archive 404s, so every attempt takes the listing path."""

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
        name = absolute_url.removeprefix(_BASE)
        self.fetched.append(name)
        return "" if name == _HEADERS else self.bodies.get(name)

    def fetch_document_bytes(self, absolute_url: str, *, max_bytes: int) -> bytes | None:
        name = absolute_url.removeprefix(_BASE)
        self.fetched.append(name)
        body = self.bodies.get(name)
        return None if body is None or name.endswith("-xbrl.zip") else body.encode()


def _target(cik: str | None = "0001411342") -> Target:
    return Target("0001628280-26-033463", date(2026, 5, 1), cik, (7,), None, None)


def _listed(provider: _Provider) -> list[str]:
    return [n for n, _ in acquire_document_source(provider, _target()).candidates]


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
    listed = _listed(provider)
    assert listed == ["0001628280-26-033463-index.html", "efc.htm", "ex31.HTM", "x.xhtml"]
    assert provider.fetched == ["0001628280-26-033463-xbrl.zip", "FilingSummary.xml", *listed, _HEADERS]
    assert provider.index_cik == "0001411342"


def test_summary_is_only_an_exclusion_list() -> None:
    assert _listed(_Provider(["FilingSummary.xml", "R1.htm"], {"FilingSummary.xml": _SUMMARY, "R1.htm": "r"})) == []
    # No summary in the listing: nothing excluded, at the cost of fetching R1.htm.
    assert _listed(_Provider(["R1.htm"], {"R1.htm": "r"})) == ["R1.htm"]


@pytest.mark.parametrize(
    ("provider", "reason"),
    [
        (_Provider(None, {}), "index_not_found"),
        (_Provider([], {}), "index_unparseable"),
        (_Provider(["a.htm"], {}), "document_not_found:a.htm"),
    ],
)
def test_a_missing_listing_or_document_fails_the_attempt(provider: _Provider, reason: str) -> None:
    with pytest.raises(DocumentSetError, match=reason):
        acquire_document_source(provider, _target())


def test_finishing_a_run_on_a_dead_connection_keeps_the_original_error() -> None:
    import psycopg

    from app.services.dps_inline_scale_scan import _finish_run

    class _Dead:
        def transaction(self) -> object:
            raise psycopg.OperationalError("connection is dead")

    # An error is already propagating: the failed finish is logged, not raised over it.
    _finish_run(_Dead(), 1, status="failed", upserted=0, error="ValueError('unexpected')")  # type: ignore[arg-type]
    # Nothing propagating: the failure to finish is the error.
    with pytest.raises(psycopg.OperationalError):
        _finish_run(_Dead(), 1, status="success", upserted=0, error=None)  # type: ignore[arg-type]
