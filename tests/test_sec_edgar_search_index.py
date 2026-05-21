"""Tests for SecFilingsProvider.fetch_search_index_json (#1233 PR11).

The PR11 13D/G discovery job hits the efts.sec.gov full-text
search-index endpoint to enumerate which blockholder filings exist
per universe issuer. The discovery service MUST go through this
provider method so the request shares the SEC 10 req/s budget via
the process-wide rate-limit clock + lock (lint guard I in spec §3.6
catches future raw-httpx side-channels).

These tests pin:
  1. URL shape (host, path, query string) so a future refactor that
     accidentally changes the parameter set or drops dateRange=custom
     fails loudly.
  2. Pagination offset wiring (``from_offset`` flows into the URL).
  3. 404 → ``None`` contract (matches :func:`fetch_filing_index`'s
     shape so the discovery loop can treat both endpoints the same).
"""

from __future__ import annotations

from datetime import date
from typing import Any
from urllib.parse import parse_qs, urlsplit

from app.providers.implementations.sec_edgar import SecFilingsProvider


class _FakeResponse:
    """Minimal stand-in for an httpx.Response. Only exposes the
    surface ``fetch_search_index_json`` reads: ``status_code``,
    ``raise_for_status``, and ``json``."""

    def __init__(self, status_code: int, body: object | None = None) -> None:
        self.status_code = status_code
        self._body = body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise AssertionError(f"raise_for_status invoked unexpectedly for status {self.status_code}")

    def json(self) -> object:
        return self._body


def _make_provider() -> SecFilingsProvider:
    return SecFilingsProvider(user_agent="test test@example.com")


def test_query_url_shape_minimal() -> None:
    """First-page query: every documented parameter appears in the URL
    with the expected encoding. Host is efts.sec.gov, path is
    /LATEST/search-index, and the dict returned by ``json()`` is
    returned verbatim by the method."""
    captured: list[str] = []
    fake_body: dict[str, object] = {"hits": {"hits": [], "total": {"value": 0}}}

    def fake_get(url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
        captured.append(url)
        return _FakeResponse(200, fake_body)

    provider = _make_provider()
    provider._http_tickers.get = fake_get  # type: ignore[method-assign]  # noqa: SLF001

    result = provider.fetch_search_index_json(
        ciks="0000320193",
        forms=("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"),
        startdt=date(2024, 12, 18),
        enddt=date(2026, 5, 21),
        from_offset=0,
        size=100,
    )

    assert result is fake_body
    assert len(captured) == 1
    url = captured[0]

    # Host + path
    split = urlsplit(url)
    assert split.netloc == "efts.sec.gov"
    assert split.path == "/LATEST/search-index"

    # Parse the query string to assert on values without depending on
    # whether urlencode picked '+' or '%20' for the space inside
    # form names — both are legal per the URL spec and ``parse_qs``
    # normalises them.
    qs = parse_qs(split.query, keep_blank_values=True)
    assert qs["ciks"] == ["0000320193"]
    assert qs["dateRange"] == ["custom"]
    assert qs["startdt"] == ["2024-12-18"]
    assert qs["enddt"] == ["2026-05-21"]
    assert qs["from"] == ["0"]
    assert qs["size"] == ["100"]
    assert qs["q"] == [""]
    # Forms is a CSV inside a single query value
    assert qs["forms"] == ["SC 13D,SC 13D/A,SC 13G,SC 13G/A"]

    # Belt-and-braces raw-substring check on the URL itself: the
    # encoded forms substring must match one of the two valid
    # urlencode-impl outputs (plus-encoded space OR %20 + %2C for
    # comma + %2F for slash). Either is a valid query encoding per
    # RFC 3986.
    encoded_forms_variants = (
        "SC+13D%2CSC+13D%2FA%2CSC+13G%2CSC+13G%2FA",
        "SC%2013D%2CSC%2013D%2FA%2CSC%2013G%2CSC%2013G%2FA",
    )
    assert any(variant in url for variant in encoded_forms_variants), f"forms encoding not found in URL: {url}"
    assert "ciks=0000320193" in url
    assert "dateRange=custom" in url
    assert "startdt=2024-12-18" in url
    assert "enddt=2026-05-21" in url
    assert "from=0" in url
    assert "size=100" in url


def test_pagination_offset_passes_through() -> None:
    """``from_offset`` flows into the URL as ``from=N``. Used by the
    discovery loop to walk past the SEC ``size=100`` page cap for
    outlier issuers (>100 filings in the 3y window)."""
    captured: list[str] = []

    def fake_get(url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
        captured.append(url)
        return _FakeResponse(200, {"hits": {"hits": [], "total": {"value": 0}}})

    provider = _make_provider()
    provider._http_tickers.get = fake_get  # type: ignore[method-assign]  # noqa: SLF001

    provider.fetch_search_index_json(
        ciks="0000320193",
        forms=("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"),
        startdt=date(2024, 12, 18),
        enddt=date(2026, 5, 21),
        from_offset=100,
        size=100,
    )

    assert len(captured) == 1
    qs = parse_qs(urlsplit(captured[0]).query, keep_blank_values=True)
    assert qs["from"] == ["100"]
    assert "from=100" in captured[0]


def test_404_returns_none() -> None:
    """SEC returns 404 when a CIK has zero matching filings in the
    window — the method MUST return ``None`` so the caller can treat
    it as an empty result rather than an error (mirrors
    :func:`fetch_filing_index`'s 404 contract)."""

    def fake_get(url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
        return _FakeResponse(404)

    provider = _make_provider()
    provider._http_tickers.get = fake_get  # type: ignore[method-assign]  # noqa: SLF001

    result = provider.fetch_search_index_json(
        ciks="0000320193",
        forms=("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"),
        startdt=date(2024, 12, 18),
        enddt=date(2026, 5, 21),
    )
    assert result is None
