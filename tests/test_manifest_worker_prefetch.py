"""Tests for the #1686 Phase 2 concurrent body prefetch.

Pure-logic (no DB): the fetch-chokepoint cache read, the str-only prefetch
filter + hooked-source selection, and the no-leak contextvar lifecycle.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from app.jobs.sec_manifest_worker import _prefetch_bodies, clear_registered_parsers, register_parser
from app.providers.implementations.sec_edgar import (
    _PREFETCH_BODY_CACHE,
    SecFilingsProvider,
    reset_prefetch_body_cache,
    set_prefetch_body_cache,
)
from app.services.sec_manifest import ManifestRow


@pytest.fixture(autouse=True)
def _isolate_registry() -> Any:
    """Clear the module-global parser registry before each test (clean slate
    for fake registrations) and RESTORE the real registry after, so a test
    that registers fakes can't leak an empty/partial registry into another
    xdist-colocated test (e.g. the runbooks gate that asserts the registry
    is populated)."""
    clear_registered_parsers()
    yield
    clear_registered_parsers()
    from app.services.manifest_parsers import register_all_parsers

    register_all_parsers()


def _row(*, source: str, url: str | None, accession: str) -> ManifestRow:
    return ManifestRow(
        accession_number=accession,
        cik="0000000001",
        form="4",
        source=source,  # type: ignore[arg-type]
        subject_type="issuer",
        subject_id="1",
        instrument_id=1,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
        accepted_at=None,
        primary_document_url=url,
        is_amendment=False,
        amends_accession=None,
        ingest_status="pending",
        parser_version=None,
        raw_status="absent",
        last_attempted_at=None,
        next_retry_at=None,
        error=None,
    )


# --- fetch chokepoint cache read ------------------------------------------


def test_fetch_document_text_returns_cached_body_without_http(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = SecFilingsProvider(user_agent="test/1.0")

    # If the cache is consulted, the HTTP client is never touched — make
    # any .get() call blow up so a cache MISS would be obvious.
    def _boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("HTTP fetch must not run on a cache hit")

    monkeypatch.setattr(provider._http_tickers, "get", _boom)

    token = set_prefetch_body_cache({"https://sec.gov/doc.xml": "<xml>body</xml>"})
    try:
        assert provider.fetch_document_text("https://sec.gov/doc.xml") == "<xml>body</xml>"
    finally:
        reset_prefetch_body_cache(token)


def test_fetch_document_text_cache_miss_falls_through_to_http(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = SecFilingsProvider(user_agent="test/1.0")

    class _Resp:
        status_code = 200
        text = "live-body"

        def raise_for_status(self) -> None:  # noqa: D401
            return None

    monkeypatch.setattr(provider._http_tickers, "get", lambda _u: _Resp())

    # cache present but does NOT contain this URL → miss → live fetch
    token = set_prefetch_body_cache({"https://sec.gov/other.xml": "x"})
    try:
        assert provider.fetch_document_text("https://sec.gov/doc.xml") == "live-body"
    finally:
        reset_prefetch_body_cache(token)


def test_fetch_document_text_no_active_cache_uses_http(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = SecFilingsProvider(user_agent="test/1.0")

    class _Resp:
        status_code = 200
        text = "live-body"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(provider._http_tickers, "get", lambda _u: _Resp())
    assert _PREFETCH_BODY_CACHE.get() is None  # no tick scope active
    assert provider.fetch_document_text("https://sec.gov/doc.xml") == "live-body"


# --- _prefetch_bodies -----------------------------------------------------


def test_prefetch_keeps_successful_bodies_only_and_only_hooked_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    # Hooked source: fetch_url returns the row's URL verbatim.
    register_parser("sec_form4", lambda c, r: None, fetch_url=lambda r: r.primary_document_url)  # type: ignore[arg-type,return-value]
    # Unhooked source: no fetch_url -> never prefetched.
    register_parser("sec_10q", lambda c, r: None)  # type: ignore[arg-type,return-value]

    captured: dict[str, list[str]] = {}

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        captured["urls"] = sorted(urls)
        # url_ok -> success body; url_bad -> None (404 / transient, must drop)
        return {"https://sec.gov/ok.xml": "BODY", "https://sec.gov/bad.xml": None}

    class _DummyProvider:
        def __init__(self, **_k: Any) -> None: ...
        def __enter__(self) -> _DummyProvider:
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _DummyProvider)

    rows = [
        _row(source="sec_form4", url="https://sec.gov/ok.xml", accession="0000000001-26-000001"),
        _row(source="sec_form4", url="https://sec.gov/bad.xml", accession="0000000001-26-000002"),
        _row(source="sec_10q", url="https://sec.gov/skip.htm", accession="0000000001-26-000003"),  # unhooked
        _row(source="sec_form4", url=None, accession="0000000001-26-000004"),  # no URL
    ]
    cache = _prefetch_bodies(rows)

    # Only hooked sources with a URL are prefetched (10q + the None-URL row excluded).
    assert captured["urls"] == ["https://sec.gov/bad.xml", "https://sec.gov/ok.xml"]
    # str bodies only — the None (transient/404) is dropped so the parser re-fetches serially.
    assert cache == {"https://sec.gov/ok.xml": "BODY"}


def test_prefetch_empty_when_no_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    register_parser("sec_10q", lambda c, r: None)  # type: ignore[arg-type,return-value]

    def _should_not_run(*_a: Any, **_k: Any) -> dict[str, str | None]:
        raise AssertionError("no hooked rows -> must not fetch")

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _should_not_run)
    rows = [_row(source="sec_10q", url="https://sec.gov/x.htm", accession="0000000001-26-000001")]
    assert _prefetch_bodies(rows) == {}


# --- _insider_fetch_url mirrors the parser's pre-fetch gates ---------------


def test_insider_fetch_url_mirrors_prefetch_gates() -> None:
    """The hook must NOT return a URL for a row the parser would tombstone
    before fetching — else the prefetch wastes SEC budget (Codex ckpt-2 HIGH)."""
    from datetime import timedelta

    from app.services.manifest_parsers.insider_345 import _insider_fetch_url

    recent = datetime.now(tz=UTC) - timedelta(days=30)
    old = datetime(2015, 1, 1, tzinfo=UTC)  # past both the 3y + 18mo caps
    base = "https://www.sec.gov/Archives/edgar/data/1/000/primary_doc.xml"

    # In-retention Form 4 with a URL + instrument_id → prefetch (canonical, XSL-free).
    assert _insider_fetch_url(_mk(source="sec_form4", url=base, filed_at=recent, iid=1)) is not None
    # Missing URL / instrument_id / filed_at → None (parser tombstones pre-fetch).
    assert _insider_fetch_url(_mk(source="sec_form4", url=None, filed_at=recent, iid=1)) is None
    assert _insider_fetch_url(_mk(source="sec_form4", url=base, filed_at=recent, iid=None)) is None
    assert _insider_fetch_url(_mk(source="sec_form4", url=base, filed_at=None, iid=1)) is None
    # Past-cap Form 4 / Form 5 → None (retention pre-fetch tombstone).
    assert _insider_fetch_url(_mk(source="sec_form4", url=base, filed_at=old, iid=1)) is None
    assert _insider_fetch_url(_mk(source="sec_form5", url=base, filed_at=old, iid=1)) is None
    # Form 3 has NO retention gate — an old Form 3 with a URL is still fetched.
    assert _insider_fetch_url(_mk(source="sec_form3", url=base, filed_at=old, iid=1)) is not None


def _mk(*, source: str, url: str | None, filed_at: Any, iid: int | None) -> ManifestRow:
    r = _row(source=source, url=url, accession="0000000001-26-000001")
    # _row hard-codes filed_at/instrument_id; rebuild via dataclasses.replace.
    import dataclasses

    return dataclasses.replace(r, filed_at=filed_at, instrument_id=iid)
