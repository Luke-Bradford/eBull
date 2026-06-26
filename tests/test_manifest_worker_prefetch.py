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


class _FakeTx:
    """Minimal context manager standing in for ``conn.transaction()`` — the
    #1700 prefetch wraps each ``fetch_url`` hook in a savepoint. Propagates
    any exception (returns False from __exit__) so the worker's except still
    fires, mirroring a real savepoint rollback-then-reraise."""

    def __enter__(self) -> _FakeTx:
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False


class _FakeConn:
    def transaction(self) -> _FakeTx:
        return _FakeTx()


_FAKE_CONN: Any = _FakeConn()


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


# Accession `_mk` stamps on every gate-test row (via `_row`); the stored-body
# stub keys on it so a hook querying the WRONG accession is caught.
_MK_ACCESSION = "0000000001-26-000001"


def _fake_stored_body(*present_pairs: tuple[str, str]) -> Any:
    """A ``stored_body`` stand-in for the pure hook tests: returns a body for
    the given ``(accession_number, document_kind)`` pairs (simulating a re-drain
    where that exact filing's payload is on disk), ``None`` otherwise. Keying on
    BOTH accession and kind means a hook that queries the wrong accession (e.g. a
    hardcoded literal instead of ``row.accession_number``) or the wrong kind is
    caught — the stub returns None and the hook fails to skip (Claude review
    NITPICK on #1727). Lets the #1591 reuse gate be exercised without a DB."""

    def _f(_conn: Any, *, accession_number: str, document_kind: str) -> str | None:
        return "<stored/>" if (accession_number, document_kind) in present_pairs else None

    return _f


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
    register_parser("sec_form4", lambda c, r: None, fetch_url=lambda c, r: r.primary_document_url)  # type: ignore[arg-type,return-value]
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
    cache = _prefetch_bodies(_FAKE_CONN, rows)

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
    assert _prefetch_bodies(_FAKE_CONN, rows) == {}


# --- _insider_fetch_url mirrors the parser's pre-fetch gates ---------------


def test_insider_fetch_url_mirrors_prefetch_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    """The hook must NOT return a URL for a row the parser would tombstone
    before fetching — else the prefetch wastes SEC budget (Codex ckpt-2 HIGH)."""
    from datetime import timedelta

    import app.services.manifest_parsers.insider_345 as ins
    from app.services.manifest_parsers.insider_345 import _insider_fetch_url

    recent = datetime.now(tz=UTC) - timedelta(days=30)
    old = datetime(2015, 1, 1, tzinfo=UTC)  # past both the 3y + 18mo caps
    base = "https://www.sec.gov/Archives/edgar/data/1/000/primary_doc.xml"

    # Default: nothing stored, so the gate asserts below exercise the
    # row-local gates only (the #1591 stored-body gate is a no-op here).
    monkeypatch.setattr(ins, "stored_body", _fake_stored_body())

    # In-retention Form 4 with a URL + instrument_id → prefetch (canonical, XSL-free).
    assert _insider_fetch_url(None, _mk(source="sec_form4", url=base, filed_at=recent, iid=1)) is not None
    # Missing URL / instrument_id / filed_at → None (parser tombstones pre-fetch).
    assert _insider_fetch_url(None, _mk(source="sec_form4", url=None, filed_at=recent, iid=1)) is None
    assert _insider_fetch_url(None, _mk(source="sec_form4", url=base, filed_at=recent, iid=None)) is None
    assert _insider_fetch_url(None, _mk(source="sec_form4", url=base, filed_at=None, iid=1)) is None
    # Past-cap Form 4 / Form 5 → None (retention pre-fetch tombstone).
    assert _insider_fetch_url(None, _mk(source="sec_form4", url=base, filed_at=old, iid=1)) is None
    assert _insider_fetch_url(None, _mk(source="sec_form5", url=base, filed_at=old, iid=1)) is None
    # Form 3 has NO retention gate — an old Form 3 with a URL is still fetched.
    assert _insider_fetch_url(None, _mk(source="sec_form3", url=base, filed_at=old, iid=1)) is not None

    # #1591 — body already stored → skip prefetch (parser reuses it from the DB).
    monkeypatch.setattr(ins, "stored_body", _fake_stored_body((_MK_ACCESSION, "form4_xml")))
    assert _insider_fetch_url(None, _mk(source="sec_form4", url=base, filed_at=recent, iid=1)) is None
    # Fail-closed map (Codex ckpt-1 #4): Form 5 is NOT reused by _parse_form5,
    # so its source is absent from the reuse map — a stored form5_xml body must
    # NOT skip the prefetch (the parser will fetch it).
    monkeypatch.setattr(ins, "stored_body", _fake_stored_body((_MK_ACCESSION, "form5_xml")))
    assert _insider_fetch_url(None, _mk(source="sec_form5", url=base, filed_at=recent, iid=1)) is not None


def _mk(*, source: str, url: str | None, filed_at: Any, iid: int | None) -> ManifestRow:
    r = _row(source=source, url=url, accession="0000000001-26-000001")
    # _row hard-codes filed_at/instrument_id; rebuild via dataclasses.replace.
    import dataclasses

    return dataclasses.replace(r, filed_at=filed_at, instrument_id=iid)


# --- #1700 two-pass prefetch (multi-doc expander) -------------------------


def test_prefetch_two_pass_expands_only_on_pass1_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pass 2 prefetches the expander URLs ONLY for rows whose pass-1 URL was a
    successful fetch; a pass-1 miss skips that row's expansion (#1700)."""
    register_parser(
        "sec_13f_hr",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        fetch_url=lambda c, r: r.primary_document_url,
        expand_urls=lambda body, r: [f"https://sec.gov/{r.accession_number}/primary.xml"],
    )

    calls: list[list[str]] = []

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        batch = sorted(urls)
        calls.append(batch)
        # Pass 1: idx-hit.json succeeds, idx-miss.json is a None (transient/404).
        if "https://sec.gov/idx-hit.json" in batch or "https://sec.gov/idx-miss.json" in batch:
            return {"https://sec.gov/idx-hit.json": "INDEX", "https://sec.gov/idx-miss.json": None}
        # Pass 2: the expanded primary for the hit row only.
        return {u: "PRIMARY" for u in batch}

    class _DummyProvider:
        def __init__(self, **_k: Any) -> None: ...
        def __enter__(self) -> _DummyProvider:
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _DummyProvider)

    rows = [
        _row(source="sec_13f_hr", url="https://sec.gov/idx-hit.json", accession="HIT"),
        _row(source="sec_13f_hr", url="https://sec.gov/idx-miss.json", accession="MISS"),
    ]
    cache = _prefetch_bodies(_FAKE_CONN, rows)

    # Two batches issued: pass 1 (both index URLs), pass 2 (HIT's primary only).
    assert len(calls) == 2
    assert calls[0] == ["https://sec.gov/idx-hit.json", "https://sec.gov/idx-miss.json"]
    assert calls[1] == ["https://sec.gov/HIT/primary.xml"]  # MISS row not expanded
    assert cache == {"https://sec.gov/idx-hit.json": "INDEX", "https://sec.gov/HIT/primary.xml": "PRIMARY"}


def test_prefetch_fetch_url_raise_skips_row_not_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fetch_url hook raising (e.g. DEF 14A cap DB error) must NOT abort the
    prefetch — the row is skipped and OTHER rows still prefetch (Codex ckpt-2 P2)."""

    def _hook(c: Any, r: Any) -> str | None:
        if r.accession_number == "BOOM":
            raise RuntimeError("cap query blew up")
        return r.primary_document_url

    register_parser("sec_def14a", lambda c, r: None, fetch_url=_hook)  # type: ignore[arg-type,return-value]

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        return {u: "BODY" for u in urls}

    class _DummyProvider:
        def __init__(self, **_k: Any) -> None: ...
        def __enter__(self) -> _DummyProvider:
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _DummyProvider)

    rows = [
        _row(source="sec_def14a", url="https://sec.gov/boom.htm", accession="BOOM"),
        _row(source="sec_def14a", url="https://sec.gov/ok.htm", accession="OK"),
    ]
    cache = _prefetch_bodies(_FAKE_CONN, rows)
    # BOOM skipped (hook raised); OK still prefetched — tick survives.
    assert cache == {"https://sec.gov/ok.htm": "BODY"}


def test_prefetch_expand_raise_skips_pass2_not_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """An expand_urls hook raising on a malformed pass-1 body must NOT abort the
    prefetch — pass-2 is skipped for that row; the pass-1 body stays cached so
    the serial parser re-parses it (Codex ckpt-2 P2)."""

    def _boom_expand(body: str, r: Any) -> list[str]:
        raise ValueError("malformed index.json")

    register_parser(
        "sec_13f_hr",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        fetch_url=lambda c, r: r.primary_document_url,
        expand_urls=_boom_expand,
    )

    calls: list[list[str]] = []

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        calls.append(sorted(urls))
        return {u: "INDEX" for u in urls}

    class _DummyProvider:
        def __init__(self, **_k: Any) -> None: ...
        def __enter__(self) -> _DummyProvider:
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _DummyProvider)

    rows = [_row(source="sec_13f_hr", url="https://sec.gov/idx.json", accession="A")]
    cache = _prefetch_bodies(_FAKE_CONN, rows)
    assert len(calls) == 1  # only pass 1 issued (expander raised → no pass-2 batch)
    assert cache == {"https://sec.gov/idx.json": "INDEX"}  # pass-1 body kept


def test_prefetch_no_pass2_when_no_expander(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hooked source WITHOUT an expander prefetches pass 1 only (#1700)."""
    register_parser(
        "sec_form4",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        fetch_url=lambda c, r: r.primary_document_url,
    )

    calls: list[list[str]] = []

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        calls.append(sorted(urls))
        return {"https://sec.gov/ok.xml": "BODY"}

    class _DummyProvider:
        def __init__(self, **_k: Any) -> None: ...
        def __enter__(self) -> _DummyProvider:
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _DummyProvider)

    rows = [_row(source="sec_form4", url="https://sec.gov/ok.xml", accession="A")]
    cache = _prefetch_bodies(_FAKE_CONN, rows)
    assert len(calls) == 1  # no second batch
    assert cache == {"https://sec.gov/ok.xml": "BODY"}


# --- #1700 new per-source hooks mirror their parser pre-fetch gates --------


def test_blockholder_fetch_url_mirrors_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.manifest_parsers.sec_13dg as d13dg
    from app.services.manifest_parsers.sec_13dg import _blockholder_fetch_url

    recent = datetime.now(tz=UTC)  # within blockholders retention (post-mandate)
    old = datetime(2010, 1, 1, tzinfo=UTC)  # pre-cutoff → tombstone pre-fetch

    monkeypatch.setattr(d13dg, "stored_body", _fake_stored_body())  # nothing stored

    ok = _mk(source="sec_13d", url=None, filed_at=recent, iid=1)
    assert _blockholder_fetch_url(None, ok) is not None  # cik present, not agent, in retention
    assert _blockholder_fetch_url(None, ok).endswith("primary_doc.xml")  # type: ignore[union-attr]
    # Retention floor → None.
    assert _blockholder_fetch_url(None, _mk(source="sec_13d", url=None, filed_at=old, iid=1)) is None
    # Missing cik → None.
    import dataclasses

    no_cik = dataclasses.replace(ok, cik="")
    assert _blockholder_fetch_url(None, no_cik) is None
    # #1591 — body already stored → skip prefetch (parser reuses it).
    monkeypatch.setattr(d13dg, "stored_body", _fake_stored_body((_MK_ACCESSION, "primary_doc_13dg")))
    assert _blockholder_fetch_url(None, ok) is None
    monkeypatch.setattr(d13dg, "stored_body", _fake_stored_body())  # reset: nothing stored
    # Agent CIK → None.
    monkeypatch.setattr("app.providers.implementations.sec_edgar.KNOWN_FILING_AGENT_CIKS", {"0000000001"})
    assert _blockholder_fetch_url(None, ok) is None


def test_def14a_fetch_url_mirrors_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.manifest_parsers.def14a as d14a
    from app.services.manifest_parsers.def14a import _def14a_fetch_url

    monkeypatch.setattr(d14a, "def14a_within_cap", lambda *a, **k: True)
    monkeypatch.setattr(d14a, "stored_body", _fake_stored_body())  # nothing stored
    base = "https://www.sec.gov/Archives/edgar/data/1/000/proxy.htm"
    recent = datetime.now(tz=UTC)
    ok = _mk(source="sec_def14a", url=base, filed_at=recent, iid=1)
    assert _def14a_fetch_url(None, ok) == base  # type: ignore[arg-type]
    # Missing url / instrument_id → None.
    assert _def14a_fetch_url(None, _mk(source="sec_def14a", url=None, filed_at=recent, iid=1)) is None  # type: ignore[arg-type]
    assert _def14a_fetch_url(None, _mk(source="sec_def14a", url=base, filed_at=recent, iid=None)) is None  # type: ignore[arg-type]
    # PRE 14A preliminary → None.
    import dataclasses

    pre = dataclasses.replace(ok, form="PRE 14A")
    assert _def14a_fetch_url(None, pre) is None  # type: ignore[arg-type]
    # Past the latest-N cap → None (gate needs conn; helper stubbed False).
    monkeypatch.setattr(d14a, "def14a_within_cap", lambda *a, **k: False)
    assert _def14a_fetch_url(None, ok) is None  # type: ignore[arg-type]
    # #1591 — body already stored → skip prefetch (parser reuses it). Cap reset
    # to True so the None below is attributable to the stored-body gate alone.
    monkeypatch.setattr(d14a, "def14a_within_cap", lambda *a, **k: True)
    monkeypatch.setattr(d14a, "stored_body", _fake_stored_body((_MK_ACCESSION, "def14a_body")))
    assert _def14a_fetch_url(None, ok) is None  # type: ignore[arg-type]


def test_thirteen_f_index_url_and_expander(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services.manifest_parsers.sec_13f_hr import _thirteen_f_expand, _thirteen_f_index_url

    ok = _mk(source="sec_13f_hr", url=None, filed_at=datetime.now(tz=UTC), iid=1)
    url = _thirteen_f_index_url(None, ok)
    assert url is not None and url.endswith("index.json")
    # Missing cik → None.
    import dataclasses

    assert _thirteen_f_index_url(None, dataclasses.replace(ok, cik="")) is None
    # Agent CIK → None.
    monkeypatch.setattr("app.providers.implementations.sec_edgar.KNOWN_FILING_AGENT_CIKS", {"0000000001"})
    assert _thirteen_f_index_url(None, ok) is None

    # Expander returns primary_doc.xml ONLY (NOT infotable) on a valid index.
    monkeypatch.setattr(
        "app.services.manifest_parsers.sec_13f_hr.parse_archive_index",
        lambda _body: ("primary_doc.xml", "infotable.xml"),
    )
    expanded = _thirteen_f_expand("<index/>", ok)
    assert len(expanded) == 1
    assert expanded[0].endswith("primary_doc.xml")
    assert not any("infotable" in u for u in expanded)  # infotable stays serial (retention gate)
    # Unresolvable primary name → [] (no prefetch wasted).
    monkeypatch.setattr(
        "app.services.manifest_parsers.sec_13f_hr.parse_archive_index",
        lambda _body: (None, None),
    )
    assert _thirteen_f_expand("<index/>", ok) == []
    # primary present but infotable MISSING → [] (parser tombstones before
    # the primary fetch when EITHER name is None — Codex ckpt-2 P2).
    monkeypatch.setattr(
        "app.services.manifest_parsers.sec_13f_hr.parse_archive_index",
        lambda _body: ("primary_doc.xml", None),
    )
    assert _thirteen_f_expand("<index/>", ok) == []


# --- #1591 Part 2 — 10-K / 8-K prefetch hooks + per-source rebuild routing ---


def test_sec10k_fetch_url_mirrors_gates() -> None:
    """`_sec10k_fetch_url` returns the primary URL only when `_parse_sec_10k`
    would fetch it — i.e. url + instrument_id present (no retention gate). The
    10-K parser GETs `row.primary_document_url` verbatim (no canonicalisation),
    so the hook returns it unchanged."""
    from app.services.manifest_parsers.sec_10k import _sec10k_fetch_url

    base = "https://www.sec.gov/Archives/edgar/data/1/000/10k.htm"
    recent = datetime(2026, 1, 1, tzinfo=UTC)
    assert _sec10k_fetch_url(None, _mk(source="sec_10k", url=base, filed_at=recent, iid=1)) == base
    assert _sec10k_fetch_url(None, _mk(source="sec_10k", url=None, filed_at=recent, iid=1)) is None
    assert _sec10k_fetch_url(None, _mk(source="sec_10k", url=base, filed_at=recent, iid=None)) is None


def test_eight_k_fetch_url_mirrors_gates() -> None:
    """`_eight_k_fetch_url` mirrors `_parse_eight_k`'s pre-fetch gates (url +
    instrument_id); single-doc, so the URL is the whole fetch."""
    from app.services.manifest_parsers.eight_k import _eight_k_fetch_url

    base = "https://www.sec.gov/Archives/edgar/data/1/000/8k.htm"
    recent = datetime(2026, 1, 1, tzinfo=UTC)
    assert _eight_k_fetch_url(None, _mk(source="sec_8k", url=base, filed_at=recent, iid=1)) == base
    assert _eight_k_fetch_url(None, _mk(source="sec_8k", url=None, filed_at=recent, iid=1)) is None
    assert _eight_k_fetch_url(None, _mk(source="sec_8k", url=base, filed_at=recent, iid=None)) is None


def test_per_source_rebuild_routes_through_prefetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1591 Part 2 — the per-source rebuild path (`source is not None`, what
    `sec_rebuild` drains into) now prefetches concurrently via
    `_prefetch_then_dispatch` instead of the old direct `_dispatch_rows` serial
    call. Pure-logic: iter_pending/iter_retryable + the prefetch entry point are
    monkeypatched so no DB is touched."""
    import app.jobs.sec_manifest_worker as w

    monkeypatch.setattr(w, "iter_pending", lambda conn, *, source, limit: [])
    monkeypatch.setattr(w, "iter_retryable", lambda conn, *, source, limit: [])
    routed = {"prefetch": False, "serial": False}

    def _spy_prefetch(conn: Any, rows: Any, *, now: Any) -> Any:
        routed["prefetch"] = True
        return w.WorkerStats(rows_processed=0, parsed=0, tombstoned=0, failed=0, skipped_no_parser=0)

    def _spy_serial(conn: Any, rows: Any, *, now: Any) -> Any:
        routed["serial"] = True
        return w.WorkerStats(rows_processed=0, parsed=0, tombstoned=0, failed=0, skipped_no_parser=0)

    monkeypatch.setattr(w, "_prefetch_then_dispatch", _spy_prefetch)
    monkeypatch.setattr(w, "_dispatch_rows", _spy_serial)

    w.run_manifest_worker(None, source="sec_10k", max_rows=10)  # type: ignore[arg-type]  # faked iter_* ignore conn

    assert routed["prefetch"] is True
    assert routed["serial"] is False  # not the old direct-serial path


# --- #1730 prefetch_chain (independent-doc-chain prefetch) ------------------


def _dummy_provider() -> Any:
    class _P:
        def __init__(self, **_k: Any) -> None: ...
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *_a: Any) -> None:
            return None

    return _P


def test_prefetch_chain_runs_and_merges(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — a source's ``prefetch_chain`` output is merged into the tick cache
    alongside the pass-1 body."""
    register_parser(
        "sec_10k",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        fetch_url=lambda c, r: r.primary_document_url,
        prefetch_chain=lambda rows, provider: {"https://sec.gov/xbrl/i.xml": "XBRL"},
    )

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        return {u: "BODY" for u in urls}

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _dummy_provider())

    rows = [_row(source="sec_10k", url="https://sec.gov/10k.htm", accession="A")]
    cache = _prefetch_bodies(_FAKE_CONN, rows)
    assert cache == {"https://sec.gov/10k.htm": "BODY", "https://sec.gov/xbrl/i.xml": "XBRL"}


def test_prefetch_chain_runs_even_with_no_fetch_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — a chain-only source (NO ``fetch_url``) must still run its chain;
    the ``pass1_url_to_rows`` empty early-return must not skip it."""
    register_parser(
        "sec_10k",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        prefetch_chain=lambda rows, provider: {"https://sec.gov/xbrl/i.xml": "XBRL"},
    )

    def _fail_texts(*_a: Any, **_k: Any) -> dict[str, str | None]:
        raise AssertionError("no fetch_url rows → worker must not run pass-1 fetch")

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fail_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _dummy_provider())

    rows = [_row(source="sec_10k", url=None, accession="A")]
    cache = _prefetch_bodies(_FAKE_CONN, rows)
    assert cache == {"https://sec.gov/xbrl/i.xml": "XBRL"}


def test_prefetch_chain_raise_skips_chain_not_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — a ``prefetch_chain`` raising must NOT abort the tick; the pass-1
    bodies from other sources survive (best-effort, mirrors the fetch_url/expand
    hook handling)."""

    def _boom_chain(rows: Any, provider: Any) -> dict[str, str]:
        raise RuntimeError("xbrl prefetch blew up")

    register_parser(
        "sec_form4",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        fetch_url=lambda c, r: r.primary_document_url,
    )
    register_parser(
        "sec_10k",
        lambda c, r: None,  # type: ignore[arg-type,return-value]
        prefetch_chain=_boom_chain,
    )

    def _fake_fetch_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        return {u: "BODY" for u in urls}

    monkeypatch.setattr("app.providers.concurrent_fetch.fetch_document_texts", _fake_fetch_texts)
    monkeypatch.setattr("app.providers.implementations.sec_edgar.SecFilingsProvider", _dummy_provider())

    rows = [
        _row(source="sec_form4", url="https://sec.gov/ok.xml", accession="A"),
        _row(source="sec_10k", url=None, accession="B"),
    ]
    cache = _prefetch_bodies(_FAKE_CONN, rows)
    assert cache == {"https://sec.gov/ok.xml": "BODY"}  # form4 survived; chain contributed nothing


# --- #1730 _sec10k_xbrl_prefetch + _xbrl_index_locator ----------------------

_TENK_URL = "https://www.sec.gov/Archives/edgar/data/1/000/10k.htm"
_TENK_ACCESSION = "0000000001-26-000001"


def _tenk_base() -> str:
    from app.providers.implementations.sec_edgar import archive_dir_url

    return archive_dir_url(_TENK_ACCESSION.replace("-", ""), 1)


def test_xbrl_index_locator_shares_urls_with_filing_index() -> None:
    """#1730 — the locator's index URL is built via the SAME ``archive_dir_url``
    builder ``fetch_filing_index`` routes through, so the prefetched index URL
    byte-matches the serial lookup. primary_name strips a ``.txt`` full-submission
    name; a non-numeric cik → None."""
    from app.services.manifest_parsers.sec_10k import _xbrl_index_locator

    loc = _xbrl_index_locator(accession=_TENK_ACCESSION, issuer_cik="0000000001", primary_document_url=_TENK_URL)
    assert loc is not None
    index_url, base, primary_name = loc
    assert base == _tenk_base()
    assert index_url == _tenk_base() + "index.json"
    assert primary_name == "10k.htm"
    # full-submission .txt → primary_name None (discovery falls back to size rules).
    txt = _xbrl_index_locator(
        accession=_TENK_ACCESSION,
        issuer_cik="0000000001",
        primary_document_url="https://www.sec.gov/Archives/edgar/data/1/0000000001-26-000001.txt",
    )
    assert txt is not None and txt[2] is None
    # non-numeric / missing cik → None (no usable archive path).
    assert _xbrl_index_locator(accession=_TENK_ACCESSION, issuer_cik=None, primary_document_url=_TENK_URL) is None
    assert _xbrl_index_locator(accession=_TENK_ACCESSION, issuer_cik="abc", primary_document_url=_TENK_URL) is None


def test_sec10k_xbrl_prefetch_fetches_index_then_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — two concurrent rounds: index.json, then the discovered
    instance/label/def. All bodies returned for the tick cache."""
    from app.services.dimensional_facts import XbrlFileRefs
    from app.services.manifest_parsers import sec_10k

    batches: list[list[str]] = []
    index_json = '{"directory": {"item": []}}'

    def _fake_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        batch = sorted(urls)
        batches.append(batch)
        return {u: (index_json if u.endswith("index.json") else "ARTIFACT") for u in batch}

    monkeypatch.setattr(sec_10k, "fetch_document_texts", _fake_texts)
    monkeypatch.setattr(
        sec_10k,
        "discover_xbrl_files",
        lambda raw_index, *, primary_document_name: XbrlFileRefs(
            instance_name="i.xml", label_name="l.xml", definition_name="d.xml"
        ),
    )

    row = _row(source="sec_10k", url=_TENK_URL, accession=_TENK_ACCESSION)
    cache = sec_10k._sec10k_xbrl_prefetch([row], object())

    base = _tenk_base()
    assert cache[base + "index.json"] == index_json
    assert cache[base + "i.xml"] == "ARTIFACT"
    assert cache[base + "l.xml"] == "ARTIFACT"
    assert cache[base + "d.xml"] == "ARTIFACT"
    # Round 1 = the index, round 2 = the three artifacts.
    assert len(batches) == 2
    assert batches[0] == [base + "index.json"]
    assert batches[1] == sorted(base + n for n in ("i.xml", "l.xml", "d.xml"))


def test_sec10k_xbrl_prefetch_dedups_def_equals_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — when discovery resolves def==label (xsd fallback serves both), the
    artifact set collapses to ONE fetch, matching the serial path's single-fetch."""
    from app.services.dimensional_facts import XbrlFileRefs
    from app.services.manifest_parsers import sec_10k

    batches: list[list[str]] = []

    def _fake_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        batches.append(sorted(urls))
        return {u: ('{"directory": {"item": []}}' if u.endswith("index.json") else "B") for u in urls}

    monkeypatch.setattr(sec_10k, "fetch_document_texts", _fake_texts)
    monkeypatch.setattr(
        sec_10k,
        "discover_xbrl_files",
        lambda raw_index, *, primary_document_name: XbrlFileRefs(
            instance_name="i.xml", label_name="shared.xsd", definition_name="shared.xsd"
        ),
    )
    cache = sec_10k._sec10k_xbrl_prefetch([_row(source="sec_10k", url=_TENK_URL, accession=_TENK_ACCESSION)], object())
    base = _tenk_base()
    assert batches[1] == sorted(base + n for n in ("i.xml", "shared.xsd"))  # one artifact, not two
    assert cache[base + "shared.xsd"] == "B"


def test_sec10k_xbrl_prefetch_skips_gated_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — rows the parser would skip before the XBRL step (missing url /
    instrument_id / filed_at / numeric cik) are not prefetched (gate mirror)."""
    import dataclasses

    from app.services.manifest_parsers import sec_10k

    def _fail(*_a: Any, **_k: Any) -> dict[str, str | None]:
        raise AssertionError("gated rows must not fetch")

    monkeypatch.setattr(sec_10k, "fetch_document_texts", _fail)

    good = _row(source="sec_10k", url=_TENK_URL, accession=_TENK_ACCESSION)
    rows = [
        dataclasses.replace(good, primary_document_url=None),
        dataclasses.replace(good, instrument_id=None),
        dataclasses.replace(good, filed_at=None),
        dataclasses.replace(good, cik=""),
    ]
    assert sec_10k._sec10k_xbrl_prefetch(rows, object()) == {}


def test_sec10k_xbrl_prefetch_404_index_drops_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    """#1730 — a 404 (None) index has no XBRL: no discovery, no artifact round."""
    from app.services.manifest_parsers import sec_10k

    def _fake_texts(_provider: Any, urls: Any, **_k: Any) -> dict[str, str | None]:
        return {u: None for u in urls}  # index 404

    monkeypatch.setattr(sec_10k, "fetch_document_texts", _fake_texts)

    def _no_discover(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("must not discover on a 404 index")

    monkeypatch.setattr(sec_10k, "discover_xbrl_files", _no_discover)
    cache = sec_10k._sec10k_xbrl_prefetch([_row(source="sec_10k", url=_TENK_URL, accession=_TENK_ACCESSION)], object())
    assert cache == {}
