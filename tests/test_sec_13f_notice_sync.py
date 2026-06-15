"""Capture-orchestration tests for ``sec_13f_notice_sync`` (#1639).

Isolated from the DB + network: ``read_daily_index`` (which has its own parser
tests) is monkeypatched to yield ``FilingIndexRow``s directly, ``http_get`` is a
fake URL→(status, body) map, and the connection is a fake that records the
upsert calls. This pins MY logic — form filtering, per-Notice primary_doc fetch,
parse, upsert, and the skip-on-failure paths — without standing up Postgres."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from app.providers.implementations.sec_submissions import FilingIndexRow
from app.services import sec_13f_notice_sync as mod

_UA = "eBull test"


def _row(*, cik: str, form: str, accession: str) -> FilingIndexRow:
    return FilingIndexRow(
        accession_number=accession,
        cik=cik,
        form=form,
        source=None,  # NT maps to None (deliberately out of the manifest set)
        filed_at=datetime(2026, 5, 8, tzinfo=UTC),
        accepted_at=None,
        primary_document_url=None,
        is_amendment=form.endswith("/A"),
    )


def _notice_xml(cik: str, period: str) -> bytes:
    ns = "http://www.sec.gov/edgar/thirteenffiler"
    return (
        f'<edgarSubmission xmlns="{ns}"><headerData><filerInfo><filer>'
        f"<credentials><cik>{cik}</cik></credentials></filer></filerInfo></headerData>"
        f"<formData><coverPage><periodOfReport>{period}</periodOfReport>"
        f"</coverPage></formData></edgarSubmission>"
    ).encode()


class _FakeCursor:
    def __init__(self, sink: list[tuple[str, dict]]) -> None:
        self._sink = sink

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: dict | None = None) -> None:
        self._sink.append((sql, params or {}))


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict]] = []

    def cursor(self, *args: object, **kwargs: object) -> _FakeCursor:
        return _FakeCursor(self.executed)


def _http_map(mapping: dict[str, tuple[int, bytes]]):
    def _get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        if url in mapping:
            return mapping[url]
        raise AssertionError(f"unexpected URL fetched: {url}")

    return _get


def _patch_index(monkeypatch: pytest.MonkeyPatch, rows_by_day: dict[date, list[FilingIndexRow]]) -> None:
    def _fake_read(http_get, when, *, user_agent=""):  # noqa: ANN001, ARG001
        yield from rows_by_day.get(when, [])

    monkeypatch.setattr(mod, "read_daily_index", _fake_read)


def test_filters_to_notice_forms_and_upserts(monkeypatch: pytest.MonkeyPatch) -> None:
    when = date(2026, 5, 8)
    rows = [
        _row(cik="0000102909", form="13F-NT", accession="0001029090-26-002707"),
        _row(cik="0000789019", form="10-K", accession="0000789019-26-000001"),  # ignored
        _row(cik="0000320193", form="13F-NT/A", accession="0000320193-26-000099"),
    ]
    _patch_index(monkeypatch, {when: rows})
    http = _http_map(
        {
            mod._notice_primary_doc_url("0000102909", "0001029090-26-002707"): (
                200,
                _notice_xml("0000102909", "03-31-2026"),
            ),
            mod._notice_primary_doc_url("0000320193", "0000320193-26-000099"): (
                200,
                _notice_xml("0000320193", "12-31-2025"),
            ),
        }
    )
    conn: Any = _FakeConn()

    result = mod.sync_13f_notices(conn, http, user_agent=_UA, since=when, until=when)

    assert result.notices_seen == 2  # the 10-K was filtered out
    assert result.upserted == 2
    assert result.fetch_failures == 0
    assert result.parse_failures == 0
    # Two upserts, carrying the parsed period + the index form.
    upserts = [p for _, p in conn.executed]
    assert {u["filer_cik"] for u in upserts} == {"0000102909", "0000320193"}
    nt_a = next(u for u in upserts if u["accession"] == "0000320193-26-000099")
    assert nt_a["form"] == "13F-NT/A"
    assert nt_a["period_end"] == date(2025, 12, 31)


def test_fetch_failure_skips_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    when = date(2026, 5, 8)
    _patch_index(
        monkeypatch,
        {when: [_row(cik="0000102909", form="13F-NT", accession="0001029090-26-002707")]},
    )
    http = _http_map({mod._notice_primary_doc_url("0000102909", "0001029090-26-002707"): (404, b"not found")})
    conn: Any = _FakeConn()

    result = mod.sync_13f_notices(conn, http, user_agent=_UA, since=when, until=when)

    assert result.notices_seen == 1
    assert result.upserted == 0
    assert result.fetch_failures == 1
    assert conn.executed == []  # nothing written on a fetch failure


def test_parse_failure_skips_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    when = date(2026, 5, 8)
    _patch_index(
        monkeypatch,
        {when: [_row(cik="0000102909", form="13F-NT", accession="0001029090-26-002707")]},
    )
    http = _http_map(
        {
            mod._notice_primary_doc_url("0000102909", "0001029090-26-002707"): (
                200,
                b"<edgarSubmission>no period here</edgarSubmission>",
            )
        }
    )
    conn: Any = _FakeConn()

    result = mod.sync_13f_notices(conn, http, user_agent=_UA, since=when, until=when)

    assert result.parse_failures == 1
    assert result.upserted == 0
    assert conn.executed == []


def test_window_scans_every_day(monkeypatch: pytest.MonkeyPatch) -> None:
    d1, d2, d3 = date(2026, 5, 6), date(2026, 5, 7), date(2026, 5, 8)
    _patch_index(
        monkeypatch,
        {d2: [_row(cik="0000102909", form="13F-NT", accession="0001029090-26-002707")]},
    )
    http = _http_map(
        {
            mod._notice_primary_doc_url("0000102909", "0001029090-26-002707"): (
                200,
                _notice_xml("0000102909", "03-31-2026"),
            )
        }
    )
    conn: Any = _FakeConn()

    result = mod.sync_13f_notices(conn, http, user_agent=_UA, since=d1, until=d3)

    assert result.days_scanned == 3  # all three days walked
    assert result.notices_seen == 1  # only d2 had a notice
    assert result.upserted == 1


def test_since_after_until_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_index(monkeypatch, {})
    fake: Any = _FakeConn()
    with pytest.raises(ValueError, match="after until"):
        mod.sync_13f_notices(fake, _http_map({}), user_agent=_UA, since=date(2026, 5, 9), until=date(2026, 5, 8))


def test_notice_primary_doc_url_uses_int_cik_and_nodash_accession() -> None:
    url = mod._notice_primary_doc_url("0000102909", "0001029090-26-002707")
    assert url == "https://www.sec.gov/Archives/edgar/data/102909/000102909026002707/primary_doc.xml"
