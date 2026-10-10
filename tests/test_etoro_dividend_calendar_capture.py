"""#3740 slice F — the fetch keeps the response as served, minus cookies, follows no redirect, and is bounded."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from app.services import etoro_dividend_calendar_capture as capture


def _fake_stream(response: httpx.Response, seen: dict[str, Any]) -> Any:
    @contextmanager
    def stream(method: str, url: str, **kwargs: Any) -> Iterator[httpx.Response]:
        seen.update(kwargs, method=method, url=url)
        yield response

    return stream


def test_fetch_keeps_status_headers_and_body_and_drops_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}
    served = httpx.Response(
        301,
        headers=[("Location", "/en-us/investing/dividend-calendar/"), ("Set-Cookie", "__cf_bm=x"), ("X-A", "1")],
        content=b"moved",
    )
    monkeypatch.setattr(capture.httpx, "stream", _fake_stream(served, seen))
    clock = iter([datetime(2026, 10, 12, 12, 40, tzinfo=UTC), datetime(2026, 10, 12, 12, 40, 1, tzinfo=UTC)])
    response = capture.fetch_dividend_calendar(now=lambda: next(clock))

    assert (seen["method"], seen["url"]) == ("GET", capture.URL)
    assert seen["follow_redirects"] is False
    assert seen["headers"] == {"User-Agent": capture.USER_AGENT}
    assert response.status == 301
    assert response.body == b"moved"
    assert response.headers == (
        ("location", "/en-us/investing/dividend-calendar/"),
        ("x-a", "1"),
        ("content-length", "5"),
    )
    assert response.observed_at - response.requested_at == timedelta(seconds=1)


def test_fetch_refuses_a_body_over_the_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(capture, "MAX_PAYLOAD_BYTES", 4)
    monkeypatch.setattr(capture.httpx, "stream", _fake_stream(httpx.Response(200, content=b"12345"), {}))
    with pytest.raises(capture.DividendCalendarCaptureError, match="HTTP 200 body exceeds 4 bytes; not stored"):
        capture.fetch_dividend_calendar()
