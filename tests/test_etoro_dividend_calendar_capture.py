"""#3740 slice F — the fetch keeps the response as served, minus cookies, and follows no redirect."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from app.services import etoro_dividend_calendar_capture as capture


def test_fetch_keeps_status_headers_and_body_and_drops_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> httpx.Response:
        seen.update(kwargs, url=url)
        return httpx.Response(
            301,
            headers=[("Location", "/en-us/investing/dividend-calendar/"), ("Set-Cookie", "__cf_bm=x"), ("X-A", "1")],
            content=b"moved",
        )

    monkeypatch.setattr(capture.httpx, "get", fake_get)
    clock = iter([datetime(2026, 10, 12, 12, 40, tzinfo=UTC), datetime(2026, 10, 12, 12, 40, 1, tzinfo=UTC)])
    response = capture.fetch_dividend_calendar(now=lambda: next(clock))

    assert seen["url"] == capture.URL
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
