"""
Tests for ResilientClient (#168).

Scope:
  - Throttle enforcement (min_request_interval_s)
  - Retry on 429 with backoff
  - Retry on 429 with Retry-After header
  - Retry on 5xx (500, 502, 503, 504)
  - Max retries exceeded → raises HTTPStatusError
  - Non-retryable errors pass through immediately
  - GET and POST both throttled and retried
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.providers.resilient_client import ResilientClient


def _make_response(status_code: int, headers: dict[str, str] | None = None) -> httpx.Response:
    """Build a minimal httpx.Response for testing."""
    resp = httpx.Response(
        status_code=status_code,
        headers=headers or {},
        request=httpx.Request("GET", "https://example.com/test"),
    )
    return resp


def _make_client(
    responses: list[httpx.Response],
    *,
    min_interval: float = 0.0,
    max_retries: int = 3,
) -> tuple[ResilientClient, MagicMock]:
    """Build a ResilientClient with a mocked httpx.Client that returns
    the given responses in order."""
    mock_httpx = MagicMock(spec=httpx.Client)

    # build_request just returns a sentinel — we don't inspect it.
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = responses

    client = ResilientClient(
        mock_httpx,
        min_request_interval_s=min_interval,
        max_retries=max_retries,
        backoff_schedule=(0.01, 0.02, 0.04),  # fast for tests
    )
    return client, mock_httpx


# ---------------------------------------------------------------------------
# Throttle
# ---------------------------------------------------------------------------


@patch("app.providers.resilient_client.time")
def test_throttle_sleeps_when_requests_are_too_fast(mock_time: MagicMock) -> None:
    """Requests closer than min_request_interval_s trigger a sleep."""
    mock_time.monotonic.side_effect = [
        # First call: _throttle check → no prior request
        0.0,
        # First call: record _last_request_at
        0.0,
        # Second call: _throttle check → only 0.1s elapsed (need 1.0s)
        0.1,
        # Second call: record _last_request_at
        1.0,
    ]
    mock_time.sleep = MagicMock()

    resp = _make_response(200)
    client, mock_httpx = _make_client([resp, resp], min_interval=1.0)

    # Patch time on the module
    with patch("app.providers.resilient_client.time", mock_time):
        client._last_request_at = 0.0
        client._min_interval = 1.0

        # We need a fresh client with the patched time
        client2 = ResilientClient(
            mock_httpx,
            min_request_interval_s=1.0,
        )
        # Override time refs
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            client2.get("/test1")
            client2.get("/test2")
        finally:
            mod.time = original_time

    # Sleep should have been called to fill the gap
    mock_time.sleep.assert_called()
    sleep_arg = mock_time.sleep.call_args_list[-1][0][0]
    assert sleep_arg > 0


def test_no_throttle_when_interval_is_zero() -> None:
    """With min_request_interval_s=0, no sleeping occurs."""
    resp = _make_response(200)
    client, mock_httpx = _make_client([resp], min_interval=0.0)

    result = client.get("/test")
    assert result.status_code == 200
    assert mock_httpx.send.call_count == 1


# ---------------------------------------------------------------------------
# Retry on 429
# ---------------------------------------------------------------------------


def test_retry_on_429_then_success() -> None:
    """A 429 followed by 200 returns the 200 response."""
    r429 = _make_response(429)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r429, r200])

    with patch("app.providers.resilient_client.time"):
        result = client.get("/test")

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 2


def test_retry_on_429_respects_retry_after_header() -> None:
    """Retry-After header value is used as sleep duration."""
    r429 = _make_response(429, headers={"retry-after": "5"})
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r429, r200])

    with patch("app.providers.resilient_client.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        mock_time.sleep = MagicMock()
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            result = client.get("/test")
        finally:
            mod.time = original_time

    assert result.status_code == 200
    # The retry sleep (not the throttle sleep) should use 5.0
    retry_sleeps = [
        call[0][0]
        for call in mock_time.sleep.call_args_list
        if call[0][0] >= 1.0  # filter out throttle micro-sleeps
    ]
    assert any(s == 5.0 for s in retry_sleeps)


def test_max_retries_exceeded_raises() -> None:
    """After max_retries 429s, the final 429 raises HTTPStatusError."""
    responses = [_make_response(429)] * 4  # 1 initial + 3 retries
    client, _ = _make_client(responses, max_retries=3)

    with patch("app.providers.resilient_client.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        mock_time.sleep = MagicMock()
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                client.get("/test")
        finally:
            mod.time = original_time

    assert exc_info.value.response.status_code == 429


# ---------------------------------------------------------------------------
# Retry on 5xx
# ---------------------------------------------------------------------------


def test_retry_on_500_then_success() -> None:
    """A 500 followed by 200 returns the 200 response."""
    r500 = _make_response(500)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r500, r200])

    with patch("app.providers.resilient_client.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        mock_time.sleep = MagicMock()
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            result = client.get("/test")
        finally:
            mod.time = original_time

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 2


def test_retry_on_502() -> None:
    """502 is retryable."""
    r502 = _make_response(502)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r502, r200])

    with patch("app.providers.resilient_client.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        mock_time.sleep = MagicMock()
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            result = client.get("/test")
        finally:
            mod.time = original_time

    assert result.status_code == 200


def test_retry_on_503_and_504() -> None:
    """503 then 504 then 200 all retried successfully."""
    r503 = _make_response(503)
    r504 = _make_response(504)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r503, r504, r200])

    with patch("app.providers.resilient_client.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        mock_time.sleep = MagicMock()
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            result = client.get("/test")
        finally:
            mod.time = original_time

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 3


# ---------------------------------------------------------------------------
# Non-retryable errors
# ---------------------------------------------------------------------------


def test_non_retryable_error_passes_through() -> None:
    """A 403 is not retryable — returned immediately."""
    r403 = _make_response(403)
    client, mock_httpx = _make_client([r403])

    result = client.get("/test")
    assert result.status_code == 403
    assert mock_httpx.send.call_count == 1


def test_404_passes_through() -> None:
    """A 404 is not retryable — returned immediately."""
    r404 = _make_response(404)
    client, mock_httpx = _make_client([r404])

    result = client.get("/test")
    assert result.status_code == 404
    assert mock_httpx.send.call_count == 1


# ---------------------------------------------------------------------------
# POST requests
# ---------------------------------------------------------------------------


def test_post_is_throttled_and_retried() -> None:
    """POST requests go through the same throttle + retry path."""
    r429 = _make_response(429)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r429, r200])

    with patch("app.providers.resilient_client.time") as mock_time:
        mock_time.monotonic.return_value = 100.0
        mock_time.sleep = MagicMock()
        import app.providers.resilient_client as mod

        original_time = mod.time
        mod.time = mock_time
        try:
            result = client.post("/test", json={"key": "value"})
        finally:
            mod.time = original_time

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 2
    # Verify it was built as a POST
    mock_httpx.build_request.assert_called()
    assert mock_httpx.build_request.call_args_list[0][0][0] == "POST"
