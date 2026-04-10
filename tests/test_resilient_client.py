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
  - Shared throttle state between instances
"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock

import httpx
import pytest

import app.providers.resilient_client as _mod
from app.providers.resilient_client import ResilientClient


def _make_response(status_code: int, headers: dict[str, str] | None = None) -> httpx.Response:
    """Build a minimal httpx.Response for testing."""
    return httpx.Response(
        status_code=status_code,
        headers=headers or {},
        request=httpx.Request("GET", "https://example.com/test"),
    )


def _make_client(
    responses: list[httpx.Response],
    *,
    min_interval: float = 0.0,
    max_retries: int = 3,
) -> tuple[ResilientClient, MagicMock]:
    """Build a ResilientClient with a mocked httpx.Client that returns
    the given responses in order."""
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = responses

    client = ResilientClient(
        mock_httpx,
        min_request_interval_s=min_interval,
        max_retries=max_retries,
        backoff_schedule=(0.01, 0.02, 0.04),  # fast for tests
    )
    return client, mock_httpx


@pytest.fixture()
def _patch_time() -> Generator[MagicMock, None, None]:
    """Patch time.monotonic and time.sleep on the resilient_client module."""
    mock_time = MagicMock()
    mock_time.monotonic.return_value = 100.0
    mock_time.sleep = MagicMock()
    original = _mod.time
    _mod.time = mock_time
    try:
        yield mock_time
    finally:
        _mod.time = original


# ---------------------------------------------------------------------------
# Throttle
# ---------------------------------------------------------------------------


def test_throttle_sleeps_when_requests_are_too_fast(_patch_time: MagicMock) -> None:
    """Requests closer than min_request_interval_s trigger a sleep."""
    _patch_time.monotonic.side_effect = [0.0, 0.0, 0.1, 1.0]

    resp = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.return_value = resp

    client = ResilientClient(mock_httpx, min_request_interval_s=1.0)
    client.get("/test1")
    client.get("/test2")

    _patch_time.sleep.assert_called()
    sleep_arg = _patch_time.sleep.call_args_list[-1][0][0]
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


def test_retry_on_429_then_success(_patch_time: MagicMock) -> None:
    """A 429 followed by 200 returns the 200 response."""
    r429 = _make_response(429)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r429, r200])

    result = client.get("/test")

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 2


def test_retry_on_429_respects_retry_after_header(_patch_time: MagicMock) -> None:
    """Retry-After header value is used as sleep duration."""
    r429 = _make_response(429, headers={"retry-after": "5"})
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r429, r200])

    result = client.get("/test")

    assert result.status_code == 200
    # The retry sleep should use 5.0 from Retry-After header
    retry_sleeps = [
        call[0][0]
        for call in _patch_time.sleep.call_args_list
        if call[0][0] >= 1.0  # filter out throttle micro-sleeps
    ]
    assert any(s == 5.0 for s in retry_sleeps)


def test_max_retries_exceeded_raises(_patch_time: MagicMock) -> None:
    """After max_retries 429s, the final 429 raises HTTPStatusError."""
    responses = [_make_response(429)] * 4  # 1 initial + 3 retries
    client, _ = _make_client(responses, max_retries=3)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.get("/test")

    assert exc_info.value.response.status_code == 429


# ---------------------------------------------------------------------------
# Retry on 5xx
# ---------------------------------------------------------------------------


def test_retry_on_500_then_success(_patch_time: MagicMock) -> None:
    """A 500 followed by 200 returns the 200 response."""
    r500 = _make_response(500)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r500, r200])

    result = client.get("/test")

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 2


def test_retry_on_502(_patch_time: MagicMock) -> None:
    """502 is retryable."""
    r502 = _make_response(502)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r502, r200])

    result = client.get("/test")
    assert result.status_code == 200


def test_retry_on_503_and_504(_patch_time: MagicMock) -> None:
    """503 then 504 then 200 all retried successfully."""
    r503 = _make_response(503)
    r504 = _make_response(504)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r503, r504, r200])

    result = client.get("/test")

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


def test_post_is_throttled_and_retried(_patch_time: MagicMock) -> None:
    """POST requests go through the same throttle + retry path."""
    r429 = _make_response(429)
    r200 = _make_response(200)
    client, mock_httpx = _make_client([r429, r200])

    result = client.post("/test", json={"key": "value"})

    assert result.status_code == 200
    assert mock_httpx.send.call_count == 2
    mock_httpx.build_request.assert_called()
    assert mock_httpx.build_request.call_args_list[0][0][0] == "POST"


# ---------------------------------------------------------------------------
# Shared throttle state
# ---------------------------------------------------------------------------


def test_shared_last_request_coordinates_throttle(_patch_time: MagicMock) -> None:
    """Two ResilientClient instances sharing a timestamp coordinate throttle."""
    _patch_time.monotonic.side_effect = [
        0.0,  # client_a _throttle check
        0.0,  # client_a record timestamp
        0.5,  # client_b _throttle check → sees 0.5s since client_a, needs 1.0s
        1.0,  # client_b record timestamp
    ]

    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.return_value = _make_response(200)

    shared_ts: list[float] = [0.0]
    client_a = ResilientClient(mock_httpx, min_request_interval_s=1.0, shared_last_request=shared_ts)
    client_b = ResilientClient(mock_httpx, min_request_interval_s=1.0, shared_last_request=shared_ts)

    client_a.get("/a")
    client_b.get("/b")

    # client_b should have slept to respect the interval since client_a's request
    _patch_time.sleep.assert_called()
    sleep_arg = _patch_time.sleep.call_args_list[0][0][0]
    assert sleep_arg > 0
