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
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import MagicMock

import httpx
import pytest

import app.providers.resilient_client as _mod
from app.providers.resilient_client import ResilientClient


def _make_response(
    status_code: int,
    headers: dict[str, str] | None = None,
    *,
    content: bytes | None = None,
) -> httpx.Response:
    """Build a minimal httpx.Response for testing."""
    return httpx.Response(
        status_code=status_code,
        headers=headers or {},
        content=content if content is not None else b"",
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
def _patch_time() -> Generator[MagicMock]:
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


def test_429_respects_retry_after_http_date(
    _patch_time: MagicMock,
) -> None:
    """429 with HTTP-date Retry-After parses to a delta and overrides backoff."""
    near_future = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=3)
    header = format_datetime(near_future, usegmt=True)
    r429 = _make_response(429, headers={"retry-after": header})
    r200 = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = [r429, r200]
    client = ResilientClient(mock_httpx, max_retries=3, backoff_schedule=(60.0, 120.0, 240.0))

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list]
    # 429 path uses Retry-After verbatim (override, not min). Delta is
    # ~3s so a sleep in (0.1, 4) range proves the date drove it, not
    # the 60s backoff.
    matched = [s for s in retry_sleeps if 0.1 <= s <= 4.0]
    assert matched, f"no Retry-After-driven sleep found in {retry_sleeps}"


def test_429_unparseable_retry_after_falls_back_to_backoff(
    _patch_time: MagicMock,
) -> None:
    """429 with garbage Retry-After falls back to backoff."""
    r429 = _make_response(429, headers={"retry-after": "soon-ish"})
    r200 = _make_response(200)
    client, _ = _make_client([r429, r200])

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list if call[0][0] >= 0.001]
    assert 0.01 in retry_sleeps


def test_429_subsecond_retry_after_floored_to_min(
    _patch_time: MagicMock,
) -> None:
    """429 with a sub-floor Retry-After (e.g. ``0``) is clamped to 0.1s."""
    r429 = _make_response(429, headers={"retry-after": "0"})
    r200 = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = [r429, r200]
    client = ResilientClient(mock_httpx, max_retries=3, backoff_schedule=(1.0, 2.0, 4.0))

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list]
    assert 0.1 in retry_sleeps
    assert 0 not in retry_sleeps


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


# ---------------------------------------------------------------------------
# 5xx Retry-After + diagnostics logging (#685)
# ---------------------------------------------------------------------------


def test_5xx_retry_after_caps_backoff_when_shorter(
    _patch_time: MagicMock,
) -> None:
    """5xx with Retry-After shorter than backoff should sleep for Retry-After."""
    # Custom backoff well above the 0.1s parse floor so Retry-After=0.2
    # is genuinely "shorter than backoff".
    r503 = _make_response(503, headers={"retry-after": "0.2"})
    r200 = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = [r503, r200]
    client = ResilientClient(
        mock_httpx,
        max_retries=3,
        backoff_schedule=(1.0, 2.0, 4.0),
    )

    result = client.get("/test")

    assert result.status_code == 200
    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list]
    # Retry-After 0.2 < backoff[0] 1.0 — the shorter value wins.
    assert 0.2 in retry_sleeps


def test_5xx_retry_after_does_not_extend_backoff(
    _patch_time: MagicMock,
) -> None:
    """5xx with Retry-After longer than backoff still sleeps for backoff."""
    # Backoff[0] = 0.01s; Retry-After of 60s would be way too long.
    r503 = _make_response(503, headers={"retry-after": "60"})
    r200 = _make_response(200)
    client, _ = _make_client([r503, r200])

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list if call[0][0] >= 0.001]
    assert 60.0 not in retry_sleeps
    assert 0.01 in retry_sleeps


def test_5xx_unparseable_retry_after_falls_back_to_backoff(
    _patch_time: MagicMock,
) -> None:
    """Retry-After that is neither delta-seconds nor an HTTP-date falls back to backoff."""
    r503 = _make_response(503, headers={"retry-after": "soon-ish"})
    r200 = _make_response(200)
    client, _ = _make_client([r503, r200])

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list if call[0][0] >= 0.001]
    assert 0.01 in retry_sleeps


def test_5xx_retry_after_http_date_in_past_uses_floor(
    _patch_time: MagicMock,
) -> None:
    """An HTTP-date Retry-After in the past should not busy-loop —
    it gets clamped to the 0.1s floor and then capped by the backoff
    schedule. With backoff[0]=1.0s, the floor wins as the shorter cap.
    """
    r503 = _make_response(503, headers={"retry-after": "Mon, 01 Jan 1990 00:00:00 GMT"})
    r200 = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = [r503, r200]
    client = ResilientClient(mock_httpx, max_retries=3, backoff_schedule=(1.0, 2.0, 4.0))

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list]
    assert 0.1 in retry_sleeps


def test_5xx_retry_after_http_date_near_future_wins(
    _patch_time: MagicMock,
) -> None:
    """An HTTP-date Retry-After in the near future (delta < backoff)
    should win as the shorter cap. Constructs a date ~0.5s ahead so
    delta < backoff[0]=2.0s.
    """
    near_future = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=2)
    header = format_datetime(near_future, usegmt=True)
    r503 = _make_response(503, headers={"retry-after": header})
    r200 = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = [r503, r200]
    client = ResilientClient(mock_httpx, max_retries=3, backoff_schedule=(60.0, 120.0, 240.0))

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list]
    # Some value 0.1 < s <= 2.0 (delta), well below the 60s backoff.
    matched = [s for s in retry_sleeps if 0.1 <= s <= 2.5]
    assert matched, f"no Retry-After-driven sleep found in {retry_sleeps}"


def test_5xx_retry_after_subsecond_value_floored_to_min(
    _patch_time: MagicMock,
) -> None:
    """A sub-floor Retry-After (e.g. 0.05s) is clamped to the 0.1s
    floor — defends against hostile/buggy servers that would force
    us into a busy-loop.
    """
    r503 = _make_response(503, headers={"retry-after": "0.05"})
    r200 = _make_response(200)
    mock_httpx = MagicMock(spec=httpx.Client)
    mock_httpx.build_request.return_value = httpx.Request("GET", "https://example.com/test")
    mock_httpx.send.side_effect = [r503, r200]
    client = ResilientClient(mock_httpx, max_retries=3, backoff_schedule=(1.0, 2.0, 4.0))

    client.get("/test")

    retry_sleeps = [call[0][0] for call in _patch_time.sleep.call_args_list]
    assert 0.05 not in retry_sleeps
    assert 0.1 in retry_sleeps


def test_5xx_warning_logs_actual_sleep_duration(
    _patch_time: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The WARNING line must report the real sleep duration (no
    truncation of sub-second values to '0.0s').
    """
    r503 = _make_response(503)
    r200 = _make_response(200)
    client, _ = _make_client([r503, r200])

    with caplog.at_level("WARNING", logger="app.providers.resilient_client"):
        client.get("/test")

    retry_logs = [r for r in caplog.records if "Retryable" in r.getMessage()]
    msg = retry_logs[0].getMessage()
    # backoff[0]=0.01 — the format must surface that, not "0.0s".
    assert "sleeping 0.01s" in msg


def test_5xx_warning_includes_body_correlation_and_retry_after(
    _patch_time: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """5xx WARNING log carries body preview, correlation ID, Retry-After."""
    body = b'{"error":"upstream_unavailable","trace":"abc-123"}'
    r503 = _make_response(
        503,
        headers={
            "retry-after": "0.005",
            "X-Correlation-ID": "corr-xyz-1",
        },
        content=body,
    )
    r200 = _make_response(200)
    client, _ = _make_client([r503, r200])

    with caplog.at_level("WARNING", logger="app.providers.resilient_client"):
        client.get("/test")

    # Exactly one retry warning — the second response (200) is not retried.
    retry_logs = [r for r in caplog.records if "Retryable" in r.getMessage()]
    assert len(retry_logs) == 1
    msg = retry_logs[0].getMessage()
    assert "corr-xyz-1" in msg
    assert "upstream_unavailable" in msg
    assert "retry_after=0.005" in msg


def test_5xx_warning_handles_missing_headers_and_body(
    _patch_time: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Bare 5xx (no Retry-After, no correlation, no body) still logs cleanly."""
    r500 = _make_response(500)
    r200 = _make_response(200)
    client, _ = _make_client([r500, r200])

    with caplog.at_level("WARNING", logger="app.providers.resilient_client"):
        client.get("/test")

    retry_logs = [r for r in caplog.records if "Retryable" in r.getMessage()]
    assert len(retry_logs) == 1
    msg = retry_logs[0].getMessage()
    assert "retry_after=None" in msg
    assert "correlation_id=None" in msg
    assert "body=None" in msg


def test_5xx_correlation_id_falls_back_to_x_request_id(
    _patch_time: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When X-Correlation-ID is absent, use X-Request-ID."""
    r503 = _make_response(503, headers={"X-Request-ID": "req-id-42"})
    r200 = _make_response(200)
    client, _ = _make_client([r503, r200])

    with caplog.at_level("WARNING", logger="app.providers.resilient_client"):
        client.get("/test")

    retry_logs = [r for r in caplog.records if "Retryable" in r.getMessage()]
    assert "req-id-42" in retry_logs[0].getMessage()


def test_5xx_body_preview_truncates_long_bodies(
    _patch_time: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Bodies longer than the preview cap are truncated with an ellipsis."""
    long_body = ("x" * 500).encode("utf-8")
    r502 = _make_response(502, content=long_body)
    r200 = _make_response(200)
    client, _ = _make_client([r502, r200])

    with caplog.at_level("WARNING", logger="app.providers.resilient_client"):
        client.get("/test")

    retry_logs = [r for r in caplog.records if "Retryable" in r.getMessage()]
    msg = retry_logs[0].getMessage()
    # Truncated to 200 chars + ellipsis; the original 500 chars must not appear.
    assert "x" * 500 not in msg
    assert "x" * 200 in msg
    assert "…" in msg


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
