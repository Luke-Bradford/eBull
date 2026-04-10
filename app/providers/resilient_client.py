"""
Shared resilient HTTP client for all providers.

Wraps httpx.Client with:
  - Throttle: configurable minimum interval between requests
  - Retry on 429: respects Retry-After header, exponential backoff
  - Retry on 5xx: same backoff for transient server errors
  - Logging: WARNING on each retry, ERROR on final failure

Single implementation used by all providers — not copy-pasted.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping

import httpx

logger = logging.getLogger(__name__)

# Retryable server error codes.
_RETRYABLE_5XX = frozenset({500, 502, 503, 504})

# Default backoff schedule (seconds) for retries.  Length = max retries.
_DEFAULT_BACKOFF = (1.0, 2.0, 4.0)


class ResilientClient:
    """Rate-limited, auto-retrying wrapper around ``httpx.Client``.

    Parameters
    ----------
    client:
        The underlying httpx.Client (caller owns lifecycle / close).
    min_request_interval_s:
        Minimum seconds between consecutive requests.  The wrapper
        sleeps if needed before each request to enforce this floor.
    max_retries:
        Maximum number of retries on 429 or 5xx (default 3).
    backoff_schedule:
        Tuple of sleep durations (seconds) for each retry.
        ``Retry-After`` header overrides these when present on 429.
    """

    def __init__(
        self,
        client: httpx.Client,
        *,
        min_request_interval_s: float = 0.0,
        max_retries: int = 3,
        backoff_schedule: tuple[float, ...] = _DEFAULT_BACKOFF,
        shared_last_request: list[float] | None = None,
    ) -> None:
        self._client = client
        self._min_interval = min_request_interval_s
        self._max_retries = max_retries
        self._backoff = backoff_schedule
        # Mutable list used as a shared reference so multiple ResilientClient
        # instances wrapping the same httpx.Client can coordinate throttle
        # timing.  When two clients share this list, a request from either
        # one advances the shared timestamp, preventing combined rates from
        # exceeding the API limit.
        self._last_request_at: list[float] = shared_last_request if shared_last_request is not None else [0.0]

    # ------------------------------------------------------------------
    # Public API — mirrors httpx.Client.get / .post
    # ------------------------------------------------------------------

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, str | int | float | bool] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Throttled, retried GET request."""
        return self._request("GET", url, params=params, headers=headers)

    def post(
        self,
        url: str,
        *,
        json: object | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Throttled, retried POST request."""
        return self._request("POST", url, json=json, headers=headers)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Sleep if needed to enforce the minimum inter-request interval."""
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at[0]
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, str | int | float | bool] | None = None,
        headers: dict[str, str] | None = None,
        json: object | None = None,
    ) -> httpx.Response:
        """Execute a request with throttle + retry.

        On the final attempt, a retryable status (429/5xx) is raised via
        ``raise_for_status()`` unconditionally — no continue, no fallthrough.
        """
        last_response: httpx.Response | None = None

        for attempt in range(1 + self._max_retries):
            self._throttle()
            self._last_request_at[0] = time.monotonic()

            request = self._client.build_request(
                method,
                url,
                params=params,
                headers=headers,
                json=json,
            )
            response = self._client.send(request)

            if response.status_code == 429 or response.status_code in _RETRYABLE_5XX:
                last_response = response
                if attempt >= self._max_retries:
                    # Final attempt — raise unconditionally and exit.
                    response.raise_for_status()
                    # Unreachable for real httpx.Response, but guard against
                    # mocks that swallow raise_for_status.
                    raise httpx.HTTPStatusError(
                        f"Max retries exceeded: {response.status_code}",
                        request=request,
                        response=response,
                    )

                sleep_s = self._retry_delay(response, attempt)
                logger.warning(
                    "Retryable %d from %s %s — attempt %d/%d, sleeping %.1fs",
                    response.status_code,
                    method,
                    url,
                    attempt + 1,
                    self._max_retries,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue

            # Non-retryable status — return as-is (caller calls raise_for_status)
            return response

        # Post-loop: only reachable if all attempts were retryable and the
        # final-attempt raise was somehow swallowed (should not happen).
        if last_response is not None:
            raise httpx.HTTPStatusError(
                f"Max retries exceeded: {last_response.status_code}",
                request=last_response.request,
                response=last_response,
            )
        raise RuntimeError("Unreachable: retry loop exited without return or raise")

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        """Determine how long to sleep before the next retry.

        Uses ``Retry-After`` header if present (429 responses),
        otherwise falls back to the backoff schedule.
        """
        if response.status_code == 429:
            retry_after = response.headers.get("retry-after")
            if retry_after is not None:
                try:
                    return max(float(retry_after), 0.1)
                except ValueError:
                    # Retry-After may be an HTTP-date (RFC 7231 §7.1.3).
                    # We don't parse dates — log and fall back to backoff.
                    logger.warning(
                        "Unparseable Retry-After header %r, using backoff schedule",
                        retry_after,
                    )

        idx = min(attempt, len(self._backoff) - 1)
        return self._backoff[idx]
