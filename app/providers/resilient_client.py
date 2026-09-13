"""
Shared resilient HTTP client for all providers.

Wraps httpx.Client with:
  - Throttle: configurable minimum interval between requests
  - Retry on 429: respects Retry-After header, exponential backoff
  - Retry on 5xx: backoff for transient server errors; honours
    Retry-After (delta-seconds or HTTP-date) when present and
    shorter than the backoff schedule
  - Logging: WARNING on each retry (status, Retry-After, correlation
    ID, body preview), ERROR on final failure

Single implementation used by all providers — not copy-pasted.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable

    from app.providers.rate_gate import RateGate

logger = logging.getLogger(__name__)

# Retryable server error codes.
_RETRYABLE_5XX = frozenset({500, 502, 503, 504})

# Default backoff schedule (seconds) for retries.  Length = max retries.
_DEFAULT_BACKOFF = (1.0, 2.0, 4.0)

# Cap on the response-body excerpt logged on retryable errors. Bodies
# above this are truncated in-place; oversized payloads (e.g. an HTML
# 502 page from a CDN) would otherwise blow up log lines.
_BODY_PREVIEW_LIMIT = 200


@dataclass(frozen=True)
class RequestAttempt:
    """One HTTP attempt this client actually issued (#2946 step 3 item 3).

    Per ATTEMPT, not per logical call: a rolling-window quota counts stamps and a
    retry places one, so a retry is a separate record.

    ⚠ This is evidence that we ISSUED traffic, not proof that a remote quota was
    spent.  A connect-pool failure may never reach the server, and how a server
    accounts for a request it rejected is not observable from here.

    ``status`` is ``None`` exactly when ``send()`` raised.  ⚠ ``httpx.Client.send``
    reads the body before returning, so a body-read failure on an already-received
    response also lands here -- a 429 can hide behind ``status=None``.

    ``pre_request_wait_s`` is wall-clock around the whole throttle step, so it covers
    the ``RateGate`` branch as well as the shared-clock one.  It is **wait before
    send, lock contention included** -- not sleep time -- and it excludes retry
    backoff, which is spent after this record is emitted.

    ⚠⚠ ``ts`` is when the attempt was ISSUED, captured immediately before ``send()``
    and carried through both outcomes.  Stamping it on COMPLETION instead would make
    the rolling-window count a function of response latency: two requests dispatched
    at 0s and 59s but completing at 0s and 61s would report a maximum of one per
    minute when the quota saw two.  A quota counts dispatches.
    """

    ts: datetime
    method: str
    url: str
    status: int | None
    error: str | None
    pre_request_wait_s: float
    attempt: int


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
        shared_throttle_lock: threading.Lock | None = None,
        gate: RateGate | None = None,
        on_429: Callable[[], None] | None = None,
        on_attempt: Callable[[RequestAttempt], None] | None = None,
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
        # Lock around the read-modify-write of ``_last_request_at`` so
        # concurrent fetchers (#726) cannot race past the rate-limit
        # floor. Each instance holds its own lock so providers with
        # independent rate budgets are isolated; providers sharing a
        # ``shared_last_request`` list also need to share this lock to
        # keep the throttle atomic across instances. We surface that
        # via the ``shared_throttle_lock`` parameter — callers that
        # share a clock pass the same lock object.
        self._throttle_lock: threading.Lock = (
            shared_throttle_lock if shared_throttle_lock is not None else threading.Lock()
        )
        # #1484: when a RateGate is injected, the throttle delegates to it
        # (cross-process gate) and the legacy shared-clock path is bypassed.
        self._gate = gate
        # #1484 §4.4: optional SEC-scoped 429 counter callback.  Only SEC
        # providers wire this — ResilientClient itself has no knowledge of
        # the counter (it's a generic class shared by non-SEC providers).
        self._on_429 = on_429
        # #2946 step 3 item 3: optional per-ATTEMPT observer.  eToro providers wire
        # this and deliberately do NOT wire ``on_429`` -- ``on_attempt`` already sees
        # every 429, and wiring both would double-count.  With ``on_attempt=None``
        # the request path is exactly what it was before this parameter existed.
        self._on_attempt = on_attempt

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

    def patch(
        self,
        url: str,
        *,
        json: object | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Throttled, retried PATCH request."""
        return self._request("PATCH", url, json=json, headers=headers)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _throttle_and_stamp(self) -> float:
        """Sleep if needed to enforce the inter-request floor, then
        advance the shared timestamp atomically.  Returns the wall-clock
        seconds spent here.

        ⚠ The return value is NOT sleep time.  It is the whole wait before the
        request goes out, so it includes lock contention behind another caller
        and any work a ``RateGate`` does.  #2946 step 3 item 3 reports it under
        that name; callers must not read it as "how long the floor made us wait".

        Pre-#726 this was a separate ``_throttle`` step + an
        unsynchronised ``_last_request_at[0] = ...`` write at the
        request site. Under concurrent fetchers (issue #726) the
        check-and-write race let multiple threads pass the floor
        simultaneously and burst past the API rate limit. Combining
        the two steps under a single lock keeps the floor atomic
        across N concurrent callers — at most one thread is firing
        a request per ``min_request_interval_s``.
        """
        # ⚠ Read BEFORE the lock: waiting behind another caller is the thing worth
        # measuring here (``etoro_broker.py`` documents up to 2.4s of head-of-line
        # blocking behind a write's 3.5s sleep).  Every branch reuses its existing
        # clock read for the end point, so this adds exactly one read per request.
        started = time.monotonic()
        if self._gate is not None:
            self._gate.acquire()
            return time.monotonic() - started
        if self._min_interval <= 0:
            with self._throttle_lock:
                now = time.monotonic()
                self._last_request_at[0] = now
            return now - started
        with self._throttle_lock:
            elapsed = time.monotonic() - self._last_request_at[0]
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            now = time.monotonic()
            self._last_request_at[0] = now
        return now - started

    def _record_attempt(
        self,
        method: str,
        url: str,
        status: int | None,
        error: str | None,
        pre_request_wait_s: float,
        attempt: int,
        issued_at: datetime,
    ) -> None:
        """Hand one attempt to the observer.  Never raises, never blocks on a lock.

        Called after the throttle lock has already been released, so an observer
        cannot serialise unrelated lanes or deadlock by re-entering the client.  An
        observer that raises is logged and swallowed: instrumentation must not be
        able to fail a request.
        """
        if self._on_attempt is None:
            return
        try:
            self._on_attempt(
                RequestAttempt(
                    ts=issued_at,
                    method=method,
                    url=url,
                    status=status,
                    error=error,
                    pre_request_wait_s=pre_request_wait_s,
                    attempt=attempt,
                )
            )
        except Exception:
            logger.warning("on_attempt observer raised for %s %s", method, url, exc_info=True)

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
            wait_s = self._throttle_and_stamp()

            request = self._client.build_request(
                method,
                url,
                params=params,
                headers=headers,
                json=json,
            )
            # ⚠ The observer is called IMMEDIATELY around ``send`` and nowhere else.
            # This loop exits three ways -- ``return`` on a non-retryable status,
            # ``continue`` on a retry, and ``raise_for_status()`` on the final
            # retryable attempt -- so a loop-tail hook would miss the exhausted-retry
            # attempt, which is the one a quota question cares about most.  Nothing is
            # recorded when ``build_request`` or the gate raises: no request went out.
            issued_at = datetime.now(UTC)
            try:
                response = self._client.send(request)
            except Exception as exc:
                self._record_attempt(method, url, None, type(exc).__name__, wait_s, attempt, issued_at)
                raise
            self._record_attempt(method, url, response.status_code, None, wait_s, attempt, issued_at)

            if response.status_code == 429 or response.status_code in _RETRYABLE_5XX:
                last_response = response
                if response.status_code == 429 and self._on_429 is not None:
                    self._on_429()
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
                diag = _diagnostics(response)
                logger.warning(
                    "Retryable %d from %s %s — attempt %d/%d, sleeping %gs"
                    " (retry_after=%s, correlation_id=%s, body=%r)",
                    response.status_code,
                    method,
                    url,
                    attempt + 1,
                    self._max_retries,
                    sleep_s,
                    diag["retry_after"],
                    diag["correlation_id"],
                    diag["body_preview"],
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

        - 429: ``Retry-After`` header overrides the backoff schedule
          when parseable.
        - 5xx: ``Retry-After`` (if parseable) caps the backoff from
          above — server hint shortens our wait but cannot extend it
          beyond our configured budget.
        - Otherwise: fall back to the backoff schedule.
        """
        idx = min(attempt, len(self._backoff) - 1)
        backoff = self._backoff[idx]

        retry_after = _parse_retry_after(response.headers.get("retry-after"))

        if response.status_code == 429:
            return retry_after if retry_after is not None else backoff

        if response.status_code in _RETRYABLE_5XX and retry_after is not None:
            return min(retry_after, backoff)

        return backoff


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header value (RFC 7231 §7.1.3) to seconds.

    Accepts both forms the spec defines:
      - delta-seconds: an integer or float number of seconds
      - HTTP-date: an RFC 7231 IMF-fixdate timestamp

    Returns ``None`` when the header is absent, blank, or unparseable —
    callers fall back to the backoff schedule. A 0.1s floor stops a
    malicious or buggy ``Retry-After: 0`` (or a past HTTP-date) from
    busy-looping; on a healthy server hint the floor is a no-op.
    """
    if value is None or not value.strip():
        return None
    try:
        return max(float(value), 0.1)
    except ValueError:
        pass

    # HTTP-date form — parse, return delta vs now.
    try:
        target = parsedate_to_datetime(value)
    except TypeError, ValueError:
        target = None
    if target is not None:
        if target.tzinfo is None:
            target = target.replace(tzinfo=UTC)
        delta = (target - datetime.now(UTC)).total_seconds()
        return max(delta, 0.1)

    logger.warning(
        "Unparseable Retry-After header %r, using backoff schedule",
        value,
    )
    return None


def _diagnostics(response: httpx.Response) -> dict[str, object]:
    """Extract structured diagnostics from a retryable response for
    logging. Every field is best-effort — providers vary in which
    headers they emit and a missing field never blocks logging.
    """
    correlation = response.headers.get("X-Correlation-ID") or response.headers.get("X-Request-ID")
    body_preview: str | None
    try:
        text = response.text
    except Exception:
        body_preview = None
    else:
        if not text:
            body_preview = None
        elif len(text) > _BODY_PREVIEW_LIMIT:
            body_preview = text[:_BODY_PREVIEW_LIMIT] + "…"
        else:
            body_preview = text
    return {
        "retry_after": response.headers.get("Retry-After"),
        "correlation_id": correlation,
        "body_preview": body_preview,
    }
