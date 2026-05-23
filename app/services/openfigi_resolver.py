"""OpenFIGI CUSIP → ticker resolver (#1233 PR-1b).

Adapter over ``https://api.openfigi.com/v3/mapping``. Provides the
fallback path the bootstrap sweep stage S13 (``cusip_resolver_post_bulk_sweep``)
uses when SEC's 13F Official List fuzzy-name match cannot bridge an
unresolved CUSIP to an existing ``instruments.symbol``.

## Why this module is small

* No transport layer abstraction — directly uses ``httpx``. The only
  HTTP path is one POST to a known JSON endpoint; wrapping it in a
  provider hierarchy adds ceremony without value.
* No background queue — the sweep stage is the sole caller. Batches
  iterate inline under the per-process rate-limiter.
* No persistent client — each ``OpenFigiResolver`` instance owns its
  own ``httpx.Client``; the sweep creates one, drains the batch, closes.

## Contract pins (probed by PR-0)

The behaviour assumptions encoded here come from
``tests/fixtures/openfigi/*.json`` recorded by
``scripts/probe_openfigi.py``. See ``.claude/skills/data-sources/openfigi.md``
for the empirical findings:

* Response is positional-parallel to request — index N in response
  maps to CUSIP at index N in request body. We enforce this via
  ``zip(..., strict=True)`` so a future API change that injects null
  placeholders fails loudly instead of silently mis-aligning.
* Per-row response is one of:
    - ``{"data": [<mapping>, ...]}`` for a resolved CUSIP. The ``data``
      array contains EVERY worldwide listing for that CUSIP — AAPL
      returns 255 entries. We MUST filter to the US-primary common-stock
      row (``exchCode='US' AND securityType='Common Stock'``) before
      promoting; ``data[0]`` is empirically the US-primary today but
      the contract gives no ordering guarantee, so blind indexing is
      a latent prod bug.
    - ``{"warning": "<text>"}`` for an unresolved CUSIP.
    - ``{"error": "<text>"}`` (theoretical — not in probe set; treated
      identically to ``warning``).
* On 429: response body is plain text ``"Too many requests, …"`` —
  NOT JSON. We MUST branch on ``status_code == 429`` BEFORE attempting
  ``resp.json()``, honour the ``Retry-After`` header (in seconds), and
  retry exactly once. A second 429 surfaces as
  ``OpenFigiRateLimited`` to the caller.

## Rate-limit budgets (SD-1 cross-reference)

Unkeyed tier (default): 25 requests per 60s window × 10 jobs/POST
= 250 mappings/min.
Keyed tier (``OPENFIGI_API_KEY`` set): 25 requests per 6s window ×
100 jobs/POST = 25,000 mappings/min.

The class enforces the per-window request budget via a token-bucket
clock; OpenFIGI's IETF draft ``ratelimit-remaining`` header is the
canonical post-response signal but we ALSO clamp client-side to avoid
burning into the 429 cliff. Caller MUST drive the rate-limit budget
through the same resolver instance (the throttle clock is
per-instance, not module-global).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Final

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------


OPENFIGI_BASE_URL: Final[str] = "https://api.openfigi.com/v3/mapping"
"""OpenFIGI v3 mapping endpoint. Single fixed URL — versioning is in
the path so a future v4 cutover is a code edit, not config."""


# Default ceilings per tier (probed 2026-05-22 unkeyed; doc-derived keyed).
_UNKEYED_WINDOW_SECONDS: Final[int] = 60
_UNKEYED_REQUESTS_PER_WINDOW: Final[int] = 25
_UNKEYED_JOBS_PER_POST: Final[int] = 10

_KEYED_WINDOW_SECONDS: Final[int] = 6
_KEYED_REQUESTS_PER_WINDOW: Final[int] = 25
_KEYED_JOBS_PER_POST: Final[int] = 100


# Defensive filter for the data[] array. AAPL CUSIP returns 255
# worldwide listings; we want the US-primary common-stock row.
_US_PRIMARY_EXCH_CODE: Final[str] = "US"
"""Composite-US exchange code — the only value PR-0's invariant admits.

PR-0 fixtures pin the US-primary row to ``exchCode == 'US'`` (composite)
+ ``securityType == 'Common Stock'``. NYSE / NASDAQ listed-exchange
variants (``UN``, ``UQ``) and cross-listings (``UA``, ``UC``, ``UP``)
are excluded so we never bind ownership rows to a listed-exchange
mirror when the composite row is absent or reordered — the resolver
returns None and the bulk row stays pending for operator triage.

Strict single-code match per Codex 2 pre-push review (#1233 PR-1b):
widening to a set of US-ish codes admits row promotion in the
``data[]`` reorder case where the composite row is missing, which is
exactly the scenario the defensive filter exists to refuse."""


@dataclass(frozen=True)
class OpenFigiMapping:
    """Resolved-CUSIP payload — fields the sweep promotes into
    ``external_identifiers`` and (later) ``unresolved_13f_cusips.name_of_issuer``.

    All optional because OpenFIGI's ``data`` entry shape is loosely
    typed — a future API change that drops one field still keeps the
    caller's promotion path workable as long as ``ticker`` is present
    (the only field the sweep promotes-on; the others are audit
    metadata).
    """

    ticker: str
    name: str | None
    exch_code: str | None
    share_class_figi: str | None


class OpenFigiError(Exception):
    """Base class for OpenFIGI resolver errors."""


class OpenFigiRateLimited(OpenFigiError):
    """Resolver tripped 429 after one ``Retry-After`` backoff."""


class OpenFigiTransportError(OpenFigiError):
    """HTTP transport failure (timeout, DNS, 5xx) after no retry."""


# ---------------------------------------------------------------------------
# Per-instance rate limiter
# ---------------------------------------------------------------------------


class _RateLimiter:
    """Token-bucket per-instance rate limiter for OpenFIGI POSTs.

    Tracks request timestamps in a deque of monotonic-clock floats.
    Each ``acquire()`` strips the head of expired entries (older than
    ``window_seconds``), and if the deque is at capacity sleeps until
    the oldest entry expires.

    Thread-safety: callers ``with limiter:`` — the context manager
    holds a ``threading.Lock`` for the duration of acquire+release so
    a concurrent caller in the same lane cannot oversubscribe.

    Not async-aware. The sweep is sync — if a future caller wants
    async, swap in ``asyncio.Lock`` and ``asyncio.sleep``.
    """

    def __init__(self, *, per_window: int, window_seconds: int) -> None:
        if per_window < 1:
            raise ValueError(f"per_window must be >= 1, got {per_window}")
        if window_seconds < 1:
            raise ValueError(f"window_seconds must be >= 1, got {window_seconds}")
        self._per_window = per_window
        self._window_seconds = window_seconds
        # Use a list as a FIFO; OpenFIGI's per-window budget is small
        # (25) so O(n) trim is dominated by the network round-trip.
        self._timestamps: list[float] = []
        self._lock = threading.Lock()

    def __enter__(self) -> _RateLimiter:
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        # No release needed — we record the request timestamp on acquire,
        # not on exit. The OpenFIGI window is wall-clock-fixed, not
        # request-duration-bound.
        return None

    def acquire(self) -> None:
        """Block until a request slot is available in the current window."""
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - self._window_seconds
                # Drop expired timestamps.
                self._timestamps = [t for t in self._timestamps if t > cutoff]
                if len(self._timestamps) < self._per_window:
                    self._timestamps.append(now)
                    return
                # Need to wait until the oldest timestamp expires.
                sleep_for = self._timestamps[0] - cutoff
            # Sleep OUTSIDE the lock so a concurrent acquire on another
            # thread can re-evaluate when its own slot opens.
            if sleep_for > 0:
                time.sleep(sleep_for)


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


def _batched(items: Iterable[str], n: int) -> Iterator[list[str]]:
    """Yield successive ``n``-sized batches from ``items``.

    Bespoke implementation rather than ``itertools.batched`` so the
    minimum-Python target stays portable. Sweep call sites pass either
    set-comprehensions or lists; this normalises both into deterministic
    list batches.
    """
    if n < 1:
        raise ValueError(f"batch size must be >= 1, got {n}")
    bucket: list[str] = []
    for item in items:
        bucket.append(item)
        if len(bucket) >= n:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def _pick_us_primary(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the US-primary common-stock entry from a ``data`` array.

    Defensive against future re-ordering of OpenFIGI's array — does
    NOT assume ``entries[0]`` is the US primary even though empirically
    it is today (PR-0 probe §7.4).

    Returns ``None`` when no US common-stock entry exists. Callers
    should treat that case as unresolved (no ADR / pink-sheet fallback
    — operator action documented in ``.claude/skills/data-sources/openfigi.md``
    §7.5).
    """
    for entry in entries:
        if entry.get("exchCode") == _US_PRIMARY_EXCH_CODE and entry.get("securityType") == "Common Stock":
            return entry
    return None


def _normalise_cusip(cusip: str) -> str:
    """Strip + uppercase. CUSIPs are mixed-case 9-char identifiers; the
    upstream pipeline already validates length but we belt-and-brace
    by uppercasing so the OpenFIGI request body is canonical."""
    return cusip.strip().upper()


class OpenFigiResolver:
    """Synchronous CUSIP-batch resolver against OpenFIGI v3 mapping API.

    Instantiate once per sweep — the rate limiter is per-instance, NOT
    module-global, so two concurrent sweep invocations on the same
    process would over-subscribe the OpenFIGI window. (The bootstrap
    orchestrator's ``openfigi`` Lane caps at 1, so single-process
    concurrency cannot occur there. Tests can instantiate freely.)

    Usage::

        resolver = OpenFigiResolver.from_env()  # reads settings.openfigi_api_key
        mappings = resolver.resolve_cusips(["037833100", "594918104"])
        # → {"037833100": OpenFigiMapping(ticker="AAPL", ...),
        #    "594918104": OpenFigiMapping(ticker="MSFT", ...)}

    Lifecycle: the resolver owns its own ``httpx.Client``. Use as
    context manager OR call :meth:`close` when done — the sweep stage
    uses the context-manager pattern.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        client: httpx.Client | None = None,
        request_timeout: float = 30.0,
    ) -> None:
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client if client is not None else httpx.Client(timeout=request_timeout)
        if api_key:
            self._per_window = _KEYED_REQUESTS_PER_WINDOW
            self._window_seconds = _KEYED_WINDOW_SECONDS
            self._jobs_per_post = _KEYED_JOBS_PER_POST
        else:
            self._per_window = _UNKEYED_REQUESTS_PER_WINDOW
            self._window_seconds = _UNKEYED_WINDOW_SECONDS
            self._jobs_per_post = _UNKEYED_JOBS_PER_POST
        self._rate_limiter = _RateLimiter(
            per_window=self._per_window,
            window_seconds=self._window_seconds,
        )

    @classmethod
    def from_env(cls, *, client: httpx.Client | None = None) -> OpenFigiResolver:
        """Read OpenFIGI key from ``Settings`` and instantiate.

        Resolution precedence (delegated to ``pydantic_settings``):
          1. ``OPENFIGI_API_KEY`` environment variable (live env wins)
          2. ``OPENFIGI_API_KEY=`` line in ``.env``
          3. ``None`` → unkeyed tier (250 mappings/min)

        Reading via ``settings.openfigi_api_key`` keeps the env-file vs
        live-env precedence consistent with every other secret in the
        repo. The prior ``os.environ.get`` shortcut silently bypassed
        the .env loader — a key written into .env didn't actually reach
        the resolver. Fixed in #1233 post-merge follow-up.
        """
        from app.config import settings

        return cls(api_key=settings.openfigi_api_key or None, client=client)

    # -------- Context manager -------------------------------------------

    def __enter__(self) -> OpenFigiResolver:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying httpx client iff we own it. No-op when
        the caller supplied a shared client."""
        if self._owns_client:
            self._client.close()

    # -------- Properties (read-only, for callers / tests) ---------------

    @property
    def jobs_per_post(self) -> int:
        return self._jobs_per_post

    @property
    def keyed(self) -> bool:
        return self._api_key is not None

    # -------- Resolution --------------------------------------------------

    def resolve_cusips(self, cusips: Iterable[str]) -> dict[str, OpenFigiMapping]:
        """Batch-resolve an iterable of CUSIPs → mapping dict.

        Returns ONLY successful resolutions (warning / error / no-US-row
        entries are omitted). Caller can compute the unresolved
        complement via ``set(cusips) - mappings.keys()``.

        Idempotent for duplicate CUSIPs: a duplicate in the input is
        de-duplicated before the request (avoids burning rate-limit
        budget on duplicate CUSIPs and keeps the result dict shape
        unambiguous).

        Empty input is a no-op — returns ``{}`` without any HTTP call.
        """
        # Normalise + drop empty / whitespace-only strings + de-dup
        # while preserving first-seen order. ``dict.fromkeys`` gives O(n)
        # de-dup keeping first-seen ordering — useful for tests that
        # assert deterministic per-batch composition. The two-pass shape
        # (normalise first, then filter falsy) is deliberate: a CUSIP
        # like ``"   "`` collapses to ``""`` post-strip and MUST drop
        # before hitting OpenFIGI (a request with idValue='' would be
        # a 400 from the API and a wasted rate-limit slot).
        normalised = [c for c in (_normalise_cusip(c) for c in cusips) if c]
        normalised = list(dict.fromkeys(normalised))
        if not normalised:
            return {}

        results: dict[str, OpenFigiMapping] = {}
        for chunk in _batched(normalised, self._jobs_per_post):
            chunk_results = self._post_and_parse(chunk)
            results.update(chunk_results)
        return results

    def _post_and_parse(self, cusips: list[str]) -> dict[str, OpenFigiMapping]:
        """Issue one POST + parse the parallel-array response.

        Each HTTP attempt (including the post-429 retry) acquires its
        own rate-limit token via :meth:`_post`. The token-bucket clock
        sees every POST, not just the first one.
        """
        resp = self._post(cusips, attempt=1)
        return self._parse_response(cusips, resp)

    def _post(self, cusips: list[str], *, attempt: int) -> httpx.Response:
        """Single POST with ``Retry-After``-honouring backoff on 429.

        Retries exactly ONCE on 429. A second 429 raises
        :class:`OpenFigiRateLimited` so callers can defer the rest of
        the sweep (the spec acceptance is "5xx extended outage → run
        completes with ``coverage_floor_met=FALSE``"; same applies to
        sustained 429s).

        Rate-limit token acquired HERE so the retry POST is accounted
        too — Codex 2 pre-push: ``with self._rate_limiter`` at the
        ``_post_and_parse`` level was outside the recursive retry path,
        and a 429+200 sequence consumed two HTTP slots but only one
        token. Moving the acquire into the per-POST path means the
        retry sleep (``time.sleep(retry_after)``) happens BEFORE the
        retry's token acquire — the post-sleep ``acquire()`` re-checks
        the window and either grants immediately (window rolled over)
        or blocks (window still saturated, which is benign — we
        already slept ``Retry-After``).
        """
        body = [{"idType": "ID_CUSIP", "idValue": c} for c in cusips]
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._api_key:
            headers["X-OPENFIGI-APIKEY"] = self._api_key

        try:
            with self._rate_limiter:
                resp = self._client.post(OPENFIGI_BASE_URL, json=body, headers=headers)
        except httpx.HTTPError as exc:
            # Transport-level failures (timeout, connection refused,
            # DNS). The sweep is daily-cadence — failing fast lets the
            # outer caller record coverage_floor_met=FALSE and move on.
            raise OpenFigiTransportError(f"OpenFIGI transport failure: {exc!r}") from exc

        # Branch BEFORE .json() — 429 body is plain-text, not JSON
        # (PR-0 probe rate_limit_429.json + .claude/skills/data-sources/openfigi.md §3).
        if resp.status_code == 429:
            if attempt >= 2:
                logger.warning(
                    "OpenFIGI: 429 persists after retry; surfacing OpenFigiRateLimited (cusip_count=%d)",
                    len(cusips),
                )
                raise OpenFigiRateLimited("OpenFIGI 429 persisted after one Retry-After backoff")
            retry_after_raw = resp.headers.get("retry-after") or resp.headers.get("Retry-After")
            try:
                retry_after = int(retry_after_raw) if retry_after_raw is not None else self._window_seconds
            except ValueError:
                # Retry-After can be HTTP-date format too; OpenFIGI's
                # probed shape is always integer seconds, but if a
                # future change emits a date we fall back to the
                # window-length sleep (conservative).
                retry_after = self._window_seconds
            # Clamp to a sane ceiling so a bad header doesn't park us
            # for hours. The PR-0 probe recorded retry-after=58 against
            # a 60-second window — anything beyond 2x the window is
            # almost certainly a server bug; sleep the window length
            # and retry.
            retry_after = max(1, min(retry_after, self._window_seconds * 2))
            logger.info(
                "OpenFIGI: 429 received; sleeping %ds before retry (attempt=%d)",
                retry_after,
                attempt,
            )
            time.sleep(retry_after)
            return self._post(cusips, attempt=attempt + 1)

        if resp.status_code >= 500:
            # 5xx is rare; the spec says treat as transport failure.
            # We don't retry 5xx automatically — the caller's outer
            # ``coverage_floor_met=FALSE`` accommodation is the deferral.
            raise OpenFigiTransportError(f"OpenFIGI {resp.status_code} on POST: {resp.text[:200]!r}")

        # 4xx (other than 429) — typically a client-side bug (bad
        # idType, malformed body). Surface as transport error so
        # operator audit sees it instead of silently producing zero
        # mappings.
        if resp.status_code >= 400:
            raise OpenFigiTransportError(f"OpenFIGI {resp.status_code} on POST: {resp.text[:200]!r}")

        return resp

    def _parse_response(
        self,
        cusips: list[str],
        resp: httpx.Response,
    ) -> dict[str, OpenFigiMapping]:
        """Parse the parallel-array JSON response into a dict.

        Positional contract enforced via ``zip(..., strict=True)``: a
        future API change that injects null placeholders or drops an
        entry will raise ``ValueError`` rather than silently mis-aligning
        CUSIP → mapping pairs.
        """
        try:
            payload = resp.json()
        except ValueError as exc:
            raise OpenFigiTransportError(f"OpenFIGI 2xx body was not JSON: {resp.text[:200]!r}") from exc

        if not isinstance(payload, list):
            raise OpenFigiTransportError(f"OpenFIGI 2xx body was not a JSON array: type={type(payload).__name__}")

        out: dict[str, OpenFigiMapping] = {}
        # ``strict=True`` non-negotiable — see module docstring.
        for cusip, entry in zip(cusips, payload, strict=True):
            mapping = _entry_to_mapping(entry)
            if mapping is not None:
                out[cusip] = mapping
        return out


def _entry_to_mapping(entry: object) -> OpenFigiMapping | None:
    """Convert one parallel-array response entry to a mapping.

    Returns ``None`` for:
      * ``{"warning": ...}`` — OpenFIGI says "no such CUSIP".
      * ``{"error": ...}`` — defensive; not in PR-0 probe set but
        documented as a possible shape.
      * Any other shape (missing ``data``, empty ``data``, no US-row).

    Returns an :class:`OpenFigiMapping` when the response contains a
    US-primary common-stock row with a non-empty ``ticker``.
    """
    if not isinstance(entry, dict):
        return None
    if "data" not in entry:
        # warning / error / unknown — all map to "no mapping".
        return None
    data = entry["data"]
    if not isinstance(data, list) or not data:
        return None
    primary = _pick_us_primary(data)
    if primary is None:
        return None
    ticker = primary.get("ticker")
    if not isinstance(ticker, str) or not ticker.strip():
        return None
    name = primary.get("name")
    exch_code = primary.get("exchCode")
    share_class_figi = primary.get("shareClassFIGI")
    return OpenFigiMapping(
        ticker=ticker.strip(),
        name=name if isinstance(name, str) else None,
        exch_code=exch_code if isinstance(exch_code, str) else None,
        share_class_figi=share_class_figi if isinstance(share_class_figi, str) else None,
    )


__all__ = [
    "OPENFIGI_BASE_URL",
    "OpenFigiError",
    "OpenFigiMapping",
    "OpenFigiRateLimited",
    "OpenFigiResolver",
    "OpenFigiTransportError",
]
