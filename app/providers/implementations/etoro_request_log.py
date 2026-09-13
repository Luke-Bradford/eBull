# app/providers/implementations/etoro_request_log.py
"""Per-request accounting for every eToro REST request we issue (#2946 step 3 item 3).

#2946 step 2's census could not report an OBSERVED request rate for any eToro lane,
because **there is no per-request artefact for any eToro lane**.  Every number it
reports is CONFIGURED (a constant read by import), DERIVED (a bound from a constant)
or UNCOUNTED.  This module is the artefact: one structured log line per HTTP attempt,
plus a process-local per-lane counter.

Read ``docs/proposals/execution/2026-09-13-etoro-request-instrumentation.md`` for the
design and ``2026-09-13-etoro-quota-load-census.md`` for what it is measuring against.

Three things that are easy to get wrong here:

* **The lane vocabulary is not ours.**  Every lane assignment is resolved from
  ``etoro_quota_lanes.CALL_SITES`` and ``KNOWN_UNTHROTTLED`` -- the portal census taken
  in step 1.  Nothing is hand-listed, and a path the tables do not cover classifies as
  ``unclassified`` carrying its URL verbatim.  Guessing it into a neighbouring lane
  would put an invented number in front of a quota decision.
* **The counters are PROCESS-LOCAL.**  The API process and the jobs daemon do not share
  them, and the interesting question (#2946 step 2 clause 2 -- two concurrent lane-B
  callers) is precisely a cross-PROCESS one.  The LOG LINE is the cross-process
  artefact; the counters are a live in-process readout and nothing more.
* **A recorded attempt is traffic we ISSUED, not quota we PROVED spent.**  See
  ``ResilientClient.RequestAttempt``.

⚠ WebSocket traffic is out of scope and stays uncounted -- a different transport, not a
REST quota lane.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx

from app.providers.implementations.etoro_quota_lanes import CALL_SITES, KNOWN_UNTHROTTLED

if TYPE_CHECKING:
    from collections.abc import Callable

    from app.providers.resilient_client import RequestAttempt

# Dedicated logger so the artefact can be routed or levelled without touching
# provider logging.  The reader greps LINE_PREFIX, not the logger name.
logger = logging.getLogger("app.etoro.requests")

LINE_PREFIX = "etoro_request"
SCHEMA_VERSION = 1

#: Lane key used when neither table covers the path.  Never a guess, never prettified.
UNCLASSIFIED = "unclassified"

_PLACEHOLDER = re.compile(r"(\{[^{}]*\})")
#: A template ending in this is a PREFIX template: everything before it must match and
#: any remaining segments are accepted.  ``KNOWN_UNTHROTTLED`` writes one of its entries
#: that way (the debug candle probe).  Escaping the marker would make that entry dead;
#: leaving it unescaped would make it match any three characters.
_PREFIX_MARKER = "..."


def _segment_regex(segment: str) -> str:
    """Regex for one path segment: literals escaped, ``{param}`` widened."""
    out: list[str] = []
    for token in _PLACEHOLDER.split(segment):
        if not token:
            continue
        if token.startswith("{") and token.endswith("}"):
            # ⚠ ``{env}`` is NOT an open wildcard.  ``KNOWN_UNTHROTTLED`` writes the raw
            # pnl probe as ``/api/v1/trading/info/{env}/pnl``; ``[^/]+`` there would
            # classify ``/api/v1/trading/info/staging/pnl`` as lane E.
            out.append("(?:demo|real)" if token == "{env}" else "[^/]+")
        else:
            out.append(re.escape(token))
    return "".join(out)


def _template_to_pattern(template: str) -> re.Pattern[str]:
    """Compile one documented path template into a fully-anchored matcher.

    A literal ``demo`` segment expands to ``(?:/(?:demo|real))?`` -- **the leading slash
    is inside the optional group**.  Writing it as ``/(?:demo|real)?/`` would demand a
    literal ``//`` when the segment is absent, and every real-environment path would
    fall out as ``unclassified``.  Those three shapes (demo / real / absent) are exactly
    the ``KNOWN_PATH_DRIFT`` kinds: the providers build ``/{env}`` for demo and ``""``
    for real on the v1 paths, and ``/{env}`` always on v2 info.
    """
    body = template
    prefix_open = body.endswith(_PREFIX_MARKER)
    if prefix_open:
        body = body[: -len(_PREFIX_MARKER)]
    parts: list[str] = []
    for segment in body.split("/"):
        if not segment:
            continue
        if segment == "demo":
            parts.append("(?:/(?:demo|real))?")
        else:
            parts.append("/" + _segment_regex(segment))
    if prefix_open:
        parts.append("(?:/.*)?")
    return re.compile("".join(parts))


@dataclass(frozen=True)
class LaneMatch:
    """Which documented lane a request path draws on."""

    lane: str
    #: The table template that matched, or ``None`` when unclassified.
    template: str | None

    @property
    def is_classified(self) -> bool:
        return self.lane != UNCLASSIFIED


@dataclass(frozen=True)
class _Route:
    verb: str
    template: str
    lane: str
    pattern: re.Pattern[str]


def _build_routes() -> tuple[_Route, ...]:
    """Compile both source tables, in order, into one match list.

    ⚠ The two tables use DIFFERENT accessors -- ``CallSite.demo_path`` and
    ``UnthrottledCall.path``.  Reading ``.demo_path`` off both silently drops every raw
    call site, which is the half this unit exists to make visible.

    Order matters only for which TEMPLATE is reported: several paths legitimately match
    more than one entry (the raw pnl probe and the broker's own pnl call site; the two
    submitters on ``/api/v2/trading/execution/{env}/orders``).  They agree on the lane --
    ``tests/test_2946_etoro_request_log.py`` asserts no path resolves to two different
    lanes -- and a request is recorded exactly once regardless.
    """
    routes: list[_Route] = []
    for site in CALL_SITES:
        routes.append(_Route(site.verb.upper(), site.demo_path, site.lane, _template_to_pattern(site.demo_path)))
    for raw in KNOWN_UNTHROTTLED:
        routes.append(_Route(raw.verb.upper(), raw.path, raw.lane, _template_to_pattern(raw.path)))
    return tuple(routes)


ROUTES: tuple[_Route, ...] = _build_routes()


def normalise_path(url: str) -> str:
    """Reduce a request URL to the path a template is matched against.

    Drops the fragment and query, reduces an absolute URL to its path, collapses
    repeated slashes and drops a trailing one.  Deliberately does NOT use
    ``urlsplit`` on a relative input: ``orders:lookup`` would read as a URL scheme.
    """
    raw = url.split("#", 1)[0].split("?", 1)[0]
    if "://" in raw:
        raw = raw.split("://", 1)[1]
        raw = raw[raw.index("/") :] if "/" in raw else "/"
    if not raw.startswith("/"):
        raw = "/" + raw
    raw = re.sub(r"/{2,}", "/", raw)
    return raw.rstrip("/") if len(raw) > 1 else raw


def lane_for_request(verb: str, url: str) -> LaneMatch:
    """Resolve a request to its documented quota lane, or ``unclassified``."""
    path = normalise_path(url)
    wanted = verb.upper()
    for route in ROUTES:
        if route.verb == wanted and route.pattern.fullmatch(path):
            return LaneMatch(route.lane, route.template)
    return LaneMatch(UNCLASSIFIED, None)


#: Characters left alone when a URL goes into a log line.  ``%`` is deliberately NOT in
#: the set: with it, an already-malformed ``%`` sequence in the input would pass through
#: un-re-encoded and the field would not be reversible.  Escaping it costs a legitimately
#: percent-encoded URL one visible round of encoding (``%20`` renders as ``%2520``) and
#: buys an exact contract instead: **``urllib.parse.unquote`` of the field returns the
#: original bytes**, whatever they were.
_LINE_SAFE_CHARS = "/:?&=@._~-"


def _line_safe(value: str) -> str:
    """Percent-encode anything that could break the line's space-delimited fields."""
    return quote(value, safe=_LINE_SAFE_CHARS)


@dataclass(frozen=True)
class LaneCounter:
    """Attempts this PROCESS issued against one lane, split by client and environment.

    Split by ``(lane, src, env)`` because an eToro quota is per user key and per
    environment: a single lane total that merges demo and real traffic is not a number
    any budget can be compared against.
    """

    attempts: int = 0
    http_429: int = 0
    transport_errors: int = 0
    wait_s: float = 0.0


_lock = threading.Lock()
_counters: dict[tuple[str, str, str], LaneCounter] = {}


def etoro_lane_counters() -> dict[tuple[str, str, str], LaneCounter]:
    """Snapshot of this process's counters, keyed by ``(lane, src, env)``."""
    with _lock:
        return dict(_counters)


def reset_etoro_lane_counters() -> None:
    with _lock:
        _counters.clear()


def record_etoro_request(
    *,
    verb: str,
    url: str,
    status: int | None,
    src: str,
    env: str,
    error: str | None = None,
    pre_request_wait_s: float = 0.0,
    attempt: int = 0,
    ts: datetime | None = None,
) -> None:
    """Account for one issued eToro REST attempt: bump the counter, emit the line.

    ``src`` names the client (``broker_read`` / ``broker_write`` / ``market``, or the
    raw call site) and ``env`` the broker environment.  Both are LABELS -- no credential
    material is logged, ever.

    ⚠ ``ts`` is carried IN the line and not taken from the log handler.  The API process
    and the jobs daemon format logs differently and the API's format carries no timestamp
    at all, so a reader that parses the handler's prefix cannot merge the two -- and
    merging them is the whole of #2946 step 2's open clause 2.
    """
    match = lane_for_request(verb, url)
    key = (match.lane, src, env)
    with _lock:
        current = _counters.get(key, LaneCounter())
        _counters[key] = replace(
            current,
            attempts=current.attempts + 1,
            http_429=current.http_429 + (1 if status == 429 else 0),
            transport_errors=current.transport_errors + (1 if status is None else 0),
            wait_s=current.wait_s + pre_request_wait_s,
        )
    # BOTH url-bearing fields go through the same escape.  ``path`` is table-matched but
    # not therefore safe: a template's ``{param}`` is ``[^/]+``, which admits a space, so
    # a classified path can break the line's separator just as an unclassified one can.
    path = _line_safe(normalise_path(url))
    # The raw URL is emitted only when nothing matched -- that is the one case where the
    # input shape is unknown, so it is the one case worth keeping in full.
    raw_field = "" if match.is_classified else f" raw={_line_safe(url)}"
    logger.info(
        "%s v=%d ts=%s lane=%s src=%s env=%s verb=%s status=%s err=%s wait_s=%.3f attempt=%d path=%s%s",
        LINE_PREFIX,
        SCHEMA_VERSION,
        (ts or datetime.now(UTC)).isoformat(),
        match.lane,
        src,
        env,
        verb.upper(),
        "-" if status is None else status,
        error or "-",
        pre_request_wait_s,
        attempt,
        path,
        raw_field,
    )


def issue_raw_request(
    client: httpx.Client,
    verb: str,
    url: str,
    *,
    src: str,
    env: str,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Issue one UNTHROTTLED eToro request and account for it, however it ends.

    The four ``etoro_quota_lanes.KNOWN_UNTHROTTLED`` sites bypass every
    ``ResilientClient``, so they have to account for themselves.  Two rules are easy to
    get wrong one site at a time, which is why this exists instead of a recipe:

    * the record is written BEFORE the caller inspects the status, so a non-200 early
      return cannot hide the attempt that spent the quota; and
    * a raised request is recorded too, with ``status=None`` and the exception class.
      Step 2's finding was precisely that "a transport failure writes no row and still
      spends quota" -- a read timeout can arrive after eToro received the request.

    The original exception is re-raised untouched, so existing error handling is
    unchanged.

    ⚠ GET only, deliberately: all four ``KNOWN_UNTHROTTLED`` sites are GETs, and it
    dispatches through ``client.get`` rather than ``client.request`` so the call shape
    at each site is unchanged.  A raw write appearing on this path should be a
    conscious decision, not a silent one -- hence the raise rather than a wider
    dispatch.
    """
    if verb.upper() != "GET":
        raise ValueError(f"issue_raw_request covers GET only; got {verb!r}")
    issued_at = datetime.now(UTC)
    try:
        response = client.get(url, headers=headers)
    except Exception as exc:
        record_etoro_request(verb=verb, url=url, status=None, src=src, env=env, error=type(exc).__name__, ts=issued_at)
        raise
    record_etoro_request(verb=verb, url=url, status=response.status_code, src=src, env=env, ts=issued_at)
    return response


def attempt_observer(src: str, env: str) -> Callable[[RequestAttempt], None]:
    """Build the ``ResilientClient(on_attempt=...)`` callback for one eToro client."""

    def _observe(attempt: RequestAttempt) -> None:
        record_etoro_request(
            verb=attempt.method,
            url=attempt.url,
            status=attempt.status,
            src=src,
            env=env,
            error=attempt.error,
            pre_request_wait_s=attempt.pre_request_wait_s,
            attempt=attempt.attempt,
            ts=attempt.ts,
        )

    return _observe
