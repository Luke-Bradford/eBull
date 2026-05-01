"""Bounded-concurrency fetch helpers for SEC ingest paths (#726, #761).

Sequential ``for url in urls: fetch(url)`` loops are bottlenecked by
SEC's per-request response time (~700-900ms) rather than the
rate-limit floor — at ~1 req/s, every SEC ingest job uses about 10%
of the allowed 10 req/s budget.

This module is the single home for SEC concurrency. Every ingest path
that fans out per-CIK / per-URL fetches against SEC routes through
``concurrent_map`` so the throttle is honoured consistently and the
threadpool semantics live in one place rather than being reinvented in
each ingester.

The shared ``_PROCESS_RATE_LIMIT_CLOCK`` + ``_PROCESS_RATE_LIMIT_LOCK``
in ``sec_edgar.py`` keep the aggregate request rate under SEC's
fair-use ceiling regardless of how many threads are active —
concurrency overlaps wait time, it does NOT bypass the floor.

Per-future exceptions are caught and surfaced as ``None`` results so
one bad item cannot crash the whole batch — callers handle ``None``
the same way they handle a 404 (typically: tombstone + continue).

Worker count default 8: at 0.11s floor + 800ms response time, 8
workers is enough to saturate the rate ceiling. More workers wouldn't
help; fewer would leave throughput on the table.
"""

from __future__ import annotations

import concurrent.futures
import logging
from collections.abc import Callable, Iterable, Iterator
from typing import Protocol

logger = logging.getLogger(__name__)


# Default thread count. Bigger isn't better — the floor is the
# bottleneck above this. Keeps memory + connection-pool pressure
# bounded.
DEFAULT_FETCH_WORKERS: int = 8


def concurrent_map[T, R](
    fn: Callable[[T], R | None],
    items: Iterable[T],
    *,
    max_workers: int = DEFAULT_FETCH_WORKERS,
    log_label: str = "concurrent_map",
) -> list[tuple[T, R | None]]:
    """Run ``fn`` over ``items`` in a bounded threadpool, return
    ``[(item, result_or_None), ...]`` in submission order.

    Per-item exceptions are caught and converted to ``None`` results
    so one failure cannot abort the batch. The caller treats ``None``
    the same way they would a 404 — typically log + skip.

    .. note::
       The result channel is **lossy**: a ``None`` could mean either
       "``fn`` raised and was caught" or "``fn`` legitimately returned
       ``None``" (e.g. SEC 404). Callers that need to distinguish
       must wrap their fetcher to return a discriminated result type
       (e.g. ``("ok", value)`` vs ``("missing", None)``).

    Order in the returned list matches submission order — callers can
    zip against the input items list when they need to. Duplicate
    items are NOT de-duplicated here (caller's responsibility); the
    text-fetch wrapper below does dedupe by URL.

    Memory: the full result list is materialised before return — for
    streaming consumers (memory bounded by ``max_workers``) use
    :func:`concurrent_iter` instead.

    The shared SEC throttle clock in ``sec_edgar.py`` keeps aggregate
    HTTP rate under 10 req/s regardless of worker count. ``fn`` is
    expected to be the call that actually issues the HTTP request —
    putting heavy CPU work inside ``fn`` ties up worker slots and
    starves throughput.
    """
    items_list = list(items)
    if not items_list:
        return []

    workers = max(1, min(max_workers, len(items_list)))

    def _safe(item: T) -> tuple[T, R | None]:
        try:
            return item, fn(item)
        except Exception:
            logger.warning(
                "%s: per-future failure item=%r",
                log_label,
                item,
                exc_info=True,
            )
            return item, None

    out: list[tuple[T, R | None]] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="sec-fetch",
    ) as pool:
        # ``pool.map`` preserves submission order.
        for item, result in pool.map(_safe, items_list):
            out.append((item, result))
    return out


def concurrent_iter[T, R](
    fn: Callable[[T], R | None],
    items: Iterable[T],
    *,
    max_workers: int = DEFAULT_FETCH_WORKERS,
    log_label: str = "concurrent_iter",
) -> Iterator[tuple[T, R | None]]:
    """Streaming variant of :func:`concurrent_map` — yields
    ``(item, result_or_None)`` pairs as workers complete.

    Use this when the consumer can act on each result independently
    (e.g. fetch+parse in parallel, then upsert serially as each
    payload arrives). Memory is bounded by ``max_workers`` rather
    than the full input length, so it scales to large universes
    without buffering every parsed payload.

    Order is **completion order**, not submission order — fast
    fetches return first. Callers that need submission order should
    use ``concurrent_map`` (which materialises the full list) or
    sort the yielded pairs themselves.

    Same lossy ``None`` semantics + same shared-throttle behaviour
    as ``concurrent_map``.
    """
    items_list = list(items)
    if not items_list:
        return

    workers = max(1, min(max_workers, len(items_list)))

    def _safe(item: T) -> tuple[T, R | None]:
        try:
            return item, fn(item)
        except Exception:
            logger.warning(
                "%s: per-future failure item=%r",
                log_label,
                item,
                exc_info=True,
            )
            return item, None

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="sec-fetch",
    ) as pool:
        futures = [pool.submit(_safe, item) for item in items_list]
        for fut in concurrent.futures.as_completed(futures):
            yield fut.result()


# ---------------------------------------------------------------------------
# Text-fetch wrapper (legacy entry point)
# ---------------------------------------------------------------------------


class _TextFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


def fetch_document_texts(
    fetcher: _TextFetcher,
    urls: Iterable[str],
    *,
    max_workers: int = DEFAULT_FETCH_WORKERS,
) -> dict[str, str | None]:
    """Fetch every URL concurrently and return a ``{url: body}`` map.

    ``body`` is the response text on a 2xx, ``None`` on 404 / fetch
    error / per-future exception. Order is NOT preserved; callers
    index by URL through the returned dict. Duplicate URLs are
    de-duplicated before submission so we never spend the rate budget
    twice on the same document.

    Implemented in terms of :func:`concurrent_map` — same throttle-
    sharing semantics, single home for the threadpool plumbing.
    """
    unique = list({u for u in urls if u})
    pairs = concurrent_map(
        fetcher.fetch_document_text,
        unique,
        max_workers=max_workers,
        log_label="fetch_document_texts",
    )
    return {url: body for url, body in pairs}
