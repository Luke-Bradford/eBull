"""Bounded-concurrency text-fetch helper for SEC ingest paths (#726).

Sequential ``for url in urls: fetch_document_text(url)`` loops are
bottlenecked by SEC's per-request response time (~700-900ms) rather
than the rate-limit floor — at ~1 req/s, every SEC ingest job uses
about 10% of the allowed 10 req/s budget.

This helper runs ``fetch_document_text`` in a ``ThreadPoolExecutor``
so a single ingest job can keep multiple requests in flight against
SEC, overlapping response wait time. The shared
``_PROCESS_RATE_LIMIT_CLOCK`` + ``_PROCESS_RATE_LIMIT_LOCK`` in
``sec_edgar.py`` keep the aggregate request rate under SEC's
fair-use ceiling regardless of how many threads are active —
concurrency overlaps wait time, it does NOT bypass the floor.

Per-future exceptions are caught and surfaced as ``None`` results so
one bad URL cannot crash the whole batch — callers handle ``None``
the same way they handle a 404 (typically: tombstone + continue).

Worker count default 8: at 0.11s floor + 800ms response time, 8
workers is enough to saturate the rate ceiling. More workers wouldn't
help; fewer would leave throughput on the table.
"""

from __future__ import annotations

import concurrent.futures
import logging
from collections.abc import Iterable
from typing import Protocol

logger = logging.getLogger(__name__)


# Default thread count. Bigger isn't better — the floor is the
# bottleneck above this. Keeps memory + connection-pool pressure
# bounded.
DEFAULT_FETCH_WORKERS: int = 8


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
    error / per-future exception. Callers cannot tell the three
    apart from the result alone — by convention the failure modes
    map to the same downstream action (tombstone, skip, or count
    as ``fetch_errors``).

    Order is NOT preserved; callers index by URL through the
    returned dict. Duplicate URLs in ``urls`` are de-duplicated
    before submission so we never spend the rate budget twice on
    the same document.
    """
    unique = list({u for u in urls if u})
    if not unique:
        return {}

    workers = max(1, min(max_workers, len(unique)))
    out: dict[str, str | None] = {}

    def _safe_fetch(url: str) -> tuple[str, str | None]:
        try:
            return url, fetcher.fetch_document_text(url)
        except Exception:
            # Caller treats None identically to 404 — log once at
            # WARNING with the URL so the operator can grep without
            # losing the failure cause.
            logger.warning(
                "concurrent_fetch: per-future failure url=%s",
                url,
                exc_info=True,
            )
            return url, None

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="sec-fetch",
    ) as pool:
        for url, body in pool.map(_safe_fetch, unique):
            out[url] = body

    return out
