"""Bounded Anthropic SDK client factory (#1479 PR2).

The Anthropic Python SDK's default per-request timeout is **600s** on
the read/write/pool phases (connect defaults to 5s), with **two**
automatic retries. A black-holed outbound read therefore hangs a
worker thread for ~3×600s ≈ 30 min before giving up — the outbound
analogue of the unbounded ``psycopg.connect`` that the merged
``PGCONNECT_TIMEOUT`` guard (#1475) closed on the DB side. During the
2026-06-04 jobs-boot freeze this exact class of hang (a boot-reachable
``daily_research_refresh`` → ``cascade_refresh`` Anthropic call, on the
then-synchronous boot freshness sweep) wedged startup ~43 min.

This module is the **single construction site** for every
``anthropic.Anthropic`` client under ``app/``. Constructing one
anywhere else is forbidden by ``scripts/check_anthropic_timeout.sh``
(wired into the pre-push hook + CI) so a future provider can't silently
reintroduce the unbounded default.

Timeout shape rationale (these are non-streaming ``messages.create``
calls, NOT streaming — so the *read* timeout must be generous enough
not to truncate a legitimate completion, while still bounding a
black-hole):

  * ``connect=5.0`` — a reachable Anthropic endpoint completes the TLS
    handshake well under this; a dead host fails fast. (The 2026-06-04
    trace showed the hang in the *read* phase, post-handshake, so this
    was never the unbounded leg — but it is bounded for completeness.)
  * ``read=180.0`` — 3 min is comfortably above the wall-clock of any
    reasonable non-streaming completion at the token budgets these
    callers use (sentiment is 64 tokens on Haiku; thesis/research
    generations complete well under this), yet bounds a wedged read to
    minutes, not the 600s default. A call that legitimately needs more
    than this should migrate to streaming + ``get_final_message`` (the
    SDK's recommended pattern for long output) rather than widen this.
  * ``write=30.0`` / ``pool=10.0`` — request bodies are small; pool
    checkout is local.

``max_retries=1`` (down from the SDK default of 2): one retry still
absorbs a transient 429/5xx, but caps the worst-case black-hole at
~2×(read window) instead of ~3×. Callers with their own retry loop
(e.g. ``SentimentClassifier._call_with_retry``) stack on top of this —
keeping the SDK count low avoids a multiplicative retry blowup.
"""

from __future__ import annotations

import anthropic
import httpx

# Bounded per-request timeout for every app-side Anthropic call. See the
# module docstring for the per-phase rationale. Single source of truth —
# do NOT inline these literals at call sites.
ANTHROPIC_REQUEST_TIMEOUT: httpx.Timeout = httpx.Timeout(
    connect=5.0,
    read=180.0,
    write=30.0,
    pool=10.0,
)

# SDK auto-retry count. Lower than the SDK default (2) so a wedged read
# cannot compound into ~3× the read window before failing.
ANTHROPIC_MAX_RETRIES: int = 1


def make_anthropic_client(api_key: str | None = None) -> anthropic.Anthropic:
    """Construct an ``anthropic.Anthropic`` client with a bounded timeout.

    The ONE permitted ``anthropic.Anthropic(...)`` construction under
    ``app/`` (enforced by ``scripts/check_anthropic_timeout.sh``). Every
    caller routes through here so the bounded timeout + retry policy is
    applied uniformly and can never regress to the unbounded SDK default.

    ``api_key`` accepts ``None`` (the type of ``settings.anthropic_api_key``):
    the SDK then resolves the key from the ``ANTHROPIC_API_KEY`` env var,
    matching the pre-#1479 call-site behaviour exactly.
    """
    return anthropic.Anthropic(
        api_key=api_key,
        timeout=ANTHROPIC_REQUEST_TIMEOUT,
        max_retries=ANTHROPIC_MAX_RETRIES,
    )
