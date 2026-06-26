"""Manifest-driven SEC ingest worker (#869).

Issue #869 / spec §"#868 — manifest-driven worker"
(``docs/superpowers/specs/2026-05-04-etl-coverage-model.md``).

The worker scans ``sec_filing_manifest`` for rows in:

  - ``ingest_status='pending'``      (fresh discovery; awaiting fetch)
  - ``ingest_status='failed'`` AND
    ``next_retry_at <= NOW()``        (retry after backoff)

For each row, dispatches to a per-source parser callable. The parser
returns a ``ParseOutcome`` describing what happened; the worker
transitions the manifest row's state based on that outcome
(parsed / tombstoned / failed).

Parser registry: pluggable. Each per-form parser is registered via
``register_parser(source, callable)``. The legacy
``app/services/{def14a,form4,institutional_holdings,blockholder_filings}_ingest.py``
modules are NOT auto-wired here; rewiring them to feed off the
manifest is the scope of #873 (write-through observations + retire
periodic sync). Until then, the worker is a thin dispatcher whose
shape lets the rest of the ETL chain (#870 per-CIK polling, #871
first-install drain, #872 targeted rebuild) push pending rows
through to ``parsed`` end-to-end without touching the legacy
ingester batch-limit logic.

Rate budget: bounded externally — the worker passes through the SEC
10 req/s token bucket via the parser callables it dispatches to. The
worker itself imposes a per-tick row limit (``max_rows``) to keep
batch sizes predictable.
"""

from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg

from app.services.sec_manifest import (
    IngestStatus,
    ManifestRow,
    ManifestSource,
    iter_pending,
    iter_pending_recent,
    iter_pending_topup,
    iter_retryable,
    iter_retryable_topup,
    transition_status,
)

logger = logging.getLogger(__name__)

# #1685 — recent-first slice budget. Of each ``max_rows`` fairness tick, up to
# this many rows are reserved for the NEWEST pending filings (filed within
# ``RECENT_WINDOW``) per source, so recent events (8-K-class, 13D/G, DEF 14A,
# …) stay fresh regardless of the oldest-first historical backlog the main
# drain works through. The worker caps it at ``max_rows // 2`` so the backlog
# drain is never starved to zero.
RECENT_SLICE_ROWS = 30
RECENT_WINDOW = timedelta(days=90)

# #1703 — sources kept OFF the Phase B global-oldest top-up (fairness path only).
# The top-up is the ONLY phase that admits a source BEYOND its per-source quota
# (it picks global-oldest across ``sources``). ``sec_13f_hr`` is the one heavy
# source: each filing's ``infotable.xml`` fetch+parse is serial (post-primary
# retention gate, NOT prefetch-overlapped) and costs ~3-4.4s — thousands of
# holdings/filing (measured 2026-06-22: 200 oldest in-retention 13F = 878s).
# Leaving 13F in the top-up lets a drained-backlog regime flood a tick with
# heavy 13F and overrun the 5-min cadence. Excluding it caps 13F at its Phase R
# + Phase A quota share (~max_rows/n ≈ 13-16/tick at max_rows=200) — drained by
# fair quota, never by the oldest-first flood. The freed top-up budget rolls to
# other sources via the top-up SQL's own ``source = ANY(...)`` LIMIT (no
# under-fill while form4/form3 hold a backlog). The per-source rebuild path
# (``source is not None``) has no top-up phase and is unaffected. A future
# ``max_rows`` raise scales the quota share, so re-check the 13F slice cost then.
_TOPUP_EXCLUDED_SOURCES: frozenset[ManifestSource] = frozenset({"sec_13f_hr"})


# Tick counter for Phase A `lead` rotation (#1179). Module-global
# because the production scheduled tick wrapper passes
# ``tick_id=None`` and the worker must advance by exactly +1 per
# call regardless of scheduler cadence (avoids the
# ``gcd(tick_step, n) > 1`` regime that would visit only a subset
# of lead offsets). Tests inject ``tick_id`` explicitly so the
# counter is irrelevant under test.
_TICK_COUNTER = itertools.count(0)


def compute_quotas(
    sources: Sequence[ManifestSource],
    max_rows: int,
    tick_id: int,
) -> dict[ManifestSource, int]:
    """Per-source quota with tick-rotated lead (#1179).

    Returns a ``{source: slot_count}`` mapping such that
    ``sum(quotas.values()) == max_rows`` for non-empty ``sources``.
    Rotation: ``lead = tick_id % len(sources)``; the first
    ``max_rows mod n`` sources at rotated index get ``base + 1``
    slots, the rest get ``base = max_rows // n``. Every source
    receives a Phase A slot within ``n - remainder + 1`` consecutive
    ticks regardless of scheduler cadence (independent of
    ``gcd(tick_step, n)``).
    """
    n = len(sources)
    if n == 0:
        return {}
    base, remainder = divmod(max_rows, n)
    lead = tick_id % n
    return {s: base + (1 if (i - lead) % n < remainder else 0) for i, s in enumerate(sources)}


ParseStatus = Literal["parsed", "tombstoned", "failed"]


@dataclass(frozen=True)
class ParseOutcome:
    """Result of one parser invocation against a manifest row.

    The worker uses the ``status`` to drive the manifest state
    transition; ``parser_version``, ``raw_status``, ``error``, and
    ``next_retry_at`` are forwarded into ``transition_status``."""

    status: ParseStatus
    parser_version: str | None = None
    raw_status: Literal["absent", "stored", "compacted"] | None = None
    error: str | None = None
    next_retry_at: datetime | None = None


ParserFn = Callable[[psycopg.Connection[Any], ManifestRow], ParseOutcome]
"""Per-source parser contract. Receives the manifest row + a DB
connection; returns a ParseOutcome. The parser is responsible for
fetching + parsing + persisting typed-table rows. The worker handles
the manifest state transition based on the outcome."""

FetchUrlFn = Callable[[psycopg.Connection[Any], ManifestRow], str | None]
"""#1686 — optional per-source hook returning the FIRST canonical URL
the parser will GET, so the worker can prefetch it concurrently (Phase 2).
Returns ``None`` when the row has no prefetchable doc OR when any of the
parser's PRE-FETCH gates would tombstone the row (so prefetch never burns
SEC budget on a doc the parser discards — #1686 Codex ckpt-2 HIGH).

Receives the worker's DB connection (#1700) so a hook can mirror a gate
that needs a DB read (e.g. DEF 14A's latest-N-per-filer cap). The conn is
the same one the dispatch loop uses; the hook must only READ (no writes /
no commit) — it runs before the serial dispatch transaction work.

For SINGLE-primary-doc parsers (Form 3/4/5, 13D/G, DEF 14A) this URL is the
whole fetch. For MULTI-doc parsers (13F: index.json -> primary_doc.xml ->
infotable.xml) this is the FIRST doc only; the next doc is prefetched via
``expand_urls`` once the first body is in hand. The hook MUST return the
same URL the parser passes to ``fetch_document_text``; a mismatch is SAFE
(cache miss -> serial fetch, never wrong data) but yields no speedup, so
keep it in sync with the parser's canonicalizer."""

ExpandUrlsFn = Callable[[str, ManifestRow], list[str]]
"""#1700 — optional per-source SECOND-phase prefetch hook for multi-doc
parsers. Given the SUCCESSFULLY-prefetched ``fetch_url`` body (pass 1) and
the row, return additional URLs the parser will GET next, to prefetch
concurrently in pass 2. Pure (no DB). Only invoked for rows whose pass-1
URL was a cache hit (a pass-1 miss means the serial parser re-fetches from
the top, so pass 2 would be wasted).

The expander MUST NOT return a URL gated by a check the parser applies
AFTER the pass-1 doc but BEFORE that URL's fetch. 13F is the canonical
case: ``infotable.xml`` is gated by ``thirteen_f_within_retention`` on the
period parsed FROM ``primary_doc.xml`` — so the 13F expander returns
``primary_doc.xml`` ONLY (infotable stays serial, fetched live after the
parser's retention gate passes). Returning ``[]`` is always safe."""

PrefetchChainFn = Callable[[list["ManifestRow"], Any], dict[str, str]]
"""#1730 — optional per-source prefetch for an INDEPENDENT doc-chain the
pass-1/pass-2 (``fetch_url`` -> ``expand_urls``) body-keyed mechanism can't
reach. pass-2 expands the pass-1 BODY; a chain discovered from a DIFFERENT doc
(e.g. the 10-K's XBRL linkbases, discovered from the archive ``index.json`` —
independent of the primary HTML pass-1) needs its own self-contained pass.

Given the batch's rows for one source + an open ``SecFilingsProvider``, the hook
runs its OWN concurrent fetch rounds (``concurrent_fetch.fetch_document_texts``,
the same shared ≤10 req/s throttle) and returns ``{url: body}`` to merge into the
tick cache. Pure HTTP keyed on row-local fields (no DB) — it MUST mirror the
parser's pre-fetch gates for the chain (return nothing for a row the parser would
skip) so it never burns SEC budget on a doc the parser discards. A raise is
best-effort-skipped (serial parser re-fetches). Runs regardless of whether the
source registered ``fetch_url``."""


@dataclass(frozen=True)
class ParserSpec:
    """Registry entry for one ManifestSource.

    ``requires_raw_payload`` enforces the audit invariant from #938:
    payload-backed parsers (Form 4, 13F-HR, 13D/G, NPORT-P, DEF 14A)
    cannot transition a row to ``parsed`` while ``raw_status='absent'``.
    The worker turns such an outcome into a ``failed`` transition with
    a descriptive error rather than silently retaining unauditable
    rows. Synthesised / non-payload parsers leave the flag at False
    (default) and are unaffected.
    """

    fn: ParserFn
    requires_raw_payload: bool = False
    fetch_url: FetchUrlFn | None = None
    expand_urls: ExpandUrlsFn | None = None
    prefetch_chain: PrefetchChainFn | None = None


_PARSERS: dict[ManifestSource, ParserSpec] = {}


def register_parser(
    source: ManifestSource,
    parser: ParserFn,
    *,
    requires_raw_payload: bool = False,
    fetch_url: FetchUrlFn | None = None,
    expand_urls: ExpandUrlsFn | None = None,
    prefetch_chain: PrefetchChainFn | None = None,
) -> None:
    """Register a parser callable for one ManifestSource.

    Idempotent on re-registration (last-write-wins). The legacy
    ingest services will register their callables in #873 when the
    write-through wiring lands; until then, ``run_manifest_worker``
    skips rows whose source has no registered parser (logs a debug
    line per skipped row).

    ``requires_raw_payload=True`` opts the source into the #938 audit
    invariant: a ``parsed`` outcome with ``raw_status not in
    ('stored', 'compacted')`` is rejected and the row is transitioned
    to ``failed`` instead. Use for every parser that pulls upstream
    body bytes (Form 4 XML, 13F infotable, 13D/G primary doc, DEF 14A
    HTML, NPORT-P XML). Leave at the default for synthesised /
    non-payload sources."""
    _PARSERS[source] = ParserSpec(
        fn=parser,
        requires_raw_payload=requires_raw_payload,
        fetch_url=fetch_url,
        expand_urls=expand_urls,
        prefetch_chain=prefetch_chain,
    )


def _backoff_for(attempt_count: int) -> timedelta:
    """Exponential backoff for ``failed`` rows.

    Doubles per attempt, capped at 24h. We don't track attempt_count
    on the manifest yet (would need a column); for the initial cut
    we use a flat 1h backoff. ``next_retry_at`` is recomputed on each
    failure regardless."""
    return timedelta(hours=1)


@dataclass(frozen=True)
class WorkerStats:
    """Per-tick summary for observability."""

    rows_processed: int
    parsed: int
    tombstoned: int
    failed: int
    skipped_no_parser: int
    raw_payload_violations: int = 0
    # Per-source breakdown of ``skipped_no_parser``. Operators
    # reading this from job_runs / a future status endpoint can see
    # exactly which manifest sources are dropping work because no
    # parser is registered (#940). Empty when ``skipped_no_parser=0``.
    skipped_no_parser_by_source: dict[ManifestSource, int] = field(default_factory=dict)
    # Per-source breakdown of dispatched rows (#1179). Bumped once
    # per row reaching the parser dispatch entry point (i.e. rows
    # that exit via parsed / tombstoned / failed /
    # raw_payload_violations). Sum equals
    # ``rows_processed - skipped_no_parser``.
    processed_by_source: dict[ManifestSource, int] = field(default_factory=dict)


def run_manifest_worker(
    conn: psycopg.Connection[Any],
    *,
    source: ManifestSource | None = None,
    max_rows: int = 100,
    now: datetime | None = None,
    tick_id: int | None = None,
) -> WorkerStats:
    """One worker tick: drain pending + retryable manifest rows.

    Two paths:

    - ``source is None`` (scheduled tick): per-source Phase A slice
      via :func:`compute_quotas` + Phase B residual top-up. This is
      the fairness path (#1179) that prevents the globally-oldest
      source from starving every other source. ``tick_id`` rotates
      Phase A's lead window by +1 per tick — defaults to a
      process-local :data:`_TICK_COUNTER` for production callers;
      tests inject explicitly.
    - ``source is not None`` (per-source rebuild): unchanged shape;
      drains ``max_rows`` from one source (pending then retryable).

    Returns a :class:`WorkerStats` summary including a
    ``processed_by_source`` per-source breakdown.
    """
    # Normalise ``now`` BEFORE branching so the dispatch helper
    # always has a tz-aware UTC value for parser-exception +
    # raw-payload-violation backoff math (``now + _backoff_for(0)``).
    if now is None:
        now = datetime.now(tz=UTC)

    if source is not None:
        # Per-source rebuild path (sec_rebuild, scoped operator re-drain).
        rows: list[ManifestRow] = list(iter_pending(conn, source=source, limit=max_rows))
        if len(rows) < max_rows:
            rows.extend(iter_retryable(conn, source=source, limit=max_rows - len(rows)))
        # #1591 Part 2 — prefetch this batch's bodies concurrently against the
        # shared SEC throttle before the serial dispatch, same as the fairness
        # path. The dominant re-drain cost is the per-row SEC fetch
        # (~4.7s/row, ~1 req/s observed in the #554 backlog); overlapping the
        # fetches saturates the 10 req/s floor. No-op for sources without a
        # ``fetch_url`` hook (empty cache → identical to the old serial path).
        return _prefetch_then_dispatch(conn, rows, now=now)

    # Fairness path (#1179) — Phase R recent-first slice (#1685) +
    # Phase A per-source oldest slice + Phase B global oldest top-up.
    sources = sorted(registered_parser_sources())
    n = len(sources)
    if n == 0:
        return WorkerStats(
            rows_processed=0,
            parsed=0,
            tombstoned=0,
            failed=0,
            skipped_no_parser=0,
        )

    if tick_id is None:
        tick_id = next(_TICK_COUNTER)

    rows: list[ManifestRow] = []
    seen: set[str] = set()

    # Phase R (#1685) — recent-first slice. Reserve a bounded budget for the
    # NEWEST pending filings (within RECENT_WINDOW) per source, allocated with
    # the same #1179 rotation, newest first. ``max_rows // 2`` guarantees the
    # backlog budget below stays > 0 even for a small ``max_rows`` (Codex
    # ckpt-1: a base-zero backlog would collapse the fairness rotation).
    recent_budget = min(RECENT_SLICE_ROWS, max_rows // 2)
    if recent_budget > 0:
        recent_cutoff = now - RECENT_WINDOW
        recent_quotas = compute_quotas(sources, recent_budget, tick_id)
        for s in sources:
            q = recent_quotas[s]
            if q == 0:
                continue
            recent = list(iter_pending_recent(conn, source=s, since=recent_cutoff, limit=q))
            rows.extend(recent)
            seen.update(r.accession_number for r in recent)

    # Phase A — per-source oldest-first slice over whatever budget Phase R did
    # NOT consume (unused recent budget rolls into the backlog). The pending
    # pick is filtered against ``seen`` so a tiny all-recent source cannot have
    # a row dispatched twice in one tick (Codex ckpt-1: the only overlap case —
    # for a large backlog Phase R's newest prefix and Phase A's oldest prefix
    # are disjoint). Retryable rows are ``failed`` so can never collide with the
    # ``pending`` Phase R picks — no filter needed there.
    backlog_budget = max_rows - len(rows)
    quotas = compute_quotas(sources, backlog_budget, tick_id)
    for s in sources:
        q = quotas[s]
        if q == 0:
            continue
        per_source: list[ManifestRow] = [
            r for r in iter_pending(conn, source=s, limit=q) if r.accession_number not in seen
        ]
        if len(per_source) < q:
            per_source.extend(iter_retryable(conn, source=s, limit=q - len(per_source)))
        rows.extend(per_source)
        seen.update(r.accession_number for r in per_source)

    # Phase B — top-up pending, then retryable, both scoped to
    # registered sources, excluding everything already picked. #1703:
    # ``_TOPUP_EXCLUDED_SOURCES`` (13F) are dropped here so the
    # global-oldest flood cannot push a heavy source past its quota —
    # those sources are drained by Phase R + Phase A only. The freed
    # budget rolls to the remaining sources via the top-up SQL's own
    # ``source = ANY(...)`` LIMIT.
    topup_sources: list[ManifestSource] = [s for s in sources if s not in _TOPUP_EXCLUDED_SOURCES]
    remaining = max_rows - len(rows)
    if remaining > 0:
        topup_pending = list(
            iter_pending_topup(
                conn,
                sources=topup_sources,
                exclude_accessions=sorted(seen),
                limit=remaining,
            )
        )
        rows.extend(topup_pending)
        seen.update(r.accession_number for r in topup_pending)
        remaining = max_rows - len(rows)
    if remaining > 0:
        topup_retryable = list(
            iter_retryable_topup(
                conn,
                sources=topup_sources,
                exclude_accessions=sorted(seen),
                limit=remaining,
            )
        )
        rows.extend(topup_retryable)

    # Fairness path = steady-state backlog drain → prefetch bodies concurrently.
    return _prefetch_then_dispatch(conn, rows, now=now)


def _prefetch_bodies(
    conn: psycopg.Connection[Any],
    rows: list[ManifestRow],
) -> dict[str, str]:
    """#1686/#1700 Phase 2 — concurrently fetch parser bodies for ``rows``.

    Two passes so multi-doc parsers (13F) get concurrency too:

    - **Pass 1:** for each row whose source registered a ``fetch_url`` hook,
      resolve the FIRST canonical URL (the hook mirrors the parser's
      pre-fetch gates, reading ``conn`` if needed) and fetch all of them
      concurrently (bounded threadpool, shared SEC throttle ≤10 req/s).
    - **Pass 2:** for each row whose source also registered ``expand_urls``
      AND whose pass-1 URL was a SUCCESSFUL fetch, call the expander with
      that body to get the NEXT URLs (e.g. 13F's ``primary_doc.xml`` once
      ``index.json`` is in hand), then fetch those concurrently too.

    Returns ``{url: body}`` for SUCCESSFUL fetches across both passes ONLY —
    a ``None`` (404 or caught exception, indistinguishable through
    ``fetch_document_texts``) is dropped so the serial parse path re-fetches
    it and keeps its retry/tombstone discrimination (#1698 / Codex ckpt-1
    HIGH). A pass-1 miss skips that row's pass-2 entirely (the serial parser
    re-fetches from the top, so pass-2 work would be wasted).

    Returns ``{}`` when no row has a hook — the caller then skips the cache
    entirely and the tick behaves exactly as pre-#1686.
    """
    # row index keyed by pass-1 URL so pass 2 can find the rows whose
    # first doc was fetched successfully (multiple rows can share a URL in
    # principle; we keep them all so each gets its expander invoked).
    pass1_url_to_rows: dict[str, list[ManifestRow]] = defaultdict(list)
    for row in rows:
        spec = _PARSERS.get(row.source)
        if spec is None or spec.fetch_url is None:
            continue
        # A hook raising (e.g. DEF 14A's cap query hitting a DB error) must
        # NOT abort the tick — prefetch is best-effort. Run it inside a
        # SAVEPOINT (``conn.transaction()``) so a raised DB error rolls back
        # to the savepoint, leaving the shared connection's outer transaction
        # usable for the remaining hooks + the serial ``_dispatch_rows``
        # path; without it the conn would be stuck in InFailedSqlTransaction
        # and every later DB op would fail (Codex ckpt-2 P2 + P1). Skip this
        # row's prefetch on any exception — the serial parser still runs and
        # its per-row failure handling applies.
        try:
            with conn.transaction():
                url = spec.fetch_url(conn, row)
        except Exception:
            logger.exception(
                "manifest prefetch: fetch_url hook raised for source=%s accession=%s; "
                "skipping prefetch (serial parser will handle it)",
                row.source,
                row.accession_number,
            )
            continue
        if url:
            pass1_url_to_rows[url].append(row)

    # #1730 — rows whose source registered a self-contained INDEPENDENT-doc-chain
    # prefetch (e.g. the 10-K XBRL linkbases, discovered from index.json — NOT
    # reachable via the primary-HTML-keyed pass-2 expander). Collected separately
    # so the chain runs even when NO source registered a ``fetch_url`` (pass-1
    # empty) — the early return below must not skip a chain-only source.
    chain_rows_by_source: dict[ManifestSource, list[ManifestRow]] = defaultdict(list)
    for row in rows:
        spec = _PARSERS.get(row.source)
        if spec is not None and spec.prefetch_chain is not None:
            chain_rows_by_source[row.source].append(row)

    if not pass1_url_to_rows and not chain_rows_by_source:
        return {}

    # Lazy imports — keep the provider/HTTP + config deps off the worker's
    # import-time path (also avoids any import cycle through the providers).
    from app.config import settings
    from app.providers.concurrent_fetch import fetch_document_texts
    from app.providers.implementations.sec_edgar import SecFilingsProvider

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        pass1 = fetch_document_texts(provider, set(pass1_url_to_rows)) if pass1_url_to_rows else {}
        cache: dict[str, str] = {url: body for url, body in pass1.items() if body is not None}

        # Pass 2 — expand only rows whose pass-1 doc was fetched OK.
        pass2_urls: set[str] = set()
        for url, body in cache.items():
            for row in pass1_url_to_rows.get(url, ()):
                spec = _PARSERS.get(row.source)
                if spec is None or spec.expand_urls is None:
                    continue
                # A malformed pass-1 body that makes the expander /
                # parse_archive_index raise must NOT abort the tick — skip
                # this row's pass-2 expansion so the serial parser re-parses
                # the (cached) body and applies its own tombstone/failure
                # path (Codex ckpt-2 P2).
                try:
                    extra = spec.expand_urls(body, row)
                except Exception:
                    logger.exception(
                        "manifest prefetch: expand_urls hook raised for source=%s accession=%s; "
                        "skipping pass-2 prefetch (serial parser will handle it)",
                        row.source,
                        row.accession_number,
                    )
                    continue
                pass2_urls.update(u for u in extra if u)
        # Don't re-fetch anything already cached in pass 1.
        pass2_urls -= cache.keys()
        if pass2_urls:
            pass2 = fetch_document_texts(provider, pass2_urls)
            cache.update({url: body for url, body in pass2.items() if body is not None})

        # #1730 — independent-doc-chain prefetch (e.g. 10-K XBRL linkbases). Each
        # hook runs its OWN concurrent fetch rounds against the shared throttle and
        # returns {url: body}; merge into the cache. Best-effort — a raise must NOT
        # abort the tick (the serial parser re-fetches), mirroring the pass-1/pass-2
        # hook handling. Pure HTTP (no DB), so no savepoint wrapper.
        for source, chain_rows in chain_rows_by_source.items():
            spec = _PARSERS.get(source)
            if spec is None or spec.prefetch_chain is None:
                continue
            try:
                extra_bodies = spec.prefetch_chain(chain_rows, provider)
            except Exception:
                logger.exception(
                    "manifest prefetch: prefetch_chain hook raised for source=%s "
                    "(%d rows); skipping chain prefetch (serial parser will handle it)",
                    source,
                    len(chain_rows),
                )
                continue
            cache.update({url: body for url, body in extra_bodies.items() if body is not None})

    return cache


def _prefetch_then_dispatch(
    conn: psycopg.Connection[Any],
    rows: list[ManifestRow],
    *,
    now: datetime,
) -> WorkerStats:
    """#1686 Phase 2 — prefetch single-doc bodies concurrently, bind the
    tick-scoped cache, then run the serial per-row :func:`_dispatch_rows`.

    Used by BOTH worker paths: the steady-state fairness path (``source is
    None`` — the every-5-min cron + boot catch-up, the original #1686 target)
    AND, since #1591 Part 2, the per-source rebuild path (``sec_rebuild``,
    scoped operator re-drain). The re-drain's dominant cost is the per-row SEC
    fetch (#554: ~4.7s/row, ~1 req/s against a 10 req/s floor); overlapping
    the batch's fetches saturates the floor. Born-compacted sources (10-K/8-K)
    benefit most — their body cannot be reused (#1591 PR1) so it must be
    (re-)fetched, and concurrency is their only lever.

    Accepted tradeoff (Codex ckpt-1 #5 + ckpt-2): prefetching the whole batch
    up front widens the window between row-select and the parser's transition,
    so if another drainer finalises a row in that window this worker re-parses
    the stale row (rate-budget + redundant-write cost; the writes are
    idempotent via the filed_at gate / replace-then-insert). The manifest
    transition that follows:
      * same-status collision (drainer B also parses → ``parsed -> parsed``,
        the common case) is absorbed by an idempotent no-op
        (``sec_manifest.transition_status``, #1591 Part 2, mirroring the #1686
        ``tombstoned -> tombstoned`` no-op) — no raise.
      * cross-terminal collision (B's redundant re-parse returns
        failed/tombstoned where A committed parsed — needs a TRANSIENT fetch
        discrepancy in the window) still raises ``illegal transition``. It is
        NOT made a no-op on purpose: that would have to allow ``parsed ->
        failed``, masking the single-drainer bug ``test_illegal_transition_
        raises`` guards. This path is caught by the standalone drain's
        broad-except rollback-retry (``scripts/drain_554_sec10k.py``) — the
        only context that creates concurrent drainers.
    Production runs a SINGLETON fairness tick (``scheduler.py`` max-instances
    =1) and the per-source path has no scheduled caller, so production never
    races; any non-finalised row simply stays pending and re-drains. No path
    corrupts data.

    The cache is ALWAYS bound (even when empty, so a prior tick's value can
    never leak across ticks on the apscheduler threadpool) and reset in
    ``finally`` so an exception can't strand it. With no ``fetch_url`` hooks
    the cache is empty → every ``fetch_document_text`` misses → behaviour is
    identical to pre-#1686.
    """
    from app.providers.implementations.sec_edgar import (
        reset_prefetch_body_cache,
        set_prefetch_body_cache,
    )

    cache = _prefetch_bodies(conn, rows)
    token = set_prefetch_body_cache(cache)
    try:
        return _dispatch_rows(conn, rows, now=now)
    finally:
        reset_prefetch_body_cache(token)


def _dispatch_rows(
    conn: psycopg.Connection[Any],
    rows: list[ManifestRow],
    *,
    now: datetime,
) -> WorkerStats:
    """Per-row dispatch loop shared by both worker paths.

    For each row: skip if no parser registered, else invoke parser
    and translate :class:`ParseOutcome` into a ``transition_status``
    call. Parser-internal exceptions → ``failed`` + 1h backoff.
    #938 raw-payload audit invariant fires here (payload-backed
    parsers cannot transition ``parsed + raw_status='absent'``).

    Returns a :class:`WorkerStats` summary; the caller has already
    decided WHICH rows to dispatch (fairness allocation or per-source
    rebuild).
    """
    parsed = 0
    tombstoned = 0
    failed = 0
    skipped = 0
    raw_violations = 0
    skipped_by_source: dict[ManifestSource, int] = defaultdict(int)
    processed_by_source: dict[ManifestSource, int] = defaultdict(int)

    for row in rows:
        spec = _PARSERS.get(row.source)
        if spec is None:
            logger.debug(
                "manifest worker: no parser registered for source=%s; skipping accession=%s",
                row.source,
                row.accession_number,
            )
            skipped += 1
            skipped_by_source[row.source] += 1
            continue

        # #1179: bump processed-by-source ONCE per dispatched row,
        # BEFORE parser invocation. Every code path below exits via
        # parsed / tombstoned / failed / raw-payload-violation — the
        # counter must not double-count for the raw-violation path
        # (which writes both ``failed += 1`` and ``raw_violations += 1``).
        processed_by_source[row.source] += 1

        try:
            outcome = spec.fn(conn, row)
        except Exception as exc:  # parser-internal failure — fail loudly
            logger.exception(
                "manifest worker: parser raised for source=%s accession=%s",
                row.source,
                row.accession_number,
            )
            transition_status(
                conn,
                row.accession_number,
                ingest_status="failed",
                error=f"{type(exc).__name__}: {exc}"[:500],
                next_retry_at=now + _backoff_for(0),
            )
            failed += 1
            continue

        # #938 audit invariant: payload-backed parsers cannot transition
        # to ``parsed`` while the row's effective raw_status is
        # ``absent``. Convert to a ``failed`` transition with a
        # descriptive error so the row remains visible to the operator
        # + retry path. Silent ``parsed + absent`` would leave an
        # unauditable row in the manifest forever.
        #
        # Effective raw_status falls back to the row's existing value
        # when the parser doesn't restamp (``outcome.raw_status is
        # None``). This matches ``transition_status`` semantics — a
        # ``parsed`` transition with ``raw_status=None`` preserves the
        # row's existing column — so a rebuild/retry flow where raw
        # evidence already exists on disk doesn't get misclassified
        # as a violation. (Codex pre-push catch.)
        effective_raw_status = outcome.raw_status or row.raw_status
        if (
            outcome.status == "parsed"
            and spec.requires_raw_payload
            and effective_raw_status not in ("stored", "compacted")
        ):
            logger.error(
                "manifest worker: source=%s accession=%s parser returned parsed but "
                "effective raw_status=%r — payload-backed parsers must persist evidence; "
                "transitioning to failed for retry",
                row.source,
                row.accession_number,
                effective_raw_status,
            )
            transition_status(
                conn,
                row.accession_number,
                ingest_status="failed",
                error=(
                    "raw payload missing: parser returned parsed without storing "
                    f"the upstream body (effective raw_status={effective_raw_status!r}). "
                    "Payload-backed parsers must persist evidence (#938)."
                ),
                next_retry_at=now + _backoff_for(0),
            )
            failed += 1
            raw_violations += 1
            continue

        target_status: IngestStatus = outcome.status
        transition_status(
            conn,
            row.accession_number,
            ingest_status=target_status,
            parser_version=outcome.parser_version,
            raw_status=outcome.raw_status,
            error=outcome.error,
            next_retry_at=outcome.next_retry_at if outcome.status == "failed" else None,
        )
        if outcome.status == "parsed":
            parsed += 1
        elif outcome.status == "tombstoned":
            tombstoned += 1
        else:
            failed += 1

    # #940: surface no-parser drops at WARNING level with per-source
    # breakdown. Per-row debug logs above let operators dig in if
    # needed; the once-per-tick summary is the loud signal that real
    # work is being silently dropped because no parser is registered.
    if skipped:
        logger.warning(
            "manifest worker: skipped %d row(s) with no registered parser; per-source counts: %s",
            skipped,
            dict(sorted(skipped_by_source.items())),
        )

    return WorkerStats(
        rows_processed=len(rows),
        parsed=parsed,
        tombstoned=tombstoned,
        failed=failed,
        skipped_no_parser=skipped,
        raw_payload_violations=raw_violations,
        skipped_no_parser_by_source=dict(skipped_by_source),
        processed_by_source=dict(processed_by_source),
    )


def clear_registered_parsers() -> None:
    """Test helper — wipe the parser registry between cases. NOT for
    production code paths. The registry is module-global so tests that
    register fakes leak into subsequent tests without this hook."""
    _PARSERS.clear()


def registered_parser_sources() -> frozenset[ManifestSource]:
    """Return the set of ``ManifestSource`` values that have a parser
    registered with the worker right now.

    #935 §5: the audit endpoint at ``/coverage/manifest-parsers``
    reads this to flag manifest rows whose source has no parser and
    would therefore be silently debug-skipped on every worker tick.
    Returning a ``frozenset`` keeps the registry read-only at the
    caller boundary.
    """
    return frozenset(_PARSERS.keys())
