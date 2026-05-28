"""C1.b — per-CIK ``filings.files[]`` secondary-pages walker (#1027,
rewritten in Stream A PR-B T1.3 #1233 to consume the
``sec_cik_submissions_files_index`` sidecar instead of re-fetching the
primary submissions.json).

Item 7 (#1233 ``docs/proposals/etl/run-8-readiness-fixes.md``):
each secondary-page fetch rounds the SEC ``Last-Modified`` header
through ``external_data_watermarks`` under source-key
``sec.last_modified.submissions_files`` with key ``<cik>:<page_name>``.
On 304 the walker skips parse + upsert (filings already known) but
bumps ``watermark_at`` so the watermark row stays fresh.

The bulk ``submissions.zip`` archive published by SEC contains every
CIK's ``filings.recent`` block (last ~12 months / 1000 most-recent
filings). For deeper history, SEC paginates older filings under
``filings.files[]`` — secondary JSON URLs the bulk archive does NOT
include.

C1.a (bulk submissions ingester, ``sec_submissions_ingest``) reads the
bulk archive's ``recent`` block AND — since Stream A PR-B — populates
``sec_cik_submissions_files_index`` from the same in-memory payload.
C1.b walks the per-CIK page descriptors from the sidecar and fetches
each secondary page over HTTP, bounded by the per-CIK rate budget.

Pre-PR-B, this walker re-fetched the primary submissions.json for every
in-universe CIK JUST to read ``filings.files[]`` again — even though
C1.a already had that data minutes earlier. The sidecar eliminates
those ~5,105 redundant primary fetches (~12 min wall-clock at SEC's
7 req/s budget). Secondary pages are STILL fetched over HTTP (they
are NOT in the bulk archive — confirmed at the spec §0.5 grep proof
referencing this file's pre-PR-B docstring).

Sentinel-row behaviour (Codex v2 BLOCKING + spec §4):
  * CIK with ≥ 1 real sidecar page → walk those pages.
  * CIK with exactly the sentinel ``__no_overflow_pages__`` row → skip
    secondary walk; CIK is "processed with zero overflow" (e.g. AAPL
    whose ``recent`` array fits the 1000-cap).
  * CIK with zero sidecar rows (in-universe, not an agent CIK) →
    fail-closed; surface as a parse-error and continue with the rest
    of the cohort. Indicates an S8 ordering bug or a CIK added to
    universe after S8 ran.
  * CIK in ``KNOWN_FILING_AGENT_CIKS`` → not in the populated set;
    skipped at sidecar-populate time. S14 also skips them at consume
    so URL-construction never targets an agent CIK (would 404 every
    time per ``sec-edgar.md §3.7``).

Spec: docs/proposals/etl/stream-a-run-8-fixes.md v2.3 §1 T1.3 + §13 + §14
(post-Codex-1 re-pass + 3-lens code review 2026-05-24).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import (
    KNOWN_FILING_AGENT_CIKS,
    SecFilingsProvider,
    _normalise_submissions_block,
)
from app.services.bootstrap_state import (
    resolve_progress_context,
    set_stage_processed,
    set_stage_target,
)
from app.services.filings import _upsert_filing
from app.services.sec_pipelined_fetcher import (
    DEFAULT_PREFETCH_CHUNK_SIZE,
    ConditionalFetchTask,
    _CachedSubmissionsPageFetcher,
    prefetch_submissions_pages_conditional,
)
from app.services.watermarks import set_watermark

logger = logging.getLogger(__name__)


# Item 7 (#1233): dedicated source-key namespace for HTTP Last-Modified
# round-trip on secondary submissions pages. See
# ``app/services/watermarks.py`` module docstring §Source-key
# namespaces in use. Key format: ``<cik>:<page_name>``.
_SOURCE_KEY_SUBMISSIONS_FILES: str = "sec.last_modified.submissions_files"


# Stream A PR-B T1.3 (#1233): sentinel page-name pattern written by
# the sidecar populate in sec_submissions_ingest.refresh_cik_sidecar.
# Distinguishes "CIK processed; no overflow pages" from "CIK not yet
# populated".
_SIDECAR_SENTINEL_PAGE_NAME: str = "__no_overflow_pages__"


@dataclass
class FilesWalkResult:
    """Per-CIK walk telemetry."""

    ciks_visited: int = 0
    secondary_pages_fetched: int = 0
    filings_upserted: int = 0
    parse_errors: int = 0
    # Stream A PR-B T1.3 (#1233): per-CIK sidecar telemetry.
    # ``ciks_with_no_overflow`` counts CIKs that had only the sentinel
    # row (zero secondary pages — skipped without HTTP). Closed-set
    # bookkeeping per spec §15 to keep ``parse_errors`` from
    # double-counting legitimate "no-overflow" CIKs as errors.
    ciks_with_no_overflow: int = 0
    ciks_with_empty_sidecar: int = 0
    # Item 7 (#1233 run-8-readiness): HTTP 304 short-circuits saved
    # via If-Modified-Since round-trip. Distinct from parse_errors —
    # 304 is a success path that conserves the SEC 10 req/s budget.
    secondary_pages_not_modified: int = 0
    # #1341 — pipelined prefetch telemetry (bootstrap-mode only).
    # ``prefetch_pages_seeded`` counts unique pages successfully
    # prefetched (size of returned cache dicts summed across chunks).
    # ``loop_pages_from_prefetch`` / ``loop_pages_from_sync_fallback``
    # count per-loop visits (may exceed seeded count for share-class
    # siblings sharing CIK pages — these are loop consumptions, not
    # unique fetches saved). ``prefetch_window_seconds_total`` sums
    # per-chunk prefetch wall-clock deltas; ``None`` in steady-state.
    prefetch_pages_seeded: int = 0
    loop_pages_from_prefetch: int = 0
    loop_pages_from_sync_fallback: int = 0
    prefetch_window_seconds_total: float | None = None


def _list_cik_secondary_pages(
    conn: psycopg.Connection[Any],
) -> list[tuple[int, str, str, list[str]]]:
    """Return ``[(instrument_id, cik_padded, symbol, sidecar_pages)]`` for every
    CIK-mapped universe instrument.

    Joins ``external_identifiers`` to ``instruments`` so the writer
    below threads the canonical ticker through to
    ``_normalise_submissions_block`` + ``_upsert_filing`` (Codex review
    BLOCKING for PR #1035, parity with the same fix in PR #1030 / C1.a).

    Stream A PR-B (#1233): also LEFT JOINs ``sec_cik_submissions_files_index``
    and aggregates the per-CIK page list. The aggregate preserves the
    sentinel page name when present so callers can distinguish:

      * ``sidecar_pages == []``                               — empty sidecar (fail-closed)
      * ``sidecar_pages == ['__no_overflow_pages__']``         — processed; no overflow
      * ``sidecar_pages == [<one or more real CIK*-...json>]`` — overflow exists; walk

    NOT NULL-safe: a row in ``sec_cik_submissions_files_index`` is
    always either a sentinel row OR a row with a non-NULL page_name
    matching the regex CHECK (sql/172). ``array_agg`` on a NOT NULL
    column never injects NULL into the aggregate.
    """
    out: list[tuple[int, str, str, list[str]]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ei.instrument_id,
                ei.identifier_value,
                i.symbol,
                COALESCE(
                    array_agg(s.page_name) FILTER (WHERE s.page_name IS NOT NULL),
                    ARRAY[]::TEXT[]
                )
            FROM external_identifiers ei
            JOIN instruments i
              ON i.instrument_id = ei.instrument_id
            LEFT JOIN sec_cik_submissions_files_index s
              -- LPAD is defensive: sec_identity.py:44 normalises CIKs to
              -- 10-digit zero-padded BEFORE writing to external_identifiers
              -- (and the sidecar's own CHECK enforces the 10-digit shape on
              -- the s.cik side), so the LPAD is a no-op for any row written
              -- by current code. Kept against the historical-data case
              -- where pre-sec_identity.py:44 inserts may carry unpadded
              -- values. Negligible perf cost at 12k-CIK cohort (hash-join).
              ON s.cik = LPAD(ei.identifier_value, 10, '0')
            WHERE ei.provider = 'sec'
              AND ei.identifier_type = 'cik'
              AND i.is_tradable = TRUE
            GROUP BY ei.instrument_id, ei.identifier_value, i.symbol
            """,
        )
        for row in cur.fetchall():
            instrument_id, identifier, symbol, sidecar_pages = row
            cik = str(identifier).zfill(10)
            pages_list: list[str] = list(sidecar_pages or [])
            out.append((int(instrument_id), cik, str(symbol or ""), pages_list))
    return out


def _load_all_watermarks_for_pages(
    conn: psycopg.Connection[Any],
    targets: list[tuple[int, str, str, list[str]]],
) -> dict[tuple[str, str], str | None]:
    """One SELECT → ``{(cik, page_name): if_modified_since}`` for the
    whole cohort. Replaces ~17k per-page ``get_watermark`` round-trips
    inside the walker loop (#1341 §6).

    Missing rows map to ``None`` (no prior watermark — full fetch).
    """
    keys: list[str] = []
    for _iid, cik, _sym, sidecar_pages in targets:
        for page_name in sidecar_pages:
            if page_name == _SIDECAR_SENTINEL_PAGE_NAME:
                continue
            keys.append(f"{cik}:{page_name}")
    out: dict[tuple[str, str], str | None] = {}
    if not keys:
        return out
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT key, watermark
            FROM external_data_watermarks
            WHERE source = %s
              AND key = ANY(%s)
            """,
            (_SOURCE_KEY_SUBMISSIONS_FILES, keys),
        )
        for row in cur.fetchall():
            wm_key, watermark = row[0], row[1]
            cik_part, _, page_part = str(wm_key).partition(":")
            out[(cik_part, page_part)] = watermark if watermark else None
    return out


def _chunked(
    seq: list[tuple[int, str, str, str]],
    size: int,
) -> Iterator[list[tuple[int, str, str, str]]]:
    """Yield successive `size`-sized slices of ``seq``. Last slice may
    be shorter. Empty ``seq`` yields nothing."""
    if size < 1:
        raise ValueError("size must be >= 1")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _process_one_page(
    conn: psycopg.Connection[Any],
    *,
    provider: SecFilingsProvider | _CachedSubmissionsPageFetcher,
    instrument_id: int,
    cik: str,
    symbol: str,
    page_name: str,
    if_modified_since: str | None,
    result: FilesWalkResult,
) -> None:
    """Per-(cik, page) drain step extracted from the legacy nested
    loop. Transaction shape unchanged from pre-#1341:
    per-filing transaction inside the page loop; per-page watermark
    write after upsert-all-clean.
    """
    wm_key = f"{cik}:{page_name}"
    try:
        page_result = provider.fetch_submissions_page_conditional(
            page_name,
            if_modified_since=if_modified_since,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "files walk: secondary fetch failed for CIK %s/%s: %s",
            cik,
            page_name,
            exc,
        )
        result.parse_errors += 1
        return
    if page_result is None:
        # 404 — page absent. Nothing to record; no watermark
        # write either (no Last-Modified to persist).
        return
    if page_result.not_modified:
        # Server said 304. Skip parse + skip upsert (filings already
        # known). Bump watermark_at only — the stored Last-Modified
        # is still freshest the server has sent.
        result.secondary_pages_not_modified += 1
        with conn.transaction():
            if if_modified_since is not None:
                set_watermark(
                    conn,
                    source=_SOURCE_KEY_SUBMISSIONS_FILES,
                    key=wm_key,
                    watermark=if_modified_since,
                    watermark_at=None,
                )
        return

    page = page_result.payload
    if page is None:
        # Defensive: 200 with empty body should not happen,
        # but the dataclass shape permits it. Skip safely.
        return

    result.secondary_pages_fetched += 1
    try:
        # ``symbol`` is the canonical ticker (e.g. "AAPL"),
        # NOT a stringified instrument_id. Threaded from
        # the universe lookup at the top of the walk so
        # ``filing_events.raw_payload_json`` carries the
        # right value. Codex review BLOCKING for PR #1035.
        filings = _normalise_submissions_block(
            page,
            cik_padded=cik,
            symbol=symbol or cik,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("files walk: normalise failed for %s: %s", page_name, exc)
        result.parse_errors += 1
        return

    page_upsert_errors = 0
    for filing in filings:
        try:
            with conn.transaction():
                # ``_upsert_filing`` returns False when
                # the 10y retention cap (#1233 §4.2) drops
                # a pre-cutoff filing. Count only accepted
                # rows.
                if _upsert_filing(conn, str(instrument_id), "sec", filing):
                    result.filings_upserted += 1
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "files walk: upsert failed for %s/%s: %s",
                cik,
                filing.provider_filing_id,
                exc,
            )
            result.parse_errors += 1
            page_upsert_errors += 1

    # Item 7 (#1233): persist the fresh Last-Modified
    # watermark. set_watermark asserts INTRANS so it MUST
    # land inside ``with conn.transaction():``. Write only
    # when every filing on the page upserted cleanly OR
    # was intentionally retention-dropped (the
    # ``_upsert_filing returns False`` case which does NOT
    # increment page_upsert_errors). If ANY filing raised,
    # we leave the watermark unchanged so the next tick
    # re-fetches + retries the failed filings instead of
    # 304-skipping them forever. Codex 2 pre-push P1 fold
    # 2026-05-24.
    if page_result.last_modified is not None and page_upsert_errors == 0:
        with conn.transaction():
            set_watermark(
                conn,
                source=_SOURCE_KEY_SUBMISSIONS_FILES,
                key=wm_key,
                watermark=page_result.last_modified,
                watermark_at=None,
            )


def walk_files_pages(
    *,
    conn: psycopg.Connection[Any],
) -> FilesWalkResult:
    """Walk ``filings.files[]`` secondary pages for every CIK-mapped
    universe instrument and append discovered filings to
    ``filing_events``.

    Stream A PR-B (#1233): consumes the sidecar
    (``sec_cik_submissions_files_index``) instead of re-fetching each
    CIK's primary ``submissions/CIK<10>.json`` — eliminates ~5,105
    redundant primary HTTP calls per Run-#8.

    #1341 (2026-05-28): when invoked under the bootstrap orchestrator
    (``resolve_progress_context()`` returns non-None), the walker
    prefetches every chunk of secondary pages concurrently (4-way
    via ``PipelinedSecFetcher`` at the shared 7 req/s ceiling)
    BEFORE draining each chunk per-(cik, page). Steady-state cron
    / API path runs OUTSIDE the bootstrap context window → walker
    uses the serial sync ResilientClient as before.

    Secondary-page fetches over HTTP are UNCHANGED in semantics —
    they were never in the bulk archive; this PR only compresses
    the wall-clock via concurrent HTTP for the bootstrap path.
    """
    result = FilesWalkResult()
    targets = _list_cik_secondary_pages(conn)

    progress_ctx = resolve_progress_context()
    bootstrap_mode = progress_ctx is not None

    # Pre-load watermarks for the whole cohort (one SELECT). Both
    # bootstrap and steady-state paths use this; the IMS value pinned
    # in this map is what the prefetch (bootstrap) AND the per-page
    # loop send to the SEC.
    watermarks = _load_all_watermarks_for_pages(conn, targets)

    # Flatten the cohort into an ordered (instrument_id, cik, symbol,
    # page_name) task list, SKIPPING agent CIKs / empty-sidecar /
    # sentinel-only short-circuits — those don't need HTTP. Counter
    # bookkeeping mirrors the pre-#1341 loop exactly so end-of-walk
    # summary log fields line up.
    fetch_tasks_ordered: list[tuple[int, str, str, str]] = []
    for instrument_id, cik, symbol, sidecar_pages in targets:
        # Agent CIKs are excluded by the populate path
        # (sec_submissions_ingest.refresh_cik_sidecar) so their
        # sidecar_pages list is empty by design. Skip them here
        # silently — they do NOT count toward ciks_visited (the
        # counter reflects "real CIKs we did or attempted work for"
        # per Architect IMPORTANT — pre-PR-B post-review fix).
        if cik in KNOWN_FILING_AGENT_CIKS:
            continue

        result.ciks_visited += 1

        if not sidecar_pages:
            # Empty sidecar for an in-universe CIK that is NOT an
            # agent. Indicates an S8 ordering bug or a CIK added
            # to the universe after S8 ran. Per-CIK log is DEBUG
            # to avoid stderr spam at scale (8.7k CIK cohort — a
            # systemic S8 failure would otherwise log 8,700
            # WARNING lines); a single end-of-walk summary
            # WARNING is emitted below (per Architect IMPORTANT).
            logger.debug(
                "files walk: empty sidecar for in-universe CIK %s; "
                "S8 must populate sec_cik_submissions_files_index first",
                cik,
            )
            result.ciks_with_empty_sidecar += 1
            result.parse_errors += 1
            continue

        if sidecar_pages == [_SIDECAR_SENTINEL_PAGE_NAME]:
            # CIK processed with zero overflow pages (sentinel
            # row). No HTTP fetch needed.
            result.ciks_with_no_overflow += 1
            continue

        for page_name in sidecar_pages:
            # Defensive — the only sentinel allowed is
            # _SIDECAR_SENTINEL_PAGE_NAME; the schema CHECK already
            # rejects any other sentinel-shaped value. A real CIK
            # with overflow pages will never have the sentinel
            # mixed in (the populate path writes one or the other).
            # Skip sentinel rows defensively.
            if page_name == _SIDECAR_SENTINEL_PAGE_NAME:
                continue
            fetch_tasks_ordered.append((instrument_id, cik, symbol, page_name))

    # #1273 PR2 — long-pole stage instrumentation (S14). Pin
    # target_count + cohort fingerprint when called from the
    # bootstrap dispatcher AFTER the flatten loop so the bar's
    # denominator is the FINAL total (`len(targets)` flatten-pass
    # bumps + `len(fetch_tasks_ordered)` drain bumps) — otherwise
    # processed could exceed target. Fingerprint exposes both
    # buckets so the operator can audit cohort composition.
    _progress_total = len(targets) + len(fetch_tasks_ordered)
    _emit_every_n = max(1, _progress_total // 100) if _progress_total else 0
    _last_progress_emit = time.monotonic()
    _processed_count = 0
    if progress_ctx is not None:
        sentinel_count = sum(1 for t in targets if t[3] == [_SIDECAR_SENTINEL_PAGE_NAME])
        empty_count = sum(1 for t in targets if not t[3])
        real_pages_count = len(targets) - sentinel_count - empty_count
        fingerprint = (
            f"is_tradable_only=true;"
            f"sidecar_sentinel={sentinel_count};"
            f"sidecar_real_pages={real_pages_count};"
            f"sidecar_empty={empty_count};"
            f"fetch_tasks={len(fetch_tasks_ordered)}"
        )
        set_stage_target(
            run_id=progress_ctx.run_id,
            stage_key=progress_ctx.stage_key,
            target_count=_progress_total,
            cohort_fingerprint=fingerprint,
        )

    def _emit_progress() -> None:
        nonlocal _last_progress_emit
        if progress_ctx is None:
            return
        _now = time.monotonic()
        if _processed_count % _emit_every_n == 0 or _now - _last_progress_emit > 30:
            set_stage_processed(
                run_id=progress_ctx.run_id,
                stage_key=progress_ctx.stage_key,
                processed_count=_processed_count,
            )
            _last_progress_emit = _now

    # Bump once per target evaluated during flatten (covers
    # agent-skipped / empty-sidecar / sentinel-only short-circuits)
    # so the bar advances at the same per-target ratio as the
    # pre-#1341 loop. The chunk drain below bumps once per task.
    for _ in targets:
        _processed_count += 1
        _emit_progress()

    prefetch_window_seconds_total = 0.0 if bootstrap_mode else None

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        # Chunk the task list. Bootstrap → prefetch chunk via
        # PipelinedSecFetcher; steady-state → skip prefetch.
        # Per-chunk discipline bounds peak heap: the chunk's
        # cache dict and the SubmissionsPageResult payloads it
        # holds are all dropped at the chunk-loop boundary.
        for chunk in _chunked(fetch_tasks_ordered, DEFAULT_PREFETCH_CHUNK_SIZE):
            wrapper: _CachedSubmissionsPageFetcher | None = None
            if bootstrap_mode:
                prefetch_tasks = [
                    ConditionalFetchTask(
                        page_name=page_name,
                        if_modified_since=watermarks.get((cik_, page_name)),
                    )
                    for (_iid, cik_, _sym, page_name) in chunk
                ]
                _t0 = time.monotonic()
                chunk_cache = prefetch_submissions_pages_conditional(
                    prefetch_tasks,
                    user_agent=settings.sec_user_agent,
                )
                assert prefetch_window_seconds_total is not None  # bootstrap_mode guard
                prefetch_window_seconds_total += time.monotonic() - _t0
                result.prefetch_pages_seeded += len(chunk_cache)
                wrapper = _CachedSubmissionsPageFetcher(provider, chunk_cache)
            provider_for_loop: SecFilingsProvider | _CachedSubmissionsPageFetcher = (
                wrapper if wrapper is not None else provider
            )

            # Per-(cik, page) drain of THIS chunk.
            for instrument_id, cik, symbol, page_name in chunk:
                _processed_count += 1
                _emit_progress()
                wm_key = (cik, page_name)
                if_modified_since = watermarks.get(wm_key)
                _process_one_page(
                    conn,
                    provider=provider_for_loop,
                    instrument_id=instrument_id,
                    cik=cik,
                    symbol=symbol,
                    page_name=page_name,
                    if_modified_since=if_modified_since,
                    result=result,
                )

            # Drain wrapper telemetry into result BEFORE dropping the
            # wrapper — chunk-boundary `del` would otherwise lose
            # cache_hits/cache_misses for this chunk.
            if wrapper is not None:
                result.loop_pages_from_prefetch += wrapper.cache_hits
                result.loop_pages_from_sync_fallback += wrapper.cache_misses

            # Drop chunk cache + wrapper before next chunk. Python
            # GC reclaims SubmissionsPageResult payloads. Bounded
            # peak heap = one chunk's worth.
            wrapper = None

    result.prefetch_window_seconds_total = prefetch_window_seconds_total

    # #1273 PR2 — final operator-progress emit on exit.
    if progress_ctx is not None:
        set_stage_processed(
            run_id=progress_ctx.run_id,
            stage_key=progress_ctx.stage_key,
            processed_count=_processed_count,
        )

    # End-of-walk SUMMARY warning when ≥ 1 in-universe non-agent CIK
    # had an empty sidecar. Single log line replaces the per-CIK spam
    # that pre-review-v2 emitted (Architect IMPORTANT).
    if result.ciks_with_empty_sidecar > 0:
        logger.warning(
            "files walk: %d in-universe CIK(s) had empty sidecar — "
            "sec_submissions_ingest (S8) must populate "
            "sec_cik_submissions_files_index before S14 runs. "
            "Counters: visited=%d empty=%d no_overflow=%d pages=%d filings=%d errors=%d",
            result.ciks_with_empty_sidecar,
            result.ciks_visited,
            result.ciks_with_empty_sidecar,
            result.ciks_with_no_overflow,
            result.secondary_pages_fetched,
            result.filings_upserted,
            result.parse_errors,
        )

    return result


# Job invoker registered in app/jobs/runtime.py:_INVOKERS.
JOB_SEC_SUBMISSIONS_FILES_WALK = "sec_submissions_files_walk"


def sec_submissions_files_walk_job() -> None:
    """Zero-arg job invoker for the runtime registry.

    Within an orchestrated bootstrap run, validates C1.a's
    rows_written > 0 before walking. Records its own per-run
    archive_result row so D-stages can verify provenance.
    """
    from app.services.bootstrap_preconditions import (
        assert_c1b_preconditions,
        record_archive_result,
    )

    # Find current bootstrap_run.
    run_id: int | None = None
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM bootstrap_runs WHERE status='running' ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            run_id = int(row[0]) if row else None
        if run_id is not None:
            from app.security.master_key import resolve_data_dir

            bulk_dir = resolve_data_dir() / "sec" / "bulk"
            assert_c1b_preconditions(conn, bootstrap_run_id=run_id, bulk_dir=bulk_dir)

    with psycopg.connect(settings.database_url) as conn:
        result = walk_files_pages(conn=conn)
        conn.commit()
    logger.info(
        "sec_submissions_files_walk: ciks=%d pages=%d filings=%d "
        "no_overflow=%d empty_sidecar=%d not_modified=%d parse_errors=%d "
        "prefetch_seeded=%d loop_from_prefetch=%d loop_from_sync=%d "
        "prefetch_window_s=%s",
        result.ciks_visited,
        result.secondary_pages_fetched,
        result.filings_upserted,
        result.ciks_with_no_overflow,
        result.ciks_with_empty_sidecar,
        result.secondary_pages_not_modified,
        result.parse_errors,
        result.prefetch_pages_seeded,
        result.loop_pages_from_prefetch,
        result.loop_pages_from_sync_fallback,
        f"{result.prefetch_window_seconds_total:.1f}" if result.prefetch_window_seconds_total is not None else "n/a",
    )
    if run_id is not None:
        # The walker has already committed its writes; a failure of
        # the post-commit audit row must NOT propagate to the
        # orchestrator (which would mark the stage `error` and a
        # later retry would re-run the walker AND fail
        # ``assert_c1b_preconditions`` because the audit row is
        # missing — permanently blocking C1.b on a transient DB
        # hiccup). PR review WARNING (bot, PR #1038).
        try:
            with psycopg.connect(settings.database_url) as conn:
                record_archive_result(
                    conn,
                    bootstrap_run_id=run_id,
                    stage_key="sec_submissions_files_walk",
                    archive_name="__job__",
                    rows_written=result.filings_upserted,
                    rows_skipped={
                        "parse_errors": result.parse_errors,
                        "no_overflow": result.ciks_with_no_overflow,
                        "empty_sidecar": result.ciks_with_empty_sidecar,
                    },
                )
                conn.commit()
        except Exception as exc:  # noqa: BLE001 — audit must not block stage
            logger.warning(
                "sec_submissions_files_walk: failed to record __job__ audit row (walker already committed): %s",
                exc,
            )
