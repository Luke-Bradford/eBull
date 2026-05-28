# #1341 — S14 sec_submissions_files_walk: pipelined HTTP for bootstrap

**Status**: draft 1.4 · 2026-05-28 · Phase 2 of [`bootstrap-sub-1h-plan.md`](./bootstrap-sub-1h-plan.md) §7.

**Parent**: master plan v5.2 §7 Phase 2 row 3 (S14 master.idx walk — **retargeted** after grep-verified premise gap: master.idx walk writes `sec_filing_manifest`; S14 writes `filing_events` with per-accession `primary_document_url` that downstream parsers (`business_summary.py:1419`, `eight_k_events.py:734`, `ownership_observations_sync.py:225`, `rewash_filings.py:298`, `def14a_ingest.py`) depend on. master.idx primary URL is the complete-submission `.txt`, not the per-accession primary doc; substituting silently breaks those parsers).

**Approach**: keep S14's correctness (per-page secondary `submissions.json` HTTP with conditional GET → `_upsert_filing` writes to `filing_events`) and parallelise the dominant cost — per-page HTTP RTT — via the existing `PipelinedSecFetcher` (`app/services/sec_pipelined_fetcher.py:142`). Mirrors the established `bootstrap_business_summaries` / `bootstrap_def14a` / 8-K bootstrap prefetch pattern at `app/services/business_summary.py:1717`, `app/services/def14a_ingest.py:1006`, `app/services/eight_k_events.py:773`. Bootstrap-only — steady-state cron path unchanged.

**Baseline (Run #7)**: S14 ~41 min wall-clock.
**Target (this PR)**: S14 < 10 min wall-clock (Phase 2 acceptance per master plan §7).

**Changelog**:
- v1.0 — 2026-05-28 — initial draft.
- v1.4 — 2026-05-28 — Codex 1 v1.3 re-pass fold (0 BLOCKING + 1 IMPORTANT + 1 NIT):
  - IMPORTANT-D (progress invariant): `set_stage_target` MUST be called AFTER the flatten loop, with `target_count = len(targets) + len(fetch_tasks_ordered)` (since the loop bumps once per target + once per task). v1.3 left `set_stage_target(target_count=len(targets))` at the top, so final `_processed_count` could exceed target by up to `len(fetch_tasks_ordered)`. v1.4 moves the `set_stage_target` call to AFTER the flatten pass and pins the correct total.
  - NIT-D: removed "chunk boundary" from `prefetch_submissions_pages_conditional` test description — function does NOT chunk internally; the walker chunk-shape test (sibling row) covers that.
- v1.3 — 2026-05-28 — Codex 1 v1.2 re-pass fold (0 BLOCKING + 3 IMPORTANT + 3 NIT):
  - IMPORTANT-A (progress accounting): §3.4 walker pseudocode now pins explicit progress accounting. `_processed_count` increments once per `target` evaluated in the flatten-loop (covers agent-CIK / empty-sidecar / sentinel-only short-circuits exactly like the existing per-target visit) AND once more per `(cik, page)` task drained inside the chunk drain. Cadenced emit per existing `_emit_every_n` formula bumped at both sites. Final emit at end-of-walk unchanged.
  - IMPORTANT-B (telemetry drain): §3.4 walker now drains `_CachedSubmissionsPageFetcher.cache_hits / cache_misses` into `result.loop_pages_from_prefetch` / `result.loop_pages_from_sync_fallback` AFTER each chunk's drain loop completes (`result.loop_pages_from_prefetch += wrapper.cache_hits` etc.) so wrapper counters survive the chunk-boundary `del`.
  - IMPORTANT-C (multi-chunk window): `prefetch_window_seconds` renamed `prefetch_window_seconds_total` — summed across all chunks via per-chunk `time.monotonic()` deltas. Still `None` when bootstrap-mode is off.
  - NIT-A: status header bumped to draft 1.3.
  - NIT-B: §8 prose updated — "S14's writer + 4 other callers" not "7 other callers".
  - NIT-C: stale watermark line refs `315, 380` → `317, 382` in body prose (matching §0 verbatim).
- v1.2 — 2026-05-28 — Codex 1 v1.1 re-pass fold (1 BLOCKING + 2 IMPORTANT + 1 NIT):
  - BLOCKING-4 (chunking did not bound peak heap): v1.1 returned one giant `dict[page_name, SubmissionsPageResult|None]` covering all 17k pages — only per-chunk HTTP buffers freed. v1.2 refactors to a **chunk-and-drain** pattern: walker iterates the cohort task list in chunk-sized slices; for each slice it (a) prefetches that slice → small dict, (b) drains it via per-(cik, page) loop with DB writes, (c) drops the dict and moves to next chunk. Peak heap = one chunk dict + steady-state walker state ≈ 150-200 MB. Returns shape of `prefetch_submissions_pages_conditional` unchanged (still `dict`) — caller's per-chunk discipline does the heap bounding.
  - IMPORTANT-7 (§0 grep proofs not verbatim): re-grepped at v1.2. `submissions_secondary_pages_walked` and `filing_events_seeded` now show all matches; `_SOURCE_KEY_SUBMISSIONS_FILES` set-watermark lines now 317/382 (not 316/381); `JOB_SEC_SUBMISSIONS_FILES_WALK` grep narrowed to the actual one-line match.
  - IMPORTANT-8 (test placement): `_load_all_watermarks_for_pages` is walker-local (lives in `app/services/sec_submissions_files_walk.py`). Test moves from `tests/test_sec_pipelined_fetcher.py` to `tests/test_s14_uses_sidecar.py`.
  - NIT-3 (callsite count): §8 now says `_upsert_filing` is called from 5 sites (`app/services/coverage.py:1658`, `coverage.py:1778`, `filings.py:396`, `sec_submissions_files_walk.py:355`, `sec_submissions_ingest.py:412`), not "7 other callers".
- v1.1 — 2026-05-28 — Codex 1 fold (3 BLOCKING + 6 IMPORTANT + 2 NIT):
  - BLOCKING-1 (`_adapt_zero_arg` discards `StageSpec.params`): drop the `use_pipelined_prefetch` param plumb. Walker enables prefetch unconditionally when `resolve_progress_context()` returns non-None (i.e. dispatched from inside the bootstrap orchestrator). Steady-state cron / API path returns `None` → no prefetch. Mirrors #1273 PR2 pattern.
  - BLOCKING-2 (`walk_files_pages` constructs its own provider): walker continues to construct its own `SecFilingsProvider`. When `resolve_progress_context()` is non-None, walker runs the prefetch + wraps its OWN provider with `_CachedSubmissionsPageFetcher` before entering the per-CIK loop. No external provider injection. Test seam = inject `_run_prefetch` callable via module-level monkeypatch / parametrised hook (covered in §20).
  - BLOCKING-3 (sink registry incomplete): §8 now declares `app/services/fundamentals/__init__.py:_upsert_filing_from_master_index` (line 2251) as a third `filing_events` writer with COALESCE-preserve semantics on the URL columns. Interaction documented: per-accession URL ALWAYS wins (master-index writer COALESCEs into existing; `_upsert_filing` overwrites NULL/.txt URLs with per-accession URLs). No regression — S14 was already in this race before this PR.
  - IMPORTANT-1: §0 grep proofs re-run + pasted verbatim.
  - IMPORTANT-2: Test paths corrected — tree is flat `tests/test_<topic>.py`, not `tests/services/...`. Extends `tests/test_sec_pipelined_fetcher.py` (existing) + `tests/test_s14_uses_sidecar.py` (existing). Uses existing `tests/fixtures/sec/submissions_TEST.json` fixture.
  - IMPORTANT-3 (malformed 200): `prefetch_submissions_pages_conditional` catches `json.JSONDecodeError` + any per-task exception + omits failing pages from the cache. Cache-miss fallthrough via `_CachedSubmissionsPageFetcher` triggers the sync provider's existing per-page error handling (`parse_errors` counter, transaction rollback). One bad page never aborts the prefetch.
  - IMPORTANT-4 (watermark batching underspecified): §6 now spells out the batched load — single SELECT pre-loop builds `dict[(cik, page_name), str|None]`; per-task IMS resolves from this map; per-page loop uses the SAME map for IMS passed to the cached wrapper (cache hit returns the prefetch result; cache miss falls through to sync provider which reads the watermark itself for retry-correctness).
  - IMPORTANT-5 (memory estimate too low): v1.1 ADDS chunked prefetch in v1 — default `prefetch_chunk_size=1000` pages per chunk = ~50 MB raw JSON + ~150-200 MB peak heap with Python overhead. Configurable via module constant (not param — keeps invoker shape). R3 measurement can tune if needed without a new ticket.
  - IMPORTANT-6 (cache_hits semantics): renamed counters and clarified — `prefetch_pages_seeded` = unique pages successfully prefetched; `loop_pages_from_prefetch` = per-loop consumptions served from cache (will overcount unique fetches by share-class-sibling multiple-instrument_id-per-CIK factor; this is loop visits, not work saved); `loop_pages_from_sync_fallback` = cache misses that hit the sync provider in the per-CIK loop.
  - NIT-1: §21 callsite cites corrected — `business_summary.py:1717` / `def14a_ingest.py:1006` / `eight_k_events.py:773` (the import-and-use sites), not `scheduler.py` (which only holds the dispatch shell).
  - NIT-2: stale docstring in `_upsert_filing` (`filings.py:611`) referencing 2-tuple conflict key gets corrected inline in the implementation PR (skill ownership rule). Not a spec change.

---

## §0 Grep proof

> Generated 2026-05-28 against branch main @ `3129584`. Outputs reproduced verbatim from the commands below; do NOT paraphrase.

### Cap vocabulary (cited in §13)

```
$ grep -n "Capability = Literal\[" app/services/bootstrap_orchestrator.py
288:Capability = Literal[
$ grep -n '"submissions_secondary_pages_walked"' app/services/bootstrap_orchestrator.py
294:    "submissions_secondary_pages_walked",
387:    "sec_submissions_files_walk": ("submissions_secondary_pages_walked",),
394:    "sec_first_install_drain": ("filing_events_seeded", "submissions_secondary_pages_walked"),
576:    "sec_def14a_bootstrap": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
578:        all_of=("filing_events_seeded", "submissions_secondary_pages_walked")
590:    "sec_8k_events_ingest": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
$ grep -n "filing_events_seeded" app/services/bootstrap_orchestrator.py
293:    "filing_events_seeded",
314:    # legacy chain owner of ``filing_events_seeded``.
369:    "sec_submissions_ingest": ("filing_events_seeded", "submissions_processed"),
388:    "filings_history_seed": ("filing_events_seeded",),
394:    "sec_first_install_drain": ("filing_events_seeded", "submissions_secondary_pages_walked"),
511:# Content caps (``filing_events_seeded``, ``insider_inputs_seeded``,
556:    "sec_submissions_files_walk": CapRequirement(all_of=("filing_events_seeded",)),
576:    "sec_def14a_bootstrap": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
578:        all_of=("filing_events_seeded", "submissions_secondary_pages_walked")
590:    "sec_8k_events_ingest": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
```

S14 produces `submissions_secondary_pages_walked` (declared line 294; produced line 387); requires `filing_events_seeded` (line 556 — `CapRequirement(all_of=("filing_events_seeded",))`). Neither cap is new in this PR.

### filing_events writers (cited in §8) — verbatim, three writers

```
$ grep -n "INSERT INTO filing_events" app/services/filings.py app/services/fundamentals/__init__.py
app/services/filings.py:573:        INSERT INTO filing_events (
app/services/filings.py:643:        INSERT INTO filing_events (
app/services/fundamentals/__init__.py:2309:        INSERT INTO filing_events (

$ grep -n "_upsert_filing\b" app/services/filings.py
63:# into one of three chokepoints — ``_upsert_filing`` /
292:    # to `_upsert_filing`, which has historically tolerated either
396:                    if _upsert_filing(conn, instrument_id, provider_name, result):
552:    Mirrors ``_upsert_filing``'s idempotent ON CONFLICT semantics and
611:def _upsert_filing(

$ grep -rn "_upsert_filing(" app --include="*.py"
app/services/coverage.py:1658:            if _upsert_filing(conn, str(instrument_id), "sec", r):
app/services/coverage.py:1778:                if _upsert_filing(conn, str(instrument_id), "sec", r):
app/services/filings.py:396:                    if _upsert_filing(conn, instrument_id, provider_name, result):
app/services/sec_submissions_files_walk.py:355:                            if _upsert_filing(conn, str(instrument_id), "sec", filing):
app/services/sec_submissions_ingest.py:412:        if _upsert_filing(conn, str(instrument_id), "sec", filing):
```

Three writers; all keyed `(provider, provider_filing_id, instrument_id)`:

- `filings.py:543 _upsert_filing_event` — overwrites URL columns on conflict.
- `filings.py:611 _upsert_filing` — overwrites URL columns on conflict (S14's writer + 4 other callers: coverage.py:1658, coverage.py:1778, filings.py:396, sec_submissions_ingest.py:412).
- `fundamentals/__init__.py:2251 _upsert_filing_from_master_index` — `COALESCE(filing_events.<col>, EXCLUDED.<col>)` preserves existing URLs on conflict.

### Watermark namespace (cited in §6)

```
$ grep -n "_SOURCE_KEY_SUBMISSIONS_FILES" app/services/sec_submissions_files_walk.py
81:_SOURCE_KEY_SUBMISSIONS_FILES: str = "sec.last_modified.submissions_files"
287:                wm = get_watermark(conn, _SOURCE_KEY_SUBMISSIONS_FILES, wm_key)
317:                                source=_SOURCE_KEY_SUBMISSIONS_FILES,
382:                            source=_SOURCE_KEY_SUBMISSIONS_FILES,
```

Watermark key = `f"{cik}:{page_name}"`. Source = `sec.last_modified.submissions_files`. Unchanged in this PR.

### Watermark API (cited in §6)

```
$ grep -n "def get_watermark\|def set_watermark" app/services/watermarks.py
101:def get_watermark(
133:def set_watermark(
```

### PipelinedSecFetcher API (cited in §3.1)

```
$ grep -n "def prefetch_document_texts\|class PipelinedSecFetcher\|DEFAULT_TARGET_RPS\|DEFAULT_CONCURRENCY" app/services/sec_pipelined_fetcher.py
46:DEFAULT_TARGET_RPS: Final[float] = 7.0
47:DEFAULT_CONCURRENCY: Final[int] = 4
142:class PipelinedSecFetcher:
223:def prefetch_document_texts(
```

Existing prefetch returns `dict[url, str | None]` — bodies only, no Last-Modified / status. Insufficient for S14 (needs conditional GET trichotomy). New sibling required.

### Existing prefetch consumers (cited in §21)

```
$ grep -n "prefetch_document_texts\|_CachedDocFetcher" app/services/business_summary.py app/services/def14a_ingest.py app/services/eight_k_events.py
app/services/business_summary.py:1717:        from app.services.sec_pipelined_fetcher import _CachedDocFetcher, prefetch_document_texts
app/services/def14a_ingest.py:1006:        from app.services.sec_pipelined_fetcher import _CachedDocFetcher, prefetch_document_texts
app/services/eight_k_events.py:773:        from app.services.sec_pipelined_fetcher import _CachedDocFetcher, prefetch_document_texts
```

### Bootstrap context API (cited in §3.4, §13)

```
$ grep -n "def resolve_progress_context\|class BootstrapProgressContext" app/services/bootstrap_state.py
896:class BootstrapProgressContext:
911:def resolve_progress_context() -> BootstrapProgressContext | None:
```

Returns non-None inside the `active_bootstrap_run` contextvar window (bound by the orchestrator at `bootstrap_orchestrator.py:1446`); returns `None` otherwise (cron / API path).

### Invoker shape (cited in §3.4)

```
$ grep -n "JOB_SEC_SUBMISSIONS_FILES_WALK\|sec_submissions_files_walk_job" app/jobs/runtime.py
378:_INVOKERS[_files_walk.JOB_SEC_SUBMISSIONS_FILES_WALK] = _adapt_zero_arg(_files_walk.sec_submissions_files_walk_job)
```

S14's invoker is `_adapt_zero_arg`-wrapped → `StageSpec.params` are DISCARDED. Walker MUST self-detect bootstrap mode via `resolve_progress_context()` — no param-flag plumb possible without re-shaping the invoker, which is OUT of scope.

### Existing walker test (cited in §20)

```
$ ls tests/test_s14_uses_sidecar.py tests/test_sec_pipelined_fetcher.py tests/test_sec_cik_submissions_files_index.py 2>/dev/null
tests/test_s14_uses_sidecar.py
tests/test_sec_pipelined_fetcher.py
tests/test_sec_cik_submissions_files_index.py
```

---

## 1. Decisions

1. Bootstrap S14 (`sec_submissions_files_walk_job` registered at `app/jobs/runtime.py:378`) prefetches every secondary `CIK<10>-submissions-NNN.json` page concurrently (4-way via `PipelinedSecFetcher` at the shared 7 req/s ceiling) BEFORE the per-CIK upsert loop runs. The loop consumes cached responses via a `_CachedSubmissionsPageFetcher` wrapper and proceeds with unchanged parse + watermark + upsert semantics.
2. Steady-state cron path UNCHANGED — single-tenant operator invocation has no parallelism win over the shared sec_rate lane, and the prefetch's chunked memory budget is wasteful when the cohort is small. Walker self-detects bootstrap mode via `resolve_progress_context()` (returns non-None only inside the orchestrator's `active_bootstrap_run` contextvar window).
3. Operator-visible figure: `filing_events` row count for any in-universe CIK with overflow pages — must match the serial baseline exactly (delta = 0 — pure perf change, no semantics shift). Verification panel = AAPL/GME/MSFT/JPM/HD.
4. Rollback: revert this PR. No data migration, no schema change. The walker falls back to the serial sync ResilientClient path (the existing pre-PR behaviour).

## 2. Identifiers + identity-drift

CIK (zero-padded 10-digit string), page_name (regex `CIK\d{10}-submissions-\d{3}\.json`), accession_number. No new identifiers. No identity drift surface — purely lifts the existing identifier flow.

## 3. Endpoint surface

| URL | Method | Conditional | Body schema | Fixture |
|---|---|---|---|---|
| `https://data.sec.gov/submissions/<page_name>` | GET | If-Modified-Since | SEC submissions secondary-page JSON (recent[]-shape arrays) | `tests/fixtures/sec/submissions_TEST.json` (existing — re-used as the page-body fixture in `tests/test_s14_uses_sidecar.py`) |

### 3.1 New function: `prefetch_submissions_pages_conditional`

```python
# app/services/sec_pipelined_fetcher.py

@dataclass(frozen=True)
class ConditionalFetchTask:
    """Per-page prefetch task carrying its own If-Modified-Since header."""
    page_name: str  # e.g. "CIK0000320193-submissions-001.json"
    if_modified_since: str | None  # from external_data_watermarks


DEFAULT_PREFETCH_CHUNK_SIZE: Final[int] = 1000


def prefetch_submissions_pages_conditional(
    tasks: list[ConditionalFetchTask],
    *,
    user_agent: str,
    target_rps: float = DEFAULT_TARGET_RPS,
    concurrency: int = DEFAULT_CONCURRENCY,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict[str, "SubmissionsPageResult | None"]:
    """Bulk-fetch ONE CHUNK of SEC secondary submissions pages.

    The CALLER (walker) is responsible for slicing the full cohort
    task list into ``DEFAULT_PREFETCH_CHUNK_SIZE``-sized chunks and
    invoking this function once per chunk so the dict it returns
    (and any payloads it references) can be drained and dropped
    BEFORE the next chunk is fetched. Function does NOT chunk
    internally — it fetches the whole `tasks` list and returns a
    cache for it. Caller bounds peak heap by chunk size; this
    function does NOT.

    Returns ``{page_name: SubmissionsPageResult | None}``:
      * key absent — fetch failed (transport / 429 / 5xx / malformed
        body). Caller's cache-miss fallthrough hits the sync
        provider, which owns the retry / quarantine contract.
      * value ``None`` — 404; page absent (caller's existing skip path).
      * value ``SubmissionsPageResult(payload=None, last_modified=ims,
        not_modified=True)`` — 304.
      * value ``SubmissionsPageResult(payload=<dict>, last_modified=<lm>,
        not_modified=False)`` — 200.

    Per-task failures isolated via ``try/except`` (httpx.HTTPError +
    OSError + json.JSONDecodeError + ValueError); one bad page is
    omitted from the result dict, never aborts the chunk.

    Tasks with the SAME page_name are de-duped via ``dict.fromkeys``
    upfront. Returned dict keyed by ``page_name`` (NOT URL) — caller
    has page_name from the sidecar.

    Mirrors ``prefetch_document_texts`` lifecycle: shared
    ``_PROCESS_RATE_LIMIT_CLOCK`` so concurrent sync SEC traffic
    co-exists under the 7 req/s ceiling.
    """
```

### 3.2 New wrapper: `_CachedSubmissionsPageFetcher`

```python
class _CachedSubmissionsPageFetcher:
    """Wraps a sync ``SecFilingsProvider`` for the bootstrap S14 path.

    Mirror of ``_CachedDocFetcher`` but for
    ``fetch_submissions_page_conditional`` rather than
    ``fetch_document_text``. Cache lookups MUST honour the
    per-page If-Modified-Since the caller passes; cache misses fall
    through to the underlying provider's sync ResilientClient.

    Cache contract:
      * page_name in cache, value None → 404; return None.
      * page_name in cache, value SubmissionsPageResult → return it.
      * page_name NOT in cache → fall through to underlying provider.

    Telemetry:
      * ``cache_hits``  — page_name in cache (any value, including None).
      * ``cache_misses`` — page_name NOT in cache (caller's per-CIK loop
        visit went to the sync provider).
    """

    def __init__(
        self,
        underlying: SecFilingsProvider,
        cache: dict[str, SubmissionsPageResult | None],
    ) -> None:
        self._underlying = underlying
        self._cache = cache
        self.cache_hits = 0
        self.cache_misses = 0

    def fetch_submissions_page_conditional(
        self,
        page_name: str,
        *,
        if_modified_since: str | None = None,
    ) -> SubmissionsPageResult | None:
        if page_name in self._cache:
            self.cache_hits += 1
            return self._cache[page_name]
        self.cache_misses += 1
        return self._underlying.fetch_submissions_page_conditional(
            page_name, if_modified_since=if_modified_since
        )
```

`walk_files_pages` accepts the wrapper via duck-typing (only one method called). No new abstract base.

### 3.3 Watermark batched load

```python
def _load_all_watermarks_for_pages(
    conn: psycopg.Connection[Any],
    targets: list[tuple[int, str, str, list[str]]],
) -> dict[tuple[str, str], str | None]:
    """One SELECT, returns ``{(cik, page_name): if_modified_since}``.

    For every (cik, page_name) in the cohort's flattened sidecar pages,
    look up the watermark under
    ``source='sec.last_modified.submissions_files'``,
    ``key=f'{cik}:{page_name}'``. Missing rows → value None.

    Avoids ~17k per-page round-trips to external_data_watermarks
    during the per-CIK loop.
    """
```

### 3.4 Walker integration (`walk_files_pages`) — chunk-and-drain

```python
def walk_files_pages(*, conn: psycopg.Connection[Any]) -> FilesWalkResult:
    result = FilesWalkResult()
    targets = _list_cik_secondary_pages(conn)

    progress_ctx = resolve_progress_context()
    _last_progress_emit = time.monotonic()
    _processed_count = 0

    # Pre-load watermarks for the whole cohort (one SELECT).
    watermarks = _load_all_watermarks_for_pages(conn, targets)
    bootstrap_mode = progress_ctx is not None

    # Flatten the cohort into an ordered (cik, page_name) task list,
    # SKIPPING agent CIKs / empty-sidecar / sentinel-only short-
    # circuits — those don't need HTTP. We do NOT emit progress yet
    # because set_stage_target requires the FINAL total which depends
    # on len(fetch_tasks_ordered) too (one bump per target + one
    # bump per task — progress invariant: final processed ==
    # len(targets) + len(fetch_tasks_ordered)).
    fetch_tasks_ordered: list[tuple[int, str, str, str]] = []
    for instrument_id, cik, symbol, sidecar_pages in targets:
        if cik in KNOWN_FILING_AGENT_CIKS:
            continue  # not counted in ciks_visited (existing semantics)
        result.ciks_visited += 1
        if not sidecar_pages:
            result.ciks_with_empty_sidecar += 1
            result.parse_errors += 1
            continue
        if sidecar_pages == [_SIDECAR_SENTINEL_PAGE_NAME]:
            result.ciks_with_no_overflow += 1
            continue
        for page_name in sidecar_pages:
            if page_name == _SIDECAR_SENTINEL_PAGE_NAME:
                continue  # defensive — existing
            fetch_tasks_ordered.append((instrument_id, cik, symbol, page_name))

    # #1273 PR2 progress instrumentation — pin target_count to the
    # FINAL TOTAL after flatten so the bar can't overshoot. Fingerprint
    # exposes both buckets so the operator can audit cohort composition.
    _progress_total = len(targets) + len(fetch_tasks_ordered)
    _emit_every_n = max(1, _progress_total // 100) if _progress_total else 0
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

    # Bump _processed_count once per target seen during flatten (covers
    # agent-skipped / empty-sidecar / sentinel-only short-circuits) so
    # the operator-visible progress reflects work observed in the same
    # ratio as today's per-target loop.
    for _ in targets:
        _processed_count += 1
        _emit_progress()

    prefetch_window_seconds_total = 0.0 if bootstrap_mode else None

    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        # Chunk the task list. Bootstrap → prefetch chunk via
        # PipelinedSecFetcher; steady-state → skip prefetch.
        # Per-chunk discipline bounds peak heap: the chunk's cache
        # dict and the SubmissionsPageResult payloads it holds are
        # all dropped at the chunk-loop boundary.
        for chunk in _chunked(fetch_tasks_ordered, DEFAULT_PREFETCH_CHUNK_SIZE):
            chunk_cache: dict[str, SubmissionsPageResult | None] = {}
            wrapper: _CachedSubmissionsPageFetcher | None = None
            if bootstrap_mode:
                prefetch_tasks = [
                    ConditionalFetchTask(
                        page_name=page_name,
                        if_modified_since=watermarks.get((cik, page_name)),
                    )
                    for (_iid, cik, _sym, page_name) in chunk
                ]
                _t0 = time.monotonic()
                chunk_cache = prefetch_submissions_pages_conditional(
                    prefetch_tasks,
                    user_agent=settings.sec_user_agent,
                )
                prefetch_window_seconds_total += time.monotonic() - _t0
                result.prefetch_pages_seeded += len(chunk_cache)
                wrapper = _CachedSubmissionsPageFetcher(provider, chunk_cache)
            provider_for_loop: SecFilingsProvider | _CachedSubmissionsPageFetcher = (
                wrapper if wrapper is not None else provider
            )

            # Per-(cik, page) drain of THIS chunk. DB writes serial,
            # transaction shape unchanged from today (per-filing
            # transaction inside the page loop; per-page watermark
            # write after upsert-all-clean).
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

            # Drop chunk cache before fetching next chunk — Python
            # GC reclaims SubmissionsPageResult payloads. Bounded
            # peak heap = one chunk's worth.
            del chunk_cache
            wrapper = None

    result.prefetch_window_seconds_total = prefetch_window_seconds_total

    # End-of-walk summary log + final progress emit — UNCHANGED.
    if progress_ctx is not None:
        set_stage_processed(
            run_id=progress_ctx.run_id,
            stage_key=progress_ctx.stage_key,
            processed_count=_processed_count,
        )
    ...
    return result
```

Progress accounting invariant: `_progress_total == len(targets) + len(fetch_tasks_ordered)` is pinned via `set_stage_target` AFTER the flatten pass so the operator-visible bar maxes out at the correct denominator. `_processed_count` final value == `_progress_total` (one bump per target observed + one bump per task drained). Final emit pins the counter. Operator-visible meaning is "evaluations performed" — strict superset of "CIKs visited" (matches today's `_processed_count` semantics with extra page-level granularity).

`_chunked` is a trivial slicing helper (`itertools.batched` once Python 3.12+ is the floor; or a 4-line generator). `_process_one_page` is the extracted per-page body of today's nested loop (parse + per-filing transaction + watermark write). Test seam = `monkeypatch.setattr(sec_submissions_files_walk, "prefetch_submissions_pages_conditional", _fake)` (covered in §20).

**Walker takes NO new parameters.** The chunk-and-drain pattern fits inside the existing `walk_files_pages(*, conn)` signature.

## 4. Schema

N/A — no schema change.

## 5. Fetch strategy + rate-limit composition

`per_resource_http` (unchanged from baseline). Composition: bootstrap path uses `PipelinedSecFetcher` for the prefetch (concurrency=4, target_rps=7); steady-state cron path uses the sync ResilientClient (concurrency=1, target_rps=7). Both share `_PROCESS_RATE_LIMIT_CLOCK` so non-S14 SEC traffic (per-CIK poll, atom fast lane, manifest worker) co-exists safely.

**Lane contention**: S14 is on the `sec_rate` lane. Lane serialisation guarantees no concurrent stage on `sec_rate` runs during S14. The 4-way pipeline fully owns the lane's 7 req/s budget for the prefetch window — no starvation surface.

## 6. Conditional-GET semantics

**Pre-loop read** (this PR): one SELECT against `external_data_watermarks` filtered by `source='sec.last_modified.submissions_files'` AND `key IN (<all CIK:page_name pairs>)` returns the full `dict[(cik, page_name), str|None]` map. Pre-loop work; bootstrap and steady-state both use this.

**Per-page IMS resolution** (loop): `if_modified_since = watermarks.get((cik, page_name))`. SAME value passed to the prefetch call AND to the loop's `provider_for_loop.fetch_submissions_page_conditional(...)`.

**Response trichotomy** (unchanged):
- 304 → bump `watermark_at` only (Last-Modified string unchanged); skip parse + skip upsert; count `secondary_pages_not_modified`.
- 200 → parse via `_normalise_submissions_block` → per-filing `_upsert_filing` → on all-clean, persist new Last-Modified via `set_watermark`.
- 404 → no watermark write; skip silently.

**Post-prefetch ordering**: parse + upsert + watermark write happen serially in the existing per-CIK loop. No concurrency on DB writes. Cache lookup is read-only; the watermark write semantics are unchanged.

**Cache miss on transient prefetch failure**: cache key absent. `_CachedSubmissionsPageFetcher.fetch_submissions_page_conditional` falls through to the sync provider, which sends its own GET with the same `if_modified_since` (read from the same `watermarks` dict). Sync provider's ResilientClient applies its existing retry / quarantine policy. Net effect: transient failures during prefetch fall through to the sync retry path — no worse than today.

## 7. Retry posture per error-class

| HTTP status (prefetch path) | Disposition |
|---|---|
| 200 | record `SubmissionsPageResult(payload=<dict>, ...)` in cache; loop processes via cache hit |
| 304 | record `SubmissionsPageResult(payload=None, not_modified=True, ...)` in cache; loop bumps watermark_at via cache hit |
| 404 | record `None` in cache; loop's cache hit returns None → existing skip path |
| 429 / 5xx / transport error / malformed JSON / `json.JSONDecodeError` | OMIT from cache (`dict[page_name] not in cache`); loop's cache miss falls through to sync provider's existing retry path |

## 8. Multi-writer sink registry

**`filing_events`** — three writers, two conflict-resolution policies:

| Writer | File | Conflict policy on URL columns |
|---|---|---|
| `_upsert_filing` (S14 + 4 other callers — coverage.py:1658, coverage.py:1778, filings.py:396, sec_submissions_ingest.py:412) | `app/services/filings.py:611` | `EXCLUDED.source_url` / `EXCLUDED.primary_document_url` — overwrite |
| `_upsert_filing_event` (Chunk E single-accession 8-K gap fill) | `app/services/filings.py:543` | `EXCLUDED.source_url` / `EXCLUDED.primary_document_url` — overwrite |
| `_upsert_filing_from_master_index` (master-index reconcile) | `app/services/fundamentals/__init__.py:2251` | `COALESCE(filing_events.<col>, EXCLUDED.<col>)` — preserve existing |

All three keyed `(provider, provider_filing_id, instrument_id)`.

**Interaction**: per-accession URLs ALWAYS win.
- If master-index runs first → row carries the `.txt` complete-submission URL. S14 runs → `EXCLUDED` overwrites with the per-accession primary doc URL.
- If S14 runs first → row carries the per-accession URL. Master-index runs → `COALESCE` preserves the per-accession URL; does NOT overwrite.

This interaction is the SAME pre and post this PR (the prefetch is purely a perf rewrite of the HTTP layer; the writer is unchanged). Documented here per spec-template §8 because S14 is the more-correct writer in the triple and the spec must declare why the race resolves correctly.

10y retention cap via `filing_within_retention` (`app/services/filings.py:114`) — pre-cap filings silently dropped by `_upsert_filing` (returns False). Telemetry: walker counts `result.filings_upserted` only for accepted rows (existing logic; not changed).

**`external_data_watermarks`** (source `sec.last_modified.submissions_files`):
- Single writer: `set_watermark` from `walk_files_pages` (lines 317, 382).
- Conflict key: `(source, key)`. Unchanged.

## 9. Watermark + retry-budget

Unchanged. Per-page watermark write is gated on `page_upsert_errors == 0` (existing — every filing on the page upserted cleanly OR was intentionally retention-dropped via `_upsert_filing` returning False without raising). When a page raises mid-loop, the watermark stays at the prior value so the next tick re-fetches.

## 10. Encoding / precision / NULL / timezone

UTF-8 JSON. Date precision: SEC `filingDate` field (`YYYY-MM-DD`) — handled by `_normalise_submissions_block`. Timezone: filed_at stored as UTC at the `filing_events.filing_date` DATE column. No change.

## 11. Backfill horizon + retention

10y retention cap (`app/services/filings.py:79` — `filing_events_retention_cutoff`) — pre-cutoff filings silently dropped by `_upsert_filing`. Unchanged.

No backfill required for this PR — the walker re-runs each bootstrap and steady-state cron tick from the same sidecar. First run post-merge populates the watermark for any new pages.

## 12. Partition strategy + extension deadline

N/A — `filing_events` is not range-partitioned. `external_data_watermarks` is a small fixed table.

## 13. Bootstrap vs steady-state mode

**Bootstrap mode** (this PR):
- Stage 14 (`sec_submissions_files_walk` on `sec_rate` lane).
- `resolve_progress_context()` returns non-None → walker runs chunk-and-drain prefetch.
- Default `DEFAULT_PREFETCH_CHUNK_SIZE=1000`. Each chunk's prefetch dict + payloads live until the chunk drain completes; then dropped (`del`) before next chunk. Bounded peak heap ~150-200 MB per chunk (≈50 MB raw JSON + Python overhead).
- Expected HTTP count to SEC: same total as today (one GET per page in the sidecar), temporally compressed to ~1/4 the wall-clock.

**Steady-state mode** (unchanged):
- Cron path runs OUTSIDE the orchestrator's `active_bootstrap_run` window → `resolve_progress_context()` returns None → walker uses the existing serial sync ResilientClient path.
- Operator-trigger API runs OUTSIDE the orchestrator window → same as cron.

**Bootstrap-mode discipline**: this stage is `per_resource_http` (not bulk-eligible — secondary pages are not in `submissions.zip` per `app/services/sec_submissions_files_walk.py:13-17`). Stage qualifies for the `per_resource_http` carve-out under `data-engineer/SKILL.md §6.5.15` — already established.

## 14. Tombstones + soft-delete

N/A — pure walker, no tombstone logic.

## 15. `rows_skipped` closed-set + other

Existing closed set on `FilesWalkResult` (`sec_submissions_files_walk.py:91-109`):
- `ciks_with_no_overflow`
- `ciks_with_empty_sidecar`
- `parse_errors`
- `secondary_pages_not_modified` (304)

New telemetry (added to `FilesWalkResult`):
- `prefetch_pages_seeded: int` — count of unique pages successfully prefetched (size of returned cache dict).
- `loop_pages_from_prefetch: int` — per-loop visits served from the prefetch cache (may exceed `prefetch_pages_seeded` if a CIK's pages appear under multiple instrument_ids — share-class siblings; this counts loop consumptions, not unique fetches saved).
- `loop_pages_from_sync_fallback: int` — per-loop visits that missed the prefetch cache and hit the sync provider.
- `prefetch_window_seconds_total: float | None` — sum of per-chunk prefetch wall-clock deltas across all chunks; `None` when prefetch was disabled (steady-state mode).

End-of-walk INFO log extended with these four.

## 16. Schema-evolution migration path

N/A — no schema change.

## 17. Operator runbooks

No operator action required. If R3 measurement (post-merge) shows memory pressure:
- Tune `DEFAULT_PREFETCH_CHUNK_SIZE` constant down (no schema, no API change).
- Or revert this PR (rollback path described in §1.4).

No `app/runbooks/<source>_<endpoint>.py` artifact in this PR — there is no operator-driven verb (no DELETE, no re-ingest scope, no audit dump).

## 18. Smoke matrix

Bootstrap-driven, so dev-DB validation is via the live R3 measurement run (operator-driven post-merge). Pre-merge unit + integration tests cover:

| Instrument | Why | Verification |
|---|---|---|
| AAPL (CIK 0000320193) | Long history; multiple secondary pages | `filing_events` row count post-walk matches serial baseline exactly |
| GME (CIK 0001326380) | Moderate history; ~1 secondary page | Same as above |
| MSFT (CIK 0000789019) | Long history; multiple secondary pages | Same as above |
| JPM (CIK 0000019617) | Financial-services issuer | Same as above |
| HD (CIK 0000354950) | Retail issuer; moderate history | Same as above |

The pre-merge integration test in `tests/test_s14_uses_sidecar.py` (extended) asserts: walker with the cached wrapper (synthetic 5-CIK cohort, prefetch populated via in-process stub of `prefetch_submissions_pages_conditional`) produces IDENTICAL `filing_events` rows + `external_data_watermarks` rows to the walker run without prefetch.

R3 measurement run records S14 wall-clock pre vs post.

## 19. Cross-source verification

For one instrument (AAPL) in the smoke panel, spot-check the resulting `filing_events` row count vs SEC EDGAR direct browser walk of `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=&dateb=&owner=include&count=40`. Recorded in PR description.

## 20. Test placement

| Test file | Type | New / Extend | Coverage |
|---|---|---|---|
| `tests/test_sec_pipelined_fetcher.py` | Unit | Extend | `prefetch_submissions_pages_conditional` happy path (200/304/404 trichotomy) + transport error cache omission + malformed-JSON cache omission + dedupe |
| `tests/test_sec_pipelined_fetcher.py` | Unit | Extend | `_CachedSubmissionsPageFetcher` cache hit (200/304/404) / cache miss fallthrough / counter wiring |
| `tests/test_s14_uses_sidecar.py` | Integration | Extend | Walker with prefetch wrapper (monkeypatched `prefetch_submissions_pages_conditional` → in-memory cache) produces identical `filing_events` rows + watermark writes vs walker without prefetch (`resolve_progress_context` returns None branch) |
| `tests/test_s14_uses_sidecar.py` | Integration | Extend | Walker inside `active_bootstrap_run` context (monkeypatched `resolve_progress_context` → non-None) calls the prefetch function exactly once with the expected task list shape |
| `tests/test_s14_uses_sidecar.py` | Unit | New test | `_load_all_watermarks_for_pages` returns the expected dict shape for a mocked cursor (walker-local helper; lives in `app/services/sec_submissions_files_walk.py`) |
| `tests/test_s14_uses_sidecar.py` | Integration | New test | Chunk-and-drain shape: 2500-task cohort with `DEFAULT_PREFETCH_CHUNK_SIZE=1000` invokes `prefetch_submissions_pages_conditional` exactly 3 times (1000+1000+500); chunk cache is dropped between chunks (assertion: monkeypatched prefetch is called with disjoint task slices, no overlap) |

No nightly; no contract test (purely internal behaviour).

## 21. Rationale log

**Decision:** prefetch + sync-loop wrapper rather than fully-async walker.
**Rejected:** rewrite `walk_files_pages` as an async coroutine. Reason: DB writes are psycopg-sync; async sync mixing complicates lifecycle (asyncio.to_thread or sync-from-async patterns increase blast radius). Established `prefetch_document_texts` + sync-loop wrapper pattern at three callsites (`app/services/business_summary.py:1717`, `app/services/def14a_ingest.py:1006`, `app/services/eight_k_events.py:773`) proves the simpler shape is sufficient.

**Decision:** new `prefetch_submissions_pages_conditional` rather than extend `prefetch_document_texts`.
**Rejected:** generalise `prefetch_document_texts` to return `SubmissionsPageResult | str` union and handle conditional headers. Reason: function would become an unbounded union; three existing callers don't need conditional behaviour and would have to thread NULL headers everywhere. New function keeps the existing one's contract pristine; shared `_AsyncRateLimiter` + `_PROCESS_RATE_LIMIT_CLOCK` give the same rate-budget integration with zero duplication.

**Decision:** new `_CachedSubmissionsPageFetcher` rather than extend `_CachedDocFetcher`.
**Rejected:** add `fetch_submissions_page_conditional` method to `_CachedDocFetcher`. Reason: same shape-pollution concern. Duck-typed wrapper at the S14 callsite has zero collateral surface.

**Decision:** chunk-and-drain in the WALKER, not internal-chunking inside `prefetch_submissions_pages_conditional`.
**Rejected:** make `prefetch_submissions_pages_conditional` internally iterate chunks and return one giant union dict (v1.1 shape). Reason: Codex 1 v1.1 BLOCKING-4 — the union dict still holds all 17k payloads at end of run; only per-chunk HTTP buffers freed. Chunk-and-drain at the walker level lets the chunk dict + payloads be `del`-ed between chunks, bounding peak heap to one chunk's worth (~150-200 MB).

**Decision:** walker self-detects bootstrap mode via `resolve_progress_context()`; no new param.
**Rejected:** plumb `use_pipelined_prefetch=True` through `StageSpec.params`. Reason: Codex 1 BLOCKING-1 — `_adapt_zero_arg` discards params; full plumbing would require re-shaping the invoker. The contextvar approach mirrors #1273 PR2's `set_stage_target` pattern and is non-invasive.

**Decision:** walker constructs its own provider AND its own wrapper internally; no external provider injection.
**Rejected:** add `provider: SecFilingsProvider | _CachedSubmissionsPageFetcher` to `walk_files_pages` signature. Reason: Codex 1 BLOCKING-2 — the invoker only receives `(conn=...)`. Internal construction keeps the test seam at the `prefetch_submissions_pages_conditional` module symbol (monkeypatched in tests), not at the walker's signature.

**Decision:** pre-load all watermarks in one SELECT before building `ConditionalFetchTask` list.
**Rejected:** per-page `get_watermark` calls inside the prefetch loop. Reason: ~17k extra round-trips dwarf the SQL cost. One batched SELECT is O(cohort) DB cost. The pre-loaded dict is also the source of IMS for the per-CIK loop — same value used in both places, so prefetch and loop send the same conditional header.

**Decision:** the master-index writer's `COALESCE` semantics + S14's `EXCLUDED` semantics are documented but UNCHANGED in this PR.
**Rejected:** change `_upsert_filing` to use COALESCE for url columns. Reason: per-accession URLs are MORE correct (downstream parsers need them); preserving S14's "overwrite on conflict" is the intended interaction. Documented in §8 so future readers don't try to "harmonise" the writers and break the race resolution.

## 22. Open questions

None. Stale docstring in `_upsert_filing` (cited by Codex 1 NIT-2: it references a 2-tuple key but SQL uses 3-tuple) gets fixed inline in the implementation PR per the skill-ownership rule.

---

## Acceptance

- Pre-merge: tests pass; serial-vs-prefetched parity verified on AAPL/GME/MSFT/JPM/HD fixtures.
- Post-merge: R3 measurement run records S14 wall-clock < 10 min on clean-DB bootstrap.

## Out

- Master.idx walk for `sec_filing_manifest` parity. Already covered today by `sec_master_idx_quarterly_sweep` (`app/jobs/sec_master_idx_quarterly_sweep.py`) — no extra work needed.
- Refactor of `walk_files_pages` to fully-async. Out of scope (per §21).
- Refactor of `_drain_secondary_pages` redundant primary fetch (#1277 NIT-2). Out of scope.
- Make `_adapt_zero_arg` params-aware. Out of scope; the contextvar approach is sufficient for this PR's needs.
