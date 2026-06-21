# Manifest Phase 2 — extend concurrent prefetch + raise max_rows (#1700)

**Status:** spec • **Issue:** #1700 (follow-up to #1686, core merged 0e1f60db) • **Date:** 2026-06-21

## Problem — premise checked on full population (dev DB, post-#1686 sweep)

The #1686 follow-up list said: "Phase 2 extend to 13f/10k/10q/13d/g/def14a; raise max_rows once concurrent." Each target checked against the entire pending population + the live worker's per-tick behaviour (not a sample):

### Per-tick reality (observed, stable across consecutive recent ticks — `/tmp/ebull_jobs.log` + `job_runs`, max_rows=100)
```
processed_by_source = {sec_10q:8, sec_13d:8, sec_13f_hr:52, sec_13g:8, sec_def14a:8, sec_form3:8, sec_form4:8}
```
This vector is **deterministic, not a one-off sample**: the Phase B top-up (`iter_pending_topup`) is global-oldest, and 13f holds the oldest pending rows, so it dominates the top-up every tick until its oldest prefix drains (the distribution is identical across all recent successful ticks in `job_runs`). The design depends only on the *structural* fact (13f is multi-doc-serial AND dominates the top-up), not on the exact `52`.
- **13f_hr eats ~52 of 100 rows** — oldest globally-pending → Phase B global-oldest top-up fills with 13f.
- 13f does **3 serial fetches per row** (`index.json` → `primary_doc.xml` → `infotable.xml`, all via `provider.fetch_document_text` — `sec_13f_hr.py:171,245,…`). 52 × ~2.4s ≈ 125s of serial fetch.
- Measured tick duration (`job_runs`): **50–300s**, several already at the **299s** cadence ceiling (cadence = every 5 min, `scheduler.py:1414`). **A naive max_rows raise blows the tick past 300s → lock contention on the next fire.** 13f concurrency must come first.

### Pending by source (the FETCH-bound backlog)
| source | pending | hooked today? | shape | action |
|---|---|---|---|---|
| sec_form4 | 445,011 | ✅ (insider_345) | single-doc | already concurrent |
| sec_13f_hr | 191,522 | ❌ | **multi-doc** (index→primary→infotable) | **2-phase prefetch** |
| sec_10q | 59,388 | ❌ | **synth no-op — NO fetch** (`sec_10q.py:72` returns parsed, no HTTP) | **EXCLUDE** (nothing to prefetch) |
| sec_form3 | 49,769 | ✅ (insider_345) | single-doc | already concurrent |
| sec_def14a | 30,556 | ❌ | single-doc | **single-doc hook** (needs conn for cap gate) |
| sec_13g | 29,567 | ❌ | single-doc | **single-doc hook** |
| sec_13d | 4,625 | ❌ | single-doc | **single-doc hook** |
| sec_10k | 0 pending (29,789 `deferred`) | ❌ | multi-doc | **EXCLUDE** (never picked + multi-doc) |
| sec_8k | 0 pending (348,178 `deferred`) | ❌ | multi-doc | **EXCLUDE** (never picked) |

**Falsifications of the handoff premise:**
1. The handoff said "beyond the form4/5/13d/13g gates" — implying 13d/13g were already hooked. **They are not.** Only `insider_345.py` registers `fetch_url` (form3/4/5). 13d/13g/def14a still need hooks.
2. **10q is a synth no-op** (no HTTP at all) — prefetch is meaningless; it already drains free.
3. **10k/8k have 0 pending** (all `deferred`; the worker picks only `pending`+`failed`, `sec_manifest_worker.py:252,289,307`) — and are multi-doc. No drain benefit, infeasible as single-doc. Excluded.
4. The worker tick is **duration-bound by 13f's serial multi-doc fetches**, not the 10 req/s budget (which it uses <12%). Raising max_rows safely *requires* making 13f concurrent first.

`#1698`'s 130 starved Form-4s: the original tracked set (staging table dropped) is unrecoverable, but the coverage gap is now purely capacity-bound (tombstoned-out-of-retention + the pending backlog — no tombstone block); operator figures current (eBay 2026-06-18). Mid-age form4 (90d–2y, 256k pending) drains last under oldest-first Phase A/B; Phase R (<90d) + the per-instrument insider lane cover freshness. The capacity raise is the lever for the historical band; **no drain-order change** (that would alter the settled #1685 Phase R / #1179 fairness model — out of #1686 capacity scope).

## Source rule
- **Ph2 contract (settled by #1686 + #1698, prevention-log L1294/1297):** the prefetch cache holds **successful bodies only**; a `None` (404 OR caught exception, indistinguishable through `fetch_document_texts`) is dropped → the serial parser re-fetches and keeps its own transient-vs-permanent discrimination → **a prefetch failure can never become a permanent tombstone.** The `fetch_url` hook **must mirror every PRE-FETCH gate** the parser applies, or prefetch wastes SEC budget on rows the parser then tombstones (Codex ckpt-2 HIGH, #1686).
- **Pre-fetch gates per source** (verified in the parser bodies, BEFORE the first `fetch_document_text`):
  - **13d/13g** (`sec_13dg.py:105–179`): (1) `row.cik` non-empty; (2) `_zero_pad_cik(cik) not in KNOWN_FILING_AGENT_CIKS`; (3) `blockholders_within_retention(row.filed_at)`. URL = `_archive_file_url(filer_cik, accession, "primary_doc.xml")`. No `instrument_id` gate → hook must not add one. No conn needed.
  - **def14a** (`def14a.py:152–211`): (1) `row.primary_document_url` present; (2) `(row.form or "").upper() != "PRE 14A"`; (3) `row.instrument_id is not None`; (4) `def14a_within_cap(conn, accession_number, instrument_id)` — **needs the DB conn** (ranks latest-N proxies via `filing_events`). URL = `row.primary_document_url`.
  - **13f** (`sec_13f_hr.py:117–167`): the parser fetches **three** docs but with a gate **between** the 2nd and 3rd: `index.json` (gated only by cik+not-agent) → parse → `primary_doc.xml` (same gates) → parse → **`thirteen_f_within_retention(info.period_of_report)`** (`sec_13f_hr.py:332`) → only then `infotable.xml`. So `infotable.xml` IS gated by a post-primary-parse retention check (Codex ckpt-1 HIGH). **Prefetch covers index.json (pass 1) + primary_doc.xml (pass 2) ONLY; `infotable.xml` stays serial** — fetched live only after the parser's retention gate passes, exactly as today. This respects the mirror-every-gate contract: an out-of-retention 13f row tombstones after the primary parse and its (potentially large) infotable is never fetched. Out-of-retention rows (the bulk of the oldest top-up backlog) thus get FULL overlap (no serial fetch); within-retention rows keep one serial infotable fetch. Pass-1 URL = `_archive_file_url(cik, accession, "index.json")`; pass-2 URL (from `parse_archive_index`) = `_archive_file_url(cik, accession, primary_name)` only.

## Settled-decisions / prevention-log applied
- **L1294/1297 (#1698 concurrent-fetch None discrimination):** preserved unchanged — the cache stays successful-bodies-only (`sec_manifest_worker.py:377`); parsers are not modified, so their transient/permanent tombstone logic is untouched. New hooks only *add* URLs to prefetch; a cache miss is always safe.
- **L1344 (#1179 compute_quotas test ordering):** quotas scale with `max_rows`; raising the literal does not change the rotation or `sorted(registered_parser_sources())` order. Existing fairness tests parametrise on `max_rows` and stay valid.
- **#1685 Phase R / #1179 fairness:** unchanged. `recent_budget = min(30, max_rows//2)` and `compute_quotas` both scale cleanly to max_rows=200 (recent_budget stays capped at 30; backlog budget grows). No model change.
- **def14a cap gate is the source of truth (#1233 PR5):** the hook calls the SAME `def14a_within_cap` the parser calls — no duplicated rank logic.

## Design

### 1. Widen the prefetch hook to `(conn, row)` + add a second-phase expander
`app/jobs/sec_manifest_worker.py`:
- `FetchUrlFn = Callable[[psycopg.Connection, ManifestRow], str | None]` (was `Callable[[ManifestRow], str | None]`). The insider hook gains an unused `conn` param.
- New optional `ExpandUrlsFn = Callable[[str, ManifestRow], list[str]]` on `ParserSpec.expand_urls` — given a **prefetched pass-1 body** + the row, return additional URLs to prefetch concurrently in pass 2. Only 13f registers one. Pure (no conn).
- `register_parser(..., fetch_url=None, expand_urls=None)`.

`_prefetch_bodies(conn, rows)` becomes two passes:
1. **Pass 1:** collect `spec.fetch_url(conn, row)` URLs, fetch concurrently, cache successes (`{url: body}`).
2. **Pass 2:** for each row whose source has `expand_urls` AND whose pass-1 URL is a cache hit, call `expand_urls(pass1_body, row)` → more URLs; fetch those concurrently; merge successes into the cache.

Both passes use `concurrent_fetch.fetch_document_texts` (None dropped). `_prefetch_then_dispatch` already binds the merged cache via the ContextVar and resets in `finally`. Pass 2 reuses the SAME `SecFilingsProvider` context as pass 1 (one provider, two concurrent batches).

### 2. Per-source hooks
- **`sec_13dg.py`** — `_blockholder_fetch_url(conn, row)`: mirror gates (cik present, not agent CIK, `blockholders_within_retention(filed_at)`) → `_archive_file_url(filer_cik, accession, "primary_doc.xml")`. Register on `sec_13d` + `sec_13g`.
- **`def14a.py`** — `_def14a_fetch_url(conn, row)`: mirror gates (url present, not PRE 14A, instrument_id present, `def14a_within_cap(conn, …)`) → `row.primary_document_url`.
- **`sec_13f_hr.py`** — `_thirteen_f_index_url(conn, row)`: mirror gates (cik present, not agent CIK) → `_archive_file_url(cik, accession, "index.json")`. `_thirteen_f_expand(index_body, row)`: `parse_archive_index(index_body)` → `[]` if `primary_name` OR `infotable_name` is None (the parser tombstones before the primary fetch when EITHER is missing — Codex ckpt-2 P2; the ~55k pre-2013 no-infotable backlog must not prefetch a primary the parser discards), else `[_archive_file_url(cik, accession, primary_name)]` (**primary only — NOT infotable**, see source rule). Register `expand_urls=_thirteen_f_expand`. The expander calls the **identical** `parse_archive_index` the parser uses (`institutional_holdings.py:385`) — the attachment-name resolution is the parser's own treatment inherited verbatim, so the hook cannot diverge from what the parser fetches (no re-derived heuristic).

Each hook reuses the SAME predicate functions the parser calls (no duplicated date/rank math). An over-broad `None`/URL is safe (cache miss → serial, never wrong data); over-broad fetch only wastes one request, which the mirrored gates prevent.

### 3. Raise max_rows 100 → 150 (empirically tuned)
`scheduler.py::sec_manifest_worker_tick` literal `max_rows=100` → **`150`** + comment. Initially set to 200; **dev-verify falsified 200**: the first post-restart tick ran **293s** (13f=114/200, `processed=200 parsed=70 tombstoned=130 failed=0`, no 429s) — grazing the 300s cadence ceiling (worsened by boot-job contention). 13f's `infotable.xml` stays serial behind its post-primary retention gate AND 13f dominates the global-oldest top-up (~57%), so the tick wall-clock scales with 13f in-retention infotable fetches faster than the prefetch saves. **150 is the conservative steady-state value** that keeps margin under the cadence; raising further is gated on making 13f's infotable concurrent too (follow-up).

## Tests
- **Pure-logic** (`tests/test_sec_manifest_worker.py` + per-parser): each new hook returns the URL when gates pass and `None` when each gate fails (table-test per gate). 13f `expand_urls` returns `[primary_url]` (primary only) on a valid index.json, `[]` on a missing-`primary_name` index. Pass-2 only runs for rows with a pass-1 cache hit.
- **Pure-logic:** `_prefetch_bodies` two-pass — pass-2 URLs prefetched only when pass-1 hit; a pass-1 miss (None) skips that row's pass-2 (no expand on absent body); merged cache contains both passes' successes.
- **DB tier** (`-m db`): (a) a **within-retention** 13f row dispatched via the fairness path is served index + primary from cache and fetches infotable LIVE (1 serial fetch), reaching `parsed` identically to the serial path; (b) an **out-of-retention** 13f row is served index + primary from cache, **never fetches infotable** (assert via provider fetch-count / spy), and tombstones post-primary exactly as the serial path. Reuse the existing manifest-worker db harness.

## DoD (ETL clauses 8–12)
- Smoke panel + cross-source: verify 13f rollup for a known filer unchanged (e.g. AAPL Vanguard) post-change.
- Dev: restart daemon on the merge SHA; confirm tick `processed_by_source` + duration; confirm 13f drains via cache (fetch overlap visible as shorter tick), max_rows=200 holds < 300s, no 429 storm.
- Record figures + commit SHA in the PR.

## Out of scope (documented)
- 10q (no-op), 10k/8k (0 pending deferred). 13f→fully-concurrent unlocks a *later* max_rows raise beyond 200 if dev headroom allows. Drain-order / per-source-priority rebalancing (mid-age band) — separate from #1686 capacity; would touch the settled #1685/#1179 models.
