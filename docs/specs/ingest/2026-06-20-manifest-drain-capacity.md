# Manifest backlog drain capacity (#1686)

**Status:** spec • **Issue:** #1686 • **Date:** 2026-06-20

## Problem — premise FALSIFIED on full population (dev DB)

Issue premise: "~1.46M pending at the shared 10 req/s … oldest-first drain will take a very long time." Two claims, both checked against the entire pending population (not a sample):

**Claim 1 — backlog size: TRUE.** Total `ingest_status='pending'` = **1,438,812**.

| source | pending | pre-fetch-gated? | bulk-tombstone-able (exact gate) |
|---|---|---|---|
| sec_form4 | 1,053,204 | yes — `filed_at < today−3y` | **606,471** |
| sec_13f_hr | 198,136 | no (post-parse gate on `period_of_report`, not `filed_at`) | 0 |
| sec_10q | 60,423 | **no gate** | 0 |
| sec_form3 | 50,809 | **no gate** (only Form 4 has the pre-fetch gate in `insider_345`) | 0 |
| sec_def14a | 31,596 | **no gate** | 0 |
| sec_13g | 30,606 | yes — `filed_at < max(today−3y, 2024-12-18)` | 0 (all post-mandate) |
| sec_13d | 14,038 | yes — same blockholders cutoff | **8,439** |
| **total** | **1,438,812** | | **614,910 (~43%)** |

**Claim 2 — "10 req/s is the constraint": FALSE.** `app/providers/concurrent_fetch.py:4-6` documents that each SEC fetch is **latency-bound (~700-900ms ≈ 1 req/s serial)**, so "every SEC ingest job uses about 10% of the allowed 10 req/s budget." `run_manifest_worker` fetches **serially** (`for row in rows: parser fetches`) → it uses <10% of the budget. The real constraints are:
1. `max_rows=100`/tick + every-5-min cadence (`scheduler.py:1408`) = 28,800 rows/day → ~50 days oldest-first.
2. Within a tick, fetched rows issue serially at ~1 req/s (matches the observed 20-37s active window, `scheduler.py:1395`).
3. ~43% of the backlog is **pre-retention** — the parser tombstones it **without any HTTP** (`insider_345.py:163`, `sec_13dg.py:162`) — yet the worker burns its scarce 100-row/tick budget doing this free work 74-at-a-time through the full per-row parser dispatch.

## Source rule
No repo invariant requires SEC ingest order (`iter_pending*` order is our operational freshness choice, #1685); recency + retention horizons are **our settled invariants**, each enforced by an existing chokepoint function — Phase 1 reuses them verbatim (no re-derivation):
- Form 4: `insider_transactions.form4_retention_cutoff()` = `today − 3y` calendar-exact (#1233 §4.3, pre-fetch gate `insider_345.py:163`).
- Form 5: `insider_transactions.form5_retention_cutoff()` = `today − 18 months` (#1233 §4.4, pre-fetch gate `insider_345.py:610`). Included in the bulk sweep even though dev currently shows 0 `sec_form5` pending — the gate exists, so the sweep covers it for correctness/future backlog (Codex ckpt-2 MED). Form 3 has NO retention gate → excluded.
- 13D/13G: `blockholders.blockholders_retention_cutoff()` = `max(today − 3y, 2024-12-18 XML-mandate)` (`blockholders.py:106`, pre-fetch gate `sec_13dg.py:162`).
- 13F-HR: `institutional_holdings.thirteen_f_retention_cutoff()` is a **post-parse** gate on `period_of_report` (`sec_13f_hr.py:332`) — **NOT** `filed_at` → **excluded** from Phase 1 (a `filed_at` bulk sweep would wrongly tombstone in-scope filings whose period is recent).
- 10Q / 10K / Form 3 / DEF 14A: **no pre-fetch retention gate** → **excluded** (a bulk `filed_at` tombstone would destroy fetchable, in-scope rows = silent data loss).

**Full-population check:** the table above is the entire pending population; the bulk-able counts use each source's exact cutoff function, queried over all rows.

## Settled-decisions / prevention-log applied
- **Tombstone state contract** (`sec_manifest.transition_status`, L477+): a `tombstoned` write sets `ingest_status='tombstoned'`, `error=<reason>`, `next_retry_at=NULL`, `last_attempted_at=clock_timestamp()`. Phase 1's bulk UPDATE reproduces this exactly and **does not touch `raw_status`** — so the #948 evidence-downgrade guard is irrelevant regardless of any row's current `raw_status` (no claim about pending rows' raw_status is needed; Codex ckpt-1 MED).
- **Concurrent double-tombstone race (Codex ckpt-1 HIGH).** Worker row selection is non-locking (`iter_pending*` plain SELECT, `sec_manifest.py:580`); the worker only takes `FOR UPDATE` inside `transition_status` AFTER parser work (L400). So the sweep can tombstone a pending row the worker has already selected. The worker's parser then applies the **identical** pre-fetch gate and returns `status='tombstoned'` (the sweep target set ⊆ the parser's own tombstone predicate, so the worker can never *parse* a row the sweep tombstoned — Codex's `tombstoned→parsed` framing cannot occur). But `transition_status(tombstoned)` reads `current='tombstoned'`, and `_ALLOWED_TRANSITIONS['tombstoned']={pending}` (L171) → **ValueError → aborts the tick** (the final transition_status is NOT in the per-row try/except). **Fix:** make `transition_status` treat a `tombstoned→tombstoned` self-transition as an **idempotent no-op** (early return, NO re-stamp of `last_attempted_at`/`error`) — a redundant tombstone from a concurrent writer reaching the same terminal state is benign. This is the minimal, deliberate refinement of the #879 "self-transition must be explicitly allowed" rule (we allow ONLY the terminal-idempotent case, and as a no-op, so it cannot mask a double-error write). Sweep stays `ingest_status='pending'`-only, so the reverse (sweep picking an already-tombstoned row) is excluded by its WHERE clause.
- **Reversible:** `sec_rebuild` re-pends tombstoned rows. Phase 1 reaches the identical state the parser already reaches — it is not a new policy, just batched.
- **L1412-1430 (autocommit orchestrator + batched commits):** the sweep follows the `financial_facts_retention.sweep_retention_all_instruments` sister pattern — own autocommit conn, batched per-source UPDATEs each in its own `conn.transaction()`, so a 600k-row sweep never opens one giant lock-holding tx.
- **service-no-commit invariant (L1268):** the bulk-sweep service that takes a caller conn must not commit; the `_job` wrapper / autocommit orchestrator owns the lifecycle.
- **L1474 (per-source numeric coincidence):** cutoffs are sourced per-source from their own function; never a shared literal.

## Phase 1 — bulk pre-retention tombstone sweep (SQL-only, no HTTP)

New service `app/services/manifest_pre_retention_sweep.py`:

```
GATED_SOURCES = {
    "sec_form4": lambda: form4_retention_cutoff(),
    "sec_13d":   lambda: blockholders_retention_cutoff(),
    "sec_13g":   lambda: blockholders_retention_cutoff(),
}
```
(single source of truth — the cutoff functions, not literals.)

`sweep_pre_retention(*, batch_size=5000) -> dict[str,int]`: opens its own autocommit conn; per source, loops:
```sql
UPDATE sec_filing_manifest
   SET ingest_status='tombstoned', error='retention floor (bulk pre-fetch sweep)',
       next_retry_at=NULL, last_attempted_at=clock_timestamp()
 WHERE accession_number IN (
   SELECT accession_number FROM sec_filing_manifest
    WHERE source=%(source)s AND ingest_status='pending' AND filed_at::date < %(cutoff)s
    ORDER BY accession_number LIMIT %(batch)s FOR UPDATE SKIP LOCKED)
```
each batch in its own `conn.transaction()`; loop until 0 rows updated; return per-source counts. `FOR UPDATE SKIP LOCKED` so the sweep never **blocks** on a row the worker is mid-`transition_status` on (the worker briefly holds that row's `FOR UPDATE`); the sweep skips it this batch and the worker's own gate tombstones it. The residual race where the sweep tombstones a row the worker already selected is handled by the idempotent `tombstoned→tombstoned` no-op above — NOT by SKIP LOCKED (worker selection is non-locking, so SKIP LOCKED cannot see it).

Wire `JOB_SEC_MANIFEST_PRE_RETENTION_SWEEP` ScheduledJob, daily, `connect_job()` body, `catch_up_on_boot=True`. Daily keeps the boundary swept steady-state (rows age past the rolling 3y cutoff every day); the boot run clears the standing 614,910 on first restart.

**Why not just raise the worker cap for these:** the worker fetches+parses per row; these rows need neither. A direct UPDATE is the same transition without the dispatch machinery, and it does not consume the per-tick budget that fetchable rows need.

## Phase 2 — concurrent body prefetch (in-retention / no-gate slice, ~824k)

Goal: overlap the ~800ms fetch latency up to the shared 10 req/s ceiling **without** introducing concurrent DB writes (preserves every parser's transaction / advisory-lock / sibling-resolution invariant — parse+upsert stay serial on the worker conn).

Pattern = the module's documented one (`concurrent_fetch.py:119` "fetch+parse in parallel, then upsert serially"):

1. **Per-source fetch-URL hook.** Add an optional `fetch_url: Callable[[ManifestRow], str | None]` to `ParserSpec` / `register_parser` — returns the **single** canonical URL the parser will GET (mirrors the parser's own canonicalizer). `None` (default, or per-row) = no prefetch → that row stays serial. **v1 wires ONLY `sec_form3`/`sec_form4`/`sec_form5`** (all three share `_canonical_form_4_url`, a clean single-doc GET with no intervening fetch). The hook MIRRORS the parsers' pre-fetch tombstone gates — returns `None` for missing `instrument_id`/`filed_at` and for past-retention Form 4 (3y) / Form 5 (18mo), reusing the SAME `form4_within_retention`/`form5_within_retention` predicates — so the prefetch never wastes SEC budget on a row the parser would tombstone-without-fetch (Codex ckpt-2 HIGH). Form 3 has no retention gate (fetched whenever it has a URL). This is the dominant fetchable slice — Form 4 in-retention ≈446k (54% of the ~824k post-Phase-1 fetchable), +Form 3 ≈50k ≈60%. **Deferred (mechanism-ready, no hook in v1, stay serial):** `sec_13f_hr` (multi-step `index.json`→primary→infotable, `sec_13f_hr.py:161,239,323` — Codex ckpt-1 MED); `sec_10k`/`sec_10q` (multi-doc index walk, `sec_10k.py:198`); `sec_13d`/`sec_13g` (URL **built** from CIK+accession, not read from the row, `sec_13dg.py:174`); `sec_def14a` (has a pre-fetch "latest-N primary cap" that tombstones some rows → a blind prefetch would waste rate budget). Each is an incremental follow-up on the same hook.
2. **Concurrent prefetch — fairness path only.** `_prefetch_then_dispatch` wraps the serial `_dispatch_rows` for the **steady-state fairness path (`source is None`)** — the every-5-min cron + boot catch-up that drains the backlog (#1686's target). The **per-source rebuild path (`sec_rebuild`, operator-triggered + scoped)** calls `_dispatch_rows` directly and stays serial (small, operator-paced, no concurrency win needed; also keeps the per-source-path tests asserting exactly-one fetch). `_prefetch_bodies(rows)` resolves `fetch_url` for every hooked row, calls `fetch_document_texts(provider, urls)` (default 8 workers) → `{url: body}`, keeping **successful `str` bodies only**. Shared `_PROCESS_RATE_LIMIT` throttle keeps aggregate ≤10 req/s. Empty `{}` when no row has a hook → tick behaves exactly as pre-#1686.
3. **Cache-first read via tick-scoped ContextVar.** `SecFilingsProvider.fetch_document_text` consults `_PREFETCH_BODY_CACHE` (a `ContextVar` set by `_dispatch_rows` for the tick, reset in `finally`) before issuing HTTP. **DESIGN CHANGE from ckpt-1's "explicit param":** every parser constructs its OWN `SecFilingsProvider` per row, so an explicit cache param would churn 9 parser signatures + their provider construction; a ContextVar at the single fetch chokepoint is the minimal, tick-scoped injection (read in the same thread the worker set it in; serial parse path). The cache is ALWAYS set (even empty) and reset via token, so a value can never leak across ticks on the apscheduler threadpool. **Successful bodies ONLY:** a prefetch `None` (404 OR caught exception — `fetch_document_texts` collapses both, `concurrent_fetch.py:19-21`) is never cached → always falls through to the serial fetch, which re-raises non-404 errors so the parser keeps its exception→`failed`(retry) vs empty/non-200→`tombstoned` discrimination (Codex ckpt-1 HIGH — a transient prefetch failure can never become a permanent tombstone). **Safety property:** a `fetch_url`/canonicalizer mismatch → miss → serial fetch. Wrong key never yields wrong data; worst case is no speedup for that row.
4. Parse + DB writes stay **serial** on the single worker conn. No new write concurrency.

Expected: ~824k fetchable / ~10 req/s ≈ **~19h** vs ~7.8 days serial (after Phase 1 shrinks the denominator). Once concurrent, `max_rows` can rise (the fetch is no longer the per-tick wall) — bump to e.g. 200 and let the tick wall-clock stay bounded by the prefetch.

### Resolved at ckpt-1
- **Prefetch budget:** prefetch all `fetch_url`-eligible selected rows (≈`max_rows/10` s wall-clock ≈ 10s at 100 rows) — acceptable inside the own-lane worker (already 20-37s, `scheduler.py:1395`). No separate prefetch cap in v1.
- **Cache injection shape:** explicit `cache: dict[str,str]` param threaded through `_dispatch_rows` → parser dispatch (testable, no global state). Rejected the `contextvar`-at-provider alternative (hidden global state, harder to test).
- **Per-thread connection alternative: REJECTED** (Codex concurs) — concurrent *dispatch* adds concurrent writes to shared instrument/observation rows within a tick → deadlock/contention surface; prefetch-then-serial-parse avoids it for the same throughput win.

## Tradeoffs
- Phase 1 tombstones 614,910 rows in one boot sweep. Reversible (`sec_rebuild`); identical to the parser's own outcome. The daily cadence keeps the rolling boundary swept.
- Phase 2 prefetch lengthens each tick's wall-clock (overlapped fetch) but the worker owns its lane (`scheduler.py:1395`), so it does not starve producers; the 10 req/s ceiling is still enforced HTTP-side.
- `max_rows` bump is deferred behind Phase 2 — a serial worker with a big cap would just block the tick on serial fetches.

## Tests
- **`transition_status` idempotent tombstone (pure-logic / `db`):** `tombstoned→tombstoned` returns without raising AND without re-stamping `last_attempted_at`/`error` (assert the row's existing tombstone fields are unchanged); every OTHER illegal transition still raises (regression on the #879 rule — only the terminal-idempotent case is exempted).
- **Phase 1 (pure-logic where possible):** `GATED_SOURCES` maps exactly the three pre-fetch-gated sources; each value resolves to its source's cutoff function (assert identity, not a copied literal). One `db` test: seed pending rows straddling the cutoff per gated source + one row each for an **excluded** source (10q/def14a/13f) older than 3y → assert only gated pre-cutoff rows flip to `tombstoned`, excluded rows stay `pending`, in-retention gated rows stay `pending`. Batch-loop terminates at 0. `FOR UPDATE SKIP LOCKED` skips a concurrently-locked row.
- **Phase 2:** `fetch_url` hook returns the same canonical URL the parser GETs (per source — assert against the parser's own canonicalizer); `sec_13f_hr.fetch_url` returns `None` (excluded). Worker test: prefetch `str` cache hit → parser does NOT call `provider.fetch_document_text` (mock asserts 0 calls); cache miss → serial fetch (1 call); **cache value `None` → serial fetch (1 call), NOT a tombstone** (the HIGH-fix: transient-failure-as-None must re-attempt serially, never permanent-tombstone). Mirror L1337's `sorted(registered_parser_sources())`.

## DoD (ETL clauses 8-12)
- **Phase 1 on dev:** run the sweep; record per-source tombstoned counts (expect form4≈606k, 13d≈8.4k, 13g=0); confirm `SELECT count(*) … pending` drops by ~615k; confirm an **excluded** source's pre-3y pending count is **unchanged** (no over-tombstone); confirm a tombstoned row re-pends under `sec_rebuild` (reversibility).
- **Phase 2 on dev:** after wiring + a few ticks, confirm fetched-row throughput per tick rises (parsed count/tick up) with aggregate HTTP ≤10 req/s; record before/after parsed-per-tick.
- **No data-treatment change:** neither phase changes parser OUTPUT (Phase 1 = batched form of an existing transition; Phase 2 = same bodies, fetched concurrently). Clauses 9/11 (cross-source figure) N/A — record why. Smoke panel (clause 8) N/A — no ownership/fundamentals parser/schema change.
- Record commit SHA + the dev observations per phase.
