# Visibility-driven live prices — functional spec

Date: 2026-04-25
Author: Luke / Claude
Status: Draft v3 (post-Codex round 3; pending operator sign-off)

## Goal

Live prices on every visible-on-screen instrument across the eBull web UI
(portfolio, copy-trading, dashboard, instrument page), with FX
conversion to operator's display currency. **What the operator can see,
streams. What they can't see, doesn't.** No background subscriptions.

Watchlist is intentionally out of scope: `WatchlistPanel.tsx` has no
price surface today; adding one is a separate UX decision.

## Why

Operator complaint (2026-04-25):
1. After #490, app boot logs `subscribed to 5 instrument topics` even when
   no portfolio page is open. Wasted bandwidth + DB writes.
2. Copy-trader rows render with a static price, not live ticks.
3. FX rates refresh runs hourly even when nothing on screen needs them.

Codex second-opinion (default model, 2026-04-25, sessions
019dc4c0 + resume): confirmed the multi-worker overlay (#479) is overbuilt
for a single-uvicorn-worker single-operator deployment. Recommended
**keeping** `EtoroWebSocketSubscriber`, `QuoteBus`, SSE +
ref-counting; **deferring** advisory lock + LISTEN/NOTIFY + listener.
Round 2 caught real bugs in spec v1 — see "Resolved findings" below.

## Non-goals (locked)

- **Multi-worker arbitration / leader election.** Deferred until prod is
  actually multi-worker. Issue #479 stays open, descope the work.
- **Auto-pinning held positions or watchlist** at the WS subscriber level.
  Visibility owns subscriptions. If a page renders a held row, it
  subscribes; if no page renders it, no subscription.
- **Cross-worker tick fan-out** (LISTEN/NOTIFY). Same reason as above.
- **Cluster-wide page-view ref propagation.** Out of scope; trivially
  correct in single-worker.
- **Replacing REST snapshot price.** REST `current_price` stays as the
  hydration source on first paint; live tick overlays it.
- **Frontend chart wiring.** Existing chart components are out of scope.
- **eToro WS protocol changes** (auth handshake, reconnect, debounced
  reconcile, dynamic add/remove). Already correct after #487/#488/#490.
- **Watchlist live prices.** No price surface in `WatchlistPanel.tsx`
  today; adding one is a separate ticket.
- **Pre-warming `quotes` for non-visible instruments.** If a future
  scoring/ranking path needs warm prices, it should explicitly request
  them, not lean on a hidden subscriber-level pin (Codex Q1).

## Current state (anchors)

### Subscriber (auto-pin to remove)
- `app/services/etoro_websocket.py:79` — `_SOURCE_RECONCILE_INTERVAL_S = 60.0`
- `app/services/etoro_websocket.py:298` — `fetch_watched_instrument_ids`
  (held ∪ watchlist DB selector — caller of the auto-pin path).
- `app/services/etoro_websocket.py:407` — `self._source_topic_set: set[int]`
  (auto-pin worker's private ledger of refs it owns).
- `app/services/etoro_websocket.py:414-415` — source-reconcile signal + task
  fields.
- `app/services/etoro_websocket.py:488` — `await self._refresh_source_topics()`
  inside `start()`.
- `app/services/etoro_websocket.py:614` — `self._source_reconcile_signal.set()`
  hook after the post-portfolio-sync reconcile.
- `app/services/etoro_websocket.py:623-718` — `_refresh_source_topics` +
  `_source_reconcile_worker` definitions.

### SSE / hooks
- `app/api/sse_quotes.py:212` — page-view `await ws_subscriber.add_instruments(id_list)`
  on stream open.
- `app/api/sse_quotes.py:243` — page-view `await ws_subscriber.remove_instruments(id_list)`
  on stream close.
- `frontend/src/lib/useLiveQuote.ts:62-148` — **one EventSource per hook
  instance**. With per-cell hook calls this hits the browser's ~6
  SSE-per-origin cap on portfolio rows ≥7 (Codex finding 2). Hook needs
  page-level redesign — see PR B.
- `frontend/src/components/instrument/SummaryStrip.tsx:20` — only
  current call site.

### FX
- `app/workers/scheduler.py:225` — `JOB_FX_RATES_REFRESH = "fx_rates_refresh"`.
- `app/workers/scheduler.py:2611` — `fx_rates_refresh()` (Phase 1
  Frankfurter conditional GET; Phase 2 eToro batch quotes).
- `app/workers/scheduler.py:2655-2659` — `quoted_at` is set to **ECB
  publication date**, NOT fetch time. Treating its age as a freshness
  signal would spuriously refetch every weekend (Codex finding 5).
- `app/services/fx.py:49-66` — `load_live_fx_rates`,
  `load_live_fx_rates_with_metadata`.
- Non-SSE readers of `live_fx_rates` (Codex finding 6):
  - `app/api/portfolio.py:327` (overview), `:692`, `:848`
  - `app/api/copy_trading.py:215` (list), `:337` (detail)
  - `app/services/budget.py:520` (degrades `tax_usd = 0` on missing
    GBP→USD — not just a UI concern, affects safety logic)
  - Plus transitive readers via these paths.

### Copy-trader data
- `app/api/copy_trading.py:235` — `copy_mirror_positions` query with
  `instrument_id` per row. Same shape as `broker_positions`.

## Architecture invariants (post-spec)

1. **Subscription set ≡ visibility ref-count set.** No path that bumps a
   ref outside of an SSE stream open / close. No DB-backed selector
   inside the subscriber.
2. **One mechanism for held / copy-trader / instrument page.** Each row
   that renders a price calls into a page-level live-quote provider; the
   provider opens **one SSE stream per page** with the union of visible
   ids, fans ticks to consumer cells via React context. (Codex finding 2
   — fixes the 6-EventSource browser cap.)
3. **eToro snapshot is best-effort on subscribe.** `snapshot=True` causes
   eToro to push the latest cached `Trading.Instrument.Rate` *when one
   exists* (active book, recent close). For halted / never-traded /
   illiquid instruments **no snapshot frame is guaranteed**. The
   load-bearing fallback is the REST `current_price` already on the
   page when the row mounts; live tick overlays only when one arrives.
4. **FX is not visibility-bound.** Multiple non-SSE handlers
   (`/portfolio`, `/portfolio/copy-trading`, budget/execution paths) read
   `live_fx_rates` synchronously during request handling. The table must
   stay populated independent of any browser tab. (Codex finding 6 —
   forces C2 over C1.)
5. **Same id rendered twice on one page → one stream, two consumers.**
   Provider-level dedup. (Codex finding 3.)

## PRs (sequenced)

### PR A — Subscriber: visibility-only

Branch: `feature/<n>-visibility-only-ws`

**Code changes** (all in `app/services/etoro_websocket.py`):
- Delete `_SOURCE_RECONCILE_INTERVAL_S`.
- Delete `_source_topic_set`, `_source_reconcile_signal`,
  `_source_reconcile_task` fields.
- Delete `_refresh_source_topics` method.
- Delete `_source_reconcile_worker` method.
- Remove the post-portfolio-sync `_source_reconcile_signal.set()` hook
  inside `_reconcile_worker` (line 614).
- In `start()`: drop the initial `_refresh_source_topics()` call and the
  source-reconcile-task creation (lines 488, 496-498).
- In `stop()`: drop the source-reconcile-task cancel block (lines 519-523).
- `fetch_watched_instrument_ids` — keep the function (still used by
  external callers / tests) **but** no longer called from inside the
  subscriber.
- Subscriber boots with `_topic_refs == {}`. Connect path subscribes to
  `_topic_refs.keys()` (empty on first boot → log "no tracked instruments
   — connection will idle until page-view subscribe").
- Reconnect re-subscribes whatever refs accumulated during the outage.

**Tests** (`tests/test_etoro_websocket.py`):
- Delete the source-reconcile tests
  (`test_initial_refresh_adds_all_source_ids`,
  `test_subsequent_refresh_only_diffs`,
  `test_refresh_does_not_disturb_page_view_refs`,
  `test_provider_failure_does_not_commit_source_set`,
  `test_signal_kicks_immediate_refresh`,
  `test_cancel_during_send_does_not_double_add_on_next_refresh`,
  `test_startup_refresh_failure_signals_worker_for_immediate_retry`,
  `test_portfolio_reconcile_signals_source_worker`).
- Keep wire-ordering test
  (`test_concurrent_remove_blocks_until_add_send_completes`).
- Keep ref-counted add/remove tests
  (`TestRefCountedAddInstruments`, `TestRefCountedRemoveInstruments`).
- Keep `TestReconnectSubscribesFromTopicRefs`.

**Acceptance**:
- Boot log on a fresh process reads
  `EtoroWebSocketSubscriber: no tracked instruments — connection will
   idle until page-view subscribe` (no "subscribed to N topics" line
  until a page mounts).
- Opening an instrument page → log
  `EtoroWebSocketSubscriber: subscribe 1 topics` within ~1 s.
- Closing the page → log `EtoroWebSocketSubscriber: unsubscribe 1 topics`
  within ~1 s.
- `pytest -q` green.
- Lifespan shutdown completes without hang (verified against current
  reproducer: boot, observe "subscribed", trigger watchfiles reload,
  process exits cleanly within 5 s — no manual kill needed).

### PR B — Frontend: page-level live-quote provider + cells

Branch: `feature/<n>-live-quotes-everywhere`

**The shape that matters** (Codex finding 2 + 3):

We move from "one EventSource per hook call" to "one EventSource per page,
fanning out to consumer cells via React context". Same id rendered N
times on a page consumes N times from the same stream — one Subscribe
on the wire, one tick delivered to every consumer.

**New files**:
- `frontend/src/components/quotes/LiveQuoteProvider.tsx`:
  - Context provider. Accepts an `instrumentIds: number[]` prop (parent
    page collects all visible ids and passes them).
  - Opens one EventSource at `/api/sse/quotes?ids=<csv>`.
  - Maintains a `Map<instrumentId, LiveTickPayload>` updated on each
    received tick.
  - Exposes a `useLiveTick(id)` consumer hook that returns the latest
    tick (or null) for the given id and re-renders only when that id's
    value changes.
  - On `instrumentIds` prop change: compare the **canonical set
    representation** (dedup + numeric sort + join) of the new ids
    against the currently-open stream's canonical set; reopen the
    EventSource only when the canonical strings differ. Raw array
    identity / order changes from harmless re-renders or row reorders
    must NOT churn the stream (Codex round 3). Debounce reopen by
    ~300 ms on top of that so a rapidly-rendering page batches genuine
    set changes too.
  - Cleanup on unmount: close the stream → backend
    `remove_instruments(all)`.
- `frontend/src/components/quotes/LivePriceCell.tsx`:
  - Reads `useLiveTick(instrumentId)` from context, falls back to a
    `fallback` prop (the REST snapshot) until the first tick lands.
  - Currency-aware via the existing `liveTickDisplayPrice` helper.
- Tests:
  - `LiveQuoteProvider.test.tsx`: opens one EventSource for N cells
    sharing two distinct ids; same-id duplicate cells get the same tick;
    unmount closes the stream.
  - `LivePriceCell.test.tsx`: renders fallback when no tick; switches
    to live tick on arrival.

**Modify**:
- `frontend/src/components/dashboard/PositionsTable.tsx`: wrap the table
  with `LiveQuoteProvider` collecting `positions.map(p => p.instrument_id)`;
  swap the static cell at line ~101 for `<LivePriceCell instrumentId={…}
  fallback={p.current_price} currency={currency} />`.
- `frontend/src/pages/PortfolioPage.tsx`: same — provider at page level.
- `frontend/src/pages/CopyTradingPage.tsx`: provider at page level
  collecting every `MirrorPositionItem.instrument_id` across all
  mirrors. **De-dup invariant**: if a parent-CID summary cell and a
  per-position child cell render the same `instrument_id`, both share
  the same context tick — no duplicate stream, no stale parent.
- `frontend/src/components/instrument/SummaryStrip.tsx`: replace the
  current standalone `useLiveQuote` with the context consumer
  (`useLiveTick`). The InstrumentPage wraps the strip + sub-tabs in a
  `LiveQuoteProvider` for that single id.

**Out of scope (locked)**:
- Delta arrows / colour-on-change UI per row. Future polish.
- Per-row spread / bid-ask split. Mid (last) only.
- Sparkline overlays.
- Watchlist price column (not present today; out of scope).

**Acceptance**:
- Open `/portfolio` → DevTools network panel shows ONE EventSource
  carrying every held instrument id; every row updates within ~1 s.
- Open `/copy-trading` → ONE EventSource for the whole page carrying
  the union of every visible mirror's `instrument_id`s. Same-id-twice
  (parent summary + child row) consumes from the single stream.
- Open `/instrument/AAPL` → ONE EventSource for that single id.
- A page that renders the same id twice (e.g. mirror summary + position
  row) shows the same updated price in both cells without opening two
  streams.
- `pnpm --dir frontend test:unit` green; new component tests pass.

### PR C — FX: daily cron + bootstrap on empty table

Branch: `feature/<n>-fx-daily-bootstrap`

**Decision**: **C2 over C1.** Codex round 2 demonstrated C1 (fully lazy)
isn't safe — `live_fx_rates` is read by `app/api/portfolio.py`,
`app/api/copy_trading.py`, `app/services/budget.py` etc. **before any
SSE stream opens**, and `budget.py:520` silently degrades `tax_usd = 0`
on missing GBP→USD. Lazy-on-SSE-open does not cover those paths.

C2 keeps the cron, cuts cadence, and adds a bootstrap rule for fresh DBs.

**Code changes**:
- `app/workers/scheduler.py:225+`: change `fx_rates_refresh` cron
  cadence from hourly to once daily at 17:00 CET (post ECB publish
  window).
- `app/workers/scheduler.py:2611+`: drop **Phase 2** (eToro batch
  quotes). The WS live feed already populates `quotes` for any
  instrument the operator views; the batch path is redundant for the
  visibility-driven model. Keep Phase 1 (Frankfurter conditional GET).
- `app/main.py` lifespan, **after migrations and BEFORE
  `start_runtime()` at line 147** (Codex round 2 finding 3 — pinning
  ordering so scheduler catch-up cannot race the inline bootstrap
  fetch/write; both would try to write the same `live_fx_rates` rows
  otherwise):
  - Bootstrap rule: query `live_fx_rates`. If empty, fire one inline
    `fetch_latest_rates_conditional` synchronously (~500 ms) and upsert
    so the first request after a fresh DB has rates available.
  - Failure (network down at boot, Frankfurter outage): log loud,
    continue boot. **Safety implication below.**
- `app/services/budget.py:520` (Codex round 2 finding 2): the silent
  `tax_usd = 0` degrade-on-missing-rate is an execution-safety hole,
  not just a display bug. Replace the silent fallback with a
  fail-closed raise that the execution guard converts into a clear
  block on the BUY/ADD path. **In scope for PR C** — moved out of
  follow-up. Without this fix, a Frankfurter-down boot with an empty
  `live_fx_rates` table would let an order through with `tax_usd = 0`.
- Other FX **readers** are unchanged. They continue to read
  `live_fx_rates` and behave as before. The only behavioural change is
  the staleness window: rates are at most 24 h old (vs 1 h previously),
  matching Frankfurter's actual publish cadence.

**Audit (must complete BEFORE this PR merges)**:
- `grep -r "load_live_fx_rates\|live_fx_rates" app/` and confirm every
  call site is OK with rates up to 24 h old. Already-known sites:
  - `app/api/sse_quotes.py:_load_display_context` — at-stream-open
    snapshot, fine.
  - `app/api/portfolio.py:327, 692, 848` — all per-request, fine.
  - `app/api/copy_trading.py:215, 337` — same.
  - `app/services/budget.py:520` — silent degrade-to-zero on missing
    pair is an execution-safety hole (Codex round 2 finding 2). **Fixed
    in this PR**: replace the silent fallback with a fail-closed raise
    that the execution guard surfaces as a BUY/ADD block. Bootstrap
    rule populates the table on fresh DBs, but cannot guarantee
    network-down boot — fail-closed handles that case.
  - `app/services/portfolio.py`, `app/services/execution_guard.py` —
    fully audit against the same tolerance.
- If any site requires <24 h freshness (e.g. an intraday execution
  path), surface it in the PR description and either move that path
  back to its own targeted refresh or hold C2.

**Acceptance**:
- `fx_rates_refresh` runs once daily, not hourly. Verify in
  `/system/status` job_runs row count.
- Phase 2 eToro batch path fully removed; no `eToro` mentions remain in
  `fx_rates_refresh`.
- Fresh DB boots, hits `/portfolio` immediately → no `FxRateNotFound`,
  no missing display blocks. Bootstrap rule populated rates inline.
- Audit list above documented in PR description with per-site
  tolerance verdict.

## Resolved findings (Codex round 1)

| # | Finding | Resolution in v2 |
|---|---------|------------------|
| 1 | snapshot=True "guarantees" tick contradicts halted-instrument fallback | Invariant 3 weakened: snapshot is best-effort; REST fallback is load-bearing. |
| 2 | useLiveQuote opens one EventSource per cell — hits ~6/origin cap | PR B redesigned around `LiveQuoteProvider` (one stream per page). |
| 3 | Same-id-twice-on-page de-dup unspecified | Invariant 5 + PR B copy-trader bullet pin de-dup. |
| 4 | Goal mentions watchlist; no price surface exists there | "Watchlist intentionally out of scope" added to Goal + Non-goals. |
| 5 | C1 freshness rule used `quoted_at` = ECB date, not fetch time → spurious refetch on weekends | C1 dropped. C2 cadence is calendar-driven, not staleness-driven. |
| 6 | C1 SSE-only scope leaves `/portfolio`, `/copy-trading`, budget readers stale | C1 dropped. Invariant 4 + Audit list pin the multi-reader requirement. |
| 7 | Even C2 needs bootstrap for empty/fresh DB | PR C bootstrap rule on lifespan, before yield. |

## Migration / rollback

- PR A: pure refactor + delete. Rollback = revert the squash commit.
- PR B: pure additive on the frontend. Rollback = revert the row-component
  swap + delete the new provider/cell components.
- PR C: rollback = restore hourly cron + Phase 2 + remove bootstrap.
  Operator can re-enable hourly trivially.

## Risks and mitigations

1. **Operator opens N tabs simultaneously** (PR A + B): backend
   ref-counting (already wire-ordering-correct after #490) handles N
   concurrent SSE streams cleanly. Frontend per-page provider keeps
   browser SSE-per-origin cost bounded to one stream per tab.
2. **eToro snapshot frame absent for halted / illiquid instruments**
   (Invariant 3): UI falls back to REST `current_price`. Verified
   load-bearing in spec.
3. **Frankfurter outage at boot** (PR C bootstrap): logged, boot
   continues, daily cron retries. Operator-visible degraded display
   currency until rates populate. Execution safety **not** at risk
   because PR C also makes `budget.py` fail-closed on missing rate,
   so a BUY/ADD path under empty `live_fx_rates` blocks rather than
   silent-zeroing tax.
4. **A reader of `live_fx_rates` requires <24 h freshness post-PR-C**
   (audit gap): caught by the audit list above. Any such site is the
   PR's own scope to fix or hold.
5. **PR B same-id dedup regression** (Invariant 5): pinned by component
   tests in `LiveQuoteProvider.test.tsx`.

## Success criteria (overall)

- Boot is silent on subscriptions until a page renders.
- Every page that displays a price shows a live-updating one within ~1 s
  of mount.
- Closing a page tears down its subscriptions within ~1 s.
- Lifespan shutdown completes cleanly without hang.
- One EventSource per page; same-id-twice consumes from the same stream.
- FX scheduled job runs daily, not hourly. Fresh-DB boot has rates
  available without operator intervention.
