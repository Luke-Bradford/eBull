# Lane-G trade-history floor — #2946 step 3 item 1

Closes the one named entry in `etoro_quota_lanes.ACCEPTED_FLOOR_EXCEPTIONS`.

## Source rule

Two portal readings, no stated precedence, so the lane map carries both and we throttle
against the lower:

- per-endpoint page (`list-trading-history`) → lane `G_default_shared`, 60/min;
- general rate-limit page → the 20/min tier, whose prose explicitly names *"detailed
  user trade history queries"*.

`CallSite.conservative_per_minute` = `min(60, 20)` = **20/min** for
`get_trade_history`. Under the rolling-window counting rule a caller spaced at `i`
places `floor(60/i) + 1` stamps, not `60/i`, so compliance needs
`floor(60/i) <= 19` — satisfied by every `i` strictly above `60/20 = 3.0s`. That bound
is OPEN, so `min_interval_for_stamps` returns the smallest value expressible from the
budget that satisfies it, `60/19 = 3.158s`. It is sufficient, not the mathematical
minimum. The client this site rides today, `_http_read`, is configured at **1.1s** — 2.9×
over the conservative reading, shipped as an accepted exception pending step 2.

⚠ Inclusive boundary counting (a stamp exactly `60s` old still counts) is a conservative
assumption about how eToro's window works, not something the portal states. Keep it.

## Premise, measured (not recalled)

| claim | measurement |
| --- | --- |
| pagination threshold | `if len(rows) < page_size: return` — so **exactly 200** rows already costs a second request to discover exhaustion, and `page_size` is a caller-settable default (200), not a constant. |
| watermark | `compute_history_min_date` returns `MAX(executed_at) - 7 days` (`_WATERMARK_OVERLAP`) over `trade_events` where `event_kind='close' AND source='etoro_history'`, or `HISTORY_EPOCH` (2017-01-01) when there is **no qualifying history close** — other ledger rows do not count. Dev holds 1 qualifying row, so `min_date` is a real watermark and the loop exits on the first short page. |
| observed rate | instrumented build (`c383b460`) live on dev. `PYTHONPATH=. uv run python -m scripts.measure_2946_quota_load --request-log ~/Dev/eBull/var/autonomy-logs/launchd.jobs-daemon.err.log` over the window `2026-09-13 20:10:00..20:10:02Z`: one `lane=G_default_shared src=broker_read` attempt, `max/60s = 1`, 0 malformed. ⚠ That is a two-second window on one process — it corroborates the one-page reading, it does not establish a workload. |
| the ledger's own expectation | §5 of `docs/proposals/etl/2026-06-13-etoro-trade-ledger.md`: *"1 GET per sync tick (+pagination pages, **expected 1 for years yet**)"*. The one-page steady state is the documented design, not an inference from this dev row count. |
| when the worst case fires | `>= 200` closes inside the 7-day overlap window, or no qualifying history close at all → `HISTORY_EPOCH` deep backfill. A fetch or transaction failure leaves the watermark unmoved, so a *persistent* failure repeats the same multi-page window every tick; a single failed tick does not. |
| cadence | **not** 5 minutes minimum. Three triggers: the scheduled sync, `enqueue_post_trade_sync` (immediate, on a fill), and the WS reconcile (`etoro_websocket.py:1005`, event-driven, API process, its own provider). |

So this is prospective hardening. The fix must cost ~nothing in the one-page steady
state, or it buys a worst case by taxing the normal case.

## Change

A third `ResilientClient` on the broker, `_http_history`, carrying only
`get_trade_history`:

- **floor DERIVED from the lane map**, not chosen next to it — same construction as
  `CORE_ELIGIBILITY_REQUEST_INTERVAL_S` (`strategy_core_eligibility.py:70-111`), with an
  explicit `RuntimeError` at import if the entry is missing. ⚠ Unlike lane B's lookup,
  this one matches on `(module, method)` and requires **exactly one** hit: a
  method-name-only `next(...)` silently takes the first of a duplicate pair, and
  `place_order` already proves one method name can carry two `CallSite` rows;
- `conservative_per_minute - 1` of headroom → `60/18 = 3.333s`. ⚠ This is a **policy
  margin, not a derivation**: lane G's membership is UNENUMERATED and two of its known
  members draw on it outside this client (`edit_demo_strategy_position` on `_http_write`,
  and the unthrottled `/api/v1/me` in `KNOWN_UNTHROTTLED`), so the one caller that can
  burst should not spend the last documented request. Nothing measures that the margin is
  *enough*; it matches lane B so the two reserve alike;
- **joins the existing shared clock and lock** (`shared_ts`, `shared_throttle_lock`).
  That coupling is inherited, not required by the lane arithmetic — history is lane G and
  the portfolio read is lane E, so they share no documented budget. It is kept because
  breaking it would give the history paginator a second independent clock against the
  same user key, which is the failure this whole item is about. ⚠ The lock protects
  **stamp allocation** only and is released before `send()`, so two requests can be in
  flight at once; what is bounded is the spacing of dispatches, which is what a quota
  counts;
- its own `src=broker_history` on the request artefact, so the tabulator separates lane-G
  history traffic from the rest of `broker_read` without re-deriving it from the path.

Then delete the `ACCEPTED_FLOOR_EXCEPTIONS` entry. Three tables move together or the
floor guard does not activate: `CALL_SITES[get_trade_history].client_attr` →
`_http_history`, a new `FLOOR_CONSTANT_NAMES[(_BROKER, "_http_history")]` entry (asserted
against `CALL_SITES` by `_sites_by_client`), and `EXPRESSION_COUNTS` (`_http_read` 6 → 5,
`_http_history` 1).

### Cost, stated

Steady state (one page): the history call waits 3.333s instead of 1.1s since the previous
request on that provider. +2.2s on a job that runs every 5 minutes. Backfill (N pages):
`3.333(N-1)`s of pacing instead of `1.1(N-1)`s — response latency, parsing, retries and
lock contention are on top and are not modelled here.

⚠ **Both callers fetch portfolio, then all history, then persist.** Slower pagination
therefore delays the whole reconciliation, not just the history rows — worst on the
post-fill `enqueue_post_trade_sync` path. At one page that is +2.2s. At 50 pages it is
163s instead of 54s, and a 50-page fetch is already slow enough to be a problem before
this change; it is not one the floor creates or fixes.

⚠ `_throttle_and_stamp` sleeps INSIDE the shared lock, so each wait is head-of-line
blocking for a concurrent request on the **same provider instance** — ~2.2s per page per
waiter, and with several waiters the delays add rather than overlap. Accepted on the same
ground the write floor states at `etoro_broker.py:218-225`: it is over-restriction, which
is the safe direction. ⚠ Not accepted on "a 429 costs more" — the eToro skill records
that **no `Retry-After` is guaranteed** (`.claude/skills/data-sources/etoro-api.md`), so
the cost of a 429 here is unmeasured. Providers are constructed per call site, so the
blocking does not cross instances.

## What this does NOT do

- **Does not bound the pagination loop.** `while True` terminates on a short page. A cap
  would need a third posture (return truncated data — silently violating ledger §4's
  *"the transform groups the fetched batch by positionId and emits ONE open with
  `units = Σ slice units`"*, whose correctness argument is explicitly that **all** of a
  never-seen position's slices are in the batch; or raise, which `fetch_trade_history_
  safely`'s error-posture table does not cover; or handled exhaustion that skips history
  and lets positions sync, which is buildable but is a ledger-completeness decision, not
  a quota one). Out of scope for a floor change. The pacing bounds the *rate*, which is
  the quota question.
- **Does not change the retry posture.** `ResilientClient` still retries 429/5xx up to 3×
  before the exception reaches `fetch_trade_history_safely`; ledger §7 describes what
  happens *after* that — skip this tick, watermark unmoved. Pre-existing, unchanged, and
  worth knowing when reading either document.
- **Does not address offset-pagination stability.** Slower paging widens the window in
  which rows are inserted between pages. Duplicate slices are deduped by the unique
  indexes; a *skipped* slice is not detectable here. Pre-existing property of an
  offset-paginated endpoint with no stable-snapshot guarantee; noted, not fixed.
- **Does not close step 2 clause 2.** Two callers holding **independent provider
  instances** still exceed the budget — and they need not be in separate processes, since
  every `EtoroBrokerProvider.__init__` starts a fresh `shared_ts`. Unchanged.
- **Does not isolate lane G.** `edit_demo_strategy_position` still rides `_http_write` on
  lane G and `/api/v1/me` is still an unthrottled lane-G bypass. This paces the one caller
  that can burst and claims nothing about the lane total.
- **Does not re-census the portal.** `VERIFIED_ON` and every documented number unchanged;
  item 4 (pin the census against `/api-reference/openapi.json` with a content hash) stays
  open.

## Tests

- `EXPRESSION_COUNTS` moves; the AST drift test fails until both entries are recorded.
  `_sites_by_client` fails until `FLOOR_CONSTANT_NAMES` gains the client.
- `test_accepted_floor_exceptions_are_real_exceptions_with_a_reason` is what **forces the
  deletion** — it fails while an entry names a site whose floor now complies. ⚠ After the
  deletion it no longer covers this site at all, and
  `test_configured_floor_is_at_least_the_conservative_minimum` becomes **tautological**
  here, because the floor is derived from the same table the test compares it against. So
  the derivation needs its own property test; that is the next bullet, and it is the only
  live guard on this number.
- new: `floor(window_s / _ETORO_HISTORY_INTERVAL_S) + 1 <= conservative_per_minute - 1`,
  with `window_s` read from the lane (not a hardcoded 60) and the strict `- 1` pinning the
  headroom as a property rather than restating the expression. Plus an assertion that the
  derivation selected the `get_trade_history` site and that the site is on lane G — a
  wrong lane with a slower budget would otherwise pass the inequality.
- new: runtime wiring — `_http_history` is a distinct client, carries
  `_ETORO_HISTORY_INTERVAL_S`, and **shares the same `shared_last_request` list and lock
  object** as `_http_read` / `_http_write`. Constants can be right while the client is
  wired to its own clock.
- new: `get_trade_history` issues **every** page on `_http_history` — a method that
  switched clients mid-loop would pass a single-page test. Covered: empty page,
  exactly-`page_size` terminal page (two requests), multiple full pages, failure on a
  later page.
- new: instrumentation attributes `src=broker_history` on every attempt, including a
  retry and a transport failure.
