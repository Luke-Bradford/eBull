# eToro per-endpoint quota-family census — the lane map (#2946 items 1 + 3)

Status: census complete, **lane map only — no coordinator, no floor change**. Refs #2946,
#2277 (portal drift), #2602 (depends on timely reconciliation), #1484 (rate gates).

This is step 1 of #2946's own bounded spike, and the step its item-2 close-out named as the
next unit of work:

> **Recommendation: the next unit of work on this ticket is a per-endpoint quota-family
> census under the skill's verification protocol, producing a lane map, not a coordinator.**
> Until the lanes match the documented families, any shared coordinator either
> over-restricts endpoints with independent quotas or under-restricts a family whose
> members it cannot see.

**This draft is the post-Codex-ckpt-1 version.** The first draft claimed precedence had been
"established", asserted environment separation universally, and concluded that
reconciliation sits in a pool of its own. Codex refuted all three. What survives is smaller
and more useful; the corrections are listed at the end so the next reader can see which
claims were killed and why.

## Verification provenance — and its limits

Every number below was fetched from the live portal on **2026-09-13** via WebFetch, per
`.claude/skills/data-sources/etoro-api.md` §Verification protocol. `curl` is
Cloudflare-blocked; the `.md` form served every page listed.

⚠ **Read this before citing anything here.** WebFetch returns a *model-rendered reading* of
the page, not the raw response body. Rate-limit sentences were requested verbatim and are
reproduced as returned, but **no raw bytes and no hash were captured**, so this is a dated
transcript, not a pinned artefact. A second reviewer fetching the same slugs during ckpt-1
found some `.md` forms failing where the HTML form worked — so even retrieval is not
perfectly reproducible. Treat every figure as "documented on 2026-09-13 as read", and
re-fetch before relying on one for an irreversible decision.

⚠ The api-reference index did not surface an OpenAPI version string on the pages fetched, so
this census is **dated, not version-pinned**. `/api-reference/openapi.json` is listed in
`llms.txt` but was **not fetched**; pulling and hashing it is the cheapest way to pin the
next census properly, and is the single biggest improvement available to this method.

Pages read (all under `https://api-portal.etoro.com`):

| page | gave |
| --- | --- |
| `/llms.txt` | slug index |
| `/core/getting-started/rate-limits` and `…/rate-limits.md` | the general statement |
| `/api-reference/trading--demo/submit-an-order-for-asynchronous-processing.md` | lane A |
| `/api-reference/trading--demo/close-demo-position-by-units.md` | lane A (2nd witness) |
| `/api-reference/trading--demo/modify-stop-loss-and-take-profit-settings-on-an-open-position.md` | lane G |
| `/api-reference/trading--demo/check-instrument-trading-eligibility.md` | lane B |
| `/api-reference/trading--demo/get-what-if-trading-cost-breakdown.md` | lane C |
| `/api-reference/trading--demo/get-order-information-and-position-details.md` | lane D |
| `/api-reference/trading--demo/get-close-order-information-and-closed-position-details.md` | lane D (2nd witness) |
| `/api-reference/trading--demo/get-account-pnl-and-portfolio-details.md` | lane E — **single witness** |
| `/api-reference/trading--demo/list-trading-history.md` | lane G |
| `/api-reference/trading--demo/get-aggregated-portfolio-snapshot.md` | a lane we do **not** call |
| `/api-reference/market-data/retrieve-bidask-rates-for-one-or-more-instruments.md` | lane F |
| `/api-reference/market-data/get-instrument-candle-history.md` | lane F (2nd witness) |
| `/api-reference/identity/get-authenticated-user-profile.md` | lane G (`/api/v1/me`) — single witness |
| `/api-reference/trading--real/submit-an-order-for-asynchronous-processing.md` | real lane A |
| `/api-reference/trading--real/get-order-information-and-position-details.md` | real lane D |
| `/api-reference/trading--real/check-instrument-trading-eligibility.md` | real lane B path |
| `/api-reference/trading--real/get-what-if-trading-cost-breakdown.md` | real lane C path |
| `/api-reference/trading--real/get-account-pnl-and-portfolio-details.md` | real lane E |

⚠ **"Two witnesses" is weaker than it sounds, twice over.** Each page's list omits the page's
own endpoint, so agreement means agreement on `{self} ∪ peers`, not on the literal lists.
And both pages are generated from one upstream annotation, so a second page corroborates
*transcription*, not the underlying fact. Lanes **B, C, E and G are single-witness**.

## The precedence question — NOT established; a policy instead

#2946 asked for precedence between the general page and the per-endpoint pages to be
"established". **It cannot be, from the documentation.** The first draft argued the general
page is a "lossy summary" and therefore loses. That is circular — it assumes the conclusion
by calling the more convenient source authoritative. Codex was right to kill it.

What the general page actually says, with its qualifiers restored (they were paraphrased
away in the first draft, which exaggerated its rigidity):

- *"Most standard `GET` requests for retrieving data allow up to **60 requests per
  minute**"* — the tier explicitly names *"Retrieving instrument rates, historical candles,
  search, and exchanges"*.
- *"Endpoints that execute trades, modify state, or run heavier queries are limited to **20
  requests per minute**"* — the tier explicitly names *"detailed user trade history
  queries"*.
- *"Limits are tracked per user key and are measured over a 1-minute rolling window."*
- **No statement of precedence** over per-endpoint documentation.

The per-endpoint pages carry structure the general page does not express — a 120/min
market-data pool, dedicated per-endpoint budgets, named pools with enumerated membership.
That makes them *more specific*, which supports a working hypothesis, not an authority
ranking. Enforcement is unobserved either way: documentation proves a number is
**documented**, never that it is **enforced**.

So the output of this section is an **operating policy**, not a finding:

> Record **both** readings per lane. Use the per-endpoint page for **lane identity and
> membership** (the general page cannot express either). For the **number we throttle
> against**, use the lower of the two readings. Never spend the difference when the
> per-endpoint number is higher.

Two things this policy does **not** do, and the first draft wrongly implied it did:

- it does not resolve **membership ambiguity** — when the general page assigns a tier by
  prose category ("heavier queries"), taking a smaller number does not tell you whether our
  endpoint is in that category. That needs separate treatment, and it bites exactly once
  below (lane G history);
- it does not protect against an **overlapping or global** cap that neither page mentions.
  Independent families are an assumption, not an observation.

### The consequence the first draft got backwards: corpus arithmetic

The first draft said the skill's market-data corpus figure was at risk. Re-reading the
skill, it is not — `.claude/skills/data-sources/etoro-api.md:265` already calls ~56 minutes
a *"theoretical floor"*, already says it *"competes with every other market-data call"*, and
already concludes a cross-sectional study must be a scheduled harvest. It never promised a
56-minute completion budget, and criticising it for one was misreading it.

The genuinely useful correction is different and larger: **neither quota reading is the
binding constraint today — our own floor is.** At `_ETORO_READ_INTERVAL_S = 1.1`, a
6,700-instrument pass costs `6700 × 1.1 s ≈ **123 minutes**` before any contention, retry or
response latency — more than the 112 minutes the pessimistic 60/min reading implies and over
twice the 56-minute optimistic floor. A harvest planner that reasons about the venue's quota
at all is reasoning about the wrong number until the client floor moves.

⚠ And a run paced at 1.1 s **cannot distinguish 60/min from 120/min** — it never approaches
either. "Measure it on a real run" is not a protocol; a protocol needs a deliberate paced
probe, which is #2946 step 2's job and is not attempted here.

## Environment separation — established for TWO lanes, not universally

The first draft claimed demo and real are separate pools, full stop. That is over-read. What
the enumerations actually support:

- **Lane A: separated.** The demo pool's ten peers are all `/demo/` paths; the real pool's
  ten peers contain no `/demo/` path. Both directions witnessed.
- **Lane D: separated.** Demo page lists only `/demo/` peers; real page lists only `/real/`
  peers. ⚠ But note the real page's *own path* is `/api/v2/trading/info/orders:lookup` with
  no `/real/` segment — so "the real family is all `/real/` paths" is false even for D.
- **Lane F is environment-neutral** — market data has no env segment at all.
- **Lane G spans both environments** plus `/api/v1/me` plus an open set outside trading.

So: **do not partition lane state by `env` blindly.** Doing so would split lane F's single
budget into two phantom budgets and would mis-model G. The coordination key is
`(user key, quota family)`, with an environment component **only on the families where
separation is witnessed** — today A and D. Family alone merges users; environment alone both
merges users and splits shared budgets.

## The lane map

Our code reaches **seven** documented quota families through **three** throttle clocks (two
in the broker provider, one in the market-data provider) plus **four** entirely unthrottled
raw-`httpx` call sites.

`min interval` is `window ÷ limit` — the fastest sustained cadence for a caller owning the
whole pool. Both readings are carried, per the policy above.

| lane | documented family | per-endpoint | general-page tier | conservative min interval | our client | our floor | safe under conservative? |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **A** | order-write pool, **11 members** (incl. itself), shared | 20/60 s | 20/60 s (executes trades) | 3.00 s | `_http_write` | 3.5 s | ✅ |
| **B** | eligibility — **dedicated**, pooled with nothing | 20/60 s | 20/60 s (heavier query) | 3.00 s | `_http_write` | 3.5 s | ✅ |
| **C** | what-if costs — **dedicated**, pooled with nothing | 20/60 s | 20/60 s (heavier query) | 3.00 s | `_http_write` | 3.5 s | ✅ |
| **D** | order-info pool, 3 members, shared | 60/60 s | 60/60 s (GET) | 1.00 s | `_http_read` | 1.1 s | ✅ |
| **E** | account-read pool, 3 members, shared | 60/60 s | 60/60 s (GET) | 1.00 s | `_http_read` | 1.1 s | ✅ |
| **F** | market-data pool, 11 members, shared | 120/60 s | **60/60 s** (names rates/candles/search/exchanges) | **1.00 s** | `etoro.py::_http` | 1.1 s | ✅ |
| **G** | **default shared quota** — membership unbounded | 60/60 s | **ambiguous, see below** | **3.00 s if Tier 2** | mixed | 3.5 s / 1.1 s / none | ⚠ **one site: NO** |

Lane membership we call, per lane:

- **A** — `POST /api/v2/trading/execution/demo/orders` (strategy + core submit),
  `POST {exec_prefix}/market-open-orders/by-amount` **and** `/by-units` (both reachable from
  `place_order`), `POST {exec_prefix}/market-close-orders/positions/{positionId}` (reached
  from both `close_position` and `close_demo_strategy_position`).
- **B** — `POST /api/v2/trading/info/demo/eligibility`.
- **C** — `POST /api/v2/trading/info/demo/costs`.
- **D** — `GET /api/v2/trading/info/demo/orders:lookup`,
  `GET /api/v1/trading/info/demo/close-orders/{orderId}`,
  `GET /api/v1/trading/info/demo/orders/{orderId}`.
- **E** — `GET /api/v1/trading/info/demo/pnl`, `GET /api/v1/trading/info/demo/portfolio`.
- **F** — seven normalised templates in `etoro.py` (eight call expressions: the daily and
  intraday candle helpers share one template), plus two unthrottled debug probes.
- **G** — `PATCH /api/v2/trading/demo/positions/{positionId}`,
  `GET /api/v1/trading/info/trade/demo/history`, `GET /api/v1/me`.

We call `GET /api/v1/trading/info/demo/portfolio` but **not** the separate
`aggregate-portfolio` endpoint, which has its own 60/min pool shared with its POST twin.
Listed to record that it was checked and excluded, not missed.

### ⚠ The one call site that is NOT safe under the conservative policy

`GET /api/v1/trading/info/trade/{env}/history`. Its own per-endpoint page says **60/min,
default shared quota**. The general page's 20/min tier explicitly names *"detailed user
trade history queries"*. If that category covers this endpoint, the conservative minimum is
**3.00 s** and our read floor of **1.1 s is roughly 3× too fast**.

This is not a numeric disagreement the policy resolves — it is the **membership ambiguity**
the policy explicitly cannot settle. It matters more than the other lanes because
`get_trade_history` **paginates in a `while True` loop** (`etoro_broker.py:1004`), so one
logical call issues back-to-back requests at the read floor: `60 ÷ 1.1 = 54.5` requests/min
against a possibly-20/min budget.

**Not changed here, and recorded rather than glossed.** Raising a read floor is a
behavioural change to the path `trade_events.py:478` uses for trade-event sync, and the
right fix (bound the *pagination loop*, or lane-split and give history its own floor) is
step 2's call once request rates are measured. It ships as a named
`ACCEPTED_FLOOR_EXCEPTIONS` entry so the floor test stays red-by-default for anything new —
an explicit exception, not a silent pass.

## What the map changes

### 1. Three independent 60/min pools sit behind one clock — but this does NOT isolate reconciliation

Lanes D, E and G-history are three separate budgets. One `_http_read` clock at 1.1 s caps
their **sum**, so the ratio of theoretical capacities is `54.5 ÷ 180 ≈ 30%`.

⚠ **Four caveats, all of which the first draft omitted, and together they change the
conclusion.**

1. **It is a ratio of theoretical capacities, not measured utilisation** and not "headroom
   used". Nothing here observes a single real request.
2. **The read clock is shared with writes.** `shared_ts` is one timestamp across
   `_http_read` and `_http_write` (`etoro_broker.py:225-239`), so reads reach 54.5/min only
   with no competing write; the real constraint is closer to `1.1R + 3.5W ≤ 60`.
3. **Capacity is not interchangeable.** 180/min requires demand in all three pools; spare E
   budget cannot accelerate a D-only workload. And three clocks each still at 1.1 s would
   give `163.6/min`, not 180 — the remaining difference needs *floor* reductions, which the
   conservative policy forbids without measurement.
4. **The clock is per provider instance, not global.** `etoro_broker.py:225` allocates a
   fresh `shared_ts` per construction, so N instances or processes on one user key get N
   independent clocks. There is no system-wide 54.5/min cap to compare against 180 in the
   first place.

**And the conclusion the first draft drew is refuted outright.** It claimed reconciliation
lives in lane D alone, so readers cannot starve it. Reading the callers says otherwise —
reconciliation and position management consume lane **E** as well:

| caller | method | lane |
| --- | --- | --- |
| `strategy_order_reconciliation.py:492,494` | `lookup_order` | D |
| `strategy_position_manager.py:398` (`_exact_broker_position`) | `get_portfolio` | **E** |
| `strategy_core_executor.py:471`, `strategy_paper_executor.py:1207` | `get_account_risk_snapshot` | **E** |
| `scheduler.py:4412,4419`, `etoro_websocket.py:1004` | `get_portfolio` / risk snapshot | **E** |

So a lane split would **not** isolate reconciliation from portfolio reads, because
reconciliation *is* a portfolio reader. Lane separation buys headroom; it does not buy
isolation. Anything aimed at "do not starve reconciliation" has to be about **priority or
deadlines**, not partitioning alone — and average throughput never proves latency anyway, so
step 2 must measure reconciliation *latency*, not just request counts.

### 2. Two dedicated budgets are queued behind a shared clock

Lanes B and C are pooled with nothing — the portal says so in as many words: *"a
**dedicated** limit for this endpoint — it is **not shared** with (pooled across) any other
endpoint, so the full rate is available to this endpoint alone."*

They ride `_http_write`, whose clock is shared with lane A. So an eligibility census or a
what-if sweep **delays order submission**, and an order submission delays them, when the
venue gave each of the three an independent budget. Precisely: this is **latency coupling on
our side**, not budget consumption at the venue — a what-if call does not spend lane A's
20/min.

This confirms the blocker the item-2 close-out recorded against a naive per-credential pool,
and gives the shape of any fix: the unit of coordination is the **documented family**, not
the credential and not the HTTP verb.

### 3. Lane G cannot be fully bounded — but that is a matter of degree, not a theorem

The default quota is *"shared with every other endpoint that has no dedicated limit"* and
the portal does not enumerate it. Three of our calls sit in it alongside an open-ended set
outside trading (watchlists, notifications, social reads, identity).

The first draft called G uniquely uncoordinatable. That is a non sequitur: unknown external
consumers prevent a *total-quota guarantee* for **every** lane — nothing stops another
same-key client consuming D either. G's difference is only that its membership is
**unenumerated**, so the size of the blind spot is unknown rather than merely unmeasured. A
coordinator can still coordinate the G traffic it can see; it must simply not claim to bound
the family.

Two secondary notes:

- the TP/SL `PATCH` rides `_http_write` at 3.5 s. Under the per-endpoint reading (60/min)
  that is 3.5× over-restricted; under the conservative reading it is a state-modifying
  endpoint at 20/min, so it is **1.17×** over — nearly right. The first draft quoted the
  3.5× figure while advocating the conservative policy, which was internally inconsistent;
- "harmless over-restriction" was too glib. A delayed TP/SL edit is a delayed protective
  stop. Over-restriction is the safe direction *for quota compliance* and is not free
  operationally.

### 4. FOUR unthrottled raw-`httpx` call sites, in two modules and two lanes

The item-2 close-out named one. There are four:

| site | path | lane |
| --- | --- | --- |
| `broker_credentials.py:598` | `GET /api/v1/me` | G |
| `broker_credentials.py:641` | `GET /api/v1/trading/info/{env}/pnl` | **E** |
| `_debug_ws.py:91` | `GET /api/v1/market-data/instruments/{id}/…` (candles) | **F** |
| `_debug_ws.py:156` | `GET /api/v1/market-data/instruments` | **F** |

None touches `ResilientClient`, so all four are invisible to every throttle we have. The
credential pair fires on operator credential validation — and **the PnL call runs only after
identity validation succeeds**, so it is "up to two per invocation", not two. The debug pair
is dev-only but its router is enabled in development, which is where our jobs run.

That lane **E** has a bypass matters for finding 3: G is not the only lane with an
unthrottled member, so G is not uniquely uncoverable on that ground either. Recorded as
`KNOWN_UNTHROTTLED` so a fifth cannot appear silently.

⚠ Calling these "not a burst risk" would be inference, not measurement — operator-triggered
endpoints can be repeated or invoked concurrently. The honest statement is that their
frequency is **unmeasured**, and that a coordinator omitting them cannot claim lane coverage.

## Incidental finding — every REAL-env trading path we build disagrees with the portal, and the portal disagrees with itself

Found while establishing lane identity, since the lane key *is* the path. **Not fixed here.**

`etoro_broker.py:243-246` builds `env_segment = f"/{env}" if env == "demo" else ""` for v1
and `f"/api/v2/trading/info/{env}"` for v2 — so real drops the segment on v1 and *adds* it
on v2. The portal documents the opposite on both counts:

| endpoint | documented real path | we build for real | verdict |
| --- | --- | --- | --- |
| pnl | `/api/v1/trading/info/real/pnl` | `/api/v1/trading/info/pnl` | ❌ missing `/real/` |
| order info | `/api/v1/trading/info/real/orders/{orderId}` | `/api/v1/trading/info/orders/{orderId}` | ❌ missing `/real/` |
| eligibility | `/api/v2/trading/info/eligibility` | `/api/v2/trading/info/real/eligibility` | ❌ extra `/real/` |
| what-if costs | `/api/v2/trading/info/costs` | `/api/v2/trading/info/real/costs` | ❌ extra `/real/` |
| orders lookup | `/api/v2/trading/info/orders:lookup` | `/api/v2/trading/info/real/orders:lookup` | ❌ extra `/real/` |

⚠⚠ **And the portal contradicts itself inside a single page.** The real pnl page's own path
is `/api/v1/trading/info/real/pnl`, while the sharer list on that same page names
`GET /api/v1/trading/info/portfolio` — **without** `/real/`. So the documented v1 real shape
is not self-consistent, and our provider happens to match the portal on real *portfolio*
while contradicting it on real *pnl*.

⚠ A third inconsistency is internal to our own code: `broker_credentials.py:641` builds
`/api/v1/trading/info/real/pnl` — matching the portal — while the provider builds
`/api/v1/trading/info/pnl`. **Two places in this repo build the same real endpoint
differently and exactly one matches the docs.**

Our skill also records the symmetric v2 form (*"`POST /api/v2/trading/info/{demo|real}/
eligibility`"*), so the code is faithful to the skill and the **skill** has drifted.

**Not fixed, deliberately, and the reason is not caution about scope alone:** the
documentation is self-contradictory, so no path shape can be *derived* from it. Undocumented
does not mean nonexistent — aliases may exist and a mismatch is an **unknown**, not a proven
404. Settling it needs one informational call per shape on real credentials, which is a
broker-touching acceptance this loop may not run. Recorded as `KNOWN_PATH_DRIFT`, classified
`unknown` rather than `broken`, and it belongs to real-env enablement under #2843/#2844.

⚠ It is a dormant-until-live class of defect. Note the current strategy and core submission
methods explicitly reject non-demo environments, so it is not reachable today — which is
exactly why it will first be reachable at the moment real capital starts moving.

## What is deliberately NOT built

- **No coordinator, no pooled gate, no lane split.** The map is what tells a coordinator
  which lanes to have; building one before step 2 measures whether anything binds is the
  order this ticket already rejected once.
- **No floor changes**, including the lane-G history exception above.
- **No real broker request, no trading write.** Documentation reads only.
- **`PostgresFloorGate` not adopted** — unchanged from the item-2 close-out: it needs seeded
  budget rows (`sec` only today) and its DB-failure fallback is a globally shared 0.11 s
  floor. ⚠ The close-out called that *"30× below the read interval"*; that is **wrong
  arithmetic** — `1.1 ÷ 0.11 = 10×` for reads, and ~32× for the 3.5 s write interval. Either
  way it is fail-**open**. Its own ticket, its own fallback decision.

## What ships

A frozen, dated lane table in code plus drift tests — not prose that goes stale.

`app/providers/implementations/etoro_quota_lanes.py`

- `QuotaLane` — key, **both** readings (`documented_per_minute`, `general_tier_per_minute`),
  `scope` (`dedicated` | `shared` | `default`), `env_separation` (`witnessed` | `neutral` |
  `spans`), `witnesses`, enumerated members. `min_interval_s` and `conservative_min_interval_s`
  are **computed**, never written down.
- `CALL_SITES` — every `(method, path template, lane, client)` in `etoro_broker.py` and
  `etoro.py`, with `EXPRESSION_COUNTS` recorded separately because one call expression can
  serve several templates (`_submit` reaches three; `place_order` alone reaches two).
- `KNOWN_UNTHROTTLED` — the four raw-`httpx` sites.
- `KNOWN_PATH_DRIFT` — the five real-env mismatches, classified `unknown`.
- `ACCEPTED_FLOOR_EXCEPTIONS` — exactly one entry, lane-G history, with its reason.
- `VERIFIED_ON` + source URL per lane.

`tests/test_etoro_quota_lanes.py` (pure logic, no DB)

1. table invariants — positive finite limits, unique keys, every `CALL_SITES` lane known,
   every member path unique within a lane;
2. **the floor invariant** — each throttle client's configured `min_request_interval_s` is
   ≥ the largest `conservative_min_interval_s` among the lanes it serves, except entries in
   `ACCEPTED_FLOOR_EXCEPTIONS`;
3. **drift traps** — an AST pass counts `self._http_read` / `self._http_write` / `self._http`
   call expressions per module and asserts each matches `EXPRESSION_COUNTS`;
4. the same AST trap over raw `httpx` request calls in `broker_credentials.py` and
   `_debug_ws.py` against `KNOWN_UNTHROTTLED`.

⚠ **State plainly what these tests do NOT do**, because the first draft implied more:

- test 2 is a **necessary local check, not a safety proof**. It cannot see instance count,
  process count, retries, the four bypasses, or a `RateGate` overriding the interval. Two
  broker instances on one key pass it while issuing ~2× the rate. It is also **not** a
  tautology: the floors come from the provider modules' own constants and the limits from
  the lane table, so lowering a floor fails it;
- ⚠ the first draft claimed this test would catch "optimising the read floor to 0.5 s for
  lane F". It would not — lane F is on `etoro.py::_http`, a **different client** from
  `_http_read`, so F cannot break D/E/G. Asserting against the **conservative** (60/min)
  reading is what keeps F at ≥ 1.0 s;
- tests 3-4 are **counts**, so equal-size substitutions and simultaneous add/remove pass
  silently, and they cannot detect a *wrong* lane assignment. They are a re-read trigger,
  not a proof;
- ⚠ **offline tests cannot detect portal change at all.** Only a re-fetch can. The date is
  the artefact; nothing in CI refreshes it.

## ⛔ Skill delta that could NOT be applied — `.claude/**` writes are refused

Project rule is that a skill gap lands **inline, in the same PR**. That could not happen
here: editing `.claude/skills/data-sources/etoro-api.md` is refused by the same unsupported
mechanism recorded on #2403 and re-hit today. This is not an operator call and not a
permissions setting in this repo — `.claude/settings.local.json` already grants Edit/Write
on the tree.

So the delta is written down here instead, precisely enough to apply mechanically:

1. **`etoro-api.md:26-30`** (the "Rate limits have DRIFTED" bullet) — keep the numbers but
   append: *those are the PER-ENDPOINT reading only; the portal's own general page
   disagrees with three of them and states no precedence.* Point at this document and at
   `app/providers/implementations/etoro_quota_lanes.py`.
2. **Add a "Quota lanes" bullet** carrying: the two readings and the working policy (lower
   number for throttling, per-endpoint for identity/membership); *"Limits are tracked per
   user key and are measured over a 1-minute rolling window"*; the seven families; that
   eligibility and what-if costs are **pooled with nothing**; that demo/real separation is
   **witnessed for order-write and order-info only** while market data is env-neutral and
   the default quota spans both; and that no coordinator can bound the default quota
   because its membership is unpublished.
3. **`etoro-api.md:33`** — the claim *"Two trading preflight endpoints exist in BOTH demo
   and real … `POST /api/v2/trading/info/{demo|real}/eligibility`"* is **drifted**. The
   documented real path has **no env segment** (`/api/v2/trading/info/eligibility`), as do
   real `costs` and real `orders:lookup`. Mark it unresolved rather than "corrected" — see
   the path-drift section above, where the portal contradicts itself on the v1 real shape.
4. **`etoro-api.md:265`** (corpus arithmetic) — the ~56-minute figure is correct as a
   *theoretical floor under the 120/min reading* and the skill already labels it that way.
   Add that the **binding constraint today is our own client floor**: at
   `_ETORO_READ_INTERVAL_S = 1.1`, 6,700 requests cost ~123 minutes regardless of which
   quota reading holds, and a 1.1 s-paced run cannot distinguish 60/min from 120/min.

## Follow-on, in order

1. **#2946 step 2** — the real-interval census, now countable **per lane** rather than per
   verb, and measuring reconciliation **latency** as well as request rate. It does not need
   perfect classification first: capture method, path, credential, process/instance, attempts
   including retries, waits and latency, then assign lanes offline.
2. **The lane-G history floor** — decide from step 2's numbers between bounding the
   pagination loop and giving history its own floor.
3. **Lane split**, only if step 2 says headroom binds — keyed `(user key, family)` with `env`
   only on A and D, remembering it buys headroom and not reconciliation isolation.
4. **The real-env path shapes**, at real-env enablement, settled by one informational call
   per shape.
5. **Pin the next census** against `/api-reference/openapi.json` with a content hash.

## Corrections made at Codex ckpt-1

Recorded so the next reader can see which claims died rather than rediscovering them.

| first-draft claim | verdict |
| --- | --- |
| "precedence established; the general page is a lossy summary" | **circular** — replaced with an explicit operating policy |
| general page paraphrased without "most"/"generally" | **selective** — qualifiers restored |
| "the skill's corpus arithmetic is at risk" | **misread the skill** — it already says "theoretical floor"; the real point is our 1.1 s floor gives ~123 min |
| "demo and real are separate pools" (universally) | **over-read** — witnessed for A and D only; F is env-neutral, G spans |
| lane A has 10 members | **wrong** — 11 including itself |
| "reconciliation's pool is not shared with portfolio sync" | **refuted** — reconciliation calls `get_portfolio` and `get_account_risk_snapshot`, both lane E |
| "54.5/180 = 30% of headroom" | **mislabelled** — ratio of theoretical capacities; also ignores writes sharing the clock and the per-instance clock |
| "three clocks would give 180/min" | **wrong** — 163.6/min at unchanged floors |
| "every floor is safe / no under-throttling found" | **one exception** — lane-G history, under the conservative reading |
| "TP/SL is 3.5× over-restricted" | **inconsistent with the policy** — 1.17× under the conservative reading |
| "two raw bypasses" | **four** — `_debug_ws.py` adds two in lane F |
| "two calls per validation" | **up to two** — PnL runs only after identity succeeds |
| "v2-specific path drift" | **both** v1 and v2 drift, in opposite directions; the portal also self-contradicts |
| "404 on first real order" | **overstated** — undocumented ≠ nonexistent; classified `unknown` |
| "lane G uniquely cannot be coordinated" | **non sequitur** — applies in degree to every lane; E also has a bypass |
| "`PostgresFloorGate` fallback is 30× below the read interval" | **arithmetic** — 10× for reads, ~32× for writes |
| "test 2 is green today", "recorded in code" | **tense** — the module did not exist when written |
