# eToro API — live-portal freshness discipline

## When to use

Before citing, speccing, or implementing against ANY eToro API capability (endpoints, auth, rate limits, request/response shapes) — and before claiming an operation is "not supported by the public API".

## The rule

**Never cite eToro API capabilities from memory, from `docs/etoro-api-reference.md` alone, or from a previously downloaded spec.** The portal ships continuously and capabilities appear between our snapshots. Proven drift: spec v1.158.0 had `putTradeRequest` as an orphaned schema (edit-TP/SL "deliberately excluded from public API" — we designed workarounds around that); by v1.279.0 (2026-07-04) it was a shipped public endpoint, `PATCH /api/v2/trading/positions/{positionId}`, demo variant included, plus `UnitsToDeduct` partial close. A design was nearly built on the stale fact.

## Verification protocol

1. **Index:** fetch `https://api-portal.etoro.com/llms.txt` — lists every doc page slug, grouped (trading-real, trading-demo, market-data, …).
2. **Endpoint detail:** fetch the specific page as markdown: `https://api-portal.etoro.com/api-reference/<section>/<slug>.md` — full method/path/body/response/auth/rate-limit per page.
3. **Spec version:** the api-reference index page states the current OpenAPI version — record it in anything you write ("verified against vX.Y.Z on DATE").
4. **Tooling:** use WebFetch (or the running app's HTTP client). **`curl` from CLI gets Cloudflare-blocked (403 "Attention Required")** — the portal allows browser-agent fetches only.
5. When our code disagrees with the live doc but works (e.g. close body: doc says `InstrumentID` required, `close_position()` omits it), note the discrepancy where you found it and verify empirically on demo before relying on either.

## Stable facts (re-verify anything load-bearing; last verified 2026-07-04, spec v1.279.0)

- Base URL `https://public-api.etoro.com`; auth headers `x-api-key` + `x-user-key` + `x-request-id` (UUID); demo endpoints carry `/demo/` in the path.
- Rate limits: 60 GET/min shared; writes ~20/min shared across related endpoints. 429 → `{"errorCode": "TooManyRequests"}`, no guaranteed Retry-After.
- Trading (verified live): open by-amount/by-units; close per position with optional `UnitsToDeduct` (partial); **`PATCH /api/v2/trading[/demo]/positions/{positionId}`** for TP/SL edit (`stopLossRate`, `takeProfitRate`, `stopLossType` fixed|trailing, `clearStopLoss`, `clearTakeProfit`; ≥1 field; **202 async** `{operationId, positionId, referenceId}`).
- Write ops are asynchronous (202) — re-sync portfolio before treating them as landed.

## Scope boundary — what this file is NOT

This file documents **eToro's** behaviour: endpoints, limits, payload shapes,
rate semantics. It deliberately does not document **our** policy for using
them, and you will be misled if you look for that here:

- **What we subscribe to** is visibility-driven (#498) — the WS boots quiet and
  sends no Subscribe frame until an SSE stream lands. That is our decision, not
  an eToro constraint. → `.claude/skills/market-data/SKILL.md`,
  `docs/proposals/etl/visibility-driven-live-prices.md`.
- **Which job keeps which table current** → `.claude/skills/market-data/SKILL.md`.
- **Where our WS client process actually runs** (and why `lsof` against the
  uvicorn pids cannot see its socket under `--reload`) →
  `.claude/skills/ops-monitor/SKILL.md` §Process topology.

Recorded 2026-08-04 (#2271) because a session went looking for the last two
here, found only eToro protocol facts, and concluded the subscriber was
disconnected when it was not.

## WebSocket limits — MEASURED, because the portal documents none

`websocket/overview.md` and `websocket/topics.md` state no cap of any kind:
no topics/frame, no topics/connection, no concurrent-session limit. **Absence
of a documented limit is not absence of a limit.** Measured on demo
(#2241, 2026-08-04):

| limit | value | behaviour at the boundary |
| --- | --- | --- |
| topics per **session** | **4,999** | rejected: `errorCode: SubscribeFailed`, `"Too many subscriptions for session"`. Connection survives **and keeps serving**. Per-frame **atomic** — a straddling frame bounces whole, nothing partially applied. |
| bytes per **frame** | **25 KiB** (25,600) | **silent: socket dropped, no ack, no error envelope.** Not applied. Bracket 25,529 B ok / 25,719 B dead. |

- 4,999 replicated exactly on a second independent session — not a
  single-run artefact.
- **An over-cap rejection does not poison the session.** Measured after a
  rejection: 3,141 rate messages in the next 20s on already-subscribed
  topics, and both `Subscribe` and `Unsubscribe` still acked. No reconnect
  needed — handle the rejection, do not tear down.
- **The over-size drop is not a client-side limit.** `ws.send()` returns
  normally and the close is `1006 ABNORMAL_CLOSURE` with an empty reason,
  i.e. **no close frame was received at all** — the socket is dropped, not
  closed. Attribution is server-or-intermediary; do not assume either.
- The session cap is **per connection, not per key** — 3 concurrent sessions
  hold the full 12,684 universe (measured 12,684/12,684 topics **accepted**,
  2.21s; "0 failures" there means zero rejected Subscribe frames, and says
  nothing about data completeness, which S2/#2242 owns).
- `Unsubscribe` frees capacity — live occupancy, not a lifetime budget.
- **Chunk by BYTES, never by topic count** — instrument-id width varies, so a
  count-based cap does not bound frame size. 500 topics ≈ 9.4 KB, proven.
- **Correlate every frame uuid to its ack.** Over-size is silent, so a missing
  ack is the only signal that a Subscribe did not take. eToro acks both ops:
  `{"id": …, "success": true, "operation": "Subscribe"}`. ⚠ The ack carries
  **no `type` field**, so a helper that filters inner messages on `type` will
  not see it — parse acks separately.
- Both are implemented since #2249: `build_subscribe_frames()` /
  `build_unsubscribe_frames()` pack to a 20 KiB budget under the 25,600 B hard
  limit, and the subscriber holds a pending-ack registry that reports any frame
  un-acked after 10s. Dev-verified: 4,900 topics → 5 frames, max 21,615 B, 5/5
  acked; the unchunked frame the code used to send would have been 92,979 B,
  **3.6× the limit**, i.e. an unrecoverable connect → 1006 → reconnect loop
  (nothing drains the ref set, so it cannot self-heal).

The snapshot also carries undocumented `OfficialClosingPrice`, `IsMarketOpen`,
`IsExchangeOpen`, `ConversionRateBid`/`Ask` (see
`docs/etoro-api-reference.md` §WebSocket API); undocumented means
unversioned, so re-verify before depending on them.

## WS rate semantics — MEASURED (#2243, 2026-08-04)

**Rate pushes are FIELD-LEVEL SPARSE DELTAS, not "fat vs thin".** ⚠ This
corrects the two-shape model recorded under #2241. Any subset of `Bid`,
`Ask`, `LastExecution`, `BidDiscounted`, `AskDiscounted` can arrive alone;
the instrument is always on the envelope `topic`, never in the payload.
Census over 180,666 messages: 59.8% pure heartbeat, 16.8% `Bid`+`Ask`,
10.5% `Bid`+`BidDiscounted`+`LastExecution`, 10.1% `Ask`+`AskDiscounted`,
1.6% `LastExecution` alone, ~1.2% across 12 further combinations. Replicated
universe-wide (#2242, 10.95 M messages, 11,855 instruments): 59.4% / 14.7% /
12.7% / 10.8% / 0.9%, i.e. the mix is stable at scale.

⚠ **The heartbeat share tracks INSTRUMENT liquidity, not market session.** A
60-name mega-cap panel reads 37.0% inert; the full universe in the *same* US
session reads 59.4%. Names at the ~1 msg/s ceiling (below) carry proportionally
fewer heartbeats than the median name at 0.175 msg/s. Size ingest off the
universe figure — a liquid panel overstates the price-bearing fraction by ~1.6×.

- **Any consumer MUST carry per-instrument state and merge deltas.** Requiring a
  complete payload sees under half the market. Production does this since #2252:
  `parse_rate_deltas()` → `RateStateStore.apply()` in
  `app/services/etoro_websocket.py`. The pre-fix parser required both `Bid` and
  `Ask` and dropped 58.1% of price-CHANGING messages. Measured on a paired-arm
  capture (184 crypto/FX instruments, 300 s, one stream through both parsers):
  captured share of wire price-changes **45.5% → 92.8%**, median usable-update
  gap **3.27 s → 1.24 s** against a wire price-change gap of 1.24 s, i.e.
  staleness **2.64× → 1.00×** (crypto 3.96× → 1.00×, FX 1.77× → 1.00×). The
  residual 7% are instruments where one side was never quoted, so no complete
  tick can be formed at all.
- **Field PRESENCE and field VALUE mean different things.** An absent
  `LastExecution` means "unchanged"; a present one that is ≤ 0 means "not a real
  trade → NULL" (#1429). A `Decimal | None` cannot express both — the delta type
  needs explicit `has_*` flags or the #1429 rule silently un-does itself.
- **A price-less heartbeat must not emit.** 59.8% of messages carry only
  `Date` + `PriceRateID`; publishing on those advances `quoted_at` with no price
  behind it, reporting freshness that does not exist.
- "23% of messages parse" is right but does **not** mean "77% are inert" —
  only ~60% are. Size ingest against the wire.

**`LastExecution` is NOT a trade print** — the portal's *"price of the most
recent trade execution"* is wrong, the second time this field's documented
meaning has diverged from the wire (cf. #1429's `last = 0.00`). Over 26,741
observations it **never left `[bid, ask]`** (0.00%), which a real print would.
It is bid-side, and how tightly is asset-class-dependent — state the class, not
a single global label:

| arm | `== BidDiscounted` | reading |
| --- | --- | --- |
| **US equity** | **100.0%** | is the pre-markup bid (n=9,417, measured 2026-08-04 13:30–13:41 UTC) |
| Tokyo equity | 100.0% | is the pre-markup bid |
| FX | 97.8% | is the pre-markup bid |
| crypto | 58.7% | bid-*near*, not identical (median spread position 0.000, mean 0.041) |

**The sharpest discriminator is INDEPENDENCE, not containment.** On the US arm,
`last` moved while the quote did not **0 times in 608 s**; the converse happened
2,666 times. A real print is generated by the book and can move on its own —
a series that never does cannot be one. Prefer this test to the excursion test:
it needs no markup-column reasoning and gives a categorical answer.

⚠ Crypto does not fit the clean story — do not carry "it IS BidDiscounted" across
asset classes without re-measuring. `== AskDiscounted` is 0.0% everywhere.

⚠ **Run the bid/ask-excursion test against `BidDiscounted`/`AskDiscounted`, not
`Bid`/`Ask`.** eToro's markup makes the FX series look like a derived mid
(mean spread position 0.399) when against the underlying quote it sits exactly
on the bid. The wrong column pair yields the opposite verdict.

**Build 1-minute bars from `Bid`.** This is an *empirical compatibility rule* —
`Bid` is the series that reproduces eToro's own `OneMinute` REST candle — not a
claim about what the candle semantically is. Reconstruction scores, 131 complete
minutes: `Bid` 77.1% of closes / 60.3% of full OHLC; `LastExecution` 55.0% /
48.1%; mid or ask **0%**. US equities (#2243 US arm, 45 complete minutes):
`Bid` **100% / 100%**, `LastExecution` 100% / 100%, mid and ask **0%**.

⚠ `LastExecution` ties `Bid` wherever the markup is zero (US and Tokyo, where
`last == bid` exactly) and only diverges where it is not (FX: `Bid` 4–7 of 9
closes, `LastExecution` **0 of 9 on every pair**). A US-only or JP-only
comparison therefore **cannot** discriminate the two candidates — it will show a
tie and license the wrong column. Keep an FX arm in any re-measurement.

The residual is granularity, not a second series: on a re-run attributing all 42
mismatches, **zero were Tokyo equities** (100% exact there), and every mismatch
was **within 0.20%** of the REST close — median 0.0019%, max 0.0716%, none above
1%. Reconstruction is the discriminator that settled this spike; containment
alone did not (see the prevention-log entry).

**`OneMinute` volume is equity-only** — populated for US and Tokyo equities,
always `None` for crypto and FX. Volume-confirmed rules are structurally
impossible on crypto/FX from either path. Bid bars cannot carry trade volume at
all, so volume must come from the REST candle.

⚠ **Scope of this verdict: demo env, crypto / FX / Tokyo / US equities.** Not
measured: **live env** (may price, mark up or smooth differently), **HK** (shut
during both captures, 0 messages), and any stressed regime — open/close auction,
halt, wide spread, FX rollover. Re-measure before treating any of those as
settled.

## WS throughput + sizing — MEASURED (#2242, 2026-08-04)

Universe-wide capture, demo, US session: 11,855 instruments on 3 shards,
3,900 s, **10,953,924 rate messages, 3.39 GB on the wire**.

**eToro coalesces pushes to a long-run ~1 message per instrument per second.**
Full population, 3,841 s steady window:

```text
max over all 9,763 ticking instruments : 1.0229 msg/s
300th busiest                          : 1.0135 msg/s   <- 0.9% below the max
median instrument                      : 0.1750 msg/s
instruments above 1.05 msg/s           : 0 of 9,763
```

⚠ **It is a RATE CEILING, not a per-second gate.** Against per-message records:
max **3** messages for one instrument in one second, and **13 instrument-minutes
exceeded 60** (max 62). Do not design a bar builder that assumes ≤60 ticks/min.
(The tempting explanation — that a logical update splits into complementary
bid-side and ask-side deltas — is **false**: only 64 of 1,700 multi-message
seconds are a bid/ask pair; the commonest case is two *identical* full payloads.)

Consequences: throughput scales with **universe size, not volatility**; skew is
near-uniform (top 1% of tickers = 3.5% of messages, top 50% = 90.3%), so
partitioning or sharding on instrument does not balance load; and this transport
carries no sub-second detail.

| steady-state, per second | mean | p95 | p99 | peak |
| --- | --- | --- | --- | --- |
| all rate msgs | 2,817 | 3,560 | 4,312 | **11,999** |
| price-carrying only | 1,140 | 1,539 | 2,033 | 6,057 |

The one-second peak ≈ the subscribed count (11,999 vs 11,855) — a breadth burst,
every instrument at once, 4.3× the mean. It *exceeds* the subscribed count, so
that is not a hard bound. Size the write path on the peak.

**One rate message per WS frame ⇒ 50% of raw bytes are framing** (307 B/msg raw
vs 154 B/msg content). 3.1 GB/h, 20.3 GB per 6.5 h session. Storing frames
verbatim costs ~12.6× a merged-tick tier and supports only bounded-window replay
from a known snapshot (63.7% of price-carrying messages are partial deltas).
Keep a small rolling raw sample for diagnostics, not a full raw tier.

Coverage in one US session: 9,763 of 11,855 ticked (82.4%); JP and HK contributed
**0** (closed). 2,092 never ticked, and a single-session window **cannot**
separate "market shut" from "listed but never quotes" — that needs a capture per
arm's own session.

⚠ Caveats: demo env; one 65-minute US window, no open/close transition; and
"universe-wide" is the **mapped tradable subset, 11,855 of 12,684 = 93.5%** —
829 instruments sit in exchanges outside the arm map and were never subscribed.

## Maintenance

When you verify a NEW capability or find drift: update `docs/etoro-api-reference.md` + the memory reference files in the same session (skill-ownership rule — no "later").
