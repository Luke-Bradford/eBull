# eToro API — live-portal freshness discipline

## When to use

Before citing, speccing, or implementing against ANY eToro API capability (endpoints, auth, rate limits, request/response shapes) — and before claiming an operation is "not supported by the public API".

## The rule

**Never cite eToro API capabilities from memory, from `docs/etoro-api-reference.md` alone, or from a previously downloaded spec.** The portal ships continuously and capabilities appear between our snapshots. Proven drift: spec v1.158.0 had `putTradeRequest` as an orphaned schema (edit-TP/SL "deliberately excluded from public API" — we designed workarounds around that); by v1.279.0 (2026-07-04) it was a shipped public endpoint, `PATCH /api/v2/trading/positions/{positionId}`, demo variant included, plus `UnitsToDeduct` partial close. A design was nearly built on the stale fact.

## Verification protocol

1. **Index:** fetch `https://api-portal.etoro.com/llms.txt` — lists every doc page slug, grouped (trading-real, trading-demo, market-data, …).
2. **Endpoint detail:** fetch `https://api-portal.etoro.com/api-reference/<section>/<slug>`
   through the browser tool — full method/path/body/response/auth/rate-limit per
   page. The `.md` form and direct `curl` may be rejected even when the HTML page
   is live; navigate from a neighbouring live page when the safe-open check
   rejects a double-hyphen section slug.
3. **Spec version:** the api-reference index page states the current OpenAPI version — record it in anything you write ("verified against vX.Y.Z on DATE").
4. **Tooling:** use WebFetch (or the running app's HTTP client). **`curl` from CLI gets Cloudflare-blocked (403 "Attention Required")** — the portal allows browser-agent fetches only.
5. When our code disagrees with the live doc but works (e.g. close body: doc says `InstrumentID` required, `close_position()` omits it), note the discrepancy where you found it and verify empirically on demo before relying on either.

## Stable facts (re-verify anything load-bearing; market/trading index re-verified 2026-08-11)

- Base URL `https://public-api.etoro.com`; auth headers `x-api-key` + `x-user-key` + `x-request-id` (UUID); demo endpoints carry `/demo/` in the path.
- Rate limits have DRIFTED. The live portal index on 2026-08-11 documents the
  market-data family at **120 GET/min shared**, ordinary trading reads at 60/min
  shared, order writes at 20/min shared, and eligibility/what-if-cost endpoints
  at 20/min dedicated. Do not retain the older blanket 60-GET/min assumption.
  429 → `{"errorCode": "TooManyRequests"}`, no guaranteed Retry-After.
- Trading (verified live): open by-amount/by-units; close per position with optional `UnitsToDeduct` (partial); **`PATCH /api/v2/trading[/demo]/positions/{positionId}`** for TP/SL edit (`stopLossRate`, `takeProfitRate`, `stopLossType` fixed|trailing, `clearStopLoss`, `clearTakeProfit`; ≥1 field; **202 async** `{operationId, positionId, referenceId}`).
- Write ops are asynchronous (202) — re-sync portfolio before treating them as landed.
- **Two trading preflight endpoints exist in BOTH demo and real**, each at a
  dedicated 20 requests/minute: `POST /api/v2/trading/info/{demo|real}/eligibility`
  and `POST /api/v2/trading/info/{demo|real}/costs`. Eligibility accepts at most
  100 ids/symbols and returns open/close/partial-close permission, min exposure,
  max units, order-quantity/fill types and per-settlement/per-direction leverage
  configs with SL/TP limits. What-if accepts `buy` or `sellShort` opens and
  returns an OPEN vocabulary of named cost rows (documented examples include
  markup, market spread, transaction fee, overnight, weekend and tax) plus
  `lastUpdated`. **Measured demo 2026-08-09:** `etoro-preflight-v2` resolved a
  deterministic four-Stock/four-ETF cohort but refused one locally tradable
  name. Twenty complete 1x/10x long-real and x1 short-CFD requests across seven
  permitted instruments all used `value` and omitted documented `amount`;
  18/20 timestamps were about 41 hours old, 2/20 current. Scaling relationships
  varied by component (proportional, invariant, rounded/other, zero-only), so
  scale does not prove a unit. **Zero of 20 responses were execution-usable.**
  Preserve both fields and the timestamp; fail execution until the provider
  documents `value` or starts returning current documented monetary `amount`.
  Never coerce a missing field to zero. Thin, non-persisting adapters and the
  bounded census are wired in `EtoroBrokerProvider` and
  `scripts/verify_2437_trading_preflight.py`; no recurring cost writer is
  justified by this evidence.
- ⚠⚠ **`value` IS DENOMINATED IN THE ROW'S OWN `currency` — decoded 2026-08-12
  (#2598), and two of the 2026-08-09 findings above are now WRONG.** Re-verified
  against the live portal the same day: the documented response still carries
  `costType` + `amount` ("the monetary value of this cost component, expressed
  in `currency`") + `currency`, and **`value` still appears nowhere in the
  documentation**. So the drift is real and unannounced, but its unit is now
  measured rather than unknown. Three independent lines agree:
  1. **Scaling** — 1x→10x ticket moves `marketSpread` by 9.93-10.14x and
     `transactionFee` by exactly 10.00x, at a stable implied bps. A rate would
     be invariant under scaling.
  2. **An independent same-quantity measurement** — `value / ticket_amount`
     lands on the `quotes` panel's separately observed `spread_pct` for the same
     instrument. `quotes` is written by a feed path that never reads a what-if
     response, so this is not circular. Stable names match closely and reproduce
     across all three runs (XLV 0.6 vs 0.59 bps; NUVL 0.8 vs 0.81 bps; SPY 0.9
     vs 0.91 on the third).
  3. **The rounding quantum** — costs come back rounded to 0.01 USD, so a $100
     ticket has a 1.0 bp floor and every tight instrument reads ~1.0 bp there.
     The agreement appears at $1,000 and vanishes at $100, which is the
     signature a monetary field must have and a rate must not.

  ⚠ **Do NOT upgrade this to `marketSpread == the quoted spread`.** On the most
  actively quoted names both sides move between runs minutes apart: AAPL read
  0.3 → 1.3 → 1.3 bps implied while its own observed quote went 0.33 → 3.63.
  Consistent with sampling a live book through a 0.01 USD quantum, and far too
  loose to be an identity. The claim that survives is **order-of-magnitude
  agreement, tight on stable names and loose on fast ones** — and note the first
  run alone read as near-exact on all four, which is precisely the overclaim a
  single sample invites.

  ⚠⚠ **AN ABSENT COST ROW IS NOT A ZERO COST.** At a $100 ticket the
  `marketSpread` row is omitted entirely rather than sent as `0.0` when the real
  spread is under the quantum — observed on AAPL and, in a different run, on SPY,
  so it tracks the live spread rather than the instrument. Coercing a missing row
  to zero prices the tightest names as free, which is why the "never coerce" rule
  above is load-bearing rather than defensive. ⚠ **Test row MEMBERSHIP separately
  from its value** (`"marketSpread" in rows`, not `rows.get(...) is not None`):
  the two differ exactly when a row is present with a null value, and a probe
  that conflates them corroborates the absence claim with the wrong observation.
  Caught by Codex at checkpoint 2; `market_spread_value_null` is `false` on every
  observation to date, so the claim was safe — but only the corrected instrument
  can show that.

  ⚠ **`markup` and `overnightFee` read `0.0` on all 28 observations, including
  x1 short CFDs, and their unit is therefore STILL undecodable** — an all-zero
  component cannot distinguish "0 dollars" from "0 percent". Do not read that
  zero as evidence that carry is zero; a CFD short accrues financing by
  construction (see the risk-posture note in `.claude/CLAUDE.md` and #2363).

  **Freshness is per-instrument, not per-response.** In the same batch AAPL/SPY
  were seconds old, XLV about 3.6 hours, and NUVL **26 days** (`2026-07-17`).
  The 2026-08-09 "18/20 at ~41 hours" reading was a property of that cohort, not
  a contract property; the 2026-08-12 census returned 20/20 `within_24h`.
  Reproduce both with `scripts/verify_2437_trading_preflight.py --apply` and
  `scripts/verify_2598_preflight_quote_crosscheck.py`; captured responses are in
  `tests/fixtures/etoro_preflight_2598/`.
- **Order-to-position reconciliation exists in v2:**
  `GET /api/v2/trading/info/{demo|real}/orders:lookup` returns
  `positionExecutions[].positionId`. The live detail page verified 2026-08-09
  requires exactly one of numeric `orderId` or `referenceId`; `referenceId` is
  the `X-Request-Id` sent on submission. The current v2 create-order page states
  that this unique GUID is required for idempotency. Commit it before broker
  I/O, never rotate it after an uncertain response, and use the same value for
  lookup/retry. This is the durable way to bind a submitted strategy entry to
  the exact position(s) it owns; do not infer a position from instrument, time,
  units or FIFO order. The response also supplies opening units, average price,
  execution time and fees; retain these compact facts to prove partial-fill and
  one-to-many cardinality without storing every poll payload.
- **Historical account balances are documented but unavailable to this demo
  connection (verified 2026-08-11):** the live portal documents
  `GET /api/v1/balances/history`, with at most 365 daily snapshots from the last
  12 months and cash/invested/P&L/total-balance fields. An authenticated,
  read-only probe using the configured demo credentials returned HTTP 403 with
  only `errorCode` and `errorMessage`. Do not infer that demo history can be
  backfilled. eBull instead captures one prospective daily aggregate from the
  working `/api/v1/trading/info/demo/pnl` response; it stores no raw payload or
  per-position duplicate. Re-probe the history endpoint before changing that
  boundary.
- **The live P&L payload has a required envelope (verified 2026-08-11):** the
  HTTP-200 response contains a top-level `clientPortfolio` object. Formula
  inputs live inside it, including singular `credit`, `positions`, `mirrors`,
  `ordersForOpen`, and `orders`. Fixtures must preserve this observed shape.
  Fail closed if the envelope or a required component is absent; never flatten
  speculatively or turn response drift into zero account equity.

## ⚠⚠ WE HAVE INTRADAY HISTORY. Read this before saying otherwise — MEASURED 2026-08-09

**Never claim "we have no intraday data" or "we only have daily candles".** It is
false, it has been corrected by the operator four separate times, and it has
repeatedly steered strategy research away from viable ground. If you are about to
write a sentence about our intraday capability, this section is the source.

`GET /api/v1/market-data/instruments/{id}/history/candles/asc/{interval}/{count}`
serves intraday OHLCV **on demand, per instrument, with no recorder required**.
Wired as `EtoroMarketDataProvider.get_intraday_candles`
(`app/providers/implementations/etoro.py`).

⚠ **`count` caps at 1000 bars and there is NO date anchor in the URL** — you get the
*last* N bars, never an arbitrary window. So reach back is purely a function of
interval, and the only way to go deeper than the table below is to poll forward and
accumulate.

Measured on AAPL (`instrument_id=1001`) at `count=1000`, 2026-08-09:

| interval | earliest bar returned | reach back |
| --- | --- | --- |
| `OneMinute` | 2026-08-07T02:23Z | ~2 days |
| `FiveMinutes` | 2026-08-04T07:20Z | ~5 days |
| `ThirtyMinutes` | 2026-07-09T07:00Z | ~1 month |
| `OneHour` | 2026-06-08T02:00Z | ~2 months |
| `FourHours` | 2025-12-02T12:00Z | **~8 months** |

`TenMinutes` and `FifteenMinutes` are also valid tokens (`IntradayInterval` in
`app/providers/market_data.py`) and were not probed.

Reproduce: `curl -s "http://localhost:8000/_debug/etoro-candles-probe?instrument_id=1001&count=1000&interval=FourHours"`
(`app/api/_debug_ws.py:47`, dev-like envs only).

- ⚠ **Volume IS populated** on these bars (observed 273 / 7,090 / 951) — unlike the
  WS stream, where volume is equity-only (see the WS section below).
- ⚠ **Extended hours are included.** The `02:23Z` and `07:20Z` stamps are outside US
  RTH, so any RTH-only study must filter by session explicitly; the bars will not do
  it for you.
- ⚠ `price_intraday` (created in `sql/023_live_pricing_currency.sql`) **exists and has
  0 rows.** Nothing writes it. The absence of stored intraday is a *build gap*, not a
  data-availability gap — do not confuse the two.

**The three capabilities are distinct. Do not collapse them:**

| capability | what it gives | limit |
| --- | --- | --- |
| REST intraday candles | OHLCV history, any instrument, on demand | 1000 bars, no date anchor; **120 market-data GET/min shared** |
| WS rate stream | live bid/ask/last ticks, all instruments | no depth, no sizes; forward-only |
| `research_price_daily` | 25.9M daily bars 1962-2026, survivorship-controlled | daily granularity only |

⚠ **Corpus arithmetic, because the rate limit binds hard.** The currently
documented 120 market-data GET/min shared gives a full 6,700-instrument pull a
theoretical floor of **~56 minutes per interval per pass**, and it competes with
every other market-data call. A cross-sectional intraday study is therefore a
*scheduled harvest*, not an ad-hoc query — budget for it in the design, and
prefer `FourHours` (deepest history per request) when bootstrapping.

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

## eToro prices vs the public tape — MEASURED (#2240, 2026-08-04)

How far eToro's stored closes sit from the public market, and whether the two
are interchangeable for analysis. Seven instruments, ~1,035 overlapping bars
each, `price_daily.close` against **raw** (unadjusted) public closes:

| | AAPL | MSFT | GME | JPM | HD | KO | XOM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| daily-return correlation | 0.979 | 0.963 | 0.996 | 0.989 | 0.985 | 0.979 | 0.992 |
| mean level bias | −0.14% | −0.14% | −0.22% | −0.17% | −0.20% | −0.16% | −0.17% |
| median RSI-14 diff (0–100) | 0.19 | 0.16 | 0.12 | 0.18 | 0.15 | 0.18 | 0.13 |
| SMA-200 regime agreement | 100.0% | 99.8% | 99.8% | 99.9% | 99.4% | 99.9% | 99.8% |

- **The bias is consistently NEGATIVE, ~0.15–0.22%** — eToro's close sits about a
  half-spread *below* the public close. That is independent corroboration of S3
  (#2243): our bars are built from **Bid**, not from a trade print. Useful as a
  first-order execution-cost input.
- **It is a level offset, not a shape difference.** TA reads shape, so signals
  computed on either series agree — which is what licenses using public data for
  research while executing on eToro.
- ⚠ **Compare against RAW closes, not adjusted ones.** An earlier run of this
  using dividend-adjusted public prices showed JPM/HD diverging ~5% and looked
  like a data defect. It was the dividend yield. eToro candles are **price, not
  total return** — no dividend adjustment — which matters for any multi-month
  return calculation.
- ⚠ **History caps at ~4 years per instrument** (1,000-bar ceiling, #603, no
  `from_date` pagination). Public sources reach decades further. Never infer
  "this history is unavailable" from `price_daily`.

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

## Automated paper-entry boundary — VERIFIED 2026-08-09 (#2449)

- Use only `POST /api/v2/trading/execution/demo/orders` for strategy entry.
  The adapter must refuse `env != demo`; never derive a strategy endpoint from a
  generic real/demo prefix and never fall back to the legacy manual writer.
- Commit one UUID before I/O and send that exact value as `X-Request-Id`.
  Acceptance returns `orderId`, `referenceId` and `token`; require a positive
  order id and `referenceId == request UUID`. Any malformed/transport/5xx result
  is uncertain and resolves through `orders:lookup`, not a newly keyed POST.
- The MVP shape is long `buy`, `real`, market, x1, USD amount, fixed stop loss
  and take profit. Shorts are CFDs and are outside this contract.
- Current cost docs show `costs[].amount` in USD plus `lastUpdated`, but the
  measured demo spike returned undocumented `value`. `amount` and `value` are
  not aliases: automated use rejects `value`, stale/missing/negative/non-USD
  components and positive recurring fees without an explicit hold horizon.
- Do not treat portfolio `credit` as spendable cash. The official P&L formulas
  are: available cash = credits minus manual pending-open orders and other
  pending orders; total invested includes direct/mirror positions, mirror
  adjusted cash, pending orders and external costs; equity = available cash +
  total invested + unrealised P&L. All manual holdings count toward risk even
  though strategy ownership remains exact-order provenance only.

Primary pages: `trading--demo/create-an-order`,
`trading--demo/get-what-if-trading-cost-breakdown`, and guides
`calculate-available-cash`, `calculate-total-invested`, `calculate-equity`.

## Automated paper-position boundary — VERIFIED 2026-08-09 (#2452)

- Strategy edits use only `PATCH /api/v2/trading/demo/positions/{positionId}`;
  strategy closes use only
  `POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}`.
  Both adapters refuse real credentials and require the exact actively owned
  broker position id before domain code reaches broker I/O.
- PATCH returns asynchronous acceptance only. Persist the UUID first, require
  response `positionId` and `referenceId` to match, then re-fetch the portfolio;
  never mark the edit applied from HTTP 202.
- A close is complete only when
  `GET /api/v1/trading/info/demo/close-orders/{orderId}` returns the exact owned
  id in `positions[]`. Portfolio disappearance alone cannot distinguish broker
  SL/TP, manual intervention, missing data and the requested close.
- Neither endpoint documents lookup by request UUID. If the process loses the
  accepted operation/order identity, re-sync once: exact requested SL/TP already
  landed may be accepted; every other outcome is `reconcile_required`, never a
  newly keyed replay.
- Keep only policy revisions and material mutation facts. Unchanged completed
  bars and polling heartbeats write no rows; repeat rejection of the same
  material edit reuses its terminal audit result.
- Return the untouched response object with every typed mutation result/error.
  Persist it before advancing the material operation: PATCH/latest close detail
  on the operation row and close submission on its linked order. Overwrite the
  latest close-detail response rather than appending a row for every poll.

Primary pages: `trading--demo/modify-stop-loss-and-take-profit-settings-on-an-open-position`,
`trading--demo/close-demo-position-by-units`, and
`trading--demo/get-close-order-information-and-closed-position-details`.

## Strategy live-promotion boundary — VERIFIED 2026-08-09 (#2450)

- Re-check `llms.txt` and the real create-order page before any live-strategy
  change. The current portal exposes `POST /api/v2/trading/execution/orders`,
  requires the unique request id, supports fixed SL/TP, and shares the 20/minute
  order-write cap. Endpoint existence does not validate automated live use.
- Keep live strategy activation refused while what-if costs return undocumented
  `value`, the current strategy writer accepts demo credentials only, or no
  current strategy version passes recent and untouched paper evidence.
- Never let `enable_live_trading`, a generic stage transition, or a live
  deployment select the real endpoint. Require the dedicated immutable policy,
  complete report, five kill drills, explicit operator attempt and separately
  validated live writer. Until then emit
  `live_strategy_broker_contract_not_validated`.
- Keep the recurring lifecycle demo-only and bounded per cycle: 20 uncertain
  order lookups, five exact-owned positions and five new candidates. Update the
  five keyed health blocks in place; do not persist broker health/cost polling
  payloads or heartbeat rows.

Primary page: `trading--real/create-an-order`. Operational contract:
`docs/proposals/ta/2026-08-09-strategy-live-promotion-runbook.md`.
