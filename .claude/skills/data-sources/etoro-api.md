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

## WebSocket limits — MEASURED, because the portal documents none

`websocket/overview.md` and `websocket/topics.md` state no cap of any kind:
no topics/frame, no topics/connection, no concurrent-session limit. **Absence
of a documented limit is not absence of a limit.** Measured on demo
(#2241, 2026-08-04):

| limit | value | behaviour at the boundary |
| --- | --- | --- |
| topics per **session** | **4,999** | rejected: `errorCode: SubscribeFailed`, `"Too many subscriptions for session"`. Connection survives. Per-frame **atomic** — a straddling frame bounces whole, nothing partially applied. |
| bytes per **frame** | **25 KiB** (25,600) | **silent: close 1006, empty reason, no ack, no error envelope.** Not applied. Bracket 25,529 B ok / 25,719 B dead. |

- The session cap is **per connection, not per key** — 3 concurrent sessions
  hold the full 12,684 universe (measured 12,684/12,684, 0 failures, 2.21s).
- `Unsubscribe` frees capacity — live occupancy, not a lifetime budget.
- **Chunk by BYTES, never by topic count** — instrument-id width varies, so a
  count-based cap does not bound frame size. 500 topics ≈ 9.4 KB, proven.
- **Correlate every frame uuid to its ack.** Over-size is silent, so a missing
  ack is the only signal that a Subscribe did not take. eToro acks both ops:
  `{"id": …, "success": true, "operation": "Subscribe"}`.

Two rate-push shapes: a fat snapshot, and a thin delta carrying only
`{"Date","PriceRateID"}` (no price, no `InstrumentID`) that
`_parse_rate_content` drops. 23% of inner rate messages parsed to a
`QuoteUpdate` — **size ingest against the wire, not parsed ticks.** The
snapshot also carries undocumented `OfficialClosingPrice`, `IsMarketOpen`,
`IsExchangeOpen`, `ConversionRateBid`/`Ask` (see
`docs/etoro-api-reference.md` §WebSocket API); undocumented means
unversioned, so re-verify before depending on them.

## Maintenance

When you verify a NEW capability or find drift: update `docs/etoro-api-reference.md` + the memory reference files in the same session (skill-ownership rule — no "later").
