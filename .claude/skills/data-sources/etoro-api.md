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

## Maintenance

When you verify a NEW capability or find drift: update `docs/etoro-api-reference.md` + the memory reference files in the same session (skill-ownership rule — no "later").
