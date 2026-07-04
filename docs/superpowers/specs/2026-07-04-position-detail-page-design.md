# Position detail page — per-trade TP/SL edit, partial close, three-level IA

**Date:** 2026-07-04 · **Status:** approved (operator, this session) · **Supersedes direction of:** #1900 (instrument-tab close buttons), instrument-page Positions tab (#1899/#1926 placement)

## Problem

Per-trade rows shipped on the instrument page Positions tab (#1899/#1926) are display-only. Operator cannot set TP, set SL, or close a trade from eBull — all of which the eToro app supports per trade. Separately, the operator direction is that trade interaction does NOT belong on the instrument page at all.

## Source rule (fintech IA convention + live eToro API)

Three-level hierarchy, consistent across eToro, Schwab, IBKR, Fidelity:

1. **Portfolio** — aggregates only; one row per holding. "How is my book doing?"
2. **Position/holding detail** — one instrument's holding: summary header + component trades (eToro: trades with per-trade TP/SL/close; US brokers: tax lots with per-lot cost basis). **All interaction lives here.** "How am I doing in THIS, what do I do about it?"
3. **Instrument/market page** — research about the asset (chart, financials, filings, news, verdict). "What is this thing?"

Rule: act on what you OWN at level 2; study what it IS at level 3.

eToro API (live portal, spec v1.279.0, verified 2026-07-04 — see `.claude/skills/data-sources/etoro-api.md`):

- **Edit TP/SL on open position (PUBLIC, demo + real):** `PATCH /api/v2/trading/demo/positions/{positionId}` (real: no `/demo`). Body: `stopLossRate`, `takeProfitRate`, `stopLossType` (`fixed`|`trailing`), `clearStopLoss`, `clearTakeProfit` — ≥1 field required. Response 202 Accepted `{operationId, positionId, referenceId}` — **asynchronous**. Auth: same `x-api-key`/`x-user-key`/`x-request-id` headers. 60 req/60s shared quota.
- **Partial close:** `POST /api/v1/trading/execution/demo/market-close-orders/positions/{positionId}` body `UnitsToDeduct` (nullable; omit = full close). Provider `close_position()` already accepts `units_to_deduct` — never exposed above provider level. ⚠ Live doc also lists `InstrumentID` (required) in the close body; our implementation omits it and works today — verify against live behaviour at implementation time.
- **TP/SL at open:** already plumbed (`_order_body_common`: `StopLossRate`/`TakeProfitRate`/`IsTslEnabled`).

## Design

### Route + page

New page `/portfolio/holdings/:symbol` (level 2). Reached by clicking a holding row on Portfolio. Instrument name/icon in its header links on to `/instruments/:symbol` (level 3).

Top → bottom:

1. **Holding header** — symbol, company name, held units, avg entry, total invested, market value, unrealized P&L, day change. Same figures as the Portfolio row (no summary/detail divergence).
2. **Open trades table** — one row per broker trade: opened date, units, entry, current price, per-trade P&L, TP, SL (trailing badge when `stopLossType=trailing`). Row actions:
   - **Edit TP/SL** — modal: set/adjust/clear TP and SL, fixed vs trailing toggle. Submits `PATCH /portfolio/positions/{position_id}/sltp`.
   - **Close** — modal: units input (default = full trade) → partial close via `units_to_deduct`; shows est. proceeds + est. realized P&L; confirm required.
   - Table header: **Close all** — sequential per-trade closes behind one confirm summary (total units, est. proceeds, est. P&L).
3. **Closed round-trips** — the #1926 table, relocated here unchanged.
4. **Position alerts** — position_monitor breaches (`sl_breach`/`tp_breach`/`thesis_break`) scoped to this instrument.

### What moves / what dies

- Instrument page **Positions tab: REMOVED** (operator decision). #1899 per-trade table and #1926 closed round-trips move to this page. `Held: Nu` badge on the instrument page links to `/portfolio/holdings/:symbol`.
- Portfolio page: holding rows become links to the new page; existing inline Close/Add buttons on Portfolio rows delegate there (Add flow unchanged).
- Activity tab stays as the global cross-instrument ledger.

### Backend contract

- Provider: new `update_position(position_id, *, stop_loss_rate=None, take_profit_rate=None, stop_loss_type=None, clear_stop_loss=False, clear_take_profit=False) -> BrokerOrderResult` → PATCH v2 endpoint (demo/real path from env, mirroring `_exec_prefix` pattern).
- API: new `PATCH /portfolio/positions/{position_id}/sltp` (session auth). Validation: ≥1 field; TP above / SL below current price for longs (v1 long-only); rejects on instruments without an open broker position.
- API: existing close endpoint gains optional `units` body field → provider `units_to_deduct`. Bounds: `0 < units <= trade units`.
- **Execution guard + audit:** both writes go through the execution guard like existing closes; every accepted request writes an audit row (operation, operationId from the 202, requested values, operator, timestamp).
- **202-async handling:** after PATCH/close accepted, trigger a portfolio re-sync, then refresh UI from DB. No optimistic UI writes. If the operation hasn't landed after sync, surface "pending at broker" state (statusID from order-info endpoint where available).

### Safety model (unchanged invariants)

- Every close/TP/SL change is an explicit, confirmed operator action. No auto-close from eBull; position_monitor remains read-only alerting.
- Broker-side TP/SL is a standing instruction executed by eToro — setting it is the explicit act.
- Demo-first: all endpoints exercised on demo env; real env untouched until operator go-live.

### Out of scope (v1)

- Lot-matching strategies / tax-optimizer views (UK S.104 pooling lives in the Tax page, #1905; per-trade P&L here is the broker view, not the CGT view — label it so).
- Opening new positions from this page.
- WebSocket live updates (#274) — page uses existing quote polling.

## Testing

- Pure-logic: TP/SL validation rules (side/price bounds, ≥1-field, clear-vs-set exclusivity), units bounds for partial close — table tests.
- Provider: request-shape tests for `update_position` (body/path/headers, demo vs real prefix), mocked transport.
- ONE DB-backed integration test for the sltp endpoint happy path (guard + audit row).
- FE: modal validation + submit wiring unit tests; change-coupled FE-QA on the live page (screenshot, per feedback-change-coupled-fe-qa) before done.

## Tickets

- Supersede/redirect #1900 → this design (close stays, but on the new page; per-trade close UI is part of the page build).
- New feature ticket: position detail page (FE+BE per this doc).
- #1899/#1926: no code revert; tables relocate as part of the page build.
