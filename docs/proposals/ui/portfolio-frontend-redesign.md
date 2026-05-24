# Portfolio page & frontend redesign

**Status:** Draft — awaiting operator review
**Triggered by:** #221 review feedback — dashboard is disjointed, positions table doesn't scale, no portfolio management, copy-trading detail feels detached

## Problem statement

The current dashboard mixes high-level summary (AUM, system status) with a flat positions table that will break at 50+ instruments. Copy-trading mirrors sit in the same table as direct holdings but feel visually and functionally detached. There is no position management — no drill-through to individual positions, no SL/TP visibility, no order entry, no way to close or add to positions from the UI.

The frontend needs to feel like a trading application: dense, functional, keyboard-navigable. Bloomberg Terminal information density meets eToro's portfolio UX.

---

## Current state

### What exists

| Layer | State |
|-------|-------|
| **Direct positions** | `positions` table — one row per instrument, aggregated (units, avg_cost, cost_basis). No SL/TP, no leverage, no individual trade tracking. |
| **Copy positions** | `copy_mirror_positions` — per-trade with SL/TP, leverage, fees. Managed by the copied trader, not the operator. |
| **Broker: place_order** | Market orders only (by amount or units). BUY/ADD actions. Hard-coded: no SL/TP, no leverage, no limit orders. |
| **Broker: close_position** | Exists. Resolves instrument_id to eToro positionId internally. |
| **Broker: edit SL/TP** | Not implemented. eToro API supports it but no provider method exists. |
| **Frontend** | Dashboard with summary cards + flat PositionsTable. No dedicated Portfolio page. No keyboard navigation. No order entry. |

### What's missing for "full fat" portfolio management

1. **Dedicated Portfolio page** with proper drill-through
2. **Position detail view** — per-instrument breakdown with SL/TP, entry date, source
3. **Order entry panel** — buy/sell, market/limit, amount/units, SL/TP
4. **Position editing** — modify SL/TP on existing positions (new broker method needed)
5. **Keyboard navigation** — j/k to move, Enter to drill in, Esc to back out, / to search
6. **Pagination/search/filter** for 50+ positions
7. **Consistent visual language** across direct holdings and copy-trading

---

## Proposed page structure

### Navigation

```
Sidebar:
  Dashboard       /                    High-level overview
  Portfolio       /portfolio           Main working view — positions + management
  Instruments     /instruments         Browse & research
  Rankings        /rankings            Scored instruments
  Recommendations /recommendations     AI-generated trade ideas
  Admin           /admin               Jobs, system config
  Settings        /settings            Credentials, operators
```

**Changes:** Add "Portfolio" as the second nav item. Dashboard stays as the overview. Move positions OUT of dashboard into dedicated portfolio page.

### 1. Dashboard (/) — Operator overview

Stripped back to a command centre. No positions table here.

```
┌─────────────────────────────────────────────────────────────┐
│ Dashboard                                                    │
├──────────────────────────────┬──────────────────────────────┤
│ Summary cards (AUM, Cash,    │ System status                │
│ P&L, Allocation breakdown)   │ (health, kill switch, config)│
│                              │                              │
│ Quick actions:               │ Recent recommendations       │
│ • View portfolio             │ (last 5, with status pills)  │
│ • Latest alerts              │                              │
│ • Top movers                 │                              │
└──────────────────────────────┴──────────────────────────────┘
```

### 2. Portfolio (/portfolio) — The main working view

This is where the operator spends most of their time. Three visual zones:

```
┌─────────────────────────────────────────────────────────────┐
│ Portfolio                          Search: [____________] /  │
├─────────────────────────────────────────────────────────────┤
│ AUM: £142,301   Cash: £5,230   Positions: 47   Mirrors: 2  │
├─────────────────────────────────────────────────────────────┤
│ ┌─ Positions ──────────────────────────────────────────────┐│
│ │ ● Name          Units   Invested  Price    Value     P&L ││
│ │ ► thomaspj      —       £10,200   —        £14,300  +40% ││ ← avatar row
│ │   AAPL          12.5    £2,104    £191.20  £2,390  +13%  ││ ← direct position
│ │   MSFT          8.0     £1,820    £425.10  £3,401  +86%  ││
│ │   NVDA          4.2     £1,640    £880.00  £3,696 +125%  ││ ← focused row (j/k)
│ │   ▼ triangula   —       £5,100    —        £5,800  +13%  ││ ← copy trader
│ │   TSLA          3.0     £890      £175.50  £526    -40%  ││
│ │   ...                                                     ││
│ │                                     Showing 1-25 of 49   ││
│ └──────────────────────────────────────────────────────────┘│
│                                                              │
│ ┌─ Position detail (shown when row selected) ──────────────┐│
│ │ NVDA — NVIDIA Corporation                  NASDAQ | Tier 1││
│ │ Holdings: 4.2 units @ £390.48 avg                        ││
│ │ Value: £3,696 | P&L: +£2,056 (+125.3%)                  ││
│ │ SL: — | TP: — | Source: broker_sync                       ││
│ │                                                           ││
│ │ [Buy more]  [Close position]  [Edit SL/TP]  [View chart] ││
│ └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

**Key behaviours:**
- **j/k** moves the focus highlight down/up through the list
- **Enter** on a direct position opens the detail panel below the table
- **Enter** on a copy trader navigates to `/copy-trading/:mirrorId` (the sub-portfolio view)
- **Esc** closes the detail panel
- **/** focuses the search box
- **Search** filters by symbol, company name, or trader username
- Rows sorted by market value DESC (same as current)
- Pagination at 25 rows with keyboard-navigable page controls

**Detail panel actions:**
- "Buy more" → order entry modal
- "Close position" → confirmation modal → calls broker `close_position`
- "Edit SL/TP" → modal (requires new broker provider method — phase 2)
- "View chart" → navigates to instrument detail page

### 3. Copy trader detail (/copy-trading/:mirrorId)

Already implemented in this PR with instrument grouping. Enhance with:
- Read-only SL/TP per position (data exists in `copy_mirror_positions`)
- "Pause copying" / "Stop copying" actions (future — needs broker provider method)
- Back to portfolio (not dashboard)

### 4. Order entry modal

Triggered from Portfolio detail panel "Buy more" or from Instrument detail page.

```
┌─────────────────────────────────┐
│ Buy NVDA                    [×] │
├─────────────────────────────────┤
│ Current price: $880.00          │
│ Spread: 0.04%                   │
│                                 │
│ Order type:  ○ Market  ○ Limit  │
│ Limit price: [________]        │
│                                 │
│ Size:                           │
│ ○ By amount: [£_________]      │
│ ○ By units:  [__________]      │
│                                 │
│ Stop loss:   [________] or off  │
│ Take profit: [________] or off  │
│                                 │
│ Est. cost: £3,520               │
│ Cash available: £5,230          │
│                                 │
│ [Preview order]                 │
├─────────────────────────────────┤
│ ⚠ This will place a real order  │
│ on eToro demo.                  │
│                                 │
│ [Confirm]  [Cancel]             │
└─────────────────────────────────┘
```

**Safety:**
- Two-step: Preview → Confirm (no one-click trades)
- Shows environment badge (demo/live)
- Shows cash impact
- Validation: amount must be within available cash, units must be positive
- Kill-switch check before submission

---

## eToro API findings (2026-04-14)

Research against the eToro API documentation confirms per-position SL/TP:

### Confirmed capabilities

| Capability | API support | Detail |
|---|---|---|
| **SL/TP at order creation** | Yes | Both market and limit orders accept `StopLossRate`, `TakeProfitRate`, `IsTslEnabled`, `IsNoStopLoss`, `IsNoTakeProfit` |
| **Per-position SL/TP tracking** | Yes | Each `Position` object has `stopLossRate`, `takeProfitRate`, `leverage`, `stopLossVersion` |
| **Partial close** | Yes | `UnitsToDeduct` parameter on close endpoint |
| **Limit orders** | Yes | Market-if-touched: `Rate` (trigger price) + same SL/TP fields |
| **Edit SL/TP on existing positions** | **Not in public API** | Orphaned `putTradeRequest` schema exists in OpenAPI spec (positionId, stopLossRate, takeProfitRate, isTrailingStopLoss) but is referenced by ZERO path endpoints. `stopLossVersion` description: "Each time StopLossRate is manually update this value is incremented". The endpoint exists internally (eToro web app uses it) but is deliberately excluded from the public API. |

### Current architectural gap

The `positions` table (`sql/001_init.sql:159`) has `instrument_id` as PRIMARY KEY — one row per ticker. The sync function `_aggregate_by_instrument()` (`app/services/portfolio_sync.py:75`) deliberately collapses all individual broker positions into this single row, discarding:
- Individual position IDs
- Per-position SL/TP rates
- Per-position leverage
- Individual entry prices and dates
- Position fees
- The full raw payload

Meanwhile, `copy_mirror_positions` (`sql/022_copy_trading_tables.sql:66`) already does per-position tracking correctly with all of these fields.

### Resolution: `broker_positions` table

A new `broker_positions` table stores individual eToro positions for direct holdings, mirroring what `copy_mirror_positions` does for copy-trading:

```sql
CREATE TABLE broker_positions (
    position_id              BIGINT PRIMARY KEY,       -- eToro positionID
    instrument_id            BIGINT NOT NULL REFERENCES instruments(instrument_id),
    is_buy                   BOOLEAN NOT NULL,
    units                    NUMERIC(20, 8) NOT NULL,
    initial_units            NUMERIC(20, 8),           -- detect partial closes (isPartiallyAltered)
    amount                   NUMERIC(20, 4) NOT NULL,
    initial_amount_in_dollars NUMERIC(20, 4) NOT NULL, -- original investment (distinct from amount)
    open_rate                NUMERIC(20, 6) NOT NULL,
    open_conversion_rate     NUMERIC(20, 10) NOT NULL,
    open_date_time           TIMESTAMPTZ NOT NULL,
    stop_loss_rate           NUMERIC(20, 6),
    take_profit_rate         NUMERIC(20, 6),
    is_no_stop_loss          BOOLEAN NOT NULL DEFAULT TRUE,   -- "SL disabled" vs "SL rate is null"
    is_no_take_profit        BOOLEAN NOT NULL DEFAULT TRUE,   -- "TP disabled" vs "TP rate is null"
    leverage                 INTEGER NOT NULL DEFAULT 1,
    is_tsl_enabled           BOOLEAN NOT NULL DEFAULT FALSE,
    total_fees               NUMERIC(20, 4) NOT NULL DEFAULT 0,
    source                   TEXT NOT NULL DEFAULT 'broker_sync',  -- 'ebull' | 'broker_sync'
    raw_payload              JSONB NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX broker_positions_instrument_id_idx ON broker_positions (instrument_id);
```

**Columns added after Codex review:**
- `initial_amount_in_dollars` — the original investment amount, distinct from `amount` which includes margin adjustments. Needed for cost basis and tax-lot calculations.
- `initial_units` — detects partial closes when `units < initial_units` (maps to `isPartiallyAltered` in eToro payload).
- `is_no_stop_loss` / `is_no_take_profit` — eToro distinguishes "SL/TP disabled" from "SL/TP rate is null/zero". A position can have `stopLossRate = 0.0001` with `isNoStopLoss = true`. Without these flags, we'd misinterpret positions.

The existing `positions` table becomes a **materialised summary** — a denormalised per-instrument aggregate derived from `broker_positions`, kept for fast portfolio-level API responses. The sync function updates `broker_positions` first, then refreshes the aggregate `positions` table from it. The `positions.source` is derived: `'ebull'` if ANY constituent `broker_positions` row has `source = 'ebull'`, otherwise `'broker_sync'`.

---

## Safety invariant: positions are NEVER closed without explicit user action

This is a hard architectural constraint, not a convention.

**The only two code paths that may call `BrokerProvider.close_position()`:**
1. **Explicit operator close from the UI** — requires a confirmation token from the ClosePositionModal
2. **EXIT recommendation executed via execution guard** — requires a `recommendation_id` with `action=EXIT`

**Structurally enforced:**
- The `portfolio_sync` module must NEVER import or call `BrokerProvider.close_position()`. Sync observes reality; it does not cause changes.
- When a position disappears from the broker payload during sync, we DELETE the `broker_positions` row (or mark it closed) and log: "Position closed externally (eToro UI, SL/TP trigger, or manual). eBull did not initiate this close."
- The sync zeroing out `positions.current_units` for disappeared positions is correct — it reflects reality, not an action.
- Close+reopen is **never** acceptable as a workaround for SL/TP editing. It changes the positionId, resets fees, changes open price, and triggers a taxable event.
- Every `close_position` call must log: who initiated it, the recommendation_id or UI confirmation token, and the full broker response.

---

## Backend changes required

### Phase 1a — Per-position schema migration (backend only, no frontend change)

The schema migration is the foundation. All subsequent phases depend on per-position data.

1. **Create `broker_positions` table** (migration)
2. **Rewrite `_aggregate_by_instrument()`** to:
   - Upsert into `broker_positions` (one row per eToro positionID)
   - Then derive the `positions` summary from `broker_positions` via `INSERT ... ON CONFLICT` aggregating from `broker_positions`
3. **Backfill** — on first sync after migration, existing broker portfolio data populates `broker_positions`
4. **Update `order_client.py`** — when placing an order, write to `broker_positions` (not just `positions`)
5. **Change `BrokerProvider.close_position(instrument_id)` → `close_position(position_id)`** — with multiple positions per instrument, instrument-level close is ambiguous

### Phase 1b — Enrich portfolio API with per-position data

6. **Extend `/portfolio` response** to include individual `broker_positions` per instrument (nested under the per-instrument summary)
7. **Extend copy-trading detail** to include per-position SL/TP:
   - Add `stop_loss_rate`, `take_profit_rate`, `leverage` to `MirrorPositionItem`
   - Already in the `copy_mirror_positions` table — just add to the SELECT and model
8. **New endpoint: `GET /portfolio/positions/{instrument_id}`**
   - Returns enriched single-position detail (for the detail panel)
   - Includes: individual `broker_positions`, latest quote with bid/ask/spread, thesis summary, recommendation status

### Phase 2 — Order entry with SL/TP

7. **Update `broker.place_order()`** to accept optional SL/TP parameters
   - Remove hard-coded `IsNoStopLoss: true`, `IsNoTakeProfit: true`
   - Accept: `stop_loss_rate`, `take_profit_rate`, `is_tsl_enabled`

8. **`POST /portfolio/orders`** — Place a new order
   - Body: `{ instrument_id, action, amount?, units?, stop_loss_rate?, take_profit_rate?, is_tsl_enabled? }`
   - Calls `broker.place_order()`
   - Returns `BrokerOrderResult`
   - Guard: kill-switch check, live-trading flag check, amount validation

9. **`POST /portfolio/positions/{position_id}/close`** — Close a position (by positionId, not instrument)
   - Calls `broker.close_position()`
   - Optional: `units_to_deduct` for partial close
   - Guard: kill-switch check, confirmation token

### Phase 3 — SL/TP editing on existing positions

10. **Discover the edit endpoint** — manual API exploration against demo account
    - Likely: `PUT /api/v2/positions/{positionId}` with `{StopLossRate, TakeProfitRate}`
    - Evidence: `stopLossVersion` field increments in position payloads

11. **`broker.edit_position()`** — New method on `BrokerProvider` interface

12. **`PATCH /portfolio/positions/{position_id}/sl-tp`** — Frontend endpoint
    - Calls `broker.edit_position()`
    - Guard: kill-switch check

### Phase 4 — Limit orders

13. **`broker.place_limit_order()`** — eToro API: `POST /api/v2/pending-orders`
    - Market-if-touched order with trigger rate
    - Same SL/TP parameters as market orders

14. **`GET /portfolio/pending-orders`** — List pending orders
15. **`DELETE /portfolio/pending-orders/{order_id}`** — Cancel pending order

---

## Frontend implementation plan

### Phase 1 — Portfolio page & keyboard navigation (no new backend)

**Scope:** New Portfolio page, keyboard navigation hook, search/filter, pagination, position detail panel (read-only). Dashboard stripped down. Uses existing `/portfolio` endpoint data.

Files:
- `pages/PortfolioPage.tsx` — new page
- `components/portfolio/PositionsList.tsx` — paginated, searchable, keyboard-navigable table
- `components/portfolio/PositionDetail.tsx` — detail panel below table
- `components/portfolio/CopyTraderRow.tsx` — avatar row for mirror items
- `lib/useKeyboardNavigation.ts` — j/k/Enter/Esc/slash hook
- `layout/Sidebar.tsx` — add Portfolio nav item
- `App.tsx` — add /portfolio route
- `pages/DashboardPage.tsx` — remove PositionsTable, keep summary cards

Keyboard hook contract:
```typescript
function useKeyboardNavigation<T>(items: T[], options: {
  onSelect: (item: T) => void;
  onBack: () => void;
  enabled: boolean;
}): {
  focusedIndex: number;
  setFocusedIndex: (i: number) => void;
}
```

### Phase 2 — Order entry & position actions

**Scope:** Order entry modal with SL/TP, close position flow (full or partial), per-position detail view.

Files:
- `components/portfolio/OrderEntryModal.tsx` — buy/add form with SL/TP + preview→confirm
- `components/portfolio/ClosePositionModal.tsx` — confirmation dialog with optional partial close
- `components/portfolio/PositionDetail.tsx` — updated to show individual `broker_positions` per instrument
- `api/orders.ts` — API client for order endpoints
- Backend: `app/api/orders.py` — new router with order and close endpoints

### Phase 3 — SL/TP editing (blocked on API discovery)

**Scope:** Edit SL/TP on existing positions. Depends on discovering the internal eToro endpoint.

**API status:** The OpenAPI spec (v1.158.0) contains an orphaned `putTradeRequest` schema with `positionId`, `stopLossRate`, `takeProfitRate`, `isTrailingStopLoss` — but zero path endpoints reference it. The endpoint exists internally (eToro web app uses it) but is deliberately excluded from the public API.

**Discovery plan:**
1. Open eToro web app with DevTools Network tab
2. Edit SL/TP on a demo position
3. Capture the HTTP method, path, headers, and request body
4. Test the same call via our API key auth
5. If inaccessible via API key (some eToro endpoints require cookie auth), document as known limitation

**If accessible:**
- `components/portfolio/EditSlTpModal.tsx` — edit SL/TP on existing position
- Backend: `app/providers/etoro.py` — new `edit_position()` method
- Backend: `app/api/orders.py` — new `PATCH /portfolio/positions/{position_id}/sl-tp` endpoint

**If inaccessible:** Surface in UI: "SL/TP can be set when opening a position. To modify on existing positions, use the eToro app directly." Do NOT close+reopen as a workaround (see safety invariant above).

### Phase 4 — Limit orders & pending orders

**Scope:** Limit order support in order entry, pending orders list, cancel pending order.

Files:
- `components/portfolio/OrderEntryModal.tsx` — add market/limit toggle + rate field
- `components/portfolio/PendingOrdersList.tsx` — list with cancel action
- Backend: `app/providers/etoro.py` — new `place_limit_order()`, `list_pending_orders()`, `cancel_pending_order()` methods

---

## Visual design system

### Aesthetic: Utilitarian trading terminal

Not Bloomberg's information overload. Not eToro's marketing-forward consumer UI. The sweet spot: **dense, legible, functional**. A tool built for someone who uses it every day.

### Typography

- **Data/numbers:** `tabular-nums` (already used). Monospace-style rendering for financial data. Tight letter-spacing.
- **Labels:** Current sans-serif (Tailwind default). Uppercase for column headers (already used). Small, muted.
- **Headings:** Medium weight, not oversized. The data IS the content — headings are way-finding, not decoration.

### Colour system

Keep the existing palette but tighten the semantic usage:

| Role | Colour | Usage |
|------|--------|-------|
| Gain | `emerald-600` | P&L positive, price up |
| Loss | `red-600` | P&L negative, price down |
| Neutral | `slate-500` | Labels, dividers, secondary text |
| Focus | `blue-600` | Links, focused row highlight, active states |
| Copy trader | Deterministic avatar colour (already implemented) | Trader initials circle |
| Danger | `red-600` | Close position, sell actions |
| Surface | `white` / `slate-50` | Cards, expanded rows |

### Density

- Row height: 36px (current) — good for scanning
- Compact mode toggle in future for power users
- No wasted whitespace between sections — trading screens should feel packed
- Summary stats as a dense inline bar, not oversized cards

### Keyboard focus

- Focused row gets `ring-2 ring-blue-500 ring-inset` outline (visible, not just background change)
- Skip focus ring for mouse users (`:focus-visible` only)
- Shortcut hints shown as `kbd` badges in the UI: `j` `k` `Enter` `Esc` `/`

---

## What changes about copy-trading vs direct positions

| Aspect | Direct position | Copy-trading position |
|--------|----------------|----------------------|
| **Dashboard row** | Symbol + company | Avatar + trader name |
| **Drill-through** | Detail panel (inline, below table) | Navigate to `/copy-trading/:id` (sub-portfolio) |
| **Units** | Total units held | — (aggregate makes no sense) |
| **Invested** | cost_basis | funded (initial + deposits - withdrawals) |
| **Price** | Current quote price | — (multiple instruments) |
| **Value** | market_value | mirror_equity |
| **P&L** | unrealized_pnl | mirror_equity - funded |
| **SL/TP** | Per-position from `broker_positions` (Phase 1a). Set at creation (Phase 2). Edit TBD (Phase 3 — depends on API discovery). | Per-sub-position (read-only, set by copied trader) |
| **Actions** | Buy more (with SL/TP), Close (explicit only), Edit SL/TP (Phase 3) | View positions, (future: pause/stop copying) |
| **Who manages** | Operator | Copied trader (positions), Operator (mirror-level start/stop) |

---

## Implementation order (revised after Codex review)

**Key change:** Schema migration moves BEFORE the frontend portfolio page. Building the portfolio UI against the old aggregated `positions` table would create throwaway work — the detail panel needs `position_id`, SL/TP data, and per-position drill-through that only `broker_positions` provides. The API response shape changes after migration, so TypeScript types built against the old shape would need rewriting.

1. **Phase 1a** — `broker_positions` schema migration + sync rewrite (backend only)
2. **Phase 1b** — Enrich `/portfolio` API response with per-position data from `broker_positions` + copy-trading SL/TP
3. **Phase 1c** — Portfolio page scaffold + keyboard navigation + detail panel (frontend, built against final data model)
4. **Phase 1d** — Dashboard cleanup (remove positions table, keep summary cards)
5. **Phase 2a** — Update `place_order` to accept SL/TP + backend order/close endpoints
6. **Phase 2b** — Order entry modal with SL/TP + close position modal (frontend)
7. **Phase 3** — Discover & implement SL/TP editing on existing positions (blocked on API discovery — see Phase 3 notes above)
8. **Phase 4** — Limit orders (new broker method + order type toggle + pending orders list)

Each phase is a standalone PR.

---

## Resolved questions

1. **Direct position SL/TP:** → **(a) New `broker_positions` table** storing individual eToro positions, mirroring `copy_mirror_positions`. The eToro API confirms per-position SL/TP is the correct granularity. The `positions` table becomes a derived summary.

2. **Order entry scope:** → **(a) Support SL/TP at order creation.** The eToro API accepts `StopLossRate`, `TakeProfitRate`, `IsTslEnabled` on both market and limit orders. No reason to defer — the order form should include these from day one.

3. **Pagination:** 25 per page with keyboard-navigable page controls. Virtual scroll adds complexity without clear benefit when keyboard navigation already handles rapid traversal.

4. **Phasing order:** Schema migration first, then frontend. Confirmed by Codex review — building portfolio UI against the pre-migration data model creates throwaway work since the API response shape and available fields change after migration.

## Remaining open questions

1. **Edit SL/TP endpoint:** The eToro public API (v1.158.0) contains an orphaned `putTradeRequest` schema (`positionId`, `stopLossRate`, `takeProfitRate`, `isTrailingStopLoss`) referenced by zero path endpoints. The endpoint exists internally but is deliberately excluded from the public API. Discovery plan: capture the network call from the eToro web app DevTools when editing SL/TP, then test via API key auth. If inaccessible via API key, document as known limitation — do NOT close+reopen as a workaround.

2. **`putTradeRequest` as leverage for eToro request:** The existence of the orphaned schema in the public spec makes a strong case for requesting eToro expose this endpoint. Worth pursuing if we have any developer relations channel.
