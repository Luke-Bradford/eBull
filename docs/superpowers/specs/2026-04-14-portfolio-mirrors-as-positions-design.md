# Portfolio Mirrors as Positions — Design Spec

## Problem

Copy-trading mirrors are currently shown on a standalone `/copy-trading` page, separate from the main dashboard. This doesn't match eToro's model, where each mirror appears as a line item in the portfolio alongside direct positions. The operator has to navigate to a different page to see copy-trading data, and the dashboard positions table gives an incomplete picture of the portfolio.

## Goal

Integrate copy-trading mirrors into the dashboard portfolio view so they behave like any other position. Each mirror appears as a row in the positions table. Clicking drills into the mirror's component positions. Remove the standalone Copy Trading page.

## Design

### How eToro models this

In eToro's portfolio:
- Each mirror appears as a **single aggregate row** alongside direct stock positions
- The row shows: trader username, total equity, P&L
- Clicking drills into the mirror's component positions (individual stocks held by the copied trader)
- Clicking a component position shows the unit purchases / lots
- Mirrors contribute to overall portfolio equity, P&L, and AUM

### Backend changes

#### Extend `PortfolioResponse` with mirror items

Add a `mirrors` list to the existing `/portfolio` response. Each mirror is represented as a `PortfolioMirrorItem`:

```python
class PortfolioMirrorItem(BaseModel):
    mirror_id: int
    parent_username: str
    active: bool
    funded: float           # initial_investment + deposits - withdrawals (display currency)
    mirror_equity: float    # available_amount + sum(position market values) (display currency)
    unrealized_pnl: float   # mirror_equity - funded (display currency)
    position_count: int
    started_copy_date: datetime
```

The existing `PortfolioResponse` gains:

```python
class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    mirrors: list[PortfolioMirrorItem]   # NEW
    position_count: int
    total_aum: float
    cash_balance: float | None
    mirror_equity: float
    display_currency: str
    fx_rates_used: dict[str, dict[str, object]]
```

The `mirror_equity` field (already present) stays as the total across all mirrors. The `mirrors` list provides the per-mirror breakdown.

#### Mirror equity calculation

Reuse the existing logic from `app/api/copy_trading.py:_compute_position_mtm()` and `_convert_usd()`. The per-mirror equity is: `available_amount_display + sum(position_market_values_display)`.

The funded amount is: `initial_investment + deposit_summary - withdrawal_summary`, all converted to display currency.

P&L is: `mirror_equity - funded`.

#### Where the data comes from

The `/portfolio` endpoint already calls `_load_mirror_equity(conn)` to get the total. Extend this to also return per-mirror breakdowns. The queries are already written in `app/api/copy_trading.py` — extract the shared logic into a service function that both endpoints can use.

### Frontend changes

#### Unified positions table

The `PositionsTable` component currently renders `PositionItem[]`. Extend it to accept mirrors and render them in the same table, sorted together by market_value descending.

Mirror rows differ from position rows:
- **Symbol column**: trader username (e.g., "thomaspj") with a small "Copy" badge
- **Company column**: position count (e.g., "198 positions")
- **Units column**: "—" (not applicable)
- **Avg cost column**: funded amount
- **Price column**: "—" (not applicable)
- **Market value column**: mirror equity
- **P&L column**: mirror_equity - funded, with emerald/red colouring

Mirror rows link to `/copy-trading/:mirrorId` for drill-down (existing component positions table).

#### Summary cards

`SummaryCards` currently sums `unrealized_pnl` from positions only. Include mirror P&L in the total:
- Total unrealized P&L = sum(position.unrealized_pnl) + sum(mirror.unrealized_pnl)
- Total cost basis for percentage = sum(position.cost_basis) + sum(mirror.funded)

#### TypeScript types

Add `PortfolioMirrorItem` interface to `frontend/src/api/types.ts`:

```typescript
export interface PortfolioMirrorItem {
  mirror_id: number;
  parent_username: string;
  active: boolean;
  funded: number;
  mirror_equity: number;
  unrealized_pnl: number;
  position_count: number;
  started_copy_date: string;
}
```

Extend `PortfolioResponse`:

```typescript
export interface PortfolioResponse {
  positions: PositionItem[];
  mirrors: PortfolioMirrorItem[];  // NEW
  // ... existing fields unchanged
}
```

### Mirror detail view (drill-down)

#### Route change

The current `CopyTradingPage` at `/copy-trading` becomes a detail view at `/copy-trading/:mirrorId`. It shows:
- Mirror stats (initial investment, deposits, withdrawals, available cash, closed P&L, copying since)
- Component positions table (existing `MirrorPositionsTable`)

The standalone browsing page and sidebar link are removed. The only way to reach the mirror detail is by clicking a mirror row in the dashboard positions table.

#### Backend for detail view

The existing `GET /portfolio/copy-trading` endpoint can be narrowed to serve a single mirror's data, or a new `GET /portfolio/copy-trading/:mirrorId` endpoint can be added. The latter is cleaner — it avoids loading all mirrors when only one is needed.

### What gets removed

- Sidebar nav item: "Copy Trading" (from `Sidebar.tsx`)
- Route: `/copy-trading` top-level browsing page (from `App.tsx`)
- Components: `MirrorEquitySummary`, `ActiveTraders`, `ClosedMirrors` from `CopyTradingPage.tsx`
- The `CopyTradingPage` component is rewritten as `MirrorDetailPage` (single-mirror drill-down)

### What stays

- `GET /portfolio/copy-trading` endpoint — still useful, now also serves the dashboard's mirror data
- `MirrorPositionsTable`, `MirrorPositionRow`, `MirrorStats` components — reused in the detail view
- `TraderMirrorCard` — removed (replaced by table rows)
- All backend MTM computation logic — unchanged
- All test coverage for MTM computation — unchanged

## Non-goals

- Showing individual mirror positions in the main dashboard table (too noisy — 368 rows would dwarf the 5 direct positions)
- Unit purchase / lot-level drill-down (Track 2)
- Closed mirrors in the dashboard (only active mirrors appear as position rows; closed mirror history is a future feature)
- Modifying AUM calculation (already correct — includes mirror_equity)

## Testing

### Backend
- Extend existing portfolio endpoint tests to verify `mirrors` field is populated
- Test mirror P&L calculation: `mirror_equity - funded`
- Test that mirror_equity in the response equals sum of individual mirror equities

### Frontend
- Extend `DashboardPage` tests to verify mirror rows appear in positions table
- Test mirror row rendering: username, position count, equity, P&L
- Test SummaryCards includes mirror P&L in unrealized total
- Test mirror row links to `/copy-trading/:mirrorId`
- Test MirrorDetailPage renders for a specific mirror
- Test empty state: no mirrors = no mirror rows, table still works

## Files affected

### Backend
- `app/api/portfolio.py` — add `PortfolioMirrorItem` model, populate `mirrors` list
- `app/services/portfolio.py` — extract shared mirror equity logic into reusable function

### Frontend
- `frontend/src/api/types.ts` — add `PortfolioMirrorItem`, extend `PortfolioResponse`
- `frontend/src/components/dashboard/PositionsTable.tsx` — render mirror rows
- `frontend/src/components/dashboard/SummaryCards.tsx` — include mirror P&L
- `frontend/src/pages/CopyTradingPage.tsx` — rewrite as `MirrorDetailPage`
- `frontend/src/pages/CopyTradingPage.test.tsx` — update tests
- `frontend/src/App.tsx` — change route from `/copy-trading` to `/copy-trading/:mirrorId`
- `frontend/src/layout/Sidebar.tsx` — remove Copy Trading nav item
- `frontend/src/api/copyTrading.ts` — add `fetchMirrorDetail(mirrorId)` fetcher

### Tests
- `frontend/src/pages/CopyTradingPage.test.tsx` — rewrite for detail view
- Backend portfolio endpoint test — extend for mirrors
