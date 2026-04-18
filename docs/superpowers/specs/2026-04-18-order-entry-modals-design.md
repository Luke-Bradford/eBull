# P0-1 order entry + close modals — design spec

**Issue:** [#313](https://github.com/Luke-Bradford/eBull/issues/313)
**Plan:** `docs/superpowers/plans/2026-04-18-product-visibility-pivot.md`
**Size:** S-M (~1 day, frontend only)
**Depends on:** nothing (backend endpoints ship-ready)
**Blocks:** #314 (portfolio workstation consumes these modals), #316 (instrument terminal consumes these modals)

---

## 1. Goal

Expose two existing backend endpoints via frontend modals so the operator can add to a held position and close (full or partial) a specific broker position from the Portfolio page, in demo mode, using honest native-currency previews.

**Vision-check:** yes — operator goes from "looks at positions, can't act" to "adds to a holding and closes a trade end-to-end from the UI".

## 2. Backend contract (pinned)

Both endpoints already exist and are protected by `require_session_or_service_token` (cookie auth).

### 2.1 `POST /portfolio/orders` — `app/api/orders.py:405`

```python
# PlaceOrderRequest, app/api/orders.py:52-60
instrument_id: int
action: Literal["BUY", "ADD"]
amount: float | None = None           # native-currency notional
units: float | None = None
stop_loss_rate: float | None = None
take_profit_rate: float | None = None
is_tsl_enabled: bool = False
leverage: int = 1
```

Response (`OrderResponse`, `app/api/orders.py:67-74`):
```python
order_id: int
status: str                           # "filled" | "pending" | "failed"
broker_order_ref: str | None
filled_price: float | None
filled_units: float | None
fees: float
explanation: str
```

Error shapes (all `detail` strings are fixed phrases per prevention #86 / #89 — surface verbatim):
- `403` — `"Kill switch is active: {reason}"`
- `400` — validation (amount+units both provided, non-positive, etc.)
- `422` — `"No quote available for instrument {id} — cannot fill without a price."`
- `501` — `"Live trading not yet wired — use demo mode."`

**Price source used by the endpoint:** `_load_latest_quote_price` (`app/api/orders.py:136-149`) reads `quotes.last` only — no display-currency conversion, no `price_daily` fallback. If that row is missing or null, the endpoint 422s.

### 2.2 `POST /portfolio/positions/{position_id}/close` — `app/api/orders.py:473`

```python
# ClosePositionRequest, app/api/orders.py:63-64
units_to_deduct: float | None = None   # None = close entire position
```

Same `OrderResponse` shape. Errors:
- `403` — kill switch
- `404` — `"Position {pid} not found or already closed."`
- `400` — `units_to_deduct must be positive` or `exceeds position units`
- `501` — live trading

**Price source:** same raw `quotes.last`, and if null falls back to that broker position's `open_rate` (`app/api/orders.py:511-512`) — a cost-basis fill. Frontend preview must mirror this fallback.

## 3. Non-goals (drop aggressively)

- No new backend endpoints. Entire PR is frontend.
- No LIVE trading UI in this PR. `enable_live_trading` returns 501; we show the fixed phrase verbatim if an operator ever triggers the button while the flag is on. We do NOT prebuild a confirm-twice live-order gate — that's a future PR against the live-wiring branch. Rationale: dead code in a safety path is worse than no code; we ship the DEMO path only.
- No stop-loss / take-profit / TSL / leverage UI. Request body sends `null, null, false, 1`.
- No orders-list / order-history panel. Proof of fill = `portfolio.refetch()` on success; updated units appear in the Portfolio table row.
- No tax-impact preview. Plan mentioned it but no backend endpoint computes it and duplicating the Python tax engine on the frontend is off-limits. We show "Estimated realized P&L" using native-currency `current_price - open_rate` instead — that's what the operator actually reads. If tax-impact is wanted later, ship a `POST /portfolio/positions/{id}/close/preview` endpoint and reconsult this spec.
- No new-instrument BUY flow (instrument picker / search-then-buy). Every entry button on the Portfolio page triggers `ADD` (existing position). `BUY` as an action value exists in the type surface because the backend accepts it, but in this PR nothing in the UI submits `action: "BUY"`. The new-instrument BUY launcher ships with #316 (instrument terminal).
- No per-instrument multi-trade close UI. When a `PositionItem` aggregates multiple broker positions (`trades.length > 1`), the Portfolio page does not expose a Close button in this PR. The detail panel in #314 will present per-broker-position rows and their own Close buttons. This is the only correct close granularity in v1: the endpoint operates on `broker_positions.position_id`, not on aggregated instrument positions.
- No keyboard shortcuts beyond what `Modal` already provides (Esc + Tab focus trap). Global `/`, `j/k`, `b`, `c` keys belong to #314.

## 4. File plan

### New files

- `frontend/src/api/orders.ts` — two fetchers.
- `frontend/src/components/orders/OrderEntryModal.tsx` — ADD flow (BUY remains in the type surface for future reuse).
- `frontend/src/components/orders/ClosePositionModal.tsx` — full / partial close.
- `frontend/src/components/orders/OrderEntryModal.test.tsx`
- `frontend/src/components/orders/ClosePositionModal.test.tsx`
- `frontend/src/api/orders.test.ts` — fetcher contract tests.

### Modified files

- `frontend/src/api/types.ts` — add request and response shapes mirroring `PlaceOrderRequest`, `ClosePositionRequest`, `OrderResponse`.
- `frontend/src/pages/PortfolioPage.tsx` — add Actions column with an `Add` button (always) and a `Close` button (only when `trades.length === 1`).

Total ~7 files, ~650 LoC including tests.

## 5. API client (`frontend/src/api/orders.ts`)

```ts
import { apiFetch } from "@/api/client";
import type {
  PlaceOrderRequest,
  ClosePositionRequest,
  OrderResponse,
} from "@/api/types";

export function placeOrder(body: PlaceOrderRequest): Promise<OrderResponse> {
  return apiFetch<OrderResponse>("/portfolio/orders", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function closePosition(
  positionId: number,
  body: ClosePositionRequest,
): Promise<OrderResponse> {
  return apiFetch<OrderResponse>(
    `/portfolio/positions/${positionId}/close`,
    { method: "POST", body: JSON.stringify(body) },
  );
}
```

Both fetchers always send a body (never omit it). Full close is `{units_to_deduct: null}` — we do NOT rely on HTTP-body absence to signal full close.

**No business logic.** Typed wrapper only. Errors bubble as `ApiError(status, message)` from `apiFetch`.

## 6. Type additions (`frontend/src/api/types.ts`)

```ts
// ---------------------------------------------------------------------------
// /portfolio/orders, /portfolio/positions/{id}/close (app/api/orders.py)
// ---------------------------------------------------------------------------

export type OrderAction = "BUY" | "ADD";

export interface PlaceOrderRequest {
  instrument_id: number;
  action: OrderAction;
  amount: number | null;
  units: number | null;
  stop_loss_rate: number | null;
  take_profit_rate: number | null;
  is_tsl_enabled: boolean;
  leverage: number;
}

export interface ClosePositionRequest {
  units_to_deduct: number | null;
}

export interface OrderResponse {
  order_id: number;
  status: string;
  broker_order_ref: string | null;
  filled_price: number | null;
  filled_units: number | null;
  fees: number;
  explanation: string;
}
```

Drift pin: any change to `app/api/orders.py` models must update this section in the same PR.

## 7. Preview-data strategy — honest previews only

### Problem pinned by Codex

`PortfolioPage` row data (`PositionItem.current_price`) is in **display currency** and may fall back to `price_daily.close` when no quote exists (`app/api/portfolio.py:187-211`). The order endpoints use raw **native-currency** `quotes.last` with no fallback. If we preview using row data, the operator can see "£140" but the fill goes in at native USD rate, or see a stale daily-close when the endpoint will 422.

### Resolution

Every modal fetches `GET /portfolio/instruments/{instrument_id}` via the existing `fetchInstrumentPositions` (`frontend/src/api/portfolio.ts:8`) on open. This returns `InstrumentPositionDetail` with:

- `currency` — native currency of the instrument
- `current_price` — native-currency price (same source as the order endpoint for preview honesty; still a lightweight summary, may include daily_close fallback — see §7.4)
- `trades: NativeTradeItem[]` — per-broker-position rows with native `open_rate` and `current_price`

Modal UI then shows:

- All money in the instrument's native currency, explicitly labeled (`USD 140.12`, not the display-currency aware `formatMoney` helper).
- A one-line disclaimer: `"Preview uses the latest available quote; the actual fill uses the most recent quote at submission time."`

### 7.4 `current_price` caveats — both endpoints have them

`fetchInstrumentPositions` reuses the portfolio service layer, so its `current_price` may fall through to `price_daily.close` when `quotes.last` is null (`PositionItem.valuation_source` is `"quote" | "daily_close" | "cost_basis"`). Neither order endpoint has that fallback behaviour:

- **Entry** (`POST /portfolio/orders`): uses raw `quotes.last` only. If null, 422.
- **Close** (`POST /portfolio/positions/{id}/close`): uses raw `quotes.last`, and if null falls through to the broker position's own `open_rate` — i.e. a near-zero-P&L cost-basis fill (`app/api/orders.py:511-512`).

So the client cannot promise a truly honest preview without a dedicated endpoint. Mitigations in this PR:

- The preview disclaimer states plainly: `"Preview uses your latest known portfolio price. At submission time the fill may use a different quote, or (for close) fall back to your open rate if no quote is available — realized P&L may be 0 in that case."`
- Both modals read `valuation_source` on the `PositionItem` passed in by the caller (or on the aggregated summary returned by `fetchInstrumentPositions`). When `valuation_source !== "quote"`, render the price in amber with `"(may not reflect fill price)"` next to it.
- For the close modal specifically: always display BOTH `current_price` and `open_rate` side by side. The P&L estimate uses `current_price` when present, `open_rate` otherwise. A caption under the P&L line states which source was used.
- 422 errors from entry surface verbatim.

Tracked as tech-debt alongside this PR: `GET /portfolio/instruments/{iid}/quote-for-order` returning raw `quotes.last` + native currency, so modals can mirror the order endpoints' price source exactly. Reference in PR description.

## 8. Component contracts

### 8.1 `OrderEntryModal` (ADD flow)

**Props:**
```ts
interface OrderEntryModalProps {
  readonly isOpen: boolean;
  readonly instrumentId: number;
  readonly symbol: string;
  readonly companyName: string;
  readonly valuationSource: "quote" | "daily_close" | "cost_basis";
  readonly onRequestClose: () => void;
  readonly onFilled: () => void;
}
```

**Mounting contract:** the parent only renders this component when the target is non-null (§10), so the component is mounted exactly once per "open". `useAsync(() => fetchInstrumentPositions(instrumentId), [instrumentId])` therefore fires once on mount and will not fetch while the modal is closed. `isOpen` is kept in the prop surface for test ergonomics but conditional mount is the source of truth.

**On open:** the `useAsync` call loads native-currency context. Until it resolves, the form shows a skeleton. If it errors, the form shows the error text + a Retry button and the submit is disabled.

**Layout:**
- Demo/live pill (see §9).
- Heading: `Add — {symbol}`.
- Native-currency context line: `Currency: USD · Latest price: 140.12 (quote)` — the parenthesised source is the `valuation_source` value from the fetched `PositionItem`. When `valuation_source !== "quote"`, render the price + source in amber with `"(may not reflect fill price)"` next to it. Dash if null.
- Amount/Units toggle + numeric input.
- Preview block:
  - `Estimated units: {amount / currentPrice}` (if price > 0 and amount entered)
  - `Estimated cost: {units * currentPrice}` (if price > 0 and units entered)
  - `Estimated fees: 0.00 (demo)`
- Preview disclaimer (from §7.4): `"Preview uses your latest known portfolio price. At submission time the fill may use a different quote — if no quote is available the backend returns 422 and no order is placed."`
- Submit button: `Place demo order`. Disabled while `submitting`, while the instrument-detail fetch is in flight, or when the input is invalid.
- Error slot (inside the modal, below the button).

**Lifecycle (pinned):**
1. `idle` — default. Submit enabled iff input is valid and preview fetch resolved.
2. `submitting` — submit disabled, loading spinner on button. User can still Cancel (Esc).
3. On success: call `onFilled()` then `onRequestClose()`. **No intermediate filled view.** The operator sees the portfolio refetch update the row — that is the fill confirmation. Closing first, then refetching, matches prevention #125 (refresh-error-swallowed-by-overlay).
4. On error: stay in `idle`, show the error text, input remains editable, submit re-enabled.

**Client-side validation before POST:**
- Exactly one of `amount`, `units` is numeric-valid: `typeof v === "number" && Number.isFinite(v) && v > 0`. `Number.isFinite` rejects `NaN`, `+Infinity`, `-Infinity` (prevention #236).
- No upper-bound check — the backend has none either. A legitimately huge finite value is the backend's problem.
- Both fields null or both set → submit is disabled (radio forces mutual exclusion, so this is belt-and-braces).

**Error surfacing:**
- `ApiError` → surface `error.message` verbatim. Note: `ApiError.message` **is** the backend's `detail` string (client.ts:66-73 sets `message = body.detail` on non-OK responses). The spec used `detail` loosely in v1; v2 uses `error.message` throughout.
- Non-`ApiError` (network) → fixed phrase `"Network error — check connection and try again."`

**Payload:**
```ts
{
  instrument_id: props.instrumentId,
  action: "ADD",                     // always ADD in this PR
  amount: amountIfChosen ?? null,
  units: unitsIfChosen ?? null,
  stop_loss_rate: null,
  take_profit_rate: null,
  is_tsl_enabled: false,
  leverage: 1,
}
```

### 8.2 `ClosePositionModal`

**Props:**
```ts
interface ClosePositionModalProps {
  readonly isOpen: boolean;
  readonly instrumentId: number;
  readonly positionId: number;       // broker_positions.position_id
  readonly valuationSource: "quote" | "daily_close" | "cost_basis";
  readonly onRequestClose: () => void;
  readonly onFilled: () => void;
}
```

**Mounting contract:** same as §8.1 — only mounted when `closeFor !== null` in the parent, so `useAsync` fires once per open.

**On open:** `fetchInstrumentPositions(instrumentId)` call. Find the `NativeTradeItem` matching `positionId` in the response's `trades[]`. If absent (stale, concurrent close, etc.), show a fixed error `"This position no longer exists — refresh the portfolio."` and disable submit.

**Layout:**
- Demo/live pill.
- Heading: `Close — {symbol}`.
- Info strip: `{units} units @ {open_rate} {currency} · Latest price: {current_price} {currency}`. If the summary-level `valuation_source !== "quote"`, the latest-price cell renders in amber with `"(may not reflect fill price)"`.
- Close-mode radio:
  - `Full close` — no further input. Payload: `{units_to_deduct: null}`.
  - `Partial close` — reveals a numeric input + range slider.
- Partial-close input:
  - Numeric input, `min=0.000001`, `max=units`, `step=0.000001` (matches backend's `Decimal.quantize(Decimal("0.000001"))` at `app/api/orders.py:115`).
  - Slider is a visual pair with the numeric input — either updates both.
  - Validation: `Number.isFinite(v) && v > 0 && v <= units`.
- Preview block (in native currency, using the matched `NativeTradeItem`):
  - `Closing: {units_to_close} / {units} units`
  - `Open rate: {open_rate} {currency}` (always shown — see §7.4)
  - `Est. fill price: {current_price} {currency}` — or `—` if null.
  - `Est. realized P&L: {(est_fill_price - open_rate) * units_to_close} {currency}` — colored green/red. When `current_price` is null, `est_fill_price = open_rate` (matches backend fallback `app/api/orders.py:511-512`), producing a P&L estimate of 0. Caption under this line: either `"using latest quote"` or `"using open rate — no quote available; realized P&L will be ~0"`.
- Preview disclaimer: same as §8.1 disclaimer, adapted: `"Preview uses your latest known portfolio price. At submission time the fill may use a different quote — if no quote is available the fill uses your open rate and realized P&L may be 0."`
- Submit button: `Close position`.

**Full-close discrimination:** driven by the radio's `mode` state, not by a float comparison against `units`. Full close always sends `{units_to_deduct: null}`; partial close always sends `{units_to_deduct: <number>}`. No float equality involved.

**Lifecycle and error handling:** identical to OrderEntryModal.

### 8.3 Modal shell

Both components wrap `Modal` from `frontend/src/components/ui/Modal.tsx`, with `label=` (string). Esc routes through `onRequestClose` — we do NOT gate cancel via confirm-cancel (this is not a fail-closed secret flow).

### 8.4 Unmounted setState safety

Each modal holds `mountedRef` and `submittingRef` per prevention #127. Every `setState` after an `await` goes through a helper that early-returns when `!mountedRef.current`.

## 9. Demo / live indicator

Read `enable_live_trading` from the existing `/config` source `AppShell` already consumes. Follow `safety-state-ui.md` literally — cache the last confirmed snapshot and mark stale when rendering from cache.

```ts
// Fresh source
const liveFlag: boolean | null = config.data?.runtime.enable_live_trading ?? null;

// Cache (per component)
const [cached, setCached] = useState<boolean | null>(null);
useEffect(() => {
  if (liveFlag !== null) setCached(liveFlag);
  // Never write null into the cache. Errors and loading leave it untouched.
}, [liveFlag]);

// Display: prefer fresh; fall back to cache with stale marker.
const isLive = liveFlag ?? cached ?? false;
const fresh = liveFlag !== null;

return (
  <span className={isLive ? "pill-red" : "pill-demo"}>
    {isLive ? "LIVE" : "DEMO MODE"}
    {!fresh && cached !== null ? (
      <span className="ml-1 text-[10px] uppercase text-amber-600">(stale)</span>
    ) : null}
  </span>
);
```

Contract: cache only updates on non-null fresh values (both `true` and `false` are confirmed values). A fresh `false` does overwrite a cached `true` — that is correct `safety-state-ui.md` semantics: the cache mirrors the latest confirmed state, and the display OR-mechanic is `fresh ?? cached`, not `fresh || cached`. On cold start, both are null and the pill shows `DEMO MODE` without a stale marker (acceptable per `safety-state-ui.md:79` cold-start rule).

**Submit behaviour:** if `isLive === true`, the submit button still calls `placeOrder` but the backend 501s; we surface the 501 text verbatim. No confirm-twice gate in this PR (§3).

Tests pin: pill renders correctly when fresh is null but cache has a value; stale marker renders; cache is not clobbered by a transient null from `/config` refetch.

## 10. PortfolioPage integration

Add an Actions column as the last `<th>` / `<td>`.

```tsx
<td className="px-2 py-2 text-right">
  <button
    type="button"
    onClick={(e) => { e.stopPropagation(); setAddFor(p); }}
    aria-label={`Add to ${p.symbol}`}
  >Add</button>
  {p.trades.length === 1 ? (
    <button
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        setCloseFor({
          instrumentId: p.instrument_id,
          trade: p.trades[0]!,
          valuationSource: p.valuation_source,
        });
      }}
      aria-label={`Close ${p.symbol}`}
    >Close</button>
  ) : null}
</td>
```

Multi-trade positions (`trades.length > 1`) render no Close button — §3 makes this explicit.

Page-level state carries both pieces of identity — `BrokerPositionItem` lacks `instrument_id`, so we store it alongside:

```ts
interface CloseTarget {
  instrumentId: number;
  trade: BrokerPositionItem;
  valuationSource: "quote" | "daily_close" | "cost_basis";
}

const [addFor, setAddFor] = useState<PositionItem | null>(null);
const [closeFor, setCloseFor] = useState<CloseTarget | null>(null);

function handleFilled() {
  setAddFor(null);
  setCloseFor(null);
  portfolio.refetch();
}
```

**Modal mounting — gate fetch on open:** both modals are mounted only when their state slot is non-null. This makes `useAsync(() => fetchInstrumentPositions(instrumentId), [instrumentId])` fire exactly once per open, and avoids fetching while closed. Unmount on close also resets modal internal state cleanly.

```tsx
{addFor !== null ? (
  <OrderEntryModal
    isOpen
    instrumentId={addFor.instrument_id}
    symbol={addFor.symbol}
    companyName={addFor.company_name}
    valuationSource={addFor.valuation_source}
    onRequestClose={() => setAddFor(null)}
    onFilled={handleFilled}
  />
) : null}

{closeFor !== null ? (
  <ClosePositionModal
    isOpen
    instrumentId={closeFor.instrumentId}
    positionId={closeFor.trade.position_id}
    valuationSource={closeFor.valuationSource}
    onRequestClose={() => setCloseFor(null)}
    onFilled={handleFilled}
  />
) : null}
```

Because the modal is only rendered when the slot is non-null, `isOpen` is always `true` inside — kept as a prop for symmetry with the existing `Modal` shell API and for test setups that want to render with `isOpen={false}`. `handleFilled` closes before refetch (prevention #125).

The Mirror rows (`MirrorRow`) do not get Actions buttons. Copy trading is a separate execution flow.

## 11. Test plan

### 11.1 `api/orders.test.ts`

- `placeOrder` POSTs correct JSON; returns parsed `OrderResponse`.
- `placeOrder` throws `ApiError(403, "Kill switch is active: ...")` on 403.
- `closePosition` sends `{units_to_deduct: null}` body for full close.
- `closePosition` sends `{units_to_deduct: 2.5}` body for partial close.
- `closePosition` URL correctly interpolates `positionId`.

### 11.2 `OrderEntryModal.test.tsx`

- Happy path: mock `fetchInstrumentPositions` + `placeOrder`; type amount, submit, `onFilled` then `onRequestClose` called in that order.
- Instrument-detail error: renders error + Retry, submit disabled.
- Validation: `Infinity` rejected by isFinite guard; `NaN` rejected; negative rejected.
- Validation: neither amount nor units → submit disabled.
- Guard rejection: mock throws `ApiError(403, "Kill switch is active: drawdown breach")` → exact message shown via `error.message`.
- Broker 422: `"No quote available for instrument 12..."` surfaced verbatim.
- Network error: non-`ApiError` thrown → fixed phrase.
- Unmount-during-submit: start submission, unmount before resolution — no act() warning, no unhandled rejection (prevention #127).
- Fetch-on-open gating: with the modal unmounted (parent does not render it), `fetchInstrumentPositions` is not called. Mounting triggers exactly one fetch.
- Price-source flag: when `valuationSource="daily_close"`, the price is rendered in amber with `"(may not reflect fill price)"`. When `valuationSource="quote"`, no amber treatment.
- Safety pill: renders `DEMO MODE` when `enable_live_trading=false`. When the `/config` fetch subsequently returns null (loading) the pill stays visible with a `(stale)` marker — the cached boolean is preserved.

### 11.3 `ClosePositionModal.test.tsx`

- Full-close radio → payload `{units_to_deduct: null}`.
- Partial-close numeric input with fractional value (e.g. `0.5`) → payload `{units_to_deduct: 0.5}`. Verifies sub-unit closes work.
- Input > units → submit disabled + inline validation message.
- `current_price=null` → preview uses `open_rate` as the fill-price estimate; P&L shows 0; caption reads `"using open rate — no quote available; realized P&L will be ~0"`.
- `valuationSource="daily_close"`: latest-price cell rendered amber with caveat string.
- Stale broker position: `fetchInstrumentPositions` returns `trades` without the target `positionId` → fixed error + submit disabled.
- 404 path: `"Position X not found or already closed."` surfaced.
- 403 path: same as entry modal.
- Unmount-during-submit: same guard test.

### 11.4 PortfolioPage

- If `PortfolioPage.test.tsx` exists: verify Actions column renders Add for all rows and Close only when `trades.length === 1`; verify row-click navigation still fires when clicking outside action buttons; verify Add button opens OrderEntryModal with correct `instrumentId`.
- If the test file does NOT exist: do not introduce it in this PR. #314 will create it.

## 12. Settled-decisions alignment

- **Demo-first:** DEMO pill by default; backend-enforced; LIVE path stubbed but surfaces 501 verbatim.
- **Long-only, no leverage, no shorting:** `action` restricted to `"BUY" | "ADD"`; `leverage: 1` always; no SHORT.
- **Auditable:** endpoints already persist order + fill + cash_ledger + decision_audit (`app/api/orders.py:152-395`); frontend does not touch persistence.
- **Close-position safety invariant** (`app/api/orders.py:14-17`): operator UI is the allowed caller. Preserved.
- **Product-visibility pivot** (`docs/settled-decisions.md:298`): this PR is P0-1, the first in the sequence.

## 13. Prevention-log alignment

| Entry | Honored by |
|---|---|
| #127 API shape invented at boundary | §6 mirrors Pydantic field-for-field; PR description names `app/api/orders.py` |
| #127 Unmounted setState in async submit | §8.4 pins `mountedRef` + `safeSetState` |
| #125 Refresh swallowed by overlay | §10 closes modal before `portfolio.refetch()` |
| #147 Falsy-string suppression | fetcher sends the object verbatim; comparisons use `!== null` |
| #236 Literal types on API fields | §6 `OrderAction = "BUY" \| "ADD"` |
| #236 `isFinite` numeric guard | §8.1 + §8.2 validation |
| safety-state-ui.md | §9 cached pill |
| async-data-loading.md | §10 closes modal before refetch; modal holds its own error slot |
| api-shape-and-types.md | §5 typed wrapper; pages call fetcher, never `apiFetch` |

## 14. Rollout and verification

Before first push:
1. `pnpm --dir frontend typecheck` — pass
2. `pnpm --dir frontend test` — pass
3. `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`, `uv run pytest` — pass (no backend changes, but run to confirm no accidental breakage)

Browser verification (CLAUDE.md mandatory):
4. Dev stack running via VS Code task. Navigate to `/portfolio`.
5. Click `Add` on a held position. Enter an amount that works with the native price. Submit. Confirm: modal closes, portfolio refetches, row units increase.
6. With a single-trade position: click `Close`, pick `Partial`, enter `0.5` units. Submit. Confirm: modal closes, refetch, row units decrease by 0.5.
7. With the kill switch active (Admin toggle): confirm both Add and Close surface `"Kill switch is active: ..."` verbatim.
8. With an instrument whose latest `quotes.last` is null: confirm `Add` preview disclaimer fires, submit still triggers, backend 422 surfaced verbatim.

Manual browser verification is mandatory before merge.

## 15. Open questions

None that block implementation.

Tech-debt to file in the same PR description (not as blockers):
- `GET /portfolio/instruments/{iid}/quote-for-order` returning raw `quotes.last` only, so the order-entry preview can match the execution price source exactly (§7.4 residual gap).
- Tax-lot close-preview endpoint (§3 non-goal).

## 16. Reviewer cheat-sheet

- [ ] `frontend/src/api/types.ts` additions match `app/api/orders.py` field-for-field and nullability-for-nullability.
- [ ] `api/orders.ts` is a typed wrapper only. `closePosition` returns `Promise<OrderResponse>`.
- [ ] Both modals fetch `fetchInstrumentPositions` on open for native-currency preview data.
- [ ] Error surfacing reads `error.message` (not a non-existent `error.detail`).
- [ ] ClosePositionModal full-close uses a `mode` radio, not float equality against `units`; payload is `{units_to_deduct: null}` for full, numeric for partial.
- [ ] Partial-close input supports sub-unit values (`min=0.000001`, `step=0.000001`).
- [ ] Close button is suppressed on PortfolioPage when `trades.length > 1`.
- [ ] Every entry submission in this PR sends `action: "ADD"` (no `"BUY"` submit path exposed — BUY ships with #316).
- [ ] `handleFilled` closes the modal BEFORE `portfolio.refetch()`.
- [ ] `mountedRef` / `safeSetState` guard every post-`await` `setState` in both modals.
- [ ] Demo/live pill uses the cached `safety-state-ui.md` pattern: cache updates on non-null fresh only; display is `fresh ?? cached`; stale marker renders when cache is the source.
- [ ] Client-side numeric validation uses `Number.isFinite(v) && v > 0` (not `!Number.isNaN`).
- [ ] Preview disclaimer is present in both modals naming the quote-source gap.
- [ ] Close preview always shows `open_rate` alongside `current_price`, and the P&L estimate uses `open_rate` fallback when `current_price === null`.
- [ ] Both modals are conditionally mounted by the parent (`{addFor !== null ? <Modal ...> : null}`) so `useAsync` fetches only on open.
- [ ] `ClosePositionModal` callers supply `instrumentId` alongside `positionId` (BrokerPositionItem alone is insufficient).
- [ ] `PortfolioPage` passes `valuationSource={p.valuation_source}` to both modals so the amber price treatment works.
- [ ] PR description names the tech-debt issue filed alongside this PR (`GET /portfolio/instruments/{iid}/quote-for-order`).
