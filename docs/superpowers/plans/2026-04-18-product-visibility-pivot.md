# Product-visibility pivot — plan (2026-04-18)

**Trigger:** operator flagged that after weeks of infra work (filings cascade, raw housekeeping, coverage audit) the visible product "looks nothing like a broker app or Bloomberg terminal." Backend has the primitives; frontend doesn't expose them. Codex independently ranked the gap — this plan executes Codex's recommendation.

**Source of truth for next milestone:** *"An operator can open eBull, inspect portfolio risk, open an order ticket, preview a demo trade, place it, and close/part-close a position from the UI."*

Every infra ticket (Plan B richness, Plan C insider pipelines, master-plan follow-ups) is paused until P0-1 and P0-2 below ship.

---

## Current gap (verified against HEAD)

- Backend ready: `POST /portfolio/orders` at `app/api/orders.py:405`, `POST /portfolio/positions/{id}/close` at `app/api/orders.py:473`, order_client + execution_guard + tax_ledger all wired.
- Frontend gap: no `frontend/src/api/orders.ts`, no order modal, no close modal, no action surface. `PortfolioPage` rows navigate to read-only routes (`PortfolioPage.tsx:231, 277`).
- `DashboardPage` spends prime real estate on ops panels (system status + job health) instead of a command-center summary.
- `InstrumentDetailPage` is flat text + bid/ask only — no chart, no order ticket, no action panel.
- App shell is generic SaaS (`AppShell.tsx:7`) — never reads as trader tooling.

## Product backlog (prioritised, merged)

### P0-1 — Order entry + close modals (S-M, ~1 day)

Frontend only. Backend endpoints exist.

**Scope:**
- `frontend/src/api/orders.ts` — `placeOrder(params)`, `closePosition(positionId)`.
- `OrderEntryModal` component — buy/add flow, preview/confirm, error surfaces (execution-guard rejection, broker error).
- `ClosePositionModal` component — part-close (units slider) or full close.
- Demo/live indicator prominent; confirm-twice on live trades.
- Tests: happy path, guard rejection, broker error, cancel.

**Success criteria:** operator opens modal from Portfolio, enters amount, sees preview (estimated fees, tax impact if closing), places demo order, sees order appear in list.

### P0-2 — Portfolio as trading workstation (M, ~2 days)

**Scope:**
- Replace row-navigate with selected-row state + inline right-side detail panel.
- Detail panel shows: position snapshot, current thesis summary, latest filing events, score breakdown, action buttons (Buy More, Close, Full Close, View Research → existing InstrumentDetailPage).
- Keyboard nav: `/` search, `j/k` row focus, `Enter` select, `Esc` clear, `b` buy, `c` close.
- Pagination if >50 rows.

**Success criteria:** operator sees portfolio as a workbench, not a report. Zero clicks to act on any position.

### P1-3 — Dashboard as command center (M, ~2 days)

**Scope:**
- Replace default layout: AUM + cash available + deployment capacity + rolling P&L band (1d/1w/1m).
- "Needs action" panel: top recommendations awaiting operator review (deferred, conflicted, new theses).
- Alerts strip: thesis breaches, filings-status drops from analysable, execution guard rejections since last visit.
- Operator admin panels (sync layers, jobs grid) move to a new `/admin` secondary nav, demoted from prime dashboard space.

**Success criteria:** operator logs in, sees "here's your fund today + here's what needs you in the next 5 min".

### P1-4 — Instrument terminal (L, ~3-4 days)

**Scope:**
- Price chart: candle + volume + configurable range (1d/1w/1m/1y/5y). Use existing `price_daily` + `quotes`. Lightweight lib (Recharts or `lightweight-charts`).
- Quote strip: last, bid, ask, spread, day open/high/low, 52w h/l, volume vs avg.
- Left rail: thesis (latest + history sparkline), score breakdown, key metrics (EPS, revenue trend, margin).
- Right rail: order ticket inline (reuse P0-1 modal as panel), position if held, recommendations history.
- Tabs below: filings, news, analyst estimates, insider transactions (placeholder until Plan C.1 ships).

**Success criteria:** operator can make a buy/sell decision from InstrumentDetailPage alone, without needing to leave for another tool.

### P2-5 — Terminal-style app shell (M, ~2 days)

**Scope:**
- Denser layout: smaller default padding, monospace numerics, dark-mode default (terminal feel), keyboard-first nav.
- Status bar at bottom: live market status, last sync timestamp, kill-switch indicator, demo/live banner.
- Loading/empty/error states consistent across all pages.

**Deferred until P0/P1 ship** — this is polish on top of functional panels.

---

## Sequencing

Ship in order. Each PR merges before next starts. Codex pre-spec + pre-push per PR as usual.

| PR | Scope | Effort | Blocks |
|---|---|---|---|
| 1 | P0-1 order + close modals | 1 day | — |
| 2 | P0-2 portfolio workstation | 2 days | uses PR 1 modals |
| 3 | P1-3 dashboard pivot | 2 days | independent |
| 4 | P1-4 instrument terminal | 3-4 days | reuses PR 1 modals |
| 5 | P2-5 terminal shell polish | 2 days | after P0/P1 all shipped |

Total: ~10-11 days of frontend-focused work for a visibly transformed product.

## Paused work

The following remain in backlog but DO NOT ship until P0-1 + P0-2 are live:
- Plan B.1 — TRACKED_CONCEPTS expansion + financial_periods schema
- Plan B.3 — company metadata model
- Plan C.1 — insider transactions (Forms 3/4/5) pipeline
- Plan C.2 — 13F institutional ownership
- Plan C.3 — segment reporting extraction
- Chunk L flag flip + dead-code deletion (operator observes prod first)
- Raw-data retention dry-run-off (operator observes prod first)

These may still be valuable but are invisible to the operator. Rejoin backlog after the visibility pivot makes the product tangible.

## Codex checkpoints

Per CLAUDE.md, every PR in this sequence:
1. Spec Codex pass before implementing
2. Pre-push Codex pass before first `git push`
3. Standard review/CI cycle
4. Rebuttal-only rounds escalate to Codex per the decision tree

## Vision anchor

Every ticket under this plan must, before it ships, answer yes to:
*"Would the operator feel this moves the product closer to 'I can manage my fund from this screen'?"*

If not, rewrite the ticket or drop it.
