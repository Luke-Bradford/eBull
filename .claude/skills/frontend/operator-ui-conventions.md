# operator-ui-conventions

Presentation conventions for eBull operator surfaces.

> **Scope guardrail**: this file sets *presentation conventions*, not component architecture or page composition rules. Composition lives in `async-data-loading.md` and `loading-error-empty-states.md`. Do not let this file grow into a design system.

eBull is an operator/investment dashboard, not a consumer app. The UI exists to give a single operator a fast, accurate view of money and risk. Clarity beats flair every time.

## Formatting helpers — use them, don't reinvent

All formatting goes through `frontend/src/lib/format.ts`. Adding parallel formatters is a review-blocker.

| Helper | Use for | Renders `null` as |
|---|---|---|
| `formatMoney(n)` | All currency values | `—` |
| `formatPct(fraction)` | Percentages, where input is a fraction (`0.0123 → +1.23%`) | `—` |
| `formatNumber(n, digits?)` | Unit counts, share quantities, raw numerics | `—` |
| `formatDateTime(iso)` | All timestamps, anywhere | `—` |
| `pnlPct(unrealized, costBasis)` | Capital-weighted P&L percentage | `null` if `costBasis === 0` |

Rules:

- Currency is GBP for v1. Multi-currency is out of scope until a non-GBP account exists.
- Percentages are **always** computed from fractions, not pre-multiplied numbers. Backend returns `0.0123`, not `1.23`.
- Aggregate percentages are **capital-weighted** (`Σ pnl / Σ cost_basis`), never an average of per-row percentages.
- Timestamps are en-GB, short month, 24h. Never raw ISO in the DOM.
- Unit columns get `tabular-nums` so they align in tables.

If you find yourself hand-formatting a number with `.toFixed()` or `.toLocaleString()` in a component, stop and use the helper instead.

## Color semantics

Restrained palette. Each color has one job. Mixing them dilutes the signal.

| Color | Meaning | Tailwind family |
|---|---|---|
| **Red** | Error / risk / breached / EXIT | `red-50/100/200/300/600/700` |
| **Amber** | Warning / stale / degraded / proposed | `amber-50/100/700` |
| **Emerald** | OK / positive / executed / BUY | `emerald-50/100/600/700` |
| **Blue** | Neutral interactive (links, in-progress, approved) | `blue-50/100/600/700` |
| **Slate** | Neutral / unknown / muted / HOLD / borders / text | `slate-100/200/400/500/600/700/800` |

Do not introduce new color families (purple, pink, cyan, etc.) without a documented reason. Do not use red for "in progress" or amber for "ok".

## Status pill vocabulary

Pills are defined once and reused. The current set:

**Action pills** (recommendations):
- `BUY` → emerald
- `ADD` → emerald (lighter)
- `HOLD` → slate
- `EXIT` → red

**Status pills** (recommendation lifecycle):
- `proposed` → amber
- `approved` → blue
- `rejected` → red
- `executed` → emerald

**Layer / overall health pills** (system status):
- `ok` → emerald
- `stale` → amber
- `empty` → slate
- `error` / `down` → red
- `degraded` → amber

If you add a new pill, add it to the relevant component's tone map and to this list. Do not invent ad hoc colors at the call site.

## Density rules

Operator surfaces are dense but readable. Compact, not cramped.

| Element | Default |
|---|---|
| Page outer padding | `p-6` |
| Section gap | `space-y-6` |
| Card body padding | `p-4` |
| Card header padding | `px-4 py-3` |
| Table cell padding | `px-2 py-2` |
| Table base size | `text-sm` |
| Numeric table cells | `text-right tabular-nums` |
| Pill padding | `px-1.5 py-0.5` |
| Pill text | `text-[10px] font-medium` |

Do not add `p-8` or `space-y-12` to "give it room to breathe" — operator pages are scanned, not browsed. Do not drop below `text-sm` for primary data.

## Heading hierarchy

| Level | Use | Classes |
|---|---|---|
| `h1` | Page title | `text-xl font-semibold text-slate-800` |
| `h2` (section title) | Card / section header | `text-sm font-semibold text-slate-700` |
| Section eyebrow | Sub-section label inside a card | `text-xs font-semibold uppercase tracking-wide text-slate-500` |
| Body | Default text | `text-sm text-slate-700` |
| Muted | Hints, timestamps, helper text | `text-xs text-slate-500` |

No display type. No `text-4xl` hero sections. The page title is `text-xl` and that's the largest type on any operator screen.

## Read-only vs interactive

Mutating elements must look unmistakably interactive. Read-only data must not look clickable.

- **Buttons** that mutate state: full button chrome (border + bg + hover state). Never style a `<button>` as a bare link.
- **Links** to other pages: `text-blue-600 hover:underline`. No button chrome.
- **Read-only data**: never `text-blue-*`, never `hover:underline`, never `cursor-pointer`.
- **Disabled controls**: lower opacity + `cursor-not-allowed`. Never just hide the button — the operator needs to know it exists and why it's disabled.

If the operator can't tell at a glance whether a piece of UI is clickable, the styling is wrong.

## What this file is not

- Not a design system
- Not a component library
- Not a color theory document
- Not a place to record the look of a single page

Add a rule here only if it generalises across operator pages. Page-specific styling lives in the page file.
