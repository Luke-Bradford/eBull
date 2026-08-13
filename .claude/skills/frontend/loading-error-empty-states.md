# loading-error-empty-states

The visual contract for the three states every data-bearing slot must answer.

## The three states are mandatory

Every component that displays fetched data must answer all three before merge:

1. **Loading** — what shows while the fetch is in flight?
2. **Empty** — what shows when the fetch succeeded but the array is `[]`?
3. **Error** — what shows when the fetch threw?

Missing any of the three is a review-blocker. "It looks fine on the happy path" is not a passing answer.

## Reuse, do not reinvent

Use the existing primitives. Do not introduce new variants:

- **Loading** → `<SectionSkeleton rows={n} />` (animated grey bars)
- **Empty** → `<EmptyState title="…" description="…">{action}</EmptyState>`
- **Error (per section)** → `<SectionError onRetry={refetch} />`
- **Error (page-level, all sources failed only)** → `<ErrorBanner message="…" />`

If you need a new variant, justify it in the PR description and update this skill in the same PR.

## Loading and error DOM must be structurally symmetric

Transitions between loading → data → error must not shift the layout. If the error state collapses two sections into one card, the loading state must collapse the same way. If the data state renders three cards in a grid, the loading state must render three skeleton cards in the same grid.

```tsx
// WRONG — error collapses to one card; loading sprays two grids
loading: <><GridOfThreeSkeletons /><Section><SkeletonRows /></Section></>
error:   <Card><SectionError /></Card>

// RIGHT — both states share the same outer container
<Card>
  {loading ? <SkeletonRows /> : error ? <SectionError /> : <Content />}
</Card>
```

This is a real PR #89 nitpick (deferred to issue #90); future PRs should pre-empt it.

## Never render exception text in the DOM

`SectionError`, `ErrorBanner`, and `ErrorBoundary` all use **fixed phrases**. Never:

```tsx
// WRONG
<ErrorBanner message={error.message} />
<div>Failed: {String(err)}</div>
```

```tsx
// RIGHT
<ErrorBanner message="Failed to load. Check the browser console for details." />
console.error("Portfolio fetch failed:", err);
```

This mirrors the backend rule against leaking internal exception text into HTTP responses (`docs/review-prevention-log.md` → "Internal exception text leaked into HTTP response bodies"). Same principle: full detail to logs, fixed phrase to the surface.

## Empty states must give the operator the next action

A bare "No data" is a dead end. Every empty state should either:

- Link to the page that creates the data (`<EmptyState>` with a `<Link to="/rankings">`), or
- Tell the operator which job populates it ("Recommendations will appear here once the portfolio manager has run."), or
- Both.

Operator dashboards exist to drive action; an empty state is the most expensive piece of screen real estate to waste.

## Multi-surface states share one copy source

A server-driven state (`no_data`, `error`, an empty array) is usually rendered on more than one surface — the L1 panel **and** the L2 page, sometimes plus a CSV/export. When you change what that state means or how its empty-state should read, **grep every surface that branches on it** before claiming done:

```bash
grep -rn 'banner.state === "no_data"' frontend/src
grep -rn "<the hardcoded copy string>" frontend/src
```

Fixing one and assuming the rest follow leaves the page contradicting the panel (honest banner above, stale copy below). This bit #1581 twice in one review session — ckpt-1 caught the panel, ckpt-2 caught the page.

Route the per-state copy through ONE shared helper, returning `string | null` so each surface keeps its own default while sharing the new branch:

```ts
// shared (e.g. ownershipMetrics.ts)
export function staleCopy(state, asOf): string | null {
  return state === "no_data" && asOf !== null ? "<stale copy>" : null;
}
// each surface
description={staleCopy(state, asOf) ?? "<that surface's own absent copy>"}
```

Duplicating the literal in two files drifts; the shared helper is the single source of truth. "No FE change" is a claim about **absence** — it requires grepping every consumer of the changed state, not reasoning from one component.

## Top-of-page error banner has one job

It fires **only when every async source on the page has failed**. Anything narrower is a per-section inline error. See `async-data-loading.md` → "Top-of-page banner is reserved for 'all sources failed'".

## Pre-push checklist for state coverage

For every new data-bearing component:

- [ ] Loading state renders without flickering or layout shift
- [ ] Empty state renders with a next-action affordance
- [ ] Error state uses `SectionError` (or `ErrorBanner` only if page-wide) with a fixed phrase
- [ ] Loading and error DOM share the same outer container
- [ ] No `error.message` or `String(err)` appears anywhere in the JSX
- [ ] `EmptyState` / `SectionSkeleton` / `SectionError` reused — no bespoke variants
