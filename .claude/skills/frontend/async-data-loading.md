# async-data-loading

Rules for composing async data on a page using `useAsync` (or any equivalent hook).

This is the most common source of frontend review pain in eBull. Read it before touching any page that fetches data.

## Core principle

**Each async source owns its own `{loading, error, data}` lifecycle.** Composition happens at the render layer, never at the state layer.

## Rules

### One source, one error surface, one retry control

Each `useAsync` call drives **at most one** error surface and **at most one** retry button on the page. If two presentational sections both consume `portfolio.error`, you have two retry buttons for one fetch and the operator cannot tell them apart.

Fix: collapse the two sections under a single error guard, or make one of them render nothing on the failure path and let the other own the error UI.

```tsx
// WRONG — two error surfaces for one /portfolio failure
<SummaryCards error={portfolio.error} />
<Section title="Positions">
  {portfolio.error ? <SectionError onRetry={portfolio.refetch} /> : ...}
</Section>

// RIGHT — one error guard wrapping both
{portfolio.error ? (
  <div className="card-chrome"><SectionError onRetry={portfolio.refetch} /></div>
) : (
  <>
    <SummaryCards data={portfolio.data} />
    <Section title="Positions"><PositionsTable .../></Section>
  </>
)}
```

### Never combine `loading` flags across independent sources

Two sources fetching from two endpoints must render independently. A combined gate lets a slow or retrying endpoint hide the already-resolved sibling.

```tsx
// WRONG — slow /config hides resolved /system/status behind a skeleton
{system.loading || config.loading ? <Skeleton /> : <Panel ... />}

// RIGHT — each side renders its own {loading, errored, resolved} branch
<Panel
  system={system.error ? null : system.data}
  config={config.error ? null : config.data}
  systemLoading={system.loading}
  configLoading={config.loading}
  systemError={system.error !== null}
  configError={config.error !== null}
  onRetrySystem={system.refetch}
  onRetryConfig={config.refetch}
/>
```

The same rule applies to `error` flags: don't gate on `a.error && b.error`. Inline per-endpoint partial errors instead.

### `data` must be cleared on refetch start

When a refetch begins, `useAsync` must set `data = null` alongside `loading = true` and `error = null`. Otherwise a successful fetch followed by a failing retry leaves the stale prior payload visible while `loading=true` and `error=null`, and callers cannot distinguish "first load in progress" from "retry in flight after success".

If you write a new async hook, verify this contract.

### Top-of-page banner is reserved for "all sources failed"

Anything narrower is a per-section inline error. The top banner is the operator's "API unreachable" signal — it must not fire when one of four endpoints is down.

```tsx
const allFailed = a.error && b.error && c.error && d.error;
{allFailed ? <ErrorBanner message="API unreachable…" /> : null}
```

### Fetcher arrow functions do NOT need memoisation under `useAsync`

`useAsync` captures `fn` via a ref, so a fresh arrow per render is harmless. Do not wrap in `useCallback`. Do leave a one-line comment at the call site so a future reader doesn't "helpfully" memoise it and break the contract:

```tsx
// useAsync captures fn via a ref — fresh arrow per render is fine.
const recs = useAsync(() => fetchRecommendations(10), []);
```

If you need memoisation for a different reason (passing the function to a child that uses it as a dep), you're using the wrong hook.

### Refetch every dependent sibling after a mutation

When a mutation handler (toggle, POST, delete) completes, call `refetch()` on **every** async state whose data can be affected — not just the one whose UI contains the button. A panel that owns two `useAsync` calls (e.g. seed progress + per-CIK timing) and only refetches the obvious one leaves the sibling stale until the next poll tick (up to 60 s of idle polling in eBull's dashboard cadence).

```tsx
// WRONG — toggle changes pause state, but timing card stays stale
await setIngestEnabled("fundamentals_ingest", seed.ingest_paused);
seedState.refetch();

// RIGHT — refetch every sibling whose data can reflect the mutation
await setIngestEnabled("fundamentals_ingest", seed.ingest_paused);
seedState.refetch();
timingState.refetch();
```

Rule of thumb: if a sibling `useAsync` call reads any backend state the mutation touches (directly or transitively), include it in the refetch list. Write a test that asserts every expected `refetch` was called.

### Cancellation

Every effect that resolves async data must check a `cancelled` flag before calling state setters, so a stale resolution cannot overwrite a newer one. If you write a new async hook, this is non-negotiable.

## When to promote past `useAsync`

`@tanstack/react-query` is in `package.json` but currently unused. Default = stay on `useAsync`. Promote to react-query only when **at least one** of the following is true:

- Two pages need to share the same fetched data (cache).
- A fetch needs background refetch on focus / interval / mutation invalidation.
- Optimistic updates with rollback are required.
- Pagination needs cursor caching.

A single page with independent reads and no caching needs is **not** a reason to add react-query.

## Pre-push checklist for async code

Before pushing any page change, grep the page file:

```bash
grep -nE '\.loading\s*\|\|' frontend/src/pages/*.tsx     # combined loading gates
grep -nE '\.error\b' frontend/src/pages/*.tsx            # duplicate error surfaces
```

Each match must be deliberate. Each `useAsync` state should appear in the error branch exactly once.
