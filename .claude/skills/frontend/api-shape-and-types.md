# api-shape-and-types

Keeping the frontend/backend wire honest. There is no codegen — drift is caught only by humans, so the rules below are mechanical on purpose.

## Source of truth

`frontend/src/api/types.ts` mirrors the Pydantic `response_model` classes in `app/api/*.py`. The Python file is canonical; the TypeScript file is a hand-maintained shadow.

The header of `types.ts` already lists which Python file each section mirrors. Keep that mapping accurate when you add new sections.

## Drift rule

**When a backend `response_model` changes, update `types.ts` in the same PR.** Not the next one. Same PR.

A type drift between FE and BE silently breaks every page that consumes the changed shape. There is no compile-time link between them.

When you add a new endpoint:

1. Add the response interface(s) to the matching section of `types.ts`.
2. Add a thin fetcher under `frontend/src/api/<endpoint>.ts`.
3. Consume via `useAsync` in the page. Pages **never** call `apiFetch` directly.

When you change an endpoint:

1. Update the Pydantic `response_model`.
2. Update the matching `types.ts` interface in the same diff.
3. `pnpm --dir frontend build` will catch most drift via call-site type errors. It will not catch new fields you forgot to add — manual review against the Pydantic class is required.

## Pydantic → TypeScript translation table

| Pydantic | TypeScript | Notes |
|---|---|---|
| `int`, `float` | `number` | |
| `int \| None`, `float \| None` | `number \| null` | Mirror the `\| None` exactly. |
| `str` | `string` | |
| `str \| None` | `string \| null` | |
| `bool` | `boolean` | |
| `datetime`, `date` | `string` | ISO 8601 — formatters parse with `new Date()`. Never `Date` directly. |
| `Literal["a", "b"]` | `"a" \| "b"` | Use a `type` alias if reused (`type Action = "BUY" \| ...`). |
| `list[X]` | `X[]` | |
| `dict[str, Any]` | `Record<string, unknown>` | |
| `BaseModel` subclass | `interface` | Mirror field-for-field, in declaration order. |

If the Pydantic field is `field: SomeType` (no `| None`), the TS field must **not** include `| null`. The reverse is also true. Asymmetric nullability is the most common drift bug.

## `apiFetch` path contract

`apiFetch` already enforces this and will throw, but follow it by hand too:

- **Pass backend-relative paths only**: `apiFetch("/portfolio")`, not `apiFetch("/api/portfolio")`.
- The Vite dev server proxy strips `/api` and forwards the rest. Adding `/api` yourself produces `/api/api/...` after the rewrite, which 404s on the backend.
- Query strings: build with `URLSearchParams` and append. Do not interpolate user input.

## Auth lives in the client, never in pages

Auth is cookie-based (issue #98): the backend sets an HttpOnly session cookie on `/auth/login`, and `apiFetch` sends `credentials: "include"` so it travels with every request. JS never reads or writes the token — there is no token in JS to touch. Session state lives in `SessionProvider` (`frontend/src/lib/session.tsx`), read via the `useSession` hook. On any 401, `apiFetch` invokes the handler registered through `setUnauthorizedHandler` (in `frontend/src/api/client.ts`); `SessionProvider` is the sole registrant and clears state + redirects to `/login`.

Page components must not:

- Read, set, or pass an auth token (there is none)
- Call `setUnauthorizedHandler` — only `SessionProvider` registers the 401 handler
- Re-derive "am I authenticated?" ad hoc — consume `useSession`, which already exposes it as a reactive store

## Fetcher file shape

Each fetcher file is tiny and does one thing. Pattern:

```ts
import { apiFetch } from "@/api/client";
import type { PortfolioResponse } from "@/api/types";

export function fetchPortfolio(): Promise<PortfolioResponse> {
  return apiFetch<PortfolioResponse>("/portfolio");
}
```

No business logic. No transformation. No retries. No caching. The fetcher is a typed wrapper around `apiFetch` and nothing else. Anything more belongs in `useAsync` consumers or (if it grows) a service module.

## Pre-push checklist for shape changes

If your diff touches `app/api/*.py` response models or `frontend/src/api/types.ts`:

- [ ] Both files updated in the same PR
- [ ] Field names match exactly (including snake_case — do not camelCase on the wire)
- [ ] Nullability matches: every `| None` has a `| null`, every `| null` has a `| None`
- [ ] New endpoint has its own fetcher file under `frontend/src/api/`
- [ ] Page consumes via `useAsync`, not via raw `apiFetch`
- [ ] No page component touches auth outside `useSession` (no manual token handling; no `setUnauthorizedHandler` calls)

## An array field's length is not bounded by the type (#2178)

`Foo[]` says nothing about size. A field the backend "obviously" keeps small
can be unbounded in production, and the stored history keeps whatever it had
when it was written — a builder fix does not retro-shrink data already
persisted.

`score_changes` was typed `ScoreChangeV2[]` and carried **29,281 rows** for one
month (4.38 MB of a 4.39 MB snapshot). The page mapped every row to a
react-router `<Link>` — ~150k DOM nodes in one synchronous commit, which froze
the tab.

Rules:

- **Cap what you render, in the component, independent of the payload.**
  `rows.slice(0, CAP)` with the cap as a named const and a comment saying why.
  Server-side capping is necessary but not sufficient: snapshots, caches, and
  any immutable stored payload predate your fix.
- **Set the FE cap above the backend's own limit** so a later server-side bump
  is not silently truncated by the client.
- **When you cap, say so in the UI** — "N of M shown" — and carry the pre-cap
  total as its own field. Make that field optional and fall back to
  `array.length` when absent, so records written before the field existed
  report their real count instead of an invented one.
- Self-review prompt: for every `.map()` over an API array, "what is the
  largest this can be in production, and what does one row cost to render?"
  A `<Link>`, a chart, or a nested component makes the answer much worse than
  a `<span>`.

## Disclosure elements do not defer render cost (#2178)

`<details>`, `hidden`, and `display:none` are presentation-only. React still
evaluates the JSX and commits the DOM. The reports appendix built ~9 MB of
`JSON.stringify(snap, null, 2)` on every render while looking collapsed.

Gate expensive children on **state** and render `null` until opened. Applies to
`JSON.stringify`, large `.map()`s, and charts inside any collapsed region.
See `ReportsPage.tsx::RawJsonAppendix`.
