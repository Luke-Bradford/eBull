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

`setAuthToken` / `hasAuthToken` are the only auth-touching functions. Page components must not:

- Read the token
- Set the token
- Pass the token as a prop
- Branch on token presence

If a page needs to know "am I authenticated?", that's a sign the auth flow needs to expose a reactive store — open a tech-debt issue rather than reading the in-memory slot ad hoc.

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
- [ ] No page component reads or sets the auth token
