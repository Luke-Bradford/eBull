# frontend/information-architecture

## When to use

Read before any change to eBull's top-level navigation / page structure — merging or splitting pages, adding a nav item, or building a multi-view surface. These are **standing operator decisions**. Consolidating existing pages into a coherent hub is **engineering, not a taste-gate** — decide the shape from the tickets + the surfaces themselves and build it; do NOT open a "should I merge these?" sign-off loop. Escalate only a genuinely new nav paradigm or an irreversible route removal with live external links you can't shim.

## The lens-consolidation pattern — SETTLED

When **N routes render N lenses on ONE dataset** (same underlying universe, differing only in which columns + filter + default sort are shown), that is **one surface, not N pages**. Consolidate into a single hub with **view presets**.

Canonical case (#1917): Instruments / Rankings / Theses / Recommendations are four lenses on the instrument universe → one **`/research`** hub.

### Presets = a segmented control, NOT chips

A preset swaps the **endpoint + column set + default sort** — it is a **mode**, not an additive filter. Model it as a **segmented control (tabs)**: "pick one lens." Chips model "toggle several at once" and mismodel a mode swap. (Grounding: Finviz/TradingView/Investing.com screeners all use mode tabs for exactly this "same universe, different lens" problem.)

- Route the preset in the URL: `/research?view=ranked|universe|…` — deep-linkable, back-button-correct.
- **Land on the highest-value lens** (Ranked for eBull — scored universe is the daily driver).
- A preset that is a **generation/action queue** (e.g. Theses = "what has/needs a thesis + staleness") stays a queue with its own columns — it is NOT a second copy of the main ranked table. Don't collapse a queue into the grid.

### Migration is mechanical — do all of it

1. **Redirect shims** for every old route → the hub preset. Reuse the existing `InstrumentDetailRedirect` shim pattern (`App.tsx`). **Forward query strings** (e.g. `/theses?held&stale` → `/research?view=theses&held&stale`) so existing deep links + bookmarks survive.
2. **Collapse the sidebar** (`Sidebar.tsx`) — N items → 1.
3. **Repoint every in-app link** that targeted an old route (grep `to="/rankings"` etc.) at the appropriate `?view=…`.

### Decouple blocked sub-features

A blocked companion (e.g. a heat-map endpoint that isn't merged) must NOT gate the hub. Ship a **render-mode toggle** (Table | Map) with the blocked mode **present but disabled/"coming soon"**, table-only now; the blocked mode drops in later with no re-architecture.

## Lens components must PRESERVE the hub's `view` param

A lens page that owns URL query state (filters, pager, sort) commonly writes it by rebuilding a fresh `URLSearchParams` — which **drops the hub's `view` param**, so the next filter/pager click bounces the operator off the lens back to the default preset. When a page can be mounted under the hub, its `setSearchParams` must **preserve params it does not own**: start from the current params (`setSearchParams((prev) => …)` or `new URLSearchParams(base)`), delete-then-set only its own keys, and leave `view` (and any future foreign param) untouched. (Caught in #1917 Codex ckpt-2 on `ThesesPage.writeFilters`.)

## Reuse the shared table primitives

The hub composes the same extracted table primitives, not re-implemented ones (`frontend/src/lib/portfolioRows.ts`, `frontend/src/lib/avatar.tsx`, shared cell renderers from the #1901 dedup). If a primitive doesn't exist yet, extract it once and share — never fork a fifth copy.

## Standing execution rule

The consolidation shape above is decided. Implement it against this skill: behavior-preserving per lens (each preset renders what its page renders today), one PR for the hub shell + presets + redirects + sidebar collapse, run typecheck + `test:unit` + the dark gate. Surface to the operator only a genuinely new nav paradigm — never the assembly of a hub from pages that already exist.
