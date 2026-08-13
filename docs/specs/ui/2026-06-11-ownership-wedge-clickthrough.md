# Per-wedge SEC click-through (#921)

Status: live spec for #921 (second of 4 split tickets under closed parent #846).
Scope: `OwnershipSunburst.tsx`, `ownershipRings.ts`, `OwnershipPanel.tsx`,
`OwnershipPage.tsx` + tests. No backend change (`winning_edgar_url` already in the
rollup payload from #840 P1).

## Stale-premise resolution (operator decision 2026-06-11)

The issue says "current chart has no click handler at all" — stale. Wedge click
has navigated to the in-app L2 ownership page since #746 (2026-05-01, predates the
issue). Operator chose the **split** model via AskUserQuestion:

- **Leaf (outer-ring per-filer) wedge** with a known `source_url` → opens the SEC
  archive index in a new tab (the issue's audit motivation: "wedge looks
  suspicious → drill straight to the source filing").
- **Category wedge + center** → keep the existing in-app navigate (L1 panel →
  L2 page; L2 page → in-page query-param drill).
- **Leaf without a URL** (aggregated "Other" tail, treasury, defensive-null
  accessions, all L2-page-fed holders for now) → falls back to the existing
  in-app drill. Never a dead wedge. This supersedes issue acceptance item 3
  ("no click handler" on URL-less wedges), which was written on the stale
  premise.

## Design decisions

### D1 — `source_url` threads through the data model as an optional field

`SunburstHolder.source_url?: string | null` → `SunburstLeaf.source_url:
string | null` → `WedgeClick` leaf variant gains `readonly source_url: string |
null`. Optional on the input so the five L2-page holder builders
(`filerToHolder`, `aggregateInsiderHoldersForSunburst`, `baselineToInsiderHolders`,
`blockholdersToHolders`) compile unchanged and yield `null` (= in-page drill
fallback, today's exact behaviour). Only `rollupToSunburstInputs` (L1 panel) maps
`holder.winning_edgar_url`. Threading the four L2 endpoints is a follow-up ticket
if the operator wants EDGAR-from-chart on L2 — the URLs are not in those payloads
today.

"Other" tail leaves and the treasury pseudo-leaf get `source_url: null` by
construction (an aggregate has no single source filing).

### D2 — shared `openWedgeSource(target)` helper owns the new-tab dispatch

Exported from `OwnershipSunburst.tsx`: returns `false` unless `target` is a leaf
with non-null `source_url` that passes the fail-closed host guard (URL must start
with `https://www.sec.gov/` — backend builds these from accession numbers, but a
2-line prefix check means a corrupted payload degrades to the in-app drill
instead of opening an arbitrary URL; Codex ckpt-1 Low). Otherwise
`window.open(url, "_blank", "noopener,noreferrer")`; returns `window.open(...)
!== null`, so a popup-blocked open (`null` return) also falls back to the in-app
navigate rather than swallowing the click (Codex ckpt-1 Medium). Both pages'
`handleWedgeClick` call it first and fall through to their existing
navigate/param logic on `false`.

### D3 — keyboard: Enter triggers the focused sector's click

Recharts 2.15 native keyboard model (read from `node_modules`): pie root Layer is
Tab-reachable (`rootTabIndex` default 0), ArrowLeft/Right move DOM focus across
sector `<g>` refs, Escape blurs — but **no Enter handling**, and sector
`tabIndex: -1` is hardcoded after the props spread (not overridable per Cell).

Sector → datum mapping does NOT rely on DOM order (Codex ckpt-1 High). Every
`Cell` carries `data-ring="middle"|"outer"` + `data-idx={i}`; the wrapper-div
`onKeyDown` Enter handler reads the focused sector's child
`path[data-ring]` dataset and resolves `middleData[idx]` / `outerData[idx]`
directly (helper `focusedSectorDatum(activeEl, middleData, outerData)`). Then it
dispatches through the same `handleClick` path as mouse. Gap/residual sectors
resolve to `target: null` → no-op, same as mouse.

**Tab order (Codex ckpt-1 Medium):** the decorative inner pie gets
`rootTabIndex={-1}` — it is a solid band whose click ("show L2 view") duplicates
page-level navigation, so it stays mouse-only. Keyboard flow = two Tab stops
(middle ring, outer ring), ArrowLeft/Right cycles sectors within the focused
ring, Enter activates, Escape blurs. Recharts' per-pie keyboard handlers make a
single combined stop impossible without reimplementing focus management.

**AT semantics (Codex ckpt-1 High):** wrapper `role="img"` is wrong once the
chart is interactive — replaced with `role="group"` + the existing aria-label.
Known-wedge `Cell`s gain `aria-label` (`"{name}: {shares} shares, {pct} of
outstanding"`) so arrow-focused sectors announce; recharts spreads unknown Cell
props onto the sector `<path>` (same passthrough as `data-*`, verified for
`data-*` in #920 ckpt-2).

### D4 — no new cursor work

`cursor: pointer` already applies to every `data-known='true'` sector; leaf
wedges remain clickable in both branches (EDGAR or fallback drill), so the
existing affordance is already correct. Residual/gap sectors stay
non-interactive for click (#920 carve-out unchanged).

## Acceptance mapping (issue → repo reality)

No Playwright in repo (see #920 spec). Translated:

1. Vitest: `openWedgeSource` — leaf + sec.gov URL opens exact URL with
   `noopener,noreferrer` (spy), returns true; leaf+null / non-sec.gov URL /
   category / center return false; popup-blocked (`window.open` → null)
   returns false.
2. Vitest: `rollupToSunburstInputs` threads `winning_edgar_url` →
   `holders[].source_url`; rings thread holder → leaf `source_url`; "Other"
   tail + treasury leaves get `null`; `buildSunburstChartData` leaf targets
   carry `source_url`.
3. Vitest: `focusedSectorDatum` maps `data-ring`/`data-idx` skeleton DOM to
   the right datum; returns null for non-sector focus / missing dataset.
4. Vitest **rendered** keyboard test (Codex ckpt-1 Medium): mock
   `ResponsiveContainer` to a fixed-size passthrough (recharts computes sector
   geometry mathematically, so real sectors render in jsdom), focus a real
   sector path's Layer, press Enter on the wrapper, assert the wedge action
   fires (window.open spy for a leaf with URL).
5. Type-ripple sweep (Codex ckpt-1 Low): grep every constructor of leaf-kind
   `WedgeClick` (tests + fixtures) and update for the required `source_url`.
6. `typecheck` + `test:unit` + `dark:check` green.
7. Dev-verify (operator): AAPL largest institutional wedge click → SEC archive
   index new tab; Tab → Arrow → Enter does the same.
