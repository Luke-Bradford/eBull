# Ownership chart static polish (#920)

Status: live spec for #920 (first of 4 split tickets under closed parent #846).
Scope: `frontend/src/components/instrument/OwnershipSunburst.tsx` only. No backend, no API, no new state.

## What ships

1. **Residual hatching.** The `Public / unattributed` residual wedges (middle-ring
   `middle-residual` + outer-ring `outer-residual` gap datums) render with a subtle
   diagonal hatch instead of fully transparent. Within-category gap wedges
   (`{cat}-gap`, "unresolved filers") stay transparent and inert — out of scope per
   the issue.
2. **Residual hover tooltip un-suppressed.** Copy:
   `Public / unattributed: X% of outstanding — N shares not attributed to any disclosed filer.`
   ("not **attributed to**", not the issue's "not held by": the residual can include
   positions of filers outside our seeded coverage cohort — the backend residual
   tooltip says exactly this — so "not held by any disclosed filer" overclaims.
   Attribution is a statement about *our* data, which is what the wedge shows.
   Codex ckpt-1 High.)
3. **Center label third line:** `X% known coverage` under the existing
   `TOTAL SHARES <N>` block.
4. **Legend row rename** `Unaccounted` → `Public / unattributed` + hatched swatch,
   consistent with the wedge. (No references to "Unaccounted" exist outside the
   component — verified by grep.)

## Design decisions

### D1 — `is_residual` flag on `ChartDatum`

Gap wedges split into two kinds: residual (hatched, hover-tooltip, never clickable)
vs within-category gap (transparent, fully inert). `makeGapDatum` gains an
`is_residual` parameter. `target` stays `null` for both — no click semantics change.

### D2 — hatch pattern via hidden sibling `<svg>` + `useId`

Recharts filters unknown children of `<PieChart>`, so a `<defs>` child is not
reliable. Instead a zero-size `aria-hidden` `<svg>` sibling inside the component
root carries `<defs><pattern id={patternId}>`. Pattern pinned (Codex ckpt-1 Low):
6×6 px tile, one diagonal line, stroke `#94a3b8` (slate-400 — same token the focus
outline already uses; reads on white and slate-950), `strokeOpacity 0.45`,
`strokeWidth 1.2`. Residual `Cell fill` = `url(#patternId)`. `useId()` keeps the
pattern id unique if two charts mount on one page. Same-document `url(#…)`
paint-server references resolve across SVG elements. Manual acceptance: dev-verify
AAPL in light + dark mode (`dark:check` only audits class pairing, not SVG).

### D3 — pointer-events: residual hoverable, not clickable

Current CSS disables hit-testing on every `data-known='false'` sector, so
un-suppressing the tooltip alone would be dead code (the spec'd hover could never
fire). New attribute `data-residual` on each gap `Cell`:

- `data-known='false'` + `data-residual='false'` → `pointer-events: none` (unchanged behaviour).
- `data-known='false'` + `data-residual='true'` → hit-testing ON so the Recharts
  tooltip fires; **no** `cursor: pointer`, **no** hover brightness — read-only data
  must not look clickable (operator-ui-conventions).

### D4 — tooltip copy says "of outstanding", not "of float"

Issue text says "X% of float". The chart denominator is `shares_outstanding`
(rings' effective `total_shares`), and float ≠ outstanding (float excludes
insiders/restricted). Mislabeling the denominator on an auditability-first surface
is worse than a verbatim match, so the copy reads `X% of outstanding`. Conscious
deviation, recorded here + in the PR description.

The percent/shares values come from the residual datum the rings produced
(`category_residual / total_shares`) — same numbers the wedge is drawn from.

### D5 — coverage line derives from rings, not the rollup payload

`known coverage = (total_shares − category_residual) / total_shares`. Computed from
the same `buildSunburstRings` output the wedges render from, so label and chart
cannot diverge (the rollup's `concentration.pct_outstanding_known` uses the
*reported* denominator and would drift under the oversubscription bump).

**Oversubscribed edge (Codex ckpt-1 Medium):** when category totals oversubscribe
the reported outstanding, `buildSunburstRings` bumps the denominator to `sum_known`
→ `category_residual = 0` → the label honestly reads `100% known coverage` against
the bumped denominator. That is geometrically consistent with the wedges; the
oversubscription itself is surfaced by the existing panel-level
`OversubscribedWarning` (`rollup.residual.oversubscribed`), not by this label.
Covered by an explicit test.

### D6 — chart-datum construction extracted as pure function

`buildSunburstChartData(rings, theme)` (exported) produces `{middleData,
outerData}` so tests assert the `is_residual` / `is_gap` flag placement directly —
jsdom + `ResponsiveContainer` renders a zero-size chart, so sector-level DOM
assertions are not viable (Codex ckpt-1 Low, satisfied at the data layer).
`residualTooltipText(shares, pct)` exported likewise (pct = the fraction already on
the chart datum); the tooltip renders it, the test asserts the exact copy
(test-quality skill: prefer pure policy).

### D7 — legend hatched swatch via CSS gradient

The legend swatch is an HTML `<span>` — `backgroundColor` cannot reference an SVG
paint server (Codex ckpt-1 Medium). Swatch uses inline
`background: repeating-linear-gradient(45deg, transparent 0 3px, rgba(148,163,184,0.6) 3px 4px)`
and keeps the existing dashed border.

## Acceptance mapping (issue → repo reality)

Issue asks for Playwright snapshots — **no Playwright harness exists in this repo**
(verified: no playwright dep/config; FE tests = vitest + testing-library + jsdom).
Translated acceptance:

1. Vitest: center label renders `X% known coverage` (plain HTML outside
   ResponsiveContainer — testable in jsdom), incl. the oversubscribed → `100%` case.
2. Vitest: `residualTooltipText` produces the exact spec'd copy.
3. Vitest: `buildSunburstChartData` marks `middle-residual` / `outer-residual` as
   `is_residual=true` and `{cat}-gap` as `is_residual=false`.
4. Vitest: hatch `<pattern>` def present in DOM; legend residual row labelled
   `Public / unattributed` with hatched swatch.
5. `pnpm --dir frontend typecheck` + `test:unit` + `dark:check` clean.
6. Dev-verify: AAPL ownership panel, light + dark, hatch visible + tooltip fires on
   residual hover.
