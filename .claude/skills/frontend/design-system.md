# frontend/design-system

## When to use

Read before any change to eBull's visual system — surfaces/cards, badges/pills, chart theming, spacing/density, typography, or Tailwind tokens. These are **standing operator decisions**, not per-ticket taste calls. Assembling the system below is **engineering, not a taste-gate** — decide and build; do NOT open a "which look do you prefer?" loop. Only a genuinely NEW visual identity (new brand palette, a new elevated-surface language) escalates to the operator.

> Companion: `operator-ui-conventions.md` owns *presentation* (formatters, color semantics, copy). This file owns the *surface + component system*. Don't duplicate.

## Surface model (the #691 line) — SETTLED

The base surface is **settled and deliberate**: design-system **v1 is borderless "editorial chrome"** (`components/instrument/Pane.tsx:32-47`). It already replaced the prior rounded card (border + shadow + `bg-white`) with a **single hairline top-rule + small-caps title**, on an explicit rationale — financial operators **scan across panes for cross-pane signal** (revenue trend → dividend yield → insider buying), so the grid must read as **one document, not a Trello-board of separate cards**. That IS the #691 completion. `StatTile.tsx` follows the same rule ("the hairline IS the tile chrome", #1592).

**Do NOT reflexively "add cards".** A bounded/filled card fragments the cross-pane scan the v1 system was built to protect — reversing it inside the instrument grid (or any scanning grid of Panes) is a **settled-decision reversal**: surface it to the operator with a recommendation, don't just ship it.

- **Default = editorial chrome** — hairline rule + title, no fill, no border box, no shadow. Panes, stat rows, narrative sections. `Pane` / `StatTile` already encode this; reuse them.
- **A bounding surface is a narrow, JUSTIFIED exception** — only for a genuinely **standalone** tile that is NOT part of a scanning grid and reads as unanchored on bare page background (e.g. a lone chart with no neighbouring panes). Even then: flat only — subtle fill + `1px` border, `rounded`, **NO shadow, NO elevation** (the #691 line). If in doubt, prefer fixing "floating" with **spacing/grouping/density** (see Density) inside editorial chrome, not a card.

Decision rule: **editorial chrome first.** Reach for a bounding surface only for a standalone unanchored tile, and never retrofit cards onto a Pane scanning grid without operator sign-off — that's reversing v1's documented rationale.

**Answered for visual v2 (#1908, 2026-07-24): NO card variant is introduced into any Pane scanning grid.** The "stats float in empty space" complaint that motivated the card is fixed by **spacing / grouping / density inside editorial chrome** instead. Do not re-open this by adding a card the next time a stat row looks sparse — reach for Density below.

## Badge / pill — ONE component

`frontend/src/components/ui/Badge.tsx` (#1908) is the pill. Never hand-roll an inline pill; never re-declare pill geometry.

- **Tones are SEMANTIC, not colours**: `ok | warn | risk | info | neutral`, mapping 1:1 onto the `operator-ui-conventions.md` colour table (emerald / amber / red / blue / slate). A caller passes meaning; it never writes a colour class. Unmapped enum value → `neutral`, never blank (#1808 class).
- **Meaning lives in the TEXT**, colour is decorative reinforcement (a11y). A badge whose label doesn't say what it means is a bug, regardless of colour.
- **Domain enum → tone maps are shared**, not per-component: `lib/badgeTone.ts` (recommendation action / status / completeness), `components/instrument/eightKSeverity.ts` (8-K severity), `StanceBadge` / `CriticVerdictBadge` (thesis vocabularies). Two components rendering the same enum share one map.
- `title` / `data-*` / `className` pass through, so hover copy, test hooks and layout margins survive migration — there is no reason to fall back to a raw `<span>`.
- **Colour classes must not live in a tone map.** Semantic values only. A tone map holding raw Tailwind is how `eightKSeverity.ts` shipped light-only chips past the dark gate (prevention-log: "A lint gate's file-glob is part of its contract").

## Chart theming — ONE source

Charts read colors, axes, gridlines, and tooltip styling from a single `chartTheme` module (tokens), not per-chart literals. A new chart imports the theme; it does not re-pick colors. Keep chart series colors on the same restrained palette as the rest of the UI (color = signal, not decoration).

**Bind the resolved palette, then never reach past it.** The live failure (#2185): `fundamentalsCharts.tsx` bound `useChartTheme()` to `theme` AND imported `lightTheme`, then used `lightTheme.accent[…]` 23 times against 14 uses of `theme.` — hardcoding the LIGHT palette into a dark-capable page. It looked correct because `darkTheme` currently re-exports the light accents, so the bug was invisible on screen and would only surface the day an accent diverges. Importing `lightTheme` / `darkTheme` **by name** anywhere outside `lib/chartTheme.ts` is the smell; `useChartTheme()` is the only read.

The dark gate cannot catch this — `check-dark-classes.mjs` inspects Tailwind bg/border/hover **class pairs**, and these are inline hex values from a JS module (prevention log → "a lint gate's file-glob is part of its contract"). `pnpm --dir frontend charts:check` (`scripts/check-chart-integrity.mjs`, wired into `.githooks/pre-push`) is the gate that does. Since #2190 it covers **`components/` + `pages/`** tree-wide, so a new chart is guarded the moment it is written. `lib/chartTheme.ts` — the definition site, and the one file that must name the raw palettes — is outside that scope by path, not by an exemption branch.

Pre-existing debt at the widening (27 violations across 10 files) was capped by a `RATCHET` map in that script — **drained to zero and the mechanism deleted** in #2190 + #2197. The gate is now a plain tree-wide check: any violation, in any scanned file, fails. There is no cap to raise and no list to add to; fix the file.

Two things worth keeping from the drain:

- The counts measured offending **lines**, not offending references — the scanner records at most one violation per palette name per line. The same defect cost 2 in one file and 1 in another purely because of where the formatter broke the ternary. Do not read a historical count as a reference count.
- Reading a colour from the theme is only half the fix for a **lightweight-charts** series. Its colour is applied at series *creation*, so a component that binds the theme but never re-applies on change will keep a drawn line on the old palette while a tooltip that reads the theme at render moves to the new one — the line and its own readout then disagree. Recolour live series from the `[theme]` effect (`ChartWorkspaceCanvas` does this for indicator + compare series). Recharts does not have this problem: it re-renders on prop change.

Describe these props and palettes **in prose** inside doc comments — never by quoting the literal. The gate is line-based, so a quoted example is a violation with nothing to fix (prevention log → #1908 PR-2; `Sparkline.tsx` was rewritten this way in #2190).

## A display-unit mode is a property of the AXIS, not of one series (#2209)

When a chart gains a display-unit mode — % change, rebased-to-100, log scale, currency conversion — the transform belongs to every series sharing that price scale. Teaching one overlay family and missing its sibling puts two units on one axis.

The live failure: `ChartWorkspaceCanvas` compare mode re-bases the primary series to % change. The **trend** overlays (regression + channel) branched on it; the **indicator** overlays (SMA/EMA) did not and kept plotting absolute price. The single right-hand scale then spanned both — measured −80…80 with indicators off, −200…350 with them on, squashing the compare series the operator opened the mode to read into ~15% of the canvas. An axis label of `250.00` meant percent low and dollars high with nothing marking the boundary.

Nothing fails on this: every value is a valid `number`, typecheck is blind, and lightweight-charts cannot render in jsdom, so **only looking at the browser catches it**. Rules:

- Grep every `addSeries` / `setData` call site that targets the same price scale and confirm each takes the branch.
- Derive it from ONE named flag (`compareMode`), not a re-derived `compares.length > 0` per effect — one flag is greppable, and it is a boolean rather than an array identity that changes every render.
- Pin it: assert the union range of (primary ∪ every overlay) equals the primary's range. Enabling an overlay must not widen the axis. (`ChartWorkspaceCanvas.test.tsx` → "normalized indicators — compare mode invariants".)

Same family as the prevention log's #1955 sibling-gap class, in the rendering layer.

## Chart series must not draw unreported values

A financial series (quarterly/annual statements, any discrete observation) is points, not a sampled continuum. Recharts' `monotone` curve draws a smooth path *between* reported values, so the rendered line passes through magnitudes the issuer never reported and can overshoot a local extreme — and `dot={false}` removes the only cue distinguishing data from drawing. Use the **linear** curve type, and show dots while the series is short enough for them to read as markers (~40 points; above that they merge into a band). Same gate: `charts:check`.

Continuous sampled data (intraday price, an equity curve) is not covered by this — the rule is about *discrete* series.

## Density

eBull is an operator dashboard, not a consumer app — **dense by default**. Grid/table surfaces get tight spacing + line-height. When in doubt, tighter. (See `operator-ui-conventions.md` density grid prefs.)

**Adjacent stat rows share ONE column grid** (`STAT_ROW_GRID` in `components/dashboard/StatTile.tsx`, #1908 PR-5). In editorial chrome the hairline rule is the only grouping signal there is, so two stacked stat rows on different column counts break their rules at different x-positions and the spread stops reading as one document — that, not "too much whitespace", is what "stats float in empty space" actually was on the dashboard (a 4-column summary row above a 3-column rolling-P&L row). A row with fewer tiles fills from the left and leaves trailing columns empty; it does NOT redistribute. Verify by measuring, not by eye: every tile's `getBoundingClientRect().x` must match column-for-column between the rows.

**Every stat tile is `StatTile`.** A near-copy drifts: the rolling-P&L strip had its own tile with different padding and light-only tone colours (bare `text-*` classes are invisible to the dark gate, which only checks bg / border / hover pairs). `size="md"` is how a supporting row stays subordinate to the headline row — not a re-implementation.

## Tokens + type scale

Design tokens (surface fills, border colors, the type scale) live in `tailwind.config` — extract once, reference everywhere. No magic hex/px in components. A new spacing/type value that isn't a token is a smell: add the token.

## Dark mode — non-negotiable

Every surface/token pair ships both themes and passes the `frontend/scripts/check-dark-classes.mjs` gate. No bare (light-only) surface classes.

## Standing execution rule

The above is decided. When a ticket says "visual v2 / cards / badges / chart polish / density," implement it against this skill — sub-PR it small (tokens+card → badge → chartTheme → humanized copy → density), reuse existing components, run the dark gate + typecheck + `test:unit`. Surface to the operator only a genuinely new visual identity, never the assembly of this system.
