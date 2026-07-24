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

## Density

eBull is an operator dashboard, not a consumer app — **dense by default**. Grid/table surfaces get tight spacing + line-height. When in doubt, tighter. (See `operator-ui-conventions.md` density grid prefs.)

## Tokens + type scale

Design tokens (surface fills, border colors, the type scale) live in `tailwind.config` — extract once, reference everywhere. No magic hex/px in components. A new spacing/type value that isn't a token is a smell: add the token.

## Dark mode — non-negotiable

Every surface/token pair ships both themes and passes the `frontend/scripts/check-dark-classes.mjs` gate. No bare (light-only) surface classes.

## Standing execution rule

The above is decided. When a ticket says "visual v2 / cards / badges / chart polish / density," implement it against this skill — sub-PR it small (tokens+card → badge → chartTheme → humanized copy → density), reuse existing components, run the dark gate + typecheck + `test:unit`. Surface to the operator only a genuinely new visual identity, never the assembly of this system.
