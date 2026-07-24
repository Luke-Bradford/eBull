# frontend/design-system

## When to use

Read before any change to eBull's visual system — surfaces/cards, badges/pills, chart theming, spacing/density, typography, or Tailwind tokens. These are **standing operator decisions**, not per-ticket taste calls. Assembling the system below is **engineering, not a taste-gate** — decide and build; do NOT open a "which look do you prefer?" loop. Only a genuinely NEW visual identity (new brand palette, a new elevated-surface language) escalates to the operator.

> Companion: `operator-ui-conventions.md` owns *presentation* (formatters, color semantics, copy). This file owns the *surface + component system*. Don't duplicate.

## Surface model (the #691 line) — SETTLED

There is **no card/surface decision in `docs/settled-decisions.md`**. The governing history is operator taste in **#691**, which killed *shadowed, elevated "Trello card"* surfaces — it did **not** ban flat surfaces. So the standing surface system is a **hybrid**:

- **Flat-hairline base** — lists, tables, narrative sections. Border/hairline separators, no fill, no elevation. This is the default; most surfaces are this.
- **One flat card variant** — for **stat tiles and chart tiles only** (the things that otherwise "float in empty space"). Spec: subtle fill (`bg-slate-900/40` dark / `bg-slate-50` light equivalent), `1px` border, `rounded`, **NO shadow, NO elevation**. That last clause IS the #691 line — hold it. Never add `shadow-*` or a raised/Trello look to a card.

Decision rule: **narrative/list → flat-hairline; stat/chart tile → the flat card variant.** Nothing else gets a third surface. (Frame any v2 work as "completing #691's surface system," not "reversing a decision.")

## Badge / pill — ONE component

One `Badge` component; never hand-roll an inline pill. Meaning is carried by **text, never color alone** (a11y + operator-ui color semantics). Color is the decorative reinforcement of the text, drawn from the `operator-ui-conventions.md` color table (red=risk, amber=warn, emerald=ok, blue=neutral). Consolidate any inline `rounded bg-… px-… text-…` pill into `Badge`. New status chips go through `Badge`, full stop.

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
