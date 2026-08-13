# Ownership coverage banner v2 (#923, rescoped)

Status: live spec for #923 as RESCOPED by operator decision 2026-06-11.
Scope: frontend only — extract + polish the existing banner on the shipped
5-state machine. No backend change.

## Rescope rationale (operator decision 2026-06-11)

The issue asks for a 6-state machine (`no_data`, `unknown_universe`,
`partial_identifier_coverage`, `stale_category`, `issuer_does_not_disclose`,
`complete_source_universe`) and claims "backend already emits coverage.state
from #840 P1 — backend payload satisfies all 6 states". **False**: the backend
ships a 5-state machine (`no_data` / `red` / `unknown_universe` / `amber` /
`green`, `CoverageState` in `app/services/ownership_rollup.py`) with per-state
headline/body/variant already server-rendered (`_banner_for_state`). The
Phase-1 6-state vocabulary was superseded by #840's implementation (whose
Codex review separately pinned the coverage-vs-concentration split). The
issue's own out-of-scope forbids backend changes, so the literal 6-state ask
is unimplementable as written. Operator chose: **rescope to the shipped
5-state machine** (over close-as-shipped and over a backend redesign).

Recorded in `docs/settled-decisions.md` (same PR).

## What ships

1. **Extract** the `Banner` function + `_BANNER_VARIANT_CLASS` out of
   `OwnershipPanel.tsx` into `frontend/src/components/instrument/
   OwnershipCoverageBanner.tsx` (the component file the issue names), exported
   as `OwnershipCoverageBanner`. The L2 `OwnershipPage` does not render a
   banner today — unchanged (the rollup payload feeds L1 only).
2. **Glyph keyed by STATE; color keyed by server VARIANT** (Codex ckpt-1 MED
   ×2). Today `no_data` and `red` are visually identical (both
   `variant="error"`); the glyph disambiguates them. Division of ownership:
   - Glyph map is an **exhaustive** `Record<OwnershipCoverageState, string>` —
     a future backend/type state fails the FE typecheck instead of silently
     rendering glyph-less: `no_data ⊘ · red ✕ · unknown_universe ? ·
     amber ▲ · green ✓`.
   - Color renders the server `banner.variant` **verbatim** via the existing
     `Record<OwnershipBannerVariant, string>` class map — the FE does not
     re-derive variant from state, and the current green→success
     special-case is removed (state↔variant consistency is backend-owned;
     a mismatched-but-typed payload like `{state:"green",
     variant:"warning"}` renders warning colors + ✓ glyph, test-pinned).
   - Layout stays state-driven only for the compact green single-line form.
3. **A11y, exactly** (Codex ckpt-1 LOW): glyph is `<span aria-hidden="true">`,
   no `role="img"`, no `title` — headline/body remain the entire accessible
   content of the `role="status"` region; test-pinned.
4. **Per-state unit tests** in `OwnershipCoverageBanner.test.tsx`: glyph,
   variant class, `data-banner-state`, server headline/body verbatim (copy
   stays server-owned per #840 — FE must not fork copy), plus the
   mismatch-payload case. The stale "per-state assertions below" comment in
   `OwnershipPanel.test.ts` (they never existed there) is corrected to point
   here (Codex ckpt-1 LOW).
5. **Extraction cleanup** (Codex ckpt-1 LOW): no external `Banner` importers
   exist; `OwnershipPanel.tsx` drops the class map + now-unused
   `OwnershipBannerVariant` import and renders `<OwnershipCoverageBanner>` at
   its three call sites.
6. **Settled-decisions entry (required, non-code)**: "Ownership coverage
   banner = 5-state server-driven machine (#840, reaffirmed #923
   2026-06-11)" — the 6-state Phase-1 design is recorded as superseded so the
   next reader does not re-litigate it.

## Out of scope

- Backend state machine changes (any new state = new spec + new ticket).
- Storybook (not wired in this repo).
- Playwright snapshots (no Playwright harness — vitest as in #920-#922).
