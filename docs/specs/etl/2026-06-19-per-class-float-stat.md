# Per-class float value as a separate labelled stat (#1665)

Follow-up to #1662 (PR #1666). Refs #1662, #1664.

## Problem

Issue #1662 made `identity.market_cap` the **total company** capitalization
(Σ class×price, identical across GOOG/GOOGL). The per-class **float value**
(GOOGL Class A shares × Class A price ≈ $2.16T) is a genuinely distinct,
meaningful number — the tradable-class market value, what a this-ticker view
wants — but it is NOT "market cap" and must stay clearly separated from it
(conflating them is exactly the #1662 bug). Today that per-leg figure is
computed inside `_assemble_total_company_cap` and then **discarded** (only the
aggregate `value` survives on `TotalCompanyMarketCap`).

## Source rule

No SEC reg governs a *display* label. The data-treatment rule IS #1662 /
data-engineer invariant **I20** (`.claude/skills/data-engineer/SKILL.md`) /
prevention-log §"Market cap of a multi-class issuer is the total company …"
(`docs/review-prevention-log.md:1863`): "per-class float is a separate,
separately-labelled stat." This ticket implements the second half of that
settled sentence. Per-class shares are the curated #1623 FSDS table
`instrument_class_shares_outstanding`; price is the same `quotes` mark hierarchy
`compute_market_cap` / `_latest_price` use.

## Full-population verification (dev DB, 2026-06-19)

Curated dual-class universe = 3 issuers / 6 instruments (every CIK in
`instrument_class_shares_outstanding`):

| symbol | class member | shares | price | class market value |
|---|---|---|---|---|
| GOOGL | CommonClassA | 5.835B | 369.20 | **$2.1543T** |
| GOOG  | CapitalClassC | 5.515B | 357.23 | **$1.9701T** |
| HEI / HEI.A | — | 55.0M / 83.9M | none | — (no price → null) |
| METC / METCB | — | 43.8M / 9.5M | none | — (no price → null) |

GOOGL $2.1543T matches the issue's ≈$2.16T. Per-class value is **per-leg**
(NOT identical across siblings — the whole point): GOOGL $2.15T vs GOOG $1.97T,
while `market_cap` (total) is $4.4476T on both. HEICO/Ramaco have no quote on
file → their total fails closed today and the per-class value is honestly null.

The dev DB IS the operational population here (demo-first, single-operator; no
separate prod corpus — settled "Risk posture: Demo-first"). The curated set the
safety rests on is the #1623 CIK→class map in
`instrument_class_shares_outstanding`, which is environment-independent: any
deployment reads the same curated table, so this full-pop scan generalizes.

## Design — PURE READ-PATH

The per-class float for the viewed instrument = its OWN leg's `shares × price`.
That leg is already built inside `_assemble_total_company_cap`; carry it out and
let `resolve_market_cap_basis` (which knows `instrument_id`) pick the matching
leg. Reusing the assembled leg guarantees the displayed per-class value is the
EXACT leg the total summed — identical shares, identical price, identical
`class_shares_usable` filter — so the class value can never disagree with its own
contribution to `market_cap`. (It does NOT guarantee cross-class quote-timestamp
coherence: `_latest_price` reads each sibling's newest quote independently, same
as today's total. That is acceptable — both figures already inherit it.) A
separate re-read of shares+price for the per-class value, by contrast, could pick
a different tick or a row the total's usability filter rejected, publishing a
class value incoherent with the total it sits beside.

1. `TotalCompanyMarketCap`: add `legs: tuple[_ClassLeg, ...]` (the priced traded
   legs; the imputed residual class has no instrument and is excluded).
2. `_assemble_total_company_cap`: set `legs=tuple(legs)` on the return. No logic
   change — the legs already exist at that point.
3. `MarketCapResolution`: add `class_market_value: Decimal | None = None`.
4. `resolve_market_cap_basis`: when `basis == "total_company"`, select the leg
   whose `instrument_id == instrument_id` → `class_market_value = shares × price`.
   The FSDS table PK is `(instrument_id, period_end)`
   (`sql/200_instrument_class_shares_outstanding.sql:47`) and `_build_total_company_cap`
   reads a single `period_end`, so **at most one** leg can match a given
   instrument — the pick is exactly-one-or-none, not a sum. Defensive: iterate and
   take the first match (a duplicate-ID pure test locks that a hypothetical
   double-ID `legs_raw` does not double-count). `None` when the viewed instrument
   is not a priced leg (e.g. a same-CIK `.US` listing with no FSDS class row).
   Naturally null for `not_multiclass` (single-class: `market_cap` already IS the
   sole class value) and `multiclass_unavailable` → scopes the new stat to curated
   dual-class only.
5. API `_build_instrument_summary` (instruments.py ~3480): set
   `identity.class_market_value = cap_resolution.class_market_value`.
6. `InstrumentIdentity` (py + TS `types.ts`): add `class_market_value: Decimal |
   None` / `string | null` — required-nullable, mirroring `market_cap`.
7. FE `KeyStatsPane.tsx`: add a row right after "Market cap", labelled
   `{symbol} market value` (e.g. "GOOGL market value"), formatted by the existing
   `formatMarketCap`. `makeRow` returns null when the value is null → the row
   simply doesn't appear for single-class issuers.

### Label tradeoff

Label by the viewed **symbol** ("GOOGL market value"), not a parsed class name
("Class A"). The viewed ticker IS the share class, so the symbol is an
unambiguous anchor and needs zero FSDS-token humanization (the member tokens —
`CommonClassA`, `CapitalClassC`, `HeicoCommonStock` — are not a controlled
vocabulary; a humanizer is a new failure surface for marginal gain). A
"Class A" label is a future polish with a controlled map, not this ticket.

## Out of scope

- No schema / migration / backfill / job change (pure read-path).
- No new "is dual-class?" predicate — `total_company` basis already encodes it.
- Ranking view (#1664) is unaffected — float value is display-only, never a
  scoring input.

## Tests

- Pure: `_assemble_total_company_cap` returns legs; a 2-leg fixture exposes each
  leg's `shares × price`; residual leg excluded from `legs`.
- Pure coherence invariants on the returned legs: each leg value `> 0`; Σ leg
  values `== total.value - residual_shares × impute_price` (residual excluded);
  a picked class value equals exactly one leg's `shares × price` and is
  `<= total.value`.
- Pure: leg-pick — duplicate-ID `legs_raw` takes the first match (no
  double-count); viewed-instrument-absent → None.
- Service (DB-backed, `-m db`): `resolve_market_cap_basis` for a GOOGL-shaped
  fixture sets `class_market_value` to that sibling's leg; a single-class
  instrument leaves it None. Mirrors the existing `resolve_market_cap_basis` tests.
- FE: `KeyStatsPane.test.tsx` — row renders when `class_market_value` set, absent
  when null.

## DoD

Pure read-path display change (no ownership/observations data treatment, no
parser/schema). Smoke the panel via the live endpoint: GOOGL/GOOG show both
`market_cap` (4.4476T) and `class_market_value` (2.15T / 1.97T); AAPL (single
class) shows market cap only, no per-class row.
