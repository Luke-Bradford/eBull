# Ownership machine-trust envelope (#1647 PR-A)

Epic #788 / machine-trust contract #1647. PURE READ-PATH. No schema, no
migration, no parser, no backfill, no `sec_rebuild`, no jobs-proc restart.

## Scope (operator decision 2026-06-16: "Envelope now, ratio→#790")

Ships the fully-feasible half of the #1647 contract on
`GET /instruments/{symbol}/ownership-rollup`:

1. **Part 1 — per-slice as-of coherence.** Each slice exposes the as-of
   span of its deduped holders so a consumer can see it is summing across
   quarters (98.2% of dev instruments mix ≥2 13F quarters in the
   institutions slice).
2. **Part 4 — sanity invariants.** Top-level raw facts a consumer/agent
   can reason over to catch the *next* silent inflation bug. No hard
   pass/fail thresholds — facts only.
3. **Part 2 (shippable half) — honest `is_estimate` machine flag.** Per
   category. `true` when there is no real universe estimate (current Tier-0
   state: every category NULL → `unknown_universe`). The real
   `coverage_ratio` stays blocked on the per-instrument 13F universe-count
   ingest → **DEFERRED #790**.

Already shipped, NOT in scope: Part 3 structured `corrections_applied[]`
(#1639 first producer). Part 5 cross-source validation = lower priority,
deferred.

## Guardrail (settled-decisions.md:702-720)

The 5-state coverage banner is server-driven; adding a *state* needs a new
spec+ticket. This PR adds only **additive contract fields** — no new banner
state, no FE-only remap. `is_estimate` is derived from the existing
`unknown_universe`/`no_data` states, not a new state.

## Definitions

`as_of_date` = financial statement period end (settled-decisions.md:104).
For 13F holders this is the quarter-end (`periodOfReport`); for Form 4 the
transaction date; for 13D/G the event date.

Calendar quarter: `_calendar_quarter(d) = (d.year, (d.month - 1) // 3 + 1)`
→ quarter `1..4` (human convention). 13F quarter-ends
(03-31/06-30/09-30/12-31) map cleanly to Q1..Q4.

## Contract additions

### Per slice (`OwnershipSlice`, computed in `_build_slice` from holders)

```
as_of_min: date | None        # min non-null holder as_of_date
as_of_max: date | None        # max non-null holder as_of_date
distinct_quarters: int        # count distinct calendar quarters over non-null as_of
mixed_period: bool            # distinct_quarters > 1
```

Holders with NULL `as_of_date` are ignored for all four (a slice of only
NULL-as_of holders → min/max None, distinct_quarters 0, mixed_period
False). All four are intrinsic to the slice's holders — computed once in
`_build_slice`, so every slice (pie-wedge + memo overlays) carries them.

**Family blind-spot fix (Codex ckpt-1 High).** A collapsed institutional
family (#1644/#1649) is one synthetic `Holder` whose `as_of_date =
min(member dates)`, but its `family_members` may span quarters. So
`_slice_coherence` gathers, per holder, `holder.as_of_date` **plus every
`family_member.as_of_date`** — otherwise a Vanguard family spanning Q2+Q3
would falsely read single-quarter. The dedup `dropped_sources` are NOT
gathered (a dropped loser is not part of the counted figure's as-of span).

### Per category (`CategoryCoverage`)

```
is_estimate: bool             # estimated_universe is None
```

Honest machine flag: `true` means "no real filer-universe estimate exists
for this category, treat the figure as a floor not a measured share."
Derived in `_compute_coverage` as `estimated_universe is None` (Codex
ckpt-1 High). This is equivalent to `state in {unknown_universe}` for the
normal path but avoids coupling to a state and correctly treats a real
seeded `estimate == 0` as NOT an estimate (`is_estimate=False`,
vacuously-green). The `no_data` rollup path bypasses `_compute_coverage`
and ships `coverage.categories = {}` (no per-category flags) — unchanged;
the top-level `coverage.state == "no_data"` already signals untrustable.

### Top-level (`OwnershipRollup.sanity: SanityChecks`)

Raw facts over pie-wedge slices only (memo overlays excluded — they are
already-counted detail). No thresholds.

```
max_distinct_quarters: int        # max slice.distinct_quarters over pie-wedge slices
institutions_pct: Decimal         # Σ pie-wedge slices in {institutions, etfs} total / outstanding
institutions_over_100pct: bool    # institutions_pct > 1
largest_single_holder_pct: Decimal# biggest single deduped pie-wedge holder / outstanding
any_pie_slice_over_100pct: bool   # any pie-wedge slice pct_outstanding > 1
```

`institutions_pct` explicitly filters `denominator_basis == "pie_wedge"`
(Codex ckpt-1 Medium) so a future memo overlay tagged institutions/etfs
cannot leak into the bound. `outstanding <= 0` → all pct fields `Decimal(0)`,
both booleans `False`.

`largest_single_holder_pct` covers the committee's "single-family
plausibility": a collapsed Vanguard/BlackRock family is one `Holder`
(#1644/#1649), so an implausibly large family shows here. `institutions_pct`
+ the two `over_100pct` booleans are the impossibility checks the existing
`residual.oversubscribed` guard cannot express at sub-residual granularity.

`no_data` rollups get a zeroed `SanityChecks` (all 0 / False).

## API + FE

- `app/api/instruments.py`: add the fields to `_SliceModel`,
  `_CategoryCoverageModel`, and a new `_SanityChecksModel` on
  `OwnershipRollupResponse`; map in `_rollup_to_response`
  (`app/api/instruments.py:4332`).
- `frontend/src/api/ownership.ts`: mirror the types (optional fields with
  back-compat defaults, matching the existing `denominator_basis?` pattern).
- No new FE rendering required for PR-A (contract-only). The existing panel
  ignores unknown fields. A follow-up may surface a "mixed-quarter" chip.
- CSV export (`build_rollup_csv`): add a trailing `__as_of_coherence__`
  memo row per pie-wedge slice? → NO. Keep CSV unchanged; the envelope is a
  JSON-contract concern, and the CSV already carries per-holder `as_of_date`
  for manual span checks. (Documented non-change.)

## Tests (pure-logic, no DB)

Table-test the new pure helpers:
- `_calendar_quarter` boundary months.
- `_slice_coherence(holders)` — empty / all-NULL-as_of / single-quarter /
  multi-quarter / unsorted dates.
- `_compute_sanity(slices, outstanding)` — over/under 100%, memo-overlay
  exclusion, largest-holder pick, zero-outstanding guard.
- `is_estimate` derivation across all 5 coverage states.

## DoD

Pure read-path → clauses 8-12 (ETL smoke/backfill) N/A; record that. Live
endpoint verified on dev panel (AAPL/GME/MSFT/JPM/HD) showing the envelope
fields populate (AAPL institutions `mixed_period=true`, `distinct_quarters`
≥2). Codex ckpt-1 (spec) + ckpt-2 (pre-push diff).
