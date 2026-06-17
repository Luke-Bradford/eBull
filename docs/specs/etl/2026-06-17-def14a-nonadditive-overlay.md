# DEF 14A beneficial ownership → non-additive memo overlay (#1659)

**Status:** spec → Codex ckpt-1 → impl
**Type:** PURE READ-PATH (rollup read model only — no migration / parser / ingest / backfill)
**Epic:** #788. Supersedes the #1659 name-keyed-collapse heuristic. **Reverses #1627** (which made
`def14a_unmatched` an additive `pie_wedge`).

## Why (the researched correct outcome, not a heuristic)

The DEF 14A "Security Ownership of Certain Beneficial Owners and Management" table is, by SEC
design, a **Rule 13d-3 deemed-ownership disclosure where the same securities are listed under
multiple owners** — control groups, parent/sub, spouse attribution, "all officers as a group"
aggregates. **SEC Item 403** (17 CFR 229.403): *"Where more than one beneficial owner is known to
be listed for the same securities, appropriate disclosure should be made to avoid confusion."*
**Rule 13d-3/5**: a group's shares are *"aggregated once at the group level … count each share
only once."*

eBull's own settled invariants already say proxy figures are **restatements, not independent
additive holdings** — data-engineer **I14** ("MAX overlapping restatements … DEF 14A
beneficial/voting … same shares, different lens") and **I16** ("Proxy 5%-holder is a
family-consolidated restatement, **NOT an independent additive wedge**").

So summing the unmatched proxy rows into an additive `def14a_unmatched` pie wedge (#1627) is a
**category error**. The real holders it lists are already captured + de-duplicated via 13D/G
(blockholders), 13F (institutions), Form 4 (insiders). The genuinely-unmatched proxy rows are
overwhelmingly deemed-groups, "all-officers" aggregates, parent/sub restatements, and parser
fragments — almost none are *new additive* shares. The disambiguating signal (the footnote "also
deemed to own shares held by X") is **free-text prose**, not structured in EDGAR / edgartools /
our parser — so no read-path heuristic (the retired #1659 plan) can safely cluster them
(verified: dev FPs BXP "Koop"+"LaBelle" @1,875,000, FOXA "Ciongoli"+"Tomsic" @2,750,000 are
independent equal-grant executives; CIK absent, `holder_role` does not separate them).

## Decision

**Demote the `def14a_unmatched` slice from an additive `pie_wedge` to a non-additive memo
overlay** (the pattern already used for funds/N-PORT, #919). This dissolves the ENTIRE proxy
double-count class — control groups, multi-nature dups, "all-officers" aggregates, parent/sub —
in one principled move, instead of chasing each with a separate unsafe heuristic. The proxy
holders still RENDER (as a cross-check overlay + the L2 `?category=def14a` view); they simply no
longer ADD to the pie / residual / concentration / denominator.

New `denominator_basis` value **`"proxy_disclosure"`** (distinct from funds' `institution_subset`
— def14a is not a subset of institutions; it is a Rule 13d-3 deemed-ownership disclosure). All
additive math already filters `denominator_basis == "pie_wedge"`, so the slice drops out
automatically; all memo handling filters `!= "pie_wedge"`, so it surfaces as an overlay
automatically.

## Backend (`app/services/ownership_rollup.py`)

1. `DenominatorBasis` Literal (l.76) → add `"proxy_disclosure"`.
2. The `def14a_unmatched` slice build (~l.1928) → `denominator_basis="proxy_disclosure"`.
3. `max_pie_holder_shares` for the per-class denominator guard (~l.3096) → **drop the
   `+= [c.shares for c in unmatched_def14a]` term** (proxy rows are no longer pie holders; a
   deemed/overlapping proxy figure must not veto the per-class denominator).
4. `_compute_residual` (l.2064) / `_compute_concentration` (l.2085) — already filter
   `== "pie_wedge"`; def14a auto-excluded. NO change (assert in tests).
5. CSV `build_rollup_csv` — `pie_slices` / `memo_slices` split (l.3253) already keys on basis;
   def14a rows move to the memo block automatically (excluded from the SUM(shares) invariant).
   Verify the memo block renders proxy rows (it iterates `memo_slices`).
6. Slice `label` stays "Proxy-only (DEF 14A)"; the FE prefixes "Memo:".

## API (`app/api/instruments.py`)

- `_OwnershipSliceModel.denominator_basis` Literal (l.4250) → add `"proxy_disclosure"`.

## Frontend

- `frontend/src/api/ownership.ts` — `OwnershipDenominatorBasis` union → add `"proxy_disclosure"`.
- `OwnershipPanel.tsx`:
  - Overlay filter (l.391-393): `=== "institution_subset"` → `!== "pie_wedge"` (render any
    non-additive slice as a memo overlay; the comment at l.471 already anticipates this).
  - Remove `def14a_unmatched` from the additive ring flatten (l.205) + the ring totals
    (`def14a_total`/`def14a_as_of` → the slice no longer feeds the sunburst; it renders only in
    the overlay). Remove `def14a_unmatched` from `_CATEGORY_ORDER_TABLE` (l.382; harmless either
    way since the pie filter already excludes it, but keep it honest).
  - `OverlaySection` (l.475): fill the non-funds (`else`) copy for the proxy overlay — "DEF 14A
    proxy beneficial-ownership: a Rule 13d-3 disclosure where the same shares are listed under
    multiple owners (deemed/overlapping), shown as a cross-check, not added to the pie."
- `ownershipRings.ts` — the `def14a` ring (l.301-309) goes dead once `def14a_total` is null;
  update the "additive, already in the server" comment (l.112) to "non-additive overlay (#1659)".
- `OwnershipPage.tsx` (L2) — keep the `?category=def14a` drilldown VIEW (a per-category holder
  table is a legitimate view, not aggregation), but exclude `def14a_unmatched` from the AGGREGATE
  sunburst composition (map it to the `cat === null` overlay path at ~l.628, mirroring funds).

## Tests

- Backend: a rollup with a def14a_unmatched slice → that slice has `denominator_basis ==
  "proxy_disclosure"`, is EXCLUDED from residual + concentration (the known % drops vs the old
  additive behaviour), and the per-class denominator guard ignores it. Flip the existing tests
  that assert def14a in the pie sum / additive concentration.
- CSV: def14a rows appear as memo rows, excluded from the SUM(shares) pie invariant.
- FE: def14a slice renders in the overlay section, not the pie table / sunburst (vitest).

## Dev-verify (panel + the evidence instruments)

AMTM / QXO / ROL / RKT (formerly inflated proxy wedges) — confirm the def14a wedge leaves the pie
and the concentration "known %" drops to the additive (13F+13D+Form4) figure; panel
AAPL/GME/MSFT/JPM/HD — confirm no pie/residual regression (their pies were already
13F/13D/Form4-dominated). Hit `/instruments/{symbol}/ownership-rollup` + the L1/L2 chart.
