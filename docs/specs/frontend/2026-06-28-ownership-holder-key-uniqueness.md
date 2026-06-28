# Ownership holder key uniqueness (#1800)

## Symptom

`/instrument/AAPL` → Research → Ownership console warning:
`Encountered two children with the same key, 0000036405`. React may drop/duplicate
holder rows on re-render (operator-visible correctness).

## Root cause

Several React/DOM keys are derived from `filer_cik`, which is **not unique**: a
*registrant* CIK fronts many distinct holder rows (N-PORT fund series under one
registrant; a recurring insider). The reported warning is the funds overlay table.

## Source rule + full-population verification

Governing rule = the rollup payload's identity structure **and** React's key-uniqueness
requirement. Verified on the live `/instruments/{AAPL,GME}/ownership-rollup` population
(not a sample — the issue's and an earlier draft spec's AAPL-only checks both produced
wrong fixes):

| slice | maxCIKmult | dup(cik,name) | note |
|---|---|---|---|
| institutions / etfs | **1** | 0 | 13F deduped per filer — never collides |
| insiders | 2 | **1–2** | same cik+name recurs; on GME the two rows share `winning_accession` too, and `Cheng Lawrence` is a **byte-identical** row |
| funds (overlay) | 43 (AAPL) | 0 | collides on cik, unique on (cik,name) |
| def14a_unmatched | 15 | 0 | collides on cik, unique on (cik,name) |

**Conclusion: no combination of data fields is provably unique** — `Cheng Lawrence`
proves two holder rows can be byte-identical. The only guaranteed-unique discriminator
for a React key is the **array position (index)**. (This falsifies the earlier draft's
`(filer_cik, filer_name, winning_accession)` key, which GME insiders collide on.)

## Constraint — the `?filer=` drill contract (the trap)

`filer_cik ?? \`name:${filer_name}\`` is **not just a React key** — it is the deep-link
token:

- sunburst leaf click → `params.set("filer", target.leaf_key)` (`OwnershipPanel.tsx:95`)
  → `?filer=<key>`.
- the L2 page resolves the highlighted row by `row.key === filerFilter`
  (`OwnershipPage.tsx:515`) and drills history via `holderIdFromFilerKey(filerFilter)`
  (`ownershipHistorySeries.ts:116`), which requires a **pure-digit CIK** (or
  `name:`/`block:`/`baseline:`).
- the executable invariant `rollupToFilerRows` keys === `rollupToSunburstInputs` leaf
  keys is asserted by `OwnershipPage.test.ts` "wedge ↔ row key parity".

Composing the **token** with name/accession/index (as the issue and the earlier draft
proposed) breaks `holderIdFromFilerKey` → the institutions/insiders/blockholders history
drill returns `no_cik`, and breaks the parity test.

## Fix — decouple React/DOM keys from the drill token

Token builders stay UNCHANGED (bare `filer_cik ?? name:` = drill token, parity preserved):
`rollupToSunburstInputs` (`OwnershipPanel.tsx:178`) and `rollupToFilerRows`
(`OwnershipPage.tsx:649`).

Make the three **render sites** unique by appending the array index — React-reconciliation
only, never navigated/parsed:

1. `OwnershipPanel.tsx:513` — funds/def14a `OverlaySection <tr>` (the reported warning;
   no navigation): `key={\`${cik??name:}:${i}\`}`.
2. `OwnershipSunburst.tsx:396` — outer-ring `<Cell key={d.id}>` (`d.id =
   leaf-<cat>-<leaf.key>`, collides on insider cik). `d.id` is React-key-only — keyboard
   targeting reads `data-idx`, the drill rides `target.leaf_key` — so `key={\`${d.id}-${i}\`}`.
3. `OwnershipPage.tsx:526` — L2 `<tr key={\`${category}-${row.key}\`}>` (collides on
   insider cik). Append `-${i}`; keep `row.key === highlightFiler` (line 515) untouched.

The legend (`OwnershipSunburst.tsx:609`) keys by **category** (`cat.key`, a
`CategoryKey` enum) — never collides; the issue's claim it collides is wrong.

## Out of scope (verified, untouched)

- `ownershipMetrics.ts:173` — deliberate *latest-per-filer* aggregation key (a dedup;
  index/accession would break it).
- The byte-identical insider duplicate row itself is a data-quality concern (#788), not a
  FE bug; the FE renders defensively whatever the rollup returns.

## Tests

- `OwnershipPage.test.ts` — existing "wedge ↔ row key parity" stays green (tokens
  unchanged). New: two same-cik insider rows survive as distinct rows, each with the bare
  cik drill key (the GME repro).
- `holderIdFromFilerKey` coverage already asserts pure-digit/`name:` resolution
  (`ownershipHistorySeries.test.ts:45`) — the contract the fix preserves.

## Verification

- `pnpm --dir frontend typecheck` + `test`.
- Live: `/instrument/AAPL` → Research → Ownership, console clean; click a sunburst wedge,
  confirm the L2 table highlights + drills the clicked filer.
