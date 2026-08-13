# Blockholder 13D/G group collapse — count a Rule 13d-5 group once (#1645)

Part of epic #788 (ownership data-trust audit). Sibling of #1640 (owner-once),
#1639 (13F-NT supersession), #1644/#1649 (institutional family identity). Same
contract: a single beneficial-ownership unit counted once, losers preserved as
`dropped_sources`, the figure-changing fold surfaced as a structured
`corrections_applied[]` entry (#1647).

## Bug

Members of a Schedule 13D/G **group** (Rule 13d-5(b)) each report the **identical
aggregate group stake** — by law, each member is deemed to beneficially own all
shares held by the group. They file on **separate accessions** with **distinct
reporter CIKs**, so:

- `_dedup_within_source` (per-CIK) keeps all N rows — different CIKs.
- `_reconcile_owner_once` (per-CIK identity) keeps all N — different CIKs.
- The N members are **summed** in the blockholders pie wedge → an N× count.

### Dev evidence — the in-scope shape (pure-blockholder co-investor groups)

Rendered rollups where the duplicate stake lands in the **blockholders** slice
and neither member is reconciled to another channel:

| symbol | instrument | members (13D/G) | each reports | current blockholders | after fix |
|---|---|---|---|---|---|
| SKE | 15224 | Orion Resource Partners; Selwyn Lower Holdings | 7,720,340 | 15,440,679 (12.7%) | 7,720,340 (6.4%) |
| RIME | 1049230 | L1 Capital Global; S.H.N. Financial | 6,016,847 | 12,033,694 (82.1%) | 6,016,847 (41.1%) |
| BTAI | 10708 | Oaktree Capital Group Holdings GP; Qatar Investment Authority | 2,724,075 | 5,448,150 (17.6%) | 2,724,075 (8.8%) |

(TKO, instrument 6908 — the issue's headline example — currently renders
`no_data`: it has no shares-outstanding figure on file, so the rollup
short-circuits before computing slices. The 282M was measured from raw
`ownership_blockholders_current`. The denominator gap is #1581-class, separate.)

## Why `member_of_group` is NOT the key (investigated, rejected)

The handoff hypothesized capturing the parsed `<memberOfGroup>` Item-2 checkbox
into the schema and keying on it. Empirically falsified on dev:

1. **Absent in source.** The TKO group accessions' `primary_doc.xml`
   `<reportingPersonInfo>` blocks carry only `reportingPersonCIK`,
   `reportingPersonNoCIK`, `reportingPersonName`, `aggregateAmountOwned`,
   `typeOfReportingPerson` — there is **no `<memberOfGroup>` element**. The stored
   `blockholder_filings.member_of_group` is blank for every TKO row.
2. **Noise where populated.** Across all `blockholder_filings`: blank 2410,
   `'b'` 1114, `'a'` 750. Both `'a'` and `'b'` appear for genuine groups (CPP /
   Ontario Teachers / Triton = `'a'`; Amentum fund + principal = `'b'`).

Conclusion: `member_of_group` cannot gate the collapse. No schema column, no
parser change, no backfill. This is a **pure read-path** rollup change (mirrors
#1644).

## The key — near-equal aggregate, same period, non-round

Rule 13d-5 makes each member report the **same** group block. The truthful,
already-stored signal: **multiple distinct-CIK blockholder rows for one issuer,
same `period_end`, whose `aggregate_amount_owned` cluster within a tight
tolerance, where the shared figure is improbable-by-chance (non-round).**

Predicate for a set of `ownership_blockholders_current` rows (already scoped to
one instrument by the rollup):

0. **Exclude survivor-overlapping members up front.** Any blockholder whose
   identity key is in `survivor_keys` (a CIK that also filed Form 4 / Form 3 /
   13F / matched DEF 14A — i.e. appears in the non-blockholder candidate set
   `_reconcile_owner_once` will process) is removed from the clustering input and
   passed through untouched. owner-once already reconciles that CIK's 13D against
   its other channels by identity; folding it into a different rep would orphan
   those rows and double-count (Codex ckpt-1 HIGH-3). This also means the rep is
   always a clean blockholder-only CIK, so its `winning_accession` provenance
   matches its `shares` (no figure-vs-provenance mismatch — Codex ckpt-1b MED).
   Consequence: an all-insider group (TKO) yields an empty cluster → untouched
   here, left to owner-once + #1652.
1. **`period_end IS NOT NULL` and equal** across members. Rows with a null
   period are never clustered. Group members file on the same trigger event;
   empirically, relaxing to ±14d adds only ambiguous microcap cases, never a
   high-value group, so strict equality is the conservative choice. (Weaker
   signal for annual 13G calendar dates — mitigated by the tight tolerance +
   non-round guard below; see Residuals.)
2. **`shares > 0`, distinct non-null reporter CIK**, and **within the member-count
   tiered tolerance of the cluster maximum**, computed by *descending-greedy,
   max-anchored* clustering: sort the period's rows by shares descending; each
   still-unused NON-ROUND row (in descending order) seeds a cluster and pulls in
   every still-unused **non-round** row whose shares are `≥ seed.shares ×
   (1 − _GROUP_REL_TOL)` (within the loose band *of that seed*, the cluster max). A
   round row is skipped as a member too (not just as a seed) so it cannot inflate a
   cluster past the 2-member gate (Codex ckpt-2). A row below the band
   seeds its own later cluster — so a ladder `10,000,007 / 9,995,000 / 9,991,000 /
   9,980,000` at 0.1% partitions as `{a,b,c}` (all within the max's band) then
   `{d}` (0.2% below a, outside it — even though it is within 0.11% of c, which a
   transitive chainer would merge). The `> 0` guard precedes the division.
   **Tiered collapse decision** (Codex ckpt-2): a cluster collapses if it has **≥3
   members** (the loose band already applied — a 3-way same-period non-round
   coincidence is negligible; this catches the TKO sliver, 0.064%) **OR exactly 2
   members within the tighter `_GROUP_PAIR_REL_TOL`** (a 2-member near-equal is the
   coincidence-prone case → demand near-exact: every genuine 2-member group on dev
   is exact or within ~1 share). So 2.50M-vs-2.48M (0.8%) and 5,001,999-vs-5,001,234
   (0.0153%) both stay separate; SKE's 1-share pair collapses.
3. **Cluster size ≥ 2** distinct non-null CIKs (null/empty-CIK rows are excluded in
   predicate 0 — a group inference needs distinct CIK evidence). A lone filer is a
   one-element cluster, never collapsed.
4. **Round-lot coincidence guard.** A round seed (cluster max a whole multiple of
   `_ROUNDNESS_UNIT`) NEVER anchors a cluster — it passes through standalone and the
   next non-round row seeds the next cluster, so a round value cannot swallow a
   genuine non-round sub-group and then dissolve it (Codex ckpt-2 HIGH). Two
   independents matching on a round lot (700,000; 1,000,000; 100,000) stay separate;
   a genuine group on a round figure stays double-counted (conservative direction).

Constants (calibrated against the full dev blockholder set — see Validation):

- `_GROUP_REL_TOL = Decimal("0.001")` (0.1%) — the cluster band + ≥3-member collapse
  gate; catches the TKO 3-member sliver (0.064%) with margin.
- `_GROUP_PAIR_REL_TOL = Decimal("0.00001")` (0.001%) — the tighter 2-member gate;
  ~77× above SKE's 1-share spread, ~15× below the loosest synthetic false pair.
- `_ROUNDNESS_UNIT = 100_000` shares.

### Collapse action

Each qualifying cluster collapses to ONE `Holder`:

- `shares = MAX(member shares)` (Rule 13d-3 total beneficial ownership) — the
  cluster seed.
- **Representative = the cluster max-share member** (the seed), tie-broken
  `(shares, filer_cik, winning_accession)` deterministically. Because
  survivor-overlapping members were excluded up front (predicate 0), the rep is a
  clean blockholder-only CIK, the rep IS the max-share member, and its
  `winning_accession`/`winning_edgar_url`/`as_of_date` are the genuine provenance
  of the surviving `shares` (no figure-vs-provenance mismatch).
- The rep's `filer_cik` / `filer_name` / `winning_source` / `winning_accession` /
  `winning_edgar_url` / `as_of_date` identify the collapsed holder; its `shares`
  is the cluster MAX (= the rep's own shares).
- Non-rep members → `DroppedSource` entries **appended to** the rep's existing
  `dropped_sources` (the amendment-chain provenance from `_dedup_within_source`
  is preserved, never replaced).
- One `CorrectionApplied(kind="blockholder_group_collapse")`:
  `shares_removed = Σ(non-rep member shares)` (the eliminated double-count);
  `filer_cik`/`filer_name` = rep; `winning_source`/`winning_accession` = rep
  (the surviving figure's provenance); `source_channel` = rep source (this is an
  *intra*-blockholder-channel collapse, so winning == folded channel — the
  per-member fold detail lives in `detail` + the rep's `dropped_sources`).

## Placement in the rollup pipeline

`get_ownership_rollup` (read-only, inside `snapshot_read`):

```
block_candidates → _dedup_within_source                          → blockholders
_reconcile_institutional_families(survivors, blockholders, …)    → survivors, rest_blockholders
survivor_keys = { identity_key(h) for h in survivors }
_reconcile_13d_groups(rest_blockholders, survivor_keys)          → grouped_blockholders, group_corrections   ← NEW
_reconcile_owner_once(survivors + grouped_blockholders)
corrections_applied = (*notice_suppressions, *family_corrections, *group_corrections)
```

Runs AFTER family-reconcile (curated Vanguard/BlackRock 13Gs already pulled out; a
13D control group is never a curated family) and BEFORE owner-once (the group is
one unit before per-CIK reconciliation). `survivor_keys` is the identity-key set
of the non-blockholder candidates; `_reconcile_13d_groups` uses it only to
*exclude* overlapping blockholders from clustering (predicate 0) — those, plus
every non-clustered blockholder, flow through untouched (zero regression).

## Out of scope — the Form 4 cross-channel explosion (file a follow-up)

A SECOND, larger double-count exists and is **deliberately not** addressed here:
in a sponsor-controlled issuer, every entity in a PE fund's GP/LP chain files its
own **Form 4** reporting the **same indirect-beneficial block**, and group members
also restate it on **13D + DEF 14A**. `_reconcile_owner_once` collapses each CIK
once but cannot see that dozens of related CIKs restate one block, so the
**insiders** slice explodes:

- AMTM (Amentum, 8823): insiders **380%** of float — Lindsay Goldberg IV L.P. /
  IV-A / IV-PCF … each Form-4-report 45,026,743.
- VSAT (Viasat, 8988): insiders **116.5%** — Warburg Pincus entities each 8,390,687.
- TKO (6908): all three 13D members also carry Form 4 indirect-beneficial
  fragments (24.4M, 4.16M, 2.16M restated by Whitesell / Durban / VoteCo / HoldCo II).

This is a *cross-channel related-entity beneficial-restatement* problem (related
CIKs, Form 4 indirect, joint-filing chains not linked in our data), distinct from
"13D group members each report the full stake in the blockholders wedge." It needs
its own mechanism (an inferred cross-channel group identity, akin to the curated
institutional-family registry but for control groups) and its own validation.
**Filed as #1652.** This PR fixes the blockholders-wedge double-count it can fix
cleanly and correctly. It does not *attempt* the insider explosion and does not
regress it: any group member that also files Form 4 / 13F / matched DEF 14A is
excluded from clustering (predicate 0) and left exactly as owner-once handles it
today — so an all-insider control group like TKO is untouched by this pass, and a
mixed group has only its non-overlapping blockholder members collapsed. No claim
is made that owner-once reconciles the whole group across channels — that is #1652.

## Surfacing

- **`corrections_applied[]`**: new kind `blockholder_group_collapse` added to the
  closed-vocab docstring (`ownership_rollup.CorrectionApplied`), the API Pydantic
  `Literal` (`app/api/instruments.py::_CorrectionAppliedModel.kind`), and the FE
  union (`frontend/src/api/ownership.ts::OwnershipCorrectionKind`). All three or
  Pydantic rejects the kind at the boundary.
- **CSV export** (`build_rollup_csv`): one `__group_collapse:<rep_cik>__` memo row
  per correction (mirrors `__family_fold:__`), excluded from any `SUM(shares)`
  reconciliation (the eliminated shares were never real).

## Validation (dev, full blockholder set, 0.1% tolerance + non-round)

A pairwise near-equal scan over all `ownership_blockholders_current` returns
clusters on ≈15 instruments. Under the predicate:

Verified by running `get_ownership_rollup` against dev with the patch (the
"rendered" column is the live result, not a logic-level prediction):

| Cluster | Figure | Round? | Rendered result |
|---|---|---|---|
| SKE Orion / Selwyn | 7,720,340 | no | blockholders 15,440,679 → **7,720,340** + 1 correction |
| RIME L1 / S.H.N. | 6,016,847 | no | blockholders 12,033,694 → **6,016,847** + 1 correction |
| BTAI Oaktree / QIA | 2,724,075 | no | blockholders 5,448,150 → **2,724,075** + 1 correction |
| ANGH (Maroun / Habib) | 2,486,052 | no | the 2-member group **collapses** (1 correction); the unrelated larger blockholder is untouched |
| DCX (Fung Ko / Peng Jia) | 1,000,000 | **yes** | kept separate (round); both survive among 5 holders, 0 collapses |
| USAR / QNRX / MDRR | — | — | members also file 13F/Form 4 → **excluded** (predicate 0), 0 collapses |
| TKO / NA (Golden Forest/Longling) | — | — | `no_data` (no shares-outstanding on file) → rollup short-circuits, nothing rendered |
| 1052043 (Ivy 2.50M vs Richard 2.48M) | — | — | kept separate (0.8% > tolerance) |

Standard panel (AAPL / GME / MSFT / JPM / HD): zero `blockholder_group_collapse`
corrections (no spurious collapse).

Cross-channel cases (members also insiders → block dominated by Form 4) are
*not the target*: their members are excluded from clustering by predicate 0, so
this pass leaves them untouched for the #1652 follow-up — AMTM, VSAT, TKO, QNRX.

## Residuals (documented, not silently dropped)

- A genuine group on a **round** shared figure stays double-counted (roundness
  guard's conservative direction).
- A group whose members carry **different** `period_end`s (staggered amendments)
  is not collapsed. None observed on dev within ±14d.
- **Not a zero-false-positive guarantee.** The 2-member coincidence risk is the
  sharpest; it is bounded by the tight `_GROUP_PAIR_REL_TOL` (0.001%): two genuinely
  independent same-date holders would have to match within ~1 share per 100k to
  collapse — improbable, and none occurs in the dev set. The looser 0.1% band only
  applies once ≥3 distinct CIKs already coincide (negligible). The guard is
  conservative, not infallible.
- A group whose members are also insiders is only partially addressed (the
  blockholders-wedge piece); the Form 4 explosion is the out-of-scope follow-up.

## Tests

Pure-logic table tests on `_reconcile_13d_groups` + `_is_group_block` with the real
dev values:

- SKE/RIME/BTAI 2-member collapse to the single MAX with one `DroppedSource` + one
  `blockholder_group_collapse` correction (`shares_removed` = folded member); 3-member
  (TKO shape) collapse with two dropped sources + `shares_removed` = sum of losers.
- round-lot exact match (700,000) kept separate; `_is_group_block` truth table.
- **2-member loose pair** (5,001,999 vs 5,001,234, 0.0153%) kept separate (Codex
  ckpt-2 MED — within the loose band but outside the tight 2-member gate); 0.8%
  distinct values kept separate; lone filer untouched.
- **round seed does not dissolve a non-round sub-group** (Codex ckpt-2 HIGH): input
  `[1,000,000 round, 999,999, 999,998]` → round value standalone + the near-exact
  pair collapses.
- **null-CIK excluded** (Codex ckpt-2 MED): two identical-aggregate null-CIK rows
  pass through, no collapse.
- a member whose key ∈ `survivor_keys` is excluded from clustering and passes
  through (full + partial-overlap variants).
- rep is the max-share member; the collapsed holder's `winning_accession` is the
  max member's; existing `dropped_sources` are preserved (appended, not replaced).
- **max-anchored ladder**: `10,000,007 / 9,995,000 / 9,991,000 / 9,980,000` at 0.1%
  partitions as the 3-member `{a,b,c}` collapsed + `{d}` untouched (d within 0.11% of
  c but 0.2% of the max a) — asserts the precise partition, not merely "rejects chain".
- a null-`period_end` row and a `shares ≤ 0` row are never clustered.

End-to-end is exercised on the live dev rollup (SKE/RIME/BTAI/ANGH render the collapse
+ the `blockholder_group_collapse` correction; the standard panel + negatives do not).
