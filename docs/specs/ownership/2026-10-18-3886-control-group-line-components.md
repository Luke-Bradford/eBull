# #3886 — the insider control-group collapse matches a summed observation's Table I lines

Status: spec (2026-10-18). Residual of #3227 found by the mixed-groups A/B (#3887). Rung: metric
derivation at read time (corpus rung: full-population A/B); this document is the judgement
artefact (Codex checkpoint 1).

## Problem

The rollup's control-group collapse (#1652, `_reconcile_insider_control_groups`) buckets insider
and 13D/G rows by exact `shares`. Since #3227, one filer's Form 4/3 observation can be the sum of
several Table I lines (Form 3 line sum, `I` vehicle sum, holding-only sum, mixed fold). The line
that equals another holder's block is then hidden inside the sum, the bucket loses its match, and
the block is counted twice.

SMMT, stored after #3887 (`ownership_insiders_current`, accessions `0001055919-26-000007` and
`0001507650-26-000007`, both as of 2026-06-12):

| holder | nature | shares | Table I lines |
|---|---|---:|---|
| Duggan Robert W | `direct` | 573,883,879 | one `D` series |
| Duggan Robert W | `indirect` | 36,131,930 | holdings 25,824,474 + 10,199,776 (two trusts) + 76,680 (family) + 31,000 ("By Spouse") |
| Zanganeh Mahkam | `indirect` | 609,984,809 | "By Spouse" 573,883,879 + the same two trusts + the same 76,680 |
| Zanganeh Mahkam | `direct` | 31,000 | one holding |

Before the fold, Zanganeh's `indirect` was the single "By Spouse" line, equal to Duggan's `direct`,
and #1652 counted it once (insiders 645,918,028). After it, nothing matches and the insiders
wedge is 1,255,908,837. The #3887 A/B measured 13 instruments where the collapse removes less after
the fold, 720,882,000 shares in all (issue body).

## Source rule

- **#1652's deemed-block inference** (`docs/specs/etl/2026-06-16-insider-control-group-collapse.md`,
  "The key"): the same exact non-round value ≥ 1,000,000 across ≥ 2 distinct CIKs on one
  instrument, with ≥ 1 Section 16 row, is one block. This spec applies that predicate at a finer
  grain. #1652's validation covered whole-observation buckets only, so it does not carry over by
  itself: the line-grain population is verified on its own below (every bucket read).
- **Form 4 Instructions 4(b)(iii) and 4(b)(iv)** (quoted in `2026-10-18-3227-form4-mixed-groups.md`):
  each form of indirect ownership on its own line, and an indirect line may report *"the entire
  amount of the entity's interest"*. Two reporting persons each reporting the same entity's whole
  holding on their own line is the shape counted once here.
- What the instructions do **not** settle is whether a line is a disjoint vehicle or a running total
  (`form4_observation_shares` docstring). This spec does not decide that. It reads lines only from
  observations a #3227 writer already summed, and those writers refuse the running-total layouts by
  construction (vehicle-balance conditions 5a–6). The components are the writers' policy, read
  back, not a new reading of the form.
- Observations stay what each filer attested (prevention log, #3227 2026-10-18 entry: "file the
  matching gap in the dedup rather than suppressing the sum in the writer").

No source rule says how to split a stored sum back into lines; that is fixed by construction below.

## Change

### 1. Components (read path, `_collect_canonical_holders_from_current`)

`_INSIDER_LINE_COMPONENTS_SQL` reads, for each `_current` Form 4/3 row's `(accession, filer, D/I)`,
every non-zero, non-derivative Table I balance, tagged `T` (a Form 4 transaction series' final: one
series for `D`, one per normalised nature for `I`, as `form4_form_key`; final by
`(txn_date, txn_row_num)` as the writers take it, chosen before zeros are dropped) or `H` (a Form 4
or Form 3 holding line). It joins the line tables on accession and filer only: they are stored under
the filing's entity instrument, while `_current` fans out to every share-class sibling (Codex
checkpoint 2).

Each candidate sum is admitted only over a scope a writer could have summed: every line `D`/`I`
with a non-blank title, and one title (`form4_fold_shares` conditions 2 and 3,
`form3_observation_shares`). The scope is the writer's own:

| candidate sum | writer | scope that must be summable |
|---|---|---|
| every line | mixed fold | the filer's lines in the accession, transactions and holdings |
| `T` finals alone | `I` vehicle sum (kept when a mixed fold refuses) | the filer's transaction lines in the accession |
| `H` lines alone | Form 3 line sum; Form 4 holding-only sum | the filer's holding lines of that D/I |

`_line_components` takes the first candidate, in that order, with ≥ 2 terms whose scope is
summable and that equals the stored `shares` exactly. If the chosen sum repeats a balance, the row
keeps none: the Form 4 writers refuse equal finals, and the pass could not tell how many to match. A
latest-line observation adds up to none of these unless the lines coincide exactly; that residue is
not separable from source data (Accepted residuals). DERA `:NDT:`/`:NDH:` rows keep none. The winner's
components ride `_dedup_by_priority` onto the `Holder`.

### 2. The pass (`_reconcile_insider_line_components`, after #1652, before #1645)

- Buckets: each component value `V` passing #1652's value proxies (≥ `_INSIDER_GROUP_MIN_SHARES`,
  `_is_group_block`), largest first.
- Members: every eligible row (#1652's admission) still standing whose remaining components
  include `V` (a *line member*), and every eligible, unmodified row whose `shares` equal `V` (a
  *whole member*).
- Collapse when the bucket has ≥ 1 line member and ≥ 2 distinct CIKs.
- Keeper: `_select_control_group_rep` over the members, each line member presented at `V`. Its
  clause-4 release check does not see a line member's own other Form 4/3 lots, because a demoted line
  member keeps its row and owner-once sums its lots (#1941). It does see the line member's rows in
  other channels: a 13D/G restatement competes with the lot by MAX in owner-once and would undo the
  subtraction (Codex ckpt-1 finding 6).
- **Fail closed on release.** If removing any whole non-keeper member would release its identity's
  other-channel rows into another wedge (`_releases_into_another_wedge`, #2230's hazard), the bucket
  is left alone. #1652's value route is not gated this way; this pass is new and takes #2230's posture.
- Every other whole member is consumed, as in #1652. Every other line member keeps its row with `V`
  subtracted, and `V` leaves its `line_components`, so they still sum to its `shares`. A row may reach
  zero; it stays in the list so its identity stays in the owner-once grouping, and `_build_slice`
  drops zero rows. The keeper's row and components are unchanged.
- One `insider_control_group_collapse` correction per bucket (`detail` says "at Table I line grain"
  and names each member and line); each folded member is a `DroppedSource` on the keeper at `V`, and a
  consumed whole member's own `dropped_sources` move to the keeper with it.

A bucket with no line member is #1652's, already processed; this pass leaves it alone. With no
bucket collapsing, the pass returns its inputs unchanged.

### Unchanged

Observations, `_current`, every writer, #1652's whole-value pass, #2230, #1764, #1645, owner-once.
No stored output depends on the rollup, so there is no backfill.

## Full-population verification (dev, 2026-10-18, read-only)

`PYTHONPATH=. uv run python -m scripts.ab_3886_line_component_collapse --shard k/3` then `--report`.
Population: every instrument with a positive `form4`/`form3` `_current` row. Treatment =
`get_ownership_rollup` with the pass; control = the same with the pass replaced by the identity, run
where treatment applied ≥ 1 line-grain correction. Elsewhere the arms are equal by construction:
`line_components` is read only by this pass, and with no bucket collapsing the pass returns its
inputs unchanged (`test_no_bucket_returns_inputs_unchanged`). A failing read would surface as a
harness error. The new SQL costs 21 ms on AAPL, 5 ms on JPM, 4 ms on SMMT, 3 ms on HCA and 1 ms on
GME (dev, one run each).

| measure | value |
|---|---:|
| instruments | 5,003 |
| harness errors | 0 |
| instruments where the pass fired | 110 |
| line-grain buckets | 153 |
| insiders wedge change | −2,485,973,586 |
| pie (all `pie_wedge` slices) change | −2,542,587,682 |
| instruments whose pie grows / shrinks / is unchanged | 0 / 95 / 15 |
| instruments where another correction kind moved | 0 |

The 15 unchanged-pie instruments are the competing-restatement residual (Accepted residuals): the
subtracted lot's identity also has a 13D/G or other row that owner-once takes by MAX.

**Issue panel.** 11 of the 13 instruments the #3227 fold released are covered: line-grain removal ≥
the released amount (SMMT 609,908,129 ≥ 573,883,879; TNYA, MAR, ATEC, AMRX, NMRA, BANF, WHF exactly
equal; AMKR 99,002,703, HCA 206,520,090, AAPG 37,336,811 more). The excess is the holding lines #3882
added that both members report (AAPG's two trusts, HCA's Frisco and Hercules holdings across four
Frist family members). **KWY and EPSN are left alone by the release gate**: in each, the whole member
the selector would remove (Oakmont Capital; Yorktown Energy Partners) holds rows in another channel
that its removal would release. Both stay double-counted, the fail-closed direction (KWY 3,975,660,
EPSN 2,869,560 per the issue).

**Fixes after the Codex checkpoints, measured.** A first run before them fired on 112 instruments
(156 buckets, pie −2,375,207,515). Showing the 13D/G restatement to the selector (checkpoint 1,
finding 6) changed the keeper on 8 instruments and made the subtraction effective on 7 of them
(ATEC, BRT, FEIM, MCFT, ROL, RRGB, WGS; ELMT keeps the same pie). ROL goes
−6,180,846 → −151,464,912: LOR Inc's `direct` 145,284,066 is a line of the R. Randall Rollins Voting
Trust's `indirect`, and the insiders wedge was 490,637,044 before the pass. The release gate
(finding 7) drops KWY and EPSN. A unit-wide title check dropped BRO and MPB; scoping it to each
writer's own unit (checkpoint 2) restores both.

**False positives.** Every bucket of the first run was read in the `--report` listing. Every
final-run bucket is one of them by `(instrument, value)` (153 = 156 − KWY's one and EPSN's two);
only the keeper differs, on the 8 instruments named. Each is one
vehicle reported by related filers: spouses or family members (SMMT, AAPG, AMKR's Kim family,
HCA's Frist family, SMCI, HUMA's Niklason/Dougan), a fund or holding company and its principal
(TNYA's Column Group, DRVN's Roark, WEN's Trian, RMNI's Adams Street, AMX's Slim trust), or
co-trustees. The five whose names do not show the link were checked against the line's
`nature_of_ownership`: TCBK "ESOP Trustee" (three trustees, same 1,113,794); FGBI "By Smith & Hood
Holding Company, LLC"; TLSI "By HW Investment Partners, LLC"; VNRX "By Concord International, Inc.";
MPB "By Lawrence Keister & Co.". None found.

Live SMMT after the pass: insiders 646,000,708 against 645,918,028 before the fold. The 82,680
difference is two lines the #3887 fold added: Zanganeh's 76,680 family line, which Duggan also
reports and which stays doubled below the floor (Accepted residuals), and Huber's 6,000 holding
(his `indirect` is 44,052 + 6,000).

## Accepted residuals

- Sub-1M lines both spouses report stay counted twice (SMMT 76,680 and 31,000), as #1652's floor
  leaves any sub-1M exact match. Round-lot lines likewise (`_is_group_block`).
- **The #2230 deemed-chain tier is not extended to lines.** A chain whose block is below the floor
  or round, and whose member's observation became a sum, loses that member from the whole-value
  bucket and is not recovered here. The tier reads the relationship box and the observation's D/I,
  which describe the observation, not one of its lines; extending it needs its own evidence.
- **A line member with a competing 13D/G restatement** can be demoted only where the selector's
  #2785 fence allows; where it is, owner-once's MAX can restore the subtracted line, and the
  correction then removes nothing from the pie. This is the "unchanged pie" class in the A/B.
- **A consumed whole member's different-valued 13D/G block** stays for #1645 and owner-once, as
  #1652 settled (exact-value-only consumption).
- **The keeper is a labelling choice only where neither member has rows in another channel.** Where
  one does, the selector's fence decides it, as for #1652.
- A latest-line observation whose lines happen to add up to it is read as a sum. Not separable from
  source data; the full-population bucket read below found none.

## Acceptance

1. `tests/test_3886_line_component_collapse.py` (pure) and `tests/test_3886_line_component_collapse_db.py`
   (the SQL, including a unit no writer sums) pass.
2. The A/B above: 0 harness errors; no pie grows; no other correction kind moves; every
   issue-panel instrument's line-grain removal covers what the fold released, except where the
   release gate refuses the bucket (KWY and EPSN, each named with its blocking member above); every
   bucket in the `--report` listing read and attributed to one shared vehicle.
3. Live, after deploy: `/instruments/SMMT/ownership-rollup` insiders = 646,000,708 (the A/B
   treatment value; the 82,680 above the pre-fold 645,918,028 is reconciled above), not
   1,255,908,837.

## Security

Read-path arithmetic over stored rows. No new input, endpoint, query parameter or authorisation
surface; the new SQL is parameterised on `instrument_id` only.
