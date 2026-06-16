# Insider control-group collapse — count a deemed-ownership block once across channels (#1652)

Part of epic #788 (ownership data-trust audit). Sibling of #1640 (owner-once),
#1644/#1649 (institutional family identity), #1645 (13D Rule 13d-5 group collapse).
Same contract: a single beneficial-ownership unit counted once, losers preserved as
`dropped_sources`, the figure-changing fold surfaced as a structured
`corrections_applied[]` entry (#1647).

## Bug

In a sponsor-controlled issuer, every entity in a PE fund's GP/LP chain files its
**own Form 4** reporting the **same indirect-beneficial block** — each is *deemed*
under Rule 13d-3 to beneficially own the block held by the funds it controls — and the
**same related CIKs also restate that block on Schedule 13D/G** (>10% owners are both
Section-16 insiders AND 5% blockholders). Today:

- `_dedup_by_priority` (per `(CIK, nature)`) keeps every Form 4 row; owner-once keeps
  each CIK once → the block is **summed across N CIKs** in the **insiders** wedge,
  exploding past 100% of float.
- The same group's **13D/G** rows are a parallel restatement of the same block. An
  insiders-only collapse that merely drops the duplicate Form 4 CIKs would **orphan
  those CIKs' 13D rows**, which then resurface as a fresh double-count in the
  blockholders wedge (Codex ckpt-1 HIGH). So the fix must be **cross-channel**.
- A within-holder facet: a control person who reports the deemed block as both
  `direct` and `indirect` (Alan Goldberg) has those two `(CIK, nature)` survivor rows
  summed by `_ADDITIVE_SOURCES` (#905), doubling that CIK on top of the cross-CIK
  explosion.

### Dev evidence (rendered rollup baseline, this branch)

| symbol | instr | insiders shares (before) | ratio | mechanism |
|---|---|---|---|---|
| AMTM | 8823 | 1,034,338,534 | 423% | ~17 Lindsay Goldberg CIKs each 45,026,743 (form4); 3 American Securities CIKs each 43,893,904 (form3) **+ Alan Goldberg/ASP Amentum 13d 43,893,904**; Alan Goldberg direct 45M + indirect 45M |
| VSAT | 8988 | 159,118,446 | 116% | 13 Warburg Pincus CIKs each 8,390,687 (form4); 5 Apax/CPP/Triton CIKs each 4,795,334 (form4) **+ Ontario/CPP/Triton 13d 4,795,334** |
| BAC | 1011 | 2,313,142,165 | 33% | Berkshire Hathaway + Buffett each 766,629,822 (deemed) |
| LCID | 9256 | 6,708,230,323 | 1719% | PIF + Ayar Third Investment each 2,227,072,750 + more |

The AMTM/VSAT bold rows are exactly the cross-channel resurrection risk: the same
exact block value appears on Form 4 AND 13D for overlapping CIKs.

Breadth (cross-channel union of `ownership_insiders_current` form4/form3 +
`ownership_blockholders_current` 13d/13g; exact non-round value >1M across ≥2 distinct
CIKs, per instrument): **1,150 value-buckets**; insiders-only dup ≈ **99.3B shares across
1,025 instruments**. Systemic across PE-spinoff IPOs.

## The key — exact-equal non-round value across ≥2 distinct CIKs, cross-channel

Deemed ownership makes every chain entity report the **byte-identical** block (the same
number flows up the GP/LP chain and across Form 4 / 13D — verified exact on dev
including fractional shares, e.g. AMAL 7,121,505.9300 across 14 CIKs; VSAT 4,795,334 on
both form4 and 13d). This differs from #1645's pure-13D case, where each member
*computes* its own aggregate and a fuzzy near-equal band is required. **Exact equality
is therefore the predicate AND the false-positive guard.**

Empirical false-positive check (full dev scan, value-bucket level, no period filter):
**zero** cross-group collisions in 1,150 buckets. Every bucket — including all 27 with a
>60-day as-of span — is ONE genuine control group reporting a static block across time
(Berkshire||Buffett, Volkswagen AG||Volkswagen International, JAB/Reimann family chain
across 2023→2025, KKR Hawaii aggregators, Blackstone INVH, Cinven, INEOS, Carlos Slim
Control Empresarial, Qatar Investment Authority). Two unrelated filers reporting an
*identical non-round* multi-million figure on the *same instrument* does not occur; the
non-round guard removes the only plausible coincidence (a shared round lot).

**No period constraint.** A static block held for years is reported at different
`period_end`s by the same group (JAB across 819 days; KKR Hawaii aggregators across one
quarter). A period window would wrongly *split* a single block — the opposite of the
fix. The exact-non-round-value signal stands alone.

Predicate over the cross-channel union of one instrument's insider survivors
(`winning_source in {form4, form3}`) and blockholders (`13d/13g`):

0. **Eligibility.** A member must have a **non-null CIK**, a **non-round** value
   (`_is_group_block`: not a whole multiple of `_ROUNDNESS_UNIT`), AND
   `shares ≥ _INSIDER_GROUP_MIN_SHARES` (**1,000,000**). The magnitude floor is
   load-bearing: `_is_group_block` is divisibility-only and **magnitude-blind** — it flags
   a 19,532-share director grant as "precise" exactly as 45,026,743. Below ~1M, an exact
   non-round match across distinct CIKs is dominated by *coincidental equal small grants*
   (dev: ~1,800 such clusters — e.g. three EVEX directors each at 101,107, independent, not
   a deemed block), while every ≥1M exact-match cluster on dev (1,144) is a genuine control
   group (fund+principal, trust+person, parent/sub, spouses with shared control). A sub-1M
   deemed block also cannot meaningfully explode a normal float. Ineligible rows pass
   through to their channel untouched.
1. **Bucket = exact `shares` value.** All eligible members sharing one Decimal value on
   the instrument. (A CIK's two same-value `(CIK, nature)` survivor rows are simply two
   members — both fold; no nature inspection needed, and `Holder` carries none.)
2. **≥2 distinct non-null CIKs** in the bucket.
3. **≥1 insider-source member** (`form4`/`form3`) in the bucket. A bucket that is
   purely 13D/G (no Section-16 footprint — a pure co-investor group like SKE/RIME/BTAI)
   is **left for #1645**, which collapses it in the blockholders wedge with its tiered
   tolerance. This pass owns only control groups with a Form 4 footprint (the #1652
   insiders explosion); the two passes partition the work, no overlap.

### Collapse action

Each qualifying bucket collapses to ONE `Holder`, classified **insiders** (a >10% deemed
owner is a Section-16 insider; the rollup already routes them there via `form4`):

- `shares` = the block value (all members equal; MAX == the value).
- **Representative** = a member, preferring an insider-source (`form4`/`form3`) member so
  the rep's `winning_source` is an insider source, then deterministic tie-break
  `(insider-source-first, shares, filer_cik, winning_accession)` descending.
- All non-rep members (from BOTH channels) → `DroppedSource` entries **appended to** the
  rep's existing `dropped_sources` (amendment-chain provenance preserved). The rep's own
  other same-value rows (e.g. its `indirect` twin) drop too.
- One `CorrectionApplied(kind="insider_control_group_collapse")`:
  `shares_removed = Σ(non-rep member shares)`; `filer_cik`/`filer_name` = rep;
  `source_channel`/`winning_source`/`winning_accession` = rep. `detail` names the group
  size, block value, and **each folded member's CIK + name + shares** (the loser
  identities — `DroppedSource` itself has no CIK/name field, same limitation as #1645, so
  identities live in `detail`).
- The bucket's members are removed from BOTH `survivors` and `blockholders`; the rep is
  added back to `survivors`. **Nothing is orphaned** — the 13D rows of a collapsed group
  are consumed here, so neither #1645 nor owner-once can re-count the block.

## Placement in the rollup pipeline

`get_ownership_rollup` (read-only, inside `snapshot_read`):

```
survivors    = _dedup_by_priority(other_candidates)              # form4/form3/13f/def14a
blockholders = _dedup_within_source(block_candidates)            # 13d/13g
family_by_category, survivors, blockholders, unmatched_def14a, family_corrections =
    _reconcile_institutional_families(survivors, blockholders, unmatched_def14a, outstanding)
survivors, blockholders, insider_group_corrections =
    _reconcile_insider_control_groups(survivors, blockholders)   ← NEW (cross-channel)
survivor_keys = { identity_key(h) for h in survivors }
blockholders, group_corrections = _reconcile_13d_groups(blockholders, survivor_keys)
by_category = _reconcile_owner_once(survivors + blockholders)
corrections_applied = (*notice_suppressions, *family_corrections,
                       *insider_group_corrections, *group_corrections)
```

Runs AFTER family-reconcile (curated 13F/13G families already pulled out; a Section-16
control group is never a curated institutional family) and BEFORE both `_reconcile_13d_groups`
and owner-once. `survivor_keys` is computed AFTER this pass on the post-collapse
`survivors` — correct because the rep is in `survivors` (owner-once will reconcile it)
and the dropped members are not (they are already consumed, so #1645 must not treat their
absence as "reconcile elsewhere").

### Non-exact 13D residual on a consumed CIK (Codex ckpt-1 MED) — handled, not quarantined

Consumption is **exact-value only**: only the rows AT the bucket value leave `blockholders`.
A consumed insider CIK can still own a 13D/G row at a *different* value. Dev scan: 98 such
residual rows. They are **almost all LARGER than the bucket** (e.g. TKO Silver Lake 13d
94,021,358 vs its Form 4 fragment 2,155,188) — the residual is the **full group block**
(the real figure, #1645's job), the bucket is a smaller Form-4 fragment of it. So
consuming a member's *entire* 13D footprint would be wrong — it would delete the genuine
larger block. The residual is deliberately left in `blockholders`.

What happens to that residual is **strictly better than today**: because the consumed CIK
leaves `survivor_keys`, #1645's predicate 0 no longer excludes its residual 13D, so #1645
now **collapses the residual group to one** in the blockholders wedge (before this PR the
residual was MAX'd into the CIK's insider figure and exploded N× across the group's CIKs).
A singleton residual simply moves wedge (insiders→blockholders), still counted once. The
only over-count is a small **fragment ⊂ block cross-wedge overlap** (the rep's bucket
fragment in insiders while the larger block sits in blockholders under a different rep CIK)
— a documented residual, far smaller than the explosion it replaces, and exactly the
fragment-vs-block cross-channel reconciliation #1645's spec already scoped as beyond reach
("no claim is made that owner-once reconciles the whole group across channels"). No
quarantine/tolerance rule is added (it would destroy legitimate larger blocks).

**The rep-residual split (Codex ckpt-2).** The sharpest form of the above: the bucket
**rep** stays in `survivors`/`survivor_keys`, so its *different-value* residual W 13D is
excluded from #1645 clustering while a folded member's W 13D is not — owner-once then MAXes
the rep's W into insiders while the member's W collapses in blockholders, so a larger block
W can split one copy into each wedge. Dev scan for this exact shape (a collapsed bucket V
whose members also share a larger non-round ≥1M residual W) returns **one instrument,
GDRX** (founders Hirsch + Bezdek: V = 2,632,721 Form 4, W = 5,391,994 13D) — and GDRX
renders `no_data` (no shares outstanding on file → the rollup short-circuits before slices),
so **zero operator-visible instruments** are affected. It is also **not worsened** by this
PR: pre-fix the two founders already counted 2×W (both MAX'd into insiders); post-fix the
total is still 2×W, merely re-attributed across wedges (identical pie-wedge sum, identical
oversubscription). A precise fix needs the deferred fragment-vs-block reconciliation; a
fragment guard ("a CIK's smaller row is not its control block") was rejected — it would
resurface AMTM's Alan-Goldberg 43,893,904 13D into the blockholders wedge. Characterization
test `test_rep_residual_split_documented` pins the behaviour.

## Surfacing

- **`corrections_applied[]`**: new kind `insider_control_group_collapse` added to the
  closed-vocab docstring (`ownership_rollup.CorrectionApplied`), the API Pydantic
  `Literal` (`app/api/instruments.py::_CorrectionAppliedModel.kind`), and the FE union
  (`frontend/src/api/ownership.ts::OwnershipCorrectionKind`). All three or Pydantic
  rejects the kind at the boundary.
- **CSV export** (`build_rollup_csv`): one `__insider_group_collapse:<rep_cik>__` memo
  row per correction (mirrors `__group_collapse:__`), excluded from any `SUM(shares)`
  reconciliation (the eliminated shares were never real).

## Out of scope — file as follow-up

The **`def14a_unmatched`** additive wedge has a parallel control-group double-count:
name-keyed proxy 5%-holder rows restate the same block (AMTM "Lindsay Goldberg 1" + "ASP
Amentum Investco" both 43,893,904; dev breadth 84 instruments / ~4.1B shares). It is
**name-keyed (no CIK)** — outside this pass's CIK-keyed union — and overlaps #1644's
proxy-wedge territory; folding it needs name-based value clustering across a third
channel. This PR unifies the three **CIK-keyed** channels (form4/form3/13d/13g); the
name-keyed proxy channel is a distinct, smaller (24× less) follow-up.

Sanity guard: the #1647 `SanityChecks` (`largest_single_holder_pct`,
`any_pie_slice_over_100pct`) already exposes the oversubscription this fix removes — no
new invariant needed.

## Validation (dev, this branch, live `get_ownership_rollup`)

| symbol | insiders before | after (expected) | corrections |
|---|---|---|---|
| AMTM 8823 | 1,034,338,534 | 45,026,743 once + 43,893,904 once + small real insiders; **no blockholders resurrection** | 2 `insider_control_group_collapse` |
| VSAT 8988 | 159,118,446 | 8,390,687 once + 4,795,334 once + Baupost + real insiders; **13d 4,795,334 consumed** | 2 |
| BAC 1011 | 2,313,142,165 | − 766,629,822 (Berkshire/Buffett once) | 1 |
| LCID 9256 | 6,708,230,323 | − large (PIF/Ayar once + more) | ≥1 |
| GME 1699 | 39,785,607 | **unchanged** | 0 |
| AAPL/MSFT/JPM/HD | — | **unchanged** | 0 |
| SKE/RIME/BTAI | — | **#1645 still collapses the pure-13D wedge** (no insider member → skipped here) | (blockholder_group_collapse, unchanged) |

## Residuals (documented, not silently dropped)

- A **lone CIK** reporting the same exact non-round block as both `direct` and `indirect`
  (no second CIK) is not collapsed (≥2-distinct-CIK gate); its #905 additive sum stands.
  Not observed as a high-value case.
- A control group on a **round** shared figure stays double-counted (roundness guard's
  conservative direction).
- Two genuinely-unrelated groups sharing the *exact non-round* value on one instrument
  would merge (undercount). Not observed in 1,150 dev buckets; non-round guard protects.
- The **`def14a_unmatched`** name-keyed double-count is the filed follow-up.

## Tests (pure-logic table tests on `_reconcile_insider_control_groups`)

- **Cross-channel no-resurrection (Codex ckpt-1 TEST GAP):** form4 cluster + same CIKs'
  13d at the block value → collapses to ONE insiders rep; the 13d rows are removed from
  the returned `blockholders` (assert empty / not present); `shares_removed` = Σ losers
  from both channels.
- AMTM 2-cluster shape: two exact-value buckets (45,026,743 form4-only; 43,893,904
  form3+13d) each collapse to one rep; 2 corrections.
- Within-holder fold: a CIK with two same-block survivor rows folds both (no #905 double).
- BAC 2-CIK exact pair collapses; rep prefers an insider-source member.
- Pure-13D bucket (no insider member) is **left untouched** in `blockholders` (handed to
  #1645).
- Negatives: round shared value kept separate (`_is_group_block`); lone CIK untouched;
  two distinct exact values stay two buckets; null-CIK and `shares ≤ 0` never clustered;
  a 13F / matched-DEF 14A survivor at the same value is NOT pulled in (source scope).
- A control person's *separate* personal stake (different non-block value) survives.
- Rep determinism + existing `dropped_sources` preserved (appended, not replaced);
  `detail` carries folded member CIK+name+shares.

End-to-end exercised on the live dev rollup (AMTM/VSAT/BAC/LCID collapse + the
`insider_control_group_collapse` correction; GME + standard panel + SKE/RIME/BTAI #1645
behaviour unchanged).
