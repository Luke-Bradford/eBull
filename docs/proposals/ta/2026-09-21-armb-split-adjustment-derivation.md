# ARM B §7 item 2 slice B — the split-only correction, derived

Refs #2834, #2437. Code: `app/services/research_split_adjustment.py`.
Evidence: `scripts/measure_2834_split_adjustment.py`
(`--derive` / `--segments` / `--floor`).

Predecessors, all merged. `b65abd9c` (PR #3279) settled the signal basis: the archive
ships a per-bar split ratio in CSV field 6 and the split-only series is `close / scale`.
`8bb1443f` (PR #3280) established that no non-circular adjudicator is available.
`d15e680e` (PR #3281) settled the correction POLICY — `apply_all`, decided on the
direction of harm rather than on any set distance. `fc72804b` (PR #3282) stored the
stamps for all 50,134,060 bars.

This slice is the arithmetic those four left open, and the two §7 contracts that
arithmetic can settle. **It changes no stored byte and no strategy's verdict.**

## 1. Source rule

The APPLICATION rule is the archive's own reader and was settled in `b65abd9c`:
`IntraderEngine::SplitCheck()` stamps **new shares per old** on the bar that FIRST
PRINTS the post-split level. Two consequences follow directly and neither is invented
here:

- the scale of a bar is the product of the factors of every bar **strictly after** it,
  because the stamped bar is already on the new basis;
- the last bar of any series scales to exactly 1.

The CORRECTION policy — what to do with a stamp a second processing disputes — has **no
published rule**, which `d15e680e` §1 states explicitly after looking for one. It is
therefore fixed by construction and frozen: `apply_all`, carried in
`SPLIT_CORRECTION_POLICY` and hashed into `SPLIT_ADJUSTMENT_RULE_VERSION`.

⚠ Nothing in this document is offered as new support for `apply_all`. That decision was
made on exposure asymmetry (2,728 uncorrected real reverse splits against 228 refuted
forward stamps) and is not reopened by anything measured below.

## 2. Why a derivation and not a column

`sql/405` §2 refused to materialise a corrected `close`, and its reason transfers
verbatim to a materialised SCALE:

> the raw close is an observation; the corrected close is an opinion about it

A scale column is that same opinion in a cheaper encoding — derived state whose
correctness depends on a policy recorded in a different file, going stale silently the
moment the policy moves. It also buys nothing measurable: a consumer that holds a
series' bars already holds every factor the product needs, so the derivation is one
O(n) backward pass over memory it has, not a join. And it would put
`research_price_series.adjustment_basis = 'unadjusted'` into tension with a table
carrying an adjusted level beside the raw one — the single-basis property
`strategy_price_basis.CERTIFYING_ARCHIVE_BASES` depends on.

⚠ The consequence for the rung: this slice has **no migration, no ETL and no corpus
mutation**, so it is a behavioural change with data semantics, not a corpus change. The
full-population run below is still run — not because the ladder demands it here, but
because an arithmetic rule over 50.1M bars has failure modes no unit test reaches.

## 3. The precondition a consumer cannot skip

`split_scales` takes the series' `corporate_action_stamps` marker as a **mandatory
keyword argument** and refuses anything but `vendor_supplied`.

This is `fc72804b`'s own prevention lesson made unforgettable rather than remembered.
`COALESCE(split_factor, 1)` reads "this vendor ships no stamps" identically to "no split
on this bar", and the other loaded vendor (`paperswithbacktest`, 25.8M bars) is NULL
across every one of its rows. A derivation that defaults instead of refusing converts a
vendor with UNKNOWN corporate actions into a vendor with none.

⚠ `absent` raises; it does not return a scale of 1. A scale of 1 must not be
*reachable* from an absent marker, which is a stronger property than "the caller should
check first" and is the only one a test can pin.

## 4. §7 contract (b) — OHLC and volume, SETTLED

**Every price field takes the same scale; volume takes its reciprocal; turnover is
computed on the RAW basis and never on the corrected one.**

- **open / high / low / close divide by `scale`.** They are one measurement of one
  instrument in one unit and a split re-denominates the unit. Scaling `close` alone
  would leave `high < close` on the bar before a forward split, breaking an ordering
  every OHLC consumer assumes.
- **`volume` multiplies by `scale`.** A 4:1 split quarters the price and quadruples the
  share count, so a post-split share count must be multiplied to reach the pre-split
  unit the corrected price is denominated in. Measured on AAPL either side of
  2020-08-31: close 499.23 → 129.04, volume 46,907,479 → 223,505,733.
- **`close × volume` is split-invariant, so `price_quarantine`'s T3 admit-back must keep
  reading the raw basis.**

⚠⚠ **That last clause is a REQUIREMENT, and it arrived as a correction to this
document's own first draft.** The draft asserted that corrected turnover equals raw
turnover *exactly* and proposed to assert it over the full population. It does not:
`close / scale` is a DIVISION, exact only where the scale divides the close, and AAPL's
own 7:1 already breaks it (92.7 / 7 = 13.242857142…). So a consumer that corrects both
sides and multiplies gets the raw product back only to the quotient's rounding. The raw
product is exact and needs no correction at all — which makes "T3 stays on raw" the
answer to contract (b) rather than a caveat about it.

`corrected_price` and `corrected_volume` are published as a PAIR so that a consumer
correcting one and forgetting the other has to do so deliberately.

## 5. §7 contract (d) — already-adjusted segments, SETTLED as a measured residual

The worry: an Intrader segment that already carries an adjustment would be
double-adjusted by a uniform rule.

The internal discriminator is the price STEP across the stamped bar, tested against the
stamp itself. An event whose own factor sits inside the test band cannot be told from a
no-step bar by any step test, so those are reported separately rather than assigned.
Full population, all 9,354 stamped events (`--segments`):

| class | dir | events | step = factor | no step | neither |
| --- | --- | ---: | ---: | ---: | ---: |
| factor inside band | forward | 1,031 | 1,023 | 925 | 6 |
| factor inside band | reverse | 35 | 34 | 34 | 0 |
| **resolvable** | forward | 4,825 | 4,706 | **111** | 17 |
| **resolvable** | reverse | 3,430 | 2,811 | **114** | 508 |

Of **8,255 resolvable** events, **7,517 (91.1%)** print a step matching their factor —
the segment is unadjusted and applying the factor is correct. **225 (2.7%)** print no
step at all: the already-adjusted candidate class, 111 forward and 114 reverse. **525
(6.4%)** print a step of the wrong magnitude, which is a different defect (mis-sized or
mis-dated stamp) and is counted separately rather than folded in.

⚠ The 'neither' column is overwhelmingly reverse — 508 of 525, i.e. 14.8% of resolvable
reverse events against 0.35% of forward ones. That is not established as a stamp defect
rate: reverse splits happen on distressed names whose real one-day moves are large and
whose pre-event closes are tick-quantised, so the step test is at its least informative
exactly there. Reported as what it is, an upper bound on a discriminator's agreement.

**The verdict: no special treatment. Every stamp is applied, and the 225 are the
declared residual.** Gating on this discriminator would reintroduce precisely what
`d15e680e` §5 froze out — a free parameter (the band) and a discriminator of unmeasured
precision. `b65abd9c` separately measured the internal check passing 364 of 383 stamps
that an external processing disputes, so its error rate is known to be poor rather than
unknown. It bounds the residual; it cannot adjudicate it.

⚠ Direction, per §3.5's framework: the 225 split 111 forward / 114 reverse, so the
residual is not concentrated on the side that BUYS a position. That is an observation
about this class, not a general claim about the corpus.

## 6. Full-population evidence

`--derive` runs the real `split_scales` over every bar of every Intrader series. The
invariant arms gate the exit code; the disagreement arms are reported and do not.

### Arms 1-2 — the invariants

| | |
| --- | ---: |
| series derived | 22,879 |
| series refused | **0** |
| bars | 50,134,060 |
| bars the correction MOVES (`scale <> 1`) | 12,546,227 |
| series whose last bar does not scale to 1 | **0** |

A quarter of the corpus (25.0%) carries a scale other than 1, which is the size of what
`suppress_all` was leaving on the table. The extreme scales are all reverse-split
distress chains — `TOPS` at 2.2e-14, `DCTH` at 3.2e-11, `XTIA` at 8.2e-11 — i.e. names
whose uncorrected early closes are inflated by up to fourteen orders of magnitude, which
is `d15e680e` §3.5's "inflates a momentum score" in its most extreme form.

### Arm 3 — turnover

11,906,203 of 11,906,203 measured bars agree to within 1e-12 (100.00%), which is the
quotient's rounding and not exactness — see §4. **640,024 moved bars are EXCLUDED and
named**: their volume is zero or absent, so the relative error is undefined. A band
table whose shares do not sum to 100% reads as full coverage when it is not, so the
exclusion is printed rather than left to be inferred from the arithmetic.

### Arm 4 — corroboration against the vendor's own `adj_close`

19,170,987 bars sit on a dividend-free suffix and are therefore comparable.
**19,163,077 (99.96%) agree to within 1e-12.** 7,910 (0.04%) do not, and they localise
to **8 series of 22,879**:

| symbol | bars | span (d) | bars/yr | disagreeing |
| --- | ---: | ---: | ---: | ---: |
| COBR | 9,911 | 14,345 | 252.4 | 4,547 |
| RSLS | 4,245 | 6,161 | 251.7 | 2,238 |
| ACET | 1,679 | 2,436 | 251.7 | 640 |
| DBD | 725 | 15,611 | **17.0** | 441 |
| GGE / LTBR / CHLN / NUROW | — | — | ~252 | 26 / 16 / 1 / 1 |

**What this establishes: the derivation reproduces the vendor's own adjustment
arithmetic on 19.16M bars.** That is what arm 4 is for, and it is the strongest
statement available about the code.

⚠⚠ **Arm 4 is an implementation check, not an adjudicator**, and the distinction is the
whole of #3280's lesson. It can show our product reproduces the arithmetic of the party
that issued the stamps; it cannot show the stamps are right, because both sides descend
from one Yahoo observation. Circular as EVIDENCE, non-circular as a TEST OF THIS CODE.

### What the 8 disagreeing series look like — and what is NOT claimed

Three were inspected at the bar level. They are **consistent with the vendor's field-6
stamp set being INCOMPLETE relative to what its own field-9 `adj_close` encodes**, not
with an arithmetic error on our side:

- **`DBD`** ships **725 bars across a 15,611-day span — 17 bars/year**. The series is
  sparse in the SOURCE, and a bar the vendor never shipped took its stamp with it. It
  carries zero split and zero dividend stamps while its 1982 `adj_close` sits 17× below
  its `close`.
- **`ACET`** carries one stamp (1-for-7 on 2020-09-16, a flat zero-volume stub bar).
  Immediately before it the vendor's `adj_close` is exactly 7× `close` (2.37 → 16.59),
  so the vendor applies the factor on the same convention we do. Two years earlier
  `adj_close` equals `close` exactly. Both cannot be true of one stamp set, so the
  vendor's `adj_close` knows about an event between those dates that field 6 does not
  stamp.
- **`RSLS`** carries six compounding reverse splits and disagrees on roughly half its
  bars — the shape a single mis-handled event in a chain produces, since every bar
  before it inherits the error and every bar after it does not.

⚠ **This is a three-series inspection and the cause is NOT established for the other
five.** "Consistent with missing stamps" is what three bar-level reads support; it is
not a measured rate and must not be quoted as one. The check that would settle it is a
per-event comparison of the implied `close/adj_close` factor against the stamp on the
same bar, across all 8 — one query, deliberately not run here because it changes nothing
this slice decides.

⚠ **It does bound the exposure**: whatever the cause, it is confined to 8 of 22,879
series and 0.04% of comparable bars. `apply_all` under-corrects there — it cannot apply
a stamp that was never shipped — which is a MISS rather than the inflation `d15e680e`
§3.5 identified as the direction that buys a position.

### §7 item 3's inputs (`--floor`)

`MIN_CLOSE = 1.0`, applied to the raw close against the corrected close, over all
50,134,060 bars:

| | |
| --- | ---: |
| admitted raw, rejected corrected | 247,200 |
| rejected raw, admitted corrected | 949,062 |
| **bars whose membership moves** | **1,196,262 (2.386%)** |

Both directions are populated and the larger one runs the way the raw basis is too
STRICT: 949,062 bars that the raw floor rejects clear it once corrected. A membership
count is not a decision — which basis the floor reads is §7 item 3 and it rotates s2's
identity, so it belongs to the slice that mints the new id.

## 7. What this slice does NOT settle

| open item | why it is not here |
| --- | --- |
| §7 contract (a) — which basis the quarantine evaluates | It is a CONSUMER decision and nothing consumes the correction yet. Prices do not change in this slice, so every stored verdict stays valid and `RULE_SET_VERSION` staleness cannot arise — the same reason slice A did not re-run it. |
| §7 item 3 — what `MIN_CLOSE` reads on the corrected basis | A strategy-definition question that rotates s2's identity. `--floor` measures its inputs without deciding it. |
| §4 rule 11 — the new strategy id | Owed by the slice where a strategy's DATA CONTRACT changes, i.e. where a consumer starts dividing. Nothing reads `research_split_adjustment` on this branch, so no identity moves and none may be minted. |

⚠ One correction is owed and deliberately not taken here. `s2_cross_sectional_momentum`'s
`MIN_CLOSE` comment says *"Unadjusting would need per-series split factors the corpus does
not store"*. Since `fc72804b` that is FALSE. It also carries the third instance of the
cross-vendor error (it cites `sql/251`, the HF archive, for a floor applied to the
Intrader corpus). Both must be fixed — but `_source_hash()` hashes the whole module, so
editing either is an identity rotation, and an identity rotation belongs to the slice that
mints the new id rather than to one that changes no verdict.

## 8. Rung

**Behavioural change with data semantics.** A new service module with data semantics, a
pure-logic test file and a read-only measurement script; no migration, no ETL, no corpus
mutation, no consumer. Codex ckpt-1 on this document (a judgement artefact settling two
contracts) and ckpt-2 on the diff.
