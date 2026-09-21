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
moment the policy moves. A consumer that holds a series' bars also already holds every
factor the product needs, so the derivation is one backward pass over memory it has
rather than a join.

⚠ **The consistency argument is the load-bearing one; the cost argument is not
measured.** No timing or memory comparison against a materialised column was run, and
a repeated or windowed consumer could plausibly benefit from one. What is claimed is
narrower: a derivation cannot go stale against its own policy, and that is the failure
mode this corpus has actually had. A versioned materialisation could close that too — at
the price of a second copy of the corpus's price semantics — and is not ruled out for a
later slice with a consumer whose read pattern justifies it.

⚠ It would also put `research_price_series.adjustment_basis = 'unadjusted'` into tension
with a table carrying an adjusted level beside the raw one. That is a tension rather than
a contradiction — explicitly labelled columns could coexist — but resolving it is work
this slice has no consumer to justify.

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
  instrument in one unit and a split re-denominates the unit. Correcting `close` alone
  before a FORWARD split divides it while `low` keeps its raw level, so the bar can print
  `close < low`; before a REVERSE split the scale is below 1 and the same omission pushes
  `close` above `high`. The direction depends on the event, which is why the rule is "all
  four or none" rather than a bound in one direction.
- **`volume` multiplies by `scale`.** Measured on AAPL either side of 2020-08-31: close
  499.23 → 129.04, volume 46,907,479 → 223,505,733.

  ⚠ **The target unit is the series' TERMINAL basis, not a pre-split one**, and an
  earlier draft of this section said the opposite. Because `scale` is the product of
  every LATER factor, the whole corrected series is denominated in the share unit of the
  LAST bar: AAPL's 1980 bar is restated in today's shares, not today's bars restated in
  1980's. A bar recorded in OLD shares is therefore multiplied to express the same
  holding in the new.
- **`close × volume` is split-invariant, so `price_quarantine`'s T3 admit-back should
  keep reading the raw basis.**

⚠⚠ **That last clause arrived as a correction to this document's own first draft**,
which asserted corrected turnover equals raw turnover *exactly* and proposed to assert it
over the full population. It does not. `close / scale` is a DIVISION, and a decimal
quotient terminates only when the reduced denominator's prime factors are 2 and 5 **and**
the ambient `prec` is wide enough to hold it. A scale of 4 terminates; AAPL's own 7:1
does not (92.7 / 7 = 13.242857142…).

⚠ The reason for T3 is narrower than "paired adjustment is wrong" — **in exact arithmetic
paired adjustment is exactly turnover-preserving.** The reason is that the raw product
performs no division at all and therefore cannot acquire the error, so reading raw is
strictly the safer of two otherwise-equivalent routes. ⚠ And "the raw product is exact"
is itself bounded: `close * Decimal(volume)` runs in the caller's context too. What §6
measures is a TOLERANCE; rounding is the reading that tolerance supports, not a proof
about every unit in the last place.

⚠ `corrected_price` and `corrected_volume` are published as a PAIR so that correcting one
and not the other is a visible omission. That is a LEGIBILITY property and not an
enforcement one — nothing stops a consumer calling only one, and the enforcement, if
wanted, belongs to the consumer that reads both.

⚠ Both appliers validate their scale (positive, finite) rather than trusting the caller,
because they are public and reachable without `split_scales`. Measured reason: an
unguarded `NaN` scale makes the division SUCCEED and return `NaN`, which then raises
`InvalidOperation` at whichever unrelated ORDERING comparison meets it first — a
`MIN_CLOSE` filter, a decile sort — with nothing pointing back at the scale.

## 5. §7 contract (d) — already-adjusted segments, SETTLED as a measured residual

The worry: an Intrader segment that already carries an adjustment would be
double-adjusted by a uniform rule.

The internal discriminator is the price STEP across the stamped bar — `close(prev) /
close(event)` — tested against the stamp. **The band is stated, not implied**: a step
"matches the factor" when `step / factor` lands in [0.8, 1.25], and is "absent" when
`step` alone does. An event whose own factor sits inside that band cannot be told from a
no-step bar by any step test, so it is classed separately.

⚠⚠ **The classes are DISJOINT and EXHAUSTIVE, and the first version of this table was
neither** — a Codex finding at both checkpoints independently. The two predicates were
applied as independent filters, and their acceptance regions OVERLAP whenever the factor
is only just outside the band (at f = 1.26, both hold for any step in [1.008, 1.25]). The
columns summed to 100.145% of their own row. The overlap is now its own class.

Full population (`--segments`). **9,354 stamped events; 9,321 testable; 33 EXCLUDED**
because the stamp sits on a series' first bar, where no prior close exists and no step
test is defined:

| class | dir | events | step = factor | no step | both | neither |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| factor inside band | forward | 1,031 | 100 | 2 | 923 | 6 |
| factor inside band | reverse | 35 | 1 | 1 | 33 | 0 |
| **resolvable** | forward | 4,825 | 4,697 | **102** | 9 | 17 |
| **resolvable** | reverse | 3,430 | 2,808 | **111** | 3 | 508 |

Of **8,255 resolvable** events: **7,505 (90.9%)** print a step matching their factor,
**213 (2.58%)** print no step, **12** fall in the overlap and **525 (6.4%)** print a step
of the wrong magnitude.

⚠⚠ **None of these classes is a cause, and the earlier draft asserted them as such.**
A matching step is *consistent with* an unadjusted segment and equally with market
movement, a stale price or a coincidentally-sized stamp. "No step" is *consistent with* a
prior adjustment and equally with a false stamp, a mis-dated one, or a non-trading stub
bar — ACET's stamp below sits on exactly such a bar, flat at 2.28 with zero volume.
"Neither" is *consistent with* a mis-sized stamp and equally with an ordinary large
return. The discriminator separates SHAPES, not explanations.

⚠ 'neither' is overwhelmingly reverse — 508 of 525, i.e. 14.8% of resolvable reverse
events against 0.35% of forward ones. Reverse splits happen on distressed names whose
real one-day moves are large and whose pre-event closes are tick-quantised, so the step
test is at its least informative exactly there.

**The verdict: no special treatment. Every stamp is applied, and the 213 are a declared
candidate count.**

⚠ The operative reason is NOT "gating needs a parameter" — a frozen, versioned band
would be a perfectly deterministic policy, and an earlier draft leaned on that argument
wrongly. The two reasons that hold are: (1) **the discriminator is unvalidated**, and
`b65abd9c` measured the internal check passing 364 of 383 stamps an external processing
disputes — which is not an error RATE (external disagreement is not ground truth, and the
disputed set is a selected denominator) but is enough to show it is not trustworthy; and
(2) **the policy is already frozen** by `d15e680e` §5 on evidence this document does not
revisit. Reopening it needs the grounds §5's "what would reopen this" names, not a new
discriminator of unknown precision.

⚠ **213 is a candidate-EVENT count, not a bounded residual.** It excludes the 33
untestable and the 12 ambiguous events, says nothing about false negatives, and is not
converted to bars or to strategy exposure anywhere.

⚠ Direction: 102 forward / 111 reverse — but as RATES those are 2.11% of resolvable
forward events against 3.24% of reverse, so the counts being near-equal is not a
statement about balance. Neither figure establishes balance in trading harm, which would
need a return-window measurement rather than an event count.

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
`suppress_all` was leaving uncorrected. The extreme scales are far below 1 — `TOPS` at
2.2e-14, `DCTH` at 3.2e-11, `XTIA` at 8.2e-11 — i.e. compounding reverse-split chains.

⚠ **The direction, stated carefully, because an earlier draft had it backwards.** A
scale far below 1 means the correction RAISES those early prices (divide by 2.2e-14);
the raw early close is *understated* relative to its corrected form, not inflated. What
is inflated is the uncorrected momentum RATIO — `close(t) / close(t-252)` spans the
reverse split and reads the artificial jump as return. Level and ratio move opposite
ways here, and `d15e680e` §3.5's claim is about the ratio.

⚠ Calling all ten "distress chains" reads more than the output supports: it lists
symbols and scales, not chain histories. The scale magnitudes alone establish repeated
reverse splits; the distress reading is an inference from that pattern. ⚠ 12,546,227
moved bars is a measure of COVERAGE — how much the correction touches — and not of harm
avoided, which needs a return-window measurement.

### Arm 3 — turnover

11,906,203 of 11,906,203 measured bars agree to within 1e-12 (100.00%), which is the
quotient's rounding and not exactness — see §4. **640,024 moved bars are EXCLUDED and
named**: their volume is zero or absent, so the relative error is undefined. A band
table whose shares do not sum to 100% reads as full coverage when it is not, so the
exclusion is printed rather than left to be inferred from the arithmetic.

### Arm 4 — corroboration against the vendor's own `adj_close`

19,170,987 bars sit on a dividend-free suffix and are therefore comparable — **38.24% of
the corpus**, so this arm bounds nothing about the other 61.76%.

⚠⚠ **Split by whether the correction did anything, because one headline rate credits the
derivation for identity comparisons** (Codex ckpt-1). A bar at scale 1 compares `close`
against `adj_close` and exercises no arithmetic at all:

| comparable bars | agree ≤ 1e-12 | of | rate |
| --- | ---: | ---: | ---: |
| **scale ≠ 1 — the correction did something** | **4,314,873** | **4,317,795** | **99.93%** |
| scale = 1 — identity | 14,848,204 | 14,853,192 | 99.97% |
| all | 19,163,077 | 19,170,987 | 99.96% |

**4.31M bars actually exercise a non-trivial correction and 99.93% of them reproduce the
vendor's own adjusted level.** That is the figure the claim is about; the 19.17M headline
is the weaker one and must not be quoted for it.

The 7,910 disagreements (0.04%) localise to **8 series of 22,879**:

| symbol | bars | span (d) | bars/yr | disagreeing |
| --- | ---: | ---: | ---: | ---: |
| COBR | 9,911 | 14,345 | 252.4 | 4,547 |
| RSLS | 4,245 | 6,161 | 251.7 | 2,238 |
| ACET | 1,679 | 2,436 | 251.7 | 640 |
| DBD | 725 | 15,611 | **17.0** | 441 |
| GGE / LTBR / CHLN / NUROW | — | — | ~252 | 26 / 16 / 1 / 1 |

**What this establishes: the derivation reproduces the vendor's own adjustment arithmetic
on 4.31M bars that actually carry a correction.** That is what arm 4 is for, and it is
the strongest statement available about the code.

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

⚠ **These are HYPOTHESES from a three-series inspection, not established causes, and
nothing is claimed about the other five.** DBD's sparsity does not prove an omitted bar
carried a split; ACET's inconsistent ratios do not uniquely identify a missing event;
RSLS's roughly-half disagreement is consistent with a single mis-handled event in a chain
but does not establish one. The check that would settle it is a per-event comparison of
the implied `close/adj_close` factor against the stamp on the same bar, across all 8 —
one query, deliberately not run because it changes nothing this slice decides.

⚠ **What IS bounded is the extent, not the direction.** The disagreement is confined to
8 of 22,879 series and 0.04% of comparable bars. An earlier draft went further and said
that "whatever the cause" these under-correct and therefore only cause a MISS. **That is
withdrawn**: a MISSING reverse-split stamp leaves the uncorrected upward ratio distortion
in place, which is the direction §3.5 identifies as buying a position. The direction of
harm here is unresolved, and resolving it needs the per-event comparison above rather
than an argument.

⚠ And that comparison can only locate an INCONSISTENCY between two vendor products. It
cannot adjudicate which one is right — #3280's finding still binds.

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
