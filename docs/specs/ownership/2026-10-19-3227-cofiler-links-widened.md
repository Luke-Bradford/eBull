# #3227 — every owner a joint filing names is one co-filer group; 13D/G-figure institutions join, 13F figures do not

Status: spec (2026-10-19), revision 4. It builds on #3894/#3895 (co-filer groups, merged `3040ca92`). Codex checkpoint 1
ran 3 rounds, the cap. Round 3 confirmed the guard proof and the arithmetic. Its one P1 was against revision 3's 13F rule,
and revision 4 removes that rule. Its P2/P3 items are fixed here without a fourth round, as #3894's were.

- **Round 1** (revision 1) raised 34 findings, 3 of them P1. Revision 1 also lifted the Section 16 writers'
  one-owner condition. Two of the P1s were against that lift, so it is deferred with its evidence ("Deferred: the
  writers' lift").
- **Round 2** (revision 2, read path only) confirmed the guard. It tested `guarded()` against an independent
  implementation on all 59,049 nested graph pairs on five vertices. Its remaining P1/P2 findings concerned 13F
  figures: the correction's bound, Form 13F General Instruction 2, source versus category, and a gross category
  matrix.
- **Revision 3** admitted a 13F figure on value agreement within 1%. **Round 3** rejected it: equal counts of a
  discretion figure and a beneficial-ownership figure do not identify the same shares. Two disjoint 100-share positions
  would become 100. So 13F figures are deferred (proposition 3).

Rung: a read-path metric change, which is the corpus rung (full-population A/B). This document is the judgement artefact.

## Problem

#3895 counts a clique of co-filers once, at its largest member. Two gaps leave a group's members counted once each.

1. **Pairs a third member's filing names.** An edge needs one of the pair's *own* kept rows on an accession that names
   both. Two owners named on a third member's filing, each with a row elsewhere, stay unlinked. The group is then a
   star, which is a chain, and stays split.
   - Example: DRVN's three Roark entities, 82,303,059 + 73,575,368 + 68,832,571 against 164,979,816 outstanding.
2. **`institutions` identities never join.** They are outside the node set (#3894 residual 3), and the caller reads
   co-filers only for `insiders`/`blockholders` accessions.
   - An `institutions` identity is a 13F filer by role. Where its 13D/G figure, which is beneficial ownership, wins
     owner-once's fold over its 13F figure, it is counted beside its own co-filers.
   - Example: FLG, `institutions` 113,520,361 (13D/G) beside `insiders` 75,023,575 and 75,000,094.

## Source rule

- **Form 4 General Instruction 4(b)(v)** (`https://www.sec.gov/files/form4.pdf`, read 2026-10-19): *"Where more than
  one beneficial owner of the same equity securities must report the same transaction on Form 4, such owners may file
  Form 4 individually or jointly."* Form 3 Instruction 5(b)(v) is worded the same.
- **Rule 13d-1(k)(1)** (17 CFR 240.13d-1): *"Whenever two or more persons are required to file a statement containing
  the information required by Schedule 13D or Schedule 13G with respect to the same securities, only one statement need
  be filed"*.
- **Rule 13f-1(a)(1)** (17 CFR 240.13f-1, `law.cornell.edu/cfr/text/17/240.13f-1`, read 2026-10-19): a manager *"which
  exercises investment discretion with respect to accounts holding section 13(f) securities"* files Form 13F. A 13F row
  is a discretion figure, not a beneficial-ownership figure.
- **Form 13F General Instruction 2**, "Rules to Prevent Duplicative Reporting" (`https://www.sec.gov/files/form13f.pdf`,
  read 2026-10-19): *"If two or more Managers … exercise investment discretion with respect to the same securities,
  only one such Manager must include information regarding such securities in its reports on Form 13F."* Special
  Instruction 5 adds the Notice and Combination report types for a manager whose holdings another manager reports.
  - The instruction allocates shared-discretion securities to one manager's report. The prevention log reads 13F as
    "additive across filers by design" on that basis.
  - Nothing in the instructions coordinates a 13F figure with a Section 16 or 13D/G figure, and a 13F figure measures
    a different quantity.

**Three propositions, kept apart** (prevention log, "Two signals over the same rows can prove DIFFERENT propositions"):

1. **Membership in a co-filing cohort.** Owners named on one joint filing reported the same securities together.
   - This spec calls them a co-filing cohort. It does not assert Rule 13d-5 group status, which needs facts we do not
     extract.
   - The evidence is the same as #3894's for an edge, two owners on one filing. It does not depend on which owner's
     row sits on that filing.
2. **Overlap between beneficial-ownership figures** (Form 3/4/5, 13D/G). They may be nested or disjoint, and nothing
   we extract says which. #3894's declared MAX policy applies.
   - MAX and SUM are arithmetic over the selected reported figures.
   - Their dates and class scope are not verified to match: membership evidence and position dates are different
     things.
3. **A 13F figure is not comparable.** It is investment discretion, and can include client accounts outside the
   cohort. Membership does not say how much of it is the cohort's, and value agreement does not identify the same
   shares (Codex round 3, P1). **An identity with any 13F kept row is not a node.** That keeps out RXT's, BOBS's and
   SARO's equal pairs (53 cliques on 52 instruments in the revision-3 census, −760,924,577), which wait for
   position-scope evidence.

## Full-population verification (dev, 2026-10-19, census, read-only)

`~/Dev/.ebull-autonomy-scratch/3227/links_p3.py` (revision-4 `graph`; `proxies` and `clique_rows` from `star_ab.py`),
from the repo root with `PYTHONPATH=.`, main `a5cacbd2`.

**What it is.** One `REPEATABLE READ` snapshot. Every rollup runs on the real code, and its pass input is recorded. The
widened graph is then built on that same input, with co-filers **re-read over all three categories' winning
accessions**, not taken from the production caller's read (prevention log, "A census of a widened rule must re-gather
its own inputs").

**What it is not.** The census is a projection: clique sets, removal totals and an incremental category matrix, not the
pass's real output rows. The build A/B runs the real pass (Acceptance).

**Population.**

- 5,509 instruments have a positive `insiders`, `blockholders` or `institutions` `_current` row.
- 4,396 reach the pass. 1,113 do not, each with the rollup's own `no_data_reason`: 898 `absent` and 215
  `stale_denominator`. There are 0 errors.
- 4,368 of the 4,396 have an outstanding figure, and **1,910 of those are oversubscribed in main**.

**Input invariants** the guard relies on (Change 3), counted over every pass input:

- `insiders`/`blockholders` kept rows with a `13f` winning source: **0**;
- `institutions` identities with both a `13f` and another kept row: **0**.

**Effect against main (#3895's pass on the same input).**

- The clique set changes on **107 instruments, every one down: −1,430,249,770 shares**. None goes up; none is net zero.
- **0 newly oversubscribed**; 11 fixed.
- 106 of the 107 have an outstanding figure, and **86 of those were oversubscribed in main**: 81%, against 44%
  (1,910 / 4,368). This is an association, not proof of correct overlap.

**New cliques: 108.**

- **64 are Section 16 / 13D-G only**, the widened edges among `insiders`/`blockholders`:
  - BBAI: AE Industrial's five CIKs, MAX 144,286,114, −249,915,434;
  - DRVN: three Roark entities, MAX 82,303,059, −142,407,939.
- **44 have an `institutions` member with a 13D/G figure.** FLG: 113,520,361, with `insiders` 75,023,575 and
  75,000,094.

**Category effect.** Base minus treated slice totals equal the incremental removals by category. The table gives them
by the removed row's category and winning source, summed per removed row, and they reconcile exactly to the total:

| category | removed rows (winning source) | shares |
|---|---|---:|
| `insiders` −1,214,359,303 | Form 4 | 729,148,919 |
| | Form 3 | 246,306,767 |
| | 13D | 238,903,617 |
| `institutions` −152,582,717 | 13G | 111,061,578 |
| | 13D | 41,521,139 |
| `blockholders` −63,307,750 | 13D | 61,614,643 |
| | 13G | 1,693,107 |
| **total** | | **1,430,249,770** |

**This category presentation is accepted.**

- A removed member leaves its own category, and no role moves (#3894's rule).
- Most of the removal is from `insiders`, where a fund entity's or GP's Form 3/4 figure folds under its own manager's
  or sponsor's larger 13D/G or Form 4 figure.
- Moving the group's figure into the removed member's category would be role transfer, which I14 does not provide.

**Candidate proxy test** (#3894's, frozen in `scripts/ab_3227_cofiler_groups.py::candidate_status`) on the 108 new
cliques. It is descriptive. It needs exactly one matching proxy row and a non-largest member above 5%, so it cannot see
separately listed disjoint members, and its disposition counts are shown so its coverage is visible.

| stratum | = MAX | between, nearer MAX | = SUM | between, nearer SUM | outside | >1 proxy rows | minor ≤ 5% | dates | no proxy row | other |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Section 16 / 13D-G only (64) | 1 (HSIC, KKR 18,853,071) | 0 | 0 | 0 | 0 | 31 | 16 | 9 | 7 | 0 |
| `institutions` 13D/G member (44) | 4 | 1 | 0 | 0 | 1 | 8 | 12 | 11 | 6 | 1 (no outstanding) |

**Without the guard** (Change 3), the first widening raised the pie on 2 instruments in the 296-instrument lift cohort
(+30,908,990). An extra node grew a #3895 clique into a non-clique, which then stayed split: the prevention-log failure
"A widening can make an existing fold WORSE".

## Change (read path only)

### 1. The co-filer read covers `institutions`

The caller of `_read_cofilers` passes the winning accessions of every kept row with `shares > 0` in `insiders`,
`blockholders` **and `institutions`**. The query is unchanged.

### 2. Nodes and edges (`_cofiler_graph`)

- **Nodes:** identities in `insiders`, `blockholders` and `institutions` that meet all of these:
  - owner total > 0;
  - CIK-keyed;
  - no `def14a` kept row;
  - **no `13f` kept row** (proposition 3).

  `etfs` stay out.
- **Curated institutional families never reach the pass.** `_reconcile_institutional_families`
  (`ownership_rollup.py:2860`) takes their channel rows out before owner-once, and the family holders are appended
  after this pass (`:6508`). The A/B gates that no pass-input CIK is a member of an appended family holder, and a test
  pins it (Acceptance).
- **Edges:** when a node's kept row (`shares > 0`, non-blank accession) sits on an accession whose co-filers include
  that node, **every pair of nodes whose canonical CIKs are among those co-filers links**, via that accession.
  - An accession that no node's own row sits on gives no edge.
  - Every #3894 edge is still an edge.

### 3. Guard: the widening never splits a clique #3895 collapses

1. Build #3895's graph (its node set and edges) and the widened graph, both from the same pass input.
2. A widened component that is a clique collapses.
3. A widened component that is not a clique gets no additional collapse. #3895's cliques inside it collapse as they do
   today.

**Guarantee, for the same pass input.** #3895's nodes are `insiders`/`blockholders` identities with no `def14a` row.
The new node set keeps every one of them, given the first input invariant above (no `13f` kept row in those
categories). The A/B asserts that invariant. If a rollup's input ever breaks it, the pass logs a warning and returns
#3895's own result for that rollup, so the guarantee holds there too. No upstream code makes the invariant
unreachable, so a raise would be the wrong default (prevention log, PR #3895). The edges are a
superset, so every #3895 clique lies inside one widened component.

A fallback component removes exactly what #3895 removes there. For a widened clique K holding the disjoint #3895
cliques M₁…Mₙ, plus nodes U that #3895 keeps whole, the difference is:

R(K) − Σ R(Mᵢ) = Σ MAX(Mᵢ) + Σ_{u∈U} tᵤ − MAX(K) ≥ 0,

because MAX(K) is one member's total, and that member is either some Mᵢ's MAX or in U, with every total > 0. So **this
pass's pie ≤ #3895's pass's pie on the same input.**

### 4. Unchanged, except the wording

Unchanged:

- the clique-only collapse at MAX;
- the keeper tie-break: the largest total, then an `insiders` member, then the lowest canonical CIK;
- the per-row `DroppedSource`s;
- one `cofiler_group_collapse` correction per clique, with `shares_removed` = SUM − MAX;
- no role transfer.

The correction's treatment sentence drops #3895's Rule 13d-5 claim (proposition 1) and states the figures as
arithmetic: *"Co-filers counted once at the largest member, {name} {MAX} (#3227). The members' reported figures sum to
{SUM}; MAX treatment selected. Overlap, dates and class scope unverified."*

The docstrings and comments of `_COFILER_CATEGORIES`, `_cofiler_graph` and `_reconcile_cofiler_groups` and the caller's
comment are updated to the new node, edge and guard rules in the same PR.

What the pass does, and no more: each collapsed clique contributes one member's total. It does not show that the
retained figure is free of overlap with an identity outside the clique.

## Accepted residuals

1. **MAX undercounts disjoint members**, as #3894 states.
2. **A widened non-clique component gets no additional collapse.** #3895's cliques inside it are preserved, and the
   rest stays double-counted.
3. **13F figures do not join** (proposition 3). That includes the equal pairs RXT, BOBS, SARO, CVI.US and WAY: 53
   cliques on 52 instruments, −760,924,577 in the revision-3 census. Folding them needs position-scope evidence: which
   of the manager's discretion accounts the cohort holds.
4. **DEF 14A-figure identities and `etfs` do not join.**
5. **A manager that is not a reporting owner on the cohort's filing is not linked.** GUTS: General Catalyst Group
   Management (CIK 1637946) is not on `0000950170-24-012828`.
6. **Links can be stale**, from an old winning accession (#3894 residual 4).

## Deferred: the writers' lift

Revision 1 lifted the one-owner condition in three helpers:

- `_form4_holding_verdict` (`insider_transactions.py:2169`);
- `form4_observation_shares` (`:2424`);
- `form4_fold_shares` (`:2541`).

Its evidence, from `star_ab.py` phase 2, which replays 2,061 accessions on 296 instruments:

- **Rows.** Summed rows equal a DEF 14A row on 25 rows, against 2 for today's figures. That is ±1%, with no as-of
  window.
- **Pie.** Lift plus widened links gives 58 instruments up (+330,472,142) and 42 down. 0 are newly oversubscribed, but
  already-oversubscribed instruments grow.

What its own spec must answer first (Codex r1):

- **Finding 1: overlap inside the sum.** Unequal lines on one joint filing can overlap. A GP line can restate its funds'
  lines with no two balances equal, and the equal-balance guard cannot see that.
- **Finding 8: double counts the lift adds where no link exists.**
  - ACTG: Starboard's two sibling Form 4s share no reporting owner. The pie goes 98,682,528 → 151,596,065 on 97,586,805
    outstanding.
  - Managers off the filing: GOGO/GTCR, TNET/Atairos, PLAY/Hill Path, GUTS, AARD, CAMP and SFBC, 59.7M in total.
  - DEF 14A co-owners.
- **Findings 2, 3, 12, 13 and 14.**
  - The `D`-line decision.
  - The display of a filing aggregate under `filers[0]`.
  - Form 5's path through `_form4_holding_verdict`.
  - That helper's signature and its `one_line` early return.
  - First-owner versus any-owner storage.

## Acceptance (build PR)

- **Full-population A/B**, committed as `scripts/ab_3227_cofiler_links.py`. It runs the real code against a pinned
  base SHA, in one `REPEATABLE READ` snapshot.
  - **Population accounting:** every instrument with a positive `insiders`, `blockholders` or `institutions`
    `_current` row.
    - selected = reached the pass + `no_data` (by `no_data_reason`) + errors;
    - errors = 0;
    - every non-reaching rollup carries a `no_data_reason`.
  - **Arms:** #3895's pass, run in the same rollup on the recorded input, against the real new pass.
  - **An independent oracle.** The script holds its own frozen copy of Changes 1–3. It **re-reads co-filers itself**
    over all three categories' winning accessions; it does not take the production caller's map.
    - Its collapse set must equal the real pass's corrections member for member, in number and in values.
    - Output rows must equal the expected rows exactly. The one permitted difference is the keeper's largest row's
      `dropped_sources`, which must equal the exact union of its own entries and each removed row's own and inherited
      entries, de-duplicated on `(filer_cik, filer_name, source, accession_number, shares)` with existing entries kept
      first.
  - **Exact gates, per instrument:**
    - treated pie ≤ base pie on every instrument;
    - newly oversubscribed = 0;
    - input total − output total = Σ `shares_removed`;
    - the two input invariants above are 0;
    - no pass-input CIK is a member of an appended family holder.
  - **Census reconciliation:** 107 instruments, −1,430,249,770, 11 fixed, 108 new cliques (64 / 44), and the category
    table above, with any drift explained.
  - **Treatment-quality gate, on the new cliques:**
    - = SUM ≤ 1;
    - between nearer SUM ≤ 1;
    - ≥ 4 in range (6 in the census);
    - the disposition counts reported per stratum;
    - every in-range and outside case listed with its members, rows and proxy key;
    - every outside case given a disposition in the PR (a data error, a stale row, or a counterexample to MAX).

    A failure stops the build for a re-decision. It is not waived.
  - **Latency:** paired base and treated `get_ownership_rollup` wall time on a warm second run, for the full population
    and separately for the 107 affected instruments. Gates: p95 regression ≤ 10% in each. The cost share of the
    co-filer read plus the pass is reported beside them.
- **Named figures**, checked against EDGAR:

  | instrument | before | after |
  |---|---|---|
  | DRVN | three Roark rows | 82,303,059 once |
  | BBAI | pie 640,511,590 | pie 390,596,156 (479,494,493 outstanding) |
  | HSIC | four KKR entities | 18,853,071 once (the proxy row) |
  | RXT | `insiders` 129,609,000 + `institutions` 13F 129,609,000 | unchanged (residual 3) |
  | TMUS | Deutsche Telekom AG 592,065,875 (#3895) | unchanged |

- **Corpus evidence:**
  - smoke panel (AAPL, GME, MSFT, JPM, HD) pie totals before and after;
  - cross-source: DRVN's joint filing on EDGAR, naming the three Roark entities;
  - no backfill, because the change is read-path only;
  - live re-check of `/instruments/DRVN/ownership-rollup`.
- **Tests:**
  - two owners named on a third node's accession, with no row of their own there, link and collapse at MAX;
  - an owner named only on an accession no node's own row sits on does not link;
  - an `institutions` identity with a 13D/G figure links, folds and keeps its own category;
  - an institution-only origin: the only qualifying accession is an `institutions` row's, which the old caller would
    not have read;
  - an identity with a `13f` kept row never links;
  - an `institutions` keeper: the other members leave their categories;
  - the guard, on a mixed input: a #3895 clique that a widened node or edge grows into a non-clique still collapses,
    and the pie is not above #3895's;
  - two #3895 cliques merged into one larger clique collapse once;
  - a non-clique widened component gets no additional collapse;
  - a curated-family member CIK that also has an `insiders` row is absent from the pass input;
  - an `etfs` row and a DEF 14A-figure identity never link;
  - the correction sentence.

## Security

Read path over stored public SEC data. There is no new input surface and no authorisation change.
