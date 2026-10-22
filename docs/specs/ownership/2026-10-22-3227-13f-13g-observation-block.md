# #3227: the 13G extension accepts a non-uniform cover through the observation's block

Status: spec (2026-10-22), revision 3. It extends `docs/specs/ownership/2026-10-22-3227-13f-13g-covers.md` (#3961,
built in #3962 `96596972`; "the 13G extension" below). That spec, and the 13D arm's spec it extends
(`docs/specs/ownership/2026-10-21-3227-13f-position-scope.md`), stand in full: their rule, risks, Change and
Acceptance apply unless this document says otherwise. Research: the 2026-10-22 near-miss comment on #3227.

Rung: a read-path metric change, which is the corpus rung (full-population A/B). This document is the judgement
artefact (Codex checkpoint 1).

## Revision history

- **Revision 1** made (O) an alternative to uniformity inside one 13G uniqueness count. **Round 1** raised 1 P1, 7 P2s
  and 4 P3s, all accepted (§ Round 1 disposition). The P1: a shared count lets a new (O) match overturn a built (U)
  fold. Revision 2 makes (O) a third tier behind the 13D arm and (U), reserved against both, and re-runs the census as
  an oracle on the fold's input.

- **Revision 3** fixes round 2 in place (0 P1, 2 P2, 5 P3, all accepted; § Round 2 disposition). **Round 3**, the
  cap, accepted all seven dispositions and raised 0 P1, 0 P2 and 3 P3s, fixed here without a fourth round:
  - the block-order comparison is defined;
  - the BBOT test asserts the single template;
  - DH cites the route restriction.

## Problem

The 13G extension folds only when C is **uniform at T**: every stored block reports T. A 13G block carries no CIK, so
uniformity was the way to check a figure without tying any block to a person (13G-covers spec, § Why uniform). The
13D arm has no such condition: it checks M's own block and the twin's block, and other blocks on the cover may report
less.

So a joint 13G on which a parent and its manager each report T, above fund blocks that sum to it, stays counted twice.

Example: PRME. Alphabet Inc. (CIK 1652044) files a 13F of 16,562,498 for 2026-06-30.
- Its latest statement is Schedule 13G `0001845041-25-000005` (filed 2025-11-12), with 12 blocks.
  - Three of them, `Alphabet Holdings LLC`, `XXVI Holdings Inc.` and `Alphabet Inc.`, report shared dispositive =
    aggregate = 16,562,498.
  - The nine GV blocks report the three funds' holdings, 10,100,058 + 3,262,440 + 3,200,000, which sum to T.
- The current 13G observation of that accession has filer of record CIK 1845041, name `Alphabet Holdings LLC`, and
  16,562,498. It is the only kept row of the `blockholders` node for CIK 1845041.
- The pie counts 33,124,996 for the two, and stands at 190,863,072 against 181,376,856 outstanding.

## Source rule

The 13G-covers spec's Source rule applies unchanged: Rule 13d-1(k)(1), the cover-page fields, Rule 13d-3(a), the
Schedule 13G XML schema (package 23), and the Filer Manual (v75) §8.1.12. This spec adds no SEC rule. What it adds is
the **provenance of our own observation row**, which is code, cited here, plus the corpus measurements that bound it.

**Code contract.**
- `resolve_blockholder_reporter_identity` (`app/services/blockholders.py`) chooses the reporting person with the
  largest `aggregate_amount_owned` among the parsed blocks (ties: the first in XML order). It writes that person's
  `name` and `aggregate_amount_owned` to the observation, under the person's CIK when present and otherwise the filer of
  record.
- The sync path (`ownership_observations_sync.py`) writes from the largest **stored** block per accession. It skips a
  row whose `reporter_cik` is NULL; it maps any non-13D submission to source `13g` otherwise.
- The `blockholder_filings` upsert key is `(accession_number, COALESCE(reporter_cik, ''), reporter_name)`.

**Current corpus (dev, 2026-10-22).** These are measurements, not contracts:
- Every stored 13G block has a NULL `reporter_cik` (13G-covers spec, § Source rule). So in this corpus the sync path
  writes no 13G observation, and the upsert key allows at most one stored block per name on a 13G accession. The rule
  below does not rely on either: it keeps the 13G extension's all-NULL guard on the filer of record, and it requires
  exactly one block with the observation's name.
- Over every current 13G observation (`source = '13g'`, `known_to IS NULL`):
  - there are 37,977 rows, each the only current row for its `(instrument_id, source_accession)`;
  - each `reporter_name` equals exactly one stored block's `reporter_name` on that accession and instrument, byte for
    byte, in 37,977 of 37,977;
  - that block's aggregate equals the observation's, and is the accession's largest stored aggregate, in 37,975.
    The same 2 rows fail both (`0001085146-24-006081`, TD Securities (USA) LLC, 50,000 against a stored 30,000;
    `0001104659-25-005735`, Citadel Securities GP LLC, 885,153 against 885,105). The rule refuses both.

**What provenance shows, conditionally.** An exact name on the same accession locates the stored block the
observation was copied from, **provided** that stored block is the one the resolver chose. Two things can break that:
- **Same-name collapse.** The upsert key collapses XML blocks that share a name, so the surviving row may carry another
  same-named block's figures. Measured: 9 of 41,580 stored 13G bodies (13G extension, risk 8).
- **Later rewrites.** A re-parse that rewrote blocks without re-resolving the observation leaves them out of step.

The rule's figure checks catch a located block whose **current** values fail `reports(T)`, or an observation aggregate
that is not T. The observation carries no dispositive fields, so a change to the block's individual fields that keeps
sole + shared = aggregate = T passes. Nor do the checks catch
a same-named replacement that also reports T, or a change to some other block. These remain declared (risk 10).

**Block position is not used.** The XML `reportingPersonName` sequence, with later repeats of a name dropped, equals the
stored names in `filing_id` order on 41,580 of 41,580 13G bodies (`f13n_order.py`). For the 9 collapsed bodies this
compares the order of the surviving names only, and says nothing about which same-named XML block supplied the figures. Whether the filer
of record is the first block was measured as a name match, which is evidence about strings, not identity:
- 41,580 accessions: the filer of record is undefined on 3,603, and 3,871 more have no independent name for the filer's
  CIK (`insider_filers` or `institutional_filers`; `blockholder_filers.name` is not independent, being the first
  reporter's name). That leaves 34,106 compared.
- In 23,427 the filer's name matches the first block, and in 868 it matches another block but not the first. In 9,811
  it matches no block. Example: `PRICE T ROWE ASSOCIATES INC MD` against `T ROWE PRICE ASSOCIATES INC`. No match is not
  evidence of absence, and these were not adjudicated.

The 868 are enough to refuse position as a block locator. This does not dispute the Filer Manual's online-form rule:
those filings need not have used the online path. Script: `~/Dev/.ebull-autonomy-scratch/3227/f13n_filer_first.py`.

**Names** are matched as in the 13G-covers spec: a data-treatment decision with no source rule, fixed by construction
and frozen there. The observation's exact name is used only to locate a block on the accession it was copied from. It
identifies no entity.

## Rule (frozen)

Everything in the 13G-covers spec's § Rule applies. (O) is added as a **third tier**.

**Tiers, in order.** Nothing in a later tier feeds back into an earlier one.
1. **The 13D arm**, as built. Its matched set (cover reports T and has a twin, before uniqueness) is `T1`.
2. **(U), the 13G extension as built**, over candidates outside `T1`. The candidate's C must be uniform at T. Its
   matched set (C uniform and at least one twin, before uniqueness) is `T2`. Its uniqueness count and folds are
   unchanged.
3. **(O)**, over candidates outside `T1` and `T2`. Its matched set is `T3`.

**(O).** The candidate's C is selected exactly as the 13G extension selects it. (O) is evaluated only on that C, and
only when:
- C is a 13G that is not uniform at T;
- C's filer of record is defined (the 13G extension's definition, all-NULL guard included);
- the filer of record is not M.

A node k is an (O) twin when all of these hold:
1. **k is the filer.** k's canonical CIK is C's filer of record, k's canonical CIK differs from M's, and k's owner
   total is T. This is the built route.
2. **k's figure is C's observation.** Every kept row of k has `winning_source = '13g'` and `winning_accession` = C.
3. **One observation.** This instrument has exactly one current 13G observation of C (`source = '13g'`,
   `known_to IS NULL`), and its aggregate is T.
4. **The observation's block.** Exactly one stored block on C has `reporter_name` equal to the observation's
   `reporter_name`, compared byte for byte. That block reports T (`_CoverRow.reports`: both dispositive fields
   non-NULL, sole + shared = T, aggregate = T).
5. **T is C's largest figure.** No stored block on C has a non-NULL aggregate above T.
6. **M's blocks.** At least one block on C is named as M, using M's names exactly as the 13G extension takes them:
   `filer_name` of M's kept 13F rows, normalised. **Every** block named as M reports T.

Under the selection rule, condition 6's existence half always holds: with M not the filer, C was selected through a
block named as M. It is kept as a defensive predicate.

**Uniqueness in tier 3.** A candidate in `T3` is a duplicate when:
- it has exactly one twin;
- that twin is no other `T3` candidate's twin;
- that twin is in no `T1` or `T2` candidate's twin list (both taken before uniqueness).

A conflict refuses the (O) candidate only. So every 13D-arm and (U) decision of the built code is unchanged.

**Why only this route.** When M is C's filer of record, the built route finds k by a Form 3, 4 or 5 name on C. It does
not tie k's figure to C's observation, so there is no block to locate by provenance, and locating M's own block would
need a name use beyond the one the case allows. That route stays (U)-only by policy.

**What (O) shows.** C is one joint statement *"with respect to the same securities"*.
- The block the observation was copied from reports dispositive power and aggregate amount T. That figure is k's
  counted figure.
- Every block named as M reports T.
- No stored block on C has a non-NULL aggregate above T. NULL aggregates, dispositive fields on other blocks, and blocks
  missing from storage are not checked.

This is the 13D arm's test, M's block and the twin's block each reporting T, with the twin's block located by
provenance (conditionally, as above) instead of by CIK. In 2 of the census's cases the observation's block is the block
named as M, so k's counted figure is a copy of the block named as M. The other input is still M's 13F position, which
the policy treats as that block.

**What (O) does not need.** It attributes no block by position, and it needs no assumption that the filer of record is
a person on C. The 13G extension's risk 6 does not apply to a match by (O).

**The policy assumption** is the 13G extension's: M's 13F figure is the block k's figure already counts.

**Accepted identification risk (declared, not measured).** The 13D arm's risks 1–4 and the 13G extension's risks 5, 7
and 8 apply. Risk 6 does not apply to a match by (O). Also:

9. **Two persons, one figure.** The observation's block and a block named as M may be different persons. Each reports
   T on one statement, but the statement does not say their T is the same shares. The policy treats it as one block, as
   the 13D arm does for two blocks on one cover.
10. **Provenance.** The located block is the observation's source only when no same-name collapse and no later rewrite
    replaced it. A replacement that also reports T, or a change to another block, is not detected.

## Full-population verification (dev, 2026-10-22, read-only)

`~/Dev/.ebull-autonomy-scratch/3227/f13n_census_o.py` (log and JSON beside it), run from the repo root with
`PYTHONPATH=.` on main `96596972`. It exited 0.
- One `REPEATABLE READ` snapshot, session time zone UTC. Every rollup runs on the real code.
- It captures the **input** of `_fold_13f_duplicates` (covers, observation CIKs, holders), so built folds are not
  pre-applied.
- An oracle re-derives the three tiers before uniqueness, then their decisions.
- Its tier-1 and tier-2 folds equal the built code's 13D and (U) corrections on every instrument (0 mismatches).
- It applies the (O) duplicates to the co-filer pass input and re-runs the real `_cofiler_graph`. It exits 1 on an
  error, a tier-1/2 mismatch, a graph change, or a twin in a clique.

**Population.** 5,510 instruments selected; 4,396 reach the fold; 1,114 do not. Errors 0.

**Matched sets before uniqueness:** `T1` 19, `T2` 17, `T3` 14.

**(O) evaluation.** 328 candidates outside `T1` and `T2` have a non-uniform 13G C with a defined filer of record that
is not M. For each, every node on the filer's CIK with owner total T was tested as a twin; 61 such tests ran.
Candidates with no such node were not counted separately. Each twin test is counted at its **first failing check, in the
census's execution order** (the order of the rows below), so a row says nothing about the checks after it:

| outcome | count |
|---|---:|
| a kept row of k is not C's observation | 41 |
| a number of current observations other than one, or observation aggregate ≠ T | 0 |
| a number of blocks with the observation's exact name other than one, **or** that block fails `reports(T)` (one counter; the one case is IFF, whose single exact-name block has dispositive 0 against aggregate T) | 1 |
| a stored block with a non-NULL aggregate above T | 0 |
| no block named as M | 0 |
| blocks named as M, not all reporting T (all 5 had one such block, which failed) | 5 |
| **(O) twin** | **14** |

**Uniqueness:** 0 refused for more than one twin, 0 for a shared twin, 0 reserved by `T1` or `T2`.

**Effect.** **14 duplicates on 14 instruments, −72,613,547.**
- #3898's edges and cliques are unchanged on all 14, and no twin is in a clique.
- 1 instrument is fixed (PRME), 0 are newly oversubscribed, and 13 of the 14 were oversubscribed in main.
- Every k has one kept row, C's observation.
- No C is among the 9 same-name collapse accessions (13G extension, risk 8).

| instrument | M (13F CIK; kept 13F name) | k: filer of record; observation's block | T | C (filed) | observation's block named as M |
|---|---|---|---:|---|---|
| PRME | 1652044; Alphabet Inc. | 1845041; Alphabet Holdings LLC | 16,562,498 | `0001845041-25-000005` (2025-11-12) | no |
| EQPT | 1305473; Insight Holdings Group, LLC | 1629398; Insight Venture Associates IX, L.P. | 13,526,731 | `0000899140-26-000587` (2026-05-15) | no |
| ANNX | 1056807; BVF INC/IL | 918923; BVF PARTNERS L P/IL | 8,698,173 | `0000921895-26-001383` (2026-05-15) | no |
| IVVD | 1802630; Soleus Capital Management, L.P. | 1955982; Soleus Capital Management, L.P. | 7,708,727 | `0001213900-26-015651` (2026-02-12) | yes |
| XNCR | 1056807; BVF INC/IL | 918923; BVF PARTNERS L P/IL | 7,133,720 | `0000921895-26-000477` (2026-02-17) | no |
| EVH | 2032489; Rubicon Founders LLC | 1982325; Rubicon OP GP II LP | 5,816,123 | `0000950103-25-014793` (2025-11-14) | no |
| BBOT | 1009258; DEERFIELD MANAGEMENT COMPANY, L.P. | 1352546; Deerfield Management Company, L.P. | 4,885,446 | `0001193805-25-001205` (2025-08-18) | yes |
| IRD | 1813845; BIOS Capital Management, LP | 1791917; AARON G.L. FLETCHER | 3,683,429 | `0001791917-26-000005` (2026-05-13) | no |
| MAZE | 1652044; Alphabet Inc. | 1845041; Alphabet Holdings LLC | 1,388,583 | `0001168404-26-000010` (2026-02-06) | no |
| STRO | 1056807; BVF INC/IL | 918923; BVF PARTNERS L P/IL | 1,251,788 | `0000921895-26-000502` (2026-02-18) | no |
| WNEB | 1904067; STRATEGIC VALUE BANK PARTNERS LLC | 1656345; Strategic Value Investors LP | 865,565 | `0001656345-25-000008` (2025-02-12) | no |
| TYRA | 1850694; Canaan Partners XI LLC | 1712417; Canaan XI L.P. | 750,000 | `0001193125-26-177291` (2026-04-24) | no |
| KRRO | 1832749; 72 Investment Holdings, LLC | 1603466; Point72 Biotech Private Investments, LLC | 276,831 | `0000919574-26-003500` (2026-05-15) | no |
| PRAX | 1832749; 72 Investment Holdings, LLC | 1603466; Point72 Biotech Private Investments, LLC | 65,933 | `0000919574-26-003465` (2026-05-15) | no |

- P is 2026-06-30 for all 14.
- Each M has one kept 13F name, and it names exactly one block on C.
- **IVVD:** the observation's block has the same name as M's 13F filer, but the filer of record is another CIK
  (1955982). The data does not settle whether M and k are one entity. The fold counts that block once either way.

## Change (read path only)

### 1. The observation read

`_FILERS_13G_SQL` also returns `reporter_name` and `aggregate_amount_owned`, with its filters and partition bounds
unchanged. `_read_13g_filers` returns, per accession, the current rows' `(reporter_cik, reporter_name,
aggregate_amount_owned)`. `_13g_filer_of_record` reads the CIKs from that shape and is otherwise unchanged. No new
query.

### 2. `_fold_13f_duplicates`

After the (U) tier's uniqueness and folds, add tier 3 as above:
- its candidates are those outside `T1` and `T2`;
- C selection is shared with (U) and not repeated with different inputs;
- the reservations are `T1`'s and `T2`'s twins;
- it keeps its own uniqueness count;
- the fold itself is the 13D arm's.

The correction for a match by (O) has one wording. `{M blocks}` is the distinct stored names of the blocks named as M,
sorted and joined by "; ". `{same}` is the text " (one of them is that block)" when the observation's block is among
them, and empty otherwise:

*"13F duplicate counted once (#3227): {M} ({M CIK}) 13F {T} for {P}; on joint Schedule 13G {C accession} (filed {date},
filer of record {CIK}) the block of {observation name}, counted here as {k}'s figure, reports dispositive power and
aggregate amount {T}; the block(s) named {M blocks} each report the same{same}; no stored block has a non-NULL aggregate
above {T}; {M} is matched by name. Counted once by policy: identity by name, the observation's source block,
completeness of the stored blocks, overlap on that statement, across dates and in class scope are not verified."*

A match by (U) or by the 13D arm keeps its built wording.

### 3. Docstrings

- `_fold_13f_duplicates` and the section comment name this spec and the three tiers.
- `_read_13g_filers` states its new return shape.

## Accepted residuals

1. The declared identification risk (§ Rule).
2. **DH**, 62,493,676. M (Advent International, L.P.) is C's filer of record, and k (Advent International GP, LLC) is
   found by its Form 3 name. (O) does not apply on that route (§ Rule, "Why only this route").
3. **TDAY**, 12,128,756. k's kept row is a later 13G filed after P, not C's observation.
4. **IFF**, 25,356,381. The observation's block reports dispositive 0 against an aggregate of T.
5. Locating the filer's block by position stays refused (§ Source rule).
6. The 13G extension's residuals 2–4 stand for every case (O) does not reach. Class C (10-21: M's cover row is not T at
   P) stays refused.

## Acceptance (build PR)

As the 13G extension's Acceptance, with the changes below.

**Inherited tests, amended.**
- "A block off T on C" and "a NULL dispositive field or NULL aggregate on a block that is neither M's nor k's" become
  refusals only when (O) also fails. Each is paired with an (O) positive in which the same unrelated block is off T, or
  has a NULL field, and the case folds.
- "A newer non-uniform 13G naming M pre-empts an older uniform one" now expects the newer C to be evaluated under (O):
  it folds when (O) holds and refuses otherwise. The older C is never used.
- "The same, but A's C is not uniform: A has no twins, so B folds onto k1" still holds whether or not A qualifies under
  (O): B is a (U) candidate, and A, as an (O) candidate, cannot reserve against (U). When A qualifies under (O) with
  twins {k1, k2}, A refuses (two twins, and k1 reserved by B). That exact variant is added under Order below.

**Full-population A/B**, as `scripts/ab_3227_13f_13g_covers.py` or a sibling. The base is the real code at
`96596972` or later main. Combined-pass and increment assertions stay separate:
- **Combined pass:** the 13D-arm corrections and the (U) corrections equal the base's, member for member.
- **Increment:**
  - an independent oracle carries its own frozen copy of the three tiers, their pre-uniqueness matched sets, and (O);
  - its (O) duplicate set equals the treated pass's (O) corrections, member for member. They are told apart by the
    wording "counted here as".
  - per instrument, treated pie = base pie − Σ T over (O) duplicates only;
  - #3898's edges and collapse list are unchanged, and newly oversubscribed = 0;
  - input − output total = Σ `shares_removed` over all corrections;
  - base, treated and oracle errors are each 0.

**Census reconciliation:** the cases below, with any drift explained.

**Case review table in the PR**, for every (O) duplicate, against EDGAR `primary_doc.xml` (served under the issuer
CIK path) and the stored rows:
- the block count, and all three figures for every block, with the largest aggregate equal to T;
- the filer of record equals `filerCredentials/cik`;
- the observation's name is a `reportingPersonName` in the XML, and its block reports T;
- **every** block named as M is listed, each is a `reportingPersonName` in the XML, and each reports T;
- **every** kept row of M (its 13F rows, giving the matched names) and of k (each C's observation) is listed.

Any mismatch stops the build for a re-decision.

**Latency:** the 13D arm's gate, on the full population and on the affected instruments.

**Named figures:** PRME (−16,562,498); DH and TDAY unchanged (residuals 2 and 3).

**Corpus evidence:** smoke-panel pie totals before and after; no backfill; a live re-check of
`/instruments/PRME/ownership-rollup`.

**Tests** (pure unless stated):
- **Folds by (O):**
  - the PRME shape: the observation's block and the block named as M are different blocks, and unrelated blocks report
    less;
  - the BBOT shape: the observation's block is the block named as M. The correction is the single template, listing
    that block and ending its list with the "(one of them is that block)" marker;
  - M with two distinct 13F names, each naming a block on C, both reporting T: folds, and the correction lists both;
  - one 13F name of M matching two stored blocks whose names normalise equally (`ACME, LLC` and `Acme LLC`), both
    reporting T: folds, and the correction lists both;
  - the same-block shape with a second block named as M: the correction lists both, with the "(one of them is that
    block)" text.
- **Each refusal alone:**
  - M is C's filer of record (no (O) on that route);
  - C's filer of record undefined;
  - zero, or two, current observations of C;
  - the observation's aggregate ≠ T;
  - one of k's kept rows from another accession or from a Form 4, while another row is C's observation;
  - no stored block with the observation's exact name, including a name that differs only in case or punctuation;
  - two stored blocks with the observation's exact name (a defensive test; the current corpus has none);
  - the observation's block fails `reports(T)` on a NULL dispositive field, on dispositive ≠ T, and on aggregate ≠ T;
  - a block on C with an aggregate above T, while every other condition holds;
  - two distinct 13F names of M, one naming a block that fails `reports(T)`;
  - one 13F name of M matching two stored blocks that normalise equally, one of which fails `reports(T)`;
  - no block named as M (a defensive predicate test, not reachable through selection).
- **Order and uniqueness:**
  - a (U) fold on k and an (O) candidate whose only twin is k: the (U) fold stands, and the (O) candidate refuses;
  - a (U) candidate refused for uniqueness still reserves its twins against (O);
  - a 13D-arm match, folded or refused, reserves its twins against (O);
  - two (O) candidates sharing a twin both refuse;
  - an (O) candidate with two twins refuses (two nodes on the filer's CIK);
  - candidate A in `T2` with twins {k1, k2} and an (O) candidate B with {k1}: B refuses;
  - a (U) candidate B with twin {k1} and an (O) candidate A with twins {k1, k2}: B folds onto k1, and A refuses;
  - (O) is not evaluated on an older statement when M's newest statement is a non-uniform 13G that fails (O).
- **(U) unchanged:** a uniform C still folds on both routes with its built wording, and an (O)-qualifying shape on
  uniform C is decided by (U).
- **Read:** the observation read returns name and aggregate, with the same filters (the existing DB test, extended).

## Round 1 disposition

| # | finding | revision 2 |
|---|---|---|
| 1 | P1: shared (U)/(O) uniqueness overturns (U) folds | (O) is tier 3, after (U)'s folds; `T1` and `T2` twins reserved; tests for both directions |
| 2 | P2: "T is C's largest" not enforced | condition 5 added, with a test; the census applies it |
| 3 | P2: provenance overclaimed | stated conditionally; collapse and rewrite named; risk 10 restated |
| 4 | P2: census ignored refused matches | the census is an oracle on the fold's input with `T1`/`T2`/`T3` before uniqueness |
| 5 | P2: M-name source | the census uses M's kept 13F `filer_name`, as the rule does; table names come from those rows |
| 6 | P2: inherited tests conflict | amended explicitly, with paired (O) positives |
| 7 | P2: universal conditions untested | "every" in the case review; mixed k-row, multi-name and (O)-vs-(O) tests |
| 8 | P2: corpus measurements stated as contracts | split into code contract and corpus measurement; all-NULL guard kept |
| 9 | P3: positional evidence | reported as name-match outcomes with the excluded population; "mostly" removed |
| 10 | P3: opposite-route explanation | restated as the built name-source restriction |
| 11 | P3: correction wording | same-block variant; deterministic multi-name list; "counted twice" qualified |
| 12 | P3: quantifiers | the 2 rows identified; mixed pass/fail counted; defensive test labelled |

## Round 2 disposition

| # | finding | revision 3 |
|---|---|---|
| 1 | P2: inherited A/B uniqueness test contradicts the tiers | B, a (U) candidate, folds whatever A's (O) status; the exact two-twin variant is a test |
| 2 | P2: one M name matching two stored blocks | positive and refusal tests with two blocks that normalise equally |
| 3 | P3: "no block reports more" | restated as "no stored block has a non-NULL aggregate above T", in the rule text and the correction |
| 4 | P3: same-block wording drops other M blocks | one wording that always lists every block named as M, with a same-block marker; tested |
| 5 | P3: sequential counters read as independent | the table is labelled first-failure in execution order, rows in that order |
| 6 | P3: dispositive divergence overclaimed | the checks detect current values failing `reports(T)`, not field-level changes |
| 7 | P3: observation-block counter conflates causes | labelled as one combined counter; the single case is identified |

## Security

Read path over stored public SEC data. There is no new input surface and no authorisation change.
