# #3227 — the co-filers of a joint filing are counted once in the rollup, at the group's largest member

Status: spec (2026-10-19), revision 4. Codex checkpoint 1 ran 3 rounds, the cap. Round 3 accepted MAX as an explicitly
declared policy and each residual as stated. Its remaining items (evidence wording, classifier order, cohort,
acceptance gate) are fixed in this revision without a fourth round. It follows the #3227 joint-filings
decision ([comment 6259236682](https://github.com/Luke-Bradford/eBull/issues/3227#issuecomment-6259236682)): the
Section 16 writers keep the one-owner refusal until the rollup counts a co-filing group once.

Rung: metric derivation at read time, which is the corpus rung (full-population A/B). This document is the judgement
artefact (Codex checkpoint 1).

## Problem

The rollup counts each beneficial owner once, by identity (`_identity_key`: CIK, else name; data-engineer I14). Members
of a group that files jointly have different CIKs. Each member that reaches the pie through its own channel therefore
adds its whole figure, and wherever the members' figures overlap, the shared shares are counted once per member.

The exact-value passes fold a member only when its figure equals another member's: #1764 (same accession), #1652
(cross-channel), #3886 (line grain) and #1645 (13D/G band). Members' figures are seldom equal, because their filings
carry different dates and scopes.

Today's pie, dev, 2026-10-19:

| instrument | rows in the pie today | what they are |
|---|---|---|
| TMUS | Deutsche Telekom Holding B.V., Form 4, 582,065,875 · Deutsche Telekom AG, 13D, 592,065,875 | The subsidiary and its parent. The two rows alone are 109% of the 1,072,671,613 outstanding. |
| IBRX | Patrick Soon-Shiong, 13D `0001193125-26-072879`, 745,576,456 (CIK 0001189020) · Patrick Soon-Shiong, 13D `0001193125-26-338651`, 745,518,285 (CIK 0001629222) | The largest reporter of two joint 13Ds, under two CIKs. Together 141% of 1,059,836,273. |
| INIO | AI Alpine (Luxembourg), Form 4, 646,500,000 · Advent International, 13G, 646,500,000 | Equal figures. #1764 does not fold them because they are on different accessions. #1652's value route does not either: 646,500,000 is a whole multiple of 100,000. |

## Source rule

- **Form 4 General Instruction 4(b)(v)** (`https://www.sec.gov/files/form4.pdf`, read 2026-10-19):
  - *"Where more than one beneficial owner of the same equity securities must report the same transaction on Form 4,
    such owners may file Form 4 individually or jointly. …"*
  - *"Transactions with respect to securities owned separately by any joint or group filer are permitted to be included
    in the joint filing."*
  - Form 3 General Instruction 5(b)(v) is worded the same (`https://www.sec.gov/files/form3.pdf`, read 2026-10-19).
- **Rule 13d-1(k)(1)** (17 CFR 240.13d-1, `law.cornell.edu/cfr/text/17/240.13d-1`, read 2026-10-19): *"Whenever two or
  more persons are required to file a statement containing the information required by Schedule 13D or Schedule 13G
  with respect to the same securities, only one statement need be filed"*.
- **Rule 13d-5(b)(1)(i)** (17 CFR 240.13d-5, same site and date): the group *"shall be deemed to have acquired
  beneficial ownership … of all equity securities of that issuer beneficially owned by any such persons."*
- **Item 403(a) of Regulation S-K** (17 CFR 229.403, `law.cornell.edu/cfr/text/17/229.403`, read 2026-10-19):
  - Each person known to own more than 5% of a voting class must be listed, including a Section 13(d)(3) "group".
  - Instruction 3 lets the registrant rely on the 13D/G filings it knows of.
  - Instruction 5 addresses *"more than one beneficial owner … listed for the same securities"*.

**What the rules settle, and what they leave open.** The prevention-log entry "Two signals over the same rows can prove
DIFFERENT propositions" applies here, so the three propositions are kept apart:

1. **Membership.** Joint filing is for owners "of the same equity securities". Co-filing one accession is evidence of
   a group (#1764 uses it that way). It does not make the members one identity, and it does not show that their
   figures overlap.
2. **The group's holding is the union of its members' holdings** (13d-5(b)(1)(i)).
   - The union lies between two bounds: MAX, reached when every figure is nested in the largest, and SUM, reached when
     the figures are disjoint. 4(b)(v) permits both shapes.
   - **Today's rollup already chooses one bound for every group: SUM.** It does so implicitly, with no evidence.
   - No structured field we extract carries the union. A 13D's Item 5 often states the group aggregate in narrative
     text, which we do not parse. Parsing it would be a separate source-extraction slice; it is not ruled out.
   - So this change makes a treatment choice between the two bounds. It does not recite a rule. The choice is made on
     the evidence below and frozen.
3. **Classification.** I14's most-specific-role rule applies to one identity. No rule makes a different member's role
   the group's, so this change does not transfer roles (Change 2).

## Full-population verification (dev, 2026-10-19, census only, read-only)

**Population and procedure.**

- The population is every instrument with a `_current` row (insiders or blockholders) on a joint accession: ≥2
  distinct reporting CIKs in `insider_filers`, or ≥2 in `blockholder_filings`. That is **4,005** instruments.
- Each rollup was built by `get_ownership_rollup` on main `e9012e71`, with 0 failures, and its `pie_wedge` holders
  were stored.
- Groups were then formed with the pass's exact predicate (Change 2).
- The census reads the stored rollups beside the live co-filer and proxy tables. It is not one snapshot; the build
  A/B is (Acceptance).
- Reproduce from the repo root with `PYTHONPATH=.`: run `cofiler_collect.py` (~11 min), then `cofiler_proxy5.py`. Both are
  loop scratch in `~/Dev/.ebull-autonomy-scratch/3227/`, kept out of the tree so the pre-push pyright does not scan them. The second writes every component to `cofiler_audit5.json` with:
  - members, categories and rows (name, source, accession, as-of, shares);
  - links, outstanding, the proxy row's key and status;
  - the Evidence 1 excess.
- The build PR commits this procedure, with its matcher frozen (Acceptance).

**Groups.** The pass's predicate finds **729** components of ≥2 identities.

- **626 are cliques**, meaning every pair is named together on some member row's accession. These collapse.
- **103 are chains.** They are left split (Change 2).
- 221 identities are excluded because their figure is a matched DEF 14A row.

**The treatment objective.** For a group whose overlap is unverified, the rollup must not attribute the same shares to
the pie twice. MAX is the only bound that never does, so the change counts each clique at its largest member: a
declared lower-bound policy. That choice of objective is the decision. The two pieces of evidence below show that
today's opposite policy, SUM, is wrong often and visibly. They do not show that MAX equals the true union in any
particular group. The correction carries the SUM, so the alternative stays visible (Change 2).

**Evidence 1, by construction: cliques whose SUM is impossible.**

- In 69 of the 626 cliques, the group's SUM alone exceeds the shares outstanding. That cannot happen for disjoint
  holdings drawn from that share count.
- Their proven excess, Σ(SUM − outstanding), is 5,616,009,049. The change removes 4,631,635,013 from them.
- Per clique, the removal can exceed the proven excess. TMUS's SUM is 101,460,137 above outstanding, and MAX removes
  582,065,875. In 31 of the 69, MAX itself exceeds outstanding. There the denominator (class or date) is as suspect as
  the overlap.
- Comparability caveats: stale figures, and classes outside the denominator. This is evidence against SUM, not a
  measurement of the union.

**Evidence 2, candidate proxy matches (descriptive, not decisive).** A single proxy row is read as a candidate group
figure when every condition holds:

- every member's name tokenises;
- every member row has an as-of within [proxy as-of − 400 days, proxy as-of + 30 days], where the proxy as-of is
  `period_end`, else `filed_at`;
- exactly one distinct `ownership_def14a_current` row, keyed `(source_document_id, holder_name_key, shares)` and
  zero-share rows included, matches any member;
- some member other than the largest holds more than 5% of outstanding.

The name match needs ≥2 shared tokens, or 1 where a name has only one. Tokens are lower-case alphanumerics of length
≥2, outside the census's stop list; the member's name is its first row's.

Item 403(a) would list a >5% member in its own right. A lone matching row is therefore *consistent with* a group row.
It is not verified as one: a fuzzy matcher can miss a member's own row.

The classifier tests the admissible interval first:

- **outside**: below MAX × 0.99 or above SUM × 1.01; supports neither bound and is excluded;
- **ambiguous**: within 1% of both MAX and SUM;
- **= MAX** or **= SUM**: within 1% of that bound;
- **between**: anything else, with distances reported.

Cliques only (the treated cohort):

| candidate row | cliques | detail |
|---|---:|---|
| = MAX (±1%) | 16 | |
| between, within 2.4% above MAX | 3 | RM +21,206 · PROP +438,324 · CPHC +23,812 |
| between, near SUM | 1 | NEXT: MAX 43,956,591 · SUM 67,367,433 · proxy 66,567,433 |
| = SUM (±1%) | 0 | |
| outside, excluded | 14 | |

The 606 other cliques are the 14 outside rows above and 592 that fail a condition. The 592, counted by first failure:

- 316: every minor member at ≤ 5%;
- 96: dates not aligned;
- 91: no matching row;
- 83: more than one matching row (Instruction 5 permits overlapping rows);
- 6: a name does not tokenise.

The 103 chains read 2 at MAX and none in range otherwise; they are untreated.

**Withdrawn evidence.** Round 1's matcher took the largest proxy row matching any member. That favours MAX by
construction whenever disjoint members are listed separately (Codex checkpoint 1, finding 4). Round 2's test counted
rows outside the interval as support and keyed rows by name. None of their counts is evidence.

**Structured discriminators tried for the SUM shape.** Neither is usable with the data we extract:

1. **Cover-page power (13D/G rows 7–10).** "Sole power is exclusive" cannot be used as a test. On joint statements
   whose members report equal aggregates, 2,051 of 6,135 rows have more than one member claiming sole power. Equal
   aggregates do not prove the securities are identical, so this shows only that the field cannot be relied on.
2. **A direct-ownership floor.** A joint filing's `D` line is stored under `filers[0]`, not under its holder (prevention
   log, "The SEC ownership XML has no per-owner attribution for Table I"). The inputs are wrong for this purpose.

Neither result refutes the ownership rules.

**Coverage.**

- Of 24,275 joint 13D/G accessions (≥2 reporter rows), 20,483 carry fewer than 2 distinct reporter CIKs; modern 13G
  omits them. This rule cannot see those accessions.
- Among the census's `insiders`/`blockholders` identities, 0 are NAME-keyed.
- 2,543 identities in `institutions`/`etfs` have a 13D/G figure (32,855,150,355 shares). They are excluded from
  linking (residual 3).

## Change (read path only)

### 1. Co-filers: `_read_cofilers(conn, accessions)`, one query per rollup

- **Input:** the non-blank winning accessions of the kept rows with `shares > 0` in the `insiders` and `blockholders`
  categories.
- **Output:** accession → canonical CIKs, the union of `insider_filers.filer_cik` and `blockholder_filings.reporter_cik`.
  - A canonical CIK has whitespace and leading zeros stripped.
  - Empty and all-zero values are dropped.
  - Only accessions with ≥2 canonical CIKs are returned.
- An empty input returns `{}` without querying.

### 2. The pass: `_reconcile_cofiler_groups(by_category, cofilers)`

It runs immediately after `_reconcile_owner_once`, before the institutional-family holders are appended. It is pure.

- **Nodes.** Owner-once identities (`_identity_key`, unchanged; no alias merging), selected as follows:
  - the identity is in `insiders` or `blockholders`;
  - its **owner total** (the sum of its kept rows' `shares`, what the pie adds today) is > 0;
  - none of its kept rows has source `def14a`. A proxy-backed figure can include holdings outside the group, and the
    proxy accession is not a co-filing.
  - The pass raises if an identity appears in two categories, the invariant owner-once guarantees.
  - `institutions` and `etfs` are untouched. Those categories can hold a 13D/G-figure winner of a 13F filer, and it is
    excluded with them (residual 3).
- **Edges.** Node A links to node B when one of A's kept rows meets all of these:
  - it has `shares > 0` and a non-blank accession;
  - its accession's co-filer set contains A's canonical CIK and B's canonical CIK;
  - B is a node. A zero-total or excluded identity is not a node, so it cannot bridge two others.

  Canonicalisation applies only to the lookup. A NAME-keyed identity has no CIK and never links.
- **Components and collapse.** Connected components of ≥2 nodes. **Only a clique collapses**: every pair of members
  must share an edge. A chain stays split, so a shared member cannot join two owners who never co-filed (Codex
  checkpoint 1, finding 12). A clique is counted at its largest member's owner total:
  - The representative is the largest member. Ties go to an `insiders` member, then to the lowest canonical CIK. Its
    rows, fields and category are unchanged. The group is not reclassified: no role moves between identities.
  - Every other member's kept rows leave the pie. Each removed row becomes its own `DroppedSource` on the
    representative's largest row, with the row's own source, accession, date, shares, CIK and name. No subtotal is
    synthesised.
  - The removed rows' own `dropped_sources` follow. They are de-duplicated on `(filer_cik, filer_name, source,
    accession_number, shares)`; on a key collision the existing entry stays.
- **Correction.** A new kind, `cofiler_group_collapse`, added to the service and API `CorrectionKind` literals and to
  `frontend/src/api/ownership.ts`.
  - One correction per clique; `shares_removed` = SUM − MAX.
  - The detail lists every edge as `member ↔ member via <accession>`, the clique's SUM beside its MAX, and the
    treatment: *"counted once at its largest member — #3227 co-filer groups. The group's holding is the Rule
    13d-5(b)(1)(i) union, at least its largest member and at most the SUM shown. MAX treatment selected; overlap
    unverified."*
  - `build_rollup_csv` emits the memo row it emits for `blockholder_group_collapse`.

### 3. Unchanged

- Every existing pass and its order. An exact-value fold runs first and reaches this pass as one row.
- The writers, every stored table, `_collapse_owner_lots`, the display and the memo overlays.
- This change does **not** establish that lifting the writers' one-owner condition is safe (finding 30). That is a
  separate slice with its own attribution criteria and a four-arm A/B: baseline, this pass, the lift, both.

## Effect (census, the pass's predicate)

- **Pie down on 535 of the 4,005 instruments**, net −9,486,777,273 shares. No instrument goes up, because MAX ≤ SUM.
- **Oversubscribed** (pie > shares outstanding): 1,710 → 1,666, with 44 fixed and 0 newly oversubscribed.
- **Category effect.** Without role transfer, a removed member simply leaves its own category:
  - in 98 cliques, an `insiders` member is removed under a `blockholders` representative. Example: TMUS, where
    Deutsche Telekom Holding's Form 4 leaves `insiders` and Deutsche Telekom AG's 13D stays in `blockholders`.
  - in 76 cliques, the reverse happens.
- **Chains left split:** Σ(SUM − MAX) = 2,247,559,804.

## Accepted residuals

1. **Disjoint or partly disjoint holdings inside a clique are undercounted.** The union of A = 20m and B = 10m held
   apart is 30m, and this change counts 20m. The same holds for the mixed case (A and B share a block, C holds apart:
   max(block, C) instead of block + C; finding 16), and for an owner whose total includes lots held outside the group
   (finding 13).
   - Today's SUM makes the opposite error for every nested group. It is impossible by construction in 69 cliques.
   - Of the 20 in-range candidate proxy matches, 16 read MAX ±1%, 3 read within 2.4% above MAX, 1 reads near SUM, and
     none read SUM.
   - Known undercounts:
     - NEXT: the Hanwha proxy row is 22,610,842 above the MAX; the change removes 23,410,842.
     - WEST: R. Brad Martin, Form 4, 1,930,734, and RBM Acquisition, Form 3, 1,700,000. The proxy row for R. Brad
       Martin, 3,696,955, is their SUM. WEST is not a candidate match, because RBM holds ≤ 5%.
   - A discriminator would need per-owner attribution we do not extract today: 13D Item 5 text, or joint-filing
     footnotes.
2. **Chains stay double-counted** (2,247,559,804 shares).
3. **Excluded categories and figures.**
   - `institutions`/`etfs` identities do not join. That includes 2,543 with a 13D/G figure (32,855,150,355 shares); how
     many of them would link is not measured.
   - DEF 14A-figure identities (221) do not join either.
4. **Links can be stale, and some are missing.**
   - A link comes from a kept row's current winning accession, which can be old. WEST's RBM link is a 2022 Form 3.
     There is no documented group-termination signal in the data we extract.
   - An earlier fold can remove the only accession that linked a member (finding 17).
   - 20,483 joint 13D/G accessions have too few reporter CIKs to give a link.
5. **The representative is chosen by size.** It can differ from #2408's record-holder choice for an exact-value fold
   upstream. That fold is unaffected.

## Acceptance (build PR)

- **Full-population A/B**, committed as `scripts/ab_3227_cofiler_groups.py`.
  - Scope: every instrument with a pie, run under the real code against a pinned base SHA, in one `REPEATABLE READ`
    read-only snapshot.
  - The script records each clique's members before the pass, so the gates and the candidate-match test run on the
    pass's real input, not on its output.
  - It imports one frozen matcher module, which holds the tokeniser, the stop list, the member-name rule and the
    classifier. The census and the gate therefore cannot drift.
  - **Exact gates:** pie up = 0; newly oversubscribed = 0; every collapse is a clique; no identity changes category.
  - **Census reconciliation:** 626 cliques, 9,486,777,273 shares and 535 instruments, with every instrument-level
    difference explained.
  - **Treatment-quality gate.** The policy is the declared lower bound (above), so the gate checks that the evidence
    still supports it and that its cohort has not vanished:
    - ≥ 15 in-range candidate matches among cliques;
    - = SUM ≤ 1;
    - between-nearer-SUM ≤ 2;
    - every in-range and outside match listed, with members, rows and the proxy key;
    - the Evidence 1 table regenerated, with SUM − outstanding beside SUM − MAX for each of the cliques.

    A failure stops the build for a re-decision. It is not waived.
  - **Latency:** p50, p95 and max of `get_ownership_rollup` per arm. The new query adds ≤ 10% at p95.
- **Named figures**, each checked against its filing:

  | instrument | before | after |
  |---|---|---|
  | TMUS | Holding 582,065,875 in `insiders` + AG 592,065,875 in `blockholders` | AG 592,065,875 in `blockholders` only |
  | IBRX | CIK 0001189020 745,576,456 + CIK 0001629222 745,518,285 | CIK 0001189020 745,576,456 in `insiders`, once |
  | INIO | AI Alpine (Luxembourg) 646,500,000 in `insiders` + Advent 646,500,000 in `blockholders` | AI Alpine 646,500,000 in `insiders` (the tie goes to the insiders member) |
  | NEXT | Hanwha Aerospace + HGC NEXT INV | Hanwha Aerospace 43,956,591, the stated residual |

- **Corpus evidence:**
  - Smoke panel: AAPL, GME, MSFT, JPM and HD, recording pie totals before and after.
  - Cross-source: TMUS against Deutsche Telekom AG's 13D/A `0000950157-26-000408` on EDGAR.
  - No backfill: the change is read-path only and changes no stored row.
  - Live re-check: `/instruments/TMUS/ownership-rollup` after the API reload.
- **Tests:**
  - a two-member cross-channel clique collapses at MAX, with per-row `DroppedSource`s;
  - an origin not on its own accession gives no edge;
  - a padded CIK matches an unpadded one, and an empty or all-zero CIK never links;
  - a NAME-keyed, zero-total or `def14a`-figure identity never links or bridges;
  - a chain stays split;
  - the tie-break;
  - no category change;
  - an `institutions` member is untouched;
  - a group #1764 already folded is unchanged;
  - the mixed A=B, C-apart case is pinned at its stated residual value;
  - an empty co-filer map;
  - two categories for one identity raises.

## Security

Read path over stored public SEC data. There is no new input surface and no authorisation change.
