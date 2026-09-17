# #2794 verdict — the proposed key does not reach the cases, and the worked case has an upstream cause

Status: **spike discharged, 2026-09-17. No change to `ownership_rollup.py`.** Read-only
throughout; no migration, no parser version, no backfill, no row mutated.

Instrument: `scripts/audit_2794_fold_anchor.py` (`--edges` / `--joint` / `--balances` /
`--cases`). Every figure in §1, §3 and §4 is computed by it at run time; §2's trace is a
single named filing and is reproduced by the queries quoted inline.

## What #2794 asked

The control-group fold buckets candidates by an EXACT share value, so a fold is
anchor-dependent: remove a member row and the cluster can stop folding, and a block counted
ONCE is counted N times — the wedge GROWS, which is the direction #2226 tracks. The ticket
proposes keying membership on the record-holder / `natureOfOwnership` chain (#2408) instead,
and warns against widening the value match to a tolerance (#2785).

⚠ One correction to the mechanism as the ticket states it. Removing a row cannot make two
surviving values stop being equal. What removal does is take the bucket **below the fold's
own admission threshold** — `_reconcile_same_accession_groups` and
`_reconcile_insider_control_groups` both require ≥2 distinct identities in the bucket, so
dropping one member of a 2-member cluster ends the fold. The anchor-dependence is real; it
is a membership-count effect, not a value-matching effect.

## 1. The proposed key does not reach the cases #2794 names

`--edges` builds every #2408 naming edge on the full population using the module's own
`_read_record_holder_evidence` and `_normalise_holder_text`, over the rendered candidate set
(the #2788 Form 4 retention bound and the #788 dual-pipeline de-collision applied from the
module's own SQL constants).

```
PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --edges --out /tmp/a2794.jsonl
PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --summarise /tmp/a2794.jsonl
```

| | |
| --- | ---: |
| instruments scanned / insider rows | 4,966 / 87,523 |
| naming edges | 25,900 |
| … equal-value (the anchor already buckets these) | 17,312 |
| … unequal-value (the anchor is blind to these) | 8,588 |
| instruments with ≥1 edge | 937 |
| harness errors | **0** |

Abundant — and it thins sharply under the guard that already exists for its failure mode. A
control-chain footnote names every tier ("GEI Capital VI, LLC is the general partner of GEI
VI"), which is why `_named_record_holder` fails closed on a text naming ≥2 members. Applying
that per speaking line, and discarding groups where the named target itself carries more than
one balance (otherwise the verdict depends on file order):

| of 4,958 speaking lines | |
| --- | ---: |
| dropped — names ≥2 members | 3,362 |
| dropped — target holds >1 balance | 254 |
| unique-named, equal value | 775 |
| unique-named, speaker < target | **294** |
| unique-named, speaker > target | 273 |

Then the check that decides it — the seven instruments #2794 names
(`--from /tmp/a2794.jsonl --cases WBD ACI BF-B IPAR KIDS CAR DWSN`):

| symbol | edges | speaking lines | unique-named, unequal |
| --- | ---: | ---: | ---: |
| `IPAR` (the ticket's worked case) | **0** | 0 | 0 |
| `ACI` | **0** | 0 | 0 |
| `BF-B` | **0** | 0 | 0 |
| `WBD` | 4 | 1 | 0 (one speaker names 4 members) |
| `CAR` | 2 | 1 | 0 (equal-value) |
| `DWSN` | 10 | 6 | 0 (all equal-value) |
| `KIDS` | 2 | 2 | **1** |

**The proposed key produces a usable unequal-value edge on 1 of the 7.**

⚠ **What this does and does not establish.** It measures the COVERAGE of one existing
extractor over the current candidate set. It does NOT show the underlying relationships are
absent: evidence is retrieved by `(accession, shares)`, so an aggregated or changed balance
hides it; `insider_initial_holdings` carries no `footnote_refs`, so a Form 3's
`See footnote.` is unresolvable; and a conformed-name alias defeats containment. "Zero edges"
therefore means "this extractor found nothing here", not "the filing has no indirect-ownership
text". The claim is bounded to coverage, and on coverage the key is not sufficient to carry
the cross-accession pass.

## 2. The worked case's cause is upstream of the fold

`IPAR`'s cluster is `BENACIN PHILIPPE` (6,846,064, `indirect`) beside
`Philippe Benacin Holding SAS` (6,871,064, `beneficial`) on accession
`0001753926-25-001883`.

The filing is a genuine **joint Form 4 with two reporting owners** — `Philippe Benacin` and
`Philippe Benacin Holding SAS` — verified against the SEC's own rendering of
[`form4.xml`](https://www.sec.gov/Archives/edgar/data/822663/000175392625001883/xslF345X05/form4.xml).
So the two identities are co-filers and the deemed-chain reading of them is right. (Our
`insider_transactions` records only `BENACIN PHILIPPE` for the Table I rows; that is
`_extract_holdings` assigning every row to `filers[0]`, which the sec-edgar skill §2.3 already
documents, not a new defect.)

Its Table I carries two non-derivative lines for one position, both
`By personal holding company`:

| txn | code | shares | amount owned following (Table I col. 5) |
| --- | --- | ---: | ---: |
| option exercise | `M` | 25,000 | **6,871,064** |
| sale | `S` | 25,000 | **6,846,064** |

The DERA dataset fans both lines onto both reporting owners, so
`ownership_insiders_observations` holds four rows for this accession plus the XML row. The
#788 de-collision drops the DERA rows for CIK `0000901877` (an XML row exists for the same CIK
+ accession) but cannot touch the `Philippe Benacin Holding SAS` identity, which carries a
different CIK. That identity's two observations then tie on every ordering key — same
`source`, `period_end`, `filed_at`, `source_accession` — and the winner falls to the
projection's **final lexical tie-break**, `source_document_id ASC`
(`app/services/ownership_observations.py:341`). `…:NDT:8823454` sorts before
`…:NDT:8823455`, so the **pre-sale** balance 6,871,064 becomes current.

**So the two co-filers disagree by 25,000 shares only because one of them is carrying a
balance the same filing supersedes.** The falsifiable consequence: if the projection selected
the same-filing final balance for both identities, both would read 6,846,064, the existing
`(accession, shares)` bucket would match, and the fold would fire **with no change to the
anchor at all**. `IPAR` is a symptom of the projection, not of the fold.

Filed as **#3146**. It is upstream of `ownership_insiders_current`, so it reaches every
ownership consumer, not just the insiders wedge.

⚠ **This is why the first draft of this document was wrong, and the error ran the dangerous
way.** That draft proposed dropping `shares` from `_reconcile_same_accession_groups`' bucket
key and folding at MAX. On `IPAR` that "fixes" the ticket's case by **preserving the
superseded 6,871,064 balance** — locking in a 25,000-share overstatement while presenting as a
removed double-count. Caught at Codex checkpoint 1, which read the filing.

### The general shape, measured

```
PYTHONPATH=. uv run python -m scripts.audit_2794_fold_anchor --balances
```

Live `ownership_insiders_current` rows (verified against the table, not re-derived) whose
winner was settled by that lexical tie-break against another observation **from the same
filing at a different value**, compared with the lexically LAST such observation:

| | |
| --- | ---: |
| live (instrument, holder, nature) keys | **41,403** |
| instruments | **3,871** |
| Σ abs disagreement vs the lexically-last observation | 134,079,682,786 |
| winner is the larger of the pair | 24,890 |
| winner is the smaller | 16,502 |

⚠ **Three things this does not say.** "Lexically later" is not proven to mean "a later
transaction" — the `:NDT:` suffix is a DERA row identifier, and that it tracks transaction
order is verified on the ONE filing above, not on the population. Two differing balances on
one filing are not proven to be successive states of one position; they can be separate lots.
And the 60/40 direction split is not evidence of anything on its own — an ordering rule that
happened to be systematically backwards would also produce a skew. The finding is the one
that needs none of those: **the projection has no transaction-sequence criterion, so on a
multi-line same-day filing the choice falls through to string order.** That is visible in the
`ORDER BY` itself, and 41,403 live rows are decided by it.

## 3. Why the joint-accession widening was withdrawn

`_reconcile_same_accession_groups` (#1764) buckets on `(winning_accession, shares)`
(`ownership_rollup.py:3324`) while its own docstring (`:3273`) argues that a shared accession
"needs **NO** magnitude floor or roundness proxy — those guards exist only to substitute for
the membership evidence that a shared accession already provides". Dropping `shares` from that
key looked like the same argument applied consistently. `--joint` prices it:

| | |
| --- | ---: |
| joint-accession insider clusters (≥2 distinct identities) | 2,136 |
| … all members at one value | 1,728 |
| … members at ≥2 values | **408** (336 instruments) |
| Σ − MAX over the ≥2-value set | 14,227,117,543 |
| … of those, ≤1 member reporting `direct` | 313 (260 instruments), 9,823,307,408 |
| … of those, ≥2 members reporting `direct` | 95, 4,403,810,135 |

⚠ These are CANDIDATE clusters, not rendered outcomes, and Σ − MAX is not a treatment
effect — it ignores partial folds inside a cluster, cross-channel consumption, representative
changes and owner-once. It bounds the arithmetic, nothing more.

The widening is withdrawn on four grounds that do not depend on its size:

1. **The source rule points the other way.** Form 4 General Instruction 4(b)(v) expressly
   permits separately owned securities to be reported in one joint filing, and Rule 13d-1(k)
   requires information *for each person*. Co-filing evidences that the filers are related
   enough to file together; it does not establish that two reported amounts describe ONE
   block. The docstring's argument retires the magnitude floor and the roundness proxy — both
   guards against numeric COINCIDENCE, which a shared accession really does displace. The
   value equality carries a different proposition, and nothing here displaces that one.
   ⚠ Symmetrically: Instruction 4(b)(v) means equal amounts do not *prove* one block either.
   Equality remains a heuristic; it is simply the only one present.
2. **MAX is not the union.** `{block 100 + personal 10}` and `{block 100 + personal 20}` total
   130, not 120. Neither containment nor a member reporting the complete union was shown.
3. **"All existing folds preserved" is false.** `A direct 100`, `B direct 100`,
   `C beneficial 200` on one accession is **300** today — the `{100,100}` bucket folds, `C`
   passes through. Under an accession-only bucket the cluster is unequal, two members report
   `direct`, the proposed chain-shape gate refuses, and nothing folds: **400**. A widening can
   make an existing fold worse by swallowing it into a cluster that fails a gate the narrow
   bucket never had to pass. Pinned by
   `tests/test_same_accession_group_collapse.py::test_mixed_value_joint_accession_folds_only_the_equal_subcluster`.
   ⚠ This refutes *that* widening — accession-only bucketing plus a refusal gate. It does not
   refute every accession-keyed scheme; preserving equal subclusters before widening avoids
   this particular shape.
4. **The proposed gate could only refuse, never establish.** An absent `direct` does not
   demonstrate a deemed chain: all-`beneficial`, unknown-nature and independently held
   indirect positions all pass it. It is not #2230's validated tier either — that requires ≥3
   distinct CIKs, `is_ten_percent_owner`, and a POSITIVE floor of ≥2 `indirect` members, and
   the positive floor is the part that matters (see §4).

## 4. The provenance gate, and what actually makes it safe

Re-running §3's shape split with `direct` gated on Table-I provenance
(`source_document_id !~ ':(NDT|NDH):'`, which the sec-edgar skill mandates for any read-path
branch on `ownership_nature`) puts **408 of 408** clusters in the ≤1-direct bucket — it admits
every one of the 95 the raw string refuses.

The first draft read that as "the skill's rule inverts depending on whether you SELECT or
REFUSE". That framing is wrong, and `_is_deemed_chain` (`ownership_rollup.py:2676`) is the
counter-example: it already applies the provenance gate in a refusal context
(`n_direct <= _DEEMED_CHAIN_MAX_DIRECT`) and is not vacuous. What saves it is that the SAME
attested set also has to clear a **positive floor** — `n_indirect >= 2`. If no row is
attested, `n_indirect` is 0 and the gate REFUSES.

The lesson is therefore about pairing, not direction: **a provenance-gated counter used as an
upper bound is vacuous unless a positive floor is imposed on the same gated set.** A bare
`n_direct <= 1` over a population whose rows are mostly DERA-sourced cannot fail.

⚠ Stated as measured: 408 of 408 is universal admission on this population. It is not proof
that the predicate is false for every row — each cluster could in principle contain one
attested direct holder and still pass. The script prints the raw-string and Table-I-gated
counts side by side so the divergence is visible in the output rather than inferred.

⚠ Separately, the repo mis-numbers this column. `_read_record_holder_evidence`,
`_named_record_holder` and `.claude/skills/data-sources/sec-edgar.md` §2.3 all say
"Table I column 5 carries `natureOfOwnership`". Verified against the SEC's rendering of the
`IPAR` filing: **column 5 is "Amount of Securities Beneficially Owned Following Reported
Transaction(s)", column 6 is "Ownership Form: Direct (D) or Indirect (I)", and column 7 is
"Nature of Indirect Beneficial Ownership"**. The code is correct; only the citation is wrong.
`.claude/**` is write-refused from a loop worktree (#2403), so the skill correction is posted
on the ticket for a session with permission.

## Where #2794 stands

**Open, anchor-dependence unfixed.** What a next attempt now knows and need not re-derive:

- The #2408 naming chain does not COVER these clusters (§1) — measured, with the coverage
  caveat stated.
- `IPAR` is a #3146 symptom, and #3146's fix is predicted to restore that fold with no change
  to the anchor (§2). **Re-measure the remaining six cases after #3146 lands before treating
  any of them as fold defects.**
- Accession-only bucketing plus a chain-shape refusal gate is refuted by an executed
  counterexample (§3.3); a tolerance on the value match remains refused (#2785).
- A provenance-gated upper bound needs a positive floor on the same set (§4).

The model-level option — Section 16 is overlapping by design under Rule 16a-1(a)(2), which is
#1659's criterion for demoting a source out of an additive aggregate — remains open but
changes what the ownership card MEANS and belongs to #2215. ⚠ It is not the ONLY option left:
parser attribution (both reporting owners on a joint Table I), balance sequencing (#3146) and
relationship extraction are all still unexplored, and §1 bounded coverage rather than
existence.
