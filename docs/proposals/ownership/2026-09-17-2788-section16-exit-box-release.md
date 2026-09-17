# #2788 — release an insider position on the filer's own declared Section 16 exit

Status: proposal (spec), revised after Codex checkpoint 1 (37 findings; the design moved
from row-level to holder-level because of one of them). Ticket: #2788.
Refs #2226 (M1), #2229 (the 13F half), #2230, #2794.

Every figure below is reproducible with
`PYTHONPATH=. uv run python -m scripts.audit_2788_section16_exit --census`, which prints
its own SQL. No figure in this document is hand-copied from a session transcript.

## 1. What #2788 asked first, and the answer

#2788's item 3 is explicit: *"Do not assume why a row is frozen. 'The holder ceased to be a
reporting person' is the obvious story and it is **unmeasured**. That is the first question
this ticket has to answer, not a premise it may inherit."*

Measured. For most frozen rows we still cannot say. For a minority we can, because **the
filer is required to say so on the form** — and 3,992 (instrument, holder) pairs sit on a
filing where they did.

## 2. Source rule

**SEC Form 4, General Instruction 1(b)** (`https://www.sec.gov/files/form4.pdf`, page 7 of
the text layer), verbatim:

> (b) A reporting person no longer subject to Section 16 of the Securities Exchange Act
> of 1934 ("Exchange Act") **must check the exit box** appearing on this Form. However,
> Form 4 and 5 obligations may continue to be applicable. See Rule 16a-3(f); see also
> Rule 16a-2(b) (transactions after termination of insider status).

The box on the face of the form:

> ☐ Check this box if no longer subject to Section 16. Form 4 or Form 5 obligations may
> continue. See Instruction 1(b).

**Form 5** carries the same box and instruction (`https://www.sec.gov/files/form5.pdf`,
General Instruction 1(b)). **Form 3 does not**: a text search of
`https://www.sec.gov/files/form3.pdf` for "no longer subject" returns **0** hits — Form 3
is the initial statement and cannot carry an exit. ⚠ That is evidence about the form, not
about our corpus; the corpus side is §3.2, where **16,744** `source = 'form3'` `_current`
rows DO resolve to an `insider_filings` row and **0** of them carry a TRUE flag.

Four things follow, and each is load-bearing.

1. **It is affirmative, not silence.** `ownership_rollup.py:1293` records exactly why the
   insider half of M1 was never built: *"**Section 16 has no such rule** [as 13F SI 5b].
   Form 4 is transaction-triggered … and Rule 16a-2's obligation simply ENDS when the
   person ceases to be an insider, which is why a frozen row stops moving rather than going
   to zero."* That is right **about silence**, and this proposal does not contradict it. The
   exit box is a statement *inside a filing* — the same class of evidence as a 13F-NT.
2. **It says STATUS, not disposal.** The declaration is *termination of insider status*
   (Rule 16a-2(b) is titled exactly that). It is **not** a claim the holder sold; the shares
   may still sit in the same hands. What ends is their membership of the population the
   insiders channel reports. The `detail` string must say this and must never be quotable
   as an exit from the stock.
3. **What removal MEANS here.** The rollup is a Rule 13d-3 beneficial-ownership snapshot
   (`ownership_rollup.py:1`) whose unattributed remainder is the `Public / unattributed`
   residual. Removing a row moves those shares to that residual — it does not assert they
   were sold. That is the identical posture the already-shipped
   `insider_beyond_form4_retention` takes (#2788/#2792): *"we no longer hold in-retention
   evidence of the position, so we stop asserting it, per the #790 posture that the truthful
   state beats a fabricated one."* Here the evidence is stronger, because the filer told us.
4. ⚠ **One reason for an exit is NOT about the person: issuer deregistration.** §16 attaches
   to a class registered under §12. If the issuer's registration terminates, every reporting
   person becomes "no longer subject to Section 16" **while remaining a director or
   officer** — so the box would be right and a release would be wrong. §4 carves this out on
   the Form 25 register rather than arguing it away; §3.7 sizes it.

**XML carrier.** Document-level `<notSubjectToSection16>`, a sibling of `<documentType>` /
`<periodOfReport>` — verified by parsing accession `0000899140-24-000444` (`form4.xml`) and
printing the root's children in order, not from the schema doc. Already parsed
(`insider_transactions.py:466`, `:570`; `insider_form3_ingest.py:144`) and stored as
`insider_filings.not_subject_to_section_16` (`sql/057:76`), whose column comment names the
mechanism: *"Rare edge case: Section 16 doesn't apply (e.g. former insider still in the
reporting window)."*

**Nothing reads it.** `rg -n 'not_subject_to_section_16' app/` returns writers only.

## 3. Full-population verification (dev DB, `main` at `5ba41ec7`, 2026-09-17)

### 3.1 The flag

| `insider_filings` | rows |
| --- | ---: |
| total | 579,121 |
| flag TRUE | **3,270** |
| flag FALSE | 265,071 |
| flag NULL | 310,780 |
| tombstoned | 28,172 |

### 3.2 The coverage ceiling, stated before the result

`ownership_insiders_current`, `source = 'form4'` (150,875 rows), joined on its own
`source_accession`:

| | rows | share |
| --- | ---: | ---: |
| no `insider_filings` row at all | **76,721** | 50.9% |
| filing present, flag NULL | 30,630 | 20.3% |
| flag FALSE | 39,389 | 26.1% |
| **flag TRUE** | **4,135** | **2.7%** |

⚠ **Readable status is 43,524 / 150,875 = 28.8%. 71.2% of the population cannot be
consulted at all** (Codex ckpt-1 corrected an earlier draft that quoted the 49.1% *join*
rate as if it were the readable rate). Among the readable 28.8%, the exit rate is 9.5% —
do **not** extrapolate that onto the rest; the readable set is the XML-manifest cohort and
skews recent.

"No `insider_filings` row" is attributed, not assumed: **76,721 of 76,721** of those rows
carry a `:NDT:`/`:NDH:` `source_document_id` marker, i.e. every one is a DERA-bulk row, and
that path writes observations without a filing row. §8 names the follow-up.

### 3.3 The tip rule and its size

The rule is **holder-level**, not row-level (§4 explains why). For each
`(instrument_id, holder_identity_key)` take the rows at that holder's MAX `period_end` —
their *tip* — and release the holder only if **every** tip row resolves to a filing whose
exit box is TRUE.

| | |
| --- | ---: |
| holders whose whole tip carries the box | **3,992** |
| holders whose tip DISAGREES (fail-closed, not released) | **6** |
| `_current` rows released | **4,869** |
| shares carried by them | **27,007,679,629.4919** |
| distinct instruments | **1,378** |

⚠ These are the rows the predicate selects. They are **not** a treatment effect: the
released rows still pass through retention exclusion, dual-pipeline de-collision, the
owner-once and control-group folds, and slice routing before anything renders. §6 measures
the effect; this table only sizes the input.

### 3.4 Why row-level was wrong (Codex ckpt-1, and it changed the design)

Under the first draft — release the row whose OWN accession carries the box — **992 of
4,135** released rows left a sibling `_current` row for the same holder at a different
`(ownership_nature, source)`, and **233** of those siblings are `form3`. Form 3 is the
INITIAL statement, so the row that survived would frequently be an even older balance than
the one removed. The declaration is about the **person**, so the release has to be too.

Moving to holder-level also closes four other ckpt-1 findings by construction rather than
by predicate: self-match, later-filing chronology, repeated exits, and the Form 3 fallback.

### 3.5 Subsequent filing activity — what it does and does not measure

For each (flagged accession, reporting owner), does that owner file a later §16 form for
the same issuer?

| | owner-rows | files again later |
| --- | ---: | ---: |
| single-owner filings (2,656 accessions) | 2,656 | 493 (18.56%) |
| joint filings (614 accessions) | 2,737 | 311 (11.36%) |

⚠ **This does not measure whether an exit "sticks"**, and an earlier draft said it did.
Instruction 1(b) itself says *"Form 4 and 5 obligations may continue to be applicable"* —
Rule 16a-2(b) keeps a departed insider reporting opposite-way transactions for six months.
A later filing is therefore an expected consequence of a real exit, not evidence against
one. What the table IS used for: it shows post-exit filing is common enough that the tip
rule must be anchored on the holder's latest observation rather than on any one accession.

### 3.6 The joint-filing hazard, and why it did not bite

`<notSubjectToSection16>` is **document-level**, and **2,053 of 4,135** row-level candidates
sat on a **joint** filing — up to **10** reporting owners on one accession
(`0000899140-24-000444`: ten Insight Partners GP/LP entities, every one
`isTenPercentOwner`, one box between them). One box describing ten owners is the obvious
way this design dies.

It survives for a structural reason, not a statistical one: **the tip rule never takes a
co-filer's status from someone else's row.** A holder is released only if that holder's own
latest `_current` row resolves to a flagged filing — a co-filer who kept reporting has a
later tip and is not released; a co-filer whose tip is a DERA row is unreadable and is not
released.

The filing-rate split is reported because it was measured, and it points the same way
(joint co-filers re-file *less*: 11.36% vs 18.56%, consistent with control chains crossing
10% together). ⚠ It is **not** offered as proof that every co-filer exited — that is a claim
about intent which no measurement here reaches, and the design does not rest on it.

### 3.7 Residual fail-open surface, after the tip rule

| | |
| --- | ---: |
| released holders | 3,992 |
| `holder_cik` NULL (guard unevaluable) | **0** |
| a later `insider_filings` row exists beyond the tip | **36** (0.90%) |
| flagged instrument/issuer pairs whose issuer also has a Form 25 delisting | **63 of 1,401** (4.5%) |

Both get an explicit predicate in §4. The 36 are filings that produced no observation at
the tip (a parse gap); the 63 are §2 item 4's deregistration case.

⚠ `period_of_report` is NULL on 28,173 `insider_filings` rows, which would make a `>`
comparison fail open. Measured: **0** of them are live — every NULL is a tombstone, and the
predicate already excludes tombstones. Stated because a future non-tombstone NULL would
reopen it.

## 4. Design

One new read-time correction kind. Same shape as `_INSIDER_BEYOND_RETENTION_SQL`: **one**
SQL fragment interpolated into both the reader's exclusion and the complement producer, so
the pair cannot drift (#2229 kept its pair in step by comment alone, which is the hazard
that constant exists to remove).

Predicate `_INSIDER_SECTION16_EXIT_SQL`, over `oc` = `ownership_insiders_current`:

1. `oc.holder_cik IS NOT NULL` — an unidentified holder cannot be guarded, so it is never
   released. Explicit, because a NULL CIK silently satisfies a bare `NOT EXISTS` and would
   be released on an unevaluable guard. Measured 0 today; pinned by a test.
2. **Positive arm** — at least one row at this holder's tip resolves to a live filing with
   `not_subject_to_section_16 IS TRUE`, matched on `accession_number` **and**
   `instrument_id`, and requiring the holder to be among that filing's reporting owners
   (`insider_filers`). Without the positive arm the rule is satisfiable vacuously.
3. **Unanimity arm** — no row at the holder's tip fails to carry the box. `IS NOT TRUE`
   covers NULL and FALSE alike: a NULL is "we do not know", never an exit. This is what
   makes an all-NULL tip fail closed.
4. **No-later-filing arm** — no live `insider_filings` row for this `(instrument, filer_cik)`
   with `period_of_report >` the tip period. Closes §3.7's 36.
5. **Deregistration carve-out** — refuse the release when the tip filing's `issuer_cik`
   appears in `sec_form25_common_equity_delistings`. §2 item 4: a §12 deregistration ends
   §16 for everyone regardless of whether anyone's relationship to the issuer changed, so
   the box there is not evidence about the person. Fail-closed and date-free on purpose —
   a date window would be a fitted constant with no source rule (#2231).

`IS TRUE` throughout, never `IS NOT FALSE`: 310,780 filings are NULL.

**Pairing obligations for the complement producer** (it must reproduce the reader's *whole*
selection, not just this fragment): `oc.shares IS NOT NULL`, the instrument filter,
`_INSIDER_DUAL_PIPELINE_DECOLLISION_SQL` (or `shares_removed` double-counts an accession
present in both pipelines — the ckpt-2 finding recorded at `ownership_rollup.py:893`), and
survival of `_INSIDER_BEYOND_RETENTION_SQL`. A row matching both retention and exit is
reported **once, under retention** — it is already shipped and makes the weaker claim about
the same row, mirroring the NT-wins-over-HR precedent.

New kind: **`insider_section16_exit_declared`**. `superseded_period` = the removed row's own
`period_end`. `source_channel` = `"form4"`, `winning_source` = `"form4"`, NT fields `None`.
`winning_accession` = the tip accession carrying the box — ⚠ for this kind that CAN be the
removed row's own accession, unlike `superseded_by_later_13f_hr` where pointing at the
removed row was a defect; the docstring must say so or a future reader will "fix" it.

`detail`, fixed by §2 items 2 and 3 — it must not read as a disposal:

> Reporting person checked the Form 4/5 exit box on accession `{acc}` (period `{p}`),
> declaring they are no longer subject to Section 16 (Form 4 General Instruction 1(b)).
> A change of STATUS, not a sale: these shares are no longer insider ownership and move to
> the unattributed residual.

### 4.1 The closed-vocabulary contract (non-negotiable)

Prevention log, "A closed vocabulary declared in three places is validated in none of them"
(#2229): adding a kind to the service alone returned **500** on every rollup request that
fired it. Required in the same diff:

1. `app/api/instruments.py::_CorrectionAppliedModel.kind` Literal (`:4787`);
2. `frontend/src/api/ownership.ts::OwnershipCorrectionKind` union (`:313`);
3. `tests/test_correction_kind_vocab_contract.py` must pass unedited.

⚠ The FE half of that test is a **regex** (`re.findall(r'"([a-z0-9_]+)"', …)` over the union
body, `test_correction_kind_vocab_contract.py:80-82`), not an AST parse — so a quoted
lowercase literal in a comment inside the union body would be read as a member. Do not add
one.

## 5. Tests

Pure-logic where possible, one DB-backed test for the SQL mechanism (repo default):

- NULL `holder_cik` is not released even when the tip carries the box.
- All-NULL tip → not released (the vacuity arm).
- Mixed tip (one flagged, one not) → not released; this is §3.3's 6.
- Form 3 sibling at an earlier period is released **with** its holder, not left behind.
- A later filing beyond the tip blocks the release.
- A Form 25 issuer is not released.
- Reader exclusion and producer complement select the same rows (they interpolate the same
  fragment; the test asserts the *result sets* match, not the strings).
- A row matching both retention and exit is reported once, under retention.

## 6. Acceptance — the A/B, on #2788's own criterion

Harness: `scripts/audit_2230_insider_oversubscription.py` (renders the REAL read path inside
`snapshot_read`) + `scripts/ab_2230_compare.py`. Both exist; neither changes.

- **Control** — `origin/main` at `5ba41ec7`, full population, 4 shards. **Already run**:
  12,825 instruments, **0 harness errors**, 4,430 with a usable denominator, **398** whose
  insiders wedge exceeds `shares_outstanding`. (#2788's body says 664; that was measured
  2026-08-20, before #2792 landed. The control is re-measured, not inherited.)
- **Treatment** — same population, same sharding, same day, same DB.
- **Distinct-entity metric**, both sides: shrank / grew / holder-SET changed at an unchanged
  total (a swap is not a no-op — #2176).
- **The gain side is inspected.** ⚠ An earlier draft claimed the change "strictly removes
  shares; never adds". **Retracted** — #2794 measured exactly the opposite shape: removing an
  insider row can UN-fold an equal-value control group and make the wedge GROW on 7
  instruments. Any growth here must be listed and explained, not netted away.
- **Never simulate the control.**

Criterion, from #2788: *"would this, applied perfectly, bring the wedge back under
`shares_outstanding`"*. Report instruments over outstanding before → after, and the count
cleared.

⚠ A small number is a legitimate outcome and will be reported as one. The justification for
this mechanism is that it is **sound** — the alternative on the table is a 2-year staleness
bound which #2788's own body measured as stripping 26,534,264,979 shares against a
14,468,619,737-share overage (183%) with no source rule behind the window. "Clears fewer,
on evidence" beats "clears more, by fitting". ⚠ And the converse: a large number does not by
itself prove correctness — a release can clear an instrument by removing something real, so
the per-holder checks in §5 are what carry soundness, not the cleared count.

## 7. Refusals

- ⛔ **Do not widen to "no filing for N years"** (#2788 item 1; #2231's mistake).
- ⛔ **Do not read `IS NOT FALSE` as an exit.** 310,780 NULLs.
- ⛔ **Do not extrapolate the 9.5% readable-cohort exit rate onto the unreadable 71.2%.**
- ⛔ **Do not date-window the Form 25 carve-out** to recover the 63.
- ⚠ False negatives are the status quo: a filer who exits and never ticks the box keeps a
  frozen row, exactly as today. Instruction 1(b) says "must check", so that filer is
  delinquent — but ⚠ that is a statement about a filing they DID submit, not evidence that
  every departing insider submits one at all.

## 8. Named follow-up, with its size measured

The DERA bulk dataset **already carries the column** — `SUBMISSION.tsv` →
`NOT_SUBJECT_SEC16` (`docs/data-sources/sec-bulk-archives.md:257`) — and
`sec_insider_dataset_ingest.py:602` already holds that row per accession while writing
observations only. Carrying it would lift readable coverage from **28.8%** toward the
**76,721** rows (50.9%) that have no filing row at all. ⚠ It would NOT resolve the 30,630
rows whose filing row exists with a NULL flag; those need a re-parse, which is a different
job. Schema + backfill + its own A/B; deliberately not in this diff.
