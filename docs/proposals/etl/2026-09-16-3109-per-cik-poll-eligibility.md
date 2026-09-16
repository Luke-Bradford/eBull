# #3109 — the per-CIK poll queue is inverted, the loss is not locally measurable, and the fix is not one line

Status: **slice 0 DONE (§3a); slices 1-3 are plan, no implementation.** Revised after Codex
checkpoint 1 (33 findings), which **withdrew this spec's own headline measurement** and
falsified the one-line fix it originally proposed. §3a then ran slice 0's existence proof and
two further Codex passes (40 then 26 findings) reversed ITS headline too: the answer is the
**absence** of a verified missing filing, because `sec_filing_manifest` is not the record of
what we discovered — 3,415 of 3,420 apparent absences are evidenced elsewhere. Scope: slice 0
tests DISCOVERY loss only. Target of the eventual slices: `app/services/data_freshness.py`
(`subjects_due_for_poll`), `app/jobs/sec_per_cik_poll.py`. All figures read-only against the dev
DB at `548d6bdf`, 2026-09-16 ~16:20-16:40Z.

## 1. The premise re-run, including the check that could have killed it

#3109 is a handoff with a root cause already written, so its discriminator was re-run on the
full population before any code (working-order 3c), not inherited.

**It survived a falsification attempt.** `subjects_due_for_poll` orders
`expected_next_at ASC NULLS FIRST` over `state IN ('unknown','current','expected_filing_overdue')`.
If NULL-dated rows filled the `LIMIT`, the ticket's 1994-watermark story would be a red herring
and the real cause would be seeding. Measured — of the **27,182** rows in the eligible set,
**0 have a NULL `expected_next_at`**:

```sql
select (expected_next_at is null), count(*) from data_freshness_index
 where state in ('unknown','current','expected_filing_overdue')
   and (expected_next_at is null or expected_next_at <= now()) group by 1;
-- (false, 27182)   -- no true row
```

The NULLS-FIRST branch is dead on this corpus and the ticket's mechanism stands.

**The mechanism, confirmed structurally.** `record_poll_outcome` (`data_freshness.py:417-421`)
recomputes `expected_next_at = predict_next_at(source, last_known_filed_at)` whenever a filing
date exists. The `now()`-anchored fallback — added by a prior Codex review for exactly this
failure — fires **only** when `last_known_filed_at IS NULL`. A successful no-change poll of a
triple whose last filing was 1994 rewrites `expected_next_at` to 1994 again, and the
ascending-order prefix returns it again next hour.

⚠ `expected_next_at` is doing this correctly *as a prediction* — the docstring says it is
"recomputed from `last_known_filed_at` and the source cadence", i.e. a forecast of WHEN THE
FILER WILL FILE. The defect is that the same column is also **permission to spend a network
request**, and those are different questions.

## 2. The queue is not merely behind. It is INVERTED.

`sec_per_cik_poll` has run **1,947 times** since 2026-06-03 — **1,932 `success`, 14 `skipped`,
1 `failure`** (reconciled; Codex 6 was right that the first draft left 14 unexplained).
Budget is `poll_budget = max_subjects * 2 // 3 = 66` triples per fire
(`sec_per_cik_poll.py:343`), with the other 34 going to the recheck lane.

Against ~1,947 hourly fires, the whole table carries only **95 rows that have ever been
polled** — 66 stamped by the latest run, 26 from 2026-06-15, 3 from 2026-06-05.

⚠ **What that does and does not establish** (Codex 7). `last_polled_at` is overwritten, not
appended, so it is a last-seen stamp and not execution history. What is directly observed: **no
row anywhere carries a stamp from the 15:00 run, the 14:00 run, or any hour between June and
now.** Had the prefix moved, the rows it moved off would still hold their own timestamps. The
inference — the same cohort is re-polled hourly — is therefore strong but is an inference; a
per-run record of *which* triples were polled does not exist, which is itself worth fixing
(§5, slice 3).

Running the production SQL read-only at `LIMIT 100` returns rows spanning **1994-07-28 →
2006-08-18**, of which **66 carry a `last_polled_at` from the run an hour earlier** and 34 have
never been polled.

**And the two groups are the wrong way round:**

| group | rows | `last_known_filed_at` range |
| --- | --- | --- |
| polled hourly | **66** | **1994-03-30 → 2004-11-08** |
| overdue, never polled | **21,311** | **2004-02-13 → 2026-09-02** |

Of the never-polled, **12,969 have a 2026 watermark** and 2,991 a 2025 one. Per-source the
starved set is dominated by the operator-visible issuer lanes: `sec_form3` 4,493 · `sec_10k`
3,806 · `sec_8k` 3,215 · `sec_form4` 2,156 · `sec_424b` 2,121.

⚠ **These are `(subject_type, subject_id, source)` TRIPLES, not filers** (Codex 8). An ancient
Form 3 watermark does not make that CIK defunct — it may file actively in another source whose
own triple is elsewhere in the queue. The inversion claim is about which triples get the
requests, and that is what the table measures.

⚠ The 66 figure is the *ordinary* lane only; the 34 recheck slots are separate and their actual
use is unmeasured (Codex 9). "100% of the budget" would be wrong.

## 3. ⛔⛔ The loss is NOT measurable from our own data — by construction

**This section previously reported a measurement. It has been withdrawn, because Codex
checkpoint 1 showed the number could not have come out any other way.**

The withdrawn claim: a full-population join of all 21,311 never-polled rows against
`sec_filing_manifest` on `(subject_type, subject_id, source)` returned **0** rows whose newest
manifest `filed_at` exceeded the freshness watermark, which this spec read as "the other
discovery layers have found nothing we do not already know, therefore no loss today".

**Why it proves nothing.** `sec_manifest.py:350-366` — *"#956: every manifest discovery write
also seeds / updates the scheduler row for its `(subject_type, subject_id, source)` triple"* —
calls `seed_freshness_for_manifest_row` **inline on every manifest write**. The watermark is
therefore advanced BY the manifest writer. `max(manifest.filed_at) > watermark` is structurally
impossible for any triple, starved or not. The query was a tautology wearing a measurement's
clothes, and "zero" was its only possible answer.

⚠ It would have been published as *"measured: the starvation is not losing filings"*. It is a
worked example of the prevention-log rule that a full-population scan does not validate a
population definition — here, of the two sides not being independent.

**What is actually true, and it is a sharper finding than the withdrawn number:** whether the
starvation is losing filings **cannot be answered from local data at all**. Both of our records
are written by the same discovery layers, so they agree by construction. The only way to detect
that atom / daily-index have missed something is to ask SEC — which is precisely the work the
per-CIK poll exists to do and currently does not do for 21,311 triples. **The reconciliation
layer's own blindness is what makes its blindness unmeasurable.** That circularity is the
finding.

Two consequences for how this ticket is scoped:

- Nothing here supports "21,326 overdue = 21,326 missed filings", and nothing here supports
  "no loss" either. Both are unevidenced.
- ⚠ Proving loss does NOT require checking all 21,311 (Codex 10): **one** independently verified
  missing accession is sufficient to prove it. That is a bounded, cheap experiment and it is
  slice 0 below — an existence proof, explicitly not a rate estimate.

Two residual measurement gaps, recorded rather than glossed: **520 of the 21,311 (2.4%) have no
manifest row at all** for their triple and are unexplained (retention, identity mismatch, or
freshness advancing without manifest evidence — each implies something different); and the join
says nothing about holes *below* a watermark, since a maximum is not completeness (Codex 3).

## 3a. Slice 0 — the existence proof RAN, and the answer is the ABSENCE of one

Slice 0 asked for *"one verified missing accession, **or the absence of one**"*. It is the
second. Run 2026-09-16, read-only, **zero SEC requests**.

**Result: no filing was verified as lost.** Every candidate resolved to a different ingest
route, a different subject, or a coverage boundary. Five accessions remain unaccounted for and
are named below as the residual lead — not as a proven loss.

⚠ This section replaces a first draft that reported the opposite. Codex checkpoint 1 (40
findings) falsified its headline, and the instrument corrections that did so are more useful
than the number was. They are recorded in full, because each one is a trap the next measurement
would fall into.

### The instrument: SEC's own bulk archive, not the API

`submissions.zip` (`resolve_data_dir()/sec/bulk`, 1.564 GB, 990,360 members) is SEC's nightly
publication of every CIK's filing list, so it is not written by our discovery layers — which is
what §3 says local data cannot be. Zero requests against the 10 req/s budget.

⚠ Freshness verified from **content**, not mtime: #3112 is open because the bulk refresh can
accept a same-size stale ZIP and write the new ETag beside the old bytes. `MICROSOFT CORP`'s
member carries a filing dated **2026-09-15**. ⚠ That certifies one member, not the archive
(Codex 13), and independence is partial — bootstrap ingests this same ZIP, so an omission
present in SEC's own publication would escape both sides (Codex 12).

### Correction 1 ⛔⛔ — the post-watermark discriminator is a SECOND tautology

The first form of the experiment asked: *does SEC list anything NEWER than the triple's
watermark?* Over 120 starved triples it returned **0**.

`seed_freshness_for_manifest_row` sets the watermark from the newest MANIFEST row. A filing we
never discovered never advances it, and a later filing discovered by any layer jumps the
watermark **past** the hole. A miss is therefore always BEHIND the watermark, so the question
can only be true for a triple whose single most recent filing was missed and which has filed
nothing since.

§3 gestured at this (*"a maximum is not completeness"*). Worth naming as a rule, because it
caught two different queries on one ticket: **the watermark cannot be a boundary in any test of
the thing that writes it.** ⚠ Zero still constrains the sampled post-watermark population; it
is not evidence of completeness, and the sample was biased toward recent discovery success
(Codex 3).

### Correction 2 ⛔⛔ — `sec_filing_manifest` is NOT the completeness oracle. This is what reversed the result

With the watermark dropped and the discriminator changed to *"absent from
`sec_filing_manifest` entirely"*, the scan produced **3,420 absent accessions** on the
issuer-subject arm, and one of them survived every alternative explanation: Form 4
`0001339727-25-000005`, `FLR.US` (instrument 1554, tradable), filed 2025-12-29, with nine
present Form 4 neighbours before it and twenty after. It was written up as the existence proof.

**It is not missing.** Sweeping all 155 tables carrying an accession column:

```
rows for 0001339727-25-000005:
  filing_documents 4 | filing_raw_documents 1 | insider_filers 1
  insider_filings 1  | insider_transactions 1 | insider_transaction_footnotes 1
```

We hold the filing, its raw body, and its parsed transactions. It has no manifest row because
the **legacy insider path** (`app/services/insider_transactions.py`,
`insider_form3_ingest.py`) writes the typed tables directly without one. The manifest is one
ingest route's bookkeeping, not the record of what we have.

Re-testing every absence against the typed tables:

| source | manifest-absent | in `insider_filings` | in `filing_raw_documents` | in `filing_documents` | **absent everywhere** |
| --- | ---: | ---: | ---: | ---: | ---: |
| sec_8k | 1,232 | 0 | 0 | 1,232 | **0** |
| sec_form4 | 1,814 | 1,814 | 1,814 | 1,814 | **0** |
| sec_10k | 122 | 0 | 0 | 122 | **0** |
| sec_form3 | 247 | 0 | 0 | 247 | **0** |
| sec_424b | 5 | 0 | 0 | 0 | **5** |
| **TOTAL** | **3,420** | 1,814 | 1,814 | 3,415 | **5** |

**3,415 of 3,420 (99.85%) are present in `filing_documents`.** The apparent loss was an artefact
of asking the wrong table.

⚠ This also voids the first draft's entire narrative — it classified manifest absences into
"un-backfilled history" versus "interior holes" and read shape as cause. Both the classes and
the reading are withdrawn: they measured manifest membership, which is not what we hold. Codex
24/30 were independently right that shape cannot establish backfill history without
universe-entry evidence, which was never gathered.

### Correction 3 ⚠⚠ — the comparison needs per-subject-type eligibility, not one rule

`CIK 0000029915` (`DOW CHEMICAL CO /DE/`) is a starved triple with
`subject_type='institutional_filer'`, `instrument_id IS NULL`. SEC lists 12 of its 8-Ks since
2025-01-01; we hold 11; `0001193125-25-165440` is absent with both neighbours present — a
textbook interior hole. But the neighbours' manifest rows are all stored under **`0001751788`
(DOW INC.)**, `subject_type='issuer'`, `instrument_id=9060`, and the absent accession **does not
appear in Dow Inc's own SEC filing list at all**. It is a Dow-Chemical-only 8-K, and its absence
is correct.

⚠ Stated precisely, per Codex 8: filer-subject triples are **not** categorically invalid — the
repo legitimately expects `institutional_filer` 13F and `blockholder_filer` 13D/G manifests.
What is invalid is applying the ISSUER rule to them. A valid filer-arm comparison needs the
filing's own party list and that source's role/universe eligibility; the bulk archive does not
carry per-accession filer lists, so it cannot be built from this instrument alone.

⚠ The first draft claimed "255 of 259 apparent interior misses sat on filer-shaped subjects".
**Wrong, and it is arithmetic rather than judgement** (Codex 10): 255 was the Form 4 count in
the unrestricted arm. Restricting to issuer subjects leaves 194, so the filer-shaped share is
**65**, not 255.

### ⛔ Every rate this section originally reported is WITHDRAWN, for a fourth reason

The scan read only `filings.recent` from each member. The archive also contains **5,373
overflow members** (`CIK..........-submissions-NNN.json`), and for a high-volume filer the
recent page does not reach back to the window start — JPMorgan's begins **2025-09-16**. So the
SEC-side set was **truncated for exactly the busiest filers** (Codex 14).

Truncation cannot manufacture a false absence (an absence is only claimed for an accession
actually seen in SEC's list), so the **5** below stand. It does destroy every denominator and
therefore every percentage. Withdrawn accordingly: the 1.976% absence rate, the 94.2%
prefix-gap share, the 0.112% interior-gap figure, and the "~25× smaller" and "~17× larger"
comparisons. **Any rerun that wants a rate MUST follow `filings.files[]` pagination.**

Further limits, recorded rather than glossed (Codex 15-23, 26-29, 39): the population omits the
selector's `state IN (...)` filter and NULL predictions, so it is not the production queue;
2,397 triples with no in-window SEC filings were not reconciled against missing archive members;
six sources were scanned, not every issuer lane (10-Q and Form 5 are absent); the `today-3d`
guard is an unvalidated allowance, calendar-date based and timezone-dependent, not a measured
dissemination latency; a present neighbour dates a FILING, not a discovery, so it cannot prove
a layer was running; and present rows may come from bootstrap, rebuild or backfill, so none of
this measures Atom or daily-index performance. ⚠ The five leads' own windows were observed on
the recent page only, so "the only 424B in the window" means "the only one on the page
inspected" (Codex 14).

### The five residual leads — unaccounted for, NOT proven lost

Absent from `sec_filing_manifest` **and** `filing_documents`, all for tradable instruments, all
in sub-forms `docs/etl/sources/sec_424b.md` documents as tier-1 PARSE+RAW (424B1/B3/B4/B5/B7,
distinct from the volume-gated 424B2):

| instrument | CIK | form | filed | accession |
| --- | --- | --- | --- | --- |
| `TBBK` (2446) | 0001295401 | 424B1 | 2025-08-15 | `0001104659-25-079281` |
| `OMEX` (1048806) | 0000798528 | 424B1 | 2025-02-07 | `0001193125-25-022760` |
| `CBAT` (1049358) | 0002086841 | 424B3 | 2026-01-16 | `0001213900-26-005125` |
| `PACK` (10508) | 0001712463 | 424B1 | 2025-11-19 | `0000950103-25-015015` |
| `HTCR` (1051861) | 0001892322 | 424B1 | 2025-09-16 | `0001493152-25-013662` |

**No documented exclusion covers them.** All five sub-forms are mapped in `_FORM_TO_SOURCE`;
the volume cap applies to 424B2 only; and a parse gate leaves a manifest TOMBSTONE rather than
no row, so gating cannot explain a missing manifest row.

⚠ **What is genuinely unverified is historical coverage, and it is the leading alternative.**
`docs/etl/sources/sec_424b.md` records 424B as not bootstrap-covered, with historical backfill
separately driven (`scripts/backfill_1974_sec_424b.py`) off `filing_events` and resolvable CIKs.
Present tradability does not establish historical universe membership, CIK mapping, deployment
date, or a completed backfill for these instruments. Possible exemptions, not verified ones —
which is exactly why these five are leads and not findings.

⚠ Neighbour shape is deliberately NOT used to argue about them. A neighbour dates a filing, not
a discovery, and its absence does not weaken a lead; what these five lack is verification, not a
geometric pattern.

### What slice 0 settles, and what it does not

- **No verified loss.** The best candidate out of the whole scan was refuted by the oracle
  correction, and the five leads lack the shape that would prove a miss. Slices 1-3 should
  therefore be scoped as **latency and prevention** work, not as loss recovery — the queue being
  inverted is established by §2 and does not need a loss to justify a fix, but it must not be
  sold as one. ⚠ A scoping recommendation, not a proof that recovery is pointless — the
  useful-ingestion gaps behind the 1,601 directory-only rows are untested.
- ⚠ **This is not "no loss".** Absence of a verified miss over a truncated, six-source,
  issuer-only scan is weak evidence of completeness, and §3's circularity argument still holds
  for everything the bulk archive cannot see.
- ⚠ **Do not assume slice 1 recovers history — read the cursor first.**
  `sec_submissions.py:315` stops the poll when its known accession appears in the source-filtered
  recent response; with a NULL or absent known accession it returns the whole filtered page, so
  older holes on that page CAN be recovered. It does not follow overflow pages. Whether a timely
  earlier poll would have caught any specific filing is a counterfactual this slice supports
  neither way.
- ⚠ **No causal claim is made, and no failure is established.** The five leads are unexplained,
  not proven lost; all five predate the run history in §2. Present starvation alongside present
  absence identifies nothing on its own.

## 4. Why the one-line reorder is not the fix

The original proposal was to copy `expected_filings_poller.py:308,324-326` — eligibility on
`last_polled_at IS NULL OR last_polled_at < now() - interval`, ordering
`last_polled_at ASC NULLS FIRST, id ASC`. It remains the right *precedent* (reuse before
reinvent; it is the in-repo pattern for draining a bounded queue without re-picking its head)
but it is not sufficient here:

1. **Pure oldest-poll ordering starves the newly due** (Codex 11). A triple that files weekly
   and was polled yesterday sorts behind all 21,311 never-polled rows — i.e. behind ~17 days of
   backlog. Trading a stuck queue for a fair one that ignores filing deadlines is not a fix for
   an ingest freshness layer; the ordering has to carry urgency as well as fairness.
2. **Continuous NULL arrivals can prevent the queue ever rotating** (Codex 12): `NULLS FIRST`
   never drains while new eligible triples keep arriving.
3. **The cooldown cannot come from `_CADENCE`** (Codex 17/18). Those values reach **365-400
   days** and mix prediction, reconciliation and UI-suppression purposes. A cadence-derived
   cooldown would also skip a genuinely due poll: poll today, discover a filing from 119 days
   ago under a 120-day cadence, prediction falls due tomorrow, cooldown blocks it for 120 days.
   The sibling deliberately uses a **separate** `poll_interval_minutes` column
   (`sql/207_expected_filings.sql:36`, default 30 min). A reconciliation-latency target is a
   decision to make explicitly, not a constant to borrow.
4. **Batch-level commit can roll the stamps back** (Codex 29). The scheduler commits after the
   whole batch, and `NOW()` is transaction-start time; an uncaught later failure discards the
   stamps and the prefix repeats. The sibling commits per subject — a material difference that
   the ordering change silently depends on.
5. **`expected_next_at IS NULL` must stay in the candidacy filter** (Codex 14). It is 0 rows
   today, but full-wash and rebuild produce NULL predictions and today's census is not an
   invariant.
6. **Reset semantics need defining** (Codex 15). `sec_rebuild` and the operator full-wash reset
   eligibility while RETAINING `last_polled_at`, so a blanket cooldown would delay the immediate
   drain they intend, and oldest-poll ordering would demote them.

**Measured, and it does NOT apply here:** Codex 16 flagged that no-CIK rows are selected into
the budget then `continue`d without being stamped (`sec_per_cik_poll.py:358`), which under
oldest-poll ordering would let them re-occupy the head forever and make the whole fix inert.
Checked: `cik IS NULL` on **0 of 55,770** rows. The skip is dead code today. It stays a latent
hazard the moment a FINRA-universe singleton appears, so a slice-2 guard is cheap insurance,
not speculation.

## 5. Slices

**Slice 0 — ✅ DONE, §3a. The answer is the ABSENCE of a verified missing filing**, with five
424B accessions left as unaccounted-for leads rather than proven losses. ⚠ Four corrections to
this paragraph's own plan, all in §3a: the SEC **API** was not needed (the nightly bulk
`submissions.zip` costs zero requests); comparing **against our watermark** is a tautology,
because the watermark is seeded from the newest manifest row, so a miss is always behind it;
**`sec_filing_manifest` is not the record of what we discovered** — a parallel path writes the
typed tables with no manifest row, and 3,415 of 3,420 apparent absences turn out to be evidenced
in `filing_documents`. ⚠ That table is SEC's DIRECTORY LISTING, so it evidences discovery only:
useful ingestion is untested for the 1,601 candidates it alone covers; and the ISSUER rule cannot be applied to filer-subject triples. ⛔ All
rates are WITHDRAWN: the scan read only `filings.recent` and the archive has 5,373 overflow
members, so the denominator was truncated for the busiest filers. **Consequence for slices 1-3:
scope them as latency/prevention work, not loss recovery.**

**Slice 1 — the selector.** Candidacy stays `expected_next_at IS NULL OR expected_next_at <=
now()`; ordering becomes a composite of poll-staleness and filing-urgency rather than either
alone, with a deterministic tie-break on `(subject_type, subject_id, source)`. The composite is
the actual design work and needs its own source rule — the sibling's pattern covers fairness
but not the urgency half. ⚠ Check the query plan: the existing index leads with
`expected_next_at` (Codex 31).

**Slice 2 — the re-poll interval**, from an explicit reconciliation-latency target, not from
`_CADENCE`, with the reset/bypass semantics for `unknown` + full-wash written down. Plus the
no-CIK guard: exclude unprobeable rows BEFORE the budget rather than skipping them after.

**Slice 3 — per-run poll coverage.** `last_polled_at` being a last-seen stamp is why §2's
central claim is an inference. A per-run record of which triples were polled would have made it
a measurement, and is also what the ticket's rollout verification needs. ⚠ `count(last_polled_at)`
is NOT a rollout metric (Codex 32): it includes failures and rechecks, saturates, and counts
triples rather than CIKs.

## 6. Out of scope, recorded so the next session does not re-derive it

- **`subjects_due_for_recheck`** — same shape, different column. An errored subject's
  `next_recheck_at` is left NULL, i.e. immediately due, so a failing cohort can occupy the
  recheck budget the same way (recorded during #3111). ⚠ It also **limits slice 1's guarantee**
  (Codex 24): a newly reached triple that fails once moves into the error queue and escapes the
  new ordering entirely. The guarantee must be stated as covering successful ordinary polls only.
- ⛔ **A 304 can falsely certify an unchecked source** (Codex 25). The HTTP validator is keyed by
  CIK, but each 200 parses only the selected source — so source A's poll can update the
  validator and source B then receives a 304 without its filings ever being processed. **Faster
  rotation exposes MORE of this, not less**, so it is a prerequisite risk for slice 1 rather
  than an unrelated bug. Not filed separately per the standing order; recorded here and on the
  ticket.
- Budget stays 66/hour. Raising it needs a service target and a capacity calculation against
  the 10 req/s SEC budget and the zero-headroom dev connection pool (Codex 23), not a number
  chosen here.
