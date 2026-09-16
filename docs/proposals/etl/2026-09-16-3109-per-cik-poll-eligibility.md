# #3109 — the per-CIK poll queue is inverted, the loss is not locally measurable, and the fix is not one line

Status: **research + slice plan, no implementation.** Revised after Codex checkpoint 1 (33
findings), which **withdrew this spec's own headline measurement** and falsified the one-line
fix it originally proposed. Target of the eventual slices: `app/services/data_freshness.py`
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

**Slice 0 — one verified missing accession, or the absence of one.** Take a bounded sample of
starved triples with recent watermarks, ask SEC's submissions API directly (read-only, inside
the 10 req/s budget), and compare against our watermark. **Existence proof, not a rate** — state
it as such and do not extrapolate a percentage from it. This is the only evidence that can size
the rest, and §3 is why nothing local can substitute.

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
