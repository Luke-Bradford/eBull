# Close the wrong-layer insider write (#2793)

Status: proposed, 2026-08-22. Phase-0 item 8 of the R5b queue.

## Scope, declared before anything else

This ticket has a **write-side** half and a **data** half, and only the first is in scope
here. The issue's own closing line: *"Do not purge them as a side effect of a code fix —
they may be the only copy of a corpus that took a 20-year download to build … Propose,
measure, then ask."* Disposing of the 4.19M beyond-cap rows is an irreversible-loss call and therefore
operator-gated. This change **measures** them and posts a recommendation; it moves,
archives and deletes nothing.

## Source rule

`docs/specs/etl/retention-rubric.md` §4.3 — Form 4 ingest depth cap is **3 years from
today, per CIK, at the parser**, justified by half-life: *"last 90d is the alert signal;
last 12mo is the thesis signal; 5y old = decoration"*. `form4_retention_cutoff()`
implements it; it read `2023-08-22` at the time of writing. §4.4 caps Form 5 at 18 months.
Form 3 is deliberately ungated (read-side latest-per-pair).

That rule governs `ownership_insiders_observations`, which is the sole source of
`ownership_insiders_current`, which is the insiders wedge on the operator's ownership
rollup.

## Full-population measurement (dev, 2026-08-22)

⚠ **A first draft of this section was wrong twice, both caught at Codex ckpt-1, and both
errors inflated or misdirected the disposal set. They are recorded because the corrected
numbers below are only trustworthy if the corrections are visible.**

| # | measurement | rows |
| --- | --- | --- |
| A | `source='form4'` beyond `form4_retention_cutoff()` (2023-08-22) — **the violation** | **4,189,940** |
| C | of A, `filed_at < 2023-06-05` — provenance unambiguous | 4,120,242 |
| D | of A, `filed_at ∈ [2023-06-05, cutoff)` — **ambiguous** | 69,698 |
| G | `source='form3'` older than the Form 4 cutoff — **NOT a violation** | 142,495 |
| F | rows stamped `ingested_at::date = 2026-08-14` | 5,118,515 |
| H | table total | 5,578,377 |

The violation is **75% of the table**, and the issue's premise is confirmed in substance.

### Correction 1 — Form 3 is ungated, so it is not part of the violation

`retention-rubric.md` §4.3 and the injection point's own comment both say Form 3 is
deliberately not capped (it is read-side latest-per-pair). The first draft called its
142,495 old rows "beyond-cap" and summed them into a 4,282,618 total. **They violate
nothing.** The violation is 4,189,940, and it is Form-4-shaped only.

### Correction 2 — `source='form4'` also carries Form 5, under a different cap

Per `sec-edgar.md`, Form 5 observations land as `source='form4'` because the enum has no
`form5`; provenance is meant to come from an `insider_filings.document_type='5'` join.
Form 5's cap is 18 months (`form5_retention_cutoff()` = 2025-02-22), not three years, so
splitting on `source` alone applies the wrong cutoff to some rows.

⚠⚠ **And the join that would fix it does not exist for these rows.** Measured:

```sql
-- observations whose accession has NO insider_filings row at all
select count(*) from ownership_insiders_observations o
where o.source='form4'
  and not exists (select 1 from insider_filings f where f.accession_number=o.source_accession);
-- 4,134,986
```

The DERA bulk path writes only `ownership_insiders_observations` — it never wrote
`insider_filings` — so **the form type of 4.13M rows is undeterminable from the database**.
A disposal keyed on "form type × its own cutoff" is therefore not executable on exactly the
rows in question. This is a harder constraint than ckpt-1 framed it, and it is the single
most important input to the disposal decision.

### Correction 3 — provenance is contaminated by `ON CONFLICT DO UPDATE`

`_INSERT_FROM_STG_SQL` ends `DO UPDATE SET … ingest_run_id = EXCLUDED.ingest_run_id,
ingested_at = clock_timestamp()`. So a row **that already existed** and was merely
re-touched by the run now carries the run's stamp. Consistent with that, 5,118,515 rows
carry `ingested_at::date = 2026-08-14` under **87 distinct `ingest_run_id`s** — far more
than the run inserted. **`ingested_at` and `ingest_run_id` are not clean provenance**, and
a purge keyed on either would delete rows that predate the run and lose their earlier
state.

### What IS defensible

The capped path could never have written a row filed before the cap allowed it. The
earliest `filed_at` any non-2026-08-14 write reached is **2023-06-05**, so:

- `filed_at < 2023-06-05` (**4,120,242** rows) can only have come from the override run —
  unambiguous by construction, needing no provenance column.
- `filed_at ∈ [2023-06-05, 2023-08-22)` (**69,698** rows) is a genuine mixture of rows that
  were legitimate when written and have since aged out, and override rows. No stored column
  separates them.

## The research goal was not met — confirmed

`insider_filings` holds 2,581 pre-2017 rows of 569,676 (0.45%), and this function's only
INSERT target is `ownership_insiders_observations` (`grep -n "INSERT INTO"` returns exactly
one hit, line 336). The tables #2701's consumer reads are not written here and never were.

## The fix: remove the parameter

```
grep -rn "retention_cutoff_override" app/ scripts/
  app/services/sec_insider_dataset_ingest.py   (definition + the injection comment)
  scripts/backfill_2701_insider_research_ingest.py:36   ← the ONLY caller that passes it
```

The production path (`sec_bulk_orchestrator_jobs.py:594`) does not pass it.

So the change is: **delete `retention_cutoff_override` from
`ingest_insider_dataset_archive`**, making `form4_retention_cutoff()` /
`form5_retention_cutoff()` unconditional at this writer, and **delete
`scripts/backfill_2701_insider_research_ingest.py`**, whose only purpose was to pass it.

This IS the issue's candidate 1 ("route research rows away from the operator layer"),
expressed minimally and honestly. The routing does not need building, because **the
research route does not exist at this function** — it writes one table and it is the wrong
one. A parameter that cannot reach the research consumer's tables is not a research
affordance; it is a widening of the operator layer with no compensating benefit, which is
exactly what the measurement shows it produced.

⚠ Why remove rather than gate. The parameter's own comment argues at length that injecting
it was safe because *"both consumers keep the boundary they need"*. That sentence is false
and the falseness is structural: there is one table, so there is one boundary. A gated or
warned version of the parameter preserves the shape that made the sentence believable.
Removing it makes the widening unrepresentable, which is the only version of the fix a
future reader cannot misread.

⚠ Why delete the script rather than leave it. It is a loaded gun: a single `uv run` re-runs
a multi-million-row wrong-layer write. It achieved the inverse of its stated intent, and insider
research is CUT by the operator (2026-08-22). It stays in git history, where a future
research route can read what it tried to do.

## What is NOT built here

**A real research route.** #2701's consumer needs `insider_filings` /
`insider_transactions`, and building a DERA-to-those-tables path is a separate piece of
work — one that is currently out of scope anyway, since the operator CUT the insider family
on 2026-08-22 with recorded lessons. Removing a broken affordance is not the same as
promising a working one, and this change deliberately does not pretend to be the latter.

## Blast radius

Production behaviour is **unchanged**, and that is checkable rather than argued: the only
production caller never passed the parameter, so the cutoff it computes is identical before
and after. The behaviour that changes is a script path that no longer exists.

⚠ This is the ladder's corpus rung, so the A/B question has to be answered rather than
skipped: the A/B here is **degenerate by construction**, because the arm that differs
(`retention_cutoff_override` set) has exactly one caller and it is being deleted. Stated
rather than run, because running it would compare the production path against itself.

## Tests

- `ingest_insider_dataset_archive` accepts no parameter that can move the cutoff — asserted
  from its signature against the two cutoff functions.
  ⚠ **A signature test forbids the parameter, not "widening under any name"** (ckpt-1
  correctly called that claim overstated). Widening could still return via a setting, a
  global, or a second writer; what this test buys is that the *removed* affordance cannot
  come back unnoticed, which is the specific regression.
- The existing retention-cap suite (`tests/test_insider_transactions_retention_cap.py`, 8
  call sites) keeps passing unchanged; none passes the override.
- The three caps are pinned together, because ckpt-1 was right that testing Form 4 alone
  leaves the others free to drift: a beyond-cap Form 4 row is skipped, a beyond-18-month
  Form 5 row is skipped, and an old **Form 3** row is **kept** (its exemption is as much an
  invariant as the caps are, and correction 1 above is what happens when a reader forgets
  it).

## The data half — a recommendation, not an action

Posted on the issue with the measurement above. Nothing in this change touches a row.

The recommendation in one line: **if anything is removed, key it on
`source='form4' AND filed_at < '2023-06-05'` (4,120,242 rows) and nothing else** — that
predicate is unambiguous by construction, needs no contaminated provenance column, and
cannot touch ungated Form 3 or the 69,698 aged-out rows a legitimate write produced. And
the prior question is whether to remove anything at all: the corpus is a 20-year span that
took a bulk download to build, the operator's irreversible-loss warning applies, and #2788
already shipped a read-side mitigation that makes the rows invisible to the operator
without destroying them.
