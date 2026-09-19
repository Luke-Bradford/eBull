# #2329 residual — the stranded `blockholder_filings.instrument_id` link

Status: spec, revised after Codex checkpoint 1 (the first draft's design was
withdrawn — see "Design withdrawn" below). Ticket: #2329, the "Also, separately"
residual.

Rung: behavioural change with data semantics + a bounded one-off data repair.
Definition-of-Done clauses 8-12 apply.

## Problem

`blockholder_filings.instrument_id` is resolved **once**, at write time, by
`blockholders._resolve_issuer_to_instrument_id`. Nothing re-resolves a NULL
afterwards, and `_upsert_filing_row` is `ON CONFLICT DO NOTHING`
(`app/services/blockholders.py:572`), so a **later, successful re-ingest of the
same accession cannot repair the link either**.

### Full-population census (dev, 2026-09-19)

Through the resolver's own two-tier ordering, not a CUSIP-only join:

| | rows | accessions |
| --- | ---: | ---: |
| `blockholder_filings` total | 119,426 | — |
| `instrument_id IS NULL` | 9,861 | 4,207 |
| … resolvable today via CUSIP | 26 | 9 |
| … resolvable today via the CIK single-sibling fallback **only** | **83** | **18** |
| … not resolvable from the stored identifiers | 9,752 | 4,180 |

**The ticket's headline 26 is an undercount.** The census that produced it joined
`external_identifiers` on CUSIP only; the resolver's second tier — CIK → exactly
one instrument (#1628) — is the larger half. Residual against the stored
identifiers: **109 rows / 27 accessions / 15 instruments**.

⚠ That is a *stored-identifier* measurement. It does not bound what a re-parse of
the raw XML might additionally resolve; no claim is made about that here.

### Two causes with different impact, and only one is in scope

**Cause A — a link a successful re-ingest was forbidden to repair. 8 accessions / 27 rows / 8 instruments.**

Written by the ingest batch of **2026-06-14 12:44Z**. `c4f1d2e9` (#1628), which
*added* the CIK fallback to the resolver, landed **2026-06-14 14:43Z** — two
hours later. They were written by the pre-#1628 CUSIP-only resolver and correctly
got NULL at the time.

The manifest pipeline (`manifest_parsers/sec_13dg.py:353-386`) then re-ingested
all 8 on 2026-06-20…06-25 with the fixed resolver. It resolved the instrument,
wrote `ownership_blockholders_observations`, called
`refresh_blockholders_current`, and logged `status='success'` — but
`_upsert_filing_row`'s `ON CONFLICT DO NOTHING` left the pre-existing reporter
rows' `instrument_id` NULL.

**This is operator-visible.** Both blockholder drill-through readers filter
`WHERE bf.instrument_id = %(iid)s` (`app/api/instruments.py:4575` and `:4657`),
so for these 8 instruments the ownership card's blockholder wedge includes the
holder (it comes from the observation) while the drill-through does not list the
filing behind it. Same class of card-vs-drill disagreement as #3232.

**Cause B — genuine race-loss. 19 accessions / 82 rows. NOT IN SCOPE HERE.**

The mapping landed after the only ingest, so `_record_13dg_observation_for_filing`
was skipped (`blockholders.py:766-773`) and **no live observation exists today**.
Repairing these requires re-driving the filing through a parser, which is the
design Codex rejected. Carried on the issue with the objections; see "Cause B" below.

### Two premises that were false and are recorded rather than quietly dropped

1. **`external_identifiers.created_at` is not a "mapping first appeared"
   witness.** `2026-06-03 18:56:56.241482` covers **5,553 rows** — a bootstrap
   seed. Attributing cause A to the arrival race on that column would have merged
   two different defects into one. What settled cause A is the commit timestamp
   of `c4f1d2e9` against the rows' `fetched_at`.
2. **"Pinned `status='partial'` forever" was wrong** (Codex ckpt-1 finding 7).
   `blockholder_filings.status` is the 13D/G active/passive flag, constrained by
   `blockholder_filings_submission_type_status_consistent` — not an ingest
   status. All 8 accessions read `status='success'` in
   `blockholder_filings_ingest_log` today, written by the later manifest ingest.
   ⚠ My *correction* to that claim was itself wrong and is corrected here
   (Codex ckpt-1 revision, finding 22): that log is **keyed `accession_number`
   PRIMARY KEY** (`sql/096_blockholder_filer_seeds_and_log.sql:63`) and
   `_record_ingest_attempt` overwrites on conflict, so it holds the **latest
   recorded status, not an attempt history**. The ingest audit reads correct
   today; it does not preserve the earlier `partial`.

## Source rule

Resolution semantics are settled and are **not** re-derived:

- **CUSIP = security, CIK = entity** (settled #1102).
- **Two-tier resolution with a single-class-only CIK fallback** — #1628,
  `_resolve_issuer_to_instrument_id`.
- **Provider allow-list `('sec','openfigi')`, SEC-first tiebreak** — #1233 PR-1b,
  settled 2026-05-22; incident write-up in `docs/review-prevention-log.md`
  § "A widened allow-list must be pushed to every CONSUMER".
- **Per-reporter row immutability** — migration 095's unique index
  `uq_blockholder_filings_accession_reporter` on
  `(accession_number, COALESCE(reporter_cik,''), reporter_name)`, and
  `_upsert_filing_row`'s documented contract: *"one accession × one reporter ==
  one row, immutable on first ingest"*.

⚠ This change does **not** re-derive resolution anywhere. The value written is
always the one `_resolve_issuer_to_instrument_id` produced in-process on the same
call, or (for the one-off repair) the instrument the accession's own live
observation already uses.

## Design

### 1. Heal a NULL link at the point that has the answer

`_upsert_filing_row`'s conflict clause becomes a **conditional** `DO UPDATE` that
can only perform the NULL → value transition on `instrument_id`, and touches no
other column:

```sql
ON CONFLICT (accession_number, COALESCE(reporter_cik, ''), reporter_name)
DO UPDATE SET instrument_id = EXCLUDED.instrument_id
WHERE blockholder_filings.instrument_id IS NULL
  AND EXCLUDED.instrument_id IS NOT NULL
RETURNING (xmax = 0) AS inserted
```

`uq_blockholder_filings_accession_reporter` is a **full** (non-partial) unique
index on those three expressions, so arbiter inference is unambiguous.

Why this and not `DO UPDATE SET` on the whole row: the immutability contract is
deliberate, `_apply_blockholders`' DELETE-then-INSERT exists precisely because of
it, and a general `DO UPDATE` would let any re-ingest silently rewrite stored
beneficial-ownership figures. **A NULL link is an absence, not a value** — filling
it is strictly narrower than the contract forbids.

⚠ **Return-contract hazard.** The function returns `True` on insert and `False`
on conflict, and the caller counts `rows_inserted` from it. With `DO NOTHING` a
conflict yielded `rowcount == 0`; with `DO UPDATE` a *healed* conflict yields
`rowcount == 1`, so the naive `cur.rowcount > 0` would start reporting heals as
inserts. `RETURNING (xmax = 0)` distinguishes them: `xmax` is 0 on a genuine
insert and non-zero on a row updated by this statement. A suppressed conflict
(the `WHERE` fails) returns **no row at all**, which is also `False`.

### 2. One-off repair of the 27 rows already stranded — `sql/401`

The runtime fix only heals an accession the manifest pipeline re-ingests again,
which is unbounded in time. The existing 27 rows are repaired once, from the
accession's **own live observation**:

```sql
UPDATE blockholder_filings b
SET    instrument_id = o.instrument_id
FROM  (SELECT source_accession, min(instrument_id) AS instrument_id
       FROM   ownership_blockholders_observations
       WHERE  known_to IS NULL
       GROUP  BY source_accession
       HAVING count(DISTINCT instrument_id) = 1) o
WHERE b.accession_number = o.source_accession
  AND b.instrument_id IS NULL;
```

Measured on the full population, this rule selects **exactly 8 accessions / 27
rows / 8 instruments, 0 ambiguous** — i.e. precisely cause A, with cause B
excluded by construction (no live observation). `known_to IS NULL` restricts it
to live observations, so a retired row cannot supply the value.

⚠ The migration does not call the resolver — a migration cannot. Agreement
between the two authorities is asserted **out of band** and recorded in the PR:
for all 8 accessions, `_resolve_issuer_to_instrument_id` on the stored
identifiers returns the same instrument the live observation uses. A disagreement
would have meant the repair was choosing, not reconciling.

### 3. What deliberately does NOT happen

- **No observation is written and no `_current` is refreshed.** For all 8
  accessions the observation already exists and already feeds the rollup — that
  is exactly why the card and the drill-through disagree. Writing one would risk
  the `known_to` / period-end duplication the #1638 append-only rule warns about
  (`docs/specs/etl/2026-06-15-blockholder-reporter-cik.md`), for no gain.
- **No re-parse, no DELETE, no new `filing_id`, no new `fetched_at`.** Provenance
  is preserved; the only column that moves is `instrument_id`, NULL → value.
- **No scheduler change**, no new job, no change to `cusip_extid_sweep`'s
  contract or its `row_count` units.

## Design withdrawn at Codex checkpoint 1

The first draft routed the repair through a new `_rewash_13dg_accession` +
`_apply_blockholders`, mirroring `cusip_extid_sweep`. Withdrawn. The decisive
objections:

- `_apply_blockholders` DELETEs **every** reporter row for the accession and
  re-inserts, so it replaces already-correct rows in mixed accessions and
  regenerates `filing_id` / `fetched_at`.
- It re-parses with a **different parser** from the original ingest (edgartools +
  adapter vs `parse_primary_doc`), and the adapter sources `primary_filer_cik`
  and the date from the manifest — so filing dates, amendment ordering and filer
  associations could move as a side effect of a link repair.
- The observation upsert is keyed including period end and does not retire
  superseded rows, so re-writing observations for cause-A accessions risks
  duplication rather than being a no-op.
- A `no_progress` counter over "no NULL rows remain" is satisfied by rows simply
  disappearing, so it could not have detected a destructive repair.

None of those apply to the design above, because it neither re-parses nor writes
observations.

## Acceptance

1. **Link repaired:** `blockholder_filings` rows with `instrument_id IS NULL`
   whose accession has an unambiguous live observation: **27 → 0**; accessions
   **8 → 0**.
2. **Nothing else moved.** Exact keyed checksum over `blockholder_filings`
   excluding the `instrument_id` column, before and after, is **identical** —
   including `filing_id`, `fetched_at`, `filed_at`, reporter identity and every
   ownership figure. Row count unchanged at 119,426.
3. **The rollup layer does not move:** `ownership_blockholders_observations` and
   `ownership_blockholders_current` are byte-identical on exact keyed checksums
   before/after, because the observation for each of these accessions already
   existed.
4. **The drill-through DOES move, and that is the point** (Codex ckpt-1 revision,
   finding 14 — the earlier draft wrongly called this "joinability only").
   `/instruments/{symbol}/blockholders` aggregates the typed rows directly, so a
   repaired filing does not merely appear: where it is the most recent filing for
   a reporter it **wins the amendment chain** and supersedes a stale cover-page
   figure. Measure each of the 8 instruments counterfactually (the endpoint's own
   `per_reporter_chain` / `per_accession_block` SQL, with and without the
   repaired `filing_id`s) and validate the harness by matching its "after" column
   against the live endpoint. Golden panel (AAPL/GME/MSFT/JPM/HD) unchanged —
   guaranteed by acceptance 2, since none of their rows is in the differing set.
5. **Runtime fix pinned by a revert probe:** a DB test asserts (a) a re-ingest
   heals a NULL link, (b) a re-ingest does **not** overwrite a non-NULL link with
   a different value, (c) no other column changes on conflict, (d) the
   insert-vs-conflict return value is still correct for all three outcomes.
6. `sql/401` is idempotent: a second apply updates 0 rows.

## Cause B — researched, not built

19 accessions / 82 rows have no live observation and cannot be repaired without
re-driving the filing. That needs a design that answers Codex's objections above
— in particular a repair that does not replace already-correct reporter rows, and
an observation write that respects the #1638 retirement rule. Recommend it becomes
its own ticket rather than an item of #2329; the research is on the issue.
