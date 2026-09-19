# 13D/G link re-resolution sweep (#3236)

Status: spec · 2026-09-19 · Refs #3236, #2329, #788, #836, #1638

## Problem

19 accessions / 82 rows in `blockholder_filings` carry `instrument_id IS NULL`
while their issuer resolves today through
`blockholders._resolve_issuer_to_instrument_id`. Because
`_ingest_single_accession` skips the observation write-through when
`instrument_id is None` (`app/services/blockholders.py:808-822`), these filings
are **absent from the ownership layer entirely** — not merely unjoinable.

Mechanism: race-loss. The `external_identifiers` mapping landed after the only
ingest, and nothing re-resolves a NULL after write. `cusip_extid_sweep` closes
exactly this race for 13F-HR (#788 / #836); the 13D/G channel has no equivalent.

This is distinct from #2329's cause A (fixed in `1ed6ce3b`): those rows had a
live observation and only the typed link was stale, so relaxing
`_upsert_filing_row`'s conflict action repaired them on re-ingest. These 19 were
never re-ingested, so no write-path change can reach them.

## Source rule

| decision | governing rule | where |
| --- | --- | --- |
| which instrument a 13D/G subject company resolves to | CUSIP is the security-precise key (#1102 "CIK = entity, CUSIP = security"); CIK is a single-sibling-only fallback (#1628). A multi-class CIK stays unresolved — "a 5%+ holder owns ONE class", never guess the class | `blockholders.py:450-500` |
| which reporting person becomes the observation | joint filers on one accession claim the same beneficial figure, so they collapse to ONE observation at the largest `aggregate_amount_owned` (#837); identity is that person's own CIK else the **document filer of record**, NEVER the manifest/subject CIK (#1638) | `blockholders.py:862-902` |
| aggregation regime for the resulting figure | a 13D/G beneficial figure is an **overlapping restatement**, not an additive holding — MAX across restatements of the same shares (Rule 13d-3; prevention-log "a beneficial owner is ONE unit of account") | `ownership_rollup.py` |
| lock order | per-accession advisory lock, THEN per-instrument refresh lock; acquired inside the write path, never across network I/O | `raw_filings.py:253-278` |
| 13D/G structured-XML floor | mandate effective 2024-12-18 — but the final rule permitted **voluntary** XML from 2023-12-18, so a pre-mandate date is not grounds to reject a stored XML body (Codex ckpt-1 finding 35 corrected the skill's shorthand) | `.claude/skills/data-sources/sec-edgar.md` §2.4, §7.7 |

⚠ Not load-bearing here regardless: the sweep parses whatever body is stored and
gates on agreement with the typed rows, so it never reasons from a filing date
to a body format. All 19 bodies parse.

⚠⚠ **The skill and the code genuinely disagree about 13D/G sibling fan-out, and
my first draft of this paragraph got it wrong.** I wrote that `sec-edgar.md`
§3.6 governs "filing-list routing only". It does not — its column heading is
literally **"Fan-out at write time?"**, and the 13D/G row says *"issuer-scoped →
fan out across siblings"* off the back of a *"Per-security data inside? **no**"*
classification. Meanwhile `_resolve_issuer_to_instrument_id` refuses to fan out
and leaves a multi-class CIK unresolved. Codex ckpt-1 finding 30; corrected here
rather than argued away.

On the merits the **code** looks right and the skill row looks wrong: a 13D/G
carries `<issuerCusips>` and a `securities_class_title`, i.e. it *is*
per-security, so fanning a Class A stake across Class B siblings would attribute
shares to a security the filing never named. That is the same "CUSIP = security"
rule (#1102) the table's own §3.2 derivation rests on.

**It does not bind this ticket either way.** The sweep calls the same resolver
the live writer calls, so it inherits the writer's policy by construction — a
repair must never hold a different policy from the writer it repairs. Changing
that policy would move 119,426 rows' semantics and is a separate decision.
The skill correction is parked on **#2403** with the rest of the loop-worktree
`.claude/**` write-refusal queue.

## Full-population verification (dev, 2026-09-19)

`scripts/census_3236_stranded_13dg.py` — resolvability decided by **calling**
`_resolve_issuer_to_instrument_id` per candidate, with SQL used only as a
superset prefilter (PG `trim()` ≠ Python `.strip()`; `lpad(…,10,'0')` truncates
an overlong CIK, so a SQL restatement is a different predicate).

```
blockholder_filings total                119,426
  instrument_id IS NULL                    9,834  (4,199 accessions)
  … resolvable through the resolver           82  (19 accessions)
  … not resolvable today                   9,752  (4,180 accessions)

  cause A (live observation exists)            0  (0 accessions)
  cause B (NO observation at all)             82  (19 accessions)  <- #3236

  raw primary_doc_13dg present                19 / 19
  filer_cik == issuer_cik (adapter tell)       0 / 19
  distinct instruments affected                7
```

Reconciles with the issue body's `9,861 / 4,207` exactly: the 27-row / 8-accession
delta is #3235's repair.

### Evidence probe — `scripts/probe_3236_parser_drift.py`

⚠ **Snapshot isolation is ASSERTED, not claimed.** The first version of both
scripts ran `SET default_transaction_isolation` under `autocommit=True`, which
binds only transactions started *afterwards* — so every statement took its own
snapshot. Same defect the #3232 close-out recorded; caught again at Codex
ckpt-1 (finding 36). Now set on the connection before any statement and
verified with `SHOW transaction_isolation`.

Measured on the full affected population:

```
A. snapshot isolation asserted: repeatable read
targets (no observation on EITHER key): 19
B. accessions with non-uniform header/filer across rows:      0
C. accessions mixing NULL and non-NULL links (WHOLE TABLE):   0
   accessions pointing at >1 distinct instrument (WHOLE TABLE): 0
D. observations already at these accessions: live=0 retired=0
   retired blockholder observations anywhere in the table: 1,240
E. re-parse supplies a non-NULL filed_at on   9 / 19
   … and it DIFFERS from the stored value on  2 / 19
F. accessions with a tie on largest aggregate: 13 / 19
G. drift by column: {date_of_event: 19, member_of_group: 19 (6 accessions)}
H. ORDER-SENSITIVE identity-column drift:      0 / 19
```

Five of these changed the design:

1. **Ties are the MAJORITY condition — 13 of 19.** `resolve_blockholder_reporter_identity`
   uses `max()`, which returns the **first** element among equals, so the
   observation identity depends on parser element order. A sorted-multiset
   gate cannot see a reordering. **H** therefore compares the identity columns
   `(reporter_cik, reporter_name, aggregate_amount_owned, percent_of_class)`
   as an ordered **sequence** — stored rows by `filing_id` (insert order)
   against the parser's own order — and it matches on 19/19. The gate is
   order-sensitive for this reason.
2. **`filing.filed_at or ref.filed_at` lets the re-parse WIN.** On the 2
   accessions where they differ, the re-parse yields the `signatureInfo/date`
   at UTC **midnight** where the stored value is the real timestamp
   (`2026-07-14 23:20:33+00` → `2026-07-14 00:00:00+00`). Since `filed_at`
   sets `period_end` (part of the observation natural key) and drives
   amendment ordering, a synthetic `AccessionRef` is **not** sufficient — the
   sweep pins it with `dataclasses.replace(filing, filed_at=<stored>)`.
3. **Only two columns drift, and neither is consumed by the observation.**
   `date_of_event` NULL → a real date (additive: the parser gained a field;
   NULL on **100% of all 119,426 rows**, so a backfill is a corpus operation
   with its own ticket, never a step inside a bug closure). `member_of_group`
   `'a'`/`'b'` → NULL (subtractive: data-engineer invariant **I17** — the
   element is absent from modern 13D XML and the legacy column is noise, so
   nothing may depend on it). Both are excluded from the gate **by citation**,
   not by convenience.
4. **No accession anywhere in the table mixes NULL and non-NULL links, and none
   points at more than one instrument** (C). The share-class mislink Codex
   raised is unreachable on today's corpus — but it is guarded at runtime
   anyway, because the sweep is recurring and the census is a snapshot.
5. **1,240 retired observations exist table-wide**, though none at these 19 (D).
   `record_blockholder_observation` never clears `known_to`, so a retired row
   at the natural key would be updated-but-still-invisible while the link goes
   non-NULL — permanently unreachable by any later sweep. Guarded, not assumed.

`document_filer_cik == blockholder_filers.cik` on 19/19, consistent with the
in-house parser having written all of them (`sec_13dg.py:610-612` mirrors the
two; the edgartools adapter can make `primary_filer_cik` the *issuer* CIK —
#1638 — whose tell is `filer_cik == issuer_cik`, absent here). The design does
**not** rely on this: `document_filer_cik` comes from the re-parse, so an
adapter-written accession arriving later is handled rather than silently
acquiring the #1638 defect.

## Design

New service `sweep_unlinked_blockholder_filings(conn, *, limit)` in
`app/services/blockholders.py`, driven by a new daily job
`blockholder_link_sweep`, mirroring `cusip_extid_sweep` (`scheduler.py:10473`):
service returns a report dataclass, job sets `tracker.row_count`.

**Candidate selection** is whole-accession (never row-limited — a row limit
splits an accession), ordered deterministically by `accession_number`, `limit`
counted in accessions. The selection transaction commits before the repair loop.

Per candidate accession, in its **own transaction, committed individually** —
not a savepoint. `connect_job()` runs with autocommit off, so nesting
`conn.transaction()` blocks inside the outer implicit transaction would hold
every accession and instrument lock until the job's final commit. Counters
advance only after that accession's commit.

1. `raw_filings.acquire_filing_accession_write_lock(conn, accession)` — a bare
   UPDATE does not serialise against the manifest drain or rewash.
2. **Re-read under the lock.** Selection happened in an earlier transaction, so
   another writer may have repaired or replaced the accession meanwhile. Every
   precondition below is evaluated on rows read *after* the lock.
3. **Guards — each a named skip, never a silent continue:**
   - `already_linked` — no NULL-link rows remain.
   - `non_uniform_header` — issuer keys / `submission_type` / `status` /
     `filed_at` / `filer_id` disagree across the accession's rows, so "the
     stored row" is not well defined.
   - `link_conflict` — a non-NULL link already present on the accession that
     does not equal the newly resolved instrument.
   - `still_unresolved` — `_resolve_issuer_to_instrument_id` returns `None`.
   - `observation_exists` — any observation at the accession, **live or
     retired**, on either `source_document_id` or `source_accession`.
   - `no_raw_body` / `parse_failed`.
   - `drifted` — the ordered identity sequence, `submission_type` or `status`
     differ from the stored rows.
4. `UPDATE blockholder_filings SET instrument_id = %(iid)s WHERE
   accession_number = %(a)s AND instrument_id IS NULL` — narrow by
   construction; a non-NULL link is never overwritten. No DELETE in this path.
5. Observation through the live chokepoint
   `_record_13dg_observation_for_filing`, on a `dataclasses.replace` copy
   pinning the stored `filed_at`, with `source_url` taken from the **stored
   raw document's** `source_url` (verified provenance of the body actually
   parsed, not a reconstructed archive path) and `run_id=uuid4()` (the manifest
   path's precedent, `sec_13dg.py:387`).
6. **Postcondition, before commit:** exactly one **live** observation exists at
   `(instrument_id, source_document_id=accession)`. `_record_13dg_observation_for_filing`
   returns normally when identity or `filed_at` is unavailable, so without this
   the sweep could commit a link with no observation and count it a repair.
   Failure raises → that accession rolls back whole → `observation_missing`.
7. `refresh_blockholders_current(conn, instrument_id=...)` inside the same
   transaction (per-accession lock then per-instrument lock, the documented
   order). Its failure must roll the accession back, not leave a link and
   observation without `_current`.

### Why not the rewash shape

#3236 records the rejected design (Codex ckpt-1, 37 findings). The decisive
differences: no DELETE of reporter rows; the **same** parser the original ingest
used, not edgartools + `_schedule13_adapter`; and a postcondition asserting the
reporter set and the resulting observation rather than "no NULL rows remain" — a
check that rows disappearing would satisfy.

### Blast radius

- `blockholder_filings`: one column, only on rows currently NULL.
- `ownership_blockholders_observations`: one INSERT per repaired accession. The
  writer is `ON CONFLICT DO UPDATE`, not a pure INSERT — the `observation_exists`
  guard is what makes it behave as an insert here, rather than the key shape.
- `ownership_blockholders_current` **and `ownership_refresh_state`**: rewritten
  by the existing reconciler, which also reconciles other reporters for that
  instrument. "No DELETE" describes this sweep's own statements, not the
  reconciler's MERGE.
- ⚠ **Operator-visible, and figures are expected to MOVE.** A repaired filing
  joins the amendment chain and can win it — on #3235's cause-A cohort 7 of 8
  instruments moved, one by 44%. Acceptance measures counterfactually rather
  than asserting no change.

## Acceptance

The expected repair set — `(accession, instrument_id, reporter_cik,
aggregate_amount_owned)` for all 19 — is **frozen before the run**, so "zero
resolvable NULLs remain" cannot be satisfied by linking to the wrong instrument,
writing no observation, or deleting rows.

1. Resolvable `instrument_id IS NULL`: **82 → 0 rows, 19 → 0 accessions**, and
   every repaired row's `instrument_id` equals the frozen expectation.
2. `ownership_blockholders_observations`: exactly 19 new live rows, one per
   accession, each matching the frozen `(instrument_id, reporter_cik,
   aggregate_amount_owned, percent_of_class, filed_at, period_end)`. No row
   retired, no pre-existing row modified — verified by a keyed before/after
   diff of the whole table, not a count.
3. Full-population keyed diff over all 119,426 `blockholder_filings` rows: the
   differing set equals the frozen repair set — 0 added, 0 removed, and
   `instrument_id` is the only column that moved.
4. `/instruments/{symbol}/blockholders` before/after for all 7 affected
   instruments. ⚠ That endpoint reads `blockholder_filings` directly, so it
   cannot by itself evidence the ownership-layer repair — `ownership-rollup`
   (which reads `_current`) is measured alongside it for the same 7, and both
   are reported.
5. Golden panel (AAPL/GME/MSFT/JPM/HD) `ownership-rollup` unchanged, plus a
   whole-table `_current` diff bounded to the 7 expected instruments — so a
   collateral change outside them is caught rather than sampled for.
6. A second sweep pass reports `repaired=0` **and** leaves all three tables
   byte-identical to the post-first-pass state.
7. Cross-source: one accession verified against SEC EDGAR directly on issuer,
   reporting persons, class and aggregate.
