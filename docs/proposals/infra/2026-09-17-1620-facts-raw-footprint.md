# #1620 — `financial_facts_raw` footprint: what the 7.4 GiB actually was

Status: executed on dev 2026-09-17. #3117 priority 4.

## Verdict

The relation's cost was **not** identity width and **not** the surrogate PK. It was
**free space stranded by the retention sweep's own churn**, held in leaf partitions
carrying 9.9% of the rows and 71.2% of the bytes. Verdict: **COMPACT**, done —
**7,610.7 MiB → 1,856.8 MiB, 5,753.9 MiB reclaimed (75.6%)**, row count identical
before and after. The database fell from 75 GB to 69 GB.

The ticket's own two hypotheses are re-measured and answered: concept-interning
**DEFER** (real, 387.9 MiB, ~15× smaller than the reclaim), surrogate PK **RETAIN**
(0 scans, but mandated by a standing prevention rule).

Every figure here is computed at run time by
`scripts/audit_1620_facts_footprint.py`; none is hand-maintained.

## Source rule

A physical-storage decision, so the governing rule is PostgreSQL's own documented
vacuum behaviour. PostgreSQL 17 manual §24.1.2 *Recovering Disk Space*:

> *"The standard form of `VACUUM` removes dead row versions in tables and indexes and
> marks the space available for future reuse. However, it will not return the space to
> the operating system, except in the special case where one or more pages at the end
> of a table become entirely free and an exclusive table lock can be easily obtained."*

> *"In contrast, `VACUUM FULL` actively compacts tables by writing a complete new
> version of the table file with no dead space… It also requires extra disk space for
> the new copy of the table, until the operation completes."*

§24.1.1: *"`VACUUM FULL` requires an `ACCESS EXCLUSIVE` lock on the table it is working
on."*

⚠⚠ **The same section argues against this change, and that is the objection the work
had to clear:**

> *"there is not much point in this if the table will just grow again in the future.
> Thus, moderately-frequent standard `VACUUM` runs are a better approach than
> infrequent `VACUUM FULL` runs for maintaining heavily-updated tables."*

The partitions compacted here **are** heavily-updated — they carry 991,131 inserts and
994,342 deletes against 457,575 live rows. On a plain reading they are the relation's
*worst* candidates. §"Why the doc's caution did not block this" answers it; if that
answer is wrong, the change was wrong.

⚠ Note the manual's own exception, which the first draft of this document omitted:
plain `VACUUM` *can* return space when trailing pages are entirely free. It had not
done so here — the free space was interior, not trailing.

## Full-population verification

All figures are full-population reads on the dev database (`ebull`, PostgreSQL 17.9),
2026-09-17, no sampling.

### 1. The ticket's premises, re-measured

| Ticket claim | Measured 2026-09-17 | Verdict |
| --- | --- | --- |
| 14.6M live rows | **4,610,319** (exact `count(*)`) | ⛔ stale by 3.2× |
| heap 2.7 GB / indexes 4.4 GB | heap **2910.0 MiB** / indexes **4690.6 MiB** | ✅ |
| "0.13% dead tuples — NOT bloat" | dead 111,043; **free space ~2.1 GiB** | ⛔ conflates two things |
| "All 5 indexes load-bearing" | ✅ — 4 by scan count, 1 by constraint (§4) | ✅ |
| interning "could shave ~1–1.5 GB" | **387.9 MiB** | ⛔ over-stated ~3× |

The ticket's 14.6M is its own 2026-06-13 reading and is quoted, not re-derived — no
comparable exact historical count exists, so the decline is reported as *ticket-stated
baseline vs measured today*, not as a measured trend.

⚠ `pg_stat_user_tables.n_live_tup` read **1,172,543** against an exact `count(*)` of
**4,610,319**. It is an estimator refreshed by autovacuum/analyze, never a count, and
is used as one nowhere here. The instrument prints both side by side for this reason.

### 2. Measuring live bytes — the trap, and the correction

The first draft estimated live payload as `sum(pg_column_size(row)) + 27`. **That
double-counts.** `pg_column_size` of a whole-row composite already includes the
23-byte tuple header — on a one-column `bigint NOT NULL` table it returns 32, not 8.
The error overstated live bytes by 101.1 MiB and produced fill ratios above 100%,
which is how it was caught.

The estimator is now `pg_column_size(row) + 4` (the composite plus its line pointer),
and it is **cross-validated against an independent measurement** rather than trusted:
estimate **755.9 MiB** vs a fresh rebuild of the same rows at **766.5 MiB** — 1.4%
apart, the residual being per-page headers and alignment.

⚠ **The error direction is the opposite of what it looks like, and the first draft got
it backwards.** Under-stating live bytes lowers computed fill, making a partition look
emptier and therefore *more* eligible — it **overstates** reclaimable space.
Over-stating is the conservative direction. Because the `+4` estimator errs low by
~2.3 bytes/row, selection does not use it (§5).

### 3. Where the bytes were

| Band | Leaves | Rows | Live | Heap | Idx + aux | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HOT (fill ≥ 20%) | 16 | 4,152,744 | 681.3 MiB | 872.5 MiB | 1321.6 MiB | **2194.1 MiB** |
| COLD (fill < 20%) | 74 | 457,575 | 74.6 MiB | 2037.5 MiB | 3377.4 MiB | **5414.9 MiB** |
| EMPTY (0-byte heap) | 36 | 0 | 0 | 0 | 1.7 MiB | **1.7 MiB** |

Cold fill ran **1.1%–6.6%**: `financial_facts_raw_2019q3` held 2,573 live rows in
112.0 MiB.

⚠ "Indexes" (4690.6 MiB) and "total − heap" (4700.7 MiB) differ by 10.1 MiB. That gap
is auxiliary forks — FSM, visibility map, and the TOAST relation with its index — not
a missing index. The instrument prints both so the discrepancy cannot be read as an
error.

### 4. The ticket's two levers, answered

**Lever 1 — concept interning: DEFER.** `concept` has **83 distinct values** at mean
length 29.78; `taxonomy` has **3** at 6.95. Interning both to `smallint`, measured by
reconstruction:

| | As-is (rebuilt) | Interned | Saving |
| --- | ---: | ---: | ---: |
| heap | 766.5 MiB | 607.5 MiB | 159.0 MiB |
| identity index | 457.7 MiB | 307.4 MiB | 150.3 MiB |
| instrument_concept index | 176.7 MiB | 98.1 MiB | 78.6 MiB |
| **total** | | | **387.9 MiB** |

Real — 24.2% of a rebuilt relation — but ~15× smaller than the reclaim, and it costs a
4.61M-row migration across 126 partitions, a dictionary table as an FK target, and a
rewrite of the upsert identity plus every reader.

⚠ 387.9 MiB is a **prototype** figure, not a net migration saving: it excludes the
dictionary table, the FK, and the partitioned layout's own per-leaf overhead. The
ticket's "~1–1.5 GB" is not retroactively *wrong* — it was formed against a 14.6M-row
population — but it does not describe today's relation.

Sequencing is a recommendation, not a proof: reclaim-first makes the interning
decision re-measurable against a relation no longer dominated by a different defect. A
combined rewrite later could in principle capture both in one pass.

**Lever 2 — surrogate PK: RETAIN.** `financial_facts_raw_pkey` was 708.2 MiB at **0
lifetime index scans across all 126 leaves**, and **no foreign key anywhere references
`financial_facts_raw`**. Both halves of the ticket's droppability test pass. It stays:

`docs/review-prevention-log.md` §*"DISTINCT ON / ROW_NUMBER without a UNIQUE final
tie-break diverges between single-key and bulk plans"* is a standing rule, not an
observation — *"any `DISTINCT ON` / `ROW_NUMBER() OVER (ORDER BY …)` / `fetchone()
ORDER BY … LIMIT 1` whose leading keys aren't provably unique **MUST** end its
`ORDER BY` with the surrogate PK"*. It is enforced in
`app/services/instrument_analytics.py`, whose two FY readers end
`… filed_date DESC, accession_number DESC, fact_id DESC` (`:506`, `:860`, rationale at
`:479-485`). First seen in **#2127 Phase 2**, where a full-population A/B surfaced
Piotroski/Altman divergence on 495 instruments.

⚠ The precise guarantee, corrected from the first draft: the constraint is
`PRIMARY KEY (fact_id, period_end)` (`sql/156:40`), so it enforces uniqueness of the
**pair**, not of `fact_id` alone — duplicate `fact_id` at different `period_end` is
already legal. That is still sufficient, because `period_end DESC` is already a leading
`ORDER BY` key, so pair-uniqueness makes the ordering total. The first draft claimed
the PK was "the only thing enforcing `fact_id` uniqueness", which is wrong; the verdict
is unchanged but the reasoning was not.

This is #1620's own caution — *"A low scan count is not sufficient reason to remove a
constraint or index"* — resolving against the lever that produced it.

**Not a lever — the `decimals` column.** Non-NULL on **0 of 4,610,319** rows
(companyfacts carries no `decimals` key; writer at
`app/providers/implementations/sec_fundamentals.py:509`). Dropping it would not
measurably shrink anything: a NULL costs a null-bitmap bit, bitmaps are byte-rounded,
and a dropped column leaves its attribute slot until a rewrite. Recorded because "drop
the empty column" is the obvious next suggestion and it is worth ~nothing. Confirms
prevention-log line 3556 on today's population.

## Why the doc's caution did not block this

§24.1.2's model is explicit: *"each table occupies space equivalent to its minimum size
plus however much space gets used up between vacuum runs."* The caution binds a table
already at that steady state, where `VACUUM FULL` buys a shrink that churn undoes.

These partitions were not at that steady state:

- **minimum size** — measured by reconstruction, not inferred: the cold band's rows
  rebuilt to **163.2 MiB**, against **5414.9 MiB** occupied. 33×.
- **autovacuum was keeping up, not failing** — 381 runs over the cold band, holding it
  at 33,762 dead tuples. It cannot fix this: by §24.1.2 it never issues `VACUUM FULL`.

So the occupancy was a peak the churn had not re-approached, not a level the churn
sustains. ⚠ That is an inference from one snapshot plus cumulative counters with **no
recorded reset** (`pg_stat_database.stats_reset` is NULL), so it is a *rate-free*
argument: it compares bands, and it does not establish a per-day growth figure.

**The falsifiable form, which is how this should be judged.** Re-running
`audit_1620_facts_footprint.py --plan` selects partitions by reclaimable bytes. It
selected 58 before the run and **0 immediately after**. If it starts selecting
partitions again — say, more than 5 partitions or more than 500 MiB — the steady-state
argument was wrong and the correct verdict reverts to the manual's default of leaving
it alone. No duration is claimed here because none has been observed yet.

⚠ `postgres_health`'s `db_size_growth_7d` is **not** an adequate detector and the first
draft wrongly nominated it: `app/workers/scheduler.py:1626` states it is
*"informational, no alarm"*, it measures the whole database, and unrelated shrinkage
elsewhere can mask this relation's regrowth. The detector is re-running this
instrument.

The honest residual: the peak was created once, by the pre-retention backlog. A future
bulk re-ingest of the same shape recreates it. This change does not prevent that.

## What was done

`scripts/audit_1620_facts_footprint.py`, four modes — `--census` (default, read-only),
`--rebuild-probe`, `--plan`, `--apply`. No migration, no schema change, no row deleted,
no job added.

**Selection is an absolute reclaimable-byte floor (16 MiB), not a fill ratio.** A ratio
has two defects this relation exhibits: a partition holding a handful of rows still
occupies at least one 8 KiB page, so it can sit permanently below any ratio threshold
and be re-selected forever; and a ratio has an arbitrary boundary where a 19.9%
partition qualifies outright on evidence gathered from a 1.1–6.6% population. The floor
converges — demonstrated: the re-run selected 0. The 16 MiB value has no published
formulation; it is fixed **by construction** (two orders of magnitude above the 8 KiB
page granularity that makes a ratio oscillate) and frozen as a named constant.

**`count(*)` before/after is reported conditionally, not asserted unconditionally.**
`VACUUM FULL` is row-preserving by definition; it rewrites, it does not filter. A
before/after count races with legitimate concurrent writes, so a *correct* rewrite can
fail it and a broken one can pass under balanced insert/delete. The instrument captures
`n_tup_ins`/`n_tup_del` around each partition and only claims preservation where those
counters did not move; where they did, it prints the observed write volume instead of a
verdict it cannot support. On this run **all 58 partitions were quiet**, so the claim
holds for every one of them.

**`lock_timeout = 5s`, read narrowly.** It bounds how long the `ACCESS EXCLUSIVE`
request *waits*, not how long it is held, and while it waits it queues other conflicting
requests behind it. It is a blast-radius limiter, not an "only runs when idle"
guarantee. Measured hold: **0.02 s to 2.02 s** per partition.

**`ANALYZE` runs with each rewrite** (`VACUUM (FULL, ANALYZE)`). `VACUUM FULL` does not
update planner statistics, and this relation already had a 4× stale `n_live_tup`.

**Refuses to start inside 02:30–03:15 UTC.** `financial_facts_retention_sweep` runs
daily at 02:45 and its DELETE is not partition-key-restricted, so it can touch every
leaf; a blocked leaf would stall the whole sweep transaction.

### Measured result (dev, 2026-09-17 05:08 UTC)

| | Before | After | Reclaimed |
| --- | ---: | ---: | ---: |
| heap (main fork) | 2910.0 MiB | 784.8 MiB | 2125.2 MiB |
| indexes proper | 4690.6 MiB | 1070.6 MiB | 3620.0 MiB |
| auxiliary forks | 10.1 MiB | 1.4 MiB | 8.7 MiB |
| **total** | **7610.7 MiB** | **1856.8 MiB** | **5753.9 MiB (75.6%)** |

- 58 partitions compacted; `count(*)` **4,610,319 before and after**.
- Longest `ACCESS EXCLUSIVE` hold **2.02 s** (`2023q4`, 501,237 rows); 51 of 58 under
  0.25 s. Total wall clock ~90 s.
- Plan projected 5644.3 MiB; actual 5753.9 MiB. The projection was **1.9%
  conservative** — it scales current size by the live fraction and does not model the
  index rebuild's packing gain.
- Database size **75 GB → 69 GB**.
- Re-running `--plan` selects **0 partitions**.

⚠ The achieved 1856.8 MiB sits 251 MiB above the 1605.9 MiB single-relation rebuild
floor. That is expected and is the partitioned layout's own cost — 126 separate heaps
and 630 separate btrees, each with its own metapage, root and page tails — plus churn
committed since. The rebuild figure is a floor for an unpartitioned copy, not a target.

### Acceptance evidence

- **Row count preserved** — 4,610,319 before and after, and per-partition equality
  asserted on all 58 (all quiet).
- **Reader determinism preserved across the physical reorder** — this is the check that
  mattered, because `VACUUM FULL` changes physical row order and physical order is
  exactly what the #2127 defect was sensitive to. Full-population A/B of
  `_read_latest_two_fy_facts` against `_bulk_read_latest_two_fy_facts` over **all 4,185
  instruments** carrying 10-K/FY us-gaap facts: **0 mismatches**.
- **All five indexes present on all 126 leaves** after the run, with lifetime scan
  counters continuing to advance (`retention_evict` 2,244,859 → 2,245,133 during the
  session), i.e. live and being used.
- `financial_facts_retention_sweep` unaffected — no schema or row change for it to see.

### Rollback

Nothing to roll back: no row, column, index or constraint changed. The failure mode is
a partition left uncompacted, which is the prior state. ⚠ That is narrower than "no
failure is possible" — a rewrite still consumes WAL and I/O, and an interrupted run
leaves earlier partitions committed (each is its own autocommit statement). Resumption
is by re-running, which re-derives the selection.

## Excluded, recorded explicitly

- **Interning** — deferred; re-measure post-reclaim against the new 1856.8 MiB baseline.
- **Dropping the PK or `fact_id`** — refused; a standing prevention rule mandates it.
- **Dropping `decimals`** — worth ~nothing.
- **Partition-drop by age** — refused by the ticket and by the data: old partitions hold
  legitimate comparative-period facts belonging to *kept* recent accessions.
- **The 36 empty partitions** — 1.7 MiB.
- **`pg_repack`** — a new dependency to avoid a 2-second lock.
- **A scheduled reclaim job** — #3117 says don't invent a second health system. The
  instrument is run on demand.
- **`pgstattuple`** — not installed on this cluster (noted on #3115 too). Not needed:
  reconstruction measures the minimum directly rather than estimating bloat.
