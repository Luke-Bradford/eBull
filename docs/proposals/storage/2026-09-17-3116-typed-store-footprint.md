# #3116 — typed ownership + price stores: keep / compact / retire

**Status:** spike discharged. **No change implemented**, and the change an earlier draft of this
document proposed is explicitly *not* recommended — see §4.3. #3116 clause 6 forbids
`VACUUM FULL`, `REINDEX`, deletes and schema changes on the running DB during the spike, and its
acceptance says "implement only subsequently agreed concrete changes".

**Instrument:** `scripts/audit_3116_typed_stores.py` (`--census` default, `--rebuild-probe`).
Read-only census at REPEATABLE READ in one snapshot; exits 1 on invariant failure. Every *sizing
and rate* figure below is printed by that script; where a figure is a projection the script says
so and so does this document. ⚠ Two observations here are **not** script-produced and are marked
where they appear: the absence of a `sec_nport_dataset_ingest` row in `job_runs` (§5), and the
file/line citations, which are greps.

**Measured:** 2026-09-17, dev cluster, PostgreSQL 17.9.

---

## 1. Decision table

Sizes in MiB. "minimum" is measured by reconstructing the relation into a session-local TEMP
table and rebuilding its real index set (`--rebuild-probe`) — not estimated. `pgstattuple` is
**available but not installed** here; reconstruction is the better instrument anyway, since it
reports the size a rewrite would actually produce rather than an estimate of what it would save.

| store | rows | heap | idx | fill% | minimum | reclaimable | verdict |
|---|---|---|---|---|---|---|---|
| `research_price_daily` | 75,972,667 | 7,050.7 | 2,289.3 | **96.1%** | 9,326.0 ˢ | **14.0 ˢ (0.1%)** | **KEEP — no change** |
| `institutional_holdings` | 8,496,804 | 962.9 | 1,023.0 | 97.9% | 1,730.4 | 255.5 (244.1 of it index) | **KEEP — reindex only** |
| `ownership_institutions_current` | 2,406,606 | 743.0 | 267.4 | 64.0% | 658.8 | 351.5 | **KEEP — reclaim** |
| `ownership_institutions_observations` | 10,874,789 | 5,301.6 | 3,472.9 | **46.1%** | 3,968.9 ᵖ | **4,805.6 ᵖ** ᵃ | **KEEP — fix writer path first, then reclaim** |
| `ownership_funds_observations` | 3,714,185 | 1,777.1 | 763.8 | 63.6% | 1,590.2 ᵖ | 950.7 ᵖ | **KEEP — reclaim** |

- **ᵖ** = projected by the script from ONE directly-measured leaf (`…_2025q4`), at that leaf's
  measured **bytes per row**, never at its fill ratio — leaf fill ranges 41.4%–96.3% across this
  parent, so a ratio extrapolation would inherit whichever leaf was picked. Bytes-per-row is
  still an assumption (that the sampled leaf's row widths and key cardinality are typical) and
  the script labels each such line `[PROJECTION, one leaf, not a measurement of the parent]`.
- **ᵃ** = *as-is* rewrite footprint: the probe rebuilds all four current indexes at default
  settings. It is **not** the footprint after §4.1's index removal and any fillfactor change, and
  the dropped index's size must not be added to it — that index's bloat is already inside the
  4,805.6.
- **ˢ** = sampled. `research_price_daily` is probed on `series_id % 10` (7,440,331 of 75,972,667
  rows, 9.8%), because a full reconstruction is a ~7 GiB TEMP heap plus a 2.3 GiB index build
  against the cluster the dev stack runs on. The modulus keeps whole series together, so row
  width is not distorted by splitting a series — but it is a **systematic cluster sample**, and
  if `series_id` correlates with import cohort (vendor, ingest batch) the projection inherits
  that. No error bound is claimed.

Directly measured leaves, for the two projections above:

| leaf | heap live → min | idx live → min | reclaimable |
|---|---|---|---|
| `ownership_institutions_observations_2025q4` | 888.3 → 376.0 | 517.7 → 198.1 | **831.9** |
| `ownership_funds_observations_2025q4` | 472.2 → 294.5 | 188.6 → 108.7 | **257.6** |

### Nothing is retired

- **No foreign key references any of the five.** That cuts both ways: nothing is held for
  referential reasons, and nothing is orphaned by dropping one. It is **not** evidence of no
  consumers — views, endpoints and runbooks do not appear in `pg_constraint`.
- `research_price_daily` is the backtest corpus (`rg -n '\bresearch_price_daily\b'` → 39 lines
  under `app/`, 79 under `scripts/`). Its heap minimum is **99.9%** of its current heap and its
  PK minimum **99.8%** — i.e. ~0.1% is recoverable. `n_tup_upd = 0`. This is a no-change verdict
  **with evidence**, which is what #3116 asked for when it said not to "rewrite a useful 9 GB
  corpus merely because it is large".
- `institutional_holdings` is labelled legacy (`.claude/skills/data-engineer/SKILL.md:428`) and
  is not read by the ownership rollup post-#905, but it backs
  `GET /instruments/{symbol}/institutional-holdings` (`app/api/instruments.py:4362`, `:4399`)
  **and** it is the source `sync_institutions` mirrors into the observations layer. The
  observations layer is not a superset of it either: **2,357,067 in-window observations have no
  `institutional_holdings` row**, so the two are not interchangeable copies in either direction —
  exactly the distinction #3116 clause 1 asked not to assume away.

---

## 2. Why the largest store's free space is not simply reclaimable

`ownership_institutions_observations` at **46.1% fill** (5,301.6 MiB heap holding 2,445.1 MiB
live) reads as 2,856.5 MiB waiting for a rewrite. Two things complicate that.

**The table is not append-only, despite its own comment.** `sql/114:73` calls it
*"Immutable … Append-only"*. The writer at `app/services/ownership_observations.py:506` is
`ON CONFLICT … DO UPDATE`, and `ownership_observations_sync.sync_institutions` (:383) re-reads
every `institutional_holdings` row inside the 8-quarter retention cap (cutoff `2024-09-30`) and
calls it on each.

**HOT never applies.** Measured: **n_tup_upd = 7,721,662 against n_tup_hot_upd = 0.**

Postgres takes the heap-only path only when *both* no indexed column changes *and* the new tuple
fits on the original page. A zero counter alone does not say which condition failed — but here
the first is provably violated on every update: `idx_institutions_obs_instrument_ingested`
(`sql/119:64`) indexes `(instrument_id, ingested_at DESC)`, and the upsert sets
`ingested_at = clock_timestamp()` unconditionally. So each of those 7.7M updates writes a new
heap tuple *and* a fresh entry in all four indexes (3,472.9 MiB of them).

⚠ `ownership_institutions_current` is a suggestive contrast — same upsert shape, no index over
its `refreshed_at`, and **16,127 of 25,400 updates are HOT (63%)** — but it is not a controlled
comparison: different row widths, different indexes, different write path. It is consistent with
the mechanism, not proof of it.

**And the write volume is almost entirely wasted.** Joining every stored observation back to the
source row it is mirrored from, full population, comparing **every** `DO UPDATE` column with the
writer's own normalisation reproduced (midnight-UTC `filed_at` fallback, blank `voting_authority`
→ NULL):

| | |
|---|---|
| matched source ↔ observation rows | 7,755,802 |
| complete no-ops | **7,754,890 = 99.988%** |
| rows a re-run would genuinely change | **912 — all `filer_name`**, attributed per column |
| coverage: source rows with no observation | 0 |
| coverage: in-window observations with no source row | 2,357,067 |

⚠ `ingested_at` and `ingest_run_id` are excluded from that comparison and named here so their
absence is not silent: `clock_timestamp()` and a fresh UUID per run differ by construction on
every candidate row. **That is precisely why a whole-row `IS DISTINCT FROM` cannot serve as a
guard.**

⚠ The 7,721,662 figure is a **cumulative counter from an unknown epoch**
(`pg_stat_database.stats_reset` is NULL), not a measured per-run delta. It is the right order of
magnitude — section 7 shows 7,755,802 rows carrying an `ingested_at` inside the last 30 days,
which matches the matched-row count exactly — but "7.7M wasted writes per run" is not what was
measured and is not claimed.

**The free space is in use.** Eight partitions took ~0.8–1.4M re-ingests each in the last 7 days,
while `_2024q2` — untouched since 2026-06-30 — sits at **96.3% fill**. PG 17 §24.1.2 says
"`VACUUM` runs are a better approach than infrequent `VACUUM FULL` runs for maintaining
heavily-updated tables", and this relation *is* heavily updated. **So the source rule argues
against reclaiming first**, and it keeps arguing until the write path stops generating churn.

⚠ `_2024q2`'s 96.3% is what a quiesced leaf of this table looks like; it is **not** evidence
that an already-bloated leaf climbs back to 96% on its own. Ordinary `VACUUM` makes space
reusable in place, it does not repack. A quiesced leaf's fill stops *falling*; it does not rise
without inserts. Any reclaim gate must be phrased as "free space has stopped growing", not
"fill has recovered".

---

## 3. Redundant representation — real, and a distant second

Per-column bytes against distinct values, `ownership_institutions_observations` (1,962.2 MiB of
column payload over 10,874,789 rows):

| column | MiB | distinct | note |
|---|---|---|---|
| `filer_name` | 287.6 | 9,587 | denormalised from `institutional_filers` |
| `source_accession` | 217.8 | 72,251 | **byte-identical to `source_document_id` on all 10,874,789 rows** |
| `source_document_id` | 217.8 | 72,251 | in the PK |
| `ingest_run_id` | 165.9 | 7,042 | UUID |
| `source_url` | 153.7 | 32,449 | derivable from CIK + accession |
| `filer_cik` | 114.1 | 9,222 | TEXT |
| `ownership_nature` | 93.3 | **1** | one distinct value |
| `exposure_kind` | 71.6 | 3 | |
| `source` | 41.5 | **1** | one distinct value |
| `source_field`, `period_start`, `known_to` | 0.0 | 0 | already all-NULL |

`ownership_funds_observations` has the same shape plus four columns with exactly one distinct
value each — `ownership_nature`, `source`, `payoff_profile`, `asset_category`, 81.5 MiB between
them, and the last two are CHECK-pinned to that value — and a 239.0 MiB `source_url` at
67.5 B/row.

⚠ Unlike institutions, its `source_document_id` **is** genuinely per-row (differing from
`source_accession` on 3,713,591 of 3,714,185 rows, 99.984%). So the data-engineer skill's
`accession#row_num` description is right for funds and **wrong for institutions**, and the
duplicate-column lever applies to institutions only. The instrument asserts that separation and
fails if the rate drops below 90%, so a partial collapse cannot pass silently.

Measured interning gain — same rows, real index builds, not an estimate:

- `…_observations_2025q4` with accession/CIK/exposure/voting/run interned to integers:
  574.1 → **319.5 MiB** (−44%).
- `institutional_holdings`' unique index with the accession interned: **477.9 → 328.7 MiB** at
  its minimum (680.2 live).

⚠ That interned variant **drops** `filer_name`, `filer_type`, `source_url`, `source`,
`ownership_nature` and the all-NULL columns rather than encoding them. It is therefore an upper
bound on the gain and not an equivalent representation: it excludes the dictionary tables and
their indexes, the reader-side joins, and ID allocation. "Same rows" is not "same information".

**Verdict: DEFER**, recorded with its measurement rather than abandoned — the same disposition
#1620 gave its interning lever. It is a schema migration plus a reader migration across the 78
`app/` lines naming `institutional_holdings` alone, for roughly a third of what §4 is worth at
several times the risk.

---

## 4. Recommended work, in order

**Sequencing is the point.** Reclaiming before the write path is fixed buys space the next sync
spends.

### 4.1 The one finding that unblocks the largest store *(measured; the fix is not designed yet)*

**Measured, by enumerating the intersection rather than asserting it.** The four indexes on
`ownership_institutions_observations` cover
`{instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind,
ingested_at}`. Both writers' `DO UPDATE` set exactly
`{filer_name, filer_type, source_accession, source_field, source_url, filed_at, period_start,
shares, market_value_usd, voting_authority, ingest_run_id, ingested_at}`.

**The intersection is exactly `{ingested_at}` — and it is empty once
`idx_institutions_obs_instrument_ingested` is dropped.** Nothing else the upsert writes is
indexed. So that single 1,459.3 MiB index is the *only* reason the indexed-column half of the
HOT test fails, and removing it is necessary for any fix.

**It is not sufficient**, and the rest is not designed here:

- **HOT also needs room on the original page.** ⚠ `fillfactor = 100` does not mean "no space" and
  does not prevent HOT — a page with reclaimed room still qualifies. Lowering it raises the
  probability; it guarantees nothing and does not repack existing pages. **This spike measured
  neither the current fillfactors nor page-local availability**, so the residual HOT rate after
  the index goes is unknown.
- **HOT does not stop the writes, only the index entries.** Every update still writes a new heap
  tuple. §4.1 attacks the 3,472.9 MiB of index churn; it does not attack the heap churn.

**Why the obvious replacement — a side-table watermark — is not proposed as designed.** The
repair sweep compares `MAX(ingested_at)` per instrument against
`ownership_refresh_state.last_drained_observations_max_ingested_at`. Moving that maximum into a
side table maintained by the ingest path looks like a drop-in, and it is not:

- **A maximum over a mutable set cannot be maintained by upsert alone.** `rewash_filings.py:1574`
  **deletes** observations, including for instruments that receive no replacement. Deleting the
  maximum row lowers the true `MAX`; deleting the last one makes it NULL. Upward-only maintenance
  (`GREATEST`) silently diverges. A monotonic per-instrument *generation counter* answers the
  sweep's actual question ("has anything changed since I drained?") and is deletion-safe, but it
  is a different contract and needs its own decision.
- **`MAX(ingested_at)` has more consumers than the sweep.** `refresh_institutions_current` (:583)
  and `refresh_institutions_current_batch` (:2151) compute it on the ordinary write-through path,
  and they deliberately capture it **before** the MERGE so `_current` and the drained state commit
  together. A watermark read after the fact can acknowledge writes the MERGE never saw. ⚠ An
  earlier draft of this document claimed the index had "exactly one consumer". That was wrong.
- **Atomicity and cutover are unspecified.** Fact write and watermark write must commit and roll
  back together, for every writer (per-filing, bulk drain, rewash, backfill); the side table needs
  the *persisted* timestamp (`RETURNING`), not a second `clock_timestamp()` call, or the
  refreshers record a value that never matches and drift becomes permanent; and existing rows
  need initialising against concurrent traffic.

**So the deliverable here is the constraint list, not the design.** What is established: the
index is the sole indexed-column blocker, it costs 1,459.3 MiB, and it has at least three
consumers that must be re-homed together. What a subsequent ticket owes: the replacement
mechanism, deletion semantics, atomicity, cutover, and #3116 clause 4's before/after on an
isolated copy — none of which a read-only spike can produce.

### 4.2 Then reclaim, per relation

Gate: re-run `--census` and require the active leaves' **free space to have stopped growing**
(not to have shrunk — see §2). Reuse #1620's executed pattern rather than building a second one:
one partition per autocommit statement, scope recomputed each run and never read from a stored
list, refuse to run inside the relevant job's window.

Order by measured value against risk:

1. `ownership_funds_observations` — **950.7 MiB ᵖ**, the least entangled: 67 rows ingested in the
   last 30 days, against 7,755,802 for institutions. ⚠ See §5 — "quiescent" and "stalled" look
   identical from here.
2. `ownership_institutions_current` — **351.5 MiB**, small and self-contained.
3. `ownership_institutions_observations` — **4,805.6 MiB ᵖ**, the prize, gated on §4.1.
4. `institutional_holdings` — **244.1 MiB, index only** (its heap is already within 11.4 MiB of
   minimum). `REINDEX … CONCURRENTLY`, no table rewrite.

⚠ **`lock_timeout` bounds lock *acquisition*, not lock *hold*.** #1620's 2.02 s worst hold was on
a different relation and is not a bound for these leaves. A rewrite also needs transient disk
equal to the relation, and competes for WAL — none of which is sized here.

**Regression checks, before and after each reclaim.** A rewrite reorders rows physically, which
is exactly what the #2127 `DISTINCT ON` tie-break defect was sensitive to, so a row count is not
sufficient:

- `count(*)` per relation identical (necessary, not sufficient — it cannot see a value change);
- the ownership rollup for the golden panel (`AAPL`, `GME`, `MSFT`, `JPM`, `HD`) byte-identical
  through `/instruments/{symbol}/ownership-rollup`;
- `GET /instruments/TSLA/institutional-holdings` non-empty (the #2213 worked example);
- a full-population A/B of any `DISTINCT ON` reader over the rewritten relation, per #1620's
  acceptance, since the panel above is five instruments and the defect class is tie-ordering.

**Rollback:** a rewrite changes no row *value*, so there is no data rollback to perform — but
"therefore safe" would be too strong, because the same operation reorders rows physically and
this document has just named a reader class (`DISTINCT ON` without a total tie-break) whose
output that can change. The exposures are the lock, the transient disk, and tie-ordering; only
the first two have no residue.

### 4.3 What an earlier draft proposed, and why it is NOT recommended

The obvious change is a `WHERE` clause on the `DO UPDATE` so a row is only rewritten when a
payload column moved — on the evidence above it would suppress 99.988% of the writes.

**It is a reversal of a documented decision, not an optimisation.** `sql/119`'s header states the
rationale explicitly:

> *"The repair sweep needs SYSTEM-time so a re-ingest of an unchanged row still bumps the
> watermark and signals `_current` to refresh… `record_*_observation` is updated in the same PR
> to bump `ingested_at` to `clock_timestamp()` on DO UPDATE so every UPSERT advances the
> watermark."*

and `tests/test_sec_manifest.py` pins it per category — the insiders case carries the message
*"ingested_at should bump on every UPSERT (DO UPDATE)"*, and
`test_institution_upsert_bumps_ingested_at` asserts the same `t2 > t1` relation for institutions.
⚠ That test proves only that two sequential upserts produce increasing timestamps; it says
nothing about crash consistency, concurrency or deletion, so it is a weaker guard than the
contract it pins.

⚠ §4.1 and this guard are **not equivalent**, and an earlier draft said they were. §4.1 removes
the *index* churn; the guard would additionally remove the *heap* churn, because a HOT update
still writes a new heap tuple. §4.1 is the part obtainable without revising a documented
contract, which is why it is the recommendation and this is not. If it is ever revisited it needs, at minimum: a
decision on `ingest_run_id` (including it prevents all suppression; excluding it silently
redefines the column from "last run that wrote this row" to "last run that changed it"), an
accounting for `app/runbooks/stream_a_stream_c_gate.py:146`, and a revision of `sql/119` rather
than an additional test.

⚠ It would also be **incomplete**: `sec_13f_dataset_ingest`'s bulk drain has its own
unconditional conflict update, and rewash deletes and re-inserts. Guarding one writer cannot
quiesce the table.

### 4.4 Not proposed

- **Dropping any index *on scan-count evidence*.** (§4.1 does propose removing
  `idx_institutions_obs_instrument_ingested` — but on a mechanism argument, after re-homing its
  consumers, never because its counter looked low.) Four indexes read 0–3 lifetime scans (`idx_inst_current_filer`,
  `idx_inst_obs_filer_period` at 247.3 MiB, `idx_funds_obs_series_period`,
  `idx_funds_obs_filer_period`) and `institutional_holdings_pkey` reads 1 at 184.5 MiB with no FK
  and no reference to `holding_id` outside DDL. #3116 clause 3 is explicit that a low scan count
  cannot establish dispensability; the counters have an unknown epoch; and ⚠⚠ **the audit's own
  section 5 scans some of them** — `idx_holdings_instrument_period` read 0 before a run of this
  tool and 3 after. Separately, #1620 found the prevention log *mandates* a surrogate-PK final
  tie-break for `DISTINCT ON` readers (#2127 Phase 2) and retained an equivalent 708.2 MiB index
  at 0 scans on that basis. ⚠ That mandate requires a stable tie-break *column*; whether it
  requires this standalone B-tree is a separate question this spike did not answer.
- **Installing `pgstattuple`.** Available, not installed; it would decompose reclaimable-versus-
  OS-returned space, which #3117 wants and neither #3115 nor #1620 could supply. One
  `CREATE EXTENSION` — but that is a schema change, which #3116 clause 6 excludes here. Surfaced
  as a WARNING by the instrument rather than left silent.

---

## 5. What this spike did NOT establish

- **Whether `ownership_funds_observations` is quiescent or stalled.** 67 rows in 30 days, last
  ingest 2026-09-13, and no `sec_nport_dataset_ingest` run appears in `job_runs` at all. The
  recency query sees only *surviving* tuples, so it cannot exclude rows written and then deleted,
  nor a failed ingest, nor imminent quarterly work. §4.2's ordering rests on the idleness
  continuing and must be confirmed before that reclaim runs. **Out of scope here; not assumed
  either way.**
- **That `heap − live` is the reusable free space.** `pg_column_size(row)` is a composite, so it
  already carries the tuple header *and* inter-column alignment; what the estimator omits is the
  per-page header and the partly-filled last page. The gap also includes dead tuples still
  visible to an open transaction horizon, and space the freeze horizon has not released. The
  reconstruction probe is the load-bearing measurement; the fill column is only the signpost.
- **That `n_tup_upd` reflects the current cadence.** Unknown stats epoch (§2).
- **Any query-plan evidence.** No `EXPLAIN` was run. Every index statement here is a catalogue
  fact plus a grep, never a plan.
- **Whether the `source_url` asymmetry is a defect.** `source_url IS NULL` on **8,496,804** of
  10,874,789 observations, and that is *exactly* the `institutional_holdings` row count — the
  NULL set is precisely what `sync_institutions` mirrors, because that writer passes
  `source_url=None` while the bulk drain supplies an SEC URL. Whether the bulk drain had
  populated any of those rows first (making this provenance loss) or never touched them (making
  it merely absent) is **not** established by this measurement, and needs a separate look.
