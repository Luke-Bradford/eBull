# `financial_facts_raw` partition + retention

> Status: **2026-05-19 (v4 — post-Codex-1a + 1b + 1c).**
>
> Issue: **#1208 Sub 3.** Branch: `feature/1208-phase3-financial-facts-raw-partition`.
>
> Phase 3 of `docs/superpowers/plans/2026-05-18-backend-stability.md`.
>
> Closure framing: **SCHEMA PRIMITIVE.** Largest blast radius of the epic — converts a 28 GB / 62 M-row hot table to range-partitioned storage and starts enforcing the retention horizons that the data-engineer skill §13 has documented but never enforced.
>
> Codex 1a (v1 → v2) findings summary: 4 BLOCKING + 6 WARNING + 1 NITPICK — all resolved in §8.1.
> Codex 1b (v2 → v3) findings summary: 2 BLOCKING + 4 WARNING — all resolved in §8.2. Biggest correction: `BIGSERIAL`'s `OWNED BY` link means `DROP TABLE` cascades to the sequence; must `ALTER SEQUENCE ... OWNED BY NONE` BEFORE dropping the old table or the migration loses the sequence the new table depends on.
> Codex 1c (v3 → v4) findings summary: 1 BLOCKING + 4 WARNING + 2 OK validations — all resolved in §8.3. Biggest correction: INSERT-SELECT runs with no explicit write lock on old table; late writes between snapshot start and DROP TABLE silently disappear. Pre-step adds `LOCK TABLE financial_facts_raw IN SHARE MODE`.

## 1. Problem

`financial_facts_raw` is the XBRL fact landing table (sql/032). It accumulates every numeric fact from every SEC `companyfacts.json` ingest — `instrument_id × concept × unit × period × accession` rows. As of Phase 2 close (PR #1213 SHA `efff62f`) the dev cluster shape is:

```text
$ docker exec ebull-postgres psql -U postgres -d ebull -tAc "SELECT pg_size_pretty(pg_database_size('ebull'));"
46 GB

$ ... pg_size_pretty(pg_total_relation_size('financial_facts_raw')), count(*) ...
28 GB | 62,067,023
```

No partition. No retention sweep. Autovacuum runs on the whole 28 GB heap when dead-row thresholds trip — and that single-table burst is the WAL-PANIC root cause documented in `docs/review-prevention-log.md` §"Postgres on Docker Desktop macOS — defaults blow up partition-heavy workloads". Phase 1 tuned `max_wal_size` to absorb the burst; Phase 3 dismantles the burst at source by partitioning by `period_end` quarter so autovacuum operates per-partition (peak 380 MB) instead of per-heap (28 GB).

Retention horizons documented at `.claude/skills/data-engineer/SKILL.md` §13 (10-K = last 3 annual filings, 10-Q = last 8 quarterly filings) currently apply only at the **discovery layer** (manifest filter `filed_at >= cutoff`). Once a fact lands in `financial_facts_raw` it stays forever. The 62 M-row count is the cumulative residue of every ingest run since sql/032 landed.

Closure framing dictates two deliverables in one PR: partition shape + retention sweep service. Either alone is half a fix — partitioning without retention leaves the heap growing forever; retention without partitioning means the autovacuum on the deletes itself triggers the WAL burst.

## 2. Spike receipts (2026-05-19 dev cluster)

### 2.1 Quarter histogram

`SELECT date_trunc('quarter', period_end) ... GROUP BY 1` returned 202 quarters across `min(period_end)=1967-05-25` to `max(period_end)=6016-06-30`. **1055 rows** have `period_end` outside the realistic `[1900-01-01, 2099-12-31]` window — XBRL parser detritus (truncated date strings parsed as far-future). The 68 realistic quarters between `2010-Q1` and `2026-Q4` cover essentially the whole row volume:

| Bucket | Rows | Size |
|---|---|---|
| 2010-Q1 | 119,863 | 20 MB |
| 2011-Q4 (peak ramp) | 1,107,545 | 187 MB |
| 2015-Q4 | 1,412,818 | 239 MB |
| 2020-Q4 | 1,891,999 | 319 MB |
| 2022-Q4 | 2,187,645 | 369 MB |
| 2023-Q4 (peak) | 2,253,266 | 380 MB |
| 2024-Q4 | 2,019,586 | 339 MB |
| 2026-Q1 (in-flight) | 376,978 | 66 MB |

Worst-case quarterly partition ≈ **380 MB**. Autovacuum on a 380 MB partition under tuned `max_wal_size=4 GB` cannot trigger the WAL PANIC pattern Phase 1 observed against the 28 GB heap.

### 2.2 Junk rows (out-of-window `period_end`)

```text
SELECT count(*) FILTER (WHERE period_end < '1900-01-01' OR period_end > '2099-12-31')
  FROM financial_facts_raw;
-- 1055
```

These are not "future filings" — `max(period_end) = 6016-06-30` is a parser bug (likely a year-field overflow from a 4-digit truncation). They cannot live in a calendar-quarter partition (the partition for `6016-Q3` would be absurd). Two options:

1. **Drop during migration.** Filter `WHERE period_end BETWEEN '1900-01-01' AND '2099-12-31'` during the INSERT-SELECT swap. Junk evaporates. Risk: hides the parser bug from anyone re-running the ingest later.
2. **Keep in DEFAULT partition.** PARTITION BY RANGE with a DEFAULT partition catches everything outside declared ranges. Junk gets siloed but stays queryable for triage.

**Decision: option 2 (DEFAULT partition).** The 1055 rows fit easily (<200 kB total). Keeping them lets a follow-up ticket (filed at merge time) inspect the rows and patch the XBRL parser. Dropping them silently loses evidence.

### 2.3 Index inventory

```text
financial_facts_raw_pkey               PRIMARY KEY (fact_id)
idx_facts_raw_instrument_concept       btree (instrument_id, concept, period_end DESC)
uq_facts_raw_identity                  UNIQUE btree (instrument_id, concept, unit,
                                                    COALESCE(period_start, '0001-01-01'::date),
                                                    period_end, accession_number)
```

### 2.4 FK + view fan-in

```text
financial_facts_raw → instruments (instrument_id)
financial_facts_raw → data_ingestion_runs (ingestion_run_id)

inbound FKs targeting financial_facts_raw: NONE
  (verified: SELECT conname FROM pg_constraint WHERE confrelid='financial_facts_raw'::regclass; → 0 rows)

views depending on financial_facts_raw (direct):
  - share_count_history                        (sql/052)
views depending transitively (via share_count_history):
  - instrument_dilution_summary                (sql/052)
  - instrument_share_count_latest              (sql/052)
```

Zero inbound FKs is a hard precondition for the migration. Three view dependencies (Codex 1a BLOCKING #1 surfaced these — the original spec missed them) must be dropped before the table swap and re-created verbatim from `sql/052` after.

### 2.5 Reader/writer call sites

```text
app/services/fundamentals.py:374           INSERT ... ON CONFLICT (identity) DO UPDATE
app/services/fundamentals.py:1646          SELECT DISTINCT instrument_id FROM financial_facts_raw
app/services/fundamentals.py:1661          SELECT concept, unit, period_start, period_end, val ...
                                             WHERE instrument_id = %s
app/api/instruments.py:1114                SELECT period_end, val, accession_number ...
                                             WHERE instrument_id = %s AND concept = 'EntityNumberOfEmployees'
                                             ORDER BY period_end DESC, fetched_at DESC LIMIT 1
app/services/sec_companyfacts_ingest.py    upserts via fundamentals.upsert_facts_for_instrument
app/services/ownership_observations_sync.py reads at L493 (annotation only)
app/services/ownership_rollup.py:1175      reads (re-query for accession + form_type)
```

Every reader filters by `instrument_id` first. None filter by `period_end` range, so partition pruning gains are operational (autovacuum, vacuum-full, pg_dump shape) rather than query-perf. That's fine — the goal is the autovacuum-blast-radius reduction.

Critical: the upsert path's `ON CONFLICT (instrument_id, concept, unit, COALESCE(period_start, '0001-01-01'::date), period_end, accession_number)` clause must continue to fire. Postgres requires the conflict target match a UNIQUE constraint **declared on the partitioned parent**, and a UNIQUE constraint on a partitioned table must include the partition key. The identity already includes `period_end`. ✓

## 3. Scope

| Task | Deliverable | Closure framing |
|---|---|---|
| T1 | `sql/156_financial_facts_raw_partition.sql` — convert to `PARTITION BY RANGE (period_end)` via fresh-table swap-rename + drop/re-create dependent views | SCHEMA PRIMITIVE |
| T2 | `app/services/financial_facts_retention.py` — service-no-commit retention sweep enforcing §13 horizons (10-K family + 10-Q family inc. amendments) | SERVICE PRIMITIVE |
| T3 | `JOB_FINANCIAL_FACTS_RETENTION_SWEEP` `ScheduledJob` at 02:45 UTC + `_INVOKERS` entry | OPS PRIMITIVE |
| T4 | `tests/test_financial_facts_raw_partition.py` + `tests/test_financial_facts_retention.py` + `tests/test_migration_156_partition_swap.py` + `tests/test_share_count_history_views_post_swap.py` | TEST PRIMITIVE |
| T5 | `docs/review-prevention-log.md` — append "Unpartitioned mega-table autovacuum bursts WAL"; cross-link to Phase 1 entry. `.claude/skills/data-engineer/SKILL.md` §13 — add row-storage enforcement note pointing at the sweep service | DOCS PRIMITIVE |

## 4. Design

### 4.1 Partition shape

```sql
CREATE TABLE financial_facts_raw_new (
    fact_id          BIGINT NOT NULL DEFAULT nextval('financial_facts_raw_fact_id_seq'),
    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id),
    taxonomy         TEXT NOT NULL DEFAULT 'us-gaap',
    concept          TEXT NOT NULL,
    unit             TEXT NOT NULL,
    period_start     DATE,
    period_end       DATE NOT NULL,
    val              NUMERIC(30,6) NOT NULL,
    frame            TEXT,
    accession_number TEXT NOT NULL,
    form_type        TEXT NOT NULL,
    filed_date       DATE NOT NULL,
    fiscal_year      INTEGER,
    fiscal_period    TEXT,
    decimals         TEXT,
    ingestion_run_id BIGINT REFERENCES data_ingestion_runs(ingestion_run_id),
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fact_id, period_end)   -- partition key MUST be in PK
) PARTITION BY RANGE (period_end);
```

**Quarterly buckets:** `financial_facts_raw_pre2010` (`'1900-01-01'` → `'2010-01-01'`) + 84 quarterly partitions covering 2010-Q1 → 2030-Q4 + `financial_facts_raw_default` DEFAULT.

> **v2 change (Codex 1a WARNING #1):** pre2010's lower bound is `'1900-01-01'` not `MINVALUE`. Anything before 1900 (parser junk) lands in DEFAULT alongside far-future rows like `6016-06-30`. This keeps DEFAULT semantically as "out-of-window junk" rather than mixing two failure modes.

84 quarters × 380 MB peak partition ≈ 32 GB of partition headroom — fits the projected post-retention working set without ever falling back to the DEFAULT partition during normal ingest. The DEFAULT partition exists only to absorb the 1055 junk rows + any far-future parser detritus.

#### 4.1.1 DEFAULT-partition growth alarm

DEFAULT is a parking lot for parser bugs, not a normal storage destination. A test in `tests/test_financial_facts_raw_partition.py::test_default_partition_growth_alarm` asserts the DEFAULT partition's row count is `< 5000` (current count: 1055 + headroom). Crossing the threshold means either:

- A real ingest is hitting the parser bug at scale (operator should investigate).
- The 2030-Q4 upper boundary has been reached and we need a new round of quarterly partitions added.

The test fires nightly via the regular pytest cron + would fail the next push if DEFAULT growth went unmanaged. Cross-phase coupling note: Phase 4 (`/system/postgres-health`) will surface the count for live operator visibility; Phase 3 only adds the test gate.

### 4.2 Primary key — `(fact_id, period_end)`

Existing PK is `(fact_id)`. Range-partitioning requires the partition key to appear in every UNIQUE constraint, so the PK becomes the composite `(fact_id, period_end)`. The `fact_id` column itself stays globally unique (because `period_end` for a given fact never changes — facts are append-only, idempotent on the identity tuple), but Postgres can no longer enforce that uniqueness across partitions. We accept this — `fact_id` is not referenced by FK from any other table (§2.4) and is not used as a join key in any reader (§2.5); it's purely a stable per-row identifier for `RETURNING` and ad-hoc debugging.

If a future FK needs to reference `financial_facts_raw.fact_id`, the design must change. Captured in §7.2 as "out of scope" with a follow-up issue trigger.

### 4.3 Sequence reuse

The old table's `BIGSERIAL` owns sequence `financial_facts_raw_fact_id_seq`. The new table declares `fact_id BIGINT NOT NULL DEFAULT nextval('financial_facts_raw_fact_id_seq')` (not `BIGSERIAL`) so the same sequence is reused. After the rename, `ALTER SEQUENCE financial_facts_raw_fact_id_seq OWNED BY financial_facts_raw.fact_id` re-attaches ownership to the new table. App-level inserts continue to use `RETURNING fact_id` without code change.

### 4.4 Migration body (no explicit BEGIN/COMMIT)

The migration runner (`app/db/migrations.py:131-149`) wraps every non-autocommit migration in `with psycopg.connect(...) as conn: ...; conn.commit()`. Body + `INSERT INTO schema_migrations` happen in the runner's single transaction — atomic. **The migration file MUST NOT include explicit `BEGIN` / `COMMIT`** (Codex 1a WARNING #3 fix: Phase 1's autocommit migration is the exception; this one is a normal tx-bound migration).

> **v2 reorder (Codex 1a WARNINGs #2 + #6):** indexes are built **after** the INSERT-SELECT, not before. Building indexes on an empty table then populating drags every INSERT through three btree maintenance paths; building them once on the populated table is faster, smaller in WAL, and avoids the multiplicative WAL burst the partition migration is supposed to prevent.
>
> **v3 reorder (Codex 1b BLOCKING #1):** `BIGSERIAL` on the old table created a sequence `OWNED BY financial_facts_raw.fact_id`. `DROP TABLE` cascades to OWNED-BY sequences. So step 5 (DROP TABLE old) would drop `financial_facts_raw_fact_id_seq` — which the new table's `DEFAULT nextval(...)` depends on. Pre-step: `ALTER SEQUENCE financial_facts_raw_fact_id_seq OWNED BY NONE` to detach before the drop.
>
> **v3 add (Codex 1b WARNING #4):** PostgreSQL does NOT auto-rename the PK constraint's implicit index on `ALTER TABLE ... RENAME`. The PK index stays `financial_facts_raw_new_pkey` post-rename unless we explicitly rename it. Add to the index-rename block.
>
> **v3 add (Codex 1b WARNING #6):** ranking CTE scans every swept-form row per instrument. Wider retention index `(instrument_id, form_type, accession_number, filed_date)` covers both the ranking lookup AND the DELETE join — single index instead of two.

```sql
-- 1. Create the partitioned skeleton + leaf partitions (no indexes yet).
CREATE TABLE financial_facts_raw_new (
    ...,
    PRIMARY KEY (fact_id, period_end)
) PARTITION BY RANGE (period_end);

CREATE TABLE financial_facts_raw_pre2010
    PARTITION OF financial_facts_raw_new
    FOR VALUES FROM ('1900-01-01') TO ('2010-01-01');

CREATE TABLE financial_facts_raw_2010q1
    PARTITION OF financial_facts_raw_new
    FOR VALUES FROM ('2010-01-01') TO ('2010-04-01');
-- ... 83 more quarterly partitions through 2030-Q4 ...

CREATE TABLE financial_facts_raw_default
    PARTITION OF financial_facts_raw_new DEFAULT;

-- 2a. Block concurrent writes to the old table for the duration of
--     the migration. SHARE MODE allows concurrent reads but blocks
--     INSERT/UPDATE/DELETE, so late writes cannot slip in between the
--     INSERT-SELECT snapshot and the DROP TABLE in step 7. Released
--     automatically at commit/rollback of the runner's tx.
LOCK TABLE financial_facts_raw IN SHARE MODE;

-- 2b. Bulk copy.
INSERT INTO financial_facts_raw_new
SELECT * FROM financial_facts_raw;

-- 3. Build indexes on the partitioned parent (cascades to every leaf).
--    Names use `_new` suffix to avoid global-namespace collision with the
--    old table's indexes which still exist; renamed to canonical names
--    AFTER the old table is dropped (step 7).
CREATE UNIQUE INDEX uq_facts_raw_identity_new ON financial_facts_raw_new (
    instrument_id, concept, unit,
    COALESCE(period_start, '0001-01-01'::date),
    period_end, accession_number
);
CREATE INDEX idx_facts_raw_instrument_concept_new
    ON financial_facts_raw_new (instrument_id, concept, period_end DESC);

-- 4. Retention indexes. TWO indexes (Codex 1c WARNING #3):
--    - retention_ranking: covers the ranking CTE's (instrument_id,
--      form_type IN (...)) filter via prefix match.
--    - retention_evict: covers the DELETE join's
--      (instrument_id, accession_number) lookup. The wider retention
--      index alone leaves form_type as a "gap" so the DELETE has no
--      usable prefix beyond instrument_id.
CREATE INDEX idx_facts_raw_retention_ranking_new
    ON financial_facts_raw_new (instrument_id, form_type, accession_number, filed_date);
CREATE INDEX idx_facts_raw_retention_evict_new
    ON financial_facts_raw_new (instrument_id, accession_number);

-- 5. Drop dependent views (reverse dependency order).
DROP VIEW IF EXISTS instrument_dilution_summary;
DROP VIEW IF EXISTS instrument_share_count_latest;
DROP VIEW IF EXISTS share_count_history;

-- 6. Detach sequence ownership BEFORE dropping the old table.
--    BIGSERIAL on the old `fact_id` column made the sequence OWNED BY
--    the column → DROP TABLE would cascade to the sequence and the new
--    table's DEFAULT nextval(...) would break.
ALTER SEQUENCE financial_facts_raw_fact_id_seq OWNED BY NONE;

-- 7. Drop old table + its indexes.
DROP TABLE financial_facts_raw;

-- 8. Rename new table + indexes to canonical names.
ALTER TABLE financial_facts_raw_new RENAME TO financial_facts_raw;
ALTER INDEX financial_facts_raw_new_pkey       RENAME TO financial_facts_raw_pkey;
ALTER INDEX uq_facts_raw_identity_new          RENAME TO uq_facts_raw_identity;
ALTER INDEX idx_facts_raw_instrument_concept_new RENAME TO idx_facts_raw_instrument_concept;
ALTER INDEX idx_facts_raw_retention_ranking_new RENAME TO idx_facts_raw_retention_ranking;
ALTER INDEX idx_facts_raw_retention_evict_new   RENAME TO idx_facts_raw_retention_evict;

-- 9. Re-attach sequence ownership to the renamed table.
ALTER SEQUENCE financial_facts_raw_fact_id_seq
    OWNED BY financial_facts_raw.fact_id;

-- 10. Re-create dependent views VERBATIM from sql/052.
--     (Body pasted into 156 so the migration is self-contained.)
CREATE OR REPLACE VIEW share_count_history AS ...;
CREATE OR REPLACE VIEW instrument_dilution_summary AS ...;
CREATE OR REPLACE VIEW instrument_share_count_latest AS ...;
```

**Lock window (Codex 1c WARNING #2):** step 2a's `LOCK TABLE financial_facts_raw IN SHARE MODE` is the safety gate — it blocks every concurrent INSERT/UPDATE/DELETE on the old table for the full duration of the runner's tx. Step 2b's INSERT-SELECT then runs with a stable snapshot of every committed row. Concurrent SELECTs are permitted; SHARE MODE is incompatible with `ROW EXCLUSIVE` (writes) but compatible with `ACCESS SHARE` (reads). Step 7's DROP TABLE upgrades the lock to `ACCESS EXCLUSIVE` for the actual drop, also held until commit.

Two consequences:

1. **App must be down during the migration.** Migration runs in `app/main.py` lifespan startup BEFORE the HTTP server accepts traffic. Per Phase 1's API-first migration contract (`app/jobs/__main__.py` doesn't call `run_migrations()` since #719), only `app.main` applies migrations. Jobs worker must be stopped before app boot to avoid late writes through the jobs-side pool.
2. **Long step 2.** On the 28 GB / 62 M-row dev cluster, INSERT-SELECT will take O(15-30 min) under tuned `maintenance_work_mem=512 MB`. Test DBs (slim template) finish in milliseconds — the migration is no-op for empty tables.

The operator runbook in §7.1 records the dev-DB downtime window and the sequencing.

### 4.5 Retention sweep semantics

`financial_facts_raw` is the raw fact stream. Retention semantics from skill §13:

| form_type family | Keep |
|---|---|
| `10-K`, `10-K/A` | latest 3 distinct accession_numbers per instrument_id (combined family) |
| `10-Q`, `10-Q/A` | latest 8 distinct accession_numbers per instrument_id (combined family) |
| other (8-K, DEF 14A, 13F-HR, ...) | NOT swept — each source has its own horizon enforced at discovery layer |

> **v2 change (Codex 1a BLOCKING #4):** amendments (`10-K/A`, `10-Q/A`) join their respective families. Per skill §13 (and standard XBRL practice) an amendment supersedes the original, so they share the "last N filings" budget.

The sweep runs as a daily idempotent job. First run on dev will delete the bulk of historical 10-K + 10-Q residue — projected drop **62 M → ~35 M rows** based on universe size × per-instrument horizon math:

```text
12,000 US instruments × (3 × ~400 10-K facts + 8 × ~200 10-Q facts)
= 12,000 × (1,200 + 1,600) = ~34 M facts post-sweep
```

Post-sweep `financial_facts_raw` size projection: ~16 GB. Combined with Phase 1 + Phase 2 cleanup the dev DB should land **< 25 GB** (epic acceptance §8.3 targets `< 5 GB` — that's not achievable in this phase without also evicting `filing_raw_documents` 2.9 GB + `filing_events` 4.3 GB; deferred to a Phase 6/7 not currently scoped).

Spec accepts this as a **revised target: `< 25 GB` post-Phase-3**; the original `< 5 GB` aspiration is captured in §7.2 as deferred follow-up.

#### 4.5.1 Implementation

> **v2 rewrite (Codex 1a BLOCKING #3):** the v1 SQL ranked rows of `financial_facts_raw`, not distinct accessions — would have evicted facts 4..N of the latest 10-K and kept only the first 3 facts. The v2 shape ranks accessions then deletes all facts of out-of-horizon accessions.

```python
# app/services/financial_facts_retention.py
KEEP_10K = 3
KEEP_10Q = 8

ANNUAL_FORMS = frozenset({"10-K", "10-K/A"})
QUARTERLY_FORMS = frozenset({"10-Q", "10-Q/A"})
SWEPT_FORMS = ANNUAL_FORMS | QUARTERLY_FORMS

def sweep_retention_for_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    keep_10k: int = KEEP_10K,
    keep_10q: int = KEEP_10Q,
) -> int:
    """Delete `financial_facts_raw` rows whose accession is outside the
    per-form-family retention horizon. Returns deleted row count.

    Service-no-commit contract: caller owns the transaction.
    """
```

Per-instrument DELETE (single statement):

```sql
-- v3 (Codex 1b WARNING #3): GROUP BY (instrument_id, accession_number)
-- collapses duplicate metadata rows for the same accession to one
-- ranking entry. MAX(filed_date) picks the latest reported filed_date
-- if a parser bug ever emitted divergent values for the same accession.
WITH distinct_accessions AS (
    SELECT instrument_id,
           accession_number,
           MAX(filed_date) AS filed_date,
           MAX(CASE WHEN form_type IN ('10-K', '10-K/A') THEN 'ANNUAL'
                    WHEN form_type IN ('10-Q', '10-Q/A') THEN 'QUARTERLY'
               END) AS family
    FROM financial_facts_raw
    WHERE instrument_id = %(iid)s
      AND form_type IN ('10-K', '10-K/A', '10-Q', '10-Q/A')
    GROUP BY instrument_id, accession_number
),
ranked AS (
    SELECT instrument_id, accession_number, family,
           ROW_NUMBER() OVER (
             PARTITION BY instrument_id, family
             ORDER BY filed_date DESC, accession_number DESC
           ) AS rn
    FROM distinct_accessions
),
to_evict AS (
    SELECT instrument_id, accession_number
    FROM ranked
    WHERE (family = 'ANNUAL'    AND rn > %(keep_10k)s)
       OR (family = 'QUARTERLY' AND rn > %(keep_10q)s)
)
DELETE FROM financial_facts_raw f
USING to_evict e
WHERE f.instrument_id    = e.instrument_id
  AND f.accession_number = e.accession_number;
```

The ranking CTE uses `idx_facts_raw_retention_ranking (instrument_id, form_type, accession_number, filed_date)` for the per-instrument scan with form_type filter; the DELETE uses `idx_facts_raw_retention_evict (instrument_id, accession_number)` for the join. **Partition pruning does NOT apply** — neither query carries a `period_end` predicate, so the planner probes all 86 leaves. That's acceptable because per-instrument the touched leaves are bounded (a single instrument's facts cluster across 8-16 partitions worth of period_ends) and the (instrument_id, ...) index prefix makes each leaf probe ~constant-time (Codex 1c WARNING #4 — explicit acknowledgement: bounded by indexes + per-instrument tx, not by partition pruning).

Service-no-commit invariant: `sweep_retention_for_instrument` does NOT enter `with conn.transaction()`. The orchestrator iterates instruments and commits per instrument. **Codex 1b BLOCKING #2:** in default psycopg conn mode an implicit txn opens on the first SELECT, making `with conn.transaction()` a savepoint not a real commit — so the orchestrator MUST open an `autocommit=True` conn so each `with conn.transaction()` is a fresh top-level tx:

```python
def sweep_retention_all_instruments(
    *,
    database_url: str | None = None,
) -> RetentionSummary:
    """Open a dedicated autocommit conn, iterate every instrument with
    10-K/10-Q facts, sweep each in its own real top-level transaction.

    autocommit=True is non-negotiable here. See Codex 1b BLOCKING #2:
    in default conn mode the first SELECT opens an implicit tx, then
    every `with conn.transaction()` becomes a SAVEPOINT inside that
    one tx — the 12k-instrument loop would commit exactly once, on
    function exit, defeating per-instrument WAL-bounding.
    """
    url = database_url or settings.database_url
    with psycopg.connect(url, autocommit=True) as conn:
        cur = conn.execute(
            "SELECT DISTINCT instrument_id FROM financial_facts_raw "
            "WHERE form_type IN ('10-K','10-K/A','10-Q','10-Q/A')"
        )
        iids = [r[0] for r in cur.fetchall()]
        total_deleted = 0
        for iid in iids:
            with conn.transaction():   # fresh top-level tx per instrument
                total_deleted += sweep_retention_for_instrument(conn, instrument_id=iid)
        return RetentionSummary(instruments=len(iids), rows_deleted=total_deleted)
```

### 4.6 Scheduled job

```python
JOB_FINANCIAL_FACTS_RETENTION_SWEEP = "financial_facts_retention_sweep"

ScheduledJob(
    name=JOB_FINANCIAL_FACTS_RETENTION_SWEEP,
    display_name="Financial facts retention sweep",
    source="db",
    description=(
        "Evicts financial_facts_raw rows outside §13 retention horizons "
        "(10-K family = last 3 annual filings, 10-Q family = last 8 "
        "quarterly filings, amendments share the family budget). "
        "Idempotent — no-op on a swept DB. Required to bound the table "
        "footprint after the #1208 Phase 3 partition migration."
    ),
    cadence=Cadence.daily(hour=2, minute=45),  # 02:45 UTC: post raw_data_retention_sweep (02:00) + pre orchestrator_full_sync (03:00)
    prerequisite=_bootstrap_complete,  # no-op on an empty DB
    catch_up_on_boot=False,
)
```

`_INVOKERS[JOB_FINANCIAL_FACTS_RETENTION_SWEEP]` dispatches to `sweep_retention_all_instruments` via a thin wrapper that opens a fresh pool conn.

## 5. Tests

### 5.1 `tests/test_financial_facts_raw_partition.py`

Targets — read against the test template (migration 156 already applied):

- `\d+ financial_facts_raw` reports `Partitioned table` and `pg_partitioned_table` has a row with `partstrat='r'` and `partattrs` referencing `period_end`.
- `pg_inherits` shows exactly 86 leaf partitions (1 pre2010 + 84 quarterly + 1 default).
- An INSERT with `period_end='2025-08-15'` routes to `financial_facts_raw_2025q3` (verified via `tableoid::regclass`).
- An INSERT with `period_end='6016-06-30'` routes to `financial_facts_raw_default`.
- An INSERT with `period_end='1850-01-01'` routes to `financial_facts_raw_default` (Codex 1a WARNING #1 — pre-1900 junk goes to DEFAULT, not pre2010).
- An INSERT with `period_end='2008-03-15'` routes to `financial_facts_raw_pre2010`.
- `ON CONFLICT (identity)` upsert still fires across partitions (same conflict target → UPDATE-in-place).
- Each canonical partitioned index (`financial_facts_raw_pkey`, `uq_facts_raw_identity`, `idx_facts_raw_instrument_concept`, `idx_facts_raw_retention_ranking`, `idx_facts_raw_retention_evict`) exists on the parent. Each leaf partition has an attached child index — verified by joining `pg_inherits` against `pg_index` and asserting one child per leaf per parent index (Codex 1c WARNING #5 — assert parent + child attachments, not literal "every-leaf-has-canonical-name").
- `test_default_partition_growth_alarm` — assert `SELECT count(*) FROM financial_facts_raw_default < 5000`.

### 5.2 `tests/test_financial_facts_retention.py`

Targets `app.services.financial_facts_retention.sweep_retention_for_instrument`:

- Seed 5 distinct 10-K accessions for one instrument across 5 distinct `filed_date`s, each accession contributing 12 facts → sweep keeps facts for the 3 latest accessions only. **Asserts 5×12 → 3×12** (not 3 facts total — this is the Codex 1a BLOCKING #3 regression test).
- Seed 10 distinct 10-Q accessions for one instrument, each contributing 8 facts → sweep keeps 8 accessions × 8 facts = 64 facts.
- Seed a mix of 10-K and 10-K/A for one instrument: 2 10-K + 2 10-K/A → 4 distinct accessions in ANNUAL family → sweep keeps 3 (Codex 1a BLOCKING #4 regression).
- Seed a mix of 10-Q and 10-Q/A: 5 10-Q + 5 10-Q/A → 10 distinct accessions in QUARTERLY family → sweep keeps 8.
- Seed 2 8-K facts for one instrument → sweep leaves both intact (form_type not in swept set).
- Service-no-commit invariant: assert `sweep_retention_for_instrument` does NOT call `conn.commit()` and works inside a caller-owned `with conn.transaction()` block.
- Idempotency: running sweep twice in a row deletes 0 rows on the second pass.

### 5.3 `tests/test_migration_156_partition_swap.py`

The migration runner has no helper to apply migrations up to a specific version. **Phase 3 adds `apply_migrations_through(conn, *, max_filename: str) -> list[str]`** in `app/db/migrations.py` — a thin wrapper around the existing loop that stops once `path.name > max_filename`. This is the **only new code in `migrations.py`** Phase 3 introduces.

Test:

- Build a fresh empty DB, apply through `155_postgres_runtime_tuning.sql` (skipping migration 156).
- Seed `financial_facts_raw` (un-partitioned shape from sql/032) with 50 rows spanning 4 distinct `period_end` quarters + 1 row at year `6016-06-30` + 1 row at `1850-01-01`.
- Apply migration 156.
- Assert: row count preserved (52 rows total).
- Assert: 50 quarterly rows land in their respective `*q[1-4]` partitions; the 6016 row in DEFAULT; the 1850 row in DEFAULT (NOT pre2010).
- Assert: `pg_class.relname = 'financial_facts_raw_old'` does NOT exist (cleanup successful).
- Assert: `share_count_history`, `instrument_dilution_summary`, `instrument_share_count_latest` exist + return rows from the freshly partitioned table (Codex 1a BLOCKING #1 regression — views re-created after swap).
- Assert: `uq_facts_raw_identity`, `idx_facts_raw_instrument_concept`, `idx_facts_raw_retention_ranking`, `idx_facts_raw_retention_evict`, `financial_facts_raw_pkey` all exist on the partitioned parent with the canonical names (no `_new` residue).
- Assert: sequence `financial_facts_raw_fact_id_seq` exists, is `OWNED BY` `financial_facts_raw.fact_id` (verify via `pg_depend`), and `nextval()` returns a positive value `> max(fact_id)` from the seeded data (Codex 1b WARNING #5 — explicit nextval test, not sequence-state introspection).

### 5.4 `tests/test_share_count_history_views_post_swap.py`

Functional regression: seed 5 `financial_facts_raw` rows for `CommonStockSharesOutstanding` across 5 quarters; verify `share_count_history` returns one row per quarter; verify `instrument_share_count_latest` picks the newest. Runs against the test template (post-migration 156) — guarantees the re-created views are byte-identical in behaviour to the sql/052 originals.

## 6. Risk + rollback

### 6.1 Migration failure modes

| Failure | Detection | Recovery |
|---|---|---|
| INSERT-SELECT runs out of `maintenance_work_mem` during a sort | PG logs `out of memory` | Reduce `maintenance_work_mem` for the session, restart migration. Phase 1's 512 MB headroom should suffice. |
| Disk fills during INSERT-SELECT (28 GB → 56 GB transient peak) | PG `disk full` | Pre-flight: check `df -h` on the volume reports ≥ 60 GB free. Document in operator runbook. |
| Sequence already advanced past `max(fact_id)` | Sequence is reused; no `setval()` call; no risk | N/A |
| Power loss mid-tx | Runner-owned single tx → Postgres rolls back automatically on recovery | Re-apply migration 156 on next boot. |
| Index creation fails (OOM) | `CREATE INDEX` failure within the same tx aborts the migration | Same as above — single tx atomic. |
| View body in migration 156 drifts from sql/052 over time | Phase 3 introduces a test gate: `test_share_count_history_views_post_swap.py` verifies behaviour. If sql/052 is later edited without bumping migration 156's embedded copy, the test catches the drift. | Tighten the test if drift surfaces. |

### 6.2 Reader compat

All call sites (§2.5) issue queries that work transparently on partitioned tables — Postgres planner handles partition pruning + parent-table dispatch. The ON CONFLICT upsert path uses the identity tuple which includes `period_end` (partition key) so the conflict constraint is valid on the partitioned parent.

No reader code change required. Smoke test post-migration:

```text
curl http://localhost:8000/instruments/AAPL/employees   # exercises api/instruments.py:1114
curl http://localhost:8000/instruments/AAPL/fundamentals  # exercises normalize pipeline
```

### 6.3 Rollback

If migration 156 lands on dev + the operator observes a regression, rollback is **non-trivial** — the old un-partitioned table has been dropped. Two options:

1. **Forward fix.** Write migration 157 to swap the partitioned table back to un-partitioned shape. Same swap-rename pattern in reverse. Expensive but mechanical.
2. **Restore from backup.** Phase 3 deliberately leaves `financial_facts_raw_old` dropped in the migration — see §4.4 step 5. If we wanted reversibility we'd defer the DROP TABLE to migration 157 + observe one day of production behaviour. **Decision: keep step 5 in the migration.** Dev DB only; full reset is `make reset-db` (drops + re-migrates). Production is out of scope for #1208.

The spec accepts irreversibility within Phase 3's scope because the only environment running this migration is dev. When the production rollout happens (separate epic), the operator runbook will defer step 5 to a follow-up migration with a one-day delay.

## 7. Operator runbook

### 7.1 Apply on dev

```bash
# 1. Stop jobs worker so writes don't race the migration
pkill -f 'python -m app.jobs'

# 2. Pre-flight disk check (need 60 GB free for the 28 GB → 56 GB transient peak)
df -h /var/lib/docker  # or wherever the PG data volume is mounted

# 3. Boot the app — migration 156 runs in lifespan startup
uv run python -m app.main

# 4. Watch the migration log line
#    "Applying migration: 156_financial_facts_raw_partition.sql"
#    expect 15-30 min on the 28 GB dev cluster

# 5. Verify partition shape
docker exec ebull-postgres psql -U postgres -d ebull -c "\d+ financial_facts_raw" | head -30

# 6. Verify row count preserved
docker exec ebull-postgres psql -U postgres -d ebull -tAc \
  "SELECT count(*) FROM financial_facts_raw;"
# expect ≈ 62,067,023 (within drift from concurrent activity if any leaked)

# 7. Verify dependent views work
docker exec ebull-postgres psql -U postgres -d ebull -tAc \
  "SELECT count(*) FROM share_count_history;"
# expect non-zero
docker exec ebull-postgres psql -U postgres -d ebull -tAc \
  "SELECT count(*) FROM instrument_dilution_summary;"
# expect non-zero

# 8. Boot jobs worker
uv run python -m app.jobs &

# 9. Trigger one retention sweep run manually to seed the post-migration shape
curl -X POST http://localhost:8000/admin/jobs/run \
  -H 'Content-Type: application/json' \
  -d '{"name": "financial_facts_retention_sweep"}'

# 10. Verify size dropped
docker exec ebull-postgres psql -U postgres -d ebull -tAc \
  "SELECT pg_size_pretty(pg_total_relation_size('financial_facts_raw'));"
# expect ~16 GB post-sweep
```

### 7.2 Out of scope (deferred follow-ups)

| Follow-up | Trigger |
|---|---|
| Inspect the 1055 junk rows in `financial_facts_raw_default` partition; patch the XBRL parser to reject out-of-window `period_end` | Filed at merge time |
| Compact `filing_raw_documents` (2.9 GB) + `filing_events` (4.3 GB) to land DB size < 5 GB per epic §8.3 | Filed at merge time |
| Production rollout pattern: defer step 5 DROP TABLE for one-day observation window | Triggered when production deployment is in scope (separate epic) |
| Auto-create future quarterly partitions via a maintenance job (when nearing 2030-Q4) | Filed at merge time, low urgency |
| If a future FK references `financial_facts_raw.fact_id`, redesign PK | Triggered by the FK requirement |
| Surface `financial_facts_raw_default` row count in `/system/postgres-health` (Phase 4) so operator sees junk growth live | Phase 4 |

## 8. Codex iterations

### 8.1 Codex 1a — spec v1 review (2026-05-19)

Findings + resolution:

| Severity | Finding | Resolution |
|---|---|---|
| BLOCKING | Swap-rename ignores dependent views; `DROP TABLE old` fails or requires `CASCADE` | §4.4 step 5 explicitly DROPs `share_count_history` + transitive dependents; step 8 re-creates them verbatim. §5.3 + §5.4 add regression tests. |
| BLOCKING | Step 7 (v1) renames `_new` indexes while old indexes still exist → name collision | §4.4 reorders: rename indexes only AFTER `DROP TABLE financial_facts_raw` (step 6). |
| BLOCKING | Retention ranks fact rows, not distinct accessions → would evict facts 4..N of latest 10-K | §4.5.1 rewrites: rank DISTINCT accessions then DELETE all facts of out-of-horizon accessions. §5.2 adds explicit "5×12 → 3×12" assertion. |
| BLOCKING | Retention only targets exact `10-K`/`10-Q`, leaves `10-K/A`/`10-Q/A` unbounded | §4.5 + §4.5.1 define ANNUAL/QUARTERLY families incl. amendments. §5.2 adds amendment-mixing case. |
| WARNING | `pre2010 FROM MINVALUE` mixes pre-1900 junk with valid pre2010 rows | §4.1 changes pre2010 to `FROM ('1900-01-01')`. §5.1 + §5.3 add pre-1900 → DEFAULT assertion. |
| WARNING | Indexes built before bulk INSERT slows migration + bloats WAL | §4.4 reorders: INSERT first, then CREATE INDEX. |
| WARNING | Explicit `COMMIT` in migration → non-atomic with `schema_migrations` insert | §4.4: no explicit BEGIN/COMMIT; rely on runner's tx wrap. |
| WARNING | Retention DELETE has no covering index → seq-scan per partition | §4.4 adds `idx_facts_raw_instrument_accession`. |
| WARNING | 2031+ valid facts silently land in DEFAULT | §4.1.1 adds DEFAULT-growth alarm test; §7.2 cross-phase note to Phase 4 health endpoint. |
| WARNING | Tests miss views, index-name collision, post-COMMIT replay, amendments, pre-1900 DEFAULT routing | §5.1-5.4 add explicit cases for each. |
| NITPICK | Sequence test demanding `cur_val == max(fact_id)+1` is wrong (sequences can have gaps) | §5.3: asserts `>= max(fact_id)` only. |

### 8.2 Codex 1b — spec v2 review (2026-05-19)

Findings + resolution:

| Severity | Finding | Resolution |
|---|---|---|
| BLOCKING | DROP TABLE old before `ALTER SEQUENCE OWNED BY NONE` cascades to the BIGSERIAL-owned sequence the new table reuses | §4.4 step 6 adds explicit `ALTER SEQUENCE ... OWNED BY NONE` before step 7 DROP TABLE. |
| BLOCKING | Default conn mode → first SELECT opens implicit tx → `with conn.transaction()` per instrument becomes savepoint, never commits | §4.5.1 orchestrator opens `psycopg.connect(url, autocommit=True)`. Captured in §5.2 service-no-commit invariant test. |
| WARNING | `distinct_accessions` DISTINCT on (accession, form_type, filed_date) can double-rank an accession with divergent metadata | §4.5.1 SQL switched to `GROUP BY (instrument_id, accession_number)` with `MAX(filed_date)` / `MAX(family)` aggregation. |
| WARNING | `financial_facts_raw_new_pkey` doesn't auto-rename on `ALTER TABLE RENAME` | §4.4 step 8 adds explicit `ALTER INDEX financial_facts_raw_new_pkey RENAME TO financial_facts_raw_pkey`. §5.1 + §5.3 assertions check the canonical name. |
| WARNING | Sequence assertion via `pg_sequence_last_value` is under-specified for never-called/empty sequences | §5.3 switched to: verify `OWNED BY` via `pg_depend` + call `nextval()` once + assert positive value `> max(fact_id)`. |
| WARNING | Single `(instrument_id, accession_number)` index doesn't cover the ranking scan | §4.4 step 4 replaced with wider `(instrument_id, form_type, accession_number, filed_date)` retention index; covers both ranking + DELETE paths. |

### 8.3 Codex 1c — spec v3 review (2026-05-19)

Findings + resolution:

| Severity | Finding | Resolution |
|---|---|---|
| BLOCKING | INSERT-SELECT runs without write lock on old table; late writes between snapshot start and DROP TABLE silently dropped | §4.4 new step 2a: `LOCK TABLE financial_facts_raw IN SHARE MODE` before the bulk copy. |
| WARNING | §4.4 lock-window text claimed DROP TABLE blocks pre-step writes — it doesn't | §4.4 lock-window paragraph rewritten to reflect the SHARE-MODE-from-step-2a gate. |
| WARNING | Single retention index leaves `form_type` as a "gap" for DELETE join → can't use index efficiently | §4.4 split into TWO indexes: `idx_facts_raw_retention_ranking` (ranking CTE) + `idx_facts_raw_retention_evict` (DELETE join). Both small, focused. |
| WARNING | Retention DELETE has no `period_end` predicate → no partition pruning | §4.5.1 explicitly acknowledges: bounded by per-instrument tx + retention indexes, not partition pruning. Per-instrument scan probes ~8-16 of 86 leaves. |
| WARNING | "Index on every leaf partition" assertion literal-naming is impossible (PG auto-names leaf indexes) | §5.1 + §5.3 reworked: assert parent partitioned index + verify child-index attachments via `pg_inherits ⨝ pg_index`. |
| OK | Sequence detach/reattach holds | Confirmed via Codex 1c. |
| OK | ON CONFLICT preservation across partition boundaries holds (partition key included in identity) | Confirmed via Codex 1c citing PG docs. |

### 8.4 Codex 2 — pre-push diff review

Pending — invoked after T1-T5 implementations + tests pass.
