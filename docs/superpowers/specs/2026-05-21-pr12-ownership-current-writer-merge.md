# PR12 — `ownership_*_current` writer rewrite (DELETE+INSERT → diff-aware MERGE) + `ownership_refresh_state` watermark side-table

> Created: **2026-05-21** as the final PR in the data-retention rubric umbrella.
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
>
> Parent spec: [`docs/superpowers/specs/2026-05-19-data-retention-rubric.md`](2026-05-19-data-retention-rubric.md) §4.5 / §4.6 / §6.4 / §7 / §8.
>
> Status: **DESIGN (rev 5)** — Codex 1a → rev 2 (5H+7M+4L). Codex 1b → rev 3 (3H+7M+4L). Codex 1c → rev 4 (1H+3M+3L). Codex 1d on rev 4 returned 3 HIGH (backfill from obs MAX MASKS real drift — false-negative regression vs rev 3's false-positive; "<100ms × 87k LATERAL probes" cost math optimistic; orphan UNION tail still scans 3.68M-row funds obs each sweep) + 3 MED (5 of 7 ingested_at indexes already exist in sql/119 — only funds + esop missing; case 9 raw UPDATE on `known_to` doesn't bump `ingested_at`; lint P doesn't pin index/backfill load-bearing semantics) + 1 LOW (non-goal wording stale re: sweep redesign). Rev 5 folds 1d: backfill reverted to `MAX(_current.refreshed_at) GROUP BY instrument_id` (false-positive over false-negative — one extra refresh-storm-during-first-sweep is benign; missed drift is silent staleness, unacceptable); orphan UNION tail DROPPED (state-anchored primary path only; write-through invariant trusted; gap documented as known limitation); "<100ms" hard claim replaced with "sub-second steady state, requires post-impl EXPLAIN ANALYZE"; sql/163 indexes scoped to funds + esop only (avoid sql/119 duplicates); case 9 setup uses explicit `SET ingested_at = clock_timestamp()` in fixture; lint P extended to pin index + backfill shape; non-goal §2 acknowledges sweep changes.

## 0. Status snapshot (2026-05-21 dev DB)

```text
                  table family                  | total size | heap     | indexes | toast  | tuple_percent
------------------------------------------------+------------+----------+---------+--------+---------------
 ownership_funds_current (write-through)        |    2364 MB |  2080 MB |  283 MB | 624 kB |        10.22 %
 ownership_institutions_current (write-through) |     671 MB |   376 MB |  295 MB | 136 kB |        77.35 %
 ownership_insiders_current                     |      29 MB |   ~14 MB |  ~14 MB |  small |        n/a
 ownership_def14a_current                       |      14 MB |    ~7 MB |   ~7 MB |  small |        n/a
 ownership_treasury_current                     |     384 kB |    small |   small |  small |        n/a
 ownership_esop_current                         |      72 kB |    small |   small |  small |        n/a
 ownership_blockholders_current                 |      24 kB |    small |   small |  small |        n/a
```

`pgstattuple('ownership_funds_current')` raw output:

```text
 table_len  | tuple_count | tuple_len | tuple_percent | dead_tuple_count | dead_tuple_len | dead_tuple_percent | free_space  | free_percent
------------+-------------+-----------+---------------+------------------+----------------+--------------------+-------------+--------------
 2180743168 |      785688 | 222803168 |         10.22 |                0 |              0 |                  0 |  1945107808 |        89.19
```

786k live rows, ~222 MB live data, ~1855 MB free space, 0 dead tuples (autovacuum already swept). Pages emptied, never returned to OS — pure free-space bloat. Only `VACUUM FULL` or `TRUNCATE` (the operator pre-wipe) reclaims it.

All 7 `_current` tables share: `instrument_id` leads PK; `refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()` is present; sibling `refresh_X_current(conn, *, instrument_id) -> int` in [`app/services/ownership_observations.py`](../../../app/services/ownership_observations.py).

PK and per-helper WHERE filter inventory (audited against current code; this table is **load-bearing** for §4.1):

```text
 helper                            | PK                                                                | extra WHERE filter (after instrument_id = %s AND known_to IS NULL)
-----------------------------------+-------------------------------------------------------------------+------------------------------------------------------------------
 refresh_insiders_current          | (instrument_id, holder_identity_key, ownership_nature)            | (none — but cross-source priority CASE in ORDER BY)
 refresh_institutions_current      | (instrument_id, filer_cik, ownership_nature, exposure_kind)       | (none)
 refresh_blockholders_current      | (instrument_id, reporter_cik, ownership_nature)                   | (none)
 refresh_treasury_current          | (instrument_id)                                                   | AND treasury_shares IS NOT NULL                       ← null-displacement guard
 refresh_def14a_current            | (instrument_id, holder_name_key, ownership_nature)                | AND shares IS NOT NULL                                ← null-displacement guard
                                   |                                                                   | AND holder_role IS DISTINCT FROM 'esop'               ← post-#843 parser tag
                                   |                                                                   | AND holder_name !~* %s (_ESOP_HOLDER_NAME_SQL_REGEX)  ← legacy pre-#843 row exclusion
 refresh_funds_current             | (instrument_id, fund_series_id)                                   | (none)
 refresh_esop_current              | (instrument_id, plan_name)                                        | (none)
```

Postgres confirmed at **17.9** (`PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1)`). `MERGE WHEN NOT MATCHED BY SOURCE` is supported.

## 1. Problem statement

The 7 `refresh_*_current` helpers share an identical pattern:

```python
with conn.transaction(), conn.cursor() as cur:
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('refresh_X_current', 0) # %s::bigint)", (instrument_id,))
    cur.execute("DELETE FROM ownership_X_current WHERE instrument_id = %s", (instrument_id,))
    cur.execute("INSERT INTO ownership_X_current (...) SELECT DISTINCT ON (...) FROM ownership_X_observations WHERE instrument_id = %s AND known_to IS NULL <extra filters> ORDER BY ...", (...))
    cur.execute("SELECT COUNT(*) FROM ownership_X_current WHERE instrument_id = %s", (instrument_id,))
```

Two cost vectors:

**Primary** — every call manufactures **N dead tuples** (N = rows in instrument's latest-set) regardless of whether the resolved set actually changed. Bootstrap drains call the helper after every observation batch (8 distinct callers, see Problem-statement table below). `ownership_funds_current` bloats worst because each instrument's latest-set carries ~86 fund-series rows vs ~5-15 institutional-filer rows.

**Secondary (Codex 1a HIGH-1)** — the repair sweep [`app/jobs/ownership_observations_repair.py`](../../../app/jobs/ownership_observations_repair.py) uses `c.refreshed_at < (SELECT MAX(o.ingested_at) FROM obs WHERE instrument_id = c.instrument_id)` as its drift watermark. Under the current DELETE+INSERT pattern, every refresh call (no-op or not) advances `_current.refreshed_at` because the row is rewritten. Under diff-aware MERGE, no-op refreshes leave `refreshed_at` frozen, so the predicate is **permanently true** for any instrument whose observations got re-UPSERTed (re-ingest of same data, parser-version rewash, manifest worker double-drain). Repair sweep re-selects + re-calls refresh forever. Under MERGE no-op the per-call cost is cheap (~1ms), but the sweep degrades from "<100ms on healthy install" (per repair docstring) to "X seconds per affected instrument forever". Operator-visible regression — not acceptable per `feedback_fix_in_scope_default` (small + coupled + unblocked = fix-now).

Caller inventory (refresh callers across the codebase):

| Caller | Helper | Source |
| --- | --- | --- |
| `app/services/manifest_parsers/sec_n_port.py:404` | `refresh_funds_current` | manifest worker per-accession |
| `app/services/n_port_ingest.py:1150` | `refresh_funds_current` | per-CIK ingest |
| `app/services/sec_bulk_orchestrator_jobs.py:761` | `refresh_funds_current` | bulk archive drain |
| `app/services/manifest_parsers/sec_13f_hr.py:527` | `refresh_institutions_current` | manifest worker per-accession |
| `app/services/institutional_holdings.py:1552` | `refresh_institutions_current` | per-CIK ingest |
| `app/services/sec_bulk_orchestrator_jobs.py:425` | `refresh_institutions_current` | bulk archive drain |
| `app/services/ownership_observations_sync.py:404` | `refresh_institutions_current` | observations sync |
| `app/services/rewash_filings.py:1075` | `refresh_institutions_current` | rewash rescue |
| `app/jobs/ownership_observations_repair.py:70` | (5 categories pre-PR12 via lambda; expanded to 7 in §3.3) | weekly repair sweep |

## 2. Non-goals

- Row deletion of pre-existing rows (parent spec §6.3 — operator pre-wipe handles reshape).
- `VACUUM FULL` against existing tables (operator-driven, separate event; pre-wipe TRUNCATEs anyway).
- Schema changes to existing `_current` tables — PK shapes, column lists, partitioning all unchanged. (One **new** table `ownership_refresh_state` is added; §3.3.)
- Frontend / API surface — `_current` reads unaffected (same row shapes, same indexes, same PKs).
- Writer signature changes — `refresh_X_current(conn, *, instrument_id) -> int` preserved for every caller.
- Cap-shape changes — observations write-side caps from PR1–PR11 stand as-is.
- Repair-sweep redesign beyond the predicate switch + 7-category expansion — sweep keeps its weekly cron, per-category loop, savepoint-per-instrument shape. The `_CATEGORIES` list grows from 5 to 7 (adds `funds` + `esop`) to align with the state-table CHECK constraint — minimal additive change, not a redesign.

## 3. Decisions

### 3.1 Writer pattern — diff-aware MERGE

Single MERGE statement per helper. `WHEN MATCHED AND (...) IS DISTINCT FROM (...)` skips writes when business columns are identical; `WHEN NOT MATCHED BY TARGET` inserts new rows; `WHEN NOT MATCHED BY SOURCE` deletes rows that fall out of the latest set. Scope clamp on the ON clause (§4.3) keeps the planner from scanning the entire target table.

PG17 `WHEN NOT MATCHED BY SOURCE` is load-bearing — PG15/16 only have `BY TARGET`. Dev DB is PG17.9; boot-time guard added (§7).

### 3.2 Migration scope — all 7 helpers

Per parent spec §7 "ownership_*_current size audit + remediation" and `feedback_no_punting_complete_work` ("default to full scope; no multiple-choice"). Uniform pattern, single lint guard, no tech-debt followups. The 5 small-table helpers (insiders/blockholders/treasury/def14a/esop) get preventive fix at near-zero cost — their bespoke filters are pinned by lint invariants per-helper so 7-helper scope does not import 5 unaudited bespoke semantics (Codex 1a MED-7).

### 3.3 Watermark contract — separate `ownership_refresh_state` side-table

**New table** breaks the conflation between "_current's last-write timestamp" (storage-layer concern) and "repair-sweep drift watermark" (consumer concern).

```sql
-- sql/163_ownership_refresh_state.sql
CREATE TABLE IF NOT EXISTS ownership_refresh_state (
    instrument_id                             BIGINT      NOT NULL,
    category                                  TEXT        NOT NULL CHECK (category IN (
        'insiders', 'institutions', 'blockholders', 'treasury', 'def14a', 'funds', 'esop'
    )),
    last_drained_observations_max_ingested_at TIMESTAMPTZ,
    last_refresh_attempted_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, category)
);

CREATE INDEX IF NOT EXISTS idx_ownership_refresh_state_category
    ON ownership_refresh_state (category, last_drained_observations_max_ingested_at);
```

- `last_drained_observations_max_ingested_at` is the **drift watermark** — set by each `refresh_X_current` call to `MAX(ingested_at)` over **all** rows in `ownership_X_observations` for the instrument (no `known_to IS NULL` filter — the repair sweep predicate operates over the same population, so the two sides must align; otherwise a `known_to` expiry would advance only one side and produce false drift, Codex 1b HIGH-1). NULL means "no observations at all" → predicate treats matching NULLs as no-drift (via `IS DISTINCT FROM`).
- `last_refresh_attempted_at` records the most recent refresh call (whether MERGE wrote rows or not). Operator-visible "did we run". Not used by sweep predicate; useful for ops dashboards.
- PK `(instrument_id, category)` caps the table at 7 × |distinct-instrument-ids| rows (currently ~87k in dev; grows linearly with universe). UPSERT-only writer with no row growth past the cap → bloat surface tiny + autovacuum-manageable.
- CHECK lists all **7 categories** (Codex 1b MED-4 — current repair sweep iterates only 5; PR12 expands the sweep's `_CATEGORIES` list to include `funds` + `esop` for uniformity).

**Per-observations-table `(instrument_id, ingested_at DESC)` indexes** — sql/119 (#864 / #873) already provisioned these for 5 categories (insiders / institutions / blockholders / treasury / def14a). sql/163 adds the 2 missing indexes only (Codex 1d MED-1 — avoid duplicate-index landmines):

```sql
-- sql/163 — funds and esop only; the other 5 already exist from sql/119.
CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_ingested
    ON ownership_funds_observations (instrument_id, ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_ingested
    ON ownership_esop_observations (instrument_id, ingested_at DESC);
```

These indexes back the LATERAL `MAX(ingested_at)` correlated subquery in the repair-sweep predicate (below). Each lookup becomes an index-only scan returning 1 row — without them, the lookup falls back to per-partition seq-scan + aggregate (tens of seconds on funds at 3.68M obs rows).

**Migration backfill** is in the same SQL file; one INSERT per category, sourced from **`MAX(_current.refreshed_at) GROUP BY instrument_id`** (Codex 1d HIGH-1 — backfilling from `MAX(obs.ingested_at)` would *mask* real drift if `_current` was never reconciled, a silent-staleness regression unacceptable for a safety-net job; backfilling from `_current.refreshed_at` may cause a one-off first-sweep refresh storm against instruments where obs `clock_timestamp()` ran slightly later than refresh-tx `now()`, but every such refresh is a MERGE no-op (the diff predicate skips writes) → benign extra cost, never missed drift):

```sql
-- Per category, repeated 7 times in the migration:
INSERT INTO ownership_refresh_state (
    instrument_id, category,
    last_drained_observations_max_ingested_at, last_refresh_attempted_at
)
SELECT
    c.instrument_id,
    'funds',
    MAX(c.refreshed_at),
    MAX(c.refreshed_at)
FROM ownership_funds_current c
GROUP BY c.instrument_id
ON CONFLICT (instrument_id, category) DO NOTHING;
```

Operator-visible: the first repair-sweep tick after deploy may select more instruments than steady state due to the tx-time vs clock_timestamp skew, with each selection costing one MERGE no-op (~1ms per instrument under the diff predicate). One-time event; subsequent sweeps return to steady state.

**Repair-sweep predicate switch** in [`app/jobs/ownership_observations_repair.py`](../../../app/jobs/ownership_observations_repair.py) — **state-anchored single-query** (Codex 1d HIGH-3 — the UNION orphan tail from rev 4 anti-joined observations every sweep, full-table-ish cost on funds even when zero rows matched; dropped in favour of the write-through invariant + a separate operator-driven backfill if drift between obs-existence and state-row-existence is ever discovered):

```sql
SELECT s.instrument_id
FROM ownership_refresh_state s
LEFT JOIN LATERAL (
    SELECT MAX(o.ingested_at) AS obs_max
    FROM ownership_X_observations o
    WHERE o.instrument_id = s.instrument_id
) sub ON TRUE
WHERE s.category = %s
  AND s.last_drained_observations_max_ingested_at IS DISTINCT FROM sub.obs_max;
```

NULL semantics on this branch:

- Both `s.last_drained` and `sub.obs_max` present, equal → not distinct → no drift.
- Both present, different → distinct → drift → refresh fires.
- `s.last_drained` present, `sub.obs_max` NULL (all obs deleted for instrument) → distinct → drift → refresh fires → MERGE NOT MATCHED BY SOURCE deletes any orphan `_current` rows → state UPSERT writes watermark = NULL → next sweep both NULL → not distinct → no drift.
- Both NULL → not distinct → no drift (steady state for empty instrument).

**Known limitation (documented gap)** — an instrument with observations but no `ownership_refresh_state` row (write-through fired the observation write but skipped or crashed before reaching the refresh helper) will not be detected by the sweep. The write-through invariant pins this: every observation-writer caller in the codebase invokes `refresh_X_current` immediately after `record_X_observation`. If a future bug breaks that pattern, drift is detectable by an out-of-band reconciliation query (cheap aggregate vs state-table size); not part of the in-band sweep cost.

## 4. Writer rewrite

Each helper becomes one MERGE statement + one UPSERT into `ownership_refresh_state`, inside the existing transaction + advisory lock. Template (`ownership_funds_current` shown; per-helper differences in §4.1):

```python
def refresh_funds_current(conn: psycopg.Connection[Any], *, instrument_id: int) -> int:
    """Diff-aware MERGE reconciler for ``ownership_funds_current``.

    UPDATE only when business columns IS DISTINCT FROM the new set;
    INSERT new rows; DELETE rows that fall out of the latest set
    (NOT MATCHED BY SOURCE scoped via the ON clause to this instrument).
    refreshed_at advances on the UPDATE path only; the operator-visible
    drift watermark for repair-sweep lives in ownership_refresh_state
    (#1233 PR12 — separates write-side dead-tuple budget from watermark
    semantics so no-op refreshes do not trigger forever-loops in the
    repair sweep)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_funds_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
        # Codex 1b HIGH-2: capture watermark BEFORE MERGE in a separate
        # statement so the value stored in ownership_refresh_state cannot
        # advance past observations the MERGE did not see. Race direction
        # under READ COMMITTED: an obs that commits between this SELECT
        # and the MERGE's source-subquery snapshot will be merged + the
        # state watermark will lag → next sweep finds drift → extra
        # refresh (no-op for the freshly-merged row). Benign over-detect,
        # never silent under-detect. Population aligned with §3.3 repair
        # predicate (no `known_to IS NULL` filter — both sides operate
        # over all observations for the instrument).
        cur.execute(
            "SELECT MAX(ingested_at) FROM ownership_funds_observations WHERE instrument_id = %s",
            (instrument_id,),
        )
        watermark_row = cur.fetchone()
        watermark = watermark_row[0] if watermark_row else None
        cur.execute(
            """
            MERGE INTO ownership_funds_current AS tgt
            USING (
                SELECT DISTINCT ON (fund_series_id)
                    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                    ownership_nature,
                    source, source_document_id, source_accession, source_url,
                    filed_at, period_start, period_end,
                    shares, market_value_usd, payoff_profile, asset_category
                FROM ownership_funds_observations
                WHERE instrument_id = %(iid)s AND known_to IS NULL
                ORDER BY
                    fund_series_id,
                    filed_at DESC,
                    period_end DESC,
                    source_document_id ASC
            ) AS src
            ON tgt.instrument_id = %(iid)s
               AND tgt.fund_series_id = src.fund_series_id
            WHEN MATCHED AND (
                tgt.fund_series_name, tgt.fund_filer_cik, tgt.ownership_nature,
                tgt.source, tgt.source_document_id, tgt.source_accession, tgt.source_url,
                tgt.filed_at, tgt.period_start, tgt.period_end,
                tgt.shares, tgt.market_value_usd, tgt.payoff_profile, tgt.asset_category
            ) IS DISTINCT FROM (
                src.fund_series_name, src.fund_filer_cik, src.ownership_nature,
                src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.payoff_profile, src.asset_category
            ) THEN UPDATE SET
                fund_series_name   = src.fund_series_name,
                fund_filer_cik     = src.fund_filer_cik,
                ownership_nature   = src.ownership_nature,
                source             = src.source,
                source_document_id = src.source_document_id,
                source_accession   = src.source_accession,
                source_url         = src.source_url,
                filed_at           = src.filed_at,
                period_start       = src.period_start,
                period_end         = src.period_end,
                shares             = src.shares,
                market_value_usd   = src.market_value_usd,
                payoff_profile     = src.payoff_profile,
                asset_category     = src.asset_category,
                refreshed_at       = now()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                ownership_nature, source, source_document_id, source_accession, source_url,
                filed_at, period_start, period_end,
                shares, market_value_usd, payoff_profile, asset_category
            ) VALUES (
                src.instrument_id, src.fund_series_id, src.fund_series_name, src.fund_filer_cik,
                src.ownership_nature, src.source, src.source_document_id, src.source_accession, src.source_url,
                src.filed_at, src.period_start, src.period_end,
                src.shares, src.market_value_usd, src.payoff_profile, src.asset_category
            )
            WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE
            """,
            {"iid": instrument_id},
        )
        cur.execute(
            """
            INSERT INTO ownership_refresh_state (
                instrument_id, category,
                last_drained_observations_max_ingested_at, last_refresh_attempted_at
            ) VALUES (
                %(iid)s, 'funds', %(watermark)s, now()
            )
            ON CONFLICT (instrument_id, category) DO UPDATE SET
                last_drained_observations_max_ingested_at = EXCLUDED.last_drained_observations_max_ingested_at,
                last_refresh_attempted_at = EXCLUDED.last_refresh_attempted_at
            """,
            {"iid": instrument_id, "watermark": watermark},
        )
        cur.execute(
            "SELECT COUNT(*) FROM ownership_funds_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
```

### 4.1 Per-helper differences (load-bearing — Codex 1a HIGH-3)

| Helper | PK | DISTINCT ON | Extra WHERE filter | ORDER BY | category literal |
| --- | --- | --- | --- | --- | --- |
| `refresh_insiders_current` | `(instrument_id, holder_identity_key, ownership_nature)` | `(holder_identity_key, ownership_nature)` | (none) | `holder_identity_key, ownership_nature, <source-priority CASE> ASC, period_end DESC, filed_at DESC, source ASC, source_document_id ASC` | `'insiders'` |
| `refresh_institutions_current` | `(instrument_id, filer_cik, ownership_nature, exposure_kind)` | `(filer_cik, ownership_nature, exposure_kind)` | (none) | `filer_cik, ownership_nature, exposure_kind, period_end DESC, filed_at DESC, source_document_id ASC` | `'institutions'` |
| `refresh_blockholders_current` | `(instrument_id, reporter_cik, ownership_nature)` | `(reporter_cik, ownership_nature)` | (none) | `reporter_cik, ownership_nature, filed_at DESC, period_end DESC, source_document_id ASC` | `'blockholders'` |
| `refresh_treasury_current` | `(instrument_id)` | `(instrument_id)` | `AND treasury_shares IS NOT NULL` | `instrument_id, period_end DESC, filed_at DESC, source_document_id ASC` | `'treasury'` |
| `refresh_def14a_current` | `(instrument_id, holder_name_key, ownership_nature)` | `(holder_name_key, ownership_nature)` | `AND shares IS NOT NULL AND holder_role IS DISTINCT FROM 'esop' AND holder_name !~* %s (_ESOP_HOLDER_NAME_SQL_REGEX)` | `holder_name_key, ownership_nature, period_end DESC, filed_at DESC, source_document_id ASC` | `'def14a'` |
| `refresh_funds_current` | `(instrument_id, fund_series_id)` | `(fund_series_id)` | (none) | `fund_series_id, filed_at DESC, period_end DESC, source_document_id ASC` | `'funds'` |
| `refresh_esop_current` | `(instrument_id, plan_name)` | `(plan_name)` | (none) | `plan_name, filed_at DESC, period_end DESC, source_document_id ASC` | `'esop'` |

The insiders source-priority `CASE source WHEN 'form4' THEN 1 WHEN 'form3' THEN 2 ... END ASC` chain is preserved verbatim from current code (Form 4 > Form 3 > 13D > 13G > DEF14A > 13F > N-PORT/N-CSR > XBRL DEI > 10-K note > FINRA SI). The chain is part of insiders' ORDER BY clause inside the MERGE's USING subquery — no behavioural change vs today.

### 4.2 Diff-predicate column-list contract (Codex 1a MED-2)

The `IS DISTINCT FROM` tuple lists on LHS and RHS must contain **exactly the same column names** in the same order; they must contain **every non-PK column** that appears in the UPDATE SET clause; they must **not** contain `refreshed_at`; they must **not** contain PK columns (PK columns are matched by the ON clause; the diff predicate covers business cols only).

Mechanical contract that lint invariants E + I + J pin (simplified per Codex 1b MED-7 — exact ordered equality, no subset relations):

- **Diff cols** ≡ LHS tuple cols ≡ RHS tuple cols (exact ordered equality, same names same positions on both sides of `IS DISTINCT FROM`).
- **Diff cols** ≡ UPDATE SET targets **minus** `{refreshed_at}` (UPDATE writes every diff col + `refreshed_at`, and nothing else).
- **Diff cols** ∩ PK cols = ∅ (PK cols are matched by ON clause; not in the diff predicate).
- `refreshed_at` ∉ diff cols, `refreshed_at` ∉ INSERT column list (so DEFAULT now() fires on insert).

Operator-visible semantic: `_current.refreshed_at` advances IFF business cols changed. The drift watermark for repair sweep is `ownership_refresh_state.last_drained_observations_max_ingested_at` (§3.3) — updated on **every** refresh attempt (Codex 1c LOW-2 — "updated on every attempt" is the accurate phrasing; the *value* only changes when `MAX(observations.ingested_at)` changed). The companion `last_refresh_attempted_at` column records the wall-clock of the call regardless.

### 4.3 Scope clamp on ON clause + DELETE branch (Codex 1a HIGH-2 + HIGH-4)

The ON clause carries `tgt.instrument_id = %(iid)s` as a const predicate on target (NOT `tgt.instrument_id = src.instrument_id`). This lets the planner restrict target-side rows to the instrument's PK-index slice instead of considering the whole target table for `WHEN NOT MATCHED BY SOURCE` anti-join.

The `WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE` clause keeps the clamp as **defence-in-depth** — if a future refactor relaxes the ON clause to `tgt.instrument_id = src.instrument_id`, the NOT-MATCHED-BY-SOURCE branch still cannot DELETE rows from other instruments. Lint invariant D pins **both** clamps (ON-clause + DELETE-clause) by exact literal string `tgt.instrument_id = %(iid)s` — no substring tolerance, no `IS NOT NULL` decoy (Codex 1a HIGH-4).

### 4.4 Concurrency contract (Codex 1a MED-5)

- `pg_advisory_xact_lock(hashtextextended('refresh_X_current', 0) # instrument_id::bigint)` serialises **same-helper, same-instrument** refresh calls. Cross-helper or cross-instrument calls do not contend on the lock.
- The MERGE runs inside `with conn.transaction()` so the advisory lock + the MERGE writes + the `ownership_refresh_state` UPSERT all share one transaction; failure of any step rolls back the lot.
- Default isolation is **READ COMMITTED**. The USING subquery sees observations committed before the MERGE's snapshot; observations committed during the MERGE call do NOT appear until the next refresh. This matches today's DELETE+INSERT semantics (the SELECT-into-INSERT step also uses READ COMMITTED).
- Concurrent **observation writers** (record_*_observation called from a sibling code path) do not block on the advisory lock. They may commit between the lock acquisition and the MERGE's source-subquery scan; the MERGE will see whatever was committed at scan time. The repair-sweep predicate (§3.3) catches any obs writes that committed after the most recent refresh — that is the whole point of the watermark.
- PG MERGE takes row-level locks on matched/updated target rows + the locks released at COMMIT. Concurrent same-instrument refreshes (advisory lock contention) → serialised. Concurrent different-instrument refreshes → row-level locks are disjoint by PK; no contention.

## 5. Lint guard (91 clause-counts total — 81 per-helper + 10 cross-cutting)

New script: [`scripts/check_ownership_refresh_writer_pattern.sh`](../../../scripts/check_ownership_refresh_writer_pattern.sh). Wired into [`.githooks/pre-push`](../../../.githooks/pre-push) after PR11's `check_13dg_retention.sh`. Awk-based function-body block walker (PR4 Codex 1c lesson); every grep guarded against empty-input by exact-count assertion (PR10a Codex iter 1 lesson).

Per helper (×7):

| Invariant | What it pins | Mechanism |
| --- | --- | --- |
| **A** | Helper defined exactly once in `app/services/ownership_observations.py` | grep `^def refresh_<X>_current\(` → count == 1 |
| **B** | No legacy `DELETE FROM ownership_<X>_current` inside helper body | awk-extract body span; grep → count == 0 |
| **C** | `MERGE INTO ownership_<X>_current AS tgt` opener present | grep inside body span → count == 1 |
| **D** | Scope-clamp pinned by exact literal `tgt.instrument_id = %(iid)s` in **both** the ON clause and the NOT-MATCHED-BY-SOURCE DELETE clause | Block-local awk (Codex 1b MED-5 — count == 2 is satisfiable by two literals in wrong places): (D1) awk-extract the ON-clause span from `MERGE INTO ownership_<X>_current AS tgt` opener through the first `WHEN` keyword; grep literal `tgt.instrument_id = %(iid)s` inside → count == 1. (D2) awk-extract the `WHEN NOT MATCHED BY SOURCE` clause through the `THEN DELETE` terminator; grep literal `tgt.instrument_id = %(iid)s` inside → count == 1. |
| **E** | `refreshed_at` NOT inside diff predicate's `IS DISTINCT FROM` tuples (either side) | awk-extract span from `WHEN MATCHED AND (` to `) THEN UPDATE`; grep → count == 0 |
| **F** | `pg_advisory_xact_lock` preserved on function-namespace hash | grep `hashtextextended\('refresh_<X>_current'` inside body → count == 1 |
| **G** | DISTINCT ON columns match per-helper expected tuple | grep literal `DISTINCT ON (<expected cols>)` inside body → count == 1 |
| **H** | ORDER BY tuple matches per-helper expected list (whitespace-normalised) | awk-extract `ORDER BY` block; collapse whitespace; compare against per-helper expected string → equal |
| **I** | UPDATE SET clause writes every non-PK business column AND `refreshed_at` (parity with diff tuples) | awk-extract UPDATE SET block; collect column names; assert set == diff-tuple-cols ∪ {refreshed_at} |
| **J** | INSERT clause omits exactly `refreshed_at` from column list (DEFAULT now() fires) | awk-extract INSERT column list; assert `refreshed_at` ∉ list; assert set ≡ all-non-PK-cols ∪ {PK cols} |
| **K** | Per-helper extra WHERE filter clauses present inside USING subquery — see §4.1 column "Extra WHERE filter" | Per-clause grep with per-helper expected counts (Codex 1b MED-6 — def14a's 3 clauses must be counted independently; one clause passing while the other two are dropped must fail the lint): treasury `AND treasury_shares IS NOT NULL` → count == 1; def14a (K1) `AND shares IS NOT NULL` → count == 1, (K2) `AND holder_role IS DISTINCT FROM 'esop'` → count == 1, (K3) `AND holder_name !~* %s` (the `_ESOP_HOLDER_NAME_SQL_REGEX` parameter) → count == 1. Insiders / institutions / blockholders / funds / esop have no extra filter; the K-class is skipped for those helpers (per-helper expected count = 0). |
| **L** | Helper UPSERTs into `ownership_refresh_state` with the matching category literal | grep `INSERT INTO ownership_refresh_state` inside body span → count == 1 AND grep `'<expected category>'` inside same block → count == 1 |

Per-helper clause-count breakdown (counting K-class sub-clauses individually per §4.1 — def14a K = K1+K2+K3, treasury K = 1, others have no K): 7 × 10 (A-J) + 7 × 1 (L) + 1 (treasury K) + 3 (def14a K1/K2/K3) = **81 per-helper clause-counts**.

Plus 4 cross-cutting checks:

| Invariant | What it pins | Mechanism |
| --- | --- | --- |
| **M** | No `DELETE FROM ownership_*_current` anywhere under `app/` outside the 7 helper bodies | grep across `app/**/*.py` minus the 7 helper bodies → count == 0 |
| **N** | No `DELETE FROM ownership_*_current` anywhere under `scripts/` and `app/jobs/` (catches future writers landing outside `app/services/`) | grep across both trees → count == 0 |
| **O** | `ownership_observations_repair.py` predicate references `ownership_refresh_state.last_drained_observations_max_ingested_at` (not legacy `c.refreshed_at < ...` form) | grep `c.refreshed_at <` → count == 0 inside the repair file |
| **P** | Migration `sql/163_ownership_refresh_state.sql` pins all rev-5 contracts (Codex 1d MED-3): table + indexes + backfill | 7 targeted grep checks against the migration file with exact-count assertions — (P1) `CREATE TABLE IF NOT EXISTS ownership_refresh_state` opener present; (P2) PK is `(instrument_id, category)`; (P3) CHECK lists exactly the 7 category literals; (P4) `CREATE INDEX IF NOT EXISTS idx_funds_obs_instrument_ingested` present; (P5) `CREATE INDEX IF NOT EXISTS idx_esop_obs_instrument_ingested` present; (P6) backfill source shape `FROM ownership_X_current c GROUP BY c.instrument_id` (not from observations MAX); (P7) backfill repeated 7 times (one per category). |

Total: **91 lint clause-counts** (81 per-helper + 10 cross-cutting — M, N, O, P1-P7). Pure text walk, no DB dependency, sub-second runtime.

## 6. Tests (52 parametrised cases)

New file: [`tests/test_ownership_refresh_writer_merge.py`](../../../tests/test_ownership_refresh_writer_merge.py). Parametrised over the 7 helpers.

`pg_stat_user_tables` rejected as the no-op-churn assertion source (async stats-collector flush has multi-100ms lag → flaky). Switched to per-row `xmin` comparisons + `pgstattuple` for the load-bearing no-op-churn case. Per Codex 1a MED-1: every `xmin` comparison is **equality / inequality** (`xmin::text != xmin0::text`), never ordering (xid is not monotonic-comparable; wraparound + xid8 semantics make `>` wrong).

| # | Case | Setup | Action | Assertion |
| --- | --- | --- | --- | --- |
| 1 | **insert** | empty `_current` for instrument I; write 1 observation | `refresh_X_current(I)` | row present; row's `xmin` is a fresh xid (not the pre-call xid) |
| 2 | **no-op churn** | row present from prior refresh; no observation change; capture per-row `xmin0` + `pgstattuple(_current).{table_len, tuple_count, dead_tuple_count}` + `pgstattuple(ownership_refresh_state).{tuple_count, dead_tuple_count}` | `refresh_X_current(I)` again | per-row `xmin::text == xmin0::text` on `_current` (no rewrite); `pgstattuple(_current).table_len` unchanged; `pgstattuple(_current).dead_tuple_count` delta == 0; `_current.refreshed_at` unchanged (proves diff-predicate excludes it). State-table churn bounded (Codex 1b MED-2): `pgstattuple(ownership_refresh_state).tuple_count` delta == 0 (UPSERT into same PK row) AND `dead_tuple_count` delta ≤ 1 (the in-place state UPDATE produces at most 1 dead tuple per refresh call, autovacuum-bounded). **Load-bearing for primary bloat fix on `_current`; pins state-table bloat surface.** |
| 3 | **update (amendment)** | row present `xmin0`, refreshed_at0; insert later-`filed_at` observation, same PK, different `shares` | refresh | row present; per-row `xmin::text != xmin0::text`; `shares` reflects amendment; `refreshed_at > refreshed_at0` (advances on actual diff). |
| 4 | **delete (`known_to` expiry)** | row present; `UPDATE _observations SET known_to = now() WHERE …` | refresh | row absent (NOT MATCHED BY SOURCE → DELETE fired). |
| 5 | **scope clamp** | rows present for instruments A and B; capture B's per-row `xmin0` | `refresh_X_current(A)` with no observation change | B's row present; B's per-row `xmin::text == xmin0::text` (other-instrument rows untouched). **Pins the literal `tgt.instrument_id = %(iid)s` clamp in ON clause + DELETE clause.** |
| 6 | **priority-chain regression (insiders only)** | write 2 observations same `(holder_identity_key, ownership_nature)`: source='form4' (priority 1) + source='13d' (priority 3); equal period_end + filed_at | refresh | `_current.source == 'form4'` (priority chain unchanged). Pins the cross-source `CASE source WHEN ... ASC` ORDER BY chain (lint invariant H + runtime). |
| 7 | **per-helper filter regression** | (a) **treasury**: write 1 obs with `treasury_shares=NULL` AND 1 obs with `treasury_shares=12345` same period; (b) **def14a**: write 1 obs `holder_role='esop'`, 1 obs `holder_role='principal' AND holder_name ILIKE '%ESOP%'`, 1 obs `holder_role='principal' AND holder_name='Vanguard'` | refresh | (a) `_current.treasury_shares == 12345` (NULL-displacement guard works); (b) `_current` row count == 1; only the Vanguard row survives (ESOP regex + holder_role='esop' both excluded). |
| 8 | **repair-sweep no-loop** | row present in `_current` + `ownership_refresh_state`; UPSERT same obs row (DO UPDATE clause bumps `obs.ingested_at`); refresh fires (no diff → MERGE no-op) | run `_drifted_instruments` again with the new predicate | empty list (state-table watermark advanced; sweep no longer re-selects). **Pins the §3.3 watermark fix end-to-end.** |
| 9 | **`known_to` expiry watermark alignment** (Codex 1c MED-2) | row present in `_current` + state; `UPDATE _observations SET known_to = now(), ingested_at = clock_timestamp() WHERE …` on the active observation (Codex 1d MED-2 — raw UPDATE of `known_to` alone does NOT bump `ingested_at`; the production ingest path's DO UPDATE clause bumps both, the test must mirror that to actually exercise the alignment); refresh fires (MERGE NOT MATCHED BY SOURCE → DELETE on `_current`; state UPSERT advances watermark to new obs MAX which now includes the expired row's bumped `ingested_at`) | run `_drifted_instruments` again | empty list. **Pins the all-observations population alignment between watermark capture and repair predicate** — if the watermark used `known_to IS NULL` while the predicate used all obs (or vice versa), expiry would create a false drift trigger. |

Total: 5 base cases × 7 helpers = 35 + 1 priority-chain (insiders) + 2 filter-regression (treasury + def14a) + 7 repair-sweep no-loop + 7 known_to expiry watermark = **52 parametrised cases**.

**Extension provisioning + CI fail-loud** (Codex 1a MED-6): [`tests/fixtures/ebull_test_db.py`](../../../tests/fixtures/ebull_test_db.py) template provisioning gains `CREATE EXTENSION IF NOT EXISTS pgstattuple`. If the extension is missing at test time, the no-op-churn case fails loudly with a dedicated error message (`pytest.fail(f"pgstattuple extension missing in {db_name} — provisioning bug, do NOT skip")`) instead of skipping. CI provisioning step explicitly asserts the extension is present after template clone.

## 7. Postgres version guard

`MERGE WHEN NOT MATCHED BY SOURCE` requires PG ≥ 17. Without a boot-time guard, a PG < 17 deployment would pass lint and crash at first refresh with `syntax error at or near "BY"`.

Mirror #1187's `max_locks_per_transaction` boot pattern. New cross-cutting check at lifespan startup — either extends [`app/main.py`](../../../app/main.py) lifespan or sits as a sibling in `app/system/postgres_health.py`. Assertion: `SELECT current_setting('server_version_num')::int >= 170000`. Fail-closed: refuse to boot under PG < 17. Smoke test `tests/smoke/test_app_boots.py` exercises lifespan → guard gated by the pre-push smoke gate.

Single-fact assertion; cross-cutting; minimal blast radius. PR description explicitly calls out that PG16 deployments will hard-fail at boot post-merge (Codex 1a LOW-1).

## 8. Operator semantics

**Pre-existing dead space**: PR12 ships the **writer**. It does NOT shrink the pre-existing dead space:

- `ownership_funds_current` heap stays at 2080 MB post-merge.
- `ownership_institutions_current` heap stays at 376 MB post-merge.
- `GET /system/postgres-health` (Phase 4 of #1208) continues to report `db_size ≈ 41 GB` until the §6.3 operator pre-wipe.

Reclaim path is the **operator pre-wipe + clean re-run** (parent spec §6.3 + §8 acceptance). PR12's contribution is ensuring the clean re-run does not re-bloat the new write-through pattern AND that the repair sweep stays cheap on healthy installs.

**Honest bloat attribution** (Codex 1b LOW-4): the dominant bloat source under the old pattern is bootstrap / write-through refresh after every observation batch (per-call N-row DELETE+INSERT × ~9k N-PORT accessions × 86 rows-per-instrument). Repair-sweep retriggers under the legacy `c.refreshed_at < (...)` predicate are a **secondary** amplifier, not the primary mass. The diff-aware MERGE eliminates the secondary feeder; the new `ownership_refresh_state` watermark prevents the forever-loop regression that pure MERGE would have introduced. The primary write-side reduction comes from MERGE skipping no-op rewrites of business-identical rows, not from sweep behaviour.

**Lock / downtime cost of the operator pre-wipe** (Codex 1a LOW-2): the parent spec's §6.3 pre-wipe is `TRUNCATE` of every observations + `_current` + `ownership_refresh_state` table. `TRUNCATE` takes an `ACCESS EXCLUSIVE` lock per table → all SELECT/INSERT/UPDATE traffic blocks for the truncate window. Dev DB at ~41 GB the operation is typically <30s. Production-grade deployments would need a maintenance window — out of scope here (dev-DB-only operation by design).

**Read-side unchanged**: no `_current` consumer sees behavioural change. Same row shapes, same dedup ordering, same priorities, same indexes. The MERGE's `WHEN NOT MATCHED BY SOURCE THEN DELETE` is steady-state writer behaviour identical to today's DELETE+INSERT (a row drops out of `_current` only when its only observation expires via `known_to` or when caps shed it post-wipe).

**Sweep cost**: the state-anchored predicate does ~87k LATERAL `MAX(ingested_at)` lookups (index-only scans). Steady-state target is **sub-second** (≤ 1-2s) on healthy install (Codex 1d HIGH-2 — "<100ms × 87k = <100ms" math was wrong; even at index-only scan + ~10μs per row, 87k probes is ~1s wall clock under realistic PG planner overhead). Exact ceiling requires post-implementation `EXPLAIN ANALYZE` against a populated dev DB; spec asks for that as part of §9 DoD #8 verification. First sweep post-deploy may take longer due to backfill-skew false-positive refresh storm (each false-positive is a MERGE no-op; bounded by universe size × ~1ms per call); operator-acceptable one-off event.

## 9. Definition of done

CLAUDE.md §"Definition of done" + §"ETL / parser / schema-migration additional clauses" both apply.

1. 7 `refresh_*_current` helpers in [`app/services/ownership_observations.py`](../../../app/services/ownership_observations.py) rewritten to single-statement MERGE + `ownership_refresh_state` UPSERT per §4 template + §4.1 per-helper differences. Signature `(conn, *, instrument_id) -> int` preserved on every helper.
2. New migration `sql/163_ownership_refresh_state.sql` creates the table with the schema in §3.3 + 2 `(instrument_id, ingested_at DESC)` indexes on funds + esop observations tables (sql/119 already covers the other 5 — Codex 1d MED-1) + idempotent backfill via `MAX(_current.refreshed_at) GROUP BY instrument_id` per category (Codex 1d HIGH-1 — false-positive over false-negative; backfilling from obs MAX would mask drift). Runs inside the standard migration runner; no `-- runner: autocommit` directive required.
3. Repair-sweep predicate switched in [`app/jobs/ownership_observations_repair.py`](../../../app/jobs/ownership_observations_repair.py) to the §3.3 form (state-anchored single-query LATERAL `MAX(ingested_at)` with `IS DISTINCT FROM`; orphan UNION tail dropped per Codex 1d HIGH-3). `_CATEGORIES` list expanded to all 7 categories — adds `funds` + `esop` lambdas wiring `refresh_funds_current` + `refresh_esop_current` so the sweep stays uniform with the state-table CHECK constraint (Codex 1b MED-4).
4. [`scripts/check_ownership_refresh_writer_pattern.sh`](../../../scripts/check_ownership_refresh_writer_pattern.sh) (91 clause-counts: 81 per-helper + 10 cross-cutting M/N/O/P1-P7, per §5) wired into [`.githooks/pre-push`](../../../.githooks/pre-push) after `check_13dg_retention.sh`.
5. [`tests/test_ownership_refresh_writer_merge.py`](../../../tests/test_ownership_refresh_writer_merge.py) — 52 parametrised cases (7 × 5 base + insiders priority-chain + treasury null guard + def14a ESOP exclusion + 7 repair-sweep no-loop + 7 known_to expiry watermark alignment per §6). Load-bearing no-op-churn case uses per-row `xmin::text` equality + `pgstattuple` on both `_current` and `ownership_refresh_state`.
6. [`tests/fixtures/ebull_test_db.py`](../../../tests/fixtures/ebull_test_db.py) template provisions `pgstattuple` extension; failure to provision triggers `pytest.fail` in no-op-churn case (no silent skips).
7. Boot-time PG ≥ 17 guard at lifespan (mirror #1187 pattern). Pinned by `tests/smoke/test_app_boots.py`.
8. Smoke verification against AAPL / GME / MSFT / JPM / HD (CLAUDE.md panel) post-merge: for each instrument, call `refresh_funds_current` and `refresh_institutions_current` twice in succession; assert `pgstattuple.table_len` delta == 0 + per-row `xmin::text` stability across the second call. Additionally call `refresh_treasury_current` and `refresh_def14a_current` for at least one instrument each to exercise the small-table helpers + their per-helper filters (Codex 1a LOW-4). EXPLAIN ANALYZE the repair-sweep predicate against a populated dev DB and record steady-state wall clock — confirms sub-second target (Codex 1d HIGH-2). PR description records each instrument's outcome, the EXPLAIN ANALYZE timing, and the commit SHA per CLAUDE.md ETL clause 12.
9. Parent spec amendment — [`docs/superpowers/specs/2026-05-19-data-retention-rubric.md`](2026-05-19-data-retention-rubric.md) §4.5 + §4.6 + §7 + §8 updated: PR12 status flipped to SHIPPED, writer-pattern + watermark side-table documented, Phase 2 (`<15 GB hot`) acceptance unblocked.
10. New prevention-log entries:
    - "MERGE WHEN NOT MATCHED BY SOURCE must carry the per-scope `AND tgt.<scope_col> = %(scope)s` clamp on BOTH the ON clause AND the DELETE clause (lint pins both literals)" — catastrophic data-loss class.
    - "Diff-aware writers (MERGE … IS DISTINCT FROM) MUST NOT include the `refreshed_at`-style update-timestamp column in the diff predicate — drift watermarks belong in a separate side-table". Includes the forever-loop failure mode discovered in PR12 Codex 1a.
11. Data-engineer skill update ([`.claude/skills/data-engineer/SKILL.md`](../../../.claude/skills/data-engineer/SKILL.md) + memory) — write-through section gains "diff-aware MERGE replaces DELETE+INSERT" rule + watermark side-table pointer + lint-guard pointer.
12. Codex 2 pre-push green; bot review APPROVE on latest commit; CI green; merge.

## 10. Acceptance — cross-reference to parent spec §8

PR12 alone does not move the post-merge dev DB size (per §8 above). Acceptance is measured AFTER the operator-driven §6.3 pre-wipe + clean re-run.

Per parent spec §8 acceptance tiers:

- **v1 bar (`<20 GB hot`)** — PR12 audit complete (this spec) + writer remediation merged + pre-wipe + clean re-run lands `ownership_funds_current + ownership_institutions_current` somewhere in the original 5.3 GB ballpark (Codex 1a LOW-3 — DB-size projections in this spec apply only to the two `_current` tables; other hot tables are governed by their respective PR caps).
- **Phase 2 stretch (`<15 GB hot`)** — PR12 writer fix + observations caps from PR6/PR7 + clean re-run lands combined `_current` ≤ 1 GB. The 2080 MB → ~250 MB compression on funds_current comes from: (a) 786k live rows × 280 B real-data row width = ~220 MB heap + ~80 MB indexes; (b) MERGE skipping no-op rewrites on business-identical refresh calls (primary saving). The repair-sweep watermark fix is a correctness fix that keeps the sweep cheap on healthy installs; it is not the headline bloat-reduction lever (Codex 1b LOW-4). ✅ Phase 2 unblocked by PR12.
- **Ambitious (`<10 GB hot`)** — requires further `companyfacts` tightening (Phase 3 ticket; out of #1233 scope).

PR12 measurement protocol post pre-wipe + clean re-run:

```sql
SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) AS sz, pg_total_relation_size(oid) AS bytes
FROM pg_class
WHERE relname IN ('ownership_funds_current', 'ownership_institutions_current', 'ownership_refresh_state')
ORDER BY pg_total_relation_size(oid) DESC;
```

Per-helper `pgstattuple` confirms `tuple_percent` ≥ 70% on `_current` tables (healthy density floor).

State-table acceptance uses a **dead-tuple bound** rather than a fixed tuple_percent floor (Codex 1c MED-3 — default autovacuum at `scale_factor=0.2 + threshold=50` fires at `dead_tup > 0.2 × 87,000 + 50 ≈ 17,450`, so the state table's tuple_percent can dip below 70% between vacuum cycles under bootstrap-rate churn even though steady-state remains healthy). Acceptance: `pgstattuple(ownership_refresh_state).dead_tuple_count ≤ live_tuple_count` post-autovacuum (≤50% dead-tuple ratio). If a future tuning pass shows this dips too low under production load, add per-table `ALTER TABLE ownership_refresh_state SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 100)` as a tightening knob — out of PR12 scope unless the dev-DB profile shows it during the §6.3 clean re-run.

## 11. Codex review gate

Per #1208 cadence + CLAUDE.md "Codex second-opinion — mandatory checkpoints":

- **Codex 1a** on this spec — completed; 5 HIGH + 7 MED + 4 LOW folded into rev 2.
- **Codex 1b** on rev 2 — completed; 3 HIGH + 7 MED + 4 LOW folded into rev 3.
- **Codex 1c** on rev 3 — completed; 1 HIGH + 3 MED + 3 LOW folded into rev 4.
- **Codex 1d** on rev 4 — completed; 3 HIGH + 3 MED + 1 LOW folded into rev 5 (this commit).
- **Codex 1e** on rev 5 (next).
- Codex 1f if needed.
- Spec lands as DOC PR (no code yet).
- Implementation plan = separate doc (`docs/superpowers/plans/2026-05-21-pr12-ownership-current-writer-merge-impl.md`). Codex 1a / 1b on the plan.
- PR12 implementation PR follows standard #1208 cadence: self-review → Codex 2 pre-push → push → bot review → resolve → APPROVE on latest commit → merge.

## 12. Handover for next session

State at this commit:

- Parent umbrella #1233 still OPEN. PR1-PR11 all SHIPPED (PR11 #1253 020e701 merged 2026-05-21).
- This is the DESIGN doc for PR12 (rev 5 post-Codex 1d).
- Branch: `feature/1233-pr12-design-spec`.
- Next: Codex 1e on rev 5; revise if needed; doc PR; then writing-plans skill → implementation plan → execution.

After PR12 merges, the operator triggers the §6.3 pre-wipe + clean re-run and #1233 closes per parent spec §8.
