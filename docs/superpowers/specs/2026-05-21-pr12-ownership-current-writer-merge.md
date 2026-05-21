# PR12 — `ownership_*_current` writer rewrite (DELETE+INSERT → diff-aware MERGE)

> Created: **2026-05-21** as the final PR in the data-retention rubric umbrella.
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
>
> Parent spec: [`docs/superpowers/specs/2026-05-19-data-retention-rubric.md`](2026-05-19-data-retention-rubric.md) §4.5 / §4.6 / §6.4 / §7 / §8.
>
> Status: **DESIGN** — drafted post-PR11 (SHIPPED #1253 020e701). PR11 closed the SC 13D/G activation work; PR12 is the last entry in §7 implementation sequence. After PR12 lands, the operator triggers the §6.3 pre-wipe + clean re-run, and the parent umbrella #1233 closes.

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

Schema invariants verified across all 7 `_current` tables:

```text
 table                          | PK
 ownership_funds_current        | (instrument_id, fund_series_id)
 ownership_institutions_current | (instrument_id, filer_cik, ownership_nature, exposure_kind)
 ownership_insiders_current     | (instrument_id, holder_identity_key, ownership_nature)
 ownership_blockholders_current | (instrument_id, reporter_cik, ownership_nature)
 ownership_def14a_current       | (instrument_id, holder_name_key, ownership_nature)
 ownership_esop_current         | (instrument_id, plan_name)
 ownership_treasury_current     | (instrument_id)
```

All 7 carry `refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()`. All 7 lead PK with `instrument_id`. All 7 are written exclusively by sibling `refresh_*_current(conn, *, instrument_id) -> int` helpers in [`app/services/ownership_observations.py`](../../../app/services/ownership_observations.py).

Postgres confirmed at **17.9** in dev (`PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1)`). `MERGE` with `WHEN NOT MATCHED BY SOURCE` is supported.

## 1. Problem statement

The 7 `refresh_*_current` helpers share an identical pattern:

```python
with conn.transaction(), conn.cursor() as cur:
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended('refresh_X_current', 0) # %s::bigint)", (instrument_id,))
    cur.execute("DELETE FROM ownership_X_current WHERE instrument_id = %s", (instrument_id,))
    cur.execute("INSERT INTO ownership_X_current (...) SELECT DISTINCT ON (...) FROM ownership_X_observations WHERE instrument_id = %s AND known_to IS NULL ORDER BY ...", (instrument_id,))
    cur.execute("SELECT COUNT(*) FROM ownership_X_current WHERE instrument_id = %s", (instrument_id,))
```

Every call manufactures **N dead tuples** (N = rows in the instrument's latest-set) regardless of whether the resolved set actually changed. Bootstrap drains call the helper after every observation batch:

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
| `app/jobs/ownership_observations_repair.py:70` | `refresh_institutions_current` | one-shot repair |

`ownership_funds_current` bloats worst because each instrument's latest-set carries ~86 fund-series rows vs ~5-15 institutional-filer rows. Per-call dead-tuple bursts are 6-15x larger for funds than institutions; autovacuum's `vacuum_scale_factor=0.2` cannot keep pace with bootstrap-rate churn.

Result: `ownership_funds_current` 10.22% tuple density. `ownership_institutions_current` 77.35% — healthy today but the same pattern, primed to degrade if any future write-through-bound source widens its per-instrument fanout (PR12's preventive scope).

## 2. Non-goals

- Row deletion of pre-existing rows (parent spec §6.3 — operator pre-wipe handles reshape).
- `VACUUM FULL` against existing tables (operator-driven, separate event; pre-wipe TRUNCATEs anyway).
- Schema changes — PK shapes, column lists, partitioning all unchanged.
- Frontend / API surface — `_current` reads unaffected (same row shapes, same indexes, same PKs).
- Writer signature changes — `refresh_X_current(conn, *, instrument_id) -> int` preserved for every caller.
- Cap-shape changes — observations write-side caps from PR1–PR11 stand as-is.

## 3. Two-axis decision

### 3.1 Writer pattern — diff-aware MERGE

Single MERGE statement per helper. `WHEN MATCHED AND (...) IS DISTINCT FROM (...)` skips writes when business columns are identical; `WHEN NOT MATCHED BY TARGET` inserts new rows; `WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %s` deletes rows that fall out of the latest set.

PG17 `WHEN NOT MATCHED BY SOURCE` is load-bearing — PG15/16 only have `BY TARGET`, which forces a two-statement workaround. Dev DB is PG17.9; boot-time guard added (§7).

### 3.2 Migration scope — all 7 helpers

Per parent spec §7 "ownership_*_current size audit + remediation" and `feedback_no_punting_complete_work` ("default to full scope; no multiple-choice"). Uniform pattern, single lint guard, no tech-debt followups. Tiny tables get preventive fix at near-zero cost.

## 4. Writer rewrite

Each helper becomes one MERGE statement inside the existing transaction + advisory lock. Template (`ownership_funds_current` shown; per-helper differences in PK / DISTINCT ON / column list only):

```python
def refresh_funds_current(conn: psycopg.Connection[Any], *, instrument_id: int) -> int:
    """Deterministically reconcile ``ownership_funds_current`` rows for
    one instrument from its observations.

    Diff-aware MERGE: UPDATE only when business columns IS DISTINCT FROM
    the new set; INSERT new rows; DELETE rows that fall out of the
    latest set (NOT MATCHED BY SOURCE scope-clamped to this instrument).
    refreshed_at is excluded from the diff predicate so idempotent
    re-runs do not manufacture dead tuples (PR12 fix for the
    ownership_funds_current bloat surfaced post-PR11)."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_funds_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )
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
            ON tgt.instrument_id = src.instrument_id
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
            "SELECT COUNT(*) FROM ownership_funds_current WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
```

### 4.1 Per-helper differences

| Helper | PK | DISTINCT ON | Notes |
| --- | --- | --- | --- |
| `refresh_insiders_current` | `(instrument_id, holder_identity_key, ownership_nature)` | `(holder_identity_key, ownership_nature)` | Keeps cross-source priority CASE chain in `ORDER BY` (Form 4 > Form 3 > 13D/G > DEF14A > 13F > N-PORT/N-CSR > XBRL DEI > 10-K note > FINRA SI). |
| `refresh_institutions_current` | `(instrument_id, filer_cik, ownership_nature, exposure_kind)` | `(filer_cik, ownership_nature, exposure_kind)` | Three-tuple grouping preserves EQUITY/PUT/CALL split. |
| `refresh_blockholders_current` | `(instrument_id, reporter_cik, ownership_nature)` | `(reporter_cik, ownership_nature)` | Per-helper ORDER BY preserved. |
| `refresh_def14a_current` | `(instrument_id, holder_name_key, ownership_nature)` | `(holder_name_key, ownership_nature)` | Existing legacy-ESOP exclusion in USING WHERE preserved. |
| `refresh_esop_current` | `(instrument_id, plan_name)` | `(plan_name)` | Smallest column list. |
| `refresh_treasury_current` | `(instrument_id)` | `(instrument_id)` | Single-row-per-instrument table; MERGE still applies but DELETE branch fires only on shrinkage. |
| `refresh_funds_current` | `(instrument_id, fund_series_id)` | `(fund_series_id)` | Template above. |

### 4.2 Diff-predicate column list contract

`refreshed_at` is **excluded** from the `IS DISTINCT FROM` tuples on both sides. Including it would defeat the no-op optimisation — `now()` on the source side always differs from the stored value, every MATCHED row would re-fire UPDATE, dead tuples back to N per call.

`refreshed_at` is **set** in the UPDATE SET clause when the WHEN MATCHED + DISTINCT predicate fires. When business columns are unchanged, the row is skipped entirely → `refreshed_at` stays at the prior pass's value. Operator-visible semantics: `refreshed_at` advances only when an actual business-data change lands, not on every refresh attempt.

INSERT branch omits `refreshed_at` → column `DEFAULT now()` fires.

### 4.3 Scope clamp — load-bearing

`WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE` is the per-instrument scope clamp. Without the `AND tgt.instrument_id = %(iid)s` predicate, MERGE walks the **entire target table** and DELETEs every row not in the (single-instrument) source — catastrophic global data loss. Pinned by lint invariant D (§5). Also pinned at runtime by the **scope clamp** test case (§6).

### 4.4 Concurrency

Per-instrument `pg_advisory_xact_lock` serialises concurrent refreshes for the same `(helper, instrument_id)` pair. Disjoint instrument refreshes run in parallel; PG MERGE takes row-level locks on the rows it touches → disjoint instrument_id rows do not block each other. Transaction wraps the MERGE so a failure rolls back both the advisory lock acquisition and any partial MERGE writes.

## 5. Lint guard

New script: [`scripts/check_ownership_refresh_writer_pattern.sh`](../../../scripts/check_ownership_refresh_writer_pattern.sh). Wired into [`.githooks/pre-push`](../../../.githooks/pre-push) after PR11's `check_13dg_retention.sh`.

Awk-based function-body block walker (PR4 Codex 1c lesson) so reformatting cannot trip the guard; empty-grep `wc -l` guards (PR10a Codex iter 1 lesson) so missing-anchor doesn't silently pass.

Per helper (×7: `insiders / institutions / blockholders / treasury / def14a / funds / esop`):

| Invariant | What it pins | How |
| --- | --- | --- |
| **A** | Helper defined exactly once in `app/services/ownership_observations.py` | grep `^def refresh_<X>_current\(` → count == 1 |
| **B** | No legacy `DELETE FROM ownership_<X>_current` inside helper body | awk-extract body span `def refresh_<X>_current` → next `^def `; grep inside → count == 0 |
| **C** | `MERGE INTO ownership_<X>_current AS tgt` opener present | grep inside body span → count == 1 |
| **D** | `WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id` clamp present (catastrophic data-loss class) | grep inside body span → count == 1 |
| **E** | `refreshed_at` NOT inside diff predicate's `IS DISTINCT FROM` tuples | awk-extract span from `WHEN MATCHED AND (` to `) THEN UPDATE`; grep inside → count == 0 |
| **F** | `pg_advisory_xact_lock` preserved on function-namespace hash | grep `hashtextextended\('refresh_<X>_current'` inside body → count == 1 |
| **G** | DISTINCT ON columns match per-helper expected tuple | grep `DISTINCT ON \(<expected cols>\)` inside body → count == 1 |

7 helpers × 7 invariants = 49 checks. Pure text walk, no DB dependency, sub-second runtime.

Lint also audits the **legacy `DELETE FROM ownership_*_current`** pattern anywhere under `app/` (outside the 7 helper bodies) — count == 0. Catches a future regression where a sibling service tries to inline its own DELETE+INSERT against `_current`.

## 6. Tests

New file: [`tests/test_ownership_refresh_writer_merge.py`](../../../tests/test_ownership_refresh_writer_merge.py). 5 cases × 7 helpers = 35 parametrised cases.

`pg_stat_user_tables` rejected as the no-op-churn assertion source (async stats-collector flush has several-hundred-ms lag → flaky). Switched to `pgstattuple` + `xmin` per row.

| Case | Setup | Action | Assertion |
| --- | --- | --- | --- |
| **insert** | empty `_current` for instrument; write 1 observation | `refresh_X_current(conn, instrument_id=I)` | row present; `SELECT xmin FROM _current WHERE instrument_id=I` returns a fresh xid |
| **no-op churn** | row present from prior refresh; no observation change; capture `xmin0` + `pgstattuple(table).{table_len, tuple_count}` | `refresh_X_current(I)` again | `xmin == xmin0` (row not rewritten); `pgstattuple(table).table_len` unchanged; `dead_tuple_count` delta == 0 — **load-bearing; proves the bloat fix** |
| **update (amendment)** | row present `xmin0`; insert later-`filed_at` observation, same PK, different `shares` | refresh | row present; `xmin > xmin0`; `shares` reflects amendment; `refreshed_at > refreshed_at0` |
| **delete (`known_to` expiry)** | row present; `UPDATE _observations SET known_to = now() WHERE …` | refresh | row absent |
| **scope clamp** | rows present for instrument A and instrument B; capture B's `xmin0` | `refresh_X_current(I=A)` with no observation change | B's row still present; B's `xmin == xmin0`. **Pins `AND tgt.instrument_id = %s` SOURCE clamp** |

`refreshed_at` invariants:
- **no-op churn**: `refreshed_at` unchanged post-refresh (proves diff-predicate excludes it).
- **update**: `refreshed_at` advances (proves UPDATE SET sets it).

`pgstattuple` requires `CREATE EXTENSION` — [`tests/fixtures/ebull_test_db.py`](../../../tests/fixtures/ebull_test_db.py) template provisioning gains the extension. Single edit; inherited by every per-worker DB via template clone (#893 architecture).

## 7. Postgres version guard

`MERGE WHEN NOT MATCHED BY SOURCE` requires PG ≥ 17. Without a boot-time guard, a PG16 deployment would pass lint and crash at first refresh with `syntax error at or near "BY"`.

Mirror #1187's `max_locks_per_transaction` boot pattern. New cross-cutting check at lifespan startup — either extends [`app/main.py`](../../../app/main.py) lifespan or sits as a sibling in `app/system/postgres_health.py`. Assertion: `SELECT current_setting('server_version_num')::int >= 170000`. Fail-closed: refuse to boot under PG < 17. Smoke test `tests/smoke/test_app_boots.py` exercises lifespan → guard gated by the pre-push smoke gate.

Single-fact assertion; cross-cutting; minimal blast radius.

## 8. Operator semantics

PR12 ships the **writer**. It does NOT shrink the pre-existing dead space:
- `ownership_funds_current` heap stays at 2080 MB post-merge.
- `ownership_institutions_current` heap stays at 376 MB post-merge.
- `GET /system/postgres-health` (Phase 4 of #1208) continues to report `db_size ≈ 41 GB` until the §6.3 operator pre-wipe.

Reclaim path is the **operator pre-wipe + clean re-run** (parent spec §6.3 + §8 acceptance). PR12's contribution is ensuring the clean re-run does not re-bloat the new write-through pattern.

No `DELETE FROM ownership_*_current` lands in this PR. The MERGE's `WHEN NOT MATCHED BY SOURCE THEN DELETE` is steady-state writer behaviour identical to today's DELETE+INSERT (a row drops out of `_current` only when its only observation expires via `known_to` or when caps shed it post-wipe). Read-side `_current` consumers see no behavioural change — same row shapes, same dedup ordering, same priorities, same indexes.

## 9. Definition of done

CLAUDE.md §"Definition of done" + §"ETL / parser / schema-migration additional clauses" both apply.

1. 7 `refresh_*_current` helpers in [`app/services/ownership_observations.py`](../../../app/services/ownership_observations.py) rewritten to single-statement MERGE per §4 template. Signature `(conn, *, instrument_id) -> int` preserved.
2. [`scripts/check_ownership_refresh_writer_pattern.sh`](../../../scripts/check_ownership_refresh_writer_pattern.sh) (49 checks: 7 helpers × 7 invariants A-G) wired into [`.githooks/pre-push`](../../../.githooks/pre-push) after `check_13dg_retention.sh`.
3. [`tests/test_ownership_refresh_writer_merge.py`](../../../tests/test_ownership_refresh_writer_merge.py) — 35 parametrised cases. Load-bearing no-op-churn case uses `xmin` + `pgstattuple`.
4. [`tests/fixtures/ebull_test_db.py`](../../../tests/fixtures/ebull_test_db.py) template provisions `pgstattuple` extension.
5. Boot-time PG ≥ 17 guard at lifespan (mirror #1187 pattern). Pinned by `tests/smoke/test_app_boots.py`.
6. Smoke verification against AAPL / GME / MSFT / JPM / HD (CLAUDE.md panel) — for each instrument, call `refresh_funds_current` and `refresh_institutions_current` twice in succession; assert `pgstattuple.table_len` delta == 0 + `xmin` stability per row across the second call. PR description records each instrument's outcome and the commit SHA per CLAUDE.md ETL clause 12.
7. Parent spec amendment — [`docs/superpowers/specs/2026-05-19-data-retention-rubric.md`](2026-05-19-data-retention-rubric.md) §4.5 + §4.6 + §7 + §8 updated: PR12 status flipped to SHIPPED, writer-pattern documented, Phase 2 (`<15 GB hot`) acceptance unblocked.
8. New prevention-log entry — "MERGE WHEN NOT MATCHED BY SOURCE must carry the per-scope `AND tgt.<scope_col> = %(scope)s` clamp" (catastrophic data-loss class).
9. Data-engineer skill update ([`.claude/skills/data-engineer/SKILL.md`](../../../.claude/skills/data-engineer/SKILL.md) + memory) — write-through section gains "diff-aware MERGE replaces DELETE+INSERT" rule + lint-guard pointer.
10. Codex 2 pre-push green; bot review APPROVE on latest commit; CI green; merge.

## 10. Acceptance — cross-reference to parent spec §8

PR12 alone does not move the post-merge dev DB size (per §8 above). Acceptance is measured AFTER the operator-driven §6.3 pre-wipe + clean re-run, which lands the bounded set under all PR1-PR12 caps.

Per parent spec §8 acceptance tiers:

- **v1 bar (`<20 GB hot`)** — PR12 audit complete (this spec) + writer remediation merged + pre-wipe + clean re-run lands `ownership_funds_current + ownership_institutions_current` somewhere in the original 5.3 GB ballpark (writer fix alone, no cap shrinkage on either source — per spec §4.5 and §4.6 the observations caps were already shipped via PR6 + PR7). Acceptable.
- **Phase 2 stretch (`<15 GB hot`)** — PR12 writer fix + observations caps from PR6/PR7 + clean re-run lands combined `_current` ≤ 1 GB. The 2080 MB → ~250 MB compression on funds_current comes from: (a) 786k live rows × 280 B real-data row width = ~220 MB heap + ~80 MB indexes; (b) zero bloat under diff-aware MERGE. ✅ Phase 2 unblocked by PR12.
- **Ambitious (`<10 GB hot`)** — requires further `companyfacts` tightening (Phase 3 ticket; out of #1233 scope).

PR12 measurement protocol post pre-wipe + clean re-run:

```sql
SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) AS sz, pg_total_relation_size(oid) AS bytes
FROM pg_class
WHERE relname IN ('ownership_funds_current', 'ownership_institutions_current')
ORDER BY pg_total_relation_size(oid) DESC;
```

Plus per-helper pgstattuple to confirm tuple_percent ≥ 70% (healthy density floor).

## 11. Codex review gate

Per #1208 cadence + CLAUDE.md "Codex second-opinion — mandatory checkpoints":

- **Codex 1a** on this spec (after first commit on `feature/1233-pr12-design-spec`).
- Revise per findings.
- **Codex 1b** on revised spec.
- Revise.
- **Codex 1c** if needed.
- Spec lands as DOC PR (no code yet).
- Implementation plan = separate doc (`docs/superpowers/plans/2026-05-21-pr12-ownership-current-writer-merge-impl.md`). Codex 1a / 1b on the plan.
- PR12 implementation PR follows standard #1208 cadence: self-review → Codex 2 pre-push → push → bot review → resolve → APPROVE on latest commit → merge.

## 12. Handover for next session

State at this commit:

- Parent umbrella #1233 still OPEN. PR1-PR11 all SHIPPED (PR11 #1253 020e701 merged 2026-05-21).
- This is the DESIGN doc for PR12. Implementation has not started.
- Branch: `feature/1233-pr12-design-spec`.
- Next: Codex 1a on this spec; revise; Codex 1b; doc PR; then writing-plans skill → implementation plan → execution.

After PR12 merges, the operator triggers the §6.3 pre-wipe + clean re-run and #1233 closes per parent spec §8.
