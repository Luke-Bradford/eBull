# Manifest worker: recent-first drain slice (#1685)

**Status:** spec • **Issue:** #1685 • **Date:** 2026-06-20

## Problem (full-population verified, dev DB)

`run_manifest_worker` drains `filed_at ASC` (oldest-first) at `max_rows=100`/tick every 5 min against a **1,460,312-row** pending backlog. Every source has `newest_pending` at 2026-06-18/19 but `oldest_pending` years back — recents are discovered then never parsed:

| source | pending | oldest | newest |
|---|---|---|---|
| sec_form4 | 1,065,611 | 2018-02-23 | 2026-06-19 |
| sec_13f_hr | 199,650 | 2018-04-16 | 2026-06-18 |
| sec_10q | 61,931 | 2020-08-07 | 2026-06-18 |
| sec_form3 | 52,331 | 2021-03-25 | 2026-06-18 |
| sec_def14a | 33,117 | 2022-04-28 | 2026-06-18 |
| sec_13g | 32,123 | 2025-02-12 | 2026-06-18 |
| sec_13d | 15,549 | 2021-03-08 | 2026-06-19 |

Form 4 (73% of backlog) already has a dedicated newest-first lane (#1684); 13F/10Q/Form3/DEF14A/13G/13D have none → their recent events (8-K-class dividend/13D blockholder/DEF14A) stay months stale. Same starvation class as the Form 4 insider bug.

## Source rule
SEC imposes no ingest order; recency is our operational freshness strategy (recent filings carry the operator-relevant events). The #1684 spec (`docs/specs/ingest/2026-06-20-insider-recent-first-drain.md` L84-85) already prescribes the systemic fix: "a recent-first slice in `run_manifest_worker` — a `filed_at DESC` top-up alongside the oldest-first fairness path." **Full-population check:** the table above is the entire pending population (7 sources), not a sample.

## Settled-decisions / prevention-log applied
- **#1179 fairness** (`compute_quotas`): the recent slice REUSES `compute_quotas` for per-source fairness; no parallel allocator.
- **L1337**: the fairness test computes expected quotas via `compute_quotas(sorted(registered_parser_sources()), ...)` — new tests mirror that exact call, and seed rows with controlled `filed_at` so Phase R is deterministic.
- **L1712-1715 (lane-split monotonic regression): N/A** — Phase R runs INSIDE the same worker tick / same `sec_manifest` lane / same connection. No new concurrency, no new writer interleave.

## Design — Phase R (recent-first), then the unchanged backlog drain

In `run_manifest_worker` (fairness path, `source is None`), before Phase A:

- `RECENT_SLICE_ROWS = 30` (of `max_rows`), `RECENT_WINDOW = timedelta(days=90)`, `recent_cutoff = now - RECENT_WINDOW`.
- `recent_budget = min(RECENT_SLICE_ROWS, max_rows // 2)` — the `// 2` floor guarantees Phase A's `backlog_budget` stays `> 0` even when `max_rows` is small (Codex ckpt-1: a base-zero backlog would collapse the #1179 fairness rotation). The production scheduled tick is always `max_rows=100` → recent=30, backlog≥70.
- **Phase R**: `recent_quotas = compute_quotas(sources, recent_budget, tick_id)`; per source, `iter_pending_recent(conn, source=s, since=recent_cutoff, limit=q)` (`filed_at DESC`, floored at cutoff). Collect → `rows`, seed `seen`.
- `backlog_budget = max_rows - len(rows)` — **unused recent budget rolls into the backlog** (a source with no recent pending costs nothing).
- **Phase A** (oldest-first): `compute_quotas(sources, backlog_budget, tick_id)`; per source `iter_pending(source, q)` then `iter_retryable`, each **filtered `accession_number not in seen`** (dedup — only bites a tiny source whose entire pending set is recent; overlap is otherwise impossible since Phase R takes newest, Phase A oldest). Update `seen`.
- **Phase B**: unchanged (already `exclude_accessions=sorted(seen)`).

`source is not None` (per-source rebuild) path: **unchanged** — it is a targeted drain, not the steady-state freshness keeper.

### New iterator
`iter_pending_recent(conn, *, source: ManifestSource, since: datetime, limit: int)` — clone of `iter_pending`'s source-filtered branch with `AND filed_at >= %(since)s` and `ORDER BY filed_at DESC, accession_number DESC`. Same column list → `ManifestRow(**row)`.

### Migration 204 (index)
The per-source `WHERE ingest_status='pending' AND source=? AND filed_at>=? ORDER BY filed_at DESC` query must not scan 1M+ pending rows/tick. Add a partial index:
```sql
-- runner: autocommit
DROP INDEX CONCURRENTLY IF EXISTS idx_manifest_recent;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_manifest_recent
  ON sec_filing_manifest (source, filed_at DESC, accession_number DESC)
  WHERE ingest_status = 'pending';
```
`CONCURRENTLY` (no worker lock during the ~1.46M-row build) is why the file uses the `-- runner: autocommit` directive (`app/db/migrations.py` — CONCURRENTLY cannot run inside a tx block). The leading `DROP INDEX CONCURRENTLY IF EXISTS` clears any **invalid** index left by a prior interrupted concurrent build (Codex ckpt-1: bare `CREATE ... IF NOT EXISTS` would skip an invalid leftover, leaving it unused) before the rebuild; both statements are idempotent. `accession_number DESC` makes the documented tie-break fully index-ordered.

## Tradeoffs
- 30/100 rows/tick now chase recents → the historical backlog drains ~30% slower. Acceptable: backlog drain is the #1686 capacity concern (more workers / bigger tick), while recent-freshness is the operator-visible win. The split is a named constant, tunable.
- Phase R double-counts Form 4 with #1684's lane — harmless (idempotent `transition_status`; dedup is per-tick, the lane and worker don't overlap-write a single accession because `transition_status` is atomic per row).

## Tests
- `compute_quotas` unchanged (existing tests pass).
- New worker tests (seed with controlled `filed_at`, inject `tick_id` + `now`): Phase R selects the NEWEST per-source within window; a row older than `RECENT_WINDOW` is NOT picked by Phase R (still drained oldest-first); recent picks are de-duplicated from Phase A (no row dispatched twice); a source with zero recent pending yields its full budget to the backlog. Mirror L1337's `sorted(registered_parser_sources())`.
- **Backlog floor** (Codex ckpt-1): a fairness-path call with `max_rows < RECENT_SLICE_ROWS` (e.g. 4) still leaves `backlog_budget > 0` (the `// 2` cap) — Phase A is not starved to zero; assert backlog rows still selected.
- `iter_pending_recent`: one `db` test — DESC order + `since` floor + source filter.

## DoD (ETL clauses 8-12)
- **Migration applied** (dev): `idx_manifest_recent` exists; `run_migrations` clean.
- **Operator-visible freshness**: after wiring + a few ticks on dev, confirm a recent pending DEF14A/13D/13G (filed within 90d) transitions `pending→parsed` while the historical backlog is untouched at the tail. Record the accession + before/after `ingest_status`.
- **No data-treatment change**: #1685 reorders WHICH rows parse first; it does not change parser output, so no instrument-figure cross-source check applies (clauses 9/11 N/A — record why). Smoke panel (clause 8): N/A — no parser/schema change to ownership/fundamentals; the change is ingest-ordering only.
- Records commit SHA + the freshness observation.
