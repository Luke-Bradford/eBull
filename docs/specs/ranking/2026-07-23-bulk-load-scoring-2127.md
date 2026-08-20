# #2127 — Bulk-load the scoring reads: kill the per-instrument round-trip storm in `compute_rankings`

Status: spec (pre-impl, rev 2 post-Codex-ckpt1). Session 2ac5aa46, 2026-07-23.
Ticket: #2127 (#649-B). Scope reframed on evidence (operator-approved 07-23d) from
"skip unchanged instruments" to "bulk-read the scoring inputs". Skip-predicate is
deferred (see Out of scope).

## Problem

`compute_rankings` (`app/services/scoring.py:1803`) scores ~3,916 eligible
instruments each `morning_candidate_review` run in **772-844s** (`job_runs`). Per
instrument it calls `compute_score`, which does ~20 sequential per-instrument DB
queries split across two phases:

- `_load_instrument_data` (`scoring.py:1185`) — 12 input queries.
- `assemble_instrument_analytics` (`instrument_analytics.py:551`, called at
  `scoring.py:1743`) — 4 more per-instrument helpers (`_read_latest_two_fy_facts`,
  `get_insider_summary`, `_read_13f_delta`, `_read_short_interest`), each
  savepoint-wrapped.

cProfile (dev DB): **95% of `compute_score` wall-clock is `psycopg.connection.wait`**
— pure blocking on sequential round-trips, ~6.9ms each. Measured shares:

| phase | per-inst | × 3916 |
|---|---|---|
| `assemble_instrument_analytics` | 91 ms | **~357s** |
| `_load_instrument_data` + family scoring (pure) | ~106 ms | **~414s** |
| **total `compute_score`** | 197 ms | **~771s** (matches the 772-844s runs) |

The cost is **round-trip latency, not per-instrument compute** (`resolve_market_cap_basis`
= 0.8ms/inst). Evidence + full table: #2127 comments 5062616100 / this session.

## Source rule

- **Settled-decisions (`docs/settled-decisions.md:240-253`)**: the additive-nullable-
  under-stable-`metric_version` rule exists to *avoid* "a full-universe recompute of
  unchanged data". Cutting redundant round-trips is aligned, not a deviation.
- **ranking-engine skill invariants** — preserved; output must be **numerically
  identical at stored precision** (see Correctness on `now`). No `model_version`
  bump: the scoring formula is unchanged, only the DB access pattern.
  - No cohort-relative normalization in the headline score.
  - Penalties additive only. `scores` append-only; one full snapshot per run;
    `rank_delta` within a `model_version` vs most recent prior run only.
  - Each row carries full detail (`penalties_json`, `explanation`, `analytics`).
- **Per-input source rules (carried from the current SQL, not re-derived):**
  filing recency = SEC 10-K (Reg S-K annual) / 10-Q (Exchange Act §13 quarterly)
  incl. amendments (`scoring.py:1319-1327`); red_flag = `filing_events.red_flag_score`
  90d avg; valuation multiples from the `instrument_valuation` view; market-cap basis
  via `resolve_market_cap_basis` (#1662/#1664/#1623 — dual-class / FPI suppression,
  used as-is, not reasoned).

## Full-population verification (soundness of the reframe)

Risk thresholds (`scoring.py:276-277`) and Calmar thresholds (`:303-304`) are
**frozen module constants**, not per-run percentiles → `total_score(X)` depends only
on X's own inputs + `now`, never on the live universe. The only cross-sectional
computations (`compute_peer_grades` — evidence-only weight 0; `rank`/`rank_delta`)
already run over the full result set each run and are **unchanged**. So bulk-loading
X's inputs and scoring in Python yields the same `ScoreResult`. Proven by the
full-population A/B below.

## Design — two phases, two PRs

Both phases share one architectural seam:

### Seam (both PRs): make the scoring core pure

Extract `_score_from_data(instrument_id, data, weights, model_version, now, analytics)
-> ScoreResult` — the body of `compute_score` from `scoring.py:1490` onward, with the
`assemble_instrument_analytics(...)` call **hoisted out** and passed in as `analytics`.
Fully pure: no `conn`. (Codex ckpt-1 HIGH: the current body is NOT pure because it
calls `assemble_instrument_analytics(..., conn, ...)` — hoisting is what makes the
core bulk-able.)

`compute_score(instrument_id, conn, model_version)` stays as a thin back-compat
wrapper: `data = _load_instrument_data(...)`, `analytics = assemble_instrument_analytics(...)`,
`return _score_from_data(...)`. (Grep: `compute_score` has no caller outside
`compute_rankings` + tests, so this is purely for tests/safety.)

`_score_from_data` takes `now` as an argument (does not call `_utcnow()`), so the
whole batch shares one timestamp — see Correctness note 1.

### Phase 1 (PR-1): bulk `_load_instrument_data` — expected ~771s → ~380s (~2×)

`_bulk_load_instrument_data(conn, instrument_ids, now) -> dict[int, dict]` — one
set-based query per source, each `WHERE instrument_id = ANY(%(ids)s::bigint[])`
(prevention-log #1961: array param must carry the column type). Returns dicts of the
**same shape** as `_load_instrument_data`. Assembler seeds every requested id with the
default dict (`None`/`[]`/`0`/`False`), then overlays hits — so a missing id degrades
exactly as the per-instrument path.

| current per-inst query | bulk form (exact-equivalence notes) |
|---|---|
| latest-5 fundamentals | `ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY as_of_date DESC)` ≤ 5; **outer `ORDER BY instrument_id, as_of_date DESC`** so appended Python lists match `fund_rows[0]`=latest / `fund_rows[-1]`=oldest-of-5 |
| latest price row | `DISTINCT ON (instrument_id) ... WHERE close IS NOT NULL ORDER BY instrument_id, price_date DESC` — **`close IS NOT NULL` is load-bearing** (`scoring.py:1223`): a latest NULL close must not shadow an older usable row |
| latest quote | `DISTINCT ON (instrument_id) ... ORDER BY instrument_id, quoted_at DESC` |
| latest thesis | `DISTINCT ON (instrument_id) ... ORDER BY instrument_id, thesis_version DESC` |
| news 30d rows | `WHERE event_time >= now-30d AND sentiment_score IS NOT NULL`; **`ORDER BY instrument_id, event_time DESC, news_event_id DESC`** (deterministic tie-break; the sentiment aggregate is an importance-weighted sum → order-insensitive at stored precision, tie-break added for reproducibility) |
| red_flag AVG 90d | `GROUP BY instrument_id` AVG |
| fund_present EXISTS | `GROUP BY instrument_id` `bool_or(revenue_ttm IS NOT NULL AND (op OR gross))` |
| last 10-K/Q date | `GROUP BY instrument_id` MAX (filing_type IN 10-K/Q/A) |
| price_td COUNT | `GROUP BY instrument_id` `COUNT(close)` (counts non-NULL closes; NOT `COUNT(close IS NOT NULL)` — a boolean is non-null either way and would count all rows) |
| news 90d COUNT | `GROUP BY instrument_id` COUNT |
| valuation view row | `elig LEFT JOIN instrument_valuation` (degradable — see below) |
| risk metrics row | `elig LEFT JOIN instrument_risk_metrics_current` on `metric_version=%(mv)s AND window_key='3y'` (degradable) |
| sector + sic | **`FROM instruments i LEFT JOIN instrument_sec_profile p`** — MUST be LEFT JOIN so `i.sector` survives when no SEC profile exists (`scoring.py:1433`; INNER would null the peer-grade cohort) |

- **`resolve_market_cap_basis`**: keep per-instrument (0.8ms × 3916 ≈ 3s, negligible)
  inside the assembler. Bulking is a follow-up. Record in PR.
- **Partial-schema degradation** (test DBs lacking `instrument_valuation` /
  `instrument_risk_metrics_current` / `instrument_sec_profile`): each of those three
  bulk queries runs **inside its own `with conn.transaction()` savepoint** (Codex
  ckpt-1 MED: a bare try/except after a failed query leaves the tx aborted) catching
  `(UndefinedTable, UndefinedColumn)` → every id gets `None` for that block.

`compute_rankings` loop becomes:
```
now = _utcnow()
bulk = _bulk_load_instrument_data(conn, instrument_ids, now)
for iid in instrument_ids:
    try:
        analytics = assemble_instrument_analytics(iid, conn, gics_sector=..., shares_outstanding=...)  # still per-inst in P1
        results.append(_score_from_data(iid, bulk[iid], weights, model_version, now, analytics))
    except Exception:
        logger.warning(... skipping ...)   # preserve current per-instrument skip-on-error
```
Everything after (sort, peer grades, prior-rank fetch, insert tx) is unchanged.

### Phase 2 (PR-2): bulk the 4 analytics helpers — expected ~380s → ~60-90s (~10× total)

Add bulk variants of `_read_latest_two_fy_facts`, `get_insider_summary`,
`_read_13f_delta`, `_read_short_interest` (all `WHERE instrument_id = ANY(...)`), plus
`assemble_instrument_analytics_bulk(conn, ids, gics/shares maps) -> dict[int, dict]`.
`compute_rankings` then calls the bulk analytics once and passes each id's block to the
already-pure `_score_from_data`. Same A/B harness proves byte-identity. Deferred to a
second PR to keep each diff independently A/B-verifiable.

**Per-id error isolation (Codex ckpt-1 MED):** today one instrument failing analytics
is skipped via the per-instrument try/except in the loop. The bulk analytics MUST
preserve this — `assemble_instrument_analytics_bulk` returns a per-id map and any id
whose block cannot be built degrades to the empty-analytics default for that id (does
NOT raise), so a single bad id can never fail the whole run. The bulk reads themselves
still run inside savepoints (partial-schema degradation as in Phase 1).

## Correctness notes

1. **One `now` per batch (deliberate consistency improvement, documented — NOT a
   silent change).** Current `compute_rankings` calls `_utcnow()` once *per instrument*,
   so over a ~13-min run instruments drift apart in `now`; across a 30d/90d window
   anniversary or a UTC-midnight `filing_date` boundary this makes late-scored
   instruments see a later cutoff. Batch-`now` (captured at run start) scores every
   instrument "as of run start" — reproducible and strictly more consistent. No
   `model_version` bump: `now` is an execution input, not a metric computation (running
   the job at a different clock time already changes windowed results run-to-run).
   The A/B (below) pins one `now` for BOTH paths to prove the refactor math is
   identical; the only production delta vs the current path is elimination of the
   intra-run now-drift, which is called out in the PR.
2. **`ANY(%(ids)s::bigint[])`** — array param carries the column type (prevention-log #1961).
3. **Ordering** — every "latest-N"/list query carries an explicit outer `ORDER BY` so
   Python list assembly is deterministic and matches the current `ORDER BY ... LIMIT`.
4. **Memory** — 3,916 × ~13 small dicts ≪ 100 MB.

## Acceptance — full-population A/B (definition-of-done clauses 9/11)

Script (dev DB, `scratchpad`): pin one `now`; for ALL eligible instruments (n≈3917)
build `ScoreResult` via (a) current per-instrument path and (b) the bulk path, both at
the pinned `now`. Assert **identical** per id on the FULL result: every family score,
`raw_total`, `total_penalty`, `total_reward`, `total_score`, `data_completeness`,
`completeness_tier`, `sector`, `explanation`, the sorted `penalties`/`rewards` name+value
set, and the `analytics` dict (Codex ckpt-1 LOW: acceptance must cover explanation /
rewards / analytics / sector, not just total_score). Report matched/diverged; **target
3917/3917**. Then time full `compute_rankings` old vs new; record wall-clock in the PR.
Cross-source (clause 9): confirm one instrument's `total_score` unchanged vs its last
live `scores` row (e.g. AAPL).

## Test plan (lean)

- **Pure (fast tier)**: `TestScoreFromData` — `_score_from_data` + `_analytics_inputs`
  on hand-built `data`/`analytics` dicts (no DB). `TestComputeRankings` repointed off
  the removed `compute_score` call to the new internals (`_stub_scoring` helper).
- **SQL-mechanism guard = the full-population same-process A/B** (below), not a
  fixture db-test. The scoring suite has no DB harness; a 2-3 row fixture would be
  strictly weaker than proving `_bulk_load_instrument_data(conn, ids)[iid] ==
  _load_instrument_data(conn, iid)` for all ~3,916 instruments against the real dev
  DB in one process. This is the CLAUDE.md "lean on dev-verify" path. Conscious
  tradeoff: no committed db regression test; the A/B is recorded in the PR.

## Out of scope (deferred)

- **Incremental skip predicate** (original #2127 title). Revisit only if bulk-load
  misses the target after re-measure. Its cost (schema: `fundamentals_snapshot` ingest
  ts + `filing_events` red_flag watermark; 4 correctness hazards incl. time-decay
  boundaries; fail-open design) is likely unjustified once loads are ~60-90s. Kept as
  a follow-up note on #2127.
- Bulking `resolve_market_cap_basis` (3s, follow-up if needed).

## Operator runbook

Pure compute refactor, no schema/parser/ownership change → ETL clauses 8-12 and the
sec_rebuild runbook do NOT apply. Verification = full-pop A/B + one live
`compute_rankings` timing on dev, recorded in each PR. Restart the jobs daemon after
merge so scheduled `morning_candidate_review` runs the new path.
