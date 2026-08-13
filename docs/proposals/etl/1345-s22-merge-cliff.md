# #1345 — S22 MERGE perf cliff: batch the sweep refresh path

Status: PROPOSAL (pre-code). Spec-first per CLAUDE.md. Phase 4 lead.

## 1. Problem — issue is stale; one real gap remains

Issue #1345 listed 3 fixes. Current `main` state (grep-verified `b9924f3`):

| #1345 fix | Status | Evidence |
|---|---|---|
| Fix 1 `SET LOCAL jit = off` | **SHIPPED** #1346 / PR #1371 (`f964f5a`) | jit=off in all 10 refresh helpers ([ownership_observations.py:232](../../../app/services/ownership_observations.py) etc.). The ~307 ms JIT/MERGE is gone. |
| Fix 3 batch MERGE | **PARTIAL** #1233 PR-4 / PR #1284 | `refresh_{insiders,institutions,funds}_current_batch` exist but are wired **only** into the bulk-dataset orchestrator ([sec_bulk_orchestrator_jobs.py:483](../../../app/services/sec_bulk_orchestrator_jobs.py)). |
| Fix 2 drop empty partitions | **WON'T DO** | Reverses settled committee decision DE BL-2 (sql/177 extended partitions to 2040 deliberately to avoid DEFAULT-bucket pruning loss). Out of scope. |

**The real remaining gap:** the S22 `sec_13f_recent_sweep` and the daily repair sweep still refresh `_current` **one instrument at a time**:

- Sync sweep: [`_refresh_for_instruments`](../../../app/services/ownership_observations_sync.py) loops `refresh_fn(conn, instrument_id=iid)` per instrument (sync.py:111). All 7 `sync_*` callers pass the per-instrument helper.
- Repair sweep: [`run_observations_repair_sweep`](../../../app/jobs/ownership_observations_repair.py) loops `for instrument_id in drifted: refresh_fn(conn, instrument_id)` (repair.py:181).

### Why per-instrument is the cliff (root cause, refined)

- `ownership_*_current` (MERGE **target**) is **unpartitioned** (sql/114:106) — small.
- `ownership_*_observations` (MERGE **source**) is partitioned to **~124 quarters** (sql/177).
- The MERGE source filters by `instrument_id`, which is **NOT the partition key** (`period_end` quarter is). So every MERGE **probes the index of all ~124 partitions** → the `shared hit=18825` buffers in the issue's EXPLAIN. jit=off does **not** remove this; **batching amortises the 124-partition probe over N instruments** (one probe-set per batch, not per instrument).

This is the same win PR-4 already realised on the bulk path. This spec extends it to the two sweep paths that #1345's headline measurement (Run #8, S22 = 344 min) actually exercised.

## 2. Scope — split into 2 PRs (Codex CTO committee call)

Perf is unmeasurable at merge (dev PG fresh-empty) and the headline win lives in the *wiring*, not the helpers. Split so correctness and operational routing are independently provable with a clean rollback boundary:

### PR-A — batch helpers + lint + tests (NO sweep behaviour change)
1. Add 4 batch variants: `refresh_{blockholders,treasury,def14a,esop}_current_batch`. Each MUST prepend `instrument_id` to `DISTINCT ON`, `ORDER BY`, and the `ON` equi-join, and port **all** category-specific predicates verbatim (§4.1) — the per-instrument keys omit `instrument_id` because they are partition-of-one; a multi-instrument source that omits it collapses rows across instruments.
2. Extend `scripts/check_ownership_refresh_writer_pattern.sh` with batch-specific assertions for all **7** `*_batch` helpers. (Today the lint covers the 3 existing batch helpers for **invariant I only** — column-set diff shape; it does NOT assert the batch-specific `ANY(%(ids)s)` SOURCE-delete clamp, jit=off, `executemany` state-upsert, ordered-`unnest` lock, or the `instrument_id`-prefixed DISTINCT ON / def14a esop-regex predicate.)
3. Add 4 `BatchHelperCase` entries + 4 bespoke `_seed_one` branches (§6) → the 6 parametrised contracts apply to all 7 categories.

### PR-B — sweep wiring + #1382
4. Route sync sweep (`_refresh_for_instruments`, 6 call-sites across 5 `sync_X` funcs) + repair sweep (`run_observations_repair_sweep`, `_CATEGORIES`) through the batch helpers via the shared try-batch-then-per-instrument-fallback helper (§4.2/§4.3; whole-set, no chunking).
5. Add repair-sweep orphan-sink (`CategoryRepairStats.failed_instruments`) — fallback must not silently swallow permanent failures (§4.3).
6. Add wiring-regression tests proving both sweeps invoke the batch path once with the full id-set (§6) — without this the headline goal is unguarded.
7. Fix #1382 (`check_13f_hr_retention.sh` invariant H: relax expected per-row `record_institution_observation(source='13f')` count 3→2; the bulk path is gated by invariant E). Empirically confirmed the hook trips on `main` (finds 2, expects 3). In PR-B because the wiring touches 13F sweep code.

**Out:**
- Fix 2 partition surgery (settled-decision conflict).
- `ingest_all_active_filers` parallel HTTP (#1274/#482 — separate Phase 4 issue).
- Bulk-dataset ingester batching (#1276 — separate).
- Cohort recency trims (#1350/#1351 — measurement-gated).
- `funds` is **not** in `sync_all` (5-category sync vs 7-category repair — by design, data-engineer SKILL §write-through); `refresh_funds_current_batch` is wired only into the repair sweep.

## 3. Settled-decisions + prevention-log applicability

**Settled-decisions:** No entry governs the sweep refresh path directly. sql/177 DE BL-2 (partition-to-2040) is **preserved** — this spec does not touch partitions. The 5-category `sync_all` vs 7-category repair asymmetry (data-engineer SKILL §write-through) is **preserved** — batch wiring is 1:1 per existing caller, no category added/removed.

**Prevention-log — directly applicable:**
- **L1496** (SOURCE-clamp on BOTH ON + DELETE): batch clamp is `tgt.instrument_id = ANY(%(ids)s::bigint[])` in `NOT MATCHED BY SOURCE`. The 3 existing variants already do this (institutions.py:2034). The 4 new ones MUST.
- **L1501** (single-PK equi-join): `treasury` PK is `(instrument_id)`. Per-instrument helper builds its source via `SELECT DISTINCT ON (instrument_id) … WHERE instrument_id = %(iid)s ORDER BY instrument_id, period_end DESC, filed_at DESC, source_document_id ASC` (observations.py:904-916), ON = `tgt.instrument_id = src.instrument_id`. The **batch** variant keeps `DISTINCT ON (instrument_id)` (NOT `GROUP BY` — grouping can't pick a deterministic full row and would silently corrupt `_current`; Codex ckpt1 BLOCKING) and only widens the predicate to `WHERE instrument_id = ANY(%(ids)s::bigint[])`. DISTINCT ON yields exactly one full row per instrument → N×1 cardinality holds; ON stays the equi-join. **Highest-risk item — flagged for committee.**
- **L1505** (no `refreshed_at` in diff predicate; watermark side-table captured pre-MERGE): batch variants capture `watermarks` dict pre-MERGE then `executemany` the state UPSERT (institutions.py:2041). New 4 MUST follow.
- **L1503** lint `check_ownership_refresh_writer_pattern.sh` — extended here (§4.4).
- **"dev-sized EXPLAIN ≠ prod-sized EXPLAIN"** — MANDATORY new entry (§5).

## 4. Design

### 4.1 Four new batch variants

Mirror `refresh_institutions_current_batch` exactly:
- `_normalise_instrument_ids` guard → `[]` short-circuit.
- `SET LOCAL jit = off`.
- Ordered `pg_advisory_xact_lock` over `unnest(ids)` (deadlock-safe — L… existing pattern).
- Pre-MERGE `watermarks` dict via `GROUP BY instrument_id`, but the state-UPSERT row-set is `[(iid, watermarks.get(iid)) for iid in ids]` — iterate the **input ids**, NOT the GROUP BY result, so an instrument with zero surviving observations still gets a watermark row (`None`) and is not left drifting forever (matches the per-instrument helper). (DE committee.)
- MERGE with `WHERE instrument_id = ANY(%(ids)s::bigint[])` source predicate.
- `NOT MATCHED BY SOURCE AND tgt.instrument_id = ANY(%(ids)s::bigint[]) THEN DELETE`.
- `executemany` state UPSERT, one row per id.

**Per-category specifics — verified against the per-instrument helper bodies. CRITICAL (DE+ckpt1 BLOCKING): the per-instrument helpers omit `instrument_id` from `DISTINCT ON`/`ORDER BY` because each is partition-of-one; the batch variants MUST prepend `instrument_id` to `DISTINCT ON`, the leading `ORDER BY` key, AND carry `tgt.instrument_id = src.instrument_id` in `ON` — else rows collapse across instruments and one issuer's holder is silently dropped.**

| cat | per-instrument DISTINCT ON | batch DISTINCT ON | extra WHERE (port verbatim) | ON join cols |
|-----|---------------------------|-------------------|-----------------------------|--------------|
| blockholders (obs.py:718) | `(reporter_cik, ownership_nature)` | `(instrument_id, reporter_cik, ownership_nature)` | `known_to IS NULL` | `instrument_id, reporter_cik, ownership_nature` |
| treasury (obs.py:902) | `(instrument_id)` | `(instrument_id)` (already) | `known_to IS NULL AND treasury_shares IS NOT NULL` | `instrument_id` only (single-PK; lint D1=0) |
| def14a (obs.py:1130) | `(holder_name_key, ownership_nature)` | `(instrument_id, holder_name_key, ownership_nature)` | `known_to IS NULL AND shares IS NOT NULL AND holder_role IS DISTINCT FROM 'esop' AND holder_name !~* %(esop_regex)s` — **the `%(esop_regex)s` named param MUST coexist with the `%(ids)s` array (psycopg3 named-style throughout; never mix positional). Dropping any of the 3 extra clauses re-introduces the #843 ESOP-double-count bug.** | `instrument_id, holder_name_key, ownership_nature` |
| esop (obs.py:1589) | `(plan_name)` | `(instrument_id, plan_name)` | per helper (positive `shares`, non-empty `plan_name` enforced at record-time) | `instrument_id, plan_name` (`plan_name` alone is NOT instrument-scoped — two issuers sharing "401(k) Plan" would cross-match without the `instrument_id` equi-join) |

`ON` equi-join (`tgt.instrument_id = src.instrument_id`) replaces the per-instrument `tgt.instrument_id = %(iid)s` const-clamp (lint D1=0 for batch); the `ANY(%(ids)s)` clamp stays on `NOT MATCHED BY SOURCE` (D2=1). Equivalence holds for all 7 given the prefix fix (DE verified: tie-breaks fully deterministic via `source_document_id ASC` final key; NUMERIC `shares` / NULL `market_value_usd` never participate in DISTINCT ON).

### 4.2 No internal chunking — whole-set batch + global-ordered lock

Each `*_batch` helper takes the **entire** touched/drifted id-set and (as the 3 existing variants already do):
1. Acquires **all** advisory locks in ONE statement, globally ordered: `SELECT pg_advisory_xact_lock(lk) FROM (SELECT … AS lk FROM unnest(ids) ORDER BY lk)`. A single ascending-`lk` acquisition is the universal-ordering deadlock-avoidance — safe under any concurrent worker with any overlapping id-set, **regardless of the caller's transaction model**. (Already proven by `test_batch_deadlock_safe_under_concurrent_overlap`.)
2. One MERGE over `WHERE instrument_id = ANY(%(ids)s::bigint[])`.

**This deliberately drops the per-chunk SAVEPOINT design** (Codex ckpt1 BLOCKING): `pg_advisory_xact_lock` survives SAVEPOINT release and is held to the *outer* transaction end, so chunking inside a sweep that runs in an open outer tx (the sync path — see §4.3) would **accumulate** locks across chunks and let two workers with different chunk boundaries deadlock. One upfront global-ordered lock pass has no such failure mode.

### 4.3 Sweep wiring — try-batch-then-fallback (caller tx models differ)

Both callers run on **non-autocommit** connections (corrected from spec v1 — adversarial committee, verified):
- **Sync sweep** `sync_all`: open outer tx, observation INSERTs uncommitted until `conn.commit()` (sync.py:839-847).
- **Repair sweep** `run_observations_repair_sweep`: called at `scheduler.py:4392` via `with psycopg.connect(settings.database_url) as conn:` — **default autocommit=False**, so the first `_drifted_instruments` SELECT opens an implicit tx that stays open across the category loop.

So in **both** the helper's `with conn.transaction()` is a SAVEPOINT and advisory xact-locks are held to the end of the whole sweep. The whole-set + global-ordered-lock design (§4.2) is correct under both because lock **ordering**, not release timing, guarantees deadlock-freedom. (Residual: whole-set lock acquisition is upfront, so a large touched-set holds many advisory locks for the sweep duration — same total as the per-instrument loop already holds in savepoint mode, no regression, but noted as a contention surface — Codex CTO.)

**Error isolation (preserves the orphan contract):** the batch MERGE is atomic — one poison instrument fails all N. So wrap it:

```python
def _refresh_for_instruments(conn, *, instrument_ids, refresh_batch_fn, refresh_one_fn, summary) -> int:
    ids = sorted(set(int(i) for i in instrument_ids))
    if not ids:
        return 0
    try:
        refresh_batch_fn(conn, instrument_ids=ids)   # opens its own with conn.transaction()
        return len(ids)
    except Exception:
        logger.warning("batch refresh failed for %d instruments; falling back per-instrument", len(ids))
        # batch tx has fully exited/rolled back here (the `with` block closed on the
        # exception); the connection is back in its prior state. Fall back to the
        # existing per-instrument loop to isolate the poison id + record orphans.
        n = 0
        for iid in ids:
            try:
                refresh_one_fn(conn, instrument_id=iid)
                n += 1
            except Exception as exc:
                logger.exception("refresh failed instrument_id=%d", iid)
                summary.orphans.append(f"refresh failed instrument_id={iid}: {exc}")
        return n
```

The `try/except` is **outside** the helper's transaction (the helper owns its own `with conn.transaction()`), so the fallback never runs on an aborted tx (Codex ckpt1 BLOCKING). On both paths the SAVEPOINT rolls back to before the helper and the outer tx survives, leaving the connection usable for the per-instrument fallback.

**Signature migration (adversarial committee):** `_refresh_for_instruments` today is `(conn, *, instrument_ids, refresh_fn, summary)` with **6 call-sites** across **5** `sync_X` funcs (insiders/institutions/blockholders/treasury/def14a — `sync_def14a` calls it twice: def14a + esop; there is no `sync_funds`/`sync_esop`). The rewrite changes `refresh_fn` → `(refresh_batch_fn, refresh_one_fn)`; all 6 call-sites updated in the same PR-B commit. `sync_def14a`'s two calls must keep summing both refresh counts into `instruments_refreshed` (`+=`, not overwrite — sync.py:763-767).

**Repair wiring + orphan-sink (DE+test committee BLOCKING):** `_CATEGORIES` (4-tuple) gains the batch callable + keeps the per-instrument lambda (`lambda c, i: refresh_X_current(c, instrument_id=i)`) for the fallback adapter — both are `instrument_id`-keyword bridges. `run_observations_repair_sweep` routes through the same shared helper. The repair path has **no `SyncSummary`** — today it swallows per-instrument exceptions into `logger.warning` and `CategoryRepairStats` carries no failure count (repair.py:126-130,182-190). Add `CategoryRepairStats.failed_instruments: int` (and surface in `RepairSweepStats`) so a poison instrument that fails every sweep is **operator-visible**, not masked as transient drift-churn (its watermark never advances → it re-drifts forever). The shared helper takes an orphan-sink callback (or returns `(refreshed, failures)`), used by both `SyncSummary.orphans` (sync) and `failed_instruments` (repair). **Metric note:** the batch happy-path returns `len(ids)` = instrument count; the per-instrument repair path historically summed helper return values = `_current` row count. Reconcile `refreshed_rows` to a single documented meaning (instruments) or keep both counts distinct — do not silently shift rows→instruments on the operator dashboard.

### 4.4 Lint extension

`check_ownership_refresh_writer_pattern.sh` today checks the 3 existing batch helpers for **invariant I only** (column-set diff shape, lines ~504-510). Extend with batch-specific assertions for all **7** `*_batch`:
- exactly 1 `def refresh_<h>_current_batch(`
- `SET LOCAL jit = off` present
- `ANY(%(ids)s::bigint[])` in source WHERE **and** in NOT MATCHED BY SOURCE DELETE (L1496 batch form)
- no `refreshed_at` in IS DISTINCT FROM tuple (L1505)
- `executemany`/state UPSERT present with correct category literal
- ordered advisory-lock over `unnest`
- **`DISTINCT ON (instrument_id,` leads, and the leading `ORDER BY` key is `instrument_id`** (DE BLOCKING — guards cross-instrument collapse)
- **def14a batch carries all 3 extra WHERE clauses + the `%(esop_regex)s` placeholder** (DE BLOCKING — guards #843 ESOP double-count)

### 4.5 #1382 fix

`check_13f_hr_retention.sh` invariant H: relax expected per-row `record_institution_observation(source='13f')` call-sites 3 → 2, and assert the bulk path's retention gate (`thirteen_f_within_retention` in `sec_13f_dataset_ingest.py`) separately (invariant E already covers the gate; H just needs the count corrected + a comment pointing at the bulk writer). Verify bulk writer still writes `ownership_institutions_observations` before relaxing.

## 5. Acceptance gate

**Primary (functional, runnable now):** the 6 parametrised contracts in `tests/test_ownership_observations_refresh_batch.py` extended to all 7 categories — equivalence (batch == serial-loop row set), empty no-op, idempotent (xmin stable on re-run), SOURCE-clamp scoped, deadlock-safe under concurrent overlap, one-watermark-per-instrument, dup/unsorted normalisation. **These are the merge gate.**

**Perf (deferred — dev PG is fresh-empty this cycle):** the plan §4 per-helper gate (median 3-trial <200 ms heaviest instrument; EXPLAIN no Seq Scan over full `_current`; post-rewrite count(*) == pre ±1%) **cannot be run on an empty dev DB** and dev-sized EXPLAIN would be meaningless. Perf validation is **batched into the operator end-of-ETL clean bootstrap** (Phase 0.5 R-runs): after a real bootstrap, measure S22 wall-clock vs Run #8's 344 min and run the EXPLAIN gate against prod-sized `_observations`.

**MANDATORY prevention-log entry** (write in this PR): *"dev-sized EXPLAIN ≠ prod-sized EXPLAIN — a refresh-path perf claim validated on an empty/dev DB is worthless because partition-probe cost scales with populated partitions + row counts; perf acceptance for partition-heavy MERGE paths must be measured post-bootstrap on prod-sized data, never asserted from a dev EXPLAIN."*

## 6. Test plan

**PR-A:**
- **`_seed_one` extension (test committee BLOCKING — NOT free):** `_seed_one` (test file:101-184) hard-codes branches for insiders/institutions/funds and `pytest.fail`s otherwise. Add 4 branches whose seeds **survive the per-category refresh WHERE filters** or the contracts pass vacuously against an empty target: blockholders/treasury/def14a leave `known_to` NULL; def14a needs `shares IS NOT NULL` + `holder_role != 'esop'` + `holder_name` not matching the esop regex; esop needs `shares > 0` + non-empty `plan_name` (record-time `ValueError` otherwise).
- 4 new `BatchHelperCase` entries → the 6 parametrised contracts apply (6×4 = 24 assertions), incl. `test_batch_equivalent_to_serial_loop` (full-row `to_jsonb − refreshed_at`, not counts — test file:198) and `test_batch_deadlock_safe_under_concurrent_overlap`.
- Treasury N×1: explicit test that batch `[a,b]` == 2 serial `DISTINCT ON (instrument_id)` results (DE+ckpt1 BLOCKING).
- def14a: snapshot must include `holder_name_key` (generated PK) so a winner-selection regression surfaces; assert ESOP-named rows are excluded (the `%(esop_regex)s` filter).
- **Ambient-tx overlap deadlock test + the existing concurrency test MUST carry `@pytest.mark.xdist_group("ownership_refresh_lock")`** (test committee IMPORTANT) and `finally: conn.rollback()/close` — else a failed-deadlock case leaks advisory locks across the per-worker test DB and flakes CI. Two workers, overlapping differently-ordered id-sets, each with an open outer tx (mirrors the savepoint path).
- Lint: `bash scripts/check_ownership_refresh_writer_pattern.sh` green with the batch block (incl. instrument_id-prefix + def14a-regex assertions).

**PR-B:**
- **Wiring regression (test committee BLOCKING — the headline goal is otherwise unguarded):** monkeypatch/spy each `refresh_<cat>_current_batch` and assert `sync_*` and `run_observations_repair_sweep` call it **once with the full id-set**, not N times per-instrument. Without this a future refactor silently reverts the wiring and all PR-A contract tests still pass green.
- Sweep fallback: inject a synthetic poison (e.g. a pre-existing `_current` row that trips a CHECK on the batch UPDATE) → assert exactly one failure recorded (`SyncSummary.orphans` for sync, `CategoryRepairStats.failed_instruments` for repair) + the rest refreshed + connection usable after. (Test committee: realistic poison is contrived — couple the test to a known CHECK; if infeasible, downgrade to "batch raises → fallback refreshes all-but-poison, poison surfaced in the sink".)
- Repair orphan-sink: test that a permanently-failing instrument increments `failed_instruments` every sweep (not masked as transient drift).
- Lint: `bash scripts/check_13f_hr_retention.sh` green post-#1382 fix.
- Gates: ruff / ruff format / pyright / pytest + smoke; frontend untouched.

## 7. Migration / backfill

**No schema change.** Pure refresh-path rewrite. ETL DoD clauses 8-12 (smoke AAPL/GME/MSFT/JPM/HD + cross-source + backfill + operator-visible figure) are satisfied by the **operator end-of-ETL clean bootstrap** (dev PG fresh-empty) — recorded in the PR as deferred-to-operator-batch, same disposition as #1343 / #1337.

## 8. Risks

- **R1 (treasury N×1):** batch synthetic-row construction wrong → wrong treasury `_current`. Mitigation: explicit equivalence test + committee/Codex on §4.1 treasury.
- **R2 (cross-worker deadlock):** advisory xact-locks survive SAVEPOINT and are held to outer-tx end; chunked acquisition would let two workers deadlock. Mitigation: §4.2 single global-ordered lock pass over the whole id-set (no chunking) + ambient-tx overlap test.
- **R3 (lint false-confidence):** batch lint too loose → a future drift slips. Mitigation: assert the L1496/L1505 specifics, not just presence.
- **R4 (perf unproven at merge):** accepted + explicitly deferred to operator R-run; functional equivalence is the merge gate, perf is the operator gate.
