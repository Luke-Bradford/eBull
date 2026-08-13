# Operator runbook: perf-bench investigation

Per Phase 0 NEW-C of [docs/proposals/etl/bootstrap-sub-1h-plan.md](../../proposals/etl/bootstrap-sub-1h-plan.md).

Use this runbook when:

- Writing a perf-claim PR that touches a hot path against a table listed
  in [`scripts/perf_bench/floors.yaml`](../../../scripts/perf_bench/floors.yaml).
- Reproducing a perf claim from a previous PR.
- Adding a new floor table or extending the seeder coverage.

The `perf-claim-lint` CI gate refuses perf claims whose row counts do
not meet the floor — the floor protects against dev-fixture-passes-prod-fails
(the recurring failure mode that drove the §1 meta-issue of the master plan).

## 1. Prerequisites

1. Bench-only Postgres database with the eBull schema applied. Database
   name MUST contain the substring `bench` AND MUST NOT contain `dev`
   or `prod`. Example: `ebull_bench`.
2. `EBULL_BENCH_DB_URL` exported to the bench DB:
   ```bash
   export EBULL_BENCH_DB_URL=postgresql://postgres@localhost:5432/ebull_bench
   ```
3. `psql` on PATH (the harness shells out for `COUNT(*)`).
4. Repo working tree clean (the harness records the HEAD SHA on every
   artifact and refuses dirty trees).

## 2. Seed the fixture for one table

The reference implementation is
`scripts/perf_bench/seed_synthetic_fixture/seed_ownership_institutions_current.py`.
It seeds 1,000,000 rows into `ownership_institutions_current` and then
asserts seven sentinel-invariant checks before exiting.

Dry-run first to confirm the planned row count:

```bash
uv run python -m scripts.perf_bench.seed_synthetic_fixture.seed_ownership_institutions_current --dry-run
```

Then run for real:

```bash
uv run python -m scripts.perf_bench.seed_synthetic_fixture.seed_ownership_institutions_current
```

Expected last line on success:

```
verification PASS: count=1000000 floor=1000000 drifted_non_sentinel=N
```

Where `N` is the number of real-instrument ids the refresh-sweep already
sees as drifted (typically 0 on a freshly schema'd bench DB; non-zero
counts are not a seed problem).

Verify without re-seeding:

```bash
uv run python -m scripts.perf_bench.seed_synthetic_fixture.seed_ownership_institutions_current --verify-only
```

## 3. The 7 floor tables

Implemented: 1. The other 6 land when first needed by a downstream
perf claim (per the operator discipline note in
`feedback_no_ticket_count_obsession`).

| Table | Floor | Status | Strategy |
|---|---|---|---|
| `ownership_institutions_current` | 1,000,000 | **implemented** | Sentinel `instrument_id >= 1_000_000_000`; direct `_current` write only; no `_observations`/`ownership_refresh_state` writes |
| `ownership_institutions_observations` | 2,000,000 | stub | Sentinel `instrument_id`; spread `period_end` across last 8 quarters; route via existing range-partitioning. MUST also seed `ownership_refresh_state` watermark or refresh-sweep will fire on sentinels |
| `ownership_insiders_observations` | 500,000 | stub | Sentinel `instrument_id`; synthetic `holder_cik`; let DB derive `holder_identity_key` (GENERATED ALWAYS); paired refresh-state write required |
| `ownership_funds_observations` | 200,000 | stub | Sentinel `instrument_id`; synthetic `fund_series_id`; `payoff_profile='Long'`, `asset_category='EC'` (CHECK-pinned); paired refresh-state write required |
| `financial_facts_raw` | 10,000,000 | stub | **Sentinel strategy doesn't work** — FK to `instruments`. Replicate real instrument ids with multi-quarter `period_end` spread; tag with synthetic `ingestion_run_id` for cleanup |
| `sec_filing_manifest` | 1,000,000 | stub | Mixed: issuer-scoped rows use real `instrument_id` (FK fires) + `'SYN-'`-prefixed `accession_number`; institutional-filer rows have NULL `instrument_id` + sentinel `filer_cik`. Synthetic `ingest_status` MUST be terminal so manifest-worker doesn't pick them up |
| `filing_events` | 2,000,000 | stub | FK to `instruments` — same real-instrument-replication pattern as `financial_facts_raw`. Tag with distinctive `event_type` for cleanup |

Each stub module's docstring is the full implementation plan + the
cross-impact note. Read the module before extrapolating.

## 4. When (not) to use the seeder

**Use when**:
- Validating the perf-bench harness itself.
- Phase 1 (#1346) `SET LOCAL jit = off` claim — row distribution is not
  the cost driver, only row count.
- Phase 2 (bulk-first extraction) claims where the cost driver is HTTP
  vs local parse, not query plan over distribution.

**Do NOT use when**:
- Phase 4 (#1345) S22 MERGE rewrite — the cost cliff depends on the
  real distribution of `_observations` → `_current` diffs, not the row
  count alone. Synthetic rows do not exercise the MERGE matching paths.
- Any claim where the operator-visible figure on real eToro instruments
  is part of the assertion.
- Production-shaped query plans where statistics rely on real value
  cardinalities (the seeder uses low cardinality on
  `filer_cik`/`source`).

When in doubt: the seeder gives you the floor for `perf-claim-lint` to
accept the PR. The realism of the EXPLAIN result is on the PR author.

## 5. Writer-safety analysis (full rationale at
[`scripts/perf_bench/seed_synthetic_fixture/__init__.py`](../../../scripts/perf_bench/seed_synthetic_fixture/__init__.py))

The refresh-sweep helper at
`app/jobs/ownership_observations_repair.py:_drifted_instruments` is the
predicate for which `instrument_id` values get re-MERGEd into `_current`.
The query is anchored on `ownership_refresh_state` (LEFT JOIN to an
`obs_max` CTE over `_observations`). Synthetic rows are safe ONLY IF:

1. No sentinel id appears in `ownership_institutions_observations`.
2. No sentinel id appears in `ownership_refresh_state`.

The seeder enforces both invariants pre- and post-seed via the seven
real-numbers assertions in `seed_ownership_institutions_current.main`.

**Hazard**: re-applying `sql/163_ownership_refresh_state.sql` against a
seeded bench DB will walk `_current` and emit a sentinel row into
`ownership_refresh_state`. After any schema re-apply, run `--verify-only`
to confirm the invariants still hold; if not, truncate the sentinel
rows from `ownership_refresh_state` (filtering on the sentinel range)
and re-run the seeder.

## 6. Cleanup

Sentinel rows survive forever by design (the refresh-sweep never wakes
on them). To remove:

```sql
DELETE FROM ownership_institutions_current WHERE instrument_id >= 1000000000;
```

For the FK-bound stub tables (`financial_facts_raw`, `filing_events`,
issuer-scoped `sec_filing_manifest` rows), cleanup paths are documented
on a per-module basis when the implementation lands. The companion
cleanup script is a mandatory deliverable of every stub-implementation
PR.

## 7. References

- Master plan: [docs/proposals/etl/bootstrap-sub-1h-plan.md](../../proposals/etl/bootstrap-sub-1h-plan.md) §3 sub-60 commitment, §4 perf protocol
- Sub-plan: [docs/proposals/etl/phase-0-instrumentation.md](../../proposals/etl/phase-0-instrumentation.md) §2.8 NEW-C design
- Skill: [.claude/skills/engineering/etl-perf-claims.md](../../../.claude/skills/engineering/etl-perf-claims.md)
- Harness: [scripts/perf_bench/run_explain.sh](../../../scripts/perf_bench/run_explain.sh) + [`_run_explain.py`](../../../scripts/perf_bench/_run_explain.py)
- CI gate: `.github/workflows/ci.yml` `perf-claim-lint` job
- Floors: [scripts/perf_bench/floors.yaml](../../../scripts/perf_bench/floors.yaml)
