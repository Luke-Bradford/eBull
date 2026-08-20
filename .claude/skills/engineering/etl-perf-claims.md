---
name: etl-perf-claims
description: Use before writing or reviewing any PR that asserts a performance improvement (latency, wall-clock, throughput) against an ETL hot path. Codifies the §4 verification protocol (artifacts + floors) and the §5 process rules (immutable evidence, reproducibility, invariants, rollback) from docs/proposals/etl/bootstrap-sub-1h-plan.md. Triggers on perf claims touching ownership/financial/manifest/filing tables, S22 MERGE rewrites, jit=off changes, bulk-vs-iterate decisions, any wall-clock reduction claim.
---

# ETL perf claims

Spec: [docs/proposals/etl/bootstrap-sub-1h-plan.md](../../../docs/proposals/etl/bootstrap-sub-1h-plan.md) §4-§5.

Every perf-claim PR fails `perf-claim-lint` (CI required check on `main`) unless:

1. **Artifacts committed** under `var/perf_baselines/<ticket>-<sha>.*`:
   - `.txt` — `EXPLAIN (ANALYZE, BUFFERS, COSTS, FORMAT TEXT)` output
   - `.json` — 3-trial wall-clock timings + median + system fingerprint (`pg_version`, `host`, `shared_buffers`)
   - `.manifest.yaml` — fixture row counts (must meet floors)
2. **PR description sections** (line-exact headers; lint enforces):
   - `## Sibling-shape audit` — every grep-matched same-shape callsite reviewed
   - `## Rollback criteria` — metric + threshold + operator-executed SLA
   - `## Post-deploy SLO` — 1-week metric + alert wiring
3. **Floor compliance** per [scripts/perf_bench/floors.yaml](../../../scripts/perf_bench/floors.yaml). Below-floor manifests fail the lint.

## Reproducing a measurement

```bash
export EBULL_BENCH_DB_URL=postgresql://.../ebull_bench
scripts/perf_bench/run_explain.sh <ticket_id>
```

The harness refuses dirty working trees so artifact filenames pin to a committed SHA. See [docs/operator/runbooks/perf-investigation.md](../../../docs/operator/runbooks/perf-investigation.md) for bench-DB setup + the synthetic-fixture seeders.

## Process rules (regulated-desk bar)

1. **Immutable evidence**: every artifact lands in git on the PR branch.
2. **Reproducible harness**: anyone re-runs `run_explain.sh` and gets a comparable measurement on the same SHA.
3. **Data-quality invariants**: row count + distinct count + aggregate sums hold pre/post the change. The PR cites the invariant query + numbers.
4. **Rollback criteria**: written threshold; operator executes within 24h of Codex review if SLO breached.
5. **Named accountability**: one human per phase (commit author + plan signoff + post-deploy verifier).
6. **Audit trail**: Codex 1 plan-review + Codex 1 diff-review transcripts pasted into PR.
7. **Post-deploy SLO**: 1-week metric wired to `ops-monitor`; failure → alert → operator rollback decision.
8. **Regulator-reconstructible**: git history alone tells "what changed, why, when, by whom".

## Recurring failure mode

Dev-fixture-passes-prod-fails. The §4 floors exist because the previous incidents (#1255 MERGE EXPLAIN, S22 attestation gaps) all had clean dev EXPLAIN over <1k rows while prod scaled to millions. **Floor every claim** — synthetic-fixture seeders are at [scripts/perf_bench/seed_synthetic_fixture/](../../../scripts/perf_bench/seed_synthetic_fixture/) and the bench-DB setup is in [docs/operator/runbooks/perf-investigation.md](../../../docs/operator/runbooks/perf-investigation.md).

## Refusal posture

If you cannot satisfy the protocol — bench DB unavailable, fixture cannot meet floor, invariant query disagrees with the claim — DO NOT push a claim. Either:

- Land the seeder for the floor table first (per the runbook),
- Down-scope the PR to remove the perf claim,
- Or escalate to the operator with the obstacle.

`perf-claim-lint` has a bypass path, but it is not self-serve: it fires only when all three operator-controlled gates are present — the `emergency` PR label, a `## Bypass justification` section carrying `Operator:` + `Reason:` lines, and `PERF_CLAIM_LINT_BYPASS=true` in CI — and it emits a `::warning::` annotation when engaged. Do not invoke bypass to ship faster — invoke it only when the obstacle is documented and out of scope.

## Cross-references

- [.claude/skills/engineering/pre-flight-review.md](pre-flight-review.md) — links here when the diff asserts a perf improvement on an ETL hot path
- [.claude/skills/engineering/pre-pr-fresh-agent-review.md](pre-pr-fresh-agent-review.md) — links here when a filings-ETL / schema-migration PR also asserts a perf improvement
- Master plan §5 — process rules mirrored here, single source of truth for the rule list
