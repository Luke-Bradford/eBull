# DB-lane source split — Phase C bulk-ingest parallelism

Author: claude (autonomous, #1141 / Task E of #1136 audit)
Date: 2026-05-13
Status: Draft (pre-Codex)

## Problem

`app/services/bootstrap_orchestrator.py` registers five Phase C
bulk-ingest stages on a single `db` lane (`_LANE_MAX_CONCURRENCY["db"] = 1`),
and `app/jobs/sources.py::Lane` encodes one shared `db` source so
every `db` job serialises under one `JobLock`. The structural
parallelism the May 8 bulk-first design (`docs/superpowers/specs/
2026-05-08-bootstrap-etl-orchestration.md`) called out for Phase C
(C1.a/C2/C3/C4/C5 — "all 5 may run concurrently") is therefore
disabled.

PR1c #1064 retired the parallel-DB-stage claim from #1020 once
`JobLock` became source-keyed: same-source = one lock = serialised.
The audit (#1136 §2) flagged this as a deliberate-but-undocumented
trade-off. #1141 demands either:

- (a) keep `db=1` and document the trade-off, OR
- (b) split `db` into per-family sources so non-contending Phase C
  stages can run cross-source in parallel.

Decision is empirical: only path (b) survives if wall-clock cost
on a real-world install is large enough to justify the extra Lane
identities.

## Measurement — real bootstrap_run #3 on dev DB

`bootstrap_runs.id=3` (triggered 2026-05-08 17:57 UTC, terminal
2026-05-09 09:16 UTC, `partial_error`) is the only full-panel
bootstrap run on dev. Wall-clock per Phase C stage (lane=`db`,
ran serial):

| stage_order | stage_key                          | wall_sec | wall_min |
|------------:|------------------------------------|---------:|---------:|
|  8 | sec_submissions_ingest                       |    2836 |  47.3 |
|  9 | sec_companyfacts_ingest                      |    2213 |  36.9 |
| 10 | sec_13f_ingest_from_dataset                  |    6621 | 110.4 |
| 11 | sec_insider_ingest_from_dataset              |    1593 |  26.6 |
| 12 | sec_nport_ingest_from_dataset                |    3697 |  61.6 |

Serial sum (status quo path (a)): **283 min** (4 h 43 min).

Maximum single stage (parallel-5 floor under path (b)): **110 min**
(13F ingest dominates).

### Phase C' coupling

Stage 13 (`sec_submissions_files_walk`, `sec_rate` lane) gates on
`filing_events_seeded` provided by stage 8 only. Run #3 measured
`sec_submissions_files_walk` at 5417 sec (90.3 min).

- Path (a): C1-C5 serial → C' waits for ALL of C1-C5 to finish
  → C' starts at t=283 min → C' done at t=373 min.
- Path (b): C1-C5 parallel → C' waits for stage 8 (sec_submissions_
  ingest) only → C' starts at t=47 min → C' done at t=137 min.

Path (b) bootstrap wall-clock saving from C entry to C' done:
**373 - 137 = 236 min (~3 h 56 min)**.

That is operator-visible. Path (a) would lock in an extra ~4 hours
of fresh-install wall-clock on a real connection. Path (b) is
justified.

## Decision

**Path (b).** Split the `db` source by table family so the five
Phase C ingesters run cross-source-parallel under separate
`JobLock`s.

### Family lane assignments

| stage_key                          | new lane               | write target (audit §4) |
|------------------------------------|------------------------|---|
| `sec_submissions_ingest`           | `db_filings`           | `filing_events`, `instrument_sec_profile` |
| `sec_companyfacts_ingest`          | `db_fundamentals_raw`  | `company_facts` (via `upsert_facts_for_instrument`) |
| `sec_13f_ingest_from_dataset`      | `db_ownership_inst`    | `ownership_institutions_observations` |
| `sec_insider_ingest_from_dataset`  | `db_ownership_insider` | `insider_transactions`, `form3_holdings_initial` |
| `sec_nport_ingest_from_dataset`    | `db_ownership_funds`   | `n_port_*`, `sec_fund_series` |

Each new source advertises `max_concurrency=1` in
`_LANE_MAX_CONCURRENCY`. The parallelism win comes from
**cross-source dispatch**, not from intra-source concurrency:
five disjoint `hashtext(f'job_source:{family}')` keys → five
disjoint `pg_try_advisory_lock` keys → five disjoint connections
running concurrently.

### What stays on the `db` source

Bootstrap stages 23 + 24 — `ownership_observations_backfill`,
`fundamentals_sync` — are Phase E derivations that fire AFTER
every Phase C capability is satisfied. Both are also registered
in `SCHEDULED_JOBS` (`source="db"`); moving them to a family lane
would shift the source-lock identity for every scheduled
invocation too, with non-trivial blast onto the every-5-min
`orchestrator_high_frequency_sync` + daily `orchestrator_full_sync`
that share the `db` source.

The wall-clock cost of keeping E1 + E2 serial under `db` is
bounded: in run #3 `fundamentals_sync` was 5716 sec (95 min) and
`ownership_observations_backfill` would have been ~6 min if not
blocked. Worst-case extra serial cost vs splitting: ~6 min — two
orders of magnitude smaller than the C-stage win and not worth
extending the migration / scheduler audit surface.

Keep `db` as the lane + source for Phase E stages. Scheduler
`db`-source job behaviour is unchanged for those jobs.

### Accepted loss of incidental `db` serialisation during Phase C

Pre-split: Phase C ingesters held `job_source:db`. Any scheduled
`db`-source job firing mid-bootstrap (`orchestrator_high_frequency_
sync` @5 min, `retry_deferred` @hourly, `monitor_positions`
@hourly, manual `ownership_observations_backfill`) would either
raise `JobAlreadyRunning` or sit behind the lock.

Post-split: those scheduler jobs can acquire `job_source:db` while
Phase C ingesters hold their per-family sources. They run
concurrently. This is **accepted**, not a regression:

- The scheduler-side jobs touch disjoint table sets:
  - `orchestrator_high_frequency_sync` writes `portfolios` /
    `fx_rates` — neither touched by any Phase C ingester.
  - `retry_deferred` mutates `recommendations` rows where
    `status='timing_deferred'` — Phase C does not write
    `recommendations`.
  - `monitor_positions` reads open positions + writes guard
    state — disjoint from Phase C tables.
  - `ownership_observations_backfill` (legacy → observations
    backfill) writes observation rows; its bootstrap-Phase-E
    invocation gates on Phase C caps and so cannot fire mid-C.
    Its scheduler firing during bootstrap is blocked by
    `check_bootstrap_state_gate` reading `bootstrap_state` —
    the universal scheduled-fire gate routes every scheduled
    invocation past the state row before any prerequisite
    callable runs.
- The previously-incidental serialisation conflated unrelated
  rate buckets — the same anti-pattern that PR1a / #1064 fixed
  by retiring per-job-name locks for per-source locks. Restoring
  serialisation here would re-introduce the bucket conflation.
- Bootstrap-completion-gated `db` jobs (`orchestrator_full_sync`,
  `fundamentals_sync`, `ownership_observations_sync`) do not fire
  mid-bootstrap regardless; they have `prerequisite=
  _bootstrap_complete` and the gate stays closed until run
  finalisation. Verified at `app/workers/scheduler.py:516, 586,
  779`.

No additional gate is added. The bootstrap-state machine + per-job
prerequisite gates are the correct serialisation boundary; raw
source-lock conflation was a side effect, not a contract.

### What stays out of scope

- Scheduler `db`-source jobs (`orchestrator_full_sync`,
  `orchestrator_high_frequency_sync`, `retry_deferred`,
  `monitor_positions`, `ownership_observations_sync`,
  `fundamentals_sync`, `ownership_observations_backfill`).
  All retain `source="db"`.
- The five Phase C bulk ingesters are not registered in
  `SCHEDULED_JOBS` (verified by grep against `app/workers/
  scheduler.py`). They ARE in `_INVOKERS` (`app/jobs/runtime.py`)
  so manual Admin-UI dispatch via `POST /jobs/<name>/run` will
  resolve through `JOB_NAME_TO_SOURCE` to the new family source
  — a manual dispatch of `sec_submissions_ingest` will acquire
  `job_source:db_filings`, not `job_source:db`. This is the
  intended uniform behaviour: source identity follows job
  identity regardless of who triggered it.
- Phase C' walker, Phase D body parsers (all `sec_rate` —
  unaffected).

## Implementation

### 1. Extend `Lane` Literal (three sites must stay aligned)

The Lane vocabulary lives in three closed Literals that all must
grow together (Codex pre-spec round 1 BLOCKING):

- `app/jobs/sources.py::Lane` — the source-key registry truth.
- `app/services/bootstrap_state.py::Lane` — DB-row-shape Literal
  used by `bootstrap_stages.lane` reads (line 50).
- `app/api/bootstrap.py::LaneApi` — the FastAPI response-model
  Literal (line 69) surfaced to the operator UI.

All three gain the same five family names. The two row-shape
Literals (`bootstrap_state.py`, `api/bootstrap.py`) keep `"sec"`
for legacy-row compat per their existing comments:

```python
# app/services/bootstrap_state.py + app/api/bootstrap.py::LaneApi
Lane = Literal[
    "init", "etoro", "sec", "sec_rate", "sec_bulk_download", "db",
    "db_filings",
    "db_fundamentals_raw",
    "db_ownership_inst",
    "db_ownership_insider",
    "db_ownership_funds",
]
```

`sources.py::Lane` is the source-key truth and was always free of
`"sec"` (PR1c #1064 retired the legacy catch-all from the source
vocabulary). It gains the five new names but does NOT regain
`"sec"`:

```python
# app/jobs/sources.py
Lane = Literal[
    "init", "etoro", "sec_rate", "sec_bulk_download", "db",
    "db_filings",
    "db_fundamentals_raw",
    "db_ownership_inst",
    "db_ownership_insider",
    "db_ownership_funds",
]
```

`sources.py::Lane` docstring gains one bullet per new source
naming the write target. The `db` bullet narrows: "DB-bound
stages NOT owned by a finer family lane — Phase E derivations +
scheduler catch-all".

### 2. Migration `sql/147_bootstrap_stages_lane_family_split.sql`

```sql
ALTER TABLE bootstrap_stages
    DROP CONSTRAINT IF EXISTS bootstrap_stages_lane_check;

ALTER TABLE bootstrap_stages
    ADD CONSTRAINT bootstrap_stages_lane_check
    CHECK (lane IN (
        'init', 'etoro', 'sec', 'sec_rate', 'sec_bulk_download', 'db',
        'db_filings',
        'db_fundamentals_raw',
        'db_ownership_inst',
        'db_ownership_insider',
        'db_ownership_funds'
    ));
```

Preserves the legacy `'sec'` lane so the existing run #3 rows stay
valid (same pattern as migration 132).

### 3. Update `_LANE_MAX_CONCURRENCY`

Add an entry per new family lane = 1. The map's contract is
"intra-lane budget"; the parallelism is cross-lane and is enforced
by the dispatcher's per-lane `ThreadPoolExecutor`. Keep `db = 1`.

```python
_LANE_MAX_CONCURRENCY: Final[dict[str, int]] = {
    "init": 1,
    "etoro": 1,
    "sec": 1,
    "sec_rate": 1,
    "sec_bulk_download": 1,
    "db": 1,
    "db_filings": 1,
    "db_fundamentals_raw": 1,
    "db_ownership_inst": 1,
    "db_ownership_insider": 1,
    "db_ownership_funds": 1,
}
```

Delete the existing "PR1c #1064" comment that claimed `db=1`
retired #1020's parallel-DB claim — the claim is reinstated for
the bulk-ingest stages under per-family sources.

### 4. Update `_STAGE_LANE_OVERRIDES`

Re-route the five Phase C stages from `db` to their family lanes:

```python
_STAGE_LANE_OVERRIDES: Final[dict[str, str]] = {
    ...
    "sec_submissions_ingest":           "db_filings",
    "sec_companyfacts_ingest":          "db_fundamentals_raw",
    "sec_13f_ingest_from_dataset":      "db_ownership_inst",
    "sec_insider_ingest_from_dataset":  "db_ownership_insider",
    "sec_nport_ingest_from_dataset":    "db_ownership_funds",
    ...
}
```

`_BOOTSTRAP_STAGE_SPECS` entries for these five stages keep
`lane="db"` (the `StageSpec.lane` field is the default; the
override map is the dispatcher truth). This minimises the
`bootstrap_stages` row churn on existing runs.

### 5. Integration test

New file `tests/test_db_lane_family_split.py` exercising the
acceptance criterion from #1141:

> integration test demonstrating two db stages on disjoint table
> families run concurrently without lock contention

`JobLock` accepts `job_name` (not `source`); each `JobLock(...,
job_name)` resolves through `source_for(job_name)` to its source
key. The test pattern mirrors `tests/test_joblock_per_source.py`:

```python
pytestmark = pytest.mark.xdist_group(name="joblock_db_family_split")
```

Postgres advisory locks are cluster-wide; the xdist group
forces these tests onto a single worker so two xdist workers
can't fight over the same source key. Uses `settings.database_url`
(test DB; pytest fixtures point it at the per-worker template).

Shape:

1. **Cross-family concurrency** — hold `JobLock(settings.database_url,
   "sec_submissions_ingest")` (source `db_filings`). Inside that
   `with`, acquire `JobLock(settings.database_url,
   "sec_13f_ingest_from_dataset")` (source `db_ownership_inst`).
   Both must succeed without raising `JobAlreadyRunning` —
   proves cross-source dispatch is unblocked.
2. **Intra-family serialisation** — hold a `JobLock` on
   `"sec_submissions_ingest"`. Try to acquire a second `JobLock`
   on the same job. Assert it raises `JobAlreadyRunning` —
   proves the per-family source still serialises (no regression
   of #1064's source-keyed lock invariant).
3. **Disjoint from `db`** — hold a `JobLock` on
   `"sec_submissions_ingest"` (`db_filings`). Acquire a second
   `JobLock` on `"orchestrator_full_sync"` (`db`). Both succeed
   — proves family sources are disjoint from the scheduler-side
   `db` source (the "accepted loss of incidental `db`
   serialisation" subsection above; pinned by test so a future
   re-merge has to fight a red test).

Source registry assertions (cheap; no DB):

4. Round-trip `source_for("sec_submissions_ingest") ==
   "db_filings"`, etc. for each of the five C stages.
5. Round-trip the inverse: every family source has exactly one
   `job_name` mapped to it in `get_job_name_to_source()`.

Migration sanity (cheap; uses fixture's DB):

6. Create a parent `bootstrap_runs` row (the `bootstrap_stages.
   bootstrap_run_id` FK requires it), then insert a
   `bootstrap_stages` row with `lane="db_filings"` — succeeds.
   Insert another stage row with `lane="garbage_lane"` — raises
   `CheckViolation`. Insert with `lane="db"` — succeeds (legacy
   compat preserved). The fixture rolls back the run + child
   rows on exit so the test leaves no state.

`tests/test_job_registry.py::_ALLOWED_SOURCES` (line 37) must be
expanded to include the five new family lanes in the same PR or
`test_every_source_is_valid_lane` fails on `JOB_NAME_TO_SOURCE`
construction.

### 6. Spec doc update

Patch `docs/superpowers/specs/2026-05-08-bootstrap-etl-
orchestration.md`:

- Replace the §"Parallelism implementation" claim "`db` (C1.a,
  C2, C3, C4, C5, E1, E2): N (default 5)" with the per-family
  source split and the run #3 measurement that justified it.
- Add a §"DB-lane source split (#1141)" subsection citing this
  spec and the wall-clock saving.

## Failure-mode invariants (unchanged)

- A single Phase C ingester failing under its family source still
  raises `BootstrapPreconditionError` → stage `error` → caps it
  provided stay unprovided → downstream Phase D / E gates fail
  per the existing cap propagation (Task A spec).
- A boot-recovery sweep that voids in-flight stages continues to
  work — `JobLock`'s session-scoped advisory lock releases on
  connection death regardless of source.
- Cancel observation latency (audit §5) unchanged — the
  dispatcher checkpoint runs between batches, not per-source.

## Acceptance criteria

Per #1141:

- [x] Measured wall-clock on dev DB recorded in PR description
  (run #3, table above).
- [ ] Decision (a) or (b) documented at
  `docs/superpowers/specs/2026-05-08-bootstrap-etl-
  orchestration.md` (path (b); doc patch in §6).
- [ ] If (b): each split family demonstrably independent —
  integration test `tests/test_db_lane_family_split.py`
  exercising two concurrent C-family `JobLock`s + per-family
  serialisation invariant + CHECK-constraint round-trip.

## Out-of-scope follow-ups

- Phase E split (`fundamentals_sync` + `ownership_observations_
  backfill`) — deferred; ~6 min saving vs scheduler-source blast.
  Re-evaluate if Phase E grows.
- `_LANE_MAX_CONCURRENCY` map retirement (PR1c #1064 follow-up
  note line 200) — still deferred; map structure stays so
  `_phase_batched_dispatch` shape is stable.
