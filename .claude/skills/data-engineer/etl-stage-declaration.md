---
name: etl-stage-declaration
description: Use when adding a new bootstrap stage, declaring stage capabilities (provides/requires), wiring a new ScheduledJob to a bootstrap entry, or auditing _BOOTSTRAP_STAGE_SPECS for correctness. Documents the actual current 5-field StageSpec + 7 adjacent capability/lane maps that together define a bootstrap stage. Cross-references registration checklist, naming conventions, and the catalogue-invariant test. Target-state extensions (fetch_strategy enum, row_budget, etc.) are flagged as PROPOSED — not current.
---

# ETL stage declaration

## When to use

Read this skill before adding a new bootstrap stage, before declaring or modifying any `_STAGE_PROVIDES` / `_STAGE_REQUIRES_CAPS` entry, before introducing a new capability, or when auditing why `_BOOTSTRAP_STAGE_SPECS` mismatches expectations. Single source of truth for "how do I declare a new bootstrap stage end-to-end?" without guessing across `bootstrap_state.py` + `bootstrap_orchestrator.py` + `runtime.py` + `scheduler.py`.

## Current state — `StageSpec` is 5 fields

`StageSpec` lives at [`app/services/bootstrap_state.py:143`](../../../app/services/bootstrap_state.py#L143) — `@dataclass(frozen=True)`:

| Field | Type | Meaning |
|---|---|---|
| `stage_key` | `str` | Stable identifier (`sec_first_install_drain`, `cusip_universe_backfill`, etc.). Must be globally unique across `_BOOTSTRAP_STAGE_SPECS` |
| `stage_order` | `int` | Display ordering only — does NOT gate execution. Dependencies live in `_STAGE_REQUIRES_CAPS` |
| `lane` | `Lane` (Literal) | Concurrency bucket. See `app/jobs/sources.py::Lane` |
| `job_name` | `str` | Key in `_INVOKERS` ([`app/jobs/runtime.py`](../../../app/jobs/runtime.py)); the orchestrator dispatches it directly as `_INVOKERS[job_name](params)` |
| `params` | `Mapping[str, Any]` | Frozen param overrides for the registered invoker; default `{}` = "use invoker's registry default" |

That's it. NOT `fetch_strategy`, NOT `row_budget`, NOT `provides_cap` / `requires_cap`. Capabilities live in SEPARATE module-global dicts ([`bootstrap_orchestrator.py`](../../../app/services/bootstrap_orchestrator.py)):

| Map | Line | Purpose |
|---|---|---|
| `_STAGE_PROVIDES` | 339 | `stage_key → tuple[Capability, ...]` advertised on **success only** |
| `_STAGE_PROVIDES_ON_SKIP` | 393 | Subset re-advertised on `skip` for slow-connection-fallback parity |
| `_CAPABILITY_MIN_ROWS` | 428 | Per-cap floor: `rows_processed < min_rows` ⇒ cap is NOT advertised (default 0) |
| `_ORDERING_ONLY_CAPS` | 495 | Frozenset of caps that are "no concurrent writer remains" semantics — advertised on ANY terminal status (success/skip/blocked/error/cancelled) |
| `_STAGE_REQUIRES_CAPS` | 506 | `stage_key → CapRequirement` (DNF: `all_of` + `any_of`) |
| `_STAGE_LANE_OVERRIDES` | 1018 | Optional override of `StageSpec.lane` per `stage_key`; wins on collision |
| `_LANE_MAX_CONCURRENCY` | 168 | `lane → int` cap (12 lanes: db-family split #1141 + `openfigi` #1233) |

The orchestrator's helper `_spec()` ([`bootstrap_orchestrator.py:143-158`](../../../app/services/bootstrap_orchestrator.py#L143-L158)) is the ONLY supported constructor — it accepts `lane` as a plain `str` and carries the `# type: ignore[arg-type]` internally on the `StageSpec` assignment (the `Lane` Literal narrowing is lost), so call sites never add the ignore.

```python
_spec(
    stage_key="sec_first_install_drain",
    stage_order=16,
    lane="sec_rate",
    job_name=JOB_SEC_FIRST_INSTALL_DRAIN,
    params={"max_subjects": None, "use_bulk_zip": True, "follow_pagination": False},
)
```

## Naming convention

| Pattern | Use |
|---|---|
| `<source>_<verb>` | Top-level: `cusip_universe_backfill`, `sec_nport_filer_directory_sync`, `sec_8k_events_ingest` |
| `<job_name>_bootstrap` suffix | Bootstrap dispatches a dedicated backfill body distinct from the steady-state job — e.g. stage `fundamentals_sync` → job_name `fundamentals_sync_bootstrap` (registered in `_INVOKERS`, run on the `db_fundamentals_raw` lane) |
| `_first_install_drain` suffix | Reserved for stages that ONLY exist during first-install (no steady-state analogue): `sec_first_install_drain` |
| `_recent_sweep` suffix | Bounded variant of a steady-state job for bootstrap (historical: `sec_13f_recent_sweep` with `min_period_of_report=today-380d`, dropped by #1413's bulk-only bootstrap; the suffix convention stands) |

Anti-pattern: a `bootstrap_<x>` **prefix** dispatch-wrapper that re-implements dispatch just to override params — three such wrappers were removed in #1064 PR1 (see "Bootstrap-mode override patterns" below). The orchestrator passes `params` to a normally-registered invoker. A genuinely distinct backfill *body* (e.g. `fundamentals_sync_bootstrap`, added #1233 PR-C2) is fine — register it in `_INVOKERS` like any job and reference it by `job_name`.

## Registration checklist — when adding a stage

For a new stage `<NEW>` invoking job `<JOB>`:

1. **Job side** — `<JOB>` registered in `_INVOKERS` ([`app/jobs/runtime.py`](../../../app/jobs/runtime.py)) AND `SCHEDULED_JOBS` ([`app/workers/scheduler.py`](../../../app/workers/scheduler.py)) if steady-state.
2. **StageSpec** — add `_spec("<NEW>", <order>, "<lane>", "<JOB>", params={...})` to `_BOOTSTRAP_STAGE_SPECS` in `bootstrap_orchestrator.py`.
3. **Lane override (if needed)** — add `"<NEW>": "<non-default-lane>"` to `_STAGE_LANE_OVERRIDES` (only if the StageSpec.lane field doesn't match what the dispatcher needs at runtime — rare).
4. **Capabilities provided** — if `<NEW>` is a producer for any downstream consumer, add `"<NEW>": ("cap_a",)` to `_STAGE_PROVIDES`. If the cap is "no concurrent writer remains" semantics (ordering-only), ALSO add to `_ORDERING_ONLY_CAPS`. If slow-connection-fallback consumers need the cap satisfied even when this stage skips, add to `_STAGE_PROVIDES_ON_SKIP`.
5. **Capability floor** — if a cap is only valid when N rows were actually processed, add `"cap_a": N` to `_CAPABILITY_MIN_ROWS`.
6. **Capabilities required** — if `<NEW>` depends on upstream, add `"<NEW>": CapRequirement(all_of=("cap_b",), any_of=())` to `_STAGE_REQUIRES_CAPS`.
7. **`bootstrap_stages` row** — auto-created by `start_run` from the spec; no schema migration unless adding a new column.
8. **Catalogue-invariant test** — `tests/test_bootstrap_orchestrator.py::test_stage_keyed_dicts_reference_only_real_stages` asserts every key in the four stage-keyed dicts (`_STAGE_PROVIDES` / `_STAGE_PROVIDES_ON_SKIP` / `_STAGE_REQUIRES_CAPS` / `_STAGE_LANE_OVERRIDES`) appears in `_BOOTSTRAP_STAGE_SPECS`; a stranded key fails it loudly. Catalogue cardinality is separately pinned by a module-level `assert len(_BOOTSTRAP_STAGE_SPECS) == 23` in `bootstrap_orchestrator.py` that runs on import. Do not bypass either.

## Capability vocabulary (canonical at 2026-05-23)

Capabilities are typed via the `Capability = Literal[...]` declaration co-located with `_STAGE_PROVIDES`. Adding a new capability:

1. Extend the `Literal[...]` union in `bootstrap_orchestrator.py`.
2. Add provider entry to `_STAGE_PROVIDES` (and `_STAGE_PROVIDES_ON_SKIP` / `_ORDERING_ONLY_CAPS` per semantics).
3. Add consumer entry to `_STAGE_REQUIRES_CAPS`.
4. Capability name is a short snake_case noun-phrase (`cusip_mapping_ready`, `submissions_processed`, `cik_mapping_ready`, `institutional_dataset_processed`, etc.).

The cap layer is the dependency graph. `stage_order` is presentation order only — the dispatcher schedules off caps + lane availability via `_phase_batched_dispatch`'s `as_completed` loop. See [data-engineer/SKILL.md §6.5.1](SKILL.md) for the dispatcher mental model.

## Cap-ordering for shared-table writers (#1233 PR-1292)

When TWO stages on DIFFERENT lanes write to the SAME target table, they race for row locks unless explicitly serialised. Pattern:

- Bulk ingester provides `<table>_processed` cap on success AND skip.
- Legacy stage requires `<table>_processed` in `_STAGE_REQUIRES_CAPS`.
- Add cap to `_ORDERING_ONLY_CAPS` so cascade-blocked failures don't gate the legacy chain.

See [data-engineer/SKILL.md §6.5.10](SKILL.md) for the audit pattern + the four shipped pairs (S15↔S8, S22↔S10, S19↔S11, S20↔S11).

## Bootstrap-mode override patterns

A stage in `_BOOTSTRAP_STAGE_SPECS` runs ONLY during bootstrap. A `ScheduledJob` referenced by `job_name` runs ONLY in steady state. Three patterns for the bootstrap variant:

| Pattern | When | Example |
|---|---|---|
| **Same job, wider params** | Steady-state has a sliding window; bootstrap wants the full backfill | Historical: `sec_def14a_bootstrap` (dropped as a bootstrap stage by #1413) |
| **Same job, bounded params** | Steady-state is unbounded; bootstrap wants a sane cap | Historical: `sec_13f_recent_sweep` — `JOB_SEC_13F_QUARTERLY_SWEEP` with `min_period_of_report=today-380d` (dropped by #1413) |
| **Bootstrap-only stage** | No steady-state analogue (drains a one-time install state) | `sec_first_install_drain`, `cusip_universe_backfill` |

Note: #1413's bulk-only bootstrap redesign dropped the per-CIK "same job, wider/bounded params" stages; the live catalogue is bulk ingesters + bootstrap-only stages. Patterns 1-2 remain the convention if a future non-bulk source needs a bootstrap variant.

Do NOT re-wrap dispatch in a `bootstrap_<x>`-prefix callable just to override params. Three such bespoke wrappers (`bootstrap_filings_history_seed`, `sec_first_install_drain_job`, `bootstrap_sec_13f_recent_sweep_job`) were removed in #1064 PR1 and collapsed back to data-only StageSpec. (A distinct backfill *body* like `fundamentals_sync_bootstrap` is a normal `_INVOKERS` entry, not this anti-pattern.)

## Forbidden patterns

These fail the catalogue-invariant test OR violate the cap-graph contract:

- Adding a `stage_key` to `_STAGE_PROVIDES` / `_STAGE_REQUIRES_CAPS` without a matching `_spec()` entry → catalogue-invariant test fails.
- Two `_BOOTSTRAP_STAGE_SPECS` entries with the same `stage_key` → duplicate-key assertion.
- `_STAGE_LANE_OVERRIDES` pointing to an unregistered lane → the `bootstrap_stages.lane` CHECK constraint (latest: sql/165) rejects the row at `start_run` insert, and the `Lane` Literal ([`app/jobs/sources.py`](../../../app/jobs/sources.py)) fails typecheck. (A lane missing only from `_LANE_MAX_CONCURRENCY` does NOT crash — `.get(lane, 1)` silently defaults it to concurrency 1, a hidden single-file bottleneck.)
- Declaring a `_STAGE_PROVIDES_ON_SKIP` cap without the same cap in `_STAGE_PROVIDES` → cap only fires on skip; success advertises nothing.
- Hard-coding `with conn.transaction():` inside the invoker body — orchestrator owns the transaction boundary; see [data-engineer/SKILL.md §6.5.1](SKILL.md) "Caller-wraps-transaction discipline".
- Calling `record_<cat>_observation` without paired `refresh_<cat>_current(instrument_id)` (legacy pre-#1162 hazard) — leaves `_current` empty.

## Target-state extensions (PROPOSED, NOT CURRENT)

The v3 ETL rollout plan (`docs/_archive/2026-05/superseded-etl-rollout-v3.md`, REJECTED in committee 2026-05-23) proposed extending StageSpec to 17 fields including `fetch_strategy`, `row_budget`, `expected_units`, `max_http_count`, `progress_heartbeat_rows`, `idempotency_contract`, `sink_table_refs`. **None of these are in code today.** Do NOT reference them in PR descriptions or specs as if they exist — see [data-engineer/SKILL.md §6.5.16](SKILL.md) "Hallucinated-API class of defect".

The `fetch_strategy` enum is a particularly useful proposal (it gates `forbidden_http_in_bootstrap` linting per [data-engineer/SKILL.md §6.5.14](SKILL.md)) but it currently lives as a NAMING CONVENTION + per-stage documentation in code comments — not a StageSpec field. Adding it as a real field requires:

1. Extending `StageSpec` dataclass (breaks `_spec()` signature without a default).
2. Updating every `_spec(...)` call site (23 of them) to pass `fetch_strategy`.
3. Adding a catalogue-invariant test asserting every spec declares it.
4. Updating the dispatcher's bootstrap-mode HTTP detector to read from the field.

That's a real PR, NOT a documentation edit. Stream-A of the post-v3 rollout (`docs/proposals/etl/stream-a-run-8-fixes.md`) covers this scope decision.

## Cross-references

- [data-engineer/SKILL.md §6.5](SKILL.md) — pipeline orchestration invariants (lanes, transactions, sink registry, cap-ordering).
- [data-engineer/SKILL.md §6.5.14-16](SKILL.md) — fetch_strategy / bootstrap-derivation / hallucinated-API class.
- [data-engineer/etl-endpoint-coverage.md §5](etl-endpoint-coverage.md) — bootstrap stage reference + `_STAGE_LANE_OVERRIDES`.
- [bootstrap_state.py:143](../../../app/services/bootstrap_state.py#L143) — `StageSpec` source.
- [bootstrap_orchestrator.py:143](../../../app/services/bootstrap_orchestrator.py#L143) — `_spec()` constructor + cap maps.
- `tests/test_bootstrap_orchestrator.py` — catalogue-invariant tests (`test_stage_keyed_dicts_reference_only_real_stages`, `test_stage_catalogue_cardinality`).
