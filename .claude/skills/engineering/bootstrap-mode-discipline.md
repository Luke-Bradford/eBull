---
name: bootstrap-mode-discipline
description: Use before writing any code that runs during the bootstrap window (bootstrap_state.status ∈ {pending, running, partial_error}). Documents the load-bearing "bootstrap = derivation + idempotent-sink only" invariant, the explicit carve-outs (sources without bulk archives), the coverage-floor pattern, the audit-during-bootstrap trap, and the physical-separation pattern for ingest paths that exist in both modes. Cross-cutting — applies to fundamentals_sync, sec_first_install_drain, cusip_resolver_post_bulk_sweep, and any new bootstrap stage.
---

# Bootstrap-mode discipline

## When to use

Read before writing or modifying any code path that runs as part of a bootstrap stage. Specifically:

- Adding a new entry to `_BOOTSTRAP_STAGE_SPECS`.
- Modifying a service that exists in BOTH bootstrap and steady-state forms (`fundamentals_sync_bootstrap` vs steady-state `fundamentals_sync`; `sec_first_install_drain` bootstrap-params vs its steady-state form, etc.).
- Calling `audit_all_instruments` or any classifier that reads from `filing_events` / `data_freshness_index`.
- Designing a new "coverage check" that gates downstream stages.
- Reviewing a Run #N receipt where a bootstrap stage exceeded its wall-clock budget by 2× or more.

Skip for: code paths gated by `_bootstrap_complete` prerequisite (those CAN'T run during bootstrap by construction — they only run post-bootstrap).

## The invariant

**Bootstrap mode = derivation + idempotent-sink only.** A stage running while `bootstrap_state.status ∈ {pending, running, partial_error}` MUST source its inputs from already-persisted SEC bulk archives (`submissions.zip`, `companyfacts.zip`, 13F/insider/N-PORT quarterly zips) or already-derived DB rows (`financial_facts_raw`, `filing_events`, `instrument_sec_profile`). It MUST NOT issue per-CIK or per-accession HTTP fetches.

The three explicit carve-outs (CIK directory, institutional drain, OpenFIGI sweep) exist because those sources have no bulk archive. Every OTHER stage that issues HTTP during bootstrap is a defect — even if it "works" (passes Run #N wall-clock), it's redundant with a bulk-archive predecessor and adds quadratic-in-CIK-count cost.

Provenance: Run #7 measured `fundamentals_sync` at 101 min because its Phase 1 issued 5,105 sequential per-CIK XBRL fetches. The bulk archive (`companyfacts.zip`, S9) had already loaded the same 16.5M facts in 15 min. The HTTP path was redundant AND O(N) per CIK. Fix shape: separate `fundamentals_sync_bootstrap` entrypoint that DERIVES from `financial_facts_raw` instead of fetching — the load-bearing change for the Stream A path.

## Carve-outs — the three exceptions

These are the ONLY three bootstrap stages permitted to issue per-resource HTTP. Adding a fourth requires explicit justification + Codex sign-off:

| Stage | Why no bulk archive | Cap mechanism |
|---|---|---|
| **S6 `cik_refresh`** | SEC's `data.sec.gov/submissions/CIK*.json` is per-CIK; no bulk archive exists | Bounded to tradable-instrument cohort (~12k CIKs); conditional GET via ETag |
| **S16 `sec_first_install_drain`** | 13F filer registry has no bulk archive; per-CIK submissions polls needed | Bounded by `institutional_filers.last_13f_hr_at` cohort post-#1010 / #1222 (~3-5k CIKs vs full 11k) |
| **S13 `cusip_resolver_post_bulk_sweep`** | CUSIP→ticker reverse-lookup; no SEC equivalent | OpenFIGI's own `openfigi` lane (cap=1) + per-instance `_RateLimiter`; batched POST (10/100 mappings per call) |

Each carve-out's `fetch_strategy` is `per_resource_http` (S6/S16) or `batched_http` (S13). No other `fetch_strategy ∈ {per_resource_http, batched_http}` is permitted during bootstrap without joining this table.

Historical: a fourth carve-out, S27 `sec_n_csr_bootstrap_drain` (per-RIC-trust N-CSR body-walk — N-CSR annual reports have no bulk archive), was **dropped from the bootstrap graph by #1413** (bulk-only bootstrap; see the `_BOOTSTRAP_STAGE_SPECS` stage-count assert in `bootstrap_orchestrator.py`). N-CSR discovery + parse is now steady-state's job (Atom / daily-index layers + manifest worker against the S26 `mf_directory_sync`-seeded fund directory); `sec_n_csr_bootstrap_drain` survives only as an operator-triggerable invoker (`scheduler.py::sec_n_csr_bootstrap_drain`), never as a bootstrap stage.

## User-triggered lazy fill (#1343) — NOT a bootstrap-stage fetch

The bootstrap-mode HTTP rule governs bootstrap **stages** (code the orchestrator dispatches while `bootstrap_state.status != 'complete'`). It does NOT govern a per-resource HTTP fetch triggered by a **user API read** — e.g. #1343's lazy 10-K Item 1 / 8-K body fill, where viewing an instrument's panel fetches a single deferred body on first access (`app/api/instruments.py` → `fetch_business_summary_body_now` / `fetch_eight_k_body_now`).

Sanctioned because it is:

- **User-paced + single-doc** — one document per click, not an N-CIK sweep; no quadratic-in-cohort cost.
- **Not a stage** — runs in the request path, never from `_BOOTSTRAP_STAGE_SPECS`; the orchestrator can't schedule it, so it can't blow a stage's wall-clock budget.
- **The whole point** — #1343 deliberately moves the body fetch OUT of bootstrap (S16 seeds the manifest row `'deferred'`; S18/S21 seed metadata only) so the fetch happens lazily, on demand, after bootstrap.

A lazy fill may run while `bootstrap_state` is still `running` (an operator viewing a panel mid-bootstrap) — that single fetch is fine. What stays forbidden is a bootstrap **stage** issuing per-resource HTTP outside the four carve-outs above.

## Physical-separation pattern

A service that exists in both bootstrap and steady-state forms MUST physically separate the two entrypoints. Three patterns:

### Pattern 1 — Separate entrypoint, shared `_common.py`

```python
# app/services/fundamentals/__init__.py
from .bootstrap import fundamentals_sync_bootstrap  # derivation-only, NO HTTP
from .steady_state import fundamentals_sync          # HTTP fallback permitted post-bootstrap
from . import _common  # side-effect-free helpers

# app/services/fundamentals/bootstrap.py
def fundamentals_sync_bootstrap(conn: Connection) -> Result:
    """DERIVATION ONLY. Read financial_facts_raw + financial_periods_raw. No HTTP."""
    ...
    # NOT: for cik in ciks: http_get(companyconcept/CIK{cik}.json)
    # YES: SELECT instrument_id, concept, ... FROM financial_facts_raw

# app/services/fundamentals/steady_state.py
def fundamentals_sync(conn: Connection) -> Result:
    """Steady-state: bulk path FIRST, HTTP top-up for late-filing CIKs."""
    ...
```

`_common.py` MUST be side-effect-free (pure transforms, no HTTP, no DB writes). Sharing the HTTP-issuing helper between bootstrap + steady-state defeats the separation.

(Illustrative shape — filenames are the pattern's ideal, not the live layout. In the current tree the derivation-only entrypoint is real at `app/services/fundamentals/bootstrap.py::fundamentals_sync_bootstrap`; the steady-state `fundamentals_sync` lives in `app/workers/scheduler.py`, and there is no `steady_state.py` / `_common.py` split yet.)

### Pattern 2 — Same job, different params

When the only difference is bootstrap-specific params (a wider `since` window, or archive-routing knobs like `use_bulk_zip` / `follow_pagination`), use ONE invoker with `params` overrides via `StageSpec.params` ([data-engineer/etl-stage-declaration.md](../data-engineer/etl-stage-declaration.md) §Bootstrap-mode override patterns). NO separate entrypoint.

Used by: `sec_first_install_drain` — its S16 bootstrap stage carries `params={"use_bulk_zip": True, "follow_pagination": False, ...}` so the bulk-only path routes primary reads through the local `submissions.zip` and skips the per-CIK secondary-page HTTP walk, while the same `JOB_SEC_FIRST_INSTALL_DRAIN` invoker runs with its default (HTTP) params when triggered outside bootstrap. It is currently the only active bootstrap stage carrying `params` overrides; the former canonical example `sec_def14a_bootstrap` (wider `since` window on `JOB_SEC_DEF14A_INGEST`) was dropped by #1413.

### Pattern 3 — Bootstrap-only stage with no steady-state analogue

When the stage drains one-time install state (`cusip_universe_backfill` populates the 13F Official List bridge; `sec_first_install_drain` walks per-CIK submissions to bootstrap the manifest), there IS no steady-state analogue. The stage has its own `job_name` and only appears in `_BOOTSTRAP_STAGE_SPECS`, never in `SCHEDULED_JOBS`.

## Coverage-floor pattern (#1233 PR-1b)

When a bootstrap-mode entrypoint DERIVES from a multi-source pool, validate per-CIK coverage BEFORE deriving — otherwise the derivation passes wall-clock while the dataset is structurally incomplete.

```python
def fundamentals_sync_bootstrap(conn: Connection) -> Result:
    # 1. Compute coverage signal.
    coverage_ratio = _compute_coverage_ratio(conn)  # e.g. 0.85
    coverage_floor_met = coverage_ratio >= 0.80

    # 2. Stamp the bootstrap_runs row so the admin panel chip renders.
    _update_coverage_floor_met(conn, coverage_floor_met=coverage_floor_met)

    # 3. Continue deriving regardless — coverage is INFORMATIONAL, not blocking.
    return _derive_fundamentals(conn)
```

Three semantic rules:

1. **Informational, not blocking.** A FALSE coverage signal does NOT abort the stage. Aborting would create a cliff where every run with a single missing CIK fails. The signal surfaces via `bootstrap_runs.coverage_floor_met` + an amber admin-panel chip.
2. **Null = sweep didn't run.** Distinct from FALSE (sweep ran, floor missed). The admin panel renders null as "unknown coverage" not "coverage breach".
3. **Threshold is a settled decision per source.** OpenFIGI sweep settled at 0.80; other sources MAY pick different thresholds. Codify in `docs/settled-decisions.md`, not in code.

The pattern generalises: every bootstrap-mode stage that reads from a sparse table SHOULD record a coverage telemetry signal. The cost of forgetting it is a Pyrrhic Run #N that passes wall-clock + fails completeness silently.

## The audit-during-bootstrap trap

`audit_all_instruments` at [`coverage.py:1018`](../../../app/services/coverage.py#L1018) classifies instruments from `filing_events` aggregates ([`coverage.py:940-966`](../../../app/services/coverage.py#L940-L966)). When run BEFORE S8 (`sec_submissions_ingest`) has loaded `filing_events` from `submissions.zip` (and S16 `sec_first_install_drain` seeds the remaining non-issuer subjects), it returns false `insufficient` verdicts for any instrument whose filing history hasn't loaded yet. (S8 provides `filing_events_seeded` + `submissions_processed`; the legacy S14 `sec_submissions_files_walk` / S15 `filings_history_seed` stages were dropped by #1413.)

A false `insufficient` triggers the Phase 2 eligibility-gated backfill in [`scheduler.py:4042-4067`](../../../app/workers/scheduler.py#L4042-L4067), which reintroduces per-CIK HTTP — the exact thing bootstrap-mode discipline forbids. Net effect: the audit call REINTRODUCES the bug bootstrap-mode separation was supposed to fix.

**Rule:** any bootstrap-mode entrypoint calling `audit_all_instruments` MUST gate the call on `submissions_processed` AND `filing_events_seeded` capabilities being satisfied. Gate via `CapRequirement(all_of=("submissions_processed", "filing_events_seeded"))` on the calling stage, NOT by stage_order — order is presentation only; caps are the actual dependency graph (see [data-engineer/etl-stage-declaration.md](../data-engineer/etl-stage-declaration.md) §Capability vocabulary).

If you cannot gate the call on caps (e.g. the audit must run inside a non-staged service), defer the audit to post-bootstrap — the steady-state `fundamentals_sync` job already runs `audit_all_instruments` in its Phase 2 coverage audit ([`scheduler.py:4046`](../../../app/workers/scheduler.py#L4046)).

## Idempotent-sink contract

Bootstrap stages MUST be safely re-runnable. A second run after a partial failure MUST converge to the same final state. Three concrete obligations:

1. **All writes through ON CONFLICT or MERGE.** No "if not exists, INSERT" race window — that's a doubled-row defect waiting to land.
2. **No accumulating side-effects.** Don't write `+= 1` counters or append-only audit rows on every retry — those count retries instead of unique facts. Use UPSERT keyed on the natural identifier.
3. **Caller owns the transaction.** Service body MUST NOT enter its own `with conn.transaction():` — orchestrator wraps per-archive / per-CIK so a partial failure rolls back cleanly. See [data-engineer/SKILL.md §6.5.1](../data-engineer/SKILL.md) "Caller-wraps-transaction discipline".

## Forbidden patterns

These earn a BLOCKING in Codex review + a same-PR fix:

| Pattern | Why forbidden |
|---|---|
| Bootstrap stage with `fetch_strategy='per_resource_http'` not in the §"Carve-outs" table | Reintroduces N×per-CIK HTTP that defeats the bulk-archive bootstrap; #1233 Run #7 root cause |
| Bootstrap stage calling `audit_all_instruments` without cap-gating on `submissions_processed` + `filing_events_seeded` | Triggers Phase 2 HTTP backfill mid-bootstrap; reintroduces the bug bootstrap-mode prevents |
| Bootstrap stage reading from a sparse table without recording coverage telemetry | Pyrrhic Run #N — wall-clock passes, data completeness silently rots |
| Shared `_common.py` helper that issues HTTP, imported by both bootstrap + steady-state entrypoints | Defeats physical separation — both modes inherit the HTTP cost |
| Bootstrap stage with `with conn.transaction():` in the service body | Transaction boundary belongs to orchestrator; in-service tx breaks per-archive rollback semantics |
| Bootstrap stage gated on `stage_order` instead of `_STAGE_REQUIRES_CAPS` | Dispatcher schedules off caps; order is display-only — your gate doesn't fire |

## Post-bootstrap correctness gate (Stream C concept)

A Run #N that passes 60-min wall-clock has NOT proven correctness if the post-bootstrap steady-state jobs don't fire. v3's biggest residual risk (per Codex): bootstrap defers correctness to scheduled jobs (Layer 1/2/3 discovery, CUSIP resolver, fundamentals top-up) that #1155 has not yet wired. A 25-min bootstrap that breaks daily ingest is worse than a 90-min bootstrap that works.

Therefore: any spec that defers correctness work to a scheduled job MUST add an acceptance criterion proving the deferred job actually fires. Concrete shape:

> Run #N acceptance addition: "Within 24 h of bootstrap completion, observe one full daily cycle firing every `SCHEDULED_JOBS` entry without `bootstrap_not_complete` skip. Layer 1 (atom), Layer 2 (daily-index), Layer 3 (per-CIK poll) each emit ≥ 1 `job_runs` row with `status='success'`."

Without this, Run #N is a sand castle — wall-clock looks good, system silently rots.

## Cross-references

- [data-engineer/SKILL.md §6.5.14](../data-engineer/SKILL.md) — fetch_strategy enum.
- [data-engineer/SKILL.md §6.5.15](../data-engineer/SKILL.md) — bootstrap = derivation rule (this skill's twin).
- [data-engineer/SKILL.md §6.5.16](../data-engineer/SKILL.md) — hallucinated-API class.
- [data-engineer/etl-stage-declaration.md](../data-engineer/etl-stage-declaration.md) — registration checklist + cap vocabulary.
- [data-engineer/etl-spec-template-usage.md](../data-engineer/etl-spec-template-usage.md) §13 — bootstrap-vs-steady-state spec section.
- `app/services/bootstrap_orchestrator.py:168` — `_LANE_MAX_CONCURRENCY`.
- `app/services/coverage.py:1018` — `audit_all_instruments` (the trap caller).
- `app/services/bootstrap_state.py:144` — `StageSpec`.
- Memory: `project_etl_v3_review_codex.md` — Codex CTO biggest-residual-risk finding (deferring correctness to non-firing jobs).
