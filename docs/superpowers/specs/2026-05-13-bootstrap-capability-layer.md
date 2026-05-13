# Bootstrap capability layer + fallback semantics

**Date:** 2026-05-13
**Issue:** [#1138](https://github.com/Luke-Bradford/eBull/issues/1138)
(Task A of [#1136](https://github.com/Luke-Bradford/eBull/issues/1136)
audit)
**Status:** Draft — pending Codex review + user sign-off

## 1. Problem

`app/services/bootstrap_orchestrator.py::_STAGE_REQUIRES` is an
AND-only `dict[stage_key, tuple[str, ...]]`. Every named predecessor
must be `success` (or `skipped`) for a downstream stage to run. Two
fallout scenarios from the #1136 audit §1:

- **Partial bulk failure**: if `sec_bulk_download` (S7) errors
  mid-way, the Phase C ingesters propagate `blocked`. Downstream
  final stages like `ownership_observations_backfill` (S23) then
  block on the bulk-path requirement even though the legacy chain
  (S14..S22) can populate the same ownership tables.
- **Conflated skip semantics**: the slow-connection bypass (#1041)
  marks S7 + Phase C `skipped`. The current dispatcher treats
  `skipped` as dependency-satisfied. That works for the intentional
  bypass but conflates with the partial-error case — any downstream
  consumer can't tell whether the upstream succeeded or was
  deliberately skipped.

## 2. Goal

Replace the AND-only stage-name dependency graph with a named
**capability layer**. Stages `provide` capabilities on `success`;
downstream stages `require` capabilities via an explicit
all-of / any-of (DNF) shape. The fallback "bulk path OR legacy
path" becomes a first-class structural property of the graph
rather than implicit AND-only edges over both paths.

The audit and the Codex pre-spec review both agree this is the
correct shape; the alternatives (minimal DNF over raw stage keys,
or hybrid `provides` only on forking stages) leave a half-model
that Task C (#1140) would have to unwind.

## 3. Non-goals

- **Row-count gates** (`rows_written > 0` per producer). That's
  Task C / #1140; this spec adds the structural hook (a downstream
  stage requires a `Capability` — Task C can later widen
  `requires` to "capability provided AND producer.rows_written > 0").
- **DB lane concurrency** (Task E / #1141).
- **Atomic enqueue / reaper** (Task B / #1139).
- **Operator-visible `complete with warnings`** state (Task C).
- **Capability provisioning on partial-success** within a single
  stage (e.g. "S14 finished but only seeded 30% of the universe").
  Out of scope; producer status is still binary `success` vs not.

## 4. Capability vocabulary

Eleven named capabilities. The audit §1 listed eight candidates;
Codex's spec review flagged that the single `ownership_inputs_seeded`
cap collapses four distinct ownership data-families (insider / Form 3
/ institutional 13F / NPORT) and would let `ownership_observations_backfill`
fire after only one family completed. Splitting into per-family caps
preserves the original AND-ordering at the cap level. Every edge in
the existing `_STAGE_REQUIRES` graph maps onto a cap below; cap
splits add no new edges.

| Capability | Meaning |
|---|---|
| `universe_seeded` | `instruments` populated; downstream per-issuer iteration has a non-empty source. |
| `cik_mapping_ready` | `external_identifiers` populated for the universe; per-CIK SEC fetches resolve. |
| `cusip_mapping_ready` | `cusip_mappings` populated; CUSIP → instrument resolution available for 13F / NPORT ingest. |
| `bulk_archives_ready` | Bulk SEC archives downloaded **AND extracted**; Phase C DB ingesters have local files to read. Only provided on real bulk download — fallback mode does NOT advertise this cap (see §4.3). |
| `filing_events_seeded` | `filing_events` populated; typed parsers (def14a / 8K / business summary) have candidate rows. |
| `submissions_secondary_pages_walked` | Per-CIK secondary-pages walked; deep-history submissions present beyond the bulk archive truncation point. |
| `insider_inputs_seeded` | `insider_transactions` populated (Form 4 / 5); legacy or bulk path. |
| `form3_inputs_seeded` | Form 3 initial-ownership filings ingested; legacy or bulk path. |
| `institutional_inputs_seeded` | `institutional_holdings` populated (13F); legacy or bulk path. |
| `nport_inputs_seeded` | `n_port_*` tables populated; legacy or bulk path. |
| `fundamentals_raw_seeded` | `company_facts` populated; `fundamentals_sync` can derive financial-statement rows. |

### 4.1 Provider table (on `success`)

```python
_STAGE_PROVIDES: dict[str, tuple[Capability, ...]] = {
    "universe_sync":                       ("universe_seeded",),
    "cusip_universe_backfill":             ("cusip_mapping_ready",),
    "cik_refresh":                         ("cik_mapping_ready",),
    # S7 bulk download — provides bulk_archives_ready ONLY on real
    # bulk mode. Fallback mode raises BootstrapPhaseSkipped (see §4.3)
    # so the stage transitions to `skipped` not `success` and this
    # entry never fires.
    "sec_bulk_download":                   ("bulk_archives_ready",),
    "sec_submissions_ingest":              ("filing_events_seeded",),
    "sec_companyfacts_ingest":             ("fundamentals_raw_seeded",),
    # Bulk ownership ingester covers both insider txns + Form 3.
    "sec_insider_ingest_from_dataset":     ("insider_inputs_seeded", "form3_inputs_seeded"),
    "sec_13f_ingest_from_dataset":         ("institutional_inputs_seeded",),
    "sec_nport_ingest_from_dataset":       ("nport_inputs_seeded",),
    "sec_submissions_files_walk":          ("submissions_secondary_pages_walked",),
    "filings_history_seed":                ("filing_events_seeded",),
    # S15 sec_first_install_drain runs with follow_pagination=True
    # (app/workers/scheduler.py:4410), so it walks the same
    # secondary-pages surface the dedicated walker (S13) covers.
    # Providing both caps lets typed parsers run on the legacy /
    # slow-connection path where S13 is skipped.
    "sec_first_install_drain":             ("filing_events_seeded", "submissions_secondary_pages_walked"),
    "sec_insider_transactions_backfill":   ("insider_inputs_seeded",),
    "sec_form3_ingest":                    ("form3_inputs_seeded",),
    "sec_13f_recent_sweep":                ("institutional_inputs_seeded",),
    "sec_n_port_ingest":                   ("nport_inputs_seeded",),
}
```

Stages absent from `_STAGE_PROVIDES` provide nothing
(`candle_refresh`, the two filer-directory syncs, the typed
parsers def14a / 8K / business summary, the two final-derivation
stages). That's intentional — nothing downstream consumes those
caps today.

### 4.3 `sec_bulk_download` fallback fix

`app/services/sec_bulk_download.py:986-1002` currently returns
normally in `mode == "fallback"` after writing a fallback manifest,
so the stage transitions to `success`. Under the capability layer
that would falsely advertise `bulk_archives_ready` even though no
archives were downloaded.

**Change**: after `write_run_manifest(..., mode="fallback")`,
raise `BootstrapPhaseSkipped(reason=f"slow-connection fallback; mbps={result.measured_mbps}")`.
The orchestrator already maps that to `status='skipped'`
(orchestrator.py:490-500). Side effects (fallback manifest write,
Phase C bypass detection) are preserved; only the stage status
flips from `success` to `skipped`.

Existing tests for `sec_bulk_download_job` fallback path update
to expect `BootstrapPhaseSkipped` instead of normal return.

### 4.2 `_STAGE_PROVIDES_ON_SKIP` — explicit, initially empty

Skipped stages do NOT provide capabilities by default. The
intentional slow-connection fallback (#1041) works because the
**legacy chain** provides the same caps later, not because the
skipped bulk stage masquerades as a provider.

`_STAGE_PROVIDES_ON_SKIP: dict[str, tuple[Capability, ...]] = {}`
exists as a documented per-stage escape hatch but is empty in
this spec. Any future entry needs an explicit comment justifying
why the skip is semantically equivalent to success.

## 5. Requirement shape

```python
@dataclass(frozen=True)
class CapRequirement:
    all_of: tuple[Capability, ...] = ()
    any_of: tuple[tuple[Capability, ...], ...] = ()
```

Semantics: a requirement is satisfied iff
- every cap in `all_of` is present in the satisfied set, AND
- (`any_of` is empty) OR (at least one inner tuple is fully ⊆ satisfied set).

Common shapes:

- No deps: `CapRequirement()`
- Linear: `CapRequirement(all_of=("universe_seeded",))`
- Forking (bulk OR legacy): `CapRequirement(any_of=(("bulk_path_caps", ...), ("legacy_path_caps", ...)))`
- Mixed (always-required prerequisites PLUS a forking choice):
  `CapRequirement(all_of=("cik_mapping_ready",), any_of=(...))`

### 5.1 Requirement table

```python
_STAGE_REQUIRES_CAPS: dict[str, CapRequirement] = {
    # Phase A
    "universe_sync": CapRequirement(),
    "candle_refresh": CapRequirement(all_of=("universe_seeded",)),
    "cusip_universe_backfill": CapRequirement(all_of=("universe_seeded",)),
    "sec_13f_filer_directory_sync": CapRequirement(all_of=("universe_seeded",)),
    "sec_nport_filer_directory_sync": CapRequirement(all_of=("universe_seeded",)),
    "cik_refresh": CapRequirement(all_of=("universe_seeded",)),
    "sec_bulk_download": CapRequirement(all_of=("universe_seeded",)),
    # Phase C — DB-bound bulk ingesters
    "sec_submissions_ingest": CapRequirement(all_of=("bulk_archives_ready", "cik_mapping_ready")),
    "sec_companyfacts_ingest": CapRequirement(all_of=("bulk_archives_ready", "cik_mapping_ready")),
    "sec_13f_ingest_from_dataset": CapRequirement(all_of=("bulk_archives_ready", "cusip_mapping_ready")),
    "sec_insider_ingest_from_dataset": CapRequirement(all_of=("bulk_archives_ready", "cik_mapping_ready")),
    "sec_nport_ingest_from_dataset": CapRequirement(all_of=("bulk_archives_ready", "cusip_mapping_ready")),
    # Phase C' — walker
    "sec_submissions_files_walk": CapRequirement(all_of=("filing_events_seeded",)),
    # Legacy chain — straight-line caps off cik_mapping_ready
    "filings_history_seed": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_first_install_drain": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_def14a_bootstrap": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_business_summary_bootstrap": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_insider_transactions_backfill": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_form3_ingest": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_8k_events_ingest": CapRequirement(all_of=("filing_events_seeded", "submissions_secondary_pages_walked")),
    "sec_13f_recent_sweep": CapRequirement(all_of=("cik_mapping_ready",)),
    "sec_n_port_ingest": CapRequirement(all_of=("cik_mapping_ready",)),
    # Phase E — final derivations. Per-family caps (§4) mean each
    # of the 4 ownership families is satisfied by EITHER its bulk
    # ingester OR its legacy ingester. Backfill requires all 4
    # families seeded — no `any_of` needed because the alternatives
    # are encoded inside each cap's provider list.
    "ownership_observations_backfill": CapRequirement(
        all_of=(
            "cik_mapping_ready",
            "insider_inputs_seeded",
            "form3_inputs_seeded",
            "institutional_inputs_seeded",
            "nport_inputs_seeded",
        ),
    ),
    "fundamentals_sync": CapRequirement(all_of=("fundamentals_raw_seeded",)),
}
```

Each per-family cap has two providers (one bulk, one legacy). The
dispatcher considers a cap satisfied if **any** provider reached
`success`. That's the structural win — the orchestrator no longer
needs to encode "bulk OR legacy" at every consumer; the cap layer
encodes alternatives at the producer side.

Concrete examples:

- `institutional_inputs_seeded` is provided by `sec_13f_ingest_from_dataset`
  (bulk, S10) and `sec_13f_recent_sweep` (legacy, S21). If S10
  errors but S21 succeeds, the cap is satisfied and backfill
  proceeds.
- The original `_STAGE_REQUIRES` listed all 7 ownership ingesters
  (3 bulk + 4 legacy) as AND-required, so a single bulk-ingester
  failure would block backfill even when the legacy chain fully
  succeeded. The per-family cap shape eliminates that false
  negative.

## 6. Dispatcher changes

Three helper functions in `app/services/bootstrap_orchestrator.py`:

```python
def _satisfied_capabilities(stages: Sequence[StageRow]) -> set[Capability]:
    """Cap set derived from current stage statuses."""
    caps: set[Capability] = set()
    for stage in stages:
        if stage.status == "success":
            caps.update(_STAGE_PROVIDES.get(stage.stage_key, ()))
        elif stage.status == "skipped":
            caps.update(_STAGE_PROVIDES_ON_SKIP.get(stage.stage_key, ()))
    return caps


def _capability_is_dead(cap: Capability, stages: Sequence[StageRow]) -> bool:
    """A cap is dead iff EVERY provider stage is in a state where
    it cannot now (or in the future) provide the cap.

    Cannot-provide states:
    * ``error`` / ``blocked`` / ``cancelled`` — terminal failure.
    * ``skipped`` AND cap NOT in this stage's ``_STAGE_PROVIDES_ON_SKIP``
      entry — the skip explicitly does not provide this cap.

    Can-still-provide states:
    * ``pending`` / ``running`` — provider hasn't decided yet.
    * ``success`` providing the cap — the cap is alive (in fact
      satisfied; this branch is for completeness).
    * ``skipped`` AND cap IS in ``_STAGE_PROVIDES_ON_SKIP[stage]``
      — explicit skip-provides path.
    """
    providers = _CAPABILITY_PROVIDERS.get(cap, ())
    if not providers:
        # Cap with no provider listed — dead by construction.
        # The catalogue-invariant test (§8) prevents this at test
        # time, so the runtime check is defence-in-depth.
        return True
    by_key = {s.stage_key: s for s in stages}
    for provider_key in providers:
        stage = by_key.get(provider_key)
        if stage is None:
            continue  # shouldn't happen given start_run seeds every spec
        if stage.status in ("pending", "running"):
            return False
        if stage.status == "success":
            return False  # already satisfied (defensive)
        if stage.status == "skipped":
            on_skip = _STAGE_PROVIDES_ON_SKIP.get(provider_key, ())
            if cap in on_skip:
                return False  # explicit skip-provides
    return True


def _requirement_satisfied(req: CapRequirement, caps: set[Capability]) -> bool:
    if not all(c in caps for c in req.all_of):
        return False
    if not req.any_of:
        return True
    return any(all(c in caps for c in group) for group in req.any_of)
```

The existing `_build_runnable` (orchestrator.py:919-945) is
rewritten to:

1. Compute `caps = _satisfied_capabilities(stages)` once per
   dispatcher pass.
2. For each pending stage:
   - If `_requirement_satisfied(req, caps)` is true → runnable.
   - Else, evaluate **unsatisfiability**: build the set of caps
     the requirement asks for (all caps in `all_of` plus every cap
     in every `any_of` group). For the requirement to be
     unsatisfiable, **all caps in `all_of` must be alive-or-dead**
     such that the requirement can never be satisfied. Concretely:
     - If any cap in `all_of` is dead → requirement is unsatisfiable.
     - Else if `any_of` is non-empty AND every `any_of` group
       contains at least one dead cap → requirement is unsatisfiable.
     - Else → still satisfiable; leave stage as `pending`.
3. Unsatisfiable → mark stage `blocked` with structured reason.

### 6.1 Provider-state inverse map

To check unsatisfiability efficiently, build the inverse once at
module load:

```python
_CAPABILITY_PROVIDERS: dict[Capability, tuple[str, ...]]
```

Derived from `_STAGE_PROVIDES` plus `_STAGE_PROVIDES_ON_SKIP`. A
cap is **dead** per `_capability_is_dead()` above.

### 6.2 Structured blocked reason

When a stage transitions to `blocked` because a cap is dead:

```text
blocked: missing capability {cap}; no surviving provider
(providers: {provider_a}=error, {provider_b}=skipped (no provides-on-skip), ...)
```

For `any_of` requirements where every branch is unsatisfiable:

```text
blocked: every alternative dependency group has a dead capability —
group 1 missing {cap_x} (providers: ...); group 2 missing {cap_y} (providers: ...)
```

The string lands in `bootstrap_stages.last_error` and the Timeline
/ admin UI surfaces it unchanged.

### 6.3 Cascade-skip rule — distinguish skip-only-dead from error-dead

The spec inverts the implicit "skipped == satisfied" semantics
(§4.2), so a stage required cap can become dead purely because its
provider(s) skipped. Two sub-cases:

**Skip-only-dead**: every provider of the cap is in `skipped` status
(none in `error` / `blocked` / `cancelled`), AND none of those skips
provides the cap via `_STAGE_PROVIDES_ON_SKIP`. Operator intent is
"path deliberately bypassed"; downstream stages on the same path
should also skip, not block.

**Error-dead**: at least one provider is in `error` / `blocked` /
`cancelled`. Operator intent is "path failed"; downstream stages
should block with a structured reason.

Rule: when `_build_runnable` finds a stage with a dead required
cap, classify the deadness:

```python
def _classify_dead_cap(cap: Capability, stages: Sequence[StageRow]) -> Literal["skip_only", "error"]:
    """Returns the failure mode that killed a (confirmed-dead) cap.

    Precondition: `_capability_is_dead(cap, stages)` is True.

    Returns `"error"` (block downstream) when ANY provider is in
    `error` / `blocked` / `cancelled`. Returns `"skip_only"` only
    when every provider is `skipped` without an explicit
    `provides_on_skip` entry — the path was deliberately bypassed.

    Defensive default: a cap with zero registered providers is
    classified `"error"` (the catalogue-invariant test in §8 should
    have caught this at test time; at runtime we surface as a hard
    block rather than silently cascading skip).
    """
    providers = _CAPABILITY_PROVIDERS.get(cap, ())
    if not providers:
        return "error"
    by_key = {s.stage_key: s for s in stages}
    saw_skipped = False
    for provider_key in providers:
        stage = by_key.get(provider_key)
        if stage is None:
            continue
        if stage.status in ("error", "blocked", "cancelled"):
            return "error"
        if stage.status == "skipped":
            saw_skipped = True
    return "skip_only" if saw_skipped else "error"
```

The dispatcher's transition decision:

- **All required caps satisfied** → runnable.
- **Any required cap is dead AND classified `error`** → block stage
  with structured reason (§6.2).
- **All required dead caps are classified `skip_only`** → cascade
  the stage to `skipped` with reason
  `"cascaded skip: required capability {cap} provided only by skipped upstream(s)"`.
  The dispatcher transitions the stage directly to `status='skipped'`
  without invoking; the existing `mark_stage_skipped` helper applies.

Concrete examples:

- Slow-connection fallback: S7 skips via `BootstrapPhaseSkipped`
  (§4.3). `bulk_archives_ready` is skip-only-dead. Phase C C-stages
  (S8..S12) cascade to `skipped` without invoking — no need for
  the per-stage fallback-manifest pre-flight. The walker S13
  requires `filing_events_seeded`; its providers are S8 (skipped,
  doesn't provide) + S14 (`filings_history_seed`, pending) + S15
  (`sec_first_install_drain`, pending). The cap stays alive while
  S14 / S15 are pending → S13 stays pending until they resolve.
  Once S14 / S15 succeed, S13 runs to `success` over the same
  secondary-pages surface.
- `sec_companyfacts_ingest` (S9) cascades to `skipped` in
  fallback (same reason as Phase C). Then
  `fundamentals_raw_seeded` is skip-only-dead. S24
  `fundamentals_sync` cascades to `skipped`. Bootstrap completes
  with S24 = `skipped`, not `blocked`. Operator-visible result:
  fundamentals not backfilled in fallback — that's a known
  fallback-path limitation, addressed by #414 (legacy fundamentals
  redesign), out of scope for Task A.

This cascade rule replaces the implicit "dispatch first, let the
invoker self-skip" mechanism. The pre-flight self-skip logic in
existing C-stage invokers becomes dead code; deleting it is a
follow-up cleanup, not blocking for Task A.

### 6.4 Preexisting status handling — add `cancelled`

The current `_build_runnable` collects preexisting terminal
statuses as `{success, error, blocked, skipped}` — `cancelled`
is missing despite being a valid `StageStatus`. Add `cancelled`
to the preexisting set so retry-failed flows handle cancel-by-operator
correctly. (NIT 2 from spec review.)

## 7. Compatibility + migration

- `_STAGE_REQUIRES` is **removed**. No deprecation period — the
  dispatcher is the sole consumer in this repo (verified by grep).
- No DB migration. `bootstrap_stages` rows are still keyed by
  `stage_key`.
- `_STAGE_LANE_OVERRIDES` unchanged.
- Stage catalogue + count unchanged. The 24-stage assertion at
  module-load is preserved.
- Existing tests in `tests/test_bootstrap_orchestrator.py` continue
  to pass — the happy-path graph evaluates identically (every
  edge in the old graph maps onto a cap edge in the new graph,
  verified by the requirement table in §5.1).

## 8. Testing

New tests in `tests/test_bootstrap_orchestrator.py` covering the
three fallback shapes the audit §6 called out as missing. These
replace the deferred-from-#1137 fallback-shape tests.

1. **`test_partial_bulk_failure_legacy_recovers`** — force
   `sec_bulk_download` to error (NOT skip — error means terminal
   failure, not intentional fallback). All legacy ownership stages
   succeed. Assert:
   - `ownership_observations_backfill` reaches `success` — each
     per-family ownership cap (`insider_inputs_seeded`,
     `form3_inputs_seeded`, `institutional_inputs_seeded`,
     `nport_inputs_seeded`) is satisfied by its legacy provider.
   - The 5 Phase C bulk ingesters (S8..S12) transition to
     `blocked` with reason naming `bulk_archives_ready` as the
     error-dead capability (NOT cascaded-skipped — the deadness
     is error-classified per §6.3).
   - `bootstrap_state.status` is `partial_error`.

2. **`test_intentional_slow_connection_skip`** — force
   `sec_bulk_download` to raise `BootstrapPhaseSkipped` (the new
   fallback path in §4.3). All legacy stages succeed. Assert:
   - S7 ends `skipped` (not `success`).
   - The **5** Phase C bulk ingesters (S8..S12) cascade to
     `skipped` per §6.3 — `bulk_archives_ready` is skip-only-dead,
     so the dispatcher transitions them directly without invoking.
     The invokers are NOT called (assert this via the fake-invoker
     call log).
   - `sec_companyfacts_ingest` (S9) cascades to `skipped` →
     `fundamentals_raw_seeded` is skip-only-dead →
     `fundamentals_sync` (S24) cascades to `skipped`.
   - `sec_submissions_files_walk` (S13) reaches `success`:
     `filing_events_seeded` stays alive while S14 / S15 are
     pending (S8 cascading to `skipped` is not the only provider),
     and once S14 / S15 succeed the cap is satisfied so S13 runs.
     Assert S13 ends in `success`.
   - Typed parsers (S16 / S17 / S20) reach `success` —
     `submissions_secondary_pages_walked` is provided by
     `sec_first_install_drain` (legacy drain runs with
     `follow_pagination=True`).
   - `ownership_observations_backfill` (S23) reaches `success` —
     all 4 per-family ownership caps satisfied via legacy providers.
   - Skipped stages do NOT falsely advertise caps (verified
     against `_satisfied_capabilities` directly: skipped
     `sec_bulk_download` does not put `bulk_archives_ready` in
     the cap set).
   - `bootstrap_state.status` is `complete` (the audit §6
     acceptance: "complete with warnings" is Task C; for Task A,
     all-success-or-skip → complete).

3. **`test_both_paths_fail_blocks_final_stage`** — force
   `sec_bulk_download` to error AND every legacy ownership stage
   to error (so both the bulk and legacy providers of every
   per-family ownership cap are dead). Assert:
   - `ownership_observations_backfill` transitions to `blocked`.
   - `last_error` contains the structured "missing capability"
     message naming at least one of the four per-family caps
     (whichever has the alphabetically-first dead cap; the
     dispatcher reports the first one it encounters).

Also add a **catalogue invariant** test:

4. **`test_every_required_capability_has_a_provider`** — for
   every cap appearing in `_STAGE_REQUIRES_CAPS.all_of` or any
   `any_of` group, assert `_CAPABILITY_PROVIDERS[cap]` is
   non-empty. Catches typo-style drift (a downstream requires a
   cap nobody provides) at test time.

5. **`test_every_stage_appears_in_requires_caps`** — assert every
   `stage_key` in `_BOOTSTRAP_STAGE_SPECS` has a `_STAGE_REQUIRES_CAPS`
   entry (even if it's `CapRequirement()`). Catches missing entries.

## 9. Acceptance criteria (from #1136 / #1138)

- [x] Partial `sec_bulk_download` failure can recover through
  fallback OR terminally fail with clear operator-visible reason.
  No downstream stage is blocked accidentally by AND-only
  dependency when an alternate source exists. **Test 1.**
- [x] Intentional slow-connection fallback marks bulk stages
  `skipped` and still allows the legacy path to complete.
  **Test 2.**
- [x] Failed bulk archive does not silently leave bootstrap in
  misleading `complete` state. **Test 3** + dispatcher
  unsatisfiability check transitions to `blocked` not silent skip.

## 10. Operator-visible behaviour

| Scenario | Before this spec | After this spec |
|---|---|---|
| Bulk path OK, legacy path OK | All success; `complete` | Same |
| Bulk fails (S7 error), legacy OK | Phase C `blocked`, S23/S24 `blocked` (false negative) | Phase C `blocked` with cap-reason; S23 `success` via per-family legacy providers; S24 `success` if `sec_companyfacts_ingest` was the surviving fundamentals provider |
| Slow-connection fallback (S7 raises `BootstrapPhaseSkipped`), legacy OK | S7 `success` (silently advertised `bulk_archives_ready` even with no archives); Phase C `skipped` via per-stage pre-flight; downstream consumed skipped-as-success | S7 `skipped`; Phase C (S8..S12) cascade to `skipped` via §6.3 (no invocation); S9 cascade → S24 cascade `skipped`; legacy chain provides `filing_events_seeded` + `submissions_secondary_pages_walked` via `sec_first_install_drain`; typed parsers + S23 ownership backfill reach `success` via legacy per-family caps |
| Both bulk + legacy fail | Downstream `blocked` with vague reason | Downstream `blocked` with structured "missing capability X; providers ..." reason |

No frontend change required — `last_error` is already rendered in
the admin Timeline.

## 11. Pre-flight review focus

- **Cap coverage**: every existing `_STAGE_REQUIRES` edge maps
  onto at least one cap in the new graph. The single coarse
  `ownership_inputs_seeded` cap from the audit's original list
  was split into four per-family caps in §4 to preserve the
  AND-ordering the old graph encoded. A reviewer should diff §4.1
  / §5.1 against the existing `_STAGE_REQUIRES` and confirm no
  ordering edge is silently lost.
- **`sec_bulk_download` fallback fix (§4.3)**: the change from
  "return success in fallback mode" to "raise `BootstrapPhaseSkipped`"
  is the only producer-side behaviour change. Reviewer should
  verify the fallback manifest is still written before the raise
  (the manifest is preserved for ops-monitor / audit purposes
  even though Phase C no longer reads it under cascade-skip).
- **Cascade-skip rule (§6.3)**: the dispatcher transitions Phase C
  C-stages directly to `skipped` without invoking when S7
  `BootstrapPhaseSkipped`s. The per-stage fallback-manifest
  pre-flight in C-stage invokers becomes dead code — deleting it
  is a follow-up cleanup, not blocking for Task A. Test 2 in §8
  is the regression gate: it asserts the invokers are NOT called
  on the cascade path.
- **Dead-cap detection (§6.1, §6.2)**: the unsatisfiability rule
  must trigger exactly when no provider can succeed, not earlier
  (while one provider is still `pending`/`running`). Misfire would
  either deadlock or false-block. The `_capability_is_dead()` +
  `_classify_dead_cap()` helpers in §6 encode this.
- **Skipped semantics**: confirm with the reviewer that "skipped
  provides nothing by default" is correct. The audit and Codex
  pre-spec review both endorsed this; spec inverts the current
  implicit "skipped == success" behaviour, so the legacy fallback
  path tests (test 2) are the acceptance gate.
- **`submissions_secondary_pages_walked` on the legacy path**:
  `sec_first_install_drain` provides this cap because it walks
  the same secondary-pages surface as `sec_submissions_files_walk`
  (per `follow_pagination=True` at scheduler.py:4410). If that
  parity claim is wrong, the typed parsers will block in
  slow-connection fallback mode — Codex BLOCKING 4. Reviewer
  should confirm parity against `sec_first_install_drain`'s
  actual implementation.

## 12. References

- [#1136](https://github.com/Luke-Bradford/eBull/issues/1136) §1 — original audit
- [#1138](https://github.com/Luke-Bradford/eBull/issues/1138) — sub-ticket
- [#1041](https://github.com/Luke-Bradford/eBull/issues/1041) — slow-connection fallback (the existing fallback path this spec generalises)
- [#1020](https://github.com/Luke-Bradford/eBull/issues/1020) — bulk-first bootstrap umbrella
- `docs/superpowers/specs/2026-05-07-first-install-bootstrap.md` — original 17-stage spec
- `docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md` — 24-stage rewrite
- `app/services/bootstrap_orchestrator.py:199-245` — current `_STAGE_REQUIRES` (to be removed)
- `app/services/bootstrap_orchestrator.py:919-945` — current `_build_runnable` (to be rewritten)
