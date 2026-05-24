# Stream A — Run #8 fixes (post-v3, post-committee, #1233)

> Status: PROPOSAL — v2.4 post-PR-D pre-flight committee + Codex re-pass. Authored 2026-05-24.
> v2.3 → v2.4 fixed (PR-D pre-flight, post-3-lens-r1 + post-3-lens-r2 + Codex re-pass):
> (1) **Runbook path rename**: 11 occurrences of `app/cli/runbooks/` → `app/runbooks/` (sql/173 comment also updated). Reason: `app/cli.py` already exists as the break-glass operator credential CLI (`python -m app.cli set-password`); creating a sibling `app/cli/` package would have shadowed it. Caught in PR-D 3-lens-round-2 RV1 BLOCKING; pre-empted before any code shipped.
> (2) **PG advisory-locks correction**: §17 `acquire_jobs_process_fence(...)` was described as cluster-wide so it could survive `DROP DATABASE` against a sibling DB. **EMPIRICAL CORRECTION**: PG advisory locks are PER-DATABASE in PG 9.0+, not cluster-wide. Caught by `tests/test_jobs_process_probe_fence.py::test_per_database_isolation_regression_gate` during PR-D commit-1 bench (both `postgres` and `ebull_dev` connections successfully acquired the same key). The runbook design now acknowledges the TOCTOU window during `DROP DATABASE` and documents operator-policy: jobs service MUST be stopped (e.g. systemd `stop`, not just SIGINT) through the destructive phase.
> (3) **Cancel sequence redesigned**: `stream_a_run_8_verify` step 4 changed from "POST /system/bootstrap/cancel, then poll-until-idle (75 min cap)" to FIRE-AND-FORGET. Reason: with jobs stopped (pre-flight gate), no orchestrator observes `cancel_requested_at`, so polling wastes up to 75 minutes. Any `running` bootstrap_runs row vanishes with the DB drop in step 5. Caught in PR-D Codex re-pass BLOCKING 1.
> (4) **HTTP session-cookie auth**: §17 explicitly threads through that the runbook uses a single `httpx.Client()` instance with cookie persistence. `/auth/setup` returns a `Set-Cookie` (via `_set_session_cookie`); subsequent `/system/*` calls (`/system/bootstrap/run`, `/system/bootstrap-status`) inherit. Loopback (127.0.0.1) + dev `settings.host` skip the setup_token requirement (per `app/services/operator_setup.py:172` Mode A). Caught in PR-D Codex re-pass BLOCKING 2.
> (5) **C5 phantom column FIXED**: `data_freshness_index.last_seen_at` was phantom; real column is `updated_at` (sql/120). C5 query updated. Caught in PR-D Codex 1 spec-review BLOCKING 1.
> (6) **C6 7-category expansion**: previous 5-category list (insiders, institutions, blockholders, def14a, funds) was incomplete — `_CATEGORIES` at `app/jobs/ownership_observations_repair.py:69` enumerates 7 (adds treasury + esop). C6 expanded to all 7 with `CATEGORY_TO_MANIFEST_SOURCES` mapping. Treasury maps to `{sec_xbrl_facts}` (NOT `{sec_def14a}`) per `fundamentals/__init__.py:1622` (xbrl_dei source). Caught in PR-D Codex 1 IMPORTANT 11 + 12.
> (7) **`/auth/setup` credentials**: random 32-char password generated via `secrets.token_urlsafe(24)` + printed once with red banner. Username defaults to `operator`. No env-var requirement (per operator decision during PR-D plan v3 review).
> (8) **`probe_jobs_process_running` + `acquire_jobs_process_fence`** are NEW public helpers in `app/jobs/locks.py` (commit 1 of PR-D). All three runbooks use the probe; only `stream_a_run_8_verify` uses the fence.
> (9) **`refresh_cik_sidecar`** is promoted from private `_refresh_cik_sidecar` (commit 1 of PR-D). NEW `repair_cik_sidecar_from_archive` helper at the same location.
> (10) **`stream_a_stream_c_gate.py` MUST import `app.services.manifest_parsers`** at module load — otherwise `registered_parser_sources()` returns empty frozenset + C4 false-passes. Caught in PR-D Codex 1 IMPORTANT 10.
> (11) **DROP DATABASE 55006 handling**: terminate + 2s sleep + DROP; on 55006, terminate + 5s sleep + DROP; on second 55006, emit `pg_stat_activity` rows in RECOVERY footer + exit 1.
> (12) **Migration runner**: in-process `from app.db.migrations import run_migrations; run_migrations()` (NOT alphabetical sql/ walk). Caught in PR-D Codex re-pass IMPORTANT 2.
> (13) **JSONL log convention**: `var/runbooks/<runbook>-<token>-<ts>.jsonl`. `.gitignore` adds `/var/`.
> (14) **Drift detection**: `response.current_run_id` (the real field name) compared against captured `run_id`. Mismatch = exit 3 CRITICAL (data-corruption risk); foreign run NOT cancelled.
> (15) **Timeout**: 90-min poll exit 2 (distinct from gate-fail=1, success=0); bootstrap continues; log curl cancel command + status URL.
> (16) **Wait-for-jobs-start**: runbook actively waits for `probe_jobs_process_running == True` after dispatch (operator started jobs) before the 90-min poll begins. 10-min cap.
> (17) **`pyproject.toml` perf marker** registered so `pytest -m "not perf"` excludes nightly-tier tests.
> Earlier v1 → v2.3 history archived to keep this header focused on the v2.4 changeset.
> v2 → v2.1 → v2.2 → v2.3 prior changes preserved in commits; rationale lives in `docs/review-prevention-log.md` + PR-A/B/C descriptions.
> Predecessor: `docs/_archive/2026-05/superseded-etl-rollout-v3.md`.

## §0 Grep proof

> Generated 2026-05-24 against branch `docs/reorganise-plans-specs` @ `2f8894a`. Outputs reproduced verbatim from the commands below; do NOT paraphrase. Companion enforcement: [.claude/skills/data-engineer/etl-spec-template-usage.md "Pre-write checklist"](../../../.claude/skills/data-engineer/etl-spec-template-usage.md).

### §0.1 Cap vocabulary (cited in §13)

```
$ grep -n "Capability = Literal\[" app/services/bootstrap_orchestrator.py
286:Capability = Literal[

$ sed -n '286,300p' app/services/bootstrap_orchestrator.py
Capability = Literal[
    "universe_seeded",
    "cik_mapping_ready",
    "cusip_mapping_ready",
    "bulk_archives_ready",
    "filing_events_seeded",
    "submissions_secondary_pages_walked",
    "insider_inputs_seeded",
    "form3_inputs_seeded",
    "institutional_inputs_seeded",
    "nport_inputs_seeded",
    "fundamentals_raw_seeded",
```

Plus (interleaved with rationale comments): `"class_id_mapping_ready"` (line 301), `"submissions_processed"` (line 313), `"insider_dataset_processed"` (line 337), `"institutional_dataset_processed"` (line 338). **No `nport_dataset_processed` cap exists** — v2.1 first-draft §0.1 invented it (Codex 1 BLOCKING `2026-05-24`). The dataset-processed ordering caps are 3, not 4.

`filings_history_seeded` and `companyfacts_processed` (cited in v1 §13) DO NOT appear in this Literal — they are hallucinations. The real caps that match v1's intent are `submissions_processed` (S8 ordering) + `bulk_archives_ready` (S7 prerequisite) + `cik_mapping_ready` (CIK→instrument map) + `fundamentals_raw_seeded` (S9 companyfacts ingest, ALREADY gating S25).

### §0.2 Current `fundamentals_sync` cap requirement (cited in §13)

```
$ grep -n '"fundamentals_sync": CapRequirement' app/services/bootstrap_orchestrator.py
594:    "fundamentals_sync": CapRequirement(all_of=("fundamentals_raw_seeded",)),
```

v2 strengthens this to a 4-cap requirement (see §13).

### §0.3 Stage number (cited in §1, §13, §18)

```
$ grep -n '_spec("fundamentals_sync\|_spec("ownership_observations_backfill' app/services/bootstrap_orchestrator.py
1140:    _spec("ownership_observations_backfill", 24, "db", "ownership_observations_backfill"),
1141:    _spec("fundamentals_sync", 25, "db", "fundamentals_sync"),
```

v1 cited S24 throughout; correct number is **S25**. S24 is `ownership_observations_backfill`.

### §0.4 `bootstrap_runs` PK column (cited in §4 sidecar FK)

```
$ grep -n "CREATE TABLE.*bootstrap_runs\|PRIMARY KEY" sql/129_bootstrap_state.sql
65:CREATE TABLE IF NOT EXISTS bootstrap_runs (
66:    id                       BIGSERIAL PRIMARY KEY,
```

PK column is `id`, NOT `run_id`. v1 FK `REFERENCES bootstrap_runs(run_id)` would fail at migration time.

### §0.5 `submissions.zip` `files[]` shape (cited in §4 sidecar PK + §5 fetch strategy)

```
$ sed -n '40,50p' app/providers/implementations/sec_submissions.py
        "filings": {
          "recent": { "accessionNumber": ["..."], ... },
          "files": [
            { "name": "CIK0000320193-submissions-001.json",
              "filingFrom": "...", "filingTo": "..." }
          ]
        }
```

`files[]` entries are **page descriptors** `{name, filingFrom, filingTo}`, NOT accessions. v1 PK `(cik, accession_number)` was structurally wrong. Correct PK = `(cik, page_name)`. Reframe of the win (see §5): saves the ~5,105 redundant **primary** `submissions/CIK*.json` re-fetches that `sec_submissions_files_walk.py:109` currently issues; **secondary pages still fetched** (they are NOT in the bulk archive — confirmed at `sec_submissions_files_walk.py:1-7` docstring).

### §0.6 `financial_periods` writers (cited in §8)

```
$ grep -rn "INSERT INTO financial_periods\b\|MERGE INTO financial_periods\b" --include="*.py" --include="*.sql"
app/services/fundamentals.py:1451:        INSERT INTO financial_periods (
tests/test_*  (10 test-only writers)
```

**ONE production writer** at `app/services/fundamentals.py:1451`. v1 §8 claimed "≥ 2 writers" — wrong. T1.2's `fundamentals_sync_bootstrap` will call the SAME helper, so the sink stays single-writer (zero conflict-key drift).

### §0.7 Share-class fan-out helper (cited in §2 + §13 T1.2)

```
$ grep -rn "siblings_for_issuer_cik" --include="*.py" | head -3
app/services/sec_identity.py:26:def siblings_for_issuer_cik(conn: psycopg.Connection[Any], cik: str) -> list[int]:
app/services/def14a_ingest.py:758:        siblings = siblings_for_issuer_cik(conn, issuer_cik)
app/services/insider_form3_ingest.py:330:            siblings = siblings_for_issuer_cik(conn, issuer_cik)
```

Existing helper. T1.2 MUST call it to fan out from CIK (`financial_facts_raw` key) to all sibling `instrument_id`s (`financial_periods` key) — otherwise GOOGL/GOOG, BRK.A/BRK.B silently lose one share-class's row per CIK.

### §0.8 Agent-CIK filter primitive (cited in §14)

```
$ grep -rn "KNOWN_FILING_AGENT_CIKS" --include="*.py" | head -4
app/providers/implementations/sec_edgar.py:98:KNOWN_FILING_AGENT_CIKS: frozenset[str] = frozenset(
app/services/manifest_parsers/sec_n_port.py:131:    if padded_filer_cik in KNOWN_FILING_AGENT_CIKS:
app/services/manifest_parsers/sec_13f_hr.py:156:    if padded_filer_cik in KNOWN_FILING_AGENT_CIKS:
app/services/manifest_parsers/sec_13dg.py:135:    if padded_filer_cik in KNOWN_FILING_AGENT_CIKS:
```

T1.3 sidecar populate path MUST filter via this frozenset — otherwise S14 will construct URLs against agent CIKs which 404 every time.

### §0.9 Boot-guard pattern primitive (cited in §1 T1.8 + §13)

```
$ grep -n "_ensure_.*_with_cleanup" app/jobs/__main__.py | head -8
446:def _ensure_runtime_config_singleton_with_cleanup(
477:def _ensure_kill_switch_singleton_with_cleanup(
504:def _ensure_bootstrap_state_singleton_with_cleanup(
529:def _ensure_budget_config_singleton_with_cleanup(
553:def _ensure_transaction_cost_config_singleton_with_cleanup(
```

T1.8 boot guard uses the same FILE LAYOUT but a different SEMANTIC: the existing 5 helpers re-INSERT default singletons; the operator-existence check hard-fails (operator absence is unrecoverable without `/auth/setup`). v2 names the new helper `_check_operator_exists_with_cleanup` (per Architect IMPORTANT — `_check_*` signals "verify; fail if missing"; `_ensure_*` signals "create if missing"). Slots into the existing chain at `__main__.py:660`.

### §0.10 fundamentals/ package conversion (cited in §1.7)

```
$ grep -rn "from app.services.fundamentals\b\|import app.services.fundamentals\b" --include="*.py" | wc -l
45
```

**45 import sites** must continue to resolve after T1.2 converts `app/services/fundamentals.py` (flat 2954-line module) into `app/services/fundamentals/` package. Compat plan: keep `app/services/fundamentals/__init__.py` re-exporting every name the 45 callers import. Zero behavior change at call-sites; physical separation only.

### §0.11 Manifest worker registered-sources surface (cited in §1.8 Stream-C gate)

```
$ grep -n "registered_parser_sources" app/jobs/sec_manifest_worker.py
236:    sources = sorted(registered_parser_sources())
447:def registered_parser_sources() -> frozenset[ManifestSource]:
```

Stream-C gate (§1.8) checks per-source manifest drain via this primitive.

### §0.12 Index test infra paths (cited in §20)

```
$ ls tests/test_bootstrap_orchestrator*
tests/test_bootstrap_orchestrator.py        # exists
$ ls tests/integration/test_bootstrap_orchestrator* 2>/dev/null
(empty)                                     # does not exist
$ grep -n "respx\|pytest-httpx" pyproject.toml uv.lock 2>/dev/null
(empty)                                     # HTTP-mock dep not declared
$ ls app/cli/ scripts/runbooks/ 2>/dev/null
(empty)                                     # NEITHER directory exists
```

v2 declares `app/runbooks/` as NEW directory (matches `etl-spec-template-usage.md §17` convention); adds `respx` to pyproject.toml; extends real test file `tests/test_bootstrap_orchestrator.py` (NOT inventing `..._catalogue_invariants.py`).

### §0.13 Ordering-only cap terminal-status handling (cited in §13)

```
$ grep -n "_ORDERING_ONLY_CAPS" app/services/bootstrap_orchestrator.py
497:# These caps are SATISFIED on ANY terminal status of their provider
513:_ORDERING_ONLY_CAPS: Final[frozenset[Capability]] = frozenset(
703:                if cap in _ORDERING_ONLY_CAPS:
764:        if status in ("blocked", "error", "cancelled") and cap in _ORDERING_ONLY_CAPS:

$ sed -n '513,521p' app/services/bootstrap_orchestrator.py
_ORDERING_ONLY_CAPS: Final[frozenset[Capability]] = frozenset(
    {
        "submissions_processed",
        "insider_dataset_processed",
        "institutional_dataset_processed",
    }
)
```

`submissions_processed` is in `_ORDERING_ONLY_CAPS` (line 515) and the dispatcher at line 764 marks it satisfied on `blocked|error|cancelled` terminals as well as `success|skipped`. v2's addition of this cap to `fundamentals_sync`'s `all_of` therefore does NOT create a stuck-S25 failure mode when S8 errors — the cap is satisfied on any S8 terminalisation. Architect-lens BLOCKING from v2 first-pass REBUTTED via this grep.

### §0.14 `bootstrap_stages.lane` CHECK constraint (cited in §5)

```
$ grep -n "lane.*CHECK\|lane.*IN" sql/147_bootstrap_stages_lane_family_split.sql
44:    ADD CONSTRAINT bootstrap_stages_lane_check
45:    CHECK (lane IN (
46:        'init', 'etoro', 'sec', 'sec_rate', 'sec_bulk_download', 'db',
47:        'db_filings', 'db_fundamentals_raw', 'db_ownership_inst',
48:        'db_ownership_insider', 'db_ownership_funds'
49:    ));
```

`db_fundamentals_raw` lane is ALREADY in the CHECK (sql/147 family-split migration). v2 §5's T1.2 lane reassignment from `"db"` → `"db_fundamentals_raw"` does not need a new migration — only the `_spec(...)` call update. Reviewer-lens BLOCKING from v2 first-pass REBUTTED via this grep.

### §0.15 `bootstrap_runs.coverage_floor_met` column existence (cited in §18)

```
$ grep -n "coverage_floor_met" sql/167_bootstrap_runs_coverage_floor_met.sql
8:--   * ``coverage_floor_met`` — set by the S13
38:    ADD COLUMN IF NOT EXISTS coverage_floor_met BOOLEAN DEFAULT NULL;
```

Column exists. Reviewer-lens BLOCKING from v2 first-pass REBUTTED. v2 ADDS a sibling `coverage_floor_ratio NUMERIC(5,4)` column (see §16) so retroactive threshold-change analysis is possible — but the boolean was not phantom.

### §0.16 `bootstrap_runs.stream_c_gate_status` column — CONFIRMED PHANTOM (cited in §16)

```
$ grep -n "stream_c_gate_status" sql/*.sql app/services/bootstrap_state.py app/services/bootstrap_orchestrator.py 2>/dev/null
(empty)
```

Column does NOT exist. v2 §16 adds the migration. Reviewer-lens + Codex-lens BLOCKING from v2 first-pass CONFIRMED.

### §0.17 Sidecar single-writer evidence (cited in §8) + sentinel-row pattern (cited in §14)

Pre-PR-B grep (table doesn't yet exist):

```
$ grep -rn "INSERT INTO sec_cik_submissions_files_index" --include="*.py" --include="*.sql"
(empty)
```

Single-writer guarantee will be enforced post-PR-B by `tests/test_sec_cik_submissions_files_index_single_writer.py` — AST-walks the codebase for the INSERT site and asserts exactly one. **Sentinel-row pattern** (per Codex BLOCKING): a CIK with zero overflow pages writes one sentinel row `page_name='__no_overflow_pages__'` to distinguish "CIK processed, no overflow" from "CIK not yet populated". See §4 schema + §14 lifecycle.

## §1. Decisions

1. **T1.2 — Add `fundamentals_sync_bootstrap` derivation-only entrypoint.** New module `app/services/fundamentals/bootstrap.py` after `fundamentals.py` is converted to a `fundamentals/` package (§1.7). Stage **S25** in `_BOOTSTRAP_STAGE_SPECS` switches to invoke the new entrypoint. The steady-state `fundamentals_sync` (post-bootstrap path with HTTP fallback) is untouched. T1.2 calls `siblings_for_issuer_cik` to fan out from CIK (raw-facts key) to all sibling `instrument_id`s (canonical period key) — see §0.7.
2. **T1.3 — Cache S8's `submissions.zip` `files[]` page-descriptor enumeration to a sidecar table.** New table `sec_cik_submissions_files_index` PK `(cik, page_name)` populated by S8 per-CIK ingest path; consumed by S14 (`sec_submissions_files_walk`) instead of re-fetching the **primary** `submissions/CIK*.json` per CIK. **Saves ~5,105 primary fetches per bootstrap (~12 min wall-clock at SEC's 7 req/s).** Secondary `submissions/CIK*-submissions-NNN.json` pages are still fetched over HTTP (they are NOT in `submissions.zip` — see §0.5). The sidecar is page-descriptor ONLY; body-cache is explicitly out of scope (see §21 rationale).
3. **T1.8 — Replace v3's hallucinated `master_key.is_bootstrapped()` boot guard with operator-existence check at jobs-process startup.** New helper `_check_operator_exists_with_cleanup(fence_conn)` (NAMED `_check_*` NOT `_ensure_*` — semantic differs from the 5 existing `_ensure_*_with_cleanup` helpers at `app/jobs/__main__.py:446-553` which re-INSERT default singletons; operator absence is unrecoverable without `/auth/setup`, so this helper hard-fails rather than re-seeding — per Architect IMPORTANT). Hard-fail body: `raise SystemExit(2)` after persisting `bootstrap_state.last_jobs_boot_error` + calling `fence_conn.close()` + `pool.close()`. Recovery: `EBULL_JOBS_SKIP_OPERATOR_CHECK=1` environment variable (NOT a CLI flag — env-var is harder to set accidentally; per Operator-lens finding §17). Boot-failure breadcrumb persisted to `bootstrap_state.last_jobs_boot_error TEXT` for `/system/status` surfacing.
4. **T1.1 — Validate the already-shipped #1222 13F cohort bound on `last_13f_hr_at`** delivers the projected ~22% drop (11.2k → 8.7k CIKs) on Run #8 wall-clock. NO new code. Add a Run #8 verification step.
5. **Acceptance criterion includes Stream C correctness gate** — strengthened per Codex CTO + DE + Operator lens convergence (see §1.8).
6. **Operator runbook for Run #8** is executable Python under `app/runbooks/` (matches `etl-spec-template-usage.md §17`; NEW directory; NOT `scripts/runbooks/`). Default `--dry-run`; explicit `--apply` to drop dev DB and re-bootstrap. **HARD GUARD against PROD:** runbook refuses to run when `EBULL_ENV != "dev"` (per Operator-lens finding §17).
7. **T1.4 decomposition of `fundamentals.py` internals is EXPLICITLY DEFERRED to Stream B.** §1.7's package conversion ships only the directory + `__init__.py` re-exports + the new `bootstrap.py` + `_common.py` — the existing 2954-line `fundamentals.py` body is moved verbatim into `fundamentals/scheduler.py` (or kept at `fundamentals/__init__.py` as a single-file re-export — chosen at impl time per §1.7).
8. **Realistic timeline: 8-12 working days**, not "~1 week" — per PM-lens finding. See §1.6 for PR sequence + cumulative WIP.

### §1.6 PR sequence (load-bearing — implementation may NOT reorder without spec update)

| PR | Scope | Depends on | Wall-clock estimate | Reviewer cost |
|---|---|---|---|---|
| **PR-A** | T1.8 boot guard + `bootstrap_state.last_jobs_boot_error TEXT` migration + Operator-existence helper | independent | 1 working day | 1 review round |
| **PR-B** | T1.3 sidecar: schema migration (sidecar + sentinel-row pattern + tight CHECKs + populate_origin + bootstrap_runs.stream_c_gate_status + coverage_floor_ratio) + S8 populate hook (NEW read of `payload["filings"]["files"]` in the OUTER per-CIK block at `sec_submissions_ingest.py:147`, BEFORE the `for instrument_id, symbol in matched_instruments:` sibling loop — NOT inside `_ingest_one` per Codex 1 re-pass IMPORTANT) + S14 consume (sentinel-aware) + agent-CIK filter + per-CIK transaction-scoped DELETE+INSERT | PR-A merged — operator-existence boot guard prevents the jobs process from running S8 against an unprepared DB; PR-B's migration itself runs via the API process and is technically merge-independent, but PR-B MUST NOT be deployed to any environment that would auto-trigger a bootstrap before PR-A is in place (per Architect IMPORTANT) | 2-3 working days | 1-2 review rounds (schema migration warrants tighter Codex) |
| **PR-C** | T1.2 `fundamentals/` package conversion + `bootstrap.py` entrypoint + S25 cap-strengthen + `siblings_for_issuer_cik` fan-out | PR-B merged (cap gating depends on `submissions_processed` provided via S8 → relies on T1.3 sidecar not being mid-rebuild) | 3-4 working days | 1-2 review rounds (cap-vocabulary change + import-compat surface) |
| **PR-D** | T1.1 verify + Run-#8 runbook + sidecar-repair runbook + Stream-C gate runbook | PR-C merged (full Stream A landed) | 1 working day | 1 review round |

**Cumulative: 7-9 working days for PR work + 1-3 for review iteration = 8-12 working days total.** Honest band, not "fits in one sprint".

### §1.7 fundamentals/ package conversion plan — option (b) PINNED

**What changes physically:**
- `app/services/fundamentals.py` (2954 lines, flat module) → `app/services/fundamentals/` package directory.
- **`app/services/fundamentals/__init__.py` = the existing 2954-line body verbatim, moved without re-export shim** (option (b) — PINNED post-committee per Architect IMPORTANT). Reason: option (a) (move body to `scheduler.py` + re-export from `__init__.py`) would require an exhaustive re-export list that includes PRIVATE names tests already import (`_TAG_TO_COLUMN`, `_canonical_merge_instrument`, `_current_quarter_start`, `_upsert_filing_from_master_index` per Architect grep). Missing one breaks tests at PR-C land. Option (b) preserves all 45 imports (public + private) without any re-export surface.
- `app/services/fundamentals/bootstrap.py` — NEW. Implements `fundamentals_sync_bootstrap(conn)` derivation entrypoint. Imports shared helpers via `from app.services.fundamentals._common import ...`.
- `app/services/fundamentals/_common.py` — NEW. Pure transforms shared by bootstrap + steady-state (NO HTTP, NO DB writes — only data-shape helpers like `_audit_to_skip_reason`).

**Why this is Stream A scope, not Stream B punt:** §13's `fundamentals/bootstrap.py + _common.py` pattern (per `bootstrap-mode-discipline` Pattern 1) REQUIRES the package directory to exist. Without the conversion, the new files would have to live OUTSIDE `fundamentals` (e.g. `app/services/fundamentals_bootstrap.py`), defeating the physical-separation guarantee. Architect + Reviewer lenses converged: "the package conversion IS the decomposition" — there is no honest way to defer it.

**T1.4 (intra-package refactor of `scheduler.py` into multiple modules) STAYS DEFERRED** to Stream B — that's the cosmetic split with no Run-#8 win.

**Import-compat invariant pinned by test:** `tests/test_fundamentals_package_compat.py::test_all_legacy_imports_resolve` — table-driven over the 45 import sites (snapshot at PR-C land time, regenerated when callers move).

### §1.8 Stream-C correctness gate — strengthened

v1's gate was "Layer 1/2/3 fire within 24h with `status='success'` and no `bootstrap_not_complete` skips". Codex + DE + API + Operator + PM lenses all flagged this as insufficient (Run #7 had `rows_processed=NULL` on bulk ingesters succeeding silently).

v2 gate (per `app/runbooks/stream_a_stream_c_gate.py`):

| Check | Source | Passes when |
|---|---|---|
| **C1.** Layer 1 (Atom fast lane) fired post-Run-#8 | `job_runs WHERE job_name='sec_atom_fast_lane' AND status='success' AND started_at > bootstrap_runs.completed_at` | ≥ 1 row |
| **C2.** Layer 2 (Daily index reconcile) fired post-Run-#8 | `job_runs WHERE job_name='sec_daily_index_reconcile' AND status='success' AND started_at > bootstrap_runs.completed_at` | ≥ 1 row |
| **C3.** Layer 3 (per-CIK poll) fired post-Run-#8 | `job_runs WHERE job_name='sec_per_cik_poll' AND status='success' AND started_at > bootstrap_runs.completed_at` | ≥ 1 row |
| **C4.** Manifest worker drained ≥ 1 row per registered source | `sec_filing_manifest WHERE updated_at > bootstrap_runs.completed_at AND ingest_status IN ('parsed', 'tombstoned') GROUP BY source` | ≥ 1 row per source from `registered_parser_sources()` (§0.11) |
| **C5.** At least one `data_freshness_index` row transitioned `current=TRUE` post-Run-#8 | `data_freshness_index WHERE updated_at > bootstrap_runs.completed_at AND state='current'` (v2.4 fold: column was `last_seen_at` in v2.3 → phantom; real column is `updated_at` per sql/120) | ≥ 1 row |
| **C6.** Per write-through category (7 cats per `_CATEGORIES` at `app/jobs/ownership_observations_repair.py:69`): ≥ 1 NEW observation OR category quiescent | First check: `ownership_<cat>_observations WHERE ingested_at > bootstrap_runs.completed_at` for cat ∈ {insiders, institutions, blockholders, treasury, def14a, funds, esop}. If zero, second check: `sec_filing_manifest WHERE source IN <CATEGORY_TO_MANIFEST_SOURCES[cat]> AND filed_at > bootstrap_runs.completed_at - INTERVAL '24h'`. Mapping is at `app/services/capability_manifest_mapping.py` (v2.4 fold). Treasury → `{sec_xbrl_facts}` (NOT def14a — xbrl_dei source per `fundamentals/__init__.py:1622`). | EITHER ≥ 1 new obs row, OR zero new filings of the matching source (emits `warning_category_quiescent_<cat>` instead of failing). Per Codex MEDIUM + DE IMPORTANT — DEF 14A / treasury can be quiescent across a 24h window without indicating breakage. |
| **C7.** Sidecar populated for every in-universe CIK (sentinel-aware) | `sec_cik_submissions_files_index WHERE bootstrap_run_id = <latest>` GROUP BY cik | DISTINCT-CIK count ≥ `(in-universe CIK count) - COUNT(in-universe CIK ∩ KNOWN_FILING_AGENT_CIKS)`. Per Codex 1 BLOCKING: subtracting GLOBAL `KNOWN_FILING_AGENT_CIKS count` would false-pass the gate whenever most known agents are out-of-universe. The correct subtrahend is the intersection only. A CIK with only the sentinel row `__no_overflow_pages__` COUNTS toward the populated set (per Codex v2 first-pass BLOCKING — AAPL has 0 overflow pages and must not false-fail C7). |

Gate is RUN as a post-bootstrap acceptance step. Output: structured JSON with per-check pass/fail + count + a single boolean `stream_a_run_8_accepted`. Stream A merge is **gated** on this script existing + passing on a real Run #8 (recorded in PR-D's description per CLAUDE.md ETL clauses 8-12).

## §2. Identifiers + identity-drift

| Identifier | Flows where |
|---|---|
| `instrument_id BIGINT` | PK for `financial_facts_raw`, `financial_periods`, `instrument_valuation` |
| `cik` TEXT (10-digit zero-padded) | Subject identifier for T1.3 sidecar table; S14 walks the cached list keyed by CIK; T1.2 fans out via `siblings_for_issuer_cik(conn, cik) -> list[instrument_id]` |
| `page_name` TEXT | Per-CIK secondary-page filename (e.g. `CIK0000320193-submissions-001.json`); sidecar PK alongside `cik` |

**Identity-drift handling:**

- **CIK reassignment:** SEC explicitly does not recycle CIKs. T1.3 sidecar is keyed on `cik`; reassignment cannot occur. If SEC violates this invariant, sidecar rows for the affected CIK become stale — recovered by the per-CIK DELETE invalidation pattern (§14): every S8 ingest of a CIK FIRST deletes that CIK's sidecar rows, then re-inserts.
- **CUSIP retirement:** Out of scope. T1.2 derives fundamentals from `financial_facts_raw` (CIK-keyed); T1.3 walks per-CIK. No CUSIP semantics.
- **Symbol reuse:** Not relevant — T1.2 operates on `instrument_id` after `siblings_for_issuer_cik` resolution, which is symbol-agnostic.
- **Share-class fan-out (T1.2 BLOCKING):** `financial_facts_raw` is CIK-keyed (one row per CIK per fact); `financial_periods` is `instrument_id`-keyed (one row per share-class per period). Without `siblings_for_issuer_cik`, T1.2 would write 1 `financial_periods` row per CIK instead of N rows per share class — silent data loss for GOOGL/GOOG, BRK.A/BRK.B, etc. Same bug class as #1102. Fan-out point: T1.2's per-CIK loop calls `for instrument_id in siblings_for_issuer_cik(conn, cik):` before writing.

## §3. Endpoint surface

T1.2: NO network endpoints (derivation from existing `financial_facts_raw`).

T1.3 sidecar populate: NO new network endpoints — sidecar rows are derived from data S8 already reads from `submissions.zip`. T1.3 ELIMINATES per-CIK primary refetches that S14 currently issues.

T1.3 S14 consume: S14 STILL fetches secondary pages (`data.sec.gov/submissions/CIK*-submissions-NNN.json`) over HTTP — they are NOT in the bulk archive. Endpoint and rate-limit semantics for secondary-page fetches are UNCHANGED from current behaviour.

T1.8: NO network endpoints (DB-local check).

T1.1: NO new network endpoints (cohort bound applied at SQL filter level pre-fetch).

| Endpoint touched | URL | Method | Body schema | Fixture |
|---|---|---|---|---|
| (T1.3) S14 secondary-page fetch — UNCHANGED | `data.sec.gov/submissions/CIK{padded}-submissions-{NNN}.json` | GET | per `sec-edgar.md §1` | `tests/fixtures/sec/submissions/CIK0000102909-submissions-001.json` (Vanguard — multi-page; populate if missing in PR-B) |

Stream A introduces NO new endpoint surface. Net: T1.3 reduces aggregate Run #8 HTTP budget by ~5,105 primary calls; T1.2 reduces by the entire `fundamentals_sync` HTTP component (~85 min worth at Run #7 rates).

## §4. Schema

### T1.3 — new table (revised post-v2 committee — sentinel-row pattern + tightened CHECKs + audit-lineage column)

```sql
-- sql/NNN_sec_cik_submissions_files_index.sql
CREATE TABLE IF NOT EXISTS sec_cik_submissions_files_index (
    cik              TEXT       NOT NULL,
    page_name        TEXT       NOT NULL,  -- e.g. 'CIK0000320193-submissions-001.json' OR sentinel '__no_overflow_pages__'
    filing_from      DATE,                  -- NULL for sentinel rows
    filing_to        DATE,                  -- NULL for sentinel rows
    discovered_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    bootstrap_run_id BIGINT     REFERENCES bootstrap_runs(id) ON DELETE SET NULL,
    populate_origin  TEXT       NOT NULL DEFAULT 'bootstrap'
                                CHECK (populate_origin IN ('bootstrap', 'steady_state')),
    PRIMARY KEY (cik, page_name),
    CHECK (cik ~ '^[0-9]{10}$'),
    CHECK (
        page_name = '__no_overflow_pages__'
        OR page_name ~ '^CIK[0-9]{10}-submissions-[0-9]{3}\.json$'
    ),
    CHECK (
        -- Real-page rows MUST have a date range; sentinel rows MUST NOT.
        (page_name = '__no_overflow_pages__' AND filing_from IS NULL AND filing_to IS NULL)
        OR (page_name <> '__no_overflow_pages__' AND filing_from IS NOT NULL AND filing_to IS NOT NULL AND filing_from <= filing_to)
    )
);
```

**Sentinel-row pattern (per Codex BLOCKING from v2 first-pass):** a CIK with zero overflow pages (e.g. AAPL — `recent` array fits inside the 1000-cap) writes ONE row with `page_name='__no_overflow_pages__'` instead of zero rows. This lets `S14` and the Stream-C `C7` gate distinguish:

| Sidecar state for CIK X | Meaning | S14 / C7 action |
|---|---|---|
| 1+ real-page rows | X has overflow pages; S14 walks them | walk pages |
| Exactly 1 sentinel row | X processed; no overflow pages | skip secondary walk (C7 passes) |
| Zero rows | X not yet populated | S14 fail-closed; C7 fails |

The sentinel makes the populated-set explicit. Without it, AAPL (0 overflow) and a never-populated CIK look identical and either C7 false-fails everyone or S14 silently does nothing for valid CIKs. Both were live in v2 first-pass.

**Index Budget:** 1 index (PK only). No grandfathering needed. v1 had a standalone `cik` index — dropped per DE-lens finding 4: PG B-tree handles cik-only equality + range via PK prefix scan.

**Encoding / precision / NULL / timezone:**
- `cik` TEXT UTF-8, NOT NULL, 10-digit zero-padded, **CHECK-enforced** via regex `^[0-9]{10}$` (tightened per DE BLOCKING — was convention-only in v2 first-pass).
- `page_name` TEXT UTF-8, NOT NULL, **CHECK-enforced** via the disjunction above. v2 first-pass `LIKE 'CIK%-submissions-%.json'` would have admitted `CIK-submissions-.json` and similar garbage.
- `filing_from / filing_to` DATE (no TZ). NULL for sentinel rows; NOT NULL for real-page rows (composite CHECK).
- `discovered_at` TIMESTAMPTZ in UTC via `clock_timestamp()` (NOT `NOW()` / `transaction_timestamp()` — see [SKILL.md §6.5.8](../../../.claude/skills/data-engineer/SKILL.md)).
- `bootstrap_run_id` BIGINT NULLABLE — null if S8 populates the sidecar outside a tracked bootstrap run. FK → `bootstrap_runs(id)` (verified §0.4). `ON DELETE SET NULL` preserves audit lineage.
- `populate_origin` TEXT NOT NULL DEFAULT `'bootstrap'`, CHECK in `{'bootstrap', 'steady_state'}`. Per DE IMPORTANT: distinguishes "NULL bootstrap_run_id because steady-state refresh" (origin=`steady_state`) from "NULL bootstrap_run_id because buggy code path forgot to thread it" (origin=`bootstrap` + NULL run id is a violation).

**Why sidecar table, not column on `instrument_sec_profile`:** the data is per-(cik, page) not per-instrument.

**Why `page_name`, not `accession_number`:** `files[]` entries are page descriptors (§0.5). v1's PK `(cik, accession_number)` was structurally wrong.

### T1.8 + Stream-C — new columns on existing tables

```sql
-- sql/NNN_bootstrap_state_last_jobs_boot_error.sql (T1.8)
ALTER TABLE bootstrap_state
    ADD COLUMN IF NOT EXISTS last_jobs_boot_error TEXT;
COMMENT ON COLUMN bootstrap_state.last_jobs_boot_error IS
    'Operator-actionable string set by jobs-process boot guard on hard-fail; NULL when last boot succeeded';

-- sql/NNN_bootstrap_runs_stream_c_gate.sql (T1.8 — Stream-C gate)
ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS stream_c_gate_status TEXT
        CHECK (
            stream_c_gate_status IS NULL
            OR stream_c_gate_status IN ('pending', 'passed')
            OR stream_c_gate_status LIKE 'failed\_%' ESCAPE '\'  -- literal underscore, not LIKE wildcard (per Codex 1 BLOCKING)
        );
ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS coverage_floor_ratio NUMERIC(5,4);
COMMENT ON COLUMN bootstrap_runs.coverage_floor_ratio IS
    'Measured CUSIP coverage ratio post-S13; preserved alongside coverage_floor_met BOOLEAN for retroactive threshold-change analysis';
```

**Per Reviewer + Codex BLOCKING:** `stream_c_gate_status` was phantom in v2 first-pass — §17 wrote to it without a migration. Now declared explicitly. `coverage_floor_met BOOLEAN` ALREADY EXISTS (sql/167 — verified §0.15); v2 ADDS `coverage_floor_ratio NUMERIC(5,4)` so the threshold can be re-evaluated retroactively without re-running bootstrap (per DE IMPORTANT).

### T1.2, T1.8, T1.1 — schema changes

T1.2: NO schema changes. Reads `financial_facts_raw`; writes `financial_periods` + `instrument_valuation` via the existing `fundamentals.py:1451` UPSERT helper (single-writer sink — §0.6).

T1.8: ONE schema change — `bootstrap_state.last_jobs_boot_error TEXT NULL` column (default NULL; cleared when boot succeeds). Operator-lens finding: without this, boot failures are invisible until the operator notices missing jobs. Migration: `ALTER TABLE bootstrap_state ADD COLUMN IF NOT EXISTS last_jobs_boot_error TEXT`.

T1.1: already shipped via #1010 + #1222 (existing `institutional_filers.last_13f_hr_at` column).

## §5. Fetch strategy + rate-limit composition

| Component | fetch_strategy | Lane | Budget composition |
|---|---|---|---|
| T1.2 `fundamentals_sync_bootstrap` | `derive` (NO HTTP) | `db_fundamentals_raw` (PR-C reassigns S25 from current `"db"` lane — see §0.14 lane CHECK already accepts `db_fundamentals_raw`; only the `_spec(...)` arg changes) | N/A |
| T1.3 S8 sidecar populate | `derive` (NO HTTP) | inside S8's existing `bulk_archive` window | N/A — derived from `submissions.zip` contents already in memory |
| T1.3 S14 consume | `per_resource_http` for SECONDARY pages only (primary refetch eliminated by sidecar) | `sec_rate` | UNCHANGED — same per-secondary-page fetch budget as today's S14. **Scale note (per Reviewer IMPORTANT):** the ~150-300 secondary-page estimate in `sec_submissions_files_walk.py:15-17` is for a 1.5k-universe baseline; proportional scaling to the 8.7k post-#1222 cohort yields ~870-1740 pages, but deepest-history filer density is sub-linear so the real figure is bounded by `COUNT(*) FROM sec_cik_submissions_files_index WHERE page_name <> '__no_overflow_pages__' GROUP BY cik` post-PR-B. PR-D runbook captures this measurement + writes back to the spec as a measured baseline. |
| T1.8 boot guard | `derive` (DB-local) | `init` | N/A |
| T1.1 (already shipped) | `per_resource_http` (S16 carve-out per `bootstrap-mode-discipline`) | `sec_rate` | Bounded cohort ~8.7k CIKs at 7 req/s SEC budget |

**Composition:** zero new HTTP. Stream A is a HTTP-budget RECOVERY: ~5,105 primary refetches saved (T1.3) + ~85 min of `fundamentals_sync` HTTP saved (T1.2). Downstream carve-out stages (S6 / S16 / S27 / S13) inherit the headroom.

**Honest claim:** T1.3 makes S14 do LESS work, not zero work. v1's "S14 becomes a pure SQL walk" was wrong — secondary pages are NOT in the bulk archive (§0.5).

## §6. Conditional-GET semantics

T1.3 implication for S8: S8's existing `submissions.zip` reuse contract (HEAD+ETag client-side compare, `.zip.etag` + `.zip.sha256` sidecars; see `sec-edgar.md §4 Bulk-archive reuse contract`) is UNCHANGED. The T1.3 sidecar TABLE is rebuilt per-CIK on every S8 ingest of that CIK regardless of whether the zip was re-downloaded — per-CIK DELETE + INSERT pattern (§14).

T1.3 S14 consume: secondary-page fetches use the existing S14 conditional-GET behaviour (no change from today).

T1.2 + T1.8: N/A (no HTTP).

## §7. Retry posture per error-class

T1.2: pure DB; deterministic SQL errors (`IntegrityError` / `DataError`) tombstone the stage with `status='error'`. Transient `OperationalError` (`SerializationFailure` / `DeadlockDetected`) → return `failed` with 1h backoff. **CardinalityViolation trap (Operator-lens finding):** Run #7 logged 43 `cardinality_violation` retries on bulk ingesters. T1.2 mitigates via the `bulk_ingest_copy_pattern.sh` DISTINCT ON discipline (`data-engineer/SKILL.md §2.10b`); the bootstrap entrypoint reuses the existing UPSERT helper which already dedupes.

T1.3 sidecar populate: piggybacks on S8's existing retry posture per-CIK. If S8 succeeds for a CIK, that CIK's sidecar rows are consistent; if S8 fails for a CIK, sidecar is rolled back inside S8's per-CIK `conn.transaction()` block (per-CIK TOP-LEVEL transaction in psycopg3 since no outer tx is open — `_run_with_conn` at `sec_bulk_orchestrator_jobs.py:83-87` opens the connection without an enclosing tx; see `sec_submissions_ingest.py:147-168`).

T1.3 S14 read path: SQL-only for the primary check; if sidecar has rows for the CIK, S14 walks them. **If sidecar is empty for an in-universe CIK that is NOT in `KNOWN_FILING_AGENT_CIKS`**, S14 fails-closed with `status='error'` + reason `sidecar_empty_for_in_universe_cik` — does NOT silently fall back to per-CIK HTTP. Surfacing the prerequisite gap loudly is the explicit choice. CIKs in `KNOWN_FILING_AGENT_CIKS` are filtered at sidecar-populate time (§14) so an empty sidecar for them is expected.

T1.8 boot guard: 3 outcomes — `operators` row exists → boot succeeds; missing → hard-fail with operator-actionable message + `bootstrap_state.last_jobs_boot_error` populated; DB unreachable → hard-fail with `last_jobs_boot_error='db_unreachable_at_boot'`. Operator triages PG before retrying boot.

## §8. Multi-writer sink registry

**T1.3 — `sec_cik_submissions_files_index` is a NEW sink with ONE writer** (S8 `sec_submissions_ingest`). Single-writer sinks don't trigger the multi-writer contract. If a future Layer 3 per-CIK poll wanted to populate the same table, it would be the second writer — declare here NOW so the conflict-key contract is preflighted: any future writer would UPSERT on `(cik, page_name)` with `ON CONFLICT (cik, page_name) DO UPDATE SET filing_from = EXCLUDED.filing_from, filing_to = EXCLUDED.filing_to, discovered_at = EXCLUDED.discovered_at, bootstrap_run_id = EXCLUDED.bootstrap_run_id`. **However: Layer 3's `sec_per_cik_poll` deliberately bypasses the sidecar** (it's the freshness oracle — reads `data_freshness_index`, re-fetches `submissions.json` per CIK; see `app/jobs/sec_per_cik_poll.py:29`) per DE-lens finding 11. The sidecar is bootstrap-only by design; Stream B universe-expansion work MUST preserve this separation.

**T1.2 — `financial_periods` is a SINGLE-writer sink** (verified §0.6 — one prod writer at `fundamentals.py:1451`). T1.2's `fundamentals_sync_bootstrap` calls the SAME UPSERT helper via the existing function (no fork of the writer body). Sink-registry footprint unchanged.

**T1.8 + T1.1 — no sinks written.**

## §9. Watermark + retry-budget

T1.2: no per-CIK watermark (derives the full universe on each invocation). Idempotent: second invocation finds zero new facts in `financial_facts_raw` since last invocation. Retry: `bootstrap_stages.next_retry_at` via standard backoff.

T1.3 sidecar: no watermark; per-CIK DELETE + INSERT on every S8 ingest of that CIK. The `discovered_at` column is the audit watermark for "when was this row last refreshed". `bootstrap_run_id` is the audit lineage for "which run last rebuilt this CIK's sidecar entries".

T1.8: no retry — boot is one-shot. Operator must restart jobs after fixing the cause; `bootstrap_state.last_jobs_boot_error` persists the prior failure for `/system/status`.

T1.1: already shipped (#1222 380-day cutoff is the watermark).

## §10. Encoding / precision / NULL / timezone

T1.3 sidecar (per §4):
- `cik` TEXT UTF-8 NOT NULL 10-digit zero-padded.
- `page_name` TEXT UTF-8 NOT NULL, CHECK-pinned shape.
- `filing_from / filing_to` DATE (no TZ). **NOT NULL for real-page rows; NULL for sentinel rows** — composite CHECK in §4 enforces the disjunction. (Codex 1 IMPORTANT — v2.1 first-draft §10 wording contradicted §4 sentinel pattern.)
- `discovered_at` TIMESTAMPTZ UTC via `clock_timestamp()` (per [SKILL.md §6.5.8](../../../.claude/skills/data-engineer/SKILL.md)).
- `bootstrap_run_id` BIGINT NULLABLE.

T1.2: no new columns; writes via the existing `fundamentals.py:1451` UPSERT, which already enforces NUMERIC(30,10) on `value`, DATE on `period_end_date / period_start / period_end`.

T1.8: one new column on `bootstrap_state` — `last_jobs_boot_error TEXT NULLABLE`. Free-form error message; NULL when last boot succeeded.

## §11. Backfill horizon + retention

T1.3 sidecar: retention bounded by `bootstrap_runs.id` lifetime + 90-day grace. Reaper sweep moves to Stream B (Universe Expansion); steady-state sweep handled by per-CIK DELETE + INSERT (§14) — stale rows for CIKs removed from universe drift until reaper, but stale rows do NOT cause incorrect S14 behaviour (S14 enumerates secondary pages; a stale row would cause one redundant enumeration, not data corruption). Initial sidecar growth: ~8.7k CIKs (post-#1222 cohort) × ~3 pages each average = ~26k rows. Bounded.

T1.2: NO new backfill. Operates on existing `financial_facts_raw` history (10-K 3-year + 10-Q 8-quarter retention already enforced per `financial_facts_retention.py`).

T1.8 + T1.1: no retention concern.

## §12. Partition strategy + extension deadline

T1.3 sidecar: NOT partitioned. ~26k-row sidecar doesn't justify partition machinery. If universe expands past 50k CIKs (Stream B work), revisit — at 50k CIKs × 3 pages × 50 bytes ≈ 7.5 MB, partitioning is still unjustified.

N/A for T1.2, T1.8, T1.1 — no new partitioned tables.

## §13. Bootstrap vs steady-state mode

T1.2: **bootstrap-only entrypoint**. `fundamentals_sync_bootstrap` ONLY runs as **Stage 25 (S25)** of `_BOOTSTRAP_STAGE_SPECS` (verified §0.3). The steady-state `fundamentals_sync` is unchanged and unaffected. Physical separation pattern per [bootstrap-mode-discipline](../../../.claude/skills/engineering/bootstrap-mode-discipline.md) Pattern 1 — separate module file (`app/services/fundamentals/bootstrap.py` post-§1.7 conversion), shared `_common.py` for pure transforms (no HTTP, no DB writes).

T1.2 audit-during-bootstrap trap defence: `fundamentals_sync_bootstrap` calls `audit_all_instruments(conn)` ONLY AFTER ALL of the following caps are satisfied:

```python
# v2 cap-strengthening at app/services/bootstrap_orchestrator.py:594
"fundamentals_sync": CapRequirement(all_of=(
    "bulk_archives_ready",       # S7 — bulk submissions.zip + companyfacts.zip ready
    "cik_mapping_ready",         # S5/S6 — external_identifiers CIK rows populated
    "submissions_processed",     # S8 — submissions.zip per-CIK ingest terminated (success OR skip)
    "fundamentals_raw_seeded",   # S9 — companyfacts.zip ingested → financial_facts_raw populated
)),
```

**All four caps verified real (§0.1).** v1's `filings_history_seeded` + `companyfacts_processed` were hallucinated; replaced with `bulk_archives_ready` + `fundamentals_raw_seeded`. The semantic the v1 author intended ("audit sees the full picture before deciding what's missing") is preserved by the 4-cap requirement.

**Terminal-status safety (per §0.13 grep):** `submissions_processed` is in `_ORDERING_ONLY_CAPS` (line 515) and the dispatcher at line 764 marks ordering-only caps satisfied on `blocked|error|cancelled` terminals as well as `success|skipped`. Adding `submissions_processed` to S25's `all_of` therefore does NOT create a stuck-S25 failure mode when S8 errors — the cap is satisfied on any S8 terminalisation.

**Lane-criticality (per DE IMPORTANT):** S8 (order 8) and S25 (order 25) both currently run on `db` lane (`bootstrap_orchestrator.py:1027, 1141`). PR-C reassigns S25 to `db_fundamentals_raw` (separate lane); S8 stays on `db`. Cap addition does not cross-lane block — `db` lane stages 8/9/10/etc. drain before `db_fundamentals_raw` lane stage 25 even begins. DAG-acyclic: S8 (order 8) provides `submissions_processed` (line 313); S25 (order 25) requires it. No back-edge. Pinned by extension to `tests/test_bootstrap_orchestrator.py::test_cap_dag_acyclic_after_pr_c`.

Without this gate, the audit would misclassify mid-bootstrap and reintroduce HTTP backfill (Codex v3 finding #8). The gate is the load-bearing primitive — if `pyright` accepts the cap names, the safety invariant holds.

T1.3: BOTH bootstrap and steady-state benefit. S8 populates the sidecar per-CIK regardless of mode; S14 consumes it regardless of mode. Same code path; no separation needed.

T1.8: bootstrap-side concern ONLY. Steady-state assumes operator exists by the time scheduled jobs fire (otherwise prerequisite check rejects every job with `bootstrap_not_complete`).

T1.1: already-shipped #1222 carve-out — S16 is one of the 4 documented per-resource HTTP carve-outs in `bootstrap-mode-discipline` §"Carve-outs". The cohort bound is the budget cap that justifies the carve-out.

**Forbidden-HTTP-in-bootstrap declaration:** T1.2 expected HTTP count = 0. T1.3 S8 populate expected HTTP count = 0. T1.3 S14 expected HTTP count = bounded by secondary-page count (typically ~150-300 per Run-#8 per `sec_submissions_files_walk.py:15-17`). Dispatcher logs any HTTP issued by T1.2 as `forbidden_http_in_bootstrap` and FAILS the stage. T1.3 S14's secondary-page fetches are NOT forbidden — they're legitimate per-resource HTTP under the existing budget.

## §14. Tombstones + soft-delete

T1.3 sidecar: NO tombstones. Per-CIK lifecycle (revised post-committee — sentinel-row pattern + explicit S8 extension scope):

```python
# inside sec_submissions_ingest.py per-CIK transaction block at lines 147-168.
# The sidecar refresh runs ONCE PER CIK, BEFORE the per-instrument
# `for instrument_id, symbol in matched_instruments:` loop (Codex 1 IMPORTANT —
# putting it inside _ingest_one would re-DELETE+INSERT N times per share-class CIK).
# PR-B adds this block immediately after the json.load + parse-error guard, before
# the sibling-instrument loop:

with conn.transaction():  # per-CIK TOP-LEVEL transaction at sec_submissions_ingest.py:147 (no outer tx open)
    payload = json.load(fh)                              # existing
    # ... existing parse-error guard ...

    # NEW for PR-B — sidecar refresh ONCE PER CIK (not per instrument).
    if cik not in KNOWN_FILING_AGENT_CIKS:                # §0.8
        cur.execute(
            "DELETE FROM sec_cik_submissions_files_index WHERE cik = %s",
            (cik,),
        )
        files = (payload.get("filings") or {}).get("files") or []
        origin = "bootstrap" if bootstrap_run_id is not None else "steady_state"
        if files:
            cur.executemany(
                "INSERT INTO sec_cik_submissions_files_index "
                "(cik, page_name, filing_from, filing_to, bootstrap_run_id, populate_origin) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (cik, page["name"], page["filingFrom"], page["filingTo"], bootstrap_run_id, origin)
                    for page in files
                    if isinstance(page, dict) and page.get("name")
                ],
            )
        else:
            # Sentinel — CIK processed; zero overflow pages. Distinguishes from "not yet
            # populated" (zero rows). S14 + Stream-C C7 honour this explicitly.
            cur.execute(
                "INSERT INTO sec_cik_submissions_files_index "
                "(cik, page_name, bootstrap_run_id, populate_origin) "
                "VALUES (%s, '__no_overflow_pages__', %s, %s)",
                (cik, bootstrap_run_id, origin),
            )

    # Existing per-instrument loop (UNCHANGED — sidecar already refreshed):
    for instrument_id, symbol in matched_instruments:
        _ingest_one(conn, instrument_id=instrument_id, cik_padded=cik, ...)
```

**Per-CIK transaction atomicity (per DE BLOCKING — was ambiguous in v2 first-pass; precision corrected per Codex 1 re-pass IMPORTANT):** the `with conn.transaction()` block at `sec_submissions_ingest.py:147` is a per-CIK TOP-LEVEL transaction in psycopg3 — `_run_with_conn` at `sec_bulk_orchestrator_jobs.py:83-87` opens the connection without an enclosing tx, so each per-archive-entry `with conn.transaction()` starts and commits a fresh tx. If the INSERT raises mid-CIK, the entire tx rolls back, including the DELETE AND any sibling-instrument writes from the current CIK — prior committed sidecar rows for that CIK from earlier ingest cycles SURVIVE. (If a future caller wraps the archive loop in an outer `with conn.transaction()`, the per-CIK block becomes a SAVEPOINT — atomicity contract identical.) Pinned by `tests/integration/test_sidecar_per_cik_tx_atomicity.py`: injects an INSERT failure after the DELETE; asserts row count for that CIK unchanged.

**Per-CIK-not-per-instrument (Codex 1 IMPORTANT):** S8 iterates `for instrument_id, symbol in matched_instruments:` calling `_ingest_one` per matched instrument (`sec_submissions_ingest.py:156-165`). For share-class CIKs (one CIK → N sibling instruments) putting sidecar refresh INSIDE `_ingest_one` would redundantly DELETE+INSERT the same rows N times. v2.2 places it in the OUTER per-CIK loop, before the sibling loop, ensuring a single refresh per (CIK, archive-entry).

**Per-CIK DELETE + INSERT (NOT global TRUNCATE)** — S8 is per-CIK at the transaction level; a global TRUNCATE would leave the sidecar empty between CIK 1's ingest and CIK N's ingest, breaking S14 fail-closed semantics for all not-yet-processed CIKs in a long-running Run-#8.

**Stale-row drift bound:** CIKs that disappear from `submissions.zip` (rare — CIK retirement via Form 15) leave stale rows until reaper (Stream B). At Run #7's observed retirement rate (~5 per quarter), Run #N+1's S14 would enumerate ~5 extra page-walks per quarter elapsed — bounded enough to defer reaper. Stream A's PR-D runbook adds an OBSERVATION log line when an in-universe-CIK lookup returns 0 sidecar rows AND that CIK is missing from `submissions.zip` (operator-actionable signal without blocking).

**Agent-CIK filter (BLOCKING per API-lens):** S14's downstream URL construction against a known agent CIK 404s every time (`sec-edgar.md §3.7` enforcement point 2). Filtering at populate (above) is the safer choice than guarding at consume — keeps the sidecar honest as a "real-filer-only" index.

T1.2: no tombstones (operates on `financial_facts_raw` which never tombstones; that table is the raw landing zone).

T1.8: no tombstones.

T1.1: tombstones inherited from `institutional_filers` (#1222 cohort bound).

## §15. `rows_skipped` closed-set

T1.2 — closed set with PRECEDENCE order (per DE IMPORTANT — without precedence, multi-match CIKs produce unstable counters across runs):

Precedence (most specific → most general; FIRST match wins):

1. `cik_missing_from_financial_facts_raw` — CIK has no rows; expected for newly-listed instruments (skip + log; no error).
2. `no_siblings_for_issuer_cik` — `siblings_for_issuer_cik(conn, cik)` returned empty list; CIK known but no instrument mapping (data-quality flag).
3. `audit_classified_insufficient` — coverage audit returned `'insufficient'` for the instrument; derivation skipped to avoid noisy partial output.
4. `coverage_floor_unmet` — coverage_ratio < 0.80 (per [bootstrap-mode-discipline](../../../.claude/skills/engineering/bootstrap-mode-discipline.md) "Coverage-floor pattern"). NOT a hard skip — derivation continues; counter reflects the warning. Threshold rationale: §22 Q2.

Pinned by `tests/test_fundamentals_bootstrap.py::test_rows_skipped_precedence` — table-driven over `2^4 = 16` combinations of the 4 conditions.

v1 had an `other` catch-all — **removed per Reviewer + DE lens finding** (swallows defects). v2 enumeration is closed-set; any unmatched case raises. The raise is caught by the per-stage wrapper `_run_one_stage` at `bootstrap_orchestrator.py:1429` (`logger.exception("bootstrap stage %s raised; lane continues", stage_key)`) — stage tombstones with `status='error'` per the existing dispatcher path (line 1778-1780, per Reviewer OBSERVATION grep).

T1.3 sidecar populate — N/A (sidecar population skips no rows; either S8 succeeds for the CIK or S8's per-CIK transaction rolls back). **Exception:** `KNOWN_FILING_AGENT_CIKS` rows skipped at populate time, counted in S8's existing `agent_cik_skipped` counter.

T1.3 S14 — closed set:

- `sidecar_empty_for_in_universe_cik` — CIK in scope but sidecar has no rows for it. Should be impossible if S8 completed; if observed, indicates an S8 ordering bug or a CIK added to universe after S8 ran. Surfaces as hard fail.
- `accession_already_in_manifest` — row already inserted by a prior path.

`other` removed (same rationale).

T1.8 — N/A (boot is binary outcome).

T1.1 — already shipped; closed set documented in `sec_13f_quarterly_sweep.py`.

## §16. Schema-evolution migration path

PR-B migrations:

1. `sql/NNN_sec_cik_submissions_files_index.sql` — NEW table (DDL in §4). No dual-parser window needed.
2. `sql/NNN_bootstrap_runs_stream_c_gate.sql` — ALTER TABLE bootstrap_runs adds `stream_c_gate_status TEXT` (CHECK) + `coverage_floor_ratio NUMERIC(5,4)`. Phantom-column gap from v2 first-pass closed (per Reviewer + Codex BLOCKING).
3. `bootstrap_stages.lane` CHECK — **NO migration needed** (`db_fundamentals_raw` already in sql/147 CHECK per §0.14). PR-C's `_spec(...)` arg change is sufficient.

PR-A migration:

4. `sql/NNN_bootstrap_state_last_jobs_boot_error.sql` — ALTER TABLE bootstrap_state adds `last_jobs_boot_error TEXT NULL` (DDL in §4).

T1.2: NO schema changes; the entrypoint dispatch swap is atomic at deployment. The `fundamentals/` package conversion (§1.7) is a physical-layout migration only — no DB schema changes; import-compat invariant pinned by `tests/test_fundamentals_package_compat.py`.

T1.1: already migrated.

## §17. Operator runbooks

Stream A introduces THREE operator runbooks. All are executable Python under **`app/runbooks/`** (NEW directory; matches `etl-spec-template-usage.md §17` convention).

**Convention reconciliation (per Architect + Reviewer OBSERVATION):** the repo's existing `scripts/` directory hosts ad-hoc lint scripts (`check_*.sh`) + benchmark/backfill scripts (`backfill_*.py`, `audit_*.py`) — NOT operator-runnable per-source ETL runbooks under the 22-section spec template. `app/runbooks/` is the spec-template-mandated path. The two coexist; `scripts/` is NOT retired. PR-D creates `app/runbooks/__init__.py` for module resolution. NOTE (v2.4 fold of round-2 committee review): the spec originally said `app/cli/runbooks/` here, but `app/cli.py` already exists as the break-glass operator credential CLI (`python -m app.cli set-password`); creating a sibling `app/cli/` package would have shadowed it. Path was renamed to `app/runbooks/` before code landed.

**Merge-gate enforcement (per Architect IMPORTANT):** Stream-C gate is an **OPERATOR ATTESTATION**, not a CI check. PR-D description MUST include the `stream_a_stream_c_gate.py` JSON output with all 7 checks at `passed` OR `warning_*` (C6 quiescent warning is non-blocking), timestamped against the actual Run #8 `bootstrap_run_id`. CI cannot run a 90-minute real Run #8; reviewer enforces via PR description per CLAUDE.md ETL clauses 8-12. Without this clarification a future author could treat green CI as sufficient.

### `app/runbooks/stream_a_run_8_verify.py`

v2.4 shape — stdlib `argparse` (no `click` dep; `click` is only transitive via uvicorn). Single `--apply` flag (no `--dry-run` flag — default is dry-run when `--apply` absent). Three-tier safety gates from `app.runbooks.safety`. `httpx.Client()` preserves session cookie from `/auth/setup`.

```python
import argparse
from app.runbooks.safety import (
    assert_dev_env, assert_dev_db, assert_jobs_process_stopped,
    wait_for_jobs_process_started,
)
# ... opens psycopg + httpx.Client; cookie set by /auth/setup propagates
#     automatically to /system/* calls (per app/services/operator_setup.py
#     Mode A — loopback + no EBULL_BOOTSTRAP_TOKEN → no setup_token needed).
```

Actions when `--apply`:
1. **Pre-flight**: `assert_dev_env()` + `assert_dev_db(conn)` + `assert_jobs_process_stopped(url)` — all fail-closed.
2. **Pre-flight: disk + WAL check** — `db_size > 50 GB` or `wal_dir > 10 GB` → fail-closed.
3. **Cancel in-flight bootstrap** — POST `/system/bootstrap/cancel` FIRE-AND-FORGET. Reason (v2.4 fold of Codex re-pass BLOCKING 1): with jobs stopped, no orchestrator observes `cancel_requested_at`, so polling wastes up to 75 min. Any `running` row vanishes with the DB drop in step 4. Acceptable responses: 202 / 404 / 409 — log + continue.
4. **DROP + CREATE ebull_dev** under `acquire_jobs_process_fence(url)` — connect to `postgres` admin DB, `pg_terminate_backend` other sessions, DROP, on 55006 retry-twice with sleep, on second 55006 emit `pg_stat_activity` rows in RECOVERY footer + exit 1, CREATE.
   - NOTE (v2.4 fold of PR-D commit-1 empirical correction): PG advisory locks are PER-DATABASE, NOT cluster-wide. The fence dies with `DROP DATABASE ebull_dev`. Operator MUST keep jobs service stopped (e.g. `systemctl stop`) for the destructive phase; the TOCTOU window is unavoidable at the lock layer alone.
5. **Migrate** — `from app.db.migrations import run_migrations; run_migrations()` in-process (v2.4 fold of Codex re-pass IMPORTANT 4 — NOT alphabetical sql/ walk).
6. **Re-acquire jobs fence** on fresh ebull_dev DB (commit 1 helper).
7. **POST /auth/setup** with `{username='operator', password=secrets.token_urlsafe(24)}` via httpx.Client. Response `Set-Cookie` persists in the client jar. Print password ONCE with red banner. No env-var requirement (per operator decision in PR-D plan v3 review).
8. **POST /system/bootstrap/run** (same httpx.Client, cookie inherits) → capture `run_id` + `request_id` from response.
9. **Release jobs fence**. Print: "Start jobs process now; runbook will wait up to 10 min."
10. **`wait_for_jobs_process_started(url, timeout_sec=600)`** (commit 1 inverse probe) — block until jobs entrypoint holds its fence.
11. **Poll /system/bootstrap-status** every 30s up to 90 min, 3×5s retry per poll on ConnectionRefused/502. Compare `response.current_run_id == captured_run_id`; on mismatch exit 3 CRITICAL (data-corruption risk; foreign run NOT cancelled). On terminal status: capture per-stage timings + exit 0.
12. On 90-min timeout: exit 2; log curl cancel command + status URL; bootstrap continues.
13. **JSONL log** at `var/runbooks/stream_a_run_8_verify-<request_id>-<ts>.jsonl` (append-only). `.gitignore` excludes `/var/`.

### `app/runbooks/stream_a_t13_sidecar_repair.py`

v2.4 shape — same stdlib argparse + `--apply` collapse + three-tier safety. Adds `--bootstrap-run-id INT` optional (per Codex 1 IMPORTANT 8). `--archive-path` REQUIRED in `--apply`; optional in dry-run (per Operator NIT O14).

```python
import argparse
from app.runbooks.safety import (
    assert_dev_env, assert_dev_db, assert_jobs_process_stopped,
)
from app.services.sec_submissions_ingest import repair_cik_sidecar_from_archive
```

Actions when `--apply`:
1. `assert_dev_env()`
2. open psycopg → `assert_dev_db(conn)` → `assert_jobs_process_stopped(url)`
3. `repair_cik_sidecar_from_archive(conn, archive_path=..., cik=..., bootstrap_run_id=...)` (commit 1 helper)
4. Print telemetry dict + exit 0
5. On uncaught exception: print RECOVERY: footer + exit 1

Used when sidecar drifts from `submissions.zip` contents (e.g. S8 partial failure between zip extract + sidecar populate). Rebuilds entries from on-disk archive without re-fetching SEC.

### `app/runbooks/stream_a_stream_c_gate.py`

v2.4 shape — stdlib argparse + module-load import of `app.services.manifest_parsers` (v2.4 fold of Codex 1 IMPORTANT 10 — registry side-effect populates `registered_parser_sources()` used by C4; without this import C4 false-passes against an empty source set).

```python
# CRITICAL: this import side-effects-populates registered_parser_sources().
import app.services.manifest_parsers  # noqa: F401

import argparse
from app.jobs.sec_manifest_worker import registered_parser_sources
from app.jobs.ownership_observations_repair import _CATEGORIES
from app.services.capability_manifest_mapping import CATEGORY_TO_MANIFEST_SOURCES
from app.runbooks.safety import assert_dev_env, assert_dev_db
```

Actions:
1. `assert_dev_env()` + parse `--bootstrap-run-id INT` (required) + `--strict` (default True) + `--json-out PATH` (optional, default stdout).
2. open psycopg → `assert_dev_db(conn)`.
3. `UPDATE bootstrap_runs SET stream_c_gate_status='pending' WHERE id=%s` (v2.4 fold of round-1 Reviewer R6 — explicit pending stamp at gate start).
4. try: run C1..C7 → except: `UPDATE bootstrap_runs SET stream_c_gate_status='failed_runbook_crashed' WHERE id=%s; re-raise` (v2.4 fold of round-2 Operator O6).
5. `UPDATE bootstrap_runs SET stream_c_gate_status='passed' | 'failed_<cN>' WHERE id=%s`.
6. Print JSON envelope (schema_version=1) to stdout (or --json-out path).
7. Exit code: 0 = passed (incl warning_*), 1 = failed strict, 2 = invalid input.

JSON envelope shape (pinned per round-2 Operator O9):

```json
{
  "schema_version": 1,
  "runbook": "stream_a_stream_c_gate",
  "bootstrap_run_id": 123,
  "started_at": "2026-05-25T12:34:56+00:00",
  "ended_at":   "2026-05-25T12:35:01+00:00",
  "checks": [
    {"id": "c1", "status": "passed", "count": 1, "detail": "..."},
    {"id": "c6_treasury", "status": "warning_category_quiescent", "count": 0, "detail": "..."}
  ],
  "accepted": true,
  "exit_code": 0
}
```

## §18. Smoke matrix

T1.2 smoke panel (issuer-keyed): `AAPL, GME, MSFT, JPM, HD` (per CLAUDE.md ETL clauses §8). Verify:

1. `financial_periods.period_end_date` populated for each.
2. `instrument_valuation.pe_ratio` non-null for each.
3. `bootstrap_runs.coverage_floor_met = TRUE` after Run #8 (NULL = sweep didn't run; FALSE = below 0.80 floor).
4. Wall-clock for **Stage 25 (`fundamentals_sync`)** ≤ **15 min** (vs Run #7's 101 min) — **expressed as a band: target 8-15 min** per Reviewer + Codex finding (the 6× claim is defensible for fundamentals derivation; 5-min was overclaim).
5. **Share-class fan-out smoke:** GOOG (CIK 0001652044, class C) AND GOOGL (CIK 0001652044, class A) both have `financial_periods` rows for the same period. Same for BRK.A (CIK 0001067983) AND BRK.B. Without `siblings_for_issuer_cik`, only ONE row per CIK is written and one share-class is silently empty.

T1.3 smoke panel (filer-keyed for the sidecar):

- AAPL (CIK 0000320193) — 0 overflow pages expected (`recent` array fits 1000-cap).
- Vanguard (CIK 0000102909 — large trust) — multiple overflow pages.
- A KNOWN_FILING_AGENT_CIK (operator picks from `app/providers/implementations/sec_edgar.py:98+`) — sidecar MUST be empty for this CIK (filter check).
- A KNOWN tombstoned CIK (operator picks from `instrument_sec_profile` WHERE filings_count=0) — sidecar populated normally, S14 walk produces zero new accessions.

Verify sidecar entries match `submissions.zip` parser output for the three real filers.

T1.8 smoke: `pytest tests/test_jobs_boot_guard.py::test_operators_check`. Verify:
- (a) boot succeeds when 1 operator exists;
- (b) boot fails clearly when 0 operators + `last_jobs_boot_error` populated with operator-actionable string;
- (c) `EBULL_JOBS_SKIP_OPERATOR_CHECK=1` env var overrides for cold start (test sets env then invokes).

T1.1 smoke: cohort size measurement. `SELECT COUNT(*) FROM institutional_filers WHERE last_13f_hr_at >= now() - INTERVAL '380 days'` against dev DB. Expected ≤ 9,000 (vs pre-#1222 ~11,200). PR description records the observed count.

## §19. Cross-source verification

Independent reputable source for T1.2 fundamentals: SEC EDGAR direct via curl, for one of (AAPL, MSFT) — pick one fact (e.g. `EntityCommonStockSharesOutstanding`) and cross-verify the value derived in `financial_periods` against the raw companyfacts API response. Document the comparison in the PR description per CLAUDE.md ETL clause 9.

T1.3 sidecar: cross-verify ONE CIK's sidecar entries against EdgarTools' `Company(<cik>).get_filings()` enumeration (which uses its own per-CIK fetch — different code path from S8's zip parse). PAGE-NAME set MUST match `submissions.json.filings.files[*].name` for that CIK (per DE-lens finding 3 — page-set comparison, NOT filing-count comparison).

T1.8 + T1.1: N/A — boot guard and cohort bound are local concerns; no external authoritative source.

## §20. Test placement

T1.2:
- Unit: `tests/test_fundamentals_bootstrap.py` — table-driven over pure `_common.py` transforms (NOT mocked `audit_all_instruments` per Test-lens finding); ~10 cases covering coverage-floor / audit-classified / per-CIK skip / no-siblings paths.
- Integration: `tests/integration/test_fundamentals_bootstrap_e2e.py` — real per-worker DB, real `financial_facts_raw` fixture (cardinality budget ≤ 50 rows per the `test_coverage_audit_integration.py` pattern), real cap-gating; verifies `audit_all_instruments` is NOT called when caps unsatisfied + IS called when satisfied.
- Contract: extension of **existing** `tests/test_bootstrap_orchestrator.py` (verified §0.12 — `..._catalogue_invariants.py` does NOT exist) — assert `_STAGE_REQUIRES_CAPS["fundamentals_sync"]` requires the 4-cap tuple from §13.
- Package-compat: `tests/test_fundamentals_package_compat.py::test_all_legacy_imports_resolve` — table-driven over the 45 import sites from §0.10. Regenerate snapshot on PR-C land.
- Smoke: covered by Run #8 verification runbook.
- Flakiness budget: 0 retries.

T1.3:
- Unit: `tests/test_sec_cik_submissions_files_index.py` — sidecar populate/query/per-CIK-DELETE path with synthetic ZIP fixture **(NEW fixture: `tests/fixtures/sec/submissions_synthetic.zip` — 3 CIKs incl. multi-page Vanguard-shape)**.
- Integration: `tests/integration/test_s8_sidecar_populate.py` — real S8 invocation against the synthetic zip, verifying sidecar entries match expected page descriptors + agent CIK filtered out.
- Contract: `tests/test_s14_uses_sidecar.py` — assert S14's HTTP-mock layer registers ZERO PRIMARY-page calls when sidecar is populated (secondary-page fetches still allowed); assert FAIL path when sidecar is empty for in-universe CIK. **Requires `respx` — NEW dev dep added to `pyproject.toml [dependency-groups] dev`** (NOT `[tool.uv] dev-dependencies` — repo uses PEP 735 `[dependency-groups]`; Codex 1 IMPORTANT caught the wrong section).
- Smoke: as above.

T1.8:
- Unit: `tests/test_jobs_boot_guard.py::test_operators_check` — table-driven over `check_operator_exists(conn) -> BootGuardOutcome` (pure-function refactor per Test-lens finding). 3 cases: operator present / absent / skip-env-var set.
- Integration: `tests/integration/test_jobs_startup.py` — driver-level boot with various dev-DB states (NOT subprocess; pure-function refactor avoids subprocess + xdist incompatibility per Test-lens).

T1.1: already covered by #1010 / #1222 tests. NO new tests; Run #8 measurement is the regression check.

**Perf gate:** `tests/perf/test_stream_a_perf.py::test_fundamentals_sync_under_15min` — nightly-tier (NOT pre-push) test that runs T1.2 against a fixed synthetic universe + asserts wall-clock ≤ 15 min. Marks the 15-min budget regression-detectable instead of "we hope Run #8 holds" (per Test-lens finding).

**Perf marker registration (per DE IMPORTANT):** `pyproject.toml [tool.pytest.ini_options] markers` currently registers only `integration`. PR-D scope ADDS `perf = "tests requiring real DB and long wall-clock; nightly tier"` so `pytest -m "not perf"` correctly excludes the gate from pre-push runs. Without registration, `tests/perf/` runs by default and blows the pre-push budget.

**Cross-PR cumulative smoke:** `tests/integration/test_stream_a_cumulative.py` — boots a fresh per-worker DB through all 4 PRs' deltas, exercises T1.8 → T1.3 → T1.2 → T1.1, asserts each smoke condition (per PM-lens finding — no cumulative test means a regression at PR-D land could pass per-PR CI but break Stream A).

Flakiness budget across Stream A: 0 retries on unit + integration. Perf gate has 1 retry allowance (cold-cache jitter).

## §21. Rationale log

**Decision:** T1.2 lands as separate `fundamentals/bootstrap.py` module after §1.7 package conversion.
**Rejected:** "add a `mode='bootstrap'` flag to existing `fundamentals_sync`" — defeats physical separation per [bootstrap-mode-discipline](../../../.claude/skills/engineering/bootstrap-mode-discipline.md). The point is preventing accidental HTTP reintroduction; a shared function body with a mode flag is exactly the trap.

**Decision:** T1.3 sidecar PK is `(cik, page_name)` with page-descriptor columns only — NOT `(cik, accession_number)` and NOT including page bodies.
**Rejected (PK):** `(cik, accession_number)` (v1) — `files[]` entries are page descriptors not accessions (§0.5); the v1 PK would either require parsing every secondary page during S8 (defeats T1.3) or be conceptually wrong.
**Rejected (body cache):** caching secondary-page JSON bodies — secondary pages are NOT in the bulk archive (§0.5); body cache wouldn't help first install Run #8 at all. The optimization helps only second-and-subsequent first-installs (rare — dev DB rebuilds). If that need ever materialises, fileable as Stream A v2 / Stream B.

**Decision:** Per-CIK DELETE + INSERT for sidecar invalidation, NOT global TRUNCATE.
**Rejected:** TRUNCATE inside S8 — S8 is per-CIK at transaction level; global TRUNCATE between CIK 1 and CIK N leaves sidecar empty during a long-running ingest, breaking S14 fail-closed semantics.

**Decision:** Filter `KNOWN_FILING_AGENT_CIKS` at sidecar populate, NOT at consume.
**Rejected:** filter at consume (S14) — keeps the sidecar honest as a "real-filer-only" index, avoids storing rows that will always 404 downstream, single point of agent-CIK awareness.

**Decision:** T1.8 uses `_check_operator_exists_with_cleanup(fence_conn)` slotting into existing `_ensure_*_with_cleanup` chain at `app/jobs/__main__.py:660`.
**Rejected (`_ensure_*` name):** would conflate with the 5 existing helpers that RE-INSERT default singletons; operator absence is unrecoverable without `/auth/setup` so the helper hard-fails. `_check_*` signals "verify, fail if missing"; `_ensure_*` signals "create if missing" (Codex 1 IMPORTANT — caught §21 helper-name regression vs §1 + §17).
**Rejected (CLI flag):** v1's `--skip-operator-check` CLI flag — footgun (easy to leave on accidentally per Operator-lens). Env var `EBULL_JOBS_SKIP_OPERATOR_CHECK=1` is harder to set in error.
**Rejected (master_key API):** v3's `master_key.is_bootstrapped()` — does not exist (verified absent at v3 committee). The real semantic is "has the operator completed setup?", which is what `operators` row count tests.

**Decision:** T1.2 calls `siblings_for_issuer_cik` to fan out from CIK to share-class siblings (verified §0.7).
**Rejected:** keying T1.2 directly on CIK — silent data loss for GOOG/GOOGL, BRK.A/BRK.B (writes 1 row instead of N). Same bug class as #1102.

**Decision:** T1.4 fundamentals.py decomposition is DEFERRED to Stream B; the §1.7 package conversion is SCOPED to Stream A.
**Rejected (decompose-first):** "decompose first, then add bootstrap entrypoint" — 5+ days of intra-package refactor before the biggest single Run-#8 win. Architect + Reviewer convergence.
**Rejected (defer-package):** "defer package conversion to Stream B" — §13's bootstrap.py + _common.py pattern REQUIRES the package directory. Honest scoping puts the conversion in Stream A and the intra-package refactor in Stream B.

**Decision:** Stream-C correctness gate is 7 checks (C1-C7) per §1.8, NOT just "Layer 1/2/3 status='success'".
**Rejected:** v1's 3-check gate — Codex specifically: "can still pass with row_count=0, no manifest drain, or stale data_freshness_index". DE: "Run-#7 had rows_processed=NULL on bulk ingesters". v2's 7-check gate adds manifest-worker per-source drain + content+watermark assertions.

**Decision:** Runbooks live at `app/runbooks/` (NEW directory).
**Rejected:** v1's `scripts/runbooks/` — directory doesn't exist + violates `etl-spec-template-usage.md §17` convention. `app/runbooks/` matches the convention + benefits from existing Python module resolution.

**Decision:** Runbook `--apply` gated by `EBULL_ENV=='dev'` hard refusal.
**Rejected:** trust the operator to not run against PROD — same operator who triggered the bug we're fixing. Hard guard is cheap.

**Decision:** Drop redundant standalone `cik` index on sidecar; PK prefix scan suffices.
**Rejected:** keep the second index — dead weight per DE finding; PG B-tree handles cik-only equality + range via composite-PK prefix.

**Decision:** Sidecar `discovered_at` uses `clock_timestamp()` not `NOW()`.
**Rejected:** `NOW()` (= `transaction_timestamp()`, fixed at tx start) — a long-running S8 transaction could stamp newer rows with an artificially-old time per [SKILL.md §6.5.8](../../../.claude/skills/data-engineer/SKILL.md).

**Decision:** Sidecar `bootstrap_run_id` is NULLABLE; FK → `bootstrap_runs(id)`.
**Rejected (NOT NULL):** would break post-bootstrap S8 refreshes that don't fire under a `bootstrap_runs` row. NULL means "populated outside a tracked bootstrap run" — a legitimate state.
**Rejected (FK to `run_id`):** PK column is `id` (verified §0.4) — v1's `REFERENCES bootstrap_runs(run_id)` would fail at migration.

**Decision:** v2 enumerates closed-set `rows_skipped` reasons; removes `other` catch-all.
**Rejected:** v1's `other` catch-all — swallows defects per Reviewer + DE. Closed-set + raise-on-unmatched surfaces gaps.

**Decision:** Stream A PR sequence A→B→C→D is load-bearing.
**Rejected:** "land in parallel" — PR-B's sidecar must exist before PR-C's cap-strengthening lands (cap on `submissions_processed` assumes S8 sidecar populate is the cap-providing step in the new flow). Sequencing avoids merge-order foot-guns.

## §22. Open questions

1. **T1.3 sidecar growth bound at extreme scale.** If eBull's universe expands past 50k CIKs via Stream B international coverage, the sidecar grows to ~150k rows (still <100 MB). At what point does it justify partitioning? Defer to Stream B universe-expansion spec.

2. **T1.2 coverage threshold (0.80).** Picked to match OpenFIGI sweep (#1233 PR-1b). CUSIP-resolution coverage and fundamentals coverage have structurally different denominators. Stream A uses 0.80 as informational-only on first pass; codify per-source threshold in `docs/settled-decisions.md` if and when fundamentals-specific data justifies a different floor. **§16 PR-B migration adds `bootstrap_runs.coverage_floor_ratio NUMERIC(5,4)` alongside the existing `coverage_floor_met BOOLEAN`** so retroactive threshold-change analysis is possible without re-running bootstrap (per DE IMPORTANT — operators can SELECT `WHERE coverage_floor_ratio >= 0.85` after any future threshold revision). Tracked as tech-debt issue post-PR-D land.

## Cross-references

- [docs/_archive/2026-05/superseded-etl-rollout-v3.md](../../_archive/2026-05/superseded-etl-rollout-v3.md) — predecessor (rejected in committee).
- [.claude/skills/engineering/bootstrap-mode-discipline.md](../../../.claude/skills/engineering/bootstrap-mode-discipline.md) — load-bearing rules this spec applies.
- [.claude/skills/data-engineer/etl-stage-declaration.md](../../../.claude/skills/data-engineer/etl-stage-declaration.md) — StageSpec extension contract.
- [.claude/skills/data-engineer/etl-spec-template-usage.md](../../../.claude/skills/data-engineer/etl-spec-template-usage.md) — this spec's template (22 sections); §"Pre-write checklist" mandates the §0 grep proof above.
- [.claude/skills/data-engineer/SKILL.md](../../../.claude/skills/data-engineer/SKILL.md) §0.0 — before-spec gate (what §0 above evidences).
- [.claude/skills/data-engineer/SKILL.md](../../../.claude/skills/data-engineer/SKILL.md) §6.5.14 — fetch_strategy enum.
- [.claude/skills/data-engineer/SKILL.md](../../../.claude/skills/data-engineer/SKILL.md) §6.5.15 — bootstrap = derivation rule.
- [.claude/skills/data-engineer/SKILL.md](../../../.claude/skills/data-engineer/SKILL.md) §6.5.16 — hallucinated-API class.
- [.claude/skills/data-engineer/etl-endpoint-coverage.md](../../../.claude/skills/data-engineer/etl-endpoint-coverage.md) §5 — bootstrap stage reference.
- Memory: `project_etl_v3_consolidated_findings.md` — v3 reviewer synthesis.
- Memory: `project_stream_a_run_8_fixes_consolidated_findings.md` — Stream A v1 8-lens committee verdict (what this v2 fixes).
- Memory: `project_stream_a_run_8_fixes_review_codex.md` — Codex CTO biggest residual risk + what Stream A does NOT prevent (CUSIP resolution completeness; cited in §22 future-work).
- Memory: `project_1233_run7_receipts.md` — wall-clock baseline for Stream A projections.
- Memory: `project_1010_13f_cohort_bound.md` — T1.1 already-shipped context.
