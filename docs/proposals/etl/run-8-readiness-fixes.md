# Run #8 readiness fixes — bundled PR spec

**Status:** v1.4 (post-Codex-2 P1 folds). All 14 items implemented on `feature/1233-run-8-readiness-fixes`. 71 acceptance tests passing (2 skipped — sec_n_csr + sec_13dg fixtures live in `.tmp/spike-918/` + inline literals respectively; tracked).

**Codex 2 pre-push P1 folds (2026-05-24):**
- `app/services/sec_submissions_files_walk.py:298-334`: gate `set_watermark` on `page_upsert_errors == 0` per page. Codex 2 caught: upsert exceptions were swallowed + watermark advanced → next 304 hides the unrecorded filings forever. New `page_upsert_errors` counter; watermark only writes when zero per-page failures (retention-dropped is OK, errors are NOT).
- `app/jobs/sec_per_cik_poll.py:262-278`: gate `set_watermark` on `recorded == len(delta.new_filings)`. Same shape — `record_manifest_entry` `ValueError` was logged + swallowed but `last_known` still advanced. New `all_recorded` predicate.

**Implementation deviations from v1.3 (worth noting for reviewer):**
- Item 3: `assert_dev_db_name_in_url` is a SEPARATE guard (not chained into `assert_dev_env`) — chaining would break existing test contracts at `tests/test_runbook_safety.py`. All 3 runbooks (run_8_verify / t13_sidecar_repair / stream_c_gate) updated to call both guards explicitly. Same fail-fast behaviour; cleaner unit-test isolation.
- Item 4: Pydantic models in `app/runbooks/stream_a_stream_c_gate_schema.py`; `validate_envelope()` called inline at the emitter (the existing `_build_envelope` body now returns the validated payload). 6 contract tests pin the 8-key shape + extra='forbid' + Literal version pin.
- Item 7: NEW parallel API `check_freshness_conditional()` + `fetch_submissions_page_conditional()` rather than extending the existing 2-tuple `HttpGet` contract — keeps 4 other consumer lanes (drain/rebuild/atom/daily-index) on the unconditional path. Watermark namespace: `sec.last_modified.per_cik_poll` + `sec.last_modified.submissions_files`. 10 new conditional-GET tests.
- Item 8: `scripts/check_caller_owned_tx.py` (Python AST) + `scripts/check_caller_owned_tx.sh` (thin wrapper). Wired into `.githooks/pre-push` + `.github/workflows/ci.yml`. 4 tests including docstring-ignore + manifest-parser-exclusion invariants.
- Item 9: `tests/fixtures/sec/MANIFEST.toml` pins SHA-256 for 4 fixtures (sec_n_port × 2 + sec_13f_hr × 2). sec_n_csr + sec_13dg fixtures don't exist as committed files (live in `.tmp/spike-918/` + inline test literals respectively); SKIP marker tests flag the gap.
- Item 5: defensive 200-row no-cap test added to existing `tests/test_sec_cik_submissions_files_index.py` (verdict was REBUTTED — kept as regression guard only).
- Item 10: static-analysis tests at `tests/test_partition_coverage_run8_readiness.py` (4 cases pinning loop bounds + IF NOT EXISTS + canonical parent table name). Runtime partition-coverage is implicit via template-DB schema boot.

Total: **2 migrations** (sql/174 + sql/175); **8 production files** modified; **5 new test files**; **1 new README**; **1 new safety module** (`stream_a_stream_c_gate_schema.py`).

**Codex 1 diff re-pass corrections (v1.2 → v1.3):**
- Item 4 schema: pin to actual envelope shape — 8 top-level keys (`schema_version`, `runbook`, `bootstrap_run_id`, `started_at`, `ended_at`, `checks`, `accepted`, `first_failed`) per `stream_a_stream_c_gate.py:325-334`. Spec previously said `verdict` (wrong); actual is `accepted` (bool).
- Item 4 validator: use `pydantic` (already first-class repo dep via edgartools transitively) NOT `jsonschema` (not declared in `pyproject.toml`).
- Item 7 watermark: use distinct source-key namespace `sec.last_modified.<endpoint>` to avoid corrupting existing `sec.submissions` semantics (currently stores top accession at `app/services/fundamentals/__init__.py:2030`).
- Item 3 default: pre-connection check matches `assert_dev_db` post-connection default of `{"ebull_dev"}` when `EBULL_DEV_DB_NAMES` unset (was "skip with warning" which let default bad URL through).
- Item 8 filename: `app/services/finra_regsho_ingest.py` (was `finra_regsho_daily_ingest.py` — doesn't exist).
- Item 8 AST pattern: `ast.With.items[*].context_expr` matching `ast.Call(ast.Attribute(..., attr='transaction'))` (was malformed `Subscript(With(...))`).
- §6 Risks + §7 Sequencing: cleaned stale "4 migrations" / "sql/176/177" / "placeholder column migration" text.

**Codex 1 corrections (2026-05-24):** 2 BLOCKING + 4 IMPORTANT folded. Item 4 reframed (text-not-JSONB column → app-layer validation + contract test only, no DB CHECK). Item 8 scope narrowed (finra ingest files only; manifest parsers legitimately use `conn.transaction()`; must ignore docstrings). Item 3 reframed (DB name comes from `DATABASE_URL`, not `EBULL_DEV_DB_NAME`; `assert_dev_db` already partially does this — add pre-connection parse). Item 7 reuse existing `external_data_watermarks` table (no new column). Item 2 use catalog-level `pg_database.datminmxid` / `pg_class.relminmxid` probe (not just victim tables). Item 6 wrong file — `finra_short_interest.py` provider not parser. Item 1 ESOP wording nuance. Item 9 drop version-drift test (already pinned in `pyproject.toml`). Item 10 boundary syntax fixed (`'2035-04-01'` not `2035-Q1`).

**Branch:** `feature/1233-run-8-readiness-fixes`

**Closes:** unblocks #1233 operator drive. Folds 8-lens committee BLOCKINGs into one PR. Does NOT close #1233 directly (operator drive does).

**Author / date:** 2026-05-24.

---

## 0. Background

Post-Stream-A-PR-D ETL sweep (8 stages, rollup at `~/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_2026_05_24_rollup.md`) produced 8-lens committee review surfacing **18 BLOCKING + 33 IMPORTANT** findings. Triage:

- 4 BLOCKINGs already resolved by Stream A code-PR sequence (#1306-#1311).
- 5 BLOCKINGs were sweep memo numerical-count drift (cosmetic, fixed in parallel housekeeping).
- 3 BLOCKINGs reduced to doc-only fixes after investigation:
  - Architect ownership funds/esop divergence → CODEX_RIGHT verdict (doc-only, 3 docstrings).
  - PM B1 Stage H runbook owed → SHIPPED this session at `docs/operator/runbooks/run-8-readiness.md`.
  - PM B2 AND/OR rubric → folded into runbook §3.3.
- **Remaining real gaps = 14 items** spanning Cat 1 (operator-blocking) + Cat 2 (real bugs not blocking Run #8) + Cat 3 (decision/ticket-only).

User decision: bundle all 14 items into one PR. Run #8 then = clean final acceptance, not "passed-but-known-broken."

---

## 1. Definition of done

This PR is done when ALL of the following hold:

1. Every Cat 1+2 item has a code change OR docstring change matching its row in §3.
2. Every change has an acceptance test (unit or contract or smoke).
3. `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest` all pass (CLAUDE.md pre-push checklist).
4. New lint guard `scripts/check_caller_owned_tx.sh` passes against current code (and would fail against a synthetic violation — proven by negative test).
5. Stream-C gate envelope contract test catches a tampered envelope.
6. Smoke matrix (AAPL / GME / MSFT / JPM / HD) shows no regression on `/instruments/<symbol>/ownership-rollup` + `/instruments/<symbol>/financials`.
7. PR description records: smoke results / cross-source check / migration outcomes / PR-D unaffected verified.
8. Codex 1 (this spec) + Codex 2 (pre-push) both green.

---

## 2. Out of scope

- Stream B + Stream C work.
- CUSIP resolution implementation (item 11 = ticket only).
- sec_n_cen ManifestSource promotion (item 12 = decision + ticket; promotion deferred unless decided).
- FINRA UI panels (separate follow-up PR — Stage E proposed).
- sec_xbrl_facts steady-state HTTP residual (separate follow-up PR — Stage E proposed).
- Anything not in §3 below.

---

## 3. Item-by-item spec

### Cat 1 — operator-blocking

#### Item 1 — Ownership funds/esop sweep clarity (docs only)

**Why:** Architect lens flagged silent drift; investigation verdict = CODEX_RIGHT. Funds/esop ARE covered by daily 03:30 UTC `JOB_OWNERSHIP_OBSERVATIONS_SYNC` via `_CATEGORIES`. The confusion is `sync_all` vs `run_observations_repair_sweep` having different category sets for legitimate reasons. Doc gap.

**Codex 1 nuance:** ESOP wording must not claim "no legacy mirror source." `sync_def14a` (lines 691-769) DOES route legacy `def14a_beneficial_holdings` ESOP rows into `ownership_esop_observations`. The asymmetry is that `sync_all` has no SEPARATE esop entry — esop is processed transitively through `sync_def14a`. Funds genuinely has no legacy source.

**Change:**
- `app/services/ownership_observations_sync.py:797` — add docstring to `sync_all` clarifying: mirrors 5 legacy typed tables; **ESOP is processed transitively inside `sync_def14a`** (rows from `def14a_beneficial_holdings` flagged as ESOP); **funds has no legacy mirror source** (write-through only via NPORT); the drift-repair sweep at `_CATEGORIES` covers all 7 categories.
- `app/jobs/ownership_observations_repair.py:69` — comment above `_CATEGORIES`: all 7 categories including funds + esop tracked here. Funds are event-driven via NPORT write-through; ESOP also gets daily reconciliation here in addition to its transitive sync via DEF14A.
- `app/workers/scheduler.py:4355` — `ownership_observations_sync` job docstring cross-references `sync_all` (`JOB_OWNERSHIP_OBSERVATIONS_BACKFILL` at `:4426`) as the distinct legacy-backfill path.
- `.claude/skills/data-engineer/SKILL.md` §write-through — add note: "Funds: no legacy mirror; event-driven via NPORT only. ESOP: processed transitively inside `sync_def14a` (DEF14A bene-table) + daily reconciliation via `_CATEGORIES` sweep."

**Acceptance test:** new unit test asserts `sync_all` returns exactly 5 categories AND `_CATEGORIES` returns exactly 7. Test name pins the asymmetry so future-author can't "fix" by adding `sync_funds`/`sync_esop`.

**Effort:** 20min.

#### Item 2 — §6.3 pre-wipe procedure in runbook docstrings + app/runbooks/README.md

**Why:** Operator B1 BLOCKING. Procedure lives only in `docs/superpowers/specs/2026-05-19-data-retention-rubric.md` + `project_1233_pr12_ownership_merge_writer.md` memo. Operator running `--apply` cold without reading those will hit multixact wraparound mid-run.

**Codex 1 correction:** probing only `job_runtime_heartbeat` + `broker_credentials` (the known victim tables) is brittle — misses catalog-level damage. Use authoritative PG sources.

**Change:**
- New `app/runbooks/README.md` — discoverability (PM I1). Lists each runbook + when-to-use + safety-class. Cross-references `docs/operator/runbooks/run-8-readiness.md`.
- `app/runbooks/stream_a_run_8_verify.py` module docstring — add §6.3 pre-wipe summary with reference to runbook + retention-rubric spec.
- `app/runbooks/safety.py` — add `assert_no_multixact_wraparound()` primitive that probes:
  1. `pg_database.datminmxid` for current DB — warn at age > `autovacuum_multixact_freeze_max_age * 0.8`
  2. `pg_class.relminmxid` joined to `pg_namespace` — find top-5 oldest tables in `public` schema; fail if any age exceeds threshold
  3. Best-effort symptom probes against `job_runtime_heartbeat` + `broker_credentials` (the historical victims) — non-fatal warnings supplementing the catalog check
  Called from `stream_a_run_8_verify.main` BEFORE any destructive op.

**Acceptance test:** unit test for `assert_no_multixact_wraparound` against fixture DB with synthetic high-age multixact state — must raise + give actionable error message naming the §6.3 spec + retention runbook path.

**Effort:** 1h (was 45min — catalog probe more involved than symptom probe).

#### Item 3 — DATABASE_URL pre-connection parse + EBULL_DEV_DB_NAMES match

**Why:** Operator B2 BLOCKING. `assert_dev_db(conn)` at `app/runbooks/safety.py:71` already checks `current_database()` against `EBULL_DEV_DB_NAMES` AFTER opening a connection. Failure mode: deep-stack cryptic error if `DATABASE_URL` points at non-dev DB while `EBULL_DEV_DB_NAMES` excludes it. Need pre-connection parse.

**Codex 1 correction:** current DB name comes from `settings.database_url` parsed from `DATABASE_URL` env (`app/config.py:16`), not `EBULL_DEV_DB_NAME`. The pre-connection check parses DB name from URL.

**Change:**
- `app/runbooks/safety.py` — new `assert_dev_db_name_in_url()` primitive: parse DB name from `DATABASE_URL` (urllib parse), compare to `EBULL_DEV_DB_NAMES` comma list. Fail with actionable error BEFORE any connection attempt. Called from `assert_dev_env` so all runbooks inherit it.
- `app/runbooks/stream_a_run_8_verify.py` — order remains `assert_dev_env` first; the new check fires inside that.
- Keep existing `assert_dev_db(conn)` as the post-connection belt-and-braces gate. The two checks now reinforce.
- Update `docs/operator/runbooks/run-8-readiness.md` §1.1 E2 to confirm pre-connection fail-fast behaviour.

**Default behaviour when `EBULL_DEV_DB_NAMES` unset:** match `assert_dev_db(conn)` post-connection default — fall back to `{"ebull_dev"}` allowlist. (Codex 1 caught: previous "skip with warning" would let `DATABASE_URL=postgres://.../ebull` pass pre-check then fail at connection. Match the existing post-check default for parity.)

**Acceptance test:** unit test for `assert_dev_db_name_in_url` with (a) URL DB name matches `EBULL_DEV_DB_NAMES` (pass), (b) URL DB name not in list (fail), (c) `EBULL_DEV_DB_NAMES` unset + URL DB name = `ebull_dev` (pass via default), (d) `EBULL_DEV_DB_NAMES` unset + URL DB name = `ebull` (fail — matches default-list behaviour), (e) malformed URL (raise).

**Effort:** 30min.

#### Item 4 — Stream-C envelope application-layer validation + contract test

**Why:** API B4 + Codex B1 + Test B2 — three lenses converge. Envelope client-pinned only via `JSON_SCHEMA_VERSION=1` at `app/runbooks/stream_a_stream_c_gate.py:53`. No schema validation when emitted to JSONL / stdout. Tampered/wrong-shape envelopes pass; #1233 attestation can lie.

**Codex 1 BLOCKING correction:** `bootstrap_runs.stream_c_gate_status` is a **TEXT** column at `sql/173_bootstrap_runs_stream_c_gate.sql:44` storing **only** `pending|passed|failed_*` state strings. The JSON envelope is NOT persisted there — it's emitted to stdout + `var/runbooks/*.jsonl` at `stream_a_stream_c_gate.py:325` + `:337`. **A JSONB CHECK migration is impossible** — the column type forbids it.

**Reframed change — application-layer only, Pydantic-based, no DB schema change:**

- New `app/runbooks/stream_a_stream_c_gate_schema.py` — Pydantic `BaseModel` (or `TypedDict` + `pydantic.TypeAdapter`) pinning the v1 envelope. **Exactly 8 top-level keys** per current emitter at `app/runbooks/stream_a_stream_c_gate.py:325-334`:

  | Key | Type | Notes |
  |---|---|---|
  | `schema_version` | `Literal[1]` | Pin to int 1 |
  | `runbook` | `Literal["stream_a_stream_c_gate"]` | Pin to canonical name |
  | `bootstrap_run_id` | `int` | bootstrap_runs.id FK |
  | `started_at` | `str` (ISO-8601) | UTC timestamp |
  | `ended_at` | `str` (ISO-8601) | UTC timestamp |
  | `checks` | `list[CheckRecord]` | nested model per `_check_record` shape |
  | `accepted` | `bool` | overall verdict (NOT `verdict`) |
  | `first_failed` | `str \| None` | check ID or null |

  `model_config = ConfigDict(extra="forbid")` rejects unknown keys (prevents silent shape drift).
- `app/runbooks/stream_a_stream_c_gate.py` — replace the inline `return {...}` at `:325-334` with `Envelope(**payload).model_dump()`. Validation fires inside the constructor. Then emit to stdout + JSONL.
- New `tests/runbooks/test_stream_c_gate_envelope_contract.py`:
  1. **Positive**: build envelope via canonical path → model validates.
  2. **Negative — missing key**: drop `schema_version` → `ValidationError`.
  3. **Negative — wrong type**: set `accepted` to str → `ValidationError`.
  4. **Negative — wrong version**: set `schema_version=2` → `ValidationError` (until v2 model added).
  5. **Negative — unknown key**: add `verdict` field → `ValidationError` (extra="forbid").

**No new migration. No DB CHECK.** State column at `bootstrap_runs.stream_c_gate_status` continues to hold lifecycle state strings only; envelope contract lives in code + tests.

**Dependency:** uses existing `pydantic` (first-class via `edgartools==5.30.2` transitive + present in `app/services/fundamentals/*.py` direct usage). No new `pyproject.toml` entry.

**Acceptance test:** all 5 contract test cases green (positive + 4 negative).

**Effort:** 1h.

#### Item 5 — Sidecar multi-primary-page (REBUTTED — defensive test only)

**Verdict:** API_CONTRACT_WRONG (verifier 2026-05-24). SEC submissions schema has exactly **one** primary endpoint per CIK (`data.sec.gov/submissions/CIK{padded}.json`); overflow is paginated within `filings.files[]` of that single primary; sidecar indexes overflow descriptors with composite PK `(cik, page_name)` — no per-CIK row cap. Lens conflated "single primary endpoint" with "single point of enumeration."

**Change:** none in production code.

**Defensive test (optional, 15min):**
- New `tests/services/test_sec_cik_submissions_files_index.py::test_sidecar_no_per_cik_cap` — synthetic 200-entry `filings.files[]` payload → assert `refresh_cik_sidecar` writes exactly 200 rows for the CIK. Pins the no-cap invariant against a future bug that might silently truncate.

**Acceptance test:** the defensive test (if added) passes against current code.

**Effort:** 15min (test only).

### Cat 2 — real bugs (not Run-8-blocking but completeness gap)

#### Item 6 — FINRA bimonthly provider catches 403 like RegSHO (#916)

**Why:** API B1 BLOCKING. Bimonthly currently catches 404 only. FINRA CDN returns 403 for not-yet-published trade dates (confirmed at #916). Bimonthly will misfire first not-yet-published 15th-of-month run.

**Codex 1 correction:** the manifest parser at `app/services/manifest_parsers/finra_short_interest.py` is **synth no-op** (no HTTP path — pattern matches #1168 sec_10q). 403 mapping belongs in `app/providers/implementations/finra_short_interest.py:117-119` where 404 → `FinraNotFound` mapping currently lives.

**Change:**
- `app/providers/implementations/finra_short_interest.py:117-119` — add `403` alongside `404` in the `FinraNotFound` mapping. Match the RegSHO provider's pattern (`app/providers/implementations/finra_regsho_daily.py` from #916).

**Acceptance test:** unit test in `tests/providers/test_finra_short_interest_provider.py` — mock 403 response → raises `FinraNotFound` (currently leaks as uncaught `HTTPStatusError`).

**Effort:** 15min.

#### Item 7 — Conditional-GET If-Modified-Since via external_data_watermarks

**Why:** API B2 BLOCKING. SEC exposes `Last-Modified`. Some SEC fetchers DO round-trip it via `external_data_watermarks` already (`sec.submissions` / master index). Gap: per-CIK submissions + other per-filing fetchers don't yet use it. Every steady-state tick re-fetches full payload, wastes 10 req/s budget.

**Codex 1 correction:** DO NOT add a new column to `sec_filing_manifest` or `data_freshness_index`. The generic per-source/key watermark table `external_data_watermarks(source, key, watermark, watermark_at, response_hash)` at `sql/034_external_data_watermarks.sql:9` already has the exact shape needed. SEC submissions + master index use it (`app/services/watermarks.py` + `app/services/fundamentals/__init__.py:1977`).

**Codex 1 diff re-pass:** distinct source-key namespace required. Current `source='sec.submissions'` stores **top accession**, not HTTP Last-Modified (`fundamentals/__init__.py:2030, :2533`). Reusing the same key would conflate two distinct semantics. Use namespaced key `sec.last_modified.<endpoint>` (e.g. `sec.last_modified.submissions`, `sec.last_modified.atom`, `sec.last_modified.per_cik_poll`).

**Reframed change — no schema change; extend existing watermark consumer pattern with new namespace:**

- Identify SEC fetchers that DON'T currently round-trip `Last-Modified`. Target 2 highest-traffic: per-CIK submissions overflow walker + per-CIK poll fetcher. Defer atom-fast-lane + master-index to follow-up.
- For each: capture `Last-Modified` header → upsert to `external_data_watermarks` with `source='sec.last_modified.<endpoint>'` + `key='<cik or accession>'` + `watermark=<Last-Modified string>`; on next fetch read watermark + send `If-Modified-Since`; 304 → skip-without-payload + bump `watermark_at` only (no `watermark` change).
- `app/jobs/sec_per_cik_poll.py:39` — honour 304 by updating watermark_at only.
- Document the new source-key namespace in `app/services/watermarks.py` module docstring.

**Acceptance test:** integration test with fixture providers (`respx`-mocked SEC) — initial fetch persists watermark; subsequent fetch within freshness window sends `If-Modified-Since` + handles 304 without payload re-parse.

**Effort:** 2h.

**Risk:** scope creep if fetcher inventory turns out larger than expected. **Mitigation:** target 2 highest-traffic fetchers (per-CIK submissions + per-CIK poll); defer atom + others to follow-up if scope >2.5h. Document decision in PR description.

#### Item 8 — Caller-owned-tx lint script (FINRA ingest only)

**Why:** Test B1 BLOCKING. Rule "FINRA ingest services must NOT enter own `with conn.transaction():`" lives in docstrings only at `finra_*_ingest.py:13-15`. A new FINRA-shape provider could silently break atomicity.

**Codex 1 BLOCKING correction:** manifest parsers under `app/services/manifest_parsers/` LEGITIMATELY use `with conn.transaction():` (verified: `sec_10k.py:208`, `sec_n_csr.py:208`, `def14a.py:242` etc.). The original spec scope was wrong. The rule applies ONLY to caller-owned FINRA ingest modules. Also: grep MUST skip docstrings + comments — `finra_short_interest_ingest.py:13` contains the forbidden text only in a docstring explaining the rule.

**Reframed change — narrow scope + docstring-aware + correct AST pattern:**

- New `scripts/check_caller_owned_tx.py` (Python, NOT shell — wrapper `scripts/check_caller_owned_tx.sh` invokes it for `.githooks/pre-push` compatibility):
  - Scope: `app/services/finra_short_interest_ingest.py` + `app/services/finra_regsho_ingest.py` (Codex 1 corrected: file is `finra_regsho_ingest.py` NOT `finra_regsho_daily_ingest.py`). Any future `app/services/finra_*_ingest.py` matches the glob.
  - Use Python `ast` module. AST shape (corrected by Codex 1):
    ```python
    # For each ast.With node:
    for item in with_node.items:
        ctx = item.context_expr
        if isinstance(ctx, ast.Call) and isinstance(ctx.func, ast.Attribute):
            if ctx.func.attr == "transaction":
                # Check ctx.func.value is Name('conn') or Attribute('self', 'conn')
                ...
    ```
  - Docstrings are ignored automatically by AST (they're string literals, not `With` nodes).
  - Output: 1 line per violation with path:line.
- Add `.githooks/pre-push` chain entry: `bash scripts/check_caller_owned_tx.sh`.
- Add to CI workflow at `.github/workflows/ci.yml`.

**Acceptance test:** lint passes against current code (no violations in current FINRA ingest files; their docstrings ARE allowed). Negative test: synthetic violation file in `tests/fixtures/lint/` → script exits non-zero.

**Effort:** 45min (was 30min — AST not grep adds complexity).

#### Item 9 — Parser fixture pinning (accession + SHA-256 only — version already pinned)

**Why:** Test B3 BLOCKING. Fixtures aren't pinned to their source accession + SHA-256. A fixture file accidentally edited or replaced wouldn't be caught.

**Codex 1 correction:** `edgartools==5.30.2` is already hard-pinned at `pyproject.toml:21`. Version-drift test is redundant with the package pin. Keep fixture SHA + accession pinning (genuinely adds value); DROP the version-drift assert.

**Change:**
- New `tests/fixtures/sec/MANIFEST.toml` — for each fixture file: `{path, accession, sha256, source_url, captured_at}` rows.
- New `tests/test_fixture_pinning.py` — on test session start, verify every fixture file matches its recorded SHA-256. Fail loudly with diff guidance if mismatch (don't auto-rewrite).
- Populate MANIFEST entries for highest-load-bearing fixtures first: `sec_n_csr`, `sec_n_port`, `sec_13f_hr`, `sec_13dg` (the 4 with known #932-class validation cliff exposure). Defer broader 13-parser sweep to follow-up if scope-pressed.

**Acceptance test:** test session fails if any pinned fixture SHA mismatches.

**Effort:** 1.5-2h (was 2-3h — narrowed scope; version assert dropped).

#### Item 10 — Partition extensions (finra_regsho_daily + financial_facts_raw)

**Why:** DE IMP2. Hard deadlines: `finra_regsho_daily_observations` tail at `q_start < '2030-04-01'`; `financial_facts_raw` loop `2010..2030`. INSERTs fail at deadline.

**Codex 1 correction:** boundary syntax — use `'YYYY-MM-DD'` not `YYYY-Qn` literal in DO-block. Partition name templates: `finra_regsho_daily_observations_p_%sq%s` and `financial_facts_raw_%sq%s`. Parent table for facts = canonical `financial_facts_raw`.

**Change:**
- Migration `sql/174_finra_regsho_daily_partitions_2035.sql` — extend `finra_regsho_daily_observations` partitions: loop `WHILE q_start < '2035-04-01'` (5y headroom). Match `sql/154_finra_regsho_daily.sql:81` DO-block shape.
- Migration `sql/175_financial_facts_raw_partitions_2040.sql` — extend `financial_facts_raw` partitions: loop `FOR y IN 2031..2040` (10y headroom). Match `sql/156_financial_facts_raw_partition.sql:59` shape.
- (Optional) `/system/postgres-health` new probe row: `partition_tail_warning` — warn when ANY partitioned-table tail is within 12 months of `EXTRACT(YEAR FROM now())`. Defer if scope-tight.

**Acceptance test:** post-migration query confirms partition list covers target year (`SELECT COUNT(*) FROM pg_inherits WHERE inhparent = 'finra_regsho_daily_observations'::regclass` returns ≥ 45 = 25 existing + 20 new). Boot smoke at `tests/smoke/test_app_boots.py` already covers schema validity.

**Effort:** 30min (without health-probe) / 1h (with).

### Cat 3 — out-of-band (tickets + memo patches, NOT in PR)

- **Item 11 — CUSIP resolution ticket.** Draft body: "19/16M unresolved per Stage C row S13 / Run #7 receipts. Codex's pick for highest-ROI residual. Scope: investigate resolver bottleneck (OpenFIGI batch? CUSIP master from Bloomberg? SEC EDGAR cross-ref via 13F Official List?); define target resolution rate; propose phased rollout. Acceptance: ≥80% resolution rate against current 16M cohort." File via `gh issue create` after PR merge.
- **Item 12 — sec_n_cen decision ticket.** Draft body: "Decide: promote sec_n_cen to ManifestSource (filer-type is sticky; cadence value low) OR document deliberate stranding. Sticky-filer-type argues for stranding; integrity-framework argues for promotion. Recommend operator-decision after Run #8 outputs are validated." File post-merge.
- **Item 13 — Sweep memo count-drift patch.** Patcher agent running in parallel (this session). Out-of-band; no code commit.
- **Item 14 — app/runbooks/README.md.** Folded into Item 2 (no separate ticket needed).

---

## 4. Migrations

| # | File | Purpose |
|---|---|---|
| sql/174 | `finra_regsho_daily_partitions_2035.sql` | Extend partitions to 2035-Q1 |
| sql/175 | `financial_facts_raw_partitions_2040.sql` | Extend partitions to 2040 |

Total: **2 migrations** (down from 4 after Codex 1 corrections):
- Item 4 envelope validation = app-layer only (no DB CHECK; column is TEXT).
- Item 7 If-Modified-Since reuses existing `external_data_watermarks` table (no new column).

All forward-only. All idempotent via `IF NOT EXISTS`. Latest existing migration at `sql/173`; 174/175 are unclaimed.

---

## 5. Smoke matrix

Per CLAUDE.md ETL clauses 8-12:

| Instrument | Endpoint | Expectation |
|---|---|---|
| AAPL | `/instruments/AAPL/ownership-rollup` | No regression vs pre-PR baseline |
| GME | `/instruments/GME/ownership-rollup` | No regression |
| MSFT | `/instruments/MSFT/financials` | No regression |
| JPM | `/instruments/JPM/ownership-rollup` | No regression |
| HD | `/instruments/HD/ownership-rollup` | No regression |

Cross-source verify: spot-check 13F filer count for one instrument vs gurufocus.

---

## 6. Risks

1. **Item 7 (If-Modified-Since) scope creep.** Touches 2 fetchers in scope; could spread to more if shared client layer needs refactor. **Mitigation:** if impl reveals >2.5h, target only `per_cik_poll` and defer submissions walker to follow-up (record decision in PR description). No DB schema change in either path (uses existing `external_data_watermarks`).
2. **Item 9 (fixture pinning) cohort scope.** 4 high-churn parsers in v1.3 scope (sec_n_csr / sec_n_port / sec_13f_hr / sec_13dg). **Mitigation:** broader sweep deferred to follow-up.
3. **Sidecar verdict RESOLVED.** API_CONTRACT_WRONG. Scope dropped by ~1h. Only defensive test (15min) retained.
4. **Codex 2 pre-push surprise.** With 2 migrations + 7 code areas touched, Codex 2 may surface integration concerns. **Mitigation:** allocate 1-2h for Codex 2 fixes pre-push.
5. **Pydantic model placement.** New `stream_a_stream_c_gate_schema.py` adds runbook-side model. If pydantic version drifts, model behaviour drifts too. **Mitigation:** pin envelope-model tests run in same suite as edgartools-Pydantic-cliff tests; both regression-detect on `uv sync`.

---

## 7. Sequencing (per CLAUDE.md working order)

1. Schema changes — sql/174 (finra partitions) + sql/175 (financial facts partitions). Total: 2 migrations.
2. Service logic:
   - Item 1 docstrings (ownership)
   - Item 2 runbook README + safety.assert_no_multixact_wraparound
   - Item 3 safety.assert_dev_env hoist
   - Item 4 Stream-C envelope server-validation
   - Item 5 sidecar (after verdict)
   - Item 6 FINRA 403
   - Item 7 If-Modified-Since
3. Tests:
   - Item 1 sync_all 5-vs-7 invariant test
   - Item 2 multixact wraparound test
   - Item 3 EBULL_DEV_DB_NAMES tests
   - Item 4 envelope contract + negative tests
   - Item 6 FINRA 403 test
   - Item 7 If-Modified-Since integration test
   - Item 8 lint script + negative
   - Item 9 fixture pinning manifest + verifier
   - Item 10 partition coverage test
4. Self-review per pre-flight-review skill.
5. Codex 2 pre-push (`codex.cmd exec review`).
6. Push.
7. Bot iteration to APPROVE.

---

## 8. Operator runbook updates

After merge, append to `docs/operator/runbooks/run-8-readiness.md`:
- §0: re-render committee residual table marking items 1-10 as ✅ closed.
- §1.1: re-list env-var checks if E2 changed.
- §1.2: re-list postgres-health probes if new partition-tail warning added.
- §4.2: note that envelope is now server-side-validated; any tamper attempt rejected at write.

---

## 9. Rationale log

- Why bundled: per `feedback_fix_in_scope_default.md` + user explicit direction ("Run #8 should be final acceptance, not passed-but-known-broken"). Many items couple (envelope contract test + envelope server-validation; multixact assert + README discoverability).
- Why doc-only fix for Item 1: investigation verdict — Codex was right; Architect conflated two jobs. Code is correct; only docs are missing.
- Why 4 migrations not 5: Cat 2 Item 8 (lint) + Item 9 (fixture pinning) need no schema. Cat 1 Item 5 (sidecar) deferred until verdict.
- Why If-Modified-Since (Item 7) in this PR despite 2h scope: API contract lens called it BLOCKING for budget; conserves SEC rate during operator-busy Run-8-recovery scenarios.

---

## 10. Cross-references

- Sweep rollup: `~/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_2026_05_24_rollup.md`
- Stage H runbook: `docs/operator/runbooks/run-8-readiness.md`
- Committee per-lens memos: `~/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_etl_sweep_stage_g_committee_*_2026_05_24.md`
- CLAUDE.md workflow: branch + Codex 1 + push + bot iteration
- PR-D session memo (precedent for app/runbooks/ + Stream-C envelope): `project_stream_a_pr_d_session_end_2026_05_24.md`
