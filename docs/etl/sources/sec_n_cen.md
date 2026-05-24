# sec_n_cen

**Class.** SEC ad-hoc (NOT a `ManifestSource`).
**Form / endpoint.** N-CEN / N-CEN/A — annual investment-company census. SEC archive `primary_doc.xml`.

## 0. Architectural exception — READ FIRST

**sec_n_cen BYPASSES the manifest framework.** Confirm absence:
- `ManifestSource` Literal at `app/services/sec_manifest.py:106-122` lists 15 sources. `sec_n_cen` is NOT one of them.
- `_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:1035-1193` has no stage for `sec_n_cen`.
- `data_freshness.py::_CADENCE` (`app/services/data_freshness.py:69-100`) has no entry for `sec_n_cen`.
- No `SCHEDULED_JOBS` cron dispatches an `ncen_classifier`-bound job in `app/workers/scheduler.py`.

**Per Stage B sweep finding: 5/5 ❌ in the audit framework.**

The classifier lives at `app/services/ncen_classifier.py`. Its public entrypoint `classify_filers_via_ncen` (`ncen_classifier.py:472-541`) walks `institutional_filer_seeds`, fetches each CIK's submissions.json, picks the latest N-CEN via `_find_latest_ncen` (`:233-285`), parses `<investmentCompanyType>` from `primary_doc.xml`, and UPSERTs into `ncen_filer_classifications`.

**As of HEAD (post-PR #1245), the only callers of `classify_filers_via_ncen` are tests at `tests/test_ncen_classifier.py`.** No production code in `app/` invokes it. The brief's claim that the classifier is "called from `daily_cik_refresh`" does not match the codebase — `daily_cik_refresh` at `app/workers/scheduler.py:1869` does not import or invoke `ncen_classifier`. The grep is empty.

**Integrity-framework trade-off.** Every other source under `docs/etl/sources/` plugs into the standard pipeline (manifest enum → freshness cadence → bootstrap stage → scheduled job → `_CATEGORIES` repair sweep). N-CEN intentionally does NOT, because:
1. Its output (`ncen_filer_classifications`) is a READ-side cache consumed by `compose_filer_type` (`ncen_classifier.py:549-590`) — called inline from `institutional_holdings.classify_filer_type` (`app/services/institutional_holdings.py:704-726`) on every 13F-HR upsert. The latest classification flows through automatically on the next 13F ingest cycle; no backfill migration needed.
2. The classifier is annual (N-CEN is filed once per year per fund) — full-cohort refresh fits in a single cron window without needing the manifest's incremental-discovery machinery.
3. Filer-type classification is "metadata about a filer", not "an observation about an instrument" — it doesn't fit the `*_observations` / `*_current` two-layer ownership model documented in `data-engineer/SKILL.md §write-through`.

**The architectural correctness of this bypass is OPEN-DEBATE.** See **#1313** — the decision ticket on whether to fold N-CEN into the manifest framework OR formalise the ad-hoc pattern (and add a second category for filer-metadata sources that don't have an observation-shaped output). Until #1313 lands, N-CEN runs as an undriven service: classifications get refreshed only via direct test invocation OR a future scheduled-job wiring PR.

**Latest-only semantic** (per PR #1245 / `project_1233_pr9_ncen_latest_only.md`): structural cap via `ncen_filer_classifications.cik` PK + UPSERT + row-constructor no-demotion predicate `(EXCLUDED.filed_at, EXCLUDED.accession_number) >= (existing.filed_at, existing.accession_number)` at `ncen_classifier.py:354-376`. Lint guard `scripts/check_ncen_latest_only.sh` invariants A/B/C/D pin the contract: PK on `cik`, single INSERT site, UPSERT clause present, `_find_latest_ncen` single-return early-exit.

## 1. Origin
Per-filer fetch of `https://data.sec.gov/submissions/CIK{cik}.json` (`ncen_classifier.py:182, 194-196`) → newest N-CEN accession (`_find_latest_ncen` newest-first early-return at `:233-285`) → per-accession `primary_doc.xml` fetch from archive (`:198-203`). Provider contract: `SecDocFetcher` Protocol (`ncen_classifier.py:58-66`); production binding `SecFilingsProvider`.

## 2. Watermarking model
**N/A** — no `data_freshness_index` row. Classification freshness implied by `ncen_filer_classifications.fetched_at` (refreshed on every UPSERT, `ncen_classifier.py:365`). No cadence ceiling means no "overdue" signal in the operator freshness panel.

## 3. Retry posture
Per-filer crash isolation at `ncen_classifier.py:510-519` — full classify+upsert+commit block wrapped in `try/except`. A single bad filer increments `crash_failures` and the loop continues. Counter routing keys on structured `_FilerOutcome.kind` (`:387-398`) NOT error-string substring matching (Codex pre-push lesson).

No backoff machinery; no `_failed_outcome`. Re-run is the entire batch — no per-row retry budget.

## 4. Bootstrap path
**NONE** — no stage in `_BOOTSTRAP_STAGE_SPECS`. The classifier does not run during bootstrap.

## 5. Steady-state path
**NONE in production code.** Only `tests/test_ncen_classifier.py` invokes `classify_filers_via_ncen`. This is the audit-framework gap #1313 will resolve.

## 6. Manifest insert
**N/A** — no `sec_filing_manifest` row, no `subject_type='ncen_filer'` literal, no Option C `filed_at` gate. The classifier writes directly to `ncen_filer_classifications` via `_upsert_classification` (`ncen_classifier.py:302-376`).

## 7. Parser
`app/services/ncen_classifier.py::parse_ncen_primary_doc` (`:164-174`) — pure-XML walk extracting `<investmentCompanyType>`. Six valid codes (N-1A/N-2/N-3/N-4/N-5/N-6), mapped to `FilerType ∈ {ETF, INV, INS, BD, OTHER}` via `_INVESTMENT_COMPANY_TYPE_MAP` (`:120-133`). Unknown codes default to `OTHER` so a future SEC enum addition surfaces in data without breaking the classifier (`_derive_filer_type` at `:136-141`).

No parser version constant — re-classification on every batch run.

## 8. Observation insert
**N/A** — N-CEN output is filer-metadata, not an observation. Writes to `ncen_filer_classifications` (PK on `cik`, single row per filer).

## 9. Current table refresh
**N/A** — no `_current` table. `ncen_filer_classifications` IS the current state. Not in `_CATEGORIES` at `app/jobs/ownership_observations_repair.py:69`. The daily drift-repair sweep does NOT reconcile this table.

Latest-only is enforced by the UPSERT predicate (see §0) — the database itself is the monotonicity oracle.

## 10. Operator-visible endpoint
**No dedicated endpoint.** Classification surfaces indirectly via `institutional_filers.filer_type` after the next 13F-HR ingest cycle calls `compose_filer_type` (`institutional_holdings.py:704-726`). The filer-type then appears in the `ownership_rollup` institutions slice at `/instruments/{symbol}/ownership-rollup`.

## 11. Verification queries
```sql
-- Confirm N-CEN classifications exist for known fund CIKs.
SELECT cik, investment_company_type, derived_filer_type, accession_number, filed_at, fetched_at
FROM ncen_filer_classifications
ORDER BY fetched_at DESC LIMIT 20;

-- Cross-check that 13F-HR ingest flow respects N-CEN classification.
SELECT f.cik, f.filer_name, f.filer_type, n.derived_filer_type
FROM institutional_filers f
LEFT JOIN ncen_filer_classifications n ON n.cik = f.cik
WHERE f.cik IN ('0001364742', '0000895421', '0000093751')  -- BlackRock, Morgan Stanley, MetLife
ORDER BY f.cik;
```
**Smoke** is currently the test suite, NOT a live cron: `uv run pytest tests/test_ncen_classifier.py -v`. Cross-source: spot-check `investmentCompanyType` from `https://efts.sec.gov/LATEST/search-index?q=%22N-CEN%22&forms=N-CEN&ciks=<cik>`.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_sec_n_cen_wired`. Asserts: provider importable, `ncen_filer_classifications` table exists. **Skips** all manifest / bootstrap / scheduled-job / freshness-cadence checks (explicit `pytest.skip("ad-hoc bypass — see docs/etl/sources/sec_n_cen.md §0 + #1313")`). The skip is the audit framework correctly reporting "this source is exceptional".

## 13. Known gotchas
1. **NOT in the manifest framework.** Every assumption from the README's "Cross-cutting invariants" section that keys on `ManifestSource` fails for N-CEN. Specifically: invariant #2 ("Manifest source enum is the registry") explicitly carves out N-CEN as the ONE ad-hoc bypass; invariants #4-#7 do not apply.
2. **No production caller.** `classify_filers_via_ncen` is defined but currently only invoked by tests. Filer-type classifications can ONLY be refreshed manually (`uv run python -c "from app.services.ncen_classifier import classify_filers_via_ncen; ..."` in a dev shell) until #1313 wires a scheduled job.
3. **Same-day tie-break on `accession_number`** (`ncen_classifier.py:329-340`). N-CEN + N-CEN/A filed same calendar day share `filed_at`; lexicographic accession ordering is SEC's intra-day sequence chronological order. Codex 2 (PR9) caught the gap.
4. **`_find_latest_ncen` early-return is load-bearing** (`ncen_classifier.py:242-256`). The SEC submissions array orders `recent` newest-first; the function exits on FIRST form match. Refactoring to "collect all then pick newest" would break the latest-only HTTP-budget contract. Lint guard `scripts/check_ncen_latest_only.sh` invariant D pins the single-return shape.
5. **Filer-type composition priority** (`ncen_classifier.py:549-590`): (1) curated ETF seed list #742 → `ETF`; (2) N-CEN derived_filer_type; (3) default `INV`. Broker-dealer (`BD`) is NOT addressable from N-CEN — broker-dealers file Form ADV / FOCUS instead.
6. **Per-filer commit semantics** (`ncen_classifier.py:481-484`). Mid-batch crash leaves a partial persistent state. This is intentional — restart re-classifies only un-processed filers — but it means "X classifications written" in the batch report is NOT the same as "Y filers attempted".
7. **#1313 is the decision ticket.** Any new ETL audit framework that strictly enforces the 5-layer wiring matrix MUST carve out N-CEN OR the framework rejects HEAD as non-compliant. Update the carve-out as #1313 progresses.
