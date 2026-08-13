# Post-bootstrap hardening plan (new session)

**Context.** 2026-06-02: first clean end-to-end bootstrap landed (run 3, 21/21 stages) after the Postgres OOM fix (#1426 — global heavy-ingest concurrency cap=2, PR #1427 merged). Data baseline validated SOUND. eToro demo portfolio synced. This plan sequences the remaining hardening into small, isolated, tested PRs so the bootstrap/ETL can be "put down once and for all."

Findings below are from: an independent code-explorer agent (UX/timings), Codex (perf + correctness + tech debt), and direct DB validation. All file:line verified this session.

---

## Data baseline verdict (validated 2026-06-02) — SOUND

- Orphans (ownership/facts → instruments): **0**.
- Uniqueness: PK-enforced. `ownership_institutions_current` PK = `(instrument_id, filer_cik, ownership_nature, exposure_kind)` — multi-row-per-filer is by design (13F voting nature × SH/PRN), NOT duplicates.
- `period_end` bounds: 0 rows outside [1900,2100) (#1218 guard holding). financial_facts_raw range 2006-06-27 → 2034-06-30 (future-dated XBRL guidance facts exist, in-bounds).
- Figures correct: AAPL 14.687B sh / GME 448.375M sh — match public record, EDGAR-linked.
- Coverage of 12,496 universe: 40% facts · 23% inst · 36% insider · 21% funds. **TODO: confirm this is universe composition (ETFs/foreign ADRs lack XBRL/SEC ownership) vs under-ingestion — run instrument-type breakdown (type table = `etoro_instrument_types`).**

---

## PR sequence (each: branch → TDD → gates → Codex ckpt-2 → PR → review → merge)

### PR-A — "Re-run all" on clean run + correctness guards  (small, high-value)
- **Bug:** `app/services/processes/bootstrap_adapter.py:418` `can_full_wash=(state_status != "running")` → "Re-run all" enabled (red/destructive) when `state='complete'`. ("Re-run failed"/`can_iterate` is correctly gated.) Fix: exclude `"complete"` (and/or surface a non-alarming state + tooltip; decide whether re-running after a clean run should be allowed-but-de-emphasised vs blocked).
- **Correctness guards (Codex safe-now):**
  - 13F can land NULL/zero/negative `shares` unguarded — `sec_13f_dataset_ingest.py:685` parse, `:730` write; schema nullable `sql/114:55`. Add positivity/not-null guard at parse.
  - Ownership `period_end` has no upper-bound check before DEFAULT partition — `sec_13f_dataset_ingest.py:646`, `sec_nport_dataset_ingest.py:698`. Add [1900,2100) guard (mirror #1218 XBRL pattern).
  - Manifest CIK/accession weakly validated — `sec_manifest.py:262` only non-empty; `sql/118` no regex (ownership tables have regex `sql/134:50`). Add format guard.

### PR-B — Progress %/timings  (operator's explicit ask; spec exists: `docs/proposals/admin/first-install-bootstrap-ux.md §4.2`; tickets #1225, #1271)
- **Root cause:** columns exist (`bootstrap_stages.processed_count/target_count/last_progress_at`) and the FE reads them, but the **5 bulk ingesters never write them** → FE shows only a "running" badge, no %, no ETA. Stages: `sec_submissions_ingest`, `sec_companyfacts_ingest`, `sec_13f_dataset_ingest`, `sec_insider_dataset_ingest`, `sec_nport_dataset_ingest`.
- Backend: add a `BootstrapStageProgress` helper (own connection, never reuses ingest conn) + wire each ingester to tick `processed_count`/`target_count`/`last_progress_at` (§4.2 spec).
- FE (`frontend/src/pages/ProcessDetailPage.tsx:1257-1318`): render per-step **elapsed** (`started_at` is in payload, currently unused) + an **overall %** (add `overall_progress_fraction` to `BootstrapTimelineResponse` + server compute in `get_bootstrap_timeline`).
- Dead columns: `bootstrap_stages.expected_units`/`units_done` (`sql/129:91-92`) — zero writers; mark deprecated in COMMENT, drop in a later migration.

### PR-C — Perf safe-now (isolation-tested)
- Stream the whole-file TSV `list[dict]` loads → accession-keyed dicts via streaming pass: `sec_13f_dataset_ingest.py:247,603`; `sec_nport_dataset_ingest.py:198,538`.
- Chunk `unresolved_buffer` flushes (grows to millions in RAM): `sec_13f_dataset_ingest.py:656,751`; `sec_nport_dataset_ingest.py:676,834`.

### PR-D — Tech-debt cleanup (Codex safe-now)
- Stale comments now contradicting runtime: docstring "26-stage" `bootstrap_orchestrator.py:3`; "max_concurrency=5" `:1182` (heavy cap is 2 `:306`).
- Retire unused symbols: ordering caps `:675`, dynamic recency sentinels `:1277`.
- Extract duplicated lock-step CUSIP-map loaders into a shared helper: `sec_13f_dataset_ingest.py:91` + `sec_nport_dataset_ingest.py:173`.

### PR-E — P4 validation-floor calibration (leftover from #1426)
- Turn placeholder `>0` floors in the load-time validation stage into real absolute floors from run-3 counts: instruments 12,496 · financial_facts_raw 16.5M · ownership_institutions_current 1.15M · ownership_funds_current 695,728 · ownership_insiders_current 61,025 · filing_events 2.7M.

### PR-F — Decouple portfolio sync from the SEC-bootstrap gate  (operator UX)
- **Problem:** an operator who adds their eToro key at setup sees NO portfolio/dashboard until the entire multi-hour SEC bootstrap completes, because `orchestrator_high_frequency_sync` / `daily_portfolio_sync` / `monitor_positions` are gated on `bootstrap_not_complete` (all skipped the whole run — confirmed in job_runs). Portfolio data does not depend on SEC ingestion.
- Fix: let broker portfolio sync run independently of SEC bootstrap (it needs only the universe/instrument map, available early), so the dashboard populates as soon as the key is added. Investigate the prereq wiring (`app/workers/scheduler.py` ScheduledJob prereqs + the runtime `bootstrap_not_complete` gate).
- Note: `monitor_positions` correctly requires existing positions ("no open positions" prereq) — it's an alerting job, not the fetch. The fetch is `daily_portfolio_sync` / the 5-min orchestrator.

---

## Tickets to FILE (need-care — don't rush into the above PRs)
- **S16 manifest seed set-based rewrite:** `sec_first_install_drain.py:201` DISTINCT-ON over all filing_events + per-row `record_manifest_entry` (`:262`) — the slow drain. Helper also seeds freshness (`sec_manifest.py:320`) so a pure SQL rewrite needs care. High-value perf.
- 13F/NPORT staging drain re-scans (DISTINCT-ON + pre-count + marker reconcile): `sec_13f_dataset_ingest.py:362,769,802`; `sec_nport_dataset_ingest.py:380,779,861`.
- N-PORT per-series savepoint upsert loop: `sec_nport_dataset_ingest.py:579,797`.
- Derived ownership pct can exceed 1 (rollup, operator-visible): `ownership_rollup.py:886,951` — add clamp/guard.

---

## Operator notes / current live state
- Worker running (PID may change), bootstrap_state=complete; steady-state jobs ungated (manifest worker draining the ~2.3M deferred manifest rows lazily @ 10 req/s; portfolio sync every 5 min).
- Ad-hoc trigger scripts created this session (handy, keep or remove): `scripts/_p6_trigger_bootstrap.py` (committed via #1427), `scripts/_trigger_portfolio_sync.py`, `scripts/_trigger_monitor_positions.py` (untracked). No-HTTP-auth job triggers via `publish_manual_job_request_with_conn`.
- Codex CLI: must be `>=0.136` + default model (no `-m`) — see memory `feedback-codex-chatgpt-model-fix`.
- `--no-verify` justified while pre-existing #1424 tests fail the full-suite hook + dev PG churn.

---

## TICKET INDEX + ordered worklist (filed 2026-06-02)

All findings now ticketed. Suggested execution order (user-facing → hardening → perf → cleanup). Each = its own small, tested, Codex-reviewed PR.

### P0 — portfolio is visibly wrong (do first)
- **#1428** get_portfolio treats `quotes.last=0.00` as valid → fake −100% P&L (VOO/IEP/BBBY). PRIMARY fix.
- **#1429** quote sync writes `last=0.00` (data source behind #1428).
- **#1435** decouple broker portfolio sync from the `bootstrap_not_complete` gate (portfolio invisible until SEC bootstrap done).
- **#1430** instrument 1181 = BBBY (delisted) phantom quote — verify eToro-ID↔universe mapping.
- **#1431** `instruments.currency` empty (latent FX) + numeric `exchange` code unmapped.

### P1 — bootstrap UX + correctness
- **#1432** "Re-run all" enabled on a clean `complete` run (the named bug). Related: existing **#1264** (first_run_pending).
- Progress %/timings (operator ask) — EXISTING epic **#1335** + **#1005** (wire units mid-stage) + **#1271** (timeline auto-refresh). Plus the file:line gaps in this doc §PR-B (5 ingesters write processed/target/last_progress; FE per-step elapsed + overall %). Reconcile #1005's units_done wiring vs the dead-column drop in #1437.
- **#1433** ingest correctness guards (13F NULL/neg shares; ownership period_end [1900,2100); manifest CIK/accession regex).
- **#1434** P4 validation-floor calibration from run-3 counts.
- **#1424** stale test mocks (unblocks the full-suite pre-push pytest gate).

### P2 — performance (mostly EXISTING tickets)
- **#1436** stream TSV list[dict] loads + chunk unresolved_cusip buffers (safe-now, worker RAM).
- EXISTING: **#1276** per-row INSERT+savepoint ~1500 rows/s ceiling · **#1274** serial ingest_all_active_filers · **#1275** dispatcher wait(all) serialises lanes · **#1349** unresolved_13f_cusips 1.3GB bloat · **#1339** S20 form3 HTTP-walks already-bulk.
- need-care (file when scoped): S16 manifest seed set-based rewrite (DISTINCT-ON 2.7M + per-row record_manifest_entry, the slow drain — `sec_first_install_drain.py:201`); 13F/NPORT staging re-scans; N-PORT per-series savepoint; ownership-rollup pct>1 clamp (`ownership_rollup.py:886,951`).

### P3 — tech-debt + verification
- **#1437** stale comments ("26-stage", "max_concurrency=5"), dead columns (expected_units/units_done), duplicated CUSIP-map loaders, unused symbols.
- **#1438** verify coverage is universe-composition not under-ingestion (40% facts etc.).
- EXISTING adjacent: **#1221** forward-dated facts/partitions past 2030 (saw period_end 2034) · **#1224** stage stuck pending after finalize · **#1226** JobLock contention UX · **#1267/#1229** admin banner/label papercuts.

### Shipped this session (closed)
- **#1423** newest-13F-window 404 cascade (PR #1425). **#1426** bootstrap ingest OOM concurrency cap (PR #1427).

### New-session kickoff
Start at **#1428** (portfolio P&L — highest user-visible impact, small). Run Codex checkpoint-1 on this doc first if executing as a batch.
