# US ETL completion — autonomous-execution plan

> **Goal:** Drive the US ETL endpoint coverage matrix to "ALL ✅" with bootstrap + steady-state daily updates both green. No new tech-debt tickets raised during execution — fix-in-scope as discovered.
>
> **Status:** DRAFT 2026-05-17. Operator-approved one-shot; subsequent sessions execute phases autonomously.
>
> **Reference:** `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 (per-source matrix) + §7 (gap register).

## 1. Autonomy contract — what runs without asking

Across all phases below, the executing session SHALL:

- Spec → Codex 1a → revise to CLEAN — no operator signoff between iterations.
- Plan → Codex 1b → revise to CLEAN — no operator signoff between iterations.
- Implement → local gates → Codex 2 pre-push → revise to CLEAN.
- Push → Monitor PR checks with proper terminal-exit conditions (no dead polling, no sleep loops, no ScheduleWakeup).
- Read bot review on landing → resolve every comment via `FIXED` / `DEFERRED` / `REBUTTED` contract → re-push → re-monitor → loop until APPROVE on the most recent commit + CI green.
- Merge (squash + delete branch).
- Update memory + handover summary.
- **No new tech-debt tickets.** If scope grows mid-PR — implement the fix in the SAME PR. The rule is: close everything we touch.

The executing session SHALL ASK the operator ONLY when:

- A genuine product / architectural trade-off requires judgement that Codex cannot resolve unilaterally (per `feedback_design_granularity.md`).
- A destructive operation outside the diff is required (Postgres restart, branch deletion of work-in-progress, etc.).
- A scope decision would touch outside the US-ETL coverage matrix (e.g. UI work, broker integration, anything in §F of the May 17 status report).

The operator handles in parallel (out-of-band):

- Bootstrap completion via admin UI — `/admin → Bootstrap → "Retry failed"`. The 5 remaining stages from #1187 retry land at ~67min/each over ~6h serial wall-clock. Operator does this whenever; not on the engineering critical path.

## 2. Phase plan (10 PRs)

Each phase = one autonomous session. Handover between phases.

### Phase 1 — Quick wins (parser hygiene + missing registry entries)

**PR 1: G7 — `sec_xbrl_facts` synth no-op parser**

- File: `app/services/manifest_parsers/sec_xbrl_facts.py` (NEW, follows sec_10q / sec_n_csr pattern).
- Body: `return ParseOutcome(status='parsed', parser_version='xbrl-facts-noop-v1')`. Document why (Company Facts API bulk path is the real ingest; manifest rows exist for accession tracking).
- Register in `app/services/manifest_parsers/__init__.py::register_all_parsers()`.
- Test: 1-line existence + behaviour test.
- Matrix update: §2 row + §7 G7 → CLOSED.
- Acceptance: manifest rows with `source='sec_xbrl_facts'` drain to `parsed` instead of `debug-skipped`.

**PR 2: G14 — `bootstrap_orchestrator` source-registry entry + manual-queue dispatch**

- Surfaced this session (PR #1188 T9-POST). `publish_manual_job_request(bootstrap_orchestrator)` → listener `_run_manual` → `JobLock(bootstrap_orchestrator)` → `source_for(...)` → KeyError.
- File: `app/jobs/sources.py::MANUAL_TRIGGER_JOB_SOURCES` add `"bootstrap_orchestrator": "init"` (pre-everything fence; serialises with universe_sync).
- Test: integration test that `publish_manual_job_request(bootstrap_orchestrator)` dispatches without KeyError.
- Acceptance: admin retry endpoint works without bypassing JobLock; operator-side retry no longer requires direct-Python-invocation workaround.

**PR 3: G13 — `subjects_due_for_recheck` reader verification**

- Per matrix §3 G13: Layer 3 `run_per_cik_poll` was updated in #1155 to drain both `subjects_due_for_poll` AND `subjects_due_for_recheck`. Verify the wiring actually fires under production cadence.
- AST audit + integration test that exercises both reader paths.
- If wiring exists per memory `[[us-source-coverage]]` "G13 recheck path added" — close G13 with test only.
- If wiring missing — fix in-scope.

### Phase 2 — Bridge tables (G8 + G9)

**PR 4: G8 — `company_tickers_exchange.json` consumer**

- Closes pink-sheet / OTC / foreign-without-ADR gap in CIK↔ticker bridge.
- Endpoint: `https://www.sec.gov/files/company_tickers_exchange.json`.
- Consumer: existing `daily_cik_refresh` shape; add as supplemental enrichment OR new dedicated ScheduledJob `daily_cik_exchange_refresh`.
- Persistence: extend `external_identifiers` with exchange metadata OR new `instrument_exchange` table per design.
- Matrix update: §4 row + §7 G8 → CLOSED.

**PR 5: G9 — `company_tickers_mf.json` consumer**

- ~28k mutual-fund rows with `seriesId` + `classId`. Already partially used by #1174 (S25 `mf_directory_sync` populates classId → instrument_id). Verify whether this PR is the canonical seed OR if #1174's path is sufficient. If sufficient — close G9 with documentation only.
- If not — wire dedicated consumer + persistence; bundle with PR 4 if scope aligns.

### Phase 3 — Cross-quarter discovery (G12)

**PR 6: G12 — `master.idx` quarterly cross-quarter walker**

- Endpoint: `https://www.sec.gov/Archives/edgar/full-index/YYYY/QTRn/master.idx`.
- Use case: cross-quarter discovery of accessions that the per-CIK polling missed (e.g. tombstoned CIKs, late-arriving amendments).
- Consumer: new ScheduledJob `sec_master_idx_quarterly_sweep` (cadence: weekly mid-quarter, monthly after).
- Persistence: writes to `sec_filing_manifest` for the manifest worker to drain.
- Matrix update: §4 row + §7 G12 → CLOSED.

### Phase 4 — API alternatives (G10 + G11)

**PR 7: G10 — `companyconcept` API consumer**

- Endpoint: `https://data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json`.
- Use case: smaller-payload alternative to Companyfacts for known-tag pulls. Reduces SEC bandwidth + tightens fundamentals_sync inner loop.
- Implementation: extend `SecFundamentalsProvider` with a `fetch_concept(cik, taxonomy, tag)` method. fundamentals_sync opts into it for the Tier-1 metric set; full Companyfacts fetch remains the fallback.
- Performance audit: measure bandwidth + latency delta vs Companyfacts for a representative cohort.
- Matrix update: §4 row + §7 G10 → CLOSED.

**PR 8: G11 — `frames` API consumer**

- Endpoint: `https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/USD/{period}.json`.
- Use case: cross-sectional one-fact-per-filer for sector aggregates. v1 metrics surface currently doesn't expose sector heatmaps (the use case for this); evaluate whether the consumer is needed without a downstream UI. If no UI use case in v1, close G11 as DEFERRED-no-consumer via documentation; if there's a metrics ticket that benefits, wire the consumer.
- **Decision rule:** if `gh issue list --search "frames OR sector heatmap OR cross-sectional"` returns an open feature ticket, wire the consumer. Otherwise document G11 closure as "BY DESIGN — no v1 consumer; reopen when a sector-aggregate metric ticket lands."

### Phase 5 — Parser rewrites (EdgarTools drop-ins)

**PR 9: #925 — EdgarTools 13F-HR parser drop-in (follow-up to #913)**

- Current `sec_13f_hr.py` is hand-rolled. EdgarTools `Filing.obj()` returns a typed `ThirteenF` model that handles PRN/SH drop + 2023-01-03 VALUE cutover natively.
- Risk: per memory `[[edgartools]]` + #932, EdgarTools has a Pydantic validation cliff that can reject our fixtures. Pre-impl spike against the existing 13F-HR golden fixtures.
- If spike INFEASIBLE — close #925 with REBUTTED reasoning + freeze hand-rolled parser.
- If spike OK — drop-in + remove ~200 lines of hand-rolled parser code + extend tests to cover the EdgarTools path.

**PR 10: #932 — EdgarTools N-PORT FundReport parser drop-in (follow-up to #917)**

- Same shape as PR 9. Memory `[[edgartools]]` documents the Pydantic validation cliff. Pre-impl spike against existing N-PORT golden fixtures.
- If spike INFEASIBLE — close #932 with REBUTTED.
- If spike OK — drop-in + tests.

### Phase 6 — FINRA short-interest (#915 + #916)

**PR 11: #915 — FINRA bimonthly short interest ingest + schema**

- Parent **#796** + **#845** (closed). Headline real coverage gap.
- Endpoint: FINRA short interest bimonthly publication.
- Schema: `finra_short_interest_observations` (settle date, security symbol, short volume, days-to-cover). Partitioned by settle date if growth profile warrants.
- Consumer: new ScheduledJob `finra_short_interest_refresh` + writes to manifest.
- Parser: `app/services/manifest_parsers/finra_short_interest.py` registered.
- Matrix update: §2 row + §7 G6 → CLOSED (bimonthly portion).

**PR 12: #916 — FINRA RegSHO daily short volume ingest**

- Same shape as PR 11. RegSHO daily-volume files.
- Schema: `finra_regsho_daily_observations` (settle date, symbol, short volume, total volume, exchange).
- Bundles with PR 11 if cohesion is tight enough; otherwise sequential.
- Matrix update: §7 G6 → fully CLOSED.

### Phase 7 — Tech-debt hardening (#935)

**PR 13 + 14 — #935 ETL foundation contracts (split as needed)**

Per #935 issue body, six contract tasks:

1. Targeted rebuild full-history discovery (`sec_rebuild` follows `filings.files[]`).
2. First-install drain seeds scheduler rows.
3. Manifest worker enforces raw-payload persistence for payload-backed parsers.
4. Amendment detection uses `is_amendment_form` everywhere (replaces `endswith("/A")`).
5. No-parser manifest rows operator-visible (stats / log-level / job-result-detail).
6. Provider tag drift fix (capability tags ↔ implementation sources canonical mapping).

Bundle into 1-2 PRs. Sizing decision: 6 contracts is borderline for one PR; split if local gates show >2h per PR. The split point is the discovery vs invariant boundary — contracts 1+2 (rebuild + drain) in one PR; contracts 3+4+5+6 (invariants + observability + mapping) in a second.

### Phase 8 — Final validation + matrix sweep

**No PR — single audit pass + memory close-out.**

- Re-run `.claude/skills/data-engineer/etl-endpoint-coverage.md` matrix audit:
  - §2 — all 14 `ManifestSource` rows ✅.
  - §3 — all 3 discovery layers ✅.
  - §4 — all reference + bulk-archive endpoints ✅ OR formally documented BY DESIGN.
  - §7 — gap register: all `OPEN` rows → `CLOSED` OR `BY DESIGN`.
- Update `[[us-source-coverage]]` memory: replace open-gaps caveat with "ALL US ETL CLOSED — operator-side post-merge: bootstrap + daily steady-state validated end-to-end".
- Final smoke: trigger orchestrator FULL sync → confirm every db-lane + sec_rate-lane adapter target lands `job_runs.status='success'` (or legitimately `no_work` / `prereq_skip` for the right reason).
- Mark `[[legacy-cron-retirement]]` pre-condition fully MET.

## 3. Per-session handover template

At end of every session that closes a PR, append a handover block to this plan doc:

```markdown
## Handover — PR #<n> (merged <date>)

- Phase: <phase number>
- Gap / ticket closed: <ID>
- Merge SHA: <sha>
- Tests added: <list>
- Scope discoveries handled in-scope: <list, or "none">
- Matrix delta: <row + status>
- Next phase: <phase number + scope>
```

## 4. Anti-patterns to AVOID

Surfaced from the May 17 session (#1184 + #1187 retrospective):

- **No raising follow-up tickets for nits caught during PR review.** Fix in the same PR. Bot NITPICK / WARNING / PREVENTION — all resolved in the current PR, no `DEFERRED #N` escapes.
- **No long `sleep` in monitor scripts.** Use `until <terminal-condition>` loops or proper `Bash run_in_background` for one-shot waits. Per `feedback_no_sleepy_claude.md`.
- **No ScheduleWakeup mid-PR-cycle.** Drive each PR to merge in one session; if it can't fit, structure the next session to pick up at a clean handover point.
- **No "tested via mock — let CI catch real DB issues" punts.** Per `feedback_smoke_gate_swallowed_failures.md`. Every PR runs `tests/smoke/test_app_boots.py` locally.
- **No premature signoff asks.** Default to autonomous merge per §1 autonomy contract.
- **No closing-1-opening-many.** Every PR closes more tickets than it opens. The "in-scope discovery → fix-now" rule is non-negotiable. Track scope expansion in the PR body, not via new issues.

## 5. Estimated session count + sequencing

| Phase | PRs | Sessions (estimated) | Cumulative |
|---|---|---|---|
| 1 — Quick wins | 3 | 1 (PR 1+2 in one; PR 3 separate) | 1 |
| 2 — Bridge tables | 2 | 1-2 | 2-3 |
| 3 — Cross-quarter | 1 | 1 | 3-4 |
| 4 — API alternatives | 2 | 1-2 (G11 may be doc-only) | 4-6 |
| 5 — Parser rewrites | 2 | 2 (spike + impl per PR) | 6-8 |
| 6 — FINRA | 2 | 2 | 8-10 |
| 7 — #935 hardening | 1-2 | 1-2 | 10-12 |
| 8 — Final validation | 0 | 0.5 | ~12 |

Operator runs bootstrap completion in parallel — independent track, no engineering bottleneck.

## Handover — Phase 1 (PRs 1+2, session 1)

### Handover — PR #1190 (open 2026-05-17)

- Phase: 1
- Gap / ticket closed: **G7** (`sec_xbrl_facts` synth no-op manifest parser)
- Branch: `feat/g7-sec-xbrl-facts-synth-noop`
- Merge SHA: pending (awaiting Claude review bot + CI on most recent commit)
- Tests added:
  - `tests/test_manifest_parser_sec_xbrl_facts.py` — 4 tests (happy-path drain to `parsed`, form-agnostic seed (`10-K/A`), registry-wiring after `clear_registered_parsers` + `register_all_parsers`, durability gate proving the parser never calls `conn.execute` / `conn.cursor` / `conn.transaction` / `store_raw` / `fetch_document_text`).
- Scope discoveries handled in-scope:
  - `tests/test_fetch_document_text_callers.py` allow-list extension (Codex round-1 HIGH — the test would otherwise have flagged the parser/test pair as stale entries).
  - Module docstring updated to explicitly state the non-caller invariant (mirrors the sec_10q.py #1168 contract symmetry).
  - Cross-check table name corrected: `company_facts` → `financial_facts_raw` (the actual Companyfacts bulk-ingest target via `upsert_facts_for_instrument`).
- Matrix delta:
  - §2 row `sec_xbrl_facts` — `❌ by design` → `✅ sec_xbrl_facts.py (G7)`, status `WIRED`.
  - §7 G7 — `BY DESIGN` → `✅ CLOSED 2026-05-17`.

### Handover — PR #1191 (open 2026-05-17)

- Phase: 1
- Gap / ticket closed: **G14** (`bootstrap_orchestrator` source-registry entry)
- Branch: `feat/g14-bootstrap-orchestrator-source-registry`
- Merge SHA: pending (awaiting Claude review bot + CI on most recent commit)
- Tests added:
  - `tests/test_bootstrap_orchestrator_source_registry.py` — 5 tests (registry membership + value, `source_for` resolves without `KeyError`, `JobLock` constructs cleanly at the original KeyError site, `publish_manual_job_request` lands the queue row with no rejection, **disjointness invariant** walking `_BOOTSTRAP_STAGE_SPECS` directly + asserting no stage resolves to the `bootstrap` lane).
- Scope discoveries handled in-scope:
  - **Plan called for `init` source — that was wrong.** Codex round-1 BLOCKING surfaced the cross-thread `ContextVar` bug: bootstrap's `ThreadPoolExecutor` workers do NOT inherit `_HELD_SOURCES`, so the #1184 same-context re-entrancy bypass cannot fire from inside a stage worker. Any source shared with an inner stage (`init` with `nightly_universe_sync`; `db` with several Phase E stages; etc.) would have the worker thread hit `pg_try_advisory_lock` on a key the listener thread already holds → `JobAlreadyRunning` → stage fails.
  - **Pivoted to a fresh `bootstrap` lane** added to the `Lane` Literal in `app/jobs/sources.py`. Disjoint from every per-stage lane by construction; cross-thread inner acquisitions never contend with the outer lock. Multiple bootstrap triggers still serialise via the `bootstrap` advisory lock; `bootstrap_state.status='running'` remains the primary trigger-publish-time fence.
  - Added the disjointness invariant test that walks `_BOOTSTRAP_STAGE_SPECS` directly (Codex round-2 suggestion) so a future stage addition that uses `lane='bootstrap'` fails CI loudly.
- Matrix delta: G14 closes US-source-coverage memory hole flagged as PR #1188 T9-POST follow-up. No `.claude/skills/data-engineer/etl-endpoint-coverage.md` row touched (G14 is registry plumbing, not a discovery/parser entry).
- **Prevention-log candidate (extract on review)**: "ThreadPoolExecutor workers don't inherit `ContextVar`; pick a disjoint source for invokers that fan stages out to threads." Code/test comments capture this verbatim — extract to `docs/review-prevention-log.md` on first review round if the bot doesn't already cite a sibling entry.

### Next phase

- **Phase 1, PR 3 — G13 (`subjects_due_for_recheck` reader verification).** Plan §2 Phase 1 — AST audit + integration test verifying `subjects_due_for_recheck` reader path actually fires in Layer 3's `run_per_cik_poll`. If wiring exists per memory `[[us-source-coverage]]` "G13 recheck path added" — close G13 with test only. If wiring missing — fix in-scope.
- **Operator: post-merge follow-up.** Once PR #1190 + #1191 land, the admin "Retry failed" path can drop the direct-Python workaround. Confirm the remaining 5 bootstrap stages from #1187 retry land `bootstrap_state.status='complete'` end-to-end via the proper queue-listener path.

### Handover — PR #1193 (merged 2026-05-17)

- Phase: 1
- Gap / ticket closed: **G13** (`subjects_due_for_recheck` reader verification — sub-finding of #1155)
- Branch: `feat/g13-verify-recheck-reader-wiring`
- Merge SHA: `078a5e68bf9690a8b5ed6db2d9bd118a05895232`
- Tests added:
  - `tests/test_g13_recheck_reader_invariants.py` — 4 tests (imports invariant, consumed-call invariant, local-rebind invariant, return-annotation + `PerCikPollStats.recheck_*` fields invariant) under a single `_RunPerCikPollVisitor` traversal with symmetric nested-scope skips.
- Plan-defined acceptance: **closed-with-tests-only.** Wiring already exists at `app/jobs/sec_per_cik_poll.py:195-198` (added in #1155); integration coverage already at `tests/test_sec_per_cik_poll.py::TestG13RecheckPath`; hourly cadence + prereq + source already at `tests/test_layer_123_wiring.py::test_layer3_per_cik_poll_registered`. The PR added the static AST safety-net only.
- Scope discoveries handled in-scope:
  - Codex round 1 (medium): naive `ast.walk(fn)` recursed into nested scopes; reader-call result not checked as consumed. Both addressed by `_ConsumedReaderVisitor` with nested-scope skips + materialiser / iteration-shape check.
  - Codex round 2 (medium): local-rebind gap (a future stub `subjects_due_for_recheck = lambda: iter([])` would defeat the consumed-call invariant). Addressed by `test_reader_names_not_locally_rebound`.
  - Codex round 2 (note): `if False:` reachability — REBUTTED in PR body (full Python reachability is undecidable; the contrived form does not reflect a realistic regression; runtime reachability is owned by the integration suite).
  - Bot review round 1 (WARNING): rebind check used `ast.walk(stmt)` which recursed into nested scopes — asymmetric with the consumed-call visitor that skipped them. **FIXED** by merging both invariants into a single `_RunPerCikPollVisitor` (one traversal, one scope-skip rule). Module docstring records the scope-walk discipline so future intra-function AST checks reuse the unified visitor.
  - Bot review round 1 (PREVENTION): scope-walk asymmetry. **EXTRACTED** in-file (test module docstring "Scope-walk discipline" section). Not yet repo-wide-extracted to `docs/review-prevention-log.md` because this is currently a single-file pattern; if a second test file ever adds the same pattern, escalate.
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §3 sub-gap G13 narrative — "production never reaches it" → ✅ CLOSED narrative.
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §7 gap register G13 row — `OPEN` → `✅ CLOSED 2026-05-17`.
- ETL clauses #8-#12 — N/A (test-only PR, no parser / schema / data-path change; documented in PR body).

### Phase 1 close-out — all three PRs merged

PR 1 (G7 #1190) + PR 2 (G14 #1191) + PR 3 (G13 #1193) all merged. Phase 1 complete.

### Next phase (Phase 2 entry)

- **Phase 2, PR 5 — G9 (`company_tickers_mf.json` consumer).** ✅ CLOSED in-scope by PR #1194 (G8) — stale audit entry corrected. Consumer existed since #1171 (`refresh_mf_directory` bundled into `daily_cik_refresh` Stage 6) + #1174 (S25 `mf_directory_sync` dedicated bootstrap stage). Matrix §2 + §4 + §7 updated accordingly. No new code needed.

## Handover — PR #1194 (merged 2026-05-17)

- Phase: 2
- Gap / ticket closed: **G8** (`company_tickers_exchange.json` consumer) + in-scope correction of stale G9 + matrix `company_tickers_mf.json` rows (consumer existed since #1171 / #1174 — audit entry stale)
- Branch: `feat/g8-company-tickers-exchange-directory` (deleted post-merge)
- Merge SHA: `30cd582347467670dc2690462af4e3662cf27faa` (squash)
- Tests added:
  - `tests/test_exchange_directory.py` — 12 service tests (happy-path / CIK zero-pad / multi-ticker CIK preserved / null exchange normalised / null ticker skipped / malformed row skipped / upsert idempotency / empty data / missing fields key / missing single field / field reordering / empty body raises).
  - `tests/test_daily_cik_refresh_sibling_enrichments.py` — 6 integration tests (sibling enrichments fire on 304 / hash-unchanged / full-upsert paths × Stage 6 fail-soft / Stage 7 fail-soft / both fail-soft).
  - `tests/test_daily_cik_refresh_scope.py` — added `_patch_sibling_enrichments` static method + applied to 2 non-raising tests (prevents live SEC fetch after T3 lands).
  - `tests/test_fetch_document_text_callers.py` — 2 new allow-list entries (`app/services/exchange_directory.py` + `tests/test_exchange_directory.py`) per #453 contract.
  - `tests/fixtures/ebull_test_db.py::_PLANNER_TABLES` — added `cik_refresh_exchange_directory` for cross-test cleanup.
- Scope discoveries handled in-scope:
  - **Cohort observation (empirical 2026-05-17):** `company_tickers_exchange.json` shares the same row cohort COUNT as `company_tickers.json` (10,353) but is **ticker-grain not CIK-grain** — 7,996 unique CIKs / 1,446 multi-ticker CIKs. Plan's pre-cohort framing of "closes pink-sheet/OTC/foreign-without-ADR cohort gap" was empirically wrong; basic file already includes pink-sheet/OTC CIKs. The real value-add is the `(ticker, exchange)` mapping for preferred series (BAC=17 variants, JPM=9, MS=10), share-class siblings (GOOG/GOOGL), and ADR + OTC siblings (BABA/BABAF/BBAAY). Spec §1 / matrix §4 / sec-edgar.md §1 all corrected.
  - **MF Stage 6 latent skip fixed.** Pre-G8, Stage 6 MF refresh only fired on the full-upsert branch (304 / hash-unchanged early returns silently skipped it). The restructure makes Stage 6 + Stage 7 fire on every `daily_cik_refresh` invocation. Bootstrap-side authority remains `mf_directory_sync` (S25 #1174); this PR only fixes the daily-cron drift-heal path.
  - **PK granularity correction (Codex 1a round 2 HIGH 2).** Initial design was `PRIMARY KEY (cik)`; corrected to `(cik, ticker)` after re-counting the live payload showed 1,446 multi-ticker CIKs. A `(cik)`-only PK would have collapsed ~2,357 rows on every refresh.
  - **`fetch_document_text` allow-list update** added in-scope per #453 contract.
  - **Existing `test_daily_cik_refresh_scope.py` patched** to monkeypatch sibling refreshes — prevents live SEC fetch on the 2 non-raising tests that drive the real `daily_cik_refresh` after T3 lands (Codex 1b round-2 §3).
- Matrix delta:
  - `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 Stage 6 `cik_refresh` row — appended "+ Stage 7 exchange directory (G8, 2026-05-17)".
  - §4 reference endpoints row `www.sec.gov/files/company_tickers_exchange.json` — `❌ GAP` → `✅ WIRED 2026-05-17 (G8)` with full provenance.
  - §7 gap register G8 row — `OPEN (low)` → `✅ CLOSED 2026-05-17`.
  - `.claude/skills/data-sources/sec-edgar.md` §1 "Coverage gap" paragraph — rewritten with empirical ticker-grain correction + Stage 6/7 wiring map.
- Codex iteration counts:
  - Codex 1a (spec): 4 rounds to CLEAN — round-1 HIGH (early-return skip), HIGH (PK granularity), MED (txn ordering), MED (stale rows), MED (per-field tolerance), LOW (planner_tables), LOW (raw-payload-sink wording); round-2 HIGH (txn ordering wording precision); round-3 (allow-list update); round-4 CLEAN.
  - Codex 1b (plan): 3 rounds to CLEAN — round-1 HIGH (DAG dep T3→T2), HIGH (db_url monkeypatch), HIGH (FK seed), MED (stub-must-write), MED (non-string field guard), LOW (_PLANNER_TABLES sort); round-2 §1-3 (fixture names + seed helpers + scope-test live-fetch risk); round-3 CLEAN.
- ETL clauses #8-#12: Most N/A (reference-table snapshot, no per-instrument figure changes). Clause 9 spot-check: AAPL / GME / MSFT / JPM / HD against SEC live `company_tickers_exchange.json` — verification recorded in PR body.
- **Prevention-log candidate (extract on review):** "Re-count empirical cohort BEFORE finalising PK granularity for snapshot tables — same-count-row sets can still be many-to-one along the dimension you're keying on." Spec §1 documents the trap; if Codex / bot cites a sibling entry it ALREADY_COVERED, else EXTRACTED on first review round.
- Spec: `docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md` (v3 CLEAN).
- Plan: `docs/superpowers/plans/2026-05-17-g8-company-tickers-exchange-directory-plan.md` (v3 CLEAN).

## 6. Definition of done — for the whole plan

Plan is COMPLETE when:

1. All phases 1-8 merged.
2. `.claude/skills/data-engineer/etl-endpoint-coverage.md` matrix shows zero `OPEN` gaps in §7.
3. `[[us-source-coverage]]` memory caveat removed.
4. Operator confirms bootstrap reaches `status='complete'` AND next FULL orchestrator fire lands all db-lane + sec_rate-lane targets `success`.
5. `[[legacy-cron-retirement]]` pre-condition MET.

At that point: US ETL is "done, dusted, complete." Operator monitors daily steady-state via existing dashboards. Unexpected issues become anomalies, not known-bug churn.
