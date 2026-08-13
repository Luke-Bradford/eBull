# Post-#1233 polish plan

**Status.** Drafted 2026-05-24 post-Run-#8-readiness final-committee fold (PR #1317 `7d72ddd`).
**Trigger.** Operator: "we should be polishing this till nothing comes back as an issue."

Cross-references:
- `docs/operator/runbooks/pre-run-8-blockers.md` — A/B/C/D/E lists (PR #1316)
- `docs/operator/runbooks/run-8-readiness.md` — operator step-by-step
- 8 lens memos under `~/.claude/projects/.../memory/project_final_committee_*_2026_05_24.md`

## Summary

**24 unique findings** total after dedupe across 8 lenses (49 raw → 24 unique; 11 BLOCKERs already folded in PR #1317 + #1316; 24 IMPORTANT/OBSERVATION remain).

Classification:
- **NOW-FIXABLE** in next polish PR: **8** items (~3-4 h total).
- **FILE-TICKET** for future session: **13** items (~22 h total).
- **EPIC-CLASS** phased rollout candidates: **3** items.

Already-deferred residuals (B-list in `pre-run-8-blockers.md`) are NOT re-listed here — see that file for B1-B13. Items below are *fresh* committee findings beyond the B-list.

## Now-fixable in next polish PR

Small (~30min) + isolated + low-risk. Fold in a single follow-up PR this session.

| # | Item | Lens(es) | Effort | Risk | Rationale |
|---|---|---|---|---|---|
| N1 | Grep-replace 6 stale "stage 21" / "stage 22" comments in `app/services/bootstrap_orchestrator.py` (lines 151, 157, 1134, 1139, 1152, 1155) → actual stages 22 / 23 | REV IMP-REV-2 | 5 min | None — comments only | Comment-only edit; pure mechanical rename; no behaviour change. Survives a future audit grep. |
| N2 | Tighten `docs/etl/sources/sec_13f_hr.md:20` "two dynamic-resolved params:" → "two dynamic-resolved params + one static audit label:" | REV OBS-REV-1 | 2 min | None — doc only | One-line clarity fix preventing reader-doubt loop. |
| N3 | Promote `app/runbooks/safety.py:277` literal `'0'` → `0::xid` cast (multixact probe) | DE IMP-2 | 5 min | None — equivalent semantics on PG14+ | Cosmetic SQL hygiene; replaces implicit string-to-xid cast with explicit. |
| N4 | Move `_FORM_MAPPING_EXEMPT` from `tests/smoke/test_etl_source_to_sink.py:120` into `app/services/sec_manifest.py` as module-level frozenset; import into test | ARCH IMP-2 | 15 min | LOW — pure relocation + import | Production becomes authoritative for "intentionally not form-discoverable" sources; test imports. Closes "test-only list" gap. |
| N5 | DRY `_parse_db_name_from_url` — replace `app/runbooks/stream_a_run_8_verify.py:97 _postgres_url()` + `:479 urlparse(...).path.lstrip('/')` with shared `safety.py` helper | ARCH IMP-1 | 15 min | LOW — pure refactor + import | Removes duplicate URL parsing; one chokepoint reduces drift. Existing tests cover the path. |
| N6 | Add 1-line under run-8-readiness.md `:90` J1 for macOS: "On macOS dev: stop the `python -m app.jobs` VS Code task (Cmd+Shift+P → Tasks: Terminate Running Task → ebull-jobs). Verify with `pgrep -f 'python -m app.jobs'` (empty = stopped)." | OP-I4 | 2 min | None — doc only | Operator already on darwin per CLAUDE.md context; OP-I6 fold only mentions `pkill`. |
| N7 | Tighten run-8-readiness.md §6 / §10 DoD item 6 ("No B-list item escalated to A-list") → add inline rule: "if §3.3 FAIL-LOUD fires → triage against B-list; if hit corresponds to a B item, promote to A and STOP." | PM I-PM-1 | 5 min | None — doc only | Reformulates meta-criterion to a testable rule. |
| N8 | Add SHA-rolling note to `run-8-readiness.md:72` E5: "Prints `91aa214` OR newer (current main = `7d72ddd` post-#1317)" → drop hard SHA pin; reword to "Stream A PR-D merge OR newer" | Codex IMP-3 | 2 min | None — doc only | Codex flagged stale SHA; auto-rolls forward with future polish PRs. |

**N1-N8 polish PR**: ~50 min wall clock, ~7 lines of code change + ~12 lines of doc. Single commit. Ship as `fix(#1233): post-final-committee polish — 8 small items`.

## File-as-ticket (next session)

Medium scope (~1-3h) + needs design discussion or isolated work. File as GitHub issues now; address future session.

| # | Item | Lens(es) | Effort | Suggested ticket title | Cross-ref |
|---|---|---|---|---|---|
| T1 | Phantom test function names in 7 per-source specs (`sec_13f_hr`, `sec_n_csr`, `sec_n_cen`, `sec_def14a`, `sec_n_port`, `finra_short_interest`, `finra_regsho_daily`). Each §12 names a non-existent function; actual tests are parametrized. | REV IMP-REV-1 | 1 h | "spec: rewrite per-source §12 to cite real parametrized test IDs" | `docs/etl/sources/README.md:121` |
| T2 | PRE 14A → sec_def14a semantic pollution. SEC distinguishes definitive vs preliminary proxy; current mapping (`sec_manifest.py:894`) folds both. Operator querying `WHERE source='sec_def14a'` gets PRE drafts mixed in. | DE IMP-1 | 2 h | "fix: split PRE 14A from sec_def14a OR parser-gate filter form_type" | `app/services/manifest_parsers/def14a.py` |
| T3 | 6-K / Form D / S-1 / 424B5 intentionally unsupported but undocumented. Operator FAQ inevitable. | DE IMP-3 | 1 h | "docs: add `_INTENTIONALLY_UNSUPPORTED_FORMS` frozenset with one-line rationale per entry" | `app/services/sec_manifest.py:921-928` |
| T4 | Smoke gate does not assert observation/current table existence per source. Future PR drops a `_current` table silently while writer survives. | DE IMP-4 | 2 h | "test: add `test_manifest_source_has_observation_table` + `test_categories_match_ownership_writers` parametrized" | `tests/smoke/test_etl_source_to_sink.py` |
| T5 | Add §13 gotcha to per-source specs: "304 path counts as a stage success, not a noop — operator should expect attempt_count > 0 even on cold revisit." | API I1 | 1 h | "docs: per-source §13 — document 304-as-stage-success path" | `app/services/sec_per_cik_poll.py:163-200` |
| T6 | Retry-posture matrix consolidation in `docs/etl/sources/README.md`. Columns: `transient_backoff`, `tombstone_classes`, `benign_skip_status`. | API I2 | 30 min | "docs: retry-posture summary table in ETL sources README" | `docs/etl/sources/README.md` |
| T7 | Spec language: drop "multi-page payloads" from §11.5 of 13F + NPORT specs; pagination only true for sidecar overflow walks. | API I3 | 30 min | "docs: trim 'pagination' language from 13F + NPORT specs" | `docs/etl/sources/sec_13f_hr.md`, `sec_n_port.md` |
| T8 | `/system/postgres-health` 401-not-503 (Codex 2 HIGH DEFERRED from #1208). Refactor `app/api/auth.py:125` to lazy-resolve `Depends(get_conn)` so /system/* endpoints return 503 when PG itself is down. | OP-I1 | 2 h | "fix: /system/* endpoints return 503 not 401 when PG down (Codex DEFERRED from #1208)" | `app/api/auth.py:97,128`, `app/api/system.py:70-73` |
| T9 | Move `--wait-for-jobs-sec` default from 600 → 1800 in `DEFAULT_WAIT_FOR_JOBS_SEC` constant. Operator-friendly default removes mid-run trap. | OP-I2 | 30 min | "fix: raise DEFAULT_WAIT_FOR_JOBS_SEC to 1800 (3× safety margin for operator walk-away)" | `app/runbooks/safety.py:178`, `stream_a_run_8_verify.py:89` |
| T10 | `var/runbooks/*.jsonl` retention. Either add `find var/runbooks -mtime +30 -delete` to runbook close-out, OR add `_rotate_log_files` to `_write_log_jsonl`. | OP-I3, PM R2 | 1 h | "tech-debt: rotate var/runbooks/*.jsonl after 30 days" | `app/runbooks/stream_a_run_8_verify.py:90,381` |
| T11 | Mirror 14 of 16 pre-push lint scripts into `.github/workflows/ci.yml`. Currently only `check_caller_owned_tx.sh` + `check_etl_source_docs.sh` mirrored (lines 46, 55). `--no-verify` push of (e.g.) Form 4 chokepoint regression lands green at CI. | TestEng IM-3 | 2 h | "ci: mirror remaining 14 pre-push lint scripts into ci.yml (close --no-verify gap)" | `.github/workflows/ci.yml`, `.githooks/pre-push` |
| T12 | Template-version stamp in `docs/etl/sources/README.md §Template` + version-check in `scripts/check_etl_source_docs.sh`. If template adds a 14th required section, every per-source spec must bump version explicitly. | PM I-PM-2 | 1 h | "lint: per-source spec template version stamp + lint check" | `scripts/check_etl_source_docs.sh`, `docs/etl/sources/README.md` |
| T13 | Multixact stub API parity. `tests/runbooks/test_safety_multixact.py` `_StubConn` doesn't pin psycopg.Connection ABC. Future safety probe using `conn.cursor().execute(...).fetchmany()` wouldn't be caught. | TestEng IM-2 | 1 h | "test: assert multixact stub surface stays in sync with psycopg.Connection ABC" | `tests/runbooks/test_safety_multixact.py` |

**Total T1-T13:** ~22 h. Trickle in over 2-4 future sessions of 4-6 h each.

## Epic-class (phased rollout candidates)

Large + coupled + needs design. Not for next polish PR; not even individual tickets — these need spec docs first.

| # | Item | Lens(es) | Phases | Why-large |
|---|---|---|---|---|
| E1 | **CUSIP resolution SLO + fail-loud + /system/postgres-health surface** | DE IMP-5, Codex IMP (CUSIP), pre-run-8-blockers B1 | (a) Resolver completeness audit — how many of 16M actually resolvable via OpenFIGI alone? (b) Denominator-aware SLO floor (target % + breach threshold). (c) Surface `cusip_resolution_coverage` in `/system/postgres-health` snapshot. (d) Fail-loud threshold in bootstrap stage 6 (`bootstrap_orchestrator.py:1074-1079`). (e) Operator runbook gotcha-table entry. | Codex called this highest-ROI post-Run-#8 item. New feature; affects 4+ surfaces (resolver lane, postgres-health, bootstrap stage exit codes, operator runbook). Needs cohort analysis before threshold can be picked. ETA 1-2 weeks of dispersed work. |
| E2 | **Spec-claim regression class — file:LINE drift gate** | TestEng IM-1, Codex OBS (memo-pile rot half-life) | (a) Standardise `app/path.py:LINE — `phrase`` shape across all per-source `## 6. Code references` sections. (b) Write `tests/lint/test_spec_code_references.py` that for each spec parses each `path:line — phrase` cite, dereferences the line, regex-matches phrase. (c) Add to CI + pre-push. (d) Backfill existing specs to conform to shape. (e) Promote to skill-level pattern. | Spans all 21 per-source specs. Skill text must be updated. Need to decide between line-number cites (drift-prone) vs symbol-name cites (refactor-stable). ~150 LOC + 21 spec edits + skill update. Drift-class fix; not a one-PR job. Codex flagged memo-pile rot half-life ~7-10 days as evidence. |
| E3 | **Source-registry / audit-completeness alignment** | Codex OBS ("source registry equals audit completeness"), ARCH IMP-2 (`_FORM_MAPPING_EXEMPT` placement), DE IMP-3 (unsupported forms) | (a) Audit: enumerate every real ETL stream type (manifest sources, ad-hoc, bulk references, caller-owned writers, bootstrap-only sidecars, scheduler-only post-run checks). (b) Decide unified registry model — single ENUM with category tag, or N-typed Literals. (c) Migrate `_AD_HOC_SOURCES` + `_BULK_REFERENCE_SOURCES` + `_FORM_MAPPING_EXEMPT` + `_INTENTIONALLY_UNSUPPORTED_FORMS` to a single Source table. (d) Per-source spec layer becomes authoritative for category. (e) Lint enforces 1:1 registry ↔ spec. | Codex flagged this implicit assumption: per-source registry treated as audit completeness, but the real system has 6+ stream types. N4 + T3 are minor patches inside this larger gap. Needs design before implementation; potentially 1-month rollout. |

## Sequencing

1. **NOW:** Ship N1-N8 as a single polish PR (`fix(#1233): post-final-committee polish — 8 small items`). ~50 min wall clock.
2. **NEXT:** File T1-T13 as 13 separate `gh issue create` invocations (commands at `/tmp/polish_tickets.sh`).
3. **WEEK 1:** Spec drafts for E1 (CUSIP SLO) — Codex's highest-ROI residual; affects rollup endpoints.
4. **WEEK 2:** Spec drafts for E2 (spec-claim drift gate) — earliest possible date before memo rot accelerates.
5. **MONTH 1:** Spec drafts for E3 (source-registry unification) — requires E2 in place first so spec is itself trustworthy.

Sequencing constraints:
- **T9 (raise wait-for-jobs default)** should land before next operator drive (Run #9).
- **T11 (CI mirror of lint scripts)** has no dependencies; can land any time.
- **E1 spec** depends on understanding which 19/16M are resolvable — that's an offline audit, not blocked by anything else.
- **E2 / E3** are independent of each other but E3 should come AFTER E2 to ensure the spec-driven registry source-of-truth is itself drift-protected.

## Will a fresh committee give clean bill after this plan executes?

**Honest answer: Mostly yes, with explicit caveats.**

After N1-N8 + T1-T13 ship:
- **Architect:** YES. The remaining ARCH findings (IMP-1 / IMP-2) are folded.
- **Reviewer:** YES. Phantom test names + stale stage comments + "two dynamic-resolved" all gone.
- **Data engineer:** **PARTIAL.** IMP-1 / IMP-3 / IMP-4 close. IMP-5 (CUSIP coverage) is E1-class — committee will still flag this as an IMPORTANT until E1 ships. **Honest expectation: DE lens will still surface 1 IMPORTANT about CUSIP.**
- **API contract:** YES.
- **Operator:** YES.
- **Test engineer:** **PARTIAL.** IM-2 / IM-3 close. IM-1 (spec-claim regression class) is E2-class — committee will still flag this as an IMPORTANT until E2 ships. **Honest expectation: TestEng lens will still surface 1 IMPORTANT about spec drift.**
- **PM:** YES.
- **Codex:** **PARTIAL.** CUSIP-SLO finding remains until E1 ships; source-registry boundary finding remains until E3 ships. **Honest expectation: Codex will still surface 1-2 IMPORTANT items about CUSIP + registry boundaries.**

**Probability of true clean-bill committee after N+T+E1+E2+E3 ship:** ~95%. The residual 5% is "fresh committee always finds *something* new" — the system grows under it.

**Probability of clean-bill after only N+T (no E):** ~70%. Committee will surface E1/E2/E3 as IMPORTANT but classify them correctly as design-class, not regression-class. That is the realistic state operator should accept for Run #8 + Run #9 cycles.

**Recommended definition of "done" for #1233 close:**
- N1-N8 shipped (polish PR).
- T8 + T9 shipped (operator-facing — /system/* status code + wait-for-jobs default).
- T1 + T6 shipped (doc-fidelity wins — phantom test names + retry matrix).
- E1 spec drafted (does not need to ship — just have the audit data + threshold proposed).
- Remaining T-items + E2 + E3 deferred with explicit acceptance criteria in `pre-run-8-blockers.md` B-list extension.

This gets the operator a true "no committee will surface a regression" state for Run #9, with the residual being acknowledged-design-debt rather than unclassified surprise.

## Ticket commands

See `/tmp/polish_tickets.sh` for ready-to-paste `gh issue create` invocations (T1-T13).
