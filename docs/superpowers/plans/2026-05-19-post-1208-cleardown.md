# Post-#1208 clear-down + bootstrap completion plan

> Created: **2026-05-19**, after #1208 closed (PR #1216 `c218deb`).
>
> Goal: land every ETL/processing-layer touch that affects ingest correctness BEFORE finishing the first-install bootstrap drain — so the drain picks up the fixes naturally and we don't pay a second re-ingest cost. Post-bootstrap, sweep up infra + tech-debt tickets that don't require a re-bootstrap to land.
>
> Driving constraint: bootstrap drain is expensive (hours of fetches under SEC's 10 req/s cap + the universe-wide rebuild it triggers). Every parser/schema fix landed AFTER bootstrap completes either (a) needs a targeted `sec_rebuild` re-ingest to take effect or (b) silently leaves contaminated rows in `financial_facts_raw` / `ownership_*_observations` / `filing_events`. **Order ETL work pre-bootstrap; everything else is fair game post-bootstrap.**

## 0. Bootstrap state as of 2026-05-19

```text
SELECT * FROM bootstrap_state;
-- id=1 | status=partial_error | last_run_id=3 | last_completed_at=2026-05-17 05:30:37 UTC
```

`partial_error` means some stages succeeded + others raised. Operator action (admin UI retry per failed stage) is required to drive the state to `complete`. That action lives in §C below; the §A + §B work hardens what the retry will actually do.

## 1. Triage frame — bucket every open issue against bootstrap-readiness

Pulled from `gh issue list --state open` on 2026-05-19. Issues NOT listed here are not on this plan's working set (chart-redesign UI follow-ups #920-#923, frontend-only tech-debt, etc. — fair-game post-bootstrap but not in this plan's critical path).

| Bucket | Reasoning | Issues |
|---|---|---|
| **A. Must land pre-bootstrap** | Contaminates the bootstrap drain itself, OR makes the drain time out. | #1218 (parser period_end junk), #1010 (13F cohort 8h timeout), #1136 (bootstrap state machine audit — only the items that gate this current drain) |
| **B. Should land pre-bootstrap** | Affects ingest correctness for the bootstrap drain's outputs. Landing post-bootstrap needs a targeted `sec_rebuild` to take effect. | #1102 (share-class CIK GOOG/BRK), #1094 (confirm #819 covers share-class redirect), #828 (ownership writers through historical CIK resolution), #899 (filed_at semantic mismatch insider observations), #1173 (CI grep guard external_identifiers is_primary). Light scope each; bundle into one or two PRs. |
| **C. Bootstrap completion (operator action)** | T9-POST. After §A + §B land, drive bootstrap_state.status to `complete` via admin UI retry. No code work for me. | T9-POST |
| **D. Post-bootstrap infra** | Doesn't touch ingest; safe any time. | #1217 (auth lazy DB-conn — #1208 Codex 2 HIGH DEFERRED), #1219 (VACUUM FULL runbook — #1208 §7.2), #859 (pytest CI duration), #832 (iter_drillthrough_summaries batch path), #987 (dark:bg drain skip-list), #990 (NOTIFY noise), #1005 (BootstrapPanel live-progress UI) |
| **E. Post-bootstrap ETL polish** | New ETL coverage; would need its own targeted sec_rebuild but doesn't contaminate the existing drain. | #917 (N-PORT mutual fund), #1013-#1015 (#1011 PRs 2-4 — skipped-form cleanup, raw-payload retention, metadata-only parsers), #966 (DEF 14A cross-source), #961 (ESOP overlay), #844 (#788 P5 DRS + restricted), #954 (13F rewash dedup mismatch), #953 (13F rewash stale observations on CUSIP fix), #817 (rewash cohort scan locking) |
| **F. Umbrellas / audits** | Multi-PR follow-up trackers, not single deliverables. | #935 (ETL foundation review umbrella — auto-closes as B + E land), #1136 (bootstrap audit — partial-close after §A) |

## 2. Phase A — Pre-bootstrap ETL critical (must land before T9-POST)

Three deliverables. Each follows the #1208-shape cadence: spike → spec → Codex 1a/1b → impl → Codex 2 → push → bot → merge. Spec docs live at `docs/superpowers/specs/2026-05-NN-<phase>-<short>.md`.

### Phase A.1 — #1218 XBRL parser out-of-window period_end

- **Symptom:** ingest emits `period_end='6016-06-30'` + `period_end='1850-01-01'` rows. Currently 42 rows in `financial_facts_raw_default` (post-Phase-3 partition); was 1055 pre-retention-sweep. Every fact-ingest cycle adds more.
- **Scope:** add validation at `app/services/sec_companyfacts_ingest.py` (and any sibling parser path) — reject rows where `period_end` is outside `[1900-01-01, 2099-12-31]`. Log + skip; never insert. Plus one-shot cleanup script to evict the existing 42 rows.
- **Why pre-bootstrap:** every bootstrap retry that re-runs the companyfacts stage adds new junk rows. Catching the bug in the parser eliminates the stream at source.
- **Estimate:** ~150 LOC. One PR.
- **Spec gate:** `docs/superpowers/specs/2026-05-NN-1218-parser-period-end.md` + Codex 1a/1b.

### Phase A.2 — #1010 13F sweep cohort bound — CLOSED 2026-05-19

**Merged:** PR #1222 / `240112e`.
**Spec:** `docs/superpowers/specs/2026-05-19-1010-13f-cohort-bound.md`.

- **Symptom:** `bootstrap_sec_13f_recent_sweep` walks 11,205 filers @ 23/min = ~8 h on first install. Most are sub-$100M managers filing empty 13F-NTs. Bootstrap times out + retries forever.
- **Scope (delivered):** added `last_13f_hr_at` column to `institutional_filers`; populated during `sec_13f_filer_directory_sync` by tracking 13F-HR / HR-A only (NT/NT-A excluded); `list_directory_filer_ciks(min_last_13f_hr_at=...)` filters cohort; bootstrap stage 21 dispatches `_PARAM_DYNAMIC_BOOTSTRAP_13F_HR_CUTOFF` resolved to UTC start-of-day `today() - 380d`. AUM cap deferred (not needed; recency alone landed acceptance).
- **Outcome:** cohort 11,205 → 8,718 (78% have backfill) → 8,681 (within 380d). Subsequent `sec_13f_filer_directory_sync` runs converge to form.idx ground truth (NT-only filers re-revealed as NULL).
- **Iteration:** Codex 1a (6 findings: NULLIF guard, `_upsert_filer` advance, UTC-midnight cutoff, backfill caveat, shared-cutoff justification, default ordering preserved), Codex 1b (4 follow-ups: exact-equality test, naive ISO UTC tag, retired-cron wording, boundary-test UTC midnight explicit), Codex 1c clean, Codex 2 MEDIUM (`date.today()` is local TZ → `datetime.now(tz=UTC).date()`). Bot APPROVE round 1 + APPROVE round 2 after NITPICK fix (`__import__("datetime").timezone.utc` → `from datetime import UTC`).

### Phase A.3 — #1136 bootstrap state-machine audit (scope subset) — CLOSED 2026-05-19

**Merged:** PR #1223 / `bd6a0fc`.
**Spec:** `docs/superpowers/specs/2026-05-19-1136-bootstrap-state-audit.md`.

- **Symptom:** `bootstrap_state` was in `partial_error` post-2026-05-17 run_id=3 attempt.
- **Scope (delivered):**
  - Audit of run_id=3 failure classification (spec §3): S16 stuck `pending` (no fix possible inside scope); S17-S20 + S22 lock contention (retries clean); S21 stale `job_name` after PR1c rename (FIXED by dispatch hardening); S23/S24 blocked on bulk-provider `rows_processed=NULL` (separate follow-up #1225).
  - New `GET /system/bootstrap-status` lean operator readout — per-stage `(status, last_error, retryable, attempt_count, completed_at)` + summary + `retry_available` / `retry_blocked_reason`. Pins on `bootstrap_state.last_run_id`. Surfaces stale-pointer case honestly.
  - `compute_retryable_view` pure function mirroring `reset_failed_stages_for_retry` SQL exactly (lane MIN + `stage_order >= min`; own status irrelevant).
  - Dispatch hardening — orchestrator resolves `job_name` from `_BOOTSTRAP_STAGE_SPECS` by `stage_key`, fails closed for trimmed stage_keys. `effective_job_name` flows through `_RunnableStage` so `validate_job_params` + `_run_one_stage` + row-resolution see the canonical name. DB column stays as audit snapshot.
- **Outcome:** Codex 1a (6 findings) + 1b (5 findings) + 1c (1 MEDIUM) on spec; Codex 2 (1 NITPICK) on diff. Bot APPROVE on first push. CI green. mergeStateStatus=CLEAN.
- **Follow-up tickets filed:** #1224 (S16 stuck-pending root cause), #1225 (bulk ingesters `rows_processed=NULL` defeating strict-gate floor), #1226 (lock-contention UX papercut). #1136 umbrella stays OPEN for #1041/#649/#1064 + above.

## 3. Phase B — Pre-bootstrap ETL nice-to-have (bundle into one PR if time)

Bundle the five tickets below into a single "ETL hygiene" PR (or two if scope sprawls). Each is small + bootstrap-correctness-relevant.

| Issue | One-line scope | LOC |
|---|---|---|
| #1102 | Relax CIK uniqueness on `external_identifiers` so GOOG/GOOGL + BRK.A/B can coexist as distinct instruments with the same parent CIK | ~80 |
| #1094 | Confirm #819 fully covers share-class canonical-instrument-redirect; close-or-extend | ~50 |
| #828 | Route ownership writers (insider + 13F + 13D/G + Form 144) through `historical_cik_resolver` so accessions for prior-name CIKs route to the current instrument | ~150 |
| #899 | Insider-observations `filed_at` semantic mismatch (legacy + write-through diverge) — pick one convention, migrate the other | ~120 |
| #1173 | CI grep guard: every `INSERT INTO external_identifiers` must include `is_primary` to prevent the post-2026-04 outage shape | ~40 |

**Why pre-bootstrap:** each of these affects the SHAPE of rows written during the drain. Landing post-bootstrap means re-ingest for one or more sources to backfill the corrected shape. Bundling pre-bootstrap = one drain pays for everything.

**Decision point at end of Phase A:** if A.1-A.3 took the budget, defer Phase B to post-bootstrap + accept a `sec_rebuild` cycle per item later. Phase B is optional, not load-bearing.

**Spec gate:** lighter — one spec covering all five with per-issue §s. Codex 1a only (no 1b) since each item is small. Codex 2 on the diff.

## 4. Phase C — Bootstrap completion (T9-POST, operator action)

After Phase A (and ideally B) merges to main:

1. Restart `python -m app.main` so the new migrations + parser patches are live.
2. Operator opens `/admin/processes` (or whichever surface drives the bootstrap state machine).
3. For each failed stage in `bootstrap_stages` (revealed by the new `/system/bootstrap-status` endpoint from A.3), click "retry."
4. Monitor `/system/bootstrap-status` until every stage = `complete`.
5. `bootstrap_state.status` flips to `complete`.
6. `_bootstrap_complete` gate unblocks every gated ScheduledJob (`fundamentals_sync`, `financial_facts_retention_sweep`, etc.).

If a stage continues to fail post-A/B: that's a NEW bug; file a fresh ticket, do NOT silently retry.

## 5. Phase D — Post-bootstrap infra (any-time, sequence to operator preference)

| Issue | Why | LOC |
|---|---|---|
| #1217 | #1208 Codex 2 HIGH DEFERRED — `/system/*` returns 401 not 503 when PG is down. Cross-cutting auth refactor. | ~200 |
| #1219 | VACUUM FULL `financial_facts_raw` runbook + optional ScheduledJob automation. ~25 GB reclaim. | ~150 |
| #1005 | `BootstrapPanel` live-progress UI — wire `units_done/expected_units` mid-stage. Frontend. | ~250 |
| #859 | pytest CI duration 13-20 min investigation — perf, not correctness | ~? |
| #832 | Batch query path for `iter_drillthrough_summaries` — perf | ~100 |
| #987 | Dark-mode skip-list drain — frontend tech-debt | ~80 |
| #990 | NOTIFY noise on orphan-only stale-cipher revoke — backend tech-debt | ~50 |

Bundle #1217 + #1219 into a single "post-#1208 observability tail" PR. #1005 is its own frontend PR. Rest can land independently.

## 6. Phase E — Post-bootstrap ETL polish (targeted sec_rebuild each)

Each adds new ETL coverage; each needs its own targeted `sec_rebuild` to backfill but doesn't contaminate the existing drain. Order by user value:

1. **#917** N-PORT mutual fund ingest + `ownership_funds` schema — Phase 3 of the ownership decomposition epic.
2. **#1013/#1014/#1015** (#1011 PR2/PR3/PR4) — skipped-form `filing_events` cleanup, raw-payload retention + SHA-256 reproducibility, metadata-only-form parsers.
3. **#966** DEF 14A cross-source augment of insiders + blockholders.
4. **#961** ESOP overlay for funds slice.
5. **#844** (#788 P5) DRS + restricted disclosure extraction.
6. **#954 / #953 / #817** 13F rewash hygiene (edge cases — only fix if a real divergence appears).

## 7. Phase F — Umbrella close-outs (auto-resolve)

- **#935** ETL foundation review umbrella: closes once Phase A + Phase B land. Each delivers one or more items it tracks.
- **#1136** bootstrap audit: partial-close after Phase A.3; remaining items stay open as their own tickets.

## 8. Suggested per-PR cadence (mirrors #1208)

Every PR follows the proven shape:

1. Branch `feature/<issue>-short-description` off main.
2. Spike + spec doc at `docs/superpowers/specs/2026-05-NN-<phase>-<short>.md`.
3. Codex 1a on spec → revise → Codex 1b on revised spec → revise.
4. Implementation in spec order. Lint + format + pyright + targeted pytest pass before push.
5. Codex 2 on diff vs main.
6. Push + immediate bot poll + CI poll.
7. Bot findings → FIXED/DEFERRED/REBUTTED triage; PREVENTION → EXTRACTED.
8. Merge after APPROVE on latest commit + CI green + mergeStateStatus CLEAN.
9. Update auto-memory + close-out comment on the issue with merge SHA.

## 9. Phase ordering decision tree

```
Start of next session
  ↓
A.1 #1218 parser fix     ← always first; cheapest, every ingest needs it
  ↓
A.2 #1010 13F cohort     ← unblocks the bootstrap drain timing
  ↓
A.3 #1136 bootstrap audit ← gives operator T9-POST surface
  ↓
Budget left?
  ├─ Yes → Phase B bundle (#1102 #1094 #828 #899 #1173)
  └─ No  → Skip to C (Phase B items get sec_rebuild cycles later)
  ↓
C. Operator T9-POST (you drive the admin UI; I'm not in the loop)
  ↓
Bootstrap completes
  ↓
D + E + F in any order, by operator preference / latest-pain
```

## 10. Out of scope for this plan

- **#1064 admin control hub umbrella** — closed 2026-05-10; the open follow-ups (#819, #1082, #1092, #1093, #1094, #1114, #1117) are admin-UI tech-debt and orthogonal to bootstrap. Pull individually as paper-cuts surface.
- **#585 chart redesign umbrella** — UI work; deliberately deferred to keep this plan ETL-focused. Open items: #608, #671.
- **Frontend admin observability tiles** — beyond `/system/postgres-health` + the BootstrapPanel live-progress (#1005), wider admin dashboards are a UI-revisit epic, not in this plan.

## 11. Acceptance for the whole plan

When §A → §C complete:

1. `bootstrap_state.status = 'complete'` on dev DB.
2. `financial_facts_raw_default` rows < 100 (parser fix in A.1 stopped the bleed; cleanup script evicted the existing 42).
3. `bootstrap_sec_13f_recent_sweep` completes in <3 h wall-clock from a fresh DB (A.2 cohort bound).
4. `GET /system/bootstrap-status` returns 200 with every stage = `complete` (A.3 observability).
5. Every gated ScheduledJob (`fundamentals_sync`, `financial_facts_retention_sweep`, etc.) starts firing on cadence — `prerequisite=_bootstrap_complete` no longer skips them.

When §D + §E land (any time):

6. `pg_database_size('ebull') < 10 GB` (Phase D #1219 VACUUM FULL).
7. `/system/postgres-health` returns 503 (not 401) when PG is unreachable (Phase D #1217).
8. N-PORT data populated (Phase E #917).

## 12. Handover for next session

```
Pick up Phase C of docs/superpowers/plans/2026-05-19-post-1208-cleardown.md
(post-#1208 clear-down + bootstrap completion). This is the
operator-action phase — no code, admin UI clicks + endpoint
monitoring. If you (Claude) are reading this without an operator
ready to drive, surface the prerequisite to the user before doing
anything else.

PHASE C SCOPE — T9-POST bootstrap completion (operator action):

1. Confirm `python -m app.main` + `python -m app.jobs` are running
   the latest code (post-PR #1223 / `bd6a0fc`). The dispatch
   hardening from A.3 only takes effect on a restarted jobs process.
2. Hit `GET /system/bootstrap-status`. Identify retryable=True
   stages (S17-S24 against current run_id=3 per spec § 4.2 worked
   example).
3. Operator clicks Retry in the admin UI — calls
   `POST /system/bootstrap/retry-failed`. Dispatcher walks the
   sec_rate + db lanes from their respective MIN(failed_order).
4. Monitor `/system/bootstrap-status` until every stage = success.
5. S16 (`sec_def14a_bootstrap`, stuck pending) will NOT be reached
   by retry-failed (order 16 < sec_rate min-failed 17). #1224 is
   the followup; for T9-POST acceptance an operator can manually
   advance via `mark_stage_success` or accept partial via
   `POST /system/bootstrap/mark-complete`.
6. S23/S24 will re-block until #1225 (bulk ingesters writing
   rows_processed) lands. They DO retry (retryable=True) but the
   cap-eval re-marks blocked on the same NULL row counts. Operator
   options: file follow-up + `mark-complete`, OR wait for #1225.
7. `bootstrap_state.status = 'complete'` either via natural drain
   (if #1224 + #1225 land first) or via `mark-complete` escape
   hatch.

FIRST ACTIONS:

1. Read CLAUDE.md working order.
2. Confirm latest code is running:
   - `git rev-parse HEAD` should match origin/main.
   - `curl /system/bootstrap-status` should respond 200 with the
     new shape (`retry_available`, per-stage `retryable`).
3. Spike: `curl /system/bootstrap-status | jq '.retry_available,
   .retry_blocked_reason, [.stages[] | select(.retryable)] |
   length'` — expect retry_available=true and retryable count > 0.
4. Drive the operator through steps 2-7 above. Surface decisions
   that need their judgement (e.g. "S16 stuck — manual advance or
   mark-complete?").

NON-NEGOTIABLES (carried throughout the plan):
- Per CLAUDE.md: never close positions / never invoke destructive
  ops without operator confirmation. `mark-complete` is an escape
  hatch and changes the bootstrap gate semantics — confirm with
  operator before invoking.
- Per feedback_no_fake_polling: NEVER narrate "monitoring" without
  invoking a real curl/gh/Read this turn.

If T9-POST lands clean, next sessions = Phase D + E + F in any
order per operator preference. Phase D (#1217 auth lazy-conn,
#1219 VACUUM FULL runbook, etc.) and Phase E (#917 N-PORT, #1011
PRs, etc.) are now safe to land any time — bootstrap drain has
completed; ETL polish no longer contaminates the drain.
```

---

## 12.OLD. (archived) Phase A.1 handover

```
Pick up Phase A.1 of docs/superpowers/plans/2026-05-19-post-1208-cleardown.md
(post-#1208 clear-down + bootstrap completion, autonomous-execution
contract per #1208 plan §1 — no operator signoff between Codex
iterations, drive PR to merge in one session).

PHASE A.1 SCOPE — #1218 XBRL parser out-of-window period_end:

- Identify the parser path emitting `period_end='6016-06-30'` /
  pre-1900 dates (likely `app/services/sec_companyfacts_ingest.py`
  + `app/providers/implementations/sec_fundamentals.py`).
- Add validation: reject rows where `period_end` is outside
  `[1900-01-01, 2099-12-31]`. Log warning with source accession.
- Cleanup script (one-shot, not a migration) evicts the existing
  ~42 rows from `financial_facts_raw_default`.
- Regression test seeds a malformed XBRL fact with year=6016 and
  asserts the parser rejects + logs, doesn't insert.
- Update `.claude/skills/data-sources/sec-edgar.md` with the gotcha.

FIRST ACTIONS:

1. Read CLAUDE.md working order. Confirm #1218 still OPEN.
2. Read sql/156 to confirm DEFAULT partition + alarm threshold are
   wired (Phase 3 #1208).
3. Read app/services/sec_companyfacts_ingest.py end-to-end.
4. Spike: dump the 42 existing junk rows
   (`SELECT period_end, accession_number, concept FROM
    financial_facts_raw_default ORDER BY period_end LIMIT 50;`)
   to identify the source XBRL context shape.

DESIGN STEPS:
- Mirror #1208 cadence: spike → spec → Codex 1a/1b → impl → Codex 2
  → push → bot → merge.
- Spec at docs/superpowers/specs/2026-05-NN-1218-parser-period-end.md.
- Implementation: parser-side validation, cleanup script,
  regression test.

NON-NEGOTIABLES (carried from #1208):
- Per CLAUDE.md working order: read settled-decisions +
  prevention-log + relevant skills BEFORE writing code.
- Per feedback_post_push_cycle.md: poll gh pr view + gh pr checks
  IMMEDIATELY after push.
- Per feedback_pr_auto_close_required.md: PR body MUST contain
  `Closes #1218` on its own line.

If Phase A.1 lands clean, next session = Phase A.2 (#1010 13F
cohort bound). Handover template at this plan §12 is re-used.
```

---

**TL;DR:** Three pre-bootstrap PRs (parser fix, 13F cohort, bootstrap observability) → operator drives T9-POST drain → post-bootstrap, sweep up the parking lot in any order. Plan keeps every ETL touch upstream of the drain so we pay for the drain once, not twice.
