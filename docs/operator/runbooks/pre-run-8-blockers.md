# Pre-Run-#8 BLOCKERs vs genuinely-deferrable

**Operator concern (verbatim, 2026-05-24):** "I don't want to waste a run if we know there are gaps still, the test should be testing everything as if its done, complete, refined, never need to return back to it."

**Reframe:** "open residuals AFTER operator drive" was the wrong framing. Anything operator might trip over mid-Run-#8 = BLOCKER. Anything genuinely independent + monitored = deferrable.

This doc is the single source of truth for that classification. Maintained as part of the per-source spec contract (`docs/etl/sources/README.md`).

---

## A — Pre-Run-#8 BLOCKERs (must close before `--apply`)

| # | Concern | Status | Action |
|---|---|---|---|
| A1 | sql/174 + sql/175 partition extensions applied to dev | ✅ done 2026-05-24 (PR #1314 `e0f583d`) | none |
| A2 | XBRL parser-bug junk in financial_facts_raw_default cleaned | ✅ done — 48 rows deleted via baked-in cleanup in sql/175 | none |
| A3 | sec_n_cen scheduling decision | ✅ tracked at #1303 (pre-existing); #1313 closed as DUPLICATE of #1303 | decide post-Run-#8; current state is operator-run-once classifier — not a Run #8 blocker IF operator confirms acceptance |
| A4 | uvicorn `--reload` dev backend stuck | ✅ root-cause diagnosed: reload watcher has no worker child; needs restart | operator: restart VS Code dev task |
| A5 | Per-source source→sink spec | ✅ done — 21 files at `docs/etl/sources/` + lint + smoke gates (PR #1315) | none |
| A6 | All sweep memo wrong-claim regressions corrected | ✅ done — treasury attribution + sec_n_cen dead-callers + Form 3 retention mix-up all corrected in per-source files | none |
| A7 | Operator runbook `docs/operator/runbooks/run-8-readiness.md` reflects current state | ✅ done — §0.5 "Read order" added (PM B-PM-1 fold); B6 contradiction reconciled (PM B-PM-2 fold) | none |
| A8 | Final-committee BLOCKERs folded | ✅ done — Stream-C envelope (Codex C-B1 + C-B2 + API B1) + ownership/FINRA partition extensions (DE BL-1 + BL-2) + skill text (Architect) + per-source §11 SQL (Operator OP-B1, in flight) | confirm sql/176 + sql/177 applied to dev (✅ 2026-05-24); confirm Stream-C contract tests pass (✅ 9 PASS) |

## B — Genuinely deferrable (post-Run-#8, monitored)

These are real work but operator-time-safe — Run #8 won't trip over them.

| # | Issue | Why deferrable | Acceptance criterion |
|---|---|---|---|
| B1 | #740 CUSIP backfill (19/16M resolved) | Codex's highest-ROI residual; new feature, not a regression; ownership rollup degrades gracefully on unresolved CUSIPs (cards show partial data + admin-page surfaces gaps) | resolver phased rollout proposal accepted |
| B2 | #1302 13F LEI column not parsed | Additive — column added by SEC 2023-01-03; not regressing existing data | LEI consumer requirements landed |
| B3 | #1304 Form 144 + SC 13E sources not wired | Net-new manifest sources; no current data lives there | scope ticket landed |
| B4 | #1305 bulk-window depths short (13F=4q / N-PORT=4q / insider=8q) | Default cohort coverage; deep history exists via per-CIK overflow walker (Layer 4) | none until operator hits a depth-related gap |
| B5 | #1274 ingest_all_active_filers serial bottleneck | Perf — under-utilizes SEC 10 req/s budget by ~10×. Bootstrap wall-clock affected, but completes correctly | post-Run-#8 perf sweep |
| B6 | #1293 candle_refresh S2 `rows_processed=0` | **`=0` not `NULL`** — distinct from the FAIL-LOUD case in `run-8-readiness.md` §3.3 (which is `NULL`, i.e. ingester forgot to populate the field — code bug). `=0` is the documented happy-path for weekend / market-closed boot when there's nothing to fetch. Reproduce + diagnose; tagged for ops-monitor | spurious-fail rate ≤ 1/week of non-trading-day boots; if higher, escalate to A-list |
| B11 | `assert_no_multixact_wraparound` false-positive risk on freshly-initialized dev DB | PM lens R3 brainstormed risk: `datminmxid=1` on a brand-new DB could trip the 80% threshold. Empirical check pre-`--apply` — operator runs probe manually + records result. If false-positives, escalate to A-list | empirical probe result captured in §0 post-test |
| B12 | `jq` one-liner escaping in run-8-readiness.md §4.2 | PM lens R1: shell escaping varies across `jq` versions. Operator's local `jq --version` documented before drive | `jq --version >= 1.6` confirmed |
| B13 | `var/runbooks/*.jsonl` retention unmanaged | PM lens R2; local-only (`/var/` gitignored), unbounded growth on long-running dev box. Post-Run-#8 ticket | log-rotation pattern in `app/runbooks/README.md` |
| B7 | #1270 exchanges seed table TRUNCATE wipe | Operator reseeds via admin tool post-bootstrap; documented in runbook | seed-on-bootstrap automation landed |
| B8 | sec_n_csr + sec_13dg fixture gaps (2 SKIP markers) | Test infrastructure debt; fixture files live in `.tmp/spike-918/` + inline test literals respectively | fixture extraction PR |
| B9 | CAVEMAN narration comments in `scripts/check_caller_owned_tx.py` | Simplify lens NICE-TO-HAVE; cosmetic | next time the file is touched |
| B10 | `check_caller_owned_tx.py` SyntaxError mislabel | Simplify lens NICE-TO-HAVE; rare path | next time the file is touched |

## C — Process gates that MUST run BEFORE operator drive

| # | Gate | Owner |
|---|---|---|
| C1 | All PRs merged + main is up to date | ✅ pull main; verify no pending follow-up branches |
| C2 | Fresh 8-lens committee on merged main = clean bill | dispatch via `committee-review` skill |
| C3 | `bash scripts/check_etl_source_docs.sh` clean | ✅ part of pre-push |
| C4 | `uv run pytest tests/smoke/` clean | ✅ pre-push |
| C5 | `app/runbooks/safety.assert_no_multixact_wraparound` probe clean on dev DB | runs inside `--apply` first gate |
| C6 | `app/runbooks/safety.assert_dev_db_name_in_url` matches dev | runs inside `--apply` first gate |
| C7 | jobs service stopped (`systemctl stop ebull-jobs`) | operator |
| C8 | `/system/postgres-health` green | operator runs `curl` |

## D — Run-#8 success criterion

Operator can declare Run #8 done when ALL of:

1. `stream_a_run_8_verify --apply` exits 0
2. `stream_a_stream_c_gate` envelope `accepted=true`
3. JSON envelope posted as comment on #1233
4. Smoke against AAPL / GME / MSFT / JPM / HD: ownership rollup endpoints render expected figures
5. `/system/postgres-health` still green post-drive
6. **No B-list item escalated to A-list during the drive.** Testable rule (PM I-PM-1 polish fold): if `run-8-readiness.md` §3.3 FAIL-LOUD fires during the drive AND the hit corresponds to a B-list item in §B of THIS doc, **promote the B item to A + STOP** (do not proceed to gate or close-out). If the FAIL-LOUD hit corresponds to a brand-new finding (no B-list match), file a new ticket + classify A or B in §B before resuming.

If any A-list item re-opens during Run #8 → cancel + diagnose; do not declare done.

## E — Maintenance

Whenever a new ticket lands referencing #1233 or any ETL source:
- Classify A or B in this doc
- A-list items block the next Run cycle
- B-list items go to backlog with acceptance criterion

Cross-references:
- `docs/operator/runbooks/run-8-readiness.md` — operator step-by-step
- `docs/etl/sources/README.md` — per-source contract
- `.claude/skills/data-engineer/etl-source-to-sink-template.md` — workflow for adding sources
