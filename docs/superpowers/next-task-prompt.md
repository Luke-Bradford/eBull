# Next-session handover prompt — Phase 6 PR 12 (#916 RegSHO daily short volume)

Paste the block below verbatim into the next session. It is self-contained — no prior conversation context required.

---

```
Pick up Phase 6 PR 12 of docs/superpowers/plans/2026-05-17-us-etl-completion.md
(US ETL completion plan, autonomous-execution contract per §1).

PHASE 6 PR 12 SCOPE — FINRA RegSHO daily short volume ingest (#916):

- Sibling to #915 (PR 11 #1207 merged previous session). Same `finra` lane;
  different cadence (daily EOD ~6pm ET vs bimonthly); different schema +
  table; 6 prefixes per day.
- Endpoint shape (empirically verified 2026-05-18 in #915 spike §4):
    Pattern: https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt
    Prefixes: CNMS, FNQC, FNRA, FNSQ, FNYX, FORF (reporting facilities).
    Format: pipe-delimited TXT.
    Header (6 cols): Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
    Auth: anonymous CDN.
- New schema (DOES NOT reuse #915 tables — different shape):
    sql/153_finra_regsho_daily.sql — `finra_regsho_daily_observations`
    PARTITION BY RANGE (trade_date) quarterly buckets matching the #915
    pattern. PK `(instrument_id, trade_date, market, source_document_id)`.
    Cols: short_volume, short_exempt_volume, total_volume, market,
    plus provenance block (source='finra_regsho', source_document_id=
    f"{prefix}_{YYYYMMDD}", source_url, filed_at, period_end=trade_date,
    known_from, ingest_run_id).
    No `_current` snapshot — daily file is the snapshot.
- Provider extension OR sibling:
    Option A — extend `app/providers/implementations/finra_short_interest.py`
      with `fetch_regsho_daily_file(trade_date, prefix)`. Shared `finra` lane;
      shared rate-limit clock + lock.
    Option B — new `app/providers/implementations/finra_regsho.py`.
    Lean toward A unless the URL builder gets cramped. Spike decides.
- Service: `app/services/finra_regsho_ingest.py` mirrors
  `finra_short_interest_ingest.py` shape — header validation, row-shape
  validation, symbol-norm via `normalise_symbol` from sibling module,
  preloaded resolver reused. Service emits SQL only; JOB owns
  `with conn.transaction():` (HARD invariant from PR 11 Codex 1b r1
  HIGH 2 — see project_915 memory note).
- ScheduledJob: `app/jobs/finra_regsho_daily_refresh.py` —
  cadence=Cadence.daily(hour=23, minute=0) UTC (~6pm ET + 1h buffer).
  Lane=`finra`. Prerequisite=`_bootstrap_complete`.
  Per fire: for trade_date in window: for prefix in 6: store_raw +
  ingest_settlement_file (one txn per prefix; allows partial-success
  isolation per prefix not per file).
  Revision-window discipline: re-fetch most-recent 2 trade dates × 6
  prefixes regardless of manifest status (FINRA backdates corrections
  to RegSHO daily files within 1-2 cycles, same shape as bimonthly).
- Manifest source: ADD `finra_regsho_daily` to `sec_filing_manifest.source`
  CHECK enum (sql/118) via new migration. Sibling subject_type
  `finra_universe` reused; subject_id `FINRA_REGSHO` (new singleton).
- Manifest parser: synth no-op per sec_xbrl_facts (G7) / finra_short_interest
  (G6) precedent. parser_version=`finra-regsho-daily-v1`.
- Tests: provider + service + refresh + scheduler-wiring + manifest-parser
  + layer123 + universal_gate carve-out + fetch-doc-text-callers allow-list.
  Real fixture: fetch one live CNMS file (~50 KB), grep panel symbols,
  ship verbatim. Defects fixture: hand-curated truncated + malformed-int
  + blank-symbol + ambiguous-collapse.

FIRST ACTIONS:

1. Read CLAUDE.md working order. Verify PR #1207 merged (Phase 6 PR 11).
   `gh pr view 1207 --json mergedAt,mergeCommit`. If not merged: STOP,
   ask operator (PR 12 must land AFTER PR 11).
2. Read docs/settled-decisions.md for any RegSHO / short-volume decisions
   (expected: none).
3. Read docs/review-prevention-log.md for any new entries since 2026-05-18
   (PR 11 may have extracted lessons).
4. Read .claude/skills/data-sources/finra.md (Phase 6 PR 11 source-of-truth
   skill) + .claude/skills/data-engineer/etl-endpoint-coverage.md §2
   `finra_short_interest` row.
5. Read memory project_915_finra_bimonthly_short_interest.md for the
   transaction-ownership + freshness-seed lessons + the architectural
   patterns to reuse verbatim.
6. Read gh issue view 916 in full.

DESIGN STEPS (follow CLAUDE.md working order verbatim):

1. Branch: feature/916-finra-regsho-daily-short-volume.
2. Spike doc at docs/superpowers/spikes/2026-05-NN-finra-regsho-daily-
   feasibility.md — verify one live CNMS file fetch + decode + first 10
   rows. Confirm shape of all 6 prefixes (do they ALL exist daily or are
   some sparse?). Confirm anonymous CDN + ~1 req/s polite budget still
   holds.
3. Spec at docs/superpowers/specs/2026-05-NN-finra-regsho-daily.md
   mirroring the Phase 6 PR 11 shape (which Codex iterated to CLEAN over
   4 rounds — reuse the SHAPE, refine the substance for the daily +
   multi-prefix mechanics).
4. Codex 1a on spec + Codex 1b on plan + Codex 2 pre-push. Non-negotiable
   per CLAUDE.md.
5. Implementation order (DAG from PR 11 plan §1):
   - T1: sql/153 (manifest enum extension) + sql/154 (regsho table
     + partitions).
   - T9: Add `finra_regsho_daily` to ManifestSource Literal + subject_id
     `FINRA_REGSHO` constant.
   - T2: Provider extension (or sibling module — spike decides).
   - T3: Service ingester.
   - T4: ScheduledJob body.
   - T5: scheduler constants + body shim.
   - T6: NO new Lane (reuse `finra`). NEW MANUAL_TRIGGER_JOB_SOURCES entry.
   - T8: Manifest parser synth no-op + register.
   - T11: Pristine + defects fixtures (CNMS variant).
   - T10: Full test suite mirroring PR 11.
   - T12-T14: Skill update + matrix update + memory update.

ETL DoD CLAUSES #8-#12:
- #8 Smoke: AAPL / GME / MSFT / JPM / HD against the most-recent trade
  date's CNMS file. ~10k symbols per file across all 6 prefixes; expect
  ~5/5 panel resolution.
- #9 Cross-source: GME most-recent CNMS short_volume vs nasdaq.com /
  marketbeat.com daily volume page. ±5% tolerance.
- #10 Backfill: REPL invocation
  `run_finra_regsho_daily_refresh(conn, backfill_window_days=N)` —
  ~252 trading days × 6 prefixes = 1,512 fetches at 1 req/s = ~25min
  for a 1-year backfill. v1 manual-trigger surface zero-param (same
  reasoning as PR 11 — `_adapt_zero_arg` discards params).
- #11 Operator-visible figure: no live endpoint in v1 (memo-overlay
  sparkline deferred per #915 closure framing). Verify via direct SQL:
  `SELECT * FROM finra_regsho_daily_observations WHERE instrument_id=...
  ORDER BY trade_date DESC LIMIT 10`.

NON-NEGOTIABLES (carried from PR 11):

- Per plan §1 autonomy contract: no operator signoff between Codex
  iterations; no new tech-debt tickets; drive each PR to merge in one
  session.
- Service NEVER opens its own `with conn.transaction():` — silently
  commits at top-level (PR 11 Codex 1b r1 HIGH 2 lesson).
- Manual manifest UPSERT must call `seed_freshness_for_manifest_row`
  inline (PR 11 Codex 2 r1 HIGH 1 lesson).
- Per `feedback_post_push_cycle.md`: poll `gh pr view + gh pr checks`
  IMMEDIATELY after push.
- Per `feedback_pre_push_xdist_postgres_locks.md`: if local xdist pytest
  shows Postgres-recovery-mode failures, env-flake is documented;
  `--no-verify` justified if impacted-files clean + Codex green.
  Targeted pytest of impacted files + smoke test must pass.
- Per `feedback_pr_auto_close_required.md`: PR body MUST contain
  `Closes #916` on its own line (NOT `Refs`).

REFERENCES:

- Parent plan: docs/superpowers/plans/2026-05-17-us-etl-completion.md
  (Phase 6 PR 11 merged previous session; PR 12 is the next entry).
- Architectural sibling: PR #1207 (#915 bimonthly) — clone shape +
  refine for daily + multi-prefix mechanics.
- FINRA skill: .claude/skills/data-sources/finra.md — endpoint shape +
  symbol normalisation + cohort cliff already documented.
- Memory: [[us-source-coverage]] for matrix + ticket index;
  [[915-finra-bimonthly-short-interest]] for transaction-ownership
  + freshness-seed lessons; [[psycopg3-savepoint-commit]] +
  [[universal-gate-supersession]] for invariants.

OPERATOR FOLLOW-UPS PENDING (NOT blocking PR 12):

- Backend stability ticket #1208 (Phase 9 — Postgres tuning + test-fixture
  orphan sweep + financial_facts_raw partition). Separate session.
- N-PORT rebuild from PR #1205: `POST /jobs/sec_rebuild/run` body
  `{"source": "sec_n_port"}` — operator does whenever convenient.
- Bootstrap completion via admin UI "Retry failed" — 5 remaining stages
  from #1187 retry. Operator does in parallel.

If PR #1207 had any post-merge findings worth extracting to prevention-log,
process those FIRST before starting PR 12. Check
`docs/review-prevention-log.md` git log for entries dated 2026-05-18+.
```

---

## Session-1 close-out summary (for reference, not for paste)

- Phase 6 PR 11 (#915 FINRA bimonthly short interest) shipped to merge.
- New issue #1208 captures Phase 9 — backend stability + dev DB hygiene.
- Plan amended with Phase 9 entry.
- 98 tests passing locally; Codex 1a / 1b / 2 all CLEAN (12 rounds total across the three checkpoints); bot APPROVE + 2 NITPICKs FIXED in 356f14c.
- Operator follow-ups still pending: N-PORT rebuild (PR #1205), bootstrap completion via admin UI, Phase 9 backend stability (separate epic).
