# ETL functional-spec rollout — getting the bootstrap nailed

**Status:** draft (2026-05-23). Awaiting adversarial review + Codex sign-off before execution.
**Owner:** project owner; AI agent (me) running it end-to-end.
**Why now:** Run #7 hit 128 min vs 60-min target; my receipts memo had 4 material errors (Codex + reviewer agents corrected); we have no canonical per-source ETL contract; bootstrap stages reuse daily-mode code paths instead of being bulk-native.

---

## 1. Context: why this got complicated

We have **44 ETL pipelines across 14 sources** (SEC EDGAR, FINRA, eToro, OpenFIGI, Frankfurter ECB, MF directory, exchange directory, and a handful of derived/internal sinks). They were bolted on incrementally. Outcomes:

1. **Three different transaction-boundary patterns coexist** (caller-wraps #915; implicit per-archive; hybrid flag). Same job-class behaves differently depending on author.
2. **Bootstrap stages reuse daily-mode jobs** as-is. `fundamentals_sync` (S25) is a daily omnibus that does audit + per-CIK HTTP + tier review; bootstrap re-runs all of that even when the bulk path already wrote `financial_facts_raw`. That alone is the 101-min "fundamentals_sync" wall-clock.
3. **No canonical per-source functional spec exists.** Coverage skills (`sec-edgar.md`, `finra.md`, `openfigi.md`) hold prose-y reference content, but the contract — exact field types, primary keys, watermark columns, conflict keys, bootstrap budgets — is implicit in code or scattered across implementation-spec PRs.
4. **Bootstrap critical path is `sec_rate` lane** (S14 + S16 = ~113 min serial after Codex/reviewer correction). My receipts memo summed lanes that run in parallel; the real arithmetic is `max(sec_rate=113, db=101) ≈ 113 min`.
5. **Several bootstrap stages duplicate work the bulk path already did** — S14 fetches `submissions.json` PRIMARY per CIK before checking `files[]`, although `submissions.zip` is already on disk. S25 Phase 1b runs per-CIK snapshot HTTP, although `financial_facts_raw` has the data.

The pattern: we never wrote down what each source IS, what its contract looks like, what bootstrap is allowed to do with it. So fixes get bespoke per-source, and the same mistakes recur.

## 2. Goal

Land a **uniform functional spec per (source, endpoint, sink)** so:

- Any agent / engineer changing a pipeline reads ONE document to understand the contract.
- Skills point at specs — they no longer guess from sampled code.
- Bootstrap fixes have a real spec to validate against (smoke-test panel, expected row count, performance budget).
- New sources can't be ad-hoc — they fill the template or they don't ship.

This unblocks the Tier 1 bootstrap fixes (next section) and prevents Run #N+1 from surfacing fresh undocumented behaviour.

## 3. Where things live

| Artifact | Path | Purpose |
|---------|------|--------|
| This plan | `docs/superpowers/plans/2026-05-23-etl-functional-spec-rollout.md` | The rollout roadmap (you're reading it) |
| Per-source spec template | `docs/etl-specs/TEMPLATE.md` | The canonical shape every spec must take |
| Per-source spec index | `docs/etl-specs/README.md` | Table of every spec + status (drafted/reviewed/landed) |
| Per-source specs | `docs/etl-specs/<source>/<endpoint>.md` | One file per endpoint or feed |
| Skills (existing) | `.claude/skills/data-sources/*.md` | **Shrink to pointers**: "For endpoint X, see `docs/etl-specs/<source>/X.md`." Coverage rules + reading order only. |
| Coverage matrix (existing) | `.claude/skills/data-engineer/etl-endpoint-coverage.md` | **Repurpose as INDEX** linking to per-source specs. Becomes the wiring-layer dashboard. |
| Pre-flight rule (new) | `.claude/skills/data-engineer/SKILL.md` | Add: "Before changing an ETL pipeline, read its spec OR write one. Specs are non-negotiable." |

Specs live in `docs/` (not skills) because they are reference documentation — long, detailed, owned by humans + agents. Skills stay light and point at them.

## 4. The spec template (what every spec MUST cover)

Each `docs/etl-specs/<source>/<endpoint>.md` covers these 14 sections. No skipping; mark "not applicable" with one-line reason if a section genuinely doesn't apply.

1. **Identity** — provider, endpoint URL, HTTP method, auth, rate limit, conditional-GET contract (ETag / If-Modified-Since), payload size envelope.
2. **Schema (request)** — query parameters, body shape, required vs optional, validation rules.
3. **Schema (response)** — field-by-field: name, JSON path, type, length, constraints, enum values, encoding, nullability, ordering guarantees, max array sizes.
4. **Identifiers** — primary key in source; how it maps to our identity tables (CIK, instrument_id, accession_number, external_identifier_kind); FK targets in our schema.
5. **Watermark** — column name + type; source of truth (Last-Modified header / `latest_filing_date` field / monotonic in payload); late-arrival window; revision rules.
6. **Update strategy** — incremental? bulk-only? both? cadence; revision-window; tombstone rules; deletion semantics.
7. **Sink** — Postgres table(s); columns with types + lengths; PK; FKs; indexes (and their justification); conflict key; idempotent upsert SQL pattern (literal SQL, not prose).
8. **Bootstrap behaviour** — bootstrap stage `stage_key`; provides cap; consumes cap; **derivation-only or fetch-allowed**; max wall-clock budget; max HTTP count budget; expected rows.
9. **Daily behaviour** — cron job name; cadence; scope (full / incremental / changed-only); polling pattern (Atom / per-CIK poll / manifest worker / bulk-refresh).
10. **Failure modes** — per-row recoverable errors; whole-job-fail conditions; retry policy; backoff strategy; what gets WARN'd in `bootstrap_archive_results.rows_skipped` JSON.
11. **Smoke test** — 3-5 known instruments panel (default: AAPL, GME, MSFT, JPM, HD); expected row counts; cross-source verification source (gurufocus / marketbeat / SEC direct / etc.) + figure that should match.
12. **Operator runbook** — manual backfill command; wipe-and-rebuild command; freshness check endpoint; drift-detection query; rollback procedure.
13. **Gotchas** — numbered list of specific traps (e.g. "13F-HR VALUE cutover 2023-01-03 — values reported in dollars not thousands after this date"; "SEC submissions.json overflow pages live in `filings.files[]` once recent.filings has >1000 entries").
14. **Performance budget** — declared max wall-clock for bootstrap; max HTTP count; max DB roundtrips; assertion test path (a real `pytest` that asserts the budget).

The template is **non-negotiable**. Specs that ship without a budget or a smoke test are not landed.

## 5. The current diagnosis (corrected from Run #7 receipts memo)

Codex + reviewer agents corrected my Run #7 receipts memo in four material places. Final picture:

### Real critical path

**sec_rate lane**: S14 (48 min) + S16 (65 min) = ~113 min serial. This is the wall-clock dominator.
**db lane**: S25 (101 min), runs in PARALLEL with the sec_rate chain, so does not add to wall-clock floor.
**Run #7 wall-clock floor**: `max(113, 101) ≈ 113 min`, not 215 min as my memo wrongly summed.

### Where the time actually goes (per Codex + reviewer)

| Stage | Real bottleneck | Memo's wrong guess | Fix |
|-------|-----------------|---------------------|-----|
| S14 sec_submissions_files_walk (48 min) | Primary HTTP per CIK before checking `files[]` — `sec_submissions_files_walk.py:15,99,101` | "Lock contention with S25" | Use local `submissions.zip` already on disk for `files[]` discovery; only HTTP for secondary pages. **Drops S14 from 48 min → ~5 min.** |
| S16 sec_first_install_drain (65 min) | Issuer fast-path skips HTTP; 65 min = institutional_filers + blockholder per-CIK HTTP | "11k CIK sequential issuer walk" | Bound non-issuer HTTP to active/recent filers only. **Drops S16 from 65 min → ~10 min.** |
| S25 fundamentals_sync (101 min) | Bootstrap re-runs the daily omnibus: per-CIK snapshot HTTP (Phase 1b) + audit + tier review, although `financial_facts_raw` already has the data | "Parallelise fetch_workers" | **Already at fetch_workers=8.** Real fix: derive snapshot from `financial_facts_raw` in bootstrap mode; skip audit + tier review. **Drops S25 from 101 min → ~5 min.** |
| Phase 2 normalize within S25 (12 min) | Per-period single-row INSERT, 125k-250k roundtrips serial | (not mentioned) | Chunked multi-row INSERT (existing `_UPSERT_FACT_SQL_PREFIX` pattern). **Drops normalize from 12 min → ~2 min.** |
| `_canonical_merge_instrument` CardinalityViolation × 43 | Inserts into `(instrument_id, period_end_date, period_type)` without pre-deduping that conflict key — `fundamentals.py:1426,1495` | "Dedupe input array" (right idea, wrong file) | Pre-dedupe before INSERT at `fundamentals.py:1426`. |
| S13 cusip_resolver_post_bulk_sweep (5 sec, 19 rows) | Defect — should resolve ~16M; resolves 19 | (correct) | Move out of bootstrap critical path; **no cap depends on it.** Run as scheduled backfill post-bootstrap. |
| Advisory-lock conn leak (7 conns 35-130 min) | **No leak.** `JobLock` uses autocommit + properly closes — `locks.py:220,272,295`. LISTEN sessions are legitimate. | "Advisory-lock conn leak" | **Ticket withdrawn.** |

### Projected Tier 1 wall-clock after fixes 1-5

- S14: 5 min (was 48)
- S16: 10 min (was 65)
- S25 + Phase 2: 7 min (was 101 + 12)
- Other stages unchanged: ~10 min total
- **Projected total: ~25-30 min wall-clock — under 60-min Tier 1 target with headroom.**

## 6. The architectural pivot (Codex)

Bootstrap stages must be **bulk-native + idempotent-sink only**. They must NOT do per-CIK HTTP for data the bulk path provides.

Codex's standardisation shape per pipeline:

```
SourcePlan → FetchStrategy(bulk | http | cache) → ParseRows → IdempotentSink → Capability/rowcount/progress
```

Each stage **declares**:
- dataset name
- watermark column + type
- conflict key (the tuple PG's `ON CONFLICT` clause uses)
- row budget (expected count + tolerance)
- progress heartbeat (rows per emit + max gap)
- mode (bootstrap | daily | both — and which work is allowed in which)

When a stage is in **bootstrap mode**, it MUST refuse work that requires per-row HTTP if the bulk path already wrote the data. This is the contract that prevents S25 / S14 / S16 from doing duplicate work next run.

## 7. Tier 1 ticket sequence (corrected, Codex order)

Order = highest value first. Each ticket lands its own PR + updates the relevant spec's smoke-test results section.

1. **S25 bootstrap-mode derivation-only** — skip Phase 1 (per-CIK HTTP), Phase 1b (snapshot HTTP), Phase 2 (filing backfill) when bulk run completed. Derive snapshot from `financial_facts_raw`. **Largest single win.** Owner: spec `docs/etl-specs/sec-edgar/companyfacts.md` ALONG with this PR.
2. **S14 use local `submissions.zip` for `files[]` discovery** — eliminate 5000+ primary HTTP fetches. Owner: spec `docs/etl-specs/sec-edgar/submissions.md`.
3. **S16 bulk-aware non-issuer HTTP** — bound to recent/active filers only. Owner: specs `docs/etl-specs/sec-edgar/13f-filer-directory.md` + `docs/etl-specs/sec-edgar/blockholder-directory.md`.
4. **`_canonical_merge_instrument` pre-dedupe** at `fundamentals.py:1426` — fix CardinalityViolation × 43. Owner: spec `docs/etl-specs/sec-edgar/companyfacts.md` (canonical-merge subsection).
5. **Chunked multi-row INSERT in `_upsert_period_raw`** — drops Phase 2 from 12 min → 2 min. Owner: same spec.
6. **Progress heartbeats on long bootstrap stages** — closes #1225. Owner: cross-cutting; documented in `docs/etl-specs/TEMPLATE.md` §14.
7. **Move S13 cusip_resolver_post_bulk_sweep out of bootstrap** — runs as scheduled backfill. Owner: spec `docs/etl-specs/openfigi/cusip-resolver.md`.

After tickets 1-5 land: **Run #8** (verification run; same protocol as Run #7).
After 6-7 land + Run #8 verified: declare Tier 1 done.

## 8. Test gates (Codex + reviewer)

Before any Tier 1 ticket merges, these gates MUST exist:

1. **Bootstrap-mode contract test** — assert: after `sec_companyfacts_ingest` (S9) reports OK, no per-CIK `data.sec.gov/api/xbrl/companyfacts/CIK*.json` HTTP request occurs during the same bootstrap run. Implement via captured-request mock.
2. **Stage timing budget assertion** — pytest fixture reads the spec's §14 budget; bootstrap test asserts each stage's wall-clock + HTTP count stays under budget on a small CIK fixture (panel of 5).
3. **Conflict-key property test** — for every UPSERT writer (canonical merge, `_upsert_period_raw`, ownership observations, filing_events, etc.), generate random input arrays containing intentional duplicates on the declared conflict key; assert the upsert succeeds (no `CardinalityViolation`).
4. **Progress heartbeat test** — for every stage declaring `progress_heartbeat_rows=N`, run with mocked input of `5N` rows; assert at least 4 progress emits land within the max-gap window.
5. **Real-source smoke test (per source, opt-in network mark)** — once per source, against current EDGAR/FINRA/etc., on the panel of 5 CIKs/symbols; verify expected row counts + cross-source values.
6. **Capability scheduling test** — assert S25 cannot dispatch unless `fundamentals_raw_seeded` capability is held; assert the lane caps are enforced.

These tests live in `tests/integration/etl_contract/` and `tests/integration/etl_smoke/`.

## 9. Skill changes (pointer-only)

- `.claude/skills/data-sources/sec-edgar.md` — shrink. Move endpoint-specific content into `docs/etl-specs/sec-edgar/<endpoint>.md`. Skill keeps: source-wide rules (rate limit, identity, retry posture), reading order, link table to specs.
- `.claude/skills/data-sources/finra.md` — same.
- `.claude/skills/data-sources/openfigi.md` — same.
- `.claude/skills/data-sources/edgartools.md` — same.
- `.claude/skills/data-engineer/etl-endpoint-coverage.md` — repurpose as INDEX. Each ManifestSource row links to its spec. Keep the 5-wiring-layers matrix; the per-source detail moves out.
- `.claude/skills/data-engineer/SKILL.md` — add pre-flight rule: "Before changing an ETL pipeline, read its spec OR write one. Specs are non-negotiable."

## 10. Rollout sequence

| Phase | Output | When |
|------|--------|------|
| 0 | This plan reviewed + signed off (Codex + reviewer agents + user) | this session |
| 1 | `docs/etl-specs/TEMPLATE.md` + `docs/etl-specs/README.md` landed | this session |
| 2 | 4 bootstrap-critical specs drafted: SEC submissions, SEC companyfacts, SEC 13F bulk, SEC NPORT bulk | next session |
| 3 | Adversarial review of template + 4 specs | next session |
| 4 | Tier 1 tickets 1-5 land as PRs, each updating its spec | next 2-3 sessions |
| 5 | Run #8 verification | once 1-5 merged |
| 6 | Tickets 6-7 land + drill remaining 10 sources | post-Run-#8 |
| 7 | Acceptance: every wired source has a spec, every spec has a smoke test, pre-push gate enforced | end-state |

## 11. Acceptance criteria (Tier 1 done definition)

1. Every wired ETL source has a spec in `docs/etl-specs/<source>/`.
2. Each spec covers all 14 template sections (or has explicit "not applicable" + reason for any gap).
3. Each spec declares a bootstrap performance budget (wall-clock + HTTP count) AND has a passing assertion test.
4. Each spec has a smoke test against real-source data (network-marked; opt-in) AND a cross-source verification figure.
5. Skills no longer hold endpoint-specific content; they point at specs.
6. Pre-push hook: if a file in `app/jobs/`, `app/services/sec_*`, `app/services/finra_*`, `app/services/fundamentals*`, `app/providers/implementations/` changes, the relevant spec MUST also change (or a comment explains why not).
7. Bootstrap (Run #8) completes in ≤ 60 min wall-clock against current dev DB.
8. Bootstrap-mode contract test passes — no per-CIK HTTP for bulk-covered data.

## 12. Risks + mitigations

| Risk | Mitigation |
|------|------------|
| Writing 14 specs takes 2-3 weeks of focused effort | Start with the 4 bootstrap-critical ones; drill rest incrementally. Acceptance criteria 1-6 can phase in. |
| Specs drift from code | Pre-push gate (criterion 6) blocks PRs that change pipeline code without spec touch. |
| Adversarial review reveals template is incomplete | Template iteration in Phase 3 before broader rollout. |
| Tier 1 fixes 1-7 land but Run #8 still misses 60-min target | A miss reveals a bottleneck the receipts memo didn't catch. The corrected diagnosis is from sampled code; full per-stage profiling is part of Phase 4 PRs. We surface + iterate. |
| Codex/reviewer disagree with this plan's structure (specs in docs/, skill pointers, etc.) | This is the next step. Plan goes through review BEFORE any spec gets written. |

## 13. Adversarial review brief (next step)

Send this plan + the Run #7 receipts memo + agents' findings to:

1. **Codex** — does the corrected Tier 1 sequence hit 60-min? Are there sources we'd miss in spec rollout? Is `SourcePlan → FetchStrategy → ParseRows → IdempotentSink → Capability/progress` the right abstraction shape?
2. **Adversarial reviewer agent** — find holes in the template (sections missing; sections too vague); find specs we'd need that aren't on the priority list; challenge the "bootstrap mode is derivation-only" rule for edge cases.
3. **Code-architect agent** — does the layering (skill pointers → docs/etl-specs → impl code) keep coherence? Or does it create new fragmentation?
4. **Data-engineer adversarial pass** — from the perspective of "I'm a data engineer who just joined and want to ship a 15th source", does this plan actually keep me from improvising?

Each reviewer instructed: rip it apart, find gaps, propose specific section additions to the template, name sources we'd miss.

## 14. Open questions (for review to answer)

1. Should specs include a **deletion / GDPR / data-retention** section? (Not currently in template — does any source need it?)
2. Should specs include **upstream-provider SLA** (response time, uptime, known outages)? Probably yes — informs retry posture.
3. Should the spec's §14 performance budget have **per-environment** numbers (dev vs prod / cold vs warm cache)?
4. How do we handle sources that span multiple endpoints (e.g. SEC submissions has primary JSON + secondary `files[]` pages + Atom getcurrent overlay)? One spec or three?
5. Should derived sinks (e.g. `instrument_business_summary` derived from filings) have their own spec? Or is that "downstream" of the source spec?
6. Should each spec include a **migration / wipe history** subsection (which migrations created the sink table, when wipe-and-rebuild last ran)?
7. How does this plan interact with the existing `etl-endpoint-coverage.md` matrix? Repurpose as index, but who owns the cross-source coverage gates that live there today (G1-G13)?

## 15. Definition of "this is nailed"

We say Tier 1 is nailed when:

1. **Run #8 completes in ≤ 60 min** on a fresh dev DB wipe with no manual intervention beyond operator setup.
2. **A new engineer or AI agent can read `docs/etl-specs/<source>/<endpoint>.md` and have ALL the context** they need to extend, fix, or audit that pipeline — without needing me, the user, or Codex to fill gaps.
3. **Every bootstrap stage is bulk-native + idempotent-sink + budget-asserted.** No bootstrap stage ever does per-row HTTP for bulk-covered data.
4. **Skill files are < 100 lines each** — they coordinate, they don't duplicate.
5. **`docs/etl-specs/README.md` index lists every wired source as `landed`** (no `drafted` / `missing` rows).

That's the end state. The work is bounded.
