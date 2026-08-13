# #1820 — Persist CONSIDERED blocked BUYs + data-completeness gate (P0+P1 of #1815)

Parent design: #1815 (fully specified). This implements P0 (backend) + P1 (FE Recommendations) only. No live-score change, no operator decision.

## Bug (verified)
`app/services/portfolio.py:1122` appends a rec only `if should_buy`. Unheld ranked candidates that fail any BUY gate write **nothing** — `buy_reason` is discarded. When nothing clears the BUY bar, `/recommendations` is 100% held HOLDs and the operator can't tell the unheld universe was evaluated.

## Source rule
- #1815 §4 (data-completeness `C`, fully specified formula + caps). Piotroski/Altman etc. are P2 — out of scope here.
- Settled decisions: "Recommendation persistence — append-oriented; do not spam identical HOLD rows" (apply same dedup to CONSIDERED). "Score auditability — each score row carries enough detail" (C is additive evidence on `scores`). Risk-metrics evidence-layer blessing → additive nullable evidence under a stable version, **do not bump `model_version`** (headline math unchanged).
- Enum-filter prevention (review-prevention-log): unbounded enum filters must 422 on nonsense → filter Literals get the new values; response `action`/`status` stay open `str` (audit open-vocab pattern).

## Full-population note
`trade_recommendations.action` is bare `TEXT` (no CHECK) → new `CONSIDERED` value needs no constraint migration. `status` HAS `chk_recommendation_status` (sql/028) → must add `'considered'`. Execution pipeline Phase 0 (timing, scheduler.py:3441) + Phase 1 (guard, scheduler.py:3596) select `WHERE status = 'proposed'` regardless of action, and the work-signal (scheduler.py:589) is `status IN ('proposed','approved')`. **Therefore CONSIDERED rows MUST get `status='considered'` (never 'proposed') so they are invisible to every execution selector — primary safety invariant of this change.**

## Changes

### 1. Migration `sql/209_considered_and_completeness.sql`
- `ALTER TABLE scores ADD COLUMN IF NOT EXISTS data_completeness NUMERIC(10,4), ADD COLUMN IF NOT EXISTS completeness_tier TEXT;`
- Idempotent `chk_scores_completeness_tier CHECK (completeness_tier IS NULL OR completeness_tier IN ('insufficient_data','thin_data','full'))` (DROP IF EXISTS + ADD, DO $$ block).
- Rebuild `chk_recommendation_status` with the **full live vocabulary** (verified on dev DB: `pg_get_constraintdef` = the sql/028 8-value set) **+ `execution_pending` + `considered`**. `execution_pending` is written by `order_client.py:1115` but is ABSENT from the current CHECK — a latent constraint gap that any broker-pending live order would hit; including it here fixes that gap on the path we're already editing. Pattern: DROP IF EXISTS + ADD NOT VALID + VALIDATE (matches sql/028). All 55 live rows are `status='rejected'` so VALIDATE passes.

### 2. `app/services/scoring.py`
- Pure fn `_data_completeness(fund_present: bool, filing_age_months: float | None, thesis_present: bool, thesis_age_days: int | None, price_td_count: int, news_90d_count: int) -> tuple[float, str]` implementing §4 exactly:
  - `C = 0.30·fund + 0.30·filing + 0.15·thesis + 0.15·price + 0.10·news`
  - fund: 1.0 if fund_present else 0
  - filing: 1.0 if ≤15mo; 0.5 if ≤27mo; else 0 (None → 0)
  - thesis: 1.0 if present AND ≤90d; 0.5 if present AND stale; else 0
  - price: 1.0 if ≥252 td; 0.5 if ≥63 td; else 0
  - news: 1.0 if ≥3 in 90d; 0.5 if ≥1; else 0
  - tier: `insufficient_data` if C<0.40; `thin_data` if <0.70; else `full`
- `_load_instrument_data`: add (a) latest 10-K/10-Q filed date from `filing_events` (`filing_type IN ('10-K','10-Q','10-K/A','10-Q/A')`, `MAX(filing_date)`) — **source rule**: SEC annual (10-K, Reg S-K) / quarterly (10-Q, Exchange Act §13) reports; `filing_events` (sql/001_init.sql:46) is our ingested EDGAR filing-event record and is already read by this fn for red-flag; on dev it covers 4216 distinct instruments for these forms (vs 4178 in `sec_filing_manifest`) and agrees on AAPL latest=2026-05-01. NOTE: the column is `filing_type` (NOT `form_type`). (b) `COUNT(*)` price_daily rows w/ non-null close; (c) `COUNT(*)` news_events in last 90d. `fund_present` = a **dedicated full-population `EXISTS`** over `fundamentals_snapshot` (revenue_ttm non-null AND (operating_margin OR gross_margin non-null)) — NOT the `LIMIT 5` `fund_rows` sample, since this feeds a safety gate (Codex ckpt-1).
- `ScoreResult`: add `data_completeness: float | None = None`, `completeness_tier: str | None = None`.
- `compute_score`: compute and set them. **`total_score` math untouched.**
- `_insert_score`: write the two columns.

### 3. `app/services/portfolio.py`
- `Action = Literal["BUY","ADD","HOLD","EXIT","CONSIDERED"]`.
- Surface `completeness_tier` in the loaded score dict (`_load_ranked_scores` SELECT adds `completeness_tier`).
- `_evaluate_buy`: early gate — if `completeness_tier == 'insufficient_data'`, return `(False, "Insufficient data (C<0.40): blocked from BUY — max HOLD")`. (thin_data top-decile gate = deferred, P4; out of #1820 P0 scope which lists only the C<0.40 cap.)
- `_evaluate_add`: same insufficient_data short-circuit (held name caps at HOLD).
- Unowned-candidate loop: when `not should_buy`, append a `CONSIDERED` Recommendation (`suggested_size_pct=None`, `target_entry=None`, `rationale=buy_reason`).
- `_should_persist_hold` → `_should_persist_dedup(action, instrument_id, rationale, prior_recs)` deduping both `HOLD` and `CONSIDERED` (prior same action + identical rationale ⇒ skip). Persist loop uses it for both.
- `_insert_recommendation`: `status = 'considered' if rec.action == 'CONSIDERED' else 'proposed'`.
- Counts log includes CONSIDERED.

### 3b. `app/services/execution_guard.py` (defense-in-depth, Codex ckpt-1 HIGH)
`evaluate_recommendation` loads by id and `_write_audit` unconditionally flips status to approved/rejected — no action/status gate. Add `EXECUTABLE_ACTIONS = {"BUY","ADD","HOLD","EXIT"}`; raise `ValueError` at the top of `evaluate_recommendation` if `action not in EXECUTABLE_ACTIONS` (same class as the not-found programmer-error raise; prevents a stray CONSIDERED id from ever being flipped to approved/rejected via a future manual/admin path). Scheduler Phase 1 already catches per-rec exceptions.

### 4. `app/api/recommendations.py`
- Filter `Action` Literal += `"CONSIDERED"`; `Status` Literal widened to the full live vocabulary (`proposed, approved, rejected, executed, execution_pending, execution_failed, timing_deferred, timing_expired, cancelled, considered`) — enum-filter rule: a legit filter value must not 422.
- Dedup CTE: `WHEN r.action IN ('HOLD','CONSIDERED')`.
- List + detail SELECT join `scores` via `score_id` for `data_completeness`, `completeness_tier`; add to response models + `_parse_list_item`. (List already needs a scores join — add `LEFT JOIN scores s USING (score_id)` inside the CTE-fed query; keep `d.rn=1` filter.)

### 5. Frontend
- `types.ts`: `RecommendationListItem`/`RecommendationDetail` += `data_completeness: number | null; completeness_tier: string | null;`.
- `RecommendationsPage`: group rows into 4 sections — **TO-BUY** (BUY/ADD), **CONSIDERED-BLOCKED** (CONSIDERED), **HOLD**, **EXIT** — each its own subheading; empty sections show an honest "none this run" line. Render a completeness-tier badge per row. For `status==='rejected'` rows, inline the rationale + a "view guard evidence" affordance that sets the audit filter to that `instrument_id` (CONSIDERED rows carry the reason in `rationale`; they have no guard audit row by design).
- `RecommendationsTable`: `ACTION_TONE` += CONSIDERED (slate/amber), `STATUS_TONE` += considered; completeness badge.
- `RecommendationsFilters`: action dropdown += CONSIDERED; status dropdown += considered.
- Update `RecommendationsPage.test.tsx`.

### 6. `.claude/skills/ranking-engine/SKILL.md`
Rewrite stale stub → real v1.3 model (6 families + weights, additive penalties + Calmar reward, model_version semantics, completeness `C` evidence, append-only scores, rank_delta within model_version).

### Freshness semantics (Codex ckpt-1 MED)
`recommendations_is_fresh()` (freshness.py:237) is `MAX(created_at)` over all rows; CONSIDERED rows now advance it. **Intended**: a review that evaluated the universe but cleared no BUY previously wrote nothing (HOLDs deduped) and could read stale; CONSIDERED rows are the honest record that the review ran. Documented, not changed.

## Tests
- Pure: `_data_completeness` table test (each tier boundary; price-only ⇒ C=0.25 ⇒ insufficient_data).
- Pure: `_should_persist_dedup` (HOLD + CONSIDERED dedup matrix).
- DB: CONSIDERED row persisted with `status='considered'` + rationale; insufficient_data caps BUY→CONSIDERED.
- Safety: CONSIDERED `status='considered'` is excluded by the Phase 0/1 `status='proposed'` selector and the `status IN ('proposed','approved')` work-signal; `evaluate_recommendation` raises on a CONSIDERED action.
- FE: page renders the 4 sections incl CONSIDERED.

## DoD (CLAUDE.md ETL clauses — scoring/recommendation path)
Restart jobs daemon onto new main; trigger the scoring+portfolio-review job; verify `/recommendations` renders CONSIDERED + completeness; smoke AAPL/GME/MSFT/JPM/HD; record figures + commit SHA in PR.
