# #1748 — filings risk-scorer: populate `filing_events.red_flag_score`

Parent #585. Predecessor #592 (deferred the red-flag-trend chart for lack of data).

## Problem
`filing_events.red_flag_score NUMERIC(10,4)` (sql/001) is **NULL across all 2,717,528 rows** (dev-verified). Three consumers already read it and sit dark:
1. **Scoring** (`app/services/scoring.py`): `AVG(red_flag_score) … WHERE red_flag_score IS NOT NULL` over 90d → turnaround component (`1.0 − avg`, weight 30%, scoring.py:748) + additive `high_red_flag` penalty 0.10 when avg > 0.60 (scoring.py:824).
2. **Portfolio manager** (`app/services/portfolio.py`): `MAX(red_flag_score)` over 90d (:480); EXIT **recommendation** when `break_conditions` present AND max ≥ `EXIT_RED_FLAG_THRESHOLD` 0.80 (:611). Advisory only — never auto-closes (safety invariant: positions close only on explicit user action).
3. **FE**: InstrumentPage red-flag badge; #592 deferred trend chart.

## Source rule
Score is an eBull metric (no SEC reg defines a 0–1 score); its **inputs are fixed by settled invariants**:
- **8-K item severity** — `sec_8k_item_codes.severity ∈ {informational, material, critical}` (sql/053, seeded from Form 8-K General Instructions; severity is our settled editorial tier). Per-filing item set = `filing_events.items[]` (same migration), from `submissions.json filings.recent[].items`.
- **Late filing (Form NT)** — SEC Rule 12b-25: an NT filing = a periodic report missed its deadline. Form in `filing_events.filing_type`.
- **Restatement / non-reliance** — 8-K **Item 4.02**, already `critical` in the lookup → captured via `items[]`.

## Full-population verification (dev)
- `red_flag_score`: 0 / 2,717,528 non-null → premise holds.
- `items[]`: **393,519** non-null (covers the 385,169 `8-K`/`8-K/A`). **Chosen source.**
- `eight_k_items` (parsed bodies): only 20,343 → **rejected** (20× sparser; would leave most 8-Ks unscored).
- NT forms: NT 10-Q 2600, NT 10-K 1648, NT 20-F 51, + smaller. Matchable by `filing_type` prefix.

## Model (explicit, auditable — settled §191; only ever writes scores ≥ 0.7)
Pure fn `score_filing_red_flag(filing_type, items, severity_by_code) -> float | None`:

| Filing | Score |
|---|---|
| 8-K / 8-K/A with **≥1 critical** item (per `sec_8k_item_codes`) | **1.0** |
| 8-K with only material / informational / unknown items | **NULL** |
| Form NT (`filing_type ~* '^NT[ /-]'`) — late filing | **0.7** |
| everything else (Form 4, 10-Q, 10-K, 13D/G, material/info 8-K, …) | **NULL** |

**Why only critical + NT, and only high values (resolves Codex ckpt-1 HIGH-1/HIGH-2, LOW-7):**
- Scoring's turnaround is `1.0 − avg_red_flag`; a NULL defaults to **0.5-neutral**. So *any* non-null score **< 0.5 would REWARD** the instrument (a benign 8-K at 0.1 → 0.9 turnaround component, better than no-data). Therefore we never write a low score — only genuine red flags, and only at ≥ 0.7.
- Scoring penalty + portfolio guard both read **AVG / MAX over `WHERE NOT NULL`**. Scoring material/informational 8-Ks (even at 0.5) would **dilute the AVG**, suppressing the penalty after a real critical. Leaving them NULL keeps a lone critical at avg = 1.0 → penalty fires.
- `red_flag_score` is specifically a **risk** signal. `material` 8-K items are genuinely mixed (entering a material agreement is often good); asserting a red flag there would be wrong. Fail-closed: NULL = "no red flag asserted," not "score 0."
- NT at **0.7**: > 0.60 so a recent NT alone trips the *scoring* penalty; < 0.80 so it does **not** alone trigger a portfolio EXIT recommendation (a missed deadline dings the score but isn't an auto-exit-grade event; a critical 8-K is). Late annual vs quarterly not distinguished in v1 — KISS.
- Binary critical-present = 1.0 (no MAX-over-severity gradation needed once material/info are NULL). 4.02 is `critical` → 1.0.

**Unknown item codes (Codex MED-6):** an 8-K whose items are all absent from `sec_8k_item_codes` → no critical found → NULL (fail-closed: never a false critical). A genuinely-new SEC *critical* code is a blind spot until `sec_8k_item_codes` is updated — that's the existing taxonomy-maintenance norm (the lookup is the settled source; SEC item-code changes are rare and announced). Noted, not solved in code.

## No model_version bump
Current default is **`v1.3-balanced`** (scoring.py:42; settled-decisions §220 text says v1.2 but is stale post-#1635). `red_flag_score` was always a scoring input (defaults 0.5-neutral when NULL). Populating it is **data completion under the existing `v1.3-balanced` model** — no formula or threshold changes — matching settled §236 ("bump only when an EXISTING metric's *computation* changes"). Append-only history preserved; rank_delta stays within-version.

**Operator-visible effects (documented, intended — the column's designed purpose going live, demo-first):** instruments with a recent **critical 8-K** (bankruptcy / delisting / non-reliance / auditor change / impairment / cyber / control change / failed distribution) or a recent **NT late filing** will (a) lose turnaround/score weight, (b) may trip the additive `high_red_flag` penalty, (c) for a held position with a thesis carrying `break_conditions`, may surface an EXIT **recommendation** (critical only; advisory, never auto-close). No threshold/formula changed — only the data feeding existing, intended consumers.

## Implementation
1. **Pure scorer** `app/services/filings_risk.py`: `score_filing_red_flag(filing_type, items, severity_by_code)`. Helper `load_severity_by_code(conn) -> dict[str,str]` from `sec_8k_item_codes`. Table-tested.
2. **8-K write path** — extend `app/services/sec_filing_items.py::apply_8k_items_to_filing_events` to `SET items = …, red_flag_score = …` in the same UPDATE (items known there; this is the real 8-K source-of-truth, not the `filings.py` INSERTs which don't carry items — Codex HIGH-3). Pass the severity map in.
3. **NT write path** — at the `filing_events` INSERTs in `app/services/filings.py` (`_insert_filing_event`, `_upsert_filing`), compute `red_flag_score` from `filing_type` (NT → 0.7, else None — items not needed). Add to INSERT columns + `ON CONFLICT … DO UPDATE SET red_flag_score = EXCLUDED.red_flag_score`. (8-K rows get None here, filled by step 2.)
4. **Backfill job** `filings_red_flag_backfill` (registered, manual-trigger) — batched over `red_flag_score IS NULL AND (filing_type ~* '^NT' OR items IS NOT NULL)`; apply the SAME pure fn; `UPDATE … WHERE filing_event_id = ANY(batch)`. One source of truth (no SQL re-encoding). Run on dev after merge (ETL clause 10).
5. **Trend endpoint** `GET /filings/{instrument_id}/red-flag-trend` (app/api/filings.py) → per-quarter `AVG(red_flag_score)` + `COUNT` over scored rows (mirror quarterly-counts shape). Pydantic `RedFlagTrend { instrument_id, symbol, points:[{quarter, avg_score, n}] }`.
6. **FE 3rd chart** — `RedFlagTrendChart` in `filingsAnalyticsCharts.tsx` (`useChartTheme`, `defaultTooltipStyle`, empty guard, `isAnimationActive={false}`); render the deferred 3rd `<Section>` in `FilingsAnalyticsPage.tsx`; drop the #1748 deferral comment. New `fetchRedFlagTrend` + types.

## Tests
- Pure-fn table test (`tests/test_filings_risk.py`): critical→1.0, material/info only→None, NT variants→0.7, routine→None, empty items→None, unknown-only→None, mixed (critical+info)→1.0.
- FE: red-flag-trend lib transform + chart render-test (empty + populated).

## Out of scope
Going-concern / restatement-language NLP (body text); `eight_k_items`-based scoring; dense scoring of routine forms; distinguishing NT 10-K vs NT 10-Q severity.

## DoD (ETL clauses 8–12)
Smoke panel after backfill: AAPL, GME, MSFT, JPM, HD — record a scored critical 8-K (or note none in window) + the trend endpoint figure. Cross-source one critical 8-K item vs SEC EDGAR. Backfill executed on dev (clause 10); trend endpoint verified live (clause 11).
