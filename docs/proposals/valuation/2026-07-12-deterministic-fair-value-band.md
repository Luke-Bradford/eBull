# Deterministic valuation-evidence band (peer-median + own-history multiples)

- **Issue:** #2009 (design-first). Blocked-by: #2008 (landed, `bb03f62e`). Related: #2007, #1987, #2002. Upstream dep for #2010.
- **Status:** PROPOSAL v2 — committee-hardened, passive-evidence-first (operator scope decision 2026-07-12). No code until operator signs off v2 → `writing-plans`.
- **History:** v1 written + Codex ckpt-1 folded; 8-lens committee run (0 hallucinations; 5 structural reshapes + a reframe). Findings: `project_fair_value_band_consolidated_findings` (memory) + 8 raw lens memos.

## 1. Problem, inversion, and the reframe

The 2026-07-11 thesis-quality review found the Claude 14B thesis writer is the least reliable link that turns fundamentals into a price. Invert: **code computes a deterministic band from regulated fundamentals; the LLM reasons against it.**

**Committee reframe (Codex CTO, adopted):** v1 positions the band as **deterministic valuation *evidence with confidence labels*** — a mechanical prior the thesis *considers / overrides / marks absent* — NOT "fair-value truth." Rationale: the band is computable for only ~24% of the tradable universe (§3) and can be *structurally* wrong (wrong multiple/profile/cohort); an unconditional "anchor to it" instruction risks **garbage-anchor authority** — the LLM rationalizing a bad number more convincingly than an unconstrained writer. So v1 ships the band + a **quality label** + a **passive** thesis context block + a **divergence measurement**, and a data-driven fast-follow (v2) tightens the writer to "anchor unless justified" for high-quality bands only.

Long-only v1: the band is downside/upside context, never a short signal.

## 2. Source rule

Every data-treatment decision is fixed by a documented rule, not first principles.

| Decision | Governing rule |
|---|---|
| Peer cohort key | **SEC SIC**, walked up its own hierarchy. OMB *SIC Manual*: Division → Major Group (2-digit) → Industry Group (3-digit) → Industry (4-digit). Ladder SIC-4→3→2 is the standard's own generalization axis. Source `instrument_sec_profile.sic`. |
| Multiple selection | The name's own TTM profile (§4.2), mirroring `instrument_valuation` VIEW formulas (`sql/201`). Financials (SIC Division H, 60–67) use P/B — EV meaningless for deposit-funded balance sheets. |
| Market-cap / EV basis | `resolve_market_cap_basis` (#1662/#1664, `xbrl_derived_stats.py:538`). Dual-class handled in Python; prevention-log #1921: never re-implement in SQL. |
| TTM inputs | Strict `financial_periods_ttm` (#2008, `sql/220`). No 4th copy of the strict-TTM/330-day logic. |
| Band synthesis | Blend + outer envelope (operator, 2026-07-12). §4.5. |
| Reproducibility | Mutable price → the observation row IS the audit record (`sql/198` rule). Two-layer append+current; single batch as-of DATE. §5, §4.6. |
| Evidence discipline | #1632: absence statused, never a neutral default; as-of stamps; cited figures agree with block statuses. |

**Weakly-source-ruled call flagged for the plan:** the multiple-per-profile mapping (§4.2) — valuation canon, no single reg. Grounded in the name's own profile via deterministic gates + the `sql/201`-blessed formulas, but the one place to push on.

## 3. Full-population verification (dev DB, 2026-07-12)

Verified against the whole tradable universe (12,603), not a sample. Adversarial lens re-verified every figure below against schema.

**Premise correction.** `peer_grade` (#1815/#1823, `instrument_analytics.py:375`) = percentile *ranks over scoring-family scores*, no median, walled out of scoring. The median-of-a-multiple machinery is a *different* service, `peer_comparison.py` (#1751) — `percentile_cont(0.5)` over pe_ratio/roe/margins, cohort = eToro-`sector` + nearest-8 by log-`total_assets`. This spec reuses its cohort+median *pattern*, re-keyed to SIC, off `price_daily`.

**Band-eligible population** (v1 multiples). Note two population tiers:
- **Flow multiples** (require strict `is_complete_ttm`, `sql/220:69-71`): P/S **3,047** (`revenue_ttm>0`), P/E **~2,000** (`eps_diluted_ttm>0`).
- **Stock-item multiple** P/B **3,909** (`shareholders_equity>0`) — `sql/220:151` populates equity via `MAX FILTER(rn=1)` *outside* the strict-4Q gate, so P/B is a structurally **looser** population than the flow multiples (Adversarial lens).

~8,700 tradable names (non-US, ETF, pre-revenue) get **no band → statused absence**; the writer keeps its own judgment.

**Peer key — SIC beats eToro sector** (population base for this table = the 3,047 flow-eligible names):

| | SIC (`instrument_sec_profile.sic`) | eToro `instruments.sector` |
|---|---|---|
| Missing among the 3,047 | 7 (0.2%) | 614 (20%) |
| Distinct values | 368 / 243 / 66 at 4/3/2-digit | 9 opaque codes |
| Cohort ≥8 members | SIC-4 72.8% · SIC-3 82.3% · SIC-2 97.7% | 79.8% |

**Own-history depth** (comparator b) is bounded by `price_daily`, not fundamentals: median **3.4 price-years** (fundamentals avg 10.9 FY-yrs / 45 snapshot-quarters). A *recent trailing range* (~3y), labeled honestly; 23% with <2y price → peer-only.

**Dual-class:** `sql/201` NULLs cap-based multiples for curated dual-class, keeps P/E — those names get a P/E-only band.

**Worked fixture — AAPL** (SIC 3571, eps_ttm 8.26, close $315.20): own trailing P/E (7 pts, ~4.2y window) p20/p50/p80 = 31.2/34.5/36.9 → band ≈ $258/$285/$305. Reads slightly rich vs close — a defensible evidence signal.

## 4. Methodology

Pure, deterministic, auditable: identical inputs (pinned by a single as-of DATE) → identical band, every run. **v1 multiples: P/E, P/S, P/B.** EV/EBITDA deferred to v2 (§11) — its strict-D&A + net-debt back-out + missing own-history + negative-equity drops are disproportionate v1 liability.

### 4.1 Eligibility gates — a universal precondition

`§4.1 is re-applied to EVERY multiple wherever it appears` (selection §4.2, cohort §4.3, conversion §4.5) — a multiple that fails here is never assigned, never medianed, never converted (Adversarial lens: a negative-equity financial must not silently reach conversion).

A multiple is *computable* for a name at the batch as-of date iff its denominator is strictly positive on the strict-TTM row: P/E `eps_diluted_ttm > 0`; P/S `revenue_ttm > 0`; P/B `shareholders_equity > 0`. Latest `price_daily.close` (as of the batch date) exists, strictly positive (`NULLIF(GREATEST(close,0),0)`, prevention-log L113), and passes freshness (§4.6).

**Currency coherence — fail-closed:** require `financial_periods_ttm.reported_currency == instruments.currency`, else statused `currency_mismatch`. Full-pop today 0/3,047 mismatch (all USD/USD); this fail-closes the instant a non-USD-reporting name enters. Future per-region providers: reuse `fcf_yield.py`'s `_period_fx_rate`/`fx_cross_rate` (fail-closed, literal `currency_mismatch`) — no second FX path.

### 4.2 Multiple selection by profile (deterministic)

**Dual-class target gate — runs first:** resolve target basis via `resolve_market_cap_basis`. If `basis != not_multiclass`, the selected set is intersected with `{P/E}` (cap-/share-based multiples dropped, as `sql/201` suppresses). Then, on the TTM row, first match wins (each assigned multiple must pass §4.1):

1. **Financial** — SIC major group ∈ 60–67 → **{P/B, P/E}**.
2. **Profitable non-financial** — `net_income_ttm > 0` → **{P/E, P/S}**.
3. **Revenue, not profitable** — `revenue_ttm > 0` → **{P/S}**.
4. **None computable** → no band (statused absence).

### 4.3 Comparator (a): peer-median multiple — two-pass, pure synthesis

- **Cohort:** names sharing the target's SIC-4 that pass §4.1 for *that* multiple. If `n < MIN_PEERS` widen SIC-4→3→2; if still `< MIN_PEERS`, comparator (a) absent for that multiple. `MIN_PEERS = 8` (matches `peer_grade._MIN_SECTOR_PEERS`).
- **Size refinement:** nearest `PEER_LIMIT = 8` by `|ln(peer.total_assets) − ln(self.total_assets)|` (reuse `peer_comparison._rank_peers`).
- **Single as-of date:** every cohort member's multiple is computed from its close **as of the one batch date** (NOT each member's own latest close — mixed as-of → incoherent medians, non-reproducible; Data-Eng lens). Member multiples off `price_daily`, not `quotes` (avoids the #1857 gate).
- **Dual-class cohort suppression:** cap-/share-based cohort multiples (P/S, P/B) anti-join the curated oracle `instrument_class_shares_outstanding` — copying `sql/201:50-56` *exactly*: `provider='sec' AND identifier_type='cik' AND is_primary=TRUE`, `source_cik = lpad(cik,10,'0')`. Suppression is by that oracle, NOT denominator positivity (a GOOG/GOOGL peer has positive combined `shares_outstanding` and would leak a `combined×one-class-price` distortion, #1662). A dual-class member contributes only P/E.
- **Peer-member freshness — hard, statused:** exclude a member whose as-of close is staler than `PEER_STALE_DAYS` vs the batch date. Stamp `{cohort_n, excluded_stale_n, newest_close_date}` per multiple.
- **Two-pass compute** (Data-Eng lens): **pass-1** set-based `(sic_key, multiple) → percentile_cont(0.25/0.5/0.75)` materialized over the eligible universe; **pass-2** per-name pure synthesis reads pass-1. A single-name refresh reads pass-1's stored percentiles (cohort medians don't re-percentile per sibling).
- **Output:** peer `p25 / p50 / p75` of the surviving cohort multiple.

### 4.4 Comparator (b): own trailing multiple range

- **Source series:** `fundamentals_snapshot` (one TTM row per historical quarter, `as_of_date = period_end`) × `price_daily.close` **nearest-at-or-before** each `as_of_date` (`price_date <= as_of_date` — a post-quarter price is lookahead bias; Data-Eng lens), windowed to the `price_daily` span. Snapshot carries `eps`, `book_value` (per-share), `revenue_ttm`, `shares_outstanding` → **P/E, P/S, P/B**.
- **Min points:** `MIN_OWN_POINTS = 6` distinct quarters with a positive multiple, else comparator (b) absent.
- **Output:** own `p20 / p50 / p80`, labeled `recent_trailing_~Ny`.

### 4.5 Synthesis — blend + outer envelope (operator decision)

Per multiple `m` (percentile arithmetic is **pure Python over rows-as-args**, not SQL — Test lens: keeps the correctness on the fast push gate):

```text
base_mult_m = mean(peer_p50, own_p50)          # both present
low_mult_m  = min(peer_p25, own_p25)            # outer envelope
high_mult_m = max(peer_p75, own_p75)
```

Degrade to the single surviving comparator's p50 / p25 / p75 if only one present; no band for `m` if neither.

**Convert multiple → per-share** (target per-share metric; dual-class target = single-class share count only, since cap-based multiples were dropped):
- P/E: `mult × eps_diluted_ttm`
- P/S: `mult × (revenue_ttm / shares_outstanding)`
- P/B: `mult × (shareholders_equity / shares_outstanding)`

**Combine across the 1–2 selected multiples**, outer envelope:

```text
base_value = median(base_value_m for m in selected)
bear_value = min(low_value_m  for m in selected)
bull_value = max(high_value_m for m in selected)
```

`bear ≤ base ≤ bull` by construction; a final assert fail-closes (no band, statused) otherwise.

### 4.6 Freshness + as-of coherence

- The whole batch computes at ONE market-calendar as-of DATE (the newest closed session, data-anchored — NOT `now()::date`; off-by-one/tz safe). Every price read (target + cohort + own-history) is relative to that date.
- Target latest close staler than `PRICE_STALE_DAYS` → statused `stale_price` (not written with a stale price). Dev market-data-unreachable artifact; prod refreshes daily.
- `price_as_of` + `ttm_end` stamped on the row.

### 4.7 Band quality status (committee — Codex CTO)

A deterministic `band_quality_status ∈ {high, medium, low}` from: comparator count (1 vs 2 sides), own-history depth/points, cohort freshness (`excluded_stale_n / cohort_n`), SIC ladder level reached (4 tightest), selected-multiple count, and cross-multiple spread (wide disagreement = low). This label — not just presence — governs whether the band earns anchor authority in the v2 fast-follow. In v1 it is surfaced as evidence + measured.

## 5. Storage — two-layer append + current (copy `sql/198`)

Mutable price → a past band is not reconstructable from current `price_daily`; the observation row IS the audit record (`sql/198:8-17` rule). The v1 cited "extend `instrument_valuation`" was wrong — that is a VIEW. Adopt the `instrument_risk_metrics` two-layer shape:

```sql
-- append-only audit record
fair_value_band_observations (
  instrument_id    bigint      NOT NULL,          -- NO FK (survive delist/merge/re-id; instruments never hard-deleted)
  method_version   text        NOT NULL,          -- 'fvb_v1'
  computed_at      timestamptz NOT NULL,
  as_of_date       date        NOT NULL,          -- the single batch as-of
  ttm_end          date        NOT NULL,
  bear_value       numeric(18,6),
  base_value       numeric(18,6),
  bull_value       numeric(18,6),
  quality_status   text,                          -- CHECK IN (high,medium,low)
  reason           text        NOT NULL,          -- CHECK IN (ok,no_multiple,currency_mismatch,stale_price,multiclass_unavailable,thin_cohort)
  target_basis     text        NOT NULL,          -- resolve_market_cap_basis result
  n_selected       smallint    NOT NULL,
  basis_json       jsonb       NOT NULL,          -- per-multiple peer {cohort_n,excluded_stale_n,p25/50/75}, own {points,window_y,p20/50/80}
  PRIMARY KEY (instrument_id, method_version, computed_at)
);
-- write-through current (the thesis read row)
fair_value_band_current (LIKE fair_value_band_observations, PRIMARY KEY (instrument_id, method_version));
```

Typed low-cardinality columns (`reason`/`target_basis`/`quality_status`/`n_selected`) are promoted OUT of JSONB (every §10 verify query filters them; JSONB-extract = unindexable seq scan — Data-Eng lens). Partial index `fair_value_band_current(instrument_id) WHERE base_value IS NOT NULL` for the writer's real-band read. `method_version` in the PK → a `fvb_v1→v2` bump writes alongside for shadow compare (never destructive). Absence = a row with NULL bear/base/bull + a `reason` — statused, queryable, never a missing row.

## 6. Compute — DAG layer, pure two-pass

- **Topology (committee BLOCKING):** `fair_value_band` is an **orchestrator DAG layer** with `dependencies=("candles","fundamentals")` (exactly `scoring`'s tuple, `registry.py:143-151`; like `risk_metrics`, deliberately NOT a `ScheduledJob` — a cron job double-fires + can't order vs the candle/fundamentals layers, `scheduler.py:342-346`). It is driven by `refresh_cascade.cascade_refresh` (`refresh_cascade.py:362-455`) on the same `changed_ids`, computed **ahead of the synchronous `generate_thesis`** so a fundamentals-triggered thesis regen reads a fresh band. `requires_layer_initialized=("candles","fundamentals")` + reuse `fundamentals_content_ok` for empty/partial-DB self-skip.
- **Service `app/services/fair_value_band.py`:** **pure policy** (cohort ladder, percentile synthesis, envelope, per-share conversion, dual-class routing, quality scoring) — table-tested, no DB, rows passed as args (mirror `_assemble_total_company_cap(legs_raw=…)`). The IO wrapper resolves `MarketCapResolution` + oracle membership into plain values BEFORE calling pure synthesis (mirror `_apply_market_cap_basis`, `scoring.py:353`). **Per-instrument SAVEPOINT** (catch `UndefinedTable, UndefinedColumn`) — one bad row statused, batch continues.
- **Two-pass** (§4.3): pass-1 materialize cohort percentiles universe-wide; pass-2 per-name synthesis.
- **Indexes/cost:** add an index on `instrument_sec_profile.sic` + `sic2`/`sic3` generated columns for the prefix ladder (none today, `sql/051` cik-only); `financial_periods_ttm` is a VIEW re-materialized each read.
- **Bootstrap bulk first-load** (new-surface rule) — a bootstrap stage populates the whole eligible population.
- **method_version re-fire:** an operator full-universe recompute trigger (a `sec_rebuild` analogue) + runbook — a bump needs full recompute.
- **Observability:** a reason-bucket rollup endpoint (DAG layers don't auto-surface as admin ProcessRows) so the operator distinguishes dev-stale from a real bug without reading 3,000 rows.

## 7. Consumers (v1 = passive evidence)

1. **Thesis writer context — PASSIVE block** `fair_value_band` in `_assemble_context`, #1632-statused. Block contract (mirror `_shape_valuation`, `thesis.py:486`): `{available: bool, reason, quality_status, bear/base/bull, as_of_date, ttm_end, basis}`; absent → `{available:false, reason}` (context reason enum, distinct from the storage enum). `_WRITER_SYSTEM` gains a **passive** rule: *"deterministic valuation-band evidence — a mechanical prior; ground your targets against it and explain any large gap; when absent or `quality_status:low`, rely on your own judgement."* NOT "stay within it." Conditioned on `available:true` (else it would contradict the #1632 availability-mirror rule for the ~8,700 no-band names). State the band vs the existing `price_anchor`/52-week rules as a **hierarchy**, not two peer "justify if outside" constraints. Bump `_PROMPT_VERSION` (see §10 sequencing vs #2010).
2. **#2007 divergence — MEASURE only, NULL-safe.** In the same atomic thesis insert (band is in context, writer output validated pre-insert — write-once, no post-hoc UPDATE of the append-only row), record a `thesis_valuation_audit` row (insert-once, FK `theses.thesis_id`): snapshot `band_base`, band `method_version`, `price_as_of`, `band_quality_status`, and `divergence_pct = |llm_base − band_base| / band_base`, `divergence_flag = divergence_pct > DIVERGENCE_THRESHOLD`. **band_base NULL (the common ~8,700 path) → divergence_pct/flag NULL, never 0/false** (#1632). The snapshot makes a past thesis's divergence reconstructable though the live band is mutable. Audit-row (not `theses` columns) keeps `theses` append-only clean and is the extensible home for the v2 signals (e.g. `band_vs_market_extreme`). v1 does NOT raise on divergence (`_validate_writer_output` stays a hard coherence-only gate); the flag feeds QA + the critic.
3. **Scoring value family — OUT of scope** (operator-gated, `model_version`). Wiring a band discount into `_value_score` = a cohort-derived scoring input crossing the "no cohort-relative normalization" line → fresh sign-off. Not this spec.

**v2 fast-follow (data-driven, separate issue):** once divergence is measured, tighten the writer rule to "anchor unless specifically justified" for `quality_status:high` bands, add EV/EBITDA, and feed #2010 from the accumulated audit history.

## 8. Invariants preserved (settled-decisions cross-ref)

- **"No cohort-relative normalization"** preserved — that ban is on normalizing *scores*; a peer-median *multiple* is a valuation anchor, never enters `_value_score` here. If #2010 later feeds a band discount into scoring, that crosses the line → `model_version` + operator sign-off.
- Heuristic, explicit, auditable; no ML; same inputs → same band.
- Thesis rows append-only; buy zone only on `buy`; critic in `critic_json` (the audit row is neither critic nor a `theses` mutation).
- Long only v1; protective EXIT never gated by valuation.
- #1632 discipline; #2008 strict-TTM sole TTM source; `resolve_market_cap_basis` sole cap authority.

## 9. Testing

- **Pure-policy tests in a SEPARATE module** `tests/test_fair_value_band_policy.py` (fast tier — `conftest.py:322` auto-`db`-marks any module referencing a DB fixture, so pure tests must not): synthesis blend + outer envelope; single-comparator degradation; per-share conversion P/E·P/S·P/B; dual-class target → P/E-only intersection; `currency_mismatch` fail-closed (pure fn over two currency strings — 0/3,047 gives no natural DB fixture); cohort ladder SIC-4=7→widen while SIC-3=8 stops; MIN_PEERS 7-vs-8 + MIN_OWN_POINTS 5-vs-6 boundaries; percentile n=1; zero-variance percentile; cohort all-dual-class → empty after anti-join → comparator absent (no crash); one-comparator-NULL envelope; `bear≤base≤bull` invariant; `band_quality_status` tiers; pure `compute_divergence(llm_base, band_base, threshold)` incl. `band_base=None`→NULL and `llm_base=NaN`→None (no ZeroDivisionError); pure `_shape_fair_value_band(row|None)` statused-absent shape.
- **Golden-value test:** freeze the §3 AAPL band ($258/$285/$305 @ eps 8.26) as a fast-tier drift guard.
- **One integration test** (db tier) for the two-pass cohort + own-history SQL against seeded rows, **including a seeded curated dual-class cohort member** proving the oracle anti-join keeps its P/S/P/B out of the medians (BLOCKING regression guard) — plus a fast-tier `filter_dual_class(rows, dual_class_ciks)` pure twin so the guard also gates pushes.
- **Dev-verify** panel AAPL/GME/MSFT/JPM/HD: operator-window-gated (dev market-data staleness → most dev rows may be `stale_price`-absent; the panel verifies plumbing + basis_json, correctness rides the pure tier).

## 10. Rollout / definition of done

**Two PRs, hard-ordered** (the split IS the hourly-thesis-refresh race fix — PM lens):
- **PR-A "band compute+store":** schema (two-layer) + pure service + pure tests + IO wrapper + DAG layer + bootstrap stage + backfill + dev-verify. Satisfies DoD 8–12 standalone (band renders; `SELECT` over `fair_value_band_current` for the panel; one figure cross-checked vs gurufocus/marketbeat).
- **PR-B "thesis consumer":** passive context block + `_PROMPT_VERSION` bump + `thesis_valuation_audit` divergence measurement. **Hard-depends on PR-A backfill fully drained** before the prompt-version jobs-restart (else the hourly `thesis_refresh` regenerates theses at the new version against an absent band and won't re-anchor until their next natural refresh).
- **Sequence:** merge A → restart jobs → band bootstrap drains → merge B → restart jobs. Operator restarts the VS Code jobs task before backfill (#2008 write-through rule).
- **`_PROMPT_VERSION` collision:** #2010 also intends v4. Sequence: this ships the next free version; whichever lands second takes the following. (Resolve the literal in PR-B.)

**Full-population verification — safety, not smoke** (single queries over `fair_value_band_current`): dual-class targets → P/E-only (cross-check vs the curated roster); `currency_mismatch` count (expect 0); P/B looser-population count vs flow multiples; peer stale-exclusion distribution (`excluded_stale_n/cohort_n`); `quality_status` distribution; band-eligible-but-no-band bucketed by `reason` (must be explainable).

## 11. Resolved log + remaining open

**Codex ckpt-1 — folded:** dual-class cohort oracle anti-join, dual-class target gate, EV/EBITDA strict-D&A (now moot in v1 — EV/EBITDA deferred), currency fail-closed gate, peer-member freshness hard gate, scoring-boundary note, full-pop verification.

**Committee (8 lenses) — folded:** DAG-layer topology (§6), two-layer append+current storage (§5), pure two-pass compute (§4.3/§4.5/§6), NULL-safe divergence + snapshot audit row (§7.2), band_quality_status + passive-evidence framing (§4.7/§7.1), single as-of + lookahead-before (§4.3/§4.4), sic index (§6), anti-join `is_primary` (§4.3), per-instrument savepoint (§6), observability rollup (§6), method_version re-fire (§6), pure/IO boundary (§6), population-N clarity (§3), §4.1-as-universal-precondition (§4.1), sibling-model note / audit-row home (§7.2), context-block contract (§7.1), "anchor" hierarchy (§7.1), PR-A/PR-B split (§10). EV/EBITDA → v2 (operator scope). 0 hallucinations (Adversarial).

**Remaining open (plan-time / v2):**
1. Calibration constants — `MIN_PEERS=8`, `MIN_OWN_POINTS=6`, `PRICE_STALE_DAYS`, `PEER_STALE_DAYS`, `DIVERGENCE_THRESHOLD`, `band_quality_status` tier thresholds, percentile choices (peer p25/50/75 vs own p20/50/80). Pin against full-pop distributions in PR-A before hard-coding.
2. Multiple-per-profile mapping (§4.2) — ratify; is P/S-on-every-profitable-name right, or P/E-only?
3. Cross-multiple combine (§4.5) — median-of-bases + envelope vs coverage-weighted.
4. `peer_comparison` (eToro-sector) vs this (SIC) — two cohort keys now coexist; file the `peer_comparison`→SIC re-key as a follow-up so they don't drift.
5. v2: EV/EBITDA (+ own-history EBITDA question), the "anchor unless justified" tightening, #2010 coupling on accumulated audit history.
