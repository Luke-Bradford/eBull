# SEC-derived total return + coverage-gated Calmar reward (scoring v1.3)

Closes #1635 (total-return series) + #1633-vnext (Calmar return-ratio reward).
Reverses the #1635 "vendor-blocked" deferral — premise falsified: the per-share
dividend amounts are already in `financial_facts_raw`, SEC-derived, license-clean.

## Source rule

**Total return** = price return + reinvested cash dividends (CRSP / Yahoo
"adjusted-close" convention): each cash dividend `D` with ex-date `t` is
reinvested at the close on `t`, so the total-return index
`TRI_t = TRI_{t-1} · (P_t + D_t)/P_{t-1}`. Splits are already handled — eToro
delivers split-adjusted closes (settled, #1635 research). The only missing input
is the cash-dividend series.

**Per-share dividend amounts** (SEC, public-domain): the documented SEC source
rule is the **concept identity** — us-gaap per-share dividend/distribution
concepts. We REUSE the already-curated, ticket-justified alias groups in
`app/providers/implementations/sec_fundamentals.py::TRACKED_CONCEPTS`:
- `dps_declared` = `CommonStockDividendsPerShareDeclared` + the LP/LLC/member
  `DistributionMadeTo…DistributionsDeclaredPerUnit` concepts +
  `DistributionsPerLimitedPartnershipUnitOutstanding` (#674/#682) — captures
  REIT-OP / MLP / BDC issuers (O, MAIN, ET, EPD) that do NOT tag CommonStock.
- `dps_cash_paid` = `CommonStockDividendsPerShareCashPaid` + the parallel
  `…CashDistributionsPaidPerUnit` concepts.

Imported lazily inside the loader (avoids the provider import chain, mirrors the
#1632/#1633 lazy-import pattern). Do NOT re-hardcode the two bare concepts —
that would silently drop the pass-through high-yield names this feature targets.

Each fact is a **duration fact** (`period_start` → `period_end`). The fact's
duration is mapped to a frequency tier — this mapping is an **empirical
classification** (validated on the full-population duration histogram below), NOT
a first-principles SEC rule; the SEC rule is the concept identity above:
- monthly  ≈ 25–45 d
- quarterly ≈ 75–100 d
- annual   ≈ 350–380 d
- everything else (≈180 d H1, ≈270 d 9-mo, and the FY-cumulative
  `DistributionsPerLimitedPartnershipUnitOutstanding`) is a **cumulative** that
  overlaps its constituents — excluded from the fine tiers, kept only at annual.

We do NOT use `dividends_paid / shares` (YTD-cumulative, bundles preferred stock),
nor `instrument_dividend_summary.ttm_dps` (garbage: EFC=1170). `has_dividend` from
`instrument_dividend_summary` is used ONLY as the non-payer flag.

Ex-date is approximated by `period_end`. This is an approximation (true ex-dates
are days–weeks off), accepted for a **long-horizon** engine: over multi-year
windows the cumulative reinvested cash is approximately right even when individual
dates are off, and the coverage gate + `tr_status` keep the reward off any series
we cannot reconstruct confidently. We never claim daily-exact TR.

## Full-population verification (dev DB, 2026-06-19)

- Units: 66,281 `USD/shares` vs 16 `pure` → filter `unit='USD/shares'`.
- `has_dividend`: 1,539 True / 394 False (of 12,546 tradable).
- 1,528 instruments have ≥1 quarterly (75–100 d) DPS fact ever.
- Trailing-3y quarterly coverage: **1,497** payers have ≥1 quarterly fact;
  1,215 have ≥8; only **238** have ≥11. → A fixed "≥11/12 quarters" gate
  qualifies only 238 names (window-edge + filing-lag sensitive). The gate must be
  **span-relative**, not a fixed count (see Coverage gate).
- Frequency tiers confirmed on real names: AAPL/KO/AGNC/HD/JPM report clean
  **quarterly** facts; **O (Realty Income)** is **monthly** (204 monthly facts vs
  6 quarterly) — quarter-only banding would erase the highest-yield names, exactly
  the names TR matters most for. → tiered selection (monthly→quarterly→annual) is
  required, not quarter+annual only.
- Heavy duplication across accessions (KO FY 1.940 ×3, AAPL Q2 0.26 ×2) →
  dedup by `(period_start, period_end)` to the modal value.
- Outliers: after modal-dedup, JPM's 0.05 (2010) values are a **real** post-crisis
  cut (kept — the guard is HIGH-side only); MAIN is clean. The outlier guard
  targets mis-tagged cumulatives in the wrong band, not legitimate specials.
- Cross-source yields sanity-check (prior research): KO 2.8%, VZ 5.8%, O 4.9%,
  JPM ~3% — all match published yields.

## Design

### 1. Dividend stream (pure, testable without DB)

`load_dividend_facts(conn, instrument_id) -> list[DivFact]` — raw SELECT of
`(period_start, period_end, val)` for the two concepts, `unit='USD/shares'`,
`val IS NOT NULL`, `period_start IS NOT NULL`. No transform in SQL.

`select_dividend_stream(facts) -> list[tuple[date, Decimal]]` — pure:
1. **Dedup** to modal value per `(concept_group, period_start, period_end)`
   (tiebreak: max). `concept_group ∈ {declared, cash_paid}`.
2. **Classify** each into a tier by duration: monthly 25–45, quarterly 75–100,
   annual 350–380; drop all others (cumulatives).
3. **Outlier guard (HIGH-side only, global two-pass)**: compute the median of ALL
   deduped per-tier values FIRST (position-independent — fixes the "first-few
   seed the median" hole), then drop any period whose value exceeds
   `OUTLIER_MULT × global_tier_median` (default `OUTLIER_MULT = 5.0` — generous;
   targets a cumulative mis-tagged into a fine tier, keeps real specials and real
   cuts). A stream with < 3 periods skips the guard (no stable median).
4. **Dominant concept group** — pick the group (`declared` vs `cash_paid`) with
   more usable periods (tiebreak `declared`); never mix them. This resolves the
   **308 instruments reporting BOTH for the same period** (full-pop verified) AND
   the offset-period-bound case (cash_paid lags declared by a quarter) in one
   move, with no cross-group overlap arithmetic.
5. **Residual tier-fill, finest-first** (monthly → quarterly → annual). Accept
   fine periods; for each coarser fact accept only the **residual** not already
   covered by finer periods inside its span: `annual − Σ(YTD quarters) = the
   implied Q4` the issuer never tags separately. This is the documented SEC
   FY=YTD+Q4 treatment (#682) and is REQUIRED — the dominant US pattern is 3×10-Q
   + one 10-K with no standalone Q4 fact (verified: AAPL/KO/JPM/HD/MSFT all have 9
   quarterly + 3 annual facts per 3y; a plain reject-overlap drops the annual and
   loses Q4, giving 9/12 coverage = wrong tr_incomplete). A residual below
   `_DIV_RESIDUAL_EPS` (0.005/sh) is treated as fully covered (rounding noise).
6. Return `(period_end, dps)` ascending — `period_end` is the reinvest ex-date.

### 2. Total-return index + TR-CAGR (pure, in `risk_metrics.py`)

`total_return_index(closes, dividends) -> list[PricePoint]` — Decimal end-to-end,
mirrors the module's existing valid-close discipline. Walk valid closes ascending,
`shares = 1`; on the first close ≥ a dividend's ex-date, reinvest
`shares += shares · dps / close`; emit `(date, shares · close)`. Non-payers /
empty dividends → returns the price series unchanged (TR == price return, exact).

`tr_cagr` per window = `cagr(total_return_index(window-sliced closes,
window-sliced dividends))`. Reuses the existing `cagr()` — same calendar-time
formula, no new return math. `tr_calmar = tr_cagr / abs(max_drawdown)` reusing the
existing `calmar()` guard and the **existing price-based `max_drawdown`** as
denominator (dividends move drawdown only marginally; the issue scope is "add the
TR-CAGR numerator").

### 3. `tr_status` (per window, mirrors the existing `*_status` pattern)

`TrStatus ∈ {ok, tr_incomplete, no_dividends}` (added to a new
`TR_STATUSES` frozenset; the existing `RiskStatus` union is NOT widened — these
are a distinct status axis):
- **no_dividends** — `has_dividend = False` AND no usable dividend facts. TR ==
  price return, exact → trustworthy. `tr_cagr` = price `cagr`.
- **ok** — usable dividend stream that covers the window with no internal gaps.
  Two checks (both must pass):
  1. **Coverage** `observed_periods ≥ COVERAGE_MIN × expected_periods`
     (`COVERAGE_MIN = 0.80`), where `expected_periods = active_span_days /
     tier_days` (tier_days = 30.44 / 91.25 / 365 by the stream's modal tier).
     **`active_span_days` is anchored on the INITIATION date** =
     `max(window_start, first_EVER_period_end)` — NOT first-period-in-window.
     Using first-ever distinguishes a genuine mid-window initiation (first-ever
     inside the window → expected shrinks → ok) from a long-time payer whose
     early window is MISSING due to a data gap (first-ever before the window →
     expected = full window → sparse early → fails). (Codex ckpt-1 HIGH.)
  2. **No internal gap** — no consecutive `period_end` gap inside the covered span
     exceeds `1.5 × tier_days` (catches a dropped mid-window filing even when the
     count happens to clear the ratio).
  3. **No terminal staleness** (Codex ckpt-1 re-review MED) — `as_of −
     last_period_end ≤ TERMINAL_MULT × tier_days` (`TERMINAL_MULT = 2.0`, allows
     one unfiled current quarter + filing lag). Without this, a payer missing the
     latest 1–2 periods can clear the ratio with no "internal" gap. Conservative:
     a true dividend cut also trips this → tr_incomplete → price-Calmar fallback
     (which is correct for a stopped payer anyway).
  Single-period streams: ok only if that period is within one tier-interval of
  `as_of` (a current payer that just initiated).
- **tr_incomplete** — facts exist but coverage/gap checks fail. `tr_cagr` is still
  emitted (understates TR — conservative) but the reward must not trust it.

Conservatism note: a missed dividend understates TR-CAGR → a smaller reward, never
a false-large one — EXCEPT a mis-tagged cumulative leaking into a fine tier, which
the global-median outlier guard (above) is responsible for; the gate is not. The
gate exists to keep the reward off sparse / gappy series.

### 4. Schema (additive-nullable under stable `risk_v1` — settled-decision blessed)

`sql/203_instrument_risk_metrics_total_return.sql`: `ADD COLUMN IF NOT EXISTS` on
BOTH `instrument_risk_metrics_observations` and `instrument_risk_metrics_current`:
- `tr_cagr NUMERIC`
- `tr_calmar NUMERIC`
- `tr_status TEXT`
- `tr_n_periods INTEGER` (dividend periods used in the window — audit)

NO `metric_version` bump (per settled-decisions §"Additive-nullable evidence under
a stable metric_version is blessed"). Pre-existing observation rows keep NULL;
`tr_status` NULL distinguishes "not computed then" from a computed value. Appended
to `_RISK_BUSINESS_COLS` (order is load-bearing — append only).

### 5. Scoring v1.3 (Calmar return-ratio reward)

New `v1.3-{balanced,conservative,speculative}` weight modes — **identical family
weights to v1.2** (the reward is additive, like the v1.2 penalty was; family
weights untouched so v1/v1.1/v1.2 history is preserved). v1.3 = v1.2 penalties +
the Calmar reward.

**Gate widening (Codex ckpt-1 HIGH):** the two existing `model_version.startswith`
gates must include v1.3 or v1.3 silently loses v1.2 behavior — TA momentum is
gated `startswith(("v1.1","v1.2"))` ([scoring.py:1171]) and the realized-risk
penalty `startswith("v1.2")` ([scoring.py:1246]). Both widen to include `"v1.3"`.
Use a module-level `_TA_PREFIXES`/`_RISK_PENALTY_PREFIXES` tuple so the next
version is a one-line add, not a scattered string edit.

Reward (additive, mirrors the penalty tier structure; settled-decisions: additive
not multiplicative):
- Reads `tr_cagr, tr_calmar, tr_status, max_drawdown` from the **3y** risk row
  (same window the penalty uses).
- **reward basis** (the gate keeps the reward off the untrusted *TR series*, not
  off rewards entirely — Codex re-review MED): `tr_status ∈ {ok, no_dividends}` →
  reward from `tr_calmar` (for no_dividends `tr_calmar == price calmar`, exact);
  `tr_status = tr_incomplete` → the TR series is untrusted, so fall back to a
  reward computed from the existing **price-based `calmar`** column (trustworthy on
  its own terms — a dividend-blind price-return ratio) + a `tr_incomplete` caveat
  note. Price-Calmar fallback understates a high-yielder → smaller reward →
  conservative.
- Tiered additive reward, **EXTREME tested first** (if/elif, mirrors the penalty
  block so the extreme tier is reachable — Codex ckpt-1 HIGH):
  `calmar > CALMAR_EXTREME → +REWARD_EXTREME` `elif calmar > CALMAR_HIGH →
  +REWARD_HIGH`. Strict comparators. Absence (no row / non-trustworthy status with
  no price calmar / NULL) → no reward + a note (honest-absence, never a signal —
  prevention-log).
- **Mode-scaled risk appetite**: the reward is scaled by a per-mode multiplier
  (`conservative` weights risk-adjusted return most → full reward; `speculative`
  least → reduced) — small explicit constants, calibrated post-backfill.
- **Reward modeling (Codex ckpt-1 LOW):** a parallel `rewards: list[RewardRecord]`
  on `ScoreResult` (NOT a negative `PenaltyRecord` — that corrupts `total_penalty`
  / "penalties fired" / the JSON). `total_reward = sum(r.addition)`.
  `total_score = clip(raw_total − total_penalty + total_reward)`. Persisted by
  tagging entries in the existing `penalties_json` with `kind ∈ {penalty,reward}`
  (additive to the JSON shape; no `scores` schema change — verified: `scores` has
  only `penalties_json jsonb`). `total_penalty` stays penalties-only.

`_DEFAULT_MODEL_VERSION` flips `v1.2-balanced → v1.3-balanced` — single source
(one constant in `scoring.py`; grep the freshness/api/portfolio consumers that
pin the default and confirm they read the constant, as #1633 did for v1.1→v1.2).

### Thresholds — calibrated post-backfill

`CALMAR_HIGH / CALMAR_EXTREME / REWARD_HIGH / REWARD_EXTREME` and the per-mode
multipliers are set against the **full-population tr_calmar distribution** after
the dev backfill (same PR — #1633 precedent: calibrate to the universe tail, not
SPY-like absolutes). Placeholders in code until the backfill prints the
distribution; final values + the percentile basis recorded in the PR.

Before relying on the stream logic, the impl runs three **full-population** scans
(not samples — Codex ckpt-1 MED) and records the counts in the PR: (a)
both-concepts-same-period conflicts (initial scan: 308); (b) HIGH-side outlier
incidence at `OUTLIER_MULT = 5.0`; (c) fine/annual overlap rejections. If any scan
surprises, the constant is revisited before merge.

## Binary-gate vs shrinkage

**Binary gate + status** (chosen, per operator). Codex's shrinkage alternative
(`tr_cagr = price_cagr + coverage · uplift`) is rejected: less auditable (a
continuous fudge factor in the score) and the binary gate's misclassifications are
all conservative anyway. The status column makes the gate inspectable per
instrument.

## Out of scope / follow-ups

- Per-class dividends for dual-class issuers (DPS can differ per class) — the
  instrument's own close + its own concept facts are used; note as a known limit.
- Daily-exact ex-dates (would need a corporate-actions vendor — the original
  deferral; not reintroduced).
- RiskPage TR display surfacing — backend emits the columns; FE round is a
  follow-up (this PR is backend + scoring, mirrors #591 PR-B/PR-C split).

## Tests (pure-logic first)

- `select_dividend_stream`: modal dedup; tier classification; YTD-cumulative
  exclusion; monthly+annual non-overlap (O shape); quarterly+annual non-overlap
  (AAPL shape); HIGH-side outlier drop; real-cut retention (JPM 0.05).
- `total_return_index`: non-payer == price series; single dividend reinvest math;
  known closed-form on a tiny series.
- `tr_status`: no_dividends; ok (full coverage); tr_incomplete (gap); start-mid-
  window → ok; single-period edge.
- scoring v1.3: reward fires on ok/no_dividends; falls back to price calmar +
  caveat on tr_incomplete; mode-scaling; honest-absence → no reward; v1.2 history
  unchanged (regression).

## DoD (clauses 8–12)

8. Smoke panel exercised: AAPL, KO (quarterly), O (monthly), GME (non-payer), JPM.
9. Cross-source: a high-yielder (KO or O) TR-CAGR vs published total return; a
   non-payer (GME) TR-CAGR == price CAGR.
10. Backfill: `compute_and_store_risk_metrics` run on dev (or sec_rebuild-free
    risk recompute) — tr_cagr/tr_status populated; print full-pop tr_calmar
    distribution → set thresholds.
11. Live `/instruments/{symbol}/risk-metrics` renders tr_* ; `/scores` (v1.3)
    shows the reward in penalties_json + a fresh ranking run.
12. PR records each verification + commit SHA.
