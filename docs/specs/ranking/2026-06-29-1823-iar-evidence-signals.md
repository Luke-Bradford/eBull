# #1823 — Evidence-only scoring signals into the IAR (P2 of #1815)

**Status:** spec · **Type:** backend analytics, evidence-only · **Risk:** zero to live scoring (all new signals enter the headline at **weight 0**; `total_score` math unchanged).

## Goal

Compute + persist five evidence signals per instrument per scoring run, each with
presence / as-of / source flags and honest caveats, into the **Instrument
Analytical Record (IAR)** — which is the append-only `scores` row (#1820), not a
separate table. Signals: **Piotroski F**, **Altman Z″**, **insider net 90d**,
**13F QoQ**, **short-interest**, plus the **hybrid peer grade**. No live-score
change; promotion to a non-zero headline weight is #1822/P5 (gated on the §8
backtest + operator sign-off).

## Source rule

- **Piotroski F (0–9):** Piotroski, *Value Investing: The Use of Historical
  Financial Statement Information to Separate Winners from Losers*, J. Accounting
  Research 38 (2000). 9 binary points across profitability (ROA>0, CFO>0, ΔROA>0,
  CFO>NI), leverage/liquidity (ΔLeverage<0, ΔCurrentRatio>0, no new shares),
  efficiency (ΔGrossMargin>0, ΔAssetTurnover>0). 7/9 need prior-year data. Bands:
  ≥7 strong / 4–6 neutral / ≤3 weak.
- **Altman Z″ (non-manufacturer / EM recalibration), Altman 2000:**
  `Z″ = 6.56·X1 + 3.26·X2 + 6.72·X3 + 1.05·X4`, X1=(CA−CL)/TA, X2=RE/TA,
  X3=EBIT/TA, X4=Equity/TL. Bands: >2.60 safe / 1.10–2.60 grey / <1.10 distress.
  Single-period (no lag).
- **Financials/insurance suppression:** F & Z assume a current/non-current split
  and inventory turnover that banks/insurers do not report. Suppress when the
  SEC-SIC-derived GICS sector (`resolve_sector_spdr`, #1634) is exactly
  `Financials`. This is the precise signal: the crosswalk folds banks (60xx),
  thrifts, P&C/life insurance (63xx–64xx) into `Financials` BUT carves managed-care
  (SIC 6324) to Health Care — managed-care insurers DO report a current ratio, so
  computing F/Z for them is correct. Flag `quality_signal_na_financials`. When SIC
  is NULL (no `instrument_sec_profile` row) the name is not suppressed but degrades
  honestly via the missing-input guard (a bank has no `AssetsCurrent` → Z″ null,
  never a wrong number).
- **Positioning signals** (SEC Item 403 / Reg disclosure; FINRA short-interest
  reporting): normalized to [0,1], 0.5 neutral, lag-stamped, per #1815 §5. The §5
  formulas are kept; the *inputs* are bound to the data we actually have
  (full-population-verified below), reusing the de-duped read paths:
  - **insider** `0.5 + 0.5·tanh((net_shares / shares_outstanding) / 0.001)`. Reuses
    `insider_transactions.get_insider_summary` → `open_market_net_shares_90d`
    (source-ruled: only Form 4 `txn_code='P'` buy / `'S'` sale — awards/option-
    exercise/gift/tax-withholding excluded). Note `net_$ / mktcap ==
    net_shares / shares_outstanding` (price cancels), so no price lookup is needed.
  - **13F** `0.5 + 0.5·clip(Δaggregate_shares% / 0.10, −1, 1)` over the two most
    recent `period_end`s (caveat: ≤135d stale). **Δ of de-duped aggregate
    institutional SHARES, not raw holder-COUNT** — prevention-log 1866/1873 show
    raw filer-CIK counts are corrupted by manager sub-book/family fanout. Reuses
    `ownership_history.get_ownership_category_totals(..., "institutions")` (#922:
    dedup-before-sum at `(period_end, filer_cik)`, amendments collapsed). <2
    periods → unavailable.
  - **short-interest** `1 − clip((short% − 0.05) / 0.25, 0, 1)`, +0.1 if
    `current_short_interest < previous_short_interest` (falling). `short% =
    current_short_interest / shares_outstanding` — `finra_short_interest_current`
    stores short interest in **shares only, no public-float column**, so the
    denominator is shares-outstanding with an explicit caveat `% shares
    outstanding (public float not ingested); bi-monthly`. `days_to_cover` carried
    as context.
- **Hybrid peer grade** (#1815 §6, decision #2/#4): `0.70·absolute +
  0.30·sector_percentile` per family. Peer key = eToro `instruments.sector`.
  Min-peer fallback: n≥8 sector percentile; 5≤n<8 whole-universe percentile; n<5
  absolute-only + `peer_set_thin`. **Evidence-only in the IAR — the headline
  family score stays absolute** (pure percentile would reverse scoring.py's v1
  "cohort-relative normalization banned" settled decision). The percentile is
  computed over the **run-eligible** population (the same tradable + analysable +
  has-data set `compute_rankings` scores), NOT the full instrument universe;
  `basis` records this explicitly (`run_eligible_sector` /
  `run_eligible_universe`) so the record never overstates the cohort.

## Full-population verification (dev DB, 2026-06-29)

- `financial_facts_raw` (companyfacts, **non-dimensional default member only** —
  prevention-log 1879): 4,170 instruments have 10-K facts; core concepts
  well-covered (Assets 4,741 · NetIncomeLoss 4,653 · CFO 4,725 · StockholdersEquity
  4,584 · RetainedEarnings 4,609 · OperatingIncomeLoss 3,966 · AssetsCurrent/
  LiabilitiesCurrent ~3,930 — the gap is exactly the financials we suppress).
- **`LiabilitiesNoncurrent` = 0 rows** → Piotroski leverage uses `LongTermDebt`
  (2,310) → `LongTermDebtNoncurrent` (1,889) fallback; if absent the ΔLeverage
  component is unavailable (never imputed).
- **Revenue is ASC-606-fragmented** → fallback chain
  `RevenueFromContractWithCustomerExcludingAssessedTax` (2,672) → `Revenues`
  (2,127) → `RevenueFromContractWithCustomerIncludingAssessedTax` (735) →
  `SalesRevenueNet` (205). Revenue-dependent F components (ΔGrossMargin,
  ΔAssetTurnover) are frequently unavailable → emit partial `components_available
  = k/9`, never impute.
- Suppression verified: JPM/C/BAC (SIC 6021) + BRK.B/AIG (SIC 6331 insurance) →
  `Financials` → suppress; AAPL/MSFT/GME/HD → compute. MET (sic NULL) is not
  suppressed but degrades honestly — Z″ needs AssetsCurrent which an insurer
  lacks → null with reason, never a wrong number.
- **Reuse (verified to exist):** `get_insider_summary` (open-market P/S net 90d,
  CIK-deduped, tombstone-excluded), `get_ownership_category_totals("institutions")`
  (#922 de-duped per-period 13F aggregate shares), `resolve_sector_spdr`/
  `instrument_sec_profile.sic` (GICS), `resolve_market_cap_basis` +
  `shares_outstanding`/`market_cap_live` already loaded in scoring's
  `_load_instrument_data`. The only genuinely new DB read is the latest-2-FY
  concept reader for F/Z.

## Schema (sql/210)

```sql
ALTER TABLE scores ADD COLUMN IF NOT EXISTS analytics_json JSONB;
```

Single nullable JSONB evidence column under the **same `model_version`** —
additive-nullable evidence is blessed (settled-decisions: do NOT bump the
version; same blessing as the `risk_v1` / completeness layer). Pre-#1823 rows
keep NULL. Append-only; never mutated.

`analytics_json` shape (all fields nullable; each signal self-describes presence):
```json
{
  "schema": "iar_v1",
  "piotroski": {"score": 7, "components_available": 9, "band": "strong",
                "components": {"roa_positive": true, ...},
                "asof": "2025-09-28", "source": "financial_facts_raw",
                "suppressed": false},
  "altman_z": {"z": 5.81, "band": "safe", "asof": "2025-09-28",
               "source": "financial_facts_raw", "suppressed": false},
  "positioning": {
    "insider_net_90d": {"signal": 0.62, "net_shares": 120000, "shares_outstanding": 1.5e10,
                        "asof": "...", "caveat": null, "source": "insider_transactions"},
    "inst_13f_qoq":   {"signal": 0.55, "delta_shares_pct": 0.04, "asof": "...",
                        "caveat": "<=135d stale", "source": "ownership_institutions_observations"},
    "short_interest": {"signal": 0.80, "short_pct": 0.03, "days_to_cover": 1.2, "falling": true,
                        "asof": "...", "caveat": "% shares outstanding (float not ingested); bi-monthly",
                        "source": "finra_short_interest_current"}
  },
  "peer_grade": {"peer_key": "4", "peer_n": 412, "basis": "run_eligible_sector",
                 "families": {"quality": {"absolute": 0.71, "percentile": 0.88,
                              "hybrid": 0.76}, ...}}
}
```
When a signal can't be computed it is present with `null` value + a `reason`
(e.g. `{"score": null, "reason": "no_prior_year", "components_available": 2}`),
never omitted and never neutral-filled.

## Service

New pure module `app/services/instrument_analytics.py` — all signal math as pure,
table-testable functions (mirrors the `tests/test_scoring.py` pure-logic pattern):

- `piotroski_f(curr, prior) -> PiotroskiResult` — takes two FY fact dicts, returns
  score / components / components_available / band; missing component → not
  counted, never imputed.
- `altman_z2(facts) -> AltmanResult` — single-period; returns z / band, or null +
  reason if any of X1–X4 inputs absent.
- `insider_signal(net_shares, shares_outstanding)`,
  `inst_13f_signal(delta_shares_pct)`, `short_interest_signal(short_pct, falling)`
  — the §5 formulas, each returning signal + the inputs it used.
- `hybrid_grade(absolute, percentile)` — `0.70·abs + 0.30·pct`.
- `percentile_rank(value, population)` — empirical percentile for the peer pass.

DB-facing assembler `assemble_instrument_analytics(instrument_id, conn, *,
gics_sector, shares_outstanding)` — loads the latest two FY annual facts via the
new concept reader (fallback chains), and reuses `get_insider_summary` /
`get_ownership_category_totals("institutions")` (last two periods) /
`finra_short_interest_current`, calls the pure functions, returns the
per-instrument `analytics` dict **without** `peer_grade`. Reads are
savepoint-guarded (catch `UndefinedTable, UndefinedColumn`).

### Integration into scoring.py

1. `compute_score` calls `assemble_instrument_analytics(...)` after family scores,
   stores the per-instrument dict on a new frozen `ScoreResult.analytics: dict |
   None` field. The headline math is untouched (signals never enter `raw_total` /
   `total_score`).
2. `compute_rankings` post-pass (in-memory, no extra queries): group the run's
   `ScoreResult`s by `instruments.sector`, compute per-family sector percentiles
   from the run's own absolute family scores, inject `peer_grade` (with min-peer
   fallback) into each instrument's `analytics` before `_insert_score`. Standalone
   `compute_score` (no run context) emits `peer_grade = {"basis": "absolute_only",
   "reason": "no_run_context"}`.
3. `_insert_score` writes `analytics_json` (Jsonb).
4. Eligibility query adds `i.sector`; `compute_score` is passed `sector`,
   `gics_sector` (resolved from `instrument_sec_profile.sic`), and the
   market-cap basis it already resolves (#1662/#1664) for the insider signal.

DB reads for the assembler are savepoint-guarded (catch `UndefinedTable,
UndefinedColumn` per prevention-log 1941) so a partial schema degrades the signal
to null, never fails the score.

## API

No new endpoint in P2. `analytics_json` is persisted only — the per-instrument
**Verdict** tab that renders it is P3 (#1824). Confirmed not surfaced in
`/rankings` (matches the completeness-column being P4).

## Tests

- `tests/test_instrument_analytics.py` (pure, no DB): Piotroski full-9 / partial /
  all-bands; Altman bands + missing-input null; each positioning formula incl.
  neutral 0.5 + sells-floor + falling bonus; hybrid grade; percentile_rank;
  financials suppression.
- `tests/test_scoring.py`: assert `analytics` rides through `compute_score` →
  `_insert_score` (one mocked-cursor case; pure-logic for the rest).

## Definition of done (ETL/analytics clauses 8–12)

- Smoke panel AAPL/GME/MSFT/JPM/HD: F + Z computed (JPM suppressed); figures
  recorded in the PR.
- Cross-source: one F-score (e.g. AAPL) reconciled against an independent source
  (gurufocus / stockanalysis Piotroski).
- Backfill: run `compute_rankings` (scoring recompute, not `sec_rebuild` — no
  ingest path changed) on dev; confirm `analytics_json` populates.
- Operator-visible: spot-check the stored `analytics_json` for the panel.

## Settled-decisions / prevention-log preserved

- Headline untouched, weight 0 → "no cohort-relative normalization in headline"
  intact; hybrid grade evidence-only.
- Additive-nullable evidence under stable `model_version` (no version bump).
- `financial_facts_raw` = non-dimensional only (1879); concept reachability
  verified on the full population before speccing (1880).
- Savepoint guards catch UndefinedTable+UndefinedColumn (1941).
- Never impute missing fundamentals — partial F with `components_available`.
