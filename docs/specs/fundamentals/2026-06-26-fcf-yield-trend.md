# FCF yield trend on the fundamentals drill page (#671)

Parent epic: #585. Predecessor: #589 (shipped the absolute FCF line).
Spec reviewed by Codex ckpt-1 (2026-06-26); 5 findings incorporated (currency
guard, abs-capex breadth, TTM continuity, Decimal wire type, full-pop verify).

## Goal

Overlay an **FCF yield** trend on the existing absolute-FCF line on
`/instrument/:symbol/fundamentals`. Operator reads "is this getting cheaper on
cash flow?" at a glance. Tooltip shows both the quarterly FCF and the TTM yield.

## Source rule

1. **FCF = `operating_cf − |capex|`.** capex sign convention varies between
   filers (prev-log "XBRL CapEx sign convention varies", #177) → `abs`. The TTM
   view uses `ABS(SUM(capex))` (`sql/080:78-85`); the FE per-period builders do
   raw subtraction (`fundamentalsMetrics.ts:246` FCF-YoY, `:454` buildFcf;
   `dividendsMetrics.ts:153` payout FCF) — **fix all three** (prev:596 "apply to
   both latest- and historical-snapshot builders").
2. **Market cap = total-company basis, not `price × combined_shares`.**
   Dual-class `close × combined_shares` is the figure #1662 retired (prev-log
   "A SQL view cannot reproduce a fail-closed multi-step policy", #1664; data-eng
   I20; `resolve_market_cap_basis` `app/services/xbrl_derived_stats.py:426`).
   Fail-closed Python policy → cannot be reproduced client-side (issue's
   "option A client-side join" rejected). Single-class (`not_multiclass`):
   `period_end_close × period_end_shares` IS correct.
3. **Yield is TTM.** Settled metric is TTM (`instrument_valuation.fcf_yield`).
   Issue left quarterly semantics open → source-rule picks **trailing-4Q
   consecutive** (mirrors `financial_periods_ttm` `sql/032:209-228`:
   4 normalized quarters, `is_complete_ttm = COUNT(*)=4`). Annual periods use FY
   FCF directly.
4. **Currency: FCF and price must share a currency.** `financial_periods.
   reported_currency` (sql/032:121, reporting currency) vs `instruments.currency`
   (sql/001:6, eToro trading currency) — `sql/024:27-30` warns they differ for
   non-US issuers and "must add explicit currency normalisation before
   cross-currency comparison." v1 has no FX normaliser → **fail-closed suppress
   on mismatch** (same posture as dual-class).

## Scope (operator-approved 2026-06-26): single-class + USD-coherent now; suppress the rest

Suppress the yield overlay (keep the absolute FCF line + a caveat) when EITHER:
- `resolve_market_cap_basis().basis != "not_multiclass"` (any curated multi-class
  issuer — incl. `total_company`, since v1 does **no** per-period per-class price
  reconstruction, so its naive `combined_shares × this-class close` per-period
  cap would be the retired distortion); OR
- `reported_currency != instruments.currency` (cross-currency FCF/price).

**Out of scope → follow-up ticket (filed at impl):** per-period dual-class
reconstruction (Σ sibling `price_daily` × per-class FSDS shares + residual
imputation + per-period guards) AND cross-currency FX normalisation.

## Backend

### Endpoint

`GET /instruments/{symbol}/fcf-yield?period=quarterly|annual`
(`app/api/instruments.py`, sibling of `/financials` `:698`). Read-only; no
migration; reads existing `financial_periods` + `price_daily`.

```python
class FcfYieldPoint(BaseModel):
    period_end: date
    period_type: str               # Q1/Q2/Q3/Q4 (quarterly) | FY (annual)
    fcf_ttm: Decimal | None        # trailing-4Q (quarterly) or FY (annual); ABS(SUM capex); null if continuity-incomplete
    market_cap: Decimal | None     # period_end_shares × period_end_close; null if either missing
    fcf_yield_pct: Decimal | None  # fcf_ttm / market_cap × 100; null if fcf_ttm null OR market_cap null/≤0
    price: Decimal | None          # close at/before period_end
    price_as_of: date | None       # the price_daily.price_date actually used

class FcfYieldSeries(BaseModel):
    symbol: str
    suppressed_reason: Literal["multiclass", "currency_mismatch"] | None
    points: list[FcfYieldPoint]    # [] when suppressed_reason is not None
```

### Service (`app/services/xbrl_derived_stats.py`, new `fcf_yield_series` helper)

1. Resolve `instrument_id` from `symbol` (same path `/financials` uses).
2. `resolve_market_cap_basis(conn, instrument_id=iid)`; if
   `basis != "not_multiclass"` → `suppressed_reason="multiclass", points=[]`.
3. Currency gate: latest `financial_periods.reported_currency` (per the
   `MAX(...) FILTER (rn=1)` pattern, sql/032:267) vs `instruments.currency`;
   mismatch → `suppressed_reason="currency_mismatch", points=[]`.
4. **Quarterly** — single window query over `financial_periods` mirroring
   `financial_periods_ttm` semantics, but per-period:

```sql
WITH q AS (
    SELECT instrument_id, period_end_date, period_type, shares_outstanding,
           operating_cf, capex,
           SUM(operating_cf) OVER w  AS ocf_ttm,
           SUM(capex)        OVER w  AS capex_ttm,
           COUNT(*)          OVER w  AS n_q,
           MIN(period_end_date) OVER w AS ttm_start
    FROM financial_periods
    WHERE instrument_id = %(iid)s AND superseded_at IS NULL
      AND normalization_status = 'normalized'
      AND period_type IN ('Q1','Q2','Q3','Q4')
    WINDOW w AS (PARTITION BY instrument_id ORDER BY period_end_date
                 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT q.period_end_date, q.period_type, q.shares_outstanding,
       CASE WHEN q.n_q = 4                                  -- 4 consecutive quarters present
             AND q.period_end_date - q.ttm_start <= 330     -- gap guard: consecutive ~275d, missing-quarter ~365d
            THEN q.ocf_ttm - ABS(COALESCE(q.capex_ttm, 0))  -- mirror sql/080:78 ABS(SUM)
            ELSE NULL END AS fcf_ttm,
       pd.close AS price, pd.price_date AS price_as_of
FROM q
LEFT JOIN LATERAL (
    SELECT close, price_date FROM price_daily
    WHERE instrument_id = q.instrument_id
      AND price_date <= q.period_end_date AND close IS NOT NULL
    ORDER BY price_date DESC LIMIT 1
) pd ON TRUE
ORDER BY q.period_end_date DESC
LIMIT 20
```

   **Annual** — same but `period_type='FY'`, `fcf_ttm = operating_cf -
   ABS(COALESCE(capex,0))` per row (FY is already 12 months; no window).
5. Per row: `market_cap = shares_outstanding × price` when both present else
   null; `fcf_yield_pct = fcf_ttm / market_cap × 100` when `fcf_ttm` not null and
   `market_cap > 0` else null. Extract this final arithmetic into a **pure
   function** `_fcf_yield_pct(fcf_ttm, market_cap)` for table-testing (test-quality
   skill: prefer pure policy).

## Frontend

### abs(capex) fix (prev:596) — 3 sites

`fundamentalsMetrics.ts:246` (FCF-YoY), `:454` (buildFcf), `dividendsMetrics.ts:153`
(payout FCF): `operating_cf - capex` → `operating_cf - Math.abs(capex)`. Update
each comment. (Line/YoY/payout correctness; the yield's TTM FCF is server-side.)

### Types + fetcher (api-shape-and-types skill)

- `types.ts`: `FcfYieldPoint` + `FcfYieldSeries`. **Decimal → `string | null`**
  on the wire (`fcf_ttm`, `market_cap`, `fcf_yield_pct`, `price`), per the repo
  contract (types.ts:469 "Decimal|None → JSON string|null, never coerce to number
  until the chart boundary"). `suppressed_reason: "multiclass" |
  "currency_mismatch" | null`.
- `instruments.ts`: `fetchFcfYield(symbol, {period})`.

### Chart + page

- `fundamentalsCharts.tsx:504` `FcfChart` → dual-axis `ComposedChart` (template
  `DebtStructureChart:379-410`): absolute quarterly FCF on left Y (currency,
  existing `buildFcf` line), `fcf_yield_pct` on right Y (%). Coerce the wire
  `string` → number at this chart boundary only. Yield line `theme.accent[*]`
  via `useChartTheme()` (never `lightTheme`, prev:1671). Shared `ChartTooltip`
  (#1601): period, **FCF (quarter)**, **FCF yield (TTM)** — labelled distinctly
  so the quarterly line and TTM yield don't read as the same number.
- `FundamentalsPage.tsx:272` (comment already flags #671): 4th `useAsync`
  (`fetchFcfYield`, independent lifecycle per async-data-loading skill), join
  `fcf_yield_pct` by `period_end` into the chart series.
- `suppressed_reason != null` → FCF line only + caveat
  (`"FCF yield unavailable for multi-class issuers"` /
  `"...when reporting and trading currencies differ"`). Loading/empty/error per
  loading-error-empty-states skill (each `useAsync` owns one surface).

### Tests

- **pytest (pure):** `_fcf_yield_pct(fcf_ttm, market_cap)` table test — positive,
  negative-FCF (negative yield renders, not clamped), null/zero/negative
  market_cap → null.
- **vitest:** the abs(capex) fix in all 3 builders; the server-points →
  chart-series `period_end` join; suppression caveat rendering; empty-state gap
  on null `fcf_yield_pct`.

## Full-population verification (not a sample)

Suppression safety is verified on the FULL population, not the smoke panel:

- `SELECT DISTINCT source_cik FROM instrument_class_shares_outstanding` → for
  every mapped instrument, assert the endpoint returns
  `suppressed_reason="multiclass"` (no curated multi-class issuer ever emits a
  naive yield).
- `SELECT instrument_id FROM financial_periods fp JOIN instruments i USING
  (instrument_id) WHERE fp.reported_currency <> i.currency` (latest period) →
  assert `suppressed_reason="currency_mismatch"` for each.
- Single-class sample (AAPL, MSFT, JPM, HD): yield renders; **GOOGL**:
  suppressed+caveat. Cross-source one: AAPL latest TTM yield vs gurufocus +
  `instrument_valuation.fcf_yield` (same order; small drift expected — ours uses
  period-end close not live quote).
- **Dev-verify** live `/instrument/AAPL/fundamentals` (overlay + tooltip both
  figures) + `/instrument/GOOGL/fundamentals` (suppressed+caveat), light + dark.
- No backfill/rebuild (read-only over existing tables).

## Note on scope creep

Issue #671 was filed as "the simplest drill (single metric)". The source-rule pass
revealed it carries the most correctness weight in the cluster (dual-class cap +
currency + TTM-continuity + capex-sign). The v1 fail-closed posture
(suppress-on-uncertainty) keeps the **shipped operator surface** simple while
staying correct; the deferred reconstruction work is isolated to the follow-up.
