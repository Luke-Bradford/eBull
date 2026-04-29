/**
 * Fundamentals derived-metric helpers (#589).
 *
 * Pure functions over the rows shipped by
 * `GET /instruments/{symbol}/financials?statement=...`. Every chart
 * on the L2 fundamentals drill page consumes these so the metric
 * derivation lives in one tested place rather than scattered across
 * chart components — and so the unit tests can pin the formulas
 * (DuPont, ROIC, FCF, YoY) without standing up a chart layout.
 *
 * Calling convention: helpers accept the **chronological** array
 * (oldest → newest, `period_end` ascending). The page sorts once on
 * fetch; everything downstream assumes that order.
 *
 * Null handling: when a required field is missing on a period the
 * helper returns `null` for that period rather than dropping the
 * row, so the chart's time axis stays continuous and the consumer
 * can render "—" / a gap. The recharts components filter nulls at
 * render time.
 */

import type { InstrumentFinancialRow } from "@/api/types";

/** Joined per-period view across all three statements. Every value
 *  is `number | null`; `null` propagates whenever the source row
 *  was missing or non-finite. */
export interface JoinedPeriod {
  readonly period_end: string;
  readonly period_type: string;
  // Income
  readonly revenue: number | null;
  readonly cost_of_revenue: number | null;
  readonly gross_profit: number | null;
  readonly operating_income: number | null;
  readonly net_income: number | null;
  readonly eps_diluted: number | null;
  readonly research_and_dev: number | null;
  readonly sga_expense: number | null;
  readonly interest_expense: number | null;
  readonly income_tax: number | null;
  // Balance
  readonly total_assets: number | null;
  readonly total_liabilities: number | null;
  readonly shareholders_equity: number | null;
  readonly long_term_debt: number | null;
  readonly short_term_debt: number | null;
  readonly cash: number | null;
  // Cashflow
  readonly operating_cf: number | null;
  readonly investing_cf: number | null;
  readonly financing_cf: number | null;
  readonly capex: number | null;
  readonly dividends_paid: number | null;
}

function num(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** Join the three statement responses by `(period_end, period_type)`.
 *  Periods that exist on at least one statement are kept; missing
 *  fields stay null. Output is chronological (oldest → newest). */
export function joinStatements(
  income: ReadonlyArray<InstrumentFinancialRow>,
  balance: ReadonlyArray<InstrumentFinancialRow>,
  cashflow: ReadonlyArray<InstrumentFinancialRow>,
): JoinedPeriod[] {
  const map = new Map<string, JoinedPeriod>();

  function key(r: InstrumentFinancialRow): string {
    return `${r.period_end}|${r.period_type}`;
  }

  function ensure(r: InstrumentFinancialRow): JoinedPeriod {
    const k = key(r);
    let row = map.get(k);
    if (row === undefined) {
      row = {
        period_end: r.period_end,
        period_type: r.period_type,
        revenue: null,
        cost_of_revenue: null,
        gross_profit: null,
        operating_income: null,
        net_income: null,
        eps_diluted: null,
        research_and_dev: null,
        sga_expense: null,
        interest_expense: null,
        income_tax: null,
        total_assets: null,
        total_liabilities: null,
        shareholders_equity: null,
        long_term_debt: null,
        short_term_debt: null,
        cash: null,
        operating_cf: null,
        investing_cf: null,
        financing_cf: null,
        capex: null,
        dividends_paid: null,
      };
      map.set(k, row);
    }
    return row;
  }

  for (const r of income) {
    map.set(key(r), {
      ...ensure(r),
      revenue: num(r.values["revenue"]),
      cost_of_revenue: num(r.values["cost_of_revenue"]),
      gross_profit: num(r.values["gross_profit"]),
      operating_income: num(r.values["operating_income"]),
      net_income: num(r.values["net_income"]),
      eps_diluted: num(r.values["eps_diluted"]),
      research_and_dev: num(r.values["research_and_dev"]),
      sga_expense: num(r.values["sga_expense"]),
      interest_expense: num(r.values["interest_expense"]),
      income_tax: num(r.values["income_tax"]),
    });
  }
  for (const r of balance) {
    map.set(key(r), {
      ...ensure(r),
      total_assets: num(r.values["total_assets"]),
      total_liabilities: num(r.values["total_liabilities"]),
      shareholders_equity: num(r.values["shareholders_equity"]),
      long_term_debt: num(r.values["long_term_debt"]),
      short_term_debt: num(r.values["short_term_debt"]),
      cash: num(r.values["cash"]),
    });
  }
  for (const r of cashflow) {
    map.set(key(r), {
      ...ensure(r),
      operating_cf: num(r.values["operating_cf"]),
      investing_cf: num(r.values["investing_cf"]),
      financing_cf: num(r.values["financing_cf"]),
      capex: num(r.values["capex"]),
      dividends_paid: num(r.values["dividends_paid"]),
    });
  }

  return [...map.values()].sort((a, b) =>
    a.period_end < b.period_end ? -1 : a.period_end > b.period_end ? 1 : 0,
  );
}

/** Safe division — returns `null` when either operand is null or
 *  the divisor is zero. Used by every ratio metric so a missing
 *  field becomes a chart gap rather than `Infinity` or `NaN`. */
export function safeDiv(
  num_: number | null,
  den: number | null,
): number | null {
  if (num_ === null || den === null) return null;
  if (den === 0) return null;
  return num_ / den;
}

// ---------------------------------------------------------------------------
// Chart-specific shapes
// ---------------------------------------------------------------------------

export interface PnlBucket {
  readonly period_end: string;
  /** COGS (positive). */
  readonly cogs: number | null;
  /** Operating expenses (R&D + SG&A) summed. Each component is null-
   *  safe — if one is missing the other still contributes. */
  readonly opex: number | null;
  /** Operating income. May be negative. */
  readonly op_income: number | null;
  /** Revenue, included for tooltip totals. */
  readonly revenue: number | null;
}

/** Stack components for a Revenue → COGS → Opex → Op income breakdown
 *  bar. Negative op_income still renders by sitting on top of the
 *  positive COGS+Opex stack — recharts handles the sign. */
export function buildPnlBuckets(periods: ReadonlyArray<JoinedPeriod>): PnlBucket[] {
  return periods.map((p) => {
    const opexParts: number[] = [];
    if (p.research_and_dev !== null) opexParts.push(p.research_and_dev);
    if (p.sga_expense !== null) opexParts.push(p.sga_expense);
    const opex = opexParts.length > 0
      ? opexParts.reduce((a, b) => a + b, 0)
      : null;
    return {
      period_end: p.period_end,
      cogs: p.cost_of_revenue,
      opex,
      op_income: p.operating_income,
      revenue: p.revenue,
    };
  });
}

export interface MarginRow {
  readonly period_end: string;
  readonly gross_pct: number | null;
  readonly operating_pct: number | null;
  readonly net_pct: number | null;
}

/** Margin = ratio × 100 so axis ticks read as percentages. */
export function buildMargins(periods: ReadonlyArray<JoinedPeriod>): MarginRow[] {
  return periods.map((p) => {
    const gross = safeDiv(p.gross_profit, p.revenue);
    const op = safeDiv(p.operating_income, p.revenue);
    const net = safeDiv(p.net_income, p.revenue);
    return {
      period_end: p.period_end,
      gross_pct: gross !== null ? gross * 100 : null,
      operating_pct: op !== null ? op * 100 : null,
      net_pct: net !== null ? net * 100 : null,
    };
  });
}

export interface YoyRow {
  readonly period_end: string;
  readonly revenue_yoy_pct: number | null;
  readonly eps_yoy_pct: number | null;
  readonly fcf_yoy_pct: number | null;
}

/** Year-over-year growth. Lag is determined by the page-level period
 *  the rows were fetched for: quarterly → 4 lag (prior-year quarter),
 *  annual → 1 lag (prior fiscal year). Looking at row-level
 *  `period_type` is unsafe — the SEC XBRL endpoint emits
 *  `Q1`/`Q2`/`Q3`/`Q4` and `FY`, never the literal string `"annual"`,
 *  so a row-level check would silently fall back to the quarterly
 *  branch on annual data. The first `lag` rows are still emitted with
 *  null values so the time axis stays aligned with the other charts. */
export function buildYoyGrowth(
  periods: ReadonlyArray<JoinedPeriod>,
  period: "quarterly" | "annual" = "quarterly",
): YoyRow[] {
  const lag = period === "annual" ? 1 : 4;
  return periods.map((p, i) => {
    const prior = i >= lag ? periods[i - lag] : undefined;
    const fcf = (cur: JoinedPeriod): number | null => {
      if (cur.operating_cf === null || cur.capex === null) return null;
      // capex is reported as a positive outflow in XBRL `us-gaap:
      // PaymentsToAcquirePropertyPlantAndEquipment`. FCF subtracts it.
      return cur.operating_cf - cur.capex;
    };
    return {
      period_end: p.period_end,
      revenue_yoy_pct: prior
        ? yoyPct(p.revenue, prior.revenue)
        : null,
      eps_yoy_pct: prior
        ? yoyPct(p.eps_diluted, prior.eps_diluted)
        : null,
      fcf_yoy_pct: prior
        ? yoyPct(fcf(p), fcf(prior))
        : null,
    };
  });
}

/** Standard YoY growth %: `(current - prior) / |prior| × 100`. The
 *  absolute-value denominator keeps the sign of the change correct
 *  when prior is negative — a swing from -100 to +50 reads as a
 *  positive growth, not negative. Returns null when either side is
 *  null or prior is exactly 0 (undefined growth). */
function yoyPct(current: number | null, prior: number | null): number | null {
  if (current === null || prior === null) return null;
  if (prior === 0) return null;
  return ((current - prior) / Math.abs(prior)) * 100;
}

export interface CashflowWaterfallStep {
  readonly label: string;
  readonly value: number;
  /** Cumulative running total after this step. The recharts
   *  waterfall pattern uses the cumulative as the bar's start anchor
   *  (transparent base) so each step visually picks up where the
   *  prior left off. */
  readonly cumulative: number;
  readonly is_total: boolean;
}

/** Operating → Investing → Financing → Net change. The "Net" bar is
 *  the sum of the three flows — when XBRL's `NetCashChange` field is
 *  available we'd cross-check; the existing API doesn't ship it, so
 *  we compute. Returns null when the period lacks every flow. */
export function buildCashflowWaterfall(
  period: JoinedPeriod,
): CashflowWaterfallStep[] | null {
  if (
    period.operating_cf === null &&
    period.investing_cf === null &&
    period.financing_cf === null
  ) {
    return null;
  }
  const op = period.operating_cf ?? 0;
  const inv = period.investing_cf ?? 0;
  const fin = period.financing_cf ?? 0;
  const net = op + inv + fin;
  return [
    { label: "Operating", value: op, cumulative: op, is_total: false },
    { label: "Investing", value: inv, cumulative: op + inv, is_total: false },
    { label: "Financing", value: fin, cumulative: net, is_total: false },
    { label: "Net change", value: net, cumulative: net, is_total: true },
  ];
}

export interface BalanceStructure {
  readonly period_end: string;
  readonly assets: number;
  readonly liabilities: number;
  readonly equity: number;
}

/** Most-recent balance-sheet snapshot — assets vs liabilities + equity.
 *  Returns null when the latest period lacks the three core fields.
 *  The recharts component renders this as two horizontal stacked bars
 *  so the operator visually checks `assets ≈ liab + equity`. */
export function latestBalanceStructure(
  periods: ReadonlyArray<JoinedPeriod>,
): BalanceStructure | null {
  for (let i = periods.length - 1; i >= 0; i--) {
    const p = periods[i]!;
    if (
      p.total_assets !== null &&
      p.total_liabilities !== null &&
      p.shareholders_equity !== null
    ) {
      return {
        period_end: p.period_end,
        assets: p.total_assets,
        liabilities: p.total_liabilities,
        equity: p.shareholders_equity,
      };
    }
  }
  return null;
}

export interface DebtRow {
  readonly period_end: string;
  readonly long_term: number | null;
  readonly short_term: number | null;
  /** Operating income / interest expense. >1 means earnings cover
   *  interest. Negative ratios (loss-making period) are clamped to
   *  null so the chart axis isn't dragged into the negatives by
   *  one-off bad quarters that say nothing about coverage. */
  readonly interest_coverage: number | null;
}

export function buildDebtStructure(
  periods: ReadonlyArray<JoinedPeriod>,
): DebtRow[] {
  return periods.map((p) => {
    const cov = safeDiv(p.operating_income, p.interest_expense);
    return {
      period_end: p.period_end,
      long_term: p.long_term_debt,
      short_term: p.short_term_debt,
      interest_coverage: cov !== null && cov >= 0 ? cov : null,
    };
  });
}

export interface DupontRow {
  readonly period_end: string;
  /** ROE = NPM × Asset Turnover × Equity Multiplier. Stored as a
   *  ratio (0.15 = 15%); the chart multiplies by 100 at render time
   *  to keep this helper's contract sign-agnostic. */
  readonly roe: number | null;
  readonly net_margin: number | null;
  readonly asset_turnover: number | null;
  readonly equity_multiplier: number | null;
}

export function buildDupont(
  periods: ReadonlyArray<JoinedPeriod>,
): DupontRow[] {
  return periods.map((p) => {
    const npm = safeDiv(p.net_income, p.revenue);
    const turnover = safeDiv(p.revenue, p.total_assets);
    const multiplier = safeDiv(p.total_assets, p.shareholders_equity);
    const roe =
      npm !== null && turnover !== null && multiplier !== null
        ? npm * turnover * multiplier
        : null;
    return {
      period_end: p.period_end,
      roe,
      net_margin: npm,
      asset_turnover: turnover,
      equity_multiplier: multiplier,
    };
  });
}

export interface RoicRow {
  readonly period_end: string;
  /** Return on invested capital. NOPAT / Invested Capital where:
   *   - NOPAT ≈ operating_income × (1 - effective_tax_rate)
   *   - Effective tax rate ≈ income_tax / (net_income + income_tax)
   *   - Invested Capital ≈ long_term_debt + short_term_debt + equity
   *  The approximation skips lease liabilities and minority interest;
   *  good enough for trend-watching, not for absolute valuation. */
  readonly roic: number | null;
}

export function buildRoic(periods: ReadonlyArray<JoinedPeriod>): RoicRow[] {
  return periods.map((p) => {
    if (
      p.operating_income === null ||
      p.shareholders_equity === null
    ) {
      return { period_end: p.period_end, roic: null };
    }
    // Effective tax rate. When pre-tax income is zero/negative or the
    // tax field is missing, fall back to a 21% US-statutory placeholder
    // — better than dropping the period entirely, and signed
    // consistently with Bloomberg/FactSet convention.
    const preTax =
      p.net_income !== null && p.income_tax !== null
        ? p.net_income + p.income_tax
        : null;
    const taxRate =
      preTax !== null && preTax > 0 && p.income_tax !== null
        ? p.income_tax / preTax
        : 0.21;
    const nopat = p.operating_income * (1 - taxRate);
    const debt = (p.long_term_debt ?? 0) + (p.short_term_debt ?? 0);
    const invested = debt + p.shareholders_equity;
    if (invested <= 0) return { period_end: p.period_end, roic: null };
    return { period_end: p.period_end, roic: nopat / invested };
  });
}

export interface FcfRow {
  readonly period_end: string;
  /** Free cash flow = operating_cf - capex. Both are absolute values
   *  in the source statement; capex is XBRL `PaymentsToAcquirePPE`
   *  reported as a positive outflow, so subtraction is correct. */
  readonly fcf: number | null;
}

export function buildFcf(periods: ReadonlyArray<JoinedPeriod>): FcfRow[] {
  return periods.map((p) => ({
    period_end: p.period_end,
    fcf:
      p.operating_cf !== null && p.capex !== null
        ? p.operating_cf - p.capex
        : null,
  }));
}
