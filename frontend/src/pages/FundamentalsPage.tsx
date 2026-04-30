/**
 * /instrument/:symbol/fundamentals — quant-grade financials drill (#589).
 *
 * Replaces the thin "Financials" tab as the L2 analytical view. Nine
 * panes laid out top → bottom in a single scroll column, mirroring
 * the per-domain catalog in the parent spec. Each pane is a recharts
 * subcomponent that consumes pre-computed data from
 * `lib/fundamentalsMetrics`. The page itself does the fetching, the
 * (income, balance, cashflow) join, and the empty-state branching.
 *
 *   1. Quarterly P&L (revenue → COGS / Opex / Op income stack)
 *   2. Margin trends (gross / operating / net multi-line)
 *   3. YoY growth (revenue / EPS / FCF grouped bars)
 *   4. Cash flow waterfall (latest period: operating → investing →
 *      financing → net change)
 *   5. Balance-sheet structure (latest snapshot — assets vs
 *      liabilities + equity stacked)
 *   6. Debt structure (LT/ST debt bars + interest-coverage line)
 *   7. DuPont decomposition (ROE = NPM × Asset Turnover ×
 *      Equity Multiplier)
 *   8. ROIC trend (NOPAT / Invested Capital)
 *   9. Free cash flow trend
 *
 * Period toggle: `?period=quarterly|annual` (default quarterly).
 *
 * The L3 raw statement table still lives at the existing
 * `/instrument/:symbol?tab=financials` route — link to it from the
 * page header so an operator can drop from analysis into the raw
 * numbers without losing the symbol context.
 */

import { useCallback, useMemo } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { fetchInstrumentFinancials } from "@/api/instruments";
import type { InstrumentFinancials } from "@/api/types";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import {
  BalanceStructureChart,
  CashflowWaterfallChart,
  DebtStructureChart,
  DupontChart,
  FcfChart,
  MarginTrendsChart,
  PnlStackedChart,
  RoicChart,
  YoyGrowthChart,
} from "@/components/fundamentals/fundamentalsCharts";
import { Pane } from "@/components/instrument/Pane";
import { Term } from "@/components/Term";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { joinStatements } from "@/lib/fundamentalsMetrics";

type Period = "quarterly" | "annual";
const VALID_PERIODS: ReadonlyArray<Period> = ["quarterly", "annual"];

export function FundamentalsPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const rawPeriod = searchParams.get("period");
  const period: Period = VALID_PERIODS.includes(rawPeriod as Period)
    ? (rawPeriod as Period)
    : "quarterly";

  const setPeriod = useCallback(
    (next: Period) => {
      const params = new URLSearchParams(searchParams);
      if (next === "quarterly") {
        params.delete("period");
      } else {
        params.set("period", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const income = useAsync<InstrumentFinancials>(
    useCallback(
      () => fetchInstrumentFinancials(symbol, { statement: "income", period }),
      [symbol, period],
    ),
    [symbol, period],
  );
  const balance = useAsync<InstrumentFinancials>(
    useCallback(
      () => fetchInstrumentFinancials(symbol, { statement: "balance", period }),
      [symbol, period],
    ),
    [symbol, period],
  );
  const cashflow = useAsync<InstrumentFinancials>(
    useCallback(
      () => fetchInstrumentFinancials(symbol, { statement: "cashflow", period }),
      [symbol, period],
    ),
    [symbol, period],
  );

  const periods = useMemo(() => {
    if (income.data === null || balance.data === null || cashflow.data === null) {
      return [];
    }
    return joinStatements(
      income.data.rows,
      balance.data.rows,
      cashflow.data.rows,
    );
  }, [income.data, balance.data, cashflow.data]);

  const backHref = `/instrument/${encodeURIComponent(symbol)}`;
  const rawHref = `/instrument/${encodeURIComponent(symbol)}?tab=financials`;

  const loading = income.loading || balance.loading || cashflow.loading;
  const errored =
    income.error !== null || balance.error !== null || cashflow.error !== null;
  // The `/financials` endpoint returns 200 with `source="unavailable"`
  // and `rows=[]` when an instrument has no SEC coverage (non-US
  // issuer, no CIK, etc.) — see app/api/instruments.py around the
  // `_fetch_local_financials` empty-result branch. A 404 means the
  // symbol itself isn't recognised, which falls through to the
  // generic SectionError. The "no SEC XBRL coverage" empty state
  // fires when every statement explicitly reports `unavailable`.
  const noSecCoverage =
    !errored &&
    income.data?.source === "unavailable" &&
    balance.data?.source === "unavailable" &&
    cashflow.data?.source === "unavailable";

  function refetchAll(): void {
    income.refetch();
    balance.refetch();
    cashflow.refetch();
  }

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <div className="mt-1 flex flex-wrap items-baseline justify-between gap-2">
          <h1 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
            Fundamentals — {symbol}
          </h1>
          <div className="flex items-center gap-2 text-xs">
            <div className="flex gap-1">
              <button
                type="button"
                onClick={() => setPeriod("quarterly")}
                aria-pressed={period === "quarterly"}
                className={`rounded px-2 py-0.5 font-medium ${period === "quarterly" ? "bg-slate-800 text-white" : "bg-slate-100 dark:bg-slate-800 text-slate-600 hover:bg-slate-200"}`}
                data-testid="fundamentals-period-quarterly"
              >
                Quarterly
              </button>
              <button
                type="button"
                onClick={() => setPeriod("annual")}
                aria-pressed={period === "annual"}
                className={`rounded px-2 py-0.5 font-medium ${period === "annual" ? "bg-slate-800 text-white" : "bg-slate-100 dark:bg-slate-800 text-slate-600 hover:bg-slate-200"}`}
                data-testid="fundamentals-period-annual"
              >
                Annual
              </button>
            </div>
            <Link to={rawHref} className="text-sky-700 hover:underline">
              Raw statements →
            </Link>
          </div>
        </div>
        <p className="mt-1 text-xs text-slate-500">
          SEC <Term term="XBRL" /> company-facts data — every line is
          tagged in the issuer's 10-K / 10-Q so we can read them as
          numbers (not narrative). Each pane shows "—" when a metric
          is missing for a period. <Term term="ROIC" /> and{" "}
          <Term term="FCF" /> are derived; <Term term="DuPont" />{" "}
          breaks <Term term="ROE" /> into its three drivers so you
          can see which one is doing the work.
        </p>
      </header>

      {loading ? (
        <SectionSkeleton rows={6} />
      ) : errored ? (
        <SectionError onRetry={refetchAll} />
      ) : noSecCoverage ? (
        <EmptyState
          title="No fundamentals data"
          description="No SEC XBRL coverage for this instrument — likely a non-US issuer or one without an SEC CIK."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : periods.length === 0 ? (
        <EmptyState
          title="No fundamentals data"
          description="No XBRL statement rows on file for this instrument yet."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <div className="space-y-4 pt-6">
          <Pane
            title="P&L breakdown"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <PnlStackedChart periods={periods} />
          </Pane>
          <Pane
            title="Margin trends"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <MarginTrendsChart periods={periods} />
          </Pane>
          <Pane
            title="YoY growth"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <YoyGrowthChart periods={periods} period={period} />
          </Pane>
          <Pane
            title="Cash flow waterfall"
            scope="latest period"
            source={{ providers: ["sec_xbrl"] }}
          >
            <CashflowWaterfallChart period={periods[periods.length - 1] ?? null} />
          </Pane>
          <Pane
            title="Balance sheet structure"
            scope="latest snapshot"
            source={{ providers: ["sec_xbrl"] }}
          >
            <BalanceStructureChart periods={periods} />
          </Pane>
          <Pane
            title="Debt structure"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <DebtStructureChart periods={periods} />
          </Pane>
          <Pane
            title="DuPont decomposition"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <DupontChart periods={periods} />
          </Pane>
          <Pane
            title="ROIC"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <RoicChart periods={periods} />
          </Pane>
          <Pane
            title="Free cash flow"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            {/* Spec calls for FCF *yield* (FCF / market cap) over time.
                Yield needs price + shares-outstanding joined per
                period; tracked at #671. The absolute FCF line ships
                here so the trend is visible from day one. */}
            <FcfChart periods={periods} />
          </Pane>
        </div>
      )}
    </div>
  );
}

function periodScope(p: Period): string {
  return p === "quarterly" ? "quarterly history" : "annual history";
}
