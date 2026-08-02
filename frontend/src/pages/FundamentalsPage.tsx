/**
 * /instrument/:symbol/fundamentals — quant-grade financials drill (#589).
 *
 * `:symbol` is a ticker OR a numeric instrument_id — the backend resolves
 * either (#2184). The heading and the sibling links always show the
 * RESOLVED symbol, so `/instrument/1001/fundamentals` reads "AAPL".
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
 *   5. Net debt (gross debt + cash bars, net-debt line). Replaced the
 *      assets vs liabilities+equity snapshot in #2185: those two are
 *      equal by the accounting identity, so that chart could not vary.
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

import { useCallback, useMemo, useRef } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { ApiError } from "@/api/client";
import { fetchFcfYield, fetchInstrumentFinancials } from "@/api/instruments";
import type { FcfYieldSeries, InstrumentFinancials } from "@/api/types";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import {
  CashflowWaterfallChart,
  DebtStructureChart,
  DupontChart,
  FcfChart,
  MarginTrendsChart,
  NetDebtChart,
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

/**
 * "This endpoint says the ref names no instrument we hold" — an operator
 * typo or a stale bookmark. That is an EMPTY state, not a failure:
 * `SectionError`'s red "check the browser console" banner tells the
 * operator to debug a working app (#2184).
 *
 * Status alone is NOT enough. FastAPI answers a missing/renamed route or
 * a mis-proxied `/api` base with a bare `{"detail":"Not Found"}`, and
 * `client.ts` turns any non-2xx into `ApiError` (it special-cases only
 * 401). Matching on status alone would render "No instrument matches
 * AAPL" for every instrument during an outage, with no Retry — reporting
 * a broken deploy as a data absence. So also require the endpoint's own
 * message, which both drill raise sites spell `Instrument {symbol} not
 * found` (app/api/instruments.py:994 financials, :1047 fcf-yield; the
 * same string at all 30 sites in that module). The match is
 * case-sensitive, which is what separates it from FastAPI's "Not Found".
 *
 * If that backend message ever changes, this guard fails CLOSED — back to
 * `SectionError` + Retry, never to a false empty state.
 */
function isNotFound(err: unknown): boolean {
  if (!(err instanceof ApiError) || err.status !== 404) return false;
  return typeof err.detail === "string" && err.detail.includes("not found");
}

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
  // Supplementary: the FCF-yield overlay (#671). Independent lifecycle — never
  // gates the page or the absolute FCF line. Multi-class / cross-currency
  // issuers come back `suppressed`; a fetch error just leaves the FCF line
  // intact (the chart reads `yieldSeries` defensively).
  const fcfYield = useAsync<FcfYieldSeries>(
    useCallback(() => fetchFcfYield(symbol, { period }), [symbol, period]),
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

  // The route param may be a ticker OR a numeric instrument_id (#2184 —
  // the backend resolves either). The payload echoes the RESOLVED symbol,
  // so the heading and the sibling links show `AAPL`, never `1001`.
  //
  // Latched, because reading the payload alone is not enough: `useAsync`
  // calls `setData(null)` at the start of every non-preserved fetch
  // (lib/useAsync.ts:98), and the period toggle changes the deps
  // `[symbol, period]`. Without the latch, every Quarterly→Annual toggle
  // on `/instrument/1001/fundamentals` reverts the header to "1001" for
  // the duration of the request and points `backHref` / `rawHref` at
  // `/instrument/1001`, which dead-ends — `InstrumentPage` still resolves
  // by symbol only. (`preserveOnRefetch` does NOT fix this: it applies
  // only when `tick > 0`, i.e. an explicit `refetch()`, not a deps change.)
  //
  // The latch is keyed on the RAW ref so it is discarded when the operator
  // navigates to a different instrument — React Router reuses this
  // component across param changes, so an unkeyed latch would briefly show
  // the previous instrument's symbol.
  const payloadSymbol =
    income.data?.symbol ?? balance.data?.symbol ?? cashflow.data?.symbol ?? null;
  const resolvedRef = useRef<{ ref: string; symbol: string } | null>(null);
  if (payloadSymbol !== null) {
    resolvedRef.current = { ref: symbol, symbol: payloadSymbol };
  }
  const resolvedSymbol =
    payloadSymbol ??
    (resolvedRef.current?.ref === symbol ? resolvedRef.current.symbol : symbol);

  // `financial_periods.reported_currency`, surfaced by the endpoint as
  // `currency` (app/api/instruments.py:819,838). First-non-null across the
  // three statements rather than income-only, so a partially-covered
  // instrument still labels its money axes. Null (no rows / no reported
  // currency) leaves the axes unprefixed — see `formatMoneyAxis` (#2185 §1.4).
  //
  // Granularity caveat, stated rather than glossed: the endpoint reads
  // `db_rows[0]["reported_currency"]` under `ORDER BY period_end_date DESC`
  // (instruments.py:824,838), i.e. the LATEST period's currency, and this
  // stamps that one value onto all 20 periods. #2129's rule is per-item, so an
  // issuer that switched reporting currency mid-history would mislabel its own
  // back-series. Zero instruments are affected today — dev
  // `financial_periods` has one distinct code corpus-wide (USD) and no
  // instrument with more than one — so this is a known limit of the response
  // shape, not a live defect. Fixing it means moving `currency` onto the row.
  const currency =
    income.data?.currency ??
    balance.data?.currency ??
    cashflow.data?.currency ??
    null;

  // Both hrefs use the RESOLVED symbol (#2184), not the raw route param:
  // `InstrumentPage` still resolves by symbol only, so `/instrument/1001`
  // would dead-end exactly as this page used to.
  const backHref = `/instrument/${encodeURIComponent(resolvedSymbol)}`;
  const rawHref = `/instrument/${encodeURIComponent(resolvedSymbol)}?tab=financials`;

  const loading = income.loading || balance.loading || cashflow.loading;
  const errors = [income.error, balance.error, cashflow.error];
  // Classify each error ONCE, then read both conditions off the same
  // pass. The two used to call isNotFound independently, which is not
  // just duplicated work — it left "is a 404" and "is not a 404" free to
  // drift apart if the predicate were ever changed at one site only.
  // `errored` deliberately wins over `notFound` downstream: a genuine
  // failure must surface as SectionError + Retry, never as a
  // "no such instrument" empty state.
  const notFoundFlags = errors.map((e) => e !== null && isNotFound(e));
  const notFound = notFoundFlags.some(Boolean);
  const errored = errors.some((e, i) => e !== null && !notFoundFlags[i]);
  // The `/financials` endpoint returns 200 with `source="unavailable"`
  // and `rows=[]` when an instrument has no SEC coverage (non-US
  // issuer, no CIK, etc.) — see app/api/instruments.py around the
  // `_fetch_local_financials` empty-result branch. A 404 means the
  // route param itself isn't recognised — an empty state, handled
  // separately above. The "no SEC XBRL coverage" empty state fires
  // when every statement explicitly reports `unavailable`.
  const noSecCoverage =
    !errored &&
    !notFound &&
    income.data?.source === "unavailable" &&
    balance.data?.source === "unavailable" &&
    cashflow.data?.source === "unavailable";

  function refetchAll(): void {
    income.refetch();
    balance.refetch();
    cashflow.refetch();
    fcfYield.refetch();
  }

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        {/* On the not-found path both sibling links point at
            `/instrument/<unresolvable ref>`, which dead-ends the same way
            this page just did — `InstrumentPage` resolves by symbol only.
            Offering them would hand the operator a second failure instead
            of a recovery route, so the header drops to plain text and the
            EmptyState's `/instruments` link is the only way out. */}
        {notFound ? (
          <span className="text-xs text-slate-500">Instrument drill</span>
        ) : (
          <Link to={backHref} className="text-xs text-sky-700 hover:underline">
            ← Back to {resolvedSymbol}
          </Link>
        )}
        <div className="mt-1 flex flex-wrap items-baseline justify-between gap-2">
          <h1 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
            Fundamentals — {resolvedSymbol}
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
            {!notFound && (
              <Link to={rawHref} className="text-sky-700 hover:underline">
                Raw statements →
              </Link>
            )}
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
      ) : notFound ? (
        <EmptyState
          title="Instrument not found"
          description={`No instrument matches "${symbol}". The URL takes either a ticker (AAPL) or a numeric instrument id (1001).`}
        >
          <Link to="/instruments" className="text-sm text-sky-700 hover:underline">
            ← Browse instruments
          </Link>
        </EmptyState>
      ) : noSecCoverage ? (
        <EmptyState
          title="No fundamentals data"
          description="No SEC XBRL coverage for this instrument — likely a non-US issuer or one without an SEC CIK."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {resolvedSymbol}
          </Link>
        </EmptyState>
      ) : periods.length === 0 ? (
        <EmptyState
          title="No fundamentals data"
          description="No XBRL statement rows on file for this instrument yet."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {resolvedSymbol}
          </Link>
        </EmptyState>
      ) : (
        <div className="space-y-4 pt-6">
          <Pane
            title="P&L breakdown"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <PnlStackedChart periods={periods} currency={currency} />
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
            <CashflowWaterfallChart
              period={periods[periods.length - 1] ?? null}
              currency={currency}
            />
          </Pane>
          <Pane
            title="Net debt"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <NetDebtChart periods={periods} currency={currency} />
          </Pane>
          <Pane
            title="Debt structure"
            scope={periodScope(period)}
            source={{ providers: ["sec_xbrl"] }}
          >
            <DebtStructureChart periods={periods} currency={currency} />
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
            <FcfChart
              periods={periods}
              yieldSeries={fcfYield.data}
              currency={currency}
            />
          </Pane>
        </div>
      )}
    </div>
  );
}

function periodScope(p: Period): string {
  return p === "quarterly" ? "quarterly history" : "annual history";
}
