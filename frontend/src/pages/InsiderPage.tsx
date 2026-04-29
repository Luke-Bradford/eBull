/**
 * /instrument/:symbol/insider — Form 4 insider activity drill page (#588).
 *
 * Fills the gap left when #574 trimmed the on-page insider widget to a
 * 5-stat summary with no drill route. Operator's quant-grade view of
 * insider sentiment. Sections (top → bottom):
 *
 *   1. Net acquired vs disposed by month — 24m bar chart, ±coloured
 *   2. Activity by officer — horizontal bar, all officers (no top-N
 *      cap), 90d window matching the L1 lens
 *   3. Price + transaction markers — daily price line with Form 4
 *      arrows (Bloomberg INSI convention)
 *   4. All transactions — sortable + filterable table with CSV export
 *
 * Independent fetches: insider transactions (the spine — page hinges
 * on these) and daily candles (price-pane garnish). A 404 on insider
 * transactions renders "no SEC coverage" — Form 4 is SEC-only, so
 * non-US issuers fall through this branch. Other transaction errors
 * surface a retry. A candle failure degrades the price pane only:
 * the rest of the drill keeps rendering with an inline note.
 */

import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";

import {
  fetchInsiderTransactions,
  fetchInstrumentCandles,
  type InsiderTransactionsList,
} from "@/api/instruments";
import { ApiError } from "@/api/client";
import type { InstrumentCandles } from "@/api/types";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { InsiderByOfficer } from "@/components/insider/InsiderByOfficer";
import { InsiderNetByMonth } from "@/components/insider/InsiderNetByMonth";
import { InsiderPriceMarkers } from "@/components/insider/InsiderPriceMarkers";
import { InsiderTransactionsTable } from "@/components/insider/InsiderTransactionsTable";
import { Pane } from "@/components/instrument/Pane";
import { Term } from "@/components/Term";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

const TXN_LIMIT = 500;

export function InsiderPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();

  const txnState = useAsync<InsiderTransactionsList>(
    useCallback(() => fetchInsiderTransactions(symbol, TXN_LIMIT), [symbol]),
    [symbol],
  );
  const candleState = useAsync<InstrumentCandles>(
    useCallback(() => fetchInstrumentCandles(symbol, "5y"), [symbol]),
    [symbol],
  );

  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 pb-3">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <h1 className="mt-1 text-lg font-semibold text-slate-900">
          Insider activity — {symbol}
        </h1>
        <p className="mt-1 text-xs text-slate-500">
          SEC <Term term="Form 4" /> transactions — directors / officers
          / 10%+ holders disclosing share trades within 2 business days.
          Acquired = green, disposed = red. The chart panes use only
          non-derivative trades (open-market buys + sells, RSU vests,
          tax-withholding sells); the table shows every row including
          derivative grants and option exercises.
        </p>
      </header>

      <InsiderPageBody
        symbol={symbol}
        backHref={backHref}
        txnState={txnState}
        candleState={candleState}
      />
    </div>
  );
}

interface InsiderPageBodyProps {
  readonly symbol: string;
  readonly backHref: string;
  readonly txnState: ReturnType<typeof useAsync<InsiderTransactionsList>>;
  readonly candleState: ReturnType<typeof useAsync<InstrumentCandles>>;
}

function InsiderPageBody({
  symbol,
  backHref,
  txnState,
  candleState,
}: InsiderPageBodyProps): JSX.Element {
  if (txnState.loading) {
    return <SectionSkeleton rows={6} />;
  }

  if (txnState.error !== null) {
    if (txnState.error instanceof ApiError && txnState.error.status === 404) {
      return (
        <EmptyState
          title="No insider data"
          description="No SEC Form 4 coverage for this instrument — likely a non-US issuer or one without an SEC CIK."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      );
    }
    return <SectionError onRetry={txnState.refetch} />;
  }

  if (txnState.data === null || txnState.data.rows.length === 0) {
    return (
      <EmptyState
        title="No insider data"
        description="No Form 4 transactions on file for this instrument."
      >
        <Link to={backHref} className="text-sm text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
      </EmptyState>
    );
  }

  const rows = txnState.data.rows;
  return (
    <div className="space-y-4">
      <Pane
        title="Net by month"
        scope="last 24 months"
        source={{ providers: ["sec_form4"] }}
      >
        <InsiderNetByMonth transactions={rows} />
      </Pane>

      <Pane
        title="Activity by officer"
        scope="last 90 days"
        source={{ providers: ["sec_form4"] }}
      >
        <InsiderByOfficer transactions={rows} />
      </Pane>

      <Pane
        title="Price + transactions"
        scope="last 24 months"
        source={{ providers: ["sec_form4", "etoro"] }}
      >
        <PriceMarkersSection
          rows={rows}
          candleState={candleState}
        />
      </Pane>

      <Pane
        title="All transactions"
        scope={`up to ${TXN_LIMIT} most recent`}
        source={{ providers: ["sec_form4"] }}
      >
        <InsiderTransactionsTable
          symbol={symbol}
          transactions={rows}
        />
      </Pane>
    </div>
  );
}

function PriceMarkersSection({
  rows,
  candleState,
}: {
  readonly rows: ReadonlyArray<import("@/api/instruments").InsiderTransactionDetail>;
  readonly candleState: ReturnType<typeof useAsync<InstrumentCandles>>;
}): JSX.Element {
  if (candleState.loading) {
    return <SectionSkeleton rows={4} />;
  }
  if (candleState.error !== null) {
    return (
      <div className="space-y-2">
        <p className="text-xs text-slate-500">
          Price data unavailable — markers shown without context.
          <button
            type="button"
            onClick={candleState.refetch}
            className="ml-2 text-sky-700 hover:underline"
          >
            Retry price data
          </button>
        </p>
        <InsiderPriceMarkers candles={[]} transactions={rows} />
      </div>
    );
  }
  return (
    <InsiderPriceMarkers
      candles={candleState.data?.rows ?? []}
      transactions={rows}
    />
  );
}
