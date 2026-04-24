/**
 * InsiderActivityPanel — Form 4 insider transaction activity for the
 * instrument page. Backed by GET /instruments/{symbol}/insider_summary
 * + /insider_transactions (#429).
 *
 * Layout:
 *
 *   ┌───────────── 90-day summary strip ─────────────┐
 *   │ Net shares  Buys  Sells  Filers  Latest date   │
 *   ├────────────────────────────────────────────────┤
 *   │ Recent transactions table                      │
 *   │   Date · Insider · Role · Code · Shares ·      │
 *   │   Price · Post-trade balance · Security class  │
 *   │   Plan (10b5-1) · Late-filed · Footnote        │
 *   └────────────────────────────────────────────────┘
 *
 * Derivative rows (option grants / RSU vests) are shown but visually
 * de-emphasised because the sentiment signal sits on open-market
 * buy/sell activity. Every structured Form 4 field captured by the
 * ingester is available in the expanded row view on hover/click.
 */

import {
  fetchInsiderSummary,
  fetchInsiderTransactions,
} from "@/api/instruments";
import type {
  InsiderSummary,
  InsiderTransactionDetail,
  InsiderTransactionsList,
} from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";

export interface InsiderActivityPanelProps {
  readonly symbol: string;
}

// Human labels for the SEC transaction codes most operators will
// encounter. Unlisted codes fall through to the raw code — we don't
// want to editorialise / hide anything, just make the common ones
// readable at a glance.
const TXN_CODE_LABEL: Record<string, string> = {
  P: "Open-market buy",
  S: "Open-market sale",
  A: "Grant / award",
  M: "Option exercise",
  F: "Tax withholding",
  G: "Gift",
  D: "Disposition to issuer",
  X: "Option exercise (same-day)",
  C: "Conversion",
  V: "Voluntary report",
  J: "Other acquisition / disposition",
};

function txnCodeLabel(code: string, acquiredDisposed: string | null): string {
  const base = TXN_CODE_LABEL[code];
  if (base !== undefined) return base;
  if (acquiredDisposed === "A") return `Acquired (${code})`;
  if (acquiredDisposed === "D") return `Disposed (${code})`;
  return code;
}

function formatShares(raw: string | null): string {
  if (raw === null) return "—";
  const num = Number(raw);
  if (!Number.isFinite(num)) return "—";
  return num.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function formatPrice(raw: string | null): string {
  if (raw === null) return "—";
  const num = Number(raw);
  if (!Number.isFinite(num)) return "—";
  return `$${num.toFixed(2)}`;
}

function formatDate(raw: string | null): string {
  if (raw === null) return "—";
  return raw;
}

function roleBadge(role: string | null): string {
  if (role === null || role === "") return "Insider";
  // role shape: pipe-joined — "director|officer:CEO|ten_percent_owner"
  const parts = role.split("|").map((p) => {
    if (p.startsWith("officer:")) return p.slice("officer:".length);
    if (p === "director") return "Director";
    if (p === "officer") return "Officer";
    if (p === "ten_percent_owner") return "10% Owner";
    if (p.startsWith("other:")) return p.slice("other:".length);
    if (p === "other") return "Other";
    return p;
  });
  return parts.join(" · ");
}

function formatDelta(raw: string): { label: string; colour: string } {
  const num = Number(raw);
  if (!Number.isFinite(num) || num === 0) {
    return { label: "0", colour: "bg-slate-100 text-slate-700" };
  }
  if (num > 0) {
    return {
      label: `+${Math.round(num).toLocaleString("en-US")}`,
      colour: "bg-emerald-100 text-emerald-800",
    };
  }
  return {
    label: Math.round(num).toLocaleString("en-US"),
    colour: "bg-rose-100 text-rose-800",
  };
}

function SummaryStrip({ summary }: { summary: InsiderSummary }) {
  const totalAcquired = Number(summary.total_acquired_shares_90d);
  const totalDisposed = Number(summary.total_disposed_shares_90d);
  const totalNet = Number.isFinite(totalAcquired - totalDisposed)
    ? totalAcquired - totalDisposed
    : 0;
  const totalBadge = formatDelta(String(totalNet));
  const openMarketNet = Number(summary.open_market_net_shares_90d);
  const openMarketBadge = formatDelta(summary.open_market_net_shares_90d);
  return (
    <div className="mb-4 flex flex-col gap-3">
      {/* Primary view: full insider-holdings change across every
          non-derivative code. This is the "did insiders end the
          window owning more or fewer shares" signal operators
          actually want at a glance. */}
      <div>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
          <div className="flex flex-col">
            <span className="text-xs uppercase tracking-wide text-slate-500">
              Net change
            </span>
            <span
              className={`mt-1 inline-flex w-fit rounded px-2 py-0.5 text-sm font-semibold ${totalBadge.colour}`}
              title="Total shares acquired minus disposed across every non-derivative Form 4 transaction in the last 90 days. Includes grants, open-market purchases, option exercises, sales, tax withholding, gifts."
            >
              {totalBadge.label} shares
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-xs uppercase tracking-wide text-slate-500">
              Acquired
            </span>
            <span className="mt-1 font-mono text-base tabular-nums text-emerald-700">
              {totalAcquired > 0 ? "+" : ""}
              {Math.round(totalAcquired).toLocaleString("en-US")}
            </span>
            <span className="text-[10px] text-slate-500">
              {summary.acquisition_count_90d} txns
              {summary.open_market_buy_count_90d > 0 && (
                <> · {summary.open_market_buy_count_90d} open-market</>
              )}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-xs uppercase tracking-wide text-slate-500">
              Disposed
            </span>
            <span className="mt-1 font-mono text-base tabular-nums text-rose-700">
              {totalDisposed > 0 ? "-" : ""}
              {Math.round(totalDisposed).toLocaleString("en-US")}
            </span>
            <span className="text-[10px] text-slate-500">
              {summary.disposition_count_90d} txns
              {summary.open_market_sell_count_90d > 0 && (
                <> · {summary.open_market_sell_count_90d} open-market</>
              )}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-xs uppercase tracking-wide text-slate-500">
              Unique insiders
            </span>
            <span className="mt-1 font-mono text-base tabular-nums text-slate-800">
              {summary.unique_filers_90d}
            </span>
          </div>
          <div className="flex flex-col">
            <span className="text-xs uppercase tracking-wide text-slate-500">
              Latest trade
            </span>
            <span className="mt-1 font-mono text-base tabular-nums text-slate-800">
              {formatDate(summary.latest_txn_date)}
            </span>
          </div>
        </div>
      </div>

      {/* Secondary view: discretionary (P/S) only — the sentiment
          sub-signal, labelled clearly so a "0 buys" count next to
          grants doesn't read as insiders not buying. */}
      {(summary.open_market_buy_count_90d + summary.open_market_sell_count_90d > 0 ||
        openMarketNet !== 0) && (
        <div className="rounded bg-slate-50 px-3 py-1.5 text-xs text-slate-600">
          <span className="font-medium text-slate-700">
            Open-market only (discretionary P/S):
          </span>{" "}
          <span
            className={`inline-flex items-center rounded px-1.5 py-0.5 font-semibold ${openMarketBadge.colour}`}
          >
            {openMarketBadge.label} shares
          </span>{" "}
          · {summary.open_market_buy_count_90d} buys ·{" "}
          {summary.open_market_sell_count_90d} sells
          <span
            className="ml-1 text-slate-400"
            title="Only SEC transaction codes P (open-market purchase) and S (open-market sale). Excludes grants, RSU vests, option exercises, tax withholding, gifts."
          >
            ⓘ
          </span>
        </div>
      )}
    </div>
  );
}

function Row({ txn }: { txn: InsiderTransactionDetail }) {
  const code = txnCodeLabel(txn.txn_code, txn.acquired_disposed_code);
  const isBuy = txn.txn_code === "P";
  const isSell = txn.txn_code === "S";
  const codeColour = isBuy
    ? "text-emerald-700"
    : isSell
      ? "text-rose-700"
      : "text-slate-600";
  const planned = txn.deemed_execution_date !== null;
  const late = txn.transaction_timeliness === "L";
  const footnoteEntries = Object.entries(txn.footnotes);
  return (
    <tr
      className={
        txn.is_derivative ? "border-t border-slate-100 text-slate-500" : "border-t border-slate-100"
      }
    >
      <td className="py-2 pr-3 font-mono tabular-nums text-xs">
        {formatDate(txn.txn_date)}
      </td>
      <td className="py-2 pr-3">
        <div className="flex flex-col">
          <span className="font-medium text-slate-800">{txn.filer_name}</span>
          <span className="text-xs text-slate-500">{roleBadge(txn.filer_role)}</span>
        </div>
      </td>
      <td className={`py-2 pr-3 text-xs ${codeColour}`}>
        {code}
        {planned && (
          <span
            className="ml-1 rounded bg-slate-100 px-1 py-0.5 text-[10px] font-medium text-slate-600"
            title="Pre-arranged under Rule 10b5-1"
          >
            10b5-1
          </span>
        )}
        {late && (
          <span
            className="ml-1 rounded bg-amber-100 px-1 py-0.5 text-[10px] font-medium text-amber-800"
            title="Filed after the 2-business-day deadline"
          >
            Late
          </span>
        )}
      </td>
      <td className="py-2 pr-3 text-right font-mono tabular-nums text-xs">
        {formatShares(txn.shares)}
      </td>
      <td className="py-2 pr-3 text-right font-mono tabular-nums text-xs">
        {formatPrice(txn.price)}
      </td>
      <td className="py-2 pr-3 text-right font-mono tabular-nums text-xs text-slate-600">
        {formatShares(txn.post_transaction_shares)}
      </td>
      <td className="py-2 pr-3 text-xs text-slate-600">
        {txn.security_title ?? "—"}
        {txn.direct_indirect === "I" && (
          <span
            className="ml-1 rounded bg-slate-100 px-1 py-0.5 text-[10px] font-medium text-slate-600"
            title={txn.nature_of_ownership ?? "Held indirectly"}
          >
            Indirect
          </span>
        )}
        {txn.is_derivative && (
          <span className="ml-1 rounded bg-slate-100 px-1 py-0.5 text-[10px] font-medium text-slate-600">
            Derivative
          </span>
        )}
      </td>
      <td className="py-2 text-xs text-slate-500">
        {footnoteEntries.length === 0 ? (
          "—"
        ) : (
          <ul className="space-y-0.5">
            {footnoteEntries.map(([field, body]) => (
              <li key={field}>
                <span className="font-medium text-slate-600">{field}:</span>{" "}
                {body}
              </li>
            ))}
          </ul>
        )}
      </td>
    </tr>
  );
}

function Body({
  summary,
  transactions,
}: {
  summary: InsiderSummary;
  transactions: InsiderTransactionsList;
}) {
  if (transactions.rows.length === 0) {
    return (
      <>
        <SummaryStrip summary={summary} />
        <EmptyState
          title="No recent insider filings"
          description="No Form 4 filings parsed for this instrument yet. Either no insiders have transacted recently, or the daily ingester has not yet picked up the latest filings."
        />
      </>
    );
  }
  return (
    <>
      <SummaryStrip summary={summary} />
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="text-xs uppercase tracking-wide text-slate-500">
              <th className="pb-2 pr-3">Date</th>
              <th className="pb-2 pr-3">Insider</th>
              <th className="pb-2 pr-3">Transaction</th>
              <th className="pb-2 pr-3 text-right">Shares</th>
              <th className="pb-2 pr-3 text-right">Price</th>
              <th className="pb-2 pr-3 text-right">Post-trade</th>
              <th className="pb-2 pr-3">Security</th>
              <th className="pb-2">Notes</th>
            </tr>
          </thead>
          <tbody>
            {transactions.rows.map((txn) => (
              // accession_number + txn_row_num is the DB UNIQUE key,
              // so it's guaranteed stable and collision-free even for
              // filings with multiple rows per filer-date (e.g. an
              // RSU-vest pattern of same-day A + S).
              <Row key={`${txn.accession_number}-${txn.txn_row_num}`} txn={txn} />
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export function InsiderActivityPanel({ symbol }: InsiderActivityPanelProps) {
  const summaryState = useAsync<InsiderSummary>(
    useCallback(() => fetchInsiderSummary(symbol), [symbol]),
    [symbol],
  );
  const txnsState = useAsync<InsiderTransactionsList>(
    useCallback(() => fetchInsiderTransactions(symbol, 50), [symbol]),
    [symbol],
  );

  return (
    <Section title="Insider activity (Form 4)">
      {summaryState.loading || txnsState.loading ? (
        <SectionSkeleton rows={4} />
      ) : summaryState.error !== null || txnsState.error !== null ? (
        <SectionError
          onRetry={() => {
            summaryState.refetch();
            txnsState.refetch();
          }}
        />
      ) : summaryState.data === null || txnsState.data === null ? (
        <EmptyState
          title="Insider activity unavailable"
          description="Could not load Form 4 data for this instrument."
        />
      ) : (
        <Body summary={summaryState.data} transactions={txnsState.data} />
      )}
    </Section>
  );
}
