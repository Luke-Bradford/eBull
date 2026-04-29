/**
 * InsiderActivitySummary — compact insider summary block on the
 * density grid (#567). Uses total-activity lens consistently
 * (total_acquired_shares_90d, total_disposed_shares_90d,
 *  acquisition_count_90d + disposition_count_90d).
 *
 * NET 90d is computed client-side as acquired - disposed because
 * the response's `net_shares_90d` legacy alias maps to the
 * open-market net, NOT total-activity (would cross lenses).
 */

import { fetchInsiderSummary } from "@/api/instruments";
import type { InsiderSummary } from "@/api/instruments";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

export interface InsiderActivitySummaryProps {
  readonly symbol: string;
}

function fmt(n: number): string {
  if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  return n.toLocaleString();
}

function fmtSigned(n: number): string {
  if (n > 0) return `+${fmt(n)}`;
  if (n < 0) return `-${fmt(Math.abs(n))}`;
  return "0";
}

function num(v: string | null | undefined): number {
  if (v === null || v === undefined) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function InsiderActivitySummary({
  symbol,
}: InsiderActivitySummaryProps): JSX.Element {
  const state = useAsync<InsiderSummary>(
    useCallback(() => fetchInsiderSummary(symbol), [symbol]),
    [symbol],
  );
  const navigate = useNavigate();

  const handleExpand = useCallback(() => {
    navigate(`/instrument/${encodeURIComponent(symbol)}/insider`);
  }, [navigate, symbol]);

  return (
    <Pane
      title="Insider activity"
      scope="last 90 days"
      source={{ providers: ["sec_form4"] }}
      onExpand={handleExpand}
    >
      {state.loading ? (
        <SectionSkeleton rows={2} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null ? (
        <EmptyState
          title="No insider data"
          description="No Form 4 transactions on file for this instrument."
        />
      ) : (
        (() => {
          const acquired = num(state.data.total_acquired_shares_90d);
          const disposed = num(state.data.total_disposed_shares_90d);
          const net = acquired - disposed;
          const txns =
            state.data.acquisition_count_90d + state.data.disposition_count_90d;
          const arrow = net > 0 ? "↑" : net < 0 ? "↓" : "·";
          const netClass =
            net > 0
              ? "text-emerald-700"
              : net < 0
                ? "text-red-700"
                : "text-slate-700";
          return (
            <div className="grid grid-cols-5 gap-2 text-xs">
              <Field label="NET 90d">
                <span className={`font-medium tabular-nums ${netClass}`}>
                  {fmtSigned(net)} {arrow}
                </span>
              </Field>
              <Field label="ACQUIRED">
                <span className="tabular-nums">{fmt(acquired)} sh</span>
              </Field>
              <Field label="DISPOSED">
                <span className="tabular-nums">{fmt(disposed)} sh</span>
              </Field>
              <Field label="TXNS">
                <span className="tabular-nums">{txns}</span>
              </Field>
              <Field label="LATEST">
                <span className="tabular-nums">
                  {state.data.latest_txn_date ?? "—"}
                </span>
              </Field>
            </div>
          );
        })()
      )}
    </Pane>
  );
}

function Field({
  label,
  children,
}: {
  readonly label: string;
  readonly children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] uppercase tracking-wider text-slate-500">
        {label}
      </span>
      <span>{children}</span>
    </div>
  );
}
