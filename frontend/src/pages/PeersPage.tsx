/**
 * /instrument/:symbol/peers — peer-comparison drill (#594).
 *
 * Three charts over `/instruments/{symbol}/peer-comparison` (#1751) + peer
 * candles: a multi-factor radar (instrument vs cohort median), a cohort heatmap
 * (instrument + peers × factors), and a same-day peer-return scatter. Mirrors
 * the #592/#593 drill shells.
 *
 * One useAsync keyed on [symbol] (Codex ckpt-1 #4): fetch peer-comparison, then
 * Promise.allSettled the instrument + peer candles in the same lifecycle. One
 * source → one error surface — peer-comparison drives the error; a failed peer
 * candle fetch degrades the scatter to empty, it never errors the page.
 *
 * Normalization is display-only (evidence layer), not scoring — see
 * lib/peerComparison.ts header + docs/settled-decisions.md §Scoring.
 */
import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";

import { fetchInstrumentCandles, fetchPeerComparison } from "@/api/instruments";
import type { CandleBar, PeerComparison } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import {
  PeerRadarChart,
  PeerReturnScatter,
  SectorHeatmap,
} from "@/components/peers/peerComparisonCharts";
import { EmptyState } from "@/components/states/EmptyState";
import {
  buildHeatmap,
  buildRadar,
  buildScatter,
  peerCoverage,
} from "@/lib/peerComparison";
import { useAsync } from "@/lib/useAsync";

interface PeersData {
  readonly pc: PeerComparison;
  readonly candlesBySymbol: Record<string, CandleBar[]>;
}

export function PeersPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  const state = useAsync<PeersData>(
    useCallback(async () => {
      const pc = await fetchPeerComparison(symbol);
      const symbols = [pc.symbol, ...pc.peers.map((p) => p.symbol)];
      const settled = await Promise.allSettled(
        symbols.map((s) => fetchInstrumentCandles(s, "6m")),
      );
      const candlesBySymbol: Record<string, CandleBar[]> = {};
      settled.forEach((res, i) => {
        const sym = symbols[i];
        if (sym !== undefined && res.status === "fulfilled") {
          candlesBySymbol[sym] = res.value.rows;
        }
      });
      return { pc, candlesBySymbol };
    }, [symbol]),
    [symbol],
  );

  const pc = state.data?.pc ?? null;
  const isEmpty = pc !== null && pc.factors.length === 0 && pc.peers.length === 0;

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <div className="mt-1 flex flex-wrap items-baseline justify-between gap-2">
          <h1 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
            Peer comparison — {symbol}
          </h1>
        </div>
        <p className="mt-1 text-xs text-slate-500">
          How {symbol} sits against its SIC-cohort peers across valuation, profitability, and leverage
          factors, plus daily return co-movement. Factor axes are normalized per factor for
          comparison (display only).
        </p>
      </header>

      {state.loading ? (
        <SectionSkeleton rows={6} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : pc === null || isEmpty ? (
        <EmptyState
          title="No peer data"
          description="No peer set for this instrument — likely a non-US issuer, an instrument without a SIC classification, or one lacking complete-TTM fundamentals."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <PeersBody data={state.data!} />
      )}
    </div>
  );
}

function PeersBody({ data }: { data: PeersData }): JSX.Element {
  const { pc, candlesBySymbol } = data;
  const radar = buildRadar(pc);
  const heatmap = buildHeatmap(pc);
  const scatter = buildScatter(
    pc.symbol,
    pc.peers.map((p) => p.symbol),
    candlesBySymbol,
  );
  const coverage = peerCoverage(pc);

  return (
    <>
      <p className="text-[11px] text-slate-500 dark:text-slate-400">
        SIC {pc.cohort_sic}
        {pc.cohort_sic_label ? ` · ${pc.cohort_sic_label}` : ""} ·{" "}
        {pc.cohort_sic_level === 0 ? "broad SIC-2 cohort" : `SIC-${pc.cohort_sic_level} cohort`} ·{" "}
        {pc.cohort_member_count.toLocaleString()} peers · {pc.peers.length} size-matched.
        {pc.cohort_sic_level === 0
          ? " Cohort widened to the 2-digit SIC (few same-industry peers) — read medians as noisy."
          : ""}
        {coverage.devLimitedKeys.length > 0
          ? ` ${coverage.devLimitedKeys.length} factor${coverage.devLimitedKeys.length === 1 ? "" : "s"} thin-coverage (greyed; median noisy).`
          : ""}
        {coverage.minCohortN < 50
          ? ` Some cohort medians rest on as few as ${coverage.minCohortN.toLocaleString()} members — read them as noisy.`
          : ""}
      </p>
      <Section title={`Multi-factor radar — ${pc.symbol} vs cohort median`}>
        <PeerRadarChart radar={radar} symbol={pc.symbol} />
      </Section>
      <Section title={`Cohort heatmap — ${pc.symbol} + peers × factors`}>
        <SectorHeatmap heatmap={heatmap} />
      </Section>
      <Section title="Peer return scatter — daily returns vs cohort">
        <PeerReturnScatter data={scatter} />
      </Section>
    </>
  );
}
