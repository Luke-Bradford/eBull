/**
 * DividendsPanel — compact summary pane on the instrument overview page.
 * Shows last 4 quarters of history + an "Open →" button that navigates to
 * the full /instrument/:symbol/dividends drill-through page (#578).
 *
 * Full history lives at DividendsPage; this panel is intentionally trim:
 *   - NextDividendBanner (when upcoming[0] present)
 *   - DividendsSummaryBlock (TTM yield / TTM DPS / Latest DPS / Streak)
 *   - Last 4 HistoryBar rows
 *
 * Empty-state policy (Codex review of design-system v1): when both
 * history and upcoming are empty after data settles we render Pane
 * chrome with an empty-state line rather than null. Returning null
 * left an empty `lg:col-span-6` slot in the bento grid's Health row
 * (operator-visible dead space). Provider source + Open→ are
 * preserved so the operator can still drill through to the L2
 * dividends page for context.
 */

import { fetchInstrumentDividends } from "@/api/instruments";
import type { InstrumentDividends } from "@/api/instruments";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import {
  DividendsSummaryBlock,
  HistoryBar,
  NextDividendBanner,
} from "@/components/instrument/dividendsShared";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

export interface DividendsPanelProps {
  readonly symbol: string;
  /** Capability provider tag, resolved via
   *  ``summary.capabilities.dividends.providers`` upstream. The
   *  shell forwards it to the endpoint as ``?provider=<tag>``. */
  readonly provider: string;
}

const PREVIEW_ROWS = 4;

export function DividendsPanel({ symbol, provider }: DividendsPanelProps) {
  const navigate = useNavigate();
  const state = useAsync<InstrumentDividends>(
    useCallback(
      () => fetchInstrumentDividends(symbol, provider),
      [symbol, provider],
    ),
    [symbol, provider],
  );

  // Empty case: data loaded, no history AND no upcoming → render Pane
  // chrome with empty-state copy (carry source + onExpand so operator
  // can still drill to L2). Returning null would leave a dead 6-col
  // slot in the bento Health row.
  if (
    !state.loading &&
    state.error === null &&
    state.data !== null &&
    state.data.history.length === 0 &&
    state.data.upcoming.length === 0
  ) {
    return (
      <Pane
        title="Dividends"
        source={{ providers: [provider] }}
        onExpand={() =>
          navigate(
            `/instrument/${encodeURIComponent(symbol)}/dividends?provider=${encodeURIComponent(provider)}`,
          )
        }
      >
        <p className="text-xs text-slate-500">
          No dividend history or upcoming dividends on file.
        </p>
      </Pane>
    );
  }

  const last4 =
    state.data !== null ? state.data.history.slice(0, PREVIEW_ROWS) : [];
  const max =
    last4.length > 0
      ? last4.reduce((acc, p) => {
          if (p.dps_declared === null) return acc;
          const n = Number(p.dps_declared);
          return Number.isFinite(n) && n > acc ? n : acc;
        }, 0)
      : 0;

  return (
    <Pane
      title="Dividends"
      source={{ providers: [provider] }}
      onExpand={() =>
        navigate(
          `/instrument/${encodeURIComponent(symbol)}/dividends?provider=${encodeURIComponent(provider)}`,
        )
      }
    >
      {state.loading ? (
        <SectionSkeleton rows={3} />
      ) : state.error !== null || state.data === null ? (
        <SectionError onRetry={state.refetch} />
      ) : (
        <>
          {/* Upcoming banner renders above history so a company announcing
              its first-ever dividend via 8-K (with zero XBRL history yet)
              still shows the calendar. */}
          {state.data.upcoming[0] !== undefined && (
            <NextDividendBanner upcoming={state.data.upcoming[0]} />
          )}
          {state.data.history.length > 0 && (
            <div className="space-y-4">
              <DividendsSummaryBlock summary={state.data.summary} />
              <div>
                <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
                  Per-quarter DPS
                </div>
                <div className="space-y-1">
                  {last4.map((p) => (
                    <HistoryBar
                      key={`${p.period_end_date}-${p.period_type}`}
                      period={p}
                      max={max}
                    />
                  ))}
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </Pane>
  );
}
