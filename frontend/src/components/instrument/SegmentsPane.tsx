/**
 * SegmentsPane — latest-fiscal-year revenue breakdown for the
 * instrument page (#554). Backed by
 * GET /instruments/{symbol}/segments?axis=business|product|geographic.
 *
 * One axis visible at a time via the toggle; the fetch re-fires per
 * (symbol, axis) — useAsync owns the lifecycle, so switching axes
 * shows the skeleton rather than the previous axis's rows.
 *
 * 404 (fetcher → null) is the structural empty state: non-SEC issuer,
 * pre-XBRL-mandate 10-K, or no disclosure on the chosen axis (banks
 * routinely emit no revenue facts on the product axis).
 */

import { fetchInstrumentSegments } from "@/api/instruments";
import type { InstrumentSegments, SegmentAxis } from "@/api/instruments";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { GeographicMixChart } from "@/components/instrument/GeographicMixChart";
import { Pane } from "@/components/instrument/Pane";
import { SegmentsTable } from "@/components/instrument/SegmentsTable";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useState } from "react";

const AXES: ReadonlyArray<{ key: SegmentAxis; label: string }> = [
  { key: "business", label: "Segments" },
  { key: "product", label: "Products" },
  { key: "geographic", label: "Geography" },
];

export interface SegmentsPaneProps {
  readonly symbol: string;
}

export function SegmentsPane({ symbol }: SegmentsPaneProps) {
  const [axis, setAxis] = useState<SegmentAxis>("business");
  const state = useAsync<InstrumentSegments | null>(
    useCallback(() => fetchInstrumentSegments(symbol, axis), [symbol, axis]),
    [symbol, axis],
  );

  return (
    <Pane title="Revenue & segments" source={{ providers: ["sec_edgar"] }}>
      <div className="space-y-3">
        <div className="flex gap-1" role="group" aria-label="Breakdown axis">
          {AXES.map((a) => (
            <button
              key={a.key}
              type="button"
              onClick={() => setAxis(a.key)}
              className={`rounded border px-2 py-0.5 text-xs ${
                a.key === axis
                  ? "border-blue-600 bg-blue-50 text-blue-700 dark:border-blue-500 dark:bg-blue-900/30 dark:text-blue-300"
                  : "border-slate-300 text-slate-600 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
              }`}
            >
              {a.label}
            </button>
          ))}
        </div>
        {state.loading ? (
          <SectionSkeleton rows={4} />
        ) : state.error !== null ? (
          <SectionError onRetry={state.refetch} />
        ) : state.data === null || state.data.rows.length === 0 ? (
          <EmptyState
            title="No breakdown on file"
            description="The latest 10-K discloses nothing on this axis, predates the XBRL mandate, or the SEC filings ingest has not drained this instrument yet. Try another axis above."
          />
        ) : (
          <Body data={state.data} />
        )}
      </div>
    </Pane>
  );
}

function Body({ data }: { data: InstrumentSegments }) {
  const hasOpIncome = data.rows.some((r) => r.operating_income !== null);
  const hasAssets = data.rows.some((r) => r.assets !== null);
  return (
    <div className="space-y-2">
      {data.axis === "geographic" ? (
        <GeographicMixChart rows={data.rows} />
      ) : (
        <SegmentsTable rows={data.rows} showOperatingIncome={hasOpIncome} showAssets={hasAssets} />
      )}
      <p className="text-xs text-slate-500">
        FY ending {data.period_end} · {Object.values(data.sources).length > 0 && (
          <span className="font-mono">{[...new Set(Object.values(data.sources))].join(" · ")}</span>
        )}
      </p>
    </div>
  );
}
