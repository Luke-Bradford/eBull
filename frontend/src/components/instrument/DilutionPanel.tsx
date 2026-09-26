/**
 * DilutionPanel — the instrument report's share count & dilution (#3390 slice 5).
 *
 * Renders `/instruments/{symbol}/dilution` (#435, `instrument_dilution_summary`
 * + `share_count_history`, sql/259 / sql/273). Only the share COUNT measures are
 * shown. The "TTM" issuance / buyback sums are left out: the view sums the four
 * newest flow rows with no date bound, so a filer that stopped tagging the
 * concept gets a years-old figure (JPM on dev, 2026-09-26: "TTM issued" 4.47B =
 * its 2008-09-30 and 2009-06-30 facts).
 *
 * The comparison count is NOT a year back, whatever `net_dilution_pct_yoy`
 * says: the view takes the 4th-newest positive count, and cover-page and
 * balance-sheet dates both count as periods. Measured 2026-09-26 over 4,294
 * instruments: the gap from the latest count is p10 115 / p50 129 / p90 1,096
 * days. Reproduce with:
 *   WITH o AS (SELECT instrument_id, period_end, row_number() OVER
 *     (PARTITION BY instrument_id ORDER BY period_end DESC) rn
 *     FROM share_count_history WHERE shares_outstanding > 0)
 *   SELECT percentile_cont(ARRAY[0.1,0.5,0.9]) WITHIN GROUP
 *     (ORDER BY a.period_end - b.period_end), count(*)
 *   FROM o a JOIN o b USING (instrument_id) WHERE a.rn = 1 AND b.rn = 4;
 * So the change is labelled with the comparison count's own date, and the
 * view's ±2% posture, a one-year rule, is not shown.
 */

import { type JSX } from "react";

import { fetchInstrumentDilution } from "@/api/instruments";
import type { InstrumentDilution, ShareCountPeriod } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import {
  ReportCard,
  ReportNote,
  ReportRow,
} from "@/components/instrument/ReportCard";
import { Sparkline } from "@/components/instrument/Sparkline";
import { EmptyState } from "@/components/states/EmptyState";
import { formatBigNumber, formatDate, formatPct } from "@/lib/format";
import { parseDecimal } from "@/lib/riskView";
import { useAsync } from "@/lib/useAsync";

// Periods requested (the route's own default, ~10 years of quarters).
const HISTORY_PERIODS = 40;

const BASIS_NOTE =
  "Shares outstanding as the company reports them in SEC XBRL (the cover-page count, else the balance-sheet figure). Not split-adjusted: a split inside the year shows as a change in count. Only undimensioned facts are available, so a multi-class issuer may show one class. Ordinary shares, not depositary shares. Descriptive only: no weight in the score.";

/**
 * Date of the count the view compares against: its 4th-newest positive count
 * (`instrument_dilution_summary.year_ago`, sql/259), recovered from the same
 * newest-first history. Null unless that row's count is the view's figure.
 */
export function comparisonDate(
  history: ReadonlyArray<ShareCountPeriod>,
  yoyShares: string | null,
): string | null {
  const counts = history.filter((p) => (parseDecimal(p.shares_outstanding) ?? 0) > 0);
  const row = counts[3];
  if (row === undefined || yoyShares === null) return null;
  // Both are the same NUMERIC column serialised by the same model: compare the
  // wire strings exactly rather than as floats.
  return row.shares_outstanding === yoyShares ? row.period_end : null;
}

export function DilutionPanel({ symbol }: { readonly symbol: string }): JSX.Element {
  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const dilution = useAsync<InstrumentDilution>(
    () => fetchInstrumentDilution(symbol, HISTORY_PERIODS),
    [symbol],
  );
  return (
    <ReportCard title="Share count & dilution">
      {dilution.loading ? (
        <SectionSkeleton rows={4} />
      ) : dilution.error !== null || dilution.data === null ? (
        <SectionError onRetry={dilution.refetch} />
      ) : (
        <DilutionBody data={dilution.data} />
      )}
    </ReportCard>
  );
}

function DilutionBody({ data }: { data: InstrumentDilution }): JSX.Element {
  const s = data.summary;
  const latest = parseDecimal(s.latest_shares);
  if (latest === null) {
    return (
      <EmptyState
        title="No share count on file"
        description="Counts come from the company's SEC XBRL filings via the daily fundamentals sync; non-US listings and funds have none."
      />
    );
  }
  const yoyPct = parseDecimal(s.net_dilution_pct_yoy);
  // Oldest→newest for the sparkline; the API returns newest first.
  const counts = data.history
    .map((p) => parseDecimal(p.shares_outstanding))
    .filter((v): v is number => v !== null)
    .reverse();
  const since = comparisonDate(data.history, s.yoy_shares);
  const sinceLabel = since === null ? "earlier count" : formatDate(since);
  return (
    <>
      <div className="space-y-0.5">
        <ReportRow
          label={`shares outstanding (${formatDate(s.latest_as_of)})`}
          value={formatBigNumber(latest)}
        />
        <ReportRow
          label={`compared with (${sinceLabel})`}
          value={formatBigNumber(parseDecimal(s.yoy_shares))}
        />
        <ReportRow label="change" value={yoyPct === null ? "—" : formatPct(yoyPct / 100)} />
      </div>
      {yoyPct === null && (
        <p className="mt-1 text-[11px] text-slate-400">No earlier count to compare.</p>
      )}
      {counts.length >= 2 && (
        <div className="mt-2 flex items-center gap-3">
          <Sparkline
            values={counts}
            width={160}
            height={32}
            formatValue={(v) => formatBigNumber(v)}
            className="text-blue-600 dark:text-blue-400"
          />
          <span className="text-[10px] text-slate-400">
            {counts.length} reported counts
          </span>
        </div>
      )}
      <ReportNote>{BASIS_NOTE}</ReportNote>
    </>
  );
}
