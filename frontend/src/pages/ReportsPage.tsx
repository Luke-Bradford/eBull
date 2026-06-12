/**
 * /reports — Period statements (#1592 child 2).
 *
 * Renders one immutable `report_snapshots` row as a financial
 * statement per the signed-off IA (docs/proposals/ui/
 * 2026-06-12-report-ia.md §§4–6). Monthly = full statement; weekly =
 * digest (sections 1, 2, 4, 5, 6 + notes/appendix).
 *
 * ONE fetch per report type (limit=100) drives the whole page: one
 * page-level skeleton, one page-level ErrorBanner — the single source
 * IS "all sources" (§6.4). v2 snapshots render the statement; v1
 * snapshots (no schema_version) render the corrected legacy branch.
 *
 * Period navigation is URL-backed (?type=&period=) with PUSH so the
 * back button walks periods (§6.6).
 */
import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { fetchMonthlyReports, fetchWeeklyReports } from "@/api/reports";
import type { ReportSnapshot } from "@/api/reports";
import { isMonthlyV2, isSnapshotV2, type SnapshotV2 } from "@/api/reportSnapshot";
import { Section, SectionSkeleton } from "@/components/dashboard/Section";
import { AccountSummary } from "@/components/reports/AccountSummary";
import { ActivitySection } from "@/components/reports/ActivitySection";
import { AttributionChart } from "@/components/reports/AttributionChart";
import { HoldingsSection } from "@/components/reports/HoldingsSection";
import { LegacyReport } from "@/components/reports/LegacyReport";
import {
  ChargesSection,
  IncomeSection,
  ModelThesisSection,
  RiskStatsSection,
  RollingReturnsSection,
} from "@/components/reports/MonthlySections";
import { PerformanceChart } from "@/components/reports/PerformanceChart";
import { PeriodHeader, type ReportTypeId } from "@/components/reports/PeriodHeader";
import {
  Masthead,
  NotesSection,
  StatementFooter,
  notesFor,
} from "@/components/reports/StatementChrome";
import { buildTrailingSeries } from "@/components/reports/snapshotMath";
import { EmptyState } from "@/components/states/EmptyState";
import { ErrorBanner } from "@/components/states/ErrorBanner";
import { useAsync } from "@/lib/useAsync";

const FETCH_LIMIT = 100;

function parseType(raw: string | null): ReportTypeId {
  return raw === "monthly" ? "monthly" : "weekly";
}

function PageSkeleton() {
  return (
    <div className="space-y-6" role="status" aria-live="polite">
      <div className="grid grid-cols-1 gap-x-6 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <div key={i} className="border-t border-slate-200 px-1 pb-1 pt-3 dark:border-slate-800">
            <SectionSkeleton rows={2} />
          </div>
        ))}
      </div>
      {[0, 1, 2].map((i) => (
        <div key={i} className="border-t border-slate-200 pt-3 dark:border-slate-800">
          <SectionSkeleton rows={4} />
        </div>
      ))}
    </div>
  );
}

function StatementV2({
  snap,
  reports,
}: {
  snap: SnapshotV2;
  reports: ReportSnapshot[];
}) {
  const monthly = isMonthlyV2(snap) ? snap : null;
  const weekly = !isMonthlyV2(snap) ? snap : null;
  const currency = snap.cover.display_currency;
  const notes = notesFor(snap.report_type);
  const benchmarkLabel = snap.performance.benchmark?.label ?? "S&P 500 (price index)";
  // §4.2 trailing window: 13 weekly / 12 monthly points ENDING at this
  // statement's period — a statement is a record; periods after it do
  // not belong on its chart.
  const windowSize = snap.report_type === "weekly" ? 13 : 12;
  const trailing = useMemo(() => {
    const upToSelected = reports.filter((r) => r.period_end <= snap.period_end);
    return buildTrailingSeries(upToSelected, windowSize);
  }, [reports, snap.period_end, windowSize]);

  return (
    <div className="space-y-6">
      <Masthead currency={currency} periodStart={snap.period_start} periodEnd={snap.period_end} />

      <Section title="Account summary">
        <AccountSummary cover={snap.cover} marker={notes.marker} />
      </Section>

      <Section
        title="Performance vs benchmark"
        action={<span>{trailing.length} period{trailing.length === 1 ? "" : "s"}</span>}
      >
        <PerformanceChart points={trailing} benchmarkLabel={benchmarkLabel} marker={notes.marker} />
      </Section>

      {monthly !== null ? (
        <Section title="Rolling returns">
          <RollingReturnsSection rolling={monthly.rolling_returns} />
        </Section>
      ) : null}

      <Section title="Attribution">
        <AttributionChart contribution={snap.period_contribution} currency={currency} />
      </Section>

      <Section title="Holdings & exposure">
        <HoldingsSection
          holdings={snap.holdings}
          risk={monthly !== null ? monthly.risk : null}
          currency={currency}
        />
      </Section>

      <Section title="Period activity">
        {weekly !== null ? (
          <ActivitySection
            opened={weekly.positions_opened}
            closed={weekly.positions_closed}
            currency={currency}
            marker={notes.marker}
          />
        ) : (
          // Monthly v2 snapshots do not carry activity keys (the #1596
          // contract emits them weekly-only) — missing-key treatment
          // per §4, not a nil line that would falsely claim "no trades".
          <EmptyState
            title="Not included in this snapshot"
            description="The monthly snapshot contract does not carry period activity yet — see the weekly statements for own-platform trades."
          />
        )}
      </Section>

      {monthly !== null ? (
        <>
          <div className="grid gap-4 md:grid-cols-2">
            <Section title="Dividends & income">
              <IncomeSection income={monthly.income} marker={notes.marker} />
            </Section>
            <Section title="Charges">
              <ChargesSection
                costs={monthly.costs}
                currency={currency}
                fxUnavailable={false}
                marker={notes.marker}
              />
            </Section>
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            <Section title="Risk & trade statistics">
              <RiskStatsSection
                risk={monthly.risk}
                tradeStats={monthly.trade_stats}
                bestTrade={monthly.best_trade}
                worstTrade={monthly.worst_trade}
                marker={notes.marker}
              />
            </Section>
            <Section title="Model & thesis review">
              <ModelThesisSection
                attribution={monthly.attribution_summary}
                thesis={monthly.thesis_summary}
                scoreChanges={monthly.score_changes}
              />
            </Section>
          </div>
        </>
      ) : null}

      <Section title="Notes & disclosures">
        <NotesSection notes={notes} />
      </Section>

      <Section title="Appendix: snapshot data">
        <details className="text-xs">
          <summary className="cursor-pointer text-slate-500">Raw JSON</summary>
          <pre className="mt-2 overflow-x-auto rounded bg-slate-50 p-2 dark:bg-slate-900/40">
            {JSON.stringify(snap, null, 2)}
          </pre>
        </details>
      </Section>

      <StatementFooter generatedAt={snap.generated_at} benchmarkLabel={benchmarkLabel} />
    </div>
  );
}

export function ReportsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const reportType = parseType(searchParams.get("type"));
  const periodParam = searchParams.get("period");

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const list = useAsync<ReportSnapshot[]>(
    () =>
      reportType === "weekly"
        ? fetchWeeklyReports(FETCH_LIMIT)
        : fetchMonthlyReports(FETCH_LIMIT),
    [reportType],
  );

  const reports = list.data ?? [];
  const selectedIndex =
    reports.length === 0
      ? -1
      : Math.max(
          0,
          periodParam !== null ? reports.findIndex((r) => r.period_start === periodParam) : 0,
        );
  const selected = selectedIndex >= 0 ? reports[selectedIndex] : undefined;

  const navigateTo = (type: ReportTypeId, periodStart: string | null) => {
    const next = new URLSearchParams();
    next.set("type", type);
    if (periodStart !== null) next.set("period", periodStart);
    // PUSH, not replace — the back button walks periods (§6.6).
    setSearchParams(next, { replace: false });
  };

  return (
    <div className="space-y-4">
      <header className="border-b border-slate-200 pb-2 dark:border-slate-800">
        <h1 className="text-xl font-semibold">Period statements</h1>
        <p className="text-sm text-slate-500">
          Immutable weekly and monthly statements generated from report snapshots.
        </p>
      </header>

      <PeriodHeader
        reportType={reportType}
        reports={reports}
        selectedIndex={selectedIndex}
        onTypeChange={(t) => navigateTo(t, null)}
        onSelectIndex={(i) => {
          const target = reports[i];
          if (target) navigateTo(reportType, target.period_start);
        }}
      />

      {list.loading ? (
        <PageSkeleton />
      ) : list.error !== null ? (
        <div className="space-y-3">
          <ErrorBanner message="Failed to load report snapshots. Check the browser console for details." />
          <button
            type="button"
            onClick={list.refetch}
            className="rounded border border-slate-300 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
          >
            Retry
          </button>
        </div>
      ) : reports.length === 0 || selected === undefined ? (
        <EmptyState
          title={`No ${reportType} statements yet`}
          description={`Statements appear once the ${reportType === "weekly" ? "weekly_report" : "monthly_report"} job has run — trigger it from Admin → Jobs.`}
        />
      ) : isSnapshotV2(selected.snapshot_json) ? (
        <StatementV2 snap={selected.snapshot_json} reports={reports} />
      ) : (
        <LegacyReport report={selected} />
      )}
    </div>
  );
}

export default ReportsPage;
