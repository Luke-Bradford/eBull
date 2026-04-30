/**
 * Fund-data-at-a-glance stat row (AdminPage #323, spec §6).
 *
 * Renders four live cells backed by existing endpoints and three
 * `"–" (pending)` placeholders for summaries we don't yet expose.
 * Tooltips on pending cells name the blocking tech-debt.
 *
 * Each cell tolerates its backing fetch failing in isolation — a
 * dead fetch renders `–` with an amber error tone but does NOT wipe
 * any other cell.
 */
import type {
  CoverageSummaryResponse,
  RecommendationsListResponse,
} from "@/api/types";
import { formatDateTime } from "@/lib/format";

export interface FundDataRowProps {
  readonly coverage: CoverageSummaryResponse | null;
  readonly coverageError: boolean;
  readonly recommendations: RecommendationsListResponse | null;
  readonly recommendationsError: boolean;
}

interface Cell {
  readonly label: string;
  readonly value: string;
  readonly hint?: string;
  readonly tone: "ok" | "pending" | "error";
}

export function FundDataRow({
  coverage,
  coverageError,
  recommendations,
  recommendationsError,
}: FundDataRowProps): JSX.Element {
  const cells: Cell[] = [];

  cells.push(
    coverageError
      ? { label: "Tradable universe", value: "–", tone: "error" }
      : coverage === null
        ? { label: "Tradable universe", value: "–", tone: "pending" }
        : {
            label: "Tradable universe",
            value: String(coverage.total_tradable),
            tone: "ok",
          },
  );

  cells.push(
    coverageError
      ? { label: "Analysable", value: "–", tone: "error" }
      : coverage === null
        ? { label: "Analysable", value: "–", tone: "pending" }
        : {
            label: "Analysable",
            value: `${coverage.analysable} / ${coverage.total_tradable}`,
            hint:
              coverage.total_tradable > 0
                ? `${pct(coverage.analysable, coverage.total_tradable)}%`
                : undefined,
            tone: "ok",
          },
  );

  cells.push(
    coverageError
      ? { label: "Needs review", value: "–", tone: "error" }
      : coverage === null
        ? { label: "Needs review", value: "–", tone: "pending" }
        : {
            label: "Needs review",
            value: String(
              coverage.insufficient + coverage.structurally_young,
            ),
            tone: "ok",
          },
  );

  cells.push(
    recommendationsError
      ? { label: "Latest recommendation", value: "–", tone: "error" }
      : recommendations === null
        ? { label: "Latest recommendation", value: "–", tone: "pending" }
        : recommendations.items.length === 0
          ? {
              label: "Latest recommendation",
              value: "never",
              tone: "ok",
            }
          : {
              label: "Latest recommendation",
              value: formatDateTime(recommendations.items[0]!.created_at),
              tone: "ok",
            },
  );

  cells.push({
    label: "Tier 1/2/3",
    value: "–",
    hint: "endpoint pending",
    tone: "pending",
  });
  cells.push({
    label: "Latest score",
    value: "–",
    hint: "endpoint pending",
    tone: "pending",
  });
  cells.push({
    label: "Latest thesis",
    value: "–",
    hint: "endpoint pending",
    tone: "pending",
  });

  return (
    <div
      className="grid grid-cols-2 gap-x-6 gap-y-3 border-t border-slate-200 px-1 pt-3 pb-2 sm:grid-cols-4 lg:grid-cols-7"
      data-testid="fund-data-row"
    >
      {cells.map((c) => (
        <StatCell key={c.label} cell={c} />
      ))}
    </div>
  );
}

function StatCell({ cell }: { cell: Cell }): JSX.Element {
  const valueTone =
    cell.tone === "ok"
      ? "text-slate-800 dark:text-slate-100"
      : cell.tone === "error"
        ? "text-red-700"
        : "text-slate-400 dark:text-slate-500";
  return (
    <div title={cell.hint ?? undefined}>
      <div className="text-[10px] font-medium uppercase tracking-wider text-slate-400 dark:text-slate-500">
        {cell.label}
      </div>
      <div className={`text-lg font-semibold tabular-nums ${valueTone}`}>
        {cell.value}
      </div>
      {cell.hint ? (
        <div className="text-[11px] text-slate-400 dark:text-slate-500">{cell.hint}</div>
      ) : null}
    </div>
  );
}

function pct(a: number, b: number): string {
  if (b === 0) return "0.0";
  return ((a / b) * 100).toFixed(1);
}
