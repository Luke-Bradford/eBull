/**
 * Fund-data-at-a-glance stat row (AdminPage #323, spec §6).
 *
 * Six live cells backed by existing endpoints. Each tolerates its backing
 * fetch failing in isolation — a dead fetch renders `–` with a red error
 * tone but does NOT wipe any other cell.
 *
 * #3050 — "Latest score" and "Latest thesis" used to be hardcoded
 * `"–" / "endpoint pending"`, which asserts "not built yet" about layers
 * that both have rows. Worse for `theses`, which had simply STOPPED: a
 * placeholder and a stale value are different operator facts and the card
 * could not tell them apart (the #3037 shape).
 *
 * ⚠ Both read `/system/status`, which the page already fetches — NOT a new
 * endpoint. `ops_monitor` already tracks both as data layers with a
 * documented SLA (`app/services/ops_monitor.py`: theses 3 days, scores 2
 * days) and already returns the `ok | stale | empty | error` verdict, so
 * the staleness tone here is READ, never re-derived and never thresholded
 * against a number invented in the frontend.
 *
 * ⚠ A third placeholder, "Tier 1/2/3", was DELETED rather than wired:
 * there is no tier column and no tier table, so it was not pending, it was
 * a decision never made. The grid divides evenly at six.
 */
import type {
  CoverageSummaryResponse,
  LayerHealthResponse,
  RecommendationsListResponse,
} from "@/api/types";
import { formatDateTime } from "@/lib/format";

export interface FundDataRowProps {
  readonly coverage: CoverageSummaryResponse | null;
  readonly coverageError: boolean;
  readonly recommendations: RecommendationsListResponse | null;
  readonly recommendationsError: boolean;
  /** `/system/status` layers; `null` while the fetch is in flight. */
  readonly layers: readonly LayerHealthResponse[] | null;
  readonly layersError: boolean;
}

interface Cell {
  readonly label: string;
  readonly value: string;
  readonly hint?: string;
  readonly tone: "ok" | "pending" | "stale" | "error";
}

export function FundDataRow({
  coverage,
  coverageError,
  recommendations,
  recommendationsError,
  layers,
  layersError,
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

  cells.push(layerCell("Latest score", "scores", layers, layersError));
  cells.push(layerCell("Latest thesis", "theses", layers, layersError));

  return (
    <div
      // Six cells, so the track counts all divide it evenly: 3 rows / 2 rows /
      // 1 row. The old seven left a ragged 4+3 at `sm`.
      className="grid grid-cols-2 gap-x-6 gap-y-3 border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-2 sm:grid-cols-3 lg:grid-cols-6"
      data-testid="fund-data-row"
    >
      {cells.map((c) => (
        <StatCell key={c.label} cell={c} />
      ))}
    </div>
  );
}

/**
 * One cell backed by an `/system/status` data layer.
 *
 * ⚠ The `ok | stale | empty | error` vocabulary is mapped 1:1 rather than
 * collapsed. Collapsing `empty` into `stale` (or either into `pending`) is
 * how #3050 happened: three distinct operator facts — "never ran", "stopped
 * N days ago" and "not wired" — rendered as one placeholder.
 *
 * ⚠ A layer absent from the response is NOT reported as healthy or as empty.
 * It is unknown, and says so.
 */
function layerCell(
  label: string,
  layerName: string,
  layers: readonly LayerHealthResponse[] | null,
  layersError: boolean,
): Cell {
  if (layersError) return { label, value: "–", hint: "status unavailable", tone: "error" };
  if (layers === null) return { label, value: "–", tone: "pending" };
  const layer = layers.find((l) => l.layer === layerName);
  if (layer === undefined) {
    return { label, value: "–", hint: "layer not reported", tone: "pending" };
  }
  if (layer.status === "error") {
    return { label, value: "–", hint: layer.detail || "layer error", tone: "error" };
  }
  if (layer.status === "empty" || layer.latest === null) {
    return { label, value: "never", hint: "no rows yet", tone: "pending" };
  }
  if (layer.status === "stale") {
    return {
      label,
      value: formatDateTime(layer.latest),
      // Derived from the payload, never written down: a hardcoded "23 days"
      // would go stale in the one place a reader trusts most.
      hint: stalenessHint(layer),
      tone: "stale",
    };
  }
  // ⚠ `ok` is asserted, not defaulted. `LayerStatus` is a compile-time union over
  // a JSON body, so a status the backend adds later arrives as a string this
  // build has never seen — and falling through to the healthy tone would render
  // it as fine. That is this ticket's own defect one branch over (review NITPICK).
  if (layer.status !== "ok") {
    return {
      label,
      value: formatDateTime(layer.latest),
      hint: `unrecognised status "${String(layer.status)}"`,
      tone: "stale",
    };
  }
  return { label, value: formatDateTime(layer.latest), tone: "ok" };
}

function stalenessHint(layer: LayerHealthResponse): string {
  const age = layer.age_seconds === null ? null : Math.floor(layer.age_seconds / 86_400);
  const sla = layer.max_age_seconds === null ? null : Math.floor(layer.max_age_seconds / 86_400);
  if (age === null) return "stale";
  return sla === null ? `stale — ${age}d` : `stale — ${age}d, SLA ${sla}d`;
}

function StatCell({ cell }: { cell: Cell }): JSX.Element {
  const valueTone =
    cell.tone === "ok"
      ? "text-slate-800 dark:text-slate-100"
      : cell.tone === "error"
        ? "text-red-700"
        : cell.tone === "stale"
          ? "text-amber-600 dark:text-amber-400"
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
