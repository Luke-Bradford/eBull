/**
 * SummaryStrip — sticky per-instrument header (Slice 1 of per-stock
 * research page, docs/superpowers/specs/2026-04-20-per-stock-research-page.md).
 *
 * Always-visible identity + price + thesis + score + position badges
 * plus the research-page action surface (Add / Close / Generate thesis).
 * Sits above the tab nav; stays visible as the operator scrolls through
 * financials or news.
 *
 * Action gating:
 *   - Close — only when `heldUnits > 0`.
 *   - Generate thesis — when no thesis, or thesis is > 30d old.
 *   - Add — always enabled on tradable instruments.
 */
import type {
  InstrumentSummary,
  InstrumentPositionDetail,
  ThesisDetail,
} from "@/api/types";
import { Term } from "@/components/Term";
import { liveTickDisplayPrice, useLiveQuote } from "@/lib/useLiveQuote";

const THESIS_STALE_DAYS = 30;

function formatPrice(
  value: string | null | undefined,
  currency: string | null | undefined,
): string {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  const formatted = num.toLocaleString(undefined, {
    maximumFractionDigits: 2,
    minimumFractionDigits: 2,
  });
  return currency ? `${currency} ${formatted}` : formatted;
}

function formatPct(value: string | null | undefined, signed = false): string {
  if (value === null || value === undefined) return "—";
  const num = Number(value);
  if (!Number.isFinite(num)) return "—";
  const pct = (num * 100).toFixed(2);
  const sign = signed && num > 0 ? "+" : "";
  return `${sign}${pct}%`;
}

function isThesisStale(thesis: ThesisDetail | null): boolean {
  if (thesis === null) return true;
  const created = new Date(thesis.created_at);
  if (Number.isNaN(created.getTime())) return true;
  const ageDays = (Date.now() - created.getTime()) / (1000 * 60 * 60 * 24);
  return ageDays > THESIS_STALE_DAYS;
}

function thesisTone(stance: string): string {
  switch (stance.toLowerCase()) {
    case "buy":
      return "bg-emerald-50 text-emerald-700 border-emerald-300";
    case "hold":
      return "bg-slate-100 dark:bg-slate-800 text-slate-700 border-slate-300 dark:border-slate-700";
    case "exit":
    case "sell":
      return "bg-red-50 text-red-700 border-red-300";
    default:
      return "bg-slate-100 dark:bg-slate-800 text-slate-700 border-slate-300 dark:border-slate-700";
  }
}

export interface SummaryStripProps {
  summary: InstrumentSummary;
  thesis: ThesisDetail | null;
  /** True iff the thesis fetch has settled (resolved or legitimate 404). */
  thesisLoaded: boolean;
  /** True iff the thesis fetch errored on a non-404 failure. */
  thesisError: boolean;
  position: InstrumentPositionDetail | null;
  /** True iff the position fetch has settled. */
  positionLoaded: boolean;
  /** True iff the position fetch errored on a non-404 failure. */
  positionError: boolean;
  onAdd: () => void;
  onClose: () => void;
  onGenerateThesis: () => void;
  generatingThesis: boolean;
}

export function SummaryStrip({
  summary,
  thesis,
  thesisLoaded,
  thesisError,
  position,
  positionLoaded,
  positionError,
  onAdd,
  onClose,
  onGenerateThesis,
  generatingThesis,
}: SummaryStripProps): JSX.Element {
  const { identity, price } = summary;
  // Live-quote overlay (#488). When an SSE tick arrives for this
  // instrument, it overrides the REST snapshot's current/currency.
  // Day change columns keep using the snapshot — the live tick
  // only carries bid/ask/last, not the daily anchor. Hook opens a
  // stream on mount (triggering a dynamic eToro Subscribe per #487)
  // and closes on unmount.
  const live = useLiveQuote(summary.instrument_id);
  const livePrice = liveTickDisplayPrice(live.tick);
  const displayCurrent = livePrice?.value ?? price?.current ?? null;
  const displayCurrency = livePrice?.currency ?? price?.currency ?? null;
  const changeNum = price?.day_change_pct != null ? Number(price.day_change_pct) : null;
  const changeColor =
    changeNum === null
      ? "text-slate-500"
      : changeNum >= 0
        ? "text-emerald-600"
        : "text-red-600";

  const heldUnits = position?.total_units ?? 0;
  const isHeld = heldUnits > 0;
  // Multi-trade positions can't be closed from this strip because
  // ClosePositionModal needs one specific position_id. The operator
  // goes to the Positions tab instead. Also gate on `positionLoaded`
  // (NOT errored) so a stale/unresolved fetch doesn't offer a
  // dead-end click.
  const canCloseFromStrip =
    positionLoaded &&
    !positionError &&
    isHeld &&
    position !== null &&
    position.trades.length === 1;
  const thesisStale = isThesisStale(thesis);
  // Still offer Generate thesis on errored thesis state — gives the
  // operator a retry affordance instead of silent lockout.
  const showGenerateThesis = (thesisLoaded && thesisStale) || thesisError;

  return (
    <div
      data-testid="summary-strip"
      className="sticky top-0 z-20 -mx-6 border-b border-slate-200 bg-white px-6 py-3 shadow-sm dark:border-slate-800 dark:bg-slate-900 dark:shadow-none"
    >
      {/* Row 1: identity + price */}
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <h1 className="text-2xl font-semibold text-slate-800 dark:text-slate-100">
          {identity.symbol}
        </h1>
        <span className="text-lg text-slate-600">
          {identity.display_name ?? "—"}
        </span>
        {summary.coverage_tier !== null ? (
          <Term
            term={`Tier ${summary.coverage_tier}`}
            className="rounded bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700 no-underline"
          >
            Tier {summary.coverage_tier}
          </Term>
        ) : null}
        {price || livePrice ? (
          <>
            <span className="ml-auto flex items-baseline gap-1.5 text-2xl font-semibold tabular-nums text-slate-800 dark:text-slate-100">
              {formatPrice(displayCurrent, displayCurrency)}
              {live.connected ? (
                <span
                  data-testid="live-pulse"
                  title="Live price stream active"
                  className="inline-block h-2 w-2 animate-pulse rounded-full bg-emerald-500"
                />
              ) : null}
            </span>
            <span className={`text-sm tabular-nums ${changeColor}`}>
              {price?.day_change != null && Number(price.day_change) >= 0
                ? "+"
                : ""}
              {price?.day_change ?? "—"} ({formatPct(price?.day_change_pct, true)})
            </span>
          </>
        ) : null}
      </div>

      {/* Row 2: sector strip */}
      <div className="mt-1 text-xs text-slate-500">
        {identity.sector ?? "—"}
        {identity.industry ? ` · ${identity.industry}` : ""}
        {identity.exchange ? ` · ${identity.exchange}` : ""}
        {identity.country ? ` · ${identity.country}` : ""}
      </div>

      {/* Row 3: badges + actions */}
      <div className="mt-3 flex flex-wrap items-center gap-2">
        {/* Show stance badge whenever we have thesis data, even when
            the sticky error flag is set — dropping the last-known
            stance/confidence would lose useful operator context
            during a refetch hiccup. Add a stale/errored qualifier
            inline. */}
        {thesis !== null ? (
          <span
            data-testid="thesis-badge"
            className={`inline-flex items-center rounded border px-2 py-0.5 text-xs font-medium ${thesisTone(thesis.stance)}`}
          >
            Thesis: {thesis.stance.toUpperCase()}
            {thesis.confidence_score !== null
              ? ` ${Math.round(Number(thesis.confidence_score) * 100)}%`
              : ""}
            {thesisStale ? (
              <span className="ml-1.5 text-amber-600">(stale)</span>
            ) : null}
          </span>
        ) : thesisLoaded ? (
          <span
            data-testid="thesis-badge-missing"
            className="inline-flex items-center rounded border border-slate-300 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/40 px-2 py-0.5 text-xs font-medium text-slate-500"
          >
            No thesis yet
          </span>
        ) : null}

        {thesisError ? (
          <span
            data-testid="thesis-badge-error"
            className="inline-flex items-center rounded border border-red-300 bg-red-50 px-2 py-0.5 text-xs font-medium text-red-700"
          >
            Thesis unavailable
          </span>
        ) : null}

        {positionError ? (
          <span
            data-testid="position-badge-error"
            className="inline-flex items-center rounded border border-red-300 bg-red-50 px-2 py-0.5 text-xs font-medium text-red-700"
          >
            Holdings unavailable
          </span>
        ) : positionLoaded && isHeld ? (
          <span
            data-testid="held-badge"
            className="inline-flex items-center rounded border border-blue-300 bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700"
          >
            Held: {heldUnits}u
          </span>
        ) : null}

        <div className="ml-auto flex gap-2">
          {summary.is_tradable ? (
            <button
              type="button"
              data-testid="action-add"
              onClick={onAdd}
              className="rounded border border-blue-300 bg-white dark:bg-slate-900 px-3 py-1 text-xs font-medium text-blue-700 hover:bg-blue-50"
            >
              Add
            </button>
          ) : null}
          {canCloseFromStrip ? (
            <button
              type="button"
              data-testid="action-close"
              onClick={onClose}
              className="rounded border border-red-300 bg-white dark:bg-slate-900 px-3 py-1 text-xs font-medium text-red-700 hover:bg-red-50"
            >
              Close
            </button>
          ) : null}
          {showGenerateThesis ? (
            <button
              type="button"
              data-testid="action-generate-thesis"
              onClick={onGenerateThesis}
              disabled={generatingThesis}
              className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800/40"
            >
              {generatingThesis ? "Generating…" : "Generate thesis"}
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
