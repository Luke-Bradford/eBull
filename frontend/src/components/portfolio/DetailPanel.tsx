/**
 * DetailPanel — right pane of the portfolio workstation (#314).
 *
 * Renders the operator's selected position in four blocks:
 *   1. Header (symbol + company name, Add button, View Research link)
 *   2. Position snapshot + per-broker-position rows with Close buttons
 *   3. Latest thesis (stance, confidence, buy-zone, memo preview)
 *   4. Latest score (total + 5 sub-scores) and latest 3 filings
 *
 * Data sources:
 *   - Position data reuses `selectedPosition` from the already-loaded
 *     /portfolio response (display currency).
 *   - Thesis / filings / scores each fetched via useAsync on the
 *     `instrument_id` change — 404s render empty states, not errors.
 *   - Thesis valuation fields are NATIVE currency; we render them as
 *     plain numbers with a caption since ThesisDetail has no currency
 *     field and we deliberately do not fetch /portfolio/instruments/{id}
 *     here (the modals already do, and doubling the fetch blurs the
 *     panel/modal currency-boundary — see spec §2).
 */
import { Link } from "react-router-dom";

import { ApiError } from "@/api/client";
import { fetchFilings } from "@/api/filings";
import { fetchScoreHistory } from "@/api/scoreHistory";
import { fetchLatestThesis } from "@/api/theses";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import {
  formatDate,
  formatMoney,
  formatNumber,
  formatPct,
  pnlPct,
} from "@/lib/format";
import { useAsync } from "@/lib/useAsync";
import type {
  BrokerPositionItem,
  FilingItem,
  PositionItem,
  ScoreHistoryItem,
  ThesisDetail,
} from "@/api/types";

type ValuationSource = "quote" | "daily_close" | "cost_basis";

export interface CloseTargetInPanel {
  readonly instrumentId: number;
  readonly trade: BrokerPositionItem;
  readonly valuationSource: ValuationSource;
}

export interface DetailPanelProps {
  readonly selectedPosition: PositionItem | null;
  readonly currency: string;
  readonly onAdd: (p: PositionItem) => void;
  readonly onCloseTrade: (t: CloseTargetInPanel) => void;
}

export function DetailPanel({
  selectedPosition,
  currency,
  onAdd,
  onCloseTrade,
}: DetailPanelProps): JSX.Element {
  if (selectedPosition === null) {
    return (
      <aside className="hidden rounded-md border border-slate-200 bg-white p-4 text-sm text-slate-500 shadow-sm lg:block">
        Select a position to see its detail.
      </aside>
    );
  }

  return (
    <aside className="space-y-3 rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <Header
        position={selectedPosition}
        onAdd={() => onAdd(selectedPosition)}
      />
      <PositionSummary position={selectedPosition} currency={currency} />
      <BrokerPositionsTable
        position={selectedPosition}
        currency={currency}
        onCloseTrade={onCloseTrade}
      />
      <ThesisSection instrumentId={selectedPosition.instrument_id} />
      <ScoreSection instrumentId={selectedPosition.instrument_id} />
      <FilingsSection instrumentId={selectedPosition.instrument_id} />
    </aside>
  );
}

function Header({
  position,
  onAdd,
}: {
  position: PositionItem;
  onAdd: () => void;
}): JSX.Element {
  return (
    <header className="flex items-start justify-between gap-2">
      <div>
        <h2 className="text-base font-semibold text-slate-800">
          {position.symbol}
        </h2>
        <p className="text-xs text-slate-500">{position.company_name}</p>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <button
          type="button"
          onClick={onAdd}
          aria-label={`Add to ${position.symbol}`}
          className="rounded border border-blue-300 bg-white px-2 py-0.5 text-xs font-medium text-blue-700 hover:bg-blue-50"
        >
          Add
        </button>
        <Link
          to={`/instruments/${position.instrument_id}`}
          className="text-xs font-medium text-blue-700 hover:underline"
        >
          View research →
        </Link>
      </div>
    </header>
  );
}

function PositionSummary({
  position,
  currency,
}: {
  position: PositionItem;
  currency: string;
}): JSX.Element {
  const pct = pnlPct(position.unrealized_pnl, position.cost_basis);
  const positive = position.unrealized_pnl >= 0;
  return (
    <div className="grid grid-cols-2 gap-x-4 gap-y-1 rounded border border-slate-200 bg-slate-50 p-2 text-xs sm:grid-cols-4">
      <Stat label="Units" value={formatNumber(position.current_units, 6)} />
      <Stat
        label="Avg cost"
        value={position.avg_cost !== null ? formatMoney(position.avg_cost, currency) : "—"}
      />
      <Stat
        label="Market value"
        value={formatMoney(position.market_value, currency)}
      />
      <Stat
        label="P&L"
        value={
          <>
            <span className={positive ? "text-emerald-700" : "text-red-700"}>
              {formatMoney(position.unrealized_pnl, currency)}
            </span>
            {pct !== null ? (
              <span className="ml-1 text-[11px] text-slate-500">
                ({formatPct(pct)})
              </span>
            ) : null}
          </>
        }
      />
    </div>
  );
}

function Stat({
  label,
  value,
}: {
  label: string;
  value: React.ReactNode;
}): JSX.Element {
  return (
    <div>
      <div className="text-[10px] font-medium uppercase tracking-wider text-slate-400">
        {label}
      </div>
      <div className="tabular-nums">{value}</div>
    </div>
  );
}

export function BrokerPositionsTable({
  position,
  currency,
  onCloseTrade,
}: {
  position: PositionItem;
  currency: string;
  onCloseTrade: (t: CloseTargetInPanel) => void;
}): JSX.Element {
  const trades = position.trades;
  if (trades.length === 0) {
    return (
      <p className="text-xs text-slate-500">
        No broker positions for this instrument.
      </p>
    );
  }
  return (
    <div className="rounded border border-slate-200">
      <table className="w-full text-xs">
        <thead className="bg-slate-50 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-2 py-1 text-left">Position #</th>
            <th className="px-2 py-1 text-right">Units</th>
            <th className="px-2 py-1 text-right">Open rate</th>
            <th className="px-2 py-1 text-right">P&L</th>
            <th className="px-2 py-1 text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((t) => (
            <BrokerRow
              key={t.position_id}
              trade={t}
              instrumentId={position.instrument_id}
              valuationSource={position.valuation_source}
              currency={currency}
              onCloseTrade={onCloseTrade}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function BrokerRow({
  trade,
  instrumentId,
  valuationSource,
  currency,
  onCloseTrade,
}: {
  trade: BrokerPositionItem;
  instrumentId: number;
  valuationSource: ValuationSource;
  currency: string;
  onCloseTrade: (t: CloseTargetInPanel) => void;
}): JSX.Element {
  const positive = trade.unrealized_pnl >= 0;
  return (
    <tr className="border-t border-slate-100">
      <td className="px-2 py-1 text-left text-slate-600">{trade.position_id}</td>
      <td className="px-2 py-1 text-right tabular-nums">
        {formatNumber(trade.units, 6)}
      </td>
      <td className="px-2 py-1 text-right tabular-nums">
        {formatMoney(trade.open_rate, currency)}
      </td>
      <td
        className={`px-2 py-1 text-right tabular-nums ${
          positive ? "text-emerald-700" : "text-red-700"
        }`}
      >
        {formatMoney(trade.unrealized_pnl, currency)}
      </td>
      <td className="px-2 py-1 text-right">
        <button
          type="button"
          onClick={() =>
            onCloseTrade({ instrumentId, trade, valuationSource })
          }
          aria-label={`Close position ${trade.position_id}`}
          className="rounded border border-red-300 bg-white px-2 py-0.5 text-[11px] font-medium text-red-700 hover:bg-red-50"
        >
          Close
        </button>
      </td>
    </tr>
  );
}

function ThesisSection({ instrumentId }: { instrumentId: number }): JSX.Element {
  const thesis = useAsync(
    () => fetchLatestThesis(instrumentId),
    [instrumentId],
  );
  return (
    <section className="rounded border border-slate-200 p-2">
      <h3 className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-600">
        Latest thesis
      </h3>
      {thesis.loading ? (
        <SectionSkeleton rows={3} />
      ) : thesis.error ? (
        isNotFound(thesis.error) ? (
          <EmptyState
            title="No thesis yet"
            description="Runs will appear here after the thesis engine processes this instrument."
          />
        ) : (
          <SectionError onRetry={thesis.refetch} />
        )
      ) : thesis.data !== null ? (
        <ThesisBody thesis={thesis.data} instrumentId={instrumentId} />
      ) : null}
    </section>
  );
}

function ThesisBody({
  thesis,
  instrumentId,
}: {
  thesis: ThesisDetail;
  instrumentId: number;
}): JSX.Element {
  const preview = memoPreview(thesis.memo_markdown, 300);
  return (
    <div className="space-y-1 text-xs">
      <div className="flex flex-wrap items-center gap-2">
        <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[11px] font-medium text-slate-700">
          {thesis.stance}
        </span>
        <span className="text-slate-500">{thesis.thesis_type}</span>
        {thesis.confidence_score !== null ? (
          <span className="text-slate-500">
            confidence {formatNumber(thesis.confidence_score, 2)}
          </span>
        ) : null}
      </div>
      {(thesis.buy_zone_low !== null ||
        thesis.buy_zone_high !== null ||
        thesis.bull_value !== null ||
        thesis.base_value !== null ||
        thesis.bear_value !== null) && (
        <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-[11px] tabular-nums text-slate-600 sm:grid-cols-5">
          <Stat
            label="Buy low"
            value={thesis.buy_zone_low !== null ? formatNumber(thesis.buy_zone_low, 4) : "—"}
          />
          <Stat
            label="Buy high"
            value={thesis.buy_zone_high !== null ? formatNumber(thesis.buy_zone_high, 4) : "—"}
          />
          <Stat
            label="Bear"
            value={thesis.bear_value !== null ? formatNumber(thesis.bear_value, 4) : "—"}
          />
          <Stat
            label="Base"
            value={thesis.base_value !== null ? formatNumber(thesis.base_value, 4) : "—"}
          />
          <Stat
            label="Bull"
            value={thesis.bull_value !== null ? formatNumber(thesis.bull_value, 4) : "—"}
          />
        </div>
      )}
      <p className="text-[10px] text-slate-500">
        (valuations in instrument's native currency)
      </p>
      {preview !== "" ? (
        <p className="whitespace-pre-wrap text-slate-700">{preview}</p>
      ) : null}
      <Link
        to={`/instruments/${instrumentId}`}
        className="text-[11px] font-medium text-blue-700 hover:underline"
      >
        Read full thesis →
      </Link>
    </div>
  );
}

function ScoreSection({ instrumentId }: { instrumentId: number }): JSX.Element {
  const scores = useAsync(
    () => fetchScoreHistory(instrumentId, 5),
    [instrumentId],
  );
  return (
    <section className="rounded border border-slate-200 p-2">
      <h3 className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-600">
        Latest score
      </h3>
      {scores.loading ? (
        <SectionSkeleton rows={2} />
      ) : scores.error ? (
        isNotFound(scores.error) ? (
          <EmptyState
            title="No score yet"
            description="Scores appear once the ranking engine has run."
          />
        ) : (
          <SectionError onRetry={scores.refetch} />
        )
      ) : scores.data !== null && scores.data.items.length > 0 ? (
        <ScoreBody item={scores.data.items[0]!} />
      ) : (
        <EmptyState title="No score data" />
      )}
    </section>
  );
}

function ScoreBody({ item }: { item: ScoreHistoryItem }): JSX.Element {
  return (
    <div className="space-y-1 text-xs">
      <div className="flex items-center gap-3">
        <span className="text-sm font-semibold tabular-nums text-slate-800">
          {formatNumber(item.total_score, 2)}
        </span>
        {item.rank !== null ? (
          <span className="text-[11px] text-slate-500">
            rank #{item.rank}
            {item.rank_delta !== null && item.rank_delta !== 0 ? (
              <span
                className={
                  item.rank_delta < 0
                    ? "ml-1 text-emerald-700"
                    : "ml-1 text-red-700"
                }
              >
                ({item.rank_delta > 0 ? "+" : ""}
                {item.rank_delta})
              </span>
            ) : null}
          </span>
        ) : null}
        <span className="text-[10px] text-slate-400">
          {item.model_version}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-[11px] tabular-nums text-slate-600 sm:grid-cols-5">
        <Stat label="Quality" value={formatNumber(item.quality_score, 2)} />
        <Stat label="Value" value={formatNumber(item.value_score, 2)} />
        <Stat
          label="Turnaround"
          value={formatNumber(item.turnaround_score, 2)}
        />
        <Stat label="Momentum" value={formatNumber(item.momentum_score, 2)} />
        <Stat
          label="Sentiment"
          value={formatNumber(item.sentiment_score, 2)}
        />
      </div>
    </div>
  );
}

function FilingsSection({
  instrumentId,
}: {
  instrumentId: number;
}): JSX.Element {
  const filings = useAsync(
    () => fetchFilings(instrumentId, 0, 3),
    [instrumentId],
  );
  return (
    <section className="rounded border border-slate-200 p-2">
      <h3 className="mb-1 text-xs font-semibold uppercase tracking-wider text-slate-600">
        Latest filings
      </h3>
      {filings.loading ? (
        <SectionSkeleton rows={3} />
      ) : filings.error ? (
        isNotFound(filings.error) ? (
          <EmptyState title="No filings recorded" />
        ) : (
          <SectionError onRetry={filings.refetch} />
        )
      ) : filings.data !== null && filings.data.items.length > 0 ? (
        <ul className="space-y-1 text-xs">
          {filings.data.items.map((item) => (
            <FilingRow key={item.filing_event_id} item={item} />
          ))}
        </ul>
      ) : (
        <EmptyState title="No filing events" />
      )}
    </section>
  );
}

function FilingRow({ item }: { item: FilingItem }): JSX.Element {
  const summary =
    item.extracted_summary !== null && item.extracted_summary.length > 0
      ? truncate(item.extracted_summary, 80)
      : "(no summary — open filing for details)";
  const link = item.source_url ?? item.primary_document_url;
  return (
    <li className="flex flex-col border-l-2 border-slate-200 pl-2">
      <div className="flex items-center gap-2 text-[10px] text-slate-500">
        <span>{formatDate(item.filing_date)}</span>
        {item.filing_type !== null ? <span>· {item.filing_type}</span> : null}
        <span>· {item.provider}</span>
      </div>
      <div className="text-slate-700">{summary}</div>
      {link !== null ? (
        <a
          href={link}
          target="_blank"
          rel="noopener noreferrer"
          className="text-[11px] text-blue-700 hover:underline"
        >
          Open →
        </a>
      ) : null}
    </li>
  );
}

function memoPreview(memo: string, n: number): string {
  const trimmed = memo.trim();
  return trimmed.length <= n ? trimmed : `${trimmed.slice(0, n)}…`;
}

function truncate(s: string, n: number): string {
  return s.length <= n ? s : `${s.slice(0, n)}…`;
}

function isNotFound(err: unknown): boolean {
  return err instanceof ApiError && err.status === 404;
}
