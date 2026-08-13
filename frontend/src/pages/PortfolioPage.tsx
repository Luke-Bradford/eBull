import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchPortfolio } from "@/api/portfolio";
import { useAsync } from "@/lib/useAsync";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { UnconvertedBadge } from "@/components/portfolio/UnconvertedBadge";
import { ClosePositionModal } from "@/components/orders/ClosePositionModal";
import { OrderEntryModal } from "@/components/orders/OrderEntryModal";
import { ActivitySection } from "@/components/portfolio/ActivitySection";
import { PortfolioValueChart } from "@/components/dashboard/PortfolioValueChart";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { LivePriceCell } from "@/components/quotes/LivePriceCell";
import type {
  BrokerPositionItem,
  PositionItem,
  PortfolioMirrorItem,
  PortfolioResponse,
} from "@/api/types";
import { portfolioTotals } from "@/lib/portfolioTotals";
import { Avatar } from "@/lib/avatar";
import {
  buildSortedRows,
  matchesRowSearch,
  type RowItem,
} from "@/lib/portfolioRows";
import { Badge } from "@/components/ui/Badge";

interface CloseTarget {
  instrumentId: number;
  trade: BrokerPositionItem;
  valuationSource: PositionItem["valuation_source"];
}

const PAGE_SIZE = 50;

/**
 * Portfolio page — unified drill-in for positions + mirrors (#324).
 *
 * Revert of the #314 workstation split. Both row types behave the same:
 *   - Position row click → /instrument/:symbol?tab=positions
 *   - Mirror row click   → /copy-trading/:mirrorId
 * No right-side detail pane; the per-row Add / Close buttons still open
 * their modals inline so the #313 action surface is preserved.
 *
 * Keyboard:
 *   - `/` focuses search
 *   - `j` / `k` moves the focus ring
 *   - `Enter` drills into the focused row
 *   - `Esc` clears search / blurs input
 *
 * `b` / `c` shortcuts are gone with the selection model they depended
 * on — operators use the row buttons or drill into the detail page.
 */
export function PortfolioPage() {
  const portfolio = useAsync(fetchPortfolio, []);
  const navigate = useNavigate();

  const [tab, setTab] = useState<"positions" | "activity">("positions");
  const [search, setSearch] = useState("");
  const [focusedIdx, setFocusedIdx] = useState<number>(0);
  const [page, setPage] = useState<number>(1);

  const [addFor, setAddFor] = useState<PositionItem | null>(null);
  const [closeFor, setCloseFor] = useState<CloseTarget | null>(null);

  const searchRef = useRef<HTMLInputElement | null>(null);

  // Refs keep the window keyboard handler reading the freshest focus
  // index + visible rows without re-binding the listener on every
  // render.
  const focusedIdxRef = useRef(focusedIdx);
  const pageRowsRef = useRef<RowItem[]>([]);

  // Held position ids + every active-mirror underlying id, fed to
  // the page-level LiveQuoteProvider so a single SSE stream covers
  // both held rows AND the underlyings inside copy-trader rows.
  // Without the mirror underlyings, mirror_equity / unrealized_pnl
  // figures rendered on the page would only update when the
  // operator opens a copy-trader detail page. The backend computes
  // the union in `_load_mirror_underlying_instrument_ids` and ships
  // it on `PortfolioResponse.live_quote_instrument_ids`.
  const liveQuoteIds = useMemo(() => {
    if (portfolio.data === null) return [];
    return portfolio.data.live_quote_instrument_ids;
  }, [portfolio.data]);

  // Positions + mirrors merged, sorted by dollar value, filtered by
  // search, then paged. Both row types contribute to "account worth",
  // so they share the same sorted list.
  const allRows: RowItem[] = useMemo(() => {
    if (portfolio.data === null) return [];
    return buildSortedRows(portfolio.data.positions, portfolio.data.mirrors);
  }, [portfolio.data]);

  const visible = useMemo(
    () => allRows.filter((r) => matchesRowSearch(r, search)),
    [allRows, search],
  );
  const totalPages = Math.max(1, Math.ceil(visible.length / PAGE_SIZE));
  const pageRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return visible.slice(start, start + PAGE_SIZE);
  }, [visible, page]);

  useLayoutEffect(() => {
    focusedIdxRef.current = focusedIdx;
    pageRowsRef.current = pageRows;
  });

  useEffect(() => {
    if (pageRows.length === 0) return;
    setFocusedIdx((i) => Math.min(Math.max(i, 0), pageRows.length - 1));
  }, [pageRows.length]);

  useEffect(() => {
    if (page > totalPages) setPage(totalPages);
  }, [page, totalPages]);

  function handleFilled() {
    setAddFor(null);
    setCloseFor(null);
    portfolio.refetch();
  }

  // `useCallback` with `navigate` as the only dep keeps the function
  // identity stable so the window-keyboard `useEffect` can list it as
  // a dep without re-binding the listener every render (and without
  // hiding a real stale-closure risk behind eslint-disable).
  const drillInto = useCallback(
    (row: RowItem) => {
      if (row.kind === "position") {
        // Position rows drill into the research page's Positions tab
        // (per-stock research spec §4) — the operator lands on the
        // canonical research view with their position pre-selected.
        navigate(
          `/instrument/${encodeURIComponent(row.data.symbol)}?tab=positions`,
        );
      } else {
        navigate(`/copy-trading/${row.data.mirror_id}`);
      }
    },
    [navigate],
  );

  useEffect(() => {
    function isEditable(el: Element | null): boolean {
      if (el === null) return false;
      const tag = el.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
      return (el as HTMLElement).isContentEditable === true;
    }

    function onKey(e: KeyboardEvent) {
      // Positions-tab shortcuts only. On the Activity tab the table is
      // unmounted but `pageRowsRef` still holds the last positions page,
      // so an un-gated Enter would drill into a hidden row (Codex #1593).
      if (tab !== "positions") return;
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (addFor !== null || closeFor !== null) return;

      const activeEditable = isEditable(document.activeElement);

      if (e.key === "Escape") {
        if (activeEditable && document.activeElement === searchRef.current) {
          searchRef.current?.blur();
          setSearch("");
          e.preventDefault();
          return;
        }
        setFocusedIdx(0);
        e.preventDefault();
        return;
      }

      if (activeEditable) return;

      if (e.key === "/") {
        e.preventDefault();
        searchRef.current?.focus();
        return;
      }
      if (e.key === "j") {
        const rows = pageRowsRef.current;
        if (rows.length === 0) return;
        setFocusedIdx((i) => Math.min(i + 1, rows.length - 1));
        e.preventDefault();
        return;
      }
      if (e.key === "k") {
        const rows = pageRowsRef.current;
        if (rows.length === 0) return;
        setFocusedIdx((i) => Math.max(i - 1, 0));
        e.preventDefault();
        return;
      }
      if (e.key === "Enter") {
        const rows = pageRowsRef.current;
        if (rows.length === 0) return;
        const target = rows[focusedIdxRef.current];
        if (target !== undefined) drillInto(target);
        e.preventDefault();
        return;
      }
    }

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [tab, addFor, closeFor, drillInto]);

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Portfolio</h1>
      </div>

      {/* Value-over-time chart above the tabs (#1594) — same component +
          ?value= URL key as the dashboard, so the range choice carries. */}
      <PortfolioValueChart />

      <div role="tablist" className="flex gap-1 border-b border-slate-200 dark:border-slate-800">
        {(
          [
            { key: "positions", label: "Positions" },
            { key: "activity", label: "Activity" },
          ] as const
        ).map((t) => {
          const active = tab === t.key;
          return (
            <button
              key={t.key}
              type="button"
              role="tab"
              aria-selected={active}
              onClick={() => setTab(t.key)}
              className={`-mb-px rounded-t border border-b-0 px-3 py-1 text-sm font-medium ${
                active
                  ? "border-slate-300 bg-white text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                  : "border-transparent bg-transparent text-slate-500 hover:bg-slate-50 dark:text-slate-400 dark:hover:bg-slate-800/40"
              }`}
            >
              {t.label}
            </button>
          );
        })}
      </div>

      {tab === "activity" ? (
        <ActivitySection />
      ) : portfolio.error !== null ? (
        <SectionError onRetry={portfolio.refetch} />
      ) : portfolio.loading || portfolio.data === null ? (
        <SectionSkeleton rows={8} />
      ) : (
        <LiveQuoteProvider instrumentIds={liveQuoteIds}>
        <div className="space-y-3">
          <SummaryBar data={portfolio.data} />
          {allRows.length === 0 ? (
            <EmptyState
              title="No positions yet"
              description="Open a position from the rankings page to see it here."
            >
              <Link
                to="/research?view=ranked"
                className="text-sm font-medium text-blue-600 hover:underline"
              >
                Go to rankings →
              </Link>
            </EmptyState>
          ) : (
            <>
              <PortfolioTable
                pageRows={pageRows}
                displayCurrency={portfolio.data.display_currency}
                cashCurrency={portfolio.data.cash_currency}
                search={search}
                onSearchChange={(v) => {
                  setSearch(v);
                  setPage(1);
                }}
                searchRef={searchRef}
                focusedIdx={focusedIdx}
                onDrill={drillInto}
                onAdd={(p) => setAddFor(p)}
                onClose={(t) => setCloseFor(t)}
              />
              {visible.length > PAGE_SIZE ? (
                <PaginationBar
                  page={page}
                  totalPages={totalPages}
                  onPrev={() => {
                    setPage((p) => Math.max(1, p - 1));
                    setFocusedIdx(0);
                  }}
                  onNext={() => {
                    setPage((p) => Math.min(totalPages, p + 1));
                    setFocusedIdx(0);
                  }}
                />
              ) : null}
              <div className="text-[10px] text-slate-400">
                <kbd className="rounded bg-slate-100 dark:bg-slate-800 px-1">/</kbd> search ·{" "}
                <kbd className="rounded bg-slate-100 dark:bg-slate-800 px-1">j</kbd>/
                <kbd className="rounded bg-slate-100 dark:bg-slate-800 px-1">k</kbd> move ·{" "}
                <kbd className="rounded bg-slate-100 dark:bg-slate-800 px-1">Enter</kbd> open ·{" "}
                <kbd className="rounded bg-slate-100 dark:bg-slate-800 px-1">Esc</kbd> clear
              </div>
            </>
          )}
        </div>
        </LiveQuoteProvider>
      )}

      {addFor !== null ? (
        <OrderEntryModal
          isOpen
          instrumentId={addFor.instrument_id}
          symbol={addFor.symbol}
          companyName={addFor.company_name}
          valuationSource={addFor.valuation_source}
          onRequestClose={() => setAddFor(null)}
          onFilled={handleFilled}
        />
      ) : null}

      {closeFor !== null ? (
        <ClosePositionModal
          isOpen
          instrumentId={closeFor.instrumentId}
          positionId={closeFor.trade.position_id}
          valuationSource={closeFor.valuationSource}
          onRequestClose={() => setCloseFor(null)}
          onFilled={handleFilled}
        />
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Summary bar
// ---------------------------------------------------------------------------

function SummaryBar({ data }: { data: PortfolioResponse }) {
  // #1901: shared totals + #2129 display-currency labeling (single source, also
  // used by the Dashboard cockpit's SummaryCards).
  const { totalPnl, pnlFraction, displayCurrency, hasUnconverted } = portfolioTotals(data);
  const mirrorCount = (data.mirrors ?? []).length;
  const posCount = data.positions.length + mirrorCount;

  return (
    <div className="flex flex-wrap gap-x-8 gap-y-2 border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-2 text-sm">
      <Stat label="AUM" value={formatMoney(data.total_aum, displayCurrency)} />
      <Stat label="Cash" value={formatMoney(data.cash_balance, data.cash_currency)} />
      <Stat
        label="P&L"
        value={formatMoney(totalPnl, displayCurrency)}
        hint={pnlFraction === null ? undefined : formatPct(pnlFraction)}
        tone={totalPnl >= 0 ? "positive" : "negative"}
      />
      <Stat label="Positions" value={String(posCount)} />
      <Stat label="Instruments" value={String(data.positions.length)} />
      {mirrorCount > 0 ? <Stat label="Mirrors" value={String(mirrorCount)} /> : null}
      {hasUnconverted && (
        <span
          title={`Some positions couldn't be converted to ${displayCurrency}; totals may mix currencies.`}
          className="self-center text-xs font-medium text-amber-700 dark:text-amber-300"
        >
          ⚠ mixed currencies
        </span>
      )}
    </div>
  );
}

function Stat({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "positive" | "negative";
}) {
  return (
    <div className="min-w-[64px]">
      <div className="text-[11px] font-medium uppercase tracking-wider text-slate-400">
        {label}
      </div>
      <div className="text-sm font-semibold text-slate-800 dark:text-slate-100">{value}</div>
      {hint ? (
        <div
          className={`text-xs font-medium ${tone === "positive" ? "text-emerald-600" : "text-red-600"}`}
        >
          {hint}
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

function PortfolioTable({
  pageRows,
  displayCurrency,
  cashCurrency,
  search,
  onSearchChange,
  searchRef,
  focusedIdx,
  onDrill,
  onAdd,
  onClose,
}: {
  pageRows: RowItem[];
  displayCurrency: string;
  cashCurrency: string;
  search: string;
  onSearchChange: (v: string) => void;
  searchRef: React.MutableRefObject<HTMLInputElement | null>;
  focusedIdx: number;
  onDrill: (row: RowItem) => void;
  onAdd: (p: PositionItem) => void;
  onClose: (t: CloseTarget) => void;
}) {
  return (
    <div className="border-t border-slate-200 dark:border-slate-800 pt-3">
      <div className="px-1 pb-3">
        <input
          ref={searchRef}
          type="text"
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search positions…   (press / to focus)"
          aria-label="Search positions"
          className="w-full rounded border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/40 px-3 py-1.5 text-sm text-slate-700 placeholder-slate-400 outline-none focus:border-blue-300 focus:ring-1 focus:ring-blue-200"
        />
      </div>
      {pageRows.length === 0 ? (
        <div className="p-4 text-sm text-slate-500">
          No positions match your search.
        </div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/40 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
              <th className="px-4 py-2 text-left">Instrument</th>
              <th className="px-2 py-2 text-right">Trades</th>
              <th className="px-2 py-2 text-right">Units</th>
              <th className="px-2 py-2 text-right">Avg Entry</th>
              <th className="px-2 py-2 text-right">Price</th>
              <th className="px-2 py-2 text-right">Invested</th>
              <th className="px-2 py-2 text-right">Value</th>
              <th className="px-2 py-2 text-right">P&L</th>
              <th className="px-2 py-2 text-right">%</th>
              <th className="px-2 py-2 text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, idx) =>
              row.kind === "position" ? (
                <PositionRow
                  key={`pos-${row.data.instrument_id}`}
                  p={row.data}
                  displayCurrency={displayCurrency}
                  focused={idx === focusedIdx}
                  onDrill={() => onDrill(row)}
                  onAdd={onAdd}
                  onClose={onClose}
                />
              ) : (
                <MirrorRow
                  key={`mir-${row.data.mirror_id}`}
                  m={row.data}
                  currency={cashCurrency}
                  displayCurrency={displayCurrency}
                  focused={idx === focusedIdx}
                  onDrill={() => onDrill(row)}
                />
              ),
            )}
          </tbody>
        </table>
      )}
    </div>
  );
}

function PaginationBar({
  page,
  totalPages,
  onPrev,
  onNext,
}: {
  page: number;
  totalPages: number;
  onPrev: () => void;
  onNext: () => void;
}) {
  return (
    <div className="flex items-center justify-between border-t border-slate-200 dark:border-slate-800 px-1 pt-2 pb-1 text-xs">
      <button
        type="button"
        onClick={onPrev}
        disabled={page <= 1}
        className="rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-2 py-0.5 font-medium text-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800/40 disabled:opacity-40"
      >
        ← Prev
      </button>
      <span className="text-slate-600">
        Page {page} of {totalPages}
      </span>
      <button
        type="button"
        onClick={onNext}
        disabled={page >= totalPages}
        className="rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 px-2 py-0.5 font-medium text-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800/40 disabled:opacity-40"
      >
        Next →
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Rows — both drill into a dedicated detail page on click
// ---------------------------------------------------------------------------

function PositionRow({
  p,
  displayCurrency,
  focused,
  onDrill,
  onAdd,
  onClose,
}: {
  p: PositionItem;
  displayCurrency: string;
  focused: boolean;
  onDrill: () => void;
  onAdd: (p: PositionItem) => void;
  onClose: (t: CloseTarget) => void;
}) {
  const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
  const positive = p.unrealized_pnl >= 0;
  // Money is in the position's own currency — display normally, or native on an
  // FX-degrade (#2129). Label each cell with it; badge the row when it diverges.
  const rowCurrency = p.currency;
  const unconverted = rowCurrency !== displayCurrency;
  const trades = p.trades;
  const singleTrade: BrokerPositionItem | null =
    trades.length === 1 && trades[0] !== undefined ? trades[0] : null;

  const rowClass = [
    "cursor-pointer border-t border-slate-100 transition-colors",
    focused
      ? "bg-slate-100 dark:bg-slate-800 border-l-2 border-l-slate-400"
      : "hover:bg-slate-50/70 dark:hover:bg-slate-800/40",
  ].join(" ");

  return (
    <tr
      className={rowClass}
      onClick={onDrill}
      data-testid={`position-row-${p.instrument_id}`}
    >
      <td className="px-4 py-2 text-left">
        <span className="font-medium text-slate-800 dark:text-slate-100">{p.symbol}</span>
        <span className="ml-1.5 text-xs text-slate-500">{p.company_name}</span>
        {unconverted && <UnconvertedBadge currency={rowCurrency} displayCurrency={displayCurrency} />}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {trades.length || "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {formatNumber(p.current_units)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-500">
        {p.avg_cost != null ? formatMoney(p.avg_cost, rowCurrency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <LivePriceCell
          instrumentId={p.instrument_id}
          fallback={p.current_price}
          currency={rowCurrency}
        />
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {formatMoney(p.cost_basis, rowCurrency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {formatMoney(p.market_value, rowCurrency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(p.unrealized_pnl, rowCurrency)}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pct === null ? "—" : formatPct(pct)}
        </span>
      </td>
      <td className="px-2 py-2 text-right whitespace-nowrap">
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onAdd(p);
          }}
          aria-label={`Add to ${p.symbol}`}
          className="mr-1 rounded border border-blue-300 bg-white dark:bg-slate-900 px-2 py-0.5 text-xs font-medium text-blue-700 hover:bg-blue-50"
        >
          Add
        </button>
        {singleTrade !== null ? (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onClose({
                instrumentId: p.instrument_id,
                trade: singleTrade,
                valuationSource: p.valuation_source,
              });
            }}
            aria-label={`Close ${p.symbol}`}
            className="rounded border border-red-300 bg-white dark:bg-slate-900 px-2 py-0.5 text-xs font-medium text-red-700 hover:bg-red-50"
          >
            Close
          </button>
        ) : null}
      </td>
    </tr>
  );
}

function MirrorRow({
  m,
  currency,
  displayCurrency,
  focused,
  onDrill,
}: {
  m: PortfolioMirrorItem;
  currency: string;
  displayCurrency: string;
  focused: boolean;
  onDrill: () => void;
}) {
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;
  const unconverted = currency !== displayCurrency;

  const rowClass = [
    "cursor-pointer border-t border-slate-100 transition-colors",
    focused ? "bg-slate-100 dark:bg-slate-800 border-l-2 border-l-slate-400" : "hover:bg-slate-50/70 dark:hover:bg-slate-800/40",
  ].join(" ");

  return (
    <tr
      className={rowClass}
      onClick={onDrill}
      data-testid={`mirror-row-${m.mirror_id}`}
    >
      <td className="px-4 py-2 text-left">
        <span className="inline-flex items-center gap-2">
          <Avatar username={m.parent_username} size="sm" />
          <span className="font-medium text-slate-800 dark:text-slate-100">
            {m.parent_username}
          </span>
          <Badge>COPY</Badge>
          <span className="text-[10px] text-slate-400">→</span>
          {unconverted && <UnconvertedBadge currency={currency} displayCurrency={displayCurrency} />}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {m.position_count}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-300">—</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-300">—</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-300">—</td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {formatMoney(m.funded, currency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {formatMoney(m.mirror_equity, currency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(m.unrealized_pnl, currency)}
        </span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {pct === null ? "—" : formatPct(pct)}
        </span>
      </td>
      <td className="px-2 py-2" />
    </tr>
  );
}
