import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchPortfolio } from "@/api/portfolio";
import { useAsync } from "@/lib/useAsync";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { ClosePositionModal } from "@/components/orders/ClosePositionModal";
import { OrderEntryModal } from "@/components/orders/OrderEntryModal";
import { DetailPanel } from "@/components/portfolio/DetailPanel";
import type { CloseTarget } from "@/components/portfolio/DetailPanel";
import type {
  BrokerPositionItem,
  PositionItem,
  PortfolioMirrorItem,
} from "@/api/types";

type RowItem =
  | { kind: "position"; data: PositionItem }
  | { kind: "mirror"; data: PortfolioMirrorItem };

const PAGE_SIZE = 50;

/**
 * Portfolio page — the operator's trading workstation (#314).
 *
 * Split layout (≥lg):
 *   - left pane: summary bar + search + table + pagination
 *   - right pane: DetailPanel for the currently-selected position
 *
 * Selection + keyboard shortcuts:
 *   - `/` focuses search, `j`/`k` move the focus ring, `Enter` selects,
 *     `Esc` clears selection (or blurs search), `b` opens Add modal on
 *     the selected position, `c` opens Close modal only when the
 *     selected position has exactly one broker trade underneath.
 *   - Shortcuts are attached via a window listener so they work
 *     regardless of DOM focus. Gated on: no input is focused (Esc is
 *     exempt), no modal is open, no modifier keys.
 *   - Clicking a mirror row still navigates to /copy-trading/:id.
 */
export function PortfolioPage() {
  const portfolio = useAsync(fetchPortfolio, []);
  const currency = useDisplayCurrency();

  const [search, setSearch] = useState("");
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [focusedIdx, setFocusedIdx] = useState<number>(0);
  const [page, setPage] = useState<number>(1);
  const [hint, setHint] = useState<string | null>(null);

  const [addFor, setAddFor] = useState<PositionItem | null>(null);
  const [closeFor, setCloseFor] = useState<CloseTarget | null>(null);

  const searchRef = useRef<HTMLInputElement | null>(null);

  // Refs track the freshest focus index + page rows + selected
  // position so the window keyboard handler (which captures closures
  // each effect run) always reads the current values, not a snapshot
  // from an earlier render. The refs also keep the effect's deps
  // array small — re-binding the listener every time `portfolio.data`
  // changes is wasteful when the handler only *reads* the position.
  const focusedIdxRef = useRef(focusedIdx);
  const pageRowsRef = useRef<RowItem[]>([]);
  const selectedPositionRef = useRef<PositionItem | null>(null);

  // Derived: all rows (positions + mirrors, sorted by value), then
  // filtered by search, then sliced for the current page.
  const allRows: RowItem[] = useMemo(() => {
    if (portfolio.data === null) return [];
    const positions = portfolio.data.positions.map<RowItem>((p) => ({
      kind: "position",
      data: p,
    }));
    const mirrors = portfolio.data.mirrors.map<RowItem>((m) => ({
      kind: "mirror",
      data: m,
    }));
    const combined = [...positions, ...mirrors];
    combined.sort((a, b) => {
      const mvA =
        a.kind === "position" ? a.data.market_value : a.data.mirror_equity;
      const mvB =
        b.kind === "position" ? b.data.market_value : b.data.mirror_equity;
      return mvB - mvA;
    });
    return combined;
  }, [portfolio.data]);

  const visible = useMemo(
    () => allRows.filter((r) => matchesSearch(r, search)),
    [allRows, search],
  );
  const totalPages = Math.max(1, Math.ceil(visible.length / PAGE_SIZE));
  const pageRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return visible.slice(start, start + PAGE_SIZE);
  }, [visible, page]);

  // Derive selectedPosition from the UNFILTERED positions so the
  // detail panel keeps rendering when the operator narrows search.
  const selectedPosition: PositionItem | null = useMemo(() => {
    if (selectedId === null || portfolio.data === null) return null;
    return (
      portfolio.data.positions.find((p) => p.instrument_id === selectedId) ??
      null
    );
  }, [selectedId, portfolio.data]);

  // Keep refs aligned with the current render so the window listener
  // never reads a stale snapshot. Must run AFTER selectedPosition is
  // derived.
  focusedIdxRef.current = focusedIdx;
  pageRowsRef.current = pageRows;
  selectedPositionRef.current = selectedPosition;

  // Clamp focusedIdx when pageRows.length changes.
  useEffect(() => {
    if (pageRows.length === 0) return;
    setFocusedIdx((i) => Math.min(Math.max(i, 0), pageRows.length - 1));
  }, [pageRows.length]);

  // Clamp page when it exceeds totalPages after search shrinks results.
  useEffect(() => {
    if (page > totalPages) setPage(totalPages);
  }, [page, totalPages]);

  // Clear stale selectedId after a /portfolio refetch that drops it
  // (e.g. a full close in ClosePositionModal). Otherwise the detail
  // panel would collapse but `b`/`c` would still try to act on a
  // ghost position — gated below via selectedPosition !== null.
  useEffect(() => {
    if (selectedId === null || portfolio.data === null) return;
    const stillExists = portfolio.data.positions.some(
      (p) => p.instrument_id === selectedId,
    );
    if (!stillExists) setSelectedId(null);
  }, [portfolio.data, selectedId]);

  function handleFilled() {
    setAddFor(null);
    setCloseFor(null);
    portfolio.refetch();
  }

  function handleSelectRow(row: RowItem, idxOnPage: number) {
    if (row.kind === "position") {
      setSelectedId(row.data.instrument_id);
      setFocusedIdx(idxOnPage);
      // Any stale multi-trade hint becomes irrelevant once the
      // operator picks a new row — clear it.
      setHint(null);
    }
    // Mirror rows intentionally do not set selection — the row itself
    // navigates via MirrorRow's onClick.
  }

  // Window-level keyboard handler so shortcuts work before the
  // operator has clicked anything.
  useEffect(() => {
    function isEditable(el: Element | null): boolean {
      if (el === null) return false;
      const tag = el.tagName;
      if (
        tag === "INPUT" ||
        tag === "TEXTAREA" ||
        tag === "SELECT"
      )
        return true;
      return (el as HTMLElement).isContentEditable === true;
    }

    function onKey(e: KeyboardEvent) {
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (addFor !== null || closeFor !== null) return;

      const activeEditable = isEditable(document.activeElement);

      // Esc is special-cased: always processed so it can blur the
      // search input + clear the search string.
      if (e.key === "Escape") {
        if (activeEditable && document.activeElement === searchRef.current) {
          searchRef.current?.blur();
          setSearch("");
          e.preventDefault();
          return;
        }
        setSelectedId(null);
        setFocusedIdx(0);
        setHint(null);
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
        if (target !== undefined && target.kind === "position") {
          setSelectedId(target.data.instrument_id);
          setHint(null);
        }
        e.preventDefault();
        return;
      }
      if (e.key === "b") {
        const pos = selectedPositionRef.current;
        if (pos === null) return;
        setAddFor(pos);
        setHint(null);
        e.preventDefault();
        return;
      }
      if (e.key === "c") {
        const pos = selectedPositionRef.current;
        if (pos === null) return;
        const trades = pos.trades;
        if (trades.length === 1 && trades[0] !== undefined) {
          setCloseFor({
            instrumentId: pos.instrument_id,
            trade: trades[0],
            valuationSource: pos.valuation_source,
          });
          setHint(null);
        } else if (trades.length > 1) {
          setHint(
            "Close requires a single broker position — use the detail panel.",
          );
        }
        e.preventDefault();
        return;
      }
    }

    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // Modal presence flags gate the handler; everything else the
    // handler reads is carried by refs, so the listener does not need
    // to re-bind on per-render state changes.
  }, [addFor, closeFor]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Portfolio</h1>
      </div>

      {portfolio.error !== null ? (
        <SectionError onRetry={portfolio.refetch} />
      ) : portfolio.loading || portfolio.data === null ? (
        <SectionSkeleton rows={8} />
      ) : (
        <div className="grid gap-4 lg:grid-cols-5">
          <div className="space-y-3 lg:col-span-3">
            <SummaryBar data={portfolio.data} currency={currency} />
            {hint !== null ? (
              <div
                role="status"
                className="rounded border border-amber-300 bg-amber-50 px-3 py-1.5 text-xs text-amber-800"
              >
                {hint}
              </div>
            ) : null}
            {allRows.length === 0 ? (
              <EmptyState
                title="No positions yet"
                description="Open a position from the rankings page to see it here."
              >
                <Link
                  to="/rankings"
                  className="text-sm font-medium text-blue-600 hover:underline"
                >
                  Go to rankings →
                </Link>
              </EmptyState>
            ) : (
              <>
                <PortfolioTable
                  pageRows={pageRows}
                  currency={currency}
                  search={search}
                  onSearchChange={(v) => {
                    setSearch(v);
                    setPage(1);
                  }}
                  searchRef={searchRef}
                  focusedIdx={focusedIdx}
                  selectedId={selectedId}
                  onSelectRow={handleSelectRow}
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
                  <kbd className="rounded bg-slate-100 px-1">/</kbd> search ·{" "}
                  <kbd className="rounded bg-slate-100 px-1">j</kbd>/
                  <kbd className="rounded bg-slate-100 px-1">k</kbd> move ·{" "}
                  <kbd className="rounded bg-slate-100 px-1">Enter</kbd>{" "}
                  select · <kbd className="rounded bg-slate-100 px-1">Esc</kbd>{" "}
                  clear · <kbd className="rounded bg-slate-100 px-1">b</kbd>{" "}
                  Add · <kbd className="rounded bg-slate-100 px-1">c</kbd>{" "}
                  Close
                </div>
              </>
            )}
          </div>
          <div className="lg:col-span-2">
            <DetailPanel
              selectedPosition={selectedPosition}
              currency={currency}
              onAdd={(p) => setAddFor(p)}
              onCloseTrade={(t) => setCloseFor(t)}
            />
          </div>
        </div>
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

function SummaryBar({
  data,
  currency,
}: {
  data: {
    total_aum: number;
    cash_balance: number | null;
    positions: PositionItem[];
    mirrors?: PortfolioMirrorItem[];
  };
  currency: string;
}) {
  const mirrors = data.mirrors ?? [];
  const totalPnl =
    data.positions.reduce((s, p) => s + p.unrealized_pnl, 0) +
    mirrors.reduce((s, m) => s + m.unrealized_pnl, 0);
  const totalInvested =
    data.positions.reduce((s, p) => s + p.cost_basis, 0) +
    mirrors.reduce((s, m) => s + m.funded, 0);
  const pct = totalInvested !== 0 ? totalPnl / totalInvested : null;
  const posCount = data.positions.length + mirrors.length;
  const mirrorCount = mirrors.length;

  return (
    <div className="flex flex-wrap gap-6 rounded-md border border-slate-200 bg-white px-5 py-3 text-sm shadow-sm">
      <Stat label="AUM" value={formatMoney(data.total_aum, currency)} />
      <Stat label="Cash" value={formatMoney(data.cash_balance, currency)} />
      <Stat
        label="P&L"
        value={formatMoney(totalPnl, currency)}
        hint={pct === null ? undefined : formatPct(pct)}
        tone={totalPnl >= 0 ? "positive" : "negative"}
      />
      <Stat label="Positions" value={String(posCount)} />
      <Stat label="Instruments" value={String(data.positions.length)} />
      {mirrorCount > 0 ? (
        <Stat label="Mirrors" value={String(mirrorCount)} />
      ) : null}
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
      <div className="text-sm font-semibold text-slate-800">{value}</div>
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

function matchesSearch(row: RowItem, q: string): boolean {
  if (!q) return true;
  const lower = q.toLowerCase();
  if (row.kind === "position") {
    return (
      row.data.symbol.toLowerCase().includes(lower) ||
      row.data.company_name.toLowerCase().includes(lower)
    );
  }
  return row.data.parent_username.toLowerCase().includes(lower);
}

const AVATAR_TONES = [
  "bg-blue-600",
  "bg-emerald-600",
  "bg-amber-600",
  "bg-rose-600",
  "bg-violet-600",
  "bg-cyan-600",
] as const;

function avatarTone(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++)
    hash = (hash * 31 + name.charCodeAt(i)) | 0;
  return AVATAR_TONES[Math.abs(hash) % AVATAR_TONES.length] ?? "bg-blue-600";
}

function PortfolioTable({
  pageRows,
  currency,
  search,
  onSearchChange,
  searchRef,
  focusedIdx,
  selectedId,
  onSelectRow,
  onAdd,
  onClose,
}: {
  pageRows: RowItem[];
  currency: string;
  search: string;
  onSearchChange: (v: string) => void;
  searchRef: React.MutableRefObject<HTMLInputElement | null>;
  focusedIdx: number;
  selectedId: number | null;
  onSelectRow: (row: RowItem, idxOnPage: number) => void;
  onAdd: (p: PositionItem) => void;
  onClose: (t: CloseTarget) => void;
}) {
  return (
    <div className="rounded-md border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-100 px-4 py-2">
        <input
          ref={searchRef}
          type="text"
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search positions…   (press / to focus)"
          aria-label="Search positions"
          className="w-full rounded border border-slate-200 bg-slate-50 px-3 py-1.5 text-sm text-slate-700 placeholder-slate-400 outline-none focus:border-blue-300 focus:ring-1 focus:ring-blue-200"
        />
      </div>
      {pageRows.length === 0 ? (
        <div className="p-4 text-sm text-slate-500">
          No positions match your search.
        </div>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-200 bg-slate-50 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
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
                  currency={currency}
                  focused={idx === focusedIdx}
                  selected={row.data.instrument_id === selectedId}
                  onSelect={() => onSelectRow(row, idx)}
                  onAdd={onAdd}
                  onClose={onClose}
                />
              ) : (
                <MirrorRow
                  key={`mir-${row.data.mirror_id}`}
                  m={row.data}
                  currency={currency}
                  focused={idx === focusedIdx}
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
    <div className="flex items-center justify-between rounded-md border border-slate-200 bg-white px-3 py-1.5 text-xs shadow-sm">
      <button
        type="button"
        onClick={onPrev}
        disabled={page <= 1}
        className="rounded border border-slate-200 bg-white px-2 py-0.5 font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40"
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
        className="rounded border border-slate-200 bg-white px-2 py-0.5 font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40"
      >
        Next →
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Position row — click selects for the detail panel
// ---------------------------------------------------------------------------

function PositionRow({
  p,
  currency,
  focused,
  selected,
  onSelect,
  onAdd,
  onClose,
}: {
  p: PositionItem;
  currency: string;
  focused: boolean;
  selected: boolean;
  onSelect: () => void;
  onAdd: (p: PositionItem) => void;
  onClose: (t: CloseTarget) => void;
}) {
  const pct = pnlPct(p.unrealized_pnl, p.cost_basis);
  const positive = p.unrealized_pnl >= 0;
  const trades = p.trades;
  const singleTrade: BrokerPositionItem | null =
    trades.length === 1 && trades[0] !== undefined ? trades[0] : null;

  const rowClass = [
    "cursor-pointer border-t border-slate-100 transition-colors",
    selected
      ? "bg-blue-50 border-l-2 border-l-blue-500"
      : focused
        ? "bg-slate-100 border-l-2 border-l-slate-400"
        : "hover:bg-slate-50/70",
  ].join(" ");

  return (
    <tr
      className={rowClass}
      onClick={onSelect}
      aria-selected={selected}
      data-testid={`position-row-${p.instrument_id}`}
    >
      <td className="px-4 py-2 text-left">
        <span className="font-medium text-slate-800">{p.symbol}</span>
        <span className="ml-1.5 text-xs text-slate-500">{p.company_name}</span>
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {trades.length || "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {formatNumber(p.current_units)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-500">
        {p.avg_cost != null ? formatMoney(p.avg_cost, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {p.current_price != null ? formatMoney(p.current_price, currency) : "—"}
      </td>
      <td className="px-2 py-2 text-right tabular-nums text-slate-600">
        {formatMoney(p.cost_basis, currency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        {formatMoney(p.market_value, currency)}
      </td>
      <td className="px-2 py-2 text-right tabular-nums">
        <span className={positive ? "text-emerald-600" : "text-red-600"}>
          {formatMoney(p.unrealized_pnl, currency)}
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
          className="mr-1 rounded border border-blue-300 bg-white px-2 py-0.5 text-xs font-medium text-blue-700 hover:bg-blue-50"
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
            className="rounded border border-red-300 bg-white px-2 py-0.5 text-xs font-medium text-red-700 hover:bg-red-50"
          >
            Close
          </button>
        ) : null}
      </td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Mirror row — click navigates to /copy-trading/:mirrorId (unchanged)
// ---------------------------------------------------------------------------

function MirrorRow({
  m,
  currency,
  focused,
}: {
  m: PortfolioMirrorItem;
  currency: string;
  focused: boolean;
}) {
  const navigate = useNavigate();
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;

  const rowClass = [
    "cursor-pointer border-t border-slate-100 transition-colors",
    focused ? "bg-slate-100 border-l-2 border-l-slate-400" : "hover:bg-slate-50/70",
  ].join(" ");

  return (
    <tr className={rowClass} onClick={() => navigate(`/copy-trading/${m.mirror_id}`)}>
      <td className="px-4 py-2 text-left">
        <span className="inline-flex items-center gap-2">
          <span
            className={`inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-[10px] font-semibold text-white ${avatarTone(m.parent_username)}`}
          >
            {m.parent_username.charAt(0).toUpperCase()}
          </span>
          <span className="font-medium text-slate-800">
            {m.parent_username}
          </span>
          <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
            COPY
          </span>
          <span className="text-[10px] text-slate-400">→</span>
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
