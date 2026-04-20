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
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
import { formatMoney, formatNumber, formatPct, pnlPct } from "@/lib/format";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { ClosePositionModal } from "@/components/orders/ClosePositionModal";
import { OrderEntryModal } from "@/components/orders/OrderEntryModal";
import type {
  BrokerPositionItem,
  PositionItem,
  PortfolioMirrorItem,
} from "@/api/types";

type RowItem =
  | { kind: "position"; data: PositionItem }
  | { kind: "mirror"; data: PortfolioMirrorItem };

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
  const currency = useDisplayCurrency();
  const navigate = useNavigate();

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

  // Positions + mirrors merged, sorted by dollar value, filtered by
  // search, then paged. Both row types contribute to "account worth",
  // so they share the same sorted list.
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
  }, [addFor, closeFor, drillInto]);

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
        <div className="space-y-3">
          <SummaryBar data={portfolio.data} currency={currency} />
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
                <kbd className="rounded bg-slate-100 px-1">/</kbd> search ·{" "}
                <kbd className="rounded bg-slate-100 px-1">j</kbd>/
                <kbd className="rounded bg-slate-100 px-1">k</kbd> move ·{" "}
                <kbd className="rounded bg-slate-100 px-1">Enter</kbd> open ·{" "}
                <kbd className="rounded bg-slate-100 px-1">Esc</kbd> clear
              </div>
            </>
          )}
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
  onDrill,
  onAdd,
  onClose,
}: {
  pageRows: RowItem[];
  currency: string;
  search: string;
  onSearchChange: (v: string) => void;
  searchRef: React.MutableRefObject<HTMLInputElement | null>;
  focusedIdx: number;
  onDrill: (row: RowItem) => void;
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
                  onDrill={() => onDrill(row)}
                  onAdd={onAdd}
                  onClose={onClose}
                />
              ) : (
                <MirrorRow
                  key={`mir-${row.data.mirror_id}`}
                  m={row.data}
                  currency={currency}
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
// Rows — both drill into a dedicated detail page on click
// ---------------------------------------------------------------------------

function PositionRow({
  p,
  currency,
  focused,
  onDrill,
  onAdd,
  onClose,
}: {
  p: PositionItem;
  currency: string;
  focused: boolean;
  onDrill: () => void;
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
    focused
      ? "bg-slate-100 border-l-2 border-l-slate-400"
      : "hover:bg-slate-50/70",
  ].join(" ");

  return (
    <tr
      className={rowClass}
      onClick={onDrill}
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

function MirrorRow({
  m,
  currency,
  focused,
  onDrill,
}: {
  m: PortfolioMirrorItem;
  currency: string;
  focused: boolean;
  onDrill: () => void;
}) {
  const pct = pnlPct(m.unrealized_pnl, m.funded);
  const positive = m.unrealized_pnl >= 0;

  const rowClass = [
    "cursor-pointer border-t border-slate-100 transition-colors",
    focused ? "bg-slate-100 border-l-2 border-l-slate-400" : "hover:bg-slate-50/70",
  ].join(" ");

  return (
    <tr
      className={rowClass}
      onClick={onDrill}
      data-testid={`mirror-row-${m.mirror_id}`}
    >
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
