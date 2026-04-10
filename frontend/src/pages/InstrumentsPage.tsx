/**
 * Instruments browse page (#147).
 *
 * Single async source: GET /instruments. Supports search, sector/exchange/
 * coverage-tier filters, and server-side pagination. Columns are sortable
 * client-side within the current page (server-side sort is symbol ASC).
 *
 * Each instrument row links to /instruments/:id (detail page, #62).
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import {
  fetchInstruments,
  INSTRUMENTS_PAGE_LIMIT,
  type InstrumentsQuery,
} from "@/api/instruments";
import type { InstrumentListItem } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { formatMoney } from "@/lib/format";

// ---------------------------------------------------------------------------
// Filter state
// ---------------------------------------------------------------------------

interface Filters {
  search: string;
  sector: string | null;
  exchange: string | null;
  coverage_tier: number | null;
}

const INITIAL_FILTERS: Filters = {
  search: "",
  sector: null,
  exchange: null,
  coverage_tier: null,
};

// ---------------------------------------------------------------------------
// Sort state (client-side within the fetched page)
// ---------------------------------------------------------------------------

type SortKey = "symbol" | "sector" | "exchange" | "coverage_tier" | "last";
type SortDir = "asc" | "desc";

function compare(a: unknown, b: unknown, dir: SortDir): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  if (typeof a === "string" && typeof b === "string") {
    const cmp = a.localeCompare(b);
    return dir === "asc" ? cmp : -cmp;
  }
  if (typeof a === "number" && typeof b === "number") {
    return dir === "asc" ? a - b : b - a;
  }
  return 0;
}

function sortValue(item: InstrumentListItem, key: SortKey): unknown {
  switch (key) {
    case "symbol":
      return item.symbol;
    case "sector":
      return item.sector;
    case "exchange":
      return item.exchange;
    case "coverage_tier":
      return item.coverage_tier;
    case "last":
      return item.latest_quote?.last ?? null;
  }
}

// ---------------------------------------------------------------------------
// Coverage tier badge
// ---------------------------------------------------------------------------

const TIER_TONE: Record<number, string> = {
  1: "bg-emerald-50 text-emerald-700 border-emerald-200",
  2: "bg-blue-50 text-blue-700 border-blue-200",
  3: "bg-slate-50 text-slate-600 border-slate-200",
};

function TierBadge({ tier }: { tier: number | null }) {
  if (tier === null) {
    return <span className="text-xs text-slate-400">—</span>;
  }
  const tone = TIER_TONE[tier] ?? "bg-slate-50 text-slate-600 border-slate-200";
  return (
    <span
      className={`inline-block rounded border px-1.5 py-0.5 text-[10px] font-medium ${tone}`}
    >
      Tier {tier}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Page component
// ---------------------------------------------------------------------------

export function InstrumentsPage() {
  const [filters, setFilters] = useState<Filters>(INITIAL_FILTERS);
  const [page, setPage] = useState(0);
  const [sort, setSort] = useState<{ key: SortKey; dir: SortDir }>({
    key: "symbol",
    dir: "asc",
  });

  // Debounced search: apply after 300ms of inactivity.
  const [searchInput, setSearchInput] = useState("");
  useEffect(() => {
    const timer = setTimeout(() => {
      setFilters((prev) => ({ ...prev, search: searchInput }));
      setPage(0);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchInput]);

  const query: InstrumentsQuery = useMemo(
    () => ({
      search: filters.search || null,
      sector: filters.sector,
      exchange: filters.exchange,
      coverage_tier: filters.coverage_tier,
      offset: page * INSTRUMENTS_PAGE_LIMIT,
      limit: INSTRUMENTS_PAGE_LIMIT,
    }),
    [filters, page],
  );

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const result = useAsync(() => fetchInstruments(query), [query]);

  // Extract distinct sectors and exchanges from data for filter dropdowns.
  // This gives us a good-enough set from the current page; a full enum
  // endpoint would be better but is not in scope.
  const [knownSectors, setKnownSectors] = useState<string[]>([]);
  const [knownExchanges, setKnownExchanges] = useState<string[]>([]);

  useEffect(() => {
    if (!result.data) return;
    setKnownSectors((prev) => {
      const next = new Set(prev);
      for (const item of result.data!.items) {
        if (item.sector) next.add(item.sector);
      }
      return Array.from(next).sort();
    });
    setKnownExchanges((prev) => {
      const next = new Set(prev);
      for (const item of result.data!.items) {
        if (item.exchange) next.add(item.exchange);
      }
      return Array.from(next).sort();
    });
  }, [result.data]);

  const sorted = useMemo(() => {
    if (!result.data) return [];
    return [...result.data.items].sort((a, b) =>
      compare(sortValue(a, sort.key), sortValue(b, sort.key), sort.dir),
    );
  }, [result.data, sort]);

  const toggleSort = useCallback(
    (key: SortKey) => {
      setSort((prev) =>
        prev.key === key
          ? { key, dir: prev.dir === "asc" ? "desc" : "asc" }
          : { key, dir: "asc" },
      );
    },
    [],
  );

  const totalPages = result.data
    ? Math.ceil(result.data.total / INSTRUMENTS_PAGE_LIMIT)
    : 0;

  const handleFilterChange = useCallback(
    (key: keyof Omit<Filters, "search">, value: string | number | null) => {
      setFilters((prev) => ({ ...prev, [key]: value || null }));
      setPage(0);
    },
    [],
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Instruments</h1>
        {result.data && (
          <span className="text-xs text-slate-500">
            {result.data.total.toLocaleString()} instruments
          </span>
        )}
      </div>

      {/* Search + filters bar */}
      <div className="flex flex-wrap items-end gap-3">
        <div className="flex-1">
          <label className="mb-1 block text-xs font-medium text-slate-600">
            Search
          </label>
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Symbol or company name…"
            className="w-full rounded border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
          />
        </div>

        <FilterSelect
          label="Sector"
          value={filters.sector ?? ""}
          options={knownSectors}
          onChange={(v) => handleFilterChange("sector", v)}
        />
        <FilterSelect
          label="Exchange"
          value={filters.exchange ?? ""}
          options={knownExchanges}
          onChange={(v) => handleFilterChange("exchange", v)}
        />
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600">
            Tier
          </label>
          <select
            value={filters.coverage_tier ?? ""}
            onChange={(e) =>
              handleFilterChange(
                "coverage_tier",
                e.target.value ? Number(e.target.value) : null,
              )
            }
            className="rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700"
          >
            <option value="">All</option>
            <option value="1">Tier 1</option>
            <option value="2">Tier 2</option>
            <option value="3">Tier 3</option>
          </select>
        </div>
      </div>

      <Section title="Results">
        {result.loading ? (
          <SectionSkeleton rows={10} />
        ) : result.error !== null ? (
          <SectionError onRetry={result.refetch} />
        ) : sorted.length === 0 ? (
          <EmptyState
            title="No instruments found"
            description={
              filters.search || filters.sector || filters.exchange || filters.coverage_tier !== null
                ? "Try adjusting your search or filters."
                : "No instruments have been synced yet. Run the universe sync job from the Admin page."
            }
          />
        ) : (
          <>
            <InstrumentsTable
              items={sorted}
              sort={sort}
              onToggleSort={toggleSort}
            />
            {totalPages > 1 && (
              <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
                <span>
                  Page {page + 1} of {totalPages}
                </span>
                <div className="flex gap-2">
                  <button
                    type="button"
                    disabled={page === 0}
                    onClick={() => setPage((p) => p - 1)}
                    className="rounded border border-slate-200 bg-white px-2 py-1 font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40"
                  >
                    Previous
                  </button>
                  <button
                    type="button"
                    disabled={page >= totalPages - 1}
                    onClick={() => setPage((p) => p + 1)}
                    className="rounded border border-slate-200 bg-white px-2 py-1 font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </>
        )}
      </Section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Filter select
// ---------------------------------------------------------------------------

function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  onChange: (value: string | null) => void;
}) {
  return (
    <div>
      <label className="mb-1 block text-xs font-medium text-slate-600">
        {label}
      </label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value || null)}
        className="rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700"
      >
        <option value="">All</option>
        {options.map((opt) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Instruments table
// ---------------------------------------------------------------------------

const COLUMNS: { key: SortKey; label: string; align?: "right" }[] = [
  { key: "symbol", label: "Instrument" },
  { key: "sector", label: "Sector" },
  { key: "exchange", label: "Exchange" },
  { key: "coverage_tier", label: "Tier" },
  { key: "last", label: "Last price", align: "right" },
];

function SortIndicator({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) return <span className="ml-1 text-slate-300">↕</span>;
  return (
    <span className="ml-1 text-slate-600">
      {dir === "asc" ? "↑" : "↓"}
    </span>
  );
}

function InstrumentsTable({
  items,
  sort,
  onToggleSort,
}: {
  items: InstrumentListItem[];
  sort: { key: SortKey; dir: SortDir };
  onToggleSort: (key: SortKey) => void;
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            {COLUMNS.map((col) => (
              <th
                key={col.key}
                className={`cursor-pointer select-none py-2 pr-4 ${col.align === "right" ? "text-right" : ""}`}
                onClick={() => onToggleSort(col.key)}
              >
                {col.label}
                <SortIndicator
                  active={sort.key === col.key}
                  dir={sort.dir}
                />
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {items.map((item) => (
            <tr key={item.instrument_id} className="align-top hover:bg-slate-50">
              <td className="py-2 pr-4">
                <Link
                  to={`/instruments/${item.instrument_id}`}
                  className="text-blue-600 hover:underline"
                >
                  <span className="font-medium">{item.symbol}</span>
                </Link>
                <div className="text-xs text-slate-500">{item.company_name}</div>
              </td>
              <td className="py-2 pr-4 text-xs text-slate-600">
                {item.sector ?? "—"}
              </td>
              <td className="py-2 pr-4 text-xs text-slate-600">
                {item.exchange ?? "—"}
              </td>
              <td className="py-2 pr-4">
                <TierBadge tier={item.coverage_tier} />
              </td>
              <td className="py-2 pr-0 text-right text-xs tabular-nums text-slate-600">
                {item.latest_quote?.last != null
                  ? formatMoney(item.latest_quote.last)
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
