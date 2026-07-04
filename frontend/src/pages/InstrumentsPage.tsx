/**
 * Instruments browse page (#147).
 *
 * Single async source: GET /instruments. Supports search, sector/exchange/
 * coverage-tier filters, and server-side pagination. Columns are sortable
 * client-side within the current page (server-side sort is symbol ASC).
 *
 * Each instrument row links to /instrument/:symbol (research page).
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
import { Pagination } from "@/components/ui/Pagination";
import { formatMoney } from "@/lib/format";
import { SECTOR_OPTIONS } from "@/lib/sectors";
import { useAsync } from "@/lib/useAsync";

// ---------------------------------------------------------------------------
// Filter state
// ---------------------------------------------------------------------------

interface Filters {
  search: string;
  // #1675: real GICS sector-SPDR symbol (e.g. "XLK"); replaces the opaque
  // instruments.sector code as the filter dimension.
  sector_spdr: string | null;
  exchange: string | null;
  coverage_tier: number | null;
  has_dividend: boolean | null;
}

const INITIAL_FILTERS: Filters = {
  search: "",
  sector_spdr: null,
  exchange: null,
  coverage_tier: null,
  has_dividend: null,
};

// ---------------------------------------------------------------------------
// Sort state (client-side within the fetched page)
// ---------------------------------------------------------------------------

type SortKey = "symbol" | "gics_sector" | "exchange" | "coverage_tier" | "last";
type SortDir = "asc" | "desc";

// The server's default ordering (#1904): coverage_tier ASC (NULLS LAST), then
// symbol. The client's initial sort mirrors it exactly, so the default view is
// globally tier-ordered (not page-scoped) and the "sorted within this page"
// caveat is only shown once the operator picks a different column.
const SERVER_SORT: { key: SortKey; dir: SortDir } = { key: "coverage_tier", dir: "asc" };

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
    case "gics_sector":
      return item.gics_sector;
    case "exchange":
      // Sort by the visible label, not the raw eToro id (#1904) — otherwise
      // clicking "Exchange" orders by invisible numeric ids.
      return item.exchange_name ?? item.exchange;
    case "coverage_tier":
      return item.coverage_tier;
    case "last":
      // A usable mark is strictly positive (prevention-log #1428): eToro
      // persists `quotes.last = 0.00` for un-freshly-traded instruments, so a
      // non-null zero must sort with the blanks, not ahead of them.
      return isUsablePrice(item.latest_quote?.last) ? item.latest_quote!.last : null;
  }
}

/** A displayable last price: present and strictly positive (prevention-log #1428). */
function isUsablePrice(last: number | null | undefined): last is number {
  return last != null && last > 0;
}

/**
 * An "uncovered" row — an unmapped instrument (typically non-US) with no
 * sector, no coverage tier, and no usable price. Rather than render a row of
 * bare "—" cells, collapse the trailing columns into a single muted
 * "No coverage yet" note (#1924 dir #3). Tier-first ordering already sinks
 * these to the back of the list.
 */
function isUncovered(item: InstrumentListItem): boolean {
  return (
    item.gics_sector == null &&
    item.coverage_tier == null &&
    !isUsablePrice(item.latest_quote?.last)
  );
}

// ---------------------------------------------------------------------------
// Coverage tier badge
// ---------------------------------------------------------------------------

const TIER_TONE: Record<number, string> = {
  1: "bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300 border-emerald-200 dark:border-emerald-900/60",
  2: "bg-blue-50 dark:bg-blue-950/40 text-blue-700 dark:text-blue-300 border-blue-200 dark:border-blue-900/60",
  3: "bg-slate-50 dark:bg-slate-900/40 text-slate-600 border-slate-200 dark:border-slate-800",
};

function TierBadge({ tier }: { tier: number | null }) {
  if (tier === null) {
    return <span className="text-xs text-slate-400">—</span>;
  }
  const tone = TIER_TONE[tier] ?? "bg-slate-50 dark:bg-slate-900/40 text-slate-600 border-slate-200 dark:border-slate-800";
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
  const [sort, setSort] = useState<{ key: SortKey; dir: SortDir }>(SERVER_SORT);

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
      sector_spdr: filters.sector_spdr,
      exchange: filters.exchange,
      coverage_tier: filters.coverage_tier,
      has_dividend: filters.has_dividend,
      offset: page * INSTRUMENTS_PAGE_LIMIT,
      limit: INSTRUMENTS_PAGE_LIMIT,
    }),
    [filters, page],
  );

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const result = useAsync(() => fetchInstruments(query), [query]);

  // Sector dropdown uses the fixed 11 GICS sectors (#1675, SECTOR_OPTIONS), so
  // only exchange is still derived from the current page's data. A full enum
  // endpoint would be better but is not in scope. Each option carries the raw
  // exchangeId as `value` (the filter key) and the human name as `label`
  // (#1904 — never surface the raw numeric id in the dropdown).
  const [knownExchanges, setKnownExchanges] = useState<
    { value: string; label: string }[]
  >([]);

  // Reset accumulated exchange options when filters change (prevents stale
  // options from prior queries lingering in the dropdown).
  useEffect(() => {
    setKnownExchanges([]);
  }, [filters]);

  useEffect(() => {
    if (!result.data) return;
    setKnownExchanges((prev) => {
      const byId = new Map(prev.map((o) => [o.value, o.label]));
      for (const item of result.data!.items) {
        if (!item.exchange) continue;
        // Once a real name has been seen for an id, keep it — a later page
        // whose row happens to carry a null exchange_name must not revert the
        // dropdown label to the raw id (bot NITPICK, PR #1923).
        if (item.exchange_name != null) {
          byId.set(item.exchange, item.exchange_name);
        } else if (!byId.has(item.exchange)) {
          byId.set(item.exchange, item.exchange);
        }
      }
      return Array.from(byId, ([value, label]) => ({ value, label })).sort(
        (a, b) => a.label.localeCompare(b.label),
      );
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
    (
      key: keyof Omit<Filters, "search">,
      value: string | number | boolean | null,
    ) => {
      setFilters((prev) => ({ ...prev, [key]: value === "" ? null : value }));
      setPage(0);
    },
    [],
  );

  return (
    <div className="flex h-full flex-col gap-6 pt-6">
      <div className="flex flex-shrink-0 items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Instruments</h1>
        {result.data && (
          <span className="text-xs text-slate-500">
            {result.data.total.toLocaleString()} instruments
          </span>
        )}
      </div>

      {/* Search + filters bar */}
      <div className="flex flex-shrink-0 flex-wrap items-end gap-3">
        <div className="flex-1">
          <label className="mb-1 block text-xs font-medium text-slate-600">
            Search
          </label>
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Symbol or company name…"
            className="w-full rounded border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-1 focus:ring-blue-400 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
          />
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-slate-600">
            Sector
          </label>
          <select
            value={filters.sector_spdr ?? ""}
            onChange={(e) =>
              handleFilterChange("sector_spdr", e.target.value || null)
            }
            className="rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
          >
            <option value="">All</option>
            {SECTOR_OPTIONS.map((s) => (
              <option key={s.value} value={s.value}>
                {s.label}
              </option>
            ))}
          </select>
        </div>
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
            className="rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
          >
            <option value="">All</option>
            <option value="1">Tier 1</option>
            <option value="2">Tier 2</option>
            <option value="3">Tier 3</option>
          </select>
        </div>
        <div>
          <label
            htmlFor="filter-has-dividend"
            className="mb-1 block text-xs font-medium text-slate-600"
          >
            Dividend
          </label>
          <label className="flex items-center gap-2 rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100">
            <input
              id="filter-has-dividend"
              type="checkbox"
              checked={filters.has_dividend === true}
              onChange={(e) =>
                handleFilterChange(
                  "has_dividend",
                  e.target.checked ? true : null,
                )
              }
              className="h-4 w-4"
            />
            <span>Only dividend-paying</span>
          </label>
        </div>
      </div>

      <Section title="Results" scrollable>
        {result.loading ? (
          <SectionSkeleton rows={10} />
        ) : result.error !== null ? (
          <SectionError onRetry={result.refetch} />
        ) : sorted.length === 0 ? (
          <EmptyState
            title="No instruments found"
            description={
              filters.search ||
              filters.sector_spdr ||
              filters.exchange ||
              filters.coverage_tier !== null ||
              filters.has_dividend !== null
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
              pageScopedSort={totalPages > 1}
            />
            <Pagination
              page={page}
              totalPages={totalPages}
              onPageChange={setPage}
            />
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
  options: { value: string; label: string }[];
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
        className="rounded border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
      >
        <option value="">All</option>
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
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
  { key: "gics_sector", label: "Sector" },
  { key: "exchange", label: "Exchange" },
  { key: "coverage_tier", label: "Tier" },
  { key: "last", label: "Last price", align: "right" },
];

// Inline chevron SVGs (#1904): the previous unicode arrows (↕ / ↑ / ↓)
// rendered as tofu / "odd blue squares" in fonts lacking those glyphs. SVGs
// render identically everywhere and inherit the header's text colour.
function SortIndicator({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) {
    // Inactive: faint up/down stack so every column reads as sortable.
    return (
      <svg
        aria-hidden="true"
        viewBox="0 0 16 16"
        className="ml-1 inline-block h-3 w-3 text-slate-300"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
      >
        <path d="M5 6.5 8 3.5l3 3" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M5 9.5 8 12.5l3-3" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 16 16"
      className="ml-1 inline-block h-3 w-3 text-slate-600 dark:text-slate-300"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
    >
      {dir === "asc" ? (
        <path d="M4 10 8 6l4 4" strokeLinecap="round" strokeLinejoin="round" />
      ) : (
        <path d="M4 6l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
      )}
    </svg>
  );
}

function InstrumentsTable({
  items,
  sort,
  onToggleSort,
  pageScopedSort,
}: {
  items: InstrumentListItem[];
  sort: { key: SortKey; dir: SortDir };
  onToggleSort: (key: SortKey) => void;
  pageScopedSort: boolean;
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
                title={pageScopedSort ? "Sorts within this page only" : undefined}
              >
                {col.label}
                <SortIndicator
                  active={sort.key === col.key}
                  dir={sort.dir}
                />
              </th>
            ))}
          </tr>
          {pageScopedSort &&
            !(sort.key === SERVER_SORT.key && sort.dir === SERVER_SORT.dir) && (
              <tr>
                <td colSpan={COLUMNS.length} className="pb-1 text-[10px] normal-case tracking-normal text-slate-400">
                  Sorted within this page only. Server order is by coverage
                  tier, then symbol.
                </td>
              </tr>
            )}
        </thead>
        <tbody className="divide-y divide-slate-100">
          {items.map((item) =>
            isUncovered(item) ? (
              <tr
                key={item.instrument_id}
                className="align-top text-slate-400 hover:bg-slate-50 dark:text-slate-500 dark:hover:bg-slate-800/40"
              >
                <td className="py-2 pr-4">
                  <Link
                    to={`/instrument/${encodeURIComponent(item.symbol)}`}
                    className="hover:underline"
                  >
                    <span className="font-medium">{item.symbol}</span>
                  </Link>
                  <div className="text-xs">{item.company_name}</div>
                </td>
                <td
                  colSpan={COLUMNS.length - 1}
                  className="py-2 pr-0 text-xs italic"
                >
                  No coverage yet
                </td>
              </tr>
            ) : (
              <tr
                key={item.instrument_id}
                className="align-top hover:bg-slate-50 dark:hover:bg-slate-800/40"
              >
                <td className="py-2 pr-4">
                  <Link
                    to={`/instrument/${encodeURIComponent(item.symbol)}`}
                    className="text-blue-600 hover:underline"
                  >
                    <span className="font-medium">{item.symbol}</span>
                  </Link>
                  <div className="text-xs text-slate-500">{item.company_name}</div>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {item.gics_sector ?? "—"}
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {item.exchange_name ?? item.exchange ?? "—"}
                </td>
                <td className="py-2 pr-4">
                  <TierBadge tier={item.coverage_tier} />
                </td>
                <td className="py-2 pr-0 text-right text-xs tabular-nums text-slate-600">
                  {isUsablePrice(item.latest_quote?.last)
                    ? formatMoney(item.latest_quote.last, item.currency ?? "USD")
                    : "—"}
                </td>
              </tr>
            ),
          )}
        </tbody>
      </table>
    </div>
  );
}
