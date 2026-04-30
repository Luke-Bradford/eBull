/**
 * InsiderTransactionsTable — sortable, filterable Form 4 transaction
 * list with CSV export (#588).
 *
 * Columns: date · officer · role · code · A/D · shares · price ·
 * value · post-txn shares · ownership · derivative.
 *
 * Sort: any column. Date is the default. Click a header to toggle
 * direction; click again to reverse.
 *
 * Filter: case-insensitive substring match against officer name,
 * role, and security_title — covers the common "show me only the
 * CFO" / "show me option exercises" lookup paths without a
 * column-specific filter UI.
 *
 * CSV export: dumps the filtered, sorted view (the operator sees
 * the same rows they'll get in the file). Newline / quote escaping
 * follows RFC 4180.
 */

import { useMemo, useState } from "react";

import type { InsiderTransactionDetail } from "@/api/instruments";
import { directionOf, notionalValue } from "@/lib/insiderClassify";

type SortKey =
  | "txn_date"
  | "filer_name"
  | "filer_role"
  | "txn_code"
  | "direction"
  | "shares"
  | "price"
  | "value"
  | "post";
type SortDir = "asc" | "desc";

interface ColumnDef {
  readonly key: SortKey;
  readonly label: string;
  readonly align: "left" | "right";
}

const COLUMNS: ReadonlyArray<ColumnDef> = [
  { key: "txn_date", label: "Date", align: "left" },
  { key: "filer_name", label: "Officer", align: "left" },
  { key: "filer_role", label: "Role", align: "left" },
  { key: "txn_code", label: "Code", align: "left" },
  { key: "direction", label: "A/D", align: "left" },
  { key: "shares", label: "Shares", align: "right" },
  { key: "price", label: "Price", align: "right" },
  { key: "value", label: "Value", align: "right" },
  { key: "post", label: "Post-txn", align: "right" },
];

function num(v: string | null): number | null {
  if (v === null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function formatNum(n: number | null, opts?: { currency?: boolean }): string {
  if (n === null) return "—";
  if (opts?.currency === true) {
    return n.toLocaleString(undefined, {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 2,
    });
  }
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function compareString(a: string, b: string, dir: SortDir): number {
  const cmp = a.localeCompare(b);
  return dir === "asc" ? cmp : -cmp;
}

function compareNumber(
  a: number | null,
  b: number | null,
  dir: SortDir,
): number {
  if (a === null && b === null) return 0;
  if (a === null) return 1; // nulls always sort last
  if (b === null) return -1;
  const cmp = a - b;
  return dir === "asc" ? cmp : -cmp;
}

function sortRows(
  rows: ReadonlyArray<InsiderTransactionDetail>,
  key: SortKey,
  dir: SortDir,
): InsiderTransactionDetail[] {
  const copy = [...rows];
  copy.sort((a, b) => {
    switch (key) {
      case "txn_date":
        return compareString(a.txn_date, b.txn_date, dir);
      case "filer_name":
        return compareString(a.filer_name, b.filer_name, dir);
      case "filer_role":
        return compareString(a.filer_role ?? "", b.filer_role ?? "", dir);
      case "txn_code":
        return compareString(a.txn_code, b.txn_code, dir);
      case "direction": {
        const da = directionOf(a.acquired_disposed_code, a.txn_code);
        const db = directionOf(b.acquired_disposed_code, b.txn_code);
        return compareString(da, db, dir);
      }
      case "shares":
        return compareNumber(num(a.shares), num(b.shares), dir);
      case "price":
        return compareNumber(num(a.price), num(b.price), dir);
      case "value":
        return compareNumber(
          notionalValue(a.shares, a.price),
          notionalValue(b.shares, b.price),
          dir,
        );
      case "post":
        return compareNumber(
          num(a.post_transaction_shares),
          num(b.post_transaction_shares),
          dir,
        );
      default:
        return 0;
    }
  });
  return copy;
}

/** Excel/Sheets treats cells starting with `=`, `+`, `-`, `@`, or
 *  `\t` / `\r` as formulas — a Form 4 record like `=cmd|...!A1` in
 *  a `filer_name` column would execute on open. Prepending a single
 *  apostrophe keeps the cell rendered literally without changing
 *  the textual value (CSV consumers that don't run formulas are
 *  unaffected). OWASP guidance for CSV injection. */
const FORMULA_PREFIX_RE = /^[=+\-@\t\r]/;

function csvEscape(value: unknown): string {
  if (value === null || value === undefined) return "";
  let s = String(value);
  if (FORMULA_PREFIX_RE.test(s)) {
    s = `'${s}`;
  }
  if (/[",\r\n]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function buildCsv(
  rows: ReadonlyArray<InsiderTransactionDetail>,
): string {
  const header = [
    "txn_date",
    "filer_name",
    "filer_role",
    "txn_code",
    "direction",
    "is_derivative",
    "shares",
    "price",
    "value",
    "post_transaction_shares",
    "direct_indirect",
    "security_title",
    "accession_number",
  ].join(",");
  const body = rows.map((r) =>
    [
      r.txn_date,
      r.filer_name,
      r.filer_role ?? "",
      r.txn_code,
      directionOf(r.acquired_disposed_code, r.txn_code),
      r.is_derivative ? "true" : "false",
      r.shares ?? "",
      r.price ?? "",
      notionalValue(r.shares, r.price) || "",
      r.post_transaction_shares ?? "",
      r.direct_indirect ?? "",
      r.security_title ?? "",
      r.accession_number,
    ]
      .map(csvEscape)
      .join(","),
  );
  return [header, ...body].join("\r\n");
}

function downloadCsv(filename: string, csv: string): void {
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export interface InsiderTransactionsTableProps {
  readonly symbol: string;
  readonly transactions: ReadonlyArray<InsiderTransactionDetail>;
}

export function InsiderTransactionsTable({
  symbol,
  transactions,
}: InsiderTransactionsTableProps): JSX.Element {
  const [sortKey, setSortKey] = useState<SortKey>("txn_date");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [filter, setFilter] = useState<string>("");

  const filtered = useMemo<InsiderTransactionDetail[]>(() => {
    const needle = filter.trim().toLowerCase();
    if (needle === "") return [...transactions];
    return transactions.filter((r) => {
      const hay = [
        r.filer_name,
        r.filer_role ?? "",
        r.security_title ?? "",
        r.txn_code,
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(needle);
    });
  }, [transactions, filter]);

  const sorted = useMemo(
    () => sortRows(filtered, sortKey, sortDir),
    [filtered, sortKey, sortDir],
  );

  function toggleSort(key: SortKey): void {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      // Numeric columns default to desc (largest first); string
      // columns to asc (alphabetical).
      const numericKeys: ReadonlyArray<SortKey> = [
        "shares",
        "price",
        "value",
        "post",
      ];
      setSortDir(numericKeys.includes(key) ? "desc" : "asc");
    }
  }

  const handleExport = (): void => {
    const csv = buildCsv(sorted);
    downloadCsv(`${symbol.toLowerCase()}-insider-form4.csv`, csv);
  };

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <input
          type="search"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter by officer / role / security title"
          className="w-72 rounded border border-slate-200 px-2 py-1 text-xs focus:border-sky-500 focus:outline-none"
          data-testid="insider-table-filter"
        />
        <div className="flex items-center gap-2 text-[11px] text-slate-500">
          <span>
            Showing {sorted.length} of {transactions.length}
          </span>
          <button
            type="button"
            onClick={handleExport}
            disabled={sorted.length === 0}
            className="rounded bg-slate-800 px-2 py-1 text-xs font-medium text-white hover:bg-slate-700 disabled:bg-slate-300"
            data-testid="insider-table-export-csv"
          >
            Export CSV
          </button>
        </div>
      </div>
      <div className="max-h-[60vh] overflow-auto rounded border border-slate-200">
        <table className="min-w-full text-xs">
          <thead className="sticky top-0 bg-slate-50 text-slate-500">
            <tr>
              {COLUMNS.map((col) => (
                <th
                  key={col.key}
                  className={`cursor-pointer px-2 py-1 font-medium uppercase tracking-wider ${col.align === "right" ? "text-right" : "text-left"} hover:bg-slate-100`}
                  onClick={() => toggleSort(col.key)}
                  data-testid={`insider-table-sort-${col.key}`}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {sortKey === col.key ? (
                      <span aria-hidden="true">{sortDir === "asc" ? "↑" : "↓"}</span>
                    ) : null}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => {
              const dir = directionOf(r.acquired_disposed_code, r.txn_code);
              const value = notionalValue(r.shares, r.price);
              const dirClass =
                dir === "acquired"
                  ? "text-emerald-700"
                  : dir === "disposed"
                    ? "text-red-700"
                    : "text-slate-500";
              return (
                <tr
                  key={`${r.accession_number}-${r.txn_row_num}`}
                  className="border-t border-slate-100 hover:bg-slate-50"
                >
                  <td className="px-2 py-1 font-mono tabular-nums text-slate-700">
                    {r.txn_date}
                  </td>
                  <td className="px-2 py-1 text-slate-800 dark:text-slate-100">{r.filer_name}</td>
                  <td className="px-2 py-1 text-slate-600">
                    {r.filer_role ?? "—"}
                  </td>
                  <td className="px-2 py-1 font-mono text-slate-700">
                    {r.txn_code}
                    {r.is_derivative ? (
                      <span
                        className="ml-1 rounded bg-slate-100 px-1 text-[10px] text-slate-600"
                        title="Derivative transaction"
                      >
                        D
                      </span>
                    ) : null}
                  </td>
                  <td className={`px-2 py-1 font-medium ${dirClass}`}>
                    {dir === "acquired"
                      ? "A"
                      : dir === "disposed"
                        ? "D"
                        : "—"}
                  </td>
                  <td className="px-2 py-1 text-right font-mono tabular-nums text-slate-700">
                    {formatNum(num(r.shares))}
                  </td>
                  <td className="px-2 py-1 text-right font-mono tabular-nums text-slate-700">
                    {formatNum(num(r.price), { currency: true })}
                  </td>
                  <td className="px-2 py-1 text-right font-mono tabular-nums text-slate-700">
                    {value > 0
                      ? formatNum(value, { currency: true })
                      : "—"}
                  </td>
                  <td className="px-2 py-1 text-right font-mono tabular-nums text-slate-700">
                    {formatNum(num(r.post_transaction_shares))}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
