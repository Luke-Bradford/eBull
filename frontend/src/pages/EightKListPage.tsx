/**
 * /instrument/:symbol/filings/8-k — filterable 8-K list with detail
 * panel (#559).
 *
 * Filter state lives in URL query string (severity / itemCode /
 * dateFrom / dateTo / accession=) so deep-links work.
 */

import { fetchEightKFilings } from "@/api/instruments";
import type { EightKFiling, EightKFilingsResponse } from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EightKDetailPanel } from "@/components/instrument/EightKDetailPanel";
import {
  EightKFilterStrip,
  type EightKFilters,
} from "@/components/instrument/EightKFilterStrip";
import { SEVERITY_TONE } from "@/components/instrument/eightKSeverity";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useEffect, useMemo } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

const HARD_LIMIT = 250;

function readFilters(p: URLSearchParams): EightKFilters {
  const severity = p.get("severity");
  return {
    severity:
      severity === "high" || severity === "medium" || severity === "low"
        ? severity
        : "",
    itemCode: p.get("itemCode") ?? "",
    dateFrom: p.get("dateFrom") ?? "",
    dateTo: p.get("dateTo") ?? "",
  };
}

function writeFilters(p: URLSearchParams, f: EightKFilters): URLSearchParams {
  const out = new URLSearchParams(p);
  if (f.severity === "") out.delete("severity");
  else out.set("severity", f.severity);
  if (f.itemCode === "") out.delete("itemCode");
  else out.set("itemCode", f.itemCode);
  if (f.dateFrom === "") out.delete("dateFrom");
  else out.set("dateFrom", f.dateFrom);
  if (f.dateTo === "") out.delete("dateTo");
  else out.set("dateTo", f.dateTo);
  return out;
}

function highestSeverity(filing: EightKFiling): string {
  for (const item of filing.items) {
    if (item.severity === "high") return "high";
  }
  for (const item of filing.items) {
    if (item.severity === "medium") return "medium";
  }
  return "low";
}

function applyFilters(
  filings: ReadonlyArray<EightKFiling>,
  f: EightKFilters,
): EightKFiling[] {
  return filings.filter((flg) => {
    if (f.severity !== "" && highestSeverity(flg) !== f.severity) return false;
    if (f.itemCode !== "") {
      if (!flg.items.some((i) => i.item_code.includes(f.itemCode))) return false;
    }
    if (
      f.dateFrom !== "" &&
      flg.date_of_report !== null &&
      flg.date_of_report < f.dateFrom
    )
      return false;
    if (
      f.dateTo !== "" &&
      flg.date_of_report !== null &&
      flg.date_of_report > f.dateTo
    )
      return false;
    return true;
  });
}

export function EightKListPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const filters = readFilters(searchParams);
  const selectedAccession = searchParams.get("accession");

  const state = useAsync<EightKFilingsResponse>(
    useCallback(() => fetchEightKFilings(symbol, HARD_LIMIT), [symbol]),
    [symbol],
  );

  const filtered = useMemo(
    () => (state.data === null ? [] : applyFilters(state.data.filings, filters)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.data, filters.severity, filters.itemCode, filters.dateFrom, filters.dateTo],
  );

  const selected =
    selectedAccession !== null
      ? (filtered.find((f) => f.accession_number === selectedAccession) ?? null)
      : null;

  function setFilters(next: EightKFilters): void {
    setSearchParams(writeFilters(searchParams, next), { replace: true });
  }

  function selectAccession(acc: string | null): void {
    const out = new URLSearchParams(searchParams);
    if (acc === null) out.delete("accession");
    else out.set("accession", acc);
    setSearchParams(out, { replace: true });
  }

  // Auto-select first filtered row on page open; clear stale accession when
  // filter excludes selected filing (fixes empty detail pane on initial load
  // and broken deep-link contract when filters exclude selection).
  useEffect(() => {
    // Clear stale accession when filters exclude the selected filing.
    if (
      selectedAccession !== null &&
      state.data !== null &&
      !filtered.some((f) => f.accession_number === selectedAccession)
    ) {
      const out = new URLSearchParams(searchParams);
      out.delete("accession");
      setSearchParams(out, { replace: true });
      return;
    }
    // Auto-select first filtered row when no accession is selected.
    if (selectedAccession === null && filtered.length > 0) {
      const out = new URLSearchParams(searchParams);
      const first = filtered[0];
      if (first) {
        out.set("accession", first.accession_number);
        setSearchParams(out, { replace: true });
      }
    }
  }, [selectedAccession, filtered, searchParams, setSearchParams, state.data]);

  return (
    <div className="mx-auto max-w-screen-2xl space-y-3 p-4">
      <Section title={`${symbol} — 8-K filings`}>
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}`}
          className="text-xs text-sky-700 hover:underline"
        >
          ← Back to {symbol}
        </Link>
        <div className="mt-3">
          <EightKFilterStrip value={filters} onChange={setFilters} />
        </div>
        {state.loading ? (
          <SectionSkeleton rows={5} />
        ) : state.error !== null ? (
          <SectionError onRetry={state.refetch} />
        ) : state.data === null || state.data.filings.length === 0 ? (
          <EmptyState
            title="No 8-K filings"
            description="No 8-K filings on file for this instrument."
          />
        ) : (
          <div className="mt-3 grid gap-4 lg:grid-cols-[3fr_2fr]">
            <div className="overflow-x-auto">
              <table className="min-w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-200 dark:border-slate-800 text-left text-slate-500">
                    <th className="px-2 py-1">Date</th>
                    <th className="px-2 py-1">Items</th>
                    <th className="px-2 py-1">Severity</th>
                    <th className="px-2 py-1">Subject</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((f) => {
                    const isSelected = f.accession_number === selectedAccession;
                    const sev = highestSeverity(f);
                    return (
                      <tr
                        key={f.accession_number}
                        className={`cursor-pointer border-b border-slate-100 hover:bg-slate-50 dark:hover:bg-slate-800/40 ${
                          isSelected ? "bg-sky-50" : ""
                        }`}
                        onClick={() => selectAccession(f.accession_number)}
                      >
                        <td className="px-2 py-1 text-slate-700">
                          {f.date_of_report}
                        </td>
                        <td className="px-2 py-1">
                          {f.items.map((i) => (
                            <span
                              key={i.item_code}
                              className="mr-1 rounded bg-slate-100 px-1 py-0.5 text-[10px]"
                            >
                              {i.item_code}
                            </span>
                          ))}
                        </td>
                        <td className="px-2 py-1">
                          <span
                            className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${
                              SEVERITY_TONE[sev] ?? SEVERITY_TONE.low
                            }`}
                          >
                            {sev}
                          </span>
                        </td>
                        <td className="px-2 py-1 text-slate-700">
                          {f.items.map((i) => i.item_label).join(" · ")}
                        </td>
                      </tr>
                    );
                  })}
                  {filtered.length === 0 && (
                    <tr>
                      <td
                        colSpan={4}
                        className="px-2 py-4 text-center text-slate-500"
                      >
                        No filings match these filters.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
              {state.data !== null && state.data.filings.length === HARD_LIMIT && (
                <p className="mt-2 text-xs text-amber-700">
                  Showing the most recent {HARD_LIMIT} filings. Older 8-Ks not
                  shown — file a follow-up to add pagination if you need them.
                </p>
              )}
            </div>
            <EightKDetailPanel filing={selected} />
          </div>
        )}
      </Section>
    </div>
  );
}
