/**
 * EightKEventsPanel — provider-agnostic shell for the per-instrument
 * corporate-events capability (#515 PR 3b). Backed by GET
 * /instruments/{symbol}/eight_k_filings?provider=<provider>.
 *
 * Today only ``sec_8k_events`` is wired. Per-region integration PRs
 * adapting (e.g.) HKEX corporate announcements to the same normalised
 * filing-card shape reuse this shell unchanged.
 *
 *
 * Each filing renders as a timeline card with:
 *   - Date of report + document type (8-K vs 8-K/A)
 *   - Item chips (code + human label) coloured by severity
 *   - Per-item body excerpt (expand to see full text)
 *   - Exhibits list
 *
 * Complements the dividend-specific panel — this surfaces every
 * material 8-K (officer departures, acquisitions, cybersecurity
 * incidents, etc.), not just dividends.
 */

import { fetchEightKFilings } from "@/api/instruments";
import type {
  EightKFiling,
  EightKFilingsResponse,
  EightKItem,
} from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { providerLabel } from "@/lib/capabilityProviders";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useState } from "react";

export interface EightKEventsPanelProps {
  readonly symbol: string;
  /** Capability provider tag, resolved via
   *  ``summary.capabilities.corporate_events.providers`` upstream. */
  readonly provider: string;
}

function severityTone(severity: string | null): string {
  switch (severity) {
    case "critical":
      return "bg-rose-100 text-rose-800";
    case "material":
      return "bg-amber-100 text-amber-800";
    case "informational":
      return "bg-slate-100 dark:bg-slate-800 text-slate-600";
    default:
      return "bg-slate-100 dark:bg-slate-800 text-slate-600";
  }
}

function ItemBlock({ item }: { item: EightKItem }) {
  const [expanded, setExpanded] = useState(false);
  const hasBody = item.body.length > 0;
  const showToggle = item.body.length > 200;
  return (
    <div className="border-l-2 border-slate-200 dark:border-slate-800 pl-3">
      <div className="mb-1 flex items-center gap-2">
        <span
          className={`rounded px-1.5 py-0.5 font-mono text-[11px] font-semibold ${severityTone(item.severity)}`}
          title={item.severity ?? "severity unclassified"}
        >
          Item {item.item_code}
        </span>
        <span className="text-xs font-medium text-slate-700">
          {item.item_label}
        </span>
        {showToggle && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="ml-auto text-[11px] font-medium text-sky-700 hover:underline"
          >
            {expanded ? "Collapse" : "Expand"}
          </button>
        )}
      </div>
      {hasBody ? (
        <div
          className={`whitespace-pre-wrap text-xs text-slate-700 ${
            expanded ? "" : "line-clamp-2"
          }`}
        >
          {item.body}
        </div>
      ) : (
        <div className="text-xs italic text-slate-500">
          Item declared; body not parsed from this filing.
        </div>
      )}
    </div>
  );
}

function FilingCard({ filing }: { filing: EightKFiling }) {
  const dateText = filing.date_of_report ?? "—";
  return (
    <div className="rounded-sm border border-slate-200 dark:border-slate-800 p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <span className="font-mono text-xs text-slate-800 dark:text-slate-100">
          {dateText}
        </span>
        <span
          className={`rounded px-1.5 py-0.5 text-[11px] font-semibold ${
            filing.is_amendment
              ? "bg-amber-50 text-amber-800"
              : "bg-sky-50 text-sky-800"
          }`}
        >
          {filing.document_type}
        </span>
        {filing.reporting_party !== null && (
          <span className="text-xs text-slate-500">
            {filing.reporting_party}
          </span>
        )}
        <span className="ml-auto font-mono text-[10px] text-slate-400">
          {filing.accession_number}
        </span>
      </div>
      <div className="space-y-2">
        {filing.items.map((it) => (
          <ItemBlock key={`${filing.accession_number}-${it.item_code}`} item={it} />
        ))}
      </div>
      {filing.exhibits.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1 border-t border-slate-100 pt-2 text-[11px]">
          <span className="text-slate-500">Exhibits:</span>
          {filing.exhibits.map((ex) => (
            <span
              key={`${filing.accession_number}-${ex.exhibit_number}`}
              className="rounded bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 text-slate-700"
              title={ex.description ?? ""}
            >
              {ex.exhibit_number}
              {ex.description !== null && (
                <span className="text-slate-500"> · {ex.description.slice(0, 60)}</span>
              )}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function Body({ data }: { data: EightKFilingsResponse }) {
  if (data.filings.length === 0) {
    return (
      <EmptyState
        title="No 8-K events on file"
        description="No 8-K filings have been parsed for this instrument yet. Either no 8-K is on file, or the hourly ingester has not yet picked up the latest filings."
      />
    );
  }
  return (
    <div className="space-y-2">
      {data.filings.map((f) => (
        <FilingCard key={f.accession_number} filing={f} />
      ))}
    </div>
  );
}

export function EightKEventsPanel({ symbol, provider }: EightKEventsPanelProps) {
  const state = useAsync<EightKFilingsResponse>(
    useCallback(
      () => fetchEightKFilings(symbol, 25, provider),
      [symbol, provider],
    ),
    [symbol, provider],
  );
  const title = `Corporate events · ${providerLabel(provider)}`;
  return (
    <Section title={title}>
      {state.loading ? (
        <SectionSkeleton rows={4} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null ? (
        <EmptyState
          title="8-K events unavailable"
          description="Could not load 8-K filings for this instrument."
        />
      ) : (
        <Body data={state.data} />
      )}
    </Section>
  );
}
