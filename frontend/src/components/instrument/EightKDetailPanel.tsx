/**
 * EightKDetailPanel — right-side detail for the row currently
 * selected in the 8-K filterable list. Shows item bodies +
 * exhibits + primary_document_url link (#559).
 *
 * #1343 — a bootstrap-deferred filing arrives with item codes + dates
 * but empty bodies; the parent (EightKListPage) lazily fetches the body
 * on select and drives the three states here: `bodyLoading` while the
 * fetch is in flight, `bodyError` on a transient (503) failure, and the
 * filled filing on success. The filing header (accession / date) renders
 * in all three so the operator keeps context during the ~0.5-1s fetch.
 */

import type { EightKFiling } from "@/api/instruments";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { SEVERITY_TONE } from "@/components/instrument/eightKSeverity";

export interface EightKDetailPanelProps {
  readonly filing: EightKFiling | null;
  /** #1343 — deferred-body fetch is in flight for this filing. */
  readonly bodyLoading?: boolean;
  /** #1343 — deferred-body fetch failed transiently (503); show retry. */
  readonly bodyError?: boolean;
  /** #1343 — re-attempt the deferred-body fetch. */
  readonly onRetryBody?: () => void;
}

export function EightKDetailPanel({
  filing,
  bodyLoading = false,
  bodyError = false,
  onRetryBody,
}: EightKDetailPanelProps): JSX.Element {
  if (filing === null) {
    return (
      <div className="rounded border border-slate-200 bg-white p-4 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-400">
        Select a row to view item bodies + exhibits.
      </div>
    );
  }
  return (
    <div className="space-y-4 rounded border border-slate-200 bg-white p-4 text-sm dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100">
      <div>
        <div className="text-[10px] uppercase tracking-wider text-slate-500">
          Filing
        </div>
        <div className="font-mono text-xs">{filing.accession_number}</div>
        <div className="text-xs text-slate-500">
          {filing.date_of_report} · {filing.reporting_party}
          {filing.is_amendment ? " · amendment" : ""}
        </div>
      </div>
      {bodyLoading ? (
        <SectionSkeleton rows={4} />
      ) : bodyError ? (
        <SectionError onRetry={onRetryBody ?? (() => {})} />
      ) : filing.items.length === 0 ? (
        <div className="text-xs italic text-slate-500">
          No item bodies were parsed for this filing.
        </div>
      ) : (
        filing.items.map((item) => (
        <section key={item.item_code}>
          <header className="flex items-baseline gap-2">
            <span
              className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${
                SEVERITY_TONE[item.severity ?? ""] ?? SEVERITY_TONE.low
              }`}
            >
              Item {item.item_code}
            </span>
            <span className="text-xs font-medium text-slate-800 dark:text-slate-100">
              {item.item_label}
            </span>
          </header>
          <p className="mt-1 whitespace-pre-wrap leading-relaxed text-slate-700">
            {item.body}
          </p>
        </section>
        ))
      )}
      {filing.exhibits.length > 0 && (
        <section>
          <div className="text-[10px] uppercase tracking-wider text-slate-500">
            Exhibits
          </div>
          <ul className="mt-1 space-y-0.5 text-xs">
            {filing.exhibits.map((e) => (
              <li key={e.exhibit_number}>
                · {e.exhibit_number}
                {e.description !== null ? ` — ${e.description}` : ""}
              </li>
            ))}
          </ul>
        </section>
      )}
      {filing.primary_document_url !== null && (
        <a
          href={filing.primary_document_url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-sky-700 hover:underline"
        >
          Open full filing on SEC ↗
        </a>
      )}
    </div>
  );
}
