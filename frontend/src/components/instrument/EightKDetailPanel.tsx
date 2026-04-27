/**
 * EightKDetailPanel — right-side detail for the row currently
 * selected in the 8-K filterable list. Shows item bodies +
 * exhibits + primary_document_url link (#559).
 */

import type { EightKFiling } from "@/api/instruments";

export interface EightKDetailPanelProps {
  readonly filing: EightKFiling | null;
}

const SEVERITY_TONE: Record<string, string> = {
  high: "bg-red-100 text-red-700",
  medium: "bg-amber-100 text-amber-700",
  low: "bg-slate-100 text-slate-600",
};

export function EightKDetailPanel({
  filing,
}: EightKDetailPanelProps): JSX.Element {
  if (filing === null) {
    return (
      <div className="rounded border border-slate-200 bg-white p-4 text-sm text-slate-500">
        Select a row to view item bodies + exhibits.
      </div>
    );
  }
  return (
    <div className="space-y-4 rounded border border-slate-200 bg-white p-4 text-sm">
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
      {filing.items.map((item) => (
        <section key={item.item_code}>
          <header className="flex items-baseline gap-2">
            <span
              className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${
                SEVERITY_TONE[item.severity ?? ""] ?? SEVERITY_TONE.low
              }`}
            >
              Item {item.item_code}
            </span>
            <span className="text-xs font-medium text-slate-800">
              {item.item_label}
            </span>
          </header>
          <p className="mt-1 whitespace-pre-wrap leading-relaxed text-slate-700">
            {item.body}
          </p>
        </section>
      ))}
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
