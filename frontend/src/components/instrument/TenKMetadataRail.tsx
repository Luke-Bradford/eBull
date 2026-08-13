/**
 * TenKMetadataRail — right rail on the 10-K drilldown page. Shows the
 * current filing accession + a list of prior 10-Ks for cross-year
 * thesis comparison + the cross-ref items list (#559).
 */

import type { TenKHistoryFiling } from "@/api/instruments";
import { Link } from "react-router-dom";

export interface TenKMetadataRailProps {
  readonly symbol: string;
  readonly currentAccession: string | null;
  readonly history: ReadonlyArray<TenKHistoryFiling>;
  readonly relatedItems: ReadonlyArray<string>; // e.g. ["Item 1A", "Item 7"]
}

export function TenKMetadataRail({
  symbol,
  currentAccession,
  history,
  relatedItems,
}: TenKMetadataRailProps): JSX.Element {
  return (
    <aside className="space-y-4 text-xs">
      <section>
        <h3 className="mb-1 text-[10px] uppercase tracking-wider text-slate-500">
          Filing
        </h3>
        {currentAccession !== null ? (
          <p className="font-mono text-[11px] text-slate-700 break-all">
            {currentAccession}
          </p>
        ) : (
          <p className="text-slate-500">—</p>
        )}
      </section>

      {history.length > 0 && (
        <section>
          <h3 className="mb-1 text-[10px] uppercase tracking-wider text-slate-500">
            Prior 10-Ks
          </h3>
          <ul className="space-y-0.5">
            {history.map((f) => {
              const isCurrent = f.accession_number === currentAccession;
              return (
                <li key={f.accession_number}>
                  <Link
                    to={`/instrument/${encodeURIComponent(symbol)}/filings/10-k?accession=${encodeURIComponent(f.accession_number)}`}
                    className={`block hover:underline ${
                      isCurrent
                        ? "font-medium text-slate-900 dark:text-slate-100"
                        : "text-sky-700 dark:text-sky-300"
                    }`}
                  >
                    {f.filing_date.slice(0, 4)}
                    {f.filing_type === "10-K/A" ? " (amended)" : ""}
                    {isCurrent ? " · current" : ""}
                  </Link>
                </li>
              );
            })}
          </ul>
        </section>
      )}

      {relatedItems.length > 0 && (
        <section>
          <h3 className="mb-1 text-[10px] uppercase tracking-wider text-slate-500">
            Related items
          </h3>
          <ul className="space-y-0.5">
            {relatedItems.map((item) => (
              <li key={item} className="text-slate-700">
                {item}
              </li>
            ))}
          </ul>
        </section>
      )}
    </aside>
  );
}
