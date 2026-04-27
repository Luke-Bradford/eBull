/**
 * DensityGrid — Bloomberg-style 3-column grid for the instrument
 * Research tab (#559). Chart pane occupies the wide left column
 * (2fr) spanning 2 rows; right column (1fr + 1fr) stacks
 * key-stats / thesis / SEC profile / filings; bottom rows hold
 * business teaser / news; dividends + insider as a wide combined
 * card.
 *
 * Responsive: at viewport widths below `lg` the grid degrades to
 * a single column. Pane order reflects priority: chart → key-stats
 * → thesis → filings → SEC-profile → segments → dividends-insider
 * → news. Each pane scrolls internally rather than pushing the
 * page taller.
 *
 * 8-K events are accessible via the FilingsPane row click →
 * /instrument/:symbol/filings/8-k (Phase 4).
 */

import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import { DividendsPanel } from "@/components/instrument/DividendsPanel";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
import { InsiderActivitySummary } from "@/components/instrument/InsiderActivitySummary";
import { PriceChart } from "@/components/instrument/PriceChart";
import { SecProfilePanel } from "@/components/instrument/SecProfilePanel";
import { Section } from "@/components/dashboard/Section";
import type { CapabilityCell, InstrumentSummary } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export interface DensityGridProps {
  readonly summary: InstrumentSummary;
  readonly keyStatsBlock: JSX.Element;
  readonly thesisBlock: JSX.Element;
  readonly newsBlock: JSX.Element;
}

const EMPTY_CELL: CapabilityCell = { providers: [], data_present: {} };

export function DensityGrid({
  summary,
  keyStatsBlock,
  thesisBlock,
  newsBlock,
}: DensityGridProps): JSX.Element {
  const symbol = summary.identity.symbol;
  const hasSec = summary.has_sec_cik;
  const dividends = summary.capabilities.dividends ?? EMPTY_CELL;
  const insider = summary.capabilities.insider ?? EMPTY_CELL;
  const dividendProviders = activeProviders(dividends);
  const insiderProviders = activeProviders(insider);

  return (
    <div className="grid grid-cols-1 gap-2 lg:grid-cols-[2fr_1fr_1fr]">
        {/* Chart pane: wide column (2fr) × 2 rows top-left */}
        <div className="overflow-hidden rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm min-h-[440px] lg:col-start-1 lg:col-end-2 lg:row-start-1 lg:row-end-3">
          <PriceChart symbol={symbol} />
        </div>

        {/* Right column row 1 */}
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
          {keyStatsBlock}
        </div>
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
          {thesisBlock}
        </div>

        {/* Right column row 2 */}
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
          {hasSec ? (
            <SecProfilePanel symbol={symbol} />
          ) : (
            <Section title="SEC profile">
              <p className="text-xs text-slate-500">No SEC coverage</p>
            </Section>
          )}
        </div>
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
          <FilingsPane instrumentId={summary.instrument_id} symbol={symbol} summary={summary} />
        </div>

        {/* Fundamentals pane: full-width row after sec profile + filings,
            gated on sec_xbrl capability. Placed after the row-2 right-column
            panes so the chart/keyStats/thesis/secProfile/filings layout is
            preserved regardless of whether fundamentals are active. */}
        {summary.capabilities["fundamentals"]?.providers.includes("sec_xbrl") &&
         summary.capabilities["fundamentals"].data_present["sec_xbrl"] === true ? (
          <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm lg:col-span-3">
            <FundamentalsPane summary={summary} />
          </div>
        ) : null}

        {/* Bottom row: segments spans 2 cols, news spans 1 col */}
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm lg:col-span-2">
          {hasSec ? (
            <BusinessSectionsTeaser symbol={symbol} />
          ) : (
            <Section title="Company narrative">
              <p className="text-xs text-slate-500">No 10-K coverage</p>
            </Section>
          )}
        </div>
        <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm">
          {newsBlock}
        </div>

        {/* Dividends + insider combined card — spans full width */}
        {(dividendProviders.length > 0 || insiderProviders.length > 0) && (
          <div className="rounded-md border border-slate-200 bg-white px-3 py-2.5 shadow-sm lg:col-span-3">
            <div className="grid gap-3 md:grid-cols-2">
              {dividendProviders.map((p) => (
                <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
              ))}
              {insiderProviders.length > 0 && (
                <InsiderActivitySummary symbol={symbol} />
              )}
            </div>
          </div>
        )}
    </div>
  );
}
