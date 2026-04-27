/**
 * DensityGrid — Bloomberg-style 3-column grid for the instrument
 * Research tab (#559). Chart pane occupies the wide left column
 * (2fr) spanning 2 rows; right column (1fr + 1fr) stacks
 * key-stats / thesis / SEC profile / filings; bottom rows hold
 * business teaser / news; dividends + insider as a wide combined
 * card. Corporate events (8-K) follow below until Phase 4 ships
 * the dedicated /filings/8-k route.
 *
 * Responsive: at viewport widths below `lg` the grid degrades to
 * a single column. Pane order reflects priority: chart → key-stats
 * → thesis → filings → SEC-profile → segments → dividends-insider
 * → news → events. Each pane scrolls internally rather than pushing
 * the page taller.
 */

import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import { DividendsPanel } from "@/components/instrument/DividendsPanel";
import { EightKEventsPanel } from "@/components/instrument/EightKEventsPanel";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import { InsiderActivityPanel } from "@/components/instrument/InsiderActivityPanel";
import { PriceChart } from "@/components/instrument/PriceChart";
import { SecProfilePanel } from "@/components/instrument/SecProfilePanel";
import { Section } from "@/components/dashboard/Section";
import type { CapabilityCell, InstrumentSummary, ThesisDetail } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export interface DensityGridProps {
  readonly summary: InstrumentSummary;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored: boolean;
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
  const corporateEvents = summary.capabilities.corporate_events ?? EMPTY_CELL;
  const dividendProviders = activeProviders(dividends);
  const insiderProviders = activeProviders(insider);
  const eventProviders = activeProviders(corporateEvents);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-[2fr_1fr_1fr] lg:auto-rows-[220px]">
        {/* Chart pane: wide column (2fr) × 2 rows top-left */}
        <div className="overflow-hidden rounded-md border border-slate-200 bg-white p-3 shadow-sm lg:col-start-1 lg:col-end-2 lg:row-start-1 lg:row-end-3">
          <PriceChart symbol={symbol} />
        </div>

        {/* Right column row 1 */}
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
          {keyStatsBlock}
        </div>
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
          {thesisBlock}
        </div>

        {/* Right column row 2 */}
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
          {hasSec ? (
            <SecProfilePanel symbol={symbol} />
          ) : (
            <Section title="SEC profile">
              <p className="text-xs text-slate-500">No SEC coverage</p>
            </Section>
          )}
        </div>
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
          <FilingsPane instrumentId={summary.instrument_id} symbol={symbol} />
        </div>

        {/* Bottom row: segments spans 2 cols, news spans 1 col */}
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm lg:col-span-2">
          {hasSec ? (
            <BusinessSectionsTeaser symbol={symbol} />
          ) : (
            <Section title="Company narrative">
              <p className="text-xs text-slate-500">No 10-K coverage</p>
            </Section>
          )}
        </div>
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
          {newsBlock}
        </div>

        {/* Dividends + insider combined card — spans full width */}
        {(dividendProviders.length > 0 || insiderProviders.length > 0) && (
          <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm lg:col-span-3">
            <div className="grid gap-3 md:grid-cols-2">
              {dividendProviders.map((p) => (
                <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
              ))}
              {insiderProviders.map((p) => (
                <InsiderActivityPanel
                  key={`ins-${p}`}
                  symbol={symbol}
                  provider={p}
                />
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Corporate events (8-K) — transitional until Phase 4 ships /filings/8-k */}
      {eventProviders.length > 0 && (
        <div className="space-y-3">
          {eventProviders.map((p) => (
            <EightKEventsPanel
              key={`events-${p}`}
              symbol={symbol}
              provider={p}
            />
          ))}
        </div>
      )}
    </div>
  );
}
