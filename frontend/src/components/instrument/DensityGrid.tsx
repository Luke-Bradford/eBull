/**
 * DensityGrid — 12-column capability-aware grid for the instrument
 * Research tab (#575). Three profiles determine which panes render:
 *
 *   full-sec        — fundamentals (sec_xbrl) + filings active
 *   partial-filings — filings active but no sec_xbrl fundamentals
 *   minimal         — no filings capability at all
 *
 * PriceChart and KeyStatsPane are present in all profiles.
 * SecProfilePanel / BusinessSectionsTeaser gate on has_sec_cik.
 * FundamentalsPane is exclusive to the full-sec profile.
 * FilingsPane / InsiderActivitySummary / DividendsPanel / RecentNewsPane
 * / ThesisPane appear in profile-specific positions.
 *
 * No overflow-auto scroll-boxes: every pane expands to content height
 * so the grid stays a true content-driven layout.
 */

import type { InstrumentSummary, ThesisDetail } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";
import { useNavigate, useSearchParams } from "react-router-dom";
import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import { DividendsPanel } from "@/components/instrument/DividendsPanel";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
import { InsiderActivitySummary } from "@/components/instrument/InsiderActivitySummary";
import { KeyStatsPane } from "@/components/instrument/KeyStatsPane";
import { Pane } from "@/components/instrument/Pane";
import { PriceChart } from "@/components/instrument/PriceChart";
import { RecentNewsPane } from "@/components/instrument/RecentNewsPane";
import { SecProfilePanel } from "@/components/instrument/SecProfilePanel";
import { ThesisPane } from "@/components/instrument/ThesisPane";
import {
  EMPTY_CELL,
  hasFundamentalsActive,
  selectProfile,
} from "@/components/instrument/densityProfile";

export interface DensityGridProps {
  readonly summary: InstrumentSummary;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored: boolean;
}

export function DensityGrid({
  summary,
  thesis,
  thesisErrored,
}: DensityGridProps): JSX.Element {
  const symbol = summary.identity.symbol;
  const instrumentId = summary.instrument_id;
  const profile = selectProfile(summary);
  const cap = summary.capabilities;
  const insiderActive = activeProviders(cap.insider ?? EMPTY_CELL).length > 0;
  const dividendProviders = activeProviders(cap.dividends ?? EMPTY_CELL);
  const hasNarrative = summary.has_sec_cik;
  const navigate = useNavigate();
  const [overviewParams] = useSearchParams();

  const drillToWorkspace = () => {
    // Preserve the operator's currently-selected overview range when
    // expanding to the full chart workspace. PriceChart syncs its
    // range to ?chart=<id> on the instrument page; ChartPage reads
    // ?range=<id>. Translate the param name across the boundary so
    // a non-default range survives the route change.
    const overviewRange = overviewParams.get("chart");
    const target = `/instrument/${encodeURIComponent(symbol)}/chart`;
    const url =
      overviewRange !== null && overviewRange !== ""
        ? `${target}?range=${encodeURIComponent(overviewRange)}`
        : target;
    navigate(url);
  };

  // Card-click drill removed (#601 follow-up): the PaneHeader's
  // "Open →" button is the only drill affordance now. Operator
  // reported the whole-card click was firing accidentally on chart
  // hover/zoom.
  const ChartPane = (
    <Pane title="Price chart" onExpand={drillToWorkspace} fillHeight>
      <PriceChart symbol={symbol} instrumentId={instrumentId} />
    </Pane>
  );

  if (profile === "full-sec") {
    return (
      <div className="grid grid-cols-12 gap-2">
        <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
        <div className="col-span-12 lg:col-span-4">
          <KeyStatsPane summary={summary} />
        </div>
        {hasNarrative && (
          <div className="col-span-12 lg:col-span-4">
            <SecProfilePanel symbol={symbol} />
          </div>
        )}
        <div className="col-span-12">
          {/* full-sec profile guarantees sec_xbrl fundamentals + filings are active per selectProfile */}
          <FundamentalsPane summary={summary} />
        </div>
        <div className="col-span-12 lg:col-span-7">
          <FilingsPane instrumentId={instrumentId} symbol={symbol} summary={summary} />
        </div>
        {insiderActive && (
          <div className="col-span-12 lg:col-span-5">
            <InsiderActivitySummary symbol={symbol} />
          </div>
        )}
        {hasNarrative && (
          <div className="col-span-12">
            <BusinessSectionsTeaser symbol={symbol} />
          </div>
        )}
        {dividendProviders.length > 0 && (
          <div className="col-span-12">
            {dividendProviders.map((p) => (
              <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
            ))}
          </div>
        )}
        <div className="col-span-12">
          <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
        </div>
        {thesis !== null || thesisErrored ? (
          <div className="col-span-12">
            <ThesisPane thesis={thesis} errored={thesisErrored} />
          </div>
        ) : null}
      </div>
    );
  }

  if (profile === "partial-filings") {
    return (
      <div className="grid grid-cols-12 gap-2">
        <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
        <div className="col-span-12 lg:col-span-4">
          <KeyStatsPane summary={summary} />
        </div>
        {hasNarrative && (
          <div className="col-span-12 lg:col-span-4">
            <SecProfilePanel symbol={symbol} />
          </div>
        )}
        {hasFundamentalsActive(summary) && (
          <div className="col-span-12">
            <FundamentalsPane summary={summary} />
          </div>
        )}
        {activeProviders(cap.filings ?? EMPTY_CELL).length > 0 && (
          <div className="col-span-12">
            <FilingsPane instrumentId={instrumentId} symbol={symbol} summary={summary} />
          </div>
        )}
        {insiderActive && dividendProviders.length > 0 ? (
          <>
            <div className="col-span-12 lg:col-span-7">
              <InsiderActivitySummary symbol={symbol} />
            </div>
            <div className="col-span-12 lg:col-span-5">
              {dividendProviders.map((p) => (
                <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
              ))}
            </div>
          </>
        ) : insiderActive ? (
          <div className="col-span-12">
            <InsiderActivitySummary symbol={symbol} />
          </div>
        ) : dividendProviders.length > 0 ? (
          <div className="col-span-12">
            {dividendProviders.map((p) => (
              <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
            ))}
          </div>
        ) : null}
        {hasNarrative && (
          <div className="col-span-12">
            <BusinessSectionsTeaser symbol={symbol} />
          </div>
        )}
        <div className="col-span-12">
          <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
        </div>
        {thesis !== null || thesisErrored ? (
          <div className="col-span-12">
            <ThesisPane thesis={thesis} errored={thesisErrored} />
          </div>
        ) : null}
      </div>
    );
  }

  // minimal
  return (
    <div className="grid grid-cols-12 gap-2">
      <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
      <div className="col-span-12 lg:col-span-4">
        <KeyStatsPane summary={summary} />
      </div>
      {(thesis !== null || thesisErrored) && (
        <div className="col-span-12 lg:col-span-4">
          <ThesisPane thesis={thesis} errored={thesisErrored} />
        </div>
      )}
      {dividendProviders.length > 0 && (
        <div className="col-span-12">
          {dividendProviders.map((p) => (
            <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
          ))}
        </div>
      )}
      <div className="col-span-12">
        <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
      </div>
    </div>
  );
}
