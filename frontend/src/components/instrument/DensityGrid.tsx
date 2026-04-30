/**
 * DensityGrid — capability-aware 12-col grid for the instrument
 * Research tab (#575). Three profiles determine which panes render:
 *
 *   full-sec        — fundamentals (sec_xbrl) + filings active
 *   partial-filings — filings active but no sec_xbrl fundamentals
 *   minimal         — no filings capability at all
 *
 * Design-system v1 row order (operator review): the page is a five-zone
 * editorial spread from top to bottom, ordered by operator scan
 * priority for a held instrument:
 *
 *   Zone A — Hero      :  Price chart (visual anchor), Key stats,
 *                         SEC profile (identity rail under stats)
 *   Zone B — Identity  :  10-K narrative (BusinessSections) — what is
 *                         this company? Paired with Recent Filings at
 *                         8+4 so the right-half dead space the
 *                         narrative left behind is filled with
 *                         contextually-related filing history (the
 *                         narrative IS itself a filing). When filings
 *                         are inactive, narrative falls back to
 *                         full-width.
 *   Zone C — Health    :  Fundamentals + Dividends paired 6+6 — read
 *                         financial trajectory and shareholder return
 *                         together in one scan.
 *   Zone D — Activity  :  Insider activity. Filings have moved up to
 *                         pair with the narrative; when filings are
 *                         active but no narrative exists, Filings
 *                         drops back into this zone alongside Insider
 *                         at 6+6. Recent news full-width below since
 *                         news lists scan vertically.
 *   Zone E — Operator  :  Thesis pane — operator's own call, last so
 *                         it doesn't anchor your read of the data.
 *
 * Pane chrome itself is borderless (Pane.tsx — design-system v1):
 * the grid reads as one continuous editorial spread, not a card grid.
 *
 * No overflow-auto scroll-boxes anywhere — content drives height.
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
  const filingsActive = activeProviders(cap.filings ?? EMPTY_CELL).length > 0;
  const dividendProviders = activeProviders(cap.dividends ?? EMPTY_CELL);
  const fundamentalsActive = hasFundamentalsActive(summary);
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
  // "Open →" button is the only drill affordance now.
  const ChartPane = (
    <Pane title="Price chart" onExpand={drillToWorkspace} fillHeight>
      <PriceChart symbol={symbol} instrumentId={instrumentId} />
    </Pane>
  );

  const HealthRow = renderHealthRow({
    fundamentalsActive,
    fundamentalsNode: fundamentalsActive ? (
      <FundamentalsPane summary={summary} />
    ) : null,
    dividendProviders,
    dividendsNode:
      dividendProviders.length > 0 ? (
        <>
          {dividendProviders.map((p) => (
            <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
          ))}
        </>
      ) : null,
  });

  // Filings pairs with the 10-K narrative when both are active so the
  // right-half dead space next to the narrative gets contextually
  // related content. Otherwise filings stay in the activity zone.
  const filingsPairedWithNarrative = hasNarrative && filingsActive;
  const filingsNode = filingsActive ? (
    <FilingsPane instrumentId={instrumentId} symbol={symbol} summary={summary} />
  ) : null;
  const ActivityRow = renderActivityRow({
    filingsActive: !filingsPairedWithNarrative && filingsActive,
    filingsNode: filingsPairedWithNarrative ? null : filingsNode,
    insiderActive,
    insiderNode: insiderActive ? (
      <InsiderActivitySummary symbol={symbol} />
    ) : null,
  });

  // Inter-pane vertical gap is 6 (gap-y-6) — without card borders, the
  // hairline rules need breathing room above/below to read as section
  // breaks. Horizontal gap-x-8 lets paired panes (Fund+Div, Fil+Ins)
  // sit as discrete columns without their hairlines visually merging.
  const gridCls = "grid grid-cols-12 gap-x-8 gap-y-6";

  if (profile === "full-sec") {
    return (
      <div className={gridCls}>
        {/* Zone A — Hero */}
        <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
        <div className="col-span-12 lg:col-span-4">
          <KeyStatsPane summary={summary} />
        </div>
        {hasNarrative && (
          <div className="col-span-12 lg:col-span-4">
            <SecProfilePanel symbol={symbol} />
          </div>
        )}
        {/* Zone B — Identity (paired with Filings at 8+4 when both active) */}
        {hasNarrative && filingsPairedWithNarrative ? (
          <>
            <div className="col-span-12 lg:col-span-8">
              <BusinessSectionsTeaser symbol={symbol} />
            </div>
            <div className="col-span-12 lg:col-span-4">{filingsNode}</div>
          </>
        ) : hasNarrative ? (
          <div className="col-span-12">
            <BusinessSectionsTeaser symbol={symbol} />
          </div>
        ) : null}
        {/* Zone C — Health */}
        {HealthRow}
        {/* Zone D — Activity */}
        {ActivityRow}
        <div className="col-span-12">
          <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
        </div>
        {/* Zone E — Operator */}
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
      <div className={gridCls}>
        {/* Zone A — Hero */}
        <div className="col-span-12 lg:col-span-8 lg:row-span-2">{ChartPane}</div>
        <div className="col-span-12 lg:col-span-4">
          <KeyStatsPane summary={summary} />
        </div>
        {hasNarrative && (
          <div className="col-span-12 lg:col-span-4">
            <SecProfilePanel symbol={symbol} />
          </div>
        )}
        {/* Zone B — Identity (paired with Filings at 8+4 when both active) */}
        {hasNarrative && filingsPairedWithNarrative ? (
          <>
            <div className="col-span-12 lg:col-span-8">
              <BusinessSectionsTeaser symbol={symbol} />
            </div>
            <div className="col-span-12 lg:col-span-4">{filingsNode}</div>
          </>
        ) : hasNarrative ? (
          <div className="col-span-12">
            <BusinessSectionsTeaser symbol={symbol} />
          </div>
        ) : null}
        {/* Zone C — Health (fundamentals optional in partial profile) */}
        {HealthRow}
        {/* Zone D — Activity */}
        {ActivityRow}
        <div className="col-span-12">
          <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
        </div>
        {/* Zone E — Operator */}
        {thesis !== null || thesisErrored ? (
          <div className="col-span-12">
            <ThesisPane thesis={thesis} errored={thesisErrored} />
          </div>
        ) : null}
      </div>
    );
  }

  // minimal profile — no filings/fundamentals/narrative. Hero + thesis
  // in the right rail (fills the row-2 slot under KeyStats), then the
  // small set of remaining panes the operator might still have.
  return (
    <div className={gridCls}>
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
        <div className="col-span-12 lg:col-span-6">
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

/** Health zone — Fundamentals + Dividends. Pairs at 6+6 when both
 *  capabilities are active; otherwise the surviving pane caps at 6
 *  cols so a narrow stat-block doesn't stretch full-width. */
function renderHealthRow({
  fundamentalsActive,
  fundamentalsNode,
  dividendProviders,
  dividendsNode,
}: {
  readonly fundamentalsActive: boolean;
  readonly fundamentalsNode: JSX.Element | null;
  readonly dividendProviders: ReadonlyArray<string>;
  readonly dividendsNode: JSX.Element | null;
}): JSX.Element | null {
  const hasDividends = dividendProviders.length > 0;
  if (fundamentalsActive && hasDividends) {
    return (
      <>
        <div className="col-span-12 lg:col-span-6">{fundamentalsNode}</div>
        <div className="col-span-12 lg:col-span-6">{dividendsNode}</div>
      </>
    );
  }
  if (fundamentalsActive) {
    // Fundamentals alone fills wide — its 4 charts benefit from
    // horizontal room (2x2 inside a 12-col container becomes
    // wider per-cell than 2x2 in a 6-col container).
    return <div className="col-span-12">{fundamentalsNode}</div>;
  }
  if (hasDividends) {
    return <div className="col-span-12 lg:col-span-6">{dividendsNode}</div>;
  }
  return null;
}

/** Activity zone — Filings + Insider. Pairs at 6+6 when both are
 *  active. Filings alone goes full-width (list scans wide fine);
 *  insider alone caps at 6 since the 5-stat strip looks stretched
 *  full-width. */
function renderActivityRow({
  filingsActive,
  filingsNode,
  insiderActive,
  insiderNode,
}: {
  readonly filingsActive: boolean;
  readonly filingsNode: JSX.Element | null;
  readonly insiderActive: boolean;
  readonly insiderNode: JSX.Element | null;
}): JSX.Element | null {
  if (filingsActive && insiderActive) {
    return (
      <>
        <div className="col-span-12 lg:col-span-6">{filingsNode}</div>
        <div className="col-span-12 lg:col-span-6">{insiderNode}</div>
      </>
    );
  }
  if (filingsActive) {
    return <div className="col-span-12">{filingsNode}</div>;
  }
  if (insiderActive) {
    return <div className="col-span-12 lg:col-span-6">{insiderNode}</div>;
  }
  return null;
}
