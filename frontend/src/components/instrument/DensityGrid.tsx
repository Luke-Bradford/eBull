/**
 * DensityGrid — bento-grid instrument page (#684 round 3).
 *
 * Operator review 2026-04-29: previous flat 12-col layout had two
 * recurring gripes —
 *   1. Half-width tiles (Dividends at lg:col-span-6) sat next to
 *      empty grid cells when their would-be neighbour wasn't
 *      capability-active — visible dead space.
 *   2. Static row layouts (Filings col-span-7 + Insider col-span-5)
 *      didn't re-flow when one of the panes was absent — left a
 *      lone narrow stat block stretched across the row.
 *
 * Fix: every dynamic row routes through ``allocateTiles``, which
 * filters out absent panes BEFORE the grid sees them and allocates
 * column spans deterministically based on the surviving count
 * (1 → 12, 2 → 6+6, 3 → 4+4+4, 4 → 3+3+3+3). PriceChart + KeyStats
 * + SecProfile keep their existing fixed allocations because they're
 * always present (or KeyStats can render with a "no key stats" empty
 * state in-pane). Long-form panes (BusinessSections, ThesisPane)
 * stay full-width — narrative content reads fine wide given the
 * inner ``max-w-prose`` cap shipped in #687.
 *
 * Bento aesthetic per ``ui-ux-pro-max`` skill (Apple/dashboard
 * pattern): rounded-xl card chrome, subtle hover lift, varied
 * tile sizes (1x1 + 2x1), neutral shadows. Card chrome itself
 * lives in ``Pane.tsx``; this file allocates the grid spans.
 */

import type { ReactNode } from "react";
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

/** A renderable tile + the gating predicate. ``content`` is only
 *  evaluated when ``present`` is true, so we don't force-render
 *  panes that are about to null-out. */
interface TileSpec {
  readonly key: string;
  readonly present: boolean;
  readonly content: () => ReactNode;
}

/** Allocate column spans across the surviving tiles deterministically.
 *  Fills the 12-col row regardless of which subset of inputs is
 *  present — when a pane null-outs, the survivors absorb its width
 *  rather than leaving a ghost cell.
 *
 *  | Surviving | Spans                        |
 *  |-----------|------------------------------|
 *  | 1         | col-span-12                  |
 *  | 2         | 6 + 6                        |
 *  | 3         | 4 + 4 + 4                    |
 *  | 4         | 3 + 3 + 3 + 3                |
 *  | >4        | falls through to col-span-12 (unsupported — won't fire today) |
 */
function allocateTiles(tiles: ReadonlyArray<TileSpec>): ReactNode[] {
  const present = tiles.filter((t) => t.present);
  const n = present.length;
  if (n === 0) return [];
  const spanByCount: Record<number, string> = {
    1: "col-span-12",
    2: "col-span-12 lg:col-span-6",
    3: "col-span-12 lg:col-span-4",
    4: "col-span-12 lg:col-span-3",
  };
  const span = spanByCount[n] ?? "col-span-12";
  return present.map((t) => (
    <div key={t.key} className={span}>
      {t.content()}
    </div>
  ));
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
  const filingsActive = activeProviders(cap.filings ?? EMPTY_CELL).length > 0;
  const fundamentalsActive = hasFundamentalsActive(summary);
  const hasNarrative = summary.has_sec_cik;
  const hasThesis = thesis !== null || thesisErrored;
  const navigate = useNavigate();
  const [overviewParams] = useSearchParams();

  const drillToWorkspace = () => {
    const overviewRange = overviewParams.get("chart");
    const target = `/instrument/${encodeURIComponent(symbol)}/chart`;
    const url =
      overviewRange !== null && overviewRange !== ""
        ? `${target}?range=${encodeURIComponent(overviewRange)}`
        : target;
    navigate(url);
  };

  // ---- Top hero row: chart (col-span-8, row-span-2) + KeyStats +
  // SecProfile stacked on the right. PriceChart's tall row-span keeps
  // the chart visually anchored; KeyStats and SecProfile share the
  // narrow right rail.
  const heroRow = (
    <>
      <div className="col-span-12 lg:col-span-8 lg:row-span-2">
        <Pane title="Price chart" onExpand={drillToWorkspace} fillHeight>
          <PriceChart symbol={symbol} instrumentId={instrumentId} />
        </Pane>
      </div>
      <div className="col-span-12 lg:col-span-4">
        <KeyStatsPane summary={summary} />
      </div>
      {hasNarrative && (
        <div className="col-span-12 lg:col-span-4">
          <SecProfilePanel symbol={symbol} />
        </div>
      )}
    </>
  );

  // ---- Stat-block row: Filings + Insider + Dividends. Each
  // null-outs independently when its capability is inactive; the
  // allocator re-flows so the row always fills.
  const statTiles: TileSpec[] = [
    {
      key: "filings",
      present: filingsActive,
      content: () => (
        <FilingsPane
          instrumentId={instrumentId}
          symbol={symbol}
          summary={summary}
        />
      ),
    },
    {
      key: "insider",
      present: insiderActive,
      content: () => <InsiderActivitySummary symbol={symbol} />,
    },
    {
      key: "dividends",
      present: dividendProviders.length > 0,
      content: () => (
        <>
          {dividendProviders.map((p) => (
            <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
          ))}
        </>
      ),
    },
  ];

  // ---- Mid row: Fundamentals + RecentNews paired when both
  // present (fundamentals takes 2/3, news 1/3 — the 4 sparkline
  // cells fit a wide tile, news list reads fine in a narrower one).
  // When only one is present, allocator gives it the full row.
  const midTiles: TileSpec[] = [
    {
      key: "fundamentals",
      present: fundamentalsActive,
      content: () => <FundamentalsPane summary={summary} />,
    },
    {
      key: "news",
      present: true, // news pane has its own empty state; always show
      content: () => <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />,
    },
  ];
  // Override the standard 6+6 split when fundamentals is present —
  // the 4-cell sparkline strip wants 8 cols, news is happy with 4.
  const midRow =
    fundamentalsActive ? (
      <>
        <div className="col-span-12 lg:col-span-8">
          <FundamentalsPane summary={summary} />
        </div>
        <div className="col-span-12 lg:col-span-4">
          <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
        </div>
      </>
    ) : (
      allocateTiles(midTiles)
    );

  // ---- Long-form footer: narrative (when has_sec_cik) + thesis
  // (when present). Each is full-width because the inner content
  // already caps at max-w-prose.
  const narrativeRow = hasNarrative ? (
    <div className="col-span-12">
      <BusinessSectionsTeaser symbol={symbol} />
    </div>
  ) : null;

  const thesisRow = hasThesis ? (
    <div className="col-span-12">
      <ThesisPane thesis={thesis} errored={thesisErrored} />
    </div>
  ) : null;

  if (profile === "full-sec") {
    return (
      <div className="grid grid-cols-12 gap-3">
        {heroRow}
        {midRow}
        {allocateTiles(statTiles)}
        {narrativeRow}
        {thesisRow}
      </div>
    );
  }

  if (profile === "partial-filings") {
    // Same shape minus the fundamentals pane in midRow.
    return (
      <div className="grid grid-cols-12 gap-3">
        {heroRow}
        {midRow}
        {allocateTiles(statTiles)}
        {narrativeRow}
        {thesisRow}
      </div>
    );
  }

  // minimal — no SEC fundamentals, no filings. The hero row gives us
  // PriceChart (8x2) + KeyStats (4) on top; place ThesisPane in the
  // right-rail 4-col slot below KeyStats so the chart's row-span-2
  // gap fills, mirroring the SecProfile pattern in full-sec.
  return (
    <div className="grid grid-cols-12 gap-3">
      {heroRow}
      {hasThesis && (
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
      <div
        className={
          dividendProviders.length > 0
            ? "col-span-12 lg:col-span-6"
            : "col-span-12"
        }
      >
        <RecentNewsPane instrumentId={instrumentId} symbol={symbol} />
      </div>
    </div>
  );
}
