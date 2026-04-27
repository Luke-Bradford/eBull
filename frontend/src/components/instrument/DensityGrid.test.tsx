import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { DensityGrid } from "@/components/instrument/DensityGrid";

// Mock child components that make their own API calls so the DensityGrid
// unit test stays isolated.
vi.mock("@/components/instrument/PriceChart", () => ({
  PriceChart: () => <div data-testid="price-chart-stub" />,
}));
vi.mock("@/components/instrument/SecProfilePanel", () => ({
  SecProfilePanel: () => <div>SEC Profile</div>,
}));
vi.mock("@/components/instrument/BusinessSectionsTeaser", () => ({
  BusinessSectionsTeaser: () => <div>Company narrative (SEC 10-K Item 1)</div>,
}));
vi.mock("@/components/instrument/FilingsPane", () => ({
  FilingsPane: () => (
    <section>
      <header>
        <h2>Recent filings</h2>
      </header>
    </section>
  ),
}));
vi.mock("@/components/instrument/DividendsPanel", () => ({
  DividendsPanel: () => <div>Dividends</div>,
}));
vi.mock("@/components/instrument/InsiderActivityPanel", () => ({
  InsiderActivityPanel: () => <div>Insider</div>,
}));
vi.mock("@/components/instrument/FundamentalsPane", () => ({
  FundamentalsPane: () => <div>Fundamentals stub</div>,
}));

const summary = {
  instrument_id: 1,
  has_sec_cik: true,
  identity: {
    symbol: "GME",
    display_name: "GameStop",
    market_cap: "1000000",
    sector: null,
  },
  capabilities: {},
  key_stats: null,
} as never;

describe("DensityGrid", () => {
  it("renders the chart stub, the slot blocks, and FilingsPane title", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={summary}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("price-chart-stub")).toBeInTheDocument();
    expect(screen.getByText("KEY STATS BLOCK")).toBeInTheDocument();
    expect(screen.getByText("THESIS BLOCK")).toBeInTheDocument();
    expect(screen.getByText("NEWS BLOCK")).toBeInTheDocument();
    expect(screen.getByText("Recent filings")).toBeInTheDocument();
  });

  it("shows the SEC profile pane when has_sec_cik is true", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={summary}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("SEC Profile")).toBeInTheDocument();
  });

  it("shows 'No SEC coverage' fallback when has_sec_cik is false", () => {
    const noSec = {
      instrument_id: 1,
      has_sec_cik: false,
      identity: {
        symbol: "GME",
        display_name: "GameStop",
        market_cap: "1000000",
        sector: null,
      },
      capabilities: {},
      key_stats: null,
    } as never;
    render(
      <MemoryRouter>
        <DensityGrid
          summary={noSec}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("No SEC coverage")).toBeInTheDocument();
  });

  it("individual panes have no overflow-auto (content-driven, not scrollboxes)", () => {
    // summary has capabilities:{} so the dividends+insider combined card
    // (which retains overflow-auto + max-h as a scroll-bound until Phase D)
    // is not rendered — this test covers only the standard pane set.
    const { container } = render(
      <MemoryRouter>
        <DensityGrid
          summary={summary}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    const overflowAuto = container.querySelectorAll(".overflow-auto");
    expect(overflowAuto.length).toBe(0);
  });

  it("renders FundamentalsPane only when sec_xbrl fundamentals capability is active", () => {
    const summaryActive = {
      instrument_id: 1,
      has_sec_cik: true,
      identity: {
        symbol: "GME",
        display_name: "GameStop",
        market_cap: "1000000",
        sector: null,
      },
      capabilities: {
        fundamentals: {
          providers: ["sec_xbrl"],
          data_present: { sec_xbrl: true },
        },
      },
      key_stats: null,
    } as never;
    const { rerender } = render(
      <MemoryRouter>
        <DensityGrid
          summary={summaryActive}
          keyStatsBlock={<div>K</div>}
          thesisBlock={<div>T</div>}
          newsBlock={<div>N</div>}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("Fundamentals stub")).toBeInTheDocument();

    const summaryInactive = {
      instrument_id: 1,
      has_sec_cik: true,
      identity: {
        symbol: "GME",
        display_name: "GameStop",
        market_cap: "1000000",
        sector: null,
      },
      capabilities: {},
      key_stats: null,
    } as never;
    rerender(
      <MemoryRouter>
        <DensityGrid
          summary={summaryInactive}
          keyStatsBlock={<div>K</div>}
          thesisBlock={<div>T</div>}
          newsBlock={<div>N</div>}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByText("Fundamentals stub")).toBeNull();
  });

  it("combined dividends/insider card uses exactly one overflow-auto scroll-bound (Phase D regression guard)", () => {
    // When insider is active, InsiderActivityPanel renders up to 50 rows.
    // The combined card retains overflow-auto + max-h-[360px] intentionally
    // until Phase D replaces it with InsiderActivitySummary.
    // This test pins the count to 1 so Phase D's removal of that bound
    // is explicitly regression-guarded and not silently missed.
    const summaryWithInsider = {
      instrument_id: 1,
      has_sec_cik: true,
      identity: {
        symbol: "GME",
        display_name: "GameStop",
        market_cap: "1000000",
        sector: null,
      },
      capabilities: {
        insider: {
          providers: ["sec_form4"],
          data_present: { sec_form4: true },
        },
      },
      key_stats: null,
    } as never;
    const { container } = render(
      <MemoryRouter>
        <DensityGrid
          summary={summaryWithInsider}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    const overflowAuto = container.querySelectorAll(".overflow-auto");
    expect(overflowAuto.length).toBe(1);
  });
});
