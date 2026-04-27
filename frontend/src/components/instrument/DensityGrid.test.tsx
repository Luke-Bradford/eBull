import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { DensityGrid } from "@/components/instrument/DensityGrid";
import type { InstrumentSummary } from "@/api/types";

const navigateMock = vi.fn();
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual<typeof import("react-router-dom")>("react-router-dom");
  return { ...actual, useNavigate: () => navigateMock };
});

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
  FilingsPane: () => <div>Recent filings</div>,
}));
vi.mock("@/components/instrument/DividendsPanel", () => ({
  DividendsPanel: () => <div>Dividends</div>,
}));
vi.mock("@/components/instrument/InsiderActivitySummary", () => ({
  InsiderActivitySummary: () => <div>Insider summary</div>,
}));
vi.mock("@/components/instrument/FundamentalsPane", () => ({
  FundamentalsPane: () => <div>Fundamentals</div>,
}));
vi.mock("@/components/instrument/KeyStatsPane", () => ({
  KeyStatsPane: () => <div>Key statistics</div>,
}));
vi.mock("@/components/instrument/ThesisPane", () => ({
  ThesisPane: ({ thesis, errored }: { thesis: unknown; errored: boolean }) =>
    thesis === null && !errored ? null : <div>Thesis pane</div>,
}));
vi.mock("@/components/instrument/RecentNewsPane", () => ({
  RecentNewsPane: () => <div>Recent news</div>,
}));

const baseIdentity = {
  symbol: "GME",
  display_name: "GameStop",
  market_cap: "1000000000",
  sector: null,
};

function makeSummary(
  capabilities: InstrumentSummary["capabilities"],
  has_sec_cik: boolean = true,
): InstrumentSummary {
  return {
    instrument_id: 1,
    is_tradable: true,
    coverage_tier: 1,
    identity: baseIdentity,
    price: null,
    key_stats: null,
    source: {},
    has_sec_cik,
    has_filings_coverage: false,
    capabilities,
  } as InstrumentSummary;
}

describe("DensityGrid profiles", () => {
  it("full-sec profile: chart + key stats + sec profile + fundamentals + filings + insider rendered", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({
            fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
            filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
            insider: { providers: ["sec_form4"], data_present: { sec_form4: true } },
          })}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("price-chart-stub")).toBeInTheDocument();
    expect(screen.getByText("Key statistics")).toBeInTheDocument();
    expect(screen.getByText("SEC Profile")).toBeInTheDocument();
    expect(screen.getByText("Fundamentals")).toBeInTheDocument();
    expect(screen.getByText(/Recent filings/)).toBeInTheDocument();
    expect(screen.getByText(/Insider summary/)).toBeInTheDocument();
    expect(screen.getByText("Recent news")).toBeInTheDocument();
  });

  it("full-sec profile without has_sec_cik: SEC profile slot absent (no ghost div)", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary(
            {
              fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
              filings: { providers: ["sec_edgar"], data_present: { sec_edgar: true } },
            },
            false, // has_sec_cik = false
          )}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByText("SEC Profile")).not.toBeInTheDocument();
    // The chart + key stats row should not have an extra empty col-4 slot.
    // Sanity: the only col-span-4 element in this profile branch is KeyStats.
  });

  it("partial-filings profile: no fundamentals; filings full-width; insider+dividends share row when both active", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({
            filings: { providers: ["companies_house"], data_present: { companies_house: true } },
            insider: { providers: ["sec_form4"], data_present: { sec_form4: true } },
            dividends: { providers: ["sec_dividend_summary"], data_present: { sec_dividend_summary: true } },
          })}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByText("Fundamentals")).not.toBeInTheDocument();
    expect(screen.getByText(/Recent filings/)).toBeInTheDocument();
    expect(screen.getByText(/Insider summary/)).toBeInTheDocument();
    expect(screen.getByText(/Dividends/)).toBeInTheDocument();
  });

  it("partial-filings without insider or dividends: row absent", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({
            filings: { providers: ["companies_house"], data_present: { companies_house: true } },
          })}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByText(/Insider summary/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Dividends/)).not.toBeInTheDocument();
  });

  it("minimal profile: no filings/fundamentals/insider/narrative panes", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({}, false)}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByText(/Recent filings/)).not.toBeInTheDocument();
    expect(screen.queryByText("Fundamentals")).not.toBeInTheDocument();
    expect(screen.queryByText(/Insider summary/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Company narrative/)).not.toBeInTheDocument();
    expect(screen.getByText("Key statistics")).toBeInTheDocument();
    expect(screen.getByTestId("price-chart-stub")).toBeInTheDocument();
  });

  it("thesis pane absent when thesis is null and not errored", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({})}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByText("Thesis pane")).not.toBeInTheDocument();
  });

  it("thesis pane present when thesis is a real object", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({})}
          thesis={{ thesis_id: 1 } as never}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("Thesis pane")).toBeInTheDocument();
  });

  it("partial-filings profile renders FundamentalsPane when fundamentals are active without filings", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({
            fundamentals: { providers: ["sec_xbrl"], data_present: { sec_xbrl: true } },
          })}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("Fundamentals")).toBeInTheDocument();
    expect(screen.queryByText(/Recent filings/)).not.toBeInTheDocument();
  });

  it("no overflow-auto wrappers anywhere in the grid", () => {
    const { container } = render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({
            dividends: { providers: ["sec_dividend_summary"], data_present: { sec_dividend_summary: true } },
            insider: { providers: ["sec_form4"], data_present: { sec_form4: true } },
          })}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    expect(container.querySelectorAll(".overflow-auto").length).toBe(0);
  });

  it("chart pane has an Open button that navigates to /instrument/:symbol/chart", async () => {
    navigateMock.mockClear();
    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <DensityGrid
          summary={makeSummary({})}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    // PaneHeader renders the button labelled "Open →" when onExpand is set.
    const openButton = screen.getByRole("button", { name: /open/i });
    await user.click(openButton);
    expect(navigateMock).toHaveBeenCalledWith("/instrument/GME/chart");
  });

  it("chart pane Open button preserves overview ?chart= range as ?range= on chart workspace", async () => {
    navigateMock.mockClear();
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={["/instrument/GME?chart=5y"]}>
        <DensityGrid
          summary={makeSummary({})}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    const openButton = screen.getByRole("button", { name: /open/i });
    await user.click(openButton);
    expect(navigateMock).toHaveBeenCalledWith("/instrument/GME/chart?range=5y");
  });

  it("clicking the chart card body does NOT drill — only Open button drills (#601 follow-up)", async () => {
    navigateMock.mockClear();
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={["/instrument/GME?chart=1y"]}>
        <DensityGrid
          summary={makeSummary({})}
          thesis={null}
          thesisErrored={false}
        />
      </MemoryRouter>,
    );
    // Clicking the chart body must not fire the drill — operator
    // reported the whole-card click was firing accidentally during
    // chart hover/zoom interactions, so the card-click hook was
    // removed in favour of the explicit "Open →" button only.
    await user.click(screen.getByTestId("price-chart-stub"));
    expect(navigateMock).not.toHaveBeenCalled();
  });
});
