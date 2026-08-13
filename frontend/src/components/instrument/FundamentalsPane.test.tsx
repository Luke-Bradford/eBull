import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
import * as api from "@/api/instruments";
import type { InstrumentSummary } from "@/api/types";

const navigateMock = vi.fn();
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual<typeof import("react-router-dom")>(
    "react-router-dom",
  );
  return { ...actual, useNavigate: () => navigateMock };
});

function makeSummary(secXbrlActive: boolean): InstrumentSummary {
  return {
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
        providers: secXbrlActive ? ["sec_xbrl"] : [],
        data_present: secXbrlActive ? { sec_xbrl: true } : {},
      },
    },
    key_stats: null,
  } as never;
}

const incomeRows = Array.from({ length: 4 }, (_, i) => ({
  period_end: `2026-0${i + 1}-30`,
  period_type: `Q${i + 1}`,
  values: {
    revenue: String(1000 + i * 100),
    operating_income: String(50 + i * 5),
    net_income: String(40 + i * 4),
  },
}));
const balanceRows = Array.from({ length: 4 }, (_, i) => ({
  period_end: `2026-0${i + 1}-30`,
  period_type: `Q${i + 1}`,
  values: {
    long_term_debt: String(200 + i * 10),
    short_term_debt: String(50 + i * 2),
  },
}));

describe("FundamentalsPane", () => {
  it("returns null when sec_xbrl capability is inactive", () => {
    const { container } = render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(false)} />
      </MemoryRouter>,
    );
    expect(container.firstChild).toBeNull();
  });

  it("renders 4 sparklines when capability active and data present", async () => {
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: incomeRows,
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: balanceRows,
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    expect(await screen.findByText("Revenue")).toBeInTheDocument();
    expect(screen.getByText("Op income")).toBeInTheDocument();
    expect(screen.getByText("Net income")).toBeInTheDocument();
    expect(screen.getByText("Total debt")).toBeInTheDocument();
  });

  it("renders Open button and navigates to the fundamentals drill route", async () => {
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: incomeRows,
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: balanceRows,
        });
      }) as never,
    );
    navigateMock.mockReset();
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    const btn = await screen.findByRole("button", { name: /open/i });
    await userEvent.click(btn);
    expect(navigateMock).toHaveBeenCalledWith("/instrument/GME/fundamentals");
  });

  it("computes total debt as long_term_debt + short_term_debt per period", async () => {
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: [
              {
                period_end: "2026-03-30",
                period_type: "Q1",
                values: { revenue: "100", operating_income: "10", net_income: "5" },
              },
              {
                period_end: "2026-06-30",
                period_type: "Q2",
                values: { revenue: "200", operating_income: "20", net_income: "10" },
              },
            ],
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: [
            {
              period_end: "2026-03-30",
              period_type: "Q1",
              values: { long_term_debt: "100", short_term_debt: "20" },
            },
            {
              period_end: "2026-06-30",
              period_type: "Q2",
              values: { long_term_debt: "150", short_term_debt: "30" },
            },
          ],
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    // Latest total debt = 150 + 30 = 180
    expect(await screen.findByText(/180/)).toBeInTheDocument();
  });

  it("renders an in-pane empty state when capability active but joined series is empty (design-system v1)", async () => {
    // Was: returned null. Codex review of design-system v1 caught
    // that this left a dead 6-col wrapper in the bento Health row.
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) =>
        Promise.resolve({
          symbol: "GME",
          statement: query.statement,
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: [],
        })) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(
        screen.getByText(/Insufficient quarterly history yet/i),
      ).toBeInTheDocument();
    });
  });

  it("renders the pane for partnership/MLP issuers that don't file operating_income (#684)", async () => {
    // IEP / ET / EPD-style: revenue + net_income populated but
    // operating_income null on every row because the issuer files
    // ``IncomeLossFromContinuingOperations`` instead of
    // ``OperatingIncomeLoss``. Pre-fix, the joinPeriods strict gate
    // dropped every row → series.length < 2 → pane hidden. Post-fix,
    // the pane renders revenue + net income sparklines and the
    // op-income cell shows "—".
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "IEP",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: Array.from({ length: 4 }, (_, i) => ({
              period_end: `2025-0${i + 1}-30`,
              period_type: `Q${i + 1}`,
              values: {
                revenue: String(2000 + i * 100),
                // operating_income deliberately absent
                net_income: String(-100 + i * 50),
              },
            })),
          });
        }
        return Promise.resolve({
          symbol: "IEP",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: [],
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    // Pane chrome renders + the four cell labels are present.
    expect(await screen.findByText("Revenue")).toBeInTheDocument();
    expect(screen.getByText("Op income")).toBeInTheDocument();
    expect(screen.getByText("Net income")).toBeInTheDocument();
    // Latest revenue = 2300 renders via formatLatest as "2.30K".
    expect(await screen.findByText("2.30K")).toBeInTheDocument();
  });

  it("surfaces a coverage caption when one cell has fewer periods than its siblings (#684 review)", async () => {
    // Constructed case: 4 periods of revenue + net_income, only 2
    // periods of operating_income (e.g. issuer changed reporting
    // mid-history). Op income cell should annotate "2/4 periods"
    // so the operator notices the time-axis asymmetry — bot review
    // WARNING: silently-divergent sparkline shapes mislead.
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: Array.from({ length: 4 }, (_, i) => ({
              period_end: `2025-0${i + 1}-30`,
              period_type: `Q${i + 1}`,
              values: {
                revenue: String(2000 + i * 100),
                net_income: String(100 + i * 10),
                ...(i >= 2
                  ? { operating_income: String(50 + i * 5) }
                  : {}),
              },
            })),
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: [],
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    expect(await screen.findByText("2/4")).toBeInTheDocument();
  });

  it("renders an in-pane empty state when capability active but only 1 quarter has data (design-system v1)", async () => {
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: string }) => {
        if (query.statement === "income") {
          return Promise.resolve({
            symbol: "GME",
            statement: "income",
            period: "quarterly",
            currency: "USD",
            source: "sec_xbrl",
            rows: [
              {
                period_end: "2026-03-30",
                period_type: "Q1",
                values: { revenue: "100", operating_income: "10", net_income: "5" },
              },
            ],
          });
        }
        return Promise.resolve({
          symbol: "GME",
          statement: "balance",
          period: "quarterly",
          currency: "USD",
          source: "sec_xbrl",
          rows: [
            {
              period_end: "2026-03-30",
              period_type: "Q1",
              values: { long_term_debt: "100", short_term_debt: "20" },
            },
          ],
        });
      }) as never,
    );
    render(
      <MemoryRouter>
        <FundamentalsPane summary={makeSummary(true)} />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(
        screen.getByText(/Insufficient quarterly history yet/i),
      ).toBeInTheDocument();
    });
  });
});
