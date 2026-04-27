import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FundamentalsPane } from "@/components/instrument/FundamentalsPane";
import * as api from "@/api/instruments";
import type { InstrumentSummary } from "@/api/types";

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
});
