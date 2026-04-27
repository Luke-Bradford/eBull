import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import type { InstrumentDividends } from "@/api/instruments";

import { DividendsPanel } from "./DividendsPanel";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentDividends: vi.fn(),
}));

const navigateMock = vi.fn();
vi.mock("react-router-dom", async (importActual) => {
  const actual = (await importActual()) as object;
  return { ...actual, useNavigate: () => navigateMock };
});

import { fetchInstrumentDividends } from "@/api/instruments";
const mockFetch = vi.mocked(fetchInstrumentDividends);

function makePeriod(
  fy: number,
  qt: string,
  date: string,
  dps: string,
): InstrumentDividends["history"][number] {
  return {
    period_end_date: date,
    period_type: qt,
    fiscal_year: fy,
    fiscal_quarter: null,
    dps_declared: dps,
    dividends_paid: null,
    reported_currency: "USD",
  };
}

function paid(): InstrumentDividends {
  return {
    symbol: "AAPL",
    summary: {
      has_dividend: true,
      ttm_dps: "1.0000",
      ttm_dividends_paid: "15000000000.0000",
      ttm_yield_pct: "0.52",
      latest_dps: "0.2500",
      latest_dividend_at: "2025-12-28",
      dividend_streak_q: 40,
      dividend_currency: "USD",
    },
    history: [
      makePeriod(2025, "Q4", "2025-12-28", "0.2500"),
      makePeriod(2025, "Q3", "2025-09-28", "0.2500"),
    ],
    upcoming: [],
  };
}

function wrap(ui: React.ReactElement) {
  return render(<MemoryRouter>{ui}</MemoryRouter>);
}

afterEach(() => vi.clearAllMocks());

beforeEach(() => {
  navigateMock.mockReset();
});

describe("DividendsPanel", () => {
  it("renders summary + per-quarter history for a paying instrument", async () => {
    mockFetch.mockResolvedValue(paid());
    wrap(<DividendsPanel symbol="AAPL" provider="sec_dividend_summary" />);

    await waitFor(() => {
      expect(screen.getByText(/TTM yield/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/0.52%/)).toBeInTheDocument();
    expect(screen.getByText(/FY2025 Q4/)).toBeInTheDocument();
    expect(screen.getByText(/FY2025 Q3/)).toBeInTheDocument();
    expect(screen.getByText(/Consecutive quarters/i)).toBeInTheDocument();
    expect(screen.getByText("40")).toBeInTheDocument();
  });

  it("returns null when history is empty AND upcoming is empty", async () => {
    mockFetch.mockResolvedValueOnce({
      symbol: "X",
      summary: { has_dividend: false } as never,
      history: [],
      upcoming: [],
    } as never);
    const { container } = wrap(
      <DividendsPanel symbol="X" provider="sec_dividend_summary" />,
    );
    await waitFor(() => expect(container.firstChild).toBeNull());
  });

  it("renders Pane when history is empty but upcoming has 1 item", async () => {
    mockFetch.mockResolvedValueOnce({
      symbol: "X",
      summary: { has_dividend: true } as never,
      history: [],
      upcoming: [{ ex_date: "2026-05-01" } as never],
    } as never);
    wrap(<DividendsPanel symbol="X" provider="sec_dividend_summary" />);
    // Pane renders — the h2 title "Dividends" is present (source badge may also
    // contain the word; use getAllByText to avoid the "multiple elements" error).
    await waitFor(() =>
      expect(screen.getAllByText(/Dividends/i).length).toBeGreaterThan(0),
    );
  });

  it("renders error state + retry on fetch failure", async () => {
    mockFetch.mockRejectedValue(new Error("boom"));
    wrap(<DividendsPanel symbol="AAPL" provider="sec_dividend_summary" />);

    await waitFor(() => {
      expect(screen.getByText(/Failed to load/i)).toBeInTheDocument();
    });
  });

  it("renders the Next dividend banner when upcoming[] has an entry", async () => {
    const withUpcoming = paid();
    withUpcoming.upcoming = [
      {
        source_accession: "0000320193-26-000001",
        declaration_date: "2026-01-15",
        ex_date: "2026-02-10",
        record_date: "2026-02-11",
        pay_date: "2026-02-20",
        dps_declared: "0.2500",
        currency: "USD",
      },
    ];
    mockFetch.mockResolvedValue(withUpcoming);
    wrap(<DividendsPanel symbol="AAPL" provider="sec_dividend_summary" />);

    await waitFor(() => {
      expect(screen.getByText(/Next dividend/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/Ex-date/i)).toBeInTheDocument();
    expect(screen.getByText(/2026-02-10/)).toBeInTheDocument();
    expect(screen.getByText(/2026-02-11/)).toBeInTheDocument();
    expect(screen.getByText(/2026-02-20/)).toBeInTheDocument();
  });

  it("renders at most 4 HistoryBar rows regardless of history length", async () => {
    const data = paid();
    // Extend history to 7 quarters.
    data.history = [
      makePeriod(2025, "Q4", "2025-12-28", "0.25"),
      makePeriod(2025, "Q3", "2025-09-28", "0.25"),
      makePeriod(2025, "Q2", "2025-06-28", "0.24"),
      makePeriod(2025, "Q1", "2025-03-28", "0.24"),
      makePeriod(2024, "Q4", "2024-12-28", "0.23"),
      makePeriod(2024, "Q3", "2024-09-28", "0.23"),
      makePeriod(2024, "Q2", "2024-06-28", "0.22"),
    ];
    mockFetch.mockResolvedValue(data);
    wrap(<DividendsPanel symbol="AAPL" provider="sec_dividend_summary" />);

    await waitFor(() =>
      expect(screen.getByText(/FY2025 Q4/)).toBeInTheDocument(),
    );
    // Exactly 4 progressbar elements (one per HistoryBar).
    const bars = screen.getAllByRole("progressbar");
    expect(bars).toHaveLength(4);
    // FY2024 Q2 must NOT appear — it is beyond the 4-row limit.
    expect(screen.queryByText(/FY2024 Q2/)).toBeNull();
  });

  it("Open button navigates to /instrument/<symbol>/dividends preserving the provider", async () => {
    mockFetch.mockResolvedValue(paid());
    wrap(<DividendsPanel symbol="GME" provider="sec_dividend_summary" />);

    const btn = await screen.findByRole("button", { name: /open/i });
    await userEvent.click(btn);
    expect(navigateMock).toHaveBeenCalledWith(
      "/instrument/GME/dividends?provider=sec_dividend_summary",
    );
  });
});
