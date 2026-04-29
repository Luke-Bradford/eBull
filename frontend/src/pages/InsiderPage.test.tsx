import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { InsiderPage } from "@/pages/InsiderPage";
import * as api from "@/api/instruments";

// Mock the chart-rendering subtrees — recharts + lightweight-charts
// rely on layout APIs (ResponsiveContainer, canvas) that jsdom does
// not fully implement, and re-testing chart pixels here would shadow
// the unit-test coverage in the dedicated component test files. The
// page test exists to verify route mounting + data wiring, not chart
// internals.
vi.mock("@/components/insider/InsiderNetByMonth", () => ({
  InsiderNetByMonth: ({
    transactions,
  }: {
    transactions: ReadonlyArray<unknown>;
  }) => (
    <div data-testid="mock-net-by-month">net-by-month {transactions.length}</div>
  ),
}));
vi.mock("@/components/insider/InsiderByOfficer", () => ({
  InsiderByOfficer: ({
    transactions,
  }: {
    transactions: ReadonlyArray<unknown>;
  }) => (
    <div data-testid="mock-by-officer">by-officer {transactions.length}</div>
  ),
}));
vi.mock("@/components/insider/InsiderPriceMarkers", () => ({
  InsiderPriceMarkers: ({
    transactions,
    candles,
  }: {
    transactions: ReadonlyArray<unknown>;
    candles: ReadonlyArray<unknown>;
  }) => (
    <div data-testid="mock-price-markers">
      price-markers {transactions.length}/{candles.length}
    </div>
  ),
}));

const SAMPLE_TXN = {
  accession_number: "A1",
  txn_row_num: 0,
  document_type: "4",
  txn_date: "2026-04-15",
  deemed_execution_date: null,
  filer_cik: "0000000001",
  filer_name: "Jane Doe",
  filer_role: "officer:CFO",
  security_title: "Common Stock",
  txn_code: "P",
  acquired_disposed_code: "A",
  shares: "100",
  price: "10",
  post_transaction_shares: "1000",
  direct_indirect: "D",
  nature_of_ownership: null,
  is_derivative: false,
  equity_swap_involved: null,
  transaction_timeliness: null,
  conversion_exercise_price: null,
  exercise_date: null,
  expiration_date: null,
  underlying_security_title: null,
  underlying_shares: null,
  underlying_value: null,
  footnotes: {},
};

function renderAt(symbol: string) {
  return render(
    <MemoryRouter initialEntries={[`/instrument/${symbol}/insider`]}>
      <Routes>
        <Route
          path="/instrument/:symbol/insider"
          element={<InsiderPage />}
        />
      </Routes>
    </MemoryRouter>,
  );
}

describe("InsiderPage", () => {
  it("renders all four panes when the API returns transactions + candles", async () => {
    vi.spyOn(api, "fetchInsiderTransactions").mockResolvedValue({
      symbol: "GME",
      rows: [SAMPLE_TXN],
    });
    vi.spyOn(api, "fetchInstrumentCandles").mockResolvedValue({
      symbol: "GME",
      range: "5y",
      days: 1825,
      rows: [
        { date: "2026-04-14", open: "10", high: "11", low: "9", close: "10.5", volume: "1000" },
      ],
    });

    renderAt("GME");

    expect(await screen.findByTestId("mock-net-by-month")).toHaveTextContent(
      "net-by-month 1",
    );
    expect(screen.getByTestId("mock-by-officer")).toHaveTextContent(
      "by-officer 1",
    );
    expect(screen.getByTestId("mock-price-markers")).toHaveTextContent(
      "price-markers 1/1",
    );
    // Table header is rendered by the unmocked table component.
    expect(screen.getByText(/All transactions/i)).toBeInTheDocument();
  });

  it("renders empty state when the instrument has no Form 4 history", async () => {
    vi.spyOn(api, "fetchInsiderTransactions").mockResolvedValue({
      symbol: "GME",
      rows: [],
    });
    vi.spyOn(api, "fetchInstrumentCandles").mockResolvedValue({
      symbol: "GME",
      range: "5y",
      days: 1825,
      rows: [],
    });

    renderAt("GME");

    expect(await screen.findByText(/No insider data/i)).toBeInTheDocument();
    expect(screen.queryByTestId("mock-net-by-month")).not.toBeInTheDocument();
  });

  it("renders error state when the transactions endpoint fails", async () => {
    vi.spyOn(api, "fetchInsiderTransactions").mockRejectedValue(
      new Error("boom"),
    );
    vi.spyOn(api, "fetchInstrumentCandles").mockResolvedValue({
      symbol: "GME",
      range: "5y",
      days: 1825,
      rows: [],
    });

    renderAt("GME");

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /Retry/i })).toBeInTheDocument();
    });
  });

  it("renders 'no SEC coverage' empty state when transactions returns 404", async () => {
    const { ApiError } = await import("@/api/client");
    vi.spyOn(api, "fetchInsiderTransactions").mockRejectedValue(
      new ApiError(404, "Instrument GME has no SEC coverage"),
    );
    vi.spyOn(api, "fetchInstrumentCandles").mockResolvedValue({
      symbol: "GME",
      range: "5y",
      days: 1825,
      rows: [],
    });

    renderAt("GME");

    await waitFor(() => {
      expect(screen.getByText(/No SEC Form 4 coverage/i)).toBeInTheDocument();
    });
    // No retry — 404 is terminal for this instrument
    expect(screen.queryByRole("button", { name: /^Retry$/i })).not.toBeInTheDocument();
  });

  it("keeps the rest of the page rendering when only candles fail", async () => {
    vi.spyOn(api, "fetchInsiderTransactions").mockResolvedValue({
      symbol: "GME",
      rows: [SAMPLE_TXN],
    });
    vi.spyOn(api, "fetchInstrumentCandles").mockRejectedValue(
      new Error("candles down"),
    );

    renderAt("GME");

    expect(await screen.findByTestId("mock-net-by-month")).toBeInTheDocument();
    expect(
      screen.getByText(/Price data unavailable/i),
    ).toBeInTheDocument();
    // Markers pane still renders with empty candles
    expect(screen.getByTestId("mock-price-markers")).toHaveTextContent(
      "price-markers 1/0",
    );
  });
});
