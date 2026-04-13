/**
 * Tests for CopyTradingPage (#188).
 *
 * Scope:
 *   - Empty state when no traders
 *   - Renders trader cards with equity and P&L
 *   - Expanding a card shows nested positions
 *   - Closed mirrors appear in a separate section
 *   - Error state renders retry button
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { CopyTradingPage } from "@/pages/CopyTradingPage";
import { fetchCopyTrading } from "@/api/copyTrading";
import type { CopyTradingResponse } from "@/api/types";

vi.mock("@/api/copyTrading", () => ({
  fetchCopyTrading: vi.fn(),
}));

const mockedFetch = vi.mocked(fetchCopyTrading);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const emptyResponse: CopyTradingResponse = {
  traders: [],
  total_mirror_equity: 0,
  display_currency: "GBP",
};

function makeResponse(overrides: Partial<CopyTradingResponse> = {}): CopyTradingResponse {
  return {
    traders: [
      {
        parent_cid: 123,
        parent_username: "thomaspj",
        mirrors: [
          {
            mirror_id: 1001,
            active: true,
            initial_investment: 15000,
            deposit_summary: 0,
            withdrawal_summary: 0,
            available_amount: 1000,
            closed_positions_net_profit: 200,
            mirror_equity: 14500,
            position_count: 2,
            positions: [
              {
                position_id: 5001,
                instrument_id: 42,
                symbol: "AAPL",
                company_name: "Apple Inc.",
                is_buy: true,
                units: 10,
                amount: 7000,
                open_rate: 150.0,
                open_conversion_rate: 1.0,
                open_date_time: "2026-03-01T12:00:00Z",
                current_price: 160.0,
                market_value: 7500,
                unrealized_pnl: 500,
              },
              {
                position_id: 5002,
                instrument_id: 99,
                symbol: "TSLA",
                company_name: "Tesla Inc.",
                is_buy: true,
                units: 5,
                amount: 6000,
                open_rate: 200.0,
                open_conversion_rate: 1.0,
                open_date_time: "2026-03-15T12:00:00Z",
                current_price: null,
                market_value: 6000,
                unrealized_pnl: 0,
              },
            ],
            started_copy_date: "2026-01-15T10:00:00Z",
            closed_at: null,
          },
        ],
        total_equity: 14500,
      },
    ],
    total_mirror_equity: 14500,
    display_currency: "GBP",
    ...overrides,
  };
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={["/copy-trading"]}>
      <Routes>
        <Route path="/copy-trading" element={<CopyTradingPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockedFetch.mockResolvedValue(makeResponse());
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CopyTradingPage — empty state", () => {
  it("shows empty state when no traders", async () => {
    mockedFetch.mockResolvedValueOnce(emptyResponse);
    renderPage();
    expect(await screen.findByText("No copy traders")).toBeInTheDocument();
  });
});

describe("CopyTradingPage — trader cards", () => {
  it("renders trader username and position count", async () => {
    renderPage();
    expect(await screen.findByText("thomaspj")).toBeInTheDocument();
    expect(screen.getByText(/2 positions/)).toBeInTheDocument();
  });

  it("shows mirror equity in summary", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    expect(screen.getByText("Mirror equity")).toBeInTheDocument();
  });

  it("shows copied traders count", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    expect(screen.getByText("Copied traders")).toBeInTheDocument();
    expect(screen.getByText("1")).toBeInTheDocument();
  });
});

describe("CopyTradingPage — drill-down", () => {
  it("expands to show positions on click", async () => {
    const user = userEvent.setup();
    renderPage();
    const card = await screen.findByText("thomaspj");

    // Positions not visible before expansion
    expect(screen.queryByText("AAPL")).toBeNull();

    // Click to expand
    await user.click(card);

    // Positions now visible
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("TSLA")).toBeInTheDocument();
  });

  it("shows LONG pill for buy positions", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(await screen.findByText("thomaspj"));

    const longs = screen.getAllByText("LONG");
    expect(longs.length).toBe(2);
  });

  it("shows — for positions without current price", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(await screen.findByText("thomaspj"));

    // TSLA has current_price: null, should show em-dash
    const rows = screen.getAllByRole("row");
    // Find the TSLA row
    const tslaRow = rows.find((r) => within(r).queryByText("TSLA") !== null)!;
    expect(within(tslaRow).getByText("—")).toBeInTheDocument();
  });
});

describe("CopyTradingPage — closed mirrors", () => {
  it("renders closed mirrors in a separate section", async () => {
    mockedFetch.mockResolvedValueOnce(
      makeResponse({
        traders: [
          {
            parent_cid: 456,
            parent_username: "closedtrader",
            mirrors: [
              {
                mirror_id: 2001,
                active: false,
                initial_investment: 5000,
                deposit_summary: 0,
                withdrawal_summary: 0,
                available_amount: 0,
                closed_positions_net_profit: -200,
                mirror_equity: 4800,
                position_count: 0,
                positions: [],
                started_copy_date: "2025-06-01T10:00:00Z",
                closed_at: "2026-01-01T10:00:00Z",
              },
            ],
            total_equity: 4800,
          },
        ],
      }),
    );
    renderPage();
    expect(await screen.findByText("Closed mirrors")).toBeInTheDocument();
    expect(screen.getByText("closedtrader")).toBeInTheDocument();
  });

  it("does not show closed section when all mirrors are active", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    expect(screen.queryByText("Closed mirrors")).toBeNull();
  });
});

describe("CopyTradingPage — error state", () => {
  it("shows retry button on fetch failure", async () => {
    mockedFetch.mockRejectedValueOnce(new Error("network error"));
    renderPage();
    expect(await screen.findByRole("button", { name: /retry/i })).toBeInTheDocument();
  });
});
