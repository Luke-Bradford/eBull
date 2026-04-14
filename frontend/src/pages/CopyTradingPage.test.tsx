/**
 * Tests for MirrorDetailPage (#221 — mirrors as positions).
 *
 * Scope:
 *   - Invalid mirror ID renders empty state
 *   - Renders trader avatar and username heading
 *   - Renders mirror stats (initial investment, deposits, etc.)
 *   - Positions grouped by instrument
 *   - Expand to see individual positions within a group
 *   - Empty positions shows message
 *   - Error state renders retry button
 *   - Back link to portfolio
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { CopyTradingPage } from "@/pages/CopyTradingPage";
import { fetchMirrorDetail } from "@/api/copyTrading";
import type { MirrorDetailResponse, MirrorPositionItem } from "@/api/types";

vi.mock("@/api/copyTrading", () => ({
  fetchMirrorDetail: vi.fn(),
}));

const mockedFetch = vi.mocked(fetchMirrorDetail);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makePosition(overrides: Partial<MirrorPositionItem> = {}): MirrorPositionItem {
  return {
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
    ...overrides,
  };
}

function makeDetailResponse(overrides: Partial<MirrorDetailResponse> = {}): MirrorDetailResponse {
  return {
    parent_username: "thomaspj",
    mirror: {
      mirror_id: 1001,
      active: true,
      initial_investment: 15000,
      deposit_summary: 2000,
      withdrawal_summary: 500,
      available_amount: 1000,
      closed_positions_net_profit: 200,
      mirror_equity: 14500,
      position_count: 3,
      positions: [
        makePosition({ position_id: 5001, instrument_id: 42, symbol: "AAPL", units: 10, open_rate: 150 }),
        makePosition({ position_id: 5002, instrument_id: 42, symbol: "AAPL", units: 5, open_rate: 155 }),
        makePosition({
          position_id: 5003,
          instrument_id: 99,
          symbol: "TSLA",
          company_name: "Tesla Inc.",
          units: 3,
          amount: 6000,
          open_rate: 200.0,
          current_price: null,
          market_value: 6000,
          unrealized_pnl: 0,
        }),
      ],
      started_copy_date: "2026-01-15T10:00:00Z",
      closed_at: null,
    },
    display_currency: "GBP",
    ...overrides,
  };
}

function renderPage(mirrorId: string = "1001") {
  return render(
    <MemoryRouter initialEntries={[`/copy-trading/${mirrorId}`]}>
      <Routes>
        <Route path="/copy-trading/:mirrorId" element={<CopyTradingPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

beforeEach(() => {
  mockedFetch.mockResolvedValue(makeDetailResponse());
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("MirrorDetailPage — header and navigation", () => {
  it("shows trader username in the heading", async () => {
    renderPage();
    expect(await screen.findByText("thomaspj")).toBeInTheDocument();
  });

  it("shows trader avatar with first initial", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    expect(screen.getByText("T")).toBeInTheDocument();
  });

  it("has a back link to the portfolio", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    const backLink = screen.getByText("← Portfolio");
    expect(backLink).toBeInTheDocument();
    expect(backLink.closest("a")).toHaveAttribute("href", "/portfolio");
  });
});

describe("MirrorDetailPage — mirror stats", () => {
  it("renders investment details", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    expect(screen.getByText("Initial investment:")).toBeInTheDocument();
    expect(screen.getByText("Deposits:")).toBeInTheDocument();
    expect(screen.getByText("Withdrawals:")).toBeInTheDocument();
    expect(screen.getByText("Available cash:")).toBeInTheDocument();
    expect(screen.getByText("Closed P&L:")).toBeInTheDocument();
    expect(screen.getByText("Copying since:")).toBeInTheDocument();
  });
});

describe("MirrorDetailPage — grouped positions", () => {
  it("groups positions by instrument", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    // Two instruments: AAPL (2 positions) and TSLA (1 position)
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("TSLA")).toBeInTheDocument();
  });

  it("shows position count per instrument group", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    // AAPL has 2 positions — find the row and check
    const rows = screen.getAllByRole("row");
    const aaplRow = rows.find((r) => within(r).queryByText("AAPL") !== null)!;
    expect(within(aaplRow).getByText("2")).toBeInTheDocument();
  });

  it("expands to show individual positions on click", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByText("thomaspj");

    // Before expansion: sub-position rows not visible
    expect(screen.queryByText("LONG")).toBeNull();

    // Click the AAPL group row (has 2 positions, so expandable)
    const rows = screen.getAllByRole("row");
    const aaplRow = rows.find((r) => within(r).queryByText("AAPL") !== null)!;
    await user.click(aaplRow);

    // After expansion: sub-position rows visible with LONG badges
    const longs = screen.getAllByText("LONG");
    expect(longs.length).toBe(2);
  });

  it("single-position instruments are not expandable", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    // TSLA has 1 position — row should not be clickable (no expand arrow)
    const rows = screen.getAllByRole("row");
    const tslaRow = rows.find((r) => within(r).queryByText("TSLA") !== null)!;
    // No expand indicator (▸ or ▾) in single-position row
    expect(within(tslaRow).queryByText("▸")).toBeNull();
    expect(within(tslaRow).queryByText("▾")).toBeNull();
  });

  it("shows — for instruments without current price", async () => {
    renderPage();
    await screen.findByText("thomaspj");
    const rows = screen.getAllByRole("row");
    const tslaRow = rows.find((r) => within(r).queryByText("TSLA") !== null)!;
    expect(within(tslaRow).getByText("—")).toBeInTheDocument();
  });
});

describe("MirrorDetailPage — empty positions", () => {
  it("shows empty message when mirror has no positions", async () => {
    mockedFetch.mockResolvedValueOnce(
      makeDetailResponse({
        mirror: {
          ...makeDetailResponse().mirror,
          positions: [],
          position_count: 0,
        },
      }),
    );
    renderPage();
    expect(await screen.findByText("No open positions in this mirror.")).toBeInTheDocument();
  });
});

describe("MirrorDetailPage — error state", () => {
  it("shows retry button on fetch failure", async () => {
    mockedFetch.mockRejectedValueOnce(new Error("network error"));
    renderPage();
    expect(await screen.findByRole("button", { name: /retry/i })).toBeInTheDocument();
  });
});

describe("MirrorDetailPage — invalid mirror", () => {
  it("shows invalid state for non-numeric ID", async () => {
    renderPage("abc");
    expect(await screen.findByText("Invalid mirror")).toBeInTheDocument();
  });
});
