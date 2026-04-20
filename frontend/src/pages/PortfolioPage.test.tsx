/**
 * Tests for PortfolioPage after the #314 workstation refactor.
 *
 * Covers the spec behaviors: row selection drives the detail panel,
 * keyboard shortcuts work before any click, modals integrate, and
 * edge cases (stale selection after refetch, zero rows, page clamp)
 * all behave as the spec pins.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { ApiError } from "@/api/client";
import { PortfolioPage } from "@/pages/PortfolioPage";
import type {
  BrokerPositionItem,
  ConfigResponse,
  FilingsListResponse,
  InstrumentPositionDetail,
  PortfolioResponse,
  PositionItem,
  ScoreHistoryResponse,
  ThesisDetail,
} from "@/api/types";

vi.mock("@/api/portfolio", () => ({
  fetchPortfolio: vi.fn(),
  fetchInstrumentPositions: vi.fn(),
}));
vi.mock("@/api/theses", () => ({ fetchLatestThesis: vi.fn() }));
vi.mock("@/api/filings", () => ({ fetchFilings: vi.fn() }));
vi.mock("@/api/scoreHistory", () => ({ fetchScoreHistory: vi.fn() }));
vi.mock("@/api/orders", () => ({ placeOrder: vi.fn(), closePosition: vi.fn() }));

import { TestConfigProvider } from "@/lib/ConfigContext";
import { fetchPortfolio, fetchInstrumentPositions } from "@/api/portfolio";
import { fetchLatestThesis } from "@/api/theses";
import { fetchFilings } from "@/api/filings";
import { fetchScoreHistory } from "@/api/scoreHistory";
import { placeOrder, closePosition } from "@/api/orders";

const mockedFetchPortfolio = vi.mocked(fetchPortfolio);
const mockedFetchInstrumentPositions = vi.mocked(fetchInstrumentPositions);
const mockedFetchThesis = vi.mocked(fetchLatestThesis);
const mockedFetchFilings = vi.mocked(fetchFilings);
const mockedFetchScores = vi.mocked(fetchScoreHistory);
const mockedPlaceOrder = vi.mocked(placeOrder);
const mockedClosePosition = vi.mocked(closePosition);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function demoConfig(): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "GBP",
      updated_at: "2026-04-18T00:00:00Z",
      updated_by: "system",
      reason: "",
    },
    kill_switch: {
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    },
  };
}

function trade(positionId: number, overrides: Partial<BrokerPositionItem> = {}): BrokerPositionItem {
  return {
    position_id: positionId,
    is_buy: true,
    units: 2,
    amount: 260,
    open_rate: 130,
    open_date_time: "2026-01-01T10:00:00Z",
    current_price: 140,
    market_value: 280,
    unrealized_pnl: 20,
    stop_loss_rate: null,
    take_profit_rate: null,
    is_tsl_enabled: false,
    leverage: 1,
    total_fees: 0,
    ...overrides,
  };
}

function position(
  instrumentId: number,
  symbol: string,
  overrides: Partial<PositionItem> = {},
): PositionItem {
  return {
    instrument_id: instrumentId,
    symbol,
    company_name: `${symbol} Inc.`,
    open_date: "2026-01-01",
    avg_cost: 130,
    current_price: 140,
    current_units: 2,
    cost_basis: 260,
    market_value: 280,
    unrealized_pnl: 20,
    valuation_source: "quote",
    source: "broker",
    updated_at: "2026-04-18T00:00:00Z",
    trades: [trade(100 + instrumentId)],
    ...overrides,
  };
}

function portfolioWith(positions: PositionItem[]): PortfolioResponse {
  return {
    positions,
    mirrors: [],
    position_count: positions.length,
    total_aum: positions.reduce((s, p) => s + p.market_value, 0),
    cash_balance: 5000,
    mirror_equity: 0,
    display_currency: "GBP",
    fx_rates_used: {},
  };
}

function emptyThesis(): ThesisDetail {
  return {
    thesis_id: 1,
    instrument_id: 7,
    thesis_version: 1,
    thesis_type: "compounder",
    stance: "buy",
    confidence_score: 0.72,
    buy_zone_low: 120,
    buy_zone_high: 135,
    base_value: 150,
    bull_value: 180,
    bear_value: 100,
    break_conditions_json: null,
    memo_markdown: "Compounder thesis text.",
    critic_json: null,
    created_at: "2026-04-18T00:00:00Z",
  };
}

function emptyFilings(): FilingsListResponse {
  return {
    instrument_id: 7,
    symbol: "AAPL",
    items: [],
    total: 0,
    offset: 0,
    limit: 3,
  };
}

function emptyScores(): ScoreHistoryResponse {
  return { instrument_id: 7, items: [] };
}

function emptyInstrumentDetail(): InstrumentPositionDetail {
  return {
    instrument_id: 7,
    symbol: "AAPL",
    company_name: "AAPL Inc.",
    currency: "USD",
    current_price: 140,
    total_units: 2,
    avg_entry: 130,
    total_invested: 260,
    total_value: 280,
    total_pnl: 20,
    trades: [
      {
        position_id: 107,
        is_buy: true,
        units: 2,
        amount: 260,
        open_rate: 130,
        open_date_time: "2026-01-01T10:00:00Z",
        current_price: 140,
        market_value: 280,
        unrealized_pnl: 20,
        stop_loss_rate: null,
        take_profit_rate: null,
        is_tsl_enabled: false,
        leverage: 1,
        total_fees: 0,
      },
    ],
  };
}

function renderPage() {
  return render(
    <TestConfigProvider value={{ data: demoConfig(), loading: false }}>
      <MemoryRouter initialEntries={["/portfolio"]}>
        <PortfolioPage />
      </MemoryRouter>
    </TestConfigProvider>,
  );
}

beforeEach(() => {
  mockedFetchThesis.mockResolvedValue(emptyThesis());
  mockedFetchFilings.mockResolvedValue(emptyFilings());
  mockedFetchScores.mockResolvedValue(emptyScores());
  mockedFetchInstrumentPositions.mockResolvedValue(emptyInstrumentDetail());
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("PortfolioPage — detail panel + selection", () => {
  it("shows a placeholder in the detail panel until a row is clicked", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    renderPage();

    await waitFor(() => screen.getByText(/AAPL Inc\./));
    expect(
      screen.getByText(/Select a position to see its detail/i),
    ).toBeInTheDocument();
    // Detail-panel fetches have NOT fired yet.
    expect(mockedFetchThesis).not.toHaveBeenCalled();
    expect(mockedFetchFilings).not.toHaveBeenCalled();
    expect(mockedFetchScores).not.toHaveBeenCalled();
  });

  it("clicking a position row loads the detail panel and drives thesis/filings/scores", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();

    const row = await screen.findByTestId("position-row-7");
    await user.click(row);

    await waitFor(() => {
      expect(mockedFetchThesis).toHaveBeenCalledWith(7);
      expect(mockedFetchFilings).toHaveBeenCalledWith(7, 0, 3);
      expect(mockedFetchScores).toHaveBeenCalledWith(7, 5);
    });
    // Placeholder gone, detail panel content present.
    expect(
      screen.queryByText(/Select a position to see its detail/i),
    ).not.toBeInTheDocument();
  });
});

describe("PortfolioPage — keyboard shortcuts", () => {
  it("j / k move the focus ring without any prior click", async () => {
    // Distinct market_values give a stable sort: AAA=300 > BBB=200 > CCC=100.
    // Row ordering is deterministic so the focused-row assertion does
    // not rely on engine-specific equal-key sort behaviour.
    mockedFetchPortfolio.mockResolvedValue(
      portfolioWith([
        position(1, "AAA", { market_value: 300 }),
        position(2, "BBB", { market_value: 200 }),
        position(3, "CCC", { market_value: 100 }),
      ]),
    );
    const user = userEvent.setup();
    renderPage();

    await screen.findByTestId("position-row-1");

    // Initial focused index 0 = AAA. Press `j` twice → focus on CCC.
    await user.keyboard("jj");
    const cccRow = screen.getByTestId("position-row-3");
    expect(cccRow.className).toContain("border-l-slate-400");
  });

  it("/ focuses the search box without inserting a slash", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(1, "AAA")]));
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("/");
    const searchInput = screen.getByLabelText("Search positions");
    expect(searchInput).toHaveFocus();
    expect((searchInput as HTMLInputElement).value).toBe("");
  });

  it("Enter promotes focused row to selected", async () => {
    // Distinct market_values for deterministic sort order: AAA first, BBB second.
    mockedFetchPortfolio.mockResolvedValue(
      portfolioWith([
        position(1, "AAA", { market_value: 200 }),
        position(2, "BBB", { market_value: 100 }),
      ]),
    );
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("j{Enter}");
    // Selected should be instrument 2; fetches fire for 2.
    await waitFor(() => {
      expect(mockedFetchThesis).toHaveBeenCalledWith(2);
    });
  });

  it("Esc clears selection when the search box is not focused", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-7");

    await user.click(screen.getByTestId("position-row-7"));
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalled());

    await user.keyboard("{Escape}");
    expect(
      await screen.findByText(/Select a position to see its detail/i),
    ).toBeInTheDocument();
  });

  it("Esc blurs and clears the search input when focused", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(1, "AAA")]));
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    const searchInput = screen.getByLabelText("Search positions") as HTMLInputElement;
    await user.click(searchInput);
    await user.type(searchInput, "APPL");
    expect(searchInput.value).toBe("APPL");
    await user.keyboard("{Escape}");
    expect(searchInput).not.toHaveFocus();
    expect(searchInput.value).toBe("");
  });

  it("b opens the Add modal for the selected position", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    mockedPlaceOrder.mockResolvedValue({
      order_id: 1,
      status: "filled",
      broker_order_ref: "DEMO-7-ADD",
      filled_price: 140,
      filled_units: 1,
      fees: 0,
      explanation: "Demo ADD",
    });
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalled());

    await user.keyboard("b");
    await waitFor(() => {
      expect(screen.getAllByRole("heading", { name: /Add — AAPL/i }).length).toBeGreaterThan(0);
    });
  });

  it("c opens the Close modal when the selected position has a single trade", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalled());

    await user.keyboard("c");
    await waitFor(() => {
      // The close modal uses `Close — {symbol}` heading.
      expect(screen.getAllByText(/Close — AAPL/i).length).toBeGreaterThan(0);
    });
  });

  it("c on a multi-trade position renders the hint and does NOT open a modal", async () => {
    const multi = position(7, "AAPL", {
      trades: [trade(100), trade(101)],
    });
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([multi]));
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalled());

    await user.keyboard("c");
    expect(
      screen.getByText(
        /Close requires a single broker position — use the detail panel/i,
      ),
    ).toBeInTheDocument();
    // No close modal heading was rendered.
    expect(screen.queryByText(/^Close —/)).not.toBeInTheDocument();
  });

  it("modifier keys do not trigger shortcuts (Ctrl+b is a no-op)", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalled());

    await user.keyboard("{Control>}b{/Control}");
    expect(screen.queryByText(/^Add — AAPL$/)).not.toBeInTheDocument();
  });
});

describe("PortfolioPage — stale selection after refetch", () => {
  it("clears selectedId when the refetch removes the instrument; b becomes a no-op", async () => {
    mockedFetchPortfolio.mockResolvedValueOnce(
      portfolioWith([position(7, "AAPL"), position(8, "MSFT")]),
    );
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalledWith(7));

    // Simulate a refetch that drops instrument 7 (e.g. fully closed).
    // handleFilled is called from inside a modal on success, which
    // triggers portfolio.refetch; we replay the same effect by swapping
    // the next resolved value and firing refetch via a new render.
    // Simplest: dispatch a keydown that fires refetch indirectly is not
    // available in the public surface. So we trigger the close modal
    // flow end-to-end: click Close, full close, resolve, let refetch
    // pick up the new response.
    mockedClosePosition.mockResolvedValueOnce({
      order_id: 1,
      status: "filled",
      broker_order_ref: "DEMO-7-EXIT",
      filled_price: 140,
      filled_units: 2,
      fees: 0,
      explanation: "Demo EXIT",
    });
    mockedFetchPortfolio.mockResolvedValueOnce(
      portfolioWith([position(8, "MSFT")]),
    );

    await user.keyboard("c");
    await waitFor(() =>
      expect(screen.getAllByText(/Close — AAPL/i).length).toBeGreaterThan(0),
    );
    await user.click(
      screen.getByRole("button", { name: /^Close position$/ }),
    );

    await waitFor(() => {
      expect(
        screen.queryByText(/^Close — AAPL/),
      ).not.toBeInTheDocument();
    });
    await waitFor(() => {
      // After refetch, the placeholder returns — selection cleared.
      expect(
        screen.getByText(/Select a position to see its detail/i),
      ).toBeInTheDocument();
    });

    // Pressing `b` now is a no-op (no modal opens).
    await user.keyboard("b");
    expect(screen.queryByText(/^Add — /)).not.toBeInTheDocument();
  });
});

describe("PortfolioPage — search + pagination edge cases", () => {
  it("shows 'No positions match' when search filters to zero rows but keeps the detail panel populated", async () => {
    mockedFetchPortfolio.mockResolvedValue(
      portfolioWith([position(7, "AAPL"), position(8, "MSFT")]),
    );
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);
    await waitFor(() => expect(mockedFetchThesis).toHaveBeenCalledWith(7));

    const searchInput = screen.getByLabelText("Search positions");
    await user.type(searchInput, "ZZZZ");

    expect(screen.getByText(/No positions match your search/)).toBeInTheDocument();
    // Detail panel still populated for AAPL.
    expect(
      screen.queryByText(/Select a position to see its detail/i),
    ).not.toBeInTheDocument();
  });

  it("paginates when >50 positions exist", async () => {
    // Distinct market_values guarantee deterministic sort order so
    // the 51st position lands deterministically on page 2 regardless
    // of engine sort stability for equal keys.
    const positions = Array.from({ length: 51 }, (_, i) =>
      position(i + 1, `SYM${i + 1}`, { market_value: 1000 - i }),
    );
    mockedFetchPortfolio.mockResolvedValue(portfolioWith(positions));
    const user = userEvent.setup();
    renderPage();

    await screen.findByTestId("position-row-1");
    // Page 1 shows 50 rows; page 2 shows 1.
    const rowsPage1 = await screen.findAllByTestId(/^position-row-/);
    expect(rowsPage1.length).toBe(50);

    expect(screen.getByText(/Page 1 of 2/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Next →/ }));
    await waitFor(() => {
      expect(screen.getByText(/Page 2 of 2/)).toBeInTheDocument();
    });
    const rowsPage2 = screen.getAllByTestId(/^position-row-/);
    expect(rowsPage2.length).toBe(1);
  });

  it("clamps page back when search shrinks results below the current page", async () => {
    const positions = Array.from({ length: 51 }, (_, i) =>
      position(i + 1, `SYM${i + 1}`, { market_value: 1000 - i }),
    );
    mockedFetchPortfolio.mockResolvedValue(portfolioWith(positions));
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");
    await user.click(screen.getByRole("button", { name: /Next →/ }));
    await waitFor(() =>
      expect(screen.getByText(/Page 2 of 2/)).toBeInTheDocument(),
    );

    // Type a search that matches < PAGE_SIZE rows — pagination bar
    // disappears (no longer needed) and the remaining rows render
    // (they would be on page 1 after the clamp; a stale page=2
    // without the clamp would leave the list empty).
    await user.type(screen.getByLabelText("Search positions"), "SYM50");
    await waitFor(() => {
      expect(screen.getByTestId("position-row-50")).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: /Next →/ })).not.toBeInTheDocument();
  });
});

describe("PortfolioPage — detail panel error handling", () => {
  it("thesis 404 renders empty state instead of error", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    mockedFetchThesis.mockRejectedValue(new ApiError(404, "No thesis"));
    const user = userEvent.setup();
    renderPage();
    const row = await screen.findByTestId("position-row-7");
    await user.click(row);

    await waitFor(() => {
      expect(screen.getByText(/No thesis yet/i)).toBeInTheDocument();
    });
    expect(screen.queryByText(/Failed to load/i)).not.toBeInTheDocument();
  });
});
