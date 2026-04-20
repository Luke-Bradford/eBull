/**
 * Tests for PortfolioPage after the #324 unified drill-in revert.
 *
 * Behaviour pinned:
 *   - Position row click → navigates to /portfolio/:instrumentId.
 *   - Mirror row click   → navigates to /copy-trading/:mirrorId.
 *   - Row Add / Close buttons open modals without drilling.
 *   - Keyboard: `/` focuses search, `j`/`k` moves focus ring, Enter drills
 *     the focused row, Esc blurs + clears search.
 *   - Search + pagination still work and clamp correctly.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

import { PortfolioPage } from "@/pages/PortfolioPage";
import type {
  BrokerPositionItem,
  ConfigResponse,
  PortfolioMirrorItem,
  PortfolioResponse,
  PositionItem,
} from "@/api/types";

vi.mock("@/api/portfolio", () => ({
  fetchPortfolio: vi.fn(),
  fetchInstrumentPositions: vi.fn(),
}));
vi.mock("@/api/orders", () => ({ placeOrder: vi.fn(), closePosition: vi.fn() }));

import { TestConfigProvider } from "@/lib/ConfigContext";
import { fetchPortfolio, fetchInstrumentPositions } from "@/api/portfolio";

const mockedFetchPortfolio = vi.mocked(fetchPortfolio);
const mockedFetchInstrumentPositions = vi.mocked(fetchInstrumentPositions);

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

function trade(
  positionId: number,
  overrides: Partial<BrokerPositionItem> = {},
): BrokerPositionItem {
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

function mirror(
  mirrorId: number,
  parentUsername: string,
  overrides: Partial<PortfolioMirrorItem> = {},
): PortfolioMirrorItem {
  return {
    mirror_id: mirrorId,
    parent_username: parentUsername,
    active: true,
    funded: 1000,
    mirror_equity: 1200,
    unrealized_pnl: 200,
    position_count: 5,
    started_copy_date: "2026-01-01",
    ...overrides,
  };
}

function portfolioWith(
  positions: PositionItem[],
  mirrors: PortfolioMirrorItem[] = [],
): PortfolioResponse {
  return {
    positions,
    mirrors,
    position_count: positions.length,
    total_aum:
      positions.reduce((s, p) => s + p.market_value, 0) +
      mirrors.reduce((s, m) => s + m.mirror_equity, 0),
    cash_balance: 5000,
    mirror_equity: mirrors.reduce((s, m) => s + m.mirror_equity, 0),
    display_currency: "GBP",
    fx_rates_used: {},
  };
}

// Location probe rendered alongside PortfolioPage so tests can assert
// the current URL after a navigation without needing a real router.
function LocationProbe() {
  const loc = useLocation();
  return <div data-testid="location">{loc.pathname}</div>;
}

function renderPage() {
  return render(
    <TestConfigProvider value={{ data: demoConfig(), loading: false }}>
      <MemoryRouter initialEntries={["/portfolio"]}>
        <Routes>
          <Route
            path="/portfolio"
            element={
              <>
                <PortfolioPage />
                <LocationProbe />
              </>
            }
          />
          <Route path="/portfolio/:id" element={<LocationProbe />} />
          <Route path="/copy-trading/:id" element={<LocationProbe />} />
        </Routes>
      </MemoryRouter>
    </TestConfigProvider>,
  );
}

beforeEach(() => {
  // Modals fetch per-instrument detail — give them a default stub so
  // the promise chain resolves even when the modal never opens.
  mockedFetchInstrumentPositions.mockResolvedValue({
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
  });
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("PortfolioPage — unified drill-in", () => {
  it("position row click navigates to /portfolio/:instrumentId", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();

    const row = await screen.findByTestId("position-row-7");
    await user.click(row);

    await waitFor(() => {
      expect(screen.getByTestId("location").textContent).toBe("/portfolio/7");
    });
  });

  it("mirror row click navigates to /copy-trading/:mirrorId", async () => {
    mockedFetchPortfolio.mockResolvedValue(
      portfolioWith([], [mirror(42, "@gurutrader")]),
    );
    const user = userEvent.setup();
    renderPage();

    const row = await screen.findByTestId("mirror-row-42");
    await user.click(row);

    await waitFor(() => {
      expect(screen.getByTestId("location").textContent).toBe("/copy-trading/42");
    });
  });

  it("Add button on a row opens Add modal (does NOT drill)", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();

    await screen.findByTestId("position-row-7");
    await user.click(screen.getByRole("button", { name: /Add to AAPL/ }));

    expect(
      screen.getAllByRole("heading", { name: /Add — AAPL/i }).length,
    ).toBeGreaterThan(0);
    expect(screen.getByTestId("location").textContent).toBe("/portfolio");
  });

  it("Close button on a single-trade row opens Close modal", async () => {
    mockedFetchPortfolio.mockResolvedValue(portfolioWith([position(7, "AAPL")]));
    const user = userEvent.setup();
    renderPage();

    await screen.findByTestId("position-row-7");
    await user.click(screen.getByRole("button", { name: /Close AAPL/ }));

    await waitFor(() => {
      expect(screen.getAllByText(/Close — AAPL/i).length).toBeGreaterThan(0);
    });
    expect(screen.getByTestId("location").textContent).toBe("/portfolio");
  });
});

describe("PortfolioPage — keyboard", () => {
  beforeEach(() => {
    mockedFetchPortfolio.mockResolvedValue(
      portfolioWith([
        position(1, "AAA", { market_value: 300 }),
        position(2, "BBB", { market_value: 200 }),
        position(3, "CCC", { market_value: 100 }),
      ]),
    );
  });

  it("j / k moves the focus ring without a prior click", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("jj");
    expect(screen.getByTestId("position-row-3").className).toContain(
      "border-l-slate-400",
    );
  });

  it("/ focuses search without inserting a slash", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("/");
    const input = screen.getByLabelText("Search positions") as HTMLInputElement;
    expect(input).toHaveFocus();
    expect(input.value).toBe("");
  });

  it("Enter drills the focused row to /portfolio/:id", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("j{Enter}"); // focus second row (BBB → id 2), drill
    await waitFor(() => {
      expect(screen.getByTestId("location").textContent).toBe("/portfolio/2");
    });
  });

  it("Enter drills a focused mirror row to /copy-trading/:id", async () => {
    // Use `mockResolvedValueOnce` so only THIS test sees the mixed
    // fixture — subsequent keyboard tests keep the describe-level
    // three-position default (vi stacks `Once` queue first, falls
    // back to `mockResolvedValue` default for any extra calls, so
    // no `mockReset()` needed).
    mockedFetchPortfolio.mockResolvedValueOnce(
      portfolioWith(
        [position(1, "AAA", { market_value: 300 })],
        [mirror(42, "@gurutrader", { mirror_equity: 200 })],
      ),
    );
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("j{Enter}");
    await waitFor(() => {
      expect(screen.getByTestId("location").textContent).toBe("/copy-trading/42");
    });
  });

  it("Esc blurs and clears search input when focused", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    const input = screen.getByLabelText("Search positions") as HTMLInputElement;
    await user.click(input);
    await user.type(input, "AAA");
    expect(input.value).toBe("AAA");

    await user.keyboard("{Escape}");
    expect(input).not.toHaveFocus();
    expect(input.value).toBe("");
  });

  it("modifier keys do not trigger Enter (Ctrl+Enter is a no-op)", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByTestId("position-row-1");

    await user.keyboard("{Control>}{Enter}{/Control}");
    expect(screen.getByTestId("location").textContent).toBe("/portfolio");
  });
});

describe("PortfolioPage — search + pagination", () => {
  it("paginates when >50 positions exist", async () => {
    const positions = Array.from({ length: 51 }, (_, i) =>
      position(i + 1, `SYM${i + 1}`, { market_value: 1000 - i }),
    );
    mockedFetchPortfolio.mockResolvedValue(portfolioWith(positions));
    const user = userEvent.setup();
    renderPage();

    await screen.findByTestId("position-row-1");
    expect(screen.getAllByTestId(/^position-row-/).length).toBe(50);
    expect(screen.getByText(/Page 1 of 2/)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Next →/ }));
    await waitFor(() => {
      expect(screen.getByText(/Page 2 of 2/)).toBeInTheDocument();
    });
    expect(screen.getAllByTestId(/^position-row-/).length).toBe(1);
  });

  it("clamps page back when search shrinks results below current page", async () => {
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

    await user.type(screen.getByLabelText("Search positions"), "SYM50");
    await waitFor(() => {
      expect(screen.getByTestId("position-row-50")).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: /Next →/ })).not.toBeInTheDocument();
  });

  it("shows 'No positions match' when search filters to zero rows", async () => {
    mockedFetchPortfolio.mockResolvedValue(
      portfolioWith([position(7, "AAPL"), position(8, "MSFT")]),
    );
    const user = userEvent.setup();
    renderPage();

    await screen.findByTestId("position-row-7");
    await user.type(screen.getByLabelText("Search positions"), "ZZZZ");

    expect(
      screen.getByText(/No positions match your search/),
    ).toBeInTheDocument();
  });
});
