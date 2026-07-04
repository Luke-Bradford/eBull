/**
 * Tests for InstrumentTradesTable (#1899 slice 1 — per-trade rows on the
 * instrument Positions tab).
 */
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";

import { InstrumentTradesTable } from "@/components/instrument/InstrumentTradesTable";
import type { NativeTradeItem } from "@/api/types";

function trade(overrides: Partial<NativeTradeItem> = {}): NativeTradeItem {
  return {
    position_id: 1,
    is_buy: true,
    units: 10,
    amount: 1855.0,
    open_rate: 185.5,
    open_date_time: "2026-03-15T09:30:00Z",
    current_price: 190.0,
    market_value: 1900.0,
    unrealized_pnl: 45.0,
    stop_loss_rate: null,
    take_profit_rate: null,
    is_tsl_enabled: false,
    leverage: 1,
    total_fees: 1.25,
    ...overrides,
  };
}

describe("InstrumentTradesTable", () => {
  it("renders one row per trade with the open date and side", () => {
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[
          trade({ position_id: 1, is_buy: true }),
          trade({ position_id: 2, is_buy: false, open_date_time: "2026-04-01T14:00:00Z" }),
        ]}
      />,
    );
    // Header (Open trades (2)) plus a labelled table.
    expect(screen.getByText("Open trades (2)")).toBeInTheDocument();
    const rows = screen.getAllByRole("row");
    // header row + 2 data rows
    expect(rows).toHaveLength(3);
    expect(screen.getByText("2026-03-15")).toBeInTheDocument();
    expect(screen.getByText("2026-04-01")).toBeInTheDocument();
    expect(screen.getByText("Buy")).toBeInTheDocument();
    expect(screen.getByText("Sell")).toBeInTheDocument();
  });

  it("shows a signed, currency-formatted P&L", () => {
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[trade({ unrealized_pnl: 45 }), trade({ position_id: 2, unrealized_pnl: -12.5 })]}
      />,
    );
    const table = screen.getByRole("table");
    // formatMoney uses en-GB, so USD renders as "US$" (disambiguated).
    expect(within(table).getByText("+US$45.00")).toBeInTheDocument();
    expect(within(table).getByText("-US$12.50")).toBeInTheDocument();
  });

  it("renders a dash for a missing current price", () => {
    render(
      <InstrumentTradesTable currency="USD" trades={[trade({ current_price: null })]} />,
    );
    // formatMoney(null) → "—"
    expect(screen.getByRole("table").textContent).toContain("—");
  });

  it("renders nothing when there are no trades", () => {
    const { container } = render(
      <InstrumentTradesTable currency="USD" trades={[]} />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});
