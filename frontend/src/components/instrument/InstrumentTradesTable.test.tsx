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
    investment_type: null,
    is_underlying: null,
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

describe("InstrumentTradesTable — product identity (#2602 item 3)", () => {
  it("labels a real-asset position with the broker's own wording", () => {
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[trade({ investment_type: "Real Asset", is_underlying: true })]}
      />,
    );
    const badge = screen.getByText("Real Asset");
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveAttribute("title", expect.stringContaining("you own the underlying"));
  });

  it("labels a CFD as not the real asset held outright", () => {
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[trade({ investment_type: "CFD", is_underlying: false })]}
      />,
    );
    const badge = screen.getByText("CFD");
    expect(badge).toHaveAttribute("title", expect.stringContaining("not the real asset held outright"));
  });

  it("does not call Crypto MarginTrade a contract with the broker", () => {
    // Type 3 IS the real asset by eToro's own wording, held on margin — so it
    // is not underlying (the no-leverage posture bars it) but it is also not a
    // derivative, and the tooltip must not claim otherwise (Codex ckpt-2).
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[trade({ investment_type: "Crypto MarginTrade", is_underlying: false })]}
      />,
    );
    const title = screen.getByText("Crypto MarginTrade").getAttribute("title") ?? "";
    expect(title).toContain("not the real asset held outright");
    expect(title).not.toContain("contract with the broker");
  });

  it("renders an unreported type as Unknown rather than guessing a product", () => {
    // The distinction is load-bearing: "the broker told us nothing" must not
    // render as "this is a derivative", or the panel asserts a product identity
    // that was never observed.
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[trade({ investment_type: null, is_underlying: null })]}
      />,
    );
    expect(screen.getByText("Unknown")).toBeInTheDocument();
    expect(screen.queryByText("CFD")).not.toBeInTheDocument();
    expect(screen.queryByText("Real Asset")).not.toBeInTheDocument();
  });

  it("shows a per-row product, since two trades on one instrument can differ", () => {
    render(
      <InstrumentTradesTable
        currency="USD"
        trades={[
          trade({ position_id: 1, investment_type: "Real Asset", is_underlying: true }),
          trade({ position_id: 2, investment_type: "CFD", is_underlying: false }),
        ]}
      />,
    );
    expect(screen.getByText("Real Asset")).toBeInTheDocument();
    expect(screen.getByText("CFD")).toBeInTheDocument();
  });
});
