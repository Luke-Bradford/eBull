import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { WatchlistItem } from "@/api/watchlist";
import { WatchlistPanel } from "@/components/dashboard/WatchlistPanel";

function item(overrides: Partial<WatchlistItem> = {}): WatchlistItem {
  return {
    instrument_id: 1,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    exchange: "NMS",
    currency: "USD",
    sector: "8",
    sector_name: "Technology",
    added_at: "2026-04-15",
    notes: null,
    ...overrides,
  };
}

describe("WatchlistPanel", () => {
  it("renders the resolved sector name, never the raw eToro id (#1599)", () => {
    render(
      <MemoryRouter>
        <WatchlistPanel items={[item()]} />
      </MemoryRouter>,
    );
    expect(screen.getByText("Technology")).toBeInTheDocument();
    // The opaque eToro numeric id must never reach the operator.
    expect(screen.queryByText("8")).not.toBeInTheDocument();
  });

  it("falls back to em-dash when the sector is unmapped", () => {
    render(
      <MemoryRouter>
        <WatchlistPanel items={[item({ sector: null, sector_name: null })]} />
      </MemoryRouter>,
    );
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("shows the empty state with no items", () => {
    render(
      <MemoryRouter>
        <WatchlistPanel items={[]} />
      </MemoryRouter>,
    );
    expect(screen.getByText("Watchlist empty")).toBeInTheDocument();
  });
});
