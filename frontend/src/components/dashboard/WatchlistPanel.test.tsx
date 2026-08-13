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
    gics_sector: "Information Technology",
    added_at: "2026-04-15",
    notes: null,
    ...overrides,
  };
}

describe("WatchlistPanel", () => {
  it("prefers the GICS sector over the coarse eToro label (#1851)", () => {
    // AAPL's eToro label is "Consumer Goods" (wrong); the SEC-SIC GICS sector
    // is "Information Technology". The watchlist must agree with the instrument
    // page, which already prefers gics_sector.
    render(
      <MemoryRouter>
        <WatchlistPanel
          items={[item({ sector_name: "Consumer Goods", gics_sector: "Information Technology" })]}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("Information Technology")).toBeInTheDocument();
    expect(screen.queryByText("Consumer Goods")).not.toBeInTheDocument();
  });

  it("falls back to the eToro label when GICS is unavailable (#1851)", () => {
    // Non-SEC instrument: no SIC → no GICS. Show the eToro label, not "—".
    render(
      <MemoryRouter>
        <WatchlistPanel items={[item({ gics_sector: null, sector_name: "Technology" })]} />
      </MemoryRouter>,
    );
    expect(screen.getByText("Technology")).toBeInTheDocument();
    // The opaque eToro numeric id must never reach the operator (#1599).
    expect(screen.queryByText("8")).not.toBeInTheDocument();
  });

  it("falls back to em-dash when the sector is unmapped", () => {
    render(
      <MemoryRouter>
        <WatchlistPanel
          items={[item({ sector: null, sector_name: null, gics_sector: null })]}
        />
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
