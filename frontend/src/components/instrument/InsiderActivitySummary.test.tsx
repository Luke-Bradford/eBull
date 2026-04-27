import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { InsiderActivitySummary } from "@/components/instrument/InsiderActivitySummary";
import * as api from "@/api/instruments";

const payload = {
  symbol: "GME",
  open_market_net_shares_90d: "999999",
  open_market_buy_count_90d: 99,
  open_market_sell_count_90d: 99,
  total_acquired_shares_90d: "42392",
  total_disposed_shares_90d: "18331",
  acquisition_count_90d: 22,
  disposition_count_90d: 16,
  unique_filers_90d: 12,
  latest_txn_date: "2026-04-13",
  net_shares_90d: "999999",
  buy_count_90d: 99,
  sell_count_90d: 99,
};

describe("InsiderActivitySummary", () => {
  it("renders NET 90d, ACQUIRED, DISPOSED, TXNS, LATEST from total-activity lens", async () => {
    vi.spyOn(api, "fetchInsiderSummary").mockResolvedValue(payload);
    render(
      <MemoryRouter>
        <InsiderActivitySummary symbol="GME" />
      </MemoryRouter>,
    );
    // NET 90d = 42392 - 18331 = 24061 (positive → leading +)
    expect(await screen.findByText(/\+24,?061/)).toBeInTheDocument();
    expect(screen.getByText(/42,?392/)).toBeInTheDocument();
    expect(screen.getByText(/18,?331/)).toBeInTheDocument();
    // TXNS = acquisition_count_90d + disposition_count_90d = 22 + 16 = 38
    expect(screen.getByText("38")).toBeInTheDocument();
    expect(screen.getByText("2026-04-13")).toBeInTheDocument();
  });

  it("renders Pane chrome with scope and source", async () => {
    vi.spyOn(api, "fetchInsiderSummary").mockResolvedValue(payload);
    render(
      <MemoryRouter>
        <InsiderActivitySummary symbol="GME" />
      </MemoryRouter>,
    );
    // After data loads, chrome shows scope and source
    expect(await screen.findByText("last 90 days")).toBeInTheDocument();
    expect(screen.getByText(/SEC Form 4/)).toBeInTheDocument();
  });

  it("renders negative NET with leading minus when disposed > acquired", async () => {
    vi.spyOn(api, "fetchInsiderSummary").mockResolvedValue({
      ...payload,
      total_acquired_shares_90d: "10000",
      total_disposed_shares_90d: "30000",
    });
    render(
      <MemoryRouter>
        <InsiderActivitySummary symbol="GME" />
      </MemoryRouter>,
    );
    // NET = 10000 - 30000 = -20000
    expect(await screen.findByText(/-20,?000/)).toBeInTheDocument();
  });
});
