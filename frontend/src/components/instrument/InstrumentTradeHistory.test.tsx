/**
 * Tests for InstrumentTradeHistory (#1926 slice 2 — closed round-trips on the
 * instrument Positions tab). The component fetches the trade ledger scoped to
 * one instrument, so `fetchActivity` is mocked.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";

import { InstrumentTradeHistory } from "@/components/instrument/InstrumentTradeHistory";
import { fetchActivity } from "@/api/portfolio";
import type { ActivityEventItem, ActivityResponse } from "@/api/types";

vi.mock("@/api/portfolio");

function closeEvent(overrides: Partial<ActivityEventItem> = {}): ActivityEventItem {
  return {
    event_id: 1,
    position_id: 100,
    event_kind: "close",
    side: "sell",
    symbol: "AAPL",
    etoro_instrument_id: 1,
    units: 82.14,
    price: 120.56,
    executed_at: "2025-11-14T19:24:35Z",
    fees: 0,
    realized_pnl: 1490.17,
    holding_period_days: 94.1,
    source: "etoro_history",
    is_mirror: false,
    ...overrides,
  };
}

function response(
  events: ActivityEventItem[],
  displayCurrency = "GBP",
  total?: number,
): ActivityResponse {
  return {
    events,
    total: total ?? events.length,
    include_mirrors: false,
    display_currency: displayCurrency,
  };
}

describe("InstrumentTradeHistory", () => {
  beforeEach(() => {
    vi.mocked(fetchActivity).mockReset();
  });

  it("renders one row per close with date, held days, and signed display-currency P&L", async () => {
    vi.mocked(fetchActivity).mockResolvedValue(
      response([
        closeEvent({ event_id: 1 }),
        closeEvent({
          event_id: 2,
          executed_at: "2025-10-01T10:00:00Z",
          price: 100.0,
          realized_pnl: -50.5,
          holding_period_days: 3.4,
        }),
      ]),
    );
    render(<InstrumentTradeHistory instrumentId={4077} currency="USD" />);

    const heading = await screen.findByText("Trade history (2)");
    expect(heading).toBeInTheDocument();
    const table = screen.getByRole("table");
    expect(screen.getByText("14 Nov 2025")).toBeInTheDocument();
    expect(screen.getByText("01 Oct 2025")).toBeInTheDocument();
    // Exit price is native (USD → "US$"); realised P&L is display (GBP → "£").
    expect(within(table).getByText("US$120.56")).toBeInTheDocument();
    expect(within(table).getByText("+£1,490.17")).toBeInTheDocument();
    expect(within(table).getByText("-£50.50")).toBeInTheDocument();
    expect(within(table).getByText("94d")).toBeInTheDocument();
    expect(within(table).getByText("3d")).toBeInTheDocument();
  });

  it("filters out open events — only closed round-trips render", async () => {
    vi.mocked(fetchActivity).mockResolvedValue(
      response([
        closeEvent({ event_id: 1 }),
        closeEvent({ event_id: 2, event_kind: "open", side: "buy", realized_pnl: null }),
      ]),
    );
    render(<InstrumentTradeHistory instrumentId={4077} currency="USD" />);

    await screen.findByText("Trade history (1)");
    // header row + 1 close row only
    expect(screen.getAllByRole("row")).toHaveLength(2);
  });

  it("scopes the fetch to the instrument, excluding mirrors, at the max page", async () => {
    vi.mocked(fetchActivity).mockResolvedValue(response([closeEvent()]));
    render(<InstrumentTradeHistory instrumentId={4077} currency="USD" />);
    await waitFor(() => expect(fetchActivity).toHaveBeenCalledWith(false, 4077, 500));
  });

  it("surfaces a truncation hint when the ledger page omits older rows", async () => {
    // total (600) exceeds the returned events (1) → some rows were dropped.
    vi.mocked(fetchActivity).mockResolvedValue(response([closeEvent()], "GBP", 600));
    render(<InstrumentTradeHistory instrumentId={4077} currency="USD" />);
    expect(
      await screen.findByText(/older trades may be omitted/i),
    ).toBeInTheDocument();
  });

  it("shows no truncation hint when the full history fits", async () => {
    vi.mocked(fetchActivity).mockResolvedValue(response([closeEvent()]));
    render(<InstrumentTradeHistory instrumentId={4077} currency="USD" />);
    await screen.findByText("Trade history (1)");
    expect(screen.queryByText(/older trades may be omitted/i)).toBeNull();
  });

  it("renders nothing when there are no closed trades", async () => {
    vi.mocked(fetchActivity).mockResolvedValue(response([]));
    const { container } = render(
      <InstrumentTradeHistory instrumentId={4077} currency="USD" />,
    );
    await waitFor(() => expect(fetchActivity).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it("shows a muted line on error", async () => {
    vi.mocked(fetchActivity).mockRejectedValue(new Error("boom"));
    render(<InstrumentTradeHistory instrumentId={4077} currency="USD" />);
    expect(await screen.findByText("Trade history unavailable.")).toBeInTheDocument();
  });
});
