/**
 * Render-state tests for the Activity tab (#1593 PR-2): loading / error /
 * empty / table, mirror-toggle refetch param, symbol fallback, and the
 * USD money + holding-period rendering — the state contract per
 * loading-error-empty-states.md.
 */

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ActivityEventItem, ActivityResponse } from "@/api/types";

import { ActivitySection } from "./ActivitySection";

vi.mock("@/api/portfolio", () => ({
  fetchActivity: vi.fn(),
}));

import { fetchActivity } from "@/api/portfolio";

const fetchMock = vi.mocked(fetchActivity);

afterEach(() => {
  vi.restoreAllMocks();
  fetchMock.mockReset();
});

function event(over: Partial<ActivityEventItem> = {}): ActivityEventItem {
  return {
    event_id: 9,
    position_id: 3308442654,
    event_kind: "close",
    side: "sell",
    symbol: "ILMN",
    etoro_instrument_id: 4077,
    units: 82.135523,
    price: 120.56,
    executed_at: "2025-11-14T19:24:35.307Z",
    fees_usd: 0,
    realized_pnl_usd: 1910.47,
    holding_period_days: 94.1,
    source: "etoro_history",
    is_mirror: false,
    ...over,
  };
}

function response(events: ActivityEventItem[], total?: number): ActivityResponse {
  return { events, total: total ?? events.length, include_mirrors: false };
}

describe("ActivitySection states", () => {
  it("shows the skeleton while loading", () => {
    fetchMock.mockReturnValue(new Promise(() => {}));
    render(<ActivitySection />);
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("shows the error surface with retry on failure", async () => {
    fetchMock.mockRejectedValue(new Error("boom"));
    render(<ActivitySection />);
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument();
    });
    // Fixed phrase only — never exception text in the DOM.
    expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
  });

  it("shows the empty state with the next-action hint", async () => {
    fetchMock.mockResolvedValue(response([]));
    render(<ActivitySection />);
    await waitFor(() => {
      expect(screen.getByText("No trade activity yet")).toBeInTheDocument();
    });
    expect(screen.getByText(/portfolio sync/)).toBeInTheDocument();
  });

  it("renders a close row with USD P&L, side pill and holding period", async () => {
    fetchMock.mockResolvedValue(response([event()]));
    render(<ActivitySection />);
    await waitFor(() => {
      expect(screen.getByText("ILMN")).toBeInTheDocument();
    });
    expect(screen.getByText("SELL")).toBeInTheDocument();
    // en-GB locale renders USD as "US$…"
    expect(screen.getByText("US$1,910.47")).toBeInTheDocument();
    expect(screen.getByText("94 d")).toBeInTheDocument();
  });

  it("falls back to #etoro_id when the symbol is unresolved", async () => {
    fetchMock.mockResolvedValue(response([event({ symbol: null })]));
    render(<ActivitySection />);
    await waitFor(() => {
      expect(screen.getByText("#4077")).toBeInTheDocument();
    });
  });

  it("defaults to own activity and refetches with mirrors on toggle", async () => {
    fetchMock.mockResolvedValue(response([event()]));
    render(<ActivitySection />);
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(false);
    });
    await userEvent.click(screen.getByLabelText(/include copy-trading/i));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(true);
    });
  });

  it("flags mirror rows and shows the capped-count note", async () => {
    fetchMock.mockResolvedValue(response([event({ is_mirror: true })], 250));
    render(<ActivitySection />);
    await waitFor(() => {
      expect(screen.getByText("mirror")).toBeInTheDocument();
    });
    expect(screen.getByText(/showing 1 of 250/)).toBeInTheDocument();
  });
});
