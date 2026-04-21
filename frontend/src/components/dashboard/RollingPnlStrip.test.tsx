import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import { RollingPnlStrip } from "@/components/dashboard/RollingPnlStrip";
import { DisplayCurrencyProvider } from "@/lib/DisplayCurrencyContext";
import { TestConfigProvider } from "@/lib/ConfigContext";
import type { ConfigResponse } from "@/api/types";

vi.mock("@/api/portfolio", () => ({ fetchRollingPnl: vi.fn() }));

import { fetchRollingPnl } from "@/api/portfolio";

const mocked = vi.mocked(fetchRollingPnl);

function cfg(): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "GBP",
      updated_at: "2026-04-21T00:00:00Z",
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

function renderStrip() {
  return render(
    <TestConfigProvider value={{ data: cfg(), loading: false }}>
      <DisplayCurrencyProvider>
        <RollingPnlStrip />
      </DisplayCurrencyProvider>
    </TestConfigProvider>,
  );
}

beforeEach(() => {
  mocked.mockReset();
});

describe("RollingPnlStrip", () => {
  it("renders three pills (1d / 1w / 1m) when data arrives", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [
        { period: "1d", pnl: 150, pnl_pct: 0.015, coverage: 5 },
        { period: "1w", pnl: 850, pnl_pct: 0.082, coverage: 5 },
        { period: "1m", pnl: 1200, pnl_pct: 0.115, coverage: 5 },
      ],
    });
    renderStrip();
    await waitFor(() => {
      expect(screen.getByTestId("rolling-pnl-1d")).toBeInTheDocument();
    });
    expect(screen.getByTestId("rolling-pnl-1w")).toBeInTheDocument();
    expect(screen.getByTestId("rolling-pnl-1m")).toBeInTheDocument();
  });

  it("renders '—' for pnl_pct when the server returned null", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [
        { period: "1d", pnl: 0, pnl_pct: null, coverage: 0 },
        { period: "1w", pnl: 0, pnl_pct: null, coverage: 0 },
        { period: "1m", pnl: 0, pnl_pct: null, coverage: 0 },
      ],
    });
    renderStrip();
    await waitFor(() => {
      expect(screen.getByTestId("rolling-pnl-1d")).toBeInTheDocument();
    });
    // All three pills show em-dash rather than NaN%.
    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(3);
  });

  it("hides the strip on fetch error", async () => {
    mocked.mockRejectedValue(new Error("offline"));
    const { container } = renderStrip();
    await waitFor(() => {
      expect(mocked).toHaveBeenCalled();
    });
    // Error path renders null — no pill testids present.
    await waitFor(() => {
      expect(container.querySelectorAll("[data-testid^='rolling-pnl-']").length).toBe(0);
    });
  });
});
