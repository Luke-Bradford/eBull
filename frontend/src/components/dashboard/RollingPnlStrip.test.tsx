import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import { RollingPnlStrip } from "@/components/dashboard/RollingPnlStrip";
import { STAT_ROW_GRID } from "@/components/dashboard/StatTile";
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
      llm_provider: "openai_compatible",
      llm_base_url: "http://localhost:11434/v1",
      llm_model_writer: "qwen3:14b",
      llm_model_critic: "qwen3:14b",
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

  it("lays its tiles on the SHARED stat-row grid so the hairlines align with the summary row above (#1908 PR-5)", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [
        { period: "1d", pnl: 150, pnl_pct: 0.015, coverage: 5 },
        { period: "1w", pnl: 850, pnl_pct: 0.082, coverage: 5 },
        { period: "1m", pnl: 1200, pnl_pct: 0.115, coverage: 5 },
      ],
    });
    const { container } = renderStrip();
    await waitFor(() => {
      expect(screen.getByTestId("rolling-pnl-1d")).toBeInTheDocument();
    });
    // jsdom has no layout, so assert the invariant that GUARANTEES alignment:
    // this row is on the same grid constant as SummaryCards. A 3-column grid
    // here would spread the tiles across the full width and break every
    // hairline out of alignment with the 4-column row above.
    const grid = container.querySelector(`.${CSS.escape("lg:grid-cols-4")}`);
    expect(grid).not.toBeNull();
    expect(grid?.className).toBe(STAT_ROW_GRID);
    expect(grid?.className).not.toContain("grid-cols-3");
  });

  it("renders a positive delta with a dark-mode tone partner, not a light-only text colour", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [{ period: "1d", pnl: 150, pnl_pct: 0.015, coverage: 5 }],
    });
    const { container } = renderStrip();
    await waitFor(() => {
      expect(screen.getByTestId("rolling-pnl-1d")).toBeInTheDocument();
    });
    const value = container.querySelector(".tabular-nums.font-semibold");
    expect(value?.className).toContain("text-emerald-600");
    expect(value?.className).toContain("dark:text-emerald-400");
  });

  it("tones the percentage with the value — it restates the same signal (review round 1)", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [{ period: "1w", pnl: -672.4, pnl_pct: -0.0137, coverage: 5 }],
    });
    renderStrip();
    const pct = await screen.findByText("-1.37%");
    expect(pct.className).toContain("text-red-600");
    expect(pct.className).toContain("dark:text-red-400");
    expect(pct.className).not.toContain("text-slate-500");
  });

  it("leaves the em-dash placeholder untoned — a missing percentage is not a signal (review round 2)", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [{ period: "1d", pnl: 150, pnl_pct: null, coverage: 0 }],
    });
    renderStrip();
    const dash = await screen.findByText("—");
    // The value beside it IS emerald (the pnl is genuinely positive); the
    // no-data placeholder must not borrow that colour.
    expect(dash.className).toContain("text-slate-500");
    expect(dash.className).not.toContain("text-emerald-600");
  });

  it("renders a zero delta muted, not at full headline strength (review round 1)", async () => {
    mocked.mockResolvedValue({
      display_currency: "GBP",
      periods: [{ period: "1m", pnl: 0, pnl_pct: 0, coverage: 5 }],
    });
    const { container } = renderStrip();
    await waitFor(() => {
      expect(screen.getByTestId("rolling-pnl-1m")).toBeInTheDocument();
    });
    const value = container.querySelector(".tabular-nums.font-semibold");
    expect(value?.className).toContain("text-slate-600");
    // Neither a direction nor the full-strength default reserved for
    // non-directional headline stats.
    expect(value?.className).not.toContain("text-emerald-600");
    expect(value?.className).not.toContain("text-slate-900");
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
