/**
 * Tests for PortfolioValueChart (#204). lightweight-charts is mocked
 * wholesale because jsdom can't paint Canvas — we pin the component's
 * contract (range picker, empty/error branches, series data shape)
 * not the library's rendering.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

const libState = vi.hoisted(() => ({
  setData: vi.fn(),
  fitContent: vi.fn(),
  remove: vi.fn(),
}));

vi.mock("lightweight-charts", () => {
  const series = { setData: libState.setData };
  const chart = {
    addSeries: vi.fn(() => series),
    priceScale: vi.fn(() => ({ applyOptions: vi.fn() })),
    timeScale: vi.fn(() => ({ fitContent: libState.fitContent })),
    subscribeCrosshairMove: vi.fn(),
    remove: libState.remove,
  };
  return {
    createChart: vi.fn(() => chart),
    AreaSeries: "__area__",
  };
});

import { PortfolioValueChart } from "@/components/dashboard/PortfolioValueChart";
import type { ValueHistoryResponse } from "@/api/types";

vi.mock("@/api/portfolio", () => ({ fetchValueHistory: vi.fn() }));

import { fetchValueHistory } from "@/api/portfolio";

const mocked = vi.mocked(fetchValueHistory);

function resp(
  points: ValueHistoryResponse["points"],
  overrides: Partial<ValueHistoryResponse> = {},
): ValueHistoryResponse {
  return {
    display_currency: "GBP",
    range: "1y",
    days: 365,
    fx_mode: "live",
    fx_skipped: 0,
    points,
    ...overrides,
  };
}

beforeEach(() => {
  mocked.mockReset();
  libState.setData.mockClear();
  libState.fitContent.mockClear();
  libState.remove.mockClear();
});

describe("PortfolioValueChart", () => {
  it("renders all six range buttons + an fx_mode caption on live", async () => {
    mocked.mockResolvedValue(resp([]));
    render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    for (const r of ["1m", "3m", "6m", "1y", "5y", "max"]) {
      expect(screen.getByTestId(`value-range-${r}`)).toBeInTheDocument();
    }
    // fx_mode caption only renders once data has loaded and reports "live".
    await waitFor(() => {
      expect(
        screen.getByText(/historical converted at today's FX/i),
      ).toBeInTheDocument();
    });
  });

  it("clicking a range refetches with the new range", async () => {
    mocked.mockResolvedValue(resp([]));
    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(mocked).toHaveBeenCalledWith("1y");
    });
    await user.click(screen.getByTestId("value-range-3m"));
    await waitFor(() => {
      expect(mocked).toHaveBeenLastCalledWith("3m");
    });
  });

  it("mounts chart + pushes ≥2 points to setData", async () => {
    mocked.mockResolvedValue(
      resp([
        { date: "2026-04-18", value: 1000 },
        { date: "2026-04-19", value: 1100 },
      ]),
    );
    render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("portfolio-value-chart")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(libState.setData).toHaveBeenCalled();
    });
    const call = libState.setData.mock.calls[0]?.[0] as Array<{ value: number }>;
    expect(call).toHaveLength(2);
    expect(call[0]?.value).toBe(1000);
    expect(call[1]?.value).toBe(1100);
  });

  it("renders empty state when fewer than two valid points", async () => {
    mocked.mockResolvedValue(resp([{ date: "2026-04-19", value: 1000 }]));
    render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByText(/No history yet/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("portfolio-value-chart")).not.toBeInTheDocument();
  });

  it("surfaces an 'FX rates missing' empty state when fx_skipped > 0", async () => {
    // All-skipped is indistinguishable from "no data" without fx_skipped;
    // the counter lets the operator know why their mixed-currency
    // portfolio is rendering empty (Codex round-1 finding).
    mocked.mockResolvedValue(resp([], { fx_skipped: 4 }));
    render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByText(/FX rates missing/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/4 native-currency rows/i)).toBeInTheDocument();
  });

  it("calls chart.remove() on unmount so Canvas is released", async () => {
    mocked.mockResolvedValue(
      resp([
        { date: "2026-04-18", value: 1000 },
        { date: "2026-04-19", value: 1100 },
      ]),
    );
    render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("portfolio-value-chart")).toBeInTheDocument();
    });
    cleanup();
    expect(libState.remove).toHaveBeenCalled();
  });

  it("silent-hides on fetch error — no blanking of the rest of the dashboard", async () => {
    mocked.mockRejectedValue(new Error("offline"));
    const { container } = render(
      <MemoryRouter>
        <PortfolioValueChart />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(mocked).toHaveBeenCalled();
    });
    // Whole widget renders null on error — no range buttons, no chart.
    await waitFor(() => {
      expect(container.querySelector('[data-testid^="value-range-"]')).toBeNull();
    });
    expect(
      container.querySelector('[data-testid="portfolio-value-chart"]'),
    ).toBeNull();
  });
});
