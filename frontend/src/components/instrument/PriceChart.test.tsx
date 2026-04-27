/**
 * Tests for PriceChart (#204 lightweight-charts migration; polished in
 * #587; intraday + 1D/5D/YTD ranges + unified fetch in #601).
 *
 * lightweight-charts renders to a Canvas that jsdom cannot paint, so
 * we mock the library wholesale. What we pin here is the component's
 * contract — not the library's rendering:
 *
 *   - All nine range buttons render (1D/5D/1M/3M/6M/YTD/1Y/5Y/MAX).
 *   - Switching range refetches via the unified chartData dispatch.
 *   - Type toggle (candle/line/area) flips visibility per series.
 *   - Log scale toggle URL-syncs to ?scale=log and applies mode=1.
 *   - Empty / single-row data → empty state, no chart mount.
 *   - ≥2 valid rows → chart canvas mounts and series.setData fires.
 *   - chart.remove() runs on unmount.
 *   - Fetch errors propagate via SectionError + retry.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation } from "react-router-dom";

const libState = vi.hoisted(() => ({
  candleSetData: vi.fn(),
  lineSetData: vi.fn(),
  areaSetData: vi.fn(),
  volumeSetData: vi.fn(),
  candleApply: vi.fn(),
  lineApply: vi.fn(),
  areaApply: vi.fn(),
  rightPriceScaleApply: vi.fn(),
  volumePriceScaleApply: vi.fn(),
  timeScaleApply: vi.fn(),
  fitContent: vi.fn(),
  crosshairHandlers: [] as Array<(p: unknown) => void>,
  remove: vi.fn(),
}));

vi.mock("lightweight-charts", () => {
  const candleSeries = {
    setData: libState.candleSetData,
    applyOptions: libState.candleApply,
  };
  const lineSeries = {
    setData: libState.lineSetData,
    applyOptions: libState.lineApply,
  };
  const areaSeries = {
    setData: libState.areaSetData,
    applyOptions: libState.areaApply,
  };
  const volumeSeries = {
    setData: libState.volumeSetData,
    applyOptions: vi.fn(),
  };
  const chart = {
    addSeries: vi.fn((seriesDef: unknown) => {
      switch (seriesDef) {
        case "__candlestick__":
          return candleSeries;
        case "__line__":
          return lineSeries;
        case "__area__":
          return areaSeries;
        default:
          return volumeSeries;
      }
    }),
    priceScale: vi.fn((id: string) =>
      id === "right"
        ? { applyOptions: libState.rightPriceScaleApply }
        : { applyOptions: libState.volumePriceScaleApply },
    ),
    timeScale: vi.fn(() => ({
      fitContent: libState.fitContent,
      applyOptions: libState.timeScaleApply,
    })),
    subscribeCrosshairMove: vi.fn((h: (p: unknown) => void) => {
      libState.crosshairHandlers.push(h);
    }),
    remove: libState.remove,
  };
  return {
    createChart: vi.fn(() => chart),
    CandlestickSeries: "__candlestick__",
    LineSeries: "__line__",
    AreaSeries: "__area__",
    HistogramSeries: "__histogram__",
    LineType: { Simple: 0, WithSteps: 1, Curved: 2 },
  };
});

import { PriceChart } from "@/components/instrument/PriceChart";
import type { NormalisedChartCandles, NormalisedBar } from "@/lib/chartData";

vi.mock("@/lib/chartData", async () => {
  const actual = await vi.importActual<typeof import("@/lib/chartData")>("@/lib/chartData");
  return {
    ...actual,
    fetchChartCandles: vi.fn(),
  };
});

import { fetchChartCandles } from "@/lib/chartData";

const mockedFetch = vi.mocked(fetchChartCandles);

const T1 = Math.floor(Date.UTC(2026, 3, 10) / 1000);
const T2 = Math.floor(Date.UTC(2026, 3, 11) / 1000);

function bars(rows: NormalisedBar[], range: NormalisedChartCandles["range"] = "1m"): NormalisedChartCandles {
  return {
    symbol: "AAPL",
    range,
    kind: range === "1d" || range === "5d" || range === "1m" || range === "3m" || range === "6m" ? "intraday" : "daily",
    rows,
  };
}

function twoValidRows(): NormalisedBar[] {
  return [
    { time: T1, open: "100", high: "102", low: "99", close: "101", volume: "1000" },
    { time: T2, open: "101", high: "104", low: "100", close: "103", volume: "1500" },
  ];
}

function LocationSpy({ onLocation }: { onLocation: (search: string) => void }) {
  const loc = useLocation();
  onLocation(loc.search);
  return null;
}

beforeEach(() => {
  mockedFetch.mockReset();
  libState.candleSetData.mockClear();
  libState.lineSetData.mockClear();
  libState.areaSetData.mockClear();
  libState.volumeSetData.mockClear();
  libState.candleApply.mockClear();
  libState.lineApply.mockClear();
  libState.areaApply.mockClear();
  libState.rightPriceScaleApply.mockClear();
  libState.volumePriceScaleApply.mockClear();
  libState.timeScaleApply.mockClear();
  libState.fitContent.mockClear();
  libState.remove.mockClear();
  libState.crosshairHandlers.length = 0;
});

describe("PriceChart — range picker", () => {
  it("renders all nine range buttons", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    for (const r of ["1d", "5d", "1m", "3m", "6m", "ytd", "1y", "5y", "max"]) {
      expect(screen.getByTestId(`chart-range-${r}`)).toBeInTheDocument();
    }
  });

  it("clicking a range button refetches with the new range", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalledWith("AAPL", "1m");
    });
    await user.click(screen.getByTestId("chart-range-1y"));
    await waitFor(() => {
      expect(mockedFetch).toHaveBeenLastCalledWith("AAPL", "1y");
    });
  });
});

describe("PriceChart — type toggle (#587)", () => {
  it("renders three type buttons defaulting to Candle", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    for (const id of ["candle", "line", "area"]) {
      expect(screen.getByTestId(`chart-type-${id}`)).toBeInTheDocument();
    }
  });

  it("clicking Line writes ?type=line; clicking Candle clears the param", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    const user = userEvent.setup();
    let lastSearch = "";
    render(
      <MemoryRouter>
        <LocationSpy onLocation={(s) => (lastSearch = s)} />
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await user.click(screen.getByTestId("chart-type-line"));
    expect(lastSearch).toContain("type=line");
    await user.click(screen.getByTestId("chart-type-candle"));
    expect(lastSearch).not.toContain("type=");
  });

  it("toggles series visibility when ?type=area is set on initial render", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows()));
    render(
      <MemoryRouter initialEntries={["/?type=area"]}>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("price-chart-AAPL")).toBeInTheDocument();
    });
    await waitFor(() => {
      const last = libState.areaApply.mock.calls.at(-1)?.[0] as { visible?: boolean } | undefined;
      expect(last?.visible).toBe(true);
    });
    const lastCandle = libState.candleApply.mock.calls.at(-1)?.[0] as { visible?: boolean } | undefined;
    const lastLine = libState.lineApply.mock.calls.at(-1)?.[0] as { visible?: boolean } | undefined;
    expect(lastCandle?.visible).toBe(false);
    expect(lastLine?.visible).toBe(false);
  });
});

describe("PriceChart — log scale toggle (#587)", () => {
  it("renders a Log toggle button defaulting to off", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    const btn = screen.getByTestId("chart-scale-log");
    expect(btn).toHaveAttribute("aria-pressed", "false");
  });

  it("clicking Log writes ?scale=log; clicking again clears the param", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    const user = userEvent.setup();
    let lastSearch = "";
    render(
      <MemoryRouter>
        <LocationSpy onLocation={(s) => (lastSearch = s)} />
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await user.click(screen.getByTestId("chart-scale-log"));
    expect(lastSearch).toContain("scale=log");
    await user.click(screen.getByTestId("chart-scale-log"));
    expect(lastSearch).not.toContain("scale=");
  });

  it("applies mode=1 to the right price scale when ?scale=log is set", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows()));
    render(
      <MemoryRouter initialEntries={["/?scale=log"]}>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      const calls = libState.rightPriceScaleApply.mock.calls.map((c) => c[0]);
      expect(calls.some((opts: { mode?: number }) => opts.mode === 1)).toBe(true);
    });
  });
});

describe("PriceChart — controls swallow card-click events (#587)", () => {
  it("clicks on the controls bar do not bubble to a parent click handler", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    const onCardClick = vi.fn();
    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <div data-testid="card" onClick={onCardClick}>
          <PriceChart symbol="AAPL" />
        </div>
      </MemoryRouter>,
    );
    await user.click(screen.getByTestId("chart-range-1y"));
    expect(onCardClick).not.toHaveBeenCalled();
    await user.click(screen.getByTestId("chart-type-line"));
    expect(onCardClick).not.toHaveBeenCalled();
    await user.click(screen.getByTestId("chart-scale-log"));
    expect(onCardClick).not.toHaveBeenCalled();
  });
});

describe("PriceChart — data states", () => {
  it("renders 'No price data' when rows is empty", async () => {
    mockedFetch.mockResolvedValue(bars([]));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("price-chart-AAPL")).not.toBeInTheDocument();
  });

  it("renders empty state with only one valid row", async () => {
    mockedFetch.mockResolvedValue(
      bars([{ time: T1, open: "100", high: "102", low: "99", close: "101", volume: "1000" }]),
    );
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
  });

  it("mounts the chart canvas and pushes ≥2 rows to candle, line, and area series", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows()));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("price-chart-AAPL")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(libState.candleSetData).toHaveBeenCalled();
    });
    const candleCall = libState.candleSetData.mock.calls[0]?.[0] as Array<{
      open: number;
      close: number;
    }>;
    expect(candleCall).toHaveLength(2);
    expect(candleCall[0]?.open).toBe(100);
    expect(candleCall[1]?.close).toBe(103);
    const lineCall = libState.lineSetData.mock.calls[0]?.[0] as Array<{ value: number }>;
    expect(lineCall).toHaveLength(2);
    expect(lineCall[0]?.value).toBe(101);
    const areaCall = libState.areaSetData.mock.calls[0]?.[0] as Array<{ value: number }>;
    expect(areaCall[1]?.value).toBe(103);
    expect(libState.volumeSetData).toHaveBeenCalled();
  });

  it("hides the chart while a new-range fetch is in flight", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows(), "5y"));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalled();
    });
    // Fetched 5y rows but range is 1m — gate suppresses chart.
    expect(screen.queryByTestId("price-chart-AAPL")).not.toBeInTheDocument();
    expect(screen.queryByText(/No price data/i)).not.toBeInTheDocument();
  });

  it("treats rows missing OHLC as dropped — empty state not blank chart", async () => {
    mockedFetch.mockResolvedValue(
      bars([
        { time: T1, open: null, high: null, low: null, close: "101", volume: "1000" },
        { time: T2, open: null, high: null, low: null, close: "103", volume: "1500" },
      ]),
    );
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("price-chart-AAPL")).not.toBeInTheDocument();
  });

  it("calls chart.remove() on unmount so the Canvas is released", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows()));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("price-chart-AAPL")).toBeInTheDocument();
    });
    cleanup();
    expect(libState.remove).toHaveBeenCalled();
  });

  it("propagates fetch errors via SectionError + shows a retry button", async () => {
    mockedFetch.mockRejectedValue(new Error("network down"));
    render(
      <MemoryRouter>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument();
    });
    expect(screen.queryByTestId("price-chart-AAPL")).not.toBeInTheDocument();
  });
});

describe("PriceChart — intraday axis formatting (#601)", () => {
  it("intraday range applies timeVisible=true on the time scale", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows(), "1d"));
    render(
      <MemoryRouter initialEntries={["/?chart=1d"]}>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      const calls = libState.timeScaleApply.mock.calls.map((c) => c[0]);
      expect(calls.some((opts: { timeVisible?: boolean }) => opts.timeVisible === true)).toBe(true);
    });
  });

  it("daily range applies timeVisible=false", async () => {
    mockedFetch.mockResolvedValue(bars(twoValidRows(), "1y"));
    render(
      <MemoryRouter initialEntries={["/?chart=1y"]}>
        <PriceChart symbol="AAPL" />
      </MemoryRouter>,
    );
    await waitFor(() => {
      const calls = libState.timeScaleApply.mock.calls.map((c) => c[0]);
      expect(calls.some((opts: { timeVisible?: boolean }) => opts.timeVisible === false)).toBe(true);
    });
  });
});
