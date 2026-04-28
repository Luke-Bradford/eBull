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
      // SessionBands subscribes to range changes when intraday + bands
      // enabled. Tests don't drive bands rendering, but the listener
      // attach happens on mount — provide no-op subscribe/unsubscribe
      // so the chart can mount without throwing.
      subscribeVisibleLogicalRangeChange: vi.fn(),
      unsubscribeVisibleLogicalRangeChange: vi.fn(),
      logicalToCoordinate: vi.fn(() => 0),
      timeToCoordinate: vi.fn(() => 0),
      getVisibleLogicalRange: vi.fn(() => null),
      getVisibleRange: vi.fn(() => null),
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

describe("PriceChart — no-flicker refetch (#650)", () => {
  it("skips wholesale series.setData when the 60s backstop refetch returns identical bars", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      mockedFetch.mockResolvedValue(bars(twoValidRows()));
      render(
        <MemoryRouter>
          <PriceChart symbol="AAPL" />
        </MemoryRouter>,
      );
      await waitFor(() => {
        expect(libState.candleSetData).toHaveBeenCalledTimes(1);
      });

      // Two backstop ticks with identical data — fingerprint guard
      // must skip the wholesale setData (the visible-flash path).
      await vi.advanceTimersByTimeAsync(60_000);
      await vi.advanceTimersByTimeAsync(60_000);

      expect(libState.candleSetData).toHaveBeenCalledTimes(1);
      expect(libState.lineSetData).toHaveBeenCalledTimes(1);
      expect(libState.areaSetData).toHaveBeenCalledTimes(1);
      expect(libState.volumeSetData).toHaveBeenCalledTimes(1);

      // Now the backstop returns a revised last bar — fingerprint
      // changes, wholesale setData fires once to pick up the revision.
      const [first, last] = twoValidRows();
      mockedFetch.mockResolvedValue(bars([first!, { ...last!, close: "999" }]));
      await vi.advanceTimersByTimeAsync(60_000);

      await waitFor(() => {
        expect(libState.candleSetData).toHaveBeenCalledTimes(2);
      });
    } finally {
      vi.useRealTimers();
    }
  });

  // Each row below is a single OHLCV field on the LAST bar that the
  // fingerprint must not false-negative on. If any of these slip past
  // the guard the chart will freeze on stale data after a backstop
  // refetch — the original short fingerprint missed open/high/low and
  // any interior-bar revision.
  const lastBarMutations: Array<[label: string, mutate: (b: NormalisedBar) => NormalisedBar]> = [
    ["open changed", (b) => ({ ...b, open: "888" })],
    ["high changed", (b) => ({ ...b, high: "888" })],
    ["low changed", (b) => ({ ...b, low: "0.5" })],
    ["volume changed", (b) => ({ ...b, volume: "9999" })],
  ];

  it.each(lastBarMutations)(
    "wholesale setData fires when the last bar's %s",
    async (_label, mutate) => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      try {
        const initial = twoValidRows();
        mockedFetch.mockResolvedValue(bars(initial));
        render(
          <MemoryRouter>
            <PriceChart symbol="AAPL" />
          </MemoryRouter>,
        );
        await waitFor(() => {
          expect(libState.candleSetData).toHaveBeenCalledTimes(1);
        });

        const mutated = [initial[0]!, mutate(initial[1]!)];
        mockedFetch.mockResolvedValue(bars(mutated));
        await vi.advanceTimersByTimeAsync(60_000);

        await waitFor(() => {
          expect(libState.candleSetData).toHaveBeenCalledTimes(2);
        });
      } finally {
        vi.useRealTimers();
      }
    },
  );

  it("wholesale setData fires when an interior bar's close changes (volume coloring depends on prior close)", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      // Three bars so we have a true interior bar between first/last.
      const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
      const initial: NormalisedBar[] = [
        { time: T0, open: "98", high: "100", low: "97", close: "99", volume: "800" },
        { time: T1, open: "100", high: "102", low: "99", close: "101", volume: "1000" },
        { time: T2, open: "101", high: "104", low: "100", close: "103", volume: "1500" },
      ];
      mockedFetch.mockResolvedValue(bars(initial));
      render(
        <MemoryRouter>
          <PriceChart symbol="AAPL" />
        </MemoryRouter>,
      );
      await waitFor(() => {
        expect(libState.candleSetData).toHaveBeenCalledTimes(1);
      });

      // Mutate ONLY the middle bar's close — first/last unchanged.
      // The volume bar's color depends on close >= prev close, so a
      // missed interior change leaves stale red/green coloring.
      const mutated = [initial[0]!, { ...initial[1]!, close: "97" }, initial[2]!];
      mockedFetch.mockResolvedValue(bars(mutated));
      await vi.advanceTimersByTimeAsync(60_000);

      await waitFor(() => {
        expect(libState.candleSetData).toHaveBeenCalledTimes(2);
        expect(libState.volumeSetData).toHaveBeenCalledTimes(2);
      });
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("barFingerprint + mergeLiveBarIntoClean (#650 helpers)", () => {
  // These pure helpers underpin the no-flicker guarantee: the same
  // fingerprint formula must be used for the REST setData path AND
  // the SSE onApplied path, otherwise REST converging on the live
  // bar would be misclassified as new data and trigger a wholesale
  // setData flash. Tests below pin both formulas + the merge rules.

  it("barFingerprint distinguishes range, length, and any field on any bar", async () => {
    const { barFingerprint } = await import("./PriceChart");
    const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
    const Tn = Math.floor(Date.UTC(2026, 3, 10) / 1000);
    const bar = (over: Partial<{ time: number; o: number; h: number; l: number; c: number; v: number }> = {}) => ({
      time: (over.time ?? T0) as number,
      open: over.o ?? 100,
      high: over.h ?? 102,
      low: over.l ?? 99,
      close: over.c ?? 101,
      volume: over.v ?? 1000,
    });
    const base = [bar(), bar({ time: Tn, c: 103 })];
    const baseFp = barFingerprint("1m", base as never);

    expect(baseFp).toBe(barFingerprint("1m", [...base] as never));
    expect(baseFp).not.toBe(barFingerprint("1y", base as never));
    expect(baseFp).not.toBe(barFingerprint("1m", [...base, bar({ time: Tn + 60 })] as never));
    expect(baseFp).not.toBe(barFingerprint("1m", [bar({ o: 99 }), base[1]!] as never));
    expect(baseFp).not.toBe(barFingerprint("1m", [base[0]!, bar({ time: Tn, c: 999 })] as never));
    expect(baseFp).not.toBe(barFingerprint("1m", [base[0]!, bar({ time: Tn, v: 9999 })] as never));
  });

  it("mergeLiveBarIntoClean.update mutates the last bar when times match, preserves volume", async () => {
    const { mergeLiveBarIntoClean } = await import("./PriceChart");
    const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
    const initial = [
      { time: T0 as never, open: 100, high: 102, low: 99, close: 101, volume: 5000 },
    ];
    const merged = mergeLiveBarIntoClean(initial, {
      kind: "update",
      time: T0,
      open: 100,
      high: 105,
      low: 98,
      close: 104,
    });
    expect(merged).toHaveLength(1);
    expect(merged[0]).toEqual({ time: T0, open: 100, high: 105, low: 98, close: 104, volume: 5000 });
  });

  it("mergeLiveBarIntoClean.update with time > last upserts as a new bar (handles append → update sequencing)", async () => {
    // The aggregator emits `update` for ticks that land in its own
    // previously-appended live bucket. From REST's perspective that
    // bar does not exist yet, so the merge must still keep the
    // overlay alive — otherwise the second tick into a fresh bucket
    // would silently roll the fingerprint back, re-opening the
    // wholesale-flash on the next 60s REST refetch.
    const { mergeLiveBarIntoClean } = await import("./PriceChart");
    const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
    const initial = [
      { time: T0 as never, open: 100, high: 102, low: 99, close: 101, volume: 5000 },
    ];
    const merged = mergeLiveBarIntoClean(initial, {
      kind: "update",
      time: T0 + 60,
      open: 102,
      high: 106,
      low: 101,
      close: 105,
    });
    expect(merged).toHaveLength(2);
    expect(merged[1]).toEqual({ time: T0 + 60, open: 102, high: 106, low: 101, close: 105, volume: 0 });
  });

  it("mergeLiveBarIntoClean leaves clean untouched when live.time is older than last (stale tick)", async () => {
    const { mergeLiveBarIntoClean } = await import("./PriceChart");
    const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
    const initial = [
      { time: T0 as never, open: 100, high: 102, low: 99, close: 101, volume: 5000 },
    ];
    const merged = mergeLiveBarIntoClean(initial, {
      kind: "update",
      time: T0 - 60,
      open: 99,
      high: 100,
      low: 98,
      close: 99.5,
    });
    expect(merged).toEqual(initial);
  });

  it("mergeLiveBarIntoClean append → update on same bucket converges to single overlay bar", async () => {
    const { mergeLiveBarIntoClean, barFingerprint } = await import("./PriceChart");
    const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
    const T1Bucket = T0 + 60;
    const rest = [
      { time: T0 as never, open: 100, high: 102, low: 99, close: 101, volume: 5000 },
    ];

    // First tick: append.
    const afterAppend = mergeLiveBarIntoClean(rest, {
      kind: "append",
      time: T1Bucket,
      open: 102,
      high: 102,
      low: 102,
      close: 102,
    });
    expect(afterAppend).toHaveLength(2);
    const fpAfterAppend = barFingerprint("1m", afterAppend as never);

    // Second tick same bucket arrives as `update` per aggregator —
    // but REST clean has not refetched, so merge against `rest`,
    // NOT `afterAppend`. The fix means this still produces the
    // upserted bar with the latest OHLC, fingerprint stays sensible.
    const afterUpdate = mergeLiveBarIntoClean(rest, {
      kind: "update",
      time: T1Bucket,
      open: 102,
      high: 105,
      low: 102,
      close: 104,
    });
    expect(afterUpdate).toHaveLength(2);
    expect(afterUpdate[1]).toMatchObject({ time: T1Bucket, open: 102, high: 105, low: 102, close: 104, volume: 0 });

    // Most importantly: the post-update fingerprint differs from the
    // post-append one (so the cached fp tracks the latest overlay).
    const fpAfterUpdate = barFingerprint("1m", afterUpdate as never);
    expect(fpAfterUpdate).not.toBe(fpAfterAppend);

    // And: when REST eventually catches up to the same overlay
    // state, fingerprint matches and the wholesale setData is
    // skipped — that's the whole point of the convergence fix.
    const restCaughtUp = [
      ...rest,
      { time: T1Bucket as never, open: 102, high: 105, low: 102, close: 104, volume: 0 },
    ];
    expect(barFingerprint("1m", restCaughtUp as never)).toBe(fpAfterUpdate);
  });

  it("mergeLiveBarIntoClean.append adds a fresh bar with volume=0", async () => {
    const { mergeLiveBarIntoClean } = await import("./PriceChart");
    const T0 = Math.floor(Date.UTC(2026, 3, 9) / 1000);
    const initial = [
      { time: T0 as never, open: 100, high: 102, low: 99, close: 101, volume: 5000 },
    ];
    const merged = mergeLiveBarIntoClean(initial, {
      kind: "append",
      time: T0 + 60,
      open: 101,
      high: 103,
      low: 100,
      close: 102,
    });
    expect(merged).toHaveLength(2);
    expect(merged[1]).toEqual({ time: T0 + 60, open: 101, high: 103, low: 100, close: 102, volume: 0 });
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
