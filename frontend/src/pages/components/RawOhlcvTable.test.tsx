import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { NormalisedBar } from "@/lib/chartData";
import { RawOhlcvTable } from "./RawOhlcvTable";

// Daily UTC-midnight epoch seconds for 2026-04-01 / 02 / 03.
const T1 = Math.floor(Date.UTC(2026, 3, 1) / 1000);
const T2 = Math.floor(Date.UTC(2026, 3, 2) / 1000);
const T3 = Math.floor(Date.UTC(2026, 3, 3) / 1000);

const rows: NormalisedBar[] = [
  { time: T1, open: "100", high: "110", low: "95", close: "105", volume: "10000" },
  { time: T2, open: "105", high: "115", low: "100", close: "110", volume: "12000" },
  { time: T3, open: "110", high: "120", low: "105", close: "115", volume: "14000" },
];

describe("RawOhlcvTable", () => {
  it("renders all rows in newest-first order by default", () => {
    render(<RawOhlcvTable rows={rows} symbol="GME" range="1m" />);
    const dateCells = screen.getAllByText(/^2026-04-/);
    expect(dateCells[0]?.textContent).toBe("2026-04-03");
    expect(dateCells[2]?.textContent).toBe("2026-04-01");
  });

  it("toggles sort to ascending on header click", async () => {
    render(<RawOhlcvTable rows={rows} symbol="GME" range="1m" />);
    await userEvent.click(screen.getByTestId("sort-date"));
    const dateCells = screen.getAllByText(/^2026-04-/);
    expect(dateCells[0]?.textContent).toBe("2026-04-01");
  });

  it("renders empty state when rows is empty", () => {
    render(<RawOhlcvTable rows={[]} symbol="GME" range="1m" />);
    expect(screen.getByText(/No raw data/)).toBeInTheDocument();
  });

  it("CSV download button triggers blob URL creation", async () => {
    const createObjectURL = vi.fn(() => "blob:mock");
    const revokeObjectURL = vi.fn();
    global.URL.createObjectURL = createObjectURL as never;
    global.URL.revokeObjectURL = revokeObjectURL;
    render(<RawOhlcvTable rows={rows} symbol="GME" range="1m" />);
    await userEvent.click(screen.getByTestId("csv-download"));
    expect(createObjectURL).toHaveBeenCalledTimes(1);
  });

  it("shows row count and range badge", () => {
    render(<RawOhlcvTable rows={rows} symbol="GME" range="1m" />);
    expect(screen.getByText(/3 rows/)).toBeInTheDocument();
    expect(screen.getByText(/1m/)).toBeInTheDocument();
  });

  it("renders null OHLCV values as em-dash", () => {
    const nullRow: NormalisedBar[] = [
      { time: T1, open: null, high: null, low: null, close: null, volume: null },
    ];
    render(<RawOhlcvTable rows={nullRow} symbol="GME" range="1m" />);
    // 5 em-dashes (open/high/low/close/volume — date is never null)
    expect(screen.getAllByText("—")).toHaveLength(5);
  });

  it("intraday=true renders timestamp in browser-local time + uses Time header label", () => {
    // Pin to a UTC instant; assert the rendered cell matches whatever
    // the browser's local-tz formatting produces for that instant.
    // The hover label rule (#602): intraday hovers show local time
    // not UTC, so a UK operator on BST sees the wall-clock time
    // they'd see on TradingView.
    const t = Math.floor(Date.UTC(2026, 3, 27, 14, 30) / 1000);
    const intradayRows: NormalisedBar[] = [
      { time: t, open: "100", high: "100", low: "100", close: "100", volume: "1" },
    ];
    render(<RawOhlcvTable rows={intradayRows} symbol="GME" range="1d" intraday />);
    const d = new Date(t * 1000);
    const expected =
      `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")} ` +
      `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
    expect(screen.getByText(expected)).toBeInTheDocument();
    expect(screen.getByText(/^Time/)).toBeInTheDocument();
  });
});
