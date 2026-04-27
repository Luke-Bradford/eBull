import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { RawOhlcvTable } from "./RawOhlcvTable";

const rows = [
  { date: "2026-04-01", open: "100", high: "110", low: "95", close: "105", volume: "10000" },
  { date: "2026-04-02", open: "105", high: "115", low: "100", close: "110", volume: "12000" },
  { date: "2026-04-03", open: "110", high: "120", low: "105", close: "115", volume: "14000" },
] as never;

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
    const nullRow = [
      { date: "2026-04-01", open: null, high: null, low: null, close: null, volume: null },
    ] as never;
    render(<RawOhlcvTable rows={nullRow} symbol="GME" range="1m" />);
    // 5 em-dashes (open/high/low/close/volume — date is never null)
    expect(screen.getAllByText("—")).toHaveLength(5);
  });
});
