import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import * as filingsApi from "@/api/filings";

describe("FilingsPane", () => {
  it("calls fetchFilings with the SIGNIFICANT_FILING_TYPES CSV and ROW_LIMIT 6", () => {
    const spy = vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 0,
      offset: 0,
      limit: 6,
      items: [],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    expect(spy).toHaveBeenCalledWith(
      1,
      0,
      6,
      expect.objectContaining({
        filing_type: expect.stringContaining("10-K"),
      }),
    );
    const callArgs = spy.mock.calls[0]!;
    const csv = (callArgs[3] as { filing_type: string }).filing_type;
    // Spot-check both US + FPI types are listed
    for (const t of ["8-K", "10-K", "10-Q", "6-K", "20-F", "40-F"]) {
      expect(csv).toContain(t);
    }
  });

  it("renders 6 rows max", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 6,
      offset: 0,
      limit: 6,
      items: Array.from({ length: 6 }, (_, i) => ({
        filing_event_id: i + 1,
        instrument_id: 1,
        filing_date: `2026-03-${(i + 1).toString().padStart(2, "0")}`,
        filing_type: i % 2 === 0 ? "10-K" : "8-K",
        provider: "sec_edgar",
        red_flag_score: null,
        extracted_summary: `summary ${i}`,
        primary_document_url: null,
        source_url: null,
        created_at: "2026-03-01T00:00:00Z",
      })),
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const rows = await screen.findAllByText(/summary \d/);
    expect(rows.length).toBe(6);
  });

  it("footer link routes to /instrument/GME?tab=filings", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 0,
      offset: 0,
      limit: 6,
      items: [],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const link = await screen.findByText(/View all filings/);
    expect(link.closest("a")).toHaveAttribute(
      "href",
      "/instrument/GME?tab=filings",
    );
  });
});
