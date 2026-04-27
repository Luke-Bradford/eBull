import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import * as filingsApi from "@/api/filings";

describe("FilingsPane", () => {
  it("renders 5 rows max with drilldown links for 8-K + 10-K", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 8,
      offset: 0,
      limit: 5,
      items: Array.from({ length: 8 }, (_, i) => ({
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
    expect(rows.length).toBe(5);
  });

  it("renders drilldown link to /filings/10-k for 10-K type", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 1,
      offset: 0,
      limit: 5,
      items: [
        {
          filing_event_id: 1,
          instrument_id: 1,
          filing_date: "2026-03-01",
          filing_type: "10-K",
          provider: "sec_edgar",
          red_flag_score: null,
          extracted_summary: "annual report",
          primary_document_url: null,
          source_url: null,
          created_at: "2026-03-01T00:00:00Z",
        },
      ],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const link = await screen.findByRole("link");
    expect(link).toHaveAttribute("href", "/instrument/GME/filings/10-k");
  });

  it("renders drilldown link to /filings/8-k for 8-K type", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 1,
      offset: 0,
      limit: 5,
      items: [
        {
          filing_event_id: 2,
          instrument_id: 1,
          filing_date: "2026-03-02",
          filing_type: "8-K",
          provider: "sec_edgar",
          red_flag_score: null,
          extracted_summary: "current report",
          primary_document_url: null,
          source_url: null,
          created_at: "2026-03-02T00:00:00Z",
        },
      ],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const link = await screen.findByRole("link");
    expect(link).toHaveAttribute("href", "/instrument/GME/filings/8-k");
  });
});
