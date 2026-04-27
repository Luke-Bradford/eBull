import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import * as filingsApi from "@/api/filings";
import type { InstrumentSummary } from "@/api/types";

function makeSummary(opts: {
  filingsProvider?: string;
  filingsDataPresent?: boolean;
} = {}): InstrumentSummary {
  const { filingsProvider = "sec_edgar", filingsDataPresent = true } = opts;
  return {
    instrument_id: 1,
    is_tradable: true,
    coverage_tier: 1,
    has_sec_cik: true,
    has_filings_coverage: true,
    identity: { symbol: "GME", display_name: "GameStop", market_cap: null, sector: null },
    price: null,
    key_stats: null,
    source: {},
    capabilities: {
      filings: {
        providers: filingsDataPresent ? [filingsProvider] : [],
        data_present: filingsDataPresent ? { [filingsProvider]: true } : {},
      },
    },
  } as never;
}

describe("FilingsPane", () => {
  it("calls fetchFilings with the SIGNIFICANT_FILING_TYPES CSV and ROW_LIMIT 6 for sec_edgar provider", () => {
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
        <FilingsPane instrumentId={1} symbol="GME" summary={makeSummary()} />
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

  it("calls fetchFilings with no filing_type filter for non-sec_edgar providers", () => {
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
        <FilingsPane
          instrumentId={1}
          symbol="GME"
          summary={makeSummary({ filingsProvider: "companies_house" })}
        />
      </MemoryRouter>,
    );
    const callArgs = spy.mock.calls[0]!;
    const opts = callArgs[3] as { filing_type?: string };
    expect(opts.filing_type).toBeUndefined();
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
        <FilingsPane instrumentId={1} symbol="GME" summary={makeSummary()} />
      </MemoryRouter>,
    );
    const rows = await screen.findAllByText(/summary \d/);
    expect(rows.length).toBe(6);
  });

  it("footer link routes to /instrument/GME?tab=filings when filings capability is active", async () => {
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
        <FilingsPane instrumentId={1} symbol="GME" summary={makeSummary()} />
      </MemoryRouter>,
    );
    const link = await screen.findByText(/View all filings/);
    expect(link.closest("a")).toHaveAttribute(
      "href",
      "/instrument/GME?tab=filings",
    );
  });

  it("hides footer link when filings capability is inactive", async () => {
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
        <FilingsPane
          instrumentId={1}
          symbol="GME"
          summary={makeSummary({ filingsDataPresent: false })}
        />
      </MemoryRouter>,
    );
    // EmptyState renders; wait for async resolution
    await screen.findByText(/No filings/);
    expect(screen.queryByText(/View all filings/)).toBeNull();
  });
});
