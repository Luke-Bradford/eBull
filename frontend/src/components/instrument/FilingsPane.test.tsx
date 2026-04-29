import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import * as filingsApi from "@/api/filings";
import type { InstrumentSummary } from "@/api/types";

const navigateMock = vi.fn();
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual<typeof import("react-router-dom")>(
    "react-router-dom",
  );
  return { ...actual, useNavigate: () => navigateMock };
});

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
        accession_number: `0000000000-26-00000${i}`,
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

  it("renders Open button when filings tab is active and navigates to ?tab=filings", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 0,
      offset: 0,
      limit: 6,
      items: [],
    });
    navigateMock.mockReset();
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" summary={makeSummary()} />
      </MemoryRouter>,
    );
    const btn = await screen.findByRole("button", { name: /open/i });
    await userEvent.click(btn);
    expect(navigateMock).toHaveBeenCalledWith("/instrument/GME?tab=filings");
  });

  it("appends accession to 10-K drilldown link (#565)", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "GME",
      total: 2,
      offset: 0,
      limit: 6,
      items: [
        {
          filing_event_id: 1,
          instrument_id: 1,
          filing_date: "2026-03-20",
          filing_type: "10-K",
          provider: "sec_edgar",
          accession_number: "0001326380-26-000013",
          red_flag_score: null,
          extracted_summary: "FY 2025 10-K",
          primary_document_url: null,
          source_url: null,
          created_at: "2026-03-20T00:00:00Z",
        },
        {
          filing_event_id: 2,
          instrument_id: 1,
          filing_date: "2024-04-01",
          filing_type: "10-K/A",
          provider: "sec_edgar",
          accession_number: "0001326380-24-000019",
          red_flag_score: null,
          extracted_summary: "FY 2023 10-K/A",
          primary_document_url: null,
          source_url: null,
          created_at: "2024-04-01T00:00:00Z",
        },
      ],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" summary={makeSummary()} />
      </MemoryRouter>,
    );
    // Wait for data + find the row links by their summary text.
    const link10k = (await screen.findByText("FY 2025 10-K")).closest("a");
    expect(link10k).not.toBeNull();
    expect(link10k!.getAttribute("href")).toBe(
      "/instrument/GME/filings/10-k?accession=0001326380-26-000013",
    );

    // 10-K/A also gets accession-targeted drilldown — historical row
    // no longer routes to "the latest" by default.
    const link10ka = (await screen.findByText("FY 2023 10-K/A")).closest("a");
    expect(link10ka).not.toBeNull();
    expect(link10ka!.getAttribute("href")).toBe(
      "/instrument/GME/filings/10-k?accession=0001326380-24-000019",
    );
  });

  it("renders the friendly form-type name when extracted_summary is null (no `8-K  8-K` dupe — #684)", async () => {
    // Operator screenshot 2026-04-29 on /instrument/IEP showed each
    // row rendering the form type twice (once as the chip, once as
    // the third-column fallback when extracted_summary was null).
    // The fix: fall back to the glossary's friendly short name
    // (e.g. "Material event" / "Annual report") so the row carries
    // distinct information.
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      instrument_id: 1,
      symbol: "IEP",
      total: 2,
      offset: 0,
      limit: 6,
      items: [
        {
          filing_event_id: 1,
          instrument_id: 1,
          filing_date: "2026-03-05",
          filing_type: "8-K",
          provider: "sec_edgar",
          accession_number: "0000000000-26-000001",
          red_flag_score: null,
          extracted_summary: null, // <- the bug condition
          primary_document_url: null,
          source_url: null,
          created_at: "2026-03-05T00:00:00Z",
        },
        {
          filing_event_id: 2,
          instrument_id: 1,
          filing_date: "2026-02-26",
          filing_type: "10-K",
          provider: "sec_edgar",
          accession_number: "0000000000-26-000002",
          red_flag_score: null,
          extracted_summary: null,
          primary_document_url: null,
          source_url: null,
          created_at: "2026-02-26T00:00:00Z",
        },
      ],
    });
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="IEP" summary={makeSummary()} />
      </MemoryRouter>,
    );
    // 8-K row's third column should now be "Material event", not
    // a second "8-K". 10-K → "Annual report".
    expect(await screen.findByText("Material event")).toBeInTheDocument();
    expect(screen.getByText("Annual report")).toBeInTheDocument();
  });

  it("hides Open button when filings capability is inactive", async () => {
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
    expect(screen.queryByRole("button", { name: /open/i })).toBeNull();
  });
});
