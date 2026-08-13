import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { InstrumentSecProfile } from "@/api/instruments";

import { SecProfilePanel } from "./SecProfilePanel";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentSecProfile: vi.fn(),
  fetchInstrumentEmployees: vi.fn().mockResolvedValue(null),
}));

import { fetchInstrumentSecProfile } from "@/api/instruments";
const mockFetch = vi.mocked(fetchInstrumentSecProfile);


function seededProfile(): InstrumentSecProfile {
  return {
    symbol: "AAPL",
    cik: "0000320193",
    sic: "3571",
    sic_description: "Electronic Computers",
    owner_org: "06 Technology",
    description: "Designs consumer electronics.",
    website: "https://apple.com",
    investor_website: null,
    ein: "EIN",
    lei: null,
    state_of_incorporation: "CA",
    state_of_incorporation_desc: "California",
    fiscal_year_end: "0930",
    category: "Large accelerated filer",
    exchanges: ["NASDAQ"],
    former_names: [
      {
        name: "APPLE COMPUTER INC",
        from_: "1977-01-01T00:00:00.000Z",
        to: "2007-01-09T00:00:00.000Z",
      },
    ],
    has_insider_issuer: true,
    has_insider_owner: true,
  };
}


afterEach(() => vi.clearAllMocks());


describe("SecProfilePanel", () => {
  it("renders description + SIC + exchanges + former names", async () => {
    mockFetch.mockResolvedValue(seededProfile());
    render(<SecProfilePanel symbol="AAPL" />);

    await waitFor(() => {
      expect(screen.getByText(/Designs consumer electronics/)).toBeInTheDocument();
    });
    expect(screen.getByText(/Electronic Computers/)).toBeInTheDocument();
    expect(screen.getByText(/NASDAQ/)).toBeInTheDocument();
    expect(screen.getByText(/Large accelerated filer/)).toBeInTheDocument();
    expect(screen.getByText(/APPLE COMPUTER INC/)).toBeInTheDocument();
    expect(screen.getByText(/Sep 30/i)).toBeInTheDocument();
    // Stale #429 placeholder must be gone — Form-4 activity lives in
    // the sibling InsiderActivityPanel now.
    expect(screen.queryByText(/Insider activity recorded/i)).toBeNull();
    expect(screen.queryByText(/detailed list pending/i)).toBeNull();
  });

  it("renders empty state when profile is null (404)", async () => {
    mockFetch.mockResolvedValue(null);
    render(<SecProfilePanel symbol="NOSEC" />);

    await waitFor(() => {
      expect(screen.getByText(/No SEC profile yet/i)).toBeInTheDocument();
    });
  });

  it("renders error state on non-404 fetch failure", async () => {
    mockFetch.mockRejectedValue(new Error("500"));
    render(<SecProfilePanel symbol="AAPL" />);

    await waitFor(() => {
      expect(screen.getByText(/Failed to load/i)).toBeInTheDocument();
    });
  });

  it("renders Pane chrome with sec_edgar source", async () => {
    mockFetch.mockResolvedValue(seededProfile());
    render(<SecProfilePanel symbol="AAPL" />);

    await screen.findByText(/Designs consumer electronics/);
    expect(screen.getByText(/SEC EDGAR/)).toBeInTheDocument();
  });
});
