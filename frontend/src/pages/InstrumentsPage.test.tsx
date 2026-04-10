/**
 * Tests for InstrumentsPage (#147).
 *
 * Scope:
 *   - loading, empty, error, data states
 *   - search input triggers refetch with debounce
 *   - filter changes trigger refetch
 *   - pagination controls
 *   - sortable column headers
 *   - coverage tier badge rendering
 *   - instrument rows link to detail page
 *
 * API mocked at module boundary — tests exercise the page state machine,
 * not the network layer.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { InstrumentsPage } from "@/pages/InstrumentsPage";
import { fetchInstruments } from "@/api/instruments";
import type { InstrumentListResponse } from "@/api/types";

vi.mock("@/api/instruments", () => ({
  fetchInstruments: vi.fn(),
  INSTRUMENTS_PAGE_LIMIT: 50,
}));

const mockedFetch = vi.mocked(fetchInstruments);

function makeResponse(
  overrides: Partial<InstrumentListResponse> = {},
): InstrumentListResponse {
  return {
    items: [
      {
        instrument_id: 1,
        symbol: "AAPL",
        company_name: "Apple Inc.",
        exchange: "NASDAQ",
        currency: "USD",
        sector: "Technology",
        is_tradable: true,
        coverage_tier: 1,
        latest_quote: {
          bid: 185.5,
          ask: 185.6,
          last: 185.55,
          spread_pct: 0.054,
          quoted_at: "2026-04-08T12:00:00Z",
        },
      },
      {
        instrument_id: 2,
        symbol: "MSFT",
        company_name: "Microsoft Corp.",
        exchange: "NASDAQ",
        currency: "USD",
        sector: "Technology",
        is_tradable: true,
        coverage_tier: 2,
        latest_quote: {
          bid: 420.0,
          ask: 420.1,
          last: 420.05,
          spread_pct: 0.024,
          quoted_at: "2026-04-08T12:00:00Z",
        },
      },
      {
        instrument_id: 3,
        symbol: "JPM",
        company_name: "JPMorgan Chase",
        exchange: "NYSE",
        currency: "USD",
        sector: "Financial Services",
        is_tradable: true,
        coverage_tier: null,
        latest_quote: null,
      },
    ],
    total: 3,
    offset: 0,
    limit: 50,
    ...overrides,
  };
}

function renderPage() {
  return render(
    <MemoryRouter>
      <InstrumentsPage />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  mockedFetch.mockReset();
  mockedFetch.mockResolvedValue(makeResponse());
});

afterEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Data rendering
// ---------------------------------------------------------------------------

describe("InstrumentsPage — data rendering", () => {
  it("renders instrument rows with symbols and company names", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    expect(screen.getByText("Apple Inc.")).toBeInTheDocument();
    expect(screen.getByText("MSFT")).toBeInTheDocument();
    expect(screen.getByText("Microsoft Corp.")).toBeInTheDocument();
    expect(screen.getByText("JPM")).toBeInTheDocument();
  });

  it("renders coverage tier badges in the table", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    // Tier badges are <span> elements inside the table, distinct from <option> elements
    const table = screen.getByRole("table");
    expect(within(table).getByText("Tier 1")).toBeInTheDocument();
    expect(within(table).getByText("Tier 2")).toBeInTheDocument();
  });

  it("renders dash for null coverage tier", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    // JPM has null coverage_tier — rendered as em-dash
    const dashes = screen.getAllByText("—");
    expect(dashes.length).toBeGreaterThanOrEqual(1);
  });

  it("renders total instrument count", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("3 instruments")).toBeInTheDocument();
    });
  });

  it("renders instrument symbols as links to detail page", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    const link = screen.getByText("AAPL").closest("a");
    expect(link).toHaveAttribute("href", "/instruments/1");
  });
});

// ---------------------------------------------------------------------------
// Empty and error states
// ---------------------------------------------------------------------------

describe("InstrumentsPage — empty and error states", () => {
  it("shows empty state when no instruments exist", async () => {
    mockedFetch.mockResolvedValue(makeResponse({ items: [], total: 0 }));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("No instruments found")).toBeInTheDocument();
    });
    expect(screen.getByText(/Run the universe sync job/)).toBeInTheDocument();
  });

  it("shows filter-aware empty state when filters are active", async () => {
    // First render with data to populate sector dropdown
    mockedFetch.mockResolvedValue(makeResponse());
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });

    // Now return empty for the filtered query
    mockedFetch.mockResolvedValue(makeResponse({ items: [], total: 0 }));
    const user = userEvent.setup();
    // Find tier select by its parent label structure
    const tierLabel = screen.getByText("Tier", { selector: "label" });
    const tierSelect = tierLabel.parentElement!.querySelector("select")!;
    await user.selectOptions(tierSelect, "1");

    await waitFor(() => {
      expect(screen.getByText("No instruments found")).toBeInTheDocument();
    });
    expect(screen.getByText(/Try adjusting/)).toBeInTheDocument();
  });

  it("shows error state on fetch failure", async () => {
    mockedFetch.mockRejectedValue(new Error("network"));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/Failed to load/)).toBeInTheDocument();
    });
  });
});

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

describe("InstrumentsPage — search", () => {
  it("debounced search triggers refetch with search param", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    const searchInput = screen.getByPlaceholderText(/Symbol or company name/);
    await user.type(searchInput, "App");

    await waitFor(() => {
      const calls = mockedFetch.mock.calls;
      const lastCall = calls[calls.length - 1]!;
      expect(lastCall[0]).toMatchObject({ search: "App" });
    });
  });
});

// ---------------------------------------------------------------------------
// Filters
// ---------------------------------------------------------------------------

describe("InstrumentsPage — filters", () => {
  it("tier filter triggers refetch with coverage_tier param", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    // Find tier select by its parent label structure
    const tierLabel = screen.getByText("Tier", { selector: "label" });
    const tierSelect = tierLabel.parentElement!.querySelector("select")!;
    await user.selectOptions(tierSelect, "1");

    await waitFor(() => {
      const calls = mockedFetch.mock.calls;
      const lastCall = calls[calls.length - 1]!;
      expect(lastCall[0]).toMatchObject({ coverage_tier: 1 });
    });
  });
});

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

describe("InstrumentsPage — pagination", () => {
  it("shows pagination controls when total exceeds page limit", async () => {
    mockedFetch.mockResolvedValue(makeResponse({ total: 120 }));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();
    });
    expect(screen.getByText("Previous")).toBeDisabled();
    expect(screen.getByText("Next")).not.toBeDisabled();
  });

  it("clicking Next fetches next page", async () => {
    mockedFetch.mockResolvedValue(makeResponse({ total: 120 }));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    await user.click(screen.getByText("Next"));

    await waitFor(() => {
      const calls = mockedFetch.mock.calls;
      const lastCall = calls[calls.length - 1]!;
      expect(lastCall[0]).toMatchObject({ offset: 50 });
    });
  });

  it("hides pagination when total fits in one page", async () => {
    mockedFetch.mockResolvedValue(makeResponse({ total: 3 }));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    expect(screen.queryByText("Previous")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

describe("InstrumentsPage — sorting", () => {
  it("clicking a column header toggles sort direction", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    // Symbol column is default asc — click to toggle to desc
    const symbolHeader = screen.getByText("Instrument");
    await user.click(symbolHeader);

    // After clicking default asc column, should be desc — MSFT first
    await waitFor(() => {
      const rows = screen.getAllByRole("row");
      // Row 0 is header; row 1 should be MSFT (desc order)
      expect(rows[1]).toHaveTextContent("MSFT");
    });
  });
});
