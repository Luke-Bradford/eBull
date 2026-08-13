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
        exchange: "4",
        exchange_name: "Nasdaq",
        currency: "USD",
        sector: "Technology",
        gics_sector: "Information Technology",
        sector_spdr: "XLK",
        is_tradable: true,
        coverage_tier: 1,
        latest_quote: {
          bid: 185.5,
          ask: 185.6,
          last: 185.55,
          spread_pct: 0.054,
          quoted_at: "2026-04-08T12:00:00Z",
        },
        day_change_pct: "0.0152",
        day_change_as_of: "2026-04-08",
      },
      {
        instrument_id: 2,
        symbol: "MSFT",
        company_name: "Microsoft Corp.",
        exchange: "4",
        exchange_name: "Nasdaq",
        currency: "USD",
        sector: "Technology",
        gics_sector: "Information Technology",
        sector_spdr: "XLK",
        is_tradable: true,
        coverage_tier: 2,
        latest_quote: {
          bid: 420.0,
          ask: 420.1,
          last: 420.05,
          spread_pct: 0.024,
          quoted_at: "2026-04-08T12:00:00Z",
        },
        day_change_pct: "-0.0093",
        day_change_as_of: "2026-04-07",
      },
      {
        instrument_id: 3,
        symbol: "JPM",
        company_name: "JPMorgan Chase",
        exchange: "5",
        exchange_name: "NYSE",
        currency: "USD",
        sector: "Financial Services",
        gics_sector: "Financials",
        sector_spdr: "XLF",
        is_tradable: true,
        coverage_tier: null,
        latest_quote: null,
        // #1924: change present from price_daily while the live quote is
        // absent — the list shows the % with a "—" price (as-of dated).
        day_change_pct: "0.0021",
        day_change_as_of: "2026-04-08",
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

  it("renders the day-change column with colored pct + as-of close date (#1924)", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    // AAPL +1.52% (day_change_pct 0.0152), stamped with its close date.
    expect(screen.getByText("+1.52%")).toBeInTheDocument();
    // MSFT negative change.
    expect(screen.getByText("-0.93%")).toBeInTheDocument();
    // JPM has a change from price_daily but no live quote → the % shows while
    // the price cell stays "—" (change is dated so it reads honestly).
    expect(screen.getByText("+0.21%")).toBeInTheDocument();
    expect(screen.getAllByText(/^as of /).length).toBeGreaterThanOrEqual(3);
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
    expect(link).toHaveAttribute("href", "/instrument/AAPL");
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
    // Page number buttons rendered
    expect(screen.getByText("1")).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
  });

  it("clicking a page number fetches that page", async () => {
    mockedFetch.mockResolvedValue(makeResponse({ total: 120 }));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    await user.click(screen.getByText("3"));

    await waitFor(() => {
      const calls = mockedFetch.mock.calls;
      const lastCall = calls[calls.length - 1]!;
      expect(lastCall[0]).toMatchObject({ offset: 100 });
    });
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
  it("defaults to coverage-tier order (#1904 — rich rows first)", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    // Default sort mirrors the server order: coverage_tier ASC, NULLs last.
    // AAPL (tier 1) → MSFT (tier 2) → JPM (null).
    const rows = screen.getAllByRole("row");
    expect(rows[1]).toHaveTextContent("AAPL");
    expect(rows[2]).toHaveTextContent("MSFT");
    expect(rows[3]).toHaveTextContent("JPM");
  });

  it("clicking a column header sorts asc then toggles to desc", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });

    const user = userEvent.setup();
    const symbolHeader = screen.getByText("Instrument");
    // First click on a non-active column → symbol ASC (AAPL, JPM, MSFT).
    await user.click(symbolHeader);
    await waitFor(() => {
      expect(screen.getAllByRole("row")[1]).toHaveTextContent("AAPL");
    });
    // Second click toggles to DESC — MSFT first.
    await user.click(symbolHeader);
    await waitFor(() => {
      expect(screen.getAllByRole("row")[1]).toHaveTextContent("MSFT");
    });
  });
});

// ---------------------------------------------------------------------------
// Exchange label (#1904)
// ---------------------------------------------------------------------------

describe("InstrumentsPage — exchange label", () => {
  it("renders the human exchange name, not the raw eToro id", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("AAPL")).toBeInTheDocument();
    });
    const table = screen.getByRole("table");
    // exchange_name "Nasdaq" is shown; the raw id "4" never appears.
    expect(within(table).getAllByText("Nasdaq").length).toBeGreaterThanOrEqual(
      1,
    );
    expect(within(table).queryByText("4")).not.toBeInTheDocument();
  });

  it("falls back to the raw id when exchange_name is null", async () => {
    mockedFetch.mockResolvedValue(
      makeResponse({
        items: [
          {
            instrument_id: 9,
            symbol: "NEWX",
            company_name: "Newly Listed Co.",
            exchange: "99",
            exchange_name: null,
            currency: "USD",
            sector: null,
            gics_sector: null,
            sector_spdr: null,
            is_tradable: true,
            coverage_tier: 3,
            latest_quote: null,
            day_change_pct: null,
            day_change_as_of: null,
          },
        ],
        total: 1,
      }),
    );
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("NEWX")).toBeInTheDocument();
    });
    expect(
      within(screen.getByRole("table")).getByText("99"),
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Uncovered-row treatment (#1924 dir #3)
// ---------------------------------------------------------------------------

describe("InstrumentsPage — uncovered rows", () => {
  function uncoveredItem(overrides = {}) {
    return {
      instrument_id: 42,
      symbol: "ZZZ",
      company_name: "Unmapped Co.",
      exchange: "12",
      exchange_name: null,
      currency: "USD",
      sector: null,
      gics_sector: null,
      sector_spdr: null,
      is_tradable: true,
      coverage_tier: null,
      latest_quote: null,
      day_change_pct: null,
      day_change_as_of: null,
      ...overrides,
    };
  }

  it("collapses an all-dashes row into a muted 'No coverage yet' note", async () => {
    mockedFetch.mockResolvedValue(
      makeResponse({ items: [uncoveredItem()], total: 1 }),
    );
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("ZZZ")).toBeInTheDocument();
    });
    // Symbol still links; the trailing columns collapse to one note.
    expect(screen.getByText("No coverage yet")).toBeInTheDocument();
    expect(screen.getByText("ZZZ").closest("a")).toHaveAttribute(
      "href",
      "/instrument/ZZZ",
    );
  });

  it("treats a persisted last=0 quote as no usable price (prevention-log #1428)", async () => {
    mockedFetch.mockResolvedValue(
      makeResponse({
        items: [
          uncoveredItem({
            latest_quote: {
              bid: 0,
              ask: 0,
              last: 0,
              spread_pct: null,
              quoted_at: "2026-04-08T12:00:00Z",
            },
          }),
        ],
        total: 1,
      }),
    );
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("ZZZ")).toBeInTheDocument();
    });
    // A non-null zero must not render a fake "US$0.00"; the row is uncovered.
    expect(screen.getByText("No coverage yet")).toBeInTheDocument();
    expect(screen.queryByText(/0\.00/)).not.toBeInTheDocument();
  });

  it("does NOT collapse a row that still has a sector (partial coverage)", async () => {
    mockedFetch.mockResolvedValue(
      makeResponse({
        items: [
          uncoveredItem({ gics_sector: "Financials", sector_spdr: "XLF" }),
        ],
        total: 1,
      }),
    );
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("ZZZ")).toBeInTheDocument();
    });
    expect(screen.queryByText("No coverage yet")).not.toBeInTheDocument();
    expect(
      within(screen.getByRole("table")).getByText("Financials"),
    ).toBeInTheDocument();
  });
});
