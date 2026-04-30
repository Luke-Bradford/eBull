import { describe, beforeEach, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import * as api from "@/api/instruments";

const navigateMock = vi.fn();
vi.mock("react-router-dom", async (importActual) => {
  const actual = (await importActual()) as object;
  return { ...actual, useNavigate: () => navigateMock };
});

describe("BusinessSectionsTeaser", () => {
  beforeEach(() => {
    navigateMock.mockReset();
    vi.restoreAllMocks();
  });

  it("renders teaser + Pane chrome with sec_10k_item1 source", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      sections: [{ heading: "General", level: 2, body: "ACME Corp makes widgets globally." }],
      source_accession: null,
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/ACME Corp/)).toBeInTheDocument();
    // providerLabel("sec_10k_item1") === "SEC 10-K Item 1"
    expect(screen.getByText(/SEC 10-K Item 1/)).toBeInTheDocument();
  });

  it("Open button navigates to /instrument/<symbol>/filings/10-k", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      sections: [{ heading: "General", level: 2, body: "Test body." }],
      source_accession: null,
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    const btn = await screen.findByRole("button", { name: /open/i });
    await userEvent.click(btn);
    expect(navigateMock).toHaveBeenCalledWith("/instrument/GME/filings/10-k");
  });

  it("renders the no_item_1 distinct empty state when parse_status.state is no_item_1 (#648)", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      symbol: "GME",
      source_accession: null,
      cik: null,
      sections: [],
      parse_status: {
        state: "no_item_1",
        failure_reason: "no_item_1_marker",
        next_retry_at: null,
        last_attempted_at: "2026-04-01T00:00:00Z",
      },
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/10-K has no Item 1/i)).toBeInTheDocument();
    // Generic legacy copy must NOT appear when the parse_status shape is set.
    expect(screen.queryByText(/No 10-K Item 1 on file/i)).not.toBeInTheDocument();
  });

  it("renders the parse_failed empty state with reason + retry timestamps (#648)", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      symbol: "GME",
      source_accession: null,
      cik: null,
      sections: [],
      parse_status: {
        state: "parse_failed",
        failure_reason: "parse_exception",
        next_retry_at: "2026-04-29T03:00:00Z",
        last_attempted_at: "2026-04-28T03:00:00Z",
      },
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/parse failed/i)).toBeInTheDocument();
    expect(screen.getByText(/parse_exception/)).toBeInTheDocument();
    expect(screen.getByText(/2026-04-28 03:00 UTC/)).toBeInTheDocument();
    expect(screen.getByText(/2026-04-29 03:00 UTC/)).toBeInTheDocument();
  });

  it("renders the not_attempted empty state for fresh instruments (#648)", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      symbol: "GME",
      source_accession: null,
      cik: null,
      sections: [],
      parse_status: {
        state: "not_attempted",
        failure_reason: null,
        next_retry_at: null,
        last_attempted_at: null,
      },
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/not yet parsed/i)).toBeInTheDocument();
  });

  it("renders the sections_pending empty state when body is set but splitter hasn't run (#648)", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      symbol: "GME",
      source_accession: null,
      cik: null,
      sections: [],
      parse_status: {
        state: "sections_pending",
        failure_reason: null,
        next_retry_at: null,
        last_attempted_at: "2026-04-29T01:00:00Z",
      },
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/Sections pending/i)).toBeInTheDocument();
  });

  it("renders up to three section cards as a 3-column grid", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      symbol: "GME",
      source_accession: null,
      cik: null,
      sections: [
        {
          section_order: 1,
          section_key: "general",
          section_label: "General",
          body: "ACME Corp makes widgets globally.",
          cross_references: [],
          tables: [],
        },
        {
          section_order: 2,
          section_key: "products",
          section_label: "Products",
          body: "We build the best widgets in the market.",
          cross_references: [],
          tables: [],
        },
        {
          section_order: 3,
          section_key: "markets",
          section_label: "Markets",
          body: "We sell to retail and institutional buyers worldwide.",
          cross_references: [],
          tables: [],
        },
        {
          section_order: 4,
          section_key: "competition",
          section_label: "Competition",
          body: "We face several large competitors.",
          cross_references: [],
          tables: [],
        },
      ],
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    // First three section labels render (4th truncated).
    expect(await screen.findByText("General")).toBeInTheDocument();
    expect(screen.getByText("Products")).toBeInTheDocument();
    expect(screen.getByText("Markets")).toBeInTheDocument();
    expect(screen.queryByText("Competition")).not.toBeInTheDocument();
    // Body teasers render alongside their labels.
    expect(screen.getByText(/ACME Corp/)).toBeInTheDocument();
    expect(screen.getByText(/best widgets/)).toBeInTheDocument();
    expect(screen.getByText(/retail and institutional/)).toBeInTheDocument();
  });

  it("falls back to the legacy generic empty state when the API omits parse_status", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValueOnce({
      symbol: "GME",
      source_accession: null,
      cik: null,
      sections: [],
    } as never);
    render(
      <MemoryRouter>
        <BusinessSectionsTeaser symbol="GME" />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/No 10-K Item 1 on file/i)).toBeInTheDocument();
  });
});
