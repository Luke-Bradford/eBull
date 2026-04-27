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
});
