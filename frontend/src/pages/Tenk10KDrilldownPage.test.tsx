import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { Tenk10KDrilldownPage } from "@/pages/Tenk10KDrilldownPage";
import * as api from "@/api/instruments";

describe("Tenk10KDrilldownPage", () => {
  it("renders three panes: TOC, body with embedded table, metadata rail", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValue({
      symbol: "GME",
      source_accession: "0001326380-26-000001",
      cik: "0001326380",
      sections: [
        {
          section_order: 0,
          section_key: "general",
          section_label: "General",
          body: "We sell games. ␞TABLE_0␞ Stores worldwide.",
          cross_references: [],
          tables: [
            {
              order: 0,
              headers: ["Segment", "Stores"],
              rows: [
                ["United States", "1,598"],
                ["Europe", "308"],
              ],
            },
          ],
        },
      ],
    });
    vi.spyOn(api, "fetchTenKHistory").mockResolvedValue({
      symbol: "GME",
      filings: [
        {
          accession_number: "0001326380-26-000001",
          filing_date: "2026-03-24",
          filing_type: "10-K",
        },
        {
          accession_number: "0001326380-25-000001",
          filing_date: "2025-03-24",
          filing_type: "10-K",
        },
      ],
    });

    render(
      <MemoryRouter initialEntries={["/instrument/GME/filings/10-k"]}>
        <Routes>
          <Route
            path="/instrument/:symbol/filings/10-k"
            element={<Tenk10KDrilldownPage />}
          />
        </Routes>
      </MemoryRouter>,
    );

    // "General" appears in both the TOC link and the section heading — use findAllByText
    const generals = await screen.findAllByText("General");
    expect(generals.length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("United States")).toBeInTheDocument();
    expect(screen.getByText("1,598")).toBeInTheDocument();
    expect(screen.getByText("2025")).toBeInTheDocument(); // prior 10-K rail entry
    // Sentinel string must not leak to the visible text
    expect(screen.queryByText(/TABLE_0/)).toBeNull();
  });

  it("renders sections while history is still loading (#564)", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValue({
      symbol: "GME",
      source_accession: "0001326380-26-000001",
      cik: "0001326380",
      sections: [
        {
          section_order: 0,
          section_key: "general",
          section_label: "General",
          body: "We sell games.",
          cross_references: [],
          tables: [],
        },
      ],
    });
    // History never resolves — sections must still render.
    vi.spyOn(api, "fetchTenKHistory").mockReturnValue(
      new Promise<api.TenKHistoryResponse>(() => {
        /* never resolves */
      }),
    );

    render(
      <MemoryRouter initialEntries={["/instrument/GME/filings/10-k"]}>
        <Routes>
          <Route
            path="/instrument/:symbol/filings/10-k"
            element={<Tenk10KDrilldownPage />}
          />
        </Routes>
      </MemoryRouter>,
    );

    // Body content for the section is visible despite history being pending.
    expect(await screen.findByText("We sell games.")).toBeInTheDocument();
    // Prior 10-K rail content (which only appears when history resolves) is absent.
    expect(screen.queryByText(/Prior 10-Ks/i)).toBeNull();
  });
});
