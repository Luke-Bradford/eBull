import { describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { EightKListPage } from "@/pages/EightKListPage";
import * as api from "@/api/instruments";

const filings = [
  {
    accession_number: "acc-1",
    document_type: "8-K",
    is_amendment: false,
    date_of_report: "2026-03-15",
    reporting_party: "GameStop Corp.",
    signature_name: null,
    signature_title: null,
    signature_date: null,
    primary_document_url: null,
    items: [
      {
        item_code: "5.02",
        item_label: "Departure of Officer",
        severity: "high",
        body: "CFO out.",
      },
    ],
    exhibits: [],
  },
  {
    accession_number: "acc-2",
    document_type: "8-K",
    is_amendment: false,
    date_of_report: "2025-12-04",
    reporting_party: "GameStop Corp.",
    signature_name: null,
    signature_title: null,
    signature_date: null,
    primary_document_url: null,
    items: [
      {
        item_code: "8.01",
        item_label: "Other events",
        severity: "low",
        body: "Dividend.",
      },
    ],
    exhibits: [],
  },
] as never;

describe("EightKListPage", () => {
  it("auto-selects first row on page open and respects filter changes", async () => {
    vi.spyOn(api, "fetchEightKFilings").mockResolvedValue({
      symbol: "GME",
      filings,
    } as never);

    render(
      <MemoryRouter initialEntries={["/instrument/GME/filings/8-k"]}>
        <Routes>
          <Route
            path="/instrument/:symbol/filings/8-k"
            element={<EightKListPage />}
          />
        </Routes>
      </MemoryRouter>,
    );

    // Both rows render after data loads
    expect(await screen.findByText("5.02")).toBeTruthy();
    expect(screen.getByText("8.01")).toBeTruthy();

    // First row (acc-1) is auto-selected on page open
    await waitFor(() => {
      expect(screen.getByText("acc-1")).toBeTruthy();
    });

    // Filter to high severity — low-severity row disappears
    const severitySelect = screen.getByRole("combobox");
    fireEvent.change(severitySelect, { target: { value: "high" } });
    await waitFor(() => {
      expect(screen.queryByText("8.01")).toBeNull();
    });

    // acc-1 still selected after filter (it matches high severity)
    expect(screen.getByText("acc-1")).toBeTruthy();

    // Click a row to change selection
    fireEvent.click(screen.getByText("2026-03-15"));
    await waitFor(() => {
      expect(screen.getByText("acc-1")).toBeTruthy();
    });
  });
});
