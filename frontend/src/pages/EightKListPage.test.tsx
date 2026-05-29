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

const deferredFiling = {
  accession_number: "acc-def",
  document_type: "8-K",
  is_amendment: false,
  date_of_report: "2026-03-15",
  reporting_party: "GameStop Corp.",
  signature_name: null,
  signature_title: null,
  signature_date: null,
  primary_document_url: null,
  body_deferred: true,
  items: [
    {
      item_code: "5.02",
      item_label: "Departure of Officer",
      severity: "high",
      body: "",
    },
  ],
  exhibits: [],
};

const filledFiling = {
  ...deferredFiling,
  body_deferred: false,
  items: [
    {
      item_code: "5.02",
      item_label: "Departure of Officer",
      severity: "high",
      body: "The CFO stepped down effective immediately.",
    },
  ],
};

function renderPage() {
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
}

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

    // Click the OTHER row (acc-2, low severity) — selection moves
    fireEvent.click(screen.getByText("2025-12-04"));
    await waitFor(() => {
      expect(screen.getByText("acc-2")).toBeTruthy();
    });

    // Filter to high severity — acc-2 (low) disappears AND URL
    // clears the now-stale accession; auto-select picks acc-1.
    const severitySelect = screen.getByRole("combobox");
    fireEvent.change(severitySelect, { target: { value: "high" } });
    await waitFor(() => {
      expect(screen.queryByText("8.01")).toBeNull();
    });
    await waitFor(() => {
      expect(screen.getByText("acc-1")).toBeTruthy();
    });
  });

  it("lazily fetches the body when a deferred filing is selected (#1343)", async () => {
    vi.spyOn(api, "fetchEightKFilings").mockResolvedValue({
      symbol: "GME",
      filings: [deferredFiling],
    } as never);
    const bodySpy = vi
      .spyOn(api, "fetchEightKFilingBody")
      .mockResolvedValue(filledFiling as never);

    renderPage();

    // Auto-select fires the lazy body fetch for the deferred row.
    await waitFor(() => {
      expect(bodySpy).toHaveBeenCalledWith("GME", "acc-def");
    });
    // Filled body renders in the detail panel once the fetch resolves.
    expect(
      await screen.findByText(/CFO stepped down effective immediately/),
    ).toBeInTheDocument();
  });

  it("does not fetch a body for a non-deferred filing (#1343)", async () => {
    vi.spyOn(api, "fetchEightKFilings").mockResolvedValue({
      symbol: "GME",
      filings: [{ ...deferredFiling, body_deferred: false }],
    } as never);
    const bodySpy = vi
      .spyOn(api, "fetchEightKFilingBody")
      .mockResolvedValue(filledFiling as never);

    renderPage();

    // Detail panel renders the auto-selected row (accession shown).
    expect(await screen.findByText("acc-def")).toBeInTheDocument();
    // No lazy body fetch — the rail copy already carries the bodies.
    expect(bodySpy).not.toHaveBeenCalled();
  });

  it("does not leak a fetched body onto a newly selected non-deferred row (#1343)", async () => {
    const filingB = {
      ...deferredFiling,
      body_deferred: false,
      accession_number: "acc-b",
      date_of_report: "2025-01-01",
      items: [
        {
          item_code: "8.01",
          item_label: "Other events",
          severity: "low",
          body: "BBB other-events body.",
        },
      ],
    };
    vi.spyOn(api, "fetchEightKFilings").mockResolvedValue({
      symbol: "GME",
      filings: [deferredFiling, filingB],
    } as never);
    vi.spyOn(api, "fetchEightKFilingBody").mockResolvedValue(
      filledFiling as never,
    );

    renderPage();

    // Auto-select fills the deferred first row.
    expect(
      await screen.findByText(/CFO stepped down effective immediately/),
    ).toBeInTheDocument();

    // Switch to the non-deferred row B: its body shows, A's filled body
    // must not linger (the accession-gated fallback, Codex ckpt2).
    fireEvent.click(screen.getByText("2025-01-01"));
    expect(
      await screen.findByText(/BBB other-events body/),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/CFO stepped down effective immediately/),
    ).toBeNull();
  });

  it("shows a retry control (not a stuck spinner) when the deferred body fetch fails (#1343)", async () => {
    vi.spyOn(api, "fetchEightKFilings").mockResolvedValue({
      symbol: "GME",
      filings: [deferredFiling],
    } as never);
    const bodySpy = vi
      .spyOn(api, "fetchEightKFilingBody")
      .mockRejectedValueOnce(new Error("503"))
      .mockResolvedValueOnce(filledFiling as never);

    renderPage();

    // Transient failure surfaces the SectionError retry, not an endless skeleton.
    const retry = await screen.findByRole("button", { name: /retry/i });
    expect(screen.getByText(/Failed to load/i)).toBeInTheDocument();

    // Retrying re-attempts the fetch and renders the filled body.
    fireEvent.click(retry);
    await waitFor(() => {
      expect(bodySpy).toHaveBeenCalledTimes(2);
    });
    expect(
      await screen.findByText(/CFO stepped down effective immediately/),
    ).toBeInTheDocument();
  });
});
