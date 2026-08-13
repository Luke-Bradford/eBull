import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { CapabilityOverridesPage } from "@/pages/CapabilityOverridesPage";
import * as api from "@/api/capabilityOverrides";

const driftResponse = {
  checked_at: "2026-06-29T10:00:00+00:00",
  total_overrides: 1,
  rows: [
    {
      exchange_id: "33",
      exchange_name: "Regular Trading Hours - RTH",
      asset_class: "us_equity",
      diffs: [
        {
          capability: "filings",
          seed_providers: ["sec_edgar"],
          current_providers: [],
        },
        {
          capability: "ownership",
          seed_providers: ["sec_13f", "sec_13d_13g"],
          current_providers: ["sec_13f", "custom_provider"],
        },
      ],
    },
  ],
} as never;

function renderPage() {
  render(
    <MemoryRouter initialEntries={["/admin/capability-overrides"]}>
      <CapabilityOverridesPage />
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("CapabilityOverridesPage", () => {
  it("renders one row per diverging capability with seed + current providers", async () => {
    vi.spyOn(api, "fetchCapabilityOverrides").mockResolvedValue(driftResponse);

    renderPage();

    // Exchange identity surfaces — one cell per diverging capability
    // (flat table repeats the exchange across its diff rows).
    expect(
      await screen.findAllByText("Regular Trading Hours - RTH"),
    ).toHaveLength(2);
    expect(screen.getAllByText("#33")).toHaveLength(2);

    // Both diverging capabilities render as rows.
    expect(screen.getByText("filings")).toBeInTheDocument();
    expect(screen.getByText("ownership")).toBeInTheDocument();

    // Seed providers shown; the missing-from-current one is the drift.
    expect(screen.getByText("sec_edgar")).toBeInTheDocument();
    // A provider present on both sides appears in both cells.
    expect(screen.getAllByText("sec_13f").length).toBeGreaterThanOrEqual(1);
    // An operator-added provider (not in seed) surfaces on the current side.
    expect(screen.getByText("custom_provider")).toBeInTheDocument();

    // Empty current set renders an explicit none marker, not a blank cell.
    expect(screen.getByText("— (none)")).toBeInTheDocument();
  });

  it("shows the all-at-seed empty state when nothing diverges", async () => {
    vi.spyOn(api, "fetchCapabilityOverrides").mockResolvedValue({
      checked_at: "2026-06-29T10:00:00+00:00",
      total_overrides: 0,
      rows: [],
    } as never);

    renderPage();

    expect(
      await screen.findByText(/Every exchange is at its seed default/),
    ).toBeInTheDocument();
  });

  it("surfaces a retry control when the fetch fails", async () => {
    vi.spyOn(api, "fetchCapabilityOverrides").mockRejectedValue(
      new Error("500"),
    );

    renderPage();

    expect(
      await screen.findByRole("button", { name: /retry/i }),
    ).toBeInTheDocument();
  });
});
