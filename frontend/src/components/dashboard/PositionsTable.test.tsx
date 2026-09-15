import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { PortfolioMirrorItem } from "@/api/types";
import { PositionsTable } from "@/components/dashboard/PositionsTable";

/**
 * #3084 — the dashboard carries its OWN mirror renderer, a near-copy of
 * `PortfolioPage.tsx::MirrorRow`. It had no test file, which is how the two
 * drifted apart in the first place: a fix written for one renderer does not
 * travel to its twin unless something asserts on the twin.
 */
function mirror(overrides: Partial<PortfolioMirrorItem> = {}): PortfolioMirrorItem {
  return {
    mirror_id: 42,
    parent_username: "@gurutrader",
    active: true,
    funded: 1000,
    mirror_equity: 1200,
    unrealized_pnl: 200,
    closed_pnl: 0,
    position_count: 5,
    started_copy_date: "2026-01-01",
    ...overrides,
  };
}

function renderTable(m: PortfolioMirrorItem) {
  render(
    <MemoryRouter>
      <PositionsTable
        positions={[]}
        mirrors={[m]}
        displayCurrency="GBP"
        cashCurrency="GBP"
      />
    </MemoryRouter>,
  );
}

describe("PositionsTable mirror row", () => {
  it.each([
    { label: "positive closed, negative unrealised", closed: 615.38, unrealised: -226.09 },
    { label: "negative closed, positive unrealised", closed: -1514.74, unrealised: 350.68 },
    { label: "both negative", closed: -50, unrealised: -25 },
    { label: "zero closed still renders", closed: 0, unrealised: 200 },
  ])("shows the closed P&L and labels the P&L unrealised — $label", ({ closed, unrealised }) => {
    renderTable(mirror({ closed_pnl: closed, unrealized_pnl: unrealised }));

    const cell = screen.getByTestId("dashboard-mirror-closed-pnl-42");
    expect(cell.textContent).toMatch(/closed P&L/);
    // Coloured by its OWN sign. Every case pairs opposite signs, so a cell that
    // reused the unrealised figure's `positive` passes one case and fails another.
    expect(cell.className).toMatch(closed >= 0 ? /text-emerald-600/ : /text-red-600/);
    expect(screen.getByText("unrealised")).toBeTruthy();
  });
});
