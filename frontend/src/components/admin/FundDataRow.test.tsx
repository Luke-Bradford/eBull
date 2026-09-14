/**
 * #3050 — the layer-backed cells must keep four operator facts apart.
 *
 * The defect being prevented is a COLLAPSE, not a missing value: "never ran",
 * "stopped N days ago", "not wired yet" and "status unavailable" all rendered
 * as one `– endpoint pending` placeholder, so a thesis layer that had been
 * dead for 23 days read as a feature nobody had built.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { LayerHealthResponse } from "@/api/types";
import { FundDataRow } from "./FundDataRow";

function layer(over: Partial<LayerHealthResponse> & { layer: string }): LayerHealthResponse {
  return {
    status: "ok",
    latest: "2026-09-14T11:30:39Z",
    max_age_seconds: 172_800,
    age_seconds: 3_600,
    detail: "",
    ...over,
  };
}

function renderRow(
  layers: readonly LayerHealthResponse[] | null,
  layersError = false,
): void {
  render(
    <FundDataRow
      coverage={null}
      coverageError={false}
      recommendations={null}
      recommendationsError={false}
      layers={layers}
      layersError={layersError}
    />,
  );
}

/** The cell's rendered value sits in the sibling div under its label. */
function valueFor(label: string): string {
  const labelNode = screen.getByText(label);
  return labelNode.nextElementSibling?.textContent ?? "";
}

function cellFor(label: string): HTMLElement {
  return screen.getByText(label).parentElement as HTMLElement;
}

describe("FundDataRow — layer-backed cells (#3050)", () => {
  it("renders a fresh layer as its timestamp with no staleness hint", () => {
    renderRow([layer({ layer: "scores" }), layer({ layer: "theses" })]);
    expect(valueFor("Latest score")).not.toBe("–");
    expect(cellFor("Latest score").textContent).not.toContain("stale");
  });

  it("renders a STALE layer as its timestamp plus the measured age, not as a placeholder", () => {
    // The production case on 2026-09-14: theses last wrote 2026-08-22.
    renderRow([
      layer({ layer: "scores" }),
      layer({
        layer: "theses",
        status: "stale",
        latest: "2026-08-22T19:47:53Z",
        max_age_seconds: 259_200,
        age_seconds: 1_977_000,
      }),
    ]);
    const cell = cellFor("Latest thesis");
    // ⚠ The timestamp must still render. Blanking a stale value to "–" is the
    // defect inverted — it re-asserts "not built" about a layer that ran.
    expect(valueFor("Latest thesis")).not.toBe("–");
    expect(cell.textContent).toContain("stale");
    // Age and SLA are DERIVED from the payload; asserted as computed values so
    // a hardcoded day count cannot creep back in.
    expect(cell.textContent).toContain(`${Math.floor(1_977_000 / 86_400)}d`);
    expect(cell.textContent).toContain(`SLA ${Math.floor(259_200 / 86_400)}d`);
  });

  it("renders an EMPTY layer as 'never', not as a placeholder", () => {
    renderRow([layer({ layer: "scores", status: "empty", latest: null })]);
    expect(valueFor("Latest score")).toBe("never");
  });

  it("renders a layer ABSENT from the response as unknown, never as healthy", () => {
    renderRow([layer({ layer: "scores" })]);
    expect(cellFor("Latest thesis").textContent).toContain("layer not reported");
  });

  it("renders an in-flight fetch as a bare placeholder with no claim about the layer", () => {
    renderRow(null);
    expect(valueFor("Latest score")).toBe("–");
    expect(cellFor("Latest score").textContent).not.toContain("layer not reported");
    expect(cellFor("Latest score").textContent).not.toContain("never");
  });

  it("renders a failed status fetch distinctly from an in-flight one", () => {
    renderRow(null, true);
    expect(cellFor("Latest score").textContent).toContain("status unavailable");
  });

  it("drops the Tier 1/2/3 placeholder and leaves six cells", () => {
    renderRow([layer({ layer: "scores" }), layer({ layer: "theses" })]);
    expect(screen.queryByText("Tier 1/2/3")).not.toBeInTheDocument();
    const row = screen.getByTestId("fund-data-row");
    expect(row.children).toHaveLength(6);
    // Six divides every breakpoint's track count evenly; seven did not.
    expect(row.className).toContain("sm:grid-cols-3");
    expect(row.className).toContain("lg:grid-cols-6");
  });
});
