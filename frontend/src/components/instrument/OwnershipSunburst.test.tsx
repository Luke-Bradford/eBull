/**
 * Unit tests for the #920 static-polish layer of the ownership
 * sunburst: residual hatching, residual tooltip copy, center
 * known-coverage line, and the legend's residual row.
 *
 * Sector-level DOM (the actual pie arcs) is NOT asserted here —
 * jsdom gives ResponsiveContainer a zero-size box so Recharts
 * renders no sectors. The gap/residual flag placement is covered at
 * the data layer via the exported pure ``buildSunburstChartData``;
 * the copy via the exported pure ``residualTooltipText``. The parts
 * of the component that live OUTSIDE ResponsiveContainer (center
 * label, hatch ``<pattern>`` def, legend) render fine in jsdom and
 * are asserted directly.
 */

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { lightTheme } from "@/lib/chartTheme";

import type { SunburstInputs } from "./ownershipRings";
import {
  OwnershipLegend,
  OwnershipSunburst,
  RESIDUAL_LABEL,
  buildSunburstChartData,
  buildSunburstRings,
  residualTooltipText,
} from "./OwnershipSunburst";

/** 1B outstanding; institutions report 600M with one 400M named
 *  filer → 200M within-category gap; 400M residual. */
function baseInputs(overrides: Partial<SunburstInputs> = {}): SunburstInputs {
  return {
    total_shares: 1_000_000_000,
    holders: [
      {
        key: "0000102909",
        label: "Vanguard Group",
        shares: 400_000_000,
        category: "institutions",
      },
    ],
    institutions_total: 600_000_000,
    etfs_total: null,
    insiders_total: null,
    blockholders_total: null,
    treasury_shares: null,
    ...overrides,
  };
}

describe("residualTooltipText", () => {
  it("renders the exact spec'd copy", () => {
    expect(residualTooltipText(400_000_000, 0.4)).toBe(
      "Public / unattributed: 40.00% of outstanding — 400,000,000 shares not attributed to any disclosed filer.",
    );
  });
});

describe("buildSunburstChartData", () => {
  it("marks residual wedges is_residual and within-category gaps not", () => {
    const rings = buildSunburstRings(baseInputs());
    expect(rings).not.toBeNull();
    const { middleData, outerData } = buildSunburstChartData(rings!, lightTheme);

    const middleResidual = middleData.find((d) => d.id === "middle-residual");
    expect(middleResidual).toMatchObject({
      name: RESIDUAL_LABEL,
      shares: 400_000_000,
      is_gap: true,
      is_residual: true,
      target: null,
    });

    const outerResidual = outerData.find((d) => d.id === "outer-residual");
    expect(outerResidual).toMatchObject({
      name: RESIDUAL_LABEL,
      is_gap: true,
      is_residual: true,
      target: null,
    });

    const categoryGap = outerData.find((d) => d.id === "institutions-gap");
    expect(categoryGap).toMatchObject({
      shares: 200_000_000,
      is_gap: true,
      is_residual: false,
      target: null,
    });

    const known = middleData.find((d) => d.id === "cat-institutions");
    expect(known).toMatchObject({ is_gap: false, is_residual: false });
  });

  it("emits no residual datums when categories cover the denominator", () => {
    const rings = buildSunburstRings(
      baseInputs({ institutions_total: 1_000_000_000 }),
    );
    const { middleData, outerData } = buildSunburstChartData(rings!, lightTheme);
    expect(middleData.find((d) => d.id === "middle-residual")).toBeUndefined();
    expect(outerData.find((d) => d.id === "outer-residual")).toBeUndefined();
  });
});

describe("OwnershipSunburst center label", () => {
  it("shows the known-coverage line under the share count", () => {
    render(<OwnershipSunburst inputs={baseInputs()} />);
    expect(screen.getByText("Total shares")).toBeInTheDocument();
    expect(screen.getByText("1,000,000,000")).toBeInTheDocument();
    // 600M known of 1B outstanding.
    expect(screen.getByText("60.00% known coverage")).toBeInTheDocument();
  });

  it("reads 100% against the bumped denominator when oversubscribed", () => {
    // Category totals (1.5B) oversubscribe reported outstanding (1B):
    // buildSunburstRings bumps the denominator to sum_known, residual
    // clamps to 0, and the label honestly reads 100% — the
    // panel-level OversubscribedWarning carries the diagnostic
    // (spec D5 / Codex ckpt-1 Medium).
    render(
      <OwnershipSunburst
        inputs={baseInputs({ institutions_total: 1_500_000_000 })}
      />,
    );
    expect(screen.getByText("1,500,000,000")).toBeInTheDocument();
    expect(screen.getByText("100.00% known coverage")).toBeInTheDocument();
  });

  it("renders the hatch pattern def", () => {
    const { container } = render(<OwnershipSunburst inputs={baseInputs()} />);
    const pattern = container.querySelector("pattern");
    expect(pattern).not.toBeNull();
    expect(pattern!.id).toMatch(/^residual-hatch-/);
  });
});

describe("OwnershipLegend residual row", () => {
  it("labels the residual row and hatches its swatch", () => {
    const rings = buildSunburstRings(baseInputs());
    const { container } = render(<OwnershipLegend rings={rings!} />);
    expect(screen.getByText(RESIDUAL_LABEL)).toBeInTheDocument();
    const swatches = container.querySelectorAll("li span[aria-hidden]");
    const hatched = Array.from(swatches).filter(
      (s) =>
        s instanceof HTMLElement &&
        s.style.background.includes("repeating-linear-gradient"),
    );
    expect(hatched).toHaveLength(1);
  });

  it("omits the residual row at full coverage", () => {
    const rings = buildSunburstRings(
      baseInputs({ institutions_total: 1_000_000_000 }),
    );
    render(<OwnershipLegend rings={rings!} />);
    expect(screen.queryByText(RESIDUAL_LABEL)).toBeNull();
  });
});
