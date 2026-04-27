/**
 * Tests for Sparkline (#576 Phase 2 adds hover tooltip).
 */
import { describe, expect, it } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { Sparkline } from "@/components/instrument/Sparkline";

describe("Sparkline — rendering", () => {
  it("renders <polyline> with 8 comma-separated coords for 8 input values", () => {
    const { container } = render(
      <Sparkline values={[1, 2, 3, 4, 5, 4, 3, 2]} width={80} height={24} />,
    );
    const polyline = container.querySelector("polyline");
    expect(polyline).not.toBeNull();
    const points = polyline?.getAttribute("points") ?? "";
    const coords = points.trim().split(/\s+/);
    expect(coords).toHaveLength(8);
    for (const c of coords) {
      expect(c).toMatch(/^\d+(?:\.\d+)?,\d+(?:\.\d+)?$/);
    }
  });

  it("renders an empty <svg> with no <polyline> when given fewer than 2 values", () => {
    const { container } = render(<Sparkline values={[42]} />);
    const polyline = container.querySelector("polyline");
    expect(polyline).toBeNull();
  });

  it("uses currentColor as default stroke", () => {
    const { container } = render(<Sparkline values={[1, 2, 3]} />);
    const polyline = container.querySelector("polyline");
    expect(polyline?.getAttribute("stroke")).toBe("currentColor");
  });
});

describe("Sparkline — hover tooltip", () => {
  it("shows tooltip with value on mouseMove over the SVG", () => {
    // values: [10, 20, 30], width 80 → xStep = 40
    // Moving to x=0 → index 0 → value 10
    const { container } = render(
      <Sparkline values={[10, 20, 30]} width={80} height={24} />,
    );
    const svg = container.querySelector("svg")!;
    // Simulate mousemove at x=0 (left edge → index 0 → value 10)
    fireEvent.mouseMove(svg, { clientX: 0, clientY: 12 });
    const tooltip = screen.queryByTestId("sparkline-tooltip");
    expect(tooltip).not.toBeNull();
    expect(tooltip?.textContent).toMatch(/10/);
  });

  it("shows the correct value at the right edge (last index)", () => {
    // values: [10, 20, 30], width 80 → last valid index x=80
    const { container } = render(
      <Sparkline values={[10, 20, 30]} width={80} height={24} />,
    );
    const svg = container.querySelector("svg")!;
    // Move to far right → index 2 → value 30
    fireEvent.mouseMove(svg, { clientX: 80, clientY: 12 });
    const tooltip = screen.queryByTestId("sparkline-tooltip");
    expect(tooltip).not.toBeNull();
    expect(tooltip?.textContent).toMatch(/30/);
  });

  it("clears tooltip on mouseLeave", () => {
    const { container } = render(
      <Sparkline values={[10, 20, 30]} width={80} height={24} />,
    );
    const svg = container.querySelector("svg")!;
    const wrapper = container.querySelector("div")!;
    fireEvent.mouseMove(svg, { clientX: 0, clientY: 12 });
    expect(screen.queryByTestId("sparkline-tooltip")).not.toBeNull();
    fireEvent.mouseLeave(wrapper);
    expect(screen.queryByTestId("sparkline-tooltip")).toBeNull();
  });

  it("uses the custom formatValue prop", () => {
    const { container } = render(
      <Sparkline
        values={[1000000, 2000000]}
        width={80}
        height={24}
        formatValue={(v) => `$${(v / 1e6).toFixed(1)}M`}
      />,
    );
    const svg = container.querySelector("svg")!;
    fireEvent.mouseMove(svg, { clientX: 0, clientY: 12 });
    const tooltip = screen.queryByTestId("sparkline-tooltip");
    expect(tooltip?.textContent).toBe("$1.0M");
  });

  it("does not show tooltip when fewer than 2 values (no polyline case)", () => {
    const { container } = render(<Sparkline values={[42]} />);
    const svg = container.querySelector("svg")!;
    // No onMouseMove in short-circuit path — but if we fire anyway, no tooltip
    fireEvent.mouseMove(svg, { clientX: 0, clientY: 0 });
    expect(screen.queryByTestId("sparkline-tooltip")).toBeNull();
  });
});
