import { describe, expect, it } from "vitest";
import { render } from "@testing-library/react";
import { Sparkline } from "@/components/instrument/Sparkline";

describe("Sparkline", () => {
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
