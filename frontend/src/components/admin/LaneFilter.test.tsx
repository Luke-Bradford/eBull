import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { LaneFilter } from "@/components/admin/LaneFilter";

describe("LaneFilter", () => {
  it("renders all lane chips plus 'All'", () => {
    render(<LaneFilter selected={null} counts={{}} onSelect={() => {}} />);
    for (const label of [
      "All",
      "Setup",
      "Universe",
      "Candles",
      "SEC",
      "Ownership",
      "Fundamentals",
      "Ops",
      "AI",
    ]) {
      expect(screen.getByRole("button", { name: new RegExp(label, "i") }))
        .toBeTruthy();
    }
  });

  it("shows count alongside each lane chip", () => {
    render(
      <LaneFilter
        selected={null}
        counts={{ sec: 4, ownership: 2 }}
        onSelect={() => {}}
      />,
    );
    expect(screen.getByRole("button", { name: /SEC.*4/ })).toBeTruthy();
    expect(screen.getByRole("button", { name: /Ownership.*2/ })).toBeTruthy();
  });

  it("invokes onSelect with the lane on chip click", () => {
    const onSelect = vi.fn();
    render(
      <LaneFilter selected={null} counts={{}} onSelect={onSelect} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /^SEC/ }));
    expect(onSelect).toHaveBeenCalledWith("sec");
  });

  it("clicking the selected lane again deselects (returns null)", () => {
    const onSelect = vi.fn();
    render(<LaneFilter selected="sec" counts={{}} onSelect={onSelect} />);
    fireEvent.click(screen.getByRole("button", { name: /^SEC/ }));
    expect(onSelect).toHaveBeenCalledWith(null);
  });

  it("'All' chip resets selection to null", () => {
    const onSelect = vi.fn();
    render(<LaneFilter selected="sec" counts={{}} onSelect={onSelect} />);
    fireEvent.click(screen.getByRole("button", { name: /^All/ }));
    expect(onSelect).toHaveBeenCalledWith(null);
  });
});
