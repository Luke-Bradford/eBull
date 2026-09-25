import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { HEURISTIC_TOOLTIP, HeuristicBadge } from "@/components/rankings/HeuristicBadge";

describe("HeuristicBadge", () => {
  it("names the row's model version and says it is not validated", () => {
    render(<HeuristicBadge modelVersion="v1.5-balanced" />);
    const badge = screen.getByTestId("heuristic-badge");
    expect(badge).toHaveTextContent("v1.5-balanced heuristic · not validated");
    expect(badge).toHaveAttribute("title", HEURISTIC_TOOLTIP);
  });

  it("does not invent a version when the surface has none", () => {
    render(<HeuristicBadge modelVersion={null} />);
    expect(screen.getByTestId("heuristic-badge")).toHaveTextContent(
      "ranking heuristic · not validated",
    );
  });

  it("compact form keeps the full caveat in the tooltip", () => {
    render(<HeuristicBadge compact />);
    const badge = screen.getByTestId("heuristic-badge");
    expect(badge).toHaveTextContent(/^heuristic$/);
    expect(badge).toHaveAttribute("title", HEURISTIC_TOOLTIP);
  });
});
