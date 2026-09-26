import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { RankDeltaCell } from "@/components/rankings/RankDeltaCell";

// rank_delta = prior_rank − current_rank (scoring.py::compute_rankings).
describe("RankDeltaCell", () => {
  it("reads a positive delta as moving up the table", () => {
    render(<RankDeltaCell delta={4} />);
    expect(screen.getByText("improved by 4")).toBeInTheDocument();
    expect(screen.getByText("▲ 4")).toBeInTheDocument();
  });

  it("reads a negative delta as moving down the table", () => {
    render(<RankDeltaCell delta={-3} />);
    expect(screen.getByText("worsened by 3")).toBeInTheDocument();
    expect(screen.getByText("▼ 3")).toBeInTheDocument();
  });

  it("says when there is no prior rank", () => {
    render(<RankDeltaCell delta={null} />);
    expect(screen.getByText("no prior rank")).toBeInTheDocument();
  });
});
