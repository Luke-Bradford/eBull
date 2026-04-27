import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { PaneHeader } from "./PaneHeader";

describe("PaneHeader", () => {
  it("renders title only with no optional props", () => {
    render(<PaneHeader title="Recent filings" />);
    expect(screen.getByRole("heading", { name: /recent filings/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /open/i })).not.toBeInTheDocument();
  });

  it("renders scope text when provided", () => {
    render(<PaneHeader title="Insider activity" scope="last 90 days" />);
    expect(screen.getByText("last 90 days")).toBeInTheDocument();
  });

  it("renders provider source label", () => {
    render(
      <PaneHeader
        title="Recent filings"
        source={{ providers: ["sec_edgar"] }}
      />,
    );
    expect(screen.getByText(/SEC EDGAR/)).toBeInTheDocument();
  });

  it("renders Open button only when onExpand is defined and calls it on click", async () => {
    const onExpand = vi.fn();
    render(<PaneHeader title="Filings" onExpand={onExpand} />);
    const btn = screen.getByRole("button", { name: /open/i });
    await userEvent.click(btn);
    expect(onExpand).toHaveBeenCalledOnce();
  });
});
