import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { Pane } from "./Pane";

describe("Pane", () => {
  it("renders header title and body content", () => {
    render(
      <Pane title="Recent filings">
        <p>row content</p>
      </Pane>,
    );
    expect(screen.getByRole("heading", { name: /recent filings/i })).toBeInTheDocument();
    expect(screen.getByText("row content")).toBeInTheDocument();
  });

  it("does not attach onClick to the outer article (button-only drill)", async () => {
    const onExpand = vi.fn();
    render(
      <Pane title="Filings" onExpand={onExpand}>
        <p>body</p>
      </Pane>,
    );
    // Clicking the body must NOT trigger onExpand.
    await userEvent.click(screen.getByText("body"));
    expect(onExpand).not.toHaveBeenCalled();
    // Clicking the Open button MUST trigger onExpand.
    await userEvent.click(screen.getByRole("button", { name: /open/i }));
    expect(onExpand).toHaveBeenCalledOnce();
  });
});
