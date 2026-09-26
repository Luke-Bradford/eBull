import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { Sidebar } from "@/layout/Sidebar";

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Sidebar />
    </MemoryRouter>,
  );
}

describe("Sidebar (#3423)", () => {
  it("leads with Invest and folds everything else under Advanced on the landing page", async () => {
    renderAt("/");
    expect(screen.getByRole("link", { name: "Invest" })).toHaveAttribute("href", "/");
    expect(screen.queryByRole("link", { name: "Dashboard" })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Advanced/ }));
    expect(screen.getByRole("link", { name: "Dashboard" })).toHaveAttribute("href", "/dashboard");
    expect(screen.getByRole("link", { name: "Strategies" })).toBeInTheDocument();
  });

  it("keeps Advanced open on any other page so the active item is never hidden", () => {
    renderAt("/portfolio");
    expect(screen.getByRole("button", { name: /Advanced/ })).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("link", { name: "Portfolio" })).toBeInTheDocument();
  });
});
