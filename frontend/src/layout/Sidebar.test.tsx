import { render, screen } from "@testing-library/react";
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

describe("Sidebar", () => {
  it("is one flat list in drill-down order: Dashboard, Portfolio, Calendar, Strategies", () => {
    renderAt("/");
    const links = screen.getAllByRole("link").filter((link) => link.closest("nav"));
    expect(links.slice(0, 4).map((link) => link.textContent)).toEqual([
      "Dashboard",
      "Portfolio",
      "Calendar",
      "Strategies",
    ]);
    expect(links[0]).toHaveAttribute("href", "/");
    expect(screen.queryByRole("link", { name: "Invest" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Advanced/ })).not.toBeInTheDocument();
  });

  it("marks Dashboard active only on the root path", () => {
    renderAt("/strategies");
    expect(screen.getByRole("link", { name: "Dashboard" })).not.toHaveAttribute("aria-current");
    expect(screen.getByRole("link", { name: "Strategies" })).toHaveAttribute("aria-current", "page");
  });
});
