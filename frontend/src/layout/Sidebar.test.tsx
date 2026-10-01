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

describe("Sidebar (#3516)", () => {
  it("is one flat list led by Strategies, with no Invest item and no Advanced group", () => {
    renderAt("/strategies");
    const links = screen.getAllByRole("link").filter((link) => link.closest("nav"));
    expect(links[0]).toHaveTextContent("Strategies");
    expect(links[0]).toHaveAttribute("href", "/strategies");
    expect(screen.getByRole("link", { name: "Dashboard" })).toHaveAttribute("href", "/dashboard");
    expect(screen.getByRole("link", { name: "Portfolio" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Invest" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Advanced/ })).not.toBeInTheDocument();
  });
});
