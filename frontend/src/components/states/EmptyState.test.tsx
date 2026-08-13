/**
 * Smoke test for the Vitest + RTL scaffold (#92).
 *
 * Deliberately renders a real component through React Testing Library
 * rather than asserting a tautology. This validates the full chain in
 * one shot:
 *   - jsdom environment
 *   - the `@/` path alias matches the production vite config
 *   - `@testing-library/react` render + query helpers work
 *   - `@testing-library/jest-dom` matchers are wired via setup.ts
 *
 * Future component tests should follow the same import shape (named
 * `describe` / `it` / `expect` from `vitest`, no globals).
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { EmptyState } from "@/components/states/EmptyState";

describe("EmptyState", () => {
  it("renders the title and description", () => {
    render(<EmptyState title="Nothing here" description="Try again later." />);

    expect(screen.getByRole("heading", { name: "Nothing here" })).toBeInTheDocument();
    expect(screen.getByText("Try again later.")).toBeInTheDocument();
  });

  it("omits the description block when no description is provided", () => {
    render(<EmptyState title="Bare" />);

    expect(screen.getByRole("heading", { name: "Bare" })).toBeInTheDocument();
    expect(screen.queryByText("Try again later.")).not.toBeInTheDocument();
  });
});
